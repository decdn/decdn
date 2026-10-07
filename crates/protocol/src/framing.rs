//! Length-prefixed wire framing for deCDN ALPNs (ADR 013).
//!
//! Every message on every QUIC stream is `varint(len) || postcard_bytes[0..len]`.
//! The varint uses postcard's native continuation-bit encoding (7 data bits per
//! byte, MSB = continuation, little-endian, 1–5 bytes for `u32`).
//!
//! Receivers read the varint length, reject anything above [`MAX_MESSAGE_SIZE`]
//! before allocation, allocate exactly that many bytes, then deserialize the
//! frame with [`decode_message`] (a thin wrapper over
//! [`postcard::take_from_bytes`] that preserves the trailing remainder for
//! future Tier-1 extension fields).

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum framed message size. Pinned at exactly 16 MiB by ADR 013 §Wire
/// Framing, uniformly across all ALPNs in v1.
///
/// The ceiling is a pre-allocation denial-of-service bound: [`read_frame`]
/// allocates exactly `len` bytes after decoding the length prefix, so an
/// unbounded length would let a peer force arbitrary-sized allocations. 16 MiB
/// sits well above every documented ALPN message (`cdn/probe/v1` ≤ ~200 B,
/// `cdn/client/v1` non-`ChunkData` ≤ ~1 KiB) and pairs with QUIC's
/// `MAX_STREAMS` (ADR 005) for the total per-peer memory bound.
///
/// Changing this constant is a protocol-wide wire-compatibility decision —
/// any edit, whether bump or shrink, requires an ADR 013 amendment first.
/// The compile-time guard below catches accidental edits. Per-deployment
/// tightening for memory-constrained nodes is a runtime-config concern
/// (ADR 013 §Wire Framing), not a change to the constant itself.
pub const MAX_MESSAGE_SIZE: u32 = 16 * 1024 * 1024;

// Compile-time guardrail tying MAX_MESSAGE_SIZE to ADR 013. Any change to
// the constant without an ADR 013 amendment fails the build.
const _: () = assert!(
    MAX_MESSAGE_SIZE == 16 * 1024 * 1024,
    "MAX_MESSAGE_SIZE must be exactly 16 MiB per ADR 013 §Wire Framing; any change (bump or shrink) requires an ADR amendment",
);

/// Errors produced by the framing helpers. The handler-layer mapping to QUIC
/// application error codes lives in the node crate.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// The underlying stream failed while reading or writing the frame.
    #[error("frame I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The length prefix names more bytes than [`MAX_MESSAGE_SIZE`] allows.
    /// Rejected before anything is buffered.
    #[error("frame length {0} exceeds MAX_MESSAGE_SIZE ({MAX_MESSAGE_SIZE})")]
    TooLarge(u32),
    /// The length prefix is not a well-formed varint — a non-terminating
    /// continuation run through all five bytes, or a fifth byte overflowing
    /// `u32`. A short or empty buffer reads as truncated, not malformed.
    #[error("malformed varint length prefix")]
    Varint,
    /// The frame's bytes are not a valid postcard encoding of the expected
    /// message.
    #[error("postcard decode error: {0}")]
    Decode(#[from] postcard::Error),
}

/// Serialize `msg` to a postcard buffer suitable for passing to [`write_frame`].
pub fn encode_message<M: Serialize>(msg: &M) -> Result<Vec<u8>, FrameError> {
    Ok(postcard::to_allocvec(msg)?)
}

/// Decode a frame into a message and the unconsumed remainder. The remainder
/// is reserved for Tier-1 extension fields (ADR 013 §Tier 1).
pub fn decode_message<M: DeserializeOwned>(frame: &[u8]) -> Result<(M, &[u8]), FrameError> {
    Ok(postcard::take_from_bytes::<M>(frame)?)
}

/// A top-level ALPN protocol enum (one per ALPN — [`crate::ProbeMessage`],
/// [`crate::ClientMessage`], [`crate::DhtMessage`]). Serialized as the
/// outermost postcard value in a frame, so its variant discriminant is the
/// frame's leading varint.
///
/// Implementors expose their declared-variant count so the framing layer can
/// tell an *unknown* discriminant (a variant this build does not know —
/// ADR 013 `UNSUPPORTED_MESSAGE`, `0x01`) apart from a genuine parse fault
/// (`MALFORMED_MESSAGE`, `0x03`) after a failed [`decode_message`].
pub trait TopLevelEnum {
    /// Number of declared variants. Postcard assigns discriminants
    /// `0..VARIANT_COUNT` in declaration order, so any leading discriminant
    /// `>= VARIANT_COUNT` names a variant this build does not know.
    const VARIANT_COUNT: u32;
}

/// Classify a failed top-level-enum decode per ADR 013: return `true` iff the
/// frame's leading discriminant names a variant at or beyond
/// `M::VARIANT_COUNT` — i.e. an unknown/unsupported variant the receiver must
/// answer with `UNSUPPORTED_MESSAGE` (`0x01`) rather than `MALFORMED_MESSAGE`
/// (`0x03`).
///
/// A frame that does not begin with a well-formed varint (empty, a
/// non-terminating continuation run, or a 5th byte that overflows `u32`) is a
/// genuine parse fault, so this returns `false` and the caller keeps the
/// `MALFORMED` classification. Callers should only consult this after
/// [`decode_message`] has actually failed; a valid in-range discriminant whose
/// inner payload is malformed also stays `MALFORMED` (this returns `false`).
pub fn is_unknown_variant<M: TopLevelEnum>(frame: &[u8]) -> bool {
    matches!(leading_varint_u32(frame), Some(d) if d >= M::VARIANT_COUNT)
}

/// Decode the leading postcard `u32` varint from `frame` (the top-level enum
/// discriminant) without consuming the remainder. Returns `None` when the
/// bytes do not begin with a well-formed varint — the synchronous mirror of
/// [`read_varint_u32`]'s validation, over an in-memory slice.
fn leading_varint_u32(frame: &[u8]) -> Option<u32> {
    let mut result: u32 = 0;
    for (i, &byte) in frame.iter().take(5).enumerate() {
        let data = u32::from(byte & 0x7F);
        // On the 5th byte (i == 4) only the low 4 bits are valid; reject overflow.
        if i == 4 && byte & 0x70 != 0 {
            return None;
        }
        let shift = u32::try_from(i).ok()?.saturating_mul(7);
        let shifted = data.checked_shl(shift)?;
        result |= shifted;
        if byte & 0x80 == 0 {
            return Some(result);
        }
    }
    None
}

/// Try to locate one complete length-prefixed frame at the front of `buf`
/// without consuming it, returning `(header_len, payload_len)` — the byte
/// counts of the varint prefix and the payload it announces. Returns `Ok(None)`
/// when `buf` does not yet hold a complete frame (the varint is truncated, or
/// fewer than `payload_len` payload bytes have arrived).
///
/// This is the synchronous, non-consuming counterpart to [`read_frame`], for a
/// caller that fills a byte buffer incrementally with **cancellation-safe**
/// reads and must split frames off it after each top-up — e.g. the per-voucher
/// reader's read-ahead (#1486), which reads ahead under a timeout that
/// [`read_frame`] (built on `read_exact`) could not survive without losing
/// partial bytes. The
/// caller slices the payload as `buf[header_len..header_len + payload_len]`,
/// decodes it with [`decode_message`], then drains `header_len + payload_len`
/// bytes.
///
/// The announced length is validated against [`MAX_MESSAGE_SIZE`] as soon as the
/// varint is complete — before the caller waits for (or allocates) the payload —
/// mirroring [`read_frame`]'s pre-allocation denial-of-service bound.
///
/// # Errors
///
/// [`FrameError::Varint`] for a malformed length prefix (a non-terminating
/// continuation run within the first 5 bytes, or a 5th byte overflowing `u32`);
/// [`FrameError::TooLarge`] when the announced length exceeds
/// [`MAX_MESSAGE_SIZE`].
pub fn parse_frame(buf: &[u8]) -> Result<Option<(usize, usize)>, FrameError> {
    let Some((len, header_len)) = decode_varint_prefix(buf)? else {
        return Ok(None);
    };
    if len > MAX_MESSAGE_SIZE {
        return Err(FrameError::TooLarge(len));
    }
    let payload_len = len as usize;
    if buf.len().saturating_sub(header_len) < payload_len {
        return Ok(None);
    }
    Ok(Some((header_len, payload_len)))
}

/// Decode a postcard `u32` varint from the front of `buf`, returning
/// `(value, bytes_consumed)`, `Ok(None)` when the varint is truncated (fewer
/// than its continuation bytes have arrived), or [`FrameError::Varint`] when the
/// bytes present are already malformed. The synchronous mirror of
/// [`read_varint_u32`].
fn decode_varint_prefix(buf: &[u8]) -> Result<Option<(u32, usize)>, FrameError> {
    let mut result: u32 = 0;
    for (i, &byte) in buf.iter().take(5).enumerate() {
        let data = u32::from(byte & 0x7F);
        // On the 5th byte (i == 4) only the low 4 bits are valid; reject overflow.
        if i == 4 && byte & 0x70 != 0 {
            return Err(FrameError::Varint);
        }
        let shift = u32::try_from(i)
            .map_err(|_| FrameError::Varint)?
            .saturating_mul(7);
        let shifted = data.checked_shl(shift).ok_or(FrameError::Varint)?;
        result |= shifted;
        if byte & 0x80 == 0 {
            return Ok(Some((result, i + 1)));
        }
    }
    // Ran out of buffer mid-varint (< 5 bytes, all continuations) → truncated;
    // exactly 5 continuation bytes with the terminator missing is malformed.
    if buf.len() >= 5 {
        return Err(FrameError::Varint);
    }
    Ok(None)
}

/// Read one length-prefixed frame from an async reader.
///
/// The length prefix is validated against [`MAX_MESSAGE_SIZE`] *before* any
/// payload allocation, so a malicious peer cannot trigger a large allocation
/// by sending a single oversized varint.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Vec<u8>, FrameError> {
    let len = read_varint_u32(r).await?;
    if len > MAX_MESSAGE_SIZE {
        return Err(FrameError::TooLarge(len));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Write one length-prefixed frame to an async writer.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    payload: &[u8],
) -> Result<(), FrameError> {
    let len = u32::try_from(payload.len()).map_err(|_| FrameError::TooLarge(u32::MAX))?;
    if len > MAX_MESSAGE_SIZE {
        return Err(FrameError::TooLarge(len));
    }
    write_varint_u32(w, len).await?;
    w.write_all(payload).await?;
    Ok(())
}

async fn read_varint_u32<R: AsyncRead + Unpin>(r: &mut R) -> Result<u32, FrameError> {
    let mut result: u32 = 0;
    for i in 0u32..5 {
        let mut b = [0u8; 1];
        r.read_exact(&mut b).await?;
        let byte = b[0];
        let data = u32::from(byte & 0x7F);
        let shift = i.saturating_mul(7);
        // On the 5th byte (i=4) only the low 4 bits are valid; reject overflow.
        if i == 4 && byte & 0x70 != 0 {
            return Err(FrameError::Varint);
        }
        let shifted = data.checked_shl(shift).ok_or(FrameError::Varint)?;
        result |= shifted;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }
    Err(FrameError::Varint)
}

/// Encode `value` as a postcard varint into the caller's stack buffer, returning
/// the number of bytes written (1..=5).
///
/// A `u32` encodes to at most 5 varint bytes (`ceil(32/7) = 5`), which is why the
/// buffer is a fixed `[u8; 5]` and why the count returned always fits it.
///
/// The `out.get_mut(idx)` guards exist to satisfy the `indexing_slicing` clippy lint.
/// Each one stops the encode rather than dropping a byte and counting it anyway: the
/// returned length is bytes *written*, never bytes intended, so no caller can be
/// handed a count that outruns the bytes behind it.
#[must_use]
pub(crate) fn encode_varint_u32(mut value: u32, out: &mut [u8; 5]) -> usize {
    let mut idx = 0usize;
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        let last = value == 0;
        let Some(slot) = out.get_mut(idx) else {
            break;
        };
        *slot = if last { byte } else { byte | 0x80 };
        idx = idx.saturating_add(1);
        if last {
            break;
        }
    }
    debug_assert!(idx <= 5, "u32 varint must fit in 5 bytes, idx={idx}");
    idx
}

async fn write_varint_u32<W: AsyncWrite + Unpin>(w: &mut W, value: u32) -> std::io::Result<()> {
    let mut buf = [0u8; 5];
    let idx = encode_varint_u32(value, &mut buf);
    // A length prefix that silently shortens to nothing is worse than a failed write:
    // `write_frame` would put the payload on the wire behind it, the peer would read
    // the payload's first byte as the length, and the stream — and the cumulative
    // wire-byte count vouchers are paid against — would desync with no error on
    // either side. Unreachable while `buf` is `[u8; 5]`; refuse rather than rely on it.
    let Some(bytes) = buf.get(..idx) else {
        return Err(std::io::Error::other(format!(
            "varint length prefix wants {idx} bytes but only {} were encoded",
            buf.len()
        )));
    };
    w.write_all(bytes).await
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod proptests;

#[cfg(test)]
mod tests;
