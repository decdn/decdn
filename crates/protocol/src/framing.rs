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

/// Per-ALPN maximum framed message size (ADR 013). The 16 MiB ceiling bounds
/// per-stream allocation from a malicious peer.
pub const MAX_MESSAGE_SIZE: u32 = 16 * 1024 * 1024;

/// Errors produced by the framing helpers. The handler-layer mapping to QUIC
/// application error codes lives in the node crate.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("frame I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame length {0} exceeds MAX_MESSAGE_SIZE ({MAX_MESSAGE_SIZE})")]
    TooLarge(u32),
    #[error("malformed varint length prefix")]
    Varint,
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

async fn write_varint_u32<W: AsyncWrite + Unpin>(w: &mut W, mut value: u32) -> std::io::Result<()> {
    let mut buf = [0u8; 5];
    let mut idx = 0usize;
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            if let Some(slot) = buf.get_mut(idx) {
                *slot = byte;
            }
            idx += 1;
            break;
        }
        if let Some(slot) = buf.get_mut(idx) {
            *slot = byte | 0x80;
        }
        idx += 1;
    }
    w.write_all(buf.get(..idx).unwrap_or(&[])).await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn roundtrip_varint(v: u32) {
        let mut buf = Vec::new();
        write_varint_u32(&mut buf, v).await.unwrap_or_default();
        let mut cursor = std::io::Cursor::new(buf);
        let decoded = read_varint_u32(&mut cursor).await.ok();
        assert_eq!(decoded, Some(v), "varint roundtrip {v}");
    }

    #[tokio::test]
    async fn varint_edges() {
        for v in [
            0u32,
            1,
            127,
            128,
            16_383,
            16_384,
            2_097_151,
            2_097_152,
            u32::MAX,
        ] {
            roundtrip_varint(v).await;
        }
    }

    #[tokio::test]
    async fn varint_rejects_oversize_continuation() {
        // 5 bytes all with continuation set → overflow
        let bytes = [0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF];
        let mut cursor = std::io::Cursor::new(bytes);
        let r = read_varint_u32(&mut cursor).await;
        assert!(matches!(r, Err(FrameError::Varint)));
    }

    #[tokio::test]
    async fn varint_rejects_5th_byte_overflow_bits() {
        // 5 bytes, last byte has high data bits set beyond u32 range.
        let bytes = [0x80u8, 0x80, 0x80, 0x80, 0x10];
        let mut cursor = std::io::Cursor::new(bytes);
        let r = read_varint_u32(&mut cursor).await;
        assert!(matches!(r, Err(FrameError::Varint)));
    }

    #[tokio::test]
    async fn frame_roundtrip() {
        let payload = b"hello deCDN".to_vec();
        let mut buf = Vec::new();
        write_frame(&mut buf, &payload).await.ok();
        let mut cursor = std::io::Cursor::new(buf);
        let got = read_frame(&mut cursor).await.ok();
        assert_eq!(got.as_deref(), Some(payload.as_slice()));
    }

    #[tokio::test]
    async fn frame_rejects_too_large_without_reading_payload() {
        // Encode a varint for MAX+1 and follow with NO payload bytes.
        let mut header = Vec::new();
        write_varint_u32(&mut header, MAX_MESSAGE_SIZE + 1)
            .await
            .ok();
        let header_len = header.len();
        let mut cursor = std::io::Cursor::new(header);
        let r = read_frame(&mut cursor).await;
        assert!(matches!(r, Err(FrameError::TooLarge(n)) if n == MAX_MESSAGE_SIZE + 1));
        // No payload bytes were read.
        assert_eq!(usize::try_from(cursor.position()).ok(), Some(header_len));
    }

    #[tokio::test]
    async fn frame_short_read_errors() {
        // Varint says 10 bytes, but stream only has 3.
        let mut buf = Vec::new();
        write_varint_u32(&mut buf, 10).await.ok();
        buf.extend_from_slice(b"abc");
        let mut cursor = std::io::Cursor::new(buf);
        let r = read_frame(&mut cursor).await;
        assert!(matches!(r, Err(FrameError::Io(_))));
    }

    #[tokio::test]
    async fn encode_decode_roundtrip() -> Result<(), FrameError> {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
        struct M {
            a: u64,
            b: String,
        }
        let m = M {
            a: 42,
            b: "hi".into(),
        };
        let bytes = encode_message(&m)?;
        let (decoded, rest) = decode_message::<M>(&bytes)?;
        assert_eq!(decoded, m);
        assert!(rest.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn frame_matches_postcard_length() {
        // Compatibility sanity: postcard serializing a u32 as a standalone value
        // uses the same varint encoding we emit for the length prefix.
        let mut ours = Vec::new();
        write_varint_u32(&mut ours, 300).await.ok();
        let theirs = postcard::to_allocvec(&300u32).unwrap_or_default();
        assert_eq!(ours, theirs);
    }

    #[tokio::test]
    async fn empty_payload_encodes_single_zero_byte() {
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, &[]).await.ok();
        assert_eq!(buf, vec![0u8]);
    }
}
