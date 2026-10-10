//! Streaming-decode support types for the paid pull path (#1120, ADR 038).
//!
//! [`PullReader`] adapts a live [`UpstreamPull`] to the bao decoder's
//! byte-reader trait while [`StashedFault`] preserves the pull's typed faults
//! (stall, refusal, voucher rejection) so a misbehaving peer stays scoreable —
//! the decoder itself can only say "the bytes stopped arriving". The streaming
//! decode loops that consume these are
//! [`crate::ranged_store::ClientRangedStore::ingest_stream`], the node's cache
//! admit, and (`test-util`) the in-memory `stream_fetch*` wrappers.
//!
//! [`content_paid_frontier`] maps a paid WIRE-byte watermark back to the
//! content-byte frontier it covers, for the resume-at-paid-frontier gate.
//!
//! [`BlobCache`] is the injected `(hash, range)` cache the
//! [`Streamer`](crate::Streamer) consults and fills. It is pure — it names no
//! blob store — which is what keeps `decdn-client` `iroh-blobs`-free; the
//! default cache is the no-op [`NoCache`].

use bao_tree::io::DecodeError;
use bytes::{Bytes, BytesMut};
use decdn_bao_range::{CHUNK_GROUP_BYTES, align_range, bao_encoded_size};

use crate::{HashMismatch, UpstreamPull};

/// An [`iroh_io::AsyncStreamReader`] over a live [`UpstreamPull`], so the bao
/// decoder can pull wire bytes on demand instead of being handed a complete
/// buffer.
///
/// A reader never asks past the leg's wire end. The node sends its
/// `StreamEnd` only after the closing voucher, and that voucher goes out in
/// [`UpstreamPull::finish`] once the decode is done, so a read past the end
/// waits on a node that waits on the payer. The bao decoder reads exactly the
/// leg's items and no further.
///
/// # Why the error is stashed rather than propagated
///
/// The reader trait speaks `io::Error`, but `next_chunk` produces the typed
/// errors the reputation layer classifies on (`PullStalled`, `PullTimeout`,
/// `UpstreamRefused`, `UpstreamVoucherRejected`). Collapsing those into an
/// `io::Error` would silently downgrade, say, a peer's mid-stream refusal into
/// an anonymous decode failure and stop it being scored. So the original
/// `anyhow::Error` is parked in `fault` and the trait returns a placeholder; the
/// decode loop that owns the reader
/// ([`crate::ranged_store::ClientRangedStore::ingest_stream`], or the `test-util`
/// in-memory decoder) checks `fault` first and returns the real error, using the
/// decoder's complaint only when there is no parked fault.
#[derive(Debug)]
#[doc(hidden)]
pub struct PullReader {
    pull: UpstreamPull,
    /// Wire bytes received but not yet consumed by the decoder.
    buf: BytesMut,
    /// Wire bytes handed to the decoder so far. The decoder checks each item
    /// it reads (a parent pair or a leaf) against the root before it reads
    /// the next one, so every byte handed out before the current read has
    /// verified. Each read reports that count to the pull, which pays for it.
    consumed: u64,
    /// `StreamEnd` seen — no more chunks will arrive.
    ended: bool,
    /// The first `next_chunk` fault, preserved with its type. See the type docs.
    fault: Option<anyhow::Error>,
}

impl PullReader {
    pub(crate) fn new(pull: UpstreamPull) -> Self {
        Self {
            pull,
            buf: BytesMut::new(),
            consumed: 0,
            ended: false,
            fault: None,
        }
    }

    /// Recover the inner [`UpstreamPull`] once the decode loop is done with this
    /// reader, so [`crate::source::BlobSource::finish`] (or the `test-util`
    /// in-memory wrapper) can drain it to the stream end and recover the acked
    /// voucher watermark.
    ///
    /// Any buffered-but-unconsumed wire bytes and a parked fault are dropped
    /// silently: a caller only calls this after the decode loop reached a clean
    /// end (`Done`, or stopped at a steal's split). The decoder checked every
    /// item it read on the way there, so every consumed byte is reported
    /// verified, and `finish`/`stop` pay for it.
    pub(crate) fn into_inner(mut self) -> UpstreamPull {
        self.pull.mark_verified(self.consumed);
        self.pull
    }

    /// Pull chunks until `buf` holds `n` bytes, the stream ends, or a fault is
    /// parked. The decoder asks for more only once it has checked the last item
    /// it read, so everything consumed so far is reported verified first, and
    /// the pull pays for it before it waits on the node for more.
    async fn fill_to(&mut self, n: usize) {
        self.pull.mark_verified(self.consumed);
        while !self.ended && self.fault.is_none() && self.buf.len() < n {
            match self.pull.next_chunk().await {
                Ok(Some(chunk)) => self.buf.extend_from_slice(&chunk),
                Ok(None) => self.ended = true,
                Err(e) => self.fault = Some(e),
            }
        }
    }

    /// Take up to `n` buffered bytes. A short read is the decoder's EOF signal,
    /// which it turns into `ParentNotFound`/`LeafNotFound` — the driver maps
    /// that to a truncation, not to corruption.
    fn take(&mut self, n: usize) -> Bytes {
        let take = self.buf.len().min(n);
        self.consumed = self.consumed.saturating_add(take as u64);
        self.buf.split_to(take).freeze()
    }
}

impl iroh_io::AsyncStreamReader for PullReader {
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        self.fill_to(len).await;
        Ok(self.take(len))
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        self.fill_to(L).await;
        let got = self.take(L);
        let mut out = [0u8; L];
        if got.len() < L {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "paid pull ended before the bao decoder's next fixed-size read",
            ));
        }
        out.copy_from_slice(&got);
        Ok(out)
    }
}

/// A reader that may be holding back a richer error than the one the bao decoder
/// will report.
///
/// The decoder can only say "the bytes stopped arriving". When the reader knows
/// WHY — a stalled peer, a mid-stream refusal, a rejected voucher — that reason
/// is what the caller needs in order to score the peer, so it wins.
///
/// Public because it is a supertrait of [`crate::source::BaoRangeReader`]: a
/// [`crate::source::BlobSource`]'s reader must preserve the pull's typed faults
/// so the gap-driven driver (#1608) surfaces them for scoring.
#[doc(hidden)]
pub trait StashedFault {
    /// Take the parked typed fault, if any. Returns `None` on a source whose
    /// bytes carry the whole story (an in-memory buffer, a scripted double).
    fn take_fault(&mut self) -> Option<anyhow::Error>;
}

impl StashedFault for PullReader {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        self.fault.take()
    }
}

/// A plain in-memory wire buffer has no out-of-band failure mode: whatever the
/// decoder says about it is the whole story.
///
/// Test-only, and gated to say so: it exists purely so the streaming ingest
/// loop can be driven from a fixed wire buffer. A test using it exercises the
/// `None` branch, where the decoder's error is the whole story, so it must not
/// be the only coverage of the fault-precedence rule.
#[cfg(test)]
impl StashedFault for Bytes {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        None
    }
}

/// Split a bao decode failure into "the peer lied" and "the stream ended early",
/// the taxonomy every decoding consumer and the node-side tee share (ADR 038).
/// Only a hash mismatch is provably corruption; a truncated stream is
/// transport-class and must not tar the peer as a liar.
///
/// `pub(crate)`: [`crate::ranged_store::ClientRangedStore::ingest_stream`] and the
/// in-memory `stream_fetch*` decoder reuse this exact classification rather than
/// duplicating the match.
pub(crate) fn classify_decode_error(err: DecodeError) -> anyhow::Error {
    match err {
        DecodeError::ParentHashMismatch(_) | DecodeError::LeafHashMismatch(_) => {
            anyhow::Error::new(HashMismatch)
        }
        DecodeError::Io(io_err) => anyhow::Error::new(io_err).context("bao decode read failed"),
        not_found @ (DecodeError::ParentNotFound(_) | DecodeError::LeafNotFound(_)) => {
            anyhow::anyhow!("bao stream truncated mid-tree: {not_found}")
        }
    }
}

/// Map a **paid wire-byte watermark** back to the **content-byte frontier** it
/// covers, for a stream that began at content offset `fetch_start` of a
/// `total_bytes`-byte blob (#1497).
///
/// Vouchers pay for **wire** bytes — bao-encoded content PLUS interleaved proof
/// (ADR 038 §Payment metering) — so `paid_wire` (the lane-cumulative bytes an
/// ACCEPTED voucher covered, minus this stream's baseline) is a WIRE quantity.
/// Resuming at `fetch_start + paid_wire` would treat it as a CONTENT offset and,
/// since wire ≥ content, overshoot the true content paid-frontier: the content
/// span between the real frontier and that overshoot would never be billed on the
/// resumed leg (a systematic under-pay), and a large overshoot can even exceed the
/// on-disk content length. This maps `paid_wire` back into content space instead.
///
/// # What it returns, and why it never under-pays
///
/// The result is the **largest chunk-group boundary `C` in `[fetch_start,
/// total_bytes]` whose bao wire cost is fully within `paid_wire`** — i.e. the
/// greatest `C` with `wire([fetch_start, C)) <= paid_wire`, where `wire(range)` is
/// [`bao_encoded_size`] over the real `total_bytes` tree (the identical walk the
/// serve-side encoder and `aligned_wire_len` use, so it equals the bytes actually
/// on the wire, byte-for-byte).
///
/// Because a bao pre-order stream emits every chunk group's proof-and-data before
/// it descends into any later subtree, the wire position at which content offset
/// `C` finishes is exactly `wire([fetch_start, C))` (same tree, same pre-order
/// walk, same node set up to `C`; later subtrees' interior nodes are emitted only
/// after `C`). So `wire([fetch_start, C)) <= paid_wire` proves every byte needed
/// to deliver content `[fetch_start, C)` landed inside the paid wire prefix — that
/// content is paid for. Resuming at `C` therefore re-fetches only `[C, end)`;
/// nothing past a paid frontier is skipped, so it **cannot under-pay**. The only
/// slack is the sub-group tail between `C` and the true (possibly mid-group) paid
/// frontier: that content is re-fetched and re-paid once on the next leg, a
/// bounded over-pay of **strictly less than one chunk group** (16 KiB).
///
/// `fetch_start` MUST be a chunk-group boundary (every resume offset is — it comes
/// from a checkpointed group boundary or a previous call to this function). A `paid_wire` of
/// `0`, or a `total_bytes` of `0`, yields `fetch_start` (nothing paid on this leg
/// ⇒ resume where it began).
#[must_use]
#[doc(hidden)]
pub fn content_paid_frontier(fetch_start: u64, total_bytes: u64, paid_wire: u64) -> u64 {
    // Wire cost of content `[fetch_start, c)` over the full `total_bytes` tree.
    // `align_range` bound-checks and snaps to groups; `wire_len` is
    // `bao_encoded_size(total_bytes, ranges)` — the exact on-wire byte count. An
    // out-of-range `c` (only reachable if `fetch_start >= total_bytes`, which a
    // live mid-fetch never is) maps to `u64::MAX`, excluding it from the search.
    let wire_to = |c: u64| -> u64 {
        if c <= fetch_start {
            return 0;
        }
        let len = c.saturating_sub(fetch_start);
        align_range(fetch_start, len, total_bytes).map_or(u64::MAX, |r| {
            bao_encoded_size(total_bytes, r.chunk_ranges())
        })
    };

    let span = total_bytes.saturating_sub(fetch_start);
    if paid_wire == 0 || span == 0 {
        return fetch_start.min(total_bytes);
    }
    // Candidate content frontiers are the group boundaries `C(k) = fetch_start +
    // k·GROUP`, capped at `total_bytes` (the final partial group). `wire_to` is
    // monotonic non-decreasing in `C`, so binary-search the largest `k` that fits.
    let c_of = |k: u64| -> u64 {
        k.saturating_mul(CHUNK_GROUP_BYTES)
            .saturating_add(fetch_start)
            .min(total_bytes)
    };
    let k_max = span.div_ceil(CHUNK_GROUP_BYTES);
    let (mut lo, mut hi, mut best) = (0u64, k_max, 0u64);
    while lo <= hi {
        let mid = lo.saturating_add((hi - lo) / 2);
        if wire_to(c_of(mid)) <= paid_wire {
            best = mid;
            lo = mid.saturating_add(1);
        } else if mid == 0 {
            break;
        } else {
            hi = mid.saturating_sub(1);
        }
    }
    c_of(best)
}

/// A boxed, `Send` future returned by the output/cache trait methods below —
/// the same boxed-future async-trait shape [`crate::source::SourceFuture`] uses
/// on the input side, so a sink or cache can be held as `&dyn`.
pub type SinkFuture<'a, T> =
    core::pin::Pin<Box<dyn core::future::Future<Output = anyhow::Result<T>> + Send + 'a>>;

/// An injected content cache the `Streamer` consults and fills, keyed by
/// `(hash, offset)` over content bytes.
///
/// Pure by design: the trait names no blob store, which is what keeps
/// `decdn-client` `iroh-blobs`-free — a blob-store-backed implementation lives
/// ABOVE this crate and is injected. The default is the no-op [`NoCache`] (never
/// hits, never stores), so a caller that wants no revisit-reuse pays nothing.
///
/// On a clean finish the `Streamer` tees the whole verified blob to
/// [`put`](BlobCache::put). On a revisit it asks [`get`](BlobCache::get) for the
/// whole blob: a whole-blob hit is served straight from the cache with no fetch,
/// and anything less is treated as a miss and refetched in full — the store's
/// gap-driven resume, not the cache, is what avoids re-pulling a partial prefix
/// today. (Serving a cached PARTIAL prefix and fetching only the complement is a
/// planned enhancement; [`get`](BlobCache::get) already reports a partial hold as
/// a miss so an implementer need not stitch fragments.)
pub trait BlobCache: Send + Sync {
    /// Whether this cache actually stores what it is given. A caller that would
    /// have to materialize the whole blob just to tee it here (the `Streamer`'s
    /// revisit tee) skips that work — and the memory it costs — when this is
    /// `false`. Defaults to `true`; a no-op cache overrides it.
    fn caches(&self) -> bool {
        true
    }

    /// Return cached content bytes for exactly `[offset, offset + len)` of
    /// `hash`, or `None` on a miss. A partial hold is a miss — the caller fetches
    /// the whole range rather than stitching a fragment.
    ///
    /// # Errors
    ///
    /// Whatever the backing cache raises. A miss is `Ok(None)`, not an error.
    fn get(&self, hash: [u8; 32], offset: u64, len: u64) -> SinkFuture<'_, Option<Bytes>>;

    /// Store verified content `bytes` as `[offset, offset + bytes.len())` of
    /// `hash`. A cache that does not want the range may drop it.
    ///
    /// # Errors
    ///
    /// Whatever the backing cache raises.
    fn put(&self, hash: [u8; 32], offset: u64, bytes: Bytes) -> SinkFuture<'_, ()>;
}

/// The default [`BlobCache`]: never caches. Every [`get`](BlobCache::get) misses
/// and every [`put`](BlobCache::put) discards, so a `Streamer` built with it
/// always fetches the whole range and stores nothing. A caller that wants
/// revisit-reuse injects a real cache above `decdn-client`.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCache;

impl BlobCache for NoCache {
    fn caches(&self) -> bool {
        false
    }

    fn get(&self, _hash: [u8; 32], _offset: u64, _len: u64) -> SinkFuture<'_, Option<Bytes>> {
        Box::pin(async { Ok(None) })
    }

    fn put(&self, _hash: [u8; 32], _offset: u64, _bytes: Bytes) -> SinkFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// Test double: an in-memory BlobCache for the `Streamer` and cache unit tests.
// ---------------------------------------------------------------------------

/// The exact key an in-memory [`BlobCache`] entry hits on: `(hash, offset, len)`.
#[cfg(test)]
type CacheKey = ([u8; 32], u64, u64);

/// An in-memory [`BlobCache`] that stores each `put` under its exact
/// `(hash, offset, len)` key and hits only on an identical key. The minimum a
/// `Streamer` cache test needs to prove complement-only fetching.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct MemoryBlobCache {
    entries: std::sync::Mutex<std::collections::HashMap<CacheKey, Bytes>>,
}

#[cfg(test)]
impl MemoryBlobCache {
    /// An empty cache.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
impl BlobCache for MemoryBlobCache {
    fn get(&self, hash: [u8; 32], offset: u64, len: u64) -> SinkFuture<'_, Option<Bytes>> {
        Box::pin(async move {
            let entries = self
                .entries
                .lock()
                .map_err(|_| anyhow::anyhow!("memory cache lock poisoned"))?;
            Ok(entries.get(&(hash, offset, len)).cloned())
        })
    }

    fn put(&self, hash: [u8; 32], offset: u64, bytes: Bytes) -> SinkFuture<'_, ()> {
        Box::pin(async move {
            let len = u64::try_from(bytes.len())
                .map_err(|_| anyhow::anyhow!("cached range length exceeds u64"))?;
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| anyhow::anyhow!("memory cache lock poisoned"))?;
            entries.insert((hash, offset, len), bytes);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;
