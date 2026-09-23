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
pub struct PullReader {
    pull: UpstreamPull,
    /// Wire bytes received but not yet consumed by the decoder.
    buf: BytesMut,
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
    /// silently: a caller only calls this after the decode loop reached its clean
    /// `Done` state.
    pub(crate) fn into_inner(self) -> UpstreamPull {
        self.pull
    }

    /// Pull chunks until `buf` holds `n` bytes, the stream ends, or a fault is
    /// parked.
    async fn fill_to(&mut self, n: usize) {
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
/// loop can be driven from a fixed wire buffer. Note that a test using it
/// exercises the `None` branch — the one this trait was invented to avoid — so
/// it must not be the only coverage of the fault-precedence rule.
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
/// (ADR 038 §Payment metering) — so `paid_wire` (the channel-cumulative bytes an
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
mod tests {
    use super::content_paid_frontier;
    use decdn_bao_range::{CHUNK_GROUP_BYTES, align_range, bao_encoded_size};

    /// Exact wire byte count to deliver content `[fetch_start, c)` of a
    /// `total`-byte blob — the same `bao_encoded_size` walk `content_paid_frontier`
    /// inverts.
    fn wire_to(fetch_start: u64, c: u64, total: u64) -> u64 {
        if c <= fetch_start {
            return 0;
        }
        align_range(fetch_start, c - fetch_start, total)
            .map_or(u64::MAX, |r| bao_encoded_size(total, r.chunk_ranges()))
    }

    /// `content_paid_frontier` must map a paid WIRE watermark to the LARGEST
    /// content group boundary whose wire cost is fully within it: never above
    /// (that would under-pay), and tight (the next group would exceed the paid
    /// wire). Swept across a blob spanning several groups and every plausible paid
    /// wire watermark.
    #[test]
    fn content_paid_frontier_never_over_maps_and_is_tight() {
        let group = CHUNK_GROUP_BYTES;
        // A few groups plus a partial tail, and a mid-blob resume start so the
        // `fetch_start > 0` arithmetic is exercised too.
        for total in [3 * group + 123, 6 * group, group + 1] {
            for fetch_start in [0u64, group, 2 * group] {
                if fetch_start >= total {
                    continue;
                }
                let full_wire = wire_to(fetch_start, total, total);
                // Sweep paid-wire watermarks across the whole stream, including
                // values that land mid-group and mid-proof.
                for step in 0..=40u64 {
                    let paid_wire = full_wire.saturating_mul(step) / 40;
                    let c = content_paid_frontier(fetch_start, total, paid_wire);

                    assert!(c >= fetch_start, "frontier must not regress below start");
                    assert!(c <= total, "frontier must not exceed the blob");
                    assert_eq!(
                        (c - fetch_start) % group,
                        if c == total {
                            (total - fetch_start) % group
                        } else {
                            0
                        },
                        "frontier is a group boundary (or the blob end)"
                    );
                    // No under-pay: every content byte up to `c` was inside the
                    // paid wire prefix.
                    assert!(
                        wire_to(fetch_start, c, total) <= paid_wire,
                        "c={c} maps past the paid wire {paid_wire} (would under-pay)"
                    );
                    // Tight: the NEXT group would have spilled past the paid wire
                    // (unless we are already at the blob end).
                    if c < total {
                        let next = (c + group).min(total);
                        assert!(
                            wire_to(fetch_start, next, total) > paid_wire,
                            "next group {next} still fits in {paid_wire}: not tight"
                        );
                    }
                }
            }
        }
    }

    /// The per-leg precondition, stated as a test (#1497 review).
    ///
    /// `content_paid_frontier` inverts the wire cost of ONE contiguous delivery
    /// starting at `fetch_start`. Its caller derives `paid_wire` from a
    /// CHANNEL-cumulative watermark, so if the `(fetch_start, baseline)` pair is
    /// not re-anchored when a new leg begins, the second call is handed the SUM of
    /// two independent bao range encodings against the first leg's start offset.
    ///
    /// That sum strictly exceeds the contiguous cost of the same span, so it can
    /// only map the frontier FORWARD — content delivered but never billed, and a
    /// `byte_offset` past the verified on-disk prefix.
    ///
    /// Two magnitudes, both covered here:
    ///
    /// * **Disjoint legs** — the excess is only the re-sent root->`fetch_start`
    ///   proof path (~64 B per tree level). Real, but usually smaller than the
    ///   16 KiB group the answer is snapped down to, so it often lands on the same
    ///   boundary. Correctness here rests on luck, not on the invariant, which is
    ///   why the assertion is the direction (never backwards) rather than a jump.
    /// * **A re-paid leg** — a resume retry re-delivers a span already paid for
    ///   (the ledger reseeds and the next attempt reopens at the same offset), so
    ///   the cumulative watermark climbs by a WHOLE duplicated span. That is
    ///   megabytes, not bytes, and it moves the frontier by entire groups.
    #[test]
    fn summed_multi_leg_wire_over_maps_which_is_why_baselines_are_per_leg() {
        let group = CHUNK_GROUP_BYTES;
        let total = 12 * group;
        // Leg 1 delivers [0, 3 groups); leg 2 resumes there and delivers to 6.
        let (frontier1, frontier2) = (3 * group, 6 * group);
        let leg1_wire = wire_to(0, frontier1, total);
        let leg2_wire = wire_to(frontier1, frontier2, total);
        let contiguous_wire = wire_to(0, frontier2, total);

        // The sum double-counts leg 2's re-sent left-boundary proof path.
        assert!(
            leg1_wire + leg2_wire > contiguous_wire,
            "the two legs' wire ({leg1_wire} + {leg2_wire}) must exceed the contiguous \
             cost of the same span ({contiguous_wire}), else this hazard would not exist"
        );

        // Correct (per-leg) call: anchored at leg 2's own start and budget.
        let correct = content_paid_frontier(frontier1, total, leg2_wire);
        assert_eq!(
            correct, frontier2,
            "the per-leg call lands on the true frontier"
        );

        // Stale baseline, disjoint legs: never maps BEHIND the true frontier, so it
        // can only skip billing, never re-bill.
        let stale = content_paid_frontier(0, total, leg1_wire + leg2_wire);
        assert!(
            stale >= frontier2,
            "a summed budget cannot under-report the frontier: {stale} < {frontier2}"
        );

        // Stale baseline with a RE-PAID leg: the ledger's cumulative wire includes
        // [0, 3 groups) twice — once for the abandoned attempt, once for the retry.
        // Anchored at the fetch's original start, that budget runs far past the
        // frontier actually paid for, skipping whole groups of billing.
        let with_repaid = content_paid_frontier(0, total, 2 * leg1_wire + leg2_wire);
        assert!(
            with_repaid > frontier2 + group,
            "a re-paid leg must visibly overshoot the true frontier by more than a \
             group: got {with_repaid}, true frontier {frontier2}"
        );
        // And the per-leg anchoring is immune to it: leg 2's own budget is unchanged
        // by whatever earlier legs re-paid for.
        assert_eq!(
            content_paid_frontier(frontier1, total, leg2_wire),
            frontier2,
            "per-leg anchoring is unaffected by an earlier leg being re-paid"
        );
    }

    /// `paid_wire == 0` (nothing accepted on this leg) resumes exactly where the
    /// leg began — no spurious advance.
    #[test]
    fn content_paid_frontier_zero_paid_stays_at_start() {
        let group = CHUNK_GROUP_BYTES;
        assert_eq!(content_paid_frontier(0, 5 * group, 0), 0);
        assert_eq!(content_paid_frontier(group, 5 * group, 0), group);
    }

    use bytes::Bytes;

    use super::{BlobCache, MemoryBlobCache, NoCache};

    /// The default [`NoCache`] never hits and discards every `put`, so a caller
    /// that injects no cache always fetches the whole range.
    #[tokio::test]
    async fn no_cache_always_misses_and_discards() -> anyhow::Result<()> {
        let cache = NoCache;
        let hash = [7u8; 32];
        cache.put(hash, 0, Bytes::from_static(b"hello")).await?;
        anyhow::ensure!(
            cache.get(hash, 0, 5).await?.is_none(),
            "the no-op cache must never hit"
        );
        Ok(())
    }

    /// The in-memory test cache round-trips one `(hash, range)`, and any other
    /// hash / offset / length misses (the caller then fetches the complement).
    #[tokio::test]
    async fn memory_cache_round_trips_a_hash_range() -> anyhow::Result<()> {
        let cache = MemoryBlobCache::new();
        let hash = [1u8; 32];
        let other = [2u8; 32];
        cache.put(hash, 16, Bytes::from_static(b"abcd")).await?;
        anyhow::ensure!(
            cache.get(hash, 16, 4).await?.as_deref() == Some(&b"abcd"[..]),
            "an exact (hash, range) hit returns the stored bytes"
        );
        anyhow::ensure!(
            cache.get(other, 16, 4).await?.is_none(),
            "wrong hash misses"
        );
        anyhow::ensure!(
            cache.get(hash, 0, 4).await?.is_none(),
            "wrong offset misses"
        );
        anyhow::ensure!(
            cache.get(hash, 16, 2).await?.is_none(),
            "wrong length misses"
        );
        Ok(())
    }
}
