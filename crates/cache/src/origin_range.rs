//! Windowed origin range pulls (#2065): the requested span is fetched in bounded
//! windows of [`RANGE_PULL_WINDOW_BYTES`] against an `{H}.obao4` outboard that
//! the engine caches per hash, so a range pull holds `O(window + outboard)`
//! bytes whatever its length. Each origin read runs under a time budget
//! (see [`crate::CacheEngine::set_origin_read_budget`]).
//!
//! [`crate::CacheEngine::origin_range_wire`] (the own-origin serve-miss spine's
//! pull leg) builds on the `OriginRangeCursor` here. It needs ONE coherent
//! header-less wire for the whole draw — joined per-window encodings would
//! repeat the shared parent nodes — so it runs a single
//! [`bao_tree::io::fsm::encode_ranges_validated`] over an `OriginWindowReader`
//! that pulls the cursor's windows in order, and streams the output through a
//! bounded channel ([`OriginRangeWire`]).

use std::future::Future;
use std::io;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bao_tree::io::EncodeError;
use bao_tree::io::fsm::encode_ranges_validated;
use bao_tree::io::outboard::PreOrderMemOutboard;
use bao_tree::{BaoTree, ChunkRanges};
use bytes::{Bytes, BytesMut};
use iroh_blobs::Hash;
use iroh_io::{AsyncSliceReader, AsyncStreamWriter};
use tokio::sync::{OwnedSemaphorePermit, mpsc};

use crate::error::{CacheError, CacheResult};
use crate::metrics::CacheMetrics;
use crate::origin::{Origin, OriginKind, OriginRangeFetch, OriginRangeRequest};
use crate::outboard_cache::OutboardCache;

/// The time budget for one origin read of the range-pull path — an outboard
/// fetch or one data window. A read of `len` bytes gets
/// `head_start + len / min_bps`, so the budget scales with the read and never
/// caps the blob size (see [`crate::CacheEngine::set_origin_read_budget`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OriginReadBudget {
    /// Fixed allowance for the connection and the first byte.
    pub(crate) head_start: Duration,
    /// Average throughput, in bytes per second, the rest of the read must
    /// sustain. Never zero.
    pub(crate) min_bps: u64,
}

impl OriginReadBudget {
    /// The budget for a read of `len` bytes.
    pub(crate) fn for_len(self, len: u64) -> Duration {
        let nanos = (u128::from(len) * 1_000_000_000).div_ceil(u128::from(self.min_bps.max(1)));
        self.head_start.saturating_add(Duration::from_nanos(
            u64::try_from(nanos).unwrap_or(u64::MAX),
        ))
    }
}
use crate::range_pull::{AlignedRange, IROH_BLOCK_SIZE};

/// Largest data span one [`Origin::fetch_range_data`] call fetches. A multiple
/// of the 16 KiB chunk group, and windows start on the group-aligned
/// `fetch_start`, so no bao leaf straddles two windows. A range pull holds about
/// two windows (the fetched span and its encoding) plus the outboard, whatever
/// the requested length.
pub const RANGE_PULL_WINDOW_BYTES: u64 = 4 * 1024 * 1024;

/// [`crate::CacheEngine::origin_range_wire`] draws that run at once. A caller
/// past the bound waits for a permit rather than degrading, because the degrade
/// is a whole-blob origin pull — more egress, not less.
pub const MAX_CONCURRENT_RANGE_PULLS: usize = 4;

/// Encoded chunks the Flow A encoder buffers ahead of its consumer before it
/// parks. The encoder writes one parent pair or one chunk group per item, so the
/// channel holds at most this many chunk groups.
const WIRE_CHANNEL_CAP: usize = 8;

/// Run one origin read `fut` of `len` bytes of `hash`'s range-pull path under
/// `budget`. A read past its budget is an origin transport fault
/// ([`CacheError::OriginError`]), so the caller advances the origin chain or
/// fails the fill as for any other transport fault; it also bumps
/// `origin_range_timeouts`. `None` runs `fut` unbounded.
///
/// # Errors
///
/// [`CacheError::OriginError`] when `fut` does not finish within its budget.
pub(crate) async fn within_origin_timeout<F: Future>(
    budget: Option<OriginReadBudget>,
    len: u64,
    hash: Hash,
    what: &'static str,
    metrics: Option<&CacheMetrics>,
    fut: F,
) -> CacheResult<F::Output> {
    let Some(budget) = budget else {
        return Ok(fut.await);
    };
    let limit = budget.for_len(len);
    tokio::time::timeout(limit, fut).await.map_err(|_| {
        if let Some(m) = metrics {
            m.origin_range_timeouts.inc();
        }
        CacheError::OriginError {
            hash,
            source: anyhow::anyhow!(
                "origin {what} of {len} bytes took longer than its {limit:?} budget \
                 (cache.node_pull_stall_window_sec plus the size at \
                 cache.node_pull_min_throughput_bps)"
            ),
        }
    })
}

/// The window spans `[start, end)` that cover an [`AlignedRange`], in order.
/// Each is at most [`RANGE_PULL_WINDOW_BYTES`] long and starts on a chunk-group
/// boundary. An empty range (the 0-byte blob) yields one empty span, so the
/// caller still imports its empty proof.
#[derive(Debug, Clone)]
pub(crate) struct WindowSpans {
    next: u64,
    end: u64,
    done: bool,
}

impl WindowSpans {
    pub(crate) const fn new(aligned: &AlignedRange) -> Self {
        Self {
            next: aligned.fetch_start(),
            end: aligned.fetch_end(),
            done: false,
        }
    }
}

impl Iterator for WindowSpans {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<(u64, u64)> {
        if self.done {
            return None;
        }
        let start = self.next;
        let end = start.saturating_add(RANGE_PULL_WINDOW_BYTES).min(self.end);
        self.next = end;
        self.done = end >= self.end;
        Some((start, end))
    }
}

/// One window of an origin span: the still-UNVERIFIED bytes of `[start, end)`,
/// exactly `end - start` long.
#[derive(Debug)]
pub(crate) struct OriginWindow {
    pub(crate) start: u64,
    pub(crate) data: Bytes,
}

/// What an origin answered for one window. A wrong-length answer is kept apart
/// from a decline because the two paths judge it differently: the range pull
/// degrades, and Flow A reports [`CacheError::VerifyFailed`].
#[derive(Debug)]
pub(crate) enum WindowFetch {
    /// The origin served the window at its exact length.
    Window(OriginWindow),
    /// The origin declined the window (no `Range`, object absent or short).
    Declined { start: u64, end: u64 },
    /// The origin served `got` bytes for the `[start, end)` window.
    WrongLength { start: u64, end: u64, got: usize },
}

/// One origin serving the data of `hash`, walking the windows of the span it was
/// opened for, in order. The outboard is UNTRUSTED until a window verifies
/// against the root `H`.
pub(crate) struct OriginRangeCursor {
    origin: Arc<dyn Origin>,
    hash: Hash,
    outboard: Bytes,
    /// Time budget for each window fetch; `None` is unbounded.
    budget: Option<OriginReadBudget>,
    metrics: Option<Arc<CacheMetrics>>,
    spans: WindowSpans,
    /// The first window, fetched by [`Self::open`] and not yet handed out.
    pending: Option<WindowFetch>,
}

impl OriginRangeCursor {
    /// Open a cursor on `origin` for `aligned` against the already-read
    /// `outboard`, and fetch the first window. `Ok(None)` when this origin
    /// declines the first window, so the caller can advance the origin chain
    /// before it commits to this origin. Each window fetch runs under `budget`
    /// ([`within_origin_timeout`]).
    ///
    /// Meters every fetched window as `pull_through_bytes`. The outboard read
    /// is metered where it happens, in
    /// [`crate::CacheEngine::origin_fetch_outboard_bytes`].
    ///
    /// # Errors
    ///
    /// [`CacheError::OriginError`] for an origin transport fault or a window
    /// fetch past its budget.
    pub(crate) async fn open(
        origin: Arc<dyn Origin>,
        hash: Hash,
        aligned: &AlignedRange,
        outboard: Bytes,
        budget: Option<OriginReadBudget>,
        metrics: Option<Arc<CacheMetrics>>,
    ) -> CacheResult<Option<Self>> {
        let mut cursor = Self {
            origin,
            hash,
            outboard,
            budget,
            metrics,
            spans: WindowSpans::new(aligned),
            pending: None,
        };
        match cursor.fetch_next().await? {
            None | Some(WindowFetch::Declined { .. }) => Ok(None),
            Some(first) => {
                cursor.pending = Some(first);
                Ok(Some(cursor))
            }
        }
    }

    /// The untrusted pre-order outboard this cursor was opened with.
    pub(crate) fn outboard(&self) -> Bytes {
        self.outboard.clone()
    }

    /// The kind of origin this cursor reads from.
    pub(crate) fn origin_kind(&self) -> OriginKind {
        self.origin.kind()
    }

    /// Whether the origin answered the first window with the wrong length.
    pub(crate) const fn first_is_wrong_length(&self) -> bool {
        matches!(self.pending, Some(WindowFetch::WrongLength { .. }))
    }

    /// The next window of the span, in order. `Ok(None)` once every window has
    /// been handed out.
    ///
    /// # Errors
    ///
    /// [`CacheError::OriginError`] for an origin transport fault.
    pub(crate) async fn next_window(&mut self) -> CacheResult<Option<WindowFetch>> {
        if let Some(first) = self.pending.take() {
            return Ok(Some(first));
        }
        self.fetch_next().await
    }

    async fn fetch_next(&mut self) -> CacheResult<Option<WindowFetch>> {
        let Some((start, end)) = self.spans.next() else {
            return Ok(None);
        };
        let req = OriginRangeRequest {
            fetch_start: start,
            fetch_end: end,
        };
        let data = match within_origin_timeout(
            self.budget,
            end - start,
            self.hash,
            "range window fetch",
            self.metrics.as_deref(),
            self.origin.fetch_range_data(self.hash, req),
        )
        .await?
        .map_err(|e| CacheError::OriginError {
            hash: self.hash,
            source: e.into_inner(),
        })? {
            OriginRangeFetch::Ranged { data } => data,
            OriginRangeFetch::Unsupported | OriginRangeFetch::NotFound => {
                return Ok(Some(WindowFetch::Declined { start, end }));
            }
        };
        if let Some(m) = &self.metrics {
            m.pull_through_bytes
                .inc_by(u64::try_from(data.len()).unwrap_or(u64::MAX));
        }
        if u64::try_from(data.len()).ok() != Some(end - start) {
            return Ok(Some(WindowFetch::WrongLength {
                start,
                end,
                got: data.len(),
            }));
        }
        Ok(Some(WindowFetch::Window(OriginWindow { start, data })))
    }
}

/// The typed fault a Flow A encode stopped on, shared between the encode task
/// and its data reader. Only [`park_fault`] writes it; only
/// [`OriginRangeWire::next_chunk`] reads it.
type FaultSlot = Arc<Mutex<Option<CacheError>>>;

/// Record `fault` unless a fault is already parked: the first fault is the
/// most specific (the reader parks the typed cause before the encoder sees a
/// generic read error), so it wins.
fn park_fault(slot: &FaultSlot, fault: CacheError) {
    let mut guard = slot.lock().unwrap_or_else(PoisonError::into_inner);
    if guard.is_none() {
        *guard = Some(fault);
    }
}

/// A fault in this module's own window bookkeeping, not the origin's. Logged
/// and reported as a local fault so it never blames the operator's origin.
fn internal_fault(hash: Hash, what: &str) -> CacheError {
    tracing::error!(%hash, what, "origin range reader invariant broken");
    CacheError::Store(anyhow::anyhow!(
        "origin range reader invariant broken for {hash}: {what}"
    ))
}

/// An [`AsyncSliceReader`] over one origin's span that holds one window at a
/// time and pulls the cursor's next window when a read passes the held one. The
/// encoder reads the range front to back, so each window is fetched once.
///
/// A failed read parks its typed cause first:
/// - a window the origin declines, or an origin transport fault →
///   [`CacheError::OriginError`];
/// - a window of the wrong length → [`CacheError::VerifyFailed`];
/// - a read this reader cannot place (backward, or past the span) →
///   [`CacheError::Store`], a local fault.
pub(crate) struct OriginWindowReader {
    cursor: OriginRangeCursor,
    blob_size: u64,
    fetch_end: u64,
    window_start: u64,
    window: Bytes,
    fault: FaultSlot,
}

impl OriginWindowReader {
    const fn new(cursor: OriginRangeCursor, aligned: &AlignedRange, fault: FaultSlot) -> Self {
        Self {
            cursor,
            blob_size: aligned.blob_size(),
            fetch_end: aligned.fetch_end(),
            window_start: aligned.fetch_start(),
            window: Bytes::new(),
            fault,
        }
    }

    fn window_end(&self) -> u64 {
        self.window_start
            .saturating_add(u64::try_from(self.window.len()).unwrap_or(u64::MAX))
    }

    fn fail(&self, fault: CacheError) -> io::Error {
        let msg = fault.to_string();
        park_fault(&self.fault, fault);
        io::Error::other(msg)
    }

    /// Replace the held window with the cursor's next one.
    async fn load_next(&mut self) -> io::Result<()> {
        let hash = self.cursor.hash;
        // Release the old window before fetching the next one.
        self.window = Bytes::new();
        match self.cursor.next_window().await {
            Ok(Some(WindowFetch::Window(w))) if !w.data.is_empty() => {
                self.window_start = w.start;
                self.window = w.data;
                Ok(())
            }
            Ok(Some(WindowFetch::Window(_)) | None) => Err(self.fail(internal_fault(
                hash,
                "read past the last window of the span",
            ))),
            Ok(Some(WindowFetch::Declined { start, end })) => {
                tracing::warn!(
                    %hash,
                    kind = ?self.cursor.origin_kind(),
                    window_start = start,
                    window_end = end,
                    "own origin stopped serving the range mid-stream",
                );
                Err(self.fail(CacheError::OriginError {
                    hash,
                    source: anyhow::anyhow!(
                        "origin stopped serving range [{start}, {end}) of {hash} mid-stream"
                    ),
                }))
            }
            Ok(Some(WindowFetch::WrongLength { start, end, got })) => {
                tracing::warn!(
                    %hash,
                    kind = ?self.cursor.origin_kind(),
                    window_start = start,
                    window_end = end,
                    got,
                    "own origin served a wrong-length range window; hard local-origin fault \
                     (no degrade — committed to serving under H)",
                );
                Err(self.fail(CacheError::VerifyFailed { expected: hash }))
            }
            Err(e) => {
                tracing::warn!(
                    %hash,
                    kind = ?self.cursor.origin_kind(),
                    error = %e,
                    "own origin range window fetch failed mid-stream",
                );
                Err(self.fail(e))
            }
        }
    }
}

impl AsyncSliceReader for OriginWindowReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let end = offset
            .saturating_add(u64::try_from(len).unwrap_or(u64::MAX))
            .min(self.fetch_end);
        if offset < self.window_start {
            return Err(self.fail(internal_fault(
                self.cursor.hash,
                "backward read before the held window",
            )));
        }
        let mut out: Option<BytesMut> = None;
        let mut pos = offset;
        while pos < end {
            while pos >= self.window_end() {
                self.load_next().await?;
            }
            let rel = usize::try_from(pos - self.window_start).unwrap_or(usize::MAX);
            let take_end = end.min(self.window_end());
            let take = usize::try_from(take_end - pos).unwrap_or(usize::MAX);
            let Some(slice) = self.window.get(rel..rel.saturating_add(take)) else {
                return Err(self.fail(internal_fault(
                    self.cursor.hash,
                    "held window shorter than its span",
                )));
            };
            let piece = self.window.slice_ref(slice);
            pos = take_end;
            // A leaf never straddles windows (see `RANGE_PULL_WINDOW_BYTES`), so
            // a read returns a zero-copy slice of one window. The copying branch
            // is defensive: it keeps a straddling read correct, not fast.
            if pos >= end && out.is_none() {
                return Ok(piece);
            }
            out.get_or_insert_with(BytesMut::new)
                .extend_from_slice(&piece);
        }
        Ok(out.map(BytesMut::freeze).unwrap_or_default())
    }

    async fn size(&mut self) -> io::Result<u64> {
        Ok(self.blob_size)
    }
}

/// Pushes the encoder's output into the bounded channel [`OriginRangeWire`]
/// drains. A dropped receiver surfaces as a write error, ending the encode.
struct ChannelWriter {
    tx: mpsc::Sender<Bytes>,
}

impl AsyncStreamWriter for ChannelWriter {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.write_bytes(Bytes::copy_from_slice(data)).await
    }

    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.tx
            .send(data)
            .await
            .map_err(|_| io::Error::other("origin range wire consumer dropped"))
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Parks an `OriginError` if the encode task ends without reaching its verdict —
/// a panic unwinds through it, and a cancelled task drops it. So the channel
/// never closes on an unfinished wire without a fault behind it.
struct UnfinishedGuard {
    slot: FaultSlot,
    hash: Hash,
    finished: bool,
}

impl UnfinishedGuard {
    /// Mark the encode as having reached its verdict; the drop then parks
    /// nothing.
    const fn disarm(&mut self) {
        self.finished = true;
    }
}

impl Drop for UnfinishedGuard {
    fn drop(&mut self) {
        if !self.finished {
            park_fault(
                &self.slot,
                CacheError::OriginError {
                    hash: self.hash,
                    source: anyhow::anyhow!(
                        "origin range encode for {} ended without completing (panic or cancel)",
                        self.hash
                    ),
                },
            );
        }
    }
}

/// The header-less interleaved bao wire (ADR 038) for one range of a blob,
/// verified against the root `H` and produced window by window out of an
/// origin. Returned by [`crate::CacheEngine::origin_range_wire`].
///
/// A background task runs the encode and holds a range-pull permit until it
/// ends. [`Self::next_chunk`] yields the wire in order, then either one
/// terminal `Err` or `None`:
/// - `Some(Err(_))` — the encode stopped: [`CacheError::VerifyFailed`] for a
///   window that fails verification against `H`, [`CacheError::OriginError`]
///   for an origin that stops serving or a task that panics or is cancelled,
///   [`CacheError::Store`] for a local reader fault. The wire is incomplete.
/// - `None` with no `Err` before it — the encode completed; the wire is whole.
///
/// Dropping the wire aborts the encode and releases its permit.
pub struct OriginRangeWire {
    rx: mpsc::Receiver<Bytes>,
    fault: FaultSlot,
    ended: bool,
    task: tokio::task::JoinHandle<()>,
}

impl std::fmt::Debug for OriginRangeWire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginRangeWire")
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

impl OriginRangeWire {
    /// Start the encode of `aligned` over `cursor`. Checks the outboard and
    /// first-window lengths up front: either wrong length cannot verify, so it
    /// is [`CacheError::VerifyFailed`] at once, before any wire. A wrong-length
    /// outboard or a failed bao verification evicts the cursor's outboard from
    /// `outboards`, so the next draw reads it from the origin again instead of
    /// reusing a copy that may be the bad half. A wrong-length first window
    /// blames the data and keeps the outboard.
    pub(crate) fn spawn(
        cursor: OriginRangeCursor,
        aligned: &AlignedRange,
        permit: OwnedSemaphorePermit,
        outboards: OutboardCache,
    ) -> CacheResult<Self> {
        let hash = cursor.hash;
        let kind = cursor.origin_kind();
        let tree = BaoTree::new(aligned.blob_size(), IROH_BLOCK_SIZE);
        let outboard = cursor.outboard();
        if u64::try_from(outboard.len()).ok() != Some(tree.outboard_size()) {
            tracing::warn!(
                %hash,
                ?kind,
                expected = tree.outboard_size(),
                got = outboard.len(),
                "outboard length does not match the blob size; hard local-origin fault \
                 (no degrade — committed to serving under H)",
            );
            outboards.evict_if_same(hash, &outboard);
            return Err(CacheError::VerifyFailed { expected: hash });
        }
        if cursor.first_is_wrong_length() {
            tracing::warn!(
                %hash,
                ?kind,
                "own origin served a wrong-length first range window; hard local-origin \
                 fault (no degrade — committed to serving under H)",
            );
            return Err(CacheError::VerifyFailed { expected: hash });
        }
        // A verify fault does not say whether the outboard or the data was bad,
        // so the encode task evicts the outboard copy it used.
        let used_outboard = outboard.clone();
        let ob = PreOrderMemOutboard {
            root: bao_tree::blake3::Hash::from(*hash.as_bytes()),
            tree,
            data: outboard,
        };
        let ranges: ChunkRanges = aligned.chunk_ranges().clone();
        let fault: FaultSlot = Arc::new(Mutex::new(None));
        let reader = OriginWindowReader::new(cursor, aligned, Arc::clone(&fault));
        let (tx, rx) = mpsc::channel(WIRE_CHANNEL_CAP);
        let mut guard = UnfinishedGuard {
            slot: Arc::clone(&fault),
            hash,
            finished: false,
        };
        let task = tokio::spawn(async move {
            let _permit = permit;
            // `writer` owns the only sender. It drops at the end of this block,
            // after the verdict below is parked, so the consumer never sees the
            // channel close before the fault is in place.
            let mut writer = ChannelWriter { tx };
            let mut reader = reader;
            let mut ob = ob;
            // The empty range (the 0-byte blob) has an empty wire. The async
            // encoder, unlike the sync one, does not short-circuit empty ranges
            // and trips a debug assertion walking them, so skip it.
            let result = if ranges.is_empty() {
                Ok(())
            } else {
                encode_ranges_validated(&mut reader, &mut ob, ranges.as_ref(), &mut writer).await
            };
            match result {
                Ok(()) => {}
                Err(EncodeError::ParentHashMismatch(_) | EncodeError::LeafHashMismatch(_)) => {
                    tracing::warn!(
                        %hash,
                        ?kind,
                        "own origin served a range that failed bao verification against H; \
                         hard local-origin fault (no degrade — committed to serving under H)",
                    );
                    outboards.evict_if_same(hash, &used_outboard);
                    park_fault(&guard.slot, CacheError::VerifyFailed { expected: hash });
                }
                // A reader-side io error has already parked its typed cause, and
                // `park_fault` keeps it. A writer-side one (the consumer dropped
                // the wire) parks this generic fault, which nobody reads.
                Err(EncodeError::Io(e)) => park_fault(
                    &guard.slot,
                    CacheError::OriginError {
                        hash,
                        source: anyhow::Error::new(e).context("origin range encode failed"),
                    },
                ),
                Err(other) => park_fault(
                    &guard.slot,
                    CacheError::OriginError {
                        hash,
                        source: anyhow::anyhow!("origin range encode failed: {other}"),
                    },
                ),
            }
            guard.disarm();
        });
        Ok(Self {
            rx,
            fault,
            ended: false,
            task,
        })
    }

    /// The next chunk of wire, in order. After the last chunk it yields one
    /// `Some(Err(_))` if the encode stopped on a fault, and `None` otherwise
    /// (see the type docs). Every later call yields `None`.
    pub async fn next_chunk(&mut self) -> Option<CacheResult<Bytes>> {
        if self.ended {
            return None;
        }
        if let Some(chunk) = self.rx.recv().await {
            return Some(Ok(chunk));
        }
        self.ended = true;
        self.fault
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .map(Err)
    }
}

impl Drop for OriginRangeWire {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "tests"
)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::OriginPullError;
    use crate::origin::{OriginFetch, OutboardFetch};
    use crate::range_pull::align_range;

    #[test]
    fn window_spans_tile_the_range_in_bounded_steps() {
        let w = RANGE_PULL_WINDOW_BYTES;
        let blob = 3 * w + 20_000;
        let aligned = align_range(16 * 1024, 0, blob).unwrap();
        let spans: Vec<_> = WindowSpans::new(&aligned).collect();
        assert_eq!(spans.first().unwrap().0, aligned.fetch_start());
        assert_eq!(spans.last().unwrap().1, aligned.fetch_end());
        for pair in spans.windows(2) {
            assert_eq!(pair[0].1, pair[1].0, "spans must be contiguous");
        }
        assert!(spans.iter().all(|(s, e)| e > s && e - s <= w));
        assert_eq!(spans.len(), 4);
    }

    #[test]
    fn window_spans_of_the_empty_blob_is_one_empty_span() {
        let aligned = align_range(0, 0, 0).unwrap();
        assert_eq!(WindowSpans::new(&aligned).collect::<Vec<_>>(), vec![(0, 0)]);
    }

    /// Serves `data` by range and an empty outboard, counting data reads.
    #[derive(Debug)]
    struct MemOrigin {
        data: Bytes,
        reads: AtomicUsize,
    }

    impl Origin for MemOrigin {
        fn kind(&self) -> OriginKind {
            OriginKind::Http
        }

        fn fetch(
            &self,
            _hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>>
        {
            Box::pin(async { Ok(OriginFetch::NotFound) })
        }

        fn fetch_outboard(
            &self,
            _hash: Hash,
            _outboard_max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>>
        {
            Box::pin(async { Ok(OutboardFetch::Found(Bytes::new())) })
        }

        fn fetch_range_data(
            &self,
            _hash: Hash,
            req: OriginRangeRequest,
        ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>>
        {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let s = usize::try_from(req.fetch_start).unwrap();
            let e = usize::try_from(req.fetch_end).unwrap();
            let data = self.data.slice(s..e);
            Box::pin(async move { Ok(OriginRangeFetch::Ranged { data }) })
        }
    }

    /// The reader serves a read inside one window, a read that straddles two
    /// windows, and a read clamped at the span end — fetching each window once —
    /// and refuses a backward read as a local fault.
    #[tokio::test]
    async fn window_reader_reads_across_windows_and_fetches_each_once() {
        let w = usize::try_from(RANGE_PULL_WINDOW_BYTES).unwrap();
        let blob: Vec<u8> = (0..2 * w + 100).map(|i| (i % 251) as u8).collect();
        let origin = Arc::new(MemOrigin {
            data: Bytes::from(blob.clone()),
            reads: AtomicUsize::new(0),
        });
        let hash = Hash::new(&blob);
        let aligned = align_range(0, 0, u64::try_from(blob.len()).unwrap()).unwrap();
        let cursor = OriginRangeCursor::open(
            Arc::clone(&origin) as Arc<dyn Origin>,
            hash,
            &aligned,
            Bytes::new(),
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        let fault: FaultSlot = Arc::new(Mutex::new(None));
        let mut reader = OriginWindowReader::new(cursor, &aligned, Arc::clone(&fault));

        assert_eq!(reader.read_at(10, 20).await.unwrap(), &blob[10..30]);
        let w64 = RANGE_PULL_WINDOW_BYTES;
        assert_eq!(
            reader.read_at(w64 - 10, 20).await.unwrap(),
            &blob[w - 10..w + 10],
            "a read that straddles two windows is byte-exact",
        );
        assert_eq!(
            reader.read_at(2 * w64 + 90, 100).await.unwrap(),
            &blob[2 * w + 90..],
            "a read past the span end is clamped",
        );
        assert_eq!(
            origin.reads.load(Ordering::SeqCst),
            3,
            "one read per window"
        );

        assert!(reader.read_at(0, 1).await.is_err(), "a backward read fails");
        assert!(
            matches!(*fault.lock().unwrap(), Some(CacheError::Store(_))),
            "a backward read is a local fault, not an origin fault",
        );
    }
}
