//! Windowed origin range pulls (#2065): an origin's `{H}.obao4` outboard is read
//! once, and the requested span is fetched in bounded windows of
//! [`RANGE_PULL_WINDOW_BYTES`], so a range pull holds `O(window + outboard)`
//! bytes whatever its length.
//!
//! Two engine paths build on the `OriginRangeCursor` here:
//!
//! - [`crate::CacheEngine::pull_through_range`] verifies and imports each
//!   window as its own partial-blob slice. A window's bao encoding carries every
//!   parent node on the path to its chunk groups, so it verifies and imports on
//!   its own.
//! - [`crate::CacheEngine::origin_range_wire`] (Flow A) needs ONE coherent
//!   header-less wire for the whole range — joined per-window encodings would
//!   repeat the shared parent nodes. It runs a single
//!   [`bao_tree::io::fsm::encode_ranges_validated`] over an
//!   `OriginWindowReader` that loads windows on demand, and streams the output
//!   through a bounded channel ([`OriginRangeWire`]).

use std::io;
use std::sync::{Arc, Mutex, PoisonError};

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
use crate::origin::{Origin, OriginRangeFetch, OriginRangeRequest, OutboardFetch};
use crate::range_pull::{AlignedRange, IROH_BLOCK_SIZE};

/// Largest data span one [`Origin::fetch_range_data`] call fetches. A multiple
/// of the 16 KiB chunk group, so no bao leaf straddles two windows. A range
/// pull holds about two windows (the fetched span and its encoding) plus the
/// outboard, whatever the requested length.
pub const RANGE_PULL_WINDOW_BYTES: u64 = 4 * 1024 * 1024;

/// Origin range pulls that run at once, across both
/// [`crate::CacheEngine::pull_through_range`] and
/// [`crate::CacheEngine::origin_range_wire`]. A caller past the bound waits
/// for a permit rather than degrading, because the degrade is a whole-blob
/// origin pull — more egress, not less.
pub const MAX_CONCURRENT_RANGE_PULLS: usize = 4;

/// Encoded chunks the Flow A encoder buffers ahead of its consumer before it
/// parks.
const WIRE_CHANNEL_CAP: usize = 8;

/// The window spans `[start, end)` that cover `aligned`, in order. Each is at
/// most [`RANGE_PULL_WINDOW_BYTES`] long and starts on a chunk-group boundary.
/// An empty range (the 0-byte blob) yields one empty span, so the caller still
/// imports its empty proof.
pub(crate) fn window_spans(aligned: &AlignedRange) -> impl Iterator<Item = (u64, u64)> {
    let (start, end) = (aligned.fetch_start(), aligned.fetch_end());
    let first_end = start.saturating_add(RANGE_PULL_WINDOW_BYTES).min(end);
    std::iter::successors(Some((start, first_end)), move |&(_, prev_end)| {
        (prev_end < end).then(|| {
            (
                prev_end,
                prev_end.saturating_add(RANGE_PULL_WINDOW_BYTES).min(end),
            )
        })
    })
}

/// One origin that serves the outboard for `hash`, ready to fetch data windows
/// of the span it was opened for. The outboard is UNTRUSTED until a window
/// verifies against the root `H`.
pub(crate) struct OriginRangeCursor {
    origin: Arc<dyn Origin>,
    hash: Hash,
    outboard: Bytes,
    metrics: Option<Arc<CacheMetrics>>,
}

impl OriginRangeCursor {
    /// Open a cursor on `origin` for `aligned`: read the outboard once, then
    /// fetch the first window. Returns the cursor and the first window's
    /// still-unverified bytes, or `Ok(None)` when this origin declines (no
    /// outboard, no `Range`, object absent or short), so the caller can advance
    /// the origin chain before it commits to this origin.
    ///
    /// Meters the outboard and every fetched window as `pull_through_bytes`.
    ///
    /// # Errors
    ///
    /// [`CacheError::OriginError`] for an origin transport fault.
    pub(crate) async fn open(
        origin: Arc<dyn Origin>,
        hash: Hash,
        aligned: &AlignedRange,
        outboard_max: u64,
        metrics: Option<Arc<CacheMetrics>>,
    ) -> CacheResult<Option<(Self, Bytes)>> {
        let outboard = match origin
            .fetch_outboard(hash, outboard_max)
            .await
            .map_err(|e| CacheError::OriginError {
                hash,
                source: e.into_inner(),
            })? {
            OutboardFetch::Found(ob) => ob,
            OutboardFetch::NotFound | OutboardFetch::Unsupported => return Ok(None),
        };
        if let Some(m) = &metrics {
            m.pull_through_bytes
                .inc_by(u64::try_from(outboard.len()).unwrap_or(u64::MAX));
        }
        let cursor = Self {
            origin,
            hash,
            outboard,
            metrics,
        };
        let Some((start, end)) = window_spans(aligned).next() else {
            return Ok(None);
        };
        let Some(first) = cursor.fetch_window(start, end).await? else {
            return Ok(None);
        };
        Ok(Some((cursor, first)))
    }

    /// The untrusted pre-order outboard this cursor read on open.
    pub(crate) fn outboard(&self) -> Bytes {
        self.outboard.clone()
    }

    /// The origin this cursor reads from.
    pub(crate) fn origin(&self) -> &Arc<dyn Origin> {
        &self.origin
    }

    /// Fetch the still-unverified data window `[start, end)`. `Ok(None)` when
    /// the origin declines the window.
    ///
    /// # Errors
    ///
    /// [`CacheError::OriginError`] for an origin transport fault.
    pub(crate) async fn fetch_window(&self, start: u64, end: u64) -> CacheResult<Option<Bytes>> {
        let req = OriginRangeRequest {
            fetch_start: start,
            fetch_end: end,
        };
        let data = match self
            .origin
            .fetch_range_data(self.hash, req)
            .await
            .map_err(|e| CacheError::OriginError {
                hash: self.hash,
                source: e.into_inner(),
            })? {
            OriginRangeFetch::Ranged { data } => data,
            OriginRangeFetch::Unsupported | OriginRangeFetch::NotFound => return Ok(None),
        };
        if let Some(m) = &self.metrics {
            m.pull_through_bytes
                .inc_by(u64::try_from(data.len()).unwrap_or(u64::MAX));
        }
        Ok(Some(data))
    }
}

/// The typed fault a Flow A encode stopped on, shared between the encode task,
/// its data reader, and the [`OriginRangeWire`] consumer.
type FaultSlot = Arc<Mutex<Option<CacheError>>>;

fn park_fault(slot: &FaultSlot, fault: CacheError) {
    let mut guard = slot.lock().unwrap_or_else(PoisonError::into_inner);
    if guard.is_none() {
        *guard = Some(fault);
    }
}

/// An [`AsyncSliceReader`] over one origin's span that holds a single window at
/// a time and loads the window covering each read on demand. The encoder walks
/// the range front to back, so each window is fetched once.
///
/// A window the origin declines or fails to serve fails the read and parks a
/// typed [`CacheError::OriginError`] in the shared fault slot.
pub(crate) struct OriginWindowReader {
    cursor: OriginRangeCursor,
    blob_size: u64,
    fetch_start: u64,
    fetch_end: u64,
    window_start: u64,
    window: Bytes,
    fault: FaultSlot,
}

impl OriginWindowReader {
    const fn new(
        cursor: OriginRangeCursor,
        aligned: &AlignedRange,
        first: Bytes,
        fault: FaultSlot,
    ) -> Self {
        Self {
            cursor,
            blob_size: aligned.blob_size(),
            fetch_start: aligned.fetch_start(),
            fetch_end: aligned.fetch_end(),
            window_start: aligned.fetch_start(),
            window: first,
            fault,
        }
    }

    fn window_end(&self) -> u64 {
        self.window_start
            .saturating_add(u64::try_from(self.window.len()).unwrap_or(u64::MAX))
    }

    /// Make the held window the one that covers `pos`.
    async fn load(&mut self, pos: u64) -> io::Result<()> {
        if (self.window_start..self.window_end()).contains(&pos) {
            return Ok(());
        }
        if pos < self.fetch_start || pos >= self.fetch_end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "origin range read at {pos} is outside the fetched span [{}, {})",
                    self.fetch_start, self.fetch_end
                ),
            ));
        }
        let index = (pos - self.fetch_start) / RANGE_PULL_WINDOW_BYTES;
        let start = self
            .fetch_start
            .saturating_add(index.saturating_mul(RANGE_PULL_WINDOW_BYTES));
        let end = start
            .saturating_add(RANGE_PULL_WINDOW_BYTES)
            .min(self.fetch_end);
        // Release the old window before fetching the next one.
        self.window = Bytes::new();
        let fetched = self.cursor.fetch_window(start, end).await;
        match fetched {
            Ok(Some(data)) => {
                self.window_start = start;
                self.window = data;
                Ok(())
            }
            Ok(None) => {
                let hash = self.cursor.hash;
                park_fault(
                    &self.fault,
                    CacheError::OriginError {
                        hash,
                        source: anyhow::anyhow!(
                            "origin stopped serving range [{start}, {end}) of {hash} mid-stream"
                        ),
                    },
                );
                Err(io::Error::other(
                    "origin declined a range window mid-stream",
                ))
            }
            Err(e) => {
                let msg = e.to_string();
                park_fault(&self.fault, e);
                Err(io::Error::other(msg))
            }
        }
    }
}

impl AsyncSliceReader for OriginWindowReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let end = offset
            .saturating_add(u64::try_from(len).unwrap_or(u64::MAX))
            .min(self.fetch_end);
        let mut out: Option<BytesMut> = None;
        let mut pos = offset;
        while pos < end {
            self.load(pos).await?;
            let rel = usize::try_from(pos - self.window_start).unwrap_or(usize::MAX);
            let take_end = end.min(self.window_end());
            let take = usize::try_from(take_end - pos).unwrap_or(usize::MAX);
            let piece = self
                .window
                .get(rel..rel.saturating_add(take))
                .map(|s| self.window.slice_ref(s))
                .ok_or_else(|| io::Error::other("origin range window shorter than its span"))?;
            pos = take_end;
            // The common case — a leaf inside one window — returns a zero-copy
            // slice. Only a read that straddles windows is copied.
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

/// The header-less interleaved bao wire (ADR 038) for one range of a blob,
/// verified against the root `H` and produced window by window out of an
/// origin. Returned by [`crate::CacheEngine::origin_range_wire`].
///
/// A background task runs the encode and holds a range-pull permit until it
/// ends. [`Self::next_chunk`] yields the wire in order and `None` at the end.
/// An end with a fault (a window that fails verification against `H`, or an
/// origin that stops serving mid-stream) leaves the typed fault for
/// [`Self::take_fault`]: [`CacheError::VerifyFailed`] or
/// [`CacheError::OriginError`]. Dropping the wire aborts the encode.
pub struct OriginRangeWire {
    rx: mpsc::Receiver<Bytes>,
    fault: FaultSlot,
    task: tokio::task::JoinHandle<()>,
}

impl std::fmt::Debug for OriginRangeWire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginRangeWire").finish_non_exhaustive()
    }
}

impl OriginRangeWire {
    /// Start the encode of `aligned` over `cursor`, whose first window is
    /// `first`. Checks the outboard length up front: a wrong-length outboard
    /// cannot verify, so it is [`CacheError::VerifyFailed`] at once.
    pub(crate) fn spawn(
        cursor: OriginRangeCursor,
        aligned: &AlignedRange,
        first: Bytes,
        permit: OwnedSemaphorePermit,
    ) -> CacheResult<Self> {
        let hash = cursor.hash;
        let tree = BaoTree::new(aligned.blob_size(), IROH_BLOCK_SIZE);
        let outboard = cursor.outboard();
        if u64::try_from(outboard.len()).ok() != Some(tree.outboard_size()) {
            tracing::warn!(
                %hash,
                kind = ?cursor.origin().kind(),
                expected = tree.outboard_size(),
                got = outboard.len(),
                "own origin served a wrong-length outboard; hard local-origin fault \
                 (no degrade — committed to serving under H)",
            );
            return Err(CacheError::VerifyFailed { expected: hash });
        }
        let ob = PreOrderMemOutboard {
            root: bao_tree::blake3::Hash::from(*hash.as_bytes()),
            tree,
            data: outboard,
        };
        let kind = cursor.origin().kind();
        let ranges: ChunkRanges = aligned.chunk_ranges().clone();
        let fault: FaultSlot = Arc::new(Mutex::new(None));
        let reader = OriginWindowReader::new(cursor, aligned, first, Arc::clone(&fault));
        let (tx, rx) = mpsc::channel(WIRE_CHANNEL_CAP);
        let task_fault = Arc::clone(&fault);
        let task = tokio::spawn(async move {
            let _permit = permit;
            let mut writer = ChannelWriter { tx };
            let mut reader = reader;
            let mut ob = ob;
            let result =
                encode_ranges_validated(&mut reader, &mut ob, ranges.as_ref(), &mut writer).await;
            match result {
                Ok(()) => {}
                // The reader parked the typed origin fault before failing the read.
                Err(EncodeError::Io(e)) => park_fault(
                    &task_fault,
                    CacheError::OriginError {
                        hash,
                        source: anyhow::Error::new(e).context("origin range encode failed"),
                    },
                ),
                Err(e) => {
                    tracing::warn!(
                        %hash,
                        ?kind,
                        error = %e,
                        "own origin served a range that failed bao verification against H; \
                         hard local-origin fault (no degrade — committed to serving under H)",
                    );
                    park_fault(&task_fault, CacheError::VerifyFailed { expected: hash });
                }
            }
        });
        Ok(Self { rx, fault, task })
    }

    /// The next chunk of wire, in order. `None` once the encode has ended —
    /// check [`Self::take_fault`] to tell a complete wire from a failed one.
    pub async fn next_chunk(&mut self) -> Option<Bytes> {
        self.rx.recv().await
    }

    /// The typed fault the encode stopped on, if any. `None` while the encode
    /// runs and after a clean end.
    pub fn take_fault(&mut self) -> Option<CacheError> {
        self.fault
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
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
    reason = "tests"
)]
mod tests {
    use super::*;
    use crate::range_pull::align_range;

    #[test]
    fn window_spans_tile_the_range_in_bounded_steps() {
        let w = RANGE_PULL_WINDOW_BYTES;
        let blob = 3 * w + 20_000;
        let aligned = align_range(16 * 1024, 0, blob).unwrap();
        let spans: Vec<_> = window_spans(&aligned).collect();
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
        assert_eq!(window_spans(&aligned).collect::<Vec<_>>(), vec![(0, 0)]);
    }
}
