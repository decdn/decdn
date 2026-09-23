//! The `Streamer` consumption face (#1848 T6): stream one blob's verified,
//! contiguous front to a consumer AS IT ARRIVES, paced by how fast the consumer
//! reads — a fetch-like, single-blob face (no chunk dedup, unlike the
//! `Downloader`).
//!
//! One engine, two faces: the `Streamer` is an OUTPUT + SCHEDULING adapter over
//! the same paid pull engine, not a second engine. It drives the fetch through
//! the multi-source core with a [`crate::pacer::WindowPacer`] whose window is the
//! caller's [`PullConfig::read_ahead_bytes`] and whose downstream frontier is the
//! CONSUMER'S read cursor. So the pull never runs more than the read-ahead bound
//! ahead of what the consumer has read — fetch (and pay for) a two-hour movie's
//! first `read_ahead_bytes` only, for a viewer who stops after five minutes.
//!
//! [`Streamer::open`] returns two halves. The [`StreamDrive`] is the fetch
//! itself; the caller polls it for the whole stream, independently of reads, so
//! an open paid leg keeps paying and draining while the consumer is slow or
//! paused (a leg left unpolled would miss its voucher deadlines and fault). The
//! window, not the reads, bounds how far it runs. [`VerifiedReader`] is the
//! [`tokio::io::AsyncRead`] the caller drains. Its cursor is clamped to the
//! store's verified contiguous frontier BY CONSTRUCTION: it only ever reads bytes
//! the engine has already bao-verified into the store, so a tampered chunk group
//! fails the pull and surfaces as a read error rather than ever reaching the
//! consumer.

use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Notify;

use decdn_protocol::{Coverage, num_blocks};

use crate::driver::{DriveConfig, PacingWait, WaitReason};
use crate::pacer::{DownstreamFrontier, PULL_WINDOW_FLOOR, WindowPacer};
use crate::scheduler::{ConsumptionPacing, MultiSourceConfig, SourceLane, multi_source_fetch};
use crate::sink::BlobCache;
use crate::source::{BlobSource, Funder};
use crate::{ClientRangedStore, PoolContext, PoolLedger, PullConfig, RangedStore};

/// The most verified bytes one store read hands the reader. It bounds the
/// reader's buffer independently of the read-ahead window.
const MAX_READ_CHUNK: u64 = 1024 * 1024;

/// How long a reader waiting for new verified bytes sleeps before it re-reads
/// the store's present frontier on its own. The drive wakes the reader on every
/// progress report, before a lane parks, and when it ends. A batch flush that
/// lands between two progress reports has no wakeup of its own, and this bound
/// caps how long the reader can miss it.
const FRONTIER_RECHECK: Duration = Duration::from_millis(100);

/// Shared between the drive future and the [`VerifiedReader`]: the fill store,
/// the consumer cursor, the drive's outcome, and the wake signals that pace one
/// against the other.
struct StreamState {
    /// The bao-verifying fill store the drive future writes and the reader reads.
    store: ClientRangedStore,
    /// Content bytes the CONSUMER has read. It is the downstream frontier the
    /// [`WindowPacer`] gates the pull against, so the fetch stays within one
    /// read-ahead window of it.
    cursor: AtomicU64,
    /// Whole-blob content length.
    total: u64,
    /// Woken by the reader when `cursor` advances, so a pull parked on a full
    /// read-ahead window ([`PaceDecision::Wait`](crate::pacer::PaceDecision::Wait))
    /// re-decides.
    consumed: Notify,
    /// Woken by the drive when the verified frontier may have moved or the drive
    /// has ended, so a reader waiting for bytes re-reads the store.
    progressed: Notify,
    /// The drive's outcome: `None` while it runs, then its result. A drive
    /// dropped before it finishes records an error, so the reader never waits
    /// on a fetch that nobody polls.
    outcome: Mutex<Option<Result<(), Arc<anyhow::Error>>>>,
}

impl StreamState {
    /// Record the drive's outcome once (the first record wins) and wake the reader.
    fn finish(&self, result: anyhow::Result<()>) {
        if let Ok(mut slot) = self.outcome.lock()
            && slot.is_none()
        {
            *slot = Some(result.map_err(Arc::new));
        }
        self.progressed.notify_waiters();
    }

    /// The drive's outcome, or `None` while it still runs. A poisoned lock reads
    /// as a failed drive rather than a running one, so the reader cannot hang.
    fn outcome(&self) -> Option<Result<(), Arc<anyhow::Error>>> {
        match self.outcome.lock() {
            Ok(slot) => slot.clone(),
            Err(_) => Some(Err(Arc::new(anyhow::anyhow!(
                "stream outcome lock poisoned"
            )))),
        }
    }
}

/// The [`PacingWait`] the streaming drive parks on when its read-ahead window is
/// full: it resolves once the consumer's cursor advances past what the `Wait`
/// observed.
struct ConsumedWait {
    state: Arc<StreamState>,
}

impl PacingWait for ConsumedWait {
    fn wait(
        &self,
        observed: DownstreamFrontier,
        _reason: WaitReason,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            // A lane parks only between legs, after its last leg's bytes are in
            // the store. Wake the reader so it hands them out: the cursor can
            // only advance from bytes the reader has seen.
            self.state.progressed.notify_waiters();
            loop {
                // Register interest BEFORE the check, so a consume between the
                // check and the await cannot be missed.
                let notified = self.state.consumed.notified();
                if self.state.cursor.load(Ordering::SeqCst) > observed.served_paid {
                    return;
                }
                notified.await;
            }
        })
    }
}

/// A candidate provider the `Streamer` may fetch from: the paid source plus the
/// `(ctx, ledger)` that pays it.
///
/// The `Streamer` fetches the front across up to
/// [`PullConfig::streamer_lane_cap`] candidates at once and fails over between
/// them — a candidate that faults mid-stream is dropped and its remainder
/// continues from another, resuming from the store's verified frontier so no
/// delivered byte is re-pulled or re-paid. Every candidate must name a distinct
/// on-chain provider (one voucher stream per `(signer, provider)` lane).
pub struct StreamCandidate<S> {
    /// The paid source — one provider's `cdn/client/v1` requester.
    pub source: S,
    /// The buyer context paying this candidate's provider (shared behind
    /// interior mutability so a mid-stream top-up is visible to its next open).
    pub ctx: Arc<Mutex<PoolContext>>,
    /// This candidate's per-`(signer, provider)` voucher ledger.
    pub ledger: Arc<PoolLedger>,
    /// This candidate's measured block coverage for the blob being fetched
    /// (#1506), or `None` to treat it as a full holder. A partial holder MUST
    /// set this so the scheduler never assigns it — and it never steals — a
    /// range it does not hold; `None` maps to [`Coverage::full`] sized to the
    /// blob, the right default for the common case that discovery yields whole-
    /// blob holders. The bitmap is sized to ONE blob, so a [`crate::Downloader`]
    /// given several targets applies it to each of them: set it only on a
    /// single-target fetch.
    pub coverage: Option<Coverage>,
}

/// Build the per-provider [`SourceLane`] set for one blob, threading each
/// candidate's measured coverage (or [`Coverage::full`] for a `None` candidate,
/// sized to this blob's block count). Both faces build their lanes here so a
/// partial holder is treated identically whether it is streamed or downloaded.
pub(crate) fn source_lanes<S>(
    candidates: &[StreamCandidate<S>],
    total: u64,
) -> Vec<SourceLane<'_, S>> {
    candidates
        .iter()
        .map(|c| SourceLane {
            source: &c.source,
            ctx: Arc::clone(&c.ctx),
            ledger: Arc::clone(&c.ledger),
            coverage: c
                .coverage
                .clone()
                .unwrap_or_else(|| Coverage::full(num_blocks(total))),
        })
        .collect()
}

impl<S> std::fmt::Debug for StreamCandidate<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamCandidate").finish_non_exhaustive()
    }
}

/// Run the streaming fetch across `candidates` into the store, bounded to one
/// read-ahead window ahead of the consumer's cursor and capped to `lane_cap`
/// concurrent lanes, then (on a clean finish) tee the whole verified blob to
/// `cache` for revisits.
///
/// A candidate that faults is dropped and its remainder reassigned to another
/// (shared-pool free failover, #1174), resuming from the store's verified
/// frontier so no delivered byte is re-pulled or re-paid.
#[allow(clippy::too_many_arguments)]
async fn run_drive<S, F>(
    state: Arc<StreamState>,
    candidates: Vec<StreamCandidate<S>>,
    read_ahead: u64,
    lane_cap: usize,
    funder: F,
    drive_config: DriveConfig,
    cache: Arc<dyn BlobCache>,
    hash: [u8; 32],
) -> anyhow::Result<()>
where
    S: BlobSource,
    F: Funder,
{
    // Below one pull-window floor (one payment interval plus the chunk-group
    // roundings, see `PULL_WINDOW_FLOOR`) the window can floor a lane's room to
    // zero before the consumer has a byte to read, and neither side then moves.
    let pacer = WindowPacer::new(read_ahead.max(PULL_WINDOW_FLOOR));
    let on_progress = {
        let state = Arc::clone(&state);
        // Each verified-progress report may mean new bytes in the store: wake the
        // reader to look.
        move |_position: u64, _total: u64| state.progressed.notify_waiters()
    };
    let downstream = {
        let state = Arc::clone(&state);
        move || DownstreamFrontier {
            served_paid: state.cursor.load(Ordering::SeqCst),
            serve_demand: 0,
        }
    };
    let wait = ConsumedWait {
        state: Arc::clone(&state),
    };
    let pacing = ConsumptionPacing {
        downstream: &downstream,
        pacing_wait: &wait,
    };
    // Each lane carries its candidate's measured coverage (a `None` candidate is
    // a full holder), so a partial holder (#1506) is never assigned a range it
    // does not hold.
    let lanes = source_lanes(&candidates, state.total);
    let ms = MultiSourceConfig {
        // Small, bounded front parallelism: a paced stream wants a little
        // same-region fan-out and free failover, not a full download's striping.
        max_sources: lane_cap.max(1),
        // The Streamer parks lanes on the consumer cursor — a full read-ahead
        // window is not a stall — so the no-verified-progress watchdog is off. A
        // genuinely silent source instead trips its own per-stream throughput
        // floor mid-read and is reassigned that way.
        unit_deadline: Duration::ZERO,
    };
    let result = multi_source_fetch(
        &state.store,
        &lanes,
        &pacer,
        &funder,
        hash,
        0,
        state.total,
        &drive_config,
        &ms,
        Some(&on_progress),
        None,
        Some(&pacing),
    )
    .await;
    if result.is_ok() {
        // Tee the whole verified blob to the cache for a revisit. Best-effort:
        // a cache write failure never fails the delivered stream. The store is
        // left at `.partial` (a stream is not kept as a file), and reading its
        // fully-present content needs no finalize.
        //
        // Only when the cache actually stores it: a `NoCache` (the `decdn fetch
        // -o -` path) reports `caches() == false`, so a huge blob is never read
        // whole into memory just to be dropped — the stream stays memory-bounded.
        if cache.caches()
            && let Ok(whole) = state.store.read(0, state.total).await
        {
            let _ = cache.put(hash, 0, whole).await;
        }
    }
    result
}

/// Stream a single blob's verified front to a consumer, paced by consumption.
///
/// Holds the injected pull machinery — a set of provider `candidates` (each a
/// source + its paying `(ctx, ledger)`) and one `funder` — plus a scratch
/// directory for the fill store. The front is fetched across a small, bounded
/// set of candidates with free failover between them. [`Streamer::open`] consumes
/// it to start one stream.
pub struct Streamer<'a, S, F> {
    candidates: Vec<StreamCandidate<S>>,
    funder: F,
    drive_config: DriveConfig,
    /// A scratch directory the fill store's `.partial` lives in for the stream's
    /// lifetime. The caller owns it (and its cleanup); a streamed blob is not
    /// kept, so a temporary directory is the usual choice. The `.partial` grows
    /// to the whole blob as the stream advances, so the directory needs room
    /// for all of it.
    scratch: &'a Path,
}

impl<S, F> std::fmt::Debug for Streamer<'_, S, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streamer")
            .field("candidates", &self.candidates.len())
            .field("drive_config", &self.drive_config)
            .field("scratch", &self.scratch)
            .finish_non_exhaustive()
    }
}

impl<'a, S, F> Streamer<'a, S, F> {
    /// Build a streamer over the injected pull machinery, filling into `scratch`.
    /// `candidates` are the discovered blob holders to fetch across and fail over
    /// between; each must name a distinct on-chain provider.
    #[must_use]
    pub const fn new(
        candidates: Vec<StreamCandidate<S>>,
        funder: F,
        drive_config: DriveConfig,
        scratch: &'a Path,
    ) -> Self {
        Self {
            candidates,
            funder,
            drive_config,
            scratch,
        }
    }
}

impl<S, F> Streamer<'_, S, F>
where
    S: BlobSource,
    F: Funder,
{
    /// Open a consumption-paced stream over `hash` (whose content length is
    /// `total_bytes`), returning the [`VerifiedReader`] the caller drains and the
    /// [`StreamDrive`] that fetches into it.
    ///
    /// The caller MUST poll the drive for as long as it reads — for example with
    /// [`StreamDrive::alongside`] around its read loop. The drive runs apart from
    /// the reads so a paid leg keeps paying while the consumer is slow or paused;
    /// the read-ahead window, not the reads, bounds it. Dropping the drive stops
    /// the fetch, and the reader then ends with an error after the verified bytes
    /// it already holds.
    ///
    /// The drive borrows `'a` from its sources — a real `PeerSource` over a
    /// borrowed `Endpoint` is not `'static` — so the caller keeps the endpoint
    /// (and candidates) alive for as long as it polls the drive. The reader owns
    /// its state and borrows nothing.
    ///
    /// On a REVISIT — `cache` already holds the whole blob — the reader serves
    /// straight from the cache, the drive is already complete, and the network is
    /// never touched. Otherwise the fetch never runs more than
    /// `config.read_ahead_bytes` (raised to at least [`PULL_WINDOW_FLOOR`]) ahead
    /// of the consumer's cursor, and the verified blob is teed to `cache` for the
    /// next revisit.
    ///
    /// `total_bytes` keys the fill store and `hash` is the bao root every byte is
    /// verified against; a wrong size or hash surfaces as a read error, never as
    /// silent corruption.
    ///
    /// # Errors
    ///
    /// A cache read fault at open, or a fill-store open/create I/O error. Faults
    /// DURING the fetch (a refused or stalled pull, a verification failure)
    /// surface later, from the reader.
    pub async fn open<'a>(
        self,
        hash: [u8; 32],
        total_bytes: u64,
        config: &PullConfig,
        cache: Arc<dyn BlobCache>,
    ) -> anyhow::Result<(VerifiedReader, StreamDrive<'a>)>
    where
        S: 'a,
        F: 'a,
    {
        if let Some(cached) = cache.get(hash, 0, total_bytes).await?
            && u64::try_from(cached.len()).is_ok_and(|len| len == total_bytes)
        {
            return Ok((
                VerifiedReader::Cached {
                    data: cached,
                    pos: 0,
                },
                StreamDrive {
                    fetch: None,
                    state: None,
                },
            ));
        }

        let stem = blake3::Hash::from_bytes(hash).to_hex();
        let store =
            ClientRangedStore::open_or_create(self.scratch, stem.as_str(), hash, total_bytes)
                .map_err(|e| anyhow::anyhow!("open stream store for {stem}: {e}"))?;
        let state = Arc::new(StreamState {
            store,
            cursor: AtomicU64::new(0),
            total: total_bytes,
            consumed: Notify::new(),
            progressed: Notify::new(),
            outcome: Mutex::new(None),
        });
        let fetch = Box::pin(run_drive(
            Arc::clone(&state),
            self.candidates,
            config.read_ahead_bytes,
            config.streamer_lane_cap,
            self.funder,
            self.drive_config,
            cache,
            hash,
        ));
        Ok((
            VerifiedReader::Live(LiveReader {
                state: Arc::clone(&state),
                read: None,
                buffered: Bytes::new(),
            }),
            StreamDrive {
                fetch: Some(fetch),
                state: Some(state),
            },
        ))
    }
}

/// The fetch half of an open stream: a [`Future`] that pulls the blob into the
/// fill store, paced on the reader's cursor, and resolves once the fetch ends.
/// Its outcome reaches the consumer through the [`VerifiedReader`]: a failure
/// surfaces as a read error after the verified prefix before it.
///
/// Poll it for as long as the reader is read, apart from the reads —
/// [`Self::alongside`] does exactly that. Dropping it before it resolves stops
/// the fetch and ends the reader with an error.
pub struct StreamDrive<'a> {
    /// The fetch. `None` once it has resolved, and for a cache hit.
    fetch: Option<Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>>>,
    /// The state the outcome is recorded into. `None` for a cache hit.
    state: Option<Arc<StreamState>>,
}

impl std::fmt::Debug for StreamDrive<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamDrive")
            .field("running", &self.fetch.is_some())
            .finish_non_exhaustive()
    }
}

impl StreamDrive<'_> {
    /// Run `consume` to completion while this drive runs beside it, and return
    /// `consume`'s output. The drive is polled first on every wake, so an open
    /// paid leg is never starved by a slow consumer. If the drive finishes first,
    /// `consume` keeps running to the end of the stream. If `consume` finishes
    /// first, the drive stays where it is: call this again to resume it, or drop
    /// it to stop the fetch.
    pub async fn alongside<T>(&mut self, consume: impl Future<Output = T>) -> T {
        tokio::pin!(consume);
        tokio::select! {
            biased;
            () = &mut *self => consume.await,
            out = &mut consume => out,
        }
    }
}

impl Future for StreamDrive<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        let Some(fetch) = this.fetch.as_mut() else {
            return Poll::Ready(());
        };
        match fetch.as_mut().poll(cx) {
            Poll::Ready(result) => {
                this.fetch = None;
                if let Some(state) = &this.state {
                    state.finish(result);
                }
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for StreamDrive<'_> {
    /// A drive dropped before it resolves records a failure, so a reader still
    /// waiting on it ends with an error instead of hanging.
    fn drop(&mut self) {
        if self.fetch.is_some()
            && let Some(state) = &self.state
        {
            state.finish(Err(anyhow::anyhow!(
                "stream fetch stopped before the blob was complete"
            )));
        }
    }
}

/// An [`AsyncRead`] over a blob's verified contiguous front, clamped to the
/// verified frontier by construction (it only reads bytes the engine has already
/// bao-verified). Its cursor paces the paired [`StreamDrive`].
pub enum VerifiedReader {
    /// A whole-blob cache hit: served straight from memory, no fetch.
    Cached {
        /// The cached blob.
        data: Bytes,
        /// Bytes already handed to the consumer.
        pos: usize,
    },
    /// A live fetch: the reader waits for the drive to verify bytes into the
    /// fill store and hands out the verified prefix.
    Live(LiveReader),
}

/// One in-flight [`next_verified`] read: the next verified span, or `None` at the
/// end of the stream.
type VerifiedRead = Pin<Box<dyn Future<Output = io::Result<Option<Bytes>>> + Send>>;

/// The live half of a [`VerifiedReader`]: it reads the verified prefix out of the
/// fill store as the paired [`StreamDrive`] lands it, and advances the cursor the
/// drive paces on.
pub struct LiveReader {
    state: Arc<StreamState>,
    /// An in-flight store read of the next verified span: `Some(bytes)`, or
    /// `None` at the end of the stream. It waits inside for bytes to land.
    read: Option<VerifiedRead>,
    /// Verified bytes read from the store, not yet handed to the consumer.
    buffered: Bytes,
}

/// The end of the store's contiguous verified-present run from offset 0 — the
/// frontier the reader may read up to. Several lanes can fill the store at once,
/// so the present set may hold later runs too; only the run from offset 0 is
/// readable in order, and anything else means `0`.
///
/// # Errors
///
/// A store fault reading its present ranges (I/O, a poisoned lock). Surfaced to
/// the reader rather than masked as "nothing present", so a real fault does not
/// look like an empty stream that stalls or ends early.
async fn present_frontier(store: &ClientRangedStore, total: u64) -> anyhow::Result<u64> {
    let present = store
        .present_ranges()
        .await
        .map_err(|e| anyhow::anyhow!("read present ranges: {e}"))?;
    Ok(
        match crate::driver::contiguous_byte_ranges(&present, total).first() {
            Some(&(0, len)) => len,
            _ => 0,
        },
    )
}

/// Wait until verified bytes past `cursor` are in the store, then read up to
/// [`MAX_READ_CHUNK`] of them. `None` is the end of the stream. A drive failure
/// surfaces only once every verified byte before it has been read.
async fn next_verified(state: Arc<StreamState>, cursor: u64) -> io::Result<Option<Bytes>> {
    loop {
        if cursor >= state.total {
            return Ok(None);
        }
        // Register for the drive's wakeup BEFORE reading the frontier, so a
        // progress report between the read and the wait is not lost.
        let progressed = state.progressed.notified();
        tokio::pin!(progressed);
        progressed.as_mut().enable();
        // Read the outcome before the frontier: a drive that ended before this
        // frontier read left every verified byte in it.
        let outcome = state.outcome();
        let frontier = present_frontier(&state.store, state.total)
            .await
            .map_err(|e| io::Error::other(format!("present frontier: {e:#}")))?;
        if frontier > cursor {
            let len = (frontier - cursor).min(MAX_READ_CHUNK);
            return state
                .store
                .read(cursor, len)
                .await
                .map(Some)
                .map_err(|e| io::Error::other(format!("read verified prefix: {e:#}")));
        }
        match outcome {
            Some(Err(e)) => {
                return Err(io::Error::other(format!("stream fetch failed: {e:#}")));
            }
            Some(Ok(())) => {
                return Err(io::Error::other(format!(
                    "stream fetch finished with {cursor} of {} bytes readable",
                    state.total
                )));
            }
            None => {}
        }
        // Nothing new yet: wait for the drive, or re-check on the bounded tick.
        let _ = tokio::time::timeout(FRONTIER_RECHECK, progressed).await;
    }
}

impl std::fmt::Debug for LiveReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveReader")
            .field("cursor", &self.state.cursor.load(Ordering::SeqCst))
            .field("total", &self.state.total)
            .field("buffered", &self.buffered.len())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for VerifiedReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cached { data, pos } => f
                .debug_struct("VerifiedReader::Cached")
                .field("len", &data.len())
                .field("pos", pos)
                .finish(),
            Self::Live(_) => f
                .debug_struct("VerifiedReader::Live")
                .finish_non_exhaustive(),
        }
    }
}

impl LiveReader {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        loop {
            // 1. Hand out any bytes already read from the store.
            if !self.buffered.is_empty() {
                let n = self.buffered.len().min(buf.remaining());
                if n == 0 {
                    return Poll::Ready(Ok(()));
                }
                // Convert BEFORE handing out any bytes, so a (practically
                // impossible) failure surfaces as an error rather than
                // desynchronizing the cursor from bytes already delivered.
                let Ok(taken) = u64::try_from(n) else {
                    return Poll::Ready(Err(io::Error::other("read length does not fit u64")));
                };
                let chunk = self.buffered.split_to(n);
                buf.put_slice(&chunk);
                self.state.cursor.fetch_add(taken, Ordering::SeqCst);
                // Unpark a pull parked on a full read-ahead window.
                self.state.consumed.notify_waiters();
                return Poll::Ready(Ok(()));
            }
            // 2. Wait for (or start) the read of the next verified span.
            let read = self.read.get_or_insert_with(|| {
                let state = Arc::clone(&self.state);
                let cursor = state.cursor.load(Ordering::SeqCst);
                Box::pin(next_verified(state, cursor))
            });
            match read.as_mut().poll(cx) {
                Poll::Ready(Ok(Some(bytes))) => {
                    self.read = None;
                    self.buffered = bytes;
                }
                Poll::Ready(Ok(None)) => {
                    self.read = None;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(e)) => {
                    self.read = None;
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncRead for VerifiedReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            VerifiedReader::Cached { data, pos } => {
                let remaining = data.len().saturating_sub(*pos);
                if remaining == 0 {
                    return Poll::Ready(Ok(()));
                }
                let n = remaining.min(buf.remaining());
                if let Some(slice) = data.get(*pos..pos.saturating_add(n)) {
                    buf.put_slice(slice);
                    *pos = pos.saturating_add(n);
                }
                Poll::Ready(Ok(()))
            }
            VerifiedReader::Live(live) => live.poll_read(cx, buf),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use alloy::primitives::{Address, B256, U256};
    use alloy::signers::local::PrivateKeySigner;
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use decdn_bao_range::{AlignedRange, IROH_BLOCK_SIZE, encode_verified_range};
    use decdn_incentive::DepositOutcome;
    use tokio::io::AsyncReadExt;

    use super::{StreamCandidate, Streamer, VerifiedReader};
    use crate::driver::DriveConfig;
    use crate::pacer::PULL_WINDOW_FLOOR;
    use crate::source::{BlobSource, FakeFunder, ScriptedSource, SourceFuture};
    use crate::{
        BlobCache, Cumulative, MemoryBlobCache, NoCache, PoolContext, PoolLedger, PullConfig,
        UpstreamPullHeader, VoucherProgress,
    };

    fn healthy_ctx() -> PoolContext {
        PoolContext {
            pool_id: B256::ZERO,
            provider: Address::repeat_byte(0xAB),
            deposit: U256::from(u128::MAX),
            client_signer: Arc::new(PrivateKeySigner::random()),
            voucher_domain: decdn_incentive::bind_node_id_domain(1, Address::ZERO),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        }
    }

    /// One provider candidate paying `provider` out of `ledger`. Distinct
    /// `provider` bytes give the one-lane-per-provider set the scheduler requires.
    fn candidate<S>(source: S, ledger: Arc<PoolLedger>, provider: u8) -> StreamCandidate<S> {
        let mut ctx = healthy_ctx();
        ctx.provider = Address::repeat_byte(provider);
        StreamCandidate {
            source,
            ctx: Arc::new(Mutex::new(ctx)),
            ledger,
            coverage: None,
        }
    }

    #[test]
    fn a_candidates_measured_coverage_reaches_its_lane() {
        // A partial holder's measured coverage (#1506) must reach its SourceLane
        // so the scheduler never assigns it a range it does not hold; a candidate
        // with no measured coverage (`None`) falls back to a full holder.
        let total: u64 = 500 * 1024 * 1024;
        let nb = decdn_protocol::num_blocks(total);
        assert!(
            nb >= 2,
            "test needs a multi-block blob to tell partial from full"
        );
        let partial = decdn_protocol::Coverage::from_block_indices(nb, [0].into_iter());
        let led = Arc::new(PoolLedger::new(Cumulative::default()));

        let mut c0 = candidate((), Arc::clone(&led), 1);
        c0.coverage = Some(partial.clone());
        let c1 = candidate((), Arc::clone(&led), 2);
        let candidates = vec![c0, c1];

        let built = super::source_lanes(&candidates, total);
        let [partial_lane, full_lane] = built.as_slice() else {
            unreachable!("source_lanes yields one lane per candidate");
        };
        assert_eq!(
            partial_lane.coverage, partial,
            "a partial holder's measured coverage must reach its lane",
        );
        assert_eq!(
            full_lane.coverage,
            decdn_protocol::Coverage::full(nb),
            "a `None` candidate is a full holder",
        );
    }

    fn funder() -> FakeFunder {
        FakeFunder::new(3, DepositOutcome::Added(U256::from(u128::MAX)))
    }

    fn drive_config() -> DriveConfig {
        DriveConfig {
            working_deposit: U256::from(u128::MAX),
            seller_reserve: U256::ZERO,
            max_settle_waits: 2,
            settle_backoff: Duration::ZERO,
        }
    }

    fn payload(len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        let mut x: u32 = 0x2468_ace0;
        for b in &mut out {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x.to_le_bytes()[0];
        }
        out
    }

    /// A paying `ScriptedSource` plus a shared ledger, ready to drive to Done.
    fn paying_source(blob: Vec<u8>) -> anyhow::Result<(ScriptedSource, Arc<PoolLedger>)> {
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(blob)?.paying(Arc::clone(&ledger));
        Ok((source, ledger))
    }

    /// Drive a fresh stream over `source` and return the fully drained bytes.
    async fn drain(
        source: ScriptedSource,
        ledger: Arc<PoolLedger>,
        cache: Arc<dyn BlobCache>,
        config: &PullConfig,
    ) -> anyhow::Result<Vec<u8>> {
        let root = source.root();
        let total = source.total_bytes();
        let scratch = tempfile::tempdir()?;
        let streamer = Streamer::new(
            vec![candidate(source, ledger, 0xA1)],
            funder(),
            drive_config(),
            scratch.path(),
        );
        let (mut reader, mut drive) = streamer.open(root, total, config, cache).await?;
        let mut out = Vec::new();
        drive.alongside(reader.read_to_end(&mut out)).await?;
        Ok(out)
    }

    /// The whole drained stream is BLAKE3-identical to the blob, with the default
    /// (no-op) cache.
    #[tokio::test]
    async fn drained_stream_is_blake3_identical() -> anyhow::Result<()> {
        let blob = payload(1_500_000);
        let (source, ledger) = paying_source(blob.clone())?;
        let root = source.root();
        let got = drain(source, ledger, Arc::new(NoCache), &PullConfig::default()).await?;
        anyhow::ensure!(got == blob, "drained stream must be byte-identical");
        anyhow::ensure!(
            blake3::hash(&got).as_bytes() == &root,
            "drained stream must be BLAKE3-identical to the root"
        );
        Ok(())
    }

    /// A sink that reports `caches() == false` is NEVER teed the whole blob on a
    /// clean finish — the `Streamer` skips the whole-blob read (and the memory it
    /// would cost) that only exists to populate a cache. This is what keeps a
    /// `decdn fetch -o -` of a huge blob from spiking its whole size into RAM at
    /// the end.
    #[tokio::test]
    async fn a_non_caching_sink_is_not_teed_the_whole_blob() -> anyhow::Result<()> {
        #[derive(Default)]
        struct PutSpy {
            puts: std::sync::atomic::AtomicUsize,
        }
        impl BlobCache for PutSpy {
            fn caches(&self) -> bool {
                false
            }
            fn get(
                &self,
                _hash: [u8; 32],
                _offset: u64,
                _len: u64,
            ) -> crate::SinkFuture<'_, Option<Bytes>> {
                Box::pin(async { Ok(None) })
            }
            fn put(
                &self,
                _hash: [u8; 32],
                _offset: u64,
                _bytes: Bytes,
            ) -> crate::SinkFuture<'_, ()> {
                self.puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            }
        }

        let blob = payload(1_500_000);
        let (source, ledger) = paying_source(blob.clone())?;
        let spy = Arc::new(PutSpy::default());
        let got = drain(source, ledger, spy.clone(), &PullConfig::default()).await?;

        anyhow::ensure!(got == blob, "the stream still drains correctly");
        anyhow::ensure!(
            spy.puts.load(std::sync::atomic::Ordering::SeqCst) == 0,
            "a non-caching sink must not be teed the whole blob"
        );
        Ok(())
    }

    /// A pre-populated whole-blob cache serves the stream WITHOUT touching the
    /// network: the source is never opened, and the output is still identical.
    #[tokio::test]
    async fn prepopulated_cache_serves_without_fetching() -> anyhow::Result<()> {
        let blob = payload(600_000);
        let (source, ledger) = paying_source(blob.clone())?;
        let root = source.root();
        let total = source.total_bytes();
        // A clone shares the `opened` log, so we can prove nothing was fetched.
        let probe = source.clone();

        let cache = Arc::new(MemoryBlobCache::new());
        cache.put(root, 0, Bytes::from(blob.clone())).await?;

        let scratch = tempfile::tempdir()?;
        let streamer = Streamer::new(
            vec![candidate(source, ledger, 0xA1)],
            funder(),
            drive_config(),
            scratch.path(),
        );
        let (mut reader, mut drive) = streamer
            .open(root, total, &PullConfig::default(), cache)
            .await?;
        anyhow::ensure!(
            matches!(reader, VerifiedReader::Cached { .. }),
            "a whole-blob cache hit must serve from cache"
        );
        let mut out = Vec::new();
        drive.alongside(reader.read_to_end(&mut out)).await?;
        anyhow::ensure!(out == blob, "cache-served stream must be identical");
        anyhow::ensure!(
            probe.opened_ranges().is_empty(),
            "a cache hit must not open the source at all, opened {:?}",
            probe.opened_ranges()
        );
        Ok(())
    }

    /// A tampered chunk group fails the pull: the reader yields only the verified
    /// prefix before it and then surfaces an error — never the tampered bytes.
    #[tokio::test]
    async fn tampered_tail_fails_and_never_yields_unverified() -> anyhow::Result<()> {
        let blob = payload(400_000);
        let source = TamperTailSource::new(blob.clone())?;
        let root = source.root;
        let total = source.total;

        let scratch = tempfile::tempdir()?;
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let streamer = Streamer::new(
            vec![candidate(source, ledger, 0xA1)],
            funder(),
            drive_config(),
            scratch.path(),
        );
        let (mut reader, mut drive) = streamer
            .open(root, total, &PullConfig::default(), Arc::new(NoCache))
            .await?;

        let mut out = Vec::new();
        let result = drive.alongside(reader.read_to_end(&mut out)).await;
        anyhow::ensure!(
            result.is_err(),
            "a tampered tail must surface as a read error, not EOF"
        );
        anyhow::ensure!(
            (out.len() as u64) < total,
            "the tampered tail must not be yielded ({} of {total} bytes)",
            out.len()
        );
        anyhow::ensure!(
            blob.get(..out.len()) == Some(out.as_slice()),
            "every byte yielded before the failure must be a verified prefix of the blob"
        );
        Ok(())
    }

    /// A candidate that faults mid-stream fails over to another: the faulty
    /// candidate delivers a prefix, faults (a retryable transport reset), and the
    /// healthy candidate covers the remainder — so the drained stream is still
    /// BLAKE3-identical. The verified prefix the faulty lane delivered is never
    /// re-pulled (the store resumes from `missing_ranges`).
    #[tokio::test]
    async fn a_faulting_candidate_fails_over_and_the_stream_completes() -> anyhow::Result<()> {
        let blob = payload(400_000);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        // The faulty candidate delivers ~100 KiB of wire, then parks a retryable
        // fault; the healthy candidate holds the whole blob.
        let faulty = ScriptedSource::new(blob.clone())?
            .paying(Arc::clone(&ledger_a))
            .with_fault_after(100_000, || anyhow::anyhow!("simulated transport reset"));
        let healthy = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger_b));
        let root = healthy.root();
        let total = healthy.total_bytes();
        // A probe on the faulty source proves it delivered a prefix before failing
        // over (mid-stream failover, not "never started").
        let faulty_probe = faulty.clone();

        let scratch = tempfile::tempdir()?;
        let streamer = Streamer::new(
            vec![
                candidate(faulty, ledger_a, 0xA1),
                candidate(healthy, ledger_b, 0xB2),
            ],
            funder(),
            drive_config(),
            scratch.path(),
        );
        let (mut reader, mut drive) = streamer
            .open(root, total, &PullConfig::default(), Arc::new(NoCache))
            .await?;
        let mut out = Vec::new();
        drive.alongside(reader.read_to_end(&mut out)).await?;

        anyhow::ensure!(
            out == blob,
            "the stream must complete byte-identical despite a mid-stream candidate fault"
        );
        anyhow::ensure!(
            faulty_probe.delivered_bytes() > 0,
            "the faulty candidate must have delivered a prefix before failing over"
        );
        Ok(())
    }

    /// A slow consumer keeps the fetch within one read-ahead window of its read
    /// cursor: the store never holds more than the window (+ chunk-group slack)
    /// ahead of what the consumer has taken — that bound IS the outstanding-spend
    /// bound. The whole blob still arrives byte-identical.
    #[tokio::test]
    async fn bounded_read_ahead_holds_the_spend_bound() -> anyhow::Result<()> {
        use decdn_bao_range::CHUNK_GROUP_BYTES;
        use tokio::io::AsyncReadExt as _;

        let blob = payload(4 * 1024 * 1024);
        let (source, ledger) = paying_source(blob.clone())?;
        let root = source.root();
        let total = source.total_bytes();
        // The smallest window the Streamer runs: several of them fit in the blob.
        let read_ahead = PULL_WINDOW_FLOOR;
        let config = PullConfig {
            read_ahead_bytes: read_ahead,
            ..PullConfig::default()
        };

        let scratch = tempfile::tempdir()?;
        let streamer = Streamer::new(
            vec![candidate(source, ledger, 0xA1)],
            funder(),
            drive_config(),
            scratch.path(),
        );
        let (mut reader, mut drive) = streamer
            .open(root, total, &config, Arc::new(NoCache))
            .await?;

        // Drain a few bytes at a time while the drive runs beside the reads;
        // after each read, the fetch must not have run more than one window (plus
        // a chunk-group of alignment slack) ahead.
        let out = drive
            .alongside(async {
                let mut out = Vec::new();
                let mut chunk = [0u8; 997];
                loop {
                    let n = reader.read(&mut chunk).await?;
                    if n == 0 {
                        break;
                    }
                    out.extend_from_slice(chunk.get(..n).unwrap_or_default());
                    let consumed = u64::try_from(out.len())?;
                    let ahead = match &reader {
                        VerifiedReader::Live(live) => {
                            let present = super::present_frontier(&live.state.store, total).await?;
                            present.saturating_sub(consumed)
                        }
                        VerifiedReader::Cached { .. } => 0,
                    };
                    anyhow::ensure!(
                        ahead <= read_ahead + CHUNK_GROUP_BYTES,
                        "in-flight {ahead} exceeded the read-ahead bound {read_ahead} \
                         (+ one group)"
                    );
                }
                Ok(out)
            })
            .await?;
        anyhow::ensure!(out == blob, "the bounded stream must still be identical");
        Ok(())
    }

    /// The drive runs apart from the reads: with the consumer not reading at all,
    /// it still fills the store — so an open paid leg keeps paying — and it stops
    /// at the read-ahead window rather than running on through the blob.
    #[tokio::test]
    async fn the_drive_fills_one_window_while_the_consumer_is_paused() -> anyhow::Result<()> {
        use decdn_bao_range::CHUNK_GROUP_BYTES;

        let blob = payload(4 * 1024 * 1024);
        let (source, ledger) = paying_source(blob.clone())?;
        let root = source.root();
        let total = source.total_bytes();
        let config = PullConfig {
            read_ahead_bytes: PULL_WINDOW_FLOOR,
            ..PullConfig::default()
        };
        let scratch = tempfile::tempdir()?;
        let streamer = Streamer::new(
            vec![candidate(source, ledger, 0xA1)],
            funder(),
            drive_config(),
            scratch.path(),
        );
        let (mut reader, mut drive) = streamer
            .open(root, total, &config, Arc::new(NoCache))
            .await?;

        // A paused consumer: nothing is read while the drive runs.
        drive
            .alongside(tokio::time::sleep(Duration::from_millis(300)))
            .await;
        let present = match &reader {
            VerifiedReader::Live(live) => super::present_frontier(&live.state.store, total).await?,
            VerifiedReader::Cached { .. } => anyhow::bail!("a cold stream must be live"),
        };
        anyhow::ensure!(
            present > 0,
            "the drive must fill the store while the consumer is paused"
        );
        anyhow::ensure!(
            present <= PULL_WINDOW_FLOOR + CHUNK_GROUP_BYTES,
            "a paused consumer must hold the fetch to one window, got {present}"
        );

        // The consumer resumes and the same drive carries the stream to the end.
        let mut out = Vec::new();
        drive.alongside(reader.read_to_end(&mut out)).await?;
        anyhow::ensure!(out == blob, "the resumed stream must be identical");
        Ok(())
    }

    /// A `read_ahead_bytes` below one pull-window floor is raised to it, so the
    /// stream still completes rather than parking before its first byte.
    #[tokio::test]
    async fn a_sub_floor_read_ahead_still_streams() -> anyhow::Result<()> {
        let blob = payload(3 * 1024 * 1024);
        let (source, ledger) = paying_source(blob.clone())?;
        let config = PullConfig {
            read_ahead_bytes: 1,
            ..PullConfig::default()
        };
        let out = tokio::time::timeout(
            Duration::from_secs(30),
            drain(source, ledger, Arc::new(NoCache), &config),
        )
        .await
        .map_err(|_| anyhow::anyhow!("a sub-floor read-ahead must not hang the stream"))??;
        anyhow::ensure!(out == blob, "the stream must be identical");
        Ok(())
    }

    /// The front lane faults while the other lane is parked on a range more than
    /// one window ahead of the reader. The parked lane must give up its range and
    /// take the front's remainder, or neither the reader nor the parked lane can
    /// ever move again.
    #[tokio::test]
    async fn a_front_fault_moves_a_parked_lane_to_the_front() -> anyhow::Result<()> {
        let blob = payload(4 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let faulty = ScriptedSource::new(blob.clone())?
            .paying(Arc::clone(&ledger_a))
            .with_fault_after(100_000, || anyhow::anyhow!("simulated transport reset"));
        let healthy = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger_b));
        let root = healthy.root();
        let total = healthy.total_bytes();
        let scratch = tempfile::tempdir()?;
        let streamer = Streamer::new(
            vec![
                candidate(faulty, ledger_a, 0xA1),
                candidate(healthy, ledger_b, 0xB2),
            ],
            funder(),
            drive_config(),
            scratch.path(),
        );
        let config = PullConfig {
            read_ahead_bytes: PULL_WINDOW_FLOOR,
            ..PullConfig::default()
        };
        let (mut reader, mut drive) = streamer
            .open(root, total, &config, Arc::new(NoCache))
            .await?;
        let mut out = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(30),
            drive.alongside(reader.read_to_end(&mut out)),
        )
        .await
        .map_err(|_| anyhow::anyhow!("stream hung after {} of {total} bytes", out.len()))??;
        anyhow::ensure!(out == blob, "the stream must complete byte-identical");
        Ok(())
    }

    /// A [`BlobSource`] that yields genuine bao wire with its LAST content byte
    /// flipped, so the decoder rejects the final chunk group with a hash
    /// mismatch. Unpaid (rate 0): the fetch fails at ingest before payment.
    struct TamperTailSource {
        root: [u8; 32],
        blob: Bytes,
        outboard: Bytes,
        total: u64,
    }

    impl TamperTailSource {
        fn new(blob: Vec<u8>) -> anyhow::Result<Self> {
            let blob = Bytes::from(blob);
            let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
            Ok(Self {
                root: *ob.root.as_bytes(),
                total: u64::try_from(blob.len())?,
                blob,
                outboard: Bytes::from(ob.data),
            })
        }
    }

    impl BlobSource for TamperTailSource {
        type Reader = Bytes;

        fn open(
            &self,
            hash: [u8; 32],
            range: AlignedRange,
        ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
            Box::pin(async move {
                anyhow::ensure!(hash == self.root, "tamper source opened for a foreign hash");
                let s = usize::try_from(range.fetch_start())?;
                let e = usize::try_from(range.fetch_end())?;
                let data = self
                    .blob
                    .get(s..e)
                    .ok_or_else(|| anyhow::anyhow!("range out of bounds"))?;
                let combined =
                    encode_verified_range(self.root, &range, data, self.outboard.clone())?;
                let wire = combined
                    .get(8..)
                    .ok_or_else(|| anyhow::anyhow!("wire shorter than its header"))?;
                let mut w = wire.to_vec();
                if let Some(last) = w.last_mut() {
                    *last ^= 0xFF;
                }
                let header = UpstreamPullHeader {
                    total_bytes: self.total,
                    rate_per_mb: 0,
                    interval_bytes: decdn_protocol::client::CHUNK_BYTES,
                    ttfb_ms: 0.0,
                };
                Ok((header, Bytes::from(w)))
            })
        }

        fn finish(&self, _reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
            Box::pin(async { Ok(VoucherProgress::default()) })
        }
    }
}
