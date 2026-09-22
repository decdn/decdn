//! The `Streamer` consumption face (#1848 T6): stream one blob's verified,
//! contiguous front to a consumer AS IT ARRIVES, paced by how fast the consumer
//! reads — a fetch-like, single-blob face (no chunk dedup, unlike the
//! `Downloader`).
//!
//! One engine, two faces: the `Streamer` is an OUTPUT + SCHEDULING adapter over
//! the same paid pull engine, not a second engine. It drives the fetch through
//! [`crate::driver::drive`] with a [`crate::pacer::WindowPacer`] whose window is
//! the caller's [`PullConfig::read_ahead_bytes`] and whose downstream frontier is
//! the CONSUMER'S read cursor. So the pull never runs more than the read-ahead
//! bound ahead of what the consumer has read — fetch (and pay for) a two-hour
//! movie's first `read_ahead_bytes` only, for a viewer who stops after five
//! minutes.
//!
//! [`VerifiedReader`] is the [`tokio::io::AsyncRead`] the caller drains. Its
//! cursor is clamped to the store's verified contiguous frontier BY
//! CONSTRUCTION: it only ever reads bytes the engine has already bao-verified
//! into the store, so a tampered chunk group fails the pull and surfaces as a
//! read error rather than ever reaching the consumer.

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

use crate::driver::{DriveConfig, PacingWait};
use crate::pacer::{DownstreamFrontier, WindowPacer};
use crate::scheduler::{ConsumptionPacing, MultiSourceConfig, SourceLane, multi_source_fetch};
use crate::sink::BlobCache;
use crate::source::{BlobSource, Funder};
use crate::{ClientRangedStore, PoolContext, PoolLedger, PullConfig, RangedStore};

/// Shared between the drive future and the [`VerifiedReader`]: the fill store,
/// the two frontiers, and the wake signals that pace one against the other.
struct StreamState {
    /// The bao-verifying fill store the drive future writes and the reader reads.
    store: ClientRangedStore,
    /// The verified contiguous content frontier the drive future has delivered.
    /// Published from the driver's progress callback, and set to `total` on a
    /// clean finish. The reader never reads past it.
    frontier: AtomicU64,
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
}

/// The [`PacingWait`] the streaming drive parks on when its read-ahead window is
/// full: it resolves once the consumer's cursor advances past what the `Wait`
/// observed.
struct ConsumedWait {
    state: Arc<StreamState>,
}

impl PacingWait for ConsumedWait {
    fn wait(&self, observed: DownstreamFrontier) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
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
    let pacer = WindowPacer::new(read_ahead);
    let on_progress = {
        let state = Arc::clone(&state);
        move |position: u64, _total: u64| {
            // Observability only (the reader reads the store's checkpointed present
            // frontier, not this decoder-side hint). Monotone: a `max` guards the
            // base-present emit from ever lowering it.
            let prev = state.frontier.load(Ordering::SeqCst);
            state.frontier.store(prev.max(position), Ordering::SeqCst);
        }
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
    // Every candidate holds the whole blob — discovery yields blob holders, and a
    // partial-coverage holder is the Downloader's concern (#1506), not the
    // single-blob Streamer's.
    let coverage = Coverage::full(num_blocks(state.total));
    let lanes: Vec<SourceLane<'_, S>> = candidates
        .iter()
        .map(|c| SourceLane {
            source: &c.source,
            ctx: Arc::clone(&c.ctx),
            ledger: Arc::clone(&c.ledger),
            coverage: coverage.clone(),
        })
        .collect();
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
        state.frontier.store(state.total, Ordering::SeqCst);
        // Tee the whole verified blob to the cache for a revisit. Best-effort:
        // a cache write failure never fails the delivered stream. The store is
        // left at `.partial` (a stream is not kept as a file), and reading its
        // fully-present content needs no finalize.
        if let Ok(whole) = state.store.read(0, state.total).await {
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
    /// kept, so a temporary directory is the usual choice.
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
    /// `total_bytes`), returning the [`VerifiedReader`] the caller drains.
    ///
    /// The returned reader borrows `'a` from its sources — a real `PeerSource`
    /// over a borrowed `Endpoint` is not `'static`, so the caller keeps the
    /// endpoint (and candidates) alive for as long as it drains the reader.
    ///
    /// On a REVISIT — `cache` already holds the whole blob — the reader serves
    /// straight from the cache and the network is never touched. Otherwise the
    /// fetch runs lazily: it advances only as the consumer reads, never more than
    /// `config.read_ahead_bytes` ahead of the consumer's cursor, and every
    /// verified byte is teed to `cache` for the next revisit.
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
    ) -> anyhow::Result<VerifiedReader<'a>>
    where
        S: 'a,
        F: 'a,
    {
        if let Some(cached) = cache.get(hash, 0, total_bytes).await?
            && u64::try_from(cached.len()).is_ok_and(|len| len == total_bytes)
        {
            return Ok(VerifiedReader::Cached {
                data: cached,
                pos: 0,
            });
        }

        let stem = blake3::Hash::from_bytes(hash).to_hex();
        let store =
            ClientRangedStore::open_or_create(self.scratch, stem.as_str(), hash, total_bytes)
                .map_err(|e| anyhow::anyhow!("open stream store for {stem}: {e}"))?;
        let state = Arc::new(StreamState {
            store,
            frontier: AtomicU64::new(0),
            cursor: AtomicU64::new(0),
            total: total_bytes,
            consumed: Notify::new(),
        });
        let drive = Box::pin(run_drive(
            Arc::clone(&state),
            self.candidates,
            config.read_ahead_bytes,
            config.streamer_lane_cap,
            self.funder,
            self.drive_config,
            cache,
            hash,
        ));
        Ok(VerifiedReader::Live(LiveReader {
            state,
            drive: Some(drive),
            drive_err: None,
            read: None,
            buffered: Bytes::new(),
        }))
    }
}

/// An [`AsyncRead`] over a blob's verified contiguous front, clamped to the
/// verified frontier by construction (it only reads bytes the engine has already
/// bao-verified), consumption-paced (the fetch advances only as it is read).
pub enum VerifiedReader<'a> {
    /// A whole-blob cache hit: served straight from memory, no fetch.
    Cached {
        /// The cached blob.
        data: Bytes,
        /// Bytes already handed to the consumer.
        pos: usize,
    },
    /// A live fetch: the reader cooperatively drives the fetch and reads the
    /// verified prefix from the fill store.
    Live(LiveReader<'a>),
}

/// The live half of a [`VerifiedReader`]: it owns the drive future, polls it
/// forward as the consumer reads, and hands out the verified prefix.
///
/// The `'a` is the fetch future's borrow — a real source (a `PeerSource` over a
/// borrowed `Endpoint`) is not `'static`, so the reader borrows for as long as
/// its sources do rather than forcing a `'static` bound.
pub struct LiveReader<'a> {
    state: Arc<StreamState>,
    /// The streaming fetch. `None` once it has finished (cleanly or with an
    /// error parked in `drive_err`). Borrows `'a` from its sources.
    drive: Option<Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>>>,
    /// The fetch's terminal error, surfaced to the consumer once the verified
    /// prefix before it is drained.
    drive_err: Option<anyhow::Error>,
    /// An in-flight store read for the current verified prefix span. It captures
    /// an `Arc<StreamState>`, so it needs no borrow of `'a`.
    read: Option<Pin<Box<dyn Future<Output = io::Result<Bytes>>>>>,
    /// Verified bytes read from the store, not yet handed to the consumer.
    buffered: Bytes,
}

/// The end of the store's contiguous verified-present run from offset 0 — the
/// frontier the reader may read up to. The fetch is single-source and in-order,
/// so the present set is one leading `[0, f)` run; anything else means `0`
/// (nothing contiguously readable yet).
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

impl std::fmt::Debug for LiveReader<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveReader")
            .field("cursor", &self.state.cursor.load(Ordering::SeqCst))
            .field("frontier", &self.state.frontier.load(Ordering::SeqCst))
            .field("total", &self.state.total)
            .field("draining", &self.drive.is_some())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for VerifiedReader<'_> {
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

impl LiveReader<'_> {
    /// Issue a read of the verified prefix `[cursor, present_frontier)` if none is
    /// in flight. The read future itself queries the store's present frontier, so
    /// it reads exactly what the store has durably admitted (never the decoder's
    /// unflushed lead) — and returns an empty `Bytes` when nothing new is readable.
    fn issue_read(&mut self) {
        if self.read.is_some() {
            return;
        }
        let state = Arc::clone(&self.state);
        let cursor = self.state.cursor.load(Ordering::SeqCst);
        self.read = Some(Box::pin(async move {
            let frontier = present_frontier(&state.store, state.total)
                .await
                .map_err(|e| io::Error::other(format!("present frontier: {e:#}")))?;
            if frontier <= cursor {
                return Ok(Bytes::new());
            }
            state
                .store
                .read(cursor, frontier - cursor)
                .await
                .map_err(|e| io::Error::other(format!("read verified prefix: {e:#}")))
        }));
    }

    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        // Per poll: advance the fetch at most one step past an empty read before
        // parking, so a network-bound pull cannot busy-spin the reader.
        let mut drove = false;
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
            // 2. Advance an in-flight store read.
            if let Some(read) = self.read.as_mut() {
                match read.as_mut().poll(cx) {
                    Poll::Ready(Ok(bytes)) => {
                        self.read = None;
                        if !bytes.is_empty() {
                            self.buffered = bytes;
                            continue;
                        }
                        // Empty read: nothing new readable. Fall through to advance
                        // the fetch (or, if it is done, to EOF).
                    }
                    Poll::Ready(Err(e)) => {
                        self.read = None;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
            // 3. No readable bytes right now. If the fetch is done, this is EOF (or
            //    the terminal error, surfaced after its verified prefix drained).
            if self.drive.is_none() {
                if let Some(e) = self.drive_err.take() {
                    return Poll::Ready(Err(io::Error::other(format!(
                        "stream fetch failed: {e:#}"
                    ))));
                }
                return Poll::Ready(Ok(()));
            }
            // 4. Advance the fetch one step, then re-read. Reachable only when the
            //    read-ahead window is not full (cursor == present frontier), so the
            //    pull is network-bound here, never parked on the consumer.
            if let Some(drive) = self.drive.as_mut() {
                match drive.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        self.drive = None;
                        self.issue_read();
                    }
                    Poll::Ready(Err(e)) => {
                        self.drive = None;
                        self.drive_err = Some(e);
                    }
                    Poll::Pending => {
                        if drove {
                            // Already drove once this poll and re-read empty:
                            // nothing new landed, so park until the pull wakes us.
                            return Poll::Pending;
                        }
                        drove = true;
                        self.issue_read();
                    }
                }
            }
        }
    }
}

impl AsyncRead for VerifiedReader<'_> {
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
        }
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
        let mut reader = streamer.open(root, total, config, cache).await?;
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await?;
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
        let mut reader = streamer
            .open(root, total, &PullConfig::default(), cache)
            .await?;
        anyhow::ensure!(
            matches!(reader, VerifiedReader::Cached { .. }),
            "a whole-blob cache hit must serve from cache"
        );
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await?;
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
        let mut reader = streamer
            .open(root, total, &PullConfig::default(), Arc::new(NoCache))
            .await?;

        let mut out = Vec::new();
        let result = reader.read_to_end(&mut out).await;
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
        let mut reader = streamer
            .open(root, total, &PullConfig::default(), Arc::new(NoCache))
            .await?;
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await?;

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
    /// cursor: the store never holds more than `read_ahead` (+ chunk-group slack)
    /// ahead of what the consumer has taken — that bound IS the outstanding-spend
    /// bound. The whole blob still arrives byte-identical.
    #[tokio::test]
    async fn bounded_read_ahead_holds_the_spend_bound() -> anyhow::Result<()> {
        use decdn_bao_range::CHUNK_GROUP_BYTES;
        use tokio::io::AsyncReadExt as _;

        let blob = payload(400_000);
        let (source, ledger) = paying_source(blob.clone())?;
        let root = source.root();
        let total = source.total_bytes();
        // The clone shares the fill store... no — instead read the reader's own
        // state via a small window and assert the present frontier stays close to
        // the cursor. Window is a handful of chunk groups.
        let read_ahead = 4 * CHUNK_GROUP_BYTES;
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
        let mut reader = streamer
            .open(root, total, &config, Arc::new(NoCache))
            .await?;

        // Drain a few bytes at a time; after each read, the fetch must not have run
        // more than one window (plus a chunk-group of alignment slack) ahead.
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
                "in-flight {ahead} exceeded the read-ahead bound {read_ahead} (+ one group)"
            );
        }
        anyhow::ensure!(out == blob, "the bounded stream must still be identical");
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
