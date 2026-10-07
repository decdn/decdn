//! The `Streamer` consumption face (#1848): stream one blob's verified,
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

use anyhow::Context as _;
use bytes::Bytes;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Notify;

use decdn_protocol::Coverage;

use crate::driver::{DriveConfig, PacingWait, WaitReason};
use crate::health::PeerHealth;
use crate::pacer::{DownstreamFrontier, PULL_WINDOW_FLOOR, WindowPacer};
use crate::scheduler::{AcquireEnv, AcquireTarget, ConsumptionPacing, LaneLease, acquire};
use crate::sink::BlobCache;
use crate::source::Funder;
use crate::source_set::{Holder, SourceProvider, SourceSet};
use crate::stop::{ProgressClock, StopPolicy};
use crate::{ClientRangedStore, PoolContext, PoolLedger, PullConfig, RangedStore};

/// The most verified bytes one store read hands the reader. It bounds the
/// reader's buffer independently of the read-ahead window.
const MAX_READ_CHUNK: u64 = 1024 * 1024;

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
    /// The caller's size hint: the fetch's first claim. The stream ends at the
    /// size a leg proves ([`ClientRangedStore::proven`]), whatever this says.
    hint: u64,
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
///
/// While verified bytes sit unread ahead of the consumer's cursor, the stop
/// policy's clock holds: the consumer is the bottleneck, and a consumer that
/// pauses is not a source that stalls. Once the consumer has read everything
/// present, the missing bytes are the fetch's to deliver, so the clock runs
/// even while a lane waits here.
struct ConsumedWait {
    state: Arc<StreamState>,
    clock: Arc<ProgressClock>,
}

impl ConsumedWait {
    /// Whether verified bytes wait unread ahead of the consumer's `cursor`. A
    /// store fault reads as nothing unread, so the clock runs.
    async fn unread_ahead(&self, cursor: u64) -> bool {
        present_frontier(&self.state.store)
            .await
            .is_ok_and(|frontier| frontier > cursor)
    }
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
                // Register interest BEFORE the checks, so a consume or a landed
                // byte between the checks and the await cannot be missed.
                let consumed = self.state.consumed.notified();
                let landed = self.state.progressed.notified();
                tokio::pin!(consumed, landed);
                consumed.as_mut().enable();
                landed.as_mut().enable();
                let cursor = self.state.cursor.load(Ordering::SeqCst);
                if cursor > observed.served_paid {
                    return;
                }
                let _hold = self.unread_ahead(cursor).await.then(|| self.clock.hold());
                tokio::select! {
                    () = consumed => {}
                    () = landed => {}
                }
            }
        })
    }
}

/// One provider's paid lane: the paid source plus the `(ctx, ledger)` that
/// pays it. A [`SourceProvider`] builds one per holder, and both faces fetch
/// across them.
///
/// A lane that faults mid-fetch cools and returns; its remainder continues
/// from another lane meanwhile, resuming from the store's verified frontier so
/// no delivered byte is re-pulled or re-paid. Every lane names a distinct
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
    /// What this candidate's lane holds while the fetch runs
    /// ([`crate::LaneLease`]). With `widen` set, it is released when the
    /// lane's own worker ends; it is released at the latest when the fetch
    /// returns. Like `coverage`, set it only on a single-target fetch: the
    /// first target's fetch releases it.
    pub lease: LaneLease,
    /// How this lane takes a stream beyond its `lease` ([`crate::LaneWiden`]),
    /// or `None` to stay at one stream: an extra stream for each queued range
    /// that no idle lane takes, and the stream the lane starts again on once
    /// it gave its `lease` back. Each granted stream is given back as its
    /// worker stops, so it serves every target of a [`crate::Downloader`]
    /// alike.
    pub widen: Option<crate::LaneWiden>,
}

impl<S> std::fmt::Debug for StreamCandidate<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamCandidate").finish_non_exhaustive()
    }
}

/// What one stream's drive fetches with: the sources, the payment seams and
/// the stop, moved into the drive future.
struct DriveInputs<P, F> {
    provider: P,
    holders: Vec<Holder>,
    health: Arc<PeerHealth>,
    funder: F,
    drive_config: DriveConfig,
    stop: StopPolicy,
    /// The largest blob accepted, or `0` for no cap.
    max_blob_bytes: u64,
}

/// Run the streaming fetch into the store through [`crate::acquire`], bounded
/// to one read-ahead window ahead of the consumer's cursor and capped to
/// `lane_cap` concurrent lanes, then (on a clean finish) tee the whole verified
/// blob to `cache` for revisits.
///
/// A source that faults cools and returns, and its remainder moves to another
/// meanwhile, resuming from the store's verified frontier so no delivered byte
/// is re-pulled or re-paid.
async fn run_drive<P, F>(
    state: Arc<StreamState>,
    inputs: DriveInputs<P, F>,
    read_ahead: u64,
    lane_cap: usize,
    cache: Arc<dyn BlobCache>,
    hash: [u8; 32],
) -> anyhow::Result<()>
where
    P: SourceProvider,
    F: Funder,
{
    let DriveInputs {
        provider,
        holders,
        health,
        funder,
        drive_config,
        stop,
        max_blob_bytes,
    } = inputs;
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
        clock: Arc::clone(&stop.clock),
    };
    let pacing = ConsumptionPacing {
        downstream: &downstream,
        pacing_wait: &wait,
    };
    let mut sources = SourceSet::new(&provider, hash, health, holders);
    let ranges = [(0, state.store.bound())];
    // Under consumption pacing `acquire` turns its lane watchdog off: a lane
    // parked on the consumer cursor is waiting, not stalled. A genuinely silent
    // source trips its own per-stream idle window mid-read instead.
    let env = AcquireEnv {
        pacer: &pacer,
        funder: &funder,
        drive: &drive_config,
        // Small, bounded front parallelism: a paced stream wants a little
        // same-region fan-out, not a full download's striping.
        max_lanes: lane_cap.max(1),
        stop: &stop,
        on_progress: Some(&on_progress),
        ledgers: None,
        pacing: Some(&pacing),
        max_blob_bytes,
    };
    let result = acquire(
        AcquireTarget {
            store: &state.store,
            hash,
            total_bytes: state.hint,
            ranges: &ranges,
        },
        &mut sources,
        &env,
    )
    .await;
    if result.is_ok() {
        // Tee the whole verified blob, `[0, proven)`, to the cache for a
        // revisit. Best-effort: a cache write failure never fails the delivered
        // stream. The store is left at `.partial` (a stream is not kept as a
        // file), and reading its fully-present content needs no finalize.
        //
        // Only when the cache actually stores it: a `NoCache` (the `decdn fetch
        // -o -` path) reports `caches() == false`, so a huge blob is never read
        // whole into memory just to be dropped — the stream stays memory-bounded.
        if cache.caches()
            && let Some(proven) = state.store.proven()
            && let Ok(whole) = state.store.read(0, proven).await
        {
            let _ = cache.put(hash, 0, whole).await;
        }
    }
    result
}

/// Stream a single blob's verified front to a consumer, paced by consumption.
///
/// Holds the injected pull machinery — the blob's `holders`, the
/// [`SourceProvider`] that builds their lanes, the command-wide
/// [`PeerHealth`], and one `funder` — plus a scratch directory for the fill
/// store. The front is fetched through [`crate::acquire`] across a small,
/// bounded set of lanes; a lane that faults cools and returns.
/// [`Streamer::open`] consumes it to start one stream.
///
/// Read inside [`StreamDrive::alongside`], so the drive keeps paying and
/// draining open legs while the reader waits on its consumer. See the `stream`
/// example for the whole sequence.
///
/// ```no_run
/// use std::path::Path;
/// use std::sync::Arc;
///
/// use decdn_client::driver::DriveConfig;
/// use decdn_client::source::{BlobSource, Funder};
/// use decdn_client::{
///     NoCache, ProgressClock, PullConfig, StaticSources, StopPolicy, StreamCandidate, Streamer,
/// };
///
/// async fn stream<S: BlobSource, F: Funder>(
///     candidates: Vec<StreamCandidate<S>>,
///     funder: F,
///     hash: [u8; 32],
///     total_bytes: u64,
///     scratch: &Path,
/// ) -> anyhow::Result<()> {
///     let sources = StaticSources::new(candidates)?;
///     let holders = sources.holders();
///     let drive = DriveConfig::cli(Default::default());
///     let streamer = Streamer::new(sources, holders, Default::default(), funder, drive, scratch);
///     let stop = StopPolicy::new(true, None, Arc::new(ProgressClock::new()));
///     let (mut reader, mut drive) = streamer
///         .open(hash, total_bytes, &PullConfig::new(), Arc::new(NoCache), stop)
///         .await?;
///     let mut out = tokio::io::stdout();
///     drive.alongside(tokio::io::copy(&mut reader, &mut out)).await?;
///     Ok(())
/// }
/// ```
pub struct Streamer<'a, P, F> {
    provider: P,
    holders: Vec<Holder>,
    health: Arc<PeerHealth>,
    funder: F,
    drive_config: DriveConfig,
    /// A scratch directory the fill store's `.partial` lives in for the stream's
    /// lifetime. The caller owns it (and its cleanup); a streamed blob is not
    /// kept, so a temporary directory is the usual choice. The `.partial` grows
    /// to the whole blob as the stream advances, so the directory needs room
    /// for all of it.
    scratch: &'a Path,
    /// The largest blob accepted, in bytes, or `0` for no cap
    /// ([`Self::max_blob_bytes`]).
    max_blob_bytes: u64,
}

impl<P, F> std::fmt::Debug for Streamer<'_, P, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streamer")
            .field("holders", &self.holders.len())
            .field("drive_config", &self.drive_config)
            .field("scratch", &self.scratch)
            .finish_non_exhaustive()
    }
}

impl<'a, P, F> Streamer<'a, P, F> {
    /// Build a streamer over `holders`, whose lanes `provider` builds, filling
    /// into `scratch`. `health` is the command-wide health the stream's sources
    /// record into.
    #[must_use]
    pub const fn new(
        provider: P,
        holders: Vec<Holder>,
        health: Arc<PeerHealth>,
        funder: F,
        drive_config: DriveConfig,
        scratch: &'a Path,
    ) -> Self {
        Self {
            provider,
            holders,
            health,
            funder,
            drive_config,
            scratch,
            max_blob_bytes: 0,
        }
    }

    /// Cap the stream at `max_blob_bytes` (`0` for no cap). A size claim above
    /// the cap is clamped to it before the fill store is sized, and a blob that
    /// holds bytes past the cap ends the stream with [`crate::BlobTooLarge`]
    /// ([`crate::AcquireEnv::max_blob_bytes`]).
    #[must_use]
    pub const fn max_blob_bytes(mut self, max_blob_bytes: u64) -> Self {
        self.max_blob_bytes = max_blob_bytes;
        self
    }
}

impl<P, F> Streamer<'_, P, F>
where
    P: SourceProvider,
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
    /// (and provider) alive for as long as it polls the drive. The reader owns
    /// its state and borrows nothing.
    ///
    /// `stop` decides when a stream that makes no verified progress gives up;
    /// the reader then ends with [`crate::GaveUp`]'s message.
    ///
    /// On a REVISIT — `cache` already holds the whole blob — the reader serves
    /// straight from the cache, the drive is already complete, and the network is
    /// never touched. Otherwise the fetch never runs more than
    /// `config.read_ahead_bytes` (raised to at least [`PULL_WINDOW_FLOOR`]) ahead
    /// of the consumer's cursor, and the verified blob is teed to `cache` for the
    /// next revisit.
    ///
    /// `total_bytes` is the first size claim, a hint. A resumed store's bound
    /// wins, and a leg that verifies the final chunk proves the size. `hash` is
    /// the bao root every byte is verified against; a wrong hash surfaces as a
    /// read error, never as silent corruption.
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
        stop: StopPolicy,
    ) -> anyhow::Result<(VerifiedReader, StreamDrive<'a>)>
    where
        P: 'a,
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
                    error: None,
                },
            ));
        }

        let stem = blake3::Hash::from_bytes(hash).to_hex();
        // A claim above the cap never sizes the store.
        let total_bytes = if self.max_blob_bytes > 0 {
            total_bytes.min(self.max_blob_bytes)
        } else {
            total_bytes
        };
        // Off the runtime: opening a finalized blob hashes its final file.
        let store = {
            let (scratch, stem) = (self.scratch.to_path_buf(), stem.to_string());
            tokio::task::spawn_blocking(move || {
                ClientRangedStore::open_or_create(&scratch, &stem, hash, total_bytes)
                    .map_err(|e| anyhow::anyhow!("open stream store for {stem}: {e}"))
            })
            .await
            .map_err(|e| anyhow::anyhow!("open stream store task: {e}"))??
        };
        let state = Arc::new(StreamState {
            store,
            cursor: AtomicU64::new(0),
            hint: total_bytes,
            consumed: Notify::new(),
            progressed: Notify::new(),
            outcome: Mutex::new(None),
        });
        let fetch = Box::pin(run_drive(
            Arc::clone(&state),
            DriveInputs {
                provider: self.provider,
                holders: self.holders,
                health: self.health,
                funder: self.funder,
                drive_config: self.drive_config,
                stop,
                max_blob_bytes: self.max_blob_bytes,
            },
            config.read_ahead_bytes,
            config.streamer_lane_cap,
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
                error: None,
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
    /// The typed error the fetch ended with, until [`Self::take_error`] takes
    /// it. The reader carries only its message.
    error: Option<anyhow::Error>,
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

    /// The error the fetch ended with, typed as the fetch returned it (a
    /// [`crate::GaveUp`], a fatal fault, a unanimous verdict of the sources),
    /// once the drive has resolved with one. A reader error that follows a
    /// failed fetch carries only this error's message, so a caller that needs
    /// to act on the error's type takes it here.
    pub const fn take_error(&mut self) -> Option<anyhow::Error> {
        self.error.take()
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
                let recorded = match result {
                    Ok(()) => Ok(()),
                    Err(err) => {
                        let message = anyhow::anyhow!("{err:#}");
                        this.error = Some(err);
                        Err(message)
                    }
                };
                if let Some(state) = &this.state {
                    state.finish(recorded);
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
async fn present_frontier(store: &ClientRangedStore) -> anyhow::Result<u64> {
    let present = store
        .present_ranges()
        .await
        .context("read present ranges")?;
    Ok(
        match crate::driver::contiguous_byte_ranges(&present, store.bound()).first() {
            Some(&(0, len)) => len,
            _ => 0,
        },
    )
}

/// Wait until verified bytes past `cursor` are in the store, then read up to
/// [`MAX_READ_CHUNK`] of them. `None` is the end of the stream: the cursor has
/// reached the size a leg proved. A drive failure surfaces only once every
/// verified byte before it has been read.
async fn next_verified(state: Arc<StreamState>, cursor: u64) -> io::Result<Option<Bytes>> {
    loop {
        if state.store.proven().is_some_and(|proven| cursor >= proven) {
            return Ok(None);
        }
        // Register for both wakeups BEFORE reading the frontier, so a progress
        // report or a checkpoint between the read and the wait is not lost.
        let progressed = state.progressed.notified();
        tokio::pin!(progressed);
        progressed.as_mut().enable();
        let grew = state.store.present_grew().notified();
        tokio::pin!(grew);
        grew.as_mut().enable();
        // Read the outcome before the frontier: a drive that ended before this
        // frontier read left every verified byte in it.
        let outcome = state.outcome();
        let frontier = present_frontier(&state.store)
            .await
            .map_err(|e| io::Error::other(format!("present frontier: {e:#}")))?;
        if frontier > cursor {
            let len = (frontier - cursor).min(MAX_READ_CHUNK);
            return state.store.read(cursor, len).await.map(Some).map_err(|e| {
                io::Error::other(format!("read verified prefix: {:#}", anyhow::Error::new(e)))
            });
        }
        match outcome {
            Some(Err(e)) => {
                return Err(io::Error::other(format!("stream fetch failed: {e:#}")));
            }
            Some(Ok(())) => {
                return Err(io::Error::other(format!(
                    "stream fetch finished with {cursor} of {} bytes readable",
                    state.store.bound()
                )));
            }
            None => {}
        }
        // Nothing new yet: wait for the drive or for a checkpoint.
        tokio::select! {
            () = progressed => {}
            () = grew => {}
        }
    }
}

impl std::fmt::Debug for LiveReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveReader")
            .field("cursor", &self.state.cursor.load(Ordering::SeqCst))
            .field("bound", &self.state.store.bound())
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
mod tests;
