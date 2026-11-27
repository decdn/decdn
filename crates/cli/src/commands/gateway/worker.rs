//! The gateway's pull worker (#2458): one thread whose runtime holds one
//! [`FetchHost`] for the whole gateway run, so the endpoint, the buyer-pool
//! store and the connections to nodes are built once, and runs every pull as
//! a local task on it (a fetch future is not `Send`).
//!
//! Pulls run at once, up to a bound ([`Bounds`]). The worker admits a bounded
//! number of pulls beyond those running, and manifest pulls up to a cap of
//! their own; it refuses a pull past either bound at once ([`Refused::Busy`]).
//! Each pull streams into its own pipe, so a
//! response whose reader stops holds only its own pull, and only until its
//! pipe write has waited for the stall limit ([`StallLimit`]). The pulls under one
//! capability that overlap share one [`Payer`], and with it each lane's voucher
//! ledger, so they advance the lane's cumulative voucher through one issuer.
//! The payer drops when its last pull ends; every pull persists its lanes'
//! watermarks before it ends, so the next one resumes from them.
//!
//! The worker answers the size a fetch on its host verified
//! for a blob ([`PullWorker::size`]), with no pull slot, so the gateway can
//! answer the request for the blob with a length.
//!
//! The worker also answers what the store holds ([`PullWorker::pools`]): the
//! store is open on the worker's host, and redb refuses a second open of it
//! in the same process. Each pull persists its lanes' watermarks only when it
//! ends, so the store lags the pulls in flight; each capability key's spend
//! in the answer ([`DelegateLanes`]) also counts what the live ledgers of its
//! payers committed.

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use decdn_client::Cumulative;
use decdn_common::admin::{BuyerLaneSnapshot, BuyerPoolsResponse};
use decdn_common::cli::ClientFetchArgs;
use decdn_incentive::Deployment;
use decdn_incentive::buyer_pool::{BuyerPoolStore, lane_snapshot};
use decdn_incentive::lane::{LaneKey, PoolId};
use serde::Serialize;
use tokio::io::{AsyncWrite, DuplexStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;

use super::super::fetch::{BlobFetch, CacheStores, Credential, FetchHost, Payer, Sink, fetch_on};
use super::super::fetch_timings::FetchTimings;

/// One pull the gateway hands the worker.
pub(super) struct Job {
    /// The blob to pull.
    pub(super) blob: BlobFetch,
    /// The capability that pays for it, with the delegate key it was issued
    /// to.
    pub(super) paying: Credential,
    /// Where its verified bytes go. The pull closes it before it reports. A
    /// write the response leaves waiting for the worker's stall limit fails,
    /// and the pull ends with it.
    pub(super) writer: DuplexStream,
    /// The pull's result, sent once `writer` is closed.
    pub(super) done: oneshot::Sender<anyhow::Result<()>>,
    /// What lists the pull as in flight. The worker drops it when the pull
    /// ends, before it sends `done`, so a caller that has the result no
    /// longer counts the pull.
    pub(super) listing: Box<dyn Send>,
    /// What the pull serves, for the slots its kind may hold.
    pub(super) kind: PullKind,
    /// Sent once the pull holds its worker slot, before it writes; dropped
    /// unsent when the pull ends without one.
    pub(super) slotted: oneshot::Sender<()>,
}

/// What a pull serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PullKind {
    /// A blob a response streams, or a size lookup for one.
    Blob,
    /// A bundle manifest the gateway reads, capped at
    /// [`Bounds::manifest_pulls`].
    Manifest,
}

/// How many pulls the worker holds.
#[derive(Clone, Copy, Debug)]
pub(super) struct Bounds {
    /// The pulls that run at once, each on a worker slot.
    pub(super) pulls: usize,
    /// The manifest pulls admitted at once, running or waiting for a slot.
    pub(super) manifest_pulls: usize,
    /// The pulls admitted beyond `pulls`, which wait for a slot.
    pub(super) queued: usize,
}

/// Why the worker refused a pull.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Refused {
    /// The pull's kind is at its cap, or the queue is at its bound.
    Busy,
    /// The worker has stopped.
    Stopped,
}

/// A pull the worker admitted, with the permits it holds until it ends: one
/// of the admitted pulls, and for a manifest one of the manifest pulls.
struct Admitted {
    job: Job,
    permit: OwnedSemaphorePermit,
    kind_permit: Option<OwnedSemaphorePermit>,
}

/// What the worker's buyer-pool store holds.
#[derive(Serialize, Default, Clone, Debug)]
pub(super) struct PoolSnapshot {
    /// The pools the store holds rows for.
    pub(super) pools: BuyerPoolsResponse,
    /// The lanes each held capability's key signs on. A delegate's pool has
    /// no row in the store, so its lanes are only here.
    pub(super) delegate_lanes: Vec<DelegateLanes>,
}

/// The lanes one delegate key signs on in one pool.
#[derive(Serialize, Clone, Debug)]
pub(super) struct DelegateLanes {
    /// The delegate key.
    #[serde(serialize_with = "super::eip55")]
    signer: Address,
    /// The pool the capability spends.
    pool_id: PoolId,
    /// Its lanes, by provider, as the store holds them.
    lanes: Vec<BuyerLaneSnapshot>,
    /// What the key signed for, in micro-USDC, summed over its lanes: each
    /// lane's stored watermark, or its live ledger when that is ahead.
    spent_micro_usdc: u64,
    /// The lane bytes paid for, taken per lane the same way.
    bytes_delivered: u64,
}

impl DelegateLanes {
    /// The lanes `signer` signs on in `pool_id`, as the store holds them,
    /// with their stored spend and bytes summed.
    pub(super) fn new(signer: Address, pool_id: PoolId, lanes: Vec<BuyerLaneSnapshot>) -> Self {
        let mut delegate = Self {
            signer,
            pool_id,
            lanes,
            spent_micro_usdc: 0,
            bytes_delivered: 0,
        };
        delegate.include_live(&HashMap::new());
        delegate
    }

    /// Sum the key's spend and bytes again, with each of its lanes in `live`
    /// at the larger of its stored watermark and its live ledger. A live
    /// ledger starts from the stored watermark and a pull moves it on, so the
    /// larger is the lane's spend and the two are never added. A ledger that
    /// a rebase moved back below the store leaves the stored value. A lane
    /// only `live` knows counts in full.
    fn include_live(&mut self, live: &HashMap<LaneKey, Cumulative>) {
        let mut lanes: HashMap<String, (u64, u64)> = self
            .lanes
            .iter()
            .map(|l| {
                let stored = (l.last_amount_micro_usdc, l.last_bytes_delivered);
                (l.provider.clone(), stored)
            })
            .collect();
        let mine = live
            .iter()
            .filter(|(lane, _)| lane.pool_id == self.pool_id && lane.signer == self.signer);
        for (lane, committed) in mine {
            // The store spells a provider as `lane_snapshot` does.
            let (amount, bytes) = lanes.entry(lane.provider.to_string()).or_default();
            *amount = (*amount).max(narrow(committed.amount));
            *bytes = (*bytes).max(narrow(committed.bytes));
        }
        let (spent, bytes) = lanes.values().fold((0u64, 0u64), |(spent, bytes), (a, b)| {
            (spent.saturating_add(*a), bytes.saturating_add(*b))
        });
        self.spent_micro_usdc = spent;
        self.bytes_delivered = bytes;
    }
}

/// One delegate key's lanes in one pool: the `PaymentPool` contract, the pool
/// on it, and the key.
pub(super) type DelegateKey = (Deployment, PoolId, Address);

/// Narrow a ledger's `U256` to the snapshot's `u64`, saturating.
fn narrow(value: U256) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// What the gateway sends the worker.
enum Request {
    /// A pull to run.
    Pull(Box<Admitted>),
    /// Read the store: its pools, and the lanes of each `(contract, pool,
    /// signer)`.
    Pools {
        delegates: Vec<DelegateKey>,
        reply: oneshot::Sender<anyhow::Result<PoolSnapshot>>,
    },
    /// The size a pull verified for a blob.
    Size {
        hash: [u8; 32],
        reply: oneshot::Sender<Option<u64>>,
    },
}

/// What the worker runs pulls on: [`FetchBackend`], or a stand-in in tests.
pub(super) trait Backend: Send + 'static {
    /// What every pull shares, built once.
    type Host: 'static;
    /// What the pulls under one capability share.
    type Payer: 'static;

    /// Build the host, on the worker's thread.
    async fn open(&self) -> anyhow::Result<Self::Host>;

    /// The payer for the pulls `credential` pays for.
    fn payer(&self, credential: Credential) -> anyhow::Result<Self::Payer>;

    /// Pull `blob` on `host`, paid by `payer`, into `sink`.
    async fn pull(
        &self,
        host: &Self::Host,
        payer: &Self::Payer,
        blob: &BlobFetch,
        sink: &mut Sink,
    ) -> anyhow::Result<()>;

    /// The size a fetch on `host` verified for `hash`, if one did. A backend that keeps no sizes answers `None`.
    fn size(&self, host: &Self::Host, hash: [u8; 32]) -> Option<u64> {
        let _ = (host, hash);
        None
    }

    /// What `host`'s store holds: its pools, and the lanes of each
    /// `(contract, pool, signer)` in `delegates`. A backend without a store
    /// holds nothing.
    async fn pools(
        &self,
        host: &Self::Host,
        delegates: &[DelegateKey],
    ) -> anyhow::Result<PoolSnapshot> {
        let _ = (host, delegates);
        Ok(PoolSnapshot::default())
    }

    /// Each lane `payer`'s pulls share a live ledger on, with what it has
    /// committed so far. A backend without ledgers has none.
    fn live_lanes(&self, payer: &Self::Payer) -> Vec<(LaneKey, Cumulative)> {
        let _ = payer;
        Vec::new()
    }

    /// Shut the host down once every pull has ended.
    async fn close(&self, host: Self::Host);
}

/// The real backend: `decdn get`'s blob fetch, paid with held keys.
pub(super) struct FetchBackend {
    /// The retrieval flags the host resolves.
    pub(super) common: ClientFetchArgs,
    /// The global `--config`.
    pub(super) config_path: Option<PathBuf>,
    /// The gateway cache every pull streams into, or `None` for a scratch
    /// directory per pull.
    pub(super) cache: Option<CacheStores>,
}

impl Backend for FetchBackend {
    type Host = FetchHost;
    type Payer = Payer;

    async fn open(&self) -> anyhow::Result<Self::Host> {
        let (host, _) = FetchHost::open(&self.common, self.config_path.as_deref(), true).await?;
        Ok(host.with_stream_cache(self.cache.clone()))
    }

    fn payer(&self, credential: Credential) -> anyhow::Result<Payer> {
        Payer::held(credential)
    }

    async fn pull(
        &self,
        host: &Self::Host,
        payer: &Payer,
        blob: &BlobFetch,
        sink: &mut Sink,
    ) -> anyhow::Result<()> {
        let timings = FetchTimings::start();
        let result = fetch_on(host, payer, blob, sink, &timings).await;
        timings.log("writer", result.is_ok());
        result
    }

    fn size(&self, host: &Self::Host, hash: [u8; 32]) -> Option<u64> {
        host.verified_size(&hash)
    }

    /// Read off the async thread, so a slow store read never stalls the
    /// pulls running on it.
    async fn pools(
        &self,
        host: &Self::Host,
        delegates: &[DelegateKey],
    ) -> anyhow::Result<PoolSnapshot> {
        let store = host.store();
        let queries = delegates.to_vec();
        tokio::task::spawn_blocking(move || read_pools(store.as_ref(), &queries)).await?
    }

    fn live_lanes(&self, payer: &Payer) -> Vec<(LaneKey, Cumulative)> {
        payer.committed_lanes()
    }

    async fn close(&self, host: Self::Host) {
        host.close().await;
    }
}

/// What `store` holds: its pool rows on every contract, and the delegate
/// lanes of each `(contract, pool, signer)` in `queries`.
///
/// # Errors
///
/// The store cannot be read, or a record does not decode.
pub(super) fn read_pools(
    store: &dyn BuyerPoolStore,
    queries: &[DelegateKey],
) -> anyhow::Result<PoolSnapshot> {
    let pools = BuyerPoolsResponse::from(store.load_all()?);
    let delegate_lanes = queries
        .iter()
        .map(|&(deployment, pool_id, signer)| {
            let lanes = store
                .delegate_lanes(deployment, pool_id, signer)?
                .into_iter()
                .map(|(provider, progress)| {
                    let lane = LaneKey {
                        pool_id,
                        signer,
                        provider,
                    };
                    lane_snapshot(lane, progress)
                })
                .collect();
            Ok(DelegateLanes::new(signer, pool_id, lanes))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(PoolSnapshot {
        pools,
        delegate_lanes,
    })
}

/// The handle the gateway holds on its worker thread.
pub(super) struct PullWorker {
    /// The requests to the worker. The worker takes each at once and runs a
    /// pull as a task that waits for its slot, so the channel holds nothing
    /// for long: `admitted` bounds the pulls waiting.
    jobs: mpsc::UnboundedSender<Request>,
    /// The pulls admitted and not yet ended, running or waiting: one permit
    /// each, out of [`Bounds::pulls`] plus [`Bounds::queued`].
    admitted: Arc<Semaphore>,
    /// The manifest pulls admitted and not yet ended, out of
    /// [`Bounds::manifest_pulls`].
    manifests: Arc<Semaphore>,
    stop: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    thread: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl PullWorker {
    /// Start the worker thread, build `backend`'s host on it, and run up to
    /// `bounds.pulls` pulls at once. A pull whose write to its response waits
    /// for `stall` ends ([`StallLimit`]).
    ///
    /// # Errors
    ///
    /// The thread or its runtime cannot start, or the host cannot be built.
    pub(super) async fn start<B: Backend>(
        backend: B,
        bounds: Bounds,
        stall: Duration,
    ) -> anyhow::Result<Self> {
        let max_pulls = bounds.pulls.max(1);
        let admitted = Arc::new(Semaphore::new(max_pulls.saturating_add(bounds.queued)));
        let manifests = Arc::new(Semaphore::new(bounds.manifest_pulls));
        let (jobs_tx, jobs_rx) = mpsc::unbounded_channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("decdn-gateway-pulls".to_owned())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e.into()));
                        return;
                    }
                };
                let local = tokio::task::LocalSet::new();
                local.block_on(
                    &runtime,
                    run(backend, max_pulls, stall, jobs_rx, stop_rx, ready_tx),
                );
            })?;
        ready_rx
            .await
            .map_err(|_| anyhow::anyhow!("the pull worker ended before it started"))??;
        Ok(Self {
            jobs: jobs_tx,
            admitted,
            manifests,
            stop: std::sync::Mutex::new(Some(stop_tx)),
            thread: std::sync::Mutex::new(Some(thread)),
        })
    }

    /// Hand `job` to the worker, or refuse it at once: a manifest pull past
    /// [`Bounds::manifest_pulls`], or any pull past the running and queued
    /// bound, never waits.
    ///
    /// # Errors
    ///
    /// [`Refused::Busy`] past a bound, [`Refused::Stopped`] once the worker
    /// has stopped.
    pub(super) fn pull(&self, job: Job) -> Result<(), Refused> {
        let kind = match job.kind {
            PullKind::Manifest => Some(
                Arc::clone(&self.manifests)
                    .try_acquire_owned()
                    .map_err(|_| Refused::Busy)?,
            ),
            PullKind::Blob => None,
        };
        let admitted = Arc::clone(&self.admitted)
            .try_acquire_owned()
            .map_err(|_| Refused::Busy)?;
        self.jobs
            .send(Request::Pull(Box::new(Admitted {
                job,
                permit: admitted,
                kind_permit: kind,
            })))
            .map_err(|_| Refused::Stopped)
    }

    /// What the store holds: its pools, and the lanes of each
    /// `(contract, pool, signer)` in `delegates`. Answered beside the pulls
    /// in flight.
    ///
    /// # Errors
    ///
    /// The worker has stopped, or the store cannot be read.
    pub(super) async fn pools(&self, delegates: Vec<DelegateKey>) -> anyhow::Result<PoolSnapshot> {
        let (reply, answer) = oneshot::channel();
        self.jobs
            .send(Request::Pools { delegates, reply })
            .map_err(|_| anyhow::anyhow!("the gateway is shutting down"))?;
        answer
            .await
            .map_err(|_| anyhow::anyhow!("the gateway is shutting down"))?
    }

    /// The size a fetch on the worker's host verified for `hash`
    /// ([`Backend::size`]). Answered beside the pulls in flight, with no pull
    /// slot. `None` when none verified it, and once the worker has stopped.
    pub(super) async fn size(&self, hash: [u8; 32]) -> Option<u64> {
        let (reply, answer) = oneshot::channel();
        self.jobs.send(Request::Size { hash, reply }).ok()?;
        answer.await.ok().flatten()
    }

    /// Stop the worker: drop every pull in flight, which queues what each
    /// paid for recording, wait for the queued writes, and close the host.
    pub(super) async fn stop(&self) {
        let stop = self
            .stop
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(stop) = stop {
            let _ = stop.send(());
        }
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(thread) = thread {
            let _ = tokio::task::spawn_blocking(move || thread.join()).await;
        }
    }
}

/// Whose payer a pull runs under: a held capability, by the delegate key it
/// was issued to, the pool it spends, and that pool's `PaymentPool`
/// contract. One key holds at most one capability per pool.
type PayerId = (Address, Deployment, PoolId);

/// The capability `paying` names.
fn payer_id(paying: &Credential) -> PayerId {
    (paying.signer.address(), paying.pool, paying.grant.pool_id)
}

/// The payers of the pulls in flight, by the capability that pays.
type Payers<P> = Rc<RefCell<HashMap<PayerId, Rc<P>>>>;

/// The worker thread's body: build the host, then run each job as a local
/// task until told to stop.
async fn run<B: Backend>(
    backend: B,
    max_pulls: usize,
    stall: Duration,
    mut jobs: mpsc::UnboundedReceiver<Request>,
    mut stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<anyhow::Result<()>>,
) {
    let host = match backend.open().await {
        Ok(host) => host,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    let payers: Payers<B::Payer> = Rc::default();
    let host = Rc::new(host);
    let _ = ready.send(Ok(()));
    let backend = Rc::new(backend);
    let slots = Rc::new(Semaphore::new(max_pulls));
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            request = jobs.recv() => match request {
                Some(Request::Pull(admitted)) => {
                    tasks.spawn_local(pull(
                        Rc::clone(&backend),
                        Rc::clone(&host),
                        Rc::clone(&slots),
                        Rc::clone(&payers),
                        stall,
                        *admitted,
                    ));
                }
                Some(Request::Pools { delegates, reply }) => {
                    let backend = Rc::clone(&backend);
                    let host = Rc::clone(&host);
                    let payers = Rc::clone(&payers);
                    tasks.spawn_local(async move {
                        // The ledgers are read before the store: a pull that
                        // ends during the store read is already counted live,
                        // and one that ended before it persisted first.
                        let live = live_lanes(backend.as_ref(), &payers);
                        let snapshot = backend.pools(&host, &delegates).await.map(|mut s| {
                            for delegate in &mut s.delegate_lanes {
                                delegate.include_live(&live);
                            }
                            s
                        });
                        let _ = reply.send(snapshot);
                    });
                }
                Some(Request::Size { hash, reply }) => {
                    let _ = reply.send(backend.size(&host, hash));
                }
                None => break,
            },
            _ = &mut stop => break,
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }
    tasks.shutdown().await;
    payers.borrow_mut().clear();
    if let Ok(host) = Rc::try_unwrap(host) {
        backend.close(host).await;
    } else {
        tracing::warn!("a gateway pull outlived the worker; its host is not closed");
    }
}

/// One job, holding a pull slot while it runs. Its writer fails a write the
/// response leaves waiting for `stall`, so a response nobody reads frees the
/// slot. Its admission permits drop with its listing, before `done`.
async fn pull<B: Backend>(
    backend: Rc<B>,
    host: Rc<B::Host>,
    slots: Rc<Semaphore>,
    payers: Payers<B::Payer>,
    stall: Duration,
    admitted: Admitted,
) {
    let Admitted {
        job:
            Job {
                blob,
                paying,
                writer,
                done,
                listing,
                kind: _,
                slotted,
            },
        permit,
        kind_permit,
    } = admitted;
    let admission = (permit, kind_permit, listing);
    let Ok(_slot) = slots.acquire().await else {
        return;
    };
    let _ = slotted.send(());
    let id = payer_id(&paying);
    let payer = match payer_for(&*backend, &payers, id, paying) {
        Ok(payer) => payer,
        Err(e) => {
            drop(admission);
            let _ = done.send(Err(e));
            return;
        }
    };
    let mut sink = Sink::Writer(Box::new(StallLimit::new(writer, stall, blob.hash)));
    let result = backend.pull(&host, &payer, &blob, &mut sink).await;
    // The gateway reads the pipe to its end before it waits for the result.
    drop(sink);
    release(&payers, id, &payer);
    drop(payer);
    drop(admission);
    let _ = done.send(result);
}

/// A writer that fails a write, flush or shutdown its inner writer leaves
/// pending for `limit`.
///
/// A pull parks while its response does not read: the pipe fills, the
/// read-ahead fills, and the drive waits for the consumer. That wait holds the
/// fetch's progress clock, so the no-progress limit never fires and the pull
/// keeps its slot for as long as the response stays unread. This bound ends
/// such a pull. It counts only the time a write waits on the response; the
/// time between writes, while the holders are slow, is the progress clock's
/// to judge. Each write the response lets through restarts the limit, so a
/// response keeps its pull while it reads each frame (up to
/// [`CHUNK_BYTES`](super::CHUNK_BYTES)) within the limit: a response that
/// reads slower than one frame per limit loses it.
struct StallLimit<W> {
    inner: W,
    limit: Duration,
    /// The blob the pull writes, for the log line when the limit fires.
    hash: [u8; 32],
    /// Armed when an operation first returns `Pending`; cleared when one
    /// completes or the limit fires.
    stalled: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<W> StallLimit<W> {
    const fn new(inner: W, limit: Duration, hash: [u8; 32]) -> Self {
        Self {
            inner,
            limit,
            hash,
            stalled: None,
        }
    }

    /// `poll` as it is, unless it has stayed `Pending` for the limit.
    fn limit_pending<T>(
        &mut self,
        cx: &mut Context<'_>,
        poll: Poll<std::io::Result<T>>,
    ) -> Poll<std::io::Result<T>> {
        if poll.is_ready() {
            self.stalled = None;
            return poll;
        }
        let limit = self.limit;
        let stalled = self
            .stalled
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(limit)));
        if stalled.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        self.stalled = None;
        tracing::info!(
            hash = %B256::from(self.hash),
            limit_secs = limit.as_secs_f64(),
            "a gateway response read nothing while its pull waited; the pull ends"
        );
        Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("the response read nothing for {limit:?}"),
        )))
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for StallLimit<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.limit_pending(cx, poll)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_flush(cx);
        this.limit_pending(cx, poll)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.limit_pending(cx, poll)
    }
}

/// The payer of the pulls in flight under `id`, or a new one for `paying`.
///
/// # Errors
///
/// The credential's payer cannot be built.
fn payer_for<B: Backend>(
    backend: &B,
    payers: &Payers<B::Payer>,
    id: PayerId,
    paying: Credential,
) -> anyhow::Result<Rc<B::Payer>> {
    let mut payers = payers.borrow_mut();
    if let Some(payer) = payers.get(&id) {
        return Ok(Rc::clone(payer));
    }
    let payer = Rc::new(backend.payer(paying)?);
    payers.insert(id, Rc::clone(&payer));
    Ok(payer)
}

/// Each lane a live payer holds a ledger on, with the most any payer
/// committed on it. A payer lives while its pulls run.
fn live_lanes<B: Backend>(backend: &B, payers: &Payers<B::Payer>) -> HashMap<LaneKey, Cumulative> {
    let mut live = HashMap::new();
    for payer in payers.borrow().values() {
        for (lane, committed) in backend.live_lanes(payer) {
            match live.entry(lane) {
                Entry::Vacant(e) => {
                    e.insert(committed);
                }
                Entry::Occupied(mut e) => {
                    let seen: &mut Cumulative = e.get_mut();
                    seen.amount = seen.amount.max(committed.amount);
                    seen.bytes = seen.bytes.max(committed.bytes);
                }
            }
        }
    }
    live
}

/// Drop `payer` from the map when no pull but the caller's holds it.
fn release<P>(payers: &Payers<P>, id: PayerId, payer: &Rc<P>) {
    let mut payers = payers.borrow_mut();
    // The map's copy and the caller's are the last two.
    if Rc::strong_count(payer) <= 2 {
        payers.remove(&id);
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
