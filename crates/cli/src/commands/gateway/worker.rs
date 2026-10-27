//! The gateway's pull worker (#2458): one thread whose runtime holds one
//! [`FetchHost`] for the whole gateway run, so the endpoint, the buyer-pool
//! store and the connections to nodes are built once, and runs every pull as
//! a local task on it (a fetch future is not `Send`).
//!
//! Pulls run at once, up to a bound. Each one streams into its own pipe, so a
//! response whose reader stops holds only its own pull. The pulls under one
//! capability that overlap share one [`Payer`], and with it each lane's voucher
//! ledger, so they advance the lane's cumulative voucher through one issuer.
//! The payer drops when its last pull ends; every pull persists its lanes'
//! watermarks before it ends, so the next one resumes from them. A gateway
//! that pays from its own pool ([`Payment::Pool`]) builds that payer once,
//! with the host, and every pool pull shares it for the run; a pull that ends
//! with a pool replacement retires it, so the next pull's ledgers know only
//! the new pool.
//!
//! The worker answers the size a pull or a size proof on its host verified
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
use std::rc::Rc;

use alloy::primitives::{Address, U256};
use decdn_client::Cumulative;
use decdn_common::admin::{BuyerLaneSnapshot, BuyerPoolsResponse};
use decdn_common::cli::ClientFetchArgs;
use decdn_incentive::CapabilityGrant;
use decdn_incentive::Deployment;
use decdn_incentive::buyer_pool::{BuyerPoolStore, lane_snapshot};
use decdn_incentive::lane::{LaneKey, PoolId};
use serde::Serialize;
use tokio::io::DuplexStream;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;

use super::super::fetch::{BlobFetch, Credential, FetchHost, Payer, Sink, fetch_on};
use super::super::fetch_timings::FetchTimings;

/// Who pays for a pull.
#[derive(Clone)]
pub(super) enum Payment {
    /// A capability held in memory, with the delegate key it was issued to.
    Held(Credential),
    /// The gateway's own pool, from its keystore.
    Pool {
        /// The namespace whose pointer the request is served under, so its
        /// bytes count against that namespace's allowance; `None` for every
        /// other request.
        namespace: Option<u64>,
    },
}

impl Payment {
    /// The allowance bucket the pull spends: a pointer's namespace, or the
    /// shared one.
    pub(super) const fn bucket(&self) -> Option<u64> {
        match self {
            Self::Held(_) => None,
            Self::Pool { namespace } => *namespace,
        }
    }
}

/// One pull the gateway hands the worker.
pub(super) struct Job {
    /// The blob to pull.
    pub(super) blob: BlobFetch,
    /// Who pays for it.
    pub(super) paying: Payment,
    /// Where its verified bytes go. The pull closes it before it reports.
    pub(super) writer: DuplexStream,
    /// The pull's result, sent once `writer` is closed.
    pub(super) done: oneshot::Sender<anyhow::Result<()>>,
    /// Whether to start the pull, asked once it has its slot: what changed
    /// while it waited (an allowance the pulls before it spent) can still
    /// refuse it. `false` ends the job with [`NotAdmitted`] and no paid work.
    pub(super) admit: Option<Box<dyn FnOnce() -> bool + Send>>,
}

/// What the worker's buyer-pool store holds.
#[derive(Serialize, Default, Clone, Debug)]
pub(super) struct PoolSnapshot {
    /// The pools the store holds rows for: the gateway's own pool on a
    /// public gateway.
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

/// Narrow a ledger's `U256` to the snapshot's `u64`, saturating.
fn narrow(value: U256) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// What the gateway sends the worker.
enum Request {
    /// A pull to run.
    Pull(Box<Job>),
    /// Read the store: its pools, and the lanes of each `(pool, signer)`.
    Pools {
        delegates: Vec<(PoolId, Address)>,
        reply: oneshot::Sender<anyhow::Result<PoolSnapshot>>,
    },
    /// The size a pull verified for a blob.
    Size {
        hash: [u8; 32],
        reply: oneshot::Sender<Option<u64>>,
    },
}

/// A job its [`Job::admit`] check refused at its slot.
#[derive(Debug)]
pub(super) struct NotAdmitted;

impl std::fmt::Display for NotAdmitted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the pull was not admitted once it had its slot")
    }
}

impl std::error::Error for NotAdmitted {}

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

    /// The payer every [`Payment::Pool`] pull shares, built on `host` once
    /// the host is, or `None` when the gateway pays with held credentials
    /// only.
    async fn pool_payer(&self, host: &Self::Host) -> anyhow::Result<Option<Self::Payer>> {
        let _ = host;
        Ok(None)
    }

    /// Pull `blob` on `host`, paid by `payer`, into `sink`.
    async fn pull(
        &self,
        host: &Self::Host,
        payer: &Self::Payer,
        blob: &BlobFetch,
        sink: &mut Sink,
    ) -> anyhow::Result<()>;

    /// The size a whole pull or a size proof on `host` verified for `hash`,
    /// if one did. A backend that keeps no sizes answers `None`.
    fn size(&self, host: &Self::Host, hash: [u8; 32]) -> Option<u64> {
        let _ = (host, hash);
        None
    }

    /// What `host`'s store holds: its pools, and the lanes of each
    /// `(pool, signer)` in `delegates`. A backend without a store holds
    /// nothing.
    async fn pools(
        &self,
        host: &Self::Host,
        delegates: &[(PoolId, Address)],
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

/// The real backend: `decdn get`'s blob fetch, paid with held keys or from
/// the keystore's own pool.
pub(super) struct FetchBackend {
    /// The retrieval flags the host resolves.
    pub(super) common: ClientFetchArgs,
    /// The global `--config`.
    pub(super) config_path: Option<PathBuf>,
    /// Whether the gateway pays from its keystore's own pool: the host then
    /// reads `--keystore` and funds the pool, and every [`Payment::Pool`] pull
    /// shares one payer. Otherwise every pull pays with a held key.
    pub(super) pool: bool,
}

impl Backend for FetchBackend {
    /// The host, and the `--capability` delegation the flags name, if any.
    type Host = (FetchHost, Option<CapabilityGrant>);
    type Payer = Payer;

    async fn open(&self) -> anyhow::Result<Self::Host> {
        FetchHost::open(&self.common, self.config_path.as_deref(), !self.pool).await
    }

    fn payer(&self, credential: Credential) -> anyhow::Result<Payer> {
        Payer::held(credential)
    }

    async fn pool_payer(&self, (host, grant): &Self::Host) -> anyhow::Result<Option<Payer>> {
        if !self.pool {
            return Ok(None);
        }
        let payer = Payer::pool(grant.clone(), host.escrows.clone());
        payer.unlock(host).await?;
        Ok(Some(payer))
    }

    async fn pull(
        &self,
        (host, _): &Self::Host,
        payer: &Payer,
        blob: &BlobFetch,
        sink: &mut Sink,
    ) -> anyhow::Result<()> {
        let timings = FetchTimings::start();
        let result = fetch_on(host, payer, blob, sink, &timings).await;
        timings.log("writer", result.is_ok());
        result
    }

    fn size(&self, (host, _): &Self::Host, hash: [u8; 32]) -> Option<u64> {
        host.verified_size(&hash)
    }

    /// Read off the async thread, so a slow store read never stalls the
    /// pulls running on it. The `--capability` grant a public gateway pays
    /// under signs delegate lanes too, so its key is read beside the
    /// registered capabilities'.
    async fn pools(
        &self,
        (host, grant): &Self::Host,
        delegates: &[(PoolId, Address)],
    ) -> anyhow::Result<PoolSnapshot> {
        let store = host.store();
        let deployment = host.deployment();
        let queries = lane_queries(grant.as_ref(), delegates);
        tokio::task::spawn_blocking(move || read_pools(store.as_ref(), deployment, &queries))
            .await?
    }

    fn live_lanes(&self, payer: &Payer) -> Vec<(LaneKey, Cumulative)> {
        payer.committed_lanes()
    }

    async fn close(&self, (host, _): Self::Host) {
        host.close().await;
    }
}

/// The `(pool, signer)` keys whose delegate lanes a store read lists: each
/// registered capability's, and the `--capability` grant's when the gateway
/// pays under one, once.
pub(super) fn lane_queries(
    grant: Option<&CapabilityGrant>,
    delegates: &[(PoolId, Address)],
) -> Vec<(PoolId, Address)> {
    let mut queries = delegates.to_vec();
    if let Some(grant) = grant
        && !queries.contains(&(grant.pool_id, grant.signer))
    {
        queries.push((grant.pool_id, grant.signer));
    }
    queries
}

/// What `store` holds on `deployment`: its pool rows, and the delegate lanes
/// of each `(pool, signer)` in `queries`.
///
/// # Errors
///
/// The store cannot be read, or a record does not decode.
pub(super) fn read_pools(
    store: &dyn BuyerPoolStore,
    deployment: Deployment,
    queries: &[(PoolId, Address)],
) -> anyhow::Result<PoolSnapshot> {
    let pools = BuyerPoolsResponse::from(store.load_all()?);
    let delegate_lanes = queries
        .iter()
        .map(|&(pool_id, signer)| {
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
    jobs: mpsc::UnboundedSender<Request>,
    stop: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    thread: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl PullWorker {
    /// Start the worker thread, build `backend`'s host on it, and run up to
    /// `max_pulls` pulls at once.
    ///
    /// # Errors
    ///
    /// The thread or its runtime cannot start, or the host cannot be built.
    pub(super) async fn start<B: Backend>(backend: B, max_pulls: usize) -> anyhow::Result<Self> {
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
                    run(backend, max_pulls.max(1), jobs_rx, stop_rx, ready_tx),
                );
            })?;
        ready_rx
            .await
            .map_err(|_| anyhow::anyhow!("the pull worker ended before it started"))??;
        Ok(Self {
            jobs: jobs_tx,
            stop: std::sync::Mutex::new(Some(stop_tx)),
            thread: std::sync::Mutex::new(Some(thread)),
        })
    }

    /// Hand `job` to the worker.
    ///
    /// # Errors
    ///
    /// The worker has stopped.
    pub(super) fn pull(&self, job: Job) -> anyhow::Result<()> {
        self.jobs
            .send(Request::Pull(Box::new(job)))
            .map_err(|_| anyhow::anyhow!("the gateway is shutting down"))
    }

    /// What the store holds: its pools, and the lanes of each
    /// `(pool, signer)` in `delegates`. Answered beside the pulls in flight.
    ///
    /// # Errors
    ///
    /// The worker has stopped, or the store cannot be read.
    pub(super) async fn pools(
        &self,
        delegates: Vec<(PoolId, Address)>,
    ) -> anyhow::Result<PoolSnapshot> {
        let (reply, answer) = oneshot::channel();
        self.jobs
            .send(Request::Pools { delegates, reply })
            .map_err(|_| anyhow::anyhow!("the gateway is shutting down"))?;
        answer
            .await
            .map_err(|_| anyhow::anyhow!("the gateway is shutting down"))?
    }

    /// The size a whole pull or a size proof on the worker's host verified
    /// for `hash` ([`Backend::size`]). Answered beside the pulls in flight,
    /// with no pull slot. `None` when neither verified it, and once the
    /// worker has stopped.
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

/// Whose payer a pull runs under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PayerId {
    /// The delegate key a held credential was issued to.
    Signer(Address),
    /// The gateway's own pool.
    Pool,
}

impl PayerId {
    fn of(paying: &Payment) -> Self {
        match paying {
            Payment::Held(credential) => Self::Signer(credential.signer.address()),
            Payment::Pool { .. } => Self::Pool,
        }
    }
}

/// The payers of the pulls in flight, by who pays; the pool payer, when there
/// is one, for the whole run.
type Payers<P> = Rc<RefCell<HashMap<PayerId, Rc<P>>>>;

/// The worker thread's body: build the host, then run each job as a local
/// task until told to stop.
async fn run<B: Backend>(
    backend: B,
    max_pulls: usize,
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
    match backend.pool_payer(&host).await {
        Ok(Some(payer)) => {
            payers.borrow_mut().insert(PayerId::Pool, Rc::new(payer));
        }
        Ok(None) => {}
        Err(e) => {
            backend.close(host).await;
            let _ = ready.send(Err(e));
            return;
        }
    }
    let host = Rc::new(host);
    let _ = ready.send(Ok(()));
    let backend = Rc::new(backend);
    let slots = Rc::new(Semaphore::new(max_pulls));
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            request = jobs.recv() => match request {
                Some(Request::Pull(job)) => {
                    tasks.spawn_local(pull(
                        Rc::clone(&backend),
                        Rc::clone(&host),
                        Rc::clone(&slots),
                        Rc::clone(&payers),
                        *job,
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

/// One job, holding a pull slot while it runs.
async fn pull<B: Backend>(
    backend: Rc<B>,
    host: Rc<B::Host>,
    slots: Rc<Semaphore>,
    payers: Payers<B::Payer>,
    job: Job,
) {
    let Job {
        blob,
        paying,
        writer,
        done,
        admit,
    } = job;
    let Ok(_slot) = slots.acquire().await else {
        return;
    };
    if let Some(admit) = admit
        && !admit()
    {
        let _ = done.send(Err(anyhow::Error::new(NotAdmitted)));
        return;
    }
    let id = PayerId::of(&paying);
    let payer = match payer_for(&*backend, &payers, id, paying) {
        Ok(payer) => payer,
        Err(e) => {
            let _ = done.send(Err(e));
            return;
        }
    };
    let mut sink = Sink::Writer(Box::new(writer));
    let result = backend.pull(&host, &payer, &blob, &mut sink).await;
    // The gateway reads the pipe to its end before it waits for the result.
    drop(sink);
    match id {
        PayerId::Pool => {
            if let Some(replaced) = result
                .as_ref()
                .err()
                .and_then(|e| e.downcast_ref::<decdn_client::PoolReplaced>())
            {
                rebuild_pool_payer(&*backend, &host, &payers, &payer, *replaced).await;
            }
        }
        PayerId::Signer(_) => release(&payers, id, &payer),
    }
    drop(payer);
    let _ = done.send(result);
}

/// The payer of the pulls in flight under `id`, or a new one for a held
/// credential.
///
/// # Errors
///
/// A pool pull on a gateway with no pool payer, or a credential whose payer
/// cannot be built.
fn payer_for<B: Backend>(
    backend: &B,
    payers: &Payers<B::Payer>,
    id: PayerId,
    paying: Payment,
) -> anyhow::Result<Rc<B::Payer>> {
    let mut payers = payers.borrow_mut();
    if let Some(payer) = payers.get(&id) {
        return Ok(Rc::clone(payer));
    }
    let Payment::Held(credential) = paying else {
        anyhow::bail!("this gateway has no pool of its own to pay from");
    };
    let payer = Rc::new(backend.payer(credential)?);
    payers.insert(id, Rc::clone(&payer));
    Ok(payer)
}

/// Each lane a live payer holds a ledger on, with the most any payer
/// committed on it. A held payer lives while its pulls run; the pool payer
/// lives for the run.
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

/// Retire the pool payer `retired` after a pull under it ended with
/// `replaced`, and build the next pool pull's payer: the retired one's lane
/// ledgers count the closed pool's lanes, which would understate the new
/// pool's headroom. Pulls still running under the retired payer keep it. A
/// pull that finds another already retired it changes nothing.
async fn rebuild_pool_payer<B: Backend>(
    backend: &B,
    host: &B::Host,
    payers: &Payers<B::Payer>,
    retired: &Rc<B::Payer>,
    replaced: decdn_client::PoolReplaced,
) {
    {
        let mut payers = payers.borrow_mut();
        let current = payers
            .get(&PayerId::Pool)
            .is_some_and(|current| Rc::ptr_eq(current, retired));
        if !current {
            return;
        }
        payers.remove(&PayerId::Pool);
    }
    tracing::warn!(
        closed = %replaced.closed,
        opened = %replaced.opened,
        "the gateway's pool was replaced; once its dispute window ends, reclaim the closed \
         pool with `decdn pool reclaim --pool {}`",
        replaced.closed
    );
    match backend.pool_payer(host).await {
        Ok(Some(payer)) => {
            payers.borrow_mut().insert(PayerId::Pool, Rc::new(payer));
        }
        Ok(None) => {}
        Err(e) => tracing::error!(error = %e, "the gateway cannot rebuild its pool payer"),
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
