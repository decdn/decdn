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
//! watermarks before it ends, so the next one resumes from them.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;

use alloy::primitives::Address;
use decdn_common::cli::ClientFetchArgs;
use tokio::io::DuplexStream;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;

use super::super::fetch::{BlobFetch, Credential, FetchHost, Payer, Sink, fetch_on};
use super::super::fetch_timings::FetchTimings;

/// One pull the gateway hands the worker.
pub(super) struct Job {
    /// The blob to pull.
    pub(super) blob: BlobFetch,
    /// The capability that pays for it.
    pub(super) paying: Credential,
    /// Where its verified bytes go. The pull closes it before it reports.
    pub(super) writer: DuplexStream,
    /// The pull's result, sent once `writer` is closed.
    pub(super) done: oneshot::Sender<anyhow::Result<()>>,
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

    /// Shut the host down once every pull has ended.
    async fn close(&self, host: Self::Host);
}

/// The real backend: `decdn get`'s blob fetch, paid with held keys.
pub(super) struct FetchBackend {
    /// The retrieval flags the host resolves.
    pub(super) common: ClientFetchArgs,
    /// The global `--config`.
    pub(super) config_path: Option<PathBuf>,
}

impl Backend for FetchBackend {
    type Host = FetchHost;
    type Payer = Payer;

    async fn open(&self) -> anyhow::Result<FetchHost> {
        let (host, _no_grant) =
            FetchHost::open(&self.common, self.config_path.as_deref(), true).await?;
        Ok(host)
    }

    fn payer(&self, credential: Credential) -> anyhow::Result<Payer> {
        Payer::held(credential)
    }

    async fn pull(
        &self,
        host: &FetchHost,
        payer: &Payer,
        blob: &BlobFetch,
        sink: &mut Sink,
    ) -> anyhow::Result<()> {
        let timings = FetchTimings::start();
        let result = fetch_on(host, payer, blob, sink, &timings).await;
        timings.log("writer", result.is_ok());
        result
    }

    async fn close(&self, host: FetchHost) {
        host.close().await;
    }
}

/// The handle the gateway holds on its worker thread.
pub(super) struct PullWorker {
    jobs: mpsc::UnboundedSender<Job>,
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
            .send(job)
            .map_err(|_| anyhow::anyhow!("the gateway is shutting down"))
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

/// The payers of the pulls in flight, by the signer that pays.
type Payers<P> = Rc<RefCell<HashMap<Address, Rc<P>>>>;

/// The worker thread's body: build the host, then run each job as a local
/// task until told to stop.
async fn run<B: Backend>(
    backend: B,
    max_pulls: usize,
    mut jobs: mpsc::UnboundedReceiver<Job>,
    mut stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<anyhow::Result<()>>,
) {
    let host = match backend.open().await {
        Ok(host) => Rc::new(host),
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    let _ = ready.send(Ok(()));
    let backend = Rc::new(backend);
    let slots = Rc::new(Semaphore::new(max_pulls));
    let payers: Payers<B::Payer> = Rc::default();
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            job = jobs.recv() => match job {
                Some(job) => {
                    tasks.spawn_local(pull(
                        Rc::clone(&backend),
                        Rc::clone(&host),
                        Rc::clone(&slots),
                        Rc::clone(&payers),
                        job,
                    ));
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
    } = job;
    let Ok(_slot) = slots.acquire().await else {
        return;
    };
    let signer = paying.signer.address();
    let payer = match payer_for(&*backend, &payers, signer, paying) {
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
    release(&payers, signer, &payer);
    drop(payer);
    let _ = done.send(result);
}

/// The payer of the pulls in flight under `signer`, or a new one for
/// `paying`.
fn payer_for<B: Backend>(
    backend: &B,
    payers: &Payers<B::Payer>,
    signer: Address,
    paying: Credential,
) -> anyhow::Result<Rc<B::Payer>> {
    let mut payers = payers.borrow_mut();
    if let Some(payer) = payers.get(&signer) {
        return Ok(Rc::clone(payer));
    }
    let payer = Rc::new(backend.payer(paying)?);
    payers.insert(signer, Rc::clone(&payer));
    Ok(payer)
}

/// Drop `payer` from the map when no pull but the caller's holds it.
fn release<P>(payers: &Payers<P>, signer: Address, payer: &Rc<P>) {
    let mut payers = payers.borrow_mut();
    // The map's copy and the caller's are the last two.
    if Rc::strong_count(payer) <= 2 {
        payers.remove(&signer);
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
