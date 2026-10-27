use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use std::collections::HashMap;

use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use clap::Parser as _;
use decdn_client::Cumulative;
use decdn_common::cli::ClientFetchArgs;
use decdn_incentive::CapabilityGrant;
use decdn_incentive::buyer_pool::{BuyerLaneProgress, lane_snapshot};
use decdn_incentive::lane::{LaneKey, PoolId};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{Notify, oneshot};

use super::{
    Backend, DelegateLanes, Job, Payment, PoolSnapshot, PullWorker, lane_queries, read_pools,
};
use crate::commands::fetch::{BlobFetch, Credential, Sink};

/// The blob a [`Fake`] pull stalls on: it writes more than the pipe holds.
const STALL: u8 = 0;
/// The blob a [`Fake`] pull writes `hello` for.
const HELLO: u8 = 1;
/// The blob a [`Fake`] pull holds open until the test releases it.
const HOLD: u8 = 2;
/// The blob a [`Fake`] pull ends with a pool replacement for.
const REPLACED: u8 = 3;

/// What a [`Fake`] backend did.
#[derive(Default)]
struct Log {
    opened: usize,
    closed: usize,
    payers_made: usize,
    /// The payer id each pull ran under, in the order the pulls ran.
    pulled_under: Vec<usize>,
    /// The key each payer signs its lanes with, by payer id.
    signers: HashMap<usize, Address>,
    /// Whether a store read releases the held pulls and lets them end
    /// before it answers.
    end_pulls_in_store_read: bool,
}

/// The key a [`Fake`] pool payer signs its lanes with, as a `--capability`
/// grant's.
const POOL_SIGNER: Address = Address::repeat_byte(0x77);

/// The provider of the one lane each [`Fake`] payer has a ledger on.
const LIVE_PROVIDER: Address = Address::repeat_byte(9);

/// What that ledger has committed.
const LIVE: Cumulative = Cumulative {
    bytes: U256::from_limbs([4096, 0, 0, 0]),
    amount: U256::from_limbs([500, 0, 0, 0]),
};

/// A backend whose pulls act by the blob's first hash byte.
struct Fake {
    log: Arc<Mutex<Log>>,
    release: Arc<Notify>,
    /// Whether the backend has a pool of its own to pay from.
    pool: bool,
}

impl Backend for Fake {
    type Host = ();
    type Payer = usize;

    async fn open(&self) -> anyhow::Result<()> {
        self.log.lock().unwrap().opened += 1;
        Ok(())
    }

    fn payer(&self, credential: Credential) -> anyhow::Result<usize> {
        let mut log = self.log.lock().unwrap();
        log.payers_made += 1;
        let id = log.payers_made;
        log.signers.insert(id, credential.signer.address());
        Ok(id)
    }

    async fn pool_payer(&self, (): &()) -> anyhow::Result<Option<usize>> {
        if !self.pool {
            return Ok(None);
        }
        let mut log = self.log.lock().unwrap();
        log.payers_made += 1;
        let id = log.payers_made;
        log.signers.insert(id, POOL_SIGNER);
        Ok(Some(id))
    }

    /// [`HELLO`]'s five bytes; nothing else has a size.
    fn size(&self, (): &(), hash: [u8; 32]) -> Option<u64> {
        (hash[0] == HELLO).then_some(5)
    }

    async fn pull(
        &self,
        (): &(),
        payer: &usize,
        blob: &BlobFetch,
        sink: &mut Sink,
    ) -> anyhow::Result<()> {
        self.log.lock().unwrap().pulled_under.push(*payer);
        let Sink::Writer(writer) = sink else {
            anyhow::bail!("the gateway pulls into a writer");
        };
        match blob.hash[0] {
            STALL => writer.write_all(&vec![0u8; 1 << 20]).await?,
            HOLD => self.release.notified().await,
            REPLACED => {
                return Err(anyhow::Error::new(decdn_client::PoolReplaced {
                    closed: alloy::primitives::B256::repeat_byte(1),
                    opened: alloy::primitives::B256::repeat_byte(2),
                }));
            }
            _ => writer.write_all(b"hello").await?,
        }
        Ok(())
    }

    async fn pools(
        &self,
        (): &(),
        delegates: &[(PoolId, Address)],
    ) -> anyhow::Result<PoolSnapshot> {
        if self.log.lock().unwrap().end_pulls_in_store_read {
            self.release.notify_waiters();
            // The released pulls end, and drop their payers, on this thread.
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
        }
        Ok(PoolSnapshot {
            delegate_lanes: delegates
                .iter()
                .map(|&(pool_id, signer)| DelegateLanes::new(signer, pool_id, Vec::new()))
                .collect(),
            ..PoolSnapshot::default()
        })
    }

    /// A payer has one lane on the zero pool, at [`LIVE`].
    fn live_lanes(&self, payer: &usize) -> Vec<(LaneKey, Cumulative)> {
        let log = self.log.lock().unwrap();
        log.signers
            .get(payer)
            .map(|&signer| {
                let lane = LaneKey {
                    pool_id: PoolId::ZERO,
                    signer,
                    provider: LIVE_PROVIDER,
                };
                (lane, LIVE)
            })
            .into_iter()
            .collect()
    }

    async fn close(&self, (): ()) {
        self.log.lock().unwrap().closed += 1;
    }
}

fn common() -> ClientFetchArgs {
    #[derive(clap::Parser)]
    struct T {
        #[command(flatten)]
        common: ClientFetchArgs,
    }
    T::parse_from(["t"]).common
}

/// A credential for `signer`. The fake never checks the grant's signature.
fn credential(signer: &Arc<PrivateKeySigner>) -> Credential {
    Credential {
        signer: Arc::clone(signer),
        grant: CapabilityGrant {
            pool_id: alloy::primitives::B256::ZERO,
            signer: signer.address(),
            spending_cap: 1,
            expiry: u64::MAX,
            owner_signature: vec![0; 65],
        },
    }
}

/// Hand the worker a pull of blob `kind` paid by `signer`, with a pipe of
/// `pipe` bytes. Returns the response's end of the pipe and the result.
fn send(
    worker: &PullWorker,
    kind: u8,
    signer: &Arc<PrivateKeySigner>,
    pipe: usize,
) -> (
    tokio::io::DuplexStream,
    oneshot::Receiver<anyhow::Result<()>>,
) {
    send_paid(worker, kind, Payment::Held(credential(signer)), pipe)
}

/// Hand the worker a pull of blob `kind` paid from the gateway's own pool.
fn send_pool(
    worker: &PullWorker,
    kind: u8,
) -> (
    tokio::io::DuplexStream,
    oneshot::Receiver<anyhow::Result<()>>,
) {
    send_paid(worker, kind, Payment::Pool { namespace: None }, 1024)
}

fn send_paid(
    worker: &PullWorker,
    kind: u8,
    paying: Payment,
    pipe: usize,
) -> (
    tokio::io::DuplexStream,
    oneshot::Receiver<anyhow::Result<()>>,
) {
    let (writer, reader) = tokio::io::duplex(pipe);
    let (done, result) = oneshot::channel();
    worker
        .pull(Job {
            blob: BlobFetch {
                estimate: false,
                progress: None,
                hash: [kind; 32],
                namespace: None,
                output: PathBuf::from("-"),
                common: common(),
                read_ahead: None,
                size_cap: None,
                range: None,
            },
            paying,
            writer,
            done,
            admit: None,
        })
        .expect("the worker takes the job");
    (reader, result)
}

async fn start(max_pulls: usize) -> (PullWorker, Arc<Mutex<Log>>, Arc<Notify>) {
    start_with(max_pulls, false).await
}

async fn start_with(max_pulls: usize, pool: bool) -> (PullWorker, Arc<Mutex<Log>>, Arc<Notify>) {
    let log = Arc::new(Mutex::new(Log::default()));
    let release = Arc::new(Notify::new());
    let worker = PullWorker::start(
        Fake {
            log: Arc::clone(&log),
            release: Arc::clone(&release),
            pool,
        },
        max_pulls,
    )
    .await
    .expect("start the worker");
    (worker, log, release)
}

async fn read_all(mut reader: tokio::io::DuplexStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.expect("read the pipe");
    bytes
}

/// A response whose reader stops holds only its own pull: another request
/// streams to its end meanwhile.
#[tokio::test]
async fn a_stalled_response_does_not_block_another() {
    let (worker, _log, _release) = start(4).await;
    let signer = Arc::new(PrivateKeySigner::random());
    let (_stalled, _stalled_done) = send(&worker, STALL, &signer, 1024);
    let (reader, done) = send(&worker, HELLO, &signer, 1024);
    let bytes = tokio::time::timeout(Duration::from_secs(10), read_all(reader))
        .await
        .expect("the second request streams while the first is stalled");
    assert_eq!(bytes, b"hello");
    done.await.unwrap().unwrap();
    worker.stop().await;
}

/// Pulls run at once up to the bound, and the next waits for a slot.
#[tokio::test]
async fn pulls_past_the_bound_wait_for_a_slot() {
    let (worker, _log, release) = start(1).await;
    let signer = Arc::new(PrivateKeySigner::random());
    let (_held, held_done) = send(&worker, HOLD, &signer, 1024);
    let (mut reader, done) = send(&worker, HELLO, &signer, 1024);
    let mut byte = [0u8; 1];
    let waited = tokio::time::timeout(Duration::from_millis(200), reader.read(&mut byte)).await;
    assert!(waited.is_err(), "the second pull waits for the only slot");
    release.notify_one();
    held_done.await.unwrap().unwrap();
    assert_eq!(read_all(reader).await, b"hello");
    done.await.unwrap().unwrap();
    worker.stop().await;
}

/// A size lookup takes no pull slot: it is answered while every slot is
/// held. Once the worker stops, nothing has a size.
#[tokio::test]
async fn a_size_lookup_is_answered_beside_the_pulls() {
    let (worker, _log, release) = start(1).await;
    let signer = Arc::new(PrivateKeySigner::random());
    let (_held, held_done) = send(&worker, HOLD, &signer, 1024);
    let size = tokio::time::timeout(Duration::from_secs(10), worker.size([HELLO; 32]))
        .await
        .expect("the lookup needs no slot");
    assert_eq!(size, Some(5));
    release.notify_one();
    held_done.await.unwrap().unwrap();
    worker.stop().await;
    assert_eq!(worker.size([HELLO; 32]).await, None);
}

/// The host is built once for the worker's run, and closed when it stops.
#[tokio::test]
async fn the_host_is_built_once_and_closed_on_stop() {
    let (worker, log, _release) = start(4).await;
    let signer = Arc::new(PrivateKeySigner::random());
    for _ in 0..3 {
        let (reader, done) = send(&worker, HELLO, &signer, 1024);
        assert_eq!(read_all(reader).await, b"hello");
        done.await.unwrap().unwrap();
    }
    assert_eq!(log.lock().unwrap().opened, 1);
    worker.stop().await;
    let log = log.lock().unwrap();
    assert_eq!((log.opened, log.closed), (1, 1));
}

/// Pulls under one capability that overlap share one payer; a pull after the
/// last one ended gets a new one, and another capability its own.
#[tokio::test]
async fn overlapping_pulls_under_one_capability_share_a_payer() {
    let (worker, log, release) = start(4).await;
    let alice = Arc::new(PrivateKeySigner::random());
    let bob = Arc::new(PrivateKeySigner::random());

    let (_a, a_done) = send(&worker, HOLD, &alice, 1024);
    let (_b, b_done) = send(&worker, HOLD, &alice, 1024);
    let (_c, c_done) = send(&worker, HOLD, &bob, 1024);
    // Every held pull has started before the release.
    tokio::time::timeout(Duration::from_secs(10), async {
        while log.lock().unwrap().pulled_under.len() < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the three pulls start");
    release.notify_waiters();
    for done in [a_done, b_done, c_done] {
        done.await.unwrap().unwrap();
    }

    let (reader, done) = send(&worker, HELLO, &alice, 1024);
    assert_eq!(read_all(reader).await, b"hello");
    done.await.unwrap().unwrap();
    worker.stop().await;

    let log = log.lock().unwrap();
    let under = &log.pulled_under;
    assert_eq!(
        under[0], under[1],
        "alice's overlapping pulls share a payer"
    );
    assert_ne!(under[0], under[2], "bob pays with his own");
    assert_ne!(under[3], under[0], "alice's later pull gets a new payer");
    assert_eq!(log.payers_made, 3);
}

/// Every pull from the gateway's own pool shares one payer, built with the
/// host, that outlives the pulls: a later pull resumes the same lanes.
#[tokio::test]
async fn pool_pulls_share_one_payer_that_outlives_them() {
    let (worker, log, _release) = start_with(4, true).await;
    for _ in 0..3 {
        let (reader, done) = send_pool(&worker, HELLO);
        assert_eq!(read_all(reader).await, b"hello");
        done.await.unwrap().unwrap();
    }
    // A held credential beside it gets its own payer, as before.
    let signer = Arc::new(PrivateKeySigner::random());
    let (reader, done) = send(&worker, HELLO, &signer, 1024);
    assert_eq!(read_all(reader).await, b"hello");
    done.await.unwrap().unwrap();
    worker.stop().await;

    let log = log.lock().unwrap();
    let under = &log.pulled_under;
    assert_eq!(under[0], under[1]);
    assert_eq!(under[1], under[2], "pool pulls share the pool payer");
    assert_ne!(under[3], under[0], "a held credential pays on its own");
    assert_eq!(log.payers_made, 2);
}

/// A pool pull on a worker whose backend has no pool fails, and pays nothing.
#[tokio::test]
async fn a_pool_pull_without_a_pool_fails() {
    let (worker, log, _release) = start(4).await;
    let (_reader, done) = send_pool(&worker, HELLO);
    let err = done.await.unwrap().unwrap_err();
    assert!(format!("{err:#}").contains("no pool"), "{err:#}");
    worker.stop().await;
    assert_eq!(log.lock().unwrap().payers_made, 0);
}

/// A pull that ends with a pool replacement retires the pool payer: the next
/// pull runs under a fresh one, whose ledgers know only the new pool.
#[tokio::test]
async fn a_replaced_pool_rebuilds_the_pool_payer() {
    let (worker, log, _release) = start_with(4, true).await;
    let (_reader, done) = send_pool(&worker, REPLACED);
    let err = done.await.unwrap().unwrap_err();
    assert!(err.downcast_ref::<decdn_client::PoolReplaced>().is_some());
    let (reader, done) = send_pool(&worker, HELLO);
    assert_eq!(read_all(reader).await, b"hello");
    done.await.unwrap().unwrap();
    worker.stop().await;

    let log = log.lock().unwrap();
    assert_ne!(
        log.pulled_under[0], log.pulled_under[1],
        "the pull after a replacement runs under a new payer"
    );
    assert_eq!(log.payers_made, 2);
}

/// A worker whose pool payer cannot be built does not start: a wrong
/// keystore password fails `decdn gateway` before it listens.
#[tokio::test]
async fn a_pool_payer_that_cannot_be_built_fails_the_start() {
    struct NoKey;
    impl Backend for NoKey {
        type Host = ();
        type Payer = ();
        async fn open(&self) -> anyhow::Result<()> {
            Ok(())
        }
        fn payer(&self, _credential: Credential) -> anyhow::Result<()> {
            Ok(())
        }
        async fn pool_payer(&self, (): &()) -> anyhow::Result<Option<()>> {
            anyhow::bail!("wrong password")
        }
        async fn pull(
            &self,
            (): &(),
            (): &(),
            _blob: &BlobFetch,
            _sink: &mut Sink,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn close(&self, (): ()) {}
    }
    let Err(err) = PullWorker::start(NoKey, 1).await else {
        panic!("a pool payer that cannot be built starts no worker");
    };
    assert!(format!("{err:#}").contains("wrong password"), "{err:#}");
}

/// A worker whose host cannot be built does not start.
#[tokio::test]
async fn a_host_that_cannot_be_built_fails_the_start() {
    struct Broken;
    impl Backend for Broken {
        type Host = ();
        type Payer = ();
        async fn open(&self) -> anyhow::Result<()> {
            anyhow::bail!("no endpoint")
        }
        fn payer(&self, _credential: Credential) -> anyhow::Result<()> {
            Ok(())
        }
        async fn pull(
            &self,
            (): &(),
            (): &(),
            _blob: &BlobFetch,
            _sink: &mut Sink,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn close(&self, (): ()) {}
    }
    let Err(err) = PullWorker::start(Broken, 1).await else {
        panic!("a broken host starts no worker");
    };
    assert!(format!("{err:#}").contains("no endpoint"), "{err:#}");
}

/// A job's admission check runs once the pull has its slot, so what changed
/// while it waited (an allowance spent by the pulls before it) refuses it
/// before any paid work.
#[tokio::test]
async fn a_job_refused_at_its_slot_pulls_nothing() {
    let (worker, log, _release) = start_with(4, true).await;
    let (writer, reader) = tokio::io::duplex(1024);
    let (done, result) = oneshot::channel();
    worker
        .pull(Job {
            blob: BlobFetch {
                estimate: false,
                progress: None,
                hash: [HELLO; 32],
                namespace: None,
                output: PathBuf::from("-"),
                common: common(),
                read_ahead: None,
                size_cap: None,
                range: None,
            },
            paying: Payment::Pool { namespace: None },
            writer,
            done,
            admit: Some(Box::new(|| false)),
        })
        .expect("the worker takes the job");
    let err = result.await.unwrap().unwrap_err();
    assert!(
        err.downcast_ref::<super::NotAdmitted>().is_some(),
        "{err:#}"
    );
    assert!(read_all(reader).await.is_empty());
    worker.stop().await;
    assert!(log.lock().unwrap().pulled_under.is_empty(), "no pull ran");
}

/// A pools query is answered while pulls run: it does not wait for a slot
/// or for the pulls to end.
#[tokio::test]
async fn a_pools_query_is_answered_beside_running_pulls() {
    let (worker, _log, release) = start(1).await;
    let signer = Arc::new(PrivateKeySigner::random());
    let (_held, held_done) = send(&worker, HOLD, &signer, 1024);
    let delegate = (PoolId::repeat_byte(7), signer.address());
    let snapshot = tokio::time::timeout(Duration::from_secs(10), worker.pools(vec![delegate]))
        .await
        .expect("the query is answered while the only slot is held")
        .expect("the fake answers");
    assert_eq!(snapshot.delegate_lanes.len(), 1);
    assert_eq!(snapshot.delegate_lanes[0].pool_id, delegate.0);
    assert_eq!(snapshot.delegate_lanes[0].signer, delegate.1);
    release.notify_one();
    held_done.await.unwrap().unwrap();
    worker.stop().await;
}

/// A capability's spend counts the live ledger of a pull through it while the
/// pull runs, and the store alone once it ended.
#[tokio::test]
async fn a_pools_query_counts_the_pulls_in_flight() {
    let (worker, _log, release) = start(2).await;
    let alice = Arc::new(PrivateKeySigner::random());
    let bob = Arc::new(PrivateKeySigner::random());
    let (_held, held_done) = send(&worker, HOLD, &alice, 1024);
    let delegates = vec![
        (PoolId::ZERO, alice.address()),
        (PoolId::ZERO, bob.address()),
    ];
    let spend = |snapshot: &PoolSnapshot| {
        snapshot
            .delegate_lanes
            .iter()
            .map(|d| (d.spent_micro_usdc, d.bytes_delivered))
            .collect::<Vec<_>>()
    };
    let during = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = worker.pools(delegates.clone()).await.unwrap();
            if snapshot.delegate_lanes[0].spent_micro_usdc > 0 {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the held pull's payer is live");
    assert_eq!(
        spend(&during),
        [(500, 4096), (0, 0)],
        "alice's pull in flight counts, and bob has none"
    );
    release.notify_one();
    held_done.await.unwrap().unwrap();
    let after = worker.pools(delegates).await.unwrap();
    assert_eq!(spend(&after), [(0, 0), (0, 0)], "only the store is left");
    worker.stop().await;
}

/// A lane's spend is the larger of its stored watermark and its live ledger,
/// never their sum; a lane the store does not hold yet counts in full, and
/// another key's lanes not at all.
#[test]
fn live_ledgers_lift_the_stored_spend_without_double_counting() {
    let signer = Address::repeat_byte(1);
    // Stored lanes spelled as the store's reader spells them.
    let stored = |provider: Address, amount: u64| {
        let lane = LaneKey {
            pool_id: PoolId::ZERO,
            signer,
            provider,
        };
        let progress = BuyerLaneProgress {
            last_amount: U256::from(amount),
            last_bytes: U256::from(amount * 10),
        };
        lane_snapshot(lane, progress)
    };
    let behind = Address::repeat_byte(0xa);
    let ahead = Address::repeat_byte(0xb);
    let unstored = Address::repeat_byte(0xc);
    let mut delegate = DelegateLanes::new(
        signer,
        PoolId::ZERO,
        vec![stored(behind, 300), stored(ahead, 700)],
    );
    assert_eq!(
        (delegate.spent_micro_usdc, delegate.bytes_delivered),
        (1000, 10_000)
    );
    let lane = |signer, provider| LaneKey {
        pool_id: PoolId::ZERO,
        signer,
        provider,
    };
    let other_pool = LaneKey {
        pool_id: PoolId::repeat_byte(3),
        ..lane(signer, behind)
    };
    let at = |amount: u64| Cumulative {
        bytes: U256::from(amount * 10),
        amount: U256::from(amount),
    };
    let live = HashMap::from([
        (lane(signer, behind), at(500)),
        (lane(signer, ahead), at(600)),
        (lane(signer, unstored), at(40)),
        (lane(Address::repeat_byte(2), behind), at(9000)),
        (other_pool, at(9000)),
    ]);
    delegate.include_live(&live);
    // behind 500 (not 300 + 500), ahead 700, unstored 40.
    assert_eq!(
        (delegate.spent_micro_usdc, delegate.bytes_delivered),
        (1240, 12_400)
    );
    // Counting again changes nothing.
    delegate.include_live(&live);
    assert_eq!(
        (delegate.spent_micro_usdc, delegate.bytes_delivered),
        (1240, 12_400)
    );
}

/// A pull that ends while the store is read, and drops its payer, still
/// counts: its ledger was read before the store.
#[tokio::test]
async fn a_pull_that_ends_during_the_store_read_still_counts() {
    let (worker, log, _release) = start(2).await;
    let alice = Arc::new(PrivateKeySigner::random());
    let (_held, held_done) = send(&worker, HOLD, &alice, 1024);
    tokio::time::timeout(Duration::from_secs(10), async {
        while log.lock().unwrap().pulled_under.is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the held pull starts");
    log.lock().unwrap().end_pulls_in_store_read = true;
    let snapshot = worker
        .pools(vec![(PoolId::ZERO, alice.address())])
        .await
        .unwrap();
    held_done.await.unwrap().unwrap();
    assert_eq!(
        (
            snapshot.delegate_lanes[0].spent_micro_usdc,
            snapshot.delegate_lanes[0].bytes_delivered
        ),
        (500, 4096)
    );
    worker.stop().await;
}

/// The gateway's pool payer lives for the run, so the lanes a `--capability`
/// grant signs through it count live even between pulls.
#[tokio::test]
async fn a_pools_query_counts_the_pool_payers_live_lanes() {
    let (worker, _log, _release) = start_with(2, true).await;
    let (reader, done) = send_pool(&worker, HELLO);
    assert_eq!(read_all(reader).await, b"hello");
    done.await.unwrap().unwrap();
    let snapshot = worker
        .pools(vec![(PoolId::ZERO, POOL_SIGNER)])
        .await
        .unwrap();
    assert_eq!(
        (
            snapshot.delegate_lanes[0].spent_micro_usdc,
            snapshot.delegate_lanes[0].bytes_delivered
        ),
        (500, 4096)
    );
    worker.stop().await;
}

/// A query after the worker stopped fails instead of hanging.
#[tokio::test]
async fn a_pools_query_after_stop_fails() {
    let (worker, _log, _release) = start(1).await;
    worker.stop().await;
    let err = tokio::time::timeout(Duration::from_secs(10), worker.pools(Vec::new()))
        .await
        .expect("the query does not hang")
        .expect_err("a stopped worker answers nothing");
    assert!(err.to_string().contains("shutting down"), "{err:#}");
}

const DEPLOYMENT: decdn_incentive::Deployment = decdn_incentive::Deployment {
    chain_id: 421_614,
    payment_pool: Address::repeat_byte(0x9c),
};

/// A delegate lane of `signer` on `pool`, to node `0xb2`, at `amount`.
fn seed_delegate_lane(
    store: &decdn_incentive::MemoryBuyerPoolStore,
    pool: PoolId,
    signer: Address,
    amount: u64,
) {
    use decdn_incentive::buyer_pool::{AdvanceOutcome, BuyerLaneProgress, BuyerPoolStore as _};
    let outcome = store
        .advance_delegate_progress(
            DEPLOYMENT,
            decdn_incentive::lane::LaneKey {
                pool_id: pool,
                signer,
                provider: Address::repeat_byte(0xb2),
            },
            BuyerLaneProgress {
                last_amount: alloy::primitives::U256::from(amount),
                last_bytes: alloy::primitives::U256::from(amount * 10),
            },
        )
        .unwrap();
    assert_eq!(outcome, AdvanceOutcome::Advanced);
}

/// The store read lists each queried key's delegate lanes on its pool: a
/// capability a page registered, and the `--capability` grant a public
/// gateway pays under, whose lanes are delegate lanes too.
#[test]
fn the_store_read_lists_every_queried_keys_lanes() {
    let store = decdn_incentive::MemoryBuyerPoolStore::new();
    let registered = (PoolId::repeat_byte(1), Address::repeat_byte(0x11));
    let granted = (PoolId::repeat_byte(2), Address::repeat_byte(0x22));
    seed_delegate_lane(&store, registered.0, registered.1, 100);
    seed_delegate_lane(&store, granted.0, granted.1, 250);
    let grant = CapabilityGrant {
        pool_id: granted.0,
        signer: granted.1,
        spending_cap: 1_000,
        expiry: u64::MAX,
        owner_signature: vec![0; 65],
    };
    let queries = lane_queries(Some(&grant), &[registered]);
    let snapshot = read_pools(&store, DEPLOYMENT, &queries).unwrap();
    let mut listed: Vec<_> = snapshot
        .delegate_lanes
        .iter()
        .map(|d| {
            let paid: u64 = d.lanes.iter().map(|l| l.last_amount_micro_usdc).sum();
            (d.pool_id, d.signer, paid)
        })
        .collect();
    listed.sort();
    assert_eq!(
        listed,
        vec![
            (registered.0, registered.1, 100),
            (granted.0, granted.1, 250)
        ]
    );
    assert!(snapshot.pools.pools.is_empty());
}

/// The grant's key is asked once, even when a capability names it too.
#[test]
fn lane_queries_add_the_grant_once() {
    let key = (PoolId::repeat_byte(3), Address::repeat_byte(0x33));
    let grant = CapabilityGrant {
        pool_id: key.0,
        signer: key.1,
        spending_cap: 1,
        expiry: u64::MAX,
        owner_signature: vec![0; 65],
    };
    assert_eq!(lane_queries(Some(&grant), &[key]), vec![key]);
    assert_eq!(lane_queries(None, &[key]), vec![key]);
    assert_eq!(lane_queries(Some(&grant), &[]), vec![key]);
}
