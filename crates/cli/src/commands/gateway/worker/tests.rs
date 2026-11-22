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

use super::{Backend, DelegateLanes, Job, PoolSnapshot, PullWorker, StallLimit, read_pools};
use crate::commands::fetch::{BlobFetch, Credential, Sink};

/// The blob a [`Fake`] pull stalls on: it writes more than the pipe holds.
const STALL: u8 = 0;
/// The blob a [`Fake`] pull writes `hello` for.
const HELLO: u8 = 1;
/// The blob a [`Fake`] pull holds open until the test releases it.
const HOLD: u8 = 2;

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
            _ => writer.write_all(b"hello").await?,
        }
        Ok(())
    }

    async fn pools(
        &self,
        (): &(),
        delegates: &[super::DelegateKey],
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
                .map(|&(_, pool_id, signer)| DelegateLanes::new(signer, pool_id, Vec::new()))
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

/// A credential for `signer` on the zero pool. The fake never checks the
/// grant's signature.
fn credential(signer: &Arc<PrivateKeySigner>) -> Credential {
    credential_on(signer, PoolId::ZERO)
}

/// A credential for `signer` on `pool`.
fn credential_on(signer: &Arc<PrivateKeySigner>, pool: PoolId) -> Credential {
    Credential {
        signer: Arc::clone(signer),
        grant: CapabilityGrant {
            pool_id: pool,
            signer: signer.address(),
            spending_cap: 1,
            expiry: u64::MAX,
            owner_signature: vec![0; 65],
        },
        pool: DEPLOYMENT,
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
    send_paid(worker, kind, credential(signer), pipe)
}

fn send_paid(
    worker: &PullWorker,
    kind: u8,
    paying: Credential,
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
                size_only: false,
            },
            paying,
            writer,
            done,
            listing: Box::new(()),
        })
        .expect("the worker takes the job");
    (reader, result)
}

/// A stall limit no test reaches unless it means to.
const LONG_STALL: Duration = Duration::from_secs(60);

async fn start(max_pulls: usize) -> (PullWorker, Arc<Mutex<Log>>, Arc<Notify>) {
    start_with(max_pulls, LONG_STALL).await
}

async fn start_with(
    max_pulls: usize,
    stall: Duration,
) -> (PullWorker, Arc<Mutex<Log>>, Arc<Notify>) {
    let log = Arc::new(Mutex::new(Log::default()));
    let release = Arc::new(Notify::new());
    let worker = PullWorker::start(
        Fake {
            log: Arc::clone(&log),
            release: Arc::clone(&release),
        },
        max_pulls,
        stall,
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

/// Responses that are never read hold their slots only for the stall limit:
/// with every slot held by one, a further request still streams. Each unread
/// pull ends with the stall error. The unread pipes stay open, so the pulls
/// cannot end on a closed pipe instead.
#[tokio::test]
async fn unread_responses_free_their_slots() {
    let (worker, _log, _release) = start_with(2, Duration::from_millis(200)).await;
    let signer = Arc::new(PrivateKeySigner::random());
    let (first_pipe, first_ended) = send(&worker, STALL, &signer, 1024);
    let (second_pipe, second_ended) = send(&worker, STALL, &signer, 1024);
    let (reader, done) = send(&worker, HELLO, &signer, 1024);
    let bytes = tokio::time::timeout(Duration::from_secs(10), read_all(reader))
        .await
        .expect("a further request streams once the unread pulls give up");
    assert_eq!(bytes, b"hello");
    done.await.unwrap().unwrap();
    for ended in [first_ended, second_ended] {
        let err = ended.await.unwrap().expect_err("an unread pull fails");
        assert!(
            format!("{err:#}").contains("read nothing"),
            "the pull ends on the stall limit: {err:#}"
        );
    }
    drop((first_pipe, second_pipe));
    worker.stop().await;
}

/// A response that reads each frame within the stall limit is not cut,
/// however long the whole pull takes: the limit counts each wait, not the
/// pull. Each wait here is about 50 ms against a 1 s limit, and the pull
/// takes about 1.6 s.
#[tokio::test]
async fn a_slow_reader_is_not_cut() {
    let stall = Duration::from_secs(1);
    let (worker, _log, _release) = start_with(1, stall).await;
    let signer = Arc::new(PrivateKeySigner::random());
    let (mut reader, done) = send(&worker, STALL, &signer, 32 * 1024);
    let started = tokio::time::Instant::now();
    let mut buf = vec![0u8; 32 * 1024];
    let mut total = 0;
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let n = reader.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        total += n;
    }
    assert_eq!(total, 1 << 20, "every byte arrives");
    assert!(
        started.elapsed() > stall,
        "the pull outlasts the stall limit, so the test is not vacuous"
    );
    done.await.unwrap().unwrap();
    worker.stop().await;
}

/// The stall limit counts each wait on the response, not the whole pull: a
/// write still waits just short of the limit, a read before it lands the
/// write and restarts the limit, and a wait that reaches it fails with
/// `TimedOut`.
#[tokio::test(start_paused = true)]
async fn the_stall_limit_counts_each_wait() {
    let limit = Duration::from_secs(10);
    let short = Duration::from_millis(9_999);
    let (inner, mut reader) = tokio::io::duplex(4);
    let mut writer = StallLimit::new(inner, limit, [0; 32]);
    writer.write_all(&[0; 4]).await.unwrap();
    let mut buf = [0u8; 4];
    for _ in 0..2 {
        let write = writer.write_all(&[1; 4]);
        tokio::pin!(write);
        assert!(
            tokio::time::timeout(short, &mut write).await.is_err(),
            "a write into a full pipe still waits short of the limit"
        );
        reader.read_exact(&mut buf).await.unwrap();
        write.await.expect("a read lands the waiting write");
    }
    let err = writer
        .write_all(&[2; 4])
        .await
        .expect_err("the wait reaches the limit");
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    assert!(err.to_string().contains("read nothing"), "{err}");
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

/// One key holds a capability per pool: overlapping pulls under the same key
/// on two pools run under two payers, as each pool has its own lanes.
#[tokio::test]
async fn one_key_on_two_pools_pays_through_two_payers() {
    let (worker, log, release) = start(4).await;
    let key = Arc::new(PrivateKeySigner::random());
    let one = credential_on(&key, PoolId::repeat_byte(1));
    let two = credential_on(&key, PoolId::repeat_byte(2));
    let (_a, a_done) = send_paid(&worker, HOLD, one, 1024);
    let (_b, b_done) = send_paid(&worker, HOLD, two, 1024);
    tokio::time::timeout(Duration::from_secs(10), async {
        while log.lock().unwrap().pulled_under.len() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("both pulls start");
    release.notify_waiters();
    for done in [a_done, b_done] {
        done.await.unwrap().unwrap();
    }
    worker.stop().await;
    let log = log.lock().unwrap();
    assert_ne!(log.pulled_under[0], log.pulled_under[1]);
    assert_eq!(log.payers_made, 2);
}

/// The same pool id on two `PaymentPool` contracts names two pools: pulls
/// under one key on both run under two payers.
#[tokio::test]
async fn one_pool_id_on_two_contracts_pays_through_two_payers() {
    let (worker, log, release) = start(4).await;
    let key = Arc::new(PrivateKeySigner::random());
    let here = credential(&key);
    let there = Credential {
        pool: OTHER_DEPLOYMENT,
        ..credential(&key)
    };
    let (_a, a_done) = send_paid(&worker, HOLD, here, 1024);
    let (_b, b_done) = send_paid(&worker, HOLD, there, 1024);
    tokio::time::timeout(Duration::from_secs(10), async {
        while log.lock().unwrap().pulled_under.len() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("both pulls start");
    release.notify_waiters();
    for done in [a_done, b_done] {
        done.await.unwrap().unwrap();
    }
    worker.stop().await;
    let log = log.lock().unwrap();
    assert_ne!(log.pulled_under[0], log.pulled_under[1]);
    assert_eq!(log.payers_made, 2);
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
    let Err(err) = PullWorker::start(Broken, 1, LONG_STALL).await else {
        panic!("a broken host starts no worker");
    };
    assert!(format!("{err:#}").contains("no endpoint"), "{err:#}");
}

/// A pools query is answered while pulls run: it does not wait for a slot
/// or for the pulls to end.
#[tokio::test]
async fn a_pools_query_is_answered_beside_running_pulls() {
    let (worker, _log, release) = start(1).await;
    let signer = Arc::new(PrivateKeySigner::random());
    let (_held, held_done) = send(&worker, HOLD, &signer, 1024);
    let delegate = (DEPLOYMENT, PoolId::repeat_byte(7), signer.address());
    let snapshot = tokio::time::timeout(Duration::from_secs(10), worker.pools(vec![delegate]))
        .await
        .expect("the query is answered while the only slot is held")
        .expect("the fake answers");
    assert_eq!(snapshot.delegate_lanes.len(), 1);
    assert_eq!(snapshot.delegate_lanes[0].pool_id, delegate.1);
    assert_eq!(snapshot.delegate_lanes[0].signer, delegate.2);
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
        (DEPLOYMENT, PoolId::ZERO, alice.address()),
        (DEPLOYMENT, PoolId::ZERO, bob.address()),
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
        .pools(vec![(DEPLOYMENT, PoolId::ZERO, alice.address())])
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

/// A second `PaymentPool` contract, on another payment chain.
const OTHER_DEPLOYMENT: decdn_incentive::Deployment = decdn_incentive::Deployment {
    chain_id: 84_532,
    payment_pool: Address::repeat_byte(0x9d),
};

/// A delegate lane of `signer` on `pool` of `deployment`, to node `0xb2`, at
/// `amount`.
fn seed_delegate_lane(
    store: &decdn_incentive::MemoryBuyerPoolStore,
    (deployment, pool, signer): super::DelegateKey,
    amount: u64,
) {
    use decdn_incentive::buyer_pool::{AdvanceOutcome, BuyerLaneProgress, BuyerPoolStore as _};
    let outcome = store
        .advance_delegate_progress(
            deployment,
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

/// The store read lists each queried capability's delegate lanes on its pool
/// and contract: here one capability on each of two configured contracts.
#[test]
fn the_store_read_lists_every_queried_keys_lanes() {
    let store = decdn_incentive::MemoryBuyerPoolStore::new();
    let registered = (
        OTHER_DEPLOYMENT,
        PoolId::repeat_byte(1),
        Address::repeat_byte(0x11),
    );
    let granted = (
        DEPLOYMENT,
        PoolId::repeat_byte(2),
        Address::repeat_byte(0x22),
    );
    seed_delegate_lane(&store, registered, 100);
    seed_delegate_lane(&store, granted, 250);
    let snapshot = read_pools(&store, &[registered, granted]).unwrap();
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
            (registered.1, registered.2, 100),
            (granted.1, granted.2, 250)
        ]
    );
    assert!(snapshot.pools.pools.is_empty());
    // The same pool and key on the other contract holds no lane.
    let elsewhere = read_pools(&store, &[(DEPLOYMENT, registered.1, registered.2)]).unwrap();
    assert!(elsewhere.delegate_lanes[0].lanes.is_empty());
}
