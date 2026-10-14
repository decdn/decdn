use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::signers::local::PrivateKeySigner;
use clap::Parser as _;
use decdn_common::cli::ClientFetchArgs;
use decdn_incentive::CapabilityGrant;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{Notify, oneshot};

use super::{Backend, Job, PullWorker};
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
}

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

    fn payer(&self, _credential: Credential) -> anyhow::Result<usize> {
        let mut log = self.log.lock().unwrap();
        log.payers_made += 1;
        Ok(log.payers_made)
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
    let (writer, reader) = tokio::io::duplex(pipe);
    let (done, result) = oneshot::channel();
    worker
        .pull(Job {
            blob: BlobFetch {
                hash: [kind; 32],
                namespace: None,
                output: PathBuf::from("-"),
                common: common(),
            },
            paying: credential(signer),
            writer,
            done,
        })
        .expect("the worker takes the job");
    (reader, result)
}

async fn start(max_pulls: usize) -> (PullWorker, Arc<Mutex<Log>>, Arc<Notify>) {
    let log = Arc::new(Mutex::new(Log::default()));
    let release = Arc::new(Notify::new());
    let worker = PullWorker::start(
        Fake {
            log: Arc::clone(&log),
            release: Arc::clone(&release),
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
