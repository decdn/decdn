use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context as _;
use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::Bytes;
use decdn_bao_range::{AlignedRange, IROH_BLOCK_SIZE, align_range, encode_verified_range};
use tokio::io::AsyncReadExt;

use super::{StreamCandidate, Streamer, VerifiedReader};
use crate::pacer::PULL_WINDOW_FLOOR;
use crate::sink::MemoryBlobCache;
use crate::source::{BlobSource, FakeFunder, Recovery, ScriptedSource, SourceFuture};
use crate::{
    BlobCache, Cumulative, NoCache, PoolContext, PoolLedger, ProgressClock, PullConfig,
    StaticSources, StopPolicy, UpstreamPullHeader, UpstreamRefused, VoucherProgress,
};
use decdn_protocol::client::StreamError;

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
        lease: crate::LaneLease::default(),
        widen: None,
    }
}

/// A streamer over the static lanes `candidates`, filling into `scratch`.
fn streamer<S: BlobSource>(
    candidates: Vec<StreamCandidate<S>>,
    scratch: &std::path::Path,
) -> anyhow::Result<Streamer<'_, StaticSources<S>, FakeFunder>> {
    let sources = StaticSources::new(candidates)?;
    let holders = sources.holders();
    Ok(Streamer::new(sources, funder(), scratch).holders(holders))
}

/// The stop a script gets: give up after [`crate::SCRIPT_GIVE_UP`].
fn stop() -> StopPolicy {
    StopPolicy::new(false, None, Arc::new(ProgressClock::new()))
}

#[test]
fn a_candidates_measured_coverage_reaches_its_holder() -> anyhow::Result<()> {
    // A partial holder's measured coverage (#1506) must reach its holder
    // so the scheduler never assigns it a range it does not hold; a candidate
    // with no measured coverage (`None`) is a full holder.
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
    let holders = StaticSources::new(vec![c0, c1])?.holders();
    let [partial_holder, full_holder] = holders.as_slice() else {
        anyhow::bail!("one holder per candidate");
    };
    assert_eq!(partial_holder.coverage, Some(partial));
    assert_eq!(
        full_holder.coverage, None,
        "a `None` candidate is a full holder"
    );
    Ok(())
}

fn funder() -> FakeFunder {
    FakeFunder::new(Recovery::ToppedUp(U256::from(u128::MAX)))
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
    let total = source.total_bytes();
    drain_range(source, ledger, total, (0, 0), cache, config).await
}

/// Open a stream over `source` for `window` with the given `claim` and drain
/// it, then finish the drive.
async fn drain_range(
    source: ScriptedSource,
    ledger: Arc<PoolLedger>,
    claim: u64,
    window: (u64, u64),
    cache: Arc<dyn BlobCache>,
    config: &PullConfig,
) -> anyhow::Result<Vec<u8>> {
    let root = source.root();
    let scratch = tempfile::tempdir()?;
    let streamer = streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?;
    let (mut reader, mut drive) = streamer
        .open_range(root, claim, window, config, cache, stop())
        .await?;
    let mut out = Vec::new();
    drive.alongside(reader.read_to_end(&mut out)).await?;
    drive.finish().await?;
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

/// A first claim far below the blob does not end the stream at the claim:
/// the fetch grows it, and the stream ends at the size a leg proves, with
/// every byte of the blob.
#[tokio::test]
async fn the_stream_ends_at_the_proven_size_when_the_claim_was_small() -> anyhow::Result<()> {
    let blob = payload(3 * 1024 * 1024 + 777);
    let (source, ledger) = paying_source(blob.clone())?;
    let root = source.root();
    let scratch = tempfile::tempdir()?;
    let streamer = streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?;
    let (mut reader, mut drive) = streamer
        .open(
            root,
            64 * 1024,
            &PullConfig::default(),
            Arc::new(NoCache),
            stop(),
        )
        .await?;
    let mut out = Vec::new();
    drive.alongside(reader.read_to_end(&mut out)).await?;
    anyhow::ensure!(
        out.len() == blob.len(),
        "{} of {} bytes",
        out.len(),
        blob.len()
    );
    anyhow::ensure!(out == blob, "the grown stream must be byte-identical");
    Ok(())
}

/// A sink that reports `caches() == false` is NEVER teed the whole blob on a
/// clean finish — the `Streamer` skips the whole-blob read (and the memory it
/// would cost) that only exists to populate a cache. This is what keeps a
/// `decdn get decdn:<hash> -o -` of a huge blob from spiking its whole size into RAM at
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
        ) -> crate::sink::SinkFuture<'_, Option<Bytes>> {
            Box::pin(async { Ok(None) })
        }
        fn put(
            &self,
            _hash: [u8; 32],
            _offset: u64,
            _bytes: Bytes,
        ) -> crate::sink::SinkFuture<'_, ()> {
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
    let streamer = streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?;
    let (mut reader, mut drive) = streamer
        .open(root, total, &PullConfig::default(), cache, stop())
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
/// The lone source keeps failing its tail, so the fetch gives up once the
/// stop policy's limit passes without a verified byte.
#[tokio::test(start_paused = true)]
async fn tampered_tail_fails_and_never_yields_unverified() -> anyhow::Result<()> {
    let blob = payload(400_000);
    let source = TamperTailSource::new(blob.clone())?;
    let root = source.root;
    let total = source.total;

    let scratch = tempfile::tempdir()?;
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let streamer = streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?;
    let (mut reader, mut drive) = streamer
        .open(
            root,
            total,
            &PullConfig::default(),
            Arc::new(NoCache),
            stop(),
        )
        .await?;

    let mut out = Vec::new();
    let result = drive.alongside(reader.read_to_end(&mut out)).await;
    let err = result
        .err()
        .ok_or_else(|| anyhow::anyhow!("a tampered tail must surface as a read error, not EOF"))?;
    // The reader flattens the drive's error into its message.
    let gave_up = crate::GaveUp {
        idle: crate::stop::SCRIPT_GIVE_UP,
    };
    anyhow::ensure!(
        err.to_string().contains(&gave_up.to_string()),
        "the fetch gives up on the lone failing source: {err}"
    );
    // The drive keeps the error typed, so a caller can map a give-up to
    // its own exit.
    let typed = drive
        .take_error()
        .ok_or_else(|| anyhow::anyhow!("the drive must hold the fetch's error"))?;
    anyhow::ensure!(
        typed.downcast_ref::<crate::GaveUp>() == Some(&gave_up),
        "the drive's error stays a GaveUp: {typed:#}"
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
    let streamer = streamer(
        vec![
            candidate(faulty, ledger_a, 0xA1),
            candidate(healthy, ledger_b, 0xB2),
        ],
        scratch.path(),
    )?;
    let (mut reader, mut drive) = streamer
        .open(
            root,
            total,
            &PullConfig::default(),
            Arc::new(NoCache),
            stop(),
        )
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
    let streamer = streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?;
    let (mut reader, mut drive) = streamer
        .open(root, total, &config, Arc::new(NoCache), stop())
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
                        let present = super::present_frontier(&live.state.store, 0).await?;
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
    let streamer = streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?;
    let (mut reader, mut drive) = streamer
        .open(root, total, &config, Arc::new(NoCache), stop())
        .await?;

    // A paused consumer: nothing is read while the drive runs.
    drive
        .alongside(tokio::time::sleep(Duration::from_millis(300)))
        .await;
    let present = match &reader {
        VerifiedReader::Live(live) => super::present_frontier(&live.state.store, 0).await?,
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

/// A consumer that pauses longer than the give-up limit, with the drive
/// parked on a full read-ahead window, is not a stall: the stop clock holds
/// while the drive waits on the consumer, and the stream completes.
#[tokio::test(start_paused = true)]
async fn a_paused_consumer_does_not_give_up() -> anyhow::Result<()> {
    let blob = payload(4 * 1024 * 1024);
    let (source, ledger) = paying_source(blob.clone())?;
    let root = source.root();
    let total = source.total_bytes();
    let config = PullConfig {
        read_ahead_bytes: PULL_WINDOW_FLOOR,
        ..PullConfig::default()
    };
    let scratch = tempfile::tempdir()?;
    let streamer = streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?;
    let limit = Duration::from_secs(30);
    let stop = StopPolicy::new(false, Some(limit), Arc::new(ProgressClock::new()));
    let (mut reader, mut drive) = streamer
        .open(root, total, &config, Arc::new(NoCache), stop)
        .await?;

    // The consumer reads nothing for ten times the limit.
    drive.alongside(tokio::time::sleep(limit * 10)).await;
    let mut out = Vec::new();
    drive.alongside(reader.read_to_end(&mut out)).await?;
    anyhow::ensure!(out == blob, "the stream must complete byte-identical");
    anyhow::ensure!(drive.take_error().is_none(), "the drive must not give up");
    Ok(())
}

/// A lane parked on the consumer holds the stop clock only while verified
/// bytes wait unread ahead of the cursor. With everything present already
/// read, the missing bytes (a gap whose only holder is dead) are the
/// fetch's to deliver, so the clock runs and the stream gives up at the
/// limit; with unread bytes ahead, it waits on the consumer past it.
#[tokio::test(start_paused = true)]
async fn a_parked_lane_holds_the_clock_only_while_bytes_wait_unread() -> anyhow::Result<()> {
    use crate::driver::PacingWait as _;
    use crate::pacer::DownstreamFrontier;
    use std::sync::atomic::AtomicU64;
    use tokio::sync::Notify;

    let blob = payload(2 * 1024 * 1024);
    let total = u64::try_from(blob.len())?;
    let root = *PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE)
        .root
        .as_bytes();
    let dir = tempfile::tempdir()?;
    crate::ClientRangedStore::seed_checkpointed_prefix(dir.path(), "s", &blob, total / 2)?;
    let limit = Duration::from_secs(30);

    for cursor_at_frontier in [true, false] {
        let store = crate::ClientRangedStore::open(dir.path(), "s", root)?;
        let frontier = super::present_frontier(&store, 0).await?;
        anyhow::ensure!(frontier > 0 && frontier < total, "a partial store");
        let cursor = if cursor_at_frontier { frontier } else { 0 };
        let state = Arc::new(super::StreamState {
            store,
            cursor: AtomicU64::new(cursor),
            start: 0,
            end: None,
            hint: total,
            consumed: Notify::new(),
            progressed: Notify::new(),
            outcome: Mutex::new(None),
        });
        let clock = Arc::new(ProgressClock::new());
        let stop = StopPolicy::new(false, Some(limit), Arc::clone(&clock));
        let wait = super::ConsumedWait { state, clock };
        let observed = DownstreamFrontier {
            served_paid: cursor,
            serve_demand: 0,
        };
        let start = tokio::time::Instant::now();
        tokio::select! {
            () = wait.wait(observed, crate::driver::WaitReason::WindowFull) => {
                anyhow::bail!("nothing moves the cursor, so the wait must not end");
            }
            gave_up = stop.expired() => {
                anyhow::ensure!(cursor_at_frontier, "unread bytes ahead must hold the clock");
                anyhow::ensure!(gave_up.idle == limit);
                anyhow::ensure!(tokio::time::Instant::now() - start == limit);
            }
            () = tokio::time::sleep(limit * 10) => {
                anyhow::ensure!(
                    !cursor_at_frontier,
                    "with everything read, the clock must run while the lane waits"
                );
            }
        }
    }
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
    let streamer = streamer(
        vec![
            candidate(faulty, ledger_a, 0xA1),
            candidate(healthy, ledger_b, 0xB2),
        ],
        scratch.path(),
    )?;
    let config = PullConfig {
        read_ahead_bytes: PULL_WINDOW_FLOOR,
        ..PullConfig::default()
    };
    let (mut reader, mut drive) = streamer
        .open(root, total, &config, Arc::new(NoCache), stop())
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
            let combined = encode_verified_range(self.root, &range, data, self.outboard.clone())?;
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
            };
            Ok((header, Bytes::from(w)))
        })
    }

    fn finish(&self, _reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        Box::pin(async { Ok(VoucherProgress::default()) })
    }

    fn stop(&self, _reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        Box::pin(async { Ok(VoucherProgress::default()) })
    }
}

/// Static lanes, one of whose builds ends after a delay with an error that
/// says its top-up mined but its local record was not written: a lane build
/// still in flight as the stream's last byte lands.
struct LateEscrowBuild {
    lanes: StaticSources<ScriptedSource>,
    late: Address,
}

impl crate::SourceProvider for LateEscrowBuild {
    type Source = ScriptedSource;

    fn discover(&self, hash: [u8; 32]) -> SourceFuture<'_, Vec<crate::Holder>> {
        self.lanes.discover(hash)
    }

    fn connect<'a>(
        &'a self,
        holder: &'a crate::Holder,
        ended: &'a tokio_util::sync::CancellationToken,
    ) -> SourceFuture<'a, StreamCandidate<ScriptedSource>> {
        let late = holder.provider == self.late;
        Box::pin(async move {
            if !late {
                return self.lanes.connect(holder, ended).await;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
            Err(crate::buyer_pool::escrowed_but_untracked(
                "pool 0x01 topped up by 5 µUSDC",
                alloy::primitives::TxHash::repeat_byte(0xab),
                "disk full",
            ))
        })
    }
}

/// A stream read to its end still reports what the fetch ends with after its
/// last byte: a late lane build that may have escrowed USDC no record
/// credits. The reader's end of stream does not carry it; `finish` does.
#[tokio::test(start_paused = true)]
async fn finish_reports_a_late_lane_build_escrow_after_the_last_byte() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let (source, ledger) = paying_source(blob.clone())?;
    let (root, total) = (source.root(), source.total_bytes());
    let (late_source, late_ledger) = paying_source(blob.clone())?;
    let lanes = StaticSources::new(vec![
        candidate(source, ledger, 0xA1),
        candidate(late_source, late_ledger, 0xB2),
    ])?;
    let holders = lanes.holders();
    let provider = LateEscrowBuild {
        lanes,
        late: Address::repeat_byte(0xB2),
    };
    let scratch = tempfile::tempdir()?;
    let streamer = Streamer::new(provider, funder(), scratch.path()).holders(holders);
    let (mut reader, mut drive) = streamer
        .open(
            root,
            total,
            &PullConfig::default(),
            Arc::new(NoCache),
            stop(),
        )
        .await?;
    let mut out = Vec::new();
    drive.alongside(reader.read_to_end(&mut out)).await?;
    anyhow::ensure!(out == blob, "every verified byte reaches the reader");
    let err = drive
        .finish()
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("finish must report the late escrow"))?;
    let tx = alloy::primitives::TxHash::repeat_byte(0xab).to_string();
    anyhow::ensure!(format!("{err:#}").contains(&tx), "the tx is named: {err:#}");
    Ok(())
}

/// A stream whose fetch ends cleanly finishes cleanly.
#[tokio::test]
async fn finish_after_a_clean_stream_is_ok() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let (source, ledger) = paying_source(blob.clone())?;
    let (root, total) = (source.root(), source.total_bytes());
    let scratch = tempfile::tempdir()?;
    let streamer = streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?;
    let (mut reader, mut drive) = streamer
        .open(
            root,
            total,
            &PullConfig::default(),
            Arc::new(NoCache),
            stop(),
        )
        .await?;
    let mut out = Vec::new();
    drive.alongside(reader.read_to_end(&mut out)).await?;
    anyhow::ensure!(out == blob, "every verified byte reaches the reader");
    drive.finish().await
}

/// The chunk-group-aligned span a window's legs may open and pay for (ADR
/// 038), as `(start, end)`.
fn aligned_span(offset: u64, len: u64, total: u64) -> anyhow::Result<(u64, u64)> {
    let aligned = align_range(offset, len, total)?;
    Ok((aligned.fetch_start(), aligned.fetch_end()))
}

/// A range stream hands out exactly the requested byte window, however the
/// window sits against the chunk-group grid.
#[tokio::test]
async fn a_range_stream_yields_exactly_the_window() -> anyhow::Result<()> {
    let blob = payload(3 * 1024 * 1024 + 777);
    let (source, ledger) = paying_source(blob.clone())?;
    let total = source.total_bytes();
    let (offset, len) = (1_000_001u64, 500_000u64);
    let got = drain_range(
        source,
        ledger,
        total,
        (offset, len),
        Arc::new(NoCache),
        &PullConfig::default(),
    )
    .await?;
    let want = blob
        .get(usize::try_from(offset)?..usize::try_from(offset + len)?)
        .context("window inside the blob")?;
    anyhow::ensure!(
        got.len() == want.len(),
        "{} of {} bytes",
        got.len(),
        want.len()
    );
    anyhow::ensure!(got == want, "the window must be byte-identical");
    Ok(())
}

/// A range stream opens, and pays for, only the chunk-group-aligned span
/// around its window: never the bytes before or after it.
#[tokio::test]
async fn a_range_stream_opens_only_the_aligned_span() -> anyhow::Result<()> {
    let blob = payload(3 * 1024 * 1024 + 777);
    let (source, ledger) = paying_source(blob.clone())?;
    let total = source.total_bytes();
    let probe = source.clone();
    let (offset, len) = (1_000_001u64, 500_000u64);
    drain_range(
        source,
        Arc::clone(&ledger),
        total,
        (offset, len),
        Arc::new(NoCache),
        &PullConfig::default(),
    )
    .await?;
    let (span_start, span_end) = aligned_span(offset, len, total)?;
    let opened = probe.opened_ranges();
    anyhow::ensure!(!opened.is_empty(), "the window was fetched");
    for &(start, fetch_len) in &opened {
        anyhow::ensure!(
            fetch_len > 0 && start >= span_start && start + fetch_len <= span_end,
            "leg ({start}, {fetch_len}) leaves the aligned span [{span_start}, {span_end})"
        );
    }
    // Payment counts wire bytes: the aligned span plus its bao proof, which
    // ADR 038 puts under one percent of the content.
    let paid = ledger.paid_bytes();
    let span = span_end - span_start;
    anyhow::ensure!(
        paid <= U256::from(span + span / 100) && paid < U256::from(blob.len() / 2),
        "paid {paid} bytes for a {len}-byte window of a {}-byte blob",
        blob.len()
    );
    Ok(())
}

/// A zero-length window is a tail: it streams from its offset to the size a
/// leg proves, past a claim that was too small.
#[tokio::test]
async fn a_tail_window_follows_the_proven_size() -> anyhow::Result<()> {
    let blob = payload(3 * 1024 * 1024 + 777);
    let (source, ledger) = paying_source(blob.clone())?;
    let offset = 2 * 1024 * 1024 + 5;
    let claim = 2 * 1024 * 1024 + 100;
    let got = drain_range(
        source,
        ledger,
        claim,
        (offset, 0),
        Arc::new(NoCache),
        &PullConfig::default(),
    )
    .await?;
    let want = blob
        .get(usize::try_from(offset)?..)
        .context("offset inside the blob")?;
    anyhow::ensure!(got == want, "{} of {} tail bytes", got.len(), want.len());
    Ok(())
}

/// A window that reaches past the blob's end streams to the proven size and
/// stops: no error, no hang.
#[tokio::test]
async fn a_window_past_the_end_stops_at_the_proven_size() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let (source, ledger) = paying_source(blob.clone())?;
    let total = source.total_bytes();
    let offset = total - 100;
    let got = drain_range(
        source,
        ledger,
        total,
        (offset, 10_000),
        Arc::new(NoCache),
        &PullConfig::default(),
    )
    .await?;
    let want = blob
        .get(usize::try_from(offset)?..)
        .context("offset inside the blob")?;
    anyhow::ensure!(got == want, "{} of {} bytes", got.len(), want.len());
    Ok(())
}

/// A window that starts past the size claim is refused at open: the caller
/// knows the size (a manifest entry) and answers its own "range not
/// satisfiable" before any fetch.
#[tokio::test]
async fn a_window_past_the_claim_is_refused_at_open() -> anyhow::Result<()> {
    let blob = payload(100_000);
    let (source, ledger) = paying_source(blob)?;
    let root = source.root();
    let total = source.total_bytes();
    let probe = source.clone();
    let scratch = tempfile::tempdir()?;
    let streamer = streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?;
    let opened = streamer
        .open_range(
            root,
            total,
            (total, 1),
            &PullConfig::default(),
            Arc::new(NoCache),
            stop(),
        )
        .await;
    anyhow::ensure!(
        opened.is_err(),
        "a window at or past the claim must not open"
    );
    anyhow::ensure!(
        probe.opened_ranges().is_empty(),
        "nothing is fetched for it"
    );
    Ok(())
}

/// A cache that holds the window serves it straight from the cache, without
/// opening the source: the stream asks the cache for exactly the window.
#[tokio::test]
async fn a_cached_window_is_served_without_fetching() -> anyhow::Result<()> {
    let blob = payload(600_000);
    let (source, ledger) = paying_source(blob.clone())?;
    let root = source.root();
    let total = source.total_bytes();
    let probe = source.clone();
    let (offset, len) = (123_456u64, 70_000u64);
    let window = blob
        .get(usize::try_from(offset)?..usize::try_from(offset + len)?)
        .context("window inside the blob")?;
    let cache = Arc::new(MemoryBlobCache::new());
    cache
        .put(root, offset, Bytes::copy_from_slice(window))
        .await?;
    let scratch = tempfile::tempdir()?;
    let streamer = streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?;
    let (mut reader, mut drive) = streamer
        .open_range(
            root,
            total,
            (offset, len),
            &PullConfig::default(),
            cache,
            stop(),
        )
        .await?;
    anyhow::ensure!(matches!(reader, VerifiedReader::Cached { .. }));
    let mut out = Vec::new();
    drive.alongside(reader.read_to_end(&mut out)).await?;
    let want = blob
        .get(usize::try_from(offset)?..usize::try_from(offset + len)?)
        .context("window inside the blob")?;
    anyhow::ensure!(out == want, "the cached window must be identical");
    anyhow::ensure!(
        probe.opened_ranges().is_empty(),
        "a cache hit opens no source"
    );
    Ok(())
}

/// A bounded window past a size claim too small for the blob is clipped to
/// the claim: the fetch takes nothing past the end of the blob as the caller
/// knows it, and the stream ends with an error naming the shortfall, never
/// following the blob past a reader that stopped.
#[tokio::test]
async fn a_window_past_a_short_claim_is_clipped_to_it() -> anyhow::Result<()> {
    let blob = payload(3 * 1024 * 1024 + 777);
    let (source, ledger) = paying_source(blob.clone())?;
    let claim = 2 * 1024 * 1024 + 100;
    let offset = 2 * 1024 * 1024 + 5;
    // The smallest read-ahead window, so a drive that outruns a stopped
    // reader parks on it instead of finishing within the window.
    let config = PullConfig {
        read_ahead_bytes: 1,
        ..PullConfig::default()
    };
    let probe = source.clone();
    let got = tokio::time::timeout(
        Duration::from_secs(20),
        drain_range(
            source,
            ledger,
            claim,
            (offset, 1_000),
            Arc::new(NoCache),
            &config,
        ),
    )
    .await
    .context("the stream and its drive end")?;
    let err = match got {
        Ok(bytes) => anyhow::bail!("{} bytes past the claim were streamed", bytes.len()),
        Err(err) => format!("{err:#}"),
    };
    anyhow::ensure!(err.contains("bytes readable"), "{err}");
    for &(start, fetch_len) in &probe.opened_ranges() {
        anyhow::ensure!(
            start + fetch_len <= claim,
            "leg ({start}, {fetch_len}) runs past the claim"
        );
    }
    Ok(())
}

/// A window needs a size: only offset `0` opens against a zero claim (the
/// empty blob, or a stream that learns the size from its first leg).
#[tokio::test]
async fn a_window_past_zero_needs_a_size_claim() -> anyhow::Result<()> {
    let blob = payload(100_000);
    let (source, ledger) = paying_source(blob)?;
    let root = source.root();
    let probe = source.clone();
    let scratch = tempfile::tempdir()?;
    let streamer = streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?;
    let opened = streamer
        .open_range(
            root,
            0,
            (4_096, 1_000),
            &PullConfig::default(),
            Arc::new(NoCache),
            stop(),
        )
        .await;
    anyhow::ensure!(opened.is_err(), "a deep window with no size must not open");
    anyhow::ensure!(
        probe.opened_ranges().is_empty(),
        "nothing is fetched for it"
    );
    Ok(())
}

/// A range stream never tees its bytes to the cache: a window is not the
/// whole blob a revisit would be served.
#[tokio::test]
async fn a_range_stream_is_never_teed_to_the_cache() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let (source, ledger) = paying_source(blob)?;
    let root = source.root();
    let total = source.total_bytes();
    let cache = Arc::new(MemoryBlobCache::new());
    drain_range(
        source,
        ledger,
        total,
        (4_096, 300_000),
        Arc::clone(&cache) as Arc<dyn BlobCache>,
        &PullConfig::default(),
    )
    .await?;
    anyhow::ensure!(
        cache.get(root, 0, total).await?.is_none(),
        "a window must not be cached as the blob"
    );
    Ok(())
}

/// The whole-blob window `(0, 0)` is the plain stream.
#[tokio::test]
async fn a_whole_blob_window_streams_the_whole_blob() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let (source, ledger) = paying_source(blob.clone())?;
    let total = source.total_bytes();
    let got = drain_range(
        source,
        ledger,
        total,
        (0, 0),
        Arc::new(NoCache),
        &PullConfig::default(),
    )
    .await?;
    anyhow::ensure!(got == blob, "the whole-blob window must be the blob");
    Ok(())
}

/// A window that ends exactly where the cap on held bytes puts the store's
/// bound is still a bounded window: the fetch takes it and stops, instead of
/// following the bound past the cap and failing.
#[tokio::test]
async fn a_window_ending_at_the_cap_stays_bounded() -> anyhow::Result<()> {
    let blob = payload(6 * 1024 * 1024 + 99);
    let (source, ledger) = paying_source(blob.clone())?;
    let root = source.root();
    let total = source.total_bytes();
    let probe = source.clone();
    let cap = 1024 * 1024;
    let (offset, len) = (2 * 1024 * 1024, cap);
    let scratch = tempfile::tempdir()?;
    let streamer =
        streamer(vec![candidate(source, ledger, 0xA1)], scratch.path())?.max_blob_bytes(cap);
    let (mut reader, mut drive) = streamer
        .open_range(
            root,
            total,
            (offset, len),
            &PullConfig::default(),
            Arc::new(NoCache),
            stop(),
        )
        .await?;
    let mut out = Vec::new();
    drive.alongside(reader.read_to_end(&mut out)).await?;
    drive.finish().await?;
    let want = blob
        .get(usize::try_from(offset)?..usize::try_from(offset + len)?)
        .context("window inside the blob")?;
    anyhow::ensure!(out == want, "{} of {} window bytes", out.len(), want.len());
    for &(start, fetch_len) in &probe.opened_ranges() {
        anyhow::ensure!(
            start + fetch_len <= offset + len,
            "leg ({start}, {fetch_len}) runs past the window's end"
        );
    }
    Ok(())
}

/// Prove the size of `root` under `claim`, one lane per source.
async fn prove(
    sources: Vec<ScriptedSource>,
    root: [u8; 32],
    claim: u64,
) -> anyhow::Result<Option<u64>> {
    let scratch = tempfile::tempdir()?;
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let candidates = sources
        .into_iter()
        .zip(0xA1u8..)
        .map(|(source, provider)| {
            candidate(
                source.paying(Arc::clone(&ledger)),
                Arc::clone(&ledger),
                provider,
            )
        })
        .collect();
    streamer(candidates, scratch.path())?
        .prove_size(root, claim, &PullConfig::default(), stop())
        .await
}

/// A true claim is proven by its last chunk group alone: the proof opens
/// nothing before the group that holds the final byte.
#[tokio::test]
async fn a_true_claim_is_proven_by_its_last_group() -> anyhow::Result<()> {
    let source = ScriptedSource::new(payload(3 * 1024 * 1024 + 777))?;
    let (root, total, probe) = (source.root(), source.total_bytes(), source.clone());
    let proven = prove(vec![source], root, total).await?;
    anyhow::ensure!(proven == Some(total), "proved {proven:?} of {total}");
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let last_group = (total - 1) / group * group;
    let opened = probe.opened_ranges();
    anyhow::ensure!(!opened.is_empty(), "the proof opens a leg");
    for &(start, len) in &opened {
        anyhow::ensure!(
            start >= last_group && start + len <= total,
            "leg ({start}, {len}) is not the last group of {total}"
        );
    }
    Ok(())
}

/// A claim below the true size proves nothing: the last byte under it is not
/// the blob's final chunk, so no size is given.
#[tokio::test]
async fn a_low_claim_proves_nothing() -> anyhow::Result<()> {
    let source = ScriptedSource::new(payload(3 * 1024 * 1024 + 777))?;
    let root = source.root();
    let proven = prove(vec![source], root, 1024 * 1024).await?;
    anyhow::ensure!(proven.is_none(), "a low claim proved {proven:?}");
    Ok(())
}

/// A claim past the true size gives no size: every holder declines its last
/// byte, as a node declines a range past the end, and the proof ends at once
/// with the decline.
#[tokio::test]
async fn a_high_claim_gives_no_size() -> anyhow::Result<()> {
    let source = ScriptedSource::new(payload(100_000))?.refusing_opens_from(0, || {
        anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::Declined))
    });
    let root = source.root();
    let proven = tokio::time::timeout(
        Duration::from_secs(20),
        prove(vec![source], root, 4 * 1024 * 1024),
    )
    .await
    .context("the proof ends")?;
    let err = match proven {
        Ok(proven) => anyhow::bail!("a high claim proved {proven:?}"),
        Err(err) => err,
    };
    anyhow::ensure!(
        err.downcast_ref::<crate::NoNodeWillServe>().is_some(),
        "{err:#}"
    );
    Ok(())
}

/// A node that signs a wrong size fails to verify the final chunk, and the
/// proof takes the true size from an honest node. The liar is asked, and
/// its claim never becomes the size.
#[tokio::test]
async fn a_lying_source_is_passed_over_for_an_honest_one() -> anyhow::Result<()> {
    let blob = payload(600_000);
    let honest = ScriptedSource::new(blob.clone())?;
    let (root, total) = (honest.root(), honest.total_bytes());
    let liar = ScriptedSource::new(blob)?.signing_size(total - 1);
    let (asked_honest, asked_liar) = (honest.clone(), liar.clone());
    let proven = prove(vec![liar, honest], root, total).await?;
    anyhow::ensure!(proven == Some(total), "proved {proven:?} of {total}");
    anyhow::ensure!(
        !asked_liar.opened_ranges().is_empty() && !asked_honest.opened_ranges().is_empty(),
        "liar {:?}, honest {:?}",
        asked_liar.opened_ranges(),
        asked_honest.opened_ranges()
    );
    Ok(())
}

/// A claim off by less than one chunk group, either way, proves the blob's
/// own size, never the claim: the last group under it is the blob's last.
#[tokio::test]
async fn a_claim_within_the_last_group_proves_the_true_size() -> anyhow::Result<()> {
    for offset in [-100i64, 100] {
        let source = ScriptedSource::new(payload(600_000))?;
        let (root, total) = (source.root(), source.total_bytes());
        let claim = total.checked_add_signed(offset).context("a claim")?;
        let proven = prove(vec![source], root, claim).await?;
        anyhow::ensure!(
            proven == Some(total),
            "claim {claim} proved {proven:?} of {total}"
        );
    }
    Ok(())
}

/// A claim past the cap on held bytes proves nothing and opens nothing: a
/// blob that large is not held whole, and its claim never sizes the work.
#[tokio::test]
async fn a_claim_past_the_cap_opens_nothing() -> anyhow::Result<()> {
    let source = ScriptedSource::new(payload(600_000))?;
    let (root, total, probe) = (source.root(), source.total_bytes(), source.clone());
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let scratch = tempfile::tempdir()?;
    let proven = streamer(
        vec![candidate(source.paying(Arc::clone(&ledger)), ledger, 0xA1)],
        scratch.path(),
    )?
    .max_blob_bytes(total - 1)
    .prove_size(root, total, &PullConfig::default(), stop())
    .await?;
    anyhow::ensure!(proven.is_none() && probe.opened_ranges().is_empty());
    Ok(())
}

/// A zero claim proves nothing and opens nothing.
#[tokio::test]
async fn a_zero_claim_opens_nothing() -> anyhow::Result<()> {
    let source = ScriptedSource::new(payload(100_000))?;
    let (root, probe) = (source.root(), source.clone());
    let proven = prove(vec![source], root, 0).await?;
    anyhow::ensure!(proven.is_none() && probe.opened_ranges().is_empty());
    Ok(())
}
