use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use decdn_protocol::client::{StreamError, VoucherRejectReason};

use super::first_open;
use crate::source::{ScriptedSource, ctx_with};
use crate::source_set::{SourceSet, StaticSources};
use crate::stop::{ProgressClock, StopPolicy};
use crate::streamer::StreamCandidate;
use crate::{
    Cumulative, GaveUp, HealExhausted, LaneLease, NoSourceHasBlob, PoolLedger, UpstreamRefused,
    UpstreamVoucherRejected,
};

const A: Address = Address::repeat_byte(0xA1);
const B: Address = Address::repeat_byte(0xB2);

fn lane(provider: u8) -> StreamCandidate<ScriptedSource> {
    StreamCandidate {
        source: ScriptedSource::new(vec![7u8; 1024]).unwrap(),
        ctx: Arc::new(Mutex::new(ctx_with(provider, U256::from(1_000u32)))),
        ledger: Arc::new(PoolLedger::new(Cumulative::default())),
        coverage: None,
        lease: LaneLease::new(()),
        widen: None,
    }
}

fn provider_of(lane: &StreamCandidate<ScriptedSource>) -> Address {
    lane.ctx.lock().unwrap().provider
}

fn not_found() -> anyhow::Error {
    anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
}

fn policy(limit: Option<Duration>) -> StopPolicy {
    StopPolicy::new(false, limit, Arc::new(ProgressClock::new()))
}

/// Static lanes behind a scripted discovery: each discovery returns the
/// next scripted answer, or never returns once the script runs out.
struct ScriptedDiscovery {
    lanes: StaticSources<ScriptedSource>,
    answers: Mutex<std::collections::VecDeque<Vec<crate::Holder>>>,
    discoveries: AtomicU32,
}

impl crate::SourceProvider for ScriptedDiscovery {
    type Source = ScriptedSource;

    fn discover(&self, hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
        let _ = hash;
        self.discoveries.fetch_add(1, Ordering::SeqCst);
        let next = self.answers.lock().unwrap().pop_front();
        Box::pin(async move {
            match next {
                Some(found) => Ok(found),
                None => std::future::pending().await,
            }
        })
    }

    fn connect<'a>(
        &'a self,
        holder: &'a crate::Holder,
    ) -> crate::SourceFuture<'a, StreamCandidate<ScriptedSource>> {
        self.lanes.connect(holder)
    }
}

/// A fetch that starts with no holder at all discovers until one appears.
#[tokio::test(start_paused = true)]
async fn an_empty_set_discovers_until_a_holder_appears() -> anyhow::Result<()> {
    let lanes = StaticSources::new(vec![lane(0xA1)])?;
    let found = lanes.holders();
    let provider = ScriptedDiscovery {
        lanes,
        answers: Mutex::new(vec![Vec::new(), found].into()),
        discoveries: AtomicU32::new(0),
    };
    let mut set = SourceSet::new(&provider, [0; 32], Arc::default(), Vec::new());
    let (answered, ()) = first_open(
        &mut set,
        &policy(Some(Duration::from_mins(10))),
        |_| async { Ok(()) },
    )
    .await?;
    assert_eq!(answered, A);
    assert_eq!(provider.discoveries.load(Ordering::SeqCst), 2);
    Ok(())
}

/// A slow discovery does not hold the open up: a source that cools while
/// it runs is tried again once its cooldown ends.
#[tokio::test(start_paused = true)]
async fn a_cooled_source_answers_while_discovery_hangs() -> anyhow::Result<()> {
    let lanes = StaticSources::new(vec![lane(0xA1)])?;
    let holders = lanes.holders();
    let provider = ScriptedDiscovery {
        lanes,
        answers: Mutex::new(std::collections::VecDeque::new()),
        discoveries: AtomicU32::new(0),
    };
    let mut set = SourceSet::new(&provider, [0; 32], Arc::default(), holders);
    let calls = AtomicU32::new(0);
    let (answered, ()) = first_open(&mut set, &policy(Some(Duration::from_mins(1))), |_| {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        async move {
            if call == 0 {
                anyhow::bail!("connection reset");
            }
            Ok(())
        }
    })
    .await?;
    assert_eq!(answered, A);
    assert_eq!(provider.discoveries.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_source_that_faults_once_answers_after_it_cools() -> anyhow::Result<()> {
    let sources = StaticSources::new(vec![lane(0xA1)])?;
    let mut set = SourceSet::new(&sources, [0; 32], Arc::default(), sources.holders());
    let calls = AtomicU32::new(0);
    let (provider, size) = first_open(&mut set, &policy(None), |_lane| {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        async move {
            if call == 0 {
                anyhow::bail!("connection reset");
            }
            Ok(1024u64)
        }
    })
    .await?;
    assert_eq!((provider, size), (A, 1024));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(set.cached_lane(A).is_some(), "the lane stays cached");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_faulting_source_moves_the_open_to_the_next_one() -> anyhow::Result<()> {
    let sources = StaticSources::new(vec![lane(0xA1), lane(0xB2)])?;
    let mut set = SourceSet::new(&sources, [0; 32], Arc::default(), sources.holders());
    let (provider, ()) = first_open(&mut set, &policy(None), |lane| async move {
        if provider_of(&lane) == A {
            anyhow::bail!("connection reset");
        }
        Ok(())
    })
    .await?;
    assert_eq!(provider, B);
    Ok(())
}

fn rejected(reason: VoucherRejectReason) -> anyhow::Error {
    anyhow::Error::new(UpstreamVoucherRejected {
        reason,
        bundle: None,
        proof_generation: None,
    })
}

/// A rejection that no heal took declines its source only. Once every
/// source declined, the open ends with "no node will serve this", naming
/// the reason (ADR 039 §Failure handling).
#[tokio::test(start_paused = true)]
async fn every_source_declining_ends_the_open() -> anyhow::Result<()> {
    let reason = VoucherRejectReason::AmountRegression;
    let sources = StaticSources::new(vec![lane(0xA1), lane(0xB2)])?;
    let mut set = SourceSet::new(&sources, [0; 32], Arc::default(), sources.holders());
    let err = first_open(&mut set, &policy(None), |_lane| async move {
        Err::<(), _>(rejected(reason))
    })
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("{reason:?} from every source must end the open"))?;
    let stop = err
        .downcast_ref::<crate::NoNodeWillServe>()
        .ok_or_else(|| anyhow::anyhow!("expected NoNodeWillServe, got {err:#}"))?;
    assert_eq!(
        stop.reasons,
        vec![crate::source_set::DeclineReason::Voucher(reason)]
    );
    Ok(())
}

/// A rejection that healed the lane ledger past the resume budget cools
/// only its source, so the open moves to the next one.
#[tokio::test(start_paused = true)]
async fn a_rejection_healed_past_the_budget_moves_the_open() -> anyhow::Result<()> {
    let sources = StaticSources::new(vec![lane(0xA1), lane(0xB2)])?;
    let mut set = SourceSet::new(&sources, [0; 32], Arc::default(), sources.holders());
    let (provider, ()) = first_open(&mut set, &policy(None), |lane| async move {
        if provider_of(&lane) == A {
            return Err(rejected(VoucherRejectReason::UnderFold).context(HealExhausted));
        }
        Ok(())
    })
    .await?;
    assert_eq!(provider, B);
    Ok(())
}

/// Pull-through targets (not probed holders) that each say `NotFound`
/// often enough are absent, and the open ends.
#[tokio::test(start_paused = true)]
async fn every_source_saying_not_found_ends_with_no_source_has_blob() -> anyhow::Result<()> {
    let sources = StaticSources::new(vec![lane(0xA1), lane(0xB2)])?.not_probed();
    let mut set = SourceSet::new(&sources, [0; 32], Arc::default(), sources.holders());
    let err = first_open(&mut set, &policy(None), |_lane| async {
        Err::<(), _>(not_found())
    })
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("an absent blob must end the open"))?;
    assert!(err.downcast_ref::<NoSourceHasBlob>().is_some(), "{err:#}");
    Ok(())
}

/// A sole probed holder that says `NotFound` twice (a load shed, say) and
/// then answers is waited for, not marked absent.
#[tokio::test(start_paused = true)]
async fn a_probed_holder_saying_not_found_twice_then_answers() -> anyhow::Result<()> {
    let sources = StaticSources::new(vec![lane(0xA1)])?;
    let mut set = SourceSet::new(&sources, [0; 32], Arc::default(), sources.holders());
    let calls = AtomicU32::new(0);
    let (provider, ()) = first_open(&mut set, &policy(None), |_lane| {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        async move {
            if call < 2 {
                return Err(not_found());
            }
            Ok(())
        }
    })
    .await?;
    assert_eq!(provider, A);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn it_gives_up_after_the_limit_without_an_answer() -> anyhow::Result<()> {
    let sources = StaticSources::new(vec![lane(0xA1)])?;
    let mut set = SourceSet::new(&sources, [0; 32], Arc::default(), sources.holders());
    let limit = Duration::from_mins(3);
    let start = tokio::time::Instant::now();
    let err = first_open(&mut set, &policy(Some(limit)), |_lane| async {
        Err::<(), _>(anyhow::anyhow!("connection reset"))
    })
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("a source that never answers must give up"))?;
    assert_eq!(err.downcast_ref::<GaveUp>(), Some(&GaveUp { idle: limit }));
    assert_eq!(tokio::time::Instant::now() - start, limit);
    Ok(())
}
