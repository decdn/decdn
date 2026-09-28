//! The first open of a blob: learn what only a source can sign (the blob's
//! size) before [`crate::acquire`] runs, with the same recovery as
//! [`crate::acquire`].
//!
//! A caller that does not know a blob's signed size opens one source's pull
//! for its header before it can key the ranged store. That open is a delivery
//! like any other: a source that faults cools and another is tried, a lane
//! build that fails backs off, a source that says it lacks the blob is marked
//! absent, and discovery runs when nothing can start. The open ends on a
//! header, a fatal fault, a unanimous verdict of the sources, or the stop
//! policy. The lane it builds stays cached in the [`SourceSet`].

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use alloy::primitives::{Address, U256};
use tokio::time::Instant;

use crate::fault::Fault;
use crate::source_set::{BUILD_RETRY_BASE, SourceProvider, SourceSet};
use crate::stop::StopPolicy;
use crate::streamer::StreamCandidate;

/// Run `open` on one source of `sources` at a time, nearest first, until one
/// answers, and return that source's provider and answer.
///
/// Each source's lane is built through [`SourceProvider::connect`] on first
/// use and stays cached in `sources`. A build error is chain-side: it backs
/// off and never blames the source. An `open` error is classified
/// ([`crate::classify`]) and recorded on the source: a fatal fault ends the
/// open with that error, any other fault moves on to the next source. A
/// transient `open` error holds that source off for [`BUILD_RETRY_BASE`].
/// When no source can start, the open runs discovery or sleeps until a source
/// cools down. An answer counts as progress: it clears the source's absent
/// mark and ticks the stop policy's clock.
///
/// # Errors
///
/// - a fatal fault ([`crate::Fault::Fatal`]) `open` returned, verbatim;
/// - [`crate::NoAffordableSource`], [`crate::NoSourceAgreesOnSize`] or
///   [`crate::NoSourceHasBlob`] on a unanimous verdict of the sources;
/// - [`crate::GaveUp`] once the stop policy's limit passes without an answer.
pub async fn first_open<P, T, O, Fut>(
    sources: &mut SourceSet<'_, P>,
    stop: &StopPolicy,
    open: O,
) -> anyhow::Result<(Address, T)>
where
    P: SourceProvider,
    O: Fn(Arc<StreamCandidate<P::Source>>) -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let opened = open_loop(sources, &open);
    tokio::select! {
        biased;
        opened = opened => {
            if opened.is_ok() {
                stop.clock.tick();
            }
            opened
        }
        gave_up = stop.expired() => Err(anyhow::Error::new(gave_up)),
    }
}

/// The body of [`first_open`], without the stop.
async fn open_loop<P, T, O, Fut>(
    sources: &mut SourceSet<'_, P>,
    open: &O,
) -> anyhow::Result<(Address, T)>
where
    P: SourceProvider,
    O: Fn(Arc<StreamCandidate<P::Source>>) -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    // The pool deposit the built lanes report, ZERO before any lane exists.
    let mut deposit = U256::ZERO;
    // Sources a transient `open` error holds off, until the instant given.
    let mut held_off: HashMap<Address, Instant> = HashMap::new();
    loop {
        let now = Instant::now();
        held_off.retain(|_, until| *until > now);
        let busy: HashSet<Address> = held_off.keys().copied().collect();
        if let Some(holder) = sources.next_to_start(now, deposit, &busy) {
            let provider = holder.provider;
            let lane = if let Some(lane) = sources.cached_lane(provider) {
                lane
            } else {
                let built = sources.provider().connect(&holder).await;
                match sources.lane_built(provider, built, Instant::now()) {
                    Ok(lane) => lane,
                    Err(err) => {
                        tracing::debug!(%provider, "lane build failed: {err:#}");
                        continue;
                    }
                }
            };
            if let Ok(ctx) = lane.ctx.lock() {
                deposit = deposit.max(ctx.deposit);
            }
            match open(lane).await {
                Ok(answer) => {
                    sources.record_progress(provider);
                    return Ok((provider, answer));
                }
                Err(err) => match sources.record_fault(provider, &err, Instant::now(), deposit) {
                    Fault::Fatal(_) => return Err(err),
                    Fault::Transient => {
                        held_off.insert(provider, Instant::now() + BUILD_RETRY_BASE);
                    }
                    Fault::Source | Fault::Unaffordable | Fault::WrongSize => {}
                },
            }
            continue;
        }
        if let Some(err) = sources.exhausted(deposit, false) {
            return Err(err);
        }
        if sources.wants_discovery(now, deposit, 0, false) {
            let found = sources.provider().discover(sources.hash()).await;
            sources.discovery_done(found, Instant::now(), deposit);
            continue;
        }
        let wake = sources
            .next_wake(now)
            .into_iter()
            .chain(held_off.values().copied())
            .min();
        match wake {
            Some(at) => tokio::time::sleep_until(at).await,
            None => std::future::pending().await,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
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
        Cumulative, GaveUp, LaneLease, NoSourceHasBlob, PoolLedger, UpstreamRefused,
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
            first_unit: None,
            lease: LaneLease::new(()),
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

    #[tokio::test(start_paused = true)]
    async fn a_fatal_fault_ends_the_open() -> anyhow::Result<()> {
        let sources = StaticSources::new(vec![lane(0xA1), lane(0xB2)])?;
        let mut set = SourceSet::new(&sources, [0; 32], Arc::default(), sources.holders());
        let err = first_open(&mut set, &policy(None), |_lane| async {
            Err::<(), _>(anyhow::Error::new(UpstreamVoucherRejected {
                reason: VoucherRejectReason::AmountRegression,
                bundle: None,
                proof_generation: None,
            }))
        })
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a fatal fault must end the open"))?;
        assert!(err.downcast_ref::<UpstreamVoucherRejected>().is_some());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn every_source_saying_not_found_ends_with_no_source_has_blob() -> anyhow::Result<()> {
        let sources = StaticSources::new(vec![lane(0xA1), lane(0xB2)])?;
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
}
