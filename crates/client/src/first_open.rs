//! The first open of a blob: learn a size one source signs before
//! [`crate::acquire`] runs, with the same recovery as [`crate::acquire`].
//!
//! A caller with no size hint for a blob opens one source's pull for its
//! header, and the size it signs is the fetch's first claim: a hint the fetch
//! grows or shrinks as verified bytes land. That open is a delivery
//! like any other: a source that faults cools and another is tried, a lane
//! build that fails backs off, a pull-through target that keeps saying it
//! lacks the blob is marked absent ([`crate::Holder::probed_holder`]), and
//! discovery runs when nothing can start. The open ends on a
//! header, a fatal fault, a unanimous verdict of the sources, or the stop
//! policy. The lane it builds stays cached in the [`SourceSet`].

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use alloy::primitives::{Address, U256};
use tokio::time::Instant;

use crate::credential::{
    CapabilityCause, CredentialSlot, CredentialView, FundingNeeded, QuoteMax, unix_now,
};
use crate::fault::{Fault, classify};
use crate::recovery::{RecoveryGate, SwapStep, after_step};
use crate::scheduler::{Connecting, connect_future, key_spend, sleep_until_opt};
use crate::source::{Funder, SourceFuture};
use crate::source_set::{BUILD_RETRY_BASE, Holder, NoAffordableSource, SourceProvider, SourceSet};
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
/// transient `open` error holds that source off for the first lane-build
/// retry wait (`BUILD_RETRY_BASE`).
/// When no source can start, discovery runs, with backoff, beside the open: a
/// source whose cooldown ends while a discovery is in flight is tried at once.
/// A set that starts with no holder at all discovers until one appears. An
/// answer counts as progress: it clears the source's absent mark and ticks
/// the stop policy's clock.
///
/// A source refusing the open for the deposit (`Unfunded`) is parked until
/// the deposit rises. When every source is parked or cannot serve and a fresh
/// discovery finds nothing new, the open reaches the fetch's one funding
/// recovery point: it runs a step through `funder` under the fetch's `gate`,
/// the same step [`crate::acquire`] runs, and asks the parked sources again
/// at the raised deposit, or at the same deposit after a step that settles.
/// A delegated fetch passes its `credentials`: the step is then the bounded
/// wait for a swapped credential, and the open ends with a typed
/// [`crate::FundingNeeded`], as [`crate::acquire`] does.
///
/// # Errors
///
/// - a fatal fault ([`crate::Fault::Fatal`]) `open` returned, verbatim;
/// - a fatal lane build, wrapped in [`crate::LaneBuildFault`]: one that may
///   have escrowed USDC no record credits, which a retry would escrow again;
/// - [`crate::NoAffordableSource`] or [`crate::NoSourceHasBlob`] on a
///   unanimous verdict of the sources, or [`crate::PoolReplaced`] when the
///   recovery step opened a new pool;
/// - [`crate::GaveUp`] once the stop policy's limit passes without an answer.
pub async fn first_open<P, F, T, O, Fut>(
    sources: &mut SourceSet<'_, P>,
    stop: &StopPolicy,
    funder: &F,
    gate: &RecoveryGate,
    credentials: Option<&CredentialSlot>,
    open: O,
) -> anyhow::Result<(Address, T)>
where
    P: SourceProvider,
    F: Funder,
    O: Fn(Arc<StreamCandidate<P::Source>>) -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let opened = open_loop(sources, funder, gate, credentials, &open);
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

/// Await `fut`, or never resolve without one. The future stays in its slot, so
/// a `select!` branch that loses leaves it to be polled again.
async fn poll_some<F: Future + Unpin>(fut: Option<&mut F>) -> F::Output {
    match fut {
        Some(fut) => fut.await,
        None => std::future::pending().await,
    }
}

/// The body of [`first_open`], without the stop.
///
/// One attempt runs at a time: a lane build, then the `open` on that lane.
/// Discovery runs beside it in the same `select!`, so a slow discovery never
/// holds back a source whose cooldown ended.
async fn open_loop<'p, P, F, T, O, Fut>(
    sources: &mut SourceSet<'p, P>,
    funder: &F,
    gate: &RecoveryGate,
    credentials: Option<&CredentialSlot>,
    open: &O,
) -> anyhow::Result<(Address, T)>
where
    P: SourceProvider,
    F: Funder,
    O: Fn(Arc<StreamCandidate<P::Source>>) -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    // The pool deposit the built lanes report, ZERO before any lane exists.
    let mut deposit = U256::ZERO;
    // The recovery steps that topped up or settled, as this open last saw
    // them ([`RecoveryGate::step`]).
    let mut top_ups_seen = gate.top_ups();
    let mut delegate = Delegate::new(credentials);
    // Sources a transient `open` error holds off, until the instant given.
    let mut held_off: HashMap<Address, Instant> = HashMap::new();
    let mut connecting: Option<Connecting<'p, P::Source>> = None;
    let mut opening: Option<(Address, Pin<Box<Fut>>)> = None;
    let mut discovering: Option<SourceFuture<'p, Vec<Holder>>> = None;
    loop {
        let now = Instant::now();
        held_off.retain(|_, until| *until > now);
        if connecting.is_none() && opening.is_none() {
            let busy: HashSet<Address> = held_off.keys().copied().collect();
            if let Some(holder) = sources.next_to_start(now, deposit, &busy) {
                match sources.cached_lane(holder.provider) {
                    Some(lane) => {
                        deposit = deposit.max(lane_deposit(&lane));
                        opening = Some((holder.provider, Box::pin(open(lane))));
                    }
                    None => connecting = Some(connect_future(sources.provider(), holder)),
                }
            }
        }
        delegate.follow_swap(sources);
        let attempting = connecting.is_some() || opening.is_some();
        if !attempting
            && discovering.is_none()
            && let Some(err) = sources.exhausted(deposit, false)
        {
            if err.downcast_ref::<NoAffordableSource>().is_none() {
                return Err(err);
            }
            // A delegated fetch waits for a swapped credential, as
            // [`crate::acquire`] does.
            if let Some(err) = delegate.step(sources, gate, err).await? {
                deposit =
                    owner_step(sources, funder, gate, deposit, &mut top_ups_seen, err).await?;
            }
            continue;
        }
        if discovering.is_none()
            && sources.wants_discovery(now, deposit, usize::from(attempting), false)
        {
            discovering = Some(sources.provider().discover(sources.hash()));
        }
        let wake = sources
            .next_wake(now)
            .into_iter()
            .chain(held_off.values().copied())
            .min();

        tokio::select! {
            biased;
            (provider, built) = poll_some(connecting.as_mut()), if connecting.is_some() => {
                connecting = None;
                match sources.lane_built(provider, built, Instant::now()) {
                    Ok(lane) => {
                        deposit = deposit.max(lane_deposit(&lane));
                        opening = Some((provider, Box::pin(open(lane))));
                    }
                    // A build fault is chain-side and retries with backoff,
                    // unless it may have escrowed USDC no record credits: a
                    // retry escrows again.
                    Err(err) => {
                        if let Fault::Fatal(_) = classify(&err) {
                            return Err(err);
                        }
                        tracing::debug!(%provider, error = %decdn_common::redact::sanitize_err_chain(&err), "lane build failed");
                    }
                }
            }
            answer = poll_some(opening.as_mut().map(|(_, fut)| fut)), if opening.is_some() => {
                let Some((provider, _)) = opening.take() else {
                    continue;
                };
                match answer {
                    Ok(answer) => {
                        sources.record_progress(provider);
                        return Ok((provider, answer));
                    }
                    Err(err) => {
                        let at = Instant::now();
                        delegate.saw(&err);
                        if gate.settling(at) && sources.hold_while_settling(provider, &err, at) {
                            continue;
                        }
                        match sources.record_fault(provider, &err, None, at, deposit) {
                            Fault::Fatal(_) => return Err(err),
                            Fault::Transient => {
                                held_off.insert(provider, at + BUILD_RETRY_BASE);
                            }
                            Fault::Source | Fault::Unaffordable => {}
                        }
                    }
                }
            }
            found = poll_some(discovering.as_mut()), if discovering.is_some() => {
                discovering = None;
                sources.discovery_done(found, Instant::now(), deposit);
            }
            () = sleep_until_opt(wake) => {}
        }
    }
}

/// The first open's recovery step for a pool owner at the exhausted set
/// `exhausted`: a top-up, a replacement, or a settle, through `funder` under
/// `gate`. `top_ups_seen` is the open's count of the gate's top-ups, which
/// the step brings up to date. Returns the deposit the open asks the sources
/// again at.
///
/// # Errors
///
/// Every end [`after_step`] names.
async fn owner_step<P, F>(
    sources: &SourceSet<'_, P>,
    funder: &F,
    gate: &RecoveryGate,
    deposit: U256,
    top_ups_seen: &mut u64,
    exhausted: anyhow::Error,
) -> anyhow::Result<U256>
where
    P: SourceProvider,
    F: Funder,
{
    let spent = sources
        .lanes_spent()
        .max(funder.pool_spent().unwrap_or(U256::ZERO));
    // The deposit now, as the lane contexts hold it: a sibling bundle entry's
    // step reaches them through the run's lane registry.
    let current = || sources.lanes_deposit();
    // A sibling entry may have priced a shared source out at a deposit above
    // this open's view of it.
    let seen = sources.priced_out_at(deposit);
    let raised = after_step(
        gate.step(funder, seen, top_ups_seen, current, spent).await,
        exhausted,
    )?;
    if raised > deposit {
        sources.credit_lanes(raised);
    }
    if raised <= seen {
        // A settling step: ask the priced-out sources again.
        sources.health().clear_unaffordable();
    }
    Ok(raised.max(deposit))
}

/// A first open's delegated credential: the slot, the generation its built
/// lanes sign under, and whether a node refused the capability itself since.
struct Delegate<'a> {
    slot: Option<&'a CredentialSlot>,
    generation_seen: u64,
    capability_refused: bool,
}

impl<'a> Delegate<'a> {
    fn new(slot: Option<&'a CredentialSlot>) -> Self {
        Self {
            slot,
            generation_seen: slot.map_or(0, CredentialSlot::generation),
            capability_refused: false,
        }
    }

    /// Note a source's open error.
    fn saw(&mut self, err: &anyhow::Error) {
        if crate::fault::refuses_capability(err) {
            self.capability_refused = true;
        }
    }

    /// After a swap, drop the built lanes on the old key. What the sources
    /// refused under it says nothing about the new one.
    fn follow_swap<P: SourceProvider>(&mut self, sources: &mut SourceSet<'_, P>) {
        let Some(slot) = self.slot else { return };
        if slot.generation() == self.generation_seen {
            return;
        }
        self.generation_seen = slot.generation();
        sources.drop_lanes(|lane| slot.is_stale(&lane.ctx));
        sources.health().clear_unaffordable();
        self.capability_refused = false;
    }

    /// The recovery step at the exhausted set `exhausted` for a delegate: the
    /// bounded wait for a swapped credential. `Ok(None)` after a swap. A
    /// fetch with no slot gets `exhausted` back, for the owner's step.
    ///
    /// # Errors
    ///
    /// [`FundingNeeded::PublisherPool`] when the capability still covers the
    /// work, and [`FundingNeeded::NewCapability`] when no swap comes.
    async fn step<P: SourceProvider>(
        &self,
        sources: &SourceSet<'_, P>,
        gate: &RecoveryGate,
        exhausted: anyhow::Error,
    ) -> anyhow::Result<Option<anyhow::Error>> {
        let Some(slot) = self.slot else {
            return Ok(Some(exhausted));
        };
        let view = CredentialView::of(
            &slot.current(),
            key_spend(slot, &sources.built_lanes(), None),
            0,
            &QuoteMax::default(),
            unix_now(),
        );
        slot.report(view.event());
        // No size is known yet, so no work is priced: a cap with nothing left
        // is spent.
        let cause = view
            .cause()
            .or(view.headroom.is_zero().then_some(CapabilityCause::CapSpent))
            .or(self.capability_refused.then_some(CapabilityCause::Revoked));
        let pool = slot.pool_id();
        let Some(cause) = cause else {
            return Err(exhausted.context(FundingNeeded::PublisherPool { pool }));
        };
        match gate.swap_step(slot, self.generation_seen).await {
            SwapStep::Swapped => Ok(None),
            SwapStep::NoProgress | SwapStep::TimedOut => {
                Err(exhausted.context(FundingNeeded::NewCapability { pool, cause }))
            }
        }
    }
}

/// The pool deposit `lane`'s context reports.
fn lane_deposit<S>(lane: &StreamCandidate<S>) -> U256 {
    lane.ctx.lock().map_or(U256::ZERO, |ctx| ctx.deposit)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
