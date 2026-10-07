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

use crate::fault::{Fault, classify};
use crate::scheduler::{Connecting, connect_future, sleep_until_opt};
use crate::source::SourceFuture;
use crate::source_set::{BUILD_RETRY_BASE, Holder, SourceProvider, SourceSet};
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
/// The open runs no reactive top-up. A header-only open pays nothing, so a
/// source refusing it for the deposit (`Unfunded`) is parked until
/// the deposit rises. The pool is funded where the caller's `connect` builds
/// the lane (the CLI's `open_or_reuse_pool` refills it below its low-water
/// mark), and by the [`crate::acquire`] that follows.
///
/// # Errors
///
/// - a fatal fault ([`crate::Fault::Fatal`]) `open` returned, verbatim;
/// - a fatal lane build, wrapped in [`crate::LaneBuildFault`]: one that may
///   have escrowed USDC no record credits, which a retry would escrow again;
/// - [`crate::NoAffordableSource`] or [`crate::NoSourceHasBlob`] on a
///   unanimous verdict of the sources;
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
async fn open_loop<'p, P, T, O, Fut>(
    sources: &mut SourceSet<'p, P>,
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
        let attempting = connecting.is_some() || opening.is_some();
        if !attempting
            && discovering.is_none()
            && let Some(err) = sources.exhausted(deposit, false, false)
        {
            return Err(err);
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
                    Err(err) => match sources.record_fault(provider, &err, None, Instant::now(), deposit) {
                        Fault::Fatal(_) => return Err(err),
                        Fault::Transient => {
                            held_off.insert(provider, Instant::now() + BUILD_RETRY_BASE);
                        }
                        Fault::Source | Fault::Unaffordable => {}
                    },
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

/// The pool deposit `lane`'s context reports.
fn lane_deposit<S>(lane: &StreamCandidate<S>) -> U256 {
    lane.ctx.lock().map_or(U256::ZERO, |ctx| ctx.deposit)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
