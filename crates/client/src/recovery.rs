//! The funding recovery step at the exhausted candidate set (ADR 003 § Funding
//! recovery, ADR 039 § Failure handling).
//!
//! A buyer adds funds only here. The acquire loop and the first open reach the
//! step when the candidate set is exhausted, a fresh discovery found nothing
//! new, and at least one candidate refused `Unfunded` or is priced out at the
//! current deposit. The step runs through the caller's [`Funder`], under the
//! fetch's [`RecoveryGate`]: the first step of a fetch is always allowed, and a
//! further step only after at least one new BLAKE3-verified byte. A caller that
//! reaches the gate after a sibling's step answered its exhausted set makes one
//! more pass on that step, and takes none of its own. A node cannot
//! fake a verified byte, so a node that lies with `Unfunded` costs the buyer at
//! most one step.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use alloy::primitives::U256;
use tokio::time::Instant;

use crate::source::{Funder, PoolReplaced, Recovery};

/// How long after a funding recovery step's top-up a source's `Unfunded`
/// refusal is read as its stale view of the deposit rather than a refusal at
/// the new deposit.
///
/// A serving node's chain watcher can lag the `topUp` it has to observe, so
/// its pre-serve deposit gate still sees the old deposit and refuses the
/// re-open. Retrying an open is money-safe: it sends no voucher. 15 s sits
/// comfortably above the daemon's chain-event poll cadence.
pub const RECOVERY_SETTLE: Duration = Duration::from_secs(15);

/// How long a source that refuses during the settle window waits before it is
/// asked again.
pub(crate) const SETTLE_STEP: Duration = Duration::from_millis(500);

/// One fetch's funding recovery state: the progress rule, the step that
/// replaced the pool, and the settle window after a top-up.
///
/// One gate spans one fetch. A bundle pull is one fetch for every entry, so
/// its entries share one gate; a command that runs its remaining work again
/// against a replaced pool keeps the gate for that pass too. Steps run one at
/// a time. A caller whose exhausted set a sibling's step already answered
/// takes no step of its own: the sibling topped up or settled since the
/// caller's last step, raised the deposit above the one the caller saw, or
/// swapped in a credential past the one the caller ran under.
#[derive(Debug)]
pub struct RecoveryGate {
    /// BLAKE3-verified content bytes the fetch has received, across every
    /// entry and lane that shares the gate.
    verified: AtomicU64,
    state: Mutex<GateState>,
    /// Held across a step, so concurrent entries take one step between them.
    step_lock: tokio::sync::Mutex<()>,
    /// The settle window after a top-up ([`RECOVERY_SETTLE`] by default).
    settle: Duration,
}

/// The mutable part of a [`RecoveryGate`].
#[derive(Debug, Default)]
struct GateState {
    /// The verified-byte count when the last step ran, or `None` before the
    /// first step.
    last_step: Option<u64>,
    /// The step that replaced the pool, which every later step reports.
    replaced: Option<PoolReplaced>,
    /// The top-up and settle steps the gate has taken. A caller that last saw
    /// fewer makes one more pass on the latest.
    top_ups: u64,
    /// The deposit the last top-up or settle step reached on the current
    /// pool. A caller that saw a lower deposit makes one more pass at it.
    reached: Option<U256>,
    /// The credential generation the last delegated step handed on. A caller
    /// that ran under an earlier generation makes one more pass on that step.
    swapped: Option<u64>,
    /// The end of the settle window after the last top-up.
    settle_until: Option<Instant>,
}

/// What one call of [`RecoveryGate::step`] did.
#[derive(Debug)]
#[doc(hidden)]
pub enum Stepped {
    /// The deposit is now this total: this step topped the pool up or settled
    /// at it, or a sibling's step already had. Make one more pass over the
    /// priced-out candidates.
    Raised(U256),
    /// A step opened a new pool. The remaining work runs again against it.
    Replaced(PoolReplaced),
    /// The progress rule allows no step: no new byte was verified since the
    /// last one. The fetch ends "funding needed".
    NoProgress,
    /// The funder has no way to add funds. The fetch ends "funding needed".
    Unavailable,
    /// The funder's step failed.
    Failed(anyhow::Error),
}

/// What one delegated recovery step ([`RecoveryGate::swap_step`]) did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SwapStep {
    /// A new credential is in the slot. Make one more pass under it.
    Swapped,
    /// The progress rule allows no step.
    NoProgress,
    /// No swap came within the slot's wait.
    TimedOut,
}

impl Default for RecoveryGate {
    fn default() -> Self {
        Self::with_settle(RECOVERY_SETTLE)
    }
}

impl RecoveryGate {
    /// A gate for one fetch, with the [`RECOVERY_SETTLE`] settle window.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A gate whose settle window after a top-up is `settle`.
    #[must_use]
    pub fn with_settle(settle: Duration) -> Self {
        Self {
            verified: AtomicU64::new(0),
            state: Mutex::new(GateState::default()),
            step_lock: tokio::sync::Mutex::new(()),
            settle,
        }
    }

    /// Start the pass that runs the remaining work against the pool a step
    /// replaced ([`crate::PoolReplaced`]). Later steps top up, or replace, the
    /// new pool; the progress rule carries over, so a further step still needs
    /// a byte verified since the step that replaced the pool.
    pub fn start_next_pass(&self) {
        let mut state = self.lock();
        state.replaced = None;
        state.reached = None;
    }

    /// Count `bytes` newly BLAKE3-verified content bytes toward the progress
    /// rule.
    #[doc(hidden)]
    pub fn record_verified(&self, bytes: u64) {
        self.verified.fetch_add(bytes, Ordering::Relaxed);
    }

    /// The top-up and settle steps the gate has taken: what a caller that
    /// starts its loop now has seen ([`Self::step`]).
    #[must_use]
    #[doc(hidden)]
    pub fn top_ups(&self) -> u64 {
        self.lock().top_ups
    }

    /// Whether `now` lies in the settle window after the last top-up.
    #[must_use]
    #[doc(hidden)]
    pub fn settling(&self, now: Instant) -> bool {
        self.lock().settle_until.is_some_and(|until| now < until)
    }

    /// Run one funding recovery step, if the progress rule allows one.
    ///
    /// `seen_deposit` is the deposit the caller found its candidate set
    /// exhausted at; `current_deposit` reads the deposit now. `seen_top_ups`
    /// is the caller's count of top-up and settle steps ([`Self::top_ups`]),
    /// which the call brings up to date. A step that a sibling already took is
    /// not taken twice: a caller that saw fewer top-ups than the gate took, or
    /// a deposit below the one the gate now reads, gets [`Stepped::Raised`]
    /// and takes no step. `spent` is the pool's spend,
    /// which [`Funder::recover`] tops up against. A pool replaced by an
    /// earlier step stays replaced: every later call reports it without a step.
    #[doc(hidden)]
    pub async fn step<F: Funder + ?Sized>(
        &self,
        funder: &F,
        seen_deposit: U256,
        seen_top_ups: &mut u64,
        current_deposit: impl Fn() -> U256,
        spent: U256,
    ) -> Stepped {
        let _one_step = self.step_lock.lock().await;
        let (replaced, reached, top_ups) = {
            let state = self.lock();
            (state.replaced, state.reached, state.top_ups)
        };
        if let Some(replaced) = replaced {
            return Stepped::Replaced(replaced);
        }
        let deposit = current_deposit()
            .max(seen_deposit)
            .max(reached.unwrap_or(U256::ZERO));
        if deposit > seen_deposit || top_ups > *seen_top_ups {
            *seen_top_ups = top_ups;
            return Stepped::Raised(deposit);
        }
        if !self.take_step() {
            return Stepped::NoProgress;
        }
        let remaining = deposit.saturating_sub(spent);
        tracing::info!(
            %deposit,
            %remaining,
            "no candidate serves at the current funding; running a funding recovery step"
        );
        match funder.recover(remaining).await {
            Ok(Recovery::ToppedUp(new_deposit)) => {
                let mut state = self.lock();
                state.settle_until = Some(Instant::now() + self.settle);
                state.reached = Some(state.reached.map_or(new_deposit, |d| d.max(new_deposit)));
                state.top_ups = state.top_ups.saturating_add(1);
                *seen_top_ups = state.top_ups;
                Stepped::Raised(new_deposit)
            }
            Ok(Recovery::Replaced(replaced)) => {
                self.lock().replaced = Some(replaced);
                Stepped::Replaced(replaced)
            }
            Ok(Recovery::Unavailable) => Stepped::Unavailable,
            Err(err) => Stepped::Failed(err),
        }
    }

    /// Run one delegated recovery step: wait up to the slot's swap wait for
    /// the application to swap in a new credential past `seen_generation`.
    /// The step is under the same progress rule as a top-up, whether the swap
    /// came before the call or during the wait: a pass under a swapped
    /// credential that verifies no byte allows no further step. A caller that
    /// ran under a generation before the one a sibling's step handed on gets
    /// [`SwapStep::Swapped`] and takes no step of its own.
    pub(crate) async fn swap_step(
        &self,
        slot: &crate::CredentialSlot,
        seen_generation: u64,
    ) -> SwapStep {
        let _one_step = self.step_lock.lock().await;
        if self
            .lock()
            .swapped
            .is_some_and(|handed| handed > seen_generation)
        {
            return SwapStep::Swapped;
        }
        if !self.take_step() {
            return SwapStep::NoProgress;
        }
        if slot.generation() <= seen_generation {
            tracing::info!(
                wait = ?slot.swap_wait(),
                "no candidate serves under the current capability; waiting for a new credential"
            );
            let swapped = slot.swapped_since(seen_generation);
            if tokio::time::timeout(slot.swap_wait(), swapped)
                .await
                .is_err()
            {
                return SwapStep::TimedOut;
            }
        }
        self.lock().swapped = Some(slot.generation());
        SwapStep::Swapped
    }

    /// Take a step under the progress rule: the first step always, a further
    /// one only when a byte was verified since the last.
    fn take_step(&self) -> bool {
        let verified = self.verified.load(Ordering::Relaxed);
        let mut state = self.lock();
        let allowed = state.last_step.is_none_or(|at| verified > at);
        if allowed {
            state.last_step = Some(verified);
        }
        allowed
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, GateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What an exhausted candidate set does after its recovery step: the raised
/// deposit to make one more pass at, or the fetch's end. The end is
/// `exhausted` ([`crate::NoAffordableSource`], "funding needed") with the
/// reason the step gave, the pool replacement, or a step failure only the user
/// can fix.
///
/// # Errors
///
/// Every outcome but [`Stepped::Raised`].
pub(crate) fn after_step(stepped: Stepped, exhausted: anyhow::Error) -> anyhow::Result<U256> {
    match stepped {
        Stepped::Raised(deposit) => Ok(deposit),
        Stepped::Replaced(replaced) => Err(anyhow::Error::new(replaced)),
        Stepped::NoProgress => Err(exhausted.context(
            "no node serves at the current funding, and no byte was verified since the last \
             funding recovery step",
        )),
        Stepped::Unavailable => Err(exhausted),
        Stepped::Failed(err) => Err(match crate::fault::classify(&err) {
            crate::Fault::Fatal(_) => err,
            _ => exhausted.context(format!("the funding recovery step failed: {err:#}")),
        }),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
