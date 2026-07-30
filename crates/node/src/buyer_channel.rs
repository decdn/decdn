//! On-chain `PaymentChannel` buyer-side service (#744).
//!
//! The node is the *client* (buyer) when it pulls content from an upstream
//! provider on a cache miss (ADR 003 §node→node). This service owns the
//! on-chain half of that path that the off-chain voucher signer in
//! [`crate::client_requester`] leaves open:
//!
//! - **One-time USDC approval.** `openChannel` escrows the deposit via
//!   `safeTransferFrom`, so the node must hold a standing ERC-20 allowance for
//!   the `PaymentChannel` contract. At bootstrap it reads the current allowance
//!   and, if insufficient, issues a single `approve(PaymentChannel, max)` —
//!   ADR 003 § Deposit Economics one-time-approval design.
//! - **Lazy open + per-provider reuse.** [`BuyerChannelService::open_or_reuse_channel`]
//!   returns a [`ChannelContext`] for the requester to sign vouchers against:
//!   it reuses the live channel tracked for that provider, or opens a new one
//!   (lazy-on-first-miss) and persists it. One open channel per provider keeps
//!   the deposit + gas amortized across many pulls.
//! - **Abandonment reclaim.** A background sweep reclaims the deposit of any
//!   tracked channel that has passed its on-chain expiry without the upstream
//!   closing it (`reclaimExpired`), then drops the local record.
//!
//! Wiring the cache-engine miss path to *call* `open_or_reuse_channel` (which
//! needs provider-discovery: NodeId→eth-address + a dialable target) is out of
//! scope here (ADR 001/022); this service exposes the API that hook will use.
//! The dispute monitor is deferred (#324).
//!
//! Structurally this mirrors [`crate::payment_settlement::PaymentChannelService`]:
//! a generic-over-`Provider` struct owning an `AbortOnDrop` background task.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::eips::BlockId;
use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use decdn_common::redact::sanitize_rpc_display;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{
    AdvanceOutcome, BuyerChannelState, BuyerChannelStore, BuyerLoad, ChannelId,
    ChannelOpenFailureReason, DepositOutcome, PendingSettle, PendingSettleStore, StoreError,
    Voucher,
};
use futures_util::FutureExt;
use iroh::{Endpoint, EndpointAddr, PublicKey};
use tracing::{debug, error, info, warn};

use crate::client_requester::ChannelContext;
// The buyer-channel open kernel (#940) — the `openChannel` tx + `ChannelOpened`
// decode + state/ctx build, and the one-time USDC approval — now live in the
// shared `decdn-client-pull` crate (re-exported here as `client_requester`).
use crate::chain_events::AbortOnDrop;
use crate::client_requester::buyer_channel::{
    LOW_WATER_DIVISOR, OpenedChannel, ensure_allowance, open_channel, refill_amount,
};
use crate::client_requester::cooperative_close::{
    AuthorizedWatermark, CooperativeCloseOutcome, cooperative_close,
};
use crate::dht::NodeAddressResolver;
use crate::metrics::{Metrics, SettleParty};
use crate::onchain_tx::{TxKind, TxOutcome, send_and_await_receipt};
use crate::payment_settlement::{settle_pass, unix_now};

/// How often the reclaim sweep scans tracked buyer channels for expiry.
/// Channel lifetimes are long (default 90 days), so an hourly scan is ample —
/// matches the seller expiry sweep cadence.
const RECLAIM_SWEEP_INTERVAL: Duration = Duration::from_hours(1);

/// How many consecutive failed sweeps a single channel's reclaim must rack up
/// before the per-attempt `warn!` escalates to a per-channel `error!` (#906).
/// The `buyer_reclaim_failures` metric increments on *every* failed attempt, so
/// the alertable signal exists from the first failure — this threshold gates
/// only the louder, per-channel `error!`.
/// A one-off failure is the expected transient case (host-clock-vs-chain
/// skew: not yet expired on-chain, retry next tick), so we tolerate a few
/// sweeps; ~6 hours (6 × `RECLAIM_SWEEP_INTERVAL`) of sustained failure is well
/// past any plausible skew and means a refundable deposit is genuinely stranded
/// (dead gas wallet, a never-clearing contract condition) and warrants operator
/// attention. Contrast the seller path's
/// `payment_settlement::record_pending_after_close`, which `error!`s on
/// the *first* failure: there a missed write means a fully-drawn channel never
/// auto-settles, so there is no benign-transient case to tolerate. A buyer
/// reclaim failure usually *is* benign (chain-clock skew), so we wait out a few
/// sweeps before treating it as a genuine stranded deposit.
const RECLAIM_ESCALATION_THRESHOLD: u32 = 6;

/// Overall timeout for one cooperative-close attempt in the idle-reconcile sweep
/// (dial + waiver request + on-chain submit). Short relative to the hourly sweep
/// — a provider that can't answer promptly is treated as unreachable for this
/// pass and the channel is left for the next sweep or the expiry reclaim.
const RECONCILE_DIAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Consecutive idle sweeps (no voucher-nonce progress) before an idle buyer
/// channel is cooperatively closed. At the hourly [`RECLAIM_SWEEP_INTERVAL`]
/// that is ~one day of inactivity — long enough that a channel still in active
/// use is never closed out from under a workload, short enough to free a
/// genuinely-abandoned deposit well before its (default 90-day) expiry. Not
/// config-tunable yet (YAGNI); promote to config if an operator needs a
/// different idle horizon.
const RECONCILE_IDLE_SWEEPS: u32 = 24;

/// Consecutive cooperative-close attempts that fail with a timeout-shaped
/// (dial/waiver-phase) error before the reconcile sweep gives up on a
/// cooperative close and `closeChannel`s the idle channel **unilaterally**
/// (#988). A provider that deregistered (`node_id_for` → `None`) is unreachable
/// immediately and skips this tally; this gates only the *reachable-but-silent*
/// case — a provider whose registration lingers but never answers the dial. At
/// the hourly [`RECLAIM_SWEEP_INTERVAL`] that is ~3 hours of sustained silence,
/// long enough to ride out a transient network blip before spending gas on a
/// unilateral close that the (default 90-day) expiry reclaim would eventually
/// make anyway. Not config-tunable yet (YAGNI), matching [`RECONCILE_IDLE_SWEEPS`].
const RECONCILE_CLOSE_ESCALATION_THRESHOLD: u32 = 3;

/// Dial wiring the idle-reconcile sweep needs beyond what the reclaim sweep has
/// (#972). Built by the runtime only when node→node pull-through is enabled (the
/// buyer path exists); `None` disables reconcile and the service runs the
/// expiry-reclaim sweep alone, exactly as before.
#[derive(Debug, Clone)]
pub struct BuyerReconcileConfig {
    /// The node's iroh endpoint, to dial the upstream provider for its waiver.
    pub endpoint: Endpoint,
    /// Resolves the provider's operator address back to a dialable `NodeId`.
    pub resolver: Arc<dyn NodeAddressResolver>,
}

/// Bound the wait for the `reclaimExpired` receipt on the open path's rotate leg.
///
/// Bounding this one is safe, and the contrast with `openChannel` — which
/// `decdn_client_pull::buyer_channel::open_channel` deliberately does NOT bound —
/// is the whole point (#1143):
///
/// - `openChannel` **escrows** a deposit. Giving up on its receipt does not cancel
///   the tx; it only makes us stop watching real USDC that is still going to land,
///   which is how a second deposit gets escrowed against the same provider.
/// - `reclaimExpired` **refunds** one. Giving up on its receipt strands nothing: the
///   channel row is left in place, the hourly reclaim sweep retries it, and the open
///   that triggered it fails with a retryable error.
///
/// So this bound exists purely so a stuck reclaim cannot pin the detached open task
/// — and with it the provider's in-flight slot — indefinitely. That would wedge the
/// provider for the life of the process, which is exactly the starvation #1143 set
/// out to remove. Sized like `APPROVE_RECEIPT_TIMEOUT` (#1109): well above a normal
/// inclusion window, short enough that the worst case is minutes. Not config-tunable
/// yet (YAGNI), like `RECONCILE_IDLE_SWEEPS`.
const RECLAIM_RECEIPT_TIMEOUT: Duration = Duration::from_mins(3);

/// Typed sentinel for a channel open that is still IN FLIGHT when the caller's
/// budget runs out (#1143).
///
/// It is not a failure: a detached task still owns the `openChannel`, and if it
/// lands, the channel is persisted and the next pull to this provider reuses it.
/// It means only "not ready in time — try another provider". `node_origin` meters
/// it and moves to the next candidate WITHOUT scoring reputation: a wedged
/// channel open is our own chain lane (a slow L2, a stuck nonce), not evidence of
/// anything about the peer.
#[derive(Debug)]
pub struct ChannelOpenPending {
    /// The provider whose channel is still opening.
    pub provider: Address,
    /// How long the caller waited before giving up — its budget, NOT the age of the
    /// open, which continues in a detached task and may run far longer.
    pub waited: Duration,
}

impl std::fmt::Display for ChannelOpenPending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "channel open for provider {} still in flight after {:?}; it continues in the \
             background and will be reused once it lands",
            self.provider, self.waited
        )
    }
}

impl std::error::Error for ChannelOpenPending {}

/// Typed sentinel for a pull that arrived while the boot/idle reconcile scan holds
/// the provider's open slot (`InFlightOpenGuard::claim`).
///
/// Like [`ChannelOpenPending`] it is NOT a failure — it is "retry, someone else is
/// mid-write on this provider's row". It needs its own type for the same reason
/// `ChannelOpenPending` does: a bare string error carries no verdict, so it fell into
/// `record_channel_open_failure`'s unclassified arm and was counted as a real
/// `decdn_node_pull_channel_open_failures_total` — indistinguishable from a reverting
/// tx or an under-funded wallet. Reconcile runs at every boot, so that turned each
/// restart into a spike of "channel open failures" an operator would go hunting for.
#[derive(Debug)]
pub struct OpenSlotReserved {
    /// The provider whose open slot a reconcile is holding. Retry; do not open.
    pub provider: Address,
}

impl std::fmt::Display for OpenSlotReserved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a buyer-channel reconcile holds provider {}'s open slot; retry",
            self.provider
        )
    }
}

impl std::error::Error for OpenSlotReserved {}

/// Marker attached to any error the detached open task has ALREADY logged and
/// metered (#1143).
///
/// The task is the only party guaranteed to observe an open's outcome — every
/// caller may have timed out and left with [`ChannelOpenPending`] — so the task,
/// not the caller, is the reporter. But a caller that *was* still waiting receives
/// the same error, and would otherwise log and count it a second time. This marker
/// lets `record_channel_open_failure` recognise "already reported" and stay quiet,
/// so `decdn_node_pull_channel_open_failures_total` counts opens that failed, not
/// opens that failed while someone happened to be listening.
#[derive(Debug)]
pub struct OpenReported;

impl std::fmt::Display for OpenReported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("reported by the buyer channel open task")
    }
}

impl std::error::Error for OpenReported {}

/// The outcome of a detached open, as seen by everyone waiting on it.
///
/// `Ok(())` carries no channel: a successful open *persists* to the store, so a
/// waiter simply re-reads it (`try_reuse_live`). The error side is an
/// `Arc<anyhow::Error>` because [`SharedOpen`] hands the same outcome to every
/// waiter and `anyhow::Error` is not `Clone`; [`rehydrate_open_error`] turns it
/// back into a per-waiter error that still `downcast_ref`s to its
/// [`ChannelOpenFailureReason`].
type OpenOutcome = Result<(), Arc<anyhow::Error>>;

/// A cloneable handle to an in-flight open. Every caller that arrives while an
/// open for the provider is running joins THIS future rather than starting a
/// second `openChannel` (which would escrow a second deposit).
type SharedOpen = futures_util::future::Shared<BoxFuture<'static, OpenOutcome>>;

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Rebuild a per-waiter `anyhow::Error` from the shared outcome, preserving the
/// [`ChannelOpenFailureReason`] so `record_channel_open_failure`'s `downcast_ref`
/// still classifies it (`insufficient_deposit` / `contract_revert` / `rpc_error`)
/// instead of logging `unclassified`. The alternative — handing every waiter the
/// same `Arc` — would not satisfy the `anyhow::Error` return type, and stringifying
/// would silently lose the reason.
/// Only the markers re-attached below survive; every other typed layer (the
/// `StoreError` from `store.record`, the underlying alloy transport error) is
/// flattened into the message. Nothing downcasts those off this path today, but a
/// future sentinel added to the open path MUST be re-attached here or it will
/// silently vanish — and only for callers that *joined* an open, never for the one
/// that started it, which is the nastiest possible way for it to fail.
fn rehydrate_open_error(err: &Arc<anyhow::Error>) -> anyhow::Error {
    let reason = err.downcast_ref::<ChannelOpenFailureReason>().copied();
    let reported = err.downcast_ref::<OpenReported>().is_some();
    let reserved = err
        .downcast_ref::<OpenSlotReserved>()
        .map(|r| OpenSlotReserved {
            provider: r.provider,
        });
    // `{:#}` renders the whole context chain, so the waiter's message matches what
    // the opening task saw.
    let mut rebuilt = anyhow::anyhow!("{err:#}");
    if let Some(reason) = reason {
        rebuilt = rebuilt.context(reason);
    }
    if reported {
        rebuilt = rebuilt.context(OpenReported);
    }
    // A joiner is the ONLY consumer of a reserved slot (the reconciler never awaits
    // its own parked future), so dropping this here would mean the sentinel is never
    // seen by anyone — the exact failure mode the doc above warns about.
    if let Some(reserved) = reserved {
        rebuilt = rebuilt.context(reserved);
    }
    rebuilt
}

/// RAII slot in the per-provider in-flight-open map. Dropping it removes the
/// provider so every path out of the open task — success, error, or panic —
/// releases the slot and the provider is never wedged. See
/// [`BuyerChannelService::opens_in_flight`].
///
/// The guard is held by the DETACHED OPEN TASK, not by the caller (#1143). That is
/// the whole point: a caller whose budget expires drops its *view* of the open, but
/// the task — and therefore the slot — lives until the `openChannel` actually
/// resolves. Release the slot while a tx is still in the mempool and the next miss
/// opens a SECOND channel to the same provider; the boot reconcile scan then
/// declines to adopt the first (`ReconcileOutcome::DeferredSecondOpen`), leaving its
/// deposit unreclaimed until the live row clears. The boot reconcile enumerates the
/// full client-channel history (no block lookback), so a later boot re-encounters
/// the orphan once the live row is gone — but until then its deposit stays stranded.
///
/// This is the same reasoning that keeps `openChannel`'s receipt wait unbounded (see
/// `decdn_client_pull::buyer_channel::open_channel`), and the two must not drift: a
/// bound there would release the slot for exactly the same reason and produce exactly
/// the same stranded deposit.
struct InFlightOpenGuard {
    map: Arc<Mutex<HashMap<Address, SharedOpen>>>,
    provider: Address,
}

impl InFlightOpenGuard {
    /// Claim a provider's open slot for a writer that is NOT performing an
    /// `openChannel` — the boot reconcile scan, which re-hydrates an orphaned
    /// channel's row and so is a second writer to the provider-keyed store.
    /// Returns `Ok(None)` when a real open is already in flight, in which case the
    /// reconciler skips (that open will persist the authoritative channel).
    ///
    /// The slot is parked with [`reserved_open`] rather than a live open, so a
    /// cache-miss arriving mid-reconcile is told to retry instead of racing a
    /// competing `openChannel` into the same provider row.
    fn claim(
        map: &Arc<Mutex<HashMap<Address, SharedOpen>>>,
        provider: Address,
    ) -> Result<Option<Self>> {
        // Acquisition treats a poisoned lock as fatal (`?`-bail) — unlike `Drop`
        // below, which must still release the slot. Refusing to *acquire* on a
        // poisoned lock surfaces the prior panic instead of papering over it.
        let mut in_flight = map
            .lock()
            .map_err(|err| anyhow::anyhow!("opens_in_flight mutex poisoned: {err}"))?;
        if in_flight.contains_key(&provider) {
            return Ok(None);
        }
        in_flight.insert(provider, reserved_open(provider));
        Ok(Some(Self {
            map: Arc::clone(map),
            provider,
        }))
    }

    /// Run `body` under `provider`'s open slot, releasing it when `body` returns — the ONLY
    /// safe way for a non-opening writer (the reconcile scan) to hold a claim.
    ///
    /// The guard is never bound at the call site, so it cannot be dropped early: the
    /// `let Some(_open_guard) = claim(..)` → `let Some(_) = claim(..)` edit — one character
    /// from releasing the slot with a live open racing the reconciler onto the same provider
    /// row, orphaning one of the two channels and stranding its deposit — is simply
    /// unavailable, exactly as [`Self::spawn_open`] makes it unavailable on the opening path
    /// (#1145 review). `Ok(None)` means a real open is already in flight; the caller skips,
    /// because that open persists the authoritative channel.
    fn under_claim<T>(
        map: &Arc<Mutex<HashMap<Address, SharedOpen>>>,
        provider: Address,
        body: impl FnOnce() -> Result<T>,
    ) -> Result<Option<T>> {
        let Some(_guard) = Self::claim(map, provider)? else {
            return Ok(None);
        };
        // `_guard` is in scope for the whole call and drops here, after `body` — so the slot
        // is held for exactly the body's lifetime and released on every exit (including a
        // panic unwind, via `Drop`).
        body().map(Some)
    }

    /// Reserve `provider`'s slot and run `open` under it in a DETACHED task, returning the
    /// joinable outcome. This is the ONLY way to obtain a guard for a real open.
    ///
    /// It exists to make the invariant in this type's doc — *the task owns the guard* —
    /// structural rather than remembered. Written out at the call site it is
    /// `let _guard = guard;` inside the spawned future, one character from
    /// `let _ = guard;`, which drops the guard on the task's first line and releases the
    /// slot with an `openChannel` still in the mempool. That is the stranded-deposit bug
    /// this guard exists to prevent, it is a change a reader would wave through, and no
    /// compiler diagnostic stands between the two. Here it is written once, under test, and a
    /// caller cannot express the wrong thing: this is the only way to obtain a guard for a real
    /// OPEN, and the guard never exists outside the task. (The non-opening reconcile scan takes
    /// a guard via [`Self::claim`], but only through [`Self::under_claim`], which likewise never
    /// binds it at a call site — see there.)
    ///
    /// `in_flight` is the CALLER'S LOCKED map, and taking it is what makes the reservation
    /// real (#1145 review). This function's doc has always said it "reserves the slot" — but
    /// it did not: it built the guard, and the matching `insert` lived twenty lines away in
    /// `join_or_spawn_open`. So the pairing was asymmetric — RELEASE in the type (`Drop`,
    /// unconditional), ACQUIRE in the caller — and the invariant "a guard exists ⇒ the slot
    /// is occupied" was caller-maintained. A future caller who reached for `spawn_open` and
    /// did not also remember the insert would get a guard, get no singleflight, and let the
    /// next miss open a SECOND `openChannel`: the double-escrowed deposit this whole
    /// mechanism exists to prevent. Doing the insert here makes guard construction and slot
    /// reservation one indivisible statement, under the lock the caller already holds.
    ///
    /// The `JoinError` leg is handled here for the same reason. It fires when the open
    /// task PANICS — and the wrapper it lands in is only ever polled by a caller, so with
    /// a `CHANNEL_OPEN_CALLER_BUDGET` of seconds against an unbounded receipt wait, the
    /// overwhelmingly likely case is that nobody is left to poll it. Reporting here rather
    /// than in the returned error is what stops a panicking open from being observed by
    /// nobody at all. (The slot still releases: `Drop` runs on unwind.)
    fn spawn_open<F>(
        map: &Arc<Mutex<HashMap<Address, SharedOpen>>>,
        in_flight: &mut HashMap<Address, SharedOpen>,
        provider: Address,
        metrics: &Arc<Metrics>,
        open: F,
    ) -> SharedOpen
    where
        F: Future<Output = Result<(), Arc<anyhow::Error>>> + Send + 'static,
    {
        let guard = Self {
            map: Arc::clone(map),
            provider,
        };
        let opening = tokio::spawn(async move {
            // Dropped when the task ends (any path, including a panic unwind),
            // releasing the provider slot — and NOT before.
            let _guard = guard;
            open.await
        });

        // A SPAWNED supervisor, not a combinator on the returned future.
        //
        // This distinction is the whole fix, and it is subtle enough to be worth spelling
        // out. A `Shared` advances only when some clone is POLLED, and after
        // the caller's budget expires the only clone left is the one parked in
        // `opens_in_flight`, which nobody polls. So a `handle.await.unwrap_or_else(report)`
        // written here would fire the report exactly when a caller was still waiting — and
        // stay silent in the no-caller case it exists for. Worse, when the panicking task's
        // guard then removes the map entry, the last clone drops and the future is
        // destroyed having never run at all.
        //
        // A spawned task is driven by the runtime whether or not anyone awaits it, so the
        // report happens either way. The panic that matters lands during the unbounded
        // receipt wait — minutes long, no caller by construction — which is precisely when
        // an `openChannel` may already be in the mempool: an escrowed deposit with no
        // persisted row, recoverable only by the boot reconcile scan. Making that visible
        // is this leg's entire purpose.
        let metrics = Arc::clone(metrics);
        let supervised = tokio::spawn(async move {
            match opening.await {
                Ok(outcome) => outcome,
                Err(join_err) => {
                    metrics.node_pull_channel_open_failure();
                    error!(
                        %provider,
                        %join_err,
                        "buyer channel open task died (panicked or was aborted); if its \
                         openChannel had already been broadcast the deposit is escrowed with \
                         no persisted row, and only the boot reconcile scan will recover it"
                    );
                    // `OpenReported` so a caller that IS still waiting doesn't count this a
                    // second time — the supervisor above is the report.
                    Err(Arc::new(
                        anyhow::anyhow!("buyer channel open task failed: {join_err}")
                            .context(OpenReported),
                    ))
                }
            }
        });

        let fut: BoxFuture<'static, OpenOutcome> = Box::pin(async move {
            // The supervisor only awaits and reports, so it cannot panic; this arm is
            // reachable only if the runtime aborts it at shutdown, where a caller is the
            // one party that could still be listening.
            supervised.await.unwrap_or_else(|join_err| {
                Err(Arc::new(anyhow::anyhow!(
                    "buyer channel open supervisor failed: {join_err}"
                )))
            })
        });
        let shared = fut.shared();
        // THE reservation, and it belongs here — beside the guard whose `Drop` is what
        // releases it. Under the caller's lock, so the check-then-insert in
        // `join_or_spawn_open` stays atomic and two concurrent misses cannot both open.
        in_flight.insert(provider, shared.clone());
        shared
    }
}

/// The slot value parked by a non-opening claimant ([`InFlightOpenGuard::claim`]).
/// A pull that joins it is told to retry — it must not proceed to open, because the
/// claimant is mid-write on this provider's row; and it must not wait, because the
/// claimant will never produce a channel for it.
fn reserved_open(provider: Address) -> SharedOpen {
    let fut: BoxFuture<'static, OpenOutcome> = Box::pin(std::future::ready(Err(Arc::new(
        anyhow::Error::new(OpenSlotReserved { provider }),
    ))));
    fut.shared()
}

impl Drop for InFlightOpenGuard {
    fn drop(&mut self) {
        // A poisoned lock means a prior holder panicked; recover the inner map
        // and still release the slot rather than leaving the provider stuck.
        let mut map = self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.remove(&self.provider);
    }
}

/// RAII claim on a provider's slot in [`BuyerChannelService::topups_in_flight`]
/// (#1146). Held by the detached refill task; its `Drop` frees the provider so a
/// task that finishes OR panics never wedges future refills to that provider.
struct RefillSlot {
    set: Arc<Mutex<HashSet<Address>>>,
    provider: Address,
}

impl Drop for RefillSlot {
    fn drop(&mut self) {
        // Recover a poisoned lock (a prior holder panicked) and still release the
        // slot, mirroring `InFlightOpenGuard`.
        self.set
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.provider);
    }
}

/// Claim the background-refill slot for `provider`, or `None` if a refill is
/// already in flight for it (per-provider dedup, #1146). A low-water refill is
/// best-effort, so it must never propagate a panic into the pull path: a poisoned
/// lock (a prior holder panicked while holding it) is RECOVERED via
/// `PoisonError::into_inner` and the claim proceeds, mirroring [`RefillSlot::drop`].
/// The critical section is a single `HashSet` insert that cannot leave the set
/// inconsistent, so recovering is safe — and, unlike declining on poison, keeps
/// refills self-healing rather than permanently disabled after the first panic.
fn claim_refill_slot(set: &Arc<Mutex<HashSet<Address>>>, provider: Address) -> Option<RefillSlot> {
    let mut guard = set
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !guard.insert(provider) {
        return None; // a refill for this provider is already running
    }
    drop(guard);
    Some(RefillSlot {
        set: Arc::clone(set),
        provider,
    })
}

/// The low-water top-up amount for a reused channel (#1146, #1103): `U256::ZERO`
/// when the remaining deposit still has headroom, else the amount that restores
/// it to the working `target`. `target = max(deposit_hint, working_deposit,
/// min_deposit)` — the graduation target a proven-good channel refills toward
/// (see [`open_deposit`] for the smaller, INITIAL amount a fresh open funds
/// instead) — and the trigger is `target / LOW_WATER_DIVISOR` (20% remaining).
/// `deposit` / `prior_amount` are the reused channel's on-chain deposit and
/// cumulative vouchered amount, read straight off the [`ChannelContext`] the
/// reuse path already built — no extra store read. Pure so the policy is
/// unit-testable; the shared [`refill_amount`] kernel is the same one the CLI
/// fetch auto-refill uses.
fn refill_decision(
    deposit: U256,
    prior_amount: U256,
    deposit_hint: U256,
    working_deposit: U256,
    min_deposit: U256,
) -> U256 {
    if working_deposit.is_zero() {
        // `0` disables top-up entirely (matches the CLI's auto-refill config
        // semantics for `buyer_working_deposit_micro_usdc`) — a reused channel is
        // never proactively refilled, no matter how far below what the target
        // would otherwise put its low-water mark.
        return U256::ZERO;
    }
    let target = deposit_hint.max(working_deposit).max(min_deposit);
    let low_water = target / U256::from(LOW_WATER_DIVISOR);
    refill_amount(deposit, prior_amount, target, low_water)
}

/// The deposit a FRESH open escrows (#1497 task 6): "open small, graduate on
/// proof". `max(deposit_hint, initial_deposit, min_deposit)` — deliberately the
/// small `initial_deposit`, not the larger `working_deposit` [`refill_decision`]
/// graduates a proven channel toward. `deposit_hint` still lets a caller ask for
/// more up front; the on-chain `min_deposit` floor always wins. Pure so the open
/// path's sizing is unit-testable without a live contract.
fn open_deposit(deposit_hint: U256, initial_deposit: U256, min_deposit: U256) -> U256 {
    deposit_hint.max(initial_deposit).max(min_deposit)
}

/// Buyer-side `PaymentChannel` service. Generic over the alloy [`Provider`]
/// (a wallet-filled provider is required for the `approve` / `openChannel` /
/// `topUp` / `reclaimExpired` write paths). Cheap to construct; owns its
/// background reclaim task.
pub struct BuyerChannelService<P: Provider + Clone + 'static> {
    contract: PaymentChannel::PaymentChannelInstance<P>,
    store: Arc<dyn BuyerChannelStore>,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: Eip712Domain,
    token: Address,
    self_address: Address,
    min_deposit: U256,
    /// The small deposit a fresh `openChannel` escrows (#1497 task 6):
    /// "open small, graduate on proof". Consumed by [`open_deposit`].
    initial_deposit: U256,
    /// The larger graduation target a reused channel's low-water refill tops up
    /// toward (see [`refill_decision`]). Renamed from `default_deposit` —
    /// opening and refilling now use two different deposit sizes instead of one.
    working_deposit: U256,
    /// Providers with an `openChannel` currently in flight, each mapped to a
    /// cloneable handle on the detached task performing it (#1143).
    ///
    /// Makes the one-channel-per-provider invariant real (PR #753 review): a
    /// concurrent [`Self::open_or_reuse_channel`] for a provider already mid-open
    /// JOINS the running open instead of escrowing a second deposit whose `record`
    /// would orphan the first. Only the open path touches this map — pure reuse
    /// never contends, so many concurrent pulls to an already-open provider proceed
    /// freely.
    ///
    /// In-memory: it is a process-local dedup, not a durable ledger. What survives a
    /// restart is the persisted channel row (and, for an open that landed without
    /// one, the boot-time `reconcile_orphans_once` scan).
    opens_in_flight: Arc<Mutex<HashMap<Address, SharedOpen>>>,
    /// Providers with a background low-water top-up currently in flight (#1146).
    ///
    /// A reused channel whose remaining deposit has run below its low-water mark
    /// is topped up by a detached, best-effort task so a sustained series of miss
    /// pulls to one provider is never silently stranded by a spent-down deposit.
    /// This set dedups those tasks per provider: many concurrent reuse pulls to
    /// the same provider fire at most one `topUp` tx. In-memory only — a
    /// process-local dedup, not a durable ledger; the slot is freed when the task
    /// finishes or panics (see [`RefillSlot`]).
    topups_in_flight: Arc<Mutex<HashSet<Address>>>,
    /// Per-channel consecutive `try_reclaim`-failure tally (#906), shared between
    /// the background reclaim loop and [`Self::sweep_expired_once`]. In-memory
    /// only: a restart resets it, so a persistent failure re-escalates after
    /// `RECLAIM_ESCALATION_THRESHOLD` post-restart sweeps (escalation is
    /// observability-only, so this is acceptable). Pruned each pass down to the
    /// channels still expired, so it cannot grow unbounded.
    reclaim_failures: Arc<Mutex<HashMap<ChannelId, u32>>>,
    /// Metrics sink for the reclaim sweep (#906): `buyer_reclaim_failure` on a
    /// failed attempt, paired with the threshold `error!` escalation.
    metrics: Arc<Metrics>,
    _reclaimer: AbortOnDrop,
    /// Aborts the one-shot bootstrap reconciliation scan (#763) if the service is
    /// dropped (a fast restart) before the scan finishes, so a long backfill
    /// never outlives the service. Held only for its `Drop`.
    _reconciler: AbortOnDrop,
    /// Aborts the idle-reconcile sweep (#972) on drop. `None` when reconcile is
    /// disabled (node→node pull-through off, so no dial wiring). Held only for its
    /// `Drop`.
    _idle_reconciler: Option<AbortOnDrop>,
}

impl<P: Provider + Clone + 'static> BuyerChannelService<P> {
    /// Bootstrap the service: self-check the contract, read the immutable USDC
    /// token and the governable `minDeposit` floor, issue the one-time USDC
    /// approval if requested, and spawn the reclaim sweep.
    ///
    /// `initial_deposit` is the small deposit a fresh open escrows;
    /// `working_deposit` is the larger target a reused channel's low-water
    /// refill graduates it toward (#1497 task 6). Both are clamped up to the
    /// on-chain `minDeposit`.
    ///
    /// # Errors
    ///
    /// Returns an error if the `usdc()` / `minDeposit()` self-check calls fail
    /// (a bad `payment_channel_address` or unreachable RPC is fatal at
    /// bring-up), if the persisted buyer channels cannot be loaded, or if the
    /// one-time approval transaction fails.
    // bootstrap threads the chain wiring + signer + deposit config in one place
    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap(
        provider: P,
        payment_channel_addr: Address,
        self_address: Address,
        store: Arc<dyn BuyerChannelStore>,
        signer: Arc<PrivateKeySigner>,
        voucher_domain: Eip712Domain,
        initial_deposit: U256,
        working_deposit: U256,
        ensure_max_approval: bool,
        reconcile: Option<BuyerReconcileConfig>,
        pending_store: Arc<dyn PendingSettleStore>,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let contract = PaymentChannel::new(payment_channel_addr, provider.clone());

        let token = contract.usdc().call().await.with_context(|| {
            format!("PaymentChannel.usdc() self-check at {payment_channel_addr}")
        })?;
        let min_deposit = contract
            .minDeposit()
            .call()
            .await
            .context("PaymentChannel.minDeposit() self-check")?;

        if ensure_max_approval {
            // Daemon posture: an unlimited (`None`) standing approval for a
            // long-lived node, avoiding re-approve churn across many miss pulls.
            ensure_allowance(&provider, token, self_address, payment_channel_addr, None).await?;
        }

        let load = store
            .load_all()
            .context("hydrate persisted buyer channels")?;
        metrics.buyer_channel_store_skipped_undecodable_records(load.skipped.len());
        let tracked = load.channels.len();
        info!(
            %payment_channel_addr,
            %token,
            %self_address,
            %min_deposit,
            tracked,
            "BuyerChannelService bootstrap complete"
        );

        let reclaim_failures = Arc::new(Mutex::new(HashMap::new()));
        let reclaimer = tokio::spawn(reclaim_loop(
            contract.clone(),
            Arc::clone(&store),
            self_address,
            Arc::clone(&reclaim_failures),
            Arc::clone(&pending_store),
            Arc::clone(&metrics),
        ));

        // Shared per-provider in-flight-open set: the reconciler claims the same
        // slots the live open path uses, so its re-hydration can never overwrite a
        // channel a concurrent cache-miss open just recorded.
        let opens_in_flight = Arc::new(Mutex::new(HashMap::new()));
        // Per-provider dedup for background low-water top-ups (#1146).
        let topups_in_flight = Arc::new(Mutex::new(HashSet::new()));

        // One-shot bootstrap reconciliation (#763): re-hydrate any on-chain
        // channel this node opened but lost track of (record/decode failed
        // post-escrow, or a downgrade made the row undecodable). Best-effort and
        // non-blocking — matches the non-fatal buyer-bootstrap posture; the
        // already-spawned reclaim loop reclaims any re-hydrated expired channel
        // on its next tick. Runs in the background so a transient RPC failure on
        // the head read does not fail bring-up.
        let reconciler = tokio::spawn(reconcile_orphans_once(
            contract.clone(),
            Arc::clone(&store),
            self_address,
            Arc::clone(&opens_in_flight),
        ));

        // Idle-reconcile sweep (#972): only when the runtime supplied dial wiring
        // (node→node pull-through on). Without it the service runs the
        // expiry-reclaim sweep alone, exactly as before.
        let idle_reconciler = reconcile.map(|cfg| {
            info!(
                idle_sweeps_threshold = RECONCILE_IDLE_SWEEPS,
                close_escalation_threshold = RECONCILE_CLOSE_ESCALATION_THRESHOLD,
                "buyer idle-reconcile sweep enabled"
            );
            AbortOnDrop(tokio::spawn(reconcile_loop(
                contract.clone(),
                Arc::clone(&store),
                Arc::clone(&signer),
                voucher_domain.clone(),
                cfg,
                Arc::clone(&pending_store),
                Arc::clone(&metrics),
            )))
        });

        Ok(Self {
            contract,
            store,
            signer,
            voucher_domain,
            token,
            self_address,
            min_deposit,
            initial_deposit,
            working_deposit,
            opens_in_flight,
            topups_in_flight,
            reclaim_failures,
            metrics,
            _reclaimer: AbortOnDrop(reclaimer),
            _reconciler: AbortOnDrop(reconciler),
            _idle_reconciler: idle_reconciler,
        })
    }

    /// Reuse the live (non-expired) channel tracked for `provider_addr`, if any.
    /// Returns `None` when no channel is tracked or the tracked one has expired
    /// (the caller then opens / rotates one under the per-provider open guard).
    fn try_reuse_live(&self, provider_addr: Address) -> Result<Option<ChannelContext>> {
        let Some(existing) = self
            .store
            .get_by_provider(provider_addr)
            .context("look up existing buyer channel")?
        else {
            return Ok(None);
        };
        if existing.is_expired_at(unix_now()) {
            return Ok(None);
        }
        debug!(
            provider = %provider_addr,
            channel_id = %existing.channel_id,
            "reusing existing buyer channel"
        );
        Ok(Some(ChannelContext::for_buyer_channel(
            &existing,
            Arc::clone(&self.signer),
            self.voucher_domain.clone(),
        )))
    }

    /// Return a [`ChannelContext`] for paying `provider_addr`: reuse the live
    /// channel tracked for that provider, or lazily open a new one.
    ///
    /// `deposit_hint` is the desired deposit for a freshly-opened channel; the
    /// actual deposit is `max(deposit_hint, initial_deposit, min_deposit)` — the
    /// small INITIAL size, not the working target. The
    /// hint is ignored when an existing channel is reused (call
    /// [`Self::top_up`] to add funds to a live channel).
    ///
    /// # Bounding (#1143)
    ///
    /// `budget` bounds how long the CALLER waits — not how long the open runs. When
    /// it expires this returns [`ChannelOpenPending`] and the open **keeps going**
    /// in a detached task.
    ///
    /// That split exists because the two clocks want incompatible values. A
    /// cache-miss must give up on a provider in seconds so the candidate loop can
    /// try the next one; a broadcast `openChannel` tx may legitimately need minutes
    /// to mine on a slow L2. Shortening the wait cannot satisfy both. The obvious
    /// fix — `tokio::time::timeout` around the whole thing — is worse than useless
    /// here: it drops the future mid-flight, abandoning an `openChannel` that can
    /// still land, so the deposit is escrowed for a channel nobody is tracking.
    ///
    /// So: don't cancel the open, just stop waiting on it. The task owns the tx and
    /// persists the channel if it lands, and the next pull to this provider reuses
    /// it. Nothing is abandoned; the caller is merely told "not yet".
    ///
    /// # Concurrency
    ///
    /// Opens are singleflighted per provider. A caller that arrives while an open is
    /// running JOINS it rather than starting a second `openChannel` — two racing
    /// opens would each escrow a deposit, and the provider-keyed store means the
    /// second `record` would orphan the first. Pure reuse of a live channel never
    /// contends (it does not even take the map lock), and opens for *distinct*
    /// providers run in parallel.
    ///
    /// The in-flight slot is held by the TASK, not the caller. A caller that times
    /// out must not release it: its tx may still be in the mempool, and the next
    /// miss would then open a second channel to the same provider — one the boot
    /// reconcile scan explicitly declines to adopt (`DeferredSecondOpen`), stranding
    /// its deposit indefinitely.
    ///
    /// # Errors
    ///
    /// [`ChannelOpenPending`] when `budget` expires with the open still running.
    /// Otherwise: store errors, any failure of the `openChannel` transaction
    /// (submit, revert, or receipt — carrying a [`ChannelOpenFailureReason`] the
    /// caller can `downcast_ref`), or a tracked-but-expired channel that could not
    /// be reclaimed first (so its deposit is never silently dropped; retry once the
    /// reclaim sweep clears it).
    pub async fn open_or_reuse_channel(
        &self,
        provider_addr: Address,
        deposit_hint: U256,
        budget: Duration,
    ) -> Result<ChannelContext> {
        // Fast path: reuse a live channel without touching the in-flight map, so
        // many concurrent pulls to an already-open provider never serialize.
        if let Some(ctx) = self.reuse_live_or_report(provider_addr)? {
            // Non-blocking: if this channel has run below its low-water mark, kick
            // off a detached top-up (#1146) so a later reuse isn't stranded, and
            // hand THIS pull the current channel immediately — the refill must not
            // sit in the hot reuse path behind an on-chain `topUp`. The decision
            // reads the deposit/watermark off `ctx` (already built by the reuse
            // read above), so the fast path takes no second store read.
            self.spawn_refill_if_low(provider_addr, deposit_hint, &ctx);
            return Ok(ctx);
        }

        let open = self.join_or_spawn_open(provider_addr, deposit_hint)?;

        match tokio::time::timeout(budget, open).await {
            // Still running. The task owns the tx; hand the caller a typed "not
            // yet" so it can move to the next candidate.
            Err(_) => Err(anyhow::Error::new(ChannelOpenPending {
                provider: provider_addr,
                waited: budget,
            })),
            Ok(Err(err)) => Err(rehydrate_open_error(&err)),
            // The task persisted the channel before it signalled, so re-reading the
            // store is how we collect the result — that is also exactly what a
            // *later* pull would do, so success and reuse share one code path.
            Ok(Ok(())) => self.reuse_live_or_report(provider_addr)?.ok_or_else(|| {
                self.metrics.node_pull_channel_open_failure();
                error!(
                    provider = %provider_addr,
                    "buyer channel opened on-chain but is not live in the store — a deposit \
                     is escrowed against a row we cannot see"
                );
                anyhow::anyhow!(
                    "buyer channel for provider {provider_addr} opened but is not live in the \
                     store (expired between open and read?)"
                )
                .context(OpenReported)
            }),
        }
    }

    /// [`Self::try_reuse_live`], reporting a store fault at the severity it deserves.
    ///
    /// This is the leg a SUSTAINED store fault actually takes — a corrupt page, fd
    /// exhaustion, an unwritable `data_dir`. It runs before the open task is spawned, so
    /// `run_open`'s loud store leg does not cover it: that one only fires when the read
    /// here SUCCEEDED and the in-task read then failed, i.e. an intermittent blip. Left
    /// bare, the persistent fault — the one that makes this node unable to open a channel
    /// to ANY provider — was the quieter of the two.
    ///
    /// Metered and marked [`OpenReported`], so the caller-side ladder does not restate it.
    fn reuse_live_or_report(&self, provider_addr: Address) -> Result<Option<ChannelContext>> {
        self.try_reuse_live(provider_addr).map_err(|err| {
            self.metrics.node_pull_channel_open_failure();
            error!(
                provider = %provider_addr,
                error = %format!("{err:#}"),
                "buyer channel store read failed; this node can neither open nor reuse a \
                 channel to any provider until the store recovers"
            );
            err.context(OpenReported)
        })
    }

    /// The synchronous decision behind [`Self::spawn_refill_if_low`]: given the
    /// reused channel's `deposit` and cumulative `prior_amount` (read off the
    /// [`ChannelContext`] the reuse path already built — no second store read), if
    /// its remaining deposit is below the low-water mark, claim the per-provider
    /// refill slot and return it with the top-up amount. `None` means nothing to
    /// do — still has headroom, or a refill is already in flight for this provider.
    /// The channel is known-live: `spawn_refill_if_low` is only called after the
    /// reuse gate (`try_reuse_live`) returns a non-expired channel, so there is no
    /// expiry or store-fault branch to handle here. Split out from the spawn so the
    /// decision + claim is unit-testable without spawning the on-chain task.
    fn plan_refill(
        &self,
        provider_addr: Address,
        deposit_hint: U256,
        deposit: U256,
        prior_amount: U256,
    ) -> Option<(RefillSlot, U256)> {
        let additional = refill_decision(
            deposit,
            prior_amount,
            deposit_hint,
            self.working_deposit,
            self.min_deposit,
        );
        if additional.is_zero() {
            return None; // still above the low-water mark
        }

        // `None` here = a refill for this provider is already in flight (dedup).
        let slot = claim_refill_slot(&self.topups_in_flight, provider_addr)?;
        Some((slot, additional))
    }

    /// Best-effort background top-up of a reused channel that has run below its
    /// low-water mark (#1146). Spawns a detached task and returns immediately — it
    /// NEVER blocks the pull: a synchronous on-chain `topUp` (seconds-to-minutes on
    /// a slow L2) must not sit in the non-serializing reuse fast path, so the
    /// current pull proceeds on the existing deposit and the refill lands for a
    /// later reuse. Deduped per provider via [`Self::topups_in_flight`], so many
    /// concurrent reuse pulls fire at most one `topUp`.
    ///
    /// Every leg is advisory: an already-in-flight refill, an allowance failure, or
    /// a reverted `topUp` all just skip the refill (logged / metered), leaving the
    /// pre-#1146 behavior — the channel is simply not topped up, which is strictly
    /// no worse. A channel already fully drained *now* still fails *this* pull and
    /// heals on the next; the 20% low-water trigger means the refill normally fires
    /// with headroom to spare.
    ///
    /// `ctx` is the reused channel's context (from the reuse read); its `deposit` /
    /// `prior_amount` drive the low-water decision, so this takes no store read.
    fn spawn_refill_if_low(
        &self,
        provider_addr: Address,
        deposit_hint: U256,
        ctx: &ChannelContext,
    ) {
        let Some((slot, additional)) =
            self.plan_refill(provider_addr, deposit_hint, ctx.deposit, ctx.prior_amount)
        else {
            return;
        };

        let contract = self.contract.clone();
        let store = Arc::clone(&self.store);
        let rpc = self.contract.provider().clone();
        let token = self.token;
        let self_address = self.self_address;
        let payment_channel_addr = *self.contract.address();
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            // Freed on task completion OR panic, so a wedged refill never blocks
            // future refills to this provider.
            let _slot = slot;

            // Daemon posture: ensure the standing unlimited allowance (idempotent —
            // skips when already granted) so `topUp`'s `transferFrom` can pull the
            // funds even if the standing approval was revoked. Mirrors bootstrap and
            // the CLI refill's allowance step.
            if let Err(err) =
                ensure_allowance(&rpc, token, self_address, payment_channel_addr, None).await
            {
                warn!(
                    provider = %provider_addr,
                    %additional,
                    error = %format!("{err:#}"),
                    "buyer refill: ensure_allowance failed; reused channel not topped up"
                );
                metrics.buyer_topup_failure();
                return;
            }

            match decdn_client_pull::buyer_channel::top_up(
                &contract,
                store.as_ref(),
                provider_addr,
                additional,
            )
            .await
            {
                Ok(DepositOutcome::Added(_)) => {
                    info!(
                        provider = %provider_addr,
                        %additional,
                        "buyer refill: topped up reused channel below its low-water mark (#1146)"
                    );
                    metrics.buyer_topup_ok();
                }
                // The topUp landed on-chain but the local row vanished or rotated
                // during the RPC (`top_up` already logged it at error!/warn! with
                // the tx for reconcile). Funds are escrowed-but-untracked — NOT a
                // clean success, so meter it as a failure rather than
                // `buyer_topup_ok`, or an operator watching the failure metric
                // would miss stranded deposits (#1146 review).
                Ok(DepositOutcome::UnknownChannel | DepositOutcome::ChannelMismatch) => {
                    metrics.buyer_topup_failure();
                }
                Err(err) => {
                    warn!(
                        provider = %provider_addr,
                        %additional,
                        error = %format!("{err:#}"),
                        "buyer refill: topUp failed; reused channel not topped up"
                    );
                    metrics.buyer_topup_failure();
                }
            }
        });
    }

    /// Join the in-flight open for `provider_addr`, or spawn one.
    ///
    /// The map lock is what serializes this: the check and the insert happen under
    /// one acquisition, so two callers cannot both decide to spawn. Nothing is
    /// awaited while it is held.
    fn join_or_spawn_open(&self, provider_addr: Address, deposit_hint: U256) -> Result<SharedOpen> {
        // A poisoned lock is terminal for this node's buyer side: acquisition bails
        // (unlike `Drop`, which recovers so the slot still releases), so EVERY subsequent
        // open to EVERY provider fails here for the life of the process. Report it at that
        // severity rather than letting it trickle out as one more unlabeled open failure.
        let mut in_flight = self.opens_in_flight.lock().map_err(|err| {
            self.metrics.node_pull_channel_open_failure();
            error!(
                provider = %provider_addr,
                %err,
                "opens_in_flight mutex poisoned by an earlier panic; this node can no longer \
                 open a buyer channel to ANY provider and must be restarted"
            );
            anyhow::anyhow!("opens_in_flight mutex poisoned: {err}").context(OpenReported)
        })?;

        if let Some(existing) = in_flight.get(&provider_addr) {
            debug!(
                provider = %provider_addr,
                "joining an openChannel already in flight for this provider"
            );
            return Ok(existing.clone());
        }

        // Everything the open needs, cloned so the task is `'static` and outlives
        // any single caller.
        let contract = self.contract.clone();
        let store = Arc::clone(&self.store);
        let signer = Arc::clone(&self.signer);
        let voucher_domain = self.voucher_domain.clone();
        let token = self.token;
        let self_address = self.self_address;
        // Open at the INITIAL deposit (floored by the on-chain minimum), NOT the
        // working target — "open small, graduate on proof". `deposit_hint` still
        // lets a caller ask for more up front, but the small initial is the
        // floor, not working.
        let deposit = open_deposit(deposit_hint, self.initial_deposit, self.min_deposit);
        let metrics = Arc::clone(&self.metrics);

        // `spawn_open` reserves the slot (in the map we hold locked) AND holds it in the task
        // for the open's whole life. The guard is never constructible here, so this cannot
        // release it early — and the reservation is no longer a separate line this caller
        // could forget (#1145 review).
        Ok(InFlightOpenGuard::spawn_open(
            &self.opens_in_flight,
            &mut in_flight,
            provider_addr,
            &self.metrics,
            async move {
                run_open(
                    &contract,
                    &store,
                    signer,
                    &voucher_domain,
                    token,
                    self_address,
                    provider_addr,
                    deposit,
                    &metrics,
                )
                .await
                .map_err(Arc::new)
            },
        ))
    }

    /// Persist the cumulative voucher totals after a delivery exchange so a
    /// later reuse (or a restart) resumes the channel at the right `nonce` /
    /// `bytes` / `amount`. The caller reports the totals of the last voucher it
    /// signed on `provider_addr`'s channel; `channel_id` is the channel the
    /// voucher was signed against.
    ///
    /// If the persisted row's `channel_id` no longer matches (the provider's
    /// slot was replaced by a newer open between the delivery and this write),
    /// the stale progress is logged and dropped — `Ok(())`, not an error,
    /// because writing it would clobber the live replacement channel's record.
    ///
    /// # Errors
    ///
    /// Errors if no channel is tracked for `provider_addr`, if the reported
    /// totals would regress the stored state (a caller bug), or on store write
    /// failure.
    pub fn record_progress(
        &self,
        provider_addr: Address,
        channel_id: ChannelId,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<()> {
        // Advance the committed watermark inside one write txn so a concurrent
        // `top_up` (or another `record_progress`) cannot clobber this write or
        // regress the persisted voucher watermark (#838).
        match self
            .store
            .advance_progress(provider_addr, channel_id, nonce, bytes_delivered, amount)
            .context("advance buyer channel progress")?
        {
            AdvanceOutcome::Advanced => Ok(()),
            AdvanceOutcome::UnknownChannel => {
                anyhow::bail!("record_progress for unknown provider {provider_addr}")
            }
            // The provider's slot was replaced by a newer open between the
            // delivery and this write. Recording stale progress onto the new
            // channel would be wrong; the older channel's record is gone, so this
            // is not an escalation (it mirrors the reclaim sweep's
            // `forget_if_channel` miss) and must not fail the already-paid pull.
            // But a delivered-and-paid voucher's progress was dropped, and a
            // *sustained* rate here would mean a `channel_id`-plumbing bug rather
            // than the rare benign mid-pull replacement — so it is METERED, not just
            // logged (#1145 review). It used to be a bare `warn!` + `Ok(())`, which
            // asked an operator to watch for a trend in a signal that had no series
            // to trend: `node_pull_progress_persist_failure` fires only on `Err`, so
            // this path — real USDC paid, watermark discarded — was indistinguishable
            // in metrics from a clean persist.
            AdvanceOutcome::ChannelMismatch => {
                self.metrics.node_pull_progress_dropped();
                warn!(
                    provider = %provider_addr,
                    %channel_id,
                    "record_progress: provider channel replaced by a newer open; \
                     skipping stale progress write"
                );
                Ok(())
            }
            // A CONCURRENT pull on this shared channel ledger already persisted a higher
            // watermark, and the store is monotonic, so it kept the correct (higher) value and
            // rejected ours. No voucher is lost — this is the routine outcome of two concurrent
            // settles racing under `BuyerLedgers`, not a persist failure (#1145 review).
            // Returning `Err` here fired `node_pull_progress_persist_failure` — the "we paid and
            // lost the record" alert — on ordinary, healthy concurrency. Metered as benign and
            // treated as `Ok`; a real store-write failure is still the `?` above.
            AdvanceOutcome::Regressed(_err) => {
                self.metrics.node_pull_progress_superseded();
                debug!(
                    provider = %provider_addr,
                    %channel_id,
                    "record_progress: a concurrent settle persisted a higher watermark first; \
                     ours superseded (benign under the shared ledger)"
                );
                Ok(())
            }
        }
    }

    /// Retire the tracked channel for `provider_addr` if it is still `channel_id`.
    /// See [`ChannelOpener::retire_channel`] for the contract and why it exists.
    ///
    /// # Errors
    ///
    /// On store write failure.
    pub fn retire_channel(&self, provider_addr: Address, channel_id: ChannelId) -> Result<bool> {
        self.store
            .forget_if_channel(provider_addr, channel_id)
            .context("retire buyer channel")
    }

    /// Run one reclaim-sweep pass synchronously: reclaim the deposit of every
    /// tracked channel past its on-chain expiry (or drop the record if the
    /// upstream already closed it). The background sweep calls this on a timer;
    /// it is also exposed so the runtime (or a test) can trigger an immediate
    /// pass. Best-effort — per-channel errors are logged, never propagated.
    pub async fn sweep_expired_once(&self) {
        reclaim_once(
            &self.contract,
            &self.store,
            self.self_address,
            &self.reclaim_failures,
            &self.metrics,
        )
        .await;
    }

    /// Add `additional` USDC to the channel tracked for `provider_addr`.
    /// Does not extend the channel expiry (the contract forbids it).
    ///
    /// Thin delegation to the shared
    /// [`decdn_client_pull::buyer_channel::top_up`] kernel (the same mechanism
    /// the CLI fetch buyer's auto-refill uses, #1103): read the `channel_id`,
    /// `topUp` on-chain, then reconcile the committed deposit — handling the
    /// escrowed-but-untracked / channel-replaced races.
    ///
    /// # Errors
    ///
    /// Errors if no channel is tracked for `provider_addr` *before* the RPC, or
    /// if the `topUp` transaction fails (submit, revert, or receipt).
    ///
    /// A row that vanishes or is replaced by a newer open *after* the on-chain
    /// `topUp` lands is logged (with the tx hash) for reconciliation and returns
    /// `Ok(())`, not an error — the funds are already escrowed on-chain against
    /// the topped-up channel, so failing here would not unwind them.
    pub async fn top_up(&self, provider_addr: Address, additional: U256) -> Result<()> {
        decdn_client_pull::buyer_channel::top_up(
            &self.contract,
            self.store.as_ref(),
            provider_addr,
            additional,
        )
        .await
        // This manual entry point (used by tests / operator tooling) does not
        // grade the escrowed-but-untracked outcomes the way the background refill
        // does — `top_up` already logs them; drop the outcome to keep `Result<()>`.
        .map(|_| ())
    }
}

impl<P: Provider + Clone + 'static> std::fmt::Debug for BuyerChannelService<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuyerChannelService")
            .field("address", self.contract.address())
            .field("self_address", &self.self_address)
            .finish_non_exhaustive()
    }
}

/// Object-safe seam over [`BuyerChannelService::open_or_reuse_channel`] (#831).
///
/// `BuyerChannelService` is generic over the alloy [`Provider`], but the
/// node-to-node pull origin ([`crate::node_origin::NodeOrigin`]) is stored as an
/// `Arc<dyn Origin>` and so cannot itself be generic. This trait erases the
/// provider type so the origin can hold the bootstrapped service behind an
/// `Arc<dyn ChannelOpener>` and open (or reuse) a buyer channel to an upstream
/// provider on a cache-miss pull.
#[async_trait::async_trait]
pub trait ChannelOpener: Send + Sync + std::fmt::Debug {
    /// Open or reuse a buyer payment channel to `provider_addr`, funding a new
    /// channel with `deposit_hint` (ignored on reuse). See
    /// [`BuyerChannelService::open_or_reuse_channel`] for the full contract.
    async fn open_or_reuse_channel(
        &self,
        provider_addr: Address,
        deposit_hint: U256,
        budget: Duration,
    ) -> Result<ChannelContext>;

    /// Persist the cumulative voucher totals paid on `provider_addr`'s channel so
    /// a later reuse or a restart resumes at the right `nonce` / `bytes` /
    /// `amount` (#852); `channel_id` is the channel the totals were signed
    /// against. See [`BuyerChannelService::record_progress`].
    ///
    /// A `channel_id` that no longer matches the persisted row (the slot was
    /// replaced by a newer open) is a non-error stale write: implementations
    /// MUST skip it and return `Ok(())` rather than clobber the replacement.
    ///
    /// # Errors
    ///
    /// Errors if no channel is tracked for `provider_addr`, if the totals would
    /// regress the stored state, or on store write failure.
    fn record_progress(
        &self,
        provider_addr: Address,
        channel_id: ChannelId,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<()>;

    /// Retire the tracked channel for `provider_addr` so the next pull opens a fresh
    /// one, IF the tracked row is still `channel_id` (#1145 review).
    ///
    /// Called when an upstream's voucher rejection proves the channel can never pay
    /// again: the deposit is spent, it expired, or our watermark has desynced from
    /// the nonce the upstream committed. Without this the channel is simply handed
    /// back on the next miss — the reuse fast path gates only on expiry — so the provider
    /// stays top-ranked and unable to serve a byte until the channel expires.
    ///
    /// Compare-and-delete on `channel_id`, for the same reason
    /// [`Self::record_progress`] is: a concurrent open may already have replaced the
    /// row, and retiring a channel we never used would strand its deposit.
    ///
    /// Returns whether a row was actually retired. Retiring the LOCAL row does not
    /// touch on-chain state — the deposit remains escrowed and is recovered by the
    /// settlement sweep / `reclaimExpired`, exactly as for any other channel we stop
    /// using.
    ///
    /// # Errors
    ///
    /// On store write failure.
    fn retire_channel(&self, provider_addr: Address, channel_id: ChannelId) -> Result<bool>;

    /// The on-chain expiry (Unix seconds) of `provider_addr`'s currently-tracked channel,
    /// or `None` if none is tracked or the implementation does not model expiry.
    ///
    /// Bounds wedged-provider suppression to the channel's real lifetime (#1145 review): a
    /// channel that rejected our voucher on a terminal reason is unusable until it expires and
    /// the reclaim sweep frees the provider for a fresh open, so that expiry is the horizon
    /// past which the provider becomes worth ranking again. Defaults to `None` (no expiry
    /// modelled) so an implementation that does not track channels need not override it — a
    /// `None` horizon simply falls back to the per-`(peer, hash)` suppression.
    fn channel_expiry(&self, _provider_addr: Address) -> Option<u64> {
        None
    }
}

#[async_trait::async_trait]
impl<P: Provider + Clone + 'static> ChannelOpener for BuyerChannelService<P> {
    async fn open_or_reuse_channel(
        &self,
        provider_addr: Address,
        deposit_hint: U256,
        budget: Duration,
    ) -> Result<ChannelContext> {
        BuyerChannelService::open_or_reuse_channel(self, provider_addr, deposit_hint, budget).await
    }

    fn record_progress(
        &self,
        provider_addr: Address,
        channel_id: ChannelId,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<()> {
        BuyerChannelService::record_progress(
            self,
            provider_addr,
            channel_id,
            nonce,
            bytes_delivered,
            amount,
        )
    }

    fn retire_channel(&self, provider_addr: Address, channel_id: ChannelId) -> Result<bool> {
        BuyerChannelService::retire_channel(self, provider_addr, channel_id)
    }

    fn channel_expiry(&self, provider_addr: Address) -> Option<u64> {
        // The wedged channel's row is KEPT (only it can reclaim the deposit), so its expiry is
        // readable straight from the store. An unreadable/absent row yields `None`, which falls
        // back to per-(peer, hash) suppression rather than guessing a horizon.
        self.store
            .get_by_provider(provider_addr)
            .ok()
            .flatten()
            .map(|state| state.expires_at)
    }
}

/// Background reclaim sweep: periodically reclaim the deposit of any tracked
/// buyer channel that has passed its on-chain expiry without the upstream
/// closing it, and finalize any buyer channel whose post-unilateral-close
/// dispute window has elapsed (#988). Best-effort — errors are logged, never
/// fatal.
///
/// The buyer settle pass lives here, in the ALWAYS-spawned reclaim loop, rather
/// than in the optional idle-reconcile loop: a `buyer_pending_settle_v1` entry
/// recorded by a unilateral close must keep draining even if node→node
/// pull-through (and thus the reconcile loop) is later disabled — otherwise the
/// deposit would strand until a manual `settleChannel`. Settling an
/// already-closed channel needs no dialing, so it does not depend on the
/// reconcile wiring.
async fn reclaim_loop<P: Provider + Clone>(
    contract: PaymentChannel::PaymentChannelInstance<P>,
    store: Arc<dyn BuyerChannelStore>,
    self_address: Address,
    failures: Arc<Mutex<HashMap<ChannelId, u32>>>,
    pending_store: Arc<dyn PendingSettleStore>,
    metrics: Arc<Metrics>,
) {
    let mut ticker = tokio::time::interval(RECLAIM_SWEEP_INTERVAL);
    // Skip the immediate first tick — bootstrap just ran and nothing is near
    // expiry yet (and it avoids a redundant load_all at startup). The settle
    // pass also waits one interval, matching the seller `sweeper_loop`.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        reclaim_once(&contract, &store, self_address, &failures, &metrics).await;
        settle_pass(
            &contract,
            &pending_store,
            unix_now(),
            SettleParty::Buyer,
            &metrics,
        )
        .await;
    }
}

/// Per-channel idle observation across reconcile sweeps (#972). In-memory only:
/// a restart re-seeds it, so a channel must be observed idle for
/// `idle_sweeps_threshold` *post-restart* sweeps before it is reconciled — a
/// safe bias (we never cooperatively close a channel that might still be in use).
#[derive(Debug, Clone, Copy)]
struct IdleObservation {
    /// The channel's voucher nonce at the last sweep that observed it.
    last_seen_nonce: U256,
    /// Consecutive sweeps with no nonce progress.
    stale_sweeps: u32,
}

/// Fold this sweep's observed `last_nonce` into the running idle tally for
/// `channel_id`, returning whether the channel is now idle enough to reconcile.
///
/// Any nonce progress since the last sweep resets the tally (the channel is in
/// active use). A first sighting is recorded but is never immediately idle. Pure
/// so the idle policy is unit-testable without a clock or a live channel.
fn observe_idle(
    obs: &mut HashMap<ChannelId, IdleObservation>,
    channel_id: ChannelId,
    last_nonce: U256,
    idle_sweeps_threshold: u32,
) -> bool {
    match obs.get_mut(&channel_id) {
        None => {
            obs.insert(
                channel_id,
                IdleObservation {
                    last_seen_nonce: last_nonce,
                    stale_sweeps: 0,
                },
            );
            false
        }
        Some(entry) => {
            if last_nonce > entry.last_seen_nonce {
                entry.last_seen_nonce = last_nonce;
                entry.stale_sweeps = 0;
                false
            } else {
                entry.stale_sweeps = entry.stale_sweeps.saturating_add(1);
                entry.stale_sweeps >= idle_sweeps_threshold
            }
        }
    }
}

/// Background idle-reconcile sweep (#972): cooperatively close idle buyer
/// channels to reclaim their deposit early instead of waiting for expiry. Skips
/// channels in active use (recent voucher progress) and expired channels (the
/// reclaim sweep's job). Best-effort — a provider that declines or cannot be
/// reached is left for the next sweep or the expiry reclaim.
async fn reconcile_loop<P: Provider + Clone>(
    contract: PaymentChannel::PaymentChannelInstance<P>,
    store: Arc<dyn BuyerChannelStore>,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: Eip712Domain,
    config: BuyerReconcileConfig,
    pending_store: Arc<dyn PendingSettleStore>,
    metrics: Arc<Metrics>,
) {
    let mut obs: HashMap<ChannelId, IdleObservation> = HashMap::new();
    // Per-channel consecutive cooperative-close failure tally driving the
    // unilateral-close escalation (#988); in-memory and pruned each pass, same
    // posture as `obs` and the reclaim `failures` map.
    let mut close_failures: HashMap<ChannelId, u32> = HashMap::new();
    let mut ticker = tokio::time::interval(RECLAIM_SWEEP_INTERVAL);
    ticker.tick().await; // skip the immediate first tick (bootstrap just ran)
    loop {
        ticker.tick().await;
        reconcile_once(
            &contract,
            &store,
            &signer,
            &voucher_domain,
            &config,
            &mut obs,
            &mut close_failures,
            &pending_store,
            &metrics,
        )
        .await;
        // The buyer settle pass that finalizes these unilateral closes runs in
        // `reclaim_loop` (always spawned), not here — so pending entries keep
        // draining even if the reconcile loop is later disabled (#988).
    }
}

/// One idle-reconcile pass. Errors are logged per channel and never abort the
/// sweep; the observation map is pruned to the channels still eligible so it
/// cannot grow unbounded.
#[allow(clippy::too_many_arguments)]
async fn reconcile_once<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn BuyerChannelStore>,
    signer: &Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    config: &BuyerReconcileConfig,
    obs: &mut HashMap<ChannelId, IdleObservation>,
    close_failures: &mut HashMap<ChannelId, u32>,
    pending_store: &Arc<dyn PendingSettleStore>,
    metrics: &Arc<Metrics>,
) {
    let BuyerLoad { channels, skipped } = match store.load_all() {
        Ok(load) => load,
        Err(err) => {
            warn!(%err, "buyer reconcile sweep: failed to load channel state");
            return;
        }
    };
    metrics.buyer_channel_store_skipped_undecodable_records(skipped.len());
    let now = unix_now();
    let mut seen: HashSet<ChannelId> = HashSet::new();
    for st in &channels {
        // Expired channels are the reclaim sweep's job; a never-paid channel
        // (nonce 0) has no voucher to settle cooperatively — the provider would
        // decline — so it too waits for the expiry reclaim.
        if st.is_expired_at(now) || st.last_nonce.is_zero() {
            continue;
        }
        seen.insert(st.channel_id);
        if !observe_idle(obs, st.channel_id, st.last_nonce, RECONCILE_IDLE_SWEEPS) {
            continue;
        }
        reconcile_one(
            contract,
            store,
            signer,
            voucher_domain,
            config,
            obs,
            close_failures,
            pending_store,
            metrics,
            st,
        )
        .await;
    }
    // Drop observations + close tallies for channels gone this sweep (settled,
    // reclaimed, or replaced) so the maps track only currently-eligible channels.
    obs.retain(|id, _| seen.contains(id));
    close_failures.retain(|id, _| seen.contains(id));
}

/// Attempt cooperative close of one idle channel. All failure modes are logged
/// and swallowed — the expiry-reclaim sweep is the safety net.
// Linear guard-and-act sequence (resolve NodeId → dial+close → branch on each
// outcome, with the unreachable arms escalating to a unilateral close) with
// per-arm logging; splitting it obscures the flow, mirroring `try_reclaim`.
#[allow(
    clippy::too_many_arguments,
    clippy::cognitive_complexity,
    clippy::too_many_lines
)]
async fn reconcile_one<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn BuyerChannelStore>,
    signer: &Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    config: &BuyerReconcileConfig,
    obs: &mut HashMap<ChannelId, IdleObservation>,
    close_failures: &mut HashMap<ChannelId, u32>,
    pending_store: &Arc<dyn PendingSettleStore>,
    metrics: &Arc<Metrics>,
    st: &BuyerChannelState,
) {
    // Resolve the provider's operator address back to a dialable NodeId. A
    // provider no longer registered (deregistered / gone) is unreachable for a
    // cooperative close — but the buyer holds its own voucher, so rather than
    // wait out the (default 90-day) expiry it `closeChannel`s the idle channel
    // unilaterally and settles after the dispute window (#988).
    let Some(node_id) = config.resolver.node_id_for(&st.provider) else {
        debug!(
            channel_id = %st.channel_id, provider = %st.provider,
            "reconcile: provider not registered; closing idle channel unilaterally"
        );
        escalate_unilateral_close(
            contract,
            store,
            signer,
            voucher_domain,
            pending_store,
            metrics,
            st,
            obs,
            close_failures,
        )
        .await;
        return;
    };
    let Ok(public_key) = PublicKey::from_bytes(node_id.as_bytes()) else {
        // A malformed on-chain registration is a persistent fault. Drop the idle
        // tally so we back off (~24h) instead of re-warning every hourly sweep;
        // the expiry-reclaim sweep is still the eventual safety net.
        obs.remove(&st.channel_id);
        close_failures.remove(&st.channel_id);
        warn!(
            channel_id = %st.channel_id,
            "reconcile: registered NodeId is not a valid public key; backing off"
        );
        return;
    };
    let authorized = AuthorizedWatermark {
        amount: st.last_amount,
        nonce: st.last_nonce,
        bytes_delivered: st.last_bytes_delivered,
    };
    let outcome = cooperative_close(
        &config.endpoint,
        EndpointAddr::new(public_key),
        contract,
        st.channel_id,
        st.provider,
        st.token,
        authorized,
        signer,
        voucher_domain,
        RECONCILE_DIAL_TIMEOUT,
    )
    .await;
    match outcome {
        Ok(CooperativeCloseOutcome::Settled) => {
            obs.remove(&st.channel_id);
            close_failures.remove(&st.channel_id);
            metrics.buyer_reconcile_settled();
            if let Err(err) = store.forget_if_channel(st.provider, st.channel_id) {
                warn!(
                    channel_id = %st.channel_id, %err,
                    "reconcile: channel cooperatively closed on-chain but clearing the local \
                     record failed; it will be retried and no-op against the closed channel"
                );
            } else {
                info!(
                    channel_id = %st.channel_id, provider = %st.provider,
                    "reconcile: idle buyer channel cooperatively closed; deposit reclaimed early"
                );
            }
        }
        Ok(CooperativeCloseOutcome::Declined) => {
            // A decline is sticky (the provider is reachable but has no channel /
            // no accepted voucher) — NOT unreachability, so do not escalate to a
            // unilateral close. Back off the idle tally (~24h) so we don't re-dial
            // every hourly sweep; the expiry-reclaim sweep remains the net.
            obs.remove(&st.channel_id);
            close_failures.remove(&st.channel_id);
            debug!(
                channel_id = %st.channel_id, provider = %st.provider,
                "reconcile: provider declined cooperative close; backing off, leaving for expiry reclaim"
            );
        }
        Ok(CooperativeCloseOutcome::Reverted) => {
            // A revert is persistent until something on-chain changes (the
            // provider is reachable; a unilateral close would revert too). Back
            // off the idle tally (~24h); the expiry-reclaim sweep remains the net.
            obs.remove(&st.channel_id);
            close_failures.remove(&st.channel_id);
            warn!(
                channel_id = %st.channel_id, provider = %st.provider,
                "reconcile: cooperativeClose reverted on-chain; backing off, leaving for expiry reclaim"
            );
        }
        Err(err) => {
            // A dial/waiver-phase failure is timeout-shaped unreachability: the
            // registration lingers but the provider does not answer. Tolerate a
            // few sweeps (transient blip) before escalating to a unilateral close
            // once it has failed `RECONCILE_CLOSE_ESCALATION_THRESHOLD` in a row.
            match record_close_failure(
                close_failures,
                st.channel_id,
                RECONCILE_CLOSE_ESCALATION_THRESHOLD,
            ) {
                CloseEscalation::Escalate { consecutive } => {
                    debug!(
                        channel_id = %st.channel_id, provider = %st.provider, consecutive,
                        err = %sanitize_rpc_display(&err),
                        "reconcile: cooperative close failed repeatedly (provider unreachable); \
                         closing idle channel unilaterally"
                    );
                    escalate_unilateral_close(
                        contract,
                        store,
                        signer,
                        voucher_domain,
                        pending_store,
                        metrics,
                        st,
                        obs,
                        close_failures,
                    )
                    .await;
                }
                CloseEscalation::Wait { consecutive } => {
                    debug!(
                        channel_id = %st.channel_id, provider = %st.provider, consecutive,
                        err = %sanitize_rpc_display(&err),
                        "reconcile: cooperative close failed (provider unreachable?); will retry, \
                         escalating to unilateral close after {RECONCILE_CLOSE_ESCALATION_THRESHOLD}"
                    );
                }
            }
        }
    }
}

/// Whether a run of dial/waiver-phase cooperative-close failures has crossed the
/// unilateral-close escalation threshold (#988).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CloseEscalation {
    /// Close the channel unilaterally now — the provider has been unreachable for
    /// `consecutive` sweeps.
    Escalate { consecutive: u32 },
    /// Keep waiting: only `consecutive` (< threshold) failures so far, still
    /// inside the transient-blip tolerance.
    Wait { consecutive: u32 },
}

/// Fold one dial/waiver-phase cooperative-close failure into the per-channel
/// consecutive-failure tally and decide whether to escalate to a unilateral
/// close. Pure so the escalation policy is unit-testable without a live contract
/// or network, mirroring [`record_reclaim_outcome`]. A provider that *responds*
/// (settled / declined / reverted) or deregisters is handled by the caller and
/// clears the tally via `close_failures.remove`, so this only ever counts up.
fn record_close_failure(
    close_failures: &mut HashMap<ChannelId, u32>,
    channel_id: ChannelId,
    threshold: u32,
) -> CloseEscalation {
    let consecutive = {
        let tally = close_failures.entry(channel_id).or_insert(0);
        *tally = tally.saturating_add(1);
        *tally
    };
    if consecutive >= threshold {
        CloseEscalation::Escalate { consecutive }
    } else {
        CloseEscalation::Wait { consecutive }
    }
}

/// Treat the provider as unreachable and `closeChannel` the idle channel
/// unilaterally at the buyer's own persisted watermark (#988), recording the
/// channel for post-dispute-window settlement. Counts the escalation as
/// timeout-shaped unreachability (#989) and, on a landed close, drops the local
/// idle/close tallies and the buyer channel record (the pending-settle entry now
/// owns the lifecycle, mirroring the seller's close-then-forget). A failed close
/// leaves everything in place so the next sweep (or the expiry reclaim) retries.
#[allow(clippy::too_many_arguments)]
async fn escalate_unilateral_close<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn BuyerChannelStore>,
    signer: &Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    pending_store: &Arc<dyn PendingSettleStore>,
    metrics: &Arc<Metrics>,
    st: &BuyerChannelState,
    obs: &mut HashMap<ChannelId, IdleObservation>,
    close_failures: &mut HashMap<ChannelId, u32>,
) {
    metrics.buyer_unilateral_close_unreachable();
    if close_unilateral(contract, signer, voucher_domain, metrics, st).await {
        // Close landed. Record the settle obligation and retire the local state
        // only if that handoff succeeded (see `record_then_retire`).
        record_then_retire(contract, store, pending_store, st, obs, close_failures).await;
        return;
    }
    // The close did not land. Reconcile against on-chain status before deciding
    // to retry: a deterministic revert here usually means the channel is ALREADY
    // `Closing`/`Closed` (the provider closed it, or a prior attempt of ours
    // landed but we missed the receipt). Retrying a `closeChannel` against a
    // non-`Open` channel just reverts every sweep for up to the 90-day expiry —
    // wasted RPC, and gas if the revert isn't caught at estimation. Only a
    // still-`Open` channel (a genuine transient close failure) is left for the
    // next sweep to retry.
    reconcile_failed_close(contract, store, pending_store, st, obs, close_failures).await;
}

/// Record the buyer's settle obligation for an on-chain-`Closing` channel and,
/// **only if that handoff succeeded**, retire the local record + idle/close
/// tallies (the durable pending-settle entry now owns the channel's lifecycle).
///
/// If recording fails (a transient `getChannel`/store fault) the local record is
/// KEPT: `settle_pass` drains only durable `PendingSettleStore` entries, so
/// dropping the record here would forfeit automatic recovery and force a manual
/// `settleChannel`. Keeping it lets the next reconcile sweep retry — its
/// `closeChannel` reverts against the now-`Closing` channel and routes back
/// through [`reconcile_failed_close`], which re-attempts this handoff. (The
/// seller path forgets unconditionally because its #839 closing-backfill
/// re-derives lost obligations on reboot; the buyer has no such backfill, so it
/// relies on keeping the record instead.)
async fn record_then_retire<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn BuyerChannelStore>,
    pending_store: &Arc<dyn PendingSettleStore>,
    st: &BuyerChannelState,
    obs: &mut HashMap<ChannelId, IdleObservation>,
    close_failures: &mut HashMap<ChannelId, u32>,
) {
    if !record_pending_after_unilateral_close(contract, pending_store, st.channel_id).await {
        // Obligation not durably recorded — keep the record + tallies so the
        // next sweep retries the handoff. The `error!` was already emitted.
        return;
    }
    obs.remove(&st.channel_id);
    close_failures.remove(&st.channel_id);
    forget_after_close(store, st);
}

/// Drop the local buyer record after a unilateral close (CAS, so a concurrent
/// re-open for this provider is never clobbered) — the pending-settle entry now
/// owns the channel's lifecycle, exactly as the seller forgets a channel after
/// closing it ahead of expiry.
fn forget_after_close(store: &Arc<dyn BuyerChannelStore>, st: &BuyerChannelState) {
    if let Err(err) = store.forget_if_channel(st.provider, st.channel_id) {
        warn!(
            channel_id = %st.channel_id, %err,
            "reconcile: unilateral close landed but clearing the local record failed; it will be \
             retried and no-op against the closing channel"
        );
    }
}

/// After a unilateral `closeChannel` attempt returned `false`, read the on-chain
/// status to avoid an every-sweep revert loop against an already-closed channel:
/// - `Closing` — a close already landed (ours, with a missed receipt, or a
///   co-close). Record the settle obligation, retiring the local record only if
///   that handoff succeeds (via [`record_then_retire`]); a failed handoff keeps
///   the record so a later sweep retries.
/// - `Closed` — already finalized, nothing left to settle; retire the record.
/// - `Open` — a genuine transient close failure; keep the record + tallies so
///   the next sweep retries.
/// - read error — keep everything and retry next sweep.
// Linear guard-and-act sequence (getChannel → status branch) with per-arm
// logging; splitting it obscures the flow, mirroring `try_reclaim`.
#[allow(clippy::cognitive_complexity)]
async fn reconcile_failed_close<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn BuyerChannelStore>,
    pending_store: &Arc<dyn PendingSettleStore>,
    st: &BuyerChannelState,
    obs: &mut HashMap<ChannelId, IdleObservation>,
    close_failures: &mut HashMap<ChannelId, u32>,
) {
    let ch = match contract.getChannel(st.channel_id).call().await {
        Ok(ch) => ch,
        Err(err) => {
            debug!(
                channel_id = %st.channel_id, err = %sanitize_rpc_display(&err),
                "reconcile: post-close-failure getChannel failed; retrying next sweep"
            );
            return;
        }
    };
    match ch.status {
        // A prior close landed (ours with a missed receipt, or a co-close):
        // record the settle obligation, retiring the record only if it sticks.
        PaymentChannel::Status::Closing => {
            info!(
                channel_id = %st.channel_id, provider = %st.provider,
                "reconcile: channel already Closing on-chain (a prior close landed); recording \
                 settle obligation"
            );
            record_then_retire(contract, store, pending_store, st, obs, close_failures).await;
        }
        // Already finalized — nothing left to settle; stop re-submitting against
        // a non-Open channel by retiring the local record + idle/close tallies.
        PaymentChannel::Status::Closed => {
            info!(
                channel_id = %st.channel_id, provider = %st.provider,
                "reconcile: channel already Closed on-chain; retiring the local record"
            );
            obs.remove(&st.channel_id);
            close_failures.remove(&st.channel_id);
            forget_after_close(store, st);
        }
        // `Open` (or any other status) means the close genuinely failed
        // transiently — keep the record + tallies so the next sweep retries.
        _ => {}
    }
}

/// Submit a unilateral `closeChannel` for `st` signed over the buyer's own
/// highest persisted watermark. Returns `true` only when the close landed
/// on-chain (the caller then records the settle obligation via
/// [`record_then_retire`]). Routes the on-chain outcome to the buyer close
/// metrics (#989): a landed close to `buyer_unilateral_close_ok`, an RPC/receipt
/// error or revert to `buyer_unilateral_close_rpc_failure`.
// Linear guard-and-act sequence (sign → send → receipt → branch on status) with
// per-arm metric + logging; splitting it obscures the flow, mirroring
// `try_reclaim` and the seller `send_close`.
#[allow(clippy::cognitive_complexity)]
async fn close_unilateral<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    client_signer: &Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    metrics: &Arc<Metrics>,
    st: &BuyerChannelState,
) -> bool {
    // Sign our own voucher over the persisted watermark — the contract recovers
    // the signature against the channel's pinned `voucherSigner`, never
    // `channel.client`. The node opens self-signed channels, so its own key is
    // that signer. Honest by construction: we close at the highest amount we
    // already authorized, and the dispute window protects the absent provider
    // against a stale nonce.
    let voucher = match (Voucher {
        channel_id: st.channel_id,
        amount: st.last_amount,
        nonce: st.last_nonce,
        bytes_delivered: st.last_bytes_delivered,
        token: st.token,
    })
    .sign(client_signer.as_ref(), voucher_domain)
    {
        Ok(v) => v,
        Err(err) => {
            // A signing failure is a LOCAL signer/key fault, not network
            // unreachability or an on-chain submission failure — so it is
            // counted in neither #989 bucket (the escalation that brought us
            // here already ticked `buyer_unilateral_close_unreachable`). It is
            // also near-impossible for a valid in-memory key.
            warn!(
                channel_id = %st.channel_id, %err,
                "reconcile: unilateral close voucher signing failed (local signer fault)"
            );
            return false;
        }
    };
    let sig = Bytes::from(voucher.signature.as_bytes().to_vec());
    let sent = contract
        .closeChannel(
            st.channel_id,
            st.last_amount,
            st.last_nonce,
            st.last_bytes_delivered,
            sig,
        )
        .send()
        .await;
    let outcome = send_and_await_receipt(sent, None).await;
    match &outcome {
        TxOutcome::Landed(receipt) => info!(
            channel_id = %st.channel_id, provider = %st.provider,
            tx = %receipt.transaction_hash,
            "reconcile: unilateral closeChannel landed (dispute window open); will settle after window"
        ),
        TxOutcome::Reverted(receipt) => warn!(
            channel_id = %st.channel_id, tx = %receipt.transaction_hash,
            "reconcile: unilateral closeChannel reverted on-chain (channel may already be \
             closing/closed); caller reconciles on-chain status"
        ),
        TxOutcome::SendErr(err) => warn!(
            channel_id = %st.channel_id, err = %sanitize_rpc_display(err),
            "reconcile: unilateral closeChannel send failed; leaving for next sweep / expiry reclaim"
        ),
        TxOutcome::ReceiptErr(err) => warn!(
            channel_id = %st.channel_id, err = %sanitize_rpc_display(err),
            "reconcile: unilateral closeChannel receipt failed; leaving for next sweep / expiry reclaim"
        ),
        // No receipt timeout is supplied above, so this arm is unreachable; it
        // folds into the same `rpc_failure` retry bucket as the other failures
        // rather than panicking, per the workspace anti-panic policy.
        TxOutcome::Timeout => warn!(
            channel_id = %st.channel_id,
            "reconcile: unilateral closeChannel receipt wait elapsed; leaving for next sweep / expiry reclaim"
        ),
    }
    record_unilateral_close_outcome(metrics, outcome.kind())
}

/// Record the #989 two-bucket metric for a completed unilateral-close
/// submission and report whether it succeeded: a landed receipt ticks
/// `buyer_unilateral_close_ok` and returns `true`; every other terminal —
/// revert, send/receipt error, or receipt-wait timeout — ticks
/// `buyer_unilateral_close_rpc_failure` and returns `false` (the caller
/// reconciles on-chain status on the next sweep). Split out (mirroring
/// `record_settle_receipt_outcome`) so the ok-vs-failure mapping is
/// unit-testable without a live provider.
fn record_unilateral_close_outcome(metrics: &Arc<Metrics>, kind: TxKind) -> bool {
    match kind {
        TxKind::Landed => {
            metrics.buyer_unilateral_close_ok();
            true
        }
        TxKind::Reverted | TxKind::SendErr | TxKind::ReceiptErr | TxKind::Timeout => {
            metrics.buyer_unilateral_close_rpc_failure();
            false
        }
    }
}

/// Re-read the just-closed channel for the `disputeDeadline` it set and persist
/// a [`PendingSettle`] entry so the buyer settle sweep can `settleChannel` (and
/// reclaim the deposit refund) once the window elapses (#988). Returns whether
/// the obligation was durably recorded.
///
/// A `false` return (a transient `getChannel`/store fault) is the caller's
/// signal to KEEP the local record so the next reconcile sweep retries this
/// handoff — see [`record_then_retire`]. The `error!`s flag the transient
/// failure and name the manual `settleChannel` fallback for the case where
/// reconcile is disabled before the retry lands (the buyer has no seller-style
/// #839 closing-backfill). The channel is already `Closing`, so the
/// expiry-reclaim path (which needs `Open`) is not the fallback.
async fn record_pending_after_unilateral_close<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    pending_store: &Arc<dyn PendingSettleStore>,
    channel_id: ChannelId,
) -> bool {
    let settle_after = match contract.getChannel(channel_id).call().await {
        Ok(ch) => ch.disputeDeadline,
        Err(err) => {
            error!(
                err = %sanitize_rpc_display(&err), %channel_id,
                "reconcile: post-close getChannel failed; settle obligation NOT recorded — keeping \
                 the local record to retry next sweep. If reconcile is disabled before then, call \
                 settleChannel(<channel_id>) manually after the dispute window to reclaim the refund"
            );
            return false;
        }
    };
    let entry = PendingSettle {
        channel_id,
        settle_after,
    };
    if let Err(err) = pending_store.record_pending(&entry) {
        error!(
            %err, %channel_id, settle_after,
            "reconcile: failed to persist buyer pending-settle entry; keeping the local record to \
             retry next sweep. If reconcile is disabled before then, call settleChannel(<channel_id>) \
             manually after the dispute window to reclaim the refund"
        );
        return false;
    }
    debug!(%channel_id, settle_after, "reconcile: recorded buyer channel for post-dispute settlement");
    true
}

/// Outcome of one [`try_reclaim`] attempt, consumed by [`record_reclaim_outcome`]
/// to drive the consecutive-failure escalation (#906).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReclaimOutcome {
    /// The local record was forgotten this pass — the deposit was reclaimed, or
    /// the record was dropped as bogus / already-closed. Nothing left to escalate.
    Resolved,
    /// The reclaim attempt failed; the record was left in place for a later sweep.
    Failed,
}

/// Whether a reclaim failure has crossed the escalation threshold this pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReclaimEscalation {
    None,
    Escalate { consecutive: u32 },
}

/// Update the per-channel consecutive-failure tally for `channel_id` given this
/// pass's `outcome`, and decide whether to raise the per-channel `error!` (#906).
/// A `Resolved` outcome clears the tally (the deposit is recovered or the record
/// is gone). A `Failed` outcome increments it and escalates once it reaches
/// `threshold` — and on every subsequent failed sweep, so a *sustained* failure
/// keeps surfacing an `error!` rather than going quiet after the first crossing.
/// This governs only the `error!`; the `buyer_reclaim_failures` metric is bumped
/// by the caller on every `Failed` outcome, independent of this threshold. Pure
/// so the escalation policy is unit-testable without a live contract.
fn record_reclaim_outcome(
    failures: &mut HashMap<ChannelId, u32>,
    channel_id: ChannelId,
    outcome: ReclaimOutcome,
    threshold: u32,
) -> ReclaimEscalation {
    match outcome {
        ReclaimOutcome::Resolved => {
            failures.remove(&channel_id);
            ReclaimEscalation::None
        }
        ReclaimOutcome::Failed => {
            let consecutive = failures.entry(channel_id).or_insert(0);
            *consecutive = consecutive.saturating_add(1);
            if *consecutive >= threshold {
                ReclaimEscalation::Escalate {
                    consecutive: *consecutive,
                }
            } else {
                ReclaimEscalation::None
            }
        }
    }
}

/// Drop failure tallies for channels not attempted in the latest sweep — they
/// were reclaimed, replaced by a newer open, or are no longer past expiry — so
/// the map tracks only currently-failing channels and cannot grow unbounded
/// (#906). `seen` is the set of channel ids this pass attempted.
fn prune_reclaim_failures(failures: &mut HashMap<ChannelId, u32>, seen: &HashSet<ChannelId>) {
    failures.retain(|id, _| seen.contains(id));
}

/// One reclaim-sweep pass. Errors are logged per channel and never abort the
/// sweep. Bumps the `buyer_reclaim_failures` metric on every failed attempt and
/// tracks consecutive per-channel failures in `failures` so a *persistent*
/// failure additionally escalates its per-attempt `warn!` to an `error!` once it
/// crosses `RECLAIM_ESCALATION_THRESHOLD` (#906); the map is pruned each pass to
/// the channels still expired so it cannot grow unbounded.
async fn reclaim_once<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn BuyerChannelStore>,
    self_address: Address,
    failures: &Arc<Mutex<HashMap<ChannelId, u32>>>,
    metrics: &Arc<Metrics>,
) {
    let BuyerLoad { channels, skipped } = match store.load_all() {
        Ok(load) => load,
        Err(err) => {
            warn!(%err, "buyer reclaim sweep: failed to load channel state");
            return;
        }
    };
    metrics.buyer_channel_store_skipped_undecodable_records(skipped.len());
    let now = unix_now();
    let mut seen: HashSet<ChannelId> = HashSet::new();
    for st in &channels {
        if !st.is_expired_at(now) {
            continue;
        }
        seen.insert(st.channel_id);
        let outcome = try_reclaim(contract, store, self_address, st).await;
        if outcome == ReclaimOutcome::Failed {
            metrics.buyer_reclaim_failure();
        }
        // Lock scoped to the synchronous tally update only: the guard's block
        // contains no `.await`, so it is never held across a suspension point
        // (clippy `await_holding_lock`). A poisoned lock is recovered rather than
        // propagated — the map carries no cross-element invariant — mirroring
        // `InFlightOpenGuard`'s `Drop` (its `claim` deliberately does the opposite).
        let escalation = {
            let mut guard = failures
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            record_reclaim_outcome(
                &mut guard,
                st.channel_id,
                outcome,
                RECLAIM_ESCALATION_THRESHOLD,
            )
        };
        if let ReclaimEscalation::Escalate { consecutive } = escalation {
            error!(
                channel_id = %st.channel_id,
                provider = %st.provider,
                deposit = %st.deposit,
                consecutive,
                "buyer reclaim has failed {consecutive} consecutive sweeps for this channel; \
                 the refundable deposit may be unrecovered (check this node's gas balance and \
                 RPC) or the local channel record could not be cleared (check the channel \
                 store) — reconcile manually"
            );
        }
    }
    let mut guard = failures
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    prune_reclaim_failures(&mut guard, &seen);
}

/// The body of a detached channel open (#1143): confirm no usable channel exists,
/// rotate an expired one, then `openChannel` and persist.
///
/// Runs in a `tokio::spawn`, holding the provider's in-flight slot for its whole
/// life, so it survives any individual caller giving up on it.
///
/// Returns `Ok(())` rather than the `ChannelContext`: a successful open persists to
/// the store, and every waiter reads it back from there. That keeps "I opened it"
/// and "someone else opened it" on one code path, which is also what makes the
/// no-op arm below correct.
// Linear guard-and-act sequence (reuse re-check → rotate/reclaim → open → persist);
// the tracing macros on each failure leg inflate the metric past threshold, as on
// `try_reclaim`. Splitting would scatter one flow across helpers.
#[allow(
    clippy::too_many_arguments,
    clippy::cognitive_complexity,
    clippy::too_many_lines
)]
async fn run_open<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn BuyerChannelStore>,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    token: Address,
    self_address: Address,
    provider_addr: Address,
    deposit: U256,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    // Re-check the store now that we hold the slot. The caller's fast-path miss
    // happened BEFORE we took the map lock, so a previous open for this provider
    // could have landed and persisted in between — and this task would otherwise
    // escrow a second deposit for a provider that already has a live channel. The
    // map lock alone cannot close that window: the prior open removes its map entry
    // on completion, so a caller arriving right after sees an empty map and a
    // populated store.
    // Every failure leg below reports for itself — `warn!`/`error!`, the failure
    // counter, and the `OpenReported` marker — for the same reason the `open_channel`
    // leg does: this runs detached, so by the time it fails there may be nobody left
    // waiting to observe the `Err`. A leg that only bubbles up an error is silent
    // in the common case, not the rare one.
    let existing = match store.get_by_provider(provider_addr) {
        Ok(existing) => existing,
        Err(err) => {
            // A store read fault (corrupt page, fd exhaustion, a full or unwritable
            // data_dir) makes this node unable to open a channel to ANY provider —
            // i.e. unable to pay for anything. It must never be silent.
            error!(
                provider = %provider_addr,
                %err,
                "buyer channel store read failed under the open slot; cannot open a channel to \
                 this provider"
            );
            metrics.node_pull_channel_open_failure();
            return Err(anyhow::Error::new(err))
                .context("look up existing buyer channel under the open slot")
                .context(OpenReported);
        }
    };
    if let Some(existing) = existing {
        if !existing.is_expired_at(unix_now()) {
            debug!(
                provider = %provider_addr,
                channel_id = %existing.channel_id,
                "a live buyer channel appeared while claiming the open slot; not opening a second"
            );
            return Ok(());
        }
        // Expired. Reclaim its deposit BEFORE rotating: the store is provider-keyed,
        // so opening a replacement would overwrite the expired record, and the
        // reclaim sweep (which iterates `load_all`) would never see it again —
        // silently abandoning a refundable deposit (10 USDC default, plus top-ups).
        debug!(
            provider = %provider_addr,
            channel_id = %existing.channel_id,
            "tracked buyer channel expired; reclaiming before opening a replacement"
        );
        // Outcome intentionally ignored, and the guarantee that makes that safe is
        // NOT the caller: this runs in a detached task, so the retryable error below
        // may reach nobody (#1143). What actually covers a failure here is
        // `try_reclaim`'s own internal `warn!` on every leg, plus the hourly reclaim
        // sweep, which retries and carries the consecutive-failure escalation (#906).
        // The bail below still prevents an open from overwriting an unreclaimed row —
        // it just cannot be relied on to REPORT anything.
        let _ = try_reclaim(contract, store, self_address, &existing).await;
        let after_reclaim = match store.get_by_provider(provider_addr) {
            Ok(after) => after,
            Err(err) => {
                error!(
                    provider = %provider_addr,
                    %err,
                    "buyer channel store read failed re-checking the expired channel after \
                     reclaim; cannot rotate this provider's channel"
                );
                metrics.node_pull_channel_open_failure();
                return Err(anyhow::Error::new(err))
                    .context("re-check expired channel after reclaim")
                    .context(OpenReported);
            }
        };
        if after_reclaim.is_some_and(|s| s.channel_id == existing.channel_id) {
            // The reclaim did not clear the row, so we must not open a replacement
            // (it would overwrite the provider-keyed record and abandon the old
            // deposit). Until the sweep clears it, this provider cannot be opened at
            // ALL — and the caller is USUALLY long gone: when `try_reclaim` reaches its
            // receipt wait it blocks for RECLAIM_RECEIPT_TIMEOUT (minutes) against a
            // caller budget of seconds. Not always, though: a `getChannel` RPC error or a
            // failed `send` returns `Failed` in milliseconds, well inside the budget, with
            // the caller still waiting. So report it HERE — otherwise the common case is
            // invisible and the operator sees only `node_pull_channel_open_pending`
            // climbing, whose meaning ("a slow L2 / stuck nonce") is the wrong diagnosis
            // entirely — and mark it `OpenReported` so the caller who IS still there does
            // not count it twice.
            warn!(
                provider = %provider_addr,
                channel_id = %existing.channel_id,
                "expired buyer channel is not yet reclaimable; this provider cannot be opened \
                 until the reclaim sweep clears it"
            );
            metrics.node_pull_channel_open_failure();
            return Err(anyhow::anyhow!(
                "expired buyer channel {} (provider {provider_addr}) is not yet reclaimable; \
                 retry after the reclaim sweep clears it",
                existing.channel_id
            )
            .context(OpenReported));
        }
    }

    // The openChannel tx, the authoritative ChannelOpened-from-receipt decode, and
    // the state construction are the shared kernel (#940). Its receipt wait is
    // UNBOUNDED by design — see `open_channel`'s doc: abandoning an escrowing tx is
    // how a second deposit gets opened against the same provider. This task holds
    // the provider's slot for exactly as long as that wait runs, which is what makes
    // it safe: no caller is blocked (they time out on their own budget), and no
    // second open can start behind it.
    //
    // What stays node-specific: from the moment the kernel returns, the deposit is
    // escrowed on-chain, so a failure to persist locally leaves it tracked ONLY on
    // chain. The reclaim sweep iterates `load_all` and so never sees an unpersisted
    // channel; escalate to `error!` with the open tx for manual reconcile (the
    // bootstrap reconciliation scan, #763, also adopts it on the next restart).
    let opened = open_channel(
        contract,
        signer,
        voucher_domain,
        token,
        self_address,
        provider_addr,
        deposit,
        // ZERO => self-signing (funder signs); publisher-pays passes a delegate via `channel open`.
        Address::ZERO,
    )
    .await;

    let OpenedChannel { state, tx, .. } = match opened {
        Ok(opened) => opened,
        Err(err) => {
            // Report HERE, not at the caller (#1143). This runs in a detached task,
            // and by the time an open fails, every caller that was waiting on it may
            // already have timed out and walked away with `ChannelOpenPending` — in
            // which case nobody is left to observe the `Err`. Before this, a failed
            // open could produce zero log lines and zero metric increments, and the
            // legs that go silent are the ones that matter most: a reverted or
            // under-funded open means this node cannot pay for anything.
            let reason = err.downcast_ref::<ChannelOpenFailureReason>().copied();
            warn!(
                provider = %provider_addr,
                %deposit,
                reason = reason.map_or("unclassified", ChannelOpenFailureReason::as_label),
                err = %format!("{err:#}"),
                "buyer channel open failed"
            );
            metrics.node_pull_channel_open_failure();
            if let Some(reason) = reason {
                metrics.channel_open_failure_by_reason(reason);
            }
            return Err(err.context(OpenReported));
        }
    };

    if let Err(err) = store.record(&state) {
        error!(
            %tx,
            provider = %provider_addr,
            channel_id = %state.channel_id,
            %deposit,
            %err,
            "buyer channel opened on-chain (deposit escrowed) but persisting the local record \
             failed; the deposit is UNTRACKED and will not be auto-reclaimed — reconcile \
             manually against the tx"
        );
        metrics.node_pull_channel_open_failure();
        return Err(anyhow::Error::new(err))
            .context("persist newly-opened buyer channel")
            .context(OpenReported);
    }
    Ok(())
}

/// Reclaim one expired channel's deposit (or drop the record if the upstream
/// already closed it). All failure modes are logged and swallowed.
// Linear guard-and-act sequence (getChannel → status branch → reclaim →
// forget) with nested receipt matches; splitting obscures the flow.
#[allow(clippy::cognitive_complexity)]
async fn try_reclaim<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn BuyerChannelStore>,
    self_address: Address,
    st: &BuyerChannelState,
) -> ReclaimOutcome {
    let ch = match contract.getChannel(st.channel_id).call().await {
        Ok(ch) => ch,
        Err(err) => {
            warn!(err = %sanitize_rpc_display(&err), channel_id = %st.channel_id, "buyer reclaim: getChannel failed");
            return ReclaimOutcome::Failed;
        }
    };
    // Ownership guard: `reclaimExpired` always refunds `channel.client`, never
    // the caller, so a record whose on-chain `client` is not us is either
    // corrupt or for an unknown channel (`getChannel` returns a zeroed struct,
    // client == 0). Reclaiming it would burn gas for someone else's refund —
    // drop the bogus record instead (CAS so a concurrent re-open survives).
    if ch.client != self_address {
        warn!(
            channel_id = %st.channel_id,
            provider = %st.provider,
            on_chain_client = %ch.client,
            "buyer reclaim: tracked channel's on-chain client is not this node; dropping bogus record"
        );
        return forget_reclaimed(store, st, "drop foreign/unknown record");
    }
    // If the upstream already closed/settled the channel, `reclaimExpired`
    // would revert — just drop our local record.
    if !matches!(ch.status, PaymentChannel::Status::Open) {
        return forget_reclaimed(store, st, "expired channel already closed on-chain");
    }

    // Bounded (#1143): `run_open` calls this on the rotate leg, INSIDE the
    // detached open task that holds the provider's in-flight slot. An unbounded
    // wait here would let a stuck `reclaimExpired` pin that slot for the life of
    // the process, wedging the provider — the very starvation #1143 removes.
    // Safe to bound precisely because a reclaim refunds rather than escrows: the
    // row stays, the hourly sweep retries, nothing is stranded.
    let outcome = send_and_await_receipt(
        contract.reclaimExpired(st.channel_id).send().await,
        Some(RECLAIM_RECEIPT_TIMEOUT),
    )
    .await;
    match &outcome {
        TxOutcome::Landed(_) => {}
        TxOutcome::Reverted(receipt) => warn!(
            channel_id = %st.channel_id,
            tx = %receipt.transaction_hash,
            "reclaimExpired reverted on-chain; leaving record for retry"
        ),
        TxOutcome::SendErr(err) => {
            warn!(err = %sanitize_rpc_display(err), channel_id = %st.channel_id, "buyer reclaim: send failed");
        }
        TxOutcome::ReceiptErr(err) => {
            warn!(err = %sanitize_rpc_display(err), channel_id = %st.channel_id, "buyer reclaim: receipt failed");
        }
        TxOutcome::Timeout => warn!(
            channel_id = %st.channel_id,
            timeout = ?RECLAIM_RECEIPT_TIMEOUT,
            "buyer reclaim: receipt timed out; the tx may still mine and the sweep will retry"
        ),
    }
    match reclaim_disposition(outcome.kind()) {
        ReclaimDisposition::Forget => {
            forget_reclaimed(store, st, "reclaimed expired buyer channel deposit")
        }
        ReclaimDisposition::Retry => ReclaimOutcome::Failed,
    }
}

/// Whether a completed `reclaimExpired` submission lets us drop the local
/// record. Only a landed receipt actually refunds the deposit; every other
/// terminal — revert, send/receipt error, or receipt-wait timeout — leaves the
/// row for the next hourly sweep. Split out (mirroring
/// `record_settle_receipt_outcome`) so the terminal-vs-retry split is
/// unit-testable without a live provider.
const fn reclaim_disposition(kind: TxKind) -> ReclaimDisposition {
    match kind {
        TxKind::Landed => ReclaimDisposition::Forget,
        TxKind::Reverted | TxKind::SendErr | TxKind::ReceiptErr | TxKind::Timeout => {
            ReclaimDisposition::Retry
        }
    }
}

/// Outcome of [`reclaim_disposition`]: drop the record, or keep it for the next
/// sweep.
enum ReclaimDisposition {
    Forget,
    Retry,
}

/// Compare-and-delete the buyer record for `st`'s channel after a reclaim (or a
/// drop-bogus decision), logging the outcome and returning the resulting
/// [`ReclaimOutcome`]. Uses `forget_if_channel` so a concurrent `open_or_reuse`
/// that replaced this provider's channel between the sweep's `load_all` and here
/// is NOT clobbered (lost-update guard).
///
/// Resolves the sweep when the record is gone for our purposes — either we
/// deleted it (`Ok(true)`) or a newer channel already superseded it
/// (`Ok(false)`). A store write that *errored* (`Err`) leaves the stale record
/// to be re-loaded and re-attempted next sweep, so it maps to `Failed`: a
/// persistent store-write failure must escalate (and feed the metric) rather
/// than being silently re-cleared every pass (#906 review).
fn forget_reclaimed(
    store: &Arc<dyn BuyerChannelStore>,
    st: &BuyerChannelState,
    reason: &str,
) -> ReclaimOutcome {
    match store.forget_if_channel(st.provider, st.channel_id) {
        Ok(true) => {
            info!(
                channel_id = %st.channel_id,
                provider = %st.provider,
                reason,
                "dropped buyer channel record"
            );
            ReclaimOutcome::Resolved
        }
        Ok(false) => {
            debug!(
                channel_id = %st.channel_id,
                provider = %st.provider,
                reason,
                "buyer record already replaced by a newer channel; left in place"
            );
            ReclaimOutcome::Resolved
        }
        Err(err) => {
            warn!(
                %err,
                provider = %st.provider,
                reason,
                "buyer reclaim: forget_if_channel failed"
            );
            ReclaimOutcome::Failed
        }
    }
}

/// Authoritative on-chain view of one `ChannelOpened` open, distilled from the
/// event + a `getChannel` read into just the fields the reconciliation decision
/// needs. Keeping it scalar (rather than the alloy `Channel` binding) lets
/// [`reconcile_decision`] be unit-tested without constructing contract types.
#[derive(Debug, Clone)]
struct OnChainOpen {
    channel_id: ChannelId,
    /// On-chain `channel.client` (read back via `getChannel`, not the event) so
    /// a zeroed struct from an unknown id can be rejected.
    client: Address,
    provider: Address,
    /// On-chain `channel.voucherSigner` — the pinned EIP-712 signer (#1481).
    voucher_signer: Address,
    token: Address,
    deposit: U256,
    expires_at: u64,
    claimed_nonce: U256,
    claimed_bytes: U256,
    claimed_amount: U256,
    is_open: bool,
}

/// Outcome of the pure reconciliation policy for one on-chain open. Naming the
/// reject reasons (rather than collapsing to a bare `Option`) lets the caller log
/// the one orphan it deliberately cannot auto-recover without re-deriving the
/// predicates, and makes each branch directly unit-testable.
#[derive(Debug, PartialEq, Eq)]
enum ReconcileOutcome {
    /// Genuine orphan (still `Open`, owned by this node, no decodable local row);
    /// persist the carried state so the reclaim sweep recovers the deposit.
    /// Boxed to keep the enum small (the other variants are unit).
    Rehydrate(Box<BuyerChannelState>),
    /// A *second* still-`Open` channel we own whose provider is already covered by
    /// a live local row for a *different* channel — the documented
    /// one-row-per-provider residual. Not auto-recovered now (overwriting would
    /// clobber the live row); the caller logs it so the deferred deposit is
    /// observable, and a later boot recovers it once the live row clears.
    DeferredSecondOpen,
    /// Nothing to do: not ours, not `Open`, or already covered by the same channel.
    Skip,
}

/// Decide what to do with an on-chain open. Pure (no I/O) so the policy is
/// unit-testable. A present healthy row — for any `channel_id` — is never
/// clobbered: the running node's own record is authoritative for which channel is
/// live for that provider. The caller maps a corrupt/undecodable local row to
/// `existing == None` so it is repaired by the overwrite.
fn reconcile_decision(
    view: &OnChainOpen,
    self_address: Address,
    existing: Option<&BuyerChannelState>,
) -> ReconcileOutcome {
    // `getChannel` on an unknown id returns a zeroed struct (client == 0); a
    // mined `ChannelOpened` cannot have client == 0, so a mismatch here means we
    // somehow read a foreign/empty channel — never reclaim it for someone else.
    if view.client != self_address {
        return ReconcileOutcome::Skip;
    }
    // Closing/Closed channels need no buyer reclaim (`reclaimExpired` reverts);
    // mirrors the reclaim sweep's status guard in `try_reclaim`.
    if !view.is_open {
        return ReconcileOutcome::Skip;
    }
    // A decodable local row already covers this provider — leave it untouched. If
    // it tracks a *different* channel, the on-chain one is a deferred orphan.
    if let Some(row) = existing {
        return if row.channel_id == view.channel_id {
            ReconcileOutcome::Skip
        } else {
            ReconcileOutcome::DeferredSecondOpen
        };
    }
    let mut state = BuyerChannelState::new(
        view.channel_id,
        view.provider,
        view.client,
        view.voucher_signer,
        view.token,
        view.deposit,
        view.expires_at,
    );
    // Hydrate the cumulative watermark from the authoritative on-chain claimed
    // totals so a re-hydrated channel that already saw deliveries resumes at the
    // right nonce instead of re-signing from zero (which the provider would
    // reject). `new()` zeroes `last_*` and on-chain claimed totals are `>= 0`, so
    // `advance` cannot regress here; on the impossible error keep the un-advanced
    // (zeroed-watermark) state rather than panic.
    if let Err(err) = state.advance(view.claimed_nonce, view.claimed_bytes, view.claimed_amount) {
        warn!(
            channel_id = %view.channel_id,
            provider = %view.provider,
            %err,
            "buyer reconcile: on-chain claimed totals could not seed the watermark; \
             hydrating with a zero watermark"
        );
    }
    ReconcileOutcome::Rehydrate(Box::new(state))
}

/// Reconcile one enumerated channel id: read authoritative on-chain state and
/// re-hydrate the local store if the channel is an orphan.
/// Returns `Ok(true)` when a row was (re)hydrated, `Ok(false)` when skipped, and
/// `Err` only on a per-id fault (a `getChannel` RPC error) the caller logs
/// and steps past.
// Linear guard sequence (getChannel → per-provider slot →
// store-read with fault/corrupt split → decide → record); splitting would
// scatter the atomicity reasoning.
#[allow(clippy::cognitive_complexity)]
async fn reconcile_one_opened<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn BuyerChannelStore>,
    self_address: Address,
    opens_in_flight: &Arc<Mutex<HashMap<Address, SharedOpen>>>,
    channel_id: ChannelId,
) -> Result<bool> {
    let ch = contract
        .getChannel(channel_id)
        .call()
        .await
        .with_context(|| format!("reconcile getChannel for {channel_id}"))?;

    // Serialize against the live open path for this provider. The reconciler is a
    // second writer to the provider-keyed store, racing concurrent cache-miss
    // opens the moment bootstrap returns; without this guard our re-hydration
    // could overwrite (orphan) a channel a live open just escrowed and recorded.
    // Claiming the same per-provider slot the live path uses makes the read +
    // record below atomic with respect to opens: a live open in flight → we skip
    // (it persists the real channel); otherwise the slot is ours until the body
    // returns. Held via `under_claim` so the guard is never bound here and cannot
    // be dropped early — see its docs.
    let rehydrated = InFlightOpenGuard::under_claim(opens_in_flight, ch.provider, || {
        // Read the local row UNDER the slot so the decision + record are atomic wrt
        // live opens. Distinguish an *unreadable* row (corrupt bytes / a future
        // schema after a downgrade) — whose channel_id is unrecoverable from disk, so
        // re-hydrating from chain is the repair — from a *backend/IO fault*, where the
        // row may be perfectly healthy and overwriting it would clobber a live
        // channel. Repair the former (treat as "no row"); skip the latter.
        // Match every `StoreError` variant explicitly (no catch-all) so adding a
        // future variant is a compile error that forces a repair-vs-skip decision
        // here, rather than silently defaulting to "skip" (which would strand an
        // orphan whose row became unreadable in a new way).
        let existing = match store.get_by_provider(ch.provider) {
            Ok(row) => row,
            // Unreadable row — corrupt bytes, a future on-disk schema after a
            // downgrade, or a decode failure. Its channel_id is unrecoverable from
            // disk, so re-hydrating from chain is the repair: treat as "no row".
            Err(
                err @ (StoreError::Corrupt { .. }
                | StoreError::UnsupportedSchema { .. }
                | StoreError::Codec(_)),
            ) => {
                warn!(
                    provider = %ch.provider,
                    channel_id = %channel_id,
                    %err,
                    "buyer reconcile: local row unreadable (corrupt/downgraded); re-hydrating from chain"
                );
                None
            }
            // Backend/IO/permission fault — the row may be perfectly healthy and
            // overwriting it would clobber a live channel. Skip; a later boot retries.
            // `AlreadyOpen` (another process holds the store lock) can't arise from
            // this read — the store is already open by this process — but it is the
            // same "store unavailable, don't decide" case, so skip it too.
            Err(
                err @ (StoreError::Backend(_)
                | StoreError::Io(_)
                | StoreError::PermissionTighten { .. }
                | StoreError::AlreadyOpen { .. }),
            ) => {
                warn!(
                    provider = %ch.provider,
                    channel_id = %channel_id,
                    %err,
                    "buyer reconcile: store read failed (backend/IO); skipping to avoid clobbering a possibly-healthy row"
                );
                return Ok(false);
            }
        };
        let view = OnChainOpen {
            channel_id,
            client: ch.client,
            provider: ch.provider,
            voucher_signer: ch.voucherSigner,
            token: ch.token,
            // `Channel.expiresAt` is `uint64` in the binding — no clamp needed.
            expires_at: ch.expiresAt,
            deposit: ch.deposit,
            claimed_nonce: ch.claimedNonce,
            claimed_bytes: ch.claimedBytes,
            claimed_amount: ch.claimedAmount,
            is_open: matches!(ch.status, PaymentChannel::Status::Open),
        };
        let state = match reconcile_decision(&view, self_address, existing.as_ref()) {
            ReconcileOutcome::Rehydrate(state) => state,
            // The one orphan the scan deliberately cannot auto-recover (a second
            // still-open channel for a provider a live row already covers); log it so
            // the deferred deposit is observable rather than silently skipped.
            ReconcileOutcome::DeferredSecondOpen => {
                warn!(
                    provider = %view.provider,
                    orphan_channel_id = %view.channel_id,
                    deposit = %view.deposit,
                    "buyer reconcile: a second still-open channel for this provider is already covered by a \
                     live row; its deposit is deferred to a later boot once the live row clears"
                );
                return Ok(false);
            }
            ReconcileOutcome::Skip => return Ok(false),
        };
        store
            .record(&state)
            .context("persist re-hydrated buyer channel")?;
        info!(
            provider = %state.provider,
            channel_id = %state.channel_id,
            deposit = %state.deposit,
            expires_at = state.expires_at,
            "buyer reconcile: re-hydrated orphaned channel; reclaim sweep will recover the deposit"
        );
        Ok(true)
    })?;
    let Some(did_rehydrate) = rehydrated else {
        // `under_claim` returned `None`: a real open holds the slot, so it will persist the
        // authoritative channel — the reconciler steps past.
        debug!(
            provider = %ch.provider,
            channel_id = %channel_id,
            "buyer reconcile: a live open is in flight for this provider; skipping (it persists the real channel)"
        );
        return Ok(false);
    };
    Ok(did_rehydrate)
}

/// One page of client channel ids per `clientChannels` call.
const CLIENT_CHANNEL_PAGE_SIZE: u64 = 100;

/// The two chain reads the buyer boot enumeration performs, behind a trait so the
/// paging and accumulation are unit-testable without a provider.
///
/// Spelled RPITIT with an explicit `+ Send` (rather than `async fn`, whose futures
/// carry no `Send` bound) because the enumeration runs inside the fire-and-forget
/// `tokio::spawn` bootstrap task, whose future must be `Send`. Production
/// monomorphizes to the alloy `PaymentChannel` contract impl.
trait ClientChannelReads: Send + Sync {
    /// How many channels `client` has ever opened (`clientChannelNonce`). The
    /// contract recomputes each id from the nonce, so this counter cannot drift
    /// from the enumerable set.
    fn client_channel_count(
        &self,
        client: Address,
        at_block: u64,
    ) -> impl std::future::Future<Output = Result<U256>> + Send;
    /// A page of `client`'s channel ids, oldest first (`clientChannels`).
    fn client_channels_page(
        &self,
        client: Address,
        offset: U256,
        limit: U256,
        at_block: u64,
    ) -> impl std::future::Future<Output = Result<Vec<ChannelId>>> + Send;
}

/// Enumerate every channel id `client` has opened, paging `clientChannels` from a
/// single PINNED block for a consistent snapshot of count and pages.
///
/// The client channel set is append-only per nonce — the contract recomputes each
/// id from the nonce and never removes one — so, unlike the swap-and-pop
/// enumeration views (`assignedNamespaces`), pinning is only for snapshot
/// consistency, not correctness: a strict count re-check is not load-bearing and a
/// short/empty page is a clean stop rather than a mismatch to abort on. A channel
/// opened mid-boot lands past `count` (unseen this boot) and is re-hydrated by a
/// later restart if it strands.
async fn enumerate_client_channels<R: ClientChannelReads>(
    reads: &R,
    client: Address,
    at_block: u64,
) -> Result<Vec<ChannelId>> {
    let count = reads.client_channel_count(client, at_block).await?;
    let mut ids = Vec::new();
    let mut offset = U256::ZERO;
    let page_size = U256::from(CLIENT_CHANNEL_PAGE_SIZE);
    while offset < count {
        let page = reads
            .client_channels_page(client, offset, page_size, at_block)
            .await?;
        if page.is_empty() {
            break;
        }
        offset = offset.saturating_add(U256::from(page.len()));
        ids.extend(page);
    }
    Ok(ids)
}

/// Production [`ClientChannelReads`] over the live `PaymentChannel` contract, each
/// read pinned to the boot snapshot block.
#[derive(Clone)]
struct ContractClientChannelReads<P: Provider + Clone> {
    contract: PaymentChannel::PaymentChannelInstance<P>,
}

impl<P: Provider + Clone> ClientChannelReads for ContractClientChannelReads<P> {
    async fn client_channel_count(&self, client: Address, at_block: u64) -> Result<U256> {
        self.contract
            .clientChannelNonce(client)
            .block(BlockId::Number(at_block.into()))
            .call()
            .await
            .map_err(|err| {
                anyhow::anyhow!(
                    "clientChannelNonce({client}): {}",
                    sanitize_rpc_display(&err)
                )
            })
    }

    async fn client_channels_page(
        &self,
        client: Address,
        offset: U256,
        limit: U256,
        at_block: u64,
    ) -> Result<Vec<ChannelId>> {
        self.contract
            .clientChannels(client, offset, limit)
            .block(BlockId::Number(at_block.into()))
            .call()
            .await
            .map_err(|err| {
                anyhow::anyhow!(
                    "clientChannels({client}, offset={offset}, limit={limit}): {}",
                    sanitize_rpc_display(&err)
                )
            })
    }
}

/// One-shot bootstrap reconciliation (#763): enumerate every channel this node
/// opened via the on-chain `clientChannels`/`clientChannelNonce` view and
/// re-hydrate any still-`Open` channel missing or undecodable in the local store,
/// so the reclaim sweep can recover its deposit. This replaces the previous
/// 700k-block `ChannelOpened` log replay: the enumeration covers the client's full
/// channel history with no block lookback, so an orphan can never age out of a scan
/// window. Best-effort: a head-read or enumeration failure `warn!`s and returns;
/// per-id faults are logged and skipped. The completion log reports the failed-id
/// count so a partial reconcile is observable.
// Linear flow (head → enumerate → per-id) with inline best-effort guards;
// splitting would obscure the control flow.
#[allow(clippy::cognitive_complexity)]
async fn reconcile_orphans_once<P: Provider + Clone>(
    contract: PaymentChannel::PaymentChannelInstance<P>,
    store: Arc<dyn BuyerChannelStore>,
    self_address: Address,
    opens_in_flight: Arc<Mutex<HashMap<Address, SharedOpen>>>,
) {
    // Pin every read to one head so count and pages are a consistent snapshot.
    let head = match contract.provider().get_block_number().await {
        Ok(h) => h,
        Err(err) => {
            warn!(err = %sanitize_rpc_display(&err), "buyer reconcile: head block read failed; skipping scan this boot");
            return;
        }
    };
    let reads = ContractClientChannelReads {
        contract: contract.clone(),
    };
    let ids = match enumerate_client_channels(&reads, self_address, head).await {
        Ok(ids) => ids,
        Err(err) => {
            warn!(%err, head, "buyer reconcile: client-channel enumeration failed; skipping scan this boot");
            return;
        }
    };
    let total = ids.len();
    let mut rehydrated: usize = 0;
    let mut failed: usize = 0;
    for channel_id in ids {
        match reconcile_one_opened(
            &contract,
            &store,
            self_address,
            &opens_in_flight,
            channel_id,
        )
        .await
        {
            Ok(true) => rehydrated = rehydrated.saturating_add(1),
            Ok(false) => {}
            Err(err) => {
                warn!(
                    err = %sanitize_rpc_display(&err),
                    channel_id = %channel_id,
                    "buyer reconcile: skipping channel after a per-id fault"
                );
                failed = failed.saturating_add(1);
            }
        }
    }
    info!(
        head,
        channel_count = total,
        rehydrated,
        failed,
        "buyer reconcile: bootstrap enumeration complete"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{B256, address};

    fn sample(byte: u8) -> BuyerChannelState {
        let mut prov = [0u8; 20];
        prov[19] = byte;
        BuyerChannelState::new(
            B256::repeat_byte(byte),
            Address::from(prov),
            Address::from(prov),
            Address::from(prov),
            address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            U256::from(10_000_000u64),
            1_900_000_000,
        )
    }

    /// Scripted [`ClientChannelReads`]: no provider, no chain. Ids are supplied
    /// oldest-first (append order), matching `clientChannels`' stable ordering.
    /// `count` is decoupled from the id list so a short/empty-page early stop can
    /// be exercised against a count that over-reports.
    struct StubClientReads {
        client: Address,
        /// Channel ids oldest→newest.
        ids: Vec<ChannelId>,
        /// The value `client_channel_count` reports (may exceed `ids.len()`).
        count: U256,
        /// The `at_block` every call must be pinned to.
        expect_block: u64,
        /// (offset, limit) of each page read, for the paging assertion.
        pages: std::sync::Mutex<Vec<(U256, U256)>>,
    }

    impl StubClientReads {
        fn new(client: Address, ids: Vec<ChannelId>, expect_block: u64) -> Self {
            Self {
                client,
                count: U256::from(ids.len()),
                ids,
                expect_block,
                pages: std::sync::Mutex::new(Vec::new()),
            }
        }

        /// Report a `count` larger than the id list to drive an early stop.
        fn with_count(mut self, count: u64) -> Self {
            self.count = U256::from(count);
            self
        }

        #[allow(clippy::unwrap_used)]
        fn pages_read(&self) -> Vec<(U256, U256)> {
            self.pages.lock().unwrap().clone()
        }
    }

    impl ClientChannelReads for StubClientReads {
        async fn client_channel_count(&self, client: Address, at_block: u64) -> Result<U256> {
            assert_eq!(client, self.client, "unexpected client");
            assert_eq!(at_block, self.expect_block, "count read not pinned");
            Ok(self.count)
        }

        #[allow(clippy::indexing_slicing, clippy::unwrap_used)]
        async fn client_channels_page(
            &self,
            client: Address,
            offset: U256,
            limit: U256,
            at_block: u64,
        ) -> Result<Vec<ChannelId>> {
            assert_eq!(client, self.client, "unexpected client");
            assert_eq!(at_block, self.expect_block, "page read not pinned");
            self.pages.lock().unwrap().push((offset, limit));
            let start: usize = offset.to();
            let take: usize = limit.to();
            if start >= self.ids.len() {
                return Ok(Vec::new());
            }
            let end = start.saturating_add(take).min(self.ids.len());
            Ok(self.ids[start..end].to_vec())
        }
    }

    /// Enumeration pages `clientChannels` until it has read `count` ids, preserving
    /// on-chain (oldest-first) order across page boundaries.
    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::indexing_slicing)]
    async fn enumerate_client_channels_pages_and_accumulates_in_order() {
        let client = Address::repeat_byte(0xAB);
        // 250 ids → three pages of 100/100/50 at PAGE_SIZE == 100.
        let ids: Vec<ChannelId> = (0u8..250).map(B256::repeat_byte).collect();
        let reads = StubClientReads::new(client, ids.clone(), 4_242);

        let got = enumerate_client_channels(&reads, client, 4_242)
            .await
            .unwrap();

        assert_eq!(got, ids, "all ids in oldest-first order across pages");
        assert_eq!(
            reads.pages_read(),
            vec![
                (U256::ZERO, U256::from(100u64)),
                (U256::from(100u64), U256::from(100u64)),
                (U256::from(200u64), U256::from(100u64)),
            ],
            "offset advances by the page length, limit is the page size"
        );
    }

    /// A client with no channels enumerates to an empty set and reads no page.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn enumerate_client_channels_of_a_clean_client_is_empty() {
        let client = Address::repeat_byte(0xEF);
        let reads = StubClientReads::new(client, vec![], 7);
        assert!(
            enumerate_client_channels(&reads, client, 7)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(reads.pages_read().is_empty(), "count == 0 reads no page");
    }

    /// A short/empty page stops the walk cleanly even when `count` over-reports —
    /// the append-only set cannot shrink, so there is no abort-on-mismatch.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn enumerate_client_channels_breaks_on_a_short_page() {
        let client = Address::repeat_byte(0xCD);
        let ids: Vec<ChannelId> = (0u8..30).map(B256::repeat_byte).collect();
        // count claims 500 but only 30 ids exist: the second page is empty → stop.
        let reads = StubClientReads::new(client, ids.clone(), 9).with_count(500);

        let got = enumerate_client_channels(&reads, client, 9).await.unwrap();

        assert_eq!(got, ids, "returns the real ids without erroring on the gap");
        assert_eq!(
            reads.pages_read(),
            vec![
                (U256::ZERO, U256::from(100u64)),
                (U256::from(30u64), U256::from(100u64))
            ],
            "reads the first (short) page, then one empty page, then stops"
        );
    }

    // channelId derivation moved on-chain → the `ChannelOpened` event in the
    // open receipt; the local packing helper + its test were removed with it.

    // `BuyerChannelState::advance` monotonicity is unit-tested in the incentive
    // crate (crates/incentive/src/buyer_channel.rs) where the method lives.

    #[test]
    fn for_buyer_channel_resumes_from_stored_totals() {
        let mut s = sample(2);
        s.last_nonce = U256::from(3u64);
        s.last_bytes_delivered = U256::from(3_000u64);
        s.last_amount = U256::from(30u64);
        let signer = Arc::new(PrivateKeySigner::random());
        let domain = decdn_incentive::voucher_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        );
        let ctx = ChannelContext::for_buyer_channel(&s, signer, domain);
        assert_eq!(ctx.channel_id, s.channel_id);
        assert_eq!(ctx.token, s.token);
        assert_eq!(ctx.deposit, s.deposit);
        assert_eq!(ctx.prior_nonce, U256::from(3u64));
        assert_eq!(ctx.prior_bytes_delivered, U256::from(3_000u64));
        assert_eq!(ctx.prior_amount, U256::from(30u64));
    }

    // ---- low-water refill decision (#1146) ------------------------------

    #[test]
    fn refill_decision_no_topup_with_headroom() {
        // deposit 10 USDC, nothing spent → remaining == target, well above the 20%
        // low-water mark, so no top-up.
        let ten = U256::from(10_000_000u64);
        assert_eq!(
            refill_decision(ten, U256::ZERO, ten, ten, U256::from(1u64)),
            U256::ZERO
        );
    }

    #[test]
    fn refill_decision_tops_up_to_target_when_below_low_water() {
        // remaining (1 USDC) is below the 2 USDC low-water mark
        // (target 10 / LOW_WATER_DIVISOR) → refill restores to the full target.
        let ten = U256::from(10_000_000u64);
        let prior = U256::from(9_000_000u64); // remaining == 1 USDC
        assert_eq!(
            refill_decision(ten, prior, ten, ten, U256::from(1u64)),
            U256::from(9_000_000u64)
        );
    }

    #[test]
    fn refill_decision_target_is_max_of_hint_working_and_min() {
        // A tiny deposit_hint must not shrink the target: it is
        // max(hint, working_deposit, min_deposit), exactly the fresh-open deposit.
        let ten = U256::from(10_000_000u64);
        let prior = U256::from(9_999_999u64); // remaining == 1 µUSDC
        // hint below both → target = working (10 USDC), low_water = 2 USDC.
        assert_eq!(
            refill_decision(ten, prior, U256::from(1u64), ten, U256::from(2_000_000u64)),
            ten - U256::from(1u64),
        );

        // min_deposit as the DECIDING term of the max(): min > working > hint.
        assert_eq!(
            refill_decision(ten, prior, U256::from(1u64), ten, U256::from(20_000_000u64)),
            U256::from(20_000_000u64) - U256::from(1u64),
            "min_deposit must be able to raise the target above working/hint"
        );
    }

    #[test]
    fn refill_decision_targets_working_not_initial() {
        // deposit_hint = initial (small); working is the graduation target.
        let deposit = U256::from(500_000u64); // opened at initial
        let prior = U256::from(490_000u64); // remaining 10_000 (< 20% of working)
        let hint = U256::from(500_000u64); // initial
        let working = U256::from(10_000_000u64);
        let min = U256::from(1u64);
        let add = refill_decision(deposit, prior, hint, working, min);
        assert_eq!(add, working - (deposit - prior)); // graduate toward working
    }

    #[test]
    fn refill_decision_working_deposit_zero_disables_topup() {
        // `working_deposit == 0` must disable the proactive refill entirely,
        // mirroring the CLI's "0 disables top-up entirely" documented semantics
        // for `buyer_working_deposit_micro_usdc`. Without the early return, the
        // target would still be max(deposit_hint, min_deposit), which can sit
        // well above the channel's remaining balance and trigger an unwanted
        // on-chain topUp.
        let deposit = U256::from(10_000_000u64); // deposit_hint == initial deposit
        let prior = U256::from(9_999_000u64); // remaining is far below what would
        // otherwise be the low-water mark.
        let hint = deposit;
        let min = U256::from(1u64);
        assert_eq!(
            refill_decision(deposit, prior, hint, U256::ZERO, min),
            U256::ZERO,
            "working_deposit == 0 must disable refill even when the channel is low"
        );
    }

    #[test]
    fn open_deposit_uses_initial_not_working() {
        // A fresh open must fund the small initial deposit, not the larger
        // working target — "open small, graduate on proof" (#1497 task 6).
        let initial = U256::from(500_000u64);
        let working = U256::from(10_000_000u64);
        let min = U256::from(1u64);
        assert_eq!(open_deposit(U256::ZERO, initial, min), initial);
        assert_ne!(open_deposit(U256::ZERO, initial, min), working);
    }

    #[test]
    fn open_deposit_hint_and_min_still_win_over_initial() {
        let initial = U256::from(500_000u64);
        // A caller-supplied hint larger than initial is still honored.
        let hint = U256::from(2_000_000u64);
        assert_eq!(open_deposit(hint, initial, U256::from(1u64)), hint);
        // The on-chain floor wins even over a larger initial.
        let big_min = U256::from(1_000_000u64);
        assert_eq!(open_deposit(U256::ZERO, initial, big_min), big_min);
    }

    #[test]
    fn claim_refill_slot_dedups_per_provider_and_frees_on_drop() {
        let set: Arc<Mutex<HashSet<Address>>> = Arc::new(Mutex::new(HashSet::new()));
        let p = Address::repeat_byte(7);
        let first = claim_refill_slot(&set, p);
        assert!(first.is_some(), "first claim must succeed");
        assert!(
            claim_refill_slot(&set, p).is_none(),
            "a second claim while the first is held is deduped"
        );
        assert!(
            claim_refill_slot(&set, Address::repeat_byte(8)).is_some(),
            "a distinct provider is independent"
        );
        drop(first);
        assert!(
            claim_refill_slot(&set, p).is_some(),
            "the slot frees on drop so a later refill can run"
        );
    }

    // ---- plan_refill wiring (#1146): decision + per-provider slot claim, from the
    //      reuse `ctx`'s deposit/prior_amount — no store read, no spawned task.
    //      (Expiry / no-channel are handled upstream by the reuse gate, so
    //      `spawn_refill_if_low` is only ever reached for a live channel.) ----

    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn plan_refill_claims_slot_and_amount_below_low_water() {
        let server = wiremock::MockServer::start().await;
        let service = service_against(&server);
        let provider = Address::repeat_byte(0x71);
        let ten = U256::from(10_000_000u64); // target 10 USDC, low_water 2 USDC
        // deposit 10 USDC, 9 spent → remaining 1 USDC, below the low-water mark.
        let (slot, additional) = service
            .plan_refill(provider, ten, ten, U256::from(9_000_000u64))
            .expect("a below-low-water channel must plan a refill");
        assert_eq!(additional, U256::from(9_000_000u64)); // restore to the target
        assert!(
            service.topups_in_flight.lock().unwrap().contains(&provider),
            "planning a refill claims the provider's slot"
        );
        drop(slot);
        assert!(
            !service.topups_in_flight.lock().unwrap().contains(&provider),
            "dropping the plan frees the slot"
        );
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn plan_refill_none_with_headroom() {
        let server = wiremock::MockServer::start().await;
        let service = service_against(&server);
        let provider = Address::repeat_byte(0x72);
        let ten = U256::from(10_000_000u64);
        // nothing spent → remaining above the 2 USDC low-water mark.
        assert!(
            service
                .plan_refill(provider, ten, ten, U256::ZERO)
                .is_none(),
            "a channel with headroom must not plan a refill"
        );
        assert!(
            service.topups_in_flight.lock().unwrap().is_empty(),
            "no slot is claimed when above the low-water mark"
        );
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn plan_refill_dedups_concurrent_reuse() {
        let server = wiremock::MockServer::start().await;
        let service = service_against(&server);
        let provider = Address::repeat_byte(0x75);
        let ten = U256::from(10_000_000u64);
        let prior = U256::from(9_000_000u64); // remaining 1 USDC, below low-water

        let first = service.plan_refill(provider, ten, ten, prior);
        assert!(
            first.is_some(),
            "first reuse below low-water plans a refill"
        );
        assert!(
            service.plan_refill(provider, ten, ten, prior).is_none(),
            "a concurrent reuse while the first refill is in flight is deduped"
        );
        drop(first);
        assert!(
            service.plan_refill(provider, ten, ten, prior).is_some(),
            "once the in-flight refill completes, a later reuse can plan again"
        );
    }

    /// A channel observed for the first time is tracked but never immediately
    /// idle — reconcile only fires after sustained inactivity (#972).
    #[test]
    fn observe_idle_first_sighting_is_not_idle() {
        let mut obs = HashMap::new();
        let ch = B256::repeat_byte(0x42);
        assert!(!observe_idle(&mut obs, ch, U256::from(5u64), 3));
        assert_eq!(obs.len(), 1);
    }

    /// With no voucher-nonce progress, a channel becomes idle exactly once it has
    /// been observed stale for `threshold` consecutive sweeps.
    #[test]
    fn observe_idle_marks_idle_after_threshold_without_progress() {
        let mut obs = HashMap::new();
        let ch = B256::repeat_byte(0x07);
        let nonce = U256::from(9u64);
        // First sighting + the next (threshold-1) stale sweeps are not yet idle.
        assert!(!observe_idle(&mut obs, ch, nonce, 3)); // first sight
        assert!(!observe_idle(&mut obs, ch, nonce, 3)); // stale 1
        assert!(!observe_idle(&mut obs, ch, nonce, 3)); // stale 2
        assert!(observe_idle(&mut obs, ch, nonce, 3)); // stale 3 → idle
    }

    /// Any nonce progress (an active pull) resets the idle tally, so a channel in
    /// use is never cooperatively closed out from under the workload.
    #[test]
    fn observe_idle_resets_on_nonce_progress() {
        let mut obs = HashMap::new();
        let ch = B256::repeat_byte(0x55);
        assert!(!observe_idle(&mut obs, ch, U256::from(1u64), 2)); // first sight
        assert!(!observe_idle(&mut obs, ch, U256::from(1u64), 2)); // stale 1
        // Progress to nonce 2 → reset; this sweep is not idle.
        assert!(!observe_idle(&mut obs, ch, U256::from(2u64), 2));
        // Tally restarts: one stale sweep is below threshold again.
        assert!(!observe_idle(&mut obs, ch, U256::from(2u64), 2)); // stale 1
        assert!(observe_idle(&mut obs, ch, U256::from(2u64), 2)); // stale 2 → idle
    }

    /// The reclaim terminal-vs-retry split: only a landed receipt drops the
    /// record; a revert, a send/receipt error, and a receipt-wait timeout must
    /// all leave the row for the next sweep. An inverted arm here would either
    /// strand a refundable deposit or drop a record whose deposit never came
    /// back, so pin every kind. Provider-free via [`TxKind`].
    #[test]
    fn reclaim_forgets_only_on_landed_receipt() {
        assert!(matches!(
            reclaim_disposition(TxKind::Landed),
            ReclaimDisposition::Forget
        ));
        for kind in [
            TxKind::Reverted,
            TxKind::SendErr,
            TxKind::ReceiptErr,
            TxKind::Timeout,
        ] {
            assert!(
                matches!(reclaim_disposition(kind), ReclaimDisposition::Retry),
                "{kind:?} must leave the record for retry"
            );
        }
    }

    /// The unilateral-close #989 two-bucket mapping: a landed receipt is the
    /// only success (ticks `ok`, returns `true`); every other terminal ticks
    /// `rpc_failure` and returns `false`. Assert both the returned flag and the
    /// counter that moved, against a fresh registry, so a swapped bucket can't
    /// slip through.
    #[test]
    fn unilateral_close_counts_ok_only_on_landed_receipt() {
        let landed = Arc::new(Metrics::new());
        assert!(record_unilateral_close_outcome(&landed, TxKind::Landed));
        assert_eq!(counter(&landed, "buyer_unilateral_close_ok_total"), 1);
        assert_eq!(
            counter(&landed, "buyer_unilateral_close_rpc_failure_total"),
            0
        );

        for kind in [
            TxKind::Reverted,
            TxKind::SendErr,
            TxKind::ReceiptErr,
            TxKind::Timeout,
        ] {
            let failed = Arc::new(Metrics::new());
            assert!(
                !record_unilateral_close_outcome(&failed, kind),
                "{kind:?} must report failure"
            );
            assert_eq!(counter(&failed, "buyer_unilateral_close_ok_total"), 0);
            assert_eq!(
                counter(&failed, "buyer_unilateral_close_rpc_failure_total"),
                1,
                "{kind:?} must tick rpc_failure"
            );
        }
    }

    /// A persistent reclaim failure stays silent for the first few sweeps (the
    /// expected transient case) and escalates once it reaches the threshold —
    /// then keeps escalating on every subsequent failed sweep so a sustained
    /// stranded deposit keeps surfacing rather than going quiet after the first
    /// crossing (#906).
    #[test]
    fn reclaim_failures_escalate_at_threshold_and_stay_escalated() {
        let mut failures: HashMap<ChannelId, u32> = HashMap::new();
        let ch = B256::repeat_byte(0x11);
        let note = |f: &mut HashMap<ChannelId, u32>| {
            record_reclaim_outcome(f, ch, ReclaimOutcome::Failed, RECLAIM_ESCALATION_THRESHOLD)
        };
        for _ in 1..RECLAIM_ESCALATION_THRESHOLD {
            assert_eq!(note(&mut failures), ReclaimEscalation::None);
        }
        assert_eq!(
            note(&mut failures),
            ReclaimEscalation::Escalate {
                consecutive: RECLAIM_ESCALATION_THRESHOLD
            },
        );
        assert_eq!(
            note(&mut failures),
            ReclaimEscalation::Escalate {
                consecutive: RECLAIM_ESCALATION_THRESHOLD + 1
            },
        );
    }

    /// The unilateral-close escalation (#988) waits out a few transient dial
    /// failures, then escalates on the threshold-th consecutive failure and
    /// keeps escalating on every subsequent one — same shape as the reclaim
    /// escalation, so a sustained-unreachable provider is closed unilaterally.
    #[test]
    fn close_failures_escalate_only_at_threshold() {
        let mut failures: HashMap<ChannelId, u32> = HashMap::new();
        let ch = B256::repeat_byte(0x44);
        let threshold = RECONCILE_CLOSE_ESCALATION_THRESHOLD;
        for n in 1..threshold {
            assert_eq!(
                record_close_failure(&mut failures, ch, threshold),
                CloseEscalation::Wait { consecutive: n },
                "below the threshold the sweep keeps waiting (transient-blip tolerance)"
            );
        }
        assert_eq!(
            record_close_failure(&mut failures, ch, threshold),
            CloseEscalation::Escalate {
                consecutive: threshold
            },
            "the threshold-th consecutive failure escalates to a unilateral close"
        );
        assert_eq!(
            record_close_failure(&mut failures, ch, threshold),
            CloseEscalation::Escalate {
                consecutive: threshold + 1
            },
            "a still-unreachable provider keeps escalating (the close may have failed to land)"
        );
    }

    /// A provider that responds (settled/declined/reverted) clears its close
    /// tally via `close_failures.remove`, so an intermittent dial failure never
    /// accrues toward an unwarranted unilateral close — modelled here by the
    /// remove + a fresh count restarting from one.
    #[test]
    fn close_failure_tally_resets_after_a_response() {
        let mut failures: HashMap<ChannelId, u32> = HashMap::new();
        let ch = B256::repeat_byte(0x55);
        let threshold = RECONCILE_CLOSE_ESCALATION_THRESHOLD;
        assert_eq!(
            record_close_failure(&mut failures, ch, threshold),
            CloseEscalation::Wait { consecutive: 1 }
        );
        // The caller clears the tally when the provider responds.
        failures.remove(&ch);
        assert_eq!(
            record_close_failure(&mut failures, ch, threshold),
            CloseEscalation::Wait { consecutive: 1 },
            "a response resets the run, so the next failure restarts from one"
        );
    }

    /// Close tallies are independent per channel: one provider going dark does
    /// not escalate a different, still-flaky channel.
    #[test]
    fn close_failure_tallies_are_per_channel() {
        let mut failures: HashMap<ChannelId, u32> = HashMap::new();
        let a = B256::repeat_byte(0x66);
        let b = B256::repeat_byte(0x77);
        let threshold = 2;
        assert_eq!(
            record_close_failure(&mut failures, a, threshold),
            CloseEscalation::Wait { consecutive: 1 }
        );
        assert_eq!(
            record_close_failure(&mut failures, a, threshold),
            CloseEscalation::Escalate { consecutive: 2 }
        );
        assert_eq!(
            record_close_failure(&mut failures, b, threshold),
            CloseEscalation::Wait { consecutive: 1 },
            "channel b is unaffected by channel a crossing the threshold"
        );
    }

    /// A resolved pass (deposit reclaimed, or record dropped as bogus/closed)
    /// clears the tally, so a later failure restarts the count from one (#906).
    #[test]
    fn reclaim_resolved_clears_the_failure_tally() {
        let mut failures: HashMap<ChannelId, u32> = HashMap::new();
        let ch = B256::repeat_byte(0x22);
        let threshold = 3;
        record_reclaim_outcome(&mut failures, ch, ReclaimOutcome::Failed, threshold);
        record_reclaim_outcome(&mut failures, ch, ReclaimOutcome::Failed, threshold);
        assert_eq!(
            record_reclaim_outcome(&mut failures, ch, ReclaimOutcome::Resolved, threshold),
            ReclaimEscalation::None,
        );
        assert!(!failures.contains_key(&ch), "Resolved forgets the channel");
        assert_eq!(
            record_reclaim_outcome(&mut failures, ch, ReclaimOutcome::Failed, threshold),
            ReclaimEscalation::None,
        );
        assert_eq!(failures.get(&ch), Some(&1), "count restarts from one");
    }

    /// A resolved outcome for a channel with no prior failures is a no-op (the
    /// common steady-state case: most sweeps reclaim cleanly first try).
    #[test]
    fn reclaim_resolved_on_untracked_channel_is_a_noop() {
        let mut failures: HashMap<ChannelId, u32> = HashMap::new();
        assert_eq!(
            record_reclaim_outcome(
                &mut failures,
                B256::repeat_byte(0x33),
                ReclaimOutcome::Resolved,
                3
            ),
            ReclaimEscalation::None,
        );
        assert!(failures.is_empty());
    }

    /// Tallies are independent per channel: one channel crossing the threshold
    /// does not escalate an unrelated channel still on its first failure (#906).
    #[test]
    fn reclaim_failure_tallies_are_per_channel() {
        let mut failures: HashMap<ChannelId, u32> = HashMap::new();
        let (a, b) = (B256::repeat_byte(0x44), B256::repeat_byte(0x55));
        let threshold = 2;
        record_reclaim_outcome(&mut failures, a, ReclaimOutcome::Failed, threshold);
        assert_eq!(
            record_reclaim_outcome(&mut failures, a, ReclaimOutcome::Failed, threshold),
            ReclaimEscalation::Escalate { consecutive: 2 },
        );
        assert_eq!(
            record_reclaim_outcome(&mut failures, b, ReclaimOutcome::Failed, threshold),
            ReclaimEscalation::None,
        );
    }

    /// The end-of-pass prune drops tallies for channels the sweep did not attempt
    /// (reclaimed, replaced, or no longer expired) and keeps the still-failing
    /// ones, so the map cannot grow unbounded (#906).
    #[test]
    fn reclaim_prune_keeps_only_attempted_channels() {
        let mut failures: HashMap<ChannelId, u32> = HashMap::new();
        let (still_failing, gone) = (B256::repeat_byte(0x66), B256::repeat_byte(0x77));
        failures.insert(still_failing, 4);
        failures.insert(gone, 2);
        let seen: HashSet<ChannelId> = HashSet::from([still_failing]);
        prune_reclaim_failures(&mut failures, &seen);
        assert_eq!(failures.get(&still_failing), Some(&4));
        assert!(!failures.contains_key(&gone), "untracked channel is pruned");
    }

    /// The per-provider in-flight-open slot refuses a second concurrent claim and
    /// frees on guard drop, so a later open proceeds. This is the mechanism that
    /// makes the one-channel-per-provider invariant real rather than a documented
    /// caller contract (#753 review).
    ///
    /// Since #1143 the guard is held by the detached open TASK, so "the slot is
    /// occupied" now means "an `openChannel` is genuinely still in flight" — which
    /// is exactly why a caller that times out must not release it.
    #[test]
    fn in_flight_open_guard_refuses_concurrent_then_releases() {
        let map: Arc<Mutex<HashMap<Address, SharedOpen>>> = Arc::new(Mutex::new(HashMap::new()));
        let provider = sample(7).provider;
        // Exercises the production `claim` constructor (the same call the boot
        // reconcile scan makes), not a re-implementation of it.
        // `.ok().flatten()` discards the (impossible here) poison error.
        let guard = InFlightOpenGuard::claim(&map, provider).ok().flatten();
        assert!(guard.is_some(), "first claim acquires the open slot");
        assert!(
            InFlightOpenGuard::claim(&map, provider)
                .ok()
                .flatten()
                .is_none(),
            "a concurrent claim for the same provider is refused"
        );
        drop(guard);
        let reclaimed = InFlightOpenGuard::claim(&map, provider).ok().flatten();
        assert!(
            reclaimed.is_some(),
            "the slot is free once the prior guard drops"
        );
        drop(reclaimed);
        assert!(
            map.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "dropping the guard releases the slot",
        );
    }

    /// `under_claim` is the reconcile scan's ONLY safe path to a claim (#1145 review): the
    /// guard is never bound at a call site, so it holds the slot for exactly the body's
    /// lifetime — a concurrent claim during the body is refused — and releases it on return.
    /// `Ok(None)` when a real open already holds the slot.
    #[test]
    fn under_claim_holds_the_slot_for_the_body_then_releases() {
        let map: Arc<Mutex<HashMap<Address, SharedOpen>>> = Arc::new(Mutex::new(HashMap::new()));
        let provider = sample(9).provider;

        // While the body runs, a concurrent claim for the same provider is refused.
        let held_during_body = InFlightOpenGuard::under_claim(&map, provider, || {
            Ok(InFlightOpenGuard::claim(&map, provider)
                .ok()
                .flatten()
                .is_none())
        })
        .unwrap_or(None);
        assert_eq!(
            held_during_body,
            Some(true),
            "the slot must be held for the whole body"
        );

        // The slot is released once the body returns.
        let after = InFlightOpenGuard::claim(&map, provider).ok().flatten();
        assert!(after.is_some(), "the slot is free once the body returns");
        drop(after);

        // A real open already in flight → `under_claim` skips the body and returns `None`.
        let _live = InFlightOpenGuard::claim(&map, provider).ok().flatten();
        let mut body_ran = false;
        let skipped = InFlightOpenGuard::under_claim(&map, provider, || {
            body_ran = true;
            Ok(())
        })
        .unwrap_or(Some(()));
        assert!(skipped.is_none(), "a live open makes under_claim skip");
        assert!(!body_ran, "the body must not run when the slot is taken");
    }

    /// A watermark write REGRESSED by a concurrent settle on the shared ledger is benign — the
    /// monotonic store kept the higher (correct) value — so it must NOT fire the "we paid and
    /// lost the record" persist-failure alert (#1145 review). It meters `superseded` and
    /// returns `Ok`. Fail-on-revert: restore the `Err` arm and this write reports a persist
    /// failure on ordinary, healthy concurrency.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn a_superseded_watermark_is_benign_not_a_persist_failure() {
        let server = wiremock::MockServer::start().await;
        let service = service_against(&server);
        let provider = Address::repeat_byte(0x5a);
        let channel_id = B256::repeat_byte(0x5a);
        let token = Address::repeat_byte(0x11);

        // Seed the channel and advance it to a HIGH watermark, as a concurrent winning pull would.
        service
            .store
            .record(&decdn_incentive::BuyerChannelState::new(
                channel_id,
                provider,
                provider,
                provider,
                token,
                U256::from(1_000u64),
                0,
            ))
            .expect("seed channel");
        let seeded = service
            .store
            .advance_progress(
                provider,
                channel_id,
                U256::from(10u64),
                U256::from(1_000u64),
                U256::from(100u64),
            )
            .expect("advance to high watermark");
        assert!(
            matches!(seeded, AdvanceOutcome::Advanced),
            "seed must advance"
        );

        // A LOWER write (the losing concurrent settle) regresses against the stored watermark.
        let result = service.record_progress(
            provider,
            channel_id,
            U256::from(5u64),
            U256::from(500u64),
            U256::from(50u64),
        );
        assert!(
            result.is_ok(),
            "a superseded write is benign, not an error: {result:?}"
        );
        let encoded = service.metrics.encode().expect("metrics encode");
        assert!(
            encoded.contains("decdn_node_pull_progress_superseded_total 1"),
            "a superseded write must be metered as benign. Got:\n{encoded}"
        );
        assert!(
            encoded.contains("decdn_node_pull_progress_persist_failures_total 0"),
            "a superseded write must NOT fire the persist-failure alert. Got:\n{encoded}"
        );
    }

    /// A slot claimed by the reconcile scan hands any joining pull the typed
    /// [`OpenSlotReserved`] rather than a channel (#1143). It must not resolve to
    /// `Ok`: the claimant is not opening anything, so a caller that treated it as
    /// "the open succeeded" would read an empty store and report a confusing failure
    /// — and it must not leave the caller waiting out its whole budget on an open
    /// that is never coming.
    ///
    /// Driven through the REAL join path and asserted on the typed sentinel — never by
    /// awaiting `reserved_open()` directly, and never on `.to_string().contains("retry")`.
    /// Either shortcut passes while a joining pull ignores the reserved slot entirely, and
    /// a string assertion cannot see whether the error carries the sentinel that keeps it
    /// from being metered as a hard failure.
    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn a_reserved_slot_tells_a_joining_pull_to_retry() {
        let server = wedged_chain().await;
        let service = service_against(&server);
        let provider = Address::repeat_byte(0xab);

        // The reconciler claims the slot, exactly as `reconcile_one_opened` does.
        let _claim = InFlightOpenGuard::claim(&service.opens_in_flight, provider)
            .expect("claiming a free slot cannot fail")
            .expect("the slot is free, so the claim must succeed");

        // A pull arriving mid-reconcile joins that slot rather than racing an
        // openChannel into the row the reconciler is rewriting.
        let joined = service
            .join_or_spawn_open(provider, U256::from(1u64))
            .expect("joining a reserved slot must not error at the map layer");
        let err = rehydrate_open_error(
            &joined
                .await
                .expect_err("a reserved slot never yields a channel"),
        );

        let reserved = err.downcast_ref::<OpenSlotReserved>();
        assert!(
            reserved.is_some(),
            "a joining pull must receive the typed OpenSlotReserved — a bare string lands in \
             record_channel_open_failure's unclassified arm and is counted as a real \
             channel-open failure on every single boot: {err:#}"
        );
        assert_eq!(
            reserved.map(|r| r.provider),
            Some(provider),
            "the sentinel must name the provider whose slot is held"
        );
        // No openChannel may have been attempted: the whole point is that the pull
        // does NOT race the reconciler into the store.
        assert_eq!(
            server.received_requests().await.map_or(0, |r| r.len()),
            0,
            "joining a reserved slot must not reach the chain"
        );
    }

    fn self_addr() -> Address {
        address!("00000000000000000000000000000000000000aa")
    }

    /// Build an on-chain view owned by `self_addr()` for the channel/provider
    /// keyed off `byte`, with the given liveness and on-chain claimed totals.
    fn view(byte: u8, is_open: bool, claimed: (u64, u64, u64)) -> OnChainOpen {
        let mut prov = [0u8; 20];
        prov[19] = byte;
        OnChainOpen {
            channel_id: B256::repeat_byte(byte),
            client: self_addr(),
            provider: Address::from(prov),
            voucher_signer: self_addr(),
            token: address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            deposit: U256::from(12_000_000u64),
            expires_at: 1_900_000_000,
            claimed_nonce: U256::from(claimed.0),
            claimed_bytes: U256::from(claimed.1),
            claimed_amount: U256::from(claimed.2),
            is_open,
        }
    }

    #[test]
    fn reconcile_orphan_with_no_local_row_rehydrates_from_chain() {
        // An Open channel we own with no local row re-hydrates straight from the
        // on-chain `getChannel` fields (deposit/expiry), zero watermark.
        let v = view(5, true, (0, 0, 0));
        let expected = BuyerChannelState::new(
            v.channel_id,
            v.provider,
            v.client,
            v.voucher_signer,
            v.token,
            v.deposit,
            v.expires_at,
        );
        assert_eq!(
            reconcile_decision(&v, self_addr(), None),
            ReconcileOutcome::Rehydrate(Box::new(expected))
        );
    }

    #[test]
    fn reconcile_hydrates_watermark_from_onchain_claimed_totals() {
        // A re-hydrated channel that already saw deliveries must resume at the
        // on-chain claimed totals, not zero (else the provider rejects re-signed
        // vouchers).
        let v = view(6, true, (4, 4_096, 41));
        let mut expected = BuyerChannelState::new(
            v.channel_id,
            v.provider,
            v.client,
            v.voucher_signer,
            v.token,
            v.deposit,
            v.expires_at,
        );
        expected.last_nonce = U256::from(4u64);
        expected.last_bytes_delivered = U256::from(4_096u64);
        expected.last_amount = U256::from(41u64);
        assert_eq!(
            reconcile_decision(&v, self_addr(), None),
            ReconcileOutcome::Rehydrate(Box::new(expected))
        );
    }

    #[test]
    fn reconcile_skips_when_the_same_channel_is_already_tracked() {
        // A healthy local row for the SAME channel is left untouched.
        let v = view(7, true, (0, 0, 0));
        let mut existing = sample(7);
        existing.channel_id = v.channel_id;
        assert_eq!(
            reconcile_decision(&v, self_addr(), Some(&existing)),
            ReconcileOutcome::Skip
        );
    }

    #[test]
    fn reconcile_defers_a_second_open_for_an_already_tracked_provider() {
        // A live local row for a DIFFERENT channel is authoritative and must NOT
        // be clobbered; the on-chain channel is a deferred orphan (logged, not
        // recovered now). This is the one-row-per-provider residual.
        let v = view(7, true, (0, 0, 0));
        let mut differing = sample(7);
        differing.channel_id = B256::repeat_byte(0x99);
        assert_eq!(
            reconcile_decision(&v, self_addr(), Some(&differing)),
            ReconcileOutcome::DeferredSecondOpen
        );
    }

    #[test]
    fn reconcile_skips_non_open_channels() {
        let v = view(8, false, (0, 0, 0));
        assert_eq!(
            reconcile_decision(&v, self_addr(), None),
            ReconcileOutcome::Skip,
            "a Closing/Closed channel needs no buyer reclaim"
        );
    }

    #[test]
    fn reconcile_skips_channels_not_owned_by_self() {
        let mut v = view(9, true, (0, 0, 0));
        v.client = address!("00000000000000000000000000000000000000bb");
        assert_eq!(
            reconcile_decision(&v, self_addr(), None),
            ReconcileOutcome::Skip,
            "a channel whose on-chain client is not us is never reclaimed for someone else"
        );
    }

    // ================================================================
    // #1143 — the singleflight, driven through the REAL service.
    //
    // The `WedgedOpener` fixture in `tests/node_origin_pull.rs` is a mock
    // `ChannelOpener` whose body re-implements the timeout, so it covers
    // `node_origin`'s candidate loop but says nothing about the mechanism here.
    // It passes with the budget bound removed from `open_or_reuse_channel`.
    // These drive `BuyerChannelService` itself: real `join_or_spawn_open`, real
    // `run_open` task, real `InFlightOpenGuard`.
    //
    // The chain is a JSON-RPC endpoint that answers nothing in time, so the open
    // wedges on its first fill — the state that matters, because that is when a
    // deposit is (or is about to be) escrowed and releasing the slot would let a
    // second `openChannel` through.
    // ================================================================

    /// Longer than any budget in these tests: every RPC the open issues hangs.
    const RPC_HANG: Duration = Duration::from_secs(30);
    /// What a caller in these tests is willing to wait on a wedged open.
    const CALLER_BUDGET: Duration = Duration::from_millis(200);
    /// A ceiling on the *call*, not the open. `open_or_reuse_channel` must return
    /// on `CALLER_BUDGET`; blowing this means it blocked on the open instead —
    /// which is exactly the pre-#1143 bug, so the failure must be a clean assert
    /// rather than a hung test.
    const CALL_CEILING: Duration = Duration::from_secs(5);

    async fn wedged_chain() -> wiremock::MockServer {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_delay(RPC_HANG))
            .mount(&server)
            .await;
        server
    }

    /// The real service, pointed at a chain that never answers. Built by struct
    /// literal rather than `bootstrap` because `bootstrap` self-checks the
    /// contract over RPC — which would itself wedge.
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    fn service_against(
        server: &wiremock::MockServer,
    ) -> BuyerChannelService<impl Provider + Clone + use<>> {
        let signer = Arc::new(PrivateKeySigner::random());
        let self_address = signer.address();
        let payment_channel = Address::repeat_byte(0xcc);
        let url = server
            .uri()
            .parse()
            .unwrap_or_else(|err| panic!("mock server uri must parse: {err}"));
        let provider = alloy::providers::ProviderBuilder::new()
            .wallet(alloy::network::EthereumWallet::from((*signer).clone()))
            .connect_http(url);

        BuyerChannelService {
            contract: PaymentChannel::new(payment_channel, provider),
            store: Arc::new(decdn_incentive::MemoryBuyerChannelStore::new()),
            signer: Arc::clone(&signer),
            voucher_domain: decdn_incentive::voucher::voucher_domain(1, payment_channel),
            token: Address::repeat_byte(0x11),
            self_address,
            min_deposit: U256::from(1u64),
            initial_deposit: U256::from(1u64),
            working_deposit: U256::from(1u64),
            opens_in_flight: Arc::new(Mutex::new(HashMap::new())),
            topups_in_flight: Arc::new(Mutex::new(HashSet::new())),
            reclaim_failures: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::new()),
            _reclaimer: AbortOnDrop(tokio::spawn(std::future::pending())),
            _reconciler: AbortOnDrop(tokio::spawn(std::future::pending())),
            _idle_reconciler: None,
        }
    }

    /// Call the real `open_or_reuse_channel` and require that it comes back on its
    /// own budget with a `ChannelOpenPending`.
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn expect_pending<P: Provider + Clone>(
        service: &BuyerChannelService<P>,
        provider: Address,
    ) {
        let call = tokio::time::timeout(
            CALL_CEILING,
            service.open_or_reuse_channel(provider, U256::from(1u64), CALLER_BUDGET),
        );
        let Ok(result) = call.await else {
            panic!(
                "open_or_reuse_channel blocked on the wedged open instead of returning after its \
                 {CALLER_BUDGET:?} budget — the caller's bound is gone (#1143)"
            );
        };
        let Err(err) = result else {
            panic!("a wedged open cannot yield a live channel");
        };
        assert!(
            err.downcast_ref::<ChannelOpenPending>().is_some(),
            "a caller that outran its budget gets the typed pending sentinel, not a failure: {err:#}"
        );
    }

    fn slot_held<P: Provider + Clone>(service: &BuyerChannelService<P>, provider: Address) -> bool {
        service
            .opens_in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&provider)
    }

    /// Reads clean once — satisfying `open_or_reuse_channel`'s fast-path miss — then
    /// faults on every later read, which is where `run_open`'s re-check under the
    /// slot lands. Models a `data_dir` that goes bad (corrupt page, fd exhaustion,
    /// disk full) rather than one that was never readable.
    #[derive(Debug)]
    struct StoreThatFaultsUnderTheSlot {
        reads: std::sync::atomic::AtomicUsize,
    }

    impl BuyerChannelStore for StoreThatFaultsUnderTheSlot {
        fn get_by_provider(&self, _p: Address) -> Result<Option<BuyerChannelState>, StoreError> {
            if self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                return Ok(None);
            }
            Err(StoreError::Backend("simulated store fault".to_string()))
        }
        fn get_by_channel_id(
            &self,
            _c: ChannelId,
        ) -> Result<Option<BuyerChannelState>, StoreError> {
            Ok(None)
        }
        fn load_all(&self) -> Result<BuyerLoad, StoreError> {
            Ok(BuyerLoad::default())
        }
        fn record(&self, _s: &BuyerChannelState) -> Result<(), StoreError> {
            Ok(())
        }
        fn forget(&self, _p: Address) -> Result<(), StoreError> {
            Ok(())
        }
        fn forget_if_channel(&self, _p: Address, _c: ChannelId) -> Result<bool, StoreError> {
            Ok(false)
        }
        fn advance_progress(
            &self,
            _p: Address,
            _c: ChannelId,
            _n: U256,
            _b: U256,
            _a: U256,
        ) -> Result<AdvanceOutcome, StoreError> {
            Ok(AdvanceOutcome::Advanced)
        }
        fn add_deposit(
            &self,
            _p: Address,
            _c: ChannelId,
            _additional: U256,
        ) -> Result<decdn_incentive::DepositOutcome, StoreError> {
            Ok(decdn_incentive::DepositOutcome::UnknownChannel)
        }
    }

    /// Value of a `decdn_<name>` counter in the scrape, or 0 when the line is absent.
    #[allow(clippy::expect_used)]
    fn counter(metrics: &Metrics, name: &str) -> u64 {
        let text = metrics.encode().expect("encode metrics");
        text.lines()
            .find_map(|line| line.strip_prefix(&format!("decdn_{name} ")))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0)
    }

    /// A store whose hydration always yields zero healthy channels and two
    /// undecodable rows. Lets a sweep exercise the skipped-row metric increment
    /// without any decodable channel driving a chain call. Shared by the reclaim
    /// and reconcile sweep tests so both call sites of
    /// `buyer_channel_store_skipped_undecodable_records` are covered.
    #[derive(Debug)]
    struct StoreWithSkippedRows;

    impl BuyerChannelStore for StoreWithSkippedRows {
        fn load_all(&self) -> Result<decdn_incentive::BuyerLoad, StoreError> {
            Ok(decdn_incentive::BuyerLoad {
                channels: Vec::new(),
                skipped: vec![B256::repeat_byte(0x31), B256::repeat_byte(0x32)],
            })
        }

        fn get_by_channel_id(
            &self,
            _c: ChannelId,
        ) -> Result<Option<BuyerChannelState>, StoreError> {
            Ok(None)
        }

        fn record(&self, _s: &BuyerChannelState) -> Result<(), StoreError> {
            Ok(())
        }

        fn forget(&self, _p: Address) -> Result<(), StoreError> {
            Ok(())
        }

        fn forget_if_channel(&self, _p: Address, _c: ChannelId) -> Result<bool, StoreError> {
            Ok(false)
        }

        fn get_by_provider(&self, _p: Address) -> Result<Option<BuyerChannelState>, StoreError> {
            Ok(None)
        }

        fn advance_progress(
            &self,
            _p: Address,
            _c: ChannelId,
            _n: U256,
            _b: U256,
            _a: U256,
        ) -> Result<AdvanceOutcome, StoreError> {
            Ok(AdvanceOutcome::UnknownChannel)
        }

        fn add_deposit(
            &self,
            _p: Address,
            _c: ChannelId,
            _additional: U256,
        ) -> Result<decdn_incentive::DepositOutcome, StoreError> {
            Ok(decdn_incentive::DepositOutcome::UnknownChannel)
        }
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn reclaim_sweep_counts_every_skipped_buyer_row() {
        let server = wiremock::MockServer::start().await;
        let service = service_against(&server);
        let store: Arc<dyn BuyerChannelStore> = Arc::new(StoreWithSkippedRows);
        let failures = Arc::new(Mutex::new(HashMap::new()));
        let metrics = Arc::new(Metrics::new());

        reclaim_once(
            &service.contract,
            &store,
            service.self_address,
            &failures,
            &metrics,
        )
        .await;

        assert_eq!(
            counter(
                &metrics,
                "buyer_channel_store_skipped_undecodable_records_total"
            ),
            2
        );
    }

    /// The idle-reconcile sweep counts skipped rows on its own `load_all` call,
    /// independently of the reclaim sweep (#1271). Both legs carry an identical
    /// `buyer_channel_store_skipped_undecodable_records` line, so a regression
    /// that drops or mis-destructures it on one leg while the other stays green
    /// would slip past a reclaim-only test.
    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn reconcile_sweep_counts_every_skipped_buyer_row() {
        let server = wiremock::MockServer::start().await;
        let service = service_against(&server);
        let store: Arc<dyn BuyerChannelStore> = Arc::new(StoreWithSkippedRows);
        let metrics = Arc::new(Metrics::new());

        // No healthy channel is returned, so the sweep never dials or touches the
        // chain — only the skipped-row count runs. The reconcile config is still
        // required by the signature; a bound endpoint and empty resolver satisfy
        // it without being reached.
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .bind()
            .await
            .expect("bind a local endpoint for the reconcile config");
        let config = BuyerReconcileConfig {
            endpoint,
            resolver: Arc::new(crate::dht::node_address::StaticNodeAddressDirectory::new(
                HashMap::new(),
            )),
        };
        let pending_store: Arc<dyn PendingSettleStore> =
            Arc::new(decdn_incentive::MemoryPendingSettleStore::new());
        let mut obs = HashMap::new();
        let mut close_failures = HashMap::new();

        reconcile_once(
            &service.contract,
            &store,
            &service.signer,
            &service.voucher_domain,
            &config,
            &mut obs,
            &mut close_failures,
            &pending_store,
            &metrics,
        )
        .await;

        assert_eq!(
            counter(
                &metrics,
                "buyer_channel_store_skipped_undecodable_records_total"
            ),
            2
        );
    }

    /// A store fault under the open slot must be LOUD (#1145 review). `run_open` is
    /// detached, so on the rotate leg — where `try_reclaim`'s receipt wait runs for
    /// minutes against a caller budget of seconds — every caller has already left
    /// with `ChannelOpenPending` by the time this fires. If the leg only bubbles up
    /// an error, nobody observes it: a node whose `data_dir` has gone bad silently
    /// cannot open a channel to anyone, while the operator sees only the *pending*
    /// counter climbing (whose documented meaning, "a slow L2", is the wrong
    /// diagnosis).
    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn a_store_fault_under_the_open_slot_is_reported_by_the_task() {
        let server = wedged_chain().await;
        let mut service = service_against(&server);
        service.store = Arc::new(StoreThatFaultsUnderTheSlot {
            reads: std::sync::atomic::AtomicUsize::new(0),
        });
        let metrics = Arc::clone(&service.metrics);
        let provider = Address::repeat_byte(0xab);

        // The fault is raised before any RPC, so this resolves well inside the budget.
        let err = service
            .open_or_reuse_channel(provider, U256::from(1u64), Duration::from_secs(5))
            .await
            .expect_err("a faulting store cannot yield a channel");

        assert!(
            err.downcast_ref::<OpenReported>().is_some(),
            "the open task must mark a store fault as already-reported, or a caller that happens \
             to still be waiting double-counts it: {err:#}"
        );
        assert_eq!(
            counter(&metrics, "node_pull_channel_open_failures_total"),
            1,
            "a store fault under the open slot must bump the channel-open failure counter from \
             inside the task — it is the only party guaranteed to see it"
        );
    }

    /// Panics on the read under the slot, so the open TASK unwinds.
    #[derive(Debug)]
    struct StoreThatPanicsUnderTheSlot {
        reads: std::sync::atomic::AtomicUsize,
        /// Stall before panicking, so the caller's budget expires first and the panic lands
        /// with NOBODY polling the shared open — the case that actually matters.
        delay: Duration,
    }

    impl BuyerChannelStore for StoreThatPanicsUnderTheSlot {
        #[allow(clippy::panic)]
        fn get_by_provider(&self, _p: Address) -> Result<Option<BuyerChannelState>, StoreError> {
            if self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                return Ok(None);
            }
            std::thread::sleep(self.delay);
            panic!("simulated panic inside the detached open task");
        }
        fn get_by_channel_id(
            &self,
            _c: ChannelId,
        ) -> Result<Option<BuyerChannelState>, StoreError> {
            Ok(None)
        }
        fn load_all(&self) -> Result<BuyerLoad, StoreError> {
            Ok(BuyerLoad::default())
        }
        fn record(&self, _s: &BuyerChannelState) -> Result<(), StoreError> {
            Ok(())
        }
        fn forget(&self, _p: Address) -> Result<(), StoreError> {
            Ok(())
        }
        fn forget_if_channel(&self, _p: Address, _c: ChannelId) -> Result<bool, StoreError> {
            Ok(false)
        }
        fn advance_progress(
            &self,
            _p: Address,
            _c: ChannelId,
            _n: U256,
            _b: U256,
            _a: U256,
        ) -> Result<AdvanceOutcome, StoreError> {
            Ok(AdvanceOutcome::Advanced)
        }
        fn add_deposit(
            &self,
            _p: Address,
            _c: ChannelId,
            _additional: U256,
        ) -> Result<decdn_incentive::DepositOutcome, StoreError> {
            Ok(decdn_incentive::DepositOutcome::UnknownChannel)
        }
    }

    /// A PANIC in the detached open task must be reported and must not wedge the provider
    /// (#1145 review).
    ///
    /// This leg is the one with no caller. `run_open`'s own error legs report themselves,
    /// but nothing reports the task *dying*: the `JoinError` surfaces inside a `Shared`
    /// future that only a caller ever polls, and with a caller budget of seconds against an
    /// unbounded receipt wait, the overwhelmingly likely case is that no caller is left to
    /// poll it. Reported from inside the wrapper — not handed to a caller who may not exist
    /// — a panicking open leaves a trace instead of only tokio's raw stderr line.
    ///
    /// Two assertions, and the second is the one that protects money:
    /// 1. the failure is metered, so an operator sees it at all;
    /// 2. the slot is RELEASED. `InFlightOpenGuard::drop` runs on unwind, so a panicking
    ///    open cannot wedge the provider for the life of the process. (Contrast the
    ///    timeout case above, where the slot must be HELD — the difference is that a panic
    ///    means no tx is in flight to protect.)
    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn a_panicking_open_task_is_reported_and_frees_the_provider() {
        let server = wedged_chain().await;
        let mut service = service_against(&server);
        service.store = Arc::new(StoreThatPanicsUnderTheSlot {
            reads: std::sync::atomic::AtomicUsize::new(0),
            delay: Duration::ZERO,
        });
        let metrics = Arc::clone(&service.metrics);
        let provider = Address::repeat_byte(0xab);

        let err = service
            .open_or_reuse_channel(provider, U256::from(1u64), Duration::from_secs(5))
            .await
            .expect_err("a panicking open cannot yield a channel");

        assert!(
            err.downcast_ref::<OpenReported>().is_some(),
            "a dead open task must mark itself reported — it fires the metric and the log \
             itself, precisely because no caller is guaranteed to be listening: {err:#}"
        );
        assert_eq!(
            counter(&metrics, "node_pull_channel_open_failures_total"),
            1,
            "a panicking open task must bump the channel-open failure counter; unmetered, a \
             panicking open is visible only as tokio's unstructured stderr line"
        );
        assert!(
            !slot_held(&service, provider),
            "the guard must release the provider's slot on unwind — otherwise one panic \
             wedges that provider for the life of the process"
        );
    }

    /// …and the case that actually matters: the open panics with **nobody left polling**.
    ///
    /// The test above keeps a caller waiting the whole time, which is the EASY half. The
    /// half that protects money is this one: a panic during the unbounded receipt wait —
    /// minutes long, so no caller is still there by construction — is exactly when an
    /// `openChannel` may already be in the mempool, i.e. a deposit escrowed against no
    /// persisted row.
    ///
    /// So the report must come from a SPAWNED supervisor, and this test is what holds it
    /// there. Reporting from a combinator on the returned future would fire only while a
    /// caller was still waiting: a `Shared` advances only when a clone is POLLED, and once
    /// the caller's budget expires the sole remaining clone is the one parked in
    /// `opens_in_flight`, which nobody polls. The report would then be unreachable in exactly
    /// the case it exists for, and its sibling test above — which keeps a caller waiting
    /// throughout — would stay green and say nothing. Hence this one, which lets the caller
    /// LEAVE before the panic lands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn a_panicking_open_is_reported_even_when_no_caller_is_left_to_see_it() {
        let server = wedged_chain().await;
        let mut service = service_against(&server);
        service.store = Arc::new(StoreThatPanicsUnderTheSlot {
            reads: std::sync::atomic::AtomicUsize::new(0),
            delay: Duration::from_millis(300),
        });
        let metrics = Arc::clone(&service.metrics);
        let provider = Address::repeat_byte(0xab);

        // The caller gives up long before the task panics, and drops its clone of the
        // shared open — so from here on nothing polls it.
        let err = service
            .open_or_reuse_channel(provider, U256::from(1u64), Duration::from_millis(30))
            .await
            .expect_err("the caller must give up on its budget");
        assert!(
            err.downcast_ref::<ChannelOpenPending>().is_some(),
            "the caller left on its budget, so it holds a pending sentinel: {err:#}"
        );

        // Now let the task panic, with no caller in sight. Polled rather than slept on a
        // fixed delay: the store blocks a worker thread for 300 ms, and under a loaded
        // full-suite run a fixed sleep is a flake waiting to happen.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while counter(&metrics, "node_pull_channel_open_failures_total") == 0
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert_eq!(
            counter(&metrics, "node_pull_channel_open_failures_total"),
            1,
            "a panicking open must be metered even when NOBODY is polling the shared open — \
             this is the leg where an escrowed deposit goes missing, and a report that only \
             fires for a caller who is still waiting is no report at all"
        );
        assert!(
            !slot_held(&service, provider),
            "the guard must still release the provider's slot on unwind"
        );
    }

    /// The load-bearing one. A caller that gives up must NOT take the provider's
    /// open slot with it: the `openChannel` may already be in the mempool, and a
    /// released slot lets the next miss escrow a SECOND deposit against the same
    /// provider — which the boot reconcile scan then declines to adopt
    /// (`DeferredSecondOpen`), stranding it past the event lookback.
    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn a_caller_that_times_out_leaves_the_open_slot_held() {
        let server = wedged_chain().await;
        let service = service_against(&server);
        let provider = Address::repeat_byte(0xab);

        expect_pending(&service, provider).await;

        assert!(
            slot_held(&service, provider),
            "the departed caller released the provider's open slot while its openChannel is still \
             in flight — the next miss would escrow a second deposit (#1143)"
        );
    }

    /// The other half: a second miss arriving while the first open is still wedged
    /// must JOIN it, not start its own. Asserted on what actually reaches the
    /// chain, because that is what costs money.
    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    async fn a_second_caller_joins_the_in_flight_open_rather_than_opening_again() {
        let server = wedged_chain().await;
        let service = service_against(&server);
        let provider = Address::repeat_byte(0xab);

        expect_pending(&service, provider).await;
        // Every response is stalled for RPC_HANG, so the first open cannot issue
        // any follow-up call; its request count is settled by the time it yields.
        let after_first = server.received_requests().await.map_or(0, |r| r.len());
        assert!(
            after_first > 0,
            "the first open must have actually reached the chain, or this proves nothing"
        );

        expect_pending(&service, provider).await;
        let after_second = server.received_requests().await.map_or(0, |r| r.len());

        assert_eq!(
            after_second, after_first,
            "the second caller started a SECOND openChannel instead of joining the one in flight \
             — that is a duplicate escrowed deposit (#1143)"
        );
        assert_eq!(
            service
                .opens_in_flight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1,
            "one provider mid-open must occupy exactly one slot"
        );
    }
}
