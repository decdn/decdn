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
use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use decdn_common::redact::sanitize_rpc_display;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{
    AdvanceOutcome, BuyerChannelState, BuyerChannelStore, ChannelId, DepositOutcome, PendingSettle,
    PendingSettleStore, StoreError, Voucher,
};
use iroh::{Endpoint, EndpointAddr, PublicKey};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::client_requester::ChannelContext;
// The buyer-channel open kernel (#940) — the `openChannel` tx + `ChannelOpened`
// decode + state/ctx build, and the one-time USDC approval — now live in the
// shared `decdn-client-pull` crate (re-exported here as `client_requester`).
use crate::client_requester::buyer_channel::{OpenedChannel, ensure_allowance, open_channel};
use crate::client_requester::cooperative_close::{
    AuthorizedWatermark, CooperativeCloseOutcome, cooperative_close,
};
use crate::dht::NodeAddressResolver;
use crate::metrics::{Metrics, SettleParty};
use crate::payment_settlement::{
    MAX_BACKFILL_BLOCK_SPAN, backfill_windows, check_backfill_range, settle_pass, unix_now,
};

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

/// How many blocks back from head the one-shot bootstrap reconciliation scan
/// looks for orphaned `ChannelOpened(client == self)` events (#763). The buyer
/// has no scan checkpoint (unlike the seller watcher): orphans only arise from
/// the two rare post-escrow failure legs in [`BuyerChannelService::open_and_persist`]
/// (event-decode or `store.record` failing *after* the on-chain escrow) or a
/// postcard-undecodable row after a binary downgrade — all of which strand a
/// deposit seconds before the next restart. A modest fixed lookback (~1–2 days
/// of an Arbitrum-Sepolia-class ~0.25 s/block L2) recovers those realistic cases
/// cheaply without re-scanning the full chain every boot. An orphan older than
/// this window is missed (it is reclaim-able only after its long expiry anyway);
/// promote to config if operators need a full-lifetime scan.
const BUYER_RECONCILE_LOOKBACK_BLOCKS: u64 = 700_000;

/// Aborts the wrapped task on drop so a node-restart cycle never leaks the
/// reclaim-sweep task. Same pattern as the seller service's `AbortOnDrop`.
#[derive(Debug)]
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// RAII slot in the per-provider in-flight-open set. Dropping it removes the
/// provider so every path out of the open routine — success, error, or early
/// return — releases the slot and the provider is never wedged. See
/// [`BuyerChannelService::opens_in_flight`].
struct InFlightOpenGuard {
    set: Arc<Mutex<HashSet<Address>>>,
    provider: Address,
}

impl InFlightOpenGuard {
    /// Claim the open slot for `provider`. Returns `Ok(None)` when an open for
    /// that provider is already in flight (the caller bails for retry), or
    /// `Ok(Some(guard))` holding the slot until drop. Folding the set-insert and
    /// the guard into one constructor makes the one-open-per-provider invariant
    /// impossible to violate by construction: a guard cannot exist without a
    /// successful claim, and a claim cannot succeed without yielding a guard.
    fn claim(set: &Arc<Mutex<HashSet<Address>>>, provider: Address) -> Result<Option<Self>> {
        // Acquisition treats a poisoned lock as fatal (`?`-bail) — unlike `Drop`
        // below, which must still release the slot. The set carries no
        // cross-element invariant, but refusing to *acquire* on a poisoned lock
        // surfaces the prior panic instead of papering over it.
        let mut in_flight = set
            .lock()
            .map_err(|err| anyhow::anyhow!("opens_in_flight mutex poisoned: {err}"))?;
        if !in_flight.insert(provider) {
            return Ok(None);
        }
        Ok(Some(Self {
            set: Arc::clone(set),
            provider,
        }))
    }
}

impl Drop for InFlightOpenGuard {
    fn drop(&mut self) {
        // A poisoned lock means a prior holder panicked; recover the inner set
        // and still release the slot rather than leaving the provider stuck.
        let mut set = self
            .set
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set.remove(&self.provider);
    }
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
    default_deposit: U256,
    /// Providers with an `openChannel` currently in flight. Makes the
    /// one-channel-per-provider invariant real (PR #753 review): a concurrent
    /// [`Self::open_or_reuse_channel`] for a provider already mid-open bails for
    /// retry instead of escrowing a second deposit whose `record` would orphan
    /// the first. Only the open path touches this set — pure reuse never
    /// contends, so many concurrent pulls to an already-open provider proceed
    /// freely.
    opens_in_flight: Arc<Mutex<HashSet<Address>>>,
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
    /// `default_deposit` is the deposit used when a caller does not specify a
    /// larger one; it is clamped up to the on-chain `minDeposit`.
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
        default_deposit: U256,
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
            ensure_allowance(&provider, token, self_address, payment_channel_addr).await?;
        }

        let tracked = store
            .load_all()
            .context("hydrate persisted buyer channels")?
            .len();
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
        let opens_in_flight = Arc::new(Mutex::new(HashSet::new()));

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
            default_deposit,
            opens_in_flight,
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
    /// actual deposit is `max(deposit_hint, default_deposit, min_deposit)`. The
    /// hint is ignored when an existing channel is reused (call
    /// [`Self::top_up`] to add funds to a live channel).
    ///
    /// # Errors
    ///
    /// Surfaces store errors and any failure of the `openChannel` transaction
    /// (submit, revert, or receipt). Also errors if a tracked-but-expired
    /// channel for `provider_addr` could not be reclaimed first (so its deposit
    /// is never silently dropped — see below); retry once the reclaim sweep
    /// clears it.
    ///
    /// # Concurrency
    ///
    /// Opens are serialized per provider via an in-flight-open set. Concurrent
    /// `open_or_reuse_channel` calls for the *same* `provider_addr` that both
    /// miss the reuse fast-path race for one open slot: the winner opens, the
    /// others bail with a retryable error (the resulting channel is reused on
    /// retry). This enforces the one-channel-per-provider invariant — two racing
    /// opens would otherwise each escrow a deposit and the second `record` would
    /// orphan the first. Pure reuse of an already-open channel never contends,
    /// and opens for *distinct* providers run in parallel.
    // Fast-path reuse → per-provider open guard → re-check → reclaim-expired →
    // open → decode → persist; the early-return guards read more clearly inline.
    #[allow(clippy::cognitive_complexity)]
    pub async fn open_or_reuse_channel(
        &self,
        provider_addr: Address,
        deposit_hint: U256,
    ) -> Result<ChannelContext> {
        // Fast path: reuse a live channel without touching the in-flight set, so
        // many concurrent pulls to an already-open provider never serialize.
        if let Some(ctx) = self.try_reuse_live(provider_addr)? {
            return Ok(ctx);
        }

        // No live channel — we are about to open (or rotate an expired one).
        // Claim the per-provider open slot; if another open is already in flight
        // for this provider, bail for retry rather than escrow a second deposit
        // whose `record` would orphan the first.
        let Some(_open_guard) = InFlightOpenGuard::claim(&self.opens_in_flight, provider_addr)?
        else {
            anyhow::bail!(
                "openChannel for provider {provider_addr} is already in flight; retry once it \
                 completes (the resulting channel will be reused)"
            );
        };

        // Re-check under the slot: a concurrent open may have created the channel
        // between the fast-path miss and claiming the slot (closes the TOCTOU).
        if let Some(ctx) = self.try_reuse_live(provider_addr)? {
            return Ok(ctx);
        }

        // Any record still present here is expired (the re-check above returned
        // for a live one). Reclaim its deposit BEFORE rotating: the store is
        // provider-keyed, so opening a replacement would overwrite the expired
        // record and the reclaim sweep (which iterates `load_all`) would never
        // see it — silently abandoning a refundable deposit (10 USDC default +
        // any top-ups). `try_reclaim` is best-effort and CAS-forgets on success.
        if let Some(existing) = self
            .store
            .get_by_provider(provider_addr)
            .context("look up expired buyer channel before reopen")?
        {
            debug!(
                provider = %provider_addr,
                channel_id = %existing.channel_id,
                "tracked buyer channel expired; reclaiming before opening a replacement"
            );
            // Outcome intentionally ignored here: a persistent failure on this
            // one-off open-path reclaim is already surfaced to the caller as a
            // retryable error below — the consecutive-failure escalation (#906)
            // is the background sweep's job, not this synchronous open.
            let _ = try_reclaim(&self.contract, &self.store, self.self_address, &existing).await;
            // If the expired record is still present (reclaim hit an RPC error,
            // or the chain clock has not yet reached expiry under host-clock
            // skew), do NOT open a replacement that would overwrite and orphan
            // it — surface an error so the caller retries after the sweep clears
            // it. This trades a transient open failure for never dropping funds.
            if self
                .store
                .get_by_provider(provider_addr)
                .context("re-check expired channel after reclaim")?
                .is_some_and(|s| s.channel_id == existing.channel_id)
            {
                anyhow::bail!(
                    "expired buyer channel {} (provider {provider_addr}) is not yet reclaimable; \
                     retry after the reclaim sweep clears it",
                    existing.channel_id
                );
            }
        }

        self.open_and_persist(provider_addr, deposit_hint).await
    }

    /// Open a fresh channel on-chain against `provider_addr` and persist it.
    /// Called by [`Self::open_or_reuse_channel`] once it holds the per-provider
    /// open slot and has confirmed no live channel exists. The deposit is
    /// `max(deposit_hint, default_deposit, min_deposit)`.
    async fn open_and_persist(
        &self,
        provider_addr: Address,
        deposit_hint: U256,
    ) -> Result<ChannelContext> {
        let deposit = deposit_hint.max(self.default_deposit).max(self.min_deposit);

        // The openChannel tx, the authoritative ChannelOpened-from-receipt
        // decode, and the state/ctx construction are the shared kernel (#940).
        // What stays node-specific: from the moment the kernel returns, the
        // deposit is escrowed on-chain, so a failure to persist locally leaves
        // it tracked ONLY on-chain. The reclaim sweep iterates `load_all` and so
        // never sees an unpersisted channel, so we escalate to `error!` with the
        // open tx for manual reconcile (the bootstrap reconciliation scan, #763,
        // also covers this on the next restart).
        let OpenedChannel { state, ctx, tx } = open_channel(
            &self.contract,
            Arc::clone(&self.signer),
            &self.voucher_domain,
            self.token,
            self.self_address,
            provider_addr,
            deposit,
        )
        .await?;

        if let Err(err) = self.store.record(&state) {
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
            return Err(err).context("persist newly-opened buyer channel");
        }

        Ok(ctx)
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
            AdvanceOutcome::UnknownProvider => {
                anyhow::bail!("record_progress for unknown provider {provider_addr}")
            }
            // The provider's slot was replaced by a newer open between the
            // delivery and this write. Recording stale progress onto the new
            // channel would be wrong; the older channel's record is gone, so this
            // is not an escalation (it mirrors the reclaim sweep's
            // `forget_if_channel` miss) and must not fail the already-paid pull.
            // But a delivered-and-paid voucher's progress was dropped, and a
            // *sustained* rate here would mean a `channel_id`-plumbing bug rather
            // than the rare benign mid-pull replacement — so `warn!`, not
            // `debug!`, to keep it observable.
            AdvanceOutcome::ChannelMismatch => {
                warn!(
                    provider = %provider_addr,
                    %channel_id,
                    "record_progress: provider channel replaced by a newer open; \
                     skipping stale progress write"
                );
                Ok(())
            }
            AdvanceOutcome::Regressed(err) => Err(anyhow::Error::new(err))
                .with_context(|| format!("advance progress for provider {provider_addr}")),
        }
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
        // Read the channel_id BEFORE the RPC — we need it for `topUp` and as the
        // channel-id guard on the post-RPC write.
        let channel_id = self
            .store
            .get_by_provider(provider_addr)
            .context("look up buyer channel for top-up")?
            .with_context(|| format!("top_up for unknown provider {provider_addr}"))?
            .channel_id;
        let receipt = self
            .contract
            .topUp(channel_id, additional)
            .send()
            .await
            .context("submit topUp")?
            .get_receipt()
            .await
            .context("await topUp receipt")?;
        if !receipt.status() {
            anyhow::bail!("topUp reverted for channel {channel_id}");
        }
        let tx = receipt.transaction_hash;
        // Add to the *committed* deposit inside a write txn (never the pre-RPC
        // snapshot), channel-id-guarded so a concurrent advance/reuse during the
        // RPC is not clobbered (#838). USDC is not fee-on-transfer, so the local
        // `+= additional` always matches the contract's `+= received`.
        match self
            .store
            .add_deposit(provider_addr, channel_id, additional)
            .context("persist buyer channel top-up")?
        {
            DepositOutcome::Added(new_deposit) => {
                info!(
                    provider = %provider_addr,
                    %channel_id,
                    %new_deposit,
                    %tx,
                    "topped up buyer channel"
                );
                Ok(())
            }
            // The on-chain topUp already credited `channel_id`, but the local row
            // vanished during the RPC. Funds are escrowed on-chain with zero
            // local tracking — `error!` (matching `open_and_persist`'s
            // escrowed-but-untracked posture) and surface the tx for reconcile.
            DepositOutcome::UnknownProvider => {
                error!(
                    provider = %provider_addr,
                    %channel_id,
                    %additional,
                    %tx,
                    "top_up: on-chain topUp landed but no local channel record exists to credit; \
                     deposit is escrowed on-chain and untracked — reconcile against the tx"
                );
                Ok(())
            }
            // A row still exists (for a different channel), so the provider stays
            // reclaimable — less severe than `UnknownProvider`, hence `warn!`.
            DepositOutcome::ChannelMismatch => {
                warn!(
                    provider = %provider_addr,
                    %channel_id,
                    %additional,
                    %tx,
                    "top_up: provider channel replaced during the topUp RPC; the on-chain deposit \
                     was credited to the topped-up channel but the local record now tracks a \
                     different channel — reconcile against the tx"
                );
                Ok(())
            }
        }
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
}

#[async_trait::async_trait]
impl<P: Provider + Clone + 'static> ChannelOpener for BuyerChannelService<P> {
    async fn open_or_reuse_channel(
        &self,
        provider_addr: Address,
        deposit_hint: U256,
    ) -> Result<ChannelContext> {
        BuyerChannelService::open_or_reuse_channel(self, provider_addr, deposit_hint).await
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
    let states = match store.load_all() {
        Ok(s) => s,
        Err(err) => {
            warn!(%err, "buyer reconcile sweep: failed to load channel state");
            return;
        }
    };
    let now = unix_now();
    let mut seen: HashSet<ChannelId> = HashSet::new();
    for st in &states {
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
    if !close_unilateral(contract, signer, voucher_domain, pending_store, metrics, st).await {
        // The close did not land. Reconcile against on-chain status before
        // deciding to retry: a deterministic revert here usually means the
        // channel is ALREADY `Closing`/`Closed` (the provider closed it, or a
        // prior attempt of ours landed but we missed the receipt). Retrying a
        // `closeChannel` against a non-`Open` channel just reverts every sweep
        // for up to the 90-day expiry — wasted RPC, and gas if the revert isn't
        // caught at estimation. Only a still-`Open` channel (a genuine transient
        // close failure) is left for the next sweep to retry.
        reconcile_failed_close(contract, store, pending_store, st, obs, close_failures).await;
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
///   co-close). Record the settle obligation (idempotent re-stamp) and retire
///   the local record so we stop re-submitting.
/// - `Closed` — already finalized; just retire the local record.
/// - `Open` — a genuine transient close failure; keep the record + tallies so
///   the next sweep retries.
/// - read error — keep everything and retry next sweep.
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
        // record the settle obligation before retiring the record below.
        PaymentChannel::Status::Closing => {
            info!(
                channel_id = %st.channel_id, provider = %st.provider,
                "reconcile: channel already Closing on-chain (a prior close landed); recording \
                 settle obligation and retiring the local record"
            );
            record_pending_after_unilateral_close(contract, pending_store, st.channel_id).await;
        }
        PaymentChannel::Status::Closed => {
            info!(
                channel_id = %st.channel_id, provider = %st.provider,
                "reconcile: channel already Closed on-chain; retiring the local record"
            );
        }
        // `Open` (or any other status) means the close genuinely failed
        // transiently — keep the record + tallies so the next sweep retries.
        _ => return,
    }
    // Reached only for Closing/Closed: stop re-submitting against a non-Open
    // channel by retiring the local record + idle/close tallies.
    obs.remove(&st.channel_id);
    close_failures.remove(&st.channel_id);
    forget_after_close(store, st);
}

/// Submit a unilateral `closeChannel` for `st` signed over the buyer's own
/// highest persisted watermark, and on success record a [`PendingSettle`] entry
/// (re-reading `disputeDeadline`) so the buyer settle sweep finalizes it after
/// the dispute window. Returns `true` only when the close landed on-chain.
/// Routes the on-chain outcome to the buyer close metrics (#989): a landed close
/// to `buyer_unilateral_close_ok`, an RPC/receipt error or revert to
/// `buyer_unilateral_close_rpc_failure`.
// Linear guard-and-act sequence (sign → send → receipt → branch on status) with
// per-arm metric + logging; splitting it obscures the flow, mirroring
// `try_reclaim` and the seller `send_close`.
#[allow(clippy::cognitive_complexity)]
async fn close_unilateral<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    client_signer: &Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    pending_store: &Arc<dyn PendingSettleStore>,
    metrics: &Arc<Metrics>,
    st: &BuyerChannelState,
) -> bool {
    // Sign our own voucher over the persisted watermark — the client sig the
    // contract checks against `channel.client` (this node). Honest by
    // construction: we close at the highest amount we already authorized, and
    // the dispute window protects the absent provider against a stale nonce.
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
    let pending = match contract
        .closeChannel(
            st.channel_id,
            st.last_amount,
            st.last_nonce,
            st.last_bytes_delivered,
            sig,
        )
        .send()
        .await
    {
        Ok(pending) => pending,
        Err(err) => {
            metrics.buyer_unilateral_close_rpc_failure();
            warn!(
                channel_id = %st.channel_id, err = %sanitize_rpc_display(&err),
                "reconcile: unilateral closeChannel send failed; leaving for next sweep / expiry reclaim"
            );
            return false;
        }
    };
    match pending.get_receipt().await {
        Ok(receipt) if receipt.status() => {
            metrics.buyer_unilateral_close_ok();
            info!(
                channel_id = %st.channel_id, provider = %st.provider,
                tx = %receipt.transaction_hash,
                "reconcile: unilateral closeChannel landed (dispute window open); will settle after window"
            );
            record_pending_after_unilateral_close(contract, pending_store, st.channel_id).await;
            true
        }
        Ok(receipt) => {
            metrics.buyer_unilateral_close_rpc_failure();
            warn!(
                channel_id = %st.channel_id, tx = %receipt.transaction_hash,
                "reconcile: unilateral closeChannel reverted on-chain (channel may already be \
                 closing/closed); caller reconciles on-chain status"
            );
            false
        }
        Err(err) => {
            metrics.buyer_unilateral_close_rpc_failure();
            warn!(
                channel_id = %st.channel_id, err = %sanitize_rpc_display(&err),
                "reconcile: unilateral closeChannel receipt failed; leaving for next sweep / expiry reclaim"
            );
            false
        }
    }
}

/// After a unilateral `closeChannel` lands, re-read the channel for the
/// `disputeDeadline` it just set and persist a [`PendingSettle`] entry so the
/// buyer settle sweep can `settleChannel` (and reclaim the deposit refund) once
/// the window elapses (#988). Best-effort: a failed read/write is logged, not
/// fatal — the close already opened the window. Note the channel is now
/// `Closing`, so the expiry-reclaim sweep (which needs `Open`) is NOT the
/// fallback here. Unlike the seller path — whose lost obligations are
/// re-derived on the next boot by the closing-reconciliation backfill (#839),
/// which is provider-only and never re-derives a buyer/client-side close —
/// the buyer has NO automatic recovery for a lost entry: the refund settles
/// only when someone calls `settleChannel` after the window. That call is
/// permissionless, so the operator can recover it manually (the `error!`s below
/// carry the channel id), but it will not self-heal — hence `error!`, matching
/// the seller path's `record_pending_after_close` which `error!`s on the same
/// loss.
async fn record_pending_after_unilateral_close<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    pending_store: &Arc<dyn PendingSettleStore>,
    channel_id: ChannelId,
) {
    let settle_after = match contract.getChannel(channel_id).call().await {
        Ok(ch) => ch.disputeDeadline,
        Err(err) => {
            error!(
                err = %sanitize_rpc_display(&err), %channel_id,
                "reconcile: post-close getChannel failed; settle obligation NOT recorded — the \
                 channel is Closing, so call settleChannel(<channel_id>) manually after the \
                 dispute window to reclaim the deposit refund"
            );
            return;
        }
    };
    let entry = PendingSettle {
        channel_id,
        settle_after,
    };
    if let Err(err) = pending_store.record_pending(&entry) {
        error!(
            %err, %channel_id, settle_after,
            "reconcile: failed to persist buyer pending-settle entry; call \
             settleChannel(<channel_id>) manually after the dispute window to reclaim the refund"
        );
    } else {
        debug!(%channel_id, settle_after, "reconcile: recorded buyer channel for post-dispute settlement");
    }
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
    let states = match store.load_all() {
        Ok(s) => s,
        Err(err) => {
            warn!(%err, "buyer reclaim sweep: failed to load channel state");
            return;
        }
    };
    let now = unix_now();
    let mut seen: HashSet<ChannelId> = HashSet::new();
    for st in &states {
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

    let receipt = match contract.reclaimExpired(st.channel_id).send().await {
        Ok(pending) => match pending.get_receipt().await {
            Ok(r) => r,
            Err(err) => {
                warn!(err = %sanitize_rpc_display(&err), channel_id = %st.channel_id, "buyer reclaim: receipt failed");
                return ReclaimOutcome::Failed;
            }
        },
        Err(err) => {
            warn!(err = %sanitize_rpc_display(&err), channel_id = %st.channel_id, "buyer reclaim: send failed");
            return ReclaimOutcome::Failed;
        }
    };
    if !receipt.status() {
        warn!(
            channel_id = %st.channel_id,
            tx = %receipt.transaction_hash,
            "reclaimExpired reverted on-chain; leaving record for retry"
        );
        return ReclaimOutcome::Failed;
    }
    forget_reclaimed(store, st, "reclaimed expired buyer channel deposit")
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

/// Reconcile one `ChannelOpened` event: confirm it is ours, read authoritative
/// on-chain state, and re-hydrate the local store if the channel is an orphan.
/// Returns `Ok(true)` when a row was (re)hydrated, `Ok(false)` when skipped, and
/// `Err` only on a per-event fault (a `getChannel` RPC error) the caller logs
/// and steps past.
// Linear guard sequence (ownership filter → getChannel → per-provider slot →
// store-read with fault/corrupt split → decide → record); splitting would
// scatter the atomicity reasoning.
#[allow(clippy::cognitive_complexity)]
async fn reconcile_one_opened<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn BuyerChannelStore>,
    self_address: Address,
    opens_in_flight: &Arc<Mutex<HashSet<Address>>>,
    event: &PaymentChannel::ChannelOpened,
) -> Result<bool> {
    // The query already topic-filters on `client == self_address` (the indexed
    // `client` topic), so in practice every event here is ours; this is a
    // defense-in-depth check against a misbehaving RPC that ignores the topic.
    if event.client != self_address {
        return Ok(false);
    }
    let ch = contract
        .getChannel(event.channelId)
        .call()
        .await
        .with_context(|| format!("reconcile getChannel for {}", event.channelId))?;

    // Serialize against the live open path for this provider. The reconciler is a
    // second writer to the provider-keyed store, racing concurrent cache-miss
    // opens the moment bootstrap returns; without this guard our re-hydration
    // could overwrite (orphan) a channel a live open just escrowed and recorded.
    // Claiming the same per-provider slot the live path uses makes the read +
    // record below atomic with respect to opens: a live open in flight → we skip
    // (it persists the real channel); otherwise the slot is ours until drop.
    let Some(_open_guard) = InFlightOpenGuard::claim(opens_in_flight, ch.provider)? else {
        debug!(
            provider = %ch.provider,
            channel_id = %event.channelId,
            "buyer reconcile: a live open is in flight for this provider; skipping (it persists the real channel)"
        );
        return Ok(false);
    };

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
                channel_id = %event.channelId,
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
                channel_id = %event.channelId,
                %err,
                "buyer reconcile: store read failed (backend/IO); skipping to avoid clobbering a possibly-healthy row"
            );
            return Ok(false);
        }
    };
    let view = OnChainOpen {
        channel_id: event.channelId,
        client: ch.client,
        provider: ch.provider,
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
}

/// One-shot bootstrap reconciliation scan (#763): enumerate
/// `ChannelOpened(client == self)` over the last [`BUYER_RECONCILE_LOOKBACK_BLOCKS`]
/// blocks and re-hydrate any still-`Open` channel missing or undecodable in the
/// local store, so the reclaim sweep can recover its deposit. Best-effort: a
/// head-read failure `warn!`s and returns; a single window's `query()` failure is
/// logged and skipped so the *other* windows still reconcile (unlike the seller,
/// the buyer keeps no checkpoint, so the lost window is only re-covered on a
/// later restart); per-event faults are logged and skipped. The completion log
/// reports the failed-window count so a partial scan is observable. Mirrors the
/// seller backfill in [`crate::payment_settlement`], reusing its window helpers.
// Linear scan (head → windows → query → per-event) with inline best-effort
// guards; splitting would obscure the control flow.
#[allow(clippy::cognitive_complexity)]
async fn reconcile_orphans_once<P: Provider + Clone>(
    contract: PaymentChannel::PaymentChannelInstance<P>,
    store: Arc<dyn BuyerChannelStore>,
    self_address: Address,
    opens_in_flight: Arc<Mutex<HashSet<Address>>>,
) {
    let head = match contract.provider().get_block_number().await {
        Ok(h) => h,
        Err(err) => {
            warn!(err = %sanitize_rpc_display(&err), "buyer reconcile: head block read failed; skipping scan this boot");
            return;
        }
    };
    let start = head.saturating_sub(BUYER_RECONCILE_LOOKBACK_BLOCKS);
    if let Err(err) = check_backfill_range(start, head) {
        warn!(%err, start, head, "buyer reconcile: invalid scan range; skipping");
        return;
    }
    let windows = backfill_windows(start, head, MAX_BACKFILL_BLOCK_SPAN);
    let total_windows = windows.len();
    let mut rehydrated: usize = 0;
    let mut failed_windows: usize = 0;
    for (from, to) in windows {
        let logs = match contract
            .ChannelOpened_filter()
            // RPC-level filter on the indexed `client` topic so `eth_getLogs`
            // returns only this node's own opens — bounds result-count/latency on
            // busy deployments instead of fetching every open in the window.
            .topic2(self_address)
            .from_block(from)
            .to_block(to)
            .query()
            .await
        {
            Ok(logs) => logs,
            Err(err) => {
                // Skip just this window, not the whole scan: the windows are
                // independent, so a transient `eth_getLogs` failure on one should
                // not strand orphans in later windows until the next restart.
                warn!(
                    err = %sanitize_rpc_display(&err),
                    from,
                    to,
                    "buyer reconcile: ChannelOpened query failed for this window; skipping it"
                );
                failed_windows = failed_windows.saturating_add(1);
                continue;
            }
        };
        for (event, _log) in logs {
            match reconcile_one_opened(&contract, &store, self_address, &opens_in_flight, &event)
                .await
            {
                Ok(true) => rehydrated = rehydrated.saturating_add(1),
                Ok(false) => {}
                Err(err) => warn!(
                    err = %sanitize_rpc_display(&err),
                    channel_id = %event.channelId,
                    "buyer reconcile: skipping event after a per-event fault"
                ),
            }
        }
    }
    // If every window's query failed, the scan accomplished nothing this boot —
    // escalate to `error!` so an RPC outage is visible above the per-window warns,
    // not buried under an `info!` "complete". Otherwise report normally.
    if total_windows > 0 && failed_windows == total_windows {
        error!(
            start,
            head,
            failed_windows,
            "buyer reconcile: every window's ChannelOpened query failed; reconciliation \
             accomplished nothing this boot (RPC outage?) — orphans recovered on a later restart"
        );
    } else {
        info!(
            start,
            head, rehydrated, failed_windows, "buyer reconcile: bootstrap scan complete"
        );
    }
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
            address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            U256::from(10_000_000u64),
            1_900_000_000,
        )
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

    /// The per-provider in-flight-open slot refuses a second concurrent claim
    /// (the open path bails for retry) and frees on guard drop, so a later open
    /// proceeds. This is the mechanism that makes the one-channel-per-provider
    /// invariant real rather than a documented caller contract (#753 review).
    #[test]
    fn in_flight_open_guard_refuses_concurrent_then_releases() {
        let set: Arc<Mutex<HashSet<Address>>> = Arc::new(Mutex::new(HashSet::new()));
        let provider = sample(7).provider;
        // Exercises the production `claim` constructor (the same call
        // `open_or_reuse_channel` makes), not a re-implementation of it.
        // `.ok().flatten()` discards the (impossible here) poison error.
        let guard = InFlightOpenGuard::claim(&set, provider).ok().flatten();
        assert!(guard.is_some(), "first claim acquires the open slot");
        assert!(
            InFlightOpenGuard::claim(&set, provider)
                .ok()
                .flatten()
                .is_none(),
            "a concurrent claim for the same provider is refused"
        );
        drop(guard);
        let reclaimed = InFlightOpenGuard::claim(&set, provider).ok().flatten();
        assert!(
            reclaimed.is_some(),
            "the slot is free once the prior guard drops"
        );
        drop(reclaimed);
        assert!(
            set.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "dropping the guard releases the slot",
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
        let expected =
            BuyerChannelState::new(v.channel_id, v.provider, v.token, v.deposit, v.expires_at);
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
        let mut expected =
            BuyerChannelState::new(v.channel_id, v.provider, v.token, v.deposit, v.expires_at);
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
}
