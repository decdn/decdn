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

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use decdn_incentive::erc20::Erc20;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{
    AdvanceOutcome, BuyerChannelState, BuyerChannelStore, ChannelId, DepositOutcome,
};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::client_requester::ChannelContext;
use crate::payment_settlement::unix_now;

/// How often the reclaim sweep scans tracked buyer channels for expiry.
/// Channel lifetimes are long (default 90 days), so an hourly scan is ample —
/// matches the seller expiry sweep cadence.
const RECLAIM_SWEEP_INTERVAL: Duration = Duration::from_hours(1);

/// Re-approve the `PaymentChannel` spender when the standing USDC allowance has
/// fallen below this floor. Set to half of `U256::MAX` so a single max approval
/// covers effectively unlimited deposits, and a restart with the approval
/// already in place skips the redundant `approve` tx (it stays far above this
/// floor) while a never-approved node (allowance `0`) trips it.
fn approval_floor() -> U256 {
    U256::MAX >> 1
}

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
    _reclaimer: AbortOnDrop,
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

        let reclaimer = tokio::spawn(reclaim_loop(
            contract.clone(),
            Arc::clone(&store),
            self_address,
        ));

        Ok(Self {
            contract,
            store,
            signer,
            voucher_domain,
            token,
            self_address,
            min_deposit,
            default_deposit,
            opens_in_flight: Arc::new(Mutex::new(HashSet::new())),
            _reclaimer: AbortOnDrop(reclaimer),
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
            try_reclaim(&self.contract, &self.store, self.self_address, &existing).await;
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

        let receipt = self
            .contract
            .openChannel(provider_addr, deposit)
            .send()
            .await
            .context("submit openChannel")?
            .get_receipt()
            .await
            .context("await openChannel receipt")?;
        if !receipt.status() {
            anyhow::bail!(
                "openChannel reverted (provider {provider_addr}, deposit {deposit}); \
                 check USDC balance/allowance and that the provider is active"
            );
        }

        // From here the deposit is escrowed on-chain. Until `record` persists,
        // the channel is tracked ONLY on-chain — and the buyer path has no
        // chain-log recovery yet (no `getChannelsByClient` reconciliation at
        // bootstrap), so the reclaim sweep, which only iterates `load_all`, will
        // never see an unpersisted channel. Both failure paths below therefore
        // escalate to `error!` with the tx hash so an operator can reconcile /
        // reclaim the deposit manually. (A bootstrap reconciliation scan that
        // would automate this is tracked in #763, gated on the cache-miss hook.)
        let tx = receipt.transaction_hash;

        // Decode this tx's `ChannelOpened` event from the receipt for the
        // authoritative `channelId` + `expiresAt`. This is atomic with the
        // open: a successful tx guarantees the event is present, so we can
        // always persist the channel — unlike a follow-up `getChannel` call,
        // whose transient failure would leave the on-chain deposit orphaned
        // (opened but untracked, re-opened on the next miss). Filtering on
        // `client`/`provider` also confirms we decoded our own open.
        let Some(opened) = receipt
            .inner
            .logs()
            .iter()
            .filter_map(|log| log.log_decode::<PaymentChannel::ChannelOpened>().ok())
            .map(|decoded| decoded.inner.data)
            .find(|ev| ev.client == self.self_address && ev.provider == provider_addr)
        else {
            error!(
                %tx,
                provider = %provider_addr,
                %deposit,
                "openChannel tx mined but its ChannelOpened event was not found in the receipt \
                 logs (ABI/contract skew?); the deposit is escrowed on-chain but UNTRACKED locally \
                 and will not be auto-reclaimed — reconcile manually against the tx"
            );
            anyhow::bail!(
                "ChannelOpened event for provider {provider_addr} not found in openChannel \
                 receipt logs (tx {tx})"
            );
        };
        let channel_id = opened.channelId;
        // The `ChannelOpened` event's `expiresAt` is `uint256`; clamp to `u64`
        // (a too-far expiry only ever means the reclaim sweep waits longer).
        let expires_at = u64::try_from(opened.expiresAt).unwrap_or(u64::MAX);

        let state =
            BuyerChannelState::new(channel_id, provider_addr, self.token, deposit, expires_at);
        if let Err(err) = self.store.record(&state) {
            error!(
                %tx,
                provider = %provider_addr,
                %channel_id,
                %deposit,
                %err,
                "buyer channel opened on-chain (deposit escrowed) but persisting the local record \
                 failed; the deposit is UNTRACKED and will not be auto-reclaimed — reconcile \
                 manually against the tx"
            );
            return Err(err).context("persist newly-opened buyer channel");
        }
        info!(
            provider = %provider_addr,
            %channel_id,
            %deposit,
            expires_at,
            "opened buyer payment channel"
        );

        Ok(ChannelContext::for_buyer_channel(
            &state,
            Arc::clone(&self.signer),
            self.voucher_domain.clone(),
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
        reclaim_once(&self.contract, &self.store, self.self_address).await;
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

/// Read the current USDC allowance for the `PaymentChannel` spender and, if it
/// has fallen below [`approval_floor`], issue a one-time max approval.
async fn ensure_allowance<P: Provider + Clone>(
    provider: &P,
    token: Address,
    owner: Address,
    spender: Address,
) -> Result<()> {
    let erc20 = Erc20::new(token, provider.clone());
    let current = erc20
        .allowance(owner, spender)
        .call()
        .await
        .context("read USDC allowance")?;
    if current >= approval_floor() {
        debug!(%current, "USDC allowance already sufficient; skipping approve");
        return Ok(());
    }
    let receipt = erc20
        .approve(spender, U256::MAX)
        .send()
        .await
        .context("submit USDC approve")?
        .get_receipt()
        .await
        .context("await USDC approve receipt")?;
    if !receipt.status() {
        anyhow::bail!("USDC approve transaction reverted");
    }
    info!(
        %token,
        %spender,
        "issued one-time max USDC approval for PaymentChannel deposits"
    );
    Ok(())
}

/// Background reclaim sweep: periodically reclaim the deposit of any tracked
/// buyer channel that has passed its on-chain expiry without the upstream
/// closing it. Best-effort — errors are logged, never fatal.
async fn reclaim_loop<P: Provider + Clone>(
    contract: PaymentChannel::PaymentChannelInstance<P>,
    store: Arc<dyn BuyerChannelStore>,
    self_address: Address,
) {
    let mut ticker = tokio::time::interval(RECLAIM_SWEEP_INTERVAL);
    // Skip the immediate first tick — bootstrap just ran and nothing is near
    // expiry yet (and it avoids a redundant load_all at startup).
    ticker.tick().await;
    loop {
        ticker.tick().await;
        reclaim_once(&contract, &store, self_address).await;
    }
}

/// One reclaim-sweep pass. Errors are logged per channel and never abort the
/// sweep.
async fn reclaim_once<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn BuyerChannelStore>,
    self_address: Address,
) {
    let states = match store.load_all() {
        Ok(s) => s,
        Err(err) => {
            warn!(%err, "buyer reclaim sweep: failed to load channel state");
            return;
        }
    };
    let now = unix_now();
    for st in states {
        if !st.is_expired_at(now) {
            continue;
        }
        try_reclaim(contract, store, self_address, &st).await;
    }
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
) {
    let ch = match contract.getChannel(st.channel_id).call().await {
        Ok(ch) => ch,
        Err(err) => {
            warn!(%err, channel_id = %st.channel_id, "buyer reclaim: getChannel failed");
            return;
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
        forget_reclaimed(store, st, "drop foreign/unknown record");
        return;
    }
    // If the upstream already closed/settled the channel, `reclaimExpired`
    // would revert — just drop our local record.
    if !matches!(ch.status, PaymentChannel::Status::Open) {
        forget_reclaimed(store, st, "expired channel already closed on-chain");
        return;
    }

    let receipt = match contract.reclaimExpired(st.channel_id).send().await {
        Ok(pending) => match pending.get_receipt().await {
            Ok(r) => r,
            Err(err) => {
                warn!(%err, channel_id = %st.channel_id, "buyer reclaim: receipt failed");
                return;
            }
        },
        Err(err) => {
            warn!(%err, channel_id = %st.channel_id, "buyer reclaim: send failed");
            return;
        }
    };
    if !receipt.status() {
        warn!(
            channel_id = %st.channel_id,
            tx = %receipt.transaction_hash,
            "reclaimExpired reverted on-chain; leaving record for retry"
        );
        return;
    }
    forget_reclaimed(store, st, "reclaimed expired buyer channel deposit");
}

/// Compare-and-delete the buyer record for `st`'s channel after a reclaim (or a
/// drop-bogus decision), logging the outcome. Uses `forget_if_channel` so a
/// concurrent `open_or_reuse` that replaced this provider's channel between the
/// sweep's `load_all` and here is NOT clobbered (lost-update guard).
fn forget_reclaimed(store: &Arc<dyn BuyerChannelStore>, st: &BuyerChannelState, reason: &str) {
    match store.forget_if_channel(st.provider, st.channel_id) {
        Ok(true) => info!(
            channel_id = %st.channel_id,
            provider = %st.provider,
            reason,
            "dropped buyer channel record"
        ),
        Ok(false) => debug!(
            channel_id = %st.channel_id,
            provider = %st.provider,
            reason,
            "buyer record already replaced by a newer channel; left in place"
        ),
        Err(err) => warn!(
            %err,
            provider = %st.provider,
            reason,
            "buyer reclaim: forget_if_channel failed"
        ),
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
}
