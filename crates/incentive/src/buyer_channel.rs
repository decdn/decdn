//! Buyer-side payment-channel bookkeeping (#744).
//!
//! When a node pulls content from an upstream provider on a cache miss it acts
//! as the *client*: it opens a `PaymentChannel` against the provider, signs
//! cumulative vouchers as bytes arrive (the signing primitives live in
//! [`crate::voucher`]; the requester in `decdn-node` drives them), and lets the
//! upstream close & settle — or, if the upstream abandons the channel, reclaims
//! its own deposit after expiry (ADR 003 §node→node).
//!
//! This is the *buyer's* mirror of [`crate::lane::LaneState`] /
//! [`crate::store::PoolStateStore`], which track the *seller's* view. The
//! two are deliberately separate types:
//!
//! - The seller keys channels by `channelId` and records the latest voucher
//!   *signature* (to submit on-chain). The buyer keys channels by **provider
//!   address** — the reuse unit is "one open channel per upstream provider" —
//!   and does not retain signatures (the buyer is the signer; it never submits
//!   another party's voucher on-chain).
//! - The seller validates inbound vouchers (replay guard, #527). The buyer
//!   only records its own monotonically-advancing cumulative totals so a reused
//!   channel resumes from the right `nonce`/`bytes`/`amount` after a restart.
//!
//! Persistence matters for two reasons: a restart must not re-`openChannel`
//! (wasting a fresh deposit + gas) when a live channel already exists, and the
//! reclaim path must know which expired channels still hold a refundable
//! deposit.

use std::collections::HashMap;
use std::sync::Mutex;

use alloy::primitives::{Address, U256};

use crate::lane::ChannelId;
use crate::store::StoreError;

/// "Never expires" sentinel for [`BuyerChannelState::expires_at`]. Matches the
/// [`crate::lane::LaneState`] convention for records hydrated from a
/// pre-expiry schema (and the on-chain `uint64` width, so it round-trips
/// byte-identically through the store).
pub const NEVER_EXPIRES: u64 = 0;

/// Buyer-held per-channel state for one upstream provider.
///
/// The `last_*` fields are the cumulative totals of the most recent voucher
/// the node signed on this channel. They start at zero for a freshly-opened
/// channel and only ever advance (vouchers are cumulative across the channel
/// lifetime), so a reused channel's next voucher resumes at
/// `last_nonce + 1` / `last_bytes_delivered + delta` / `last_amount + delta`.
///
/// **Field invariant:** `channel_id` is the **identity key** (the store's
/// primary key; the on-chain `channelId` decoded from the `ChannelOpened`
/// event in the open tx receipt). `provider` is a **secondary reuse index** —
/// the open-channel trigger looks it up to decide whether to reuse an
/// existing channel instead of opening a new one — not the identity key: two
/// channels can (transiently) exist for the same provider, e.g. across a
/// rotate. The `last_*` fields MUST only advance — through
/// [`BuyerChannelState::advance`] (the validated mutator) or hydration from a
/// [`BuyerChannelStore`]. The fields stay `pub` because the cross-crate
/// hydration path (`decdn-node` decoding the redb record) needs struct-literal
/// construction, but no other writer should mutate `last_*` directly: doing so
/// bypasses the monotonicity guard and can resume a reused channel at a nonce
/// the upstream rejects. Mirrors the [`crate::lane::LaneState`] posture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuyerChannelState {
    /// On-chain `channelId` (`keccak256(client, provider, channelNonce)`) —
    /// learned by decoding the `ChannelOpened` event from the open tx receipt
    /// (atomic with the open; no follow-up `getChannel` read). The store's
    /// primary key.
    pub channel_id: ChannelId,
    /// Upstream provider's Ethereum address — the per-provider reuse key
    /// (a secondary index in [`BuyerChannelStore`]; see the field invariant
    /// above).
    pub provider: Address,
    /// The on-chain `client`: put up the deposit, receives the refund, and
    /// the only address `topUp` accepts. Equals the local key for a
    /// self-opened channel; the publisher for publisher-pays.
    pub funder: Address,
    /// Address whose EIP-712 signature this channel accepts — the pinned
    /// on-chain `voucherSigner`. Equals `funder`/the local key for a
    /// self-signed channel, a delegate for publisher-pays.
    pub voucher_signer: Address,
    /// `ERC-20` token bound by the channel (`USDC`).
    pub token: Address,
    /// On-chain deposited amount in token base units (initial + any top-ups).
    pub deposit: U256,
    /// Cumulative amount of the most-recent voucher signed on this channel.
    /// `U256::ZERO` until the first voucher is signed.
    pub last_amount: U256,
    /// Nonce of the most-recent voucher signed on this channel. `U256::ZERO`
    /// before any voucher — the next voucher uses `last_nonce + 1`.
    pub last_nonce: U256,
    /// Cumulative bytes paid for as of the most-recent voucher.
    pub last_bytes_delivered: U256,
    /// On-chain channel expiry (Unix seconds), from the `ChannelOpened` event.
    /// After `expires_at` the provider can no longer `withdraw`/`closeChannel`,
    /// so the buyer may `reclaimExpired` to recover the remaining deposit.
    pub expires_at: u64,
}

/// Result of hydrating the buyer-channel store.
///
/// A disk-backed store keeps hydration available when one row cannot be
/// decoded: healthy channels remain usable and reclaimable, while the row's
/// primary key (`channel_id`) is retained as the only available repair
/// handle — the primary table is keyed by `channel_id`, so that is the one
/// piece of identity an undecodable row still exposes; the provider lives
/// inside the bytes that failed to decode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BuyerLoad {
    /// Successfully decoded buyer channels.
    pub channels: Vec<BuyerChannelState>,
    /// Channel ids whose persisted rows could not be decoded. Their deposits
    /// remain escrowed but untracked until the rows are repaired.
    pub skipped: Vec<ChannelId>,
}

impl BuyerChannelState {
    /// Construct fresh state for a newly-opened buyer channel. The `last_*`
    /// fields start at zero, matching the on-chain `Channel` defaults.
    #[must_use]
    pub const fn new(
        channel_id: ChannelId,
        provider: Address,
        funder: Address,
        voucher_signer: Address,
        token: Address,
        deposit: U256,
        expires_at: u64,
    ) -> Self {
        Self {
            channel_id,
            provider,
            funder,
            voucher_signer,
            token,
            deposit,
            last_amount: U256::ZERO,
            last_nonce: U256::ZERO,
            last_bytes_delivered: U256::ZERO,
            expires_at,
        }
    }

    /// Whether the channel is expired as of `now` (Unix seconds). An
    /// `expires_at` of [`NEVER_EXPIRES`] (`0`) is treated as "unknown / never
    /// expires".
    ///
    /// This is only a **candidate filter** for the reclaim sweep — the on-chain
    /// `block.timestamp >= expiresAt` check in `reclaimExpired` is the real
    /// gate. So host-clock skew vs. chain time only changes *when* the sweep
    /// attempts a reclaim (a too-early attempt simply reverts and is retried),
    /// never *whether* the refund is allowed.
    #[must_use]
    pub const fn is_expired_at(&self, now: u64) -> bool {
        self.expires_at != NEVER_EXPIRES && now >= self.expires_at
    }

    /// Advance the cumulative voucher totals to the reported values, enforcing
    /// monotonicity (vouchers are cumulative over a channel's lifetime, so
    /// totals may stay equal — an idempotent re-record — or rise, never fall).
    /// This is the sanctioned `last_*` mutator; prefer it over direct field
    /// assignment (see the type's field invariant).
    ///
    /// # Errors
    ///
    /// Returns [`BuyerProgressError`] if any of `nonce` / `bytes_delivered` /
    /// `amount` is below the currently-recorded value.
    pub fn advance(
        &mut self,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<(), BuyerProgressError> {
        if nonce < self.last_nonce {
            return Err(BuyerProgressError::Regressed {
                field: "nonce",
                recorded: self.last_nonce,
                got: nonce,
            });
        }
        if bytes_delivered < self.last_bytes_delivered {
            return Err(BuyerProgressError::Regressed {
                field: "bytes_delivered",
                recorded: self.last_bytes_delivered,
                got: bytes_delivered,
            });
        }
        if amount < self.last_amount {
            return Err(BuyerProgressError::Regressed {
                field: "amount",
                recorded: self.last_amount,
                got: amount,
            });
        }
        self.last_nonce = nonce;
        self.last_bytes_delivered = bytes_delivered;
        self.last_amount = amount;
        Ok(())
    }
}

/// Failure mode for [`BuyerChannelState::advance`]: a reported cumulative total
/// regressed below the recorded one (a caller bug — vouchers never decrease).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BuyerProgressError {
    /// `field`'s reported value `got` is below the recorded value.
    #[error("buyer channel {field} regressed: recorded {recorded}, got {got}")]
    Regressed {
        /// Which cumulative field regressed (`nonce` / `bytes_delivered` / `amount`).
        field: &'static str,
        /// The currently-recorded (higher) value.
        recorded: U256,
        /// The reported (lower) value that was rejected.
        got: U256,
    },
}

/// Outcome of an atomic [`BuyerChannelStore::advance_progress`].
///
/// The read-check-advance-write happens inside one serialized write
/// transaction, so these variants describe the committed-row decision rather
/// than a backend fault (those surface as [`StoreError`], as with
/// [`BuyerChannelStore::forget_if_channel`]).
///
/// `#[must_use]`: the variant is the only signal that nothing was persisted
/// (`UnknownChannel` / `ChannelMismatch`) or that the totals regressed — a
/// dropped outcome silently looks like success.
#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub enum AdvanceOutcome {
    /// The committed row was advanced and re-persisted durably.
    Advanced,
    /// No row is reachable for the provider/channel (the provider's secondary
    /// index has no entry, or the channel it names is absent — never
    /// recorded, or the table does not exist yet).
    UnknownChannel,
    /// The provider's secondary index names a *different* channel than the
    /// one the caller expected — the provider's slot was replaced by a newer
    /// open. The caller should treat this as stale and must NOT escalate
    /// (writing would clobber the live replacement).
    ChannelMismatch,
    /// The reported totals would regress the committed watermark — a real
    /// caller bug (vouchers never decrease). Carries the rejecting error.
    Regressed(BuyerProgressError),
}

/// Outcome of an atomic [`BuyerChannelStore::add_deposit`].
///
/// `#[must_use]` for the same reason as [`AdvanceOutcome`]: a dropped
/// `ChannelMismatch` / `UnknownChannel` silently looks like a successful
/// credit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum DepositOutcome {
    /// `additional` was added to the committed deposit; carries the new total.
    Added(U256),
    /// No row is reachable for the provider/channel (see
    /// [`AdvanceOutcome::UnknownChannel`]).
    UnknownChannel,
    /// The committed row is for a different channel (stale; NOT escalated).
    ChannelMismatch,
}

/// Durable backing store for [`BuyerChannelState`], keyed by `channel_id`
/// (its on-chain identity) with `provider` as a secondary reuse index.
///
/// Mirrors [`crate::store::PoolStateStore`] but for the buyer's view. The
/// reuse unit is one open channel per provider, so `get_by_provider` is the
/// hot path the channel-open trigger consults before deciding to reuse vs.
/// open — it resolves through the provider index to the primary
/// `channel_id`-keyed row. Implementations MUST persist `record`/`forget`
/// durably (fsync, for disk-backed impls) before returning `Ok`.
pub trait BuyerChannelStore: Send + Sync {
    /// Load every persisted buyer channel. Called once at bring-up to hydrate
    /// the in-memory channel map and seed the reclaim sweep.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store itself is unreadable.
    /// Individual undecodable rows are reported in [`BuyerLoad::skipped`] so
    /// they do not prevent healthy channels from loading.
    fn load_all(&self) -> Result<BuyerLoad, StoreError>;

    /// Persist (insert or overwrite) the state for one channel, keyed by
    /// `state.channel_id` (primary) with `state.provider` maintained as a
    /// secondary reuse index. MUST be durable before returning `Ok`.
    ///
    /// Callers SHOULD pass a `state` whose `last_*` tuple is a non-strict
    /// monotonic successor of any previously-recorded state for
    /// `state.channel_id` (advance via [`BuyerChannelState::advance`]). The
    /// trait does not re-validate this — it is a dumb writer; the
    /// monotonicity invariant is owned upstream.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the write or fsync fails.
    fn record(&self, state: &BuyerChannelState) -> Result<(), StoreError>;

    /// Drop the persisted entry the provider index currently maps `provider`
    /// to (after the channel is reclaimed or settled), removing it from both
    /// the primary table and the provider index. A no-op if no record exists.
    /// MUST commit durably.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the delete or durable commit fails.
    fn forget(&self, provider: Address) -> Result<(), StoreError>;

    /// Point-lookup the live channel by its primary key, or `None` if none is
    /// tracked. Unlike [`Self::get_by_provider`] this needs no secondary
    /// index hop.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable or the
    /// record is corrupt.
    fn get_by_channel_id(
        &self,
        channel_id: ChannelId,
    ) -> Result<Option<BuyerChannelState>, StoreError>;

    /// Compare-and-delete: drop `provider`'s entry **only if** the stored
    /// record's `channel_id` still equals `channel_id`. Returns `true` if a
    /// row was deleted, `false` if the stored row was for a different channel
    /// (already replaced by a newer open) or no row exists.
    ///
    /// This guards the reclaim sweep against a lost update: between the sweep
    /// loading an expired channel and forgetting it, a concurrent
    /// `open_or_reuse` may have opened a replacement under the same provider
    /// key. An unconditional [`Self::forget`] would delete the live
    /// replacement; this CAS deletes only the channel the sweep actually
    /// reclaimed. MUST commit durably.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the read, delete, or durable commit fails.
    fn forget_if_channel(
        &self,
        provider: Address,
        channel_id: ChannelId,
    ) -> Result<bool, StoreError>;

    /// Point-lookup the live channel for `provider`, or `None` if none is
    /// tracked. The open-channel trigger uses this to reuse an existing
    /// channel instead of opening a new one.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable or the
    /// record is corrupt.
    fn get_by_provider(&self, provider: Address) -> Result<Option<BuyerChannelState>, StoreError>;

    /// Atomically advance the committed progress for `provider`'s channel.
    ///
    /// Reads the row, verifies its `channel_id` still equals `channel_id` (the
    /// channel the caller actually paid on), runs [`BuyerChannelState::advance`]
    /// against the **committed** `last_*` watermark, and writes the advanced row
    /// back — all inside one serialized write transaction. This closes the
    /// lost-update / watermark-regression race that a separate
    /// `get_by_provider` → mutate → [`Self::record`] sequence exposes when a
    /// concurrent writer (e.g. [`Self::add_deposit`]) touches the same row in
    /// the gap. MUST commit durably.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] only on a backend/codec fault. The committed-row
    /// decision (advanced / unknown / replaced / regressed) is the `Ok` value.
    fn advance_progress(
        &self,
        provider: Address,
        channel_id: ChannelId,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError>;

    /// Atomically add `additional` to the committed deposit for `provider`'s
    /// channel.
    ///
    /// Reads the **committed** deposit inside the write transaction (never a
    /// stale snapshot), verifies the row's `channel_id` still equals
    /// `channel_id`, `saturating_add`s `additional`, and writes back. Used after
    /// the on-chain `topUp` receipt lands so the persisted deposit is derived
    /// from the committed row even if a concurrent writer advanced it during the
    /// RPC. MUST commit durably.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] only on a backend/codec fault; the committed-row
    /// decision is the `Ok` value.
    fn add_deposit(
        &self,
        provider: Address,
        channel_id: ChannelId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError>;
}

/// In-memory backing for [`MemoryBuyerChannelStore`]: the primary
/// `channel_id`-keyed map plus the `provider → channel_id` secondary reuse
/// index, held behind one mutex so the pair updates atomically (mirrors the
/// redb table's single-write-transaction discipline).
#[derive(Debug, Default)]
struct MemoryInner {
    channels: HashMap<ChannelId, BuyerChannelState>,
    provider_index: HashMap<Address, ChannelId>,
}

/// In-memory [`BuyerChannelStore`] for tests and the trait's reference
/// semantics. Not durable — drops with the process. The runtime uses the
/// redb-backed impl in `crates/node`.
#[derive(Debug, Default)]
pub struct MemoryBuyerChannelStore {
    inner: Mutex<MemoryInner>,
}

impl MemoryBuyerChannelStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the current entry count (test helper).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().map_or(0, |m| m.channels.len())
    }

    /// `true` when no channels are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl BuyerChannelStore for MemoryBuyerChannelStore {
    fn load_all(&self) -> Result<BuyerLoad, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(BuyerLoad {
            channels: guard.channels.values().cloned().collect(),
            skipped: Vec::new(),
        })
    }

    fn record(&self, state: &BuyerChannelState) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        guard
            .provider_index
            .insert(state.provider, state.channel_id);
        guard.channels.insert(state.channel_id, state.clone());
        Ok(())
    }

    fn forget(&self, provider: Address) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        if let Some(channel_id) = guard.provider_index.remove(&provider) {
            guard.channels.remove(&channel_id);
        }
        Ok(())
    }

    fn forget_if_channel(
        &self,
        provider: Address,
        channel_id: ChannelId,
    ) -> Result<bool, StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        if guard.provider_index.get(&provider) == Some(&channel_id) {
            guard.provider_index.remove(&provider);
            guard.channels.remove(&channel_id);
            return Ok(true);
        }
        Ok(false)
    }

    fn get_by_channel_id(
        &self,
        channel_id: ChannelId,
    ) -> Result<Option<BuyerChannelState>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(guard.channels.get(&channel_id).cloned())
    }

    fn get_by_provider(&self, provider: Address) -> Result<Option<BuyerChannelState>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(guard
            .provider_index
            .get(&provider)
            .and_then(|channel_id| guard.channels.get(channel_id))
            .cloned())
    }

    fn advance_progress(
        &self,
        provider: Address,
        channel_id: ChannelId,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        let Some(mapped) = guard.provider_index.get(&provider).copied() else {
            return Ok(AdvanceOutcome::UnknownChannel);
        };
        if mapped != channel_id {
            return Ok(AdvanceOutcome::ChannelMismatch);
        }
        let Some(state) = guard.channels.get_mut(&channel_id) else {
            return Ok(AdvanceOutcome::UnknownChannel);
        };
        match state.advance(nonce, bytes_delivered, amount) {
            Ok(()) => Ok(AdvanceOutcome::Advanced),
            Err(err) => Ok(AdvanceOutcome::Regressed(err)),
        }
    }

    fn add_deposit(
        &self,
        provider: Address,
        channel_id: ChannelId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        let Some(mapped) = guard.provider_index.get(&provider).copied() else {
            return Ok(DepositOutcome::UnknownChannel);
        };
        if mapped != channel_id {
            return Ok(DepositOutcome::ChannelMismatch);
        }
        let Some(state) = guard.channels.get_mut(&channel_id) else {
            return Ok(DepositOutcome::UnknownChannel);
        };
        state.deposit = state.deposit.saturating_add(additional);
        Ok(DepositOutcome::Added(state.deposit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{B256, address, b256};

    /// Every `provider_byte` gets a **distinct** `channel_id` too (derived from
    /// the same byte): `channel_id` is now the store's primary key, so two
    /// samples sharing one `channel_id` would collide in the primary table
    /// instead of coexisting as two independent channels.
    fn sample(provider_byte: u8) -> BuyerChannelState {
        let mut pbytes = [0u8; 20];
        pbytes[19] = provider_byte;
        let mut idbytes = [0u8; 32];
        idbytes[31] = provider_byte;
        BuyerChannelState {
            channel_id: B256::from(idbytes),
            provider: Address::from(pbytes),
            funder: Address::from(pbytes),
            voucher_signer: Address::from(pbytes),
            token: address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            deposit: U256::from(10_000_000u64),
            last_amount: U256::from(1_234u64),
            last_nonce: U256::from(7u64),
            last_bytes_delivered: U256::from(4_096u64),
            expires_at: 1_900_000_000,
        }
    }

    #[test]
    fn is_expired_honours_zero_sentinel() {
        let mut s = sample(1);
        s.expires_at = 0;
        assert!(!s.is_expired_at(u64::MAX), "0 means never-expires");
        s.expires_at = 100;
        assert!(!s.is_expired_at(99));
        assert!(s.is_expired_at(100));
        assert!(s.is_expired_at(101));
    }

    #[test]
    fn memory_store_round_trip() -> anyhow::Result<()> {
        let store = MemoryBuyerChannelStore::new();
        let a = sample(1);
        let b = sample(2);
        store.record(&a)?;
        store.record(&b)?;
        anyhow::ensure!(store.len() == 2);
        let got = store
            .get_by_provider(a.provider)?
            .ok_or_else(|| anyhow::anyhow!("missing a"))?;
        anyhow::ensure!(got == a);
        Ok(())
    }

    #[test]
    fn memory_store_record_overwrites_by_provider() -> anyhow::Result<()> {
        let store = MemoryBuyerChannelStore::new();
        let mut s = sample(1);
        store.record(&s)?;
        s.last_nonce = U256::from(99u64);
        store.record(&s)?;
        anyhow::ensure!(store.len() == 1, "same provider overwrites");
        let only = store
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("missing entry"))?;
        anyhow::ensure!(only.last_nonce == U256::from(99u64));
        Ok(())
    }

    #[test]
    fn memory_store_forget_removes_entry() -> anyhow::Result<()> {
        let store = MemoryBuyerChannelStore::new();
        let s = sample(1);
        store.record(&s)?;
        store.forget(s.provider)?;
        anyhow::ensure!(store.is_empty());
        // Forgetting an unknown provider is a no-op.
        store.forget(address!("00000000000000000000000000000000000000ff"))?;
        Ok(())
    }

    #[test]
    fn advance_accepts_monotonic_and_equal() -> anyhow::Result<()> {
        let mut s = sample(1);
        s.last_nonce = U256::ZERO;
        s.last_bytes_delivered = U256::ZERO;
        s.last_amount = U256::ZERO;
        // First advance from zero.
        s.advance(U256::from(1u64), U256::from(1_000u64), U256::from(10u64))?;
        anyhow::ensure!(s.last_nonce == U256::from(1u64));
        // Equal totals are allowed (idempotent re-record).
        s.advance(U256::from(1u64), U256::from(1_000u64), U256::from(10u64))?;
        anyhow::ensure!(s.last_amount == U256::from(10u64));
        // Strictly higher advances.
        s.advance(U256::from(2u64), U256::from(2_000u64), U256::from(20u64))?;
        anyhow::ensure!(s.last_bytes_delivered == U256::from(2_000u64));
        Ok(())
    }

    #[test]
    fn advance_rejects_regression_and_leaves_state_unchanged() -> anyhow::Result<()> {
        let mut base = sample(1);
        base.last_nonce = U256::from(5u64);
        base.last_bytes_delivered = U256::from(5_000u64);
        base.last_amount = U256::from(50u64);

        // (reported tuple, expected regressed field).
        let cases = [
            (
                (U256::from(4u64), U256::from(6_000u64), U256::from(60u64)),
                "nonce",
            ),
            (
                (U256::from(6u64), U256::from(4_000u64), U256::from(60u64)),
                "bytes_delivered",
            ),
            (
                (U256::from(6u64), U256::from(6_000u64), U256::from(40u64)),
                "amount",
            ),
        ];
        for ((nonce, bytes, amount), expected_field) in cases {
            let mut s = base.clone();
            let err = s
                .advance(nonce, bytes, amount)
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected regression error for {expected_field}"))?;
            anyhow::ensure!(
                matches!(err, BuyerProgressError::Regressed { field, .. } if field == expected_field),
                "wrong error variant/field: {err:?}"
            );
            anyhow::ensure!(s == base, "rejected advance must not mutate state");
        }
        Ok(())
    }

    #[test]
    fn forget_if_channel_only_deletes_matching_channel() -> anyhow::Result<()> {
        let store = MemoryBuyerChannelStore::new();
        let s = sample(1);
        store.record(&s)?;

        // Wrong channel id → no delete, row preserved.
        let deleted = store.forget_if_channel(
            s.provider,
            b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        )?;
        anyhow::ensure!(!deleted, "mismatched channel must not delete");
        anyhow::ensure!(store.len() == 1, "row must survive a mismatched CAS");

        // Matching channel id → deletes.
        let deleted = store.forget_if_channel(s.provider, s.channel_id)?;
        anyhow::ensure!(deleted, "matching channel must delete");
        anyhow::ensure!(store.is_empty());

        // Unknown provider → false, no-op.
        anyhow::ensure!(!store.forget_if_channel(
            address!("00000000000000000000000000000000000000ff"),
            s.channel_id
        )?);
        Ok(())
    }

    #[test]
    fn advance_progress_advances_committed_watermark() -> anyhow::Result<()> {
        let store = MemoryBuyerChannelStore::new();
        let mut s = sample(1);
        s.last_nonce = U256::ZERO;
        s.last_bytes_delivered = U256::ZERO;
        s.last_amount = U256::ZERO;
        store.record(&s)?;

        let outcome = store.advance_progress(
            s.provider,
            s.channel_id,
            U256::from(3u64),
            U256::from(3_000u64),
            U256::from(30u64),
        )?;
        anyhow::ensure!(outcome == AdvanceOutcome::Advanced, "got {outcome:?}");
        let stored = store
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("missing row"))?;
        anyhow::ensure!(stored.last_nonce == U256::from(3u64));
        anyhow::ensure!(stored.last_bytes_delivered == U256::from(3_000u64));
        anyhow::ensure!(stored.last_amount == U256::from(30u64));
        Ok(())
    }

    #[test]
    fn advance_progress_rejects_regression_without_writing() -> anyhow::Result<()> {
        let store = MemoryBuyerChannelStore::new();
        let mut s = sample(1);
        s.last_nonce = U256::from(9u64);
        s.last_bytes_delivered = U256::from(9_000u64);
        s.last_amount = U256::from(90u64);
        store.record(&s)?;

        // Each cumulative field's regression must surface through the wrapper as
        // `AdvanceOutcome::Regressed` with the right field — and must NOT touch
        // the committed watermark. The reported nonce is held at-or-above the
        // committed value so the *bytes*/*amount* branches of `advance` are the
        // ones that trip (nonce is checked first).
        let cases = [
            (
                (U256::from(5u64), U256::from(5_000u64), U256::from(50u64)),
                "nonce",
            ),
            (
                (U256::from(9u64), U256::from(8_999u64), U256::from(90u64)),
                "bytes_delivered",
            ),
            (
                (U256::from(9u64), U256::from(9_000u64), U256::from(89u64)),
                "amount",
            ),
        ];
        for ((nonce, bytes, amount), expected_field) in cases {
            let outcome = store.advance_progress(s.provider, s.channel_id, nonce, bytes, amount)?;
            anyhow::ensure!(
                matches!(outcome, AdvanceOutcome::Regressed(BuyerProgressError::Regressed { field, .. }) if field == expected_field),
                "expected {expected_field} regression, got {outcome:?}"
            );
            let stored = store
                .get_by_provider(s.provider)?
                .ok_or_else(|| anyhow::anyhow!("missing row"))?;
            anyhow::ensure!(
                stored.last_nonce == U256::from(9u64)
                    && stored.last_bytes_delivered == U256::from(9_000u64)
                    && stored.last_amount == U256::from(90u64),
                "watermark regressed after rejecting {expected_field}"
            );
        }
        Ok(())
    }

    #[test]
    fn advance_and_deposit_guard_on_channel_and_provider() -> anyhow::Result<()> {
        let store = MemoryBuyerChannelStore::new();
        let s = sample(1);
        store.record(&s)?;
        let other_channel =
            b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        let unknown = address!("00000000000000000000000000000000000000ff");

        // Channel-id mismatch → stale, no write.
        anyhow::ensure!(
            store.advance_progress(
                s.provider,
                other_channel,
                U256::from(99u64),
                U256::from(99u64),
                U256::from(99u64)
            )? == AdvanceOutcome::ChannelMismatch
        );
        anyhow::ensure!(
            store.add_deposit(s.provider, other_channel, U256::from(1u64))?
                == DepositOutcome::ChannelMismatch
        );
        let stored = store
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("missing row"))?;
        anyhow::ensure!(stored == s, "mismatched calls must not mutate the row");

        // Unknown provider → UnknownChannel, no write.
        anyhow::ensure!(
            store.advance_progress(
                unknown,
                s.channel_id,
                U256::from(1u64),
                U256::from(1u64),
                U256::from(1u64)
            )? == AdvanceOutcome::UnknownChannel
        );
        anyhow::ensure!(
            store.add_deposit(unknown, s.channel_id, U256::from(1u64))?
                == DepositOutcome::UnknownChannel
        );
        Ok(())
    }

    #[test]
    fn add_deposit_accumulates_committed_deposit() -> anyhow::Result<()> {
        let store = MemoryBuyerChannelStore::new();
        let mut s = sample(1);
        s.deposit = U256::from(100u64);
        store.record(&s)?;

        let outcome = store.add_deposit(s.provider, s.channel_id, U256::from(40u64))?;
        anyhow::ensure!(
            outcome == DepositOutcome::Added(U256::from(140u64)),
            "got {outcome:?}"
        );
        let stored = store
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("missing row"))?;
        anyhow::ensure!(stored.deposit == U256::from(140u64));
        Ok(())
    }
}
