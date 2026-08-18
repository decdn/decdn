//! Per-lane voucher state tracking.
//!
//! A **lane** is a `(pool_id, signer, provider)` triple: one capability signer
//! spending one pool against one provider node. As a node receives vouchers on
//! a lane, it keeps the latest one and rejects any that would lower the
//! cumulative `amount` or `bytes_delivered` — the on-chain `PaymentPool.redeem`
//! invariants from ADR 003 §Redemption and Close apply equally off-chain (a node
//! that retains a stale voucher just under-claims at redemption).
//!
//! In-memory state alone is insufficient: without persistence a node restart
//! resets `last_amount` to zero and a client can resubmit a previously-accepted
//! voucher (issue #527). [`LaneState::apply_voucher`] therefore requires a
//! [`PoolStateStore`] and writes the post-acceptance state durably before
//! advancing in-memory fields or returning `Ok` — see
//! [ADR 003 §Off-chain voucher state persistence](../../../adr/003-payments.md)
//! and [`crate::store`].

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};

use crate::store::{PoolStateStore, StoreError};
use crate::voucher::{SignedVoucher, VoucherError};

/// An opaque 32-byte pool identifier — the on-chain `PaymentPool` deposit id
/// (`keccak256(owner, ownerPoolNonce)`). The buyer modules key their
/// per-owner pool state by this type; the seller-side lane keys off
/// [`LaneKey`] instead.
pub type PoolId = B256;

/// Identifies one voucher lane: a `(pool_id, signer, provider)` triple. This is
/// the seller-side persistence key — one accepted-voucher watermark per lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LaneKey {
    /// The pool the vouchers draw from (on-chain `PaymentPool` deposit id).
    pub pool_id: B256,
    /// The capability signer authorizing vouchers on this lane.
    pub signer: Address,
    /// The provider node the vouchers pay.
    pub provider: Address,
}

/// Mutable per-lane state held by the delivering node.
///
/// Tracks the latest accepted voucher (`last_*` fields) so subsequent
/// [`LaneState::apply_voucher`] calls can enforce monotonicity. `cap` is the
/// capability's spending cap — vouchers exceeding it are invalid because
/// `PaymentPool.redeem` would itself revert (ADR 003 §Capability delegation).
///
/// **Field invariant (#527):** the `last_*` fields MUST only be advanced
/// through [`LaneState::apply_voucher`] (the validated, persisted-commit path)
/// or hydrated from a [`PoolStateStore`] (the trusted on-disk path). Direct
/// field assignment from outside this crate would bypass the voucher-replay
/// guard from ADR 003 §Off-chain voucher state persistence, so the three
/// replay-critical fields are **private** and reachable only through the getters
/// ([`Self::last_amount`] et al.) and the two trusted writers; the cross-crate
/// hydration path (`decdn-node` reading the pool store) goes through
/// [`Self::hydrate`] rather than a struct literal (#751).
///
/// The remaining fields stay `pub` deliberately: `pool_id`/`signer`/`provider`
/// are immutable identity set once; `cap`, `expiry`, and `registered_until`
/// are set from the registered capability by trusted node-side writers. They
/// are not replay-critical (they don't gate the amount/bytes monotonicity the
/// #527 guard protects), so they don't need the private treatment the
/// `last_*` fields do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneState {
    /// The pool these vouchers draw from (on-chain `PaymentPool` deposit id).
    pub pool_id: B256,
    /// The capability signer authorizing vouchers on this lane. Vouchers whose
    /// signature recovers to a different address are rejected.
    pub signer: Address,
    /// The provider node these vouchers pay. A voucher naming a different
    /// provider is rejected — a capability voucher is scoped to one node.
    pub provider: Address,
    /// The capability's cumulative spending cap (token base units). Vouchers
    /// MUST NOT exceed this value.
    pub cap: U256,
    /// Capability expiry (Unix seconds). The node handler holds the clock and
    /// refuses vouchers at or past this; `0` means "unknown / not tracked".
    pub expiry: u64,
    /// The observed on-chain capability expiry for this lane's signer
    /// (`authorized[pool_id][signer].expiry`), in Unix seconds; `0` means
    /// "unknown / not yet registered". The seller redeemer reads it to skip a
    /// per-lane `getAuthorization` when the registration is already known and
    /// still live. Not replay-critical (it gates no amount/bytes monotonicity),
    /// so it is `pub` like `cap`/`expiry` rather than a private `last_*` field.
    pub registered_until: u64,
    /// Cumulative amount of the most-recently-accepted voucher (token base
    /// units). `U256::ZERO` until the first voucher is applied. Private
    /// (#527/#751) — read via [`Self::last_amount`].
    last_amount: U256,
    /// Cumulative bytes delivered as of the most-recently-accepted voucher.
    /// Private (#527/#751) — read via [`Self::last_bytes_delivered`].
    last_bytes_delivered: U256,
    /// Signature (`r‖s‖v`, exactly 65 bytes) on the most-recently-accepted
    /// voucher — the `signature` the seller path submits to the on-chain
    /// `PaymentPool.redeem`. `None` until the first voucher is applied; `Some`
    /// is always exactly 65 bytes. Same `r‖s‖v` encoding as the
    /// [`crate::client_bridge`] wire form. Private — read via
    /// [`Self::last_signature`].
    last_signature: Option<[u8; 65]>,
}

impl LaneState {
    /// Reconstruct lane state from a trusted persistent store (the only
    /// cross-crate path allowed to set the private replay-critical `last_*`
    /// fields, #527/#751). `decdn-node`'s pool-store decoder calls this instead
    /// of a struct literal so the [field invariant](Self) stays
    /// compiler-enforced. `last_signature` is `None` for a lane with no accepted
    /// voucher yet and otherwise the exact 65-byte `r‖s‖v` signature.
    ///
    /// **Trust boundary.** This is the one constructor that bypasses
    /// `apply_voucher`'s validation, and several arguments share a type
    /// (`cap`/`last_amount`/`last_bytes_delivered` are all `U256`;
    /// `signer`/`provider` are both `Address`), so a transposition compiles. Do
    /// not add callers without round-trip coverage that pins a signer distinct
    /// from the provider.
    ///
    /// Hydration seeds `registered_until` to `0` (unknown) regardless of
    /// caller-supplied `expiry`; the trusted on-disk decoder
    /// (`StoredLaneState::into_state`) assigns the persisted value on the
    /// returned `Self` after construction.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn hydrate(
        pool_id: B256,
        signer: Address,
        provider: Address,
        cap: U256,
        expiry: u64,
        last_amount: U256,
        last_bytes_delivered: U256,
        last_signature: Option<[u8; 65]>,
    ) -> Self {
        Self {
            pool_id,
            signer,
            provider,
            cap,
            expiry,
            registered_until: 0,
            last_amount,
            last_bytes_delivered,
            last_signature,
        }
    }

    /// This lane's persistence key.
    #[must_use]
    pub const fn key(&self) -> LaneKey {
        LaneKey {
            pool_id: self.pool_id,
            signer: self.signer,
            provider: self.provider,
        }
    }

    /// Cumulative amount of the most-recently-accepted voucher (`U256::ZERO`
    /// until the first voucher). See the [field invariant](Self).
    #[must_use]
    pub const fn last_amount(&self) -> U256 {
        self.last_amount
    }

    /// Cumulative bytes delivered as of the most-recently-accepted voucher.
    /// See the [field invariant](Self).
    #[must_use]
    pub const fn last_bytes_delivered(&self) -> U256 {
        self.last_bytes_delivered
    }

    /// The most-recently-accepted voucher's 65-byte `r‖s‖v` signature, or
    /// `None` if no voucher has been applied. See the [field invariant](Self).
    #[must_use]
    pub const fn last_signature(&self) -> Option<&[u8; 65]> {
        self.last_signature.as_ref()
    }

    /// Validate `signed` against this lane's invariants and, on success, durably
    /// persist the post-acceptance state via `store` before advancing the
    /// in-memory `last_*` fields.
    ///
    /// Mirrors the on-chain `PaymentPool.redeem` checks:
    /// - `voucher.pool_id == self.pool_id`
    /// - `voucher.provider == self.provider`
    /// - signature recovers to `self.signer`
    /// - `voucher.amount > self.last_amount`
    /// - `voucher.bytes_delivered >= self.last_bytes_delivered`
    /// - `voucher.amount <= self.cap`
    ///
    /// Expiry (`now < self.expiry`) is checked by the node handler, which holds
    /// the clock; this method is time-agnostic.
    ///
    /// Ordering of side effects: every check above runs first; if all pass, the
    /// candidate state is sent to `store.record` and only on `Ok` is in-memory
    /// state advanced. On any failure — including store failure — `self` is left
    /// unchanged. This makes `Ok(_)` the protocol-level commit point: further
    /// bytes are delivered **only after** this method returns `Ok` (ADR 003
    /// §Off-chain voucher state persistence, issue #527).
    ///
    /// # Errors
    ///
    /// See [`PoolError`] for the full taxonomy. A persistent-store failure
    /// surfaces as [`PoolError::Store`].
    pub fn apply_voucher(
        &mut self,
        signed: &SignedVoucher,
        domain: &Eip712Domain,
        store: &dyn PoolStateStore,
    ) -> Result<VoucherApplied, PoolError> {
        // INVARIANT (#527): validate + advance on a CLONE, record the clone,
        // swap on `Ok` only. `stage_voucher` produces the advanced successor
        // without persisting; the record-then-swap here is the durable-commit
        // point. Do NOT swap before the `?` on `record`: that's the literal
        // #527 replay window in code form.
        let (next, applied) = self.stage_voucher(signed, domain)?;
        store.record(&next)?;
        *self = next;
        Ok(applied)
    }

    /// Validate `signed` against this state's invariants and return the advanced
    /// successor state plus its [`VoucherApplied`] — the pure, in-memory half of
    /// [`Self::apply_voucher`], persisting nothing.
    ///
    /// The per-voucher serve loop calls this to verify and advance one voucher
    /// with no side effect; the caller then records the successor to the buffered
    /// lane store and swaps it into the live lane. Because vouchers are cumulative
    /// — each carries the running `amount` / `bytes_delivered` — a later voucher
    /// supersedes every earlier one.
    ///
    /// **Durability.** The caller records the returned state to the lane store,
    /// which buffers it in memory and makes it durable on a background flush (ADR
    /// 003 §Off-chain voucher state persistence). A crash loses at most the
    /// frontier advanced since the last flush, which is safe — an honest client
    /// resumes forward and an un-redeemed replay stays on-chain-payable — so the
    /// serve loop advances and delivers without waiting for the fsync; the
    /// redeemed watermark is floored separately by a flush before every on-chain
    /// redeem. `stage_voucher` takes `&self` (never mutates the caller's state) so
    /// an un-recorded candidate is discarded on a rejection without touching the
    /// live lane.
    ///
    /// # Errors
    ///
    /// The same validation taxonomy as [`Self::apply_voucher`] **minus**
    /// [`PoolError::Store`] — no store is touched here.
    pub fn stage_voucher(
        &self,
        signed: &SignedVoucher,
        domain: &Eip712Domain,
    ) -> Result<(Self, VoucherApplied), PoolError> {
        if signed.voucher.pool_id != self.pool_id {
            return Err(PoolError::WrongPool {
                expected: self.pool_id,
                got: signed.voucher.pool_id,
            });
        }
        if signed.voucher.provider != self.provider {
            return Err(PoolError::WrongProvider {
                expected: self.provider,
                got: signed.voucher.provider,
            });
        }
        // Signature check before the amount/bytes monotonicity guards: an
        // unauthenticated voucher is rejected as `Signature(WrongSigner)`
        // regardless of the amounts it claims. `verify_signer` recovers a
        // 65-byte EOA signature via `ecrecover` and rejects non-canonical
        // high-`s` up front, matching the on-chain verifiable-set (#836).
        signed
            .verify_signer(self.signer, domain)
            .map_err(PoolError::Signature)?;
        self.advance_presigned(signed)
    }

    /// The amount/bytes monotonicity + cap half of [`Self::stage_voucher`], for a
    /// voucher whose signature the caller ALREADY verified against `self.signer`.
    ///
    /// The serve loop recovers the (watermark-independent) signature OUTSIDE the
    /// per-lane lock so concurrent same-lane streams do not serialize on the
    /// `ecrecover` (#1735), then calls this under the lock to re-check the
    /// (watermark-dependent) monotonicity guards against the LIVE watermark and
    /// advance atomically. The monotonicity check and the advance MUST stay under
    /// one lock hold: two streams reading the same watermark and both advancing
    /// would lose one voucher.
    ///
    /// `pool_id` / `provider` are the lane's own identity and are not re-checked:
    /// the caller reconstructs `signed` from this lane's pinned identity, so they
    /// match by construction. Every other check mirrors [`Self::stage_voucher`].
    ///
    /// # Errors
    ///
    /// [`PoolError::AmountRegression`], [`PoolError::BytesRegression`], or
    /// [`PoolError::CapExceeded`] — the same watermark-dependent taxonomy as
    /// [`Self::stage_voucher`], minus the signature and pool/provider checks.
    pub fn advance_presigned(
        &self,
        signed: &SignedVoucher,
    ) -> Result<(Self, VoucherApplied), PoolError> {
        if signed.voucher.amount <= self.last_amount {
            return Err(PoolError::AmountRegression {
                last: self.last_amount,
                got: signed.voucher.amount,
            });
        }
        if signed.voucher.bytes_delivered < self.last_bytes_delivered {
            return Err(PoolError::BytesRegression {
                last: self.last_bytes_delivered,
                got: signed.voucher.bytes_delivered,
            });
        }
        if signed.voucher.amount > self.cap {
            return Err(PoolError::CapExceeded {
                cap: self.cap,
                got: signed.voucher.amount,
            });
        }

        let mut next = self.clone();
        let amount_delta = signed.voucher.amount - self.last_amount;
        let bytes_delta = signed.voucher.bytes_delivered - self.last_bytes_delivered;
        next.last_amount = signed.voucher.amount;
        next.last_bytes_delivered = signed.voucher.bytes_delivered;
        // Retain the signature so the seller path can submit this exact voucher
        // to the on-chain `PaymentPool.redeem`. Same `r‖s‖v` encoding as the
        // wire form in `client_bridge`; `Signature::as_bytes` is exactly 65
        // bytes.
        next.last_signature = Some(signed.signature.as_bytes());

        Ok((
            next,
            VoucherApplied {
                amount_delta,
                bytes_delta,
            },
        ))
    }
}

/// Outcome of a successful [`LaneState::apply_voucher`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoucherApplied {
    /// Incremental amount this voucher added over the prior watermark
    /// (`voucher.amount - last_amount`). Always positive — the amount guard
    /// rejects a non-increasing voucher.
    amount_delta: U256,
    /// Incremental bytes this voucher added over the prior watermark
    /// (`voucher.bytes_delivered - last_bytes_delivered`). Zero or positive.
    bytes_delta: U256,
}

impl VoucherApplied {
    /// Incremental amount this voucher added over the prior watermark.
    #[must_use]
    pub const fn amount_delta(&self) -> U256 {
        self.amount_delta
    }

    /// Incremental bytes this voucher added over the prior watermark.
    #[must_use]
    pub const fn bytes_delta(&self) -> U256 {
        self.bytes_delta
    }
}

/// Failure modes for [`LaneState::apply_voucher`].
///
/// Each in-memory variant maps to an on-chain `PaymentPool.redeem` revert;
/// off-chain we surface them before they cost gas. [`PoolError::Store`] is the
/// additional off-chain-only variant for persistent-store failures (issue
/// #527).
///
/// `PartialEq`/`Eq` are intentionally not derived: [`StoreError::Io`] wraps
/// `std::io::Error`, which is not `PartialEq`. Tests pattern-match on variants
/// via the `matches!` macro instead of comparing for equality.
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    /// `voucher.pool_id` does not match the pool this lane draws from — also
    /// the "unknown pool" case surfaced by the caller.
    #[error("voucher pool_id {got} does not match expected {expected}")]
    WrongPool { expected: B256, got: B256 },
    /// `voucher.provider` names a node other than this one — a capability
    /// voucher scoped to one provider redeemed against another.
    #[error("voucher provider {got} does not match expected {expected}")]
    WrongProvider { expected: Address, got: Address },
    /// Cumulative amount did not strictly increase — a stale or replayed
    /// voucher. `amount` is the sole ordering key (there is no nonce).
    #[error("voucher amount {got} not greater than last accepted {last}")]
    AmountRegression { last: U256, got: U256 },
    /// Cumulative bytes delivered went down.
    #[error("voucher bytes_delivered {got} less than last accepted {last}")]
    BytesRegression { last: U256, got: U256 },
    /// Voucher amount exceeds the capability's spending cap — the contract
    /// would revert at redemption.
    #[error("voucher amount {got} exceeds capability cap {cap}")]
    CapExceeded { cap: U256, got: U256 },
    /// Signature is malformed or signed by the wrong address.
    #[error(transparent)]
    Signature(#[from] VoucherError),
    /// Persistent-store write (or fsync) failed; in-memory state is unchanged.
    /// See [`StoreError`] for the underlying cause. This is surfaced to the
    /// caller so the client resends the same voucher instead of proceeding on
    /// state that was never durably committed (#527, ADR 003 §Off-chain voucher
    /// state persistence).
    ///
    /// PROTOCOL NOTE: this is the only `PoolError` variant for which the client
    /// SHOULD retry the same voucher unchanged. The validation variants mean the
    /// voucher is permanently invalid for this lane state and retrying is a
    /// client bug.
    #[error("pool state store failed: {0}")]
    Store(#[from] StoreError),
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed pair up clearly here
mod tests {
    use super::*;
    use crate::store::{MemoryPoolStateStore, PoolStateStore, StoreError};
    use crate::voucher::{Voucher, voucher_domain};
    use alloy::primitives::{address, b256};
    use alloy::signers::local::PrivateKeySigner;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn registered_until_defaults_zero_and_survives_stage_voucher_clone() {
        let st = LaneState::hydrate(
            B256::ZERO,
            Address::ZERO,
            Address::ZERO,
            U256::MAX,
            0,
            U256::ZERO,
            U256::ZERO,
            None,
        );
        assert_eq!(st.registered_until, 0, "hydrate seeds unknown");
        let mut with_reg = st.clone();
        with_reg.registered_until = 1_800_000_000;
        // stage_voucher clones self; the clone must carry registered_until forward.
        let cloned = with_reg.clone();
        assert_eq!(cloned.registered_until, 1_800_000_000);
    }

    /// `PoolStateStore` whose `record` always errors. Proves the
    /// strict-durability invariant: `apply_voucher` MUST surface the store
    /// failure as `PoolError::Store(..)` and MUST NOT advance the in-memory
    /// `last_*` fields.
    struct FailingStore {
        record_calls: AtomicUsize,
    }

    impl FailingStore {
        fn new() -> Self {
            Self {
                record_calls: AtomicUsize::new(0),
            }
        }
        fn record_calls(&self) -> usize {
            self.record_calls.load(Ordering::SeqCst)
        }
    }

    impl PoolStateStore for FailingStore {
        fn load_all(&self) -> Result<Vec<LaneState>, StoreError> {
            Ok(Vec::new())
        }
        fn record(&self, _state: &LaneState) -> Result<(), StoreError> {
            self.record_calls.fetch_add(1, Ordering::SeqCst);
            Err(StoreError::Io(std::io::Error::other(
                "simulated fsync failure",
            )))
        }
        fn forget(&self, _key: LaneKey) -> Result<(), StoreError> {
            Ok(())
        }
        fn get(&self, _key: LaneKey) -> Result<Option<LaneState>, StoreError> {
            Ok(None)
        }
    }

    const CHAIN_ID: u64 = 421_614;
    const VERIFYING: Address = address!("0000000000000000000000000000000000001234");
    const PROVIDER: Address = address!("00000000000000000000000000000000000000b2");
    const POOL_ID: B256 = b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff");

    /// Shared test fixture: a fresh keypair, an empty `LaneState` (cap 10 USDC),
    /// the voucher EIP-712 domain, and an in-memory `PoolStateStore`.
    fn fixture() -> (
        PrivateKeySigner,
        LaneState,
        Eip712Domain,
        MemoryPoolStateStore,
    ) {
        let signer = PrivateKeySigner::random();
        let state = LaneState::hydrate(
            POOL_ID,
            signer.address(),
            PROVIDER,
            U256::from(10_000_000u64), // 10 USDC cap
            0,
            U256::ZERO,
            U256::ZERO,
            None,
        );
        let domain = voucher_domain(CHAIN_ID, VERIFYING);
        let store = MemoryPoolStateStore::new();
        (signer, state, domain, store)
    }

    fn build(
        pool_id: B256,
        signer: Address,
        provider: Address,
        amount: u64,
        bytes_delivered: u64,
    ) -> Voucher {
        Voucher {
            pool_id,
            signer,
            provider,
            amount: U256::from(amount),
            bytes_delivered: U256::from(bytes_delivered),
        }
    }

    fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
        r.err()
            .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
    }

    #[test]
    fn first_voucher_advances_state() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let signed =
            build(POOL_ID, signer.address(), PROVIDER, 1_000, 1_048_576).sign(&signer, &domain)?;

        let applied = state.apply_voucher(&signed, &domain, &store)?;
        anyhow::ensure!(state.last_amount == U256::from(1_000u64));
        anyhow::ensure!(state.last_bytes_delivered == U256::from(1_048_576u64));
        anyhow::ensure!(applied.amount_delta() == U256::from(1_000u64));
        anyhow::ensure!(applied.bytes_delta() == U256::from(1_048_576u64));
        Ok(())
    }

    #[test]
    fn monotonic_progression_accepted() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        for (amount, bytes) in [(1_000u64, 1_048_576u64), (2_500, 2_621_440)] {
            let signed =
                build(POOL_ID, signer.address(), PROVIDER, amount, bytes).sign(&signer, &domain)?;
            state.apply_voucher(&signed, &domain, &store)?;
        }
        anyhow::ensure!(state.last_amount == U256::from(2_500u64));
        Ok(())
    }

    #[test]
    fn wrong_pool_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let signed =
            build(B256::ZERO, signer.address(), PROVIDER, 1_000, 1).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain, &store))?;
        anyhow::ensure!(matches!(err, PoolError::WrongPool { .. }), "{err:?}");
        anyhow::ensure!(state.last_amount == U256::ZERO, "state must be unchanged");
        Ok(())
    }

    #[test]
    fn wrong_provider_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let other_provider = address!("000000000000000000000000000000000000cccc");
        let signed =
            build(POOL_ID, signer.address(), other_provider, 1_000, 1).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain, &store))?;
        anyhow::ensure!(matches!(err, PoolError::WrongProvider { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn equal_amount_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let v1 = build(POOL_ID, signer.address(), PROVIDER, 1_000, 1).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain, &store)?;

        let v2 = build(POOL_ID, signer.address(), PROVIDER, 1_000, 2).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&v2, &domain, &store))?;
        anyhow::ensure!(matches!(err, PoolError::AmountRegression { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn lower_amount_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let v1 = build(POOL_ID, signer.address(), PROVIDER, 5_000, 1).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain, &store)?;

        let v2 = build(POOL_ID, signer.address(), PROVIDER, 4_000, 2).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&v2, &domain, &store))?;
        anyhow::ensure!(matches!(err, PoolError::AmountRegression { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn bytes_decrease_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let v1 =
            build(POOL_ID, signer.address(), PROVIDER, 1_000, 1_048_576).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain, &store)?;

        let v2 =
            build(POOL_ID, signer.address(), PROVIDER, 2_000, 524_288).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&v2, &domain, &store))?;
        anyhow::ensure!(matches!(err, PoolError::BytesRegression { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn amount_exceeds_cap_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        // cap is 10_000_000 (10 USDC); attempt 11 USDC.
        let signed =
            build(POOL_ID, signer.address(), PROVIDER, 11_000_000, 1).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain, &store))?;
        anyhow::ensure!(matches!(err, PoolError::CapExceeded { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn wrong_signer_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        // Sign with an unrelated key — the recovered address is not the lane's
        // pinned capability signer.
        let interloper = PrivateKeySigner::random();
        let signed =
            build(POOL_ID, signer.address(), PROVIDER, 1_000, 1).sign(&interloper, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain, &store))?;
        anyhow::ensure!(
            matches!(err, PoolError::Signature(VoucherError::WrongSigner { .. })),
            "{err:?}"
        );
        anyhow::ensure!(
            state.last_amount == U256::ZERO,
            "rejection must not advance"
        );
        Ok(())
    }

    #[test]
    fn high_s_voucher_rejected_at_apply() -> anyhow::Result<()> {
        // A high-`s` voucher recovers the correct signer off-chain but is
        // unsettleable on-chain (#836). `apply_voucher` must reject it via the
        // transitive `recover_signer` guard and leave state untouched.
        let (signer, mut state, domain, store) = fixture();
        let signed =
            build(POOL_ID, signer.address(), PROVIDER, 1_000, 1_048_576).sign(&signer, &domain)?;
        let twin = SignedVoucher {
            signature: crate::sig_canon::high_s_twin(&signed.signature),
            ..signed
        };
        let err = err_of(state.apply_voucher(&twin, &domain, &store))?;
        anyhow::ensure!(
            matches!(err, PoolError::Signature(VoucherError::InvalidSignature)),
            "{err:?}"
        );
        anyhow::ensure!(
            state.last_amount == U256::ZERO,
            "rejection must not advance"
        );
        Ok(())
    }

    /// **Strict-durability regression (#527).** If `store.record` fails, the
    /// in-memory `LaneState` MUST NOT advance. Breaks if anyone reorders the
    /// `store.record(&next)?` and `*self = next` lines.
    #[test]
    fn store_failure_leaves_in_memory_state_unchanged() -> anyhow::Result<()> {
        let (signer, mut state, domain, _mem_store) = fixture();
        let snapshot = state.clone();
        let failing = FailingStore::new();

        let signed =
            build(POOL_ID, signer.address(), PROVIDER, 1_000, 1_048_576).sign(&signer, &domain)?;
        let err = state
            .apply_voucher(&signed, &domain, &failing)
            .err()
            .ok_or_else(|| anyhow::anyhow!("store failure must surface to caller"))?;
        anyhow::ensure!(
            matches!(err, PoolError::Store(_)),
            "expected PoolError::Store, got {err:?}",
        );
        anyhow::ensure!(
            state == snapshot,
            "in-memory state must NOT advance when store.record fails",
        );
        anyhow::ensure!(
            failing.record_calls() == 1,
            "expected exactly one `record` call, got {}",
            failing.record_calls(),
        );
        Ok(())
    }

    /// Companion: after a `record` failure, a subsequent successful apply MUST
    /// still work — the failure didn't poison the in-memory state for retries.
    #[test]
    fn store_failure_does_not_poison_subsequent_retries() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let failing = FailingStore::new();
        let signed =
            build(POOL_ID, signer.address(), PROVIDER, 1_000, 1_048_576).sign(&signer, &domain)?;

        let _err = state
            .apply_voucher(&signed, &domain, &failing)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected store failure"))?;
        anyhow::ensure!(state.last_amount == U256::ZERO);

        state.apply_voucher(&signed, &domain, &store)?;
        anyhow::ensure!(state.last_amount == U256::from(1_000u64));
        Ok(())
    }

    /// Cover every rejection path leaves state untouched and never writes
    /// through — the issue #527 regression guard at the in-memory layer.
    #[test]
    fn rejected_voucher_does_not_advance_state() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let v1 = build(POOL_ID, signer.address(), PROVIDER, 5_000, 1_000).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain, &store)?;
        let snapshot = state.clone();
        let interloper = PrivateKeySigner::random();
        let other_provider = address!("000000000000000000000000000000000000cccc");

        let cases: [(SignedVoucher, &str); 6] = [
            (
                build(B256::ZERO, signer.address(), PROVIDER, 6_000, 2_000)
                    .sign(&signer, &domain)?,
                "wrong pool",
            ),
            (
                build(POOL_ID, signer.address(), other_provider, 6_000, 2_000)
                    .sign(&signer, &domain)?,
                "wrong provider",
            ),
            (
                build(POOL_ID, signer.address(), PROVIDER, 5_000, 2_000).sign(&signer, &domain)?,
                "equal amount",
            ),
            (
                build(POOL_ID, signer.address(), PROVIDER, 6_000, 500).sign(&signer, &domain)?,
                "bytes drop",
            ),
            (
                build(POOL_ID, signer.address(), PROVIDER, 11_000_000, 2_000)
                    .sign(&signer, &domain)?,
                "amount over cap",
            ),
            (
                build(POOL_ID, signer.address(), PROVIDER, 6_000, 2_000)
                    .sign(&interloper, &domain)?,
                "wrong signer",
            ),
        ];

        for (voucher, reason) in &cases {
            let _ = state.apply_voucher(voucher, &domain, &store);
            anyhow::ensure!(state == snapshot, "{reason} must not advance state");
        }
        let persisted = store.load_all()?;
        anyhow::ensure!(persisted.len() == 1, "exactly one lane persisted");
        let only = persisted
            .first()
            .ok_or_else(|| anyhow::anyhow!("expected one persisted entry"))?;
        anyhow::ensure!(
            *only == snapshot,
            "rejected voucher path must not overwrite stored lane state",
        );
        Ok(())
    }

    /// Staging vouchers against an advancing candidate and recording ONLY the
    /// final state yields the same in-memory result AND the same single persisted
    /// row as applying each voucher through `apply_voucher`.
    #[test]
    fn stage_batch_then_record_once_equals_sequential_apply() -> anyhow::Result<()> {
        let (signer, base, domain, batch_store) = fixture();

        // Reference: apply three vouchers one-by-one (three records).
        let mut seq_state = base.clone();
        let seq_store = MemoryPoolStateStore::new();
        let vouchers = [
            (1_000u64, 1_048_576u64),
            (2_000, 2_097_152),
            (3_000, 3_145_728),
        ];
        for (amount, bytes) in vouchers {
            let signed =
                build(POOL_ID, signer.address(), PROVIDER, amount, bytes).sign(&signer, &domain)?;
            seq_state.apply_voucher(&signed, &domain, &seq_store)?;
        }

        // Batched: stage each against an advancing candidate, record ONCE.
        let mut candidate = base.clone();
        for (amount, bytes) in vouchers {
            let signed =
                build(POOL_ID, signer.address(), PROVIDER, amount, bytes).sign(&signer, &domain)?;
            let (next, _applied) = candidate.stage_voucher(&signed, &domain)?;
            candidate = next;
        }
        batch_store.record(&candidate)?;

        anyhow::ensure!(
            candidate == seq_state,
            "batched state must equal sequential"
        );
        anyhow::ensure!(candidate.last_amount() == U256::from(3_000u64));
        anyhow::ensure!(batch_store.len() == 1, "batch persists exactly one row");
        let persisted = batch_store.load_all()?;
        let only = persisted.first().ok_or_else(|| anyhow::anyhow!("no row"))?;
        anyhow::ensure!(*only == seq_state, "one record commits the whole batch");
        Ok(())
    }

    /// Staging is pure: a rejected voucher leaves the candidate that produced it
    /// untouched, so a caller can keep the advanced state and reject the offender
    /// without rolling back.
    #[test]
    fn stage_voucher_rejects_without_advancing_candidate() -> anyhow::Result<()> {
        let (signer, base, domain, _store) = fixture();
        let v1 =
            build(POOL_ID, signer.address(), PROVIDER, 1_000, 1_048_576).sign(&signer, &domain)?;
        let (after_v1, _) = base.stage_voucher(&v1, &domain)?;

        // A stale-amount voucher against the advanced candidate must reject.
        let bad =
            build(POOL_ID, signer.address(), PROVIDER, 1_000, 2_097_152).sign(&signer, &domain)?;
        let err = err_of(after_v1.stage_voucher(&bad, &domain))?;
        anyhow::ensure!(matches!(err, PoolError::AmountRegression { .. }), "{err:?}");
        anyhow::ensure!(after_v1.last_amount() == U256::from(1_000u64));
        Ok(())
    }
}
