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
    /// The chain the lane is currently metering against — the epoch opened by
    /// the most-recently-accepted voucher. [`LaneChain::NONE`] until the first
    /// voucher, and after any sealed voucher. Private for the same reason as
    /// the `last_*` fields: `verified_index` is a money-bearing watermark, so
    /// advancing it outside the validated path would forge payment. Read via
    /// [`Self::chain`].
    chain: LaneChain,
    /// A retired epoch kept because it is still the **stronger** claim.
    ///
    /// A payer that rolls a chain is supposed to fold the frontier the node
    /// actually proved into the new voucher's `amount`. One that folds *less*
    /// than that would otherwise make the node lose the difference, since
    /// adopting the new root retires the old chain. So when a rollover would
    /// regress the lane's total, the outgoing claim is retained here and the
    /// redeem planner keeps submitting it — no on-chain sequencing rule is
    /// needed, because payment is cumulative and the weaker claim simply pays
    /// `0` (ADR 003 §Rollover). Read via [`Self::retained`].
    retained: Option<RedeemClaim>,
}

/// One metering epoch's hash-chain state on a lane (ADR 003 §Hash-chain
/// metering (`PayWord`)).
///
/// `chain_root` is the head the payer signed into the epoch's voucher, and
/// `(verified_index, tip)` is the deepest preimage the node has verified under
/// it — the frontier that turns into money at redemption. `tip` is the preimage
/// **bytes**, not just the depth: only the payer can produce a value at a given
/// depth, so a node that kept the index alone would hold an unprovable claim
/// after a restart, in exactly the abandonment case the chain exists to cover.
///
/// A sealed epoch (`chain_root == B256::ZERO`) meters nothing: `chunk_price` is
/// zero, `verified_index` stays `0`, and `tip` is zero. Nothing hashes to zero,
/// so no index above `0` can ever be verified against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneChain {
    /// Head of this epoch's chain; zero seals the epoch at its anchor amount.
    pub chain_root: B256,
    /// What one chunk adds over the anchor, in token base units. Zero on a
    /// sealed epoch.
    pub chunk_price: U256,
    /// Deepest chain index verified under `chain_root`; `0` until the first
    /// preimage lands.
    pub verified_index: u8,
    /// The preimage bytes at `verified_index`, or `chain_root` itself at index
    /// `0` — which is what makes the walk uniform with no branch.
    pub tip: B256,
}

impl LaneChain {
    /// The empty chain: no root, no price, nothing verified. What a lane holds
    /// before its first voucher, and what a sealed voucher installs.
    pub const NONE: Self = Self {
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
        verified_index: 0,
        tip: B256::ZERO,
    };

    /// A fresh epoch at index 0: the root is its own tip, so a claim settling at
    /// exactly the anchor amount submits a value the payer already holds and
    /// walks nothing.
    #[must_use]
    pub const fn opened(chain_root: B256, chunk_price: U256) -> Self {
        Self {
            chain_root,
            chunk_price,
            verified_index: 0,
            tip: chain_root,
        }
    }

    /// What the verified frontier adds over its anchor:
    /// `verified_index × chunk_price`.
    #[must_use]
    pub fn accrued(&self) -> U256 {
        self.chunk_price * U256::from(self.verified_index)
    }

    /// What the verified frontier adds over its anchor on the byte axis:
    /// `verified_index × CHUNK_BYTES`.
    #[must_use]
    pub fn accrued_bytes(&self) -> U256 {
        U256::from(crate::chain::CHUNK_BYTES) * U256::from(self.verified_index)
    }
}

/// A complete, self-contained claim a node can redeem: a signed anchor plus the
/// chain frontier that extends it.
///
/// Redemption resolves it as `claimed = amount + verified_index × chunk_price`
/// over `claimed_bytes = bytes_delivered + verified_index × CHUNK_BYTES`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedeemClaim {
    /// The anchor voucher's cumulative amount.
    pub amount: U256,
    /// The anchor voucher's cumulative bytes delivered.
    pub bytes_delivered: U256,
    /// The anchor voucher's `r‖s‖v` signature.
    pub signature: [u8; 65],
    /// The chain extending that anchor.
    pub chain: LaneChain,
}

impl RedeemClaim {
    /// The claim's full value: anchor plus verified frontier. This is the
    /// number the redeem planner compares and the contract recomputes.
    #[must_use]
    pub fn value(&self) -> U256 {
        self.amount + self.chain.accrued()
    }

    /// The claim's full byte count: anchor plus verified frontier.
    #[must_use]
    pub fn bytes_value(&self) -> U256 {
        self.bytes_delivered + self.chain.accrued_bytes()
    }

    /// The packed `chainMeter` word this claim submits on-chain — its
    /// `chunk_price` and `verified_index` in one word (ADR 003 §Voucher
    /// signatures are compact).
    ///
    /// # Errors
    ///
    /// [`crate::chain::ChainMeterError`] when `chunk_price` exceeds the
    /// `uint64` the word gives it, which the contract could not settle anyway.
    pub fn chain_meter(&self) -> Result<U256, crate::chain::ChainMeterError> {
        crate::chain::pack_chain_meter(self.chain.chunk_price, self.chain.verified_index)
    }
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
        chain: LaneChain,
        retained: Option<RedeemClaim>,
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
            chain,
            retained,
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

    /// The epoch this lane is currently metering against. See the
    /// [field invariant](Self).
    #[must_use]
    pub const fn chain(&self) -> LaneChain {
        self.chain
    }

    /// A retired-but-stronger claim held over an under-folding rollover, if
    /// any. See the [field invariant](Self).
    #[must_use]
    pub const fn retained(&self) -> Option<RedeemClaim> {
        self.retained
    }

    /// The live epoch expressed as a redeemable claim: the last accepted
    /// voucher's anchor plus the frontier verified under its root. `None` until
    /// a voucher has been accepted (there is no signature to submit).
    #[must_use]
    pub fn live_claim(&self) -> Option<RedeemClaim> {
        self.last_signature.map(|signature| RedeemClaim {
            amount: self.last_amount,
            bytes_delivered: self.last_bytes_delivered,
            signature,
            chain: self.chain,
        })
    }

    /// The strongest claim this lane holds — the maximum by value over the live
    /// epoch and any retained one. This is what the redeem planner submits.
    ///
    /// It is a **maximum, never a sum**: a rollover voucher already folds the
    /// retired chain into its own `amount`, so adding the two would count those
    /// chunks twice (ADR 003 §Concurrent Streams).
    #[must_use]
    pub fn strongest_claim(&self) -> Option<RedeemClaim> {
        match (self.live_claim(), self.retained) {
            (Some(live), Some(kept)) if kept.value() > live.value() => Some(kept),
            (Some(live), _) => Some(live),
            (None, kept) => kept,
        }
    }

    /// The byte frontier the lane's strongest claim covers — its signed
    /// cumulative extended by the chunks the chain has proved. Zero on a lane
    /// holding no claim.
    ///
    /// This, not `last_bytes_delivered`, is what the node has actually been paid
    /// for on the byte axis: a released preimage advances the frontier without
    /// a signature, so counting only the signed half would leave the node's own
    /// credit accounting a whole chain behind the money it is owed.
    #[must_use]
    pub fn owed_bytes(&self) -> U256 {
        self.strongest_claim()
            .map_or(U256::ZERO, |c| c.bytes_value())
    }

    /// What this lane is **owed**: the value of its strongest claim, or zero if
    /// it holds none.
    ///
    /// Counting only the signed cumulative would understate a lane by up to one
    /// whole chain, which is what `M` and the redemption floor are sized
    /// against (ADR 003 §Tracking owed vs. paid).
    #[must_use]
    pub fn owed(&self) -> U256 {
        self.strongest_claim().map_or(U256::ZERO, |c| c.value())
    }

    /// Adopt a chain from a voucher that did **not** advance the money — the
    /// already-satisfied case (ADR 003 §Voucher ordering).
    ///
    /// This exists because the first root voucher on a fresh lane sits exactly
    /// at the watermark: its `amount` is the cumulative the lane already holds,
    /// because nothing has been metered yet. The money path treats that as
    /// already-satisfied and advances nothing, which is right — but the voucher
    /// still has to install the chain, or the reveals that follow would name a
    /// root the lane never learned and fold nothing.
    ///
    /// Three narrow things happen here, and nothing else:
    ///
    /// - A lane metering **nothing** adopts the incoming root and price.
    /// - A lane already metering **that same root** refreshes its price.
    /// - A lane holding **no signature yet** adopts this voucher as its anchor.
    ///
    /// That last one is what makes the chain redeemable at all. A claim is a
    /// signed anchor extended by a frontier, so a lane that installed a root
    /// without keeping the signature that committed it would hold a frontier it
    /// could prove but never submit. The anchor is only adopted when there is
    /// none: a lane with a signature is already at or above this voucher's
    /// cumulative, by the branch that got us here.
    ///
    /// A non-advancing voucher never *retires* a live chain. It has not paid for
    /// the frontier that chain has proved, so it has no authority to end it —
    /// and retiring one would strand every sibling stream still metering against
    /// it. A genuine rollover always advances the money (its `amount` folds the
    /// retired chain's frontier in), so it goes through [`Self::advance_presigned`]
    /// instead, where the fold and the retirement are decided together.
    ///
    /// Returns `None` when nothing changed, so the caller can skip a needless
    /// store write.
    #[must_use]
    pub fn adopt_chain(&self, signed: &SignedVoucher) -> Option<Self> {
        let chain_root = signed.voucher.chain_root;
        if chain_root.is_zero() {
            return None;
        }
        let same_root = self.chain.chain_root == chain_root;
        if !same_root && !self.chain.chain_root.is_zero() {
            return None;
        }
        let refresh_price = self.chain.chunk_price != signed.voucher.chunk_price;
        let adopt_anchor = self.last_signature.is_none();
        if same_root && !refresh_price && !adopt_anchor {
            return None;
        }

        let mut next = self.clone();
        if same_root {
            next.chain.chunk_price = signed.voucher.chunk_price;
        } else {
            next.chain = LaneChain::opened(chain_root, signed.voucher.chunk_price);
        }
        if adopt_anchor {
            next.last_amount = signed.voucher.amount;
            next.last_bytes_delivered = signed.voucher.bytes_delivered;
            next.last_signature = Some(signed.signature.as_bytes());
        }
        Some(next)
    }

    /// Fold a verified preimage into the lane, returning the advanced successor
    /// and what it added (ADR 003 §Concurrent Streams, Rule 2 — deepest wins).
    ///
    /// `root` names the epoch the reveal belongs to; the caller learns it from
    /// the receiving stream's own anchor, which is what makes an unanchored
    /// reveal decidable per stream rather than ambiguously lane-wide (Rule 1).
    /// A `root` this lane no longer tracks folds nothing: the reveal is real but
    /// worthless, since the epoch it extends has been superseded by a signature
    /// that already folded a frontier at least as deep.
    ///
    /// Placement is by index, not arrival order, so a reveal at or below the
    /// tracked frontier is **already covered**: it advances nothing and — this
    /// is the load-bearing part — hashes nothing. That is what keeps a
    /// duplicate or out-of-order reveal free rather than a wasted walk.
    ///
    /// Verification hashes forward from the *submitted* value to the tracked
    /// tip, never from a stored intermediate, so the walk is `index − verified`
    /// keccaks and the lane keeps no chain state beyond the tip itself.
    ///
    /// # Errors
    ///
    /// [`PoolError::BadPreimage`] when the value does not reach the tracked tip
    /// in `index − verified` steps.
    pub fn advance_preimage(
        &self,
        root: B256,
        index: u8,
        preimage: B256,
    ) -> Result<(Self, PreimageApplied), PoolError> {
        let Some(target) = self.chain_slot(root) else {
            return Ok((self.clone(), PreimageApplied::SUPERSEDED));
        };
        if index <= target.verified_index {
            return Ok((self.clone(), PreimageApplied::ALREADY_COVERED));
        }
        if !crate::chain::verify_forward(preimage, index - target.verified_index, target.tip) {
            return Err(PoolError::BadPreimage {
                index,
                verified: target.verified_index,
            });
        }

        let steps = U256::from(index - target.verified_index);
        let applied = PreimageApplied {
            amount_delta: target.chunk_price * steps,
            bytes_delta: U256::from(crate::chain::CHUNK_BYTES) * steps,
        };

        let mut next = self.clone();
        if root == self.chain.chain_root {
            next.chain.verified_index = index;
            next.chain.tip = preimage;
        } else if let Some(kept) = next.retained.as_mut() {
            kept.chain.verified_index = index;
            kept.chain.tip = preimage;
        }
        Ok((next, applied))
    }

    /// Which tracked epoch, if any, `root` names. The live chain first; then a
    /// retained one, so a slower sibling stream still finishing the previous
    /// epoch keeps advancing the claim that epoch backs.
    ///
    /// A zero `root` never matches: it is the sealed sentinel, and nothing
    /// hashes to zero, so no reveal can extend it.
    fn chain_slot(&self, root: B256) -> Option<LaneChain> {
        if root.is_zero() {
            return None;
        }
        if root == self.chain.chain_root {
            return Some(self.chain);
        }
        self.retained
            .filter(|kept| kept.chain.chain_root == root)
            .map(|kept| kept.chain)
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
        // The caller reconstructs `signed` from this lane's pinned identity, so
        // `pool_id`/`provider` match by construction and are not re-checked on the
        // fast path. Guard against a call site that advances a lane with a voucher
        // built for a DIFFERENT identity (it would silently mutate the watermark).
        debug_assert_eq!(
            signed.voucher.pool_id, self.pool_id,
            "advance_presigned: voucher pool_id must match the lane's"
        );
        debug_assert_eq!(
            signed.voucher.provider, self.provider,
            "advance_presigned: voucher provider must match the lane's"
        );
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

        // A voucher carrying a DIFFERENT root retires the live epoch. Two things
        // follow, and both are about not losing money the node already proved.
        //
        // First, if the outgoing epoch's frontier is worth more than the new
        // voucher's `amount`, the payer under-folded: it signed for less than
        // the chunks the node holds preimages for. Adopting the new root would
        // discard that difference, so the outgoing claim is retained and the
        // redeem planner keeps submitting it (ADR 003 §Rollover). Redeeming the
        // retired claim and then the new one collects the same total either
        // way — on-chain payment is `claimed − paid`.
        //
        // Second, the new epoch starts at index 0 with the root as its own tip,
        // so a claim at exactly the new `amount` walks nothing. A voucher that
        // repeats the SAME root is not a rollover at all (a re-send, or a
        // mid-epoch amount bump), and MUST leave the frontier where it is.
        if signed.voucher.chain_root == self.chain.chain_root {
            // Same epoch: the price is re-asserted by every voucher that names
            // the root, and the handler has already refused a price that is not
            // this node's quote, so adopting it keeps the two in step.
            next.chain.chunk_price = signed.voucher.chunk_price;
        } else {
            if let Some(live) = self.live_claim()
                && live.value() > signed.voucher.amount
            {
                next.retained = Some(match next.retained {
                    Some(kept) if kept.value() > live.value() => kept,
                    _ => live,
                });
            }
            next.chain = LaneChain::opened(signed.voucher.chain_root, signed.voucher.chunk_price);
        }

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

/// Outcome of a [`LaneState::advance_preimage`] fold.
///
/// Both deltas are zero for a reveal that advanced nothing — one at or below
/// the tracked frontier, or one naming a superseded epoch. Neither is an error:
/// a duplicate or out-of-order reveal is ordinary under concurrent streams, and
/// the node answers it by continuing to deliver, exactly as for an
/// already-satisfied voucher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreimageApplied {
    /// `steps × chunk_price` — what this reveal added to the lane's claim.
    amount_delta: U256,
    /// `steps × CHUNK_BYTES` — the byte frontier the same steps paid for.
    bytes_delta: U256,
}

impl PreimageApplied {
    /// The reveal was at or below the tracked frontier: already covered, and
    /// nothing was hashed to find that out.
    pub const ALREADY_COVERED: Self = Self {
        amount_delta: U256::ZERO,
        bytes_delta: U256::ZERO,
    };

    /// The reveal named an epoch the lane no longer tracks — a signature has
    /// since folded a frontier at least as deep. Real, but worth nothing.
    pub const SUPERSEDED: Self = Self {
        amount_delta: U256::ZERO,
        bytes_delta: U256::ZERO,
    };

    /// Amount this reveal added to the lane's claim.
    #[must_use]
    pub const fn amount_delta(&self) -> U256 {
        self.amount_delta
    }

    /// Bytes this reveal's steps paid for.
    #[must_use]
    pub const fn bytes_delta(&self) -> U256 {
        self.bytes_delta
    }

    /// `true` when the reveal advanced the lane's claim.
    #[must_use]
    pub fn advanced(&self) -> bool {
        !self.amount_delta.is_zero() || !self.bytes_delta.is_zero()
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
    /// A released hash-chain preimage does not reach the tracked tip in
    /// `index − verified` steps (ADR 003 §Concurrent Streams, Rule 2).
    ///
    /// Terminal, and deliberately **not** watermark-gated on the wire: a
    /// preimage carries no signature of its own, so a payment watermark cannot
    /// repair a hash-chain mismatch. On-chain the same mismatch reverts
    /// `BadPreimage`, which is caller error rather than transient state.
    #[error("released preimage at index {index} does not reach the tip verified at {verified}")]
    BadPreimage { index: u8, verified: u8 },
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
            LaneChain::NONE,
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
            LaneChain::NONE,
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
            chain_root: B256::ZERO,
            chunk_price: U256::ZERO,
        }
    }

    /// A metering voucher: opens (or re-asserts) an epoch on `chain_root` at
    /// `chunk_price`.
    fn build_metering(
        signer: Address,
        amount: u64,
        bytes_delivered: u64,
        chain_root: B256,
        chunk_price: u64,
    ) -> Voucher {
        Voucher {
            pool_id: POOL_ID,
            signer,
            provider: PROVIDER,
            amount: U256::from(amount),
            bytes_delivered: U256::from(bytes_delivered),
            chain_root,
            chunk_price: U256::from(chunk_price),
        }
    }

    const PRICE: u64 = 10;
    /// The payer's per-lane seed, standing in for `chain::derive_seed`.
    const SEED: B256 = B256::repeat_byte(0x5E);

    fn root() -> B256 {
        crate::chain::root_from_seed(SEED)
    }

    fn reveal(index: u8) -> B256 {
        crate::chain::preimage_at(SEED, index)
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

    // --- PayWord hash chain (ADR 003 §Hash-chain metering) -------------------

    /// The base case: a voucher opens an epoch, and the lane starts metering
    /// at index 0 with the root as its own tip — so a claim at exactly the
    /// signed `amount` walks nothing.
    #[test]
    fn a_metering_voucher_opens_an_epoch_at_index_zero() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let signed = build_metering(signer.address(), 1_000, 1_048_576, root(), PRICE)
            .sign(&signer, &domain)?;
        state.apply_voucher(&signed, &domain, &store)?;

        let chain = state.chain();
        anyhow::ensure!(chain.chain_root == root());
        anyhow::ensure!(chain.chunk_price == U256::from(PRICE));
        anyhow::ensure!(chain.verified_index == 0);
        anyhow::ensure!(chain.tip == root(), "the root is its own tip at index 0");
        anyhow::ensure!(state.owed() == U256::from(1_000u64));
        Ok(())
    }

    /// One tick: a reveal at index 1 adds exactly one `chunk_price` over the
    /// anchor and one `CHUNK_BYTES` on the byte axis.
    #[test]
    fn one_reveal_adds_one_chunk_price_over_the_anchor() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        state.apply_voucher(
            &build_metering(signer.address(), 1_000, 1_048_576, root(), PRICE)
                .sign(&signer, &domain)?,
            &domain,
            &store,
        )?;

        let (next, applied) = state.advance_preimage(root(), 1, reveal(1))?;
        anyhow::ensure!(applied.advanced());
        anyhow::ensure!(applied.amount_delta() == U256::from(PRICE));
        anyhow::ensure!(applied.bytes_delta() == U256::from(crate::chain::CHUNK_BYTES));
        anyhow::ensure!(next.chain().verified_index == 1);
        anyhow::ensure!(next.chain().tip == reveal(1));
        anyhow::ensure!(next.owed() == U256::from(1_000 + PRICE));
        Ok(())
    }

    /// A fast stream may skip indices a slower one has not reached, so the walk
    /// must span an arbitrary gap and credit every step it crossed.
    #[test]
    fn a_skipped_gap_credits_every_step_it_crossed() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        state.apply_voucher(
            &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
            &domain,
            &store,
        )?;

        let (next, applied) = state.advance_preimage(root(), 7, reveal(7))?;
        anyhow::ensure!(applied.amount_delta() == U256::from(7 * PRICE));
        anyhow::ensure!(next.owed() == U256::from(1_000 + 7 * PRICE));
        Ok(())
    }

    /// Deepest wins: a reveal at or below the frontier is already covered. It
    /// advances nothing and is NOT an error — a duplicate or out-of-order
    /// reveal is ordinary once several streams share one lane.
    #[test]
    fn a_reveal_at_or_below_the_frontier_is_already_covered() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        state.apply_voucher(
            &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
            &domain,
            &store,
        )?;
        let (state, _) = state.advance_preimage(root(), 5, reveal(5))?;

        for index in [1u8, 4, 5] {
            let (next, applied) = state.advance_preimage(root(), index, reveal(index))?;
            anyhow::ensure!(!applied.advanced(), "index {index} must advance nothing");
            anyhow::ensure!(next.chain().verified_index == 5, "frontier must hold");
        }
        Ok(())
    }

    /// A value from another chain never reaches the tip, which is the whole
    /// basis of the `BadPreimage` rejection.
    #[test]
    fn a_foreign_preimage_is_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        state.apply_voucher(
            &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
            &domain,
            &store,
        )?;

        let foreign = crate::chain::preimage_at(B256::repeat_byte(0x5F), 3);
        let err = err_of(state.advance_preimage(root(), 3, foreign))?;
        anyhow::ensure!(
            matches!(
                err,
                PoolError::BadPreimage {
                    index: 3,
                    verified: 0
                }
            ),
            "expected BadPreimage, got: {err:?}"
        );
        anyhow::ensure!(
            state.chain().verified_index == 0,
            "a rejection advances nothing"
        );
        Ok(())
    }

    /// Nothing hashes to zero, so a sealed voucher is sealed at exactly its
    /// `amount`: no reveal can extend it, at any index.
    #[test]
    fn a_sealed_voucher_cannot_be_extended() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        state.apply_voucher(
            &build(POOL_ID, signer.address(), PROVIDER, 1_000, 0).sign(&signer, &domain)?,
            &domain,
            &store,
        )?;
        anyhow::ensure!(state.chain().chain_root.is_zero());

        for index in [1u8, 255] {
            let (next, applied) = state.advance_preimage(B256::ZERO, index, reveal(index))?;
            anyhow::ensure!(!applied.advanced());
            anyhow::ensure!(next.owed() == U256::from(1_000u64));
        }
        Ok(())
    }

    /// The cooperative rollover: the payer folds the frontier the node actually
    /// proved into the new voucher's `amount`, so the lane's total is unchanged
    /// by the roll itself and the new epoch starts clean at index 0.
    #[test]
    fn a_correctly_folded_rollover_preserves_the_total() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        state.apply_voucher(
            &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
            &domain,
            &store,
        )?;
        let (state_at_3, _) = state.advance_preimage(root(), 3, reveal(3))?;
        let owed_before = state_at_3.owed();
        anyhow::ensure!(owed_before == U256::from(1_000 + 3 * PRICE));

        let next_seed = B256::repeat_byte(0x6E);
        let next_root = crate::chain::root_from_seed(next_seed);
        let folded = build_metering(
            signer.address(),
            1_000 + 3 * PRICE,
            3 * crate::chain::CHUNK_BYTES,
            next_root,
            PRICE,
        )
        .sign(&signer, &domain)?;
        let (rolled, _) = state_at_3.advance_presigned(&folded)?;

        anyhow::ensure!(rolled.owed() == owed_before, "the fold must lose nothing");
        anyhow::ensure!(
            rolled.retained().is_none(),
            "nothing to retain on a correct fold"
        );
        anyhow::ensure!(rolled.chain().chain_root == next_root);
        anyhow::ensure!(rolled.chain().verified_index == 0);
        Ok(())
    }

    /// ADR 003 §Rollover: a payer that folds LESS than the frontier the node
    /// proved would otherwise make the node lose the difference, because
    /// adopting the new root retires the old chain. The outgoing claim is kept
    /// instead, and stays the one the node redeems.
    #[test]
    fn an_under_folded_rollover_retains_the_stronger_claim() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        state.apply_voucher(
            &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
            &domain,
            &store,
        )?;
        let (state_at_9, _) = state.advance_preimage(root(), 9, reveal(9))?;
        let owed_before = state_at_9.owed();

        // Folds only 2 of the 9 chunks the node holds a preimage for.
        let next_root = crate::chain::root_from_seed(B256::repeat_byte(0x6E));
        let stingy = build_metering(
            signer.address(),
            1_000 + 2 * PRICE,
            2 * crate::chain::CHUNK_BYTES,
            next_root,
            PRICE,
        )
        .sign(&signer, &domain)?;
        let (rolled, _) = state_at_9.advance_presigned(&stingy)?;

        anyhow::ensure!(rolled.owed() == owed_before, "the node must lose nothing");
        let kept = rolled
            .retained()
            .ok_or_else(|| anyhow::anyhow!("the outgoing claim must be retained"))?;
        anyhow::ensure!(kept.chain.chain_root == root());
        anyhow::ensure!(kept.chain.verified_index == 9);
        anyhow::ensure!(kept.chain.tip == reveal(9));
        let strongest = rolled
            .strongest_claim()
            .ok_or_else(|| anyhow::anyhow!("a lane with a signature has a claim"))?;
        anyhow::ensure!(strongest == kept, "the retired claim is the stronger one");
        Ok(())
    }

    /// A slower sibling stream still finishing the retired epoch keeps
    /// advancing the claim that epoch backs — that is what "a rollover cannot
    /// strand a sibling mid-chain" means in state terms.
    #[test]
    fn a_retained_epoch_still_accepts_deeper_reveals() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        state.apply_voucher(
            &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
            &domain,
            &store,
        )?;
        let (state_at_9, _) = state.advance_preimage(root(), 9, reveal(9))?;
        let next_root = crate::chain::root_from_seed(B256::repeat_byte(0x6E));
        let (rolled, _) = state_at_9.advance_presigned(
            &build_metering(signer.address(), 1_000 + 2 * PRICE, 0, next_root, PRICE)
                .sign(&signer, &domain)?,
        )?;

        let (deeper, applied) = rolled.advance_preimage(root(), 12, reveal(12))?;
        anyhow::ensure!(applied.amount_delta() == U256::from(3 * PRICE));
        anyhow::ensure!(deeper.owed() == U256::from(1_000 + 12 * PRICE));
        anyhow::ensure!(
            deeper.chain().verified_index == 0,
            "the LIVE epoch is untouched by a retired epoch's reveal"
        );
        Ok(())
    }

    /// A reveal naming an epoch the lane no longer tracks is real but worth
    /// nothing: a signature has since folded a frontier at least as deep.
    #[test]
    fn a_reveal_for_a_superseded_epoch_folds_nothing() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        state.apply_voucher(
            &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
            &domain,
            &store,
        )?;
        let (state_at_3, _) = state.advance_preimage(root(), 3, reveal(3))?;
        let next_root = crate::chain::root_from_seed(B256::repeat_byte(0x6E));
        let (rolled, _) = state_at_3.advance_presigned(
            &build_metering(signer.address(), 1_000 + 3 * PRICE, 0, next_root, PRICE)
                .sign(&signer, &domain)?,
        )?;
        anyhow::ensure!(rolled.retained().is_none());

        let (after, applied) = rolled.advance_preimage(root(), 4, reveal(4))?;
        anyhow::ensure!(!applied.advanced());
        anyhow::ensure!(after.owed() == rolled.owed());
        Ok(())
    }

    /// Re-sending the epoch's root voucher is free and MUST NOT reset the
    /// frontier — every stream emits it before its own first reveal of an
    /// epoch, so a reset here would silently discard proved chunks.
    #[test]
    fn re_asserting_the_same_root_holds_the_frontier() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        state.apply_voucher(
            &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
            &domain,
            &store,
        )?;
        let (state_at_6, _) = state.advance_preimage(root(), 6, reveal(6))?;

        // A later voucher on the SAME epoch (a partial-chunk settlement, say).
        let same_epoch =
            build_metering(signer.address(), 1_500, 0, root(), PRICE).sign(&signer, &domain)?;
        let (after, _) = state_at_6.advance_presigned(&same_epoch)?;
        anyhow::ensure!(after.chain().verified_index == 6, "frontier must survive");
        anyhow::ensure!(after.chain().tip == reveal(6));
        anyhow::ensure!(after.owed() == U256::from(1_500 + 6 * PRICE));
        Ok(())
    }

    /// The full-depth case the walk bound exists for: a payer that abandons a
    /// stream mid-chain leaves the node holding a claim worth 255 chunks over
    /// the anchor, and every one of them is provable.
    #[test]
    fn a_full_depth_chain_is_claimable() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        state.apply_voucher(
            &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
            &domain,
            &store,
        )?;
        let (deep, applied) = state.advance_preimage(
            root(),
            crate::chain::MAX_CHAIN_LENGTH,
            reveal(crate::chain::MAX_CHAIN_LENGTH),
        )?;
        anyhow::ensure!(applied.amount_delta() == U256::from(255 * PRICE));
        anyhow::ensure!(deep.owed() == U256::from(1_000 + 255 * PRICE));
        let claim = deep
            .strongest_claim()
            .ok_or_else(|| anyhow::anyhow!("expected a claim"))?;
        anyhow::ensure!(claim.value() == deep.owed());
        anyhow::ensure!(
            claim.bytes_value() == U256::from(255u64) * U256::from(crate::chain::CHUNK_BYTES)
        );
        Ok(())
    }

    /// A lane that has never accepted a voucher holds no claim and is owed
    /// nothing — there is no signature to submit, whatever reveals arrive.
    #[test]
    fn a_lane_with_no_voucher_holds_no_claim() {
        let (_, state, _, _) = fixture();
        assert!(state.strongest_claim().is_none());
        assert_eq!(state.owed(), U256::ZERO);
    }
}
