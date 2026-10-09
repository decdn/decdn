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
//! [`PoolStateStore`] and records the post-acceptance state to it before
//! advancing in-memory fields or returning `Ok`; the store makes that record
//! durable on its own flush cadence — see
//! [ADR 003 §Off-chain voucher state persistence](https://github.com/decdn/decdn/blob/main/adr/003-payments.md)
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
/// **Field invariant (#527):** the `last_*` fields MUST only be advanced through
/// this type's validated advance methods ([`LaneState::apply_voucher`], or a
/// pure successor from [`LaneState::stage_voucher`] /
/// [`LaneState::advance_presigned`] that the caller records and swaps in) or
/// hydrated from a [`PoolStateStore`] (the trusted on-disk path). Direct field
/// assignment from outside this crate would bypass the voucher-replay guard from
/// ADR 003 §Off-chain voucher state persistence, so the three replay-critical
/// fields are **private** and reachable only through the getters
/// ([`Self::last_amount`] et al.) and those trusted writers; the cross-crate
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
    /// The cumulative spending cap the node holds (token base units): the
    /// presented capability's cap, clamped to the signer's on-chain registration
    /// once the node reads one ([`Self::clamp_to_registration`]). Vouchers MUST
    /// NOT exceed this value.
    pub cap: U256,
    /// The capability expiry the node holds (Unix seconds), clamped like
    /// [`Self::cap`]. The node handler holds the clock and refuses vouchers at or
    /// past this; `0` means "unknown / not tracked".
    pub expiry: u64,
    /// The observed on-chain capability expiry for this lane's signer
    /// (`authorized[pool_id][signer].expiry`), in Unix seconds; `0` means
    /// "unknown / not yet registered". The seller redeemer reads it to skip a
    /// `CapabilityReg` when the registration is already known and still live.
    /// Not replay-critical (it gates no amount/bytes monotonicity), so it is
    /// `pub` like `cap`/`expiry` rather than a private `last_*` field.
    pub registered_until: u64,
    /// The owner's EIP-712 signature over the presented `Capability` (`r‖s‖v`,
    /// exactly 65 bytes), which the seller redeemer submits as the `ownerSig` of
    /// a `PaymentPool.redeemMany` `CapabilityReg` to register the signer on its
    /// first on-chain redemption (ADR 003 §Capability delegation). `None` until
    /// the seller intake path verifies a grant against the pool owner and sets
    /// it. Kept ON the lane record — not a side table — so it is written in the
    /// same durable transaction as the voucher frontier it backs and can never
    /// be durable-out-of-step with it. Set by trusted node-side writers (the
    /// intake path and the on-disk decoder) like `cap`/`expiry`/`registered_until`,
    /// not replay-critical, so it is `pub` rather than a private `last_*` field.
    pub owner_sig: Option<[u8; 65]>,
    /// The lane's paid cumulative: the on-chain `newPaidCumulative` of the most
    /// recent `PoolRedeemed` this node observed for it (`U256::ZERO` until the
    /// first redemption lands). The seller redeemer subtracts it from
    /// [`Self::owed`] to get the unredeemed value it still has to cash, so a lane
    /// already redeemed to its owed amount drops out of the plan instead of being
    /// re-submitted for a silent on-chain no-op (#2052). Monotone on-chain, so it
    /// only ever advances.
    ///
    /// Kept ON the lane record — not a side table or a volatile cache — so it is
    /// written in the same durable transaction as the voucher frontier it settles
    /// against and survives a restart: without it a rebooted node forgets every
    /// redemption behind its log-poller cursor and re-batches those lanes forever.
    /// Set by the trusted node-side watcher (`PoolRedeemed`) and the on-disk
    /// decoder, like `owner_sig`/`registered_until`; not replay-critical (it gates
    /// redemption planning, not voucher acceptance), so it is `pub` rather than a
    /// private `last_*` field.
    pub paid_cumulative: U256,
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
    /// Hydration seeds `registered_until` to `0` (unknown), `owner_sig` to
    /// `None`, and `paid_cumulative` to `U256::ZERO` regardless of caller-supplied
    /// `expiry`; the trusted on-disk decoder (`StoredLaneState::into_state`)
    /// assigns the persisted values on the returned `Self` after construction, the
    /// seller intake path sets `owner_sig` on the lane it registers, and the
    /// `PoolRedeemed` watcher advances `paid_cumulative`.
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
    ) -> Self {
        Self {
            pool_id,
            signer,
            provider,
            cap,
            expiry,
            registered_until: 0,
            owner_sig: None,
            paid_cumulative: U256::ZERO,
            last_amount,
            last_bytes_delivered,
            last_signature,
            chain,
        }
    }

    /// Hold the lower of this lane's terms and its signer's on-chain registration
    /// (ADR 003 §Capability delegation). On-chain registration is write-once per
    /// `(pool, signer)`, so a voucher past the registered `cap` or `expiry`
    /// redeems to nothing whatever capability the client presented. `cap` and
    /// `expiry` are the registered `authorized[pool_id][signer]` terms; the lane
    /// keeps `min(held, registered)` for each and records the registered expiry
    /// as `registered_until`. Returns whether any field changed.
    ///
    /// The chain reads a registered `expiry` of `0` as already expired, while a
    /// lane `expiry` of `0` means "not tracked". The clamp therefore holds such a
    /// registration as expiry `1`, a moment that is always past.
    ///
    /// A clamped lane holds terms its `owner_sig` does not cover. That is sound:
    /// the clamp runs only for a registered signer, and on-chain registration
    /// returns before it checks the signature of a registered signer.
    pub fn clamp_to_registration(&mut self, cap: u64, expiry: u64) -> bool {
        let before = (self.cap, self.expiry, self.registered_until);
        self.cap = self.cap.min(U256::from(cap));
        let held_expiry = expiry.max(1);
        self.expiry = if self.expiry == 0 {
            held_expiry
        } else {
            self.expiry.min(held_expiry)
        };
        self.registered_until = self.registered_until.max(expiry);
        before != (self.cap, self.expiry, self.registered_until)
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
        self.live_claim().map_or(U256::ZERO, |c| c.bytes_value())
    }

    /// What this lane is **owed**: the value of its strongest claim, or zero if
    /// it holds none.
    ///
    /// Counting only the signed cumulative would understate a lane by up to one
    /// whole chain, which is what `M` and the redemption floor are sized
    /// against (ADR 003 §Tracking owed vs. paid).
    #[must_use]
    pub fn owed(&self) -> U256 {
        self.live_claim().map_or(U256::ZERO, |c| c.value())
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
    /// Two narrow things happen here, and nothing else:
    ///
    /// - A lane metering **nothing** adopts the incoming root and price.
    /// - A lane holding **no signature yet** adopts this voucher as its anchor,
    ///   with the root and price that voucher carries.
    ///
    /// That last one is what makes the chain redeemable at all. A claim is a
    /// signed anchor extended by a frontier, and the contract rebuilds the
    /// EIP-712 digest from BOTH halves — so the chain fields and the signature
    /// have to come from the same voucher. Installing a root beside an older
    /// voucher's signature yields a claim that recovers the wrong signer and
    /// reverts `InvalidVoucherSignature` at redemption.
    ///
    /// That pairing is also why a re-asserting voucher CANNOT restate the price.
    /// The price is half the claim — redemption resolves `amount + index ×
    /// chunk_price` and rebuilds the EIP-712 digest from the price it is handed —
    /// so moving it under a signature that covered the old one yields a claim
    /// that recovers the wrong signer, and `redeemMany` reverts
    /// `InvalidVoucherSignature` for every lane in the batch, not just this one.
    /// An already-satisfied voucher pays for nothing, so it has no authority to
    /// reprice the frontier the lane already proved: the lane keeps metering at
    /// the price its own anchor was signed at. A payer that wants a new price
    /// rolls, and a rollover advances the money and goes through
    /// [`Self::advance_presigned`], where the price and the signature that covers
    /// it are adopted together.
    ///
    /// That pairing is what bounds this method. It adopts a **new** root only
    /// when the incoming voucher re-asserts the lane's own watermark exactly, so
    /// taking it as the anchor regresses nothing, and only when the live chain
    /// has proved **nothing**: a chain at index 0 is worth exactly its anchor,
    /// which this voucher's own signature already covers, so retiring it strands
    /// no value. A chain with reveals under it is worth more than its anchor, and
    /// a voucher that did not pay for that difference has no authority to end it.
    ///
    /// A genuine rollover always advances the money — its `amount` folds the
    /// retired chain's frontier in — so it goes through
    /// [`Self::advance_presigned`] instead, where the fold and the retirement are
    /// decided together.
    ///
    /// Returns `None` when nothing changed, so the caller can skip a needless
    /// store write.
    #[must_use]
    pub fn adopt_chain(&self, signed: &SignedVoucher) -> Option<Self> {
        let chain_root = signed.voucher.chain_root;
        if chain_root.is_zero() {
            return None;
        }
        let mut next = self.clone();
        if self.chain.chain_root == chain_root {
            // Same epoch re-asserted. The only thing that can change is the
            // anchor, and only on a lane that holds no signature yet — the first
            // such voucher becomes the anchor, bringing its own price with it.
            // A lane that already has a signature takes nothing from this
            // voucher: adopting its price alone would pair a price with a
            // signature that never covered it (see above).
            if self.last_signature.is_some() {
                return None;
            }
            next.chain.chunk_price = signed.voucher.chunk_price;
            next.last_amount = signed.voucher.amount;
            next.last_bytes_delivered = signed.voucher.bytes_delivered;
            next.last_signature = Some(signed.signature.as_bytes());
            return Some(next);
        }

        // A NEW root. This is not a corner case: a lane whose previous transfer
        // closed on a rollover sits at index 0 under the retired root, and the
        // next transfer's opening voucher re-asserts the same cumulative while
        // naming a fresh one. Refusing would leave the lane metering a root the
        // payer has abandoned, and every reveal that followed would fold nothing.
        if self.chain.verified_index > 0 {
            return None;
        }
        // The root and the signature that commits it are adopted together or not
        // at all, so the voucher must stand exactly at the lane's watermark.
        // Anything else is a stale voucher, and taking its anchor would walk the
        // watermark backwards.
        if signed.voucher.amount != self.last_amount
            || signed.voucher.bytes_delivered != self.last_bytes_delivered
        {
            return None;
        }
        next.chain = LaneChain::opened(chain_root, signed.voucher.chunk_price);
        next.last_signature = Some(signed.signature.as_bytes());
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
    /// in `index − verified` steps; [`PoolError::CapExceeded`] when the reveal
    /// would push the claim past the capability's cap (both raised by
    /// [`Self::advance_preimage_verified`], which this delegates to).
    pub fn advance_preimage(
        &self,
        root: B256,
        index: u8,
        preimage: B256,
    ) -> Result<(Self, PreimageApplied), PoolError> {
        // The single-lock convenience form: snapshot the frontier, hash forward,
        // and apply, all against the same `self`. The node's serve path instead
        // splits these so the keccak walk runs OUTSIDE the per-lane lock (issue
        // #1792 item 5, [`Self::preimage_frontier`] /
        // [`Self::advance_preimage_verified`]); this is what the incentive-crate
        // tests exercise and what any caller without a lock to shed still uses.
        let walked = self.preimage_frontier(root, index);
        let walked_ok = match walked {
            Some((verified_index, tip)) => {
                crate::chain::verify_forward(preimage, index - verified_index, tip)
            }
            // No walk is needed (folds nothing); `advance_preimage_verified`
            // returns the covered outcome before it consults `walked_ok`.
            None => false,
        };
        self.advance_preimage_verified(root, index, preimage, walked, walked_ok)
    }

    /// The frontier a reveal at `index` on epoch `root` must hash forward to, or
    /// `None` when the reveal folds nothing — the root is not the one this lane
    /// meters against, or `index` sits at or below the tracked frontier.
    ///
    /// The seller-side optimistic-walk snapshot (issue #1792 item 5): read this
    /// under the per-lane lock (it is O(1) and hashes nothing), run
    /// [`crate::chain::verify_forward`] OUTSIDE the lock against the returned
    /// `(verified_index, tip)`, then re-lock and commit through
    /// [`Self::advance_preimage_verified`]. This keeps the up-to-255-keccak walk
    /// off the lock a payer can otherwise stretch with sparse indices — the same
    /// discipline the buyer half keeps off its issuance lock.
    #[must_use]
    pub fn preimage_frontier(&self, root: B256, index: u8) -> Option<(u8, B256)> {
        let target = self.chain_slot(root)?;
        (index > target.verified_index).then_some((target.verified_index, target.tip))
    }

    /// Fold a preimage whose forward walk the caller ALREADY ran (off the lane
    /// lock) into the lane, returning the advanced successor and what it added.
    ///
    /// `walked` is the `(verified_index, tip)` the caller hashed against —
    /// [`Self::preimage_frontier`]'s return — and `walked_ok` is what
    /// [`crate::chain::verify_forward`] answered for it. The caller's result is
    /// trusted ONLY while the lane's live frontier still equals `walked`: a
    /// concurrent same-lane sibling that advanced the chain in the gap moved the
    /// frontier, so this re-hashes against the live tip under the lock (the rare
    /// race). A reveal a sibling already covered folds nothing, exactly as
    /// [`Self::advance_preimage`]'s at-or-below-frontier case does. Every other
    /// check — the tracked-root gate and the spending-cap gate — is O(1) and
    /// stays under the lock.
    ///
    /// # Errors
    ///
    /// [`PoolError::BadPreimage`] when the value does not reach the live tip in
    /// `index − verified` steps; [`PoolError::CapExceeded`] when the reveal would
    /// push the claim past the capability's cap.
    pub fn advance_preimage_verified(
        &self,
        root: B256,
        index: u8,
        preimage: B256,
        walked: Option<(u8, B256)>,
        walked_ok: bool,
    ) -> Result<(Self, PreimageApplied), PoolError> {
        let Some(target) = self.chain_slot(root) else {
            return Ok((self.clone(), PreimageApplied::ZERO));
        };
        if index <= target.verified_index {
            return Ok((self.clone(), PreimageApplied::ZERO));
        }
        // Trust the off-lock walk only if the frontier it hashed against is still
        // current; otherwise (a sibling advanced the lane, or the caller walked
        // nothing) re-hash against the live tip here, under the lock.
        let verified = match walked {
            Some((wv, wtip)) if wv == target.verified_index && wtip == target.tip => walked_ok,
            _ => crate::chain::verify_forward(preimage, index - target.verified_index, target.tip),
        };
        if !verified {
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
        next.chain.verified_index = index;
        next.chain.tip = preimage;
        // The value this reveal advances the lane's claim to — the signed anchor
        // plus the frontier it just proved.
        let claimed = self.last_amount.saturating_add(next.chain.accrued());

        // The capability's spending cap binds the CLAIM, not the signature, so a
        // reveal answers for it exactly as a voucher does
        // ([`Self::advance_presigned`]). A chain extends the claim without a new
        // signature, so a cap enforced on the voucher axis alone is one the chain
        // walks straight past — the node would go on crediting deliveries against
        // a claim the on-chain `redeem` will not pay, and eat the difference.
        //
        // Refusing here is also what keeps the payer's recovery working: the reason
        // maps to `SpendingCapExhausted`, which is watermark-gated, so the reject
        // carries the bundle the payer needs to raise the cap and resume — the same
        // route an over-cap voucher already takes.
        if claimed > self.cap {
            return Err(PoolError::CapExceeded {
                cap: self.cap,
                got: claimed,
            });
        }
        Ok((next, applied))
    }

    /// The tracked epoch `root` names, if it is the one the lane meters against.
    ///
    /// A lane tracks exactly one chain. A reveal naming any other root is real
    /// but worthless: the epoch it extends was superseded by a signature that
    /// folded a frontier at least as deep, which is the only way a chain is ever
    /// retired here (a rollover that folded LESS is refused outright — see
    /// [`Self::advance_presigned`]).
    ///
    /// A zero `root` never matches: it is the sealed sentinel, and nothing
    /// hashes to zero, so no reveal can extend it.
    fn chain_slot(&self, root: B256) -> Option<LaneChain> {
        (!root.is_zero() && root == self.chain.chain_root).then_some(self.chain)
    }

    /// Validate `signed` against this lane's invariants and, on success, record
    /// the post-acceptance state to `store` before advancing the in-memory
    /// `last_*` fields.
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
        // without recording; the record-then-swap here is the commit point — the
        // store takes the row into its working set and flushes it on its own
        // cadence. Do NOT swap before the `?` on `record`: that's the literal
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
    /// [`PoolError::AmountRegression`], [`PoolError::BytesRegression`],
    /// [`PoolError::CapExceeded`], or [`PoolError::UnderFold`] — the same
    /// watermark-dependent taxonomy as [`Self::stage_voucher`], minus the
    /// signature and pool/provider checks.
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
            // At exactly the signed anchor, a voucher that opens another chain
            // has not been settled by any sibling: a sibling that re-sends the
            // anchor names the live root. With reveals proved on top of the
            // anchor, it is a rollover that folds none of them. A payer process
            // that sent reveals and exited before it persisted them resumes
            // here, so it gets the rejection that carries the fold it owes.
            // With nothing proved, the live claim is the anchor, and the node
            // may adopt the new root (see `adopt_chain`).
            //
            // A sealed voucher opens no chain. At the anchor it is a sibling's
            // closing voucher, signed before a sibling opened the live chain at
            // the same amount and delivered after it, so it stays an ordering
            // regression.
            if signed.voucher.amount == self.last_amount
                && !signed.voucher.chain_root.is_zero()
                && signed.voucher.chain_root != self.chain.chain_root
                && let Some(live) = self.live_claim()
                && live.value() > signed.voucher.amount
            {
                return Err(PoolError::UnderFold {
                    axis: FoldAxis::Amount,
                    owed: live.value(),
                    got: signed.voucher.amount,
                });
            }
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

        // A voucher carrying a DIFFERENT root retires the live epoch, and the
        // rule that makes that safe is that its `amount` and `bytes_delivered`
        // must FOLD the frontier the retired chain proved. One that folds less
        // is refused here, before anything is adopted: it would sign for fewer
        // chunks than the node holds preimages for, and adopting the new root
        // would discard the difference.
        //
        // Refusing rather than salvaging is deliberate. Within one payer process
        // it cannot happen: issuance is serialized under the payer's own lock,
        // and a voucher that folds must also roll, so the folded amount covers
        // the frontier by construction. A payer reaches it by resuming from a
        // watermark that trails the node's frontier — a new process after one
        // that sent reveals and exited before it persisted them. The reason is
        // watermark-gated, so the rejection carries the bundle that states the
        // fold owed, the payer folds it and resumes, and the lane's claim is
        // exactly as strong afterwards as before.
        //
        // A voucher that repeats the SAME root is not a rollover at all (a
        // re-send, or a mid-epoch amount bump), and MUST leave the frontier where
        // it is; the new epoch starts at index 0 with the root as its own tip, so
        // a claim at exactly the new `amount` walks nothing.
        if signed.voucher.chain_root == self.chain.chain_root {
            // Same epoch: the price is re-asserted by every voucher that names
            // the root, and the handler has already refused a price that is not
            // this node's quote, so adopting it keeps the two in step.
            next.chain.chunk_price = signed.voucher.chunk_price;
        } else {
            // The fold binds both axes (ADR 003 §Rollover): a voucher that pays
            // the whole claim but signs fewer bytes than the frontier proved
            // would still retire those chunks from the lane's byte claim.
            if let Some(live) = self.live_claim() {
                if live.value() > signed.voucher.amount {
                    return Err(PoolError::UnderFold {
                        axis: FoldAxis::Amount,
                        owed: live.value(),
                        got: signed.voucher.amount,
                    });
                }
                if live.bytes_value() > signed.voucher.bytes_delivered {
                    return Err(PoolError::UnderFold {
                        axis: FoldAxis::Bytes,
                        owed: live.bytes_value(),
                        got: signed.voucher.bytes_delivered,
                    });
                }
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
    /// The reveal advanced nothing. Two cases reach it and callers treat them
    /// identically, because the answer is the same in both: the reveal was at or
    /// below the tracked frontier (already covered, and nothing was hashed to
    /// find that out), or it named an epoch the lane no longer tracks (real, but
    /// superseded by a signature that folded a frontier at least as deep).
    pub const ZERO: Self = Self {
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

/// The axis of a lane claim a rollover voucher has to fold
/// ([`PoolError::UnderFold`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldAxis {
    /// The cumulative `amount`.
    Amount,
    /// The cumulative `bytes_delivered`.
    Bytes,
}

impl std::fmt::Display for FoldAxis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Amount => "amount",
            Self::Bytes => "bytes_delivered",
        })
    }
}

/// Failure modes for [`LaneState::apply_voucher`].
///
/// Most in-memory variants map to an on-chain `PaymentPool.redeem` revert;
/// off-chain we surface them before they cost gas. The ordering and fold
/// guards ([`PoolError::AmountRegression`], [`PoolError::BytesRegression`],
/// [`PoolError::UnderFold`]) have no revert — redemption is cumulative — and
/// keep the node's own ledger consistent. [`PoolError::Store`] is the
/// off-chain-only variant for persistent-store failures (issue #527).
///
/// `PartialEq`/`Eq` are intentionally not derived: [`StoreError::Io`] wraps
/// `std::io::Error`, which is not `PartialEq`. Tests pattern-match on variants
/// via the `matches!` macro instead of comparing for equality.
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    /// `voucher.pool_id` does not match the pool this lane draws from — also
    /// the "unknown pool" case surfaced by the caller.
    #[error("voucher pool_id {got} does not match expected {expected}")]
    WrongPool {
        /// The pool this lane draws from.
        expected: B256,
        /// The pool the voucher named.
        got: B256,
    },
    /// `voucher.provider` names a node other than this one — a capability
    /// voucher scoped to one provider redeemed against another.
    #[error("voucher provider {got} does not match expected {expected}")]
    WrongProvider {
        /// This node's provider address.
        expected: Address,
        /// The provider the voucher named.
        got: Address,
    },
    /// Cumulative amount is at or below the lane's signed watermark — a stale
    /// or replayed voucher, or one a concurrent same-lane sibling already
    /// settled. `amount` is the sole ordering key (there is no nonce).
    #[error("voucher amount {got} not greater than last accepted {last}")]
    AmountRegression {
        /// The signed watermark the voucher's cumulative amount had to beat.
        last: U256,
        /// The cumulative amount it carried.
        got: U256,
    },
    /// A voucher under a different `chain_root` retires the live epoch but
    /// does not fold the frontier the live chain proved: its `amount` or its
    /// `bytes_delivered` falls short of the signed anchor plus `verified_index`
    /// chunks (ADR 003 §Chain length and rollover). It is at or above the
    /// signed watermark under a root no sibling names — at exactly the anchor,
    /// a root that opens a chain, never a sealed one — so no sibling settled
    /// it; the payer under-signs chunks the node holds preimages for. The node
    /// refuses it and sends the resume bundle that states the fold the payer
    /// owes. A payer short only on `bytes_delivered` cannot fold from the
    /// bundle, because the bundle does not advance its amount.
    #[error("voucher {axis} {got} does not fold the live claim {owed}")]
    UnderFold {
        /// The axis that falls short. The amount is checked first.
        axis: FoldAxis,
        /// The lane's live claim on `axis`: signed anchor plus the proved
        /// frontier.
        owed: U256,
        /// The cumulative figure the voucher carried on `axis`.
        got: U256,
    },
    /// Cumulative bytes delivered went down.
    #[error("voucher bytes_delivered {got} less than last accepted {last}")]
    BytesRegression {
        /// Last accepted cumulative bytes.
        last: U256,
        /// The lower figure the voucher carried.
        got: U256,
    },
    /// Voucher amount exceeds the capability's spending cap — the contract
    /// would revert at redemption.
    #[error("voucher amount {got} exceeds capability cap {cap}")]
    CapExceeded {
        /// The capability's spending cap.
        cap: U256,
        /// The voucher amount that exceeds it.
        got: U256,
    },
    /// A released hash-chain preimage does not reach the tracked tip in
    /// `index − verified` steps (ADR 003 §Concurrent Streams, Rule 2).
    ///
    /// Terminal, and deliberately **not** watermark-gated on the wire: a
    /// preimage carries no signature of its own, so a payment watermark cannot
    /// repair a hash-chain mismatch. On-chain the same mismatch reverts
    /// `BadPreimage`, which is caller error rather than transient state.
    #[error("released preimage at index {index} does not reach the tip verified at {verified}")]
    BadPreimage {
        /// Chain index of the released preimage.
        index: u8,
        /// Index of the tip the lane has verified to.
        verified: u8,
    },
    /// Signature is malformed or signed by the wrong address.
    #[error(transparent)]
    Signature(#[from] VoucherError),
    /// Lane-store write failed; in-memory state is unchanged. See [`StoreError`]
    /// for the underlying cause. This is surfaced to the caller so the client
    /// resends the same voucher instead of proceeding on state the store never
    /// took (#527, ADR 003 §Off-chain voucher state persistence).
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
mod tests;
