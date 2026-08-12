//! Per-channel voucher state tracking.
//!
//! As a node receives vouchers from a client, it must keep the latest one and
//! reject any that would lower the cumulative `amount`, `bytes_delivered`, or
//! `nonce` — the on-chain `closeChannel` / `disputeChannel` invariants from
//! ADR 003 §Fee Routing on Disputed Closes apply equally off-chain (a node
//! that retains a stale voucher just under-claims at settlement).
//!
//! In-memory state alone is insufficient: without persistence a node restart
//! resets `last_nonce` to zero and a client can resubmit a previously-accepted
//! voucher (issue #527). [`ChannelState::apply_voucher`] therefore requires a
//! [`ChannelStateStore`] and writes the post-acceptance state durably before
//! advancing in-memory fields or returning `Ok` — see
//! [ADR 003 §Off-chain voucher state persistence](../../../adr/003-payments.md)
//! and [`crate::store`]. The contract-level open / close / dispute / settle
//! calls remain out of scope here and are tracked in #327.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};

use crate::store::{ChannelStateStore, StoreError};
use crate::voucher::{SignedVoucher, VoucherError};

/// Identifier for a payment channel — the on-chain `channelId`, computed by
/// `keccak256(client, provider, channelNonce)` per ADR 003.
pub type ChannelId = B256;

/// Mutable per-channel state held by the delivering node.
///
/// Tracks the latest accepted voucher (`last_*` fields) so subsequent
/// [`ChannelState::apply_voucher`] calls can enforce monotonicity. `deposit`
/// is the on-chain escrowed amount — vouchers exceeding it are invalid
/// because `closeChannel` would itself revert (ADR 003 invariant 1).
///
/// **Field invariant (#527):** the `last_*` fields MUST only be advanced
/// through [`ChannelState::apply_voucher`] (the validated, persisted-commit
/// path) or hydrated from a [`crate::ChannelStateStore`] (the trusted on-disk
/// path). Direct field assignment from outside this crate would bypass the
/// voucher-replay guard from ADR 003 §Off-chain voucher state persistence, so
/// the four replay-critical fields are **private** and reachable only through
/// the getters ([`Self::last_amount`] et al.) and the two trusted writers; the
/// cross-crate hydration path (`decdn-node` reading `channels.redb`) goes
/// through [`Self::hydrate`] rather than a struct literal (#751). This makes the
/// invariant compiler-enforced rather than doc-enforced.
///
/// The remaining fields stay `pub` deliberately, and the asymmetry is
/// intentional: `channel_id`/`client`/`voucher_signer`/`token` are immutable
/// identity set once at construction; `deposit` is raised by on-chain top-ups
/// ([`crate::ChannelState`] consumers via `ChannelOpened`/`ChannelToppedUp`) and
/// `expires_at` by the lifecycle watcher — both mutated only by trusted node-side
/// writers under the same clone-record-swap discipline. They are not
/// replay-critical (they don't gate the nonce/amount monotonicity the #527
/// guard protects), so they don't need the private treatment the `last_*` fields
/// do. A `pub` write that *lowered* `deposit` could retroactively break the
/// `amount <= deposit` check, but no such writer exists (top-ups only raise it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelState {
    /// Channel identifier (matches the on-chain `channelId`).
    pub channel_id: ChannelId,
    /// The Ethereum address that funded the channel — put up the deposit and
    /// receives the refund. Also what the ADR-011 blacklist gates check.
    pub client: Address,
    /// The address whose EIP-712 signature authorizes vouchers on this channel.
    ///
    /// Pinned on-chain at `openChannel` and never mutable. Distinct from
    /// [`Self::client`], which is the *funder* — the address that put up the
    /// deposit, receives the refund, and is what the ADR-011 blacklist gates
    /// check. Do not conflate the two: moving a compliance gate onto this field
    /// would let a blacklisted funder serve traffic behind a throwaway key.
    pub voucher_signer: Address,
    /// `ERC-20` token bound by this channel (`USDC` is the only token
    /// supported by `PaymentChannel`).
    pub token: Address,
    /// On-chain deposited amount in token base units. Vouchers MUST NOT
    /// exceed this value. Raised by top-ups; never lowered.
    pub deposit: U256,
    /// Cumulative amount of the most-recently-accepted voucher
    /// (token base units). `U256::ZERO` until the first voucher is applied.
    /// Private (#527/#751) — read via [`Self::last_amount`].
    last_amount: U256,
    /// Sequence number of the most-recently-accepted voucher. `U256::ZERO`
    /// before any voucher is applied — matches the on-chain
    /// `claimedNonce == 0` sentinel from ADR 003 §Voucher Nonce Convention.
    /// Private (#527/#751) — read via [`Self::last_nonce`].
    last_nonce: U256,
    /// Cumulative bytes delivered as of the most-recently-accepted voucher.
    /// Private (#527/#751) — read via [`Self::last_bytes_delivered`].
    last_bytes_delivered: U256,
    /// Signature (`r‖s‖v`, exactly 65 bytes) on the most-recently-accepted
    /// voucher — the `signature` argument the seller path submits to the
    /// on-chain `closeChannel` / `withdraw` (#327). `None` until the first
    /// voucher is applied (and for channels hydrated from a pre-signature store
    /// schema); `Some` is always exactly 65 bytes, so "empty or exactly 65
    /// bytes" is unrepresentable-when-wrong (#751). Same `r‖s‖v` encoding as the
    /// [`crate::client_bridge`] wire form. Private — read via
    /// [`Self::last_signature`].
    last_signature: Option<[u8; 65]>,
    /// On-chain channel expiry (Unix seconds), from the `ChannelOpened` event.
    /// `0` means "unknown / not tracked" (channels constructed by [`Self::new`]
    /// without a chain source, and records hydrated from a pre-expiry store
    /// schema) and is treated as never-expiring. After expiry the contract
    /// reverts `withdraw`/`closeChannel` and the client may `reclaimExpired`,
    /// so the seller path uses this to close (and stop serving) beforehand
    /// (#327). Set as a field — not advanced through [`Self::apply_voucher`].
    pub expires_at: u64,
    /// `true` once the node has signed a cooperative-close waiver for this
    /// channel (ADR 003 §Cooperative close). The node commits to settling at the
    /// current `last_*` watermark, so it MUST stop serving the channel: the
    /// seller handler refuses new delivery streams while this is set, protecting
    /// the node from delivering past the amount it waived to. Private — set only
    /// via [`Self::mark_cooperative_close_signed`] (persisted) and read via
    /// [`Self::cooperative_close_signed`]; defaults `false` (including for
    /// records hydrated from a pre-cooperative-close store schema).
    cooperative_close_signed: bool,
}

impl ChannelState {
    /// Construct fresh state for a newly-opened channel. The `last_*` fields
    /// start at zero, matching the on-chain `Channel` struct's defaults.
    ///
    /// `client` (funder) and `voucher_signer` (voucher authority) are both
    /// `Address` and adjacent, so a transposition compiles silently — pass them
    /// in on-chain `Channel` order. For a self-signing channel they are equal.
    #[must_use]
    pub const fn new(
        channel_id: ChannelId,
        client: Address,
        voucher_signer: Address,
        token: Address,
        deposit: U256,
    ) -> Self {
        Self {
            channel_id,
            client,
            voucher_signer,
            token,
            deposit,
            last_amount: U256::ZERO,
            last_nonce: U256::ZERO,
            last_bytes_delivered: U256::ZERO,
            last_signature: None,
            expires_at: 0,
            cooperative_close_signed: false,
        }
    }

    /// Reconstruct channel state from a trusted persistent store (the only
    /// cross-crate path allowed to set the private replay-critical `last_*`
    /// fields, #527/#751). `decdn-node`'s `channels.redb` decoder calls this
    /// instead of a struct literal so the [field invariant](Self) stays
    /// compiler-enforced. `last_signature` is `None` for a channel with no
    /// accepted voucher yet (or a pre-signature store schema) and otherwise the
    /// exact 65-byte `r‖s‖v` signature.
    ///
    /// **Trust boundary.** This is the one constructor that bypasses
    /// `apply_voucher`'s validation, and several arguments share a type
    /// (`deposit`/`last_amount`/`last_nonce`/`last_bytes_delivered` are all
    /// `U256`; `client`/`voucher_signer`/`token` are all `Address`), so a
    /// transposition compiles. Swapping `client` and `voucher_signer` in
    /// particular is silent *and* security-relevant — it moves both the voucher
    /// verification target and the ADR-011 blacklist subject.
    /// It has a single caller — the `channels.redb` decoder — which persists
    /// `client` and `voucher_signer` as separate on-disk segments, so its
    /// record→load round-trip tests (which pin a signer distinct from the
    /// funder) catch a swap of that pair. Do not add callers without the same
    /// coverage (a field-named init struct would be the move if a second one
    /// ever appears).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn hydrate(
        channel_id: ChannelId,
        client: Address,
        voucher_signer: Address,
        token: Address,
        deposit: U256,
        last_amount: U256,
        last_nonce: U256,
        last_bytes_delivered: U256,
        last_signature: Option<[u8; 65]>,
        expires_at: u64,
        cooperative_close_signed: bool,
    ) -> Self {
        Self {
            channel_id,
            client,
            voucher_signer,
            token,
            deposit,
            last_amount,
            last_nonce,
            last_bytes_delivered,
            last_signature,
            expires_at,
            cooperative_close_signed,
        }
    }

    /// Cumulative amount of the most-recently-accepted voucher (`U256::ZERO`
    /// until the first voucher). See the [field invariant](Self).
    #[must_use]
    pub const fn last_amount(&self) -> U256 {
        self.last_amount
    }

    /// Sequence number of the most-recently-accepted voucher (`U256::ZERO`
    /// before any voucher). See the [field invariant](Self).
    #[must_use]
    pub const fn last_nonce(&self) -> U256 {
        self.last_nonce
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

    /// `true` once the node has signed a cooperative-close waiver for this
    /// channel — the seller path MUST stop serving it (ADR 003 §Cooperative
    /// close). See the [field invariant](Self).
    #[must_use]
    pub const fn cooperative_close_signed(&self) -> bool {
        self.cooperative_close_signed
    }

    /// Durably record that a cooperative-close waiver was signed for this
    /// channel, then set the in-memory flag — mirroring [`Self::apply_voucher`]'s
    /// clone-record-swap discipline (#527): persist first, advance in-memory
    /// state only on `Ok`, so a store failure leaves `self` unchanged and the
    /// node has not yet returned a waiver it can't remember. Idempotent — a
    /// channel already flagged short-circuits to `Ok(())` without re-recording,
    /// so a retried `CooperativeCloseRequest` (UDP/QUIC retransmits are expected)
    /// does not trigger a redundant fsync.
    ///
    /// # Errors
    ///
    /// [`ChannelError::Store`] if the persistent write (or fsync) failed; the
    /// in-memory flag is left unchanged so the caller does not send a waiver for
    /// state it never committed.
    pub fn mark_cooperative_close_signed(
        &mut self,
        store: &dyn ChannelStateStore,
    ) -> Result<(), ChannelError> {
        if self.cooperative_close_signed {
            return Ok(());
        }
        let mut next = self.clone();
        next.cooperative_close_signed = true;
        store.record(&next)?;
        *self = next;
        Ok(())
    }

    /// Validate `signed` against this channel's invariants and, on success,
    /// durably persist the post-acceptance state via `store` before advancing
    /// the in-memory `last_*` fields.
    ///
    /// Mirrors the on-chain `closeChannel` + `disputeChannel` checks:
    /// - signature recovers to `self.voucher_signer`
    /// - `voucher.channel_id == self.channel_id`
    /// - `voucher.token == self.token`
    /// - `voucher.nonce > self.last_nonce`
    /// - `voucher.amount >= self.last_amount`
    /// - `voucher.bytes_delivered >= self.last_bytes_delivered`
    /// - `voucher.amount <= self.deposit`
    ///
    /// Ordering of side effects: every check above runs first; if all pass,
    /// the candidate `last_*` tuple is sent to `store.record` and only on
    /// `Ok` is in-memory state advanced. On any check failure — including
    /// store failure — `self` is left unchanged. This makes `Ok(_)` the
    /// protocol-level commit point: `VoucherAck` MUST be sent (and further
    /// bytes delivered) **only after, never before**, this method returns
    /// `Ok` (ADR 003 §Off-chain voucher state persistence, issue #527).
    ///
    /// On success returns a [`VoucherApplied`] reporting how many nonce values
    /// the accepted voucher skipped past `last_nonce + 1`
    /// ([`VoucherApplied::nonce_gap`] / [`VoucherApplied::is_gapped`], #747).
    /// The monotonicity guard only requires nonces to *increase*, so a client
    /// can skip sequence numbers; a non-zero gap is also logged here via
    /// `tracing::warn!` (with the lossless `skipped` count as a `U256`). A gap
    /// never blocks acceptance — vouchers are cumulative in
    /// `amount`/`bytes_delivered`, so settlement is unaffected — but it flags a
    /// dropped voucher (a per-voucher delivery the node never billed for) or a
    /// client that reset/forked its counter (a replay-probe signal). The caller
    /// bumps the `decdn_voucher_nonce_gaps_total` metric once per gapped
    /// voucher via [`VoucherApplied::is_gapped`], keeping `iroh-metrics` out of
    /// this leaf crate.
    ///
    /// # Caller obligations
    ///
    /// **Rate consistency is NOT enforced here (#845).** This method advances
    /// `last_amount`/`last_bytes_delivered` monotonically but does not check
    /// that the per-voucher `amount_delta / bytes_delta` meets the advertised
    /// `rate_per_mb`. The serving node MUST call [`crate::rate::verify_rate`]
    /// (with `rate_per_mb` and a tolerance) *before* accepting a voucher and
    /// delivering the bytes it pays for; `apply_voucher` is intentionally
    /// rate-agnostic so the pricing policy lives at one call site.
    ///
    /// **Off-chain/on-chain divergence on zero-payment byte advance (#864).**
    /// The guards above accept `amount == last_amount` while
    /// `bytes_delivered` advances (the `amount >= last_amount` arm). The
    /// on-chain `PaymentChannel` rejects exactly that shape: settle/dispute
    /// revert with `ByteAdvanceWithoutPayment` when `byteDelta != 0 &&
    /// claimedAmount == withdrawnAmount` (`_requireBytesTrackPayment`). With
    /// `rate_per_mb == 0` — a legal config that `verify_rate` blesses — every
    /// voucher takes this shape, so accepting it off-chain strands the close
    /// path (no on-chain redemption is possible until channel expiry +
    /// `reclaimExpired`). Operators serving for free should be aware that such
    /// channels are unsettleable; this method does not reject the shape so the
    /// off-chain state still mirrors what was signed.
    ///
    /// # Errors
    ///
    /// See [`ChannelError`] for the full taxonomy. A persistent-store
    /// failure surfaces as [`ChannelError::Store`].
    pub fn apply_voucher(
        &mut self,
        signed: &SignedVoucher,
        domain: &Eip712Domain,
        store: &dyn ChannelStateStore,
    ) -> Result<VoucherApplied, ChannelError> {
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
    /// Exposed for **group commit** (#1483): a caller stages several vouchers
    /// against an advancing candidate clone, then makes ONE durable
    /// `store.record` of the final candidate and swaps it into the live channel.
    /// Because vouchers are cumulative — each carries the running `amount` /
    /// `bytes_delivered` / `nonce` — the final staged state supersedes every
    /// intermediate one, so a single record commits the whole batch (one fsync)
    /// with no loss: the batch's highest voucher is exactly what the on-chain
    /// settlement path submits. No per-voucher store method is needed.
    ///
    /// **Durability obligation.** The returned state MUST be persisted
    /// (`store.record`) before it is swapped into a live channel or used to send
    /// `VoucherAck` / deliver further bytes — the #527 replay guard requires
    /// durability before acknowledgement, and staging alone advances nothing
    /// durable. `stage_voucher` takes `&self` (never mutates the caller's state)
    /// precisely so an un-persisted candidate can be discarded on a mid-batch
    /// rejection without touching the committed prefix.
    ///
    /// # Errors
    ///
    /// The same validation taxonomy as [`Self::apply_voucher`] **minus**
    /// [`ChannelError::Store`] — no store is touched here.
    pub fn stage_voucher(
        &self,
        signed: &SignedVoucher,
        domain: &Eip712Domain,
    ) -> Result<(Self, VoucherApplied), ChannelError> {
        if signed.voucher.channel_id != self.channel_id {
            return Err(ChannelError::WrongChannel {
                expected: self.channel_id,
                got: signed.voucher.channel_id,
            });
        }
        if signed.voucher.token != self.token {
            return Err(ChannelError::WrongToken {
                expected: self.token,
                got: signed.voucher.token,
            });
        }
        if signed.voucher.nonce <= self.last_nonce {
            return Err(ChannelError::NonceNotIncreasing {
                last: self.last_nonce,
                got: signed.voucher.nonce,
            });
        }
        if signed.voucher.amount < self.last_amount {
            return Err(ChannelError::AmountDecreasing {
                last: self.last_amount,
                got: signed.voucher.amount,
            });
        }
        if signed.voucher.bytes_delivered < self.last_bytes_delivered {
            return Err(ChannelError::BytesDecreasing {
                last: self.last_bytes_delivered,
                got: signed.voucher.bytes_delivered,
            });
        }
        if signed.voucher.amount > self.deposit {
            return Err(ChannelError::AmountExceedsDeposit {
                deposit: self.deposit,
                got: signed.voucher.amount,
            });
        }
        // Signature check is last among the cheap-fail checks — it's the most
        // expensive in-memory step (ecrecover).
        //
        // The expected signer is the channel's pinned `voucher_signer`, NOT the
        // funder: `openChannel` pins a voucher-signing address that may be a
        // delegate key (it resolves to the funder when opened with a zero
        // signer, which is the self-signing case). The four on-chain settlement
        // paths recover against the same field, so verifying against `client`
        // here would accept vouchers the contract rejects and vice versa.
        //
        // EOA-only off-chain (#845): `verify_signer` recovers a 65-byte EOA
        // signature via `ecrecover`, matching the stance of `probe_sig` and
        // `bind_sig`. The on-chain `PaymentChannel` path also accepts ERC-1271
        // smart-account signatures (via `SignatureChecker`), so this check is
        // fail-closed — a smart-account client is un-servable off-chain. The
        // ERC-1271 off-chain path needs an `isValidSignature` RPC call and is
        // deferred (ADR 024 §Off-Chain ERC-1271 Verification).
        signed
            .verify_signer(self.voucher_signer, domain)
            .map_err(ChannelError::Signature)?;

        let mut next = self.clone();
        next.last_amount = signed.voucher.amount;
        next.last_nonce = signed.voucher.nonce;
        next.last_bytes_delivered = signed.voucher.bytes_delivered;
        // Retain the signature so the seller path can submit this exact
        // voucher to the on-chain `closeChannel` / `withdraw` (#327). Same
        // `r‖s‖v` encoding as the wire form in `client_bridge`;
        // `Signature::as_bytes` is exactly 65 bytes.
        next.last_signature = Some(signed.signature.as_bytes());

        // Nonce-gap detection (#747). The monotonicity guard above rejected
        // `voucher.nonce <= self.last_nonce`, so the step from the prior
        // accepted nonce is at least one and `skipped` (`step - 1`) is the
        // count of skipped values; the first voucher of a channel is measured
        // against the `last_nonce == 0` sentinel, making its expected nonce 1.
        // `saturating_sub` keeps this branch panic- and wrap-free regardless of
        // that guard: alloy/ruint `U256` subtraction wraps silently (no debug
        // panic), so a future guard regression would otherwise turn an
        // underflow into a bogus `u64::MAX` "jump" — saturation degrades it to
        // a harmless `0` (no gap) instead. The exact count is logged as a
        // lossless `U256`; the returned `nonce_gap` narrows it to `u64`
        // (saturating) only for the event-gating metric, where magnitude is
        // not used.
        let skipped = signed
            .voucher
            .nonce
            .saturating_sub(self.last_nonce)
            .saturating_sub(U256::from(1u64));
        let nonce_gap = u64::try_from(skipped).unwrap_or(u64::MAX);
        if nonce_gap > 0 {
            tracing::warn!(
                channel_id = %self.channel_id,
                last_nonce = %self.last_nonce,
                nonce = %signed.voucher.nonce,
                skipped = %skipped,
                "accepted voucher skips nonce values (possible dropped voucher or client counter reset)",
            );
        }

        Ok((next, VoucherApplied { nonce_gap }))
    }
}

/// Outcome of a successful [`ChannelState::apply_voucher`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoucherApplied {
    /// Count of skipped nonce values past `last_nonce + 1`, saturated at
    /// `u64::MAX`. Private so callers reach for [`Self::nonce_gap`] /
    /// [`Self::is_gapped`] rather than feeding the saturated value straight
    /// into a counter's `inc_by` (which a single adversarial voucher could
    /// pin at `u64::MAX`); the metric counts gap *events*, not skipped nonces.
    nonce_gap: u64,
}

impl VoucherApplied {
    /// Number of nonce values the accepted voucher skipped past the expected
    /// `last_nonce + 1` (`voucher.nonce - last_nonce - 1`). `0` for a
    /// contiguous voucher — the normal case. Saturated at `u64::MAX`; the
    /// exact count (lossless even past `u64::MAX`) is in the `apply_voucher`
    /// warn's `skipped` field (#747).
    #[must_use]
    pub const fn nonce_gap(&self) -> u64 {
        self.nonce_gap
    }

    /// Whether the accepted voucher skipped one or more nonce values — the
    /// predicate the node uses to bump `decdn_voucher_nonce_gaps_total` (#747).
    /// A gap never blocks acceptance; it is purely a dropped-voucher /
    /// counter-reset visibility signal.
    #[must_use]
    pub const fn is_gapped(&self) -> bool {
        self.nonce_gap > 0
    }
}

/// Failure modes for [`ChannelState::apply_voucher`].
///
/// Each in-memory variant maps to an on-chain `closeChannel` /
/// `disputeChannel` revert from ADR 003 §Fee Routing on Disputed Closes;
/// off-chain we surface them before they cost gas. [`ChannelError::Store`]
/// is the additional off-chain-only variant for persistent-store failures
/// (issue #527).
///
/// `PartialEq`/`Eq` are intentionally not derived: [`StoreError::Io`] wraps
/// `std::io::Error`, which is not `PartialEq`. Tests pattern-match on
/// variants via the `matches!` macro instead of comparing for equality.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    /// `voucher.channel_id` does not match the channel this state tracks.
    /// Equivalent to attempting to apply a voucher signed for a different
    /// channel — its signature would recover but the contract would reject.
    #[error("voucher channel_id {got} does not match expected {expected}")]
    WrongChannel { expected: ChannelId, got: ChannelId },
    /// `voucher.token` does not match the channel's token. Prevents the
    /// cross-token replay attack from ADR 003 §Replay attack on vouchers.
    #[error("voucher token {got} does not match expected {expected}")]
    WrongToken { expected: Address, got: Address },
    /// Nonce did not strictly increase — would be rejected by
    /// `disputeChannel` (which requires `newNonce > claimedNonce`) and
    /// indicates a replay or out-of-order delivery.
    #[error("voucher nonce {got} not greater than last accepted {last}")]
    NonceNotIncreasing { last: U256, got: U256 },
    /// Cumulative amount went down — invariant 2 from ADR 003.
    #[error("voucher amount {got} less than last accepted {last}")]
    AmountDecreasing { last: U256, got: U256 },
    /// Cumulative bytes delivered went down — invariant 2 from ADR 003.
    #[error("voucher bytes_delivered {got} less than last accepted {last}")]
    BytesDecreasing { last: U256, got: U256 },
    /// Voucher amount exceeds the on-chain deposit — invariant 1 from
    /// ADR 003. The contract would revert at `closeChannel` time.
    #[error("voucher amount {got} exceeds channel deposit {deposit}")]
    AmountExceedsDeposit { deposit: U256, got: U256 },
    /// Signature is malformed or signed by the wrong address.
    #[error(transparent)]
    Signature(#[from] VoucherError),
    /// Persistent-store write (or fsync) failed; in-memory state is
    /// unchanged. See [`StoreError`] for the underlying cause. This is
    /// surfaced to the caller so the client retries instead of receiving a
    /// `VoucherAck` for state that was never durably committed (#527,
    /// ADR 003 §Off-chain voucher state persistence).
    ///
    /// PROTOCOL NOTE: this is the only `ChannelError` variant for which the
    /// client SHOULD retry the same voucher unchanged. The validation
    /// variants (`WrongChannel`, `NonceNotIncreasing`, `AmountDecreasing`,
    /// `BytesDecreasing`, `AmountExceedsDeposit`, `WrongToken`, `Signature`)
    /// mean the voucher is permanently invalid for this channel state and
    /// retrying is a client bug. The cdn/client/v1 wire encoding (#317)
    /// MUST distinguish the two cases — folding them together either causes
    /// silent ack of unpersisted state (retry-on-validation-failure) or
    /// hangs the channel (no-retry-on-`Store`).
    #[error("channel state store failed: {0}")]
    Store(#[from] StoreError),
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed pair up clearly here
mod tests {
    use super::*;
    use crate::store::{ChannelStateStore, MemoryChannelStateStore, StoreError};
    use crate::voucher::{Voucher, voucher_domain};
    use alloy::primitives::{address, b256};
    use alloy::signers::local::PrivateKeySigner;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// `ChannelStateStore` whose `record` always errors. Used to prove the
    /// strict-durability invariant: `apply_voucher` MUST surface the store
    /// failure as `ChannelError::Store(..)` and MUST NOT advance the
    /// in-memory `last_*` fields. The record-call count is exposed so the
    /// test can assert the failure-path is actually reached (and isn't
    /// short-circuited by an earlier validation error).
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

    impl ChannelStateStore for FailingStore {
        fn load_all(&self) -> Result<Vec<ChannelState>, StoreError> {
            Ok(Vec::new())
        }
        fn record(&self, _state: &ChannelState) -> Result<(), StoreError> {
            self.record_calls.fetch_add(1, Ordering::SeqCst);
            Err(StoreError::Io(std::io::Error::other(
                "simulated fsync failure",
            )))
        }
        fn forget(&self, _channel_id: ChannelId) -> Result<(), StoreError> {
            Ok(())
        }
        fn get(&self, _channel_id: ChannelId) -> Result<Option<ChannelState>, StoreError> {
            Ok(None)
        }
    }

    const CHAIN_ID: u64 = 421_614;
    const VERIFYING: Address = address!("0000000000000000000000000000000000001234");
    const TOKEN: Address = address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");

    /// Shared test fixture: a fresh keypair, an empty `ChannelState`, the
    /// voucher EIP-712 domain, and an in-memory `ChannelStateStore`. Each
    /// `apply_voucher` call site threads the store through; cross-test state
    /// isolation comes from each test calling `fixture()` independently.
    fn fixture() -> (
        PrivateKeySigner,
        ChannelState,
        Eip712Domain,
        MemoryChannelStateStore,
    ) {
        let signer = PrivateKeySigner::random();
        let state = ChannelState::new(
            b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"),
            signer.address(),
            signer.address(),
            TOKEN,
            U256::from(10_000_000u64), // 10 USDC deposit
        );
        let domain = voucher_domain(CHAIN_ID, VERIFYING);
        let store = MemoryChannelStateStore::new();
        (signer, state, domain, store)
    }

    fn build(
        channel_id: B256,
        amount: u64,
        nonce: u64,
        bytes_delivered: u64,
        token: Address,
    ) -> Voucher {
        Voucher {
            channel_id,
            amount: U256::from(amount),
            nonce: U256::from(nonce),
            bytes_delivered: U256::from(bytes_delivered),
            token,
        }
    }

    fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
        r.err()
            .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
    }

    #[test]
    fn first_voucher_advances_state() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let signed = build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&signer, &domain)?;

        state.apply_voucher(&signed, &domain, &store)?;
        anyhow::ensure!(state.last_amount == U256::from(1_000u64));
        anyhow::ensure!(state.last_nonce == U256::from(1u64));
        anyhow::ensure!(state.last_bytes_delivered == U256::from(1_048_576u64));
        Ok(())
    }

    #[test]
    fn monotonic_progression_accepted() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        for (amount, nonce, bytes) in [(1_000u64, 1u64, 1_048_576u64), (2_500, 2, 2_621_440)] {
            let signed =
                build(state.channel_id, amount, nonce, bytes, TOKEN).sign(&signer, &domain)?;
            state.apply_voucher(&signed, &domain, &store)?;
        }
        anyhow::ensure!(state.last_nonce == U256::from(2u64));
        Ok(())
    }

    #[test]
    fn contiguous_vouchers_report_zero_gap() -> anyhow::Result<()> {
        // The happy path: a fresh channel's first voucher is nonce 1 and each
        // subsequent voucher increments by exactly one, so `nonce_gap` stays 0
        // and no `tracing::warn!` fires (#747).
        let (signer, mut state, domain, store) = fixture();
        let v1 = build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&signer, &domain)?;
        let applied = state.apply_voucher(&v1, &domain, &store)?;
        anyhow::ensure!(
            applied.nonce_gap() == 0,
            "first voucher at nonce 1 has no gap"
        );

        let v2 = build(state.channel_id, 2_000, 2, 2_097_152, TOKEN).sign(&signer, &domain)?;
        let applied = state.apply_voucher(&v2, &domain, &store)?;
        anyhow::ensure!(applied.nonce_gap() == 0, "contiguous nonce 2 has no gap");
        Ok(())
    }

    #[test]
    fn skipped_nonce_reports_gap_but_still_advances() -> anyhow::Result<()> {
        // last_nonce 1 → voucher nonce 4 skips 2 and 3 (#747). The voucher is
        // still accepted (vouchers are cumulative; settlement is unaffected),
        // but `nonce_gap` reports the two skipped values for operator
        // visibility / the `decdn_voucher_nonce_gaps_total` metric.
        let (signer, mut state, domain, store) = fixture();
        let v1 = build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain, &store)?;

        let v4 = build(state.channel_id, 4_000, 4, 4_194_304, TOKEN).sign(&signer, &domain)?;
        let applied = state.apply_voucher(&v4, &domain, &store)?;
        anyhow::ensure!(applied.nonce_gap() == 2, "nonce 1 → 4 skips 2 values");
        anyhow::ensure!(
            state.last_nonce == U256::from(4u64),
            "gapped voucher still advances"
        );
        Ok(())
    }

    #[test]
    fn first_voucher_above_one_reports_gap() -> anyhow::Result<()> {
        // A first voucher whose nonce is above 1 is a gap relative to the
        // `last_nonce == 0` sentinel (nonces start at 1): nonce 5 means the
        // client's nonces 1–4 were never seen by this node (#747).
        let (signer, mut state, domain, store) = fixture();
        let v5 = build(state.channel_id, 5_000, 5, 5_242_880, TOKEN).sign(&signer, &domain)?;
        let applied = state.apply_voucher(&v5, &domain, &store)?;
        anyhow::ensure!(applied.nonce_gap() == 4, "first voucher nonce 5 skips 1–4");
        Ok(())
    }

    #[test]
    fn wrong_channel_id_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let signed = build(B256::ZERO, 1_000, 1, 1, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain, &store))?;
        anyhow::ensure!(matches!(err, ChannelError::WrongChannel { .. }), "{err:?}");
        anyhow::ensure!(state.last_nonce == U256::ZERO, "state must be unchanged");
        Ok(())
    }

    #[test]
    fn wrong_token_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let signed = build(
            state.channel_id,
            1_000,
            1,
            1,
            address!("0000000000000000000000000000000000000000"),
        )
        .sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain, &store))?;
        anyhow::ensure!(matches!(err, ChannelError::WrongToken { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn equal_nonce_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let v1 = build(state.channel_id, 1_000, 1, 1, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain, &store)?;

        let v2 = build(state.channel_id, 2_000, 1, 2, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&v2, &domain, &store))?;
        anyhow::ensure!(
            matches!(err, ChannelError::NonceNotIncreasing { .. }),
            "{err:?}"
        );
        Ok(())
    }

    #[test]
    fn lower_nonce_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let v1 = build(state.channel_id, 1_000, 5, 1, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain, &store)?;

        let v2 = build(state.channel_id, 2_000, 4, 2, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&v2, &domain, &store))?;
        anyhow::ensure!(
            matches!(err, ChannelError::NonceNotIncreasing { .. }),
            "{err:?}"
        );
        Ok(())
    }

    #[test]
    fn amount_decrease_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let v1 = build(state.channel_id, 5_000, 1, 1, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain, &store)?;

        let v2 = build(state.channel_id, 4_000, 2, 2, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&v2, &domain, &store))?;
        anyhow::ensure!(
            matches!(err, ChannelError::AmountDecreasing { .. }),
            "{err:?}"
        );
        Ok(())
    }

    #[test]
    fn equal_amount_higher_nonce_accepted() -> anyhow::Result<()> {
        // Same amount with strictly higher nonce is legal — e.g. the client
        // re-acks an earlier amount with a fresh nonce after a `VoucherAck`
        // got dropped (ADR 003 voucher format). The contract treats `amount`
        // as non-decreasing, not strictly increasing.
        let (signer, mut state, domain, store) = fixture();
        let v1 = build(state.channel_id, 5_000, 1, 1, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain, &store)?;

        let v2 = build(state.channel_id, 5_000, 2, 2, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v2, &domain, &store)?;
        anyhow::ensure!(state.last_nonce == U256::from(2u64));
        Ok(())
    }

    #[test]
    fn bytes_decrease_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let v1 = build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain, &store)?;

        let v2 = build(state.channel_id, 2_000, 2, 524_288, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&v2, &domain, &store))?;
        anyhow::ensure!(
            matches!(err, ChannelError::BytesDecreasing { .. }),
            "{err:?}"
        );
        Ok(())
    }

    #[test]
    fn amount_exceeds_deposit_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        // deposit is 10_000_000 (10 USDC); attempt 11 USDC.
        let signed = build(state.channel_id, 11_000_000, 1, 1, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain, &store))?;
        anyhow::ensure!(
            matches!(err, ChannelError::AmountExceedsDeposit { .. }),
            "{err:?}"
        );
        Ok(())
    }

    #[test]
    fn wrong_signer_rejected() -> anyhow::Result<()> {
        let (_signer, mut state, domain, store) = fixture();
        // Sign with an unrelated key.
        let interloper = PrivateKeySigner::random();
        let signed = build(state.channel_id, 1_000, 1, 1, TOKEN).sign(&interloper, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain, &store))?;
        anyhow::ensure!(
            matches!(
                err,
                ChannelError::Signature(VoucherError::WrongSigner { .. })
            ),
            "{err:?}"
        );
        Ok(())
    }

    /// A channel funded by one address but whose pinned `voucher_signer` is a
    /// distinct delegate key: the delegate's voucher must be accepted.
    #[test]
    fn voucher_signed_by_delegate_is_accepted() -> anyhow::Result<()> {
        let (funder, base, domain, store) = fixture();
        let delegate = PrivateKeySigner::random();
        let mut state = ChannelState::new(
            base.channel_id,
            funder.address(),
            delegate.address(),
            TOKEN,
            U256::from(10_000_000u64),
        );
        let signed =
            build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&delegate, &domain)?;

        state.apply_voucher(&signed, &domain, &store)?;
        anyhow::ensure!(state.last_nonce == U256::from(1u64));
        Ok(())
    }

    /// With a delegate pinned, the funder is no longer an authorized signer —
    /// and the rejection must name the delegate as `expected`, proving the
    /// verification target actually moved off `client`.
    #[test]
    fn voucher_signed_by_funder_is_rejected_when_a_delegate_is_pinned() -> anyhow::Result<()> {
        let (funder, base, domain, store) = fixture();
        let delegate = PrivateKeySigner::random();
        let mut state = ChannelState::new(
            base.channel_id,
            funder.address(),
            delegate.address(),
            TOKEN,
            U256::from(10_000_000u64),
        );
        let signed = build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&funder, &domain)?;

        let err = err_of(state.apply_voucher(&signed, &domain, &store))?;
        let ChannelError::Signature(VoucherError::WrongSigner {
            expected,
            recovered,
        }) = err
        else {
            anyhow::bail!("expected WrongSigner, got {err:?}");
        };
        anyhow::ensure!(
            expected == delegate.address(),
            "expected signer must be the pinned delegate, not the funder"
        );
        anyhow::ensure!(recovered == funder.address(), "{recovered}");
        anyhow::ensure!(
            state.last_nonce == U256::ZERO,
            "rejection must not advance state"
        );
        Ok(())
    }

    /// `voucher_signer == client` (the on-chain default when `openChannel` is
    /// passed a zero signer): the pre-existing self-signed path is unchanged.
    #[test]
    fn self_signed_channel_still_accepts_the_funder_voucher() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        anyhow::ensure!(state.voucher_signer == state.client);
        let signed = build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&signer, &domain)?;

        state.apply_voucher(&signed, &domain, &store)?;
        anyhow::ensure!(state.last_nonce == U256::from(1u64));
        Ok(())
    }

    #[test]
    fn high_s_voucher_rejected_at_apply() -> anyhow::Result<()> {
        // A high-`s` voucher recovers the correct signer off-chain but is
        // unsettleable on-chain (#836). `apply_voucher` must reject it via the
        // transitive `recover_signer` guard and leave state untouched — this
        // locks the channel-layer path the `voucher::tests` unit test cannot
        // reach.
        let (signer, mut state, domain, store) = fixture();
        let signed = build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&signer, &domain)?;
        let twin = SignedVoucher {
            signature: crate::sig_canon::high_s_twin(&signed.signature),
            ..signed
        };
        let err = err_of(state.apply_voucher(&twin, &domain, &store))?;
        anyhow::ensure!(
            matches!(err, ChannelError::Signature(VoucherError::InvalidSignature)),
            "{err:?}"
        );
        anyhow::ensure!(
            state.last_nonce == U256::ZERO,
            "rejection must not advance state"
        );
        Ok(())
    }

    /// Cover every `ChannelError` variant — `apply_voucher` must leave
    /// state untouched on each rejection path. Bytes for `v1` are `1_000`
    /// so a `bytes_delivered` decrease can be tested without going below
    /// the initial zero floor.
    #[test]
    fn rejected_voucher_does_not_advance_state() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let channel_id = state.channel_id;
        let v1 = build(channel_id, 5_000, 1, 1_000, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain, &store)?;
        let snapshot = state.clone();
        let interloper = PrivateKeySigner::random();

        // Build every rejection-path voucher up front so the closure below
        // borrows `state` exclusively (mutable) without re-borrowing for
        // each variant.
        let cases: [(SignedVoucher, &str); 7] = [
            // WrongChannel
            (
                build(B256::ZERO, 6_000, 2, 2_000, TOKEN).sign(&signer, &domain)?,
                "wrong channel id",
            ),
            // WrongToken
            (
                build(
                    channel_id,
                    6_000,
                    2,
                    2_000,
                    address!("dead000000000000000000000000000000000000"),
                )
                .sign(&signer, &domain)?,
                "wrong token",
            ),
            // NonceNotIncreasing
            (
                build(channel_id, 6_000, 1, 2_000, TOKEN).sign(&signer, &domain)?,
                "stale nonce",
            ),
            // AmountDecreasing
            (
                build(channel_id, 1, 2, 2_000, TOKEN).sign(&signer, &domain)?,
                "amount drop",
            ),
            // BytesDecreasing
            (
                build(channel_id, 6_000, 2, 500, TOKEN).sign(&signer, &domain)?,
                "bytes drop",
            ),
            // AmountExceedsDeposit (deposit is 10_000_000)
            (
                build(channel_id, 11_000_000, 2, 2_000, TOKEN).sign(&signer, &domain)?,
                "amount over deposit",
            ),
            // Signature (wrong signer)
            (
                build(channel_id, 6_000, 2, 2_000, TOKEN).sign(&interloper, &domain)?,
                "wrong signer",
            ),
        ];

        for (voucher, reason) in &cases {
            let _ = state.apply_voucher(voucher, &domain, &store);
            anyhow::ensure!(state == snapshot, "{reason} must not advance state");
        }
        // And: the store row for this channel must still match the v1
        // snapshot — validation checks short-circuit with `return Err(..)`
        // before any `store.record` call (see channel.rs ordering at the
        // top of `apply_voucher`), so a rejected voucher must never have
        // written through. This is the issue #527 regression guard at the
        // in-memory layer: any future rejection path that mistakenly
        // persists state lands here.
        let persisted = store.load_all()?;
        anyhow::ensure!(persisted.len() == 1, "exactly one channel persisted");
        let only = persisted
            .first()
            .ok_or_else(|| anyhow::anyhow!("expected one persisted entry"))?;
        anyhow::ensure!(
            *only == snapshot,
            "rejected voucher path must not overwrite stored channel state",
        );
        Ok(())
    }

    /// **Strict-durability regression (#527).** The protocol-level commit
    /// point is `apply_voucher`'s `Ok(_)` return. If `store.record` fails,
    /// the in-memory `ChannelState` MUST NOT advance — otherwise a future
    /// `VoucherAck` would acknowledge a voucher that was never durably
    /// persisted. This test breaks if anyone reorders the
    /// `store.record(&next)?` and `*self = next` lines, or moves the clone
    /// earlier in a way that creates a window between validation and
    /// commit.
    #[test]
    fn store_failure_leaves_in_memory_state_unchanged() -> anyhow::Result<()> {
        let (signer, mut state, domain, _mem_store) = fixture();
        let snapshot = state.clone();
        let failing = FailingStore::new();

        let signed = build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&signer, &domain)?;
        let err = state
            .apply_voucher(&signed, &domain, &failing)
            .err()
            .ok_or_else(|| anyhow::anyhow!("store failure must surface to caller"))?;
        anyhow::ensure!(
            matches!(err, ChannelError::Store(_)),
            "expected ChannelError::Store, got {err:?}",
        );
        anyhow::ensure!(
            state == snapshot,
            "in-memory state must NOT advance when store.record fails",
        );
        // The failure path must actually have been reached — a future
        // refactor that, say, skipped `record` on some optimisation path
        // would pass the equality check above for the wrong reason.
        anyhow::ensure!(
            failing.record_calls() == 1,
            "expected exactly one `record` call, got {}",
            failing.record_calls(),
        );
        Ok(())
    }

    /// Companion to the above: after a `record` failure, a subsequent
    /// successful apply (against a real store) MUST still work — i.e. the
    /// failure didn't poison the in-memory state for retries. This guards
    /// against a refactor that, e.g., set a "dirty" flag on `self` before
    /// the commit succeeded.
    #[test]
    fn store_failure_does_not_poison_subsequent_retries() -> anyhow::Result<()> {
        let (signer, mut state, domain, store) = fixture();
        let failing = FailingStore::new();
        let signed = build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&signer, &domain)?;

        // First attempt: store fails, state unchanged.
        let _err = state
            .apply_voucher(&signed, &domain, &failing)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected store failure"))?;
        anyhow::ensure!(state.last_nonce == U256::ZERO);

        // Retry against a healthy store with the same voucher — must
        // succeed and advance state normally.
        state.apply_voucher(&signed, &domain, &store)?;
        anyhow::ensure!(state.last_nonce == U256::from(1u64));
        Ok(())
    }

    /// Group-commit primitive (#1483): staging a batch of vouchers against an
    /// advancing candidate and recording ONLY the final state once yields the
    /// same in-memory result AND the same single persisted row as applying each
    /// voucher through `apply_voucher` — because vouchers are cumulative, the
    /// final staged state supersedes every intermediate one, so one `record`
    /// commits the whole batch with no loss.
    #[test]
    fn stage_batch_then_record_once_equals_sequential_apply() -> anyhow::Result<()> {
        let (signer, base, domain, batch_store) = fixture();

        // Reference: apply three vouchers one-by-one (three records).
        let mut seq_state = base.clone();
        let seq_store = MemoryChannelStateStore::new();
        let vouchers = [
            (1_000u64, 1u64, 1_048_576u64),
            (2_000, 2, 2_097_152),
            (3_000, 3, 3_145_728),
        ];
        for (amount, nonce, bytes) in vouchers {
            let signed =
                build(base.channel_id, amount, nonce, bytes, TOKEN).sign(&signer, &domain)?;
            seq_state.apply_voucher(&signed, &domain, &seq_store)?;
        }

        // Batched: stage each against an advancing candidate, record ONCE.
        let mut candidate = base.clone();
        let mut gaps = Vec::new();
        for (amount, nonce, bytes) in vouchers {
            let signed =
                build(base.channel_id, amount, nonce, bytes, TOKEN).sign(&signer, &domain)?;
            let (next, applied) = candidate.stage_voucher(&signed, &domain)?;
            candidate = next;
            gaps.push(applied.nonce_gap());
        }
        batch_store.record(&candidate)?;

        anyhow::ensure!(
            candidate == seq_state,
            "batched state must equal sequential"
        );
        anyhow::ensure!(candidate.last_nonce() == U256::from(3u64));
        anyhow::ensure!(gaps == vec![0, 0, 0], "contiguous batch has no gaps");
        // One persisted row, holding the final cumulative watermark.
        anyhow::ensure!(batch_store.len() == 1, "batch persists exactly one row");
        let persisted = batch_store.load_all()?;
        let only = persisted.first().ok_or_else(|| anyhow::anyhow!("no row"))?;
        anyhow::ensure!(*only == seq_state, "one record commits the whole batch");
        Ok(())
    }

    /// Staging is pure: a rejected voucher leaves the candidate that produced it
    /// untouched (returns `Err`, advances nothing), so a caller can commit the
    /// valid prefix and reject the offender — the group-commit mid-batch split.
    #[test]
    fn stage_voucher_rejects_without_advancing_candidate() -> anyhow::Result<()> {
        let (signer, base, domain, _store) = fixture();
        let v1 = build(base.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&signer, &domain)?;
        let (after_v1, _) = base.stage_voucher(&v1, &domain)?;

        // A stale-nonce voucher against the advanced candidate must reject.
        let bad = build(base.channel_id, 2_000, 1, 2_097_152, TOKEN).sign(&signer, &domain)?;
        let err = err_of(after_v1.stage_voucher(&bad, &domain))?;
        anyhow::ensure!(
            matches!(err, ChannelError::NonceNotIncreasing { .. }),
            "{err:?}"
        );
        // `stage_voucher` takes `&self`; the candidate it was called on is
        // unchanged by construction — assert the prefix state still holds.
        anyhow::ensure!(after_v1.last_nonce() == U256::from(1u64));
        Ok(())
    }
}
