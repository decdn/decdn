//! Per-channel voucher state tracking.
//!
//! As a node receives vouchers from a client, it must keep the latest one and
//! reject any that would lower the cumulative `amount`, `bytes_delivered`, or
//! `nonce` — the on-chain `closeChannel` / `disputeChannel` invariants from
//! ADR 003 §Fee Routing on Disputed Closes apply equally off-chain (a node
//! that retains a stale voucher just under-claims at settlement).
//!
//! State is kept in memory; persistence is wiring-layer concern (issue #406
//! covers the keystore + persistence path). The contract-level open / close /
//! dispute / settle calls are issue #327.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelState {
    /// Channel identifier (matches the on-chain `channelId`).
    pub channel_id: ChannelId,
    /// The Ethereum address that opened the channel and signs vouchers.
    pub client: Address,
    /// `ERC-20` token bound by this channel (`USDC` for the `PoC`).
    pub token: Address,
    /// On-chain deposited amount in token base units. Vouchers MUST NOT
    /// exceed this value.
    pub deposit: U256,
    /// Cumulative amount of the most-recently-accepted voucher
    /// (token base units). `U256::ZERO` until the first voucher is applied.
    pub last_amount: U256,
    /// Sequence number of the most-recently-accepted voucher. `U256::ZERO`
    /// before any voucher is applied — matches the on-chain
    /// `claimedNonce == 0` sentinel from ADR 003 §Voucher Nonce Convention.
    pub last_nonce: U256,
    /// Cumulative bytes delivered as of the most-recently-accepted voucher.
    pub last_bytes_delivered: U256,
}

impl ChannelState {
    /// Construct fresh state for a newly-opened channel. The `last_*` fields
    /// start at zero, matching the on-chain `Channel` struct's defaults.
    #[must_use]
    pub const fn new(
        channel_id: ChannelId,
        client: Address,
        token: Address,
        deposit: U256,
    ) -> Self {
        Self {
            channel_id,
            client,
            token,
            deposit,
            last_amount: U256::ZERO,
            last_nonce: U256::ZERO,
            last_bytes_delivered: U256::ZERO,
        }
    }

    /// Validate `signed` against this channel's invariants and, on success,
    /// advance the `last_*` fields.
    ///
    /// Mirrors the on-chain `closeChannel` + `disputeChannel` checks:
    /// - signature recovers to `self.client`
    /// - `voucher.channel_id == self.channel_id`
    /// - `voucher.token == self.token`
    /// - `voucher.nonce > self.last_nonce`
    /// - `voucher.amount >= self.last_amount`
    /// - `voucher.bytes_delivered >= self.last_bytes_delivered`
    /// - `voucher.amount <= self.deposit`
    ///
    /// On any check failure, state is left unchanged.
    ///
    /// # Errors
    ///
    /// See [`ChannelError`] for the full taxonomy.
    pub fn apply_voucher(
        &mut self,
        signed: &SignedVoucher,
        domain: &Eip712Domain,
    ) -> Result<(), ChannelError> {
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
        // Signature check is last — it's the most expensive (ecrecover).
        signed
            .verify_signer(self.client, domain)
            .map_err(ChannelError::Signature)?;

        self.last_amount = signed.voucher.amount;
        self.last_nonce = signed.voucher.nonce;
        self.last_bytes_delivered = signed.voucher.bytes_delivered;
        Ok(())
    }
}

/// Failure modes for [`ChannelState::apply_voucher`].
///
/// Each variant maps to an on-chain `closeChannel` / `disputeChannel` revert
/// from ADR 003 §Fee Routing on Disputed Closes; off-chain we surface them
/// before they cost gas.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
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
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed pair up clearly here
mod tests {
    use super::*;
    use crate::voucher::{Voucher, voucher_domain};
    use alloy::primitives::{address, b256};
    use alloy::signers::local::PrivateKeySigner;

    const CHAIN_ID: u64 = 421_614;
    const VERIFYING: Address = address!("0000000000000000000000000000000000001234");
    const TOKEN: Address = address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");

    fn fixture() -> (PrivateKeySigner, ChannelState, Eip712Domain) {
        let signer = PrivateKeySigner::random();
        let state = ChannelState::new(
            b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"),
            signer.address(),
            TOKEN,
            U256::from(10_000_000u64), // 10 USDC deposit
        );
        let domain = voucher_domain(CHAIN_ID, VERIFYING);
        (signer, state, domain)
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
        let (signer, mut state, domain) = fixture();
        let signed = build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&signer, &domain)?;

        state.apply_voucher(&signed, &domain)?;
        anyhow::ensure!(state.last_amount == U256::from(1_000u64));
        anyhow::ensure!(state.last_nonce == U256::from(1u64));
        anyhow::ensure!(state.last_bytes_delivered == U256::from(1_048_576u64));
        Ok(())
    }

    #[test]
    fn monotonic_progression_accepted() -> anyhow::Result<()> {
        let (signer, mut state, domain) = fixture();
        for (amount, nonce, bytes) in [(1_000u64, 1u64, 1_048_576u64), (2_500, 2, 2_621_440)] {
            let signed =
                build(state.channel_id, amount, nonce, bytes, TOKEN).sign(&signer, &domain)?;
            state.apply_voucher(&signed, &domain)?;
        }
        anyhow::ensure!(state.last_nonce == U256::from(2u64));
        Ok(())
    }

    #[test]
    fn wrong_channel_id_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain) = fixture();
        let signed = build(B256::ZERO, 1_000, 1, 1, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain))?;
        anyhow::ensure!(matches!(err, ChannelError::WrongChannel { .. }), "{err:?}");
        anyhow::ensure!(state.last_nonce == U256::ZERO, "state must be unchanged");
        Ok(())
    }

    #[test]
    fn wrong_token_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain) = fixture();
        let signed = build(
            state.channel_id,
            1_000,
            1,
            1,
            address!("0000000000000000000000000000000000000000"),
        )
        .sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain))?;
        anyhow::ensure!(matches!(err, ChannelError::WrongToken { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn equal_nonce_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain) = fixture();
        let v1 = build(state.channel_id, 1_000, 1, 1, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain)?;

        let v2 = build(state.channel_id, 2_000, 1, 2, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&v2, &domain))?;
        anyhow::ensure!(
            matches!(err, ChannelError::NonceNotIncreasing { .. }),
            "{err:?}"
        );
        Ok(())
    }

    #[test]
    fn lower_nonce_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain) = fixture();
        let v1 = build(state.channel_id, 1_000, 5, 1, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain)?;

        let v2 = build(state.channel_id, 2_000, 4, 2, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&v2, &domain))?;
        anyhow::ensure!(
            matches!(err, ChannelError::NonceNotIncreasing { .. }),
            "{err:?}"
        );
        Ok(())
    }

    #[test]
    fn amount_decrease_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain) = fixture();
        let v1 = build(state.channel_id, 5_000, 1, 1, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain)?;

        let v2 = build(state.channel_id, 4_000, 2, 2, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&v2, &domain))?;
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
        let (signer, mut state, domain) = fixture();
        let v1 = build(state.channel_id, 5_000, 1, 1, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain)?;

        let v2 = build(state.channel_id, 5_000, 2, 2, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v2, &domain)?;
        anyhow::ensure!(state.last_nonce == U256::from(2u64));
        Ok(())
    }

    #[test]
    fn bytes_decrease_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain) = fixture();
        let v1 = build(state.channel_id, 1_000, 1, 1_048_576, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain)?;

        let v2 = build(state.channel_id, 2_000, 2, 524_288, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&v2, &domain))?;
        anyhow::ensure!(
            matches!(err, ChannelError::BytesDecreasing { .. }),
            "{err:?}"
        );
        Ok(())
    }

    #[test]
    fn amount_exceeds_deposit_rejected() -> anyhow::Result<()> {
        let (signer, mut state, domain) = fixture();
        // deposit is 10_000_000 (10 USDC); attempt 11 USDC.
        let signed = build(state.channel_id, 11_000_000, 1, 1, TOKEN).sign(&signer, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain))?;
        anyhow::ensure!(
            matches!(err, ChannelError::AmountExceedsDeposit { .. }),
            "{err:?}"
        );
        Ok(())
    }

    #[test]
    fn wrong_signer_rejected() -> anyhow::Result<()> {
        let (_signer, mut state, domain) = fixture();
        // Sign with an unrelated key.
        let interloper = PrivateKeySigner::random();
        let signed = build(state.channel_id, 1_000, 1, 1, TOKEN).sign(&interloper, &domain)?;
        let err = err_of(state.apply_voucher(&signed, &domain))?;
        anyhow::ensure!(
            matches!(
                err,
                ChannelError::Signature(VoucherError::WrongSigner { .. })
            ),
            "{err:?}"
        );
        Ok(())
    }

    #[test]
    fn rejected_voucher_does_not_advance_state() -> anyhow::Result<()> {
        let (signer, mut state, domain) = fixture();
        let v1 = build(state.channel_id, 5_000, 1, 1, TOKEN).sign(&signer, &domain)?;
        state.apply_voucher(&v1, &domain)?;
        let snapshot = state.clone();

        // Multiple rejection paths — verify each leaves state unchanged.
        let bad_amount = build(state.channel_id, 1, 2, 2, TOKEN).sign(&signer, &domain)?;
        let _ = state.apply_voucher(&bad_amount, &domain);
        anyhow::ensure!(state == snapshot, "amount drop should not advance state");

        let bad_nonce = build(state.channel_id, 6_000, 1, 2, TOKEN).sign(&signer, &domain)?;
        let _ = state.apply_voucher(&bad_nonce, &domain);
        anyhow::ensure!(state == snapshot, "stale nonce should not advance state");

        let bad_token = build(
            state.channel_id,
            6_000,
            2,
            2,
            address!("dead000000000000000000000000000000000000"),
        )
        .sign(&signer, &domain)?;
        let _ = state.apply_voucher(&bad_token, &domain);
        anyhow::ensure!(state == snapshot, "wrong token should not advance state");
        Ok(())
    }
}
