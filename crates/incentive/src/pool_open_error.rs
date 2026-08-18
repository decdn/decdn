//! Failure-class taxonomy for the buyer-side `openPool` path (#966).
//!
//! The buyer `open_pool` kernel (`decdn-client-pull`) can fail three ways
//! that an operator must triage differently:
//!
//! - **`InsufficientDeposit`** — a *misconfiguration*: the node's USDC balance
//!   or standing allowance cannot cover the deposit, or the deposit is zero —
//!   either as requested, or as the balance delta actually received under a
//!   fee-on-transfer token. (A zero deposit cannot be requested: both the daemon
//!   and the CLI validate `buyer_working_deposit_micro_usdc` / the
//!   `--working-deposit-micro-usdc` flag `> 0` at config load, so the reachable
//!   zero here is the received-delta one.) The fix is operator-side (fund the
//!   deposit), not infrastructure.
//! - **`ContractRevert`** — any *other* deterministic on-chain revert (a
//!   paused contract, a future revert reason). The deposit was not escrowed;
//!   the cause is on-chain state, not this node's wallet or RPC.
//! - **`RpcError`** — a *transient* transport/RPC fault (connectivity, a
//!   timed out receipt wait, a transaction-nonce blip — the sender's tx
//!   sequence number, unrelated to a voucher). Retrying typically clears it;
//!   the fix is infrastructure-side.
//!
//! Splitting these lets the node bump the matching
//! `decdn_pool_open_failures_{insufficient_deposit,contract_revert,rpc_error}_total`
//! sibling counter (a plain counter field carries no label dimension, so each class is its own
//! counter rather than one labeled `{reason=…}` series) and emit the same
//! `reason` token as a structured-log field, so a dashboard distinguishes "the
//! operator under-funded the gas/USDC wallet" from "the RPC endpoint is flaky"
//! — the whole point of #966. The classification is driven by the alloy
//! error's *revert data*: a deterministic revert carries ABI-encoded error data
//! (caught at gas estimation, so it surfaces on `send()` before a receipt),
//! whereas a transport fault carries none. A present revert selector is decoded
//! against the known insufficient-deposit error signatures to split
//! `InsufficientDeposit` from a generic `ContractRevert`.

use alloy::primitives::Bytes;
use alloy::sol;
use alloy::sol_types::SolError;

sol! {
    /// `PaymentPool.openPool` reverts this when the requested deposit — or
    /// the balance delta actually received, under a fee-on-transfer token —
    /// is zero. Declared here only for its 4-byte selector, so the
    /// classifier needs no live contract binding.
    ///
    /// This selector is NOT unique to `openPool`: `PaymentPool.topUp`
    /// reverts it too, and `FeeRouter`, `CapacityBond` and `BuybackBurner`
    /// each declare the identical argument-less signature, so all five
    /// share one 4-byte selector. Safe here only because
    /// [`PoolOpenFailureReason::classify_revert_data`] is called on the
    /// open path alone; widening its use would need this checked.
    error ZeroAmount();

    /// `OpenZeppelin` v5 `ERC20`: the spender's balance is below the transfer
    /// amount. `openPool`'s `safeTransferFrom` bubbles this up verbatim when
    /// the node's USDC balance cannot cover the deposit.
    error ERC20InsufficientBalance(address sender, uint256 balance, uint256 needed);

    /// `OpenZeppelin` v5 `ERC20`: the standing allowance is below the transfer
    /// amount. Surfaces when the one-time `approve` never ran (or was set too
    /// low) for the `PaymentPool` spender.
    error ERC20InsufficientAllowance(address spender, uint256 allowance, uint256 needed);
}

/// Which class of failure aborted a buyer `openPool` attempt (#966). Carried
/// through the `anyhow` error chain as typed context so the metrics layer can
/// bump the matching `decdn_pool_open_failures_{reason}_total` sibling
/// counter (one counter per class — a plain counter field carries no label dimension) and
/// attach a structured `reason` log field, while the human-readable message is
/// preserved for logs.
///
/// The sibling-counter shape is the settled convention (#1475), not a stopgap:
/// these three classes have unrelated operator remedies (fund the wallet / fix
/// the contract call / chase the RPC) *and* no meaningful aggregate — "how many
/// pool opens failed" is not a question with one answer or one response. `decdn_probe_hold_unavailable_total` is the one
/// labeled reason split, and it is labeled precisely because its values *do*
/// share one aggregate and one budget axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolOpenFailureReason {
    /// The node's USDC balance/allowance cannot cover the deposit, or the
    /// deposit is zero — as requested, or as the received balance delta under a
    /// fee-on-transfer token. An operator misconfiguration either way. Metric
    /// label `insufficient_deposit`.
    InsufficientDeposit,
    /// Any other deterministic on-chain revert (the deposit was not escrowed).
    /// Metric label `contract_revert`.
    ContractRevert,
    /// A transient transport/RPC fault (no revert data) — submit or receipt
    /// wait failed at the network layer. Metric label `rpc_error`.
    RpcError,
}

impl PoolOpenFailureReason {
    /// The metric/label and structured-log `reason` token for this class —
    /// the `{reason}` slug in the `decdn_pool_open_failures_{reason}_total`
    /// sibling counter name and the value of the structured-log `reason` field.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::InsufficientDeposit => "insufficient_deposit",
            Self::ContractRevert => "contract_revert",
            Self::RpcError => "rpc_error",
        }
    }

    /// Classify the revert data (if any) attached to an `openPool` *submit*
    /// (`send()`) error. `None` revert data is a transport/RPC fault; present
    /// revert data is decoded against the insufficient-deposit error selectors
    /// to split `InsufficientDeposit` from a generic `ContractRevert`.
    ///
    /// Pass the result of alloy's `Error::as_revert_data()` (an inherent method
    /// on `alloy_contract::Error`); keeping the signature on raw bytes makes the
    /// policy unit-testable without constructing a live RPC error.
    #[must_use]
    pub fn classify_revert_data(revert_data: Option<&Bytes>) -> Self {
        let Some(data) = revert_data else {
            // No revert payload → the call never reached deterministic execution
            // (connectivity, tx-nonce, a timed-out estimate): a transport fault.
            return Self::RpcError;
        };
        if is_insufficient_deposit_selector(data) {
            Self::InsufficientDeposit
        } else {
            Self::ContractRevert
        }
    }
}

impl std::fmt::Display for PoolOpenFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pool-open failure reason: {}", self.as_label())
    }
}

/// Whether ABI-encoded `revert_data` begins with one of the known
/// insufficient-deposit error selectors (the leading 4 bytes). A too-short
/// payload (no full selector) is treated as not-matching, so it falls through to
/// the generic `ContractRevert` class rather than panicking on a slice.
fn is_insufficient_deposit_selector(revert_data: &[u8]) -> bool {
    let Some(selector) = revert_data.get(..4) else {
        return false;
    };
    selector == ZeroAmount::SELECTOR
        || selector == ERC20InsufficientBalance::SELECTOR
        || selector == ERC20InsufficientAllowance::SELECTOR
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, U256};

    #[test]
    fn no_revert_data_is_rpc_error() {
        assert_eq!(
            PoolOpenFailureReason::classify_revert_data(None),
            PoolOpenFailureReason::RpcError
        );
    }

    #[test]
    fn zero_amount_is_insufficient_deposit() {
        let data = Bytes::from(ZeroAmount {}.abi_encode());
        assert_eq!(
            PoolOpenFailureReason::classify_revert_data(Some(&data)),
            PoolOpenFailureReason::InsufficientDeposit
        );
    }

    #[test]
    fn erc20_insufficient_balance_is_insufficient_deposit() {
        let data = Bytes::from(
            ERC20InsufficientBalance {
                sender: Address::repeat_byte(0x11),
                balance: U256::ZERO,
                needed: U256::from(10_000_000u64),
            }
            .abi_encode(),
        );
        assert_eq!(
            PoolOpenFailureReason::classify_revert_data(Some(&data)),
            PoolOpenFailureReason::InsufficientDeposit
        );
    }

    #[test]
    fn erc20_insufficient_allowance_is_insufficient_deposit() {
        let data = Bytes::from(
            ERC20InsufficientAllowance {
                spender: Address::repeat_byte(0x22),
                allowance: U256::ZERO,
                needed: U256::from(10_000_000u64),
            }
            .abi_encode(),
        );
        assert_eq!(
            PoolOpenFailureReason::classify_revert_data(Some(&data)),
            PoolOpenFailureReason::InsufficientDeposit
        );
    }

    #[test]
    fn unknown_revert_selector_is_contract_revert() {
        // A 4-byte selector that matches none of the insufficient-deposit
        // errors (e.g. `ProviderNotActive`) → generic contract revert.
        let data = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x00]);
        assert_eq!(
            PoolOpenFailureReason::classify_revert_data(Some(&data)),
            PoolOpenFailureReason::ContractRevert
        );
    }

    #[test]
    fn truncated_revert_data_is_contract_revert() {
        // Fewer than 4 bytes cannot be a selector — must not panic on the slice,
        // and falls through to the generic revert class.
        let data = Bytes::from(vec![0x01, 0x02]);
        assert_eq!(
            PoolOpenFailureReason::classify_revert_data(Some(&data)),
            PoolOpenFailureReason::ContractRevert
        );
    }

    #[test]
    fn labels_are_the_documented_reason_tokens() {
        assert_eq!(
            PoolOpenFailureReason::InsufficientDeposit.as_label(),
            "insufficient_deposit"
        );
        assert_eq!(
            PoolOpenFailureReason::ContractRevert.as_label(),
            "contract_revert"
        );
        assert_eq!(PoolOpenFailureReason::RpcError.as_label(), "rpc_error");
    }
}
