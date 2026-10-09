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
fn pool_not_open_revert_is_recognised_and_nothing_else_is() {
    let data = Bytes::from(PoolNotOpen {}.abi_encode());
    assert!(is_pool_not_open(Some(&data)));
    let other = Bytes::from(ZeroAmount {}.abi_encode());
    assert!(!is_pool_not_open(Some(&other)));
    assert!(!is_pool_not_open(None));
    assert!(!is_pool_not_open(Some(&Bytes::from(vec![0u8; 2]))));
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
fn below_min_deposit_is_insufficient_deposit() {
    let data = Bytes::from(
        BelowMinDeposit {
            received: U256::from(1_000_000u64),
            minDeposit: U256::from(5_000_000u64),
        }
        .abi_encode(),
    );
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

/// A string-revert token, not `OpenZeppelin` v5 — the shape a pre-custom-error
/// USDC reverts `openPool` with. Classifying it as `ContractRevert` reports an
/// under-funded wallet as an opaque on-chain fault naming no remedy.
#[test]
fn erc20_string_revert_balance_is_insufficient_deposit() {
    let data =
        Bytes::from(Revert::from("ERC20: transfer amount exceeds balance".to_owned()).abi_encode());
    assert_eq!(
        PoolOpenFailureReason::classify_revert_data(Some(&data)),
        PoolOpenFailureReason::InsufficientDeposit
    );
}

#[test]
fn erc20_string_revert_allowance_is_insufficient_deposit_and_a_shortfall() {
    for reason in [
        "ERC20: insufficient allowance",
        "ERC20: transfer amount exceeds allowance",
    ] {
        let data = Bytes::from(Revert::from(reason.to_owned()).abi_encode());
        assert_eq!(
            PoolOpenFailureReason::classify_revert_data(Some(&data)),
            PoolOpenFailureReason::InsufficientDeposit,
            "{reason}"
        );
        assert!(is_erc20_allowance_shortfall(Some(&data)), "{reason}");
    }
}

/// A balance shortfall is terminal for the just-in-time `approve` path: no
/// `approve` recovers it, whichever revert style the token uses.
#[test]
fn erc20_string_revert_balance_is_not_an_allowance_shortfall() {
    let data =
        Bytes::from(Revert::from("ERC20: transfer amount exceeds balance".to_owned()).abi_encode());
    assert!(!is_erc20_allowance_shortfall(Some(&data)));
}

/// An unrelated string revert stays a generic `ContractRevert` — the
/// message match must not swallow every `Error(string)` payload.
#[test]
fn unrelated_string_revert_is_contract_revert() {
    let data = Bytes::from(Revert::from("Pausable: paused".to_owned()).abi_encode());
    assert_eq!(
        PoolOpenFailureReason::classify_revert_data(Some(&data)),
        PoolOpenFailureReason::ContractRevert
    );
    assert!(!is_erc20_allowance_shortfall(Some(&data)));
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
fn allowance_shortfall_true_only_for_allowance_error() {
    let allowance = Bytes::from(
        ERC20InsufficientAllowance {
            spender: Address::repeat_byte(0x22),
            allowance: U256::ZERO,
            needed: U256::from(10_000_000u64),
        }
        .abi_encode(),
    );
    assert!(is_erc20_allowance_shortfall(Some(&allowance)));

    let balance = Bytes::from(
        ERC20InsufficientBalance {
            sender: Address::repeat_byte(0x11),
            balance: U256::ZERO,
            needed: U256::from(10_000_000u64),
        }
        .abi_encode(),
    );
    assert!(!is_erc20_allowance_shortfall(Some(&balance)));
    assert!(!is_erc20_allowance_shortfall(Some(&Bytes::from(
        ZeroAmount {}.abi_encode()
    ))));
    assert!(!is_erc20_allowance_shortfall(None));
    // Truncated (< 4 bytes) must not panic on the slice and returns false.
    assert!(!is_erc20_allowance_shortfall(Some(&Bytes::from(vec![
        0x01, 0x02
    ]))));
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
