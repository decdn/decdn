//! Minimal ERC-20 ABI binding for the buyer-side USDC approval flow (#744).
//!
//! Opening a `PaymentChannel` escrows the deposit via
//! `usdc.safeTransferFrom(client, contract, deposit)`, so the buyer node must
//! hold a standing ERC-20 allowance for the `PaymentChannel` contract. Per
//! ADR 003 § Deposit Economics the node issues a one-time max approval at
//! startup; this binding exposes just the calls that flow needs:
//!
//! - `allowance(owner, spender)` — read the current allowance to skip a
//!   redundant approve on restart;
//! - `approve(spender, amount)` — set the one-time max allowance;
//! - `balanceOf(account)` — optional pre-open balance check.
//!
//! The `PaymentChannel.usdc()` view resolves the token address at runtime, so
//! no token address is hard-coded here.

// The `sol!`-generated bindings include macro-emitted code that uses patterns
// workspace clippy denies (raw indexing, `unwrap` on infallible conversions).
// These allows scope the relaxation to this module only — same posture as
// `payment_channel` and `capacity_bond`.
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    clippy::pub_underscore_fields,
    clippy::missing_docs_in_private_items,
    clippy::too_many_arguments,
    missing_debug_implementations,
    missing_docs,
    non_snake_case,
    non_camel_case_types
)]
mod sol_types {
    alloy::sol! {
        #[sol(rpc)]
        contract Erc20 {
            /// Remaining number of tokens `spender` may spend on behalf of
            /// `owner`. The buyer reads this at startup to decide whether the
            /// one-time approval is already in place.
            function allowance(address owner, address spender) external view returns (uint256);

            /// Set `spender`'s allowance over the caller's tokens to `amount`.
            /// The buyer sets `amount = type(uint256).max` once at startup.
            function approve(address spender, uint256 amount) external returns (bool);

            /// Token balance of `account`.
            function balanceOf(address account) external view returns (uint256);
        }
    }
}

pub use sol_types::Erc20;
