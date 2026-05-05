//! Incentive layer for deCDN.
//!
//! Manages off-chain USDC payment channels, staking interactions via
//! `StakingRegistry`, and voucher lifecycle (creation, validation,
//! on-chain settlement).
//!
//! Currently exposes only the off-chain payment-voucher primitives — EIP-712
//! signing/verification ([`voucher`]) — required for `cdn/client/v1`. The
//! on-chain settlement path (open / close / dispute / settle) is tracked in
//! issue #327; the keystore→signer bridge is tracked in issue #406.

pub mod channel;
pub mod rate;
pub mod voucher;

pub use channel::{ChannelError, ChannelId, ChannelState};
pub use rate::{BYTES_PER_MB, DEFAULT_TOLERANCE_BPS, RateError, verify_rate};
pub use voucher::{
    DOMAIN_NAME, DOMAIN_VERSION, SignedVoucher, Voucher, VoucherError, voucher_domain,
};
