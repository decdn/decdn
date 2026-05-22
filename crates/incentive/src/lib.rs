//! Incentive layer for deCDN.
//!
//! Manages off-chain USDC payment channels, staking interactions via
//! `StakingRegistry`, and voucher lifecycle (creation, validation,
//! on-chain settlement).
//!
//! Currently exposes the off-chain payment-voucher primitives — EIP-712
//! signing/verification ([`voucher`]) — required for `cdn/client/v1`, and
//! the keystore→signer bridge ([`eth_identity`], #406) used by both
//! `decdn key-gen` and the runtime to load a `PrivateKeySigner`. The
//! on-chain settlement path (open / close / dispute / settle) is tracked in
//! issue #327.

pub mod channel;
pub mod client_reputation;
pub mod eth_identity;
pub mod probe_sig;
pub mod rate;
pub mod staking_registry;
pub mod store;
pub mod voucher;

pub use channel::{ChannelError, ChannelId, ChannelState};
pub use client_reputation::{
    Admission, ClientReputation, ClientReputationConfig, ClientReputationLedger,
    ClientReputationStore, ConfigError as ClientReputationConfigError, MemoryClientReputationStore,
};
pub use probe_sig::{
    ProbeSlashData, ProbeSlashError, SLASH_JUDGE_DOMAIN_NAME, SLASH_JUDGE_DOMAIN_VERSION,
    slash_judge_domain,
};
pub use rate::{BYTES_PER_MB, DEFAULT_TOLERANCE_BPS, RateError, verify_rate};
pub use store::{ChannelStateStore, MemoryChannelStateStore, StoreError};
pub use voucher::{
    DOMAIN_NAME, DOMAIN_VERSION, SignedVoucher, Voucher, VoucherError, voucher_domain,
};
