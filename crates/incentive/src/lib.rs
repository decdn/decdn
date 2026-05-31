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

pub mod bind_sig;
pub mod channel;
pub mod client_bridge;
pub mod client_reputation;
pub mod eth_identity;
pub mod probe_sig;
pub mod rate;
pub mod staking_registry;
pub mod store;
pub mod stream_sig;
pub mod voucher;

pub use bind_sig::{
    BindError, CAPACITY_BOND_DOMAIN_NAME, CAPACITY_BOND_DOMAIN_VERSION, EPHEMERAL_BINDING_NONCE,
    bind_node_id_domain, binding_signing_hash, verify_binding,
};
pub use channel::{ChannelError, ChannelId, ChannelState};
pub use client_bridge::{
    RetrySignal, WireVoucherError, signed_to_wire_voucher, voucher_reject_reason,
    wire_voucher_to_signed,
};
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
pub use stream_sig::{StreamSlashData, StreamSlashError};
pub use voucher::{
    DOMAIN_NAME, DOMAIN_VERSION, SignedVoucher, Voucher, VoucherError, voucher_domain,
};
