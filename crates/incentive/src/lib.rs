//! Incentive layer for deCDN.
//!
//! Manages off-chain USDC payment channels, staking interactions via
//! `CapacityBond`, and voucher lifecycle (creation, validation,
//! on-chain settlement).
//!
//! Exposes the off-chain payment-voucher primitives — EIP-712
//! signing/verification ([`voucher`]) — required for `cdn/client/v1`, the
//! keystore→signer bridge ([`eth_identity`], #406) used by both
//! `decdn key-gen` and the runtime to load a `PrivateKeySigner`, and the
//! `alloy::sol!` contract bindings for the on-chain surface the node consumes
//! ([`capacity_bond`] reads, [`payment_channel`] reads + writes). The
//! seller-side on-chain settlement path (#327 — `ChannelOpened` →
//! persist, threshold/shutdown `withdraw` + `closeChannel`, `ChannelSettled`
//! → forget) is driven by the `decdn-node` runtime against these bindings;
//! buyer-side `openChannel` and the dispute monitor remain future work.

pub mod bind_sig;
pub mod capacity_bond;
pub mod channel;
pub mod client_bridge;
pub mod client_reputation;
pub mod eth_identity;
pub mod payment_channel;
pub mod probe_sig;
pub mod rate;
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
pub use store::{
    ChannelStateStore, MemoryChannelStateStore, MemoryPendingSettleStore, PendingSettle,
    PendingSettleStore, StoreError,
};
pub use stream_sig::{StreamSlashData, StreamSlashError};
pub use voucher::{
    DOMAIN_NAME, DOMAIN_VERSION, SignedVoucher, Voucher, VoucherError, voucher_domain,
};
