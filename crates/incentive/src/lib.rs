//! Incentive layer for deCDN.
//!
//! Manages off-chain USDC payment pools, bonding interactions via
//! `CapacityBond`, and voucher lifecycle (creation, validation, on-chain
//! settlement).
//!
//! Exposes the off-chain payment-voucher primitives — EIP-712
//! signing/verification ([`voucher`]) — required for `cdn/client/v1`, the
//! keystore→signer bridge ([`eth_identity`], #406) used by both
//! `decdn key-gen` and the runtime to load a `PrivateKeySigner`, and the
//! `alloy::sol!` contract bindings for the on-chain surface the node consumes
//! ([`capacity_bond`] reads, [`payment_pool`] reads + writes, [`erc20`]
//! approve). A `PaymentPool` fans one owner's deposit out across many capped
//! signers and many nodes: the owner opens a pool with `openPool`, authorizes
//! each signer off-chain with a [`capability::Capability`], and a signer's
//! per-`(pool, provider)` voucher lane accrues cumulative amount/bytes as
//! `redeem`/`redeemMany` pays it down. The seller-side on-chain settlement
//! path (`PoolOpened`/`Redeemed` → persist per-lane watermark in
//! [`lane::LaneState`], `PoolCloseInitiated` → grace-window monitor,
//! `PoolReclaimed` → forget) is fully driven by the `decdn-node` runtime. So
//! is the buyer-side cache-miss path (one-time USDC approve,
//! `openPool`/`topUp`, `closePool` + `reclaim` on abandonment; bookkeeping in
//! [`buyer_pool`]): the runtime bootstraps the approval and the reclaim
//! sweep, and its node-to-node pull-through origin drives pool open/reuse
//! and voucher signing on a miss.
//!
//! Stale-close defense is the one payment surface this crate does not reach,
//! and it is not built on either side yet. Per
//! `adr/appendix-fraud-detection.md` it needs no watchtower role, no escrow
//! contract, and no wire protocol: redemption against an owner's
//! `PoolCloseInitiated` grace window is permissionless, so the primary
//! mechanism is a local in-process monitor that follows the event on the
//! node's own pools and re-submits its latest voucher (operator-arranged
//! redundancy and the dispute window back it). That monitor is deferred
//! (#324) — the runtime observes the event but never redeems on its own
//! behalf outside the normal path.

pub mod bind_sig;
pub mod buyer_pool;
pub mod credit;
// NOTE: no outer `///` docs on these two — each module's `//!` header is its
// documentation. An outer doc here would be a second copy free to drift, and
// rustdoc merges it with the `//!` block and resolves the result in *this*
// module's scope, silently breaking the module's own intra-doc links.
#[cfg(feature = "redb")]
pub mod buyer_pool_redb;
#[cfg(feature = "buyer-store-core")]
pub mod buyer_pool_table;
pub mod capability;
pub mod capability_grant;
pub mod capacity_bond;
pub mod client_bridge;
pub mod content_blacklist;
pub mod erc20;
pub mod eth_identity;
pub mod lane;
pub mod node_register;
pub mod origin_assignment;
pub mod payment_pool;
pub mod pool_open_error;
pub mod probe_sig;
pub mod publisher_registry;
pub mod rate;
pub mod sig_canon;
pub mod slash_appeal;
pub mod slash_judge;
pub mod store;
pub mod stream_sig;
pub mod swap_balancer;
pub mod swap_math;
pub mod swap_uniswap;
pub mod swap_venue;
pub mod tx;
pub mod voucher;
pub mod voucher_activity;

pub use bind_sig::{
    BindError, CAPACITY_BOND_DOMAIN_NAME, CAPACITY_BOND_DOMAIN_VERSION, EPHEMERAL_BINDING_NONCE,
    bind_node_id_domain, binding_signing_hash, register_node_signing_hash, verify_binding,
};
pub use buyer_pool::{
    AdvanceOutcome, BuyerLaneProgress, BuyerLoad, BuyerPoolState, BuyerPoolStore,
    BuyerProgressError, DepositOutcome, MemoryBuyerPoolStore,
};
pub use capability::{Capability, CapabilityError, SignedCapability};
pub use capability_grant::{CapabilityGrant, GrantError, GrantOwnerError};
pub use client_bridge::{
    RetrySignal, WireVoucherError, signed_to_wire_voucher, voucher_reject_reason,
    wire_voucher_to_signed,
};
pub use credit::ramped_credit_window;
pub use erc20::Erc20;
pub use lane::{LaneKey, LaneState, PoolError, PoolId, VoucherApplied};
pub use pool_open_error::{PoolOpenFailureReason, is_erc20_allowance_shortfall};
pub use probe_sig::{
    ProbeSlashData, ProbeSlashError, SLASH_JUDGE_DOMAIN_NAME, SLASH_JUDGE_DOMAIN_VERSION,
    slash_judge_domain,
};
pub use rate::{
    BYTES_PER_MB, DEFAULT_TOLERANCE_BPS, RateError, floor_micro, min_payment, pool_budget_covers,
    verify_rate,
};
pub use store::{
    CheckpointKey, KeyedCheckpointStore, MemoryPendingSettleStore, MemoryPoolFloorLossStore,
    MemoryPoolStateStore, PendingSettle, PendingSettleStore, PoolFloorLossStore, PoolStateStore,
    StoreError,
};
pub use stream_sig::{StreamSlashData, StreamSlashError};
pub use swap_balancer::BalancerV3Venue;
pub use swap_math::{max_in_with_slippage, price_impact_bps, swap_top_up};
pub use swap_uniswap::UniswapV3Venue;
pub use swap_venue::{Quote, ResolvedSwap, SwapVenue, from_config};
pub use voucher::{
    DOMAIN_NAME, DOMAIN_VERSION, SignedVoucher, Voucher, VoucherError, voucher_domain,
};
pub use voucher_activity::VoucherActivity;
