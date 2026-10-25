//! The deCDN payment layer: USDC payment pools, the off-chain vouchers that pay
//! them down, and the signatures and contract bindings that tie them to the
//! chain.
//!
//! A client pays a node per megabyte from a `PaymentPool` deposit ([ADR 003]).
//! The pool owner opens a pool on chain and authorizes spenders off chain with
//! a signed [`Capability`]. Each spender pays each node on its own lane, a
//! `(pool, signer, provider)` triple, with cumulative EIP-712 [`Voucher`]s. A
//! `PayWord` hash chain advances a voucher between signatures, one preimage per
//! delivered chunk ([ADR 003 § Hash-chain metering]). The node redeems the
//! highest voucher on each lane with `redeem` or `redeemMany`.
//!
//! # Modules
//!
//! | Area | Modules |
//! |---|---|
//! | Vouchers and metering | [`voucher`] (sign and verify), [`chain`] (hash chains), [`rate`] (rate checks), [`credit`] (credit window), [`client_bridge`] (to and from the wire [`decdn_protocol::Voucher`]) |
//! | Spend delegation | [`capability`] (the signed capability), [`capability_grant`] (its transferable `dcap1:` form) |
//! | Lane state | [`lane`] (the seller's per-lane watermark), [`store`] (persistence traits and in-memory stores, the settlement ledger among them), [`earnings`] (sums over the ledger for `decdn node earnings`), [`deployment`] (which `PaymentPool` a state belongs to) |
//! | Buyer bookkeeping | [`buyer_pool`] (the buyer's pools and lanes), `buyer_pool_table` and `buyer_pool_redb` (an on-disk `redb` store, behind the `buyer-store-core` and `redb` features) |
//! | Response signatures | [`probe_sig`] and [`stream_sig`] (the `slash_sig` on probe and stream responses), [`bind_sig`] (client bindings), [`sig_canon`] (canonical `s`) |
//! | Keys | [`eth_identity`] (Web3 Secret Storage keystores and the `PrivateKeySigner` they load) |
//! | Contract bindings | [`payment_pool`], [`capacity_bond`], [`node_register`], [`erc20`], [`slash_judge`], [`slash_appeal`], [`origin_assignment`], [`publisher_registry`], [`content_blacklist`] |
//! | Transactions | [`tx`] (send, await the receipt, fail on a revert), [`pool_open_error`] (`PaymentPool` revert decoding), [`log_scan`] (bounded `eth_getLogs` windows that learn a provider's range cap) |
//!
//! # Sign and verify a voucher
//!
//! ```
//! use alloy::primitives::{Address, B256, U256};
//! use alloy::signers::local::PrivateKeySigner;
//! use decdn_incentive::{Voucher, voucher_domain};
//!
//! let payer = PrivateKeySigner::random();
//! let domain = voucher_domain(421_614, Address::repeat_byte(0x11));
//! let voucher = Voucher {
//!     pool_id: B256::repeat_byte(0x22),
//!     signer: payer.address(),
//!     provider: Address::repeat_byte(0x33),
//!     amount: U256::from(1_000),
//!     bytes_delivered: U256::from(1_048_576),
//!     chain_root: B256::ZERO,
//!     chunk_price: U256::ZERO,
//! };
//! let signed = voucher.sign(&payer, &domain)?;
//! signed.verify_signer(payer.address(), &domain)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Who drives settlement
//!
//! This crate holds the primitives. The `decdn-node` daemon drives both sides
//! of settlement with them. As a seller, it records each lane's watermark from
//! the `PoolOpened` and `Redeemed` events, redeems every lane worth the gas on a
//! sweep that runs well inside a closing pool's grace window, and forgets a
//! reclaimed pool. As a buyer on a cache miss,
//! it approves USDC once, opens or tops up a pool, signs the vouchers, and
//! closes and reclaims a pool it no longer uses.
//!
//! [ADR 003]: https://github.com/decdn/decdn/blob/main/adr/003-payments.md
//! [ADR 003 § Hash-chain metering]: https://github.com/decdn/decdn/blob/main/adr/003-payments.md#hash-chain-metering-payword
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod bind_sig;
pub mod buyer_pool;
pub mod credit;
pub mod deployment;
pub mod earnings;
pub mod log_scan;
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
pub mod chain;
pub mod client_bridge;
pub mod content_blacklist;
pub mod erc20;
pub mod eth_identity;
pub mod lane;
pub mod lane_signer;
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
#[cfg(feature = "test-util")]
pub mod test_rpc;
pub mod tx;
pub mod voucher;

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
pub use chain::{
    CHUNK_BYTES, ChainMeterError, MAX_CHAIN_LENGTH, pack_chain_meter, preimage_at, random_seed,
    root_from_seed, verify_forward,
};
pub use client_bridge::{
    RetrySignal, WireVoucherError, signed_to_wire_voucher, voucher_reject_reason,
    wire_voucher_to_signed,
};
pub use credit::ramped_credit_window;
pub use deployment::Deployment;
pub use erc20::Erc20;
pub use lane::{
    FoldAxis, LaneChain, LaneKey, LaneState, PoolError, PoolId, PreimageApplied, RedeemClaim,
    VoucherApplied,
};
pub use pool_open_error::{
    PoolLifecycleRevert, PoolOpenFailureReason, is_erc20_allowance_shortfall, is_pool_not_open,
};
pub use probe_sig::{
    PROBE_RESPONSE_TYPEHASH, ProbeSlashData, ProbeSlashError, SLASH_JUDGE_DOMAIN_NAME,
    SLASH_JUDGE_DOMAIN_VERSION, slash_judge_domain,
};
pub use rate::{
    BYTES_PER_MB, DEFAULT_TOLERANCE_BPS, RateError, min_payment, pool_budget_covers, verify_rate,
};
pub use store::{
    CheckpointKey, KeyedCheckpointStore, MemoryPoolStateStore, MemorySettlementLedger,
    PendingSettle, PendingSettleStore, PoolStateStore, SettlementLedger, SettlementRecord,
    StoreError,
};
pub use stream_sig::{STREAM_RESPONSE_TYPEHASH, StreamSlashData, StreamSlashError};
pub use voucher::{
    DOMAIN_NAME, DOMAIN_VERSION, SignedVoucher, Voucher, VoucherError, voucher_domain,
};
