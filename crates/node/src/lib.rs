//! deCDN node daemon library — runtime, handlers, dispatch rate limiting,
//! metrics, and the admin RPC server (ADR 025).
//!
//! The binary target (`src/main.rs`) is a thin clap shell: parse `--config`
//! plus a single `run` subcommand and call into [`commands::run`].
//! Wire types and shared CLI/config schema live in [`decdn_common`]; the
//! user-facing CLI binary (`decdn`) lives in `crates/cli/`.

pub mod admin;
pub mod binding_check;
pub mod blacklist_watcher;
pub mod buyer_channel;
pub mod buyer_ledgers;
pub mod chain_events;
pub mod channel_store;
/// The `cdn/client/v1` paid-pull requester lives in the shared
/// `decdn-client-pull` crate (reused by the CLI's client fetch / bundle pull),
/// re-exported here as `client_requester` so node call sites and the
/// integration tests use `client_requester::…` paths.
pub use decdn_client_pull as client_requester;
pub mod commands;
pub mod content_deny;
pub mod dht;
pub mod dispatch;
pub mod fee_shares;
pub mod handlers;
pub mod metrics;
pub(crate) mod net;
pub mod node_origin;
pub mod onchain_tx;
pub mod payment_settlement;
pub mod pool_view;
pub(crate) mod prune_guard;
pub mod rate_bounds;
pub mod rate_bounds_watcher;
pub mod rate_limit;
pub mod receipt_log;
pub mod runtime;
pub mod selection;
pub mod serve_economics;
pub mod slash_watcher;
