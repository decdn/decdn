//! deCDN node daemon library — runtime, handlers, dispatch rate limiting,
//! metrics, and the admin RPC server (ADR 025).
//!
//! The binary target (`src/main.rs`) is a thin clap shell: parse `--config`
//! plus a single `run` subcommand and call into [`commands::run`].
//! Wire types and shared CLI/config schema live in [`decdn_common`]; the
//! user-facing CLI binary (`decdn`) lives in `crates/cli/`.

pub mod admin;
pub mod buyer_channel;
pub mod channel_store;
pub mod client_requester;
pub mod commands;
pub mod dht;
pub mod dispatch;
pub mod handlers;
pub mod leech_governor;
pub mod metrics;
pub mod node_origin;
pub mod payment_settlement;
pub mod prefetch;
pub mod probe_client;
pub mod receipt_log;
pub mod region_accounting;
pub mod reputation_indexer;
pub mod reputation_wiring;
pub mod runtime;
pub mod selection;
