//! Implementations of the user-facing `decdn` CLI subcommands.
//!
//! `node` talks to a running daemon over the loopback admin RPC surface
//! (ADR 025). The other modules are one-shot publisher/operator
//! operations: generate a key, render or validate a config file, probe
//! a remote node.

pub mod appeal;
pub mod bond;
pub mod bundle;
pub mod bundle_pull;
pub mod chain_ctx;
pub mod config;
pub mod deregister;
pub mod doctor;
pub mod fetch;
pub mod key_gen;
pub mod manifest;
pub mod node;
pub mod node_top;
pub mod origin;
pub mod pool;
pub mod probe;
pub mod publish;
pub mod register;
pub mod rotate_key;
pub mod setup;
pub mod terms;
pub mod unbond;
