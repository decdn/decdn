//! Implementations of the user-facing `decdn` CLI subcommands.
//!
//! `node` talks to a running daemon over the loopback admin RPC surface
//! (ADR 025). The other modules are one-shot publisher/operator
//! operations: generate a key, render or validate a config file, probe
//! a remote node.

pub mod bundle;
pub mod config;
pub mod key_gen;
pub mod node;
pub mod node_top;
pub mod probe;
