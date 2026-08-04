//! User-facing deCDN CLI library.
//!
//! The binary target (`src/main.rs`) is a thin entry point that parses
//! `Cli` from [`decdn_common::cli`] and dispatches into [`commands`].
//! Integration tests and downstream tools depend on this library
//! rather than the binary's `main.rs`.

pub mod commands;
pub mod known_chains;
