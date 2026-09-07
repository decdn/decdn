//! User-facing deCDN CLI library.
//!
//! The binary target (`src/main.rs`) is a thin entry point that parses
//! `Cli` from [`decdn_common::cli`] and dispatches into [`commands`].
//! Integration tests and downstream tools depend on this library
//! rather than the binary's `main.rs`.
//!
//! Terminal UI: every `decdn` subcommand writes its result to stdout and its
//! progress and warnings to stderr, so this crate root allows the two print
//! lints the rest of the workspace denies.
#![allow(clippy::print_stdout, clippy::print_stderr)]

pub mod commands;
pub mod known_chains;
pub mod logging;
