//! deCDN node library — runtime, handlers, metrics, identity, and CLI types.
//!
//! The binary target (`src/main.rs`) is a thin entry point that parses the CLI
//! and dispatches into [`commands`]. Integration tests and downstream tools
//! should depend on this library rather than reaching into `src/` via `#[path]`.

pub mod admin;
pub mod cli;
pub mod commands;
pub mod config;
pub mod dispatch;
pub mod handlers;
pub mod identity;
pub mod metrics;
pub mod runtime;
pub mod selection;
