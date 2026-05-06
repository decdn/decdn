//! deCDN node library — runtime, handlers, metrics, identity, and CLI types.
//!
//! The binary target (`src/main.rs`) is a thin entry point that parses the CLI
//! and dispatches into [`commands`]. Integration tests and downstream tools
//! should depend on this library rather than reaching into `src/` via `#[path]`.

pub mod admin;
pub mod commands;
pub mod dispatch;
pub mod handlers;
pub mod metrics;
pub mod runtime;

// Re-exports from `decdn-common`. These keep the existing
// `decdn_node::{cli,config,identity}::*` paths working for integration tests
// and downstream tools while the binary split is in flight. Removed in the
// commit that completes the slim of the node crate.
pub use decdn_common::{cli, config, identity};
