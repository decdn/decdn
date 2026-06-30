//! Cross-layer Rust↔contract end-to-end test fixtures for deCDN (issue #1028).
//!
//! Reusable building blocks for journeys that span *both* daemon behavior and
//! on-chain state — the home the self-contained
//! `crates/node/tests/anvil_settlement_e2e.rs` monolith never had. Compose:
//!
//! - [`chain::ChainFixture`] — spin up `anvil`, deploy the full protocol via the
//!   production `DeployProtocol` script, expose typed `alloy` handles, and the
//!   helper verbs (fund / mint / onboard an operator / read served bytes).
//! - [`node::NodeFixture`] — an in-process (subprocess) `decdn-node` daemon
//!   wired to the chain, with its admin RPC client and graceful teardown.
//! - [`client::ClientFixture`] — drive the real paid client path against a node
//!   and verify delivered bytes.
//! - [`time`] — advance anvil time across dispute / timelock / unbond windows.
//! - [`mod@assert`] — on-chain + admin-RPC + delivered-bytes assertion helpers.
//!
//! The fixtures shell out to `anvil`/`forge` and spawn the built `decdn-node`
//! binary, so a journey that drives a real chain requires both Foundry and a
//! prior `cargo build` of the workspace binaries. They always *compile*; only
//! the tests that actually run a chain are gated (see the crate's `anvil-e2e`
//! feature and `tests/smoke.rs`).

// Test-infra crate: second-scale timeouts read more clearly as `from_secs`, and
// the ABI/CLI proper nouns in the docs (MintableUSDC, SIGKILL, …) aren't worth
// backticking. Both are pedantic style lints; the anvil settlement e2e allows
// the former for the same reason.
#![allow(clippy::doc_markdown, clippy::duration_suboptimal_units)]

pub mod assert;
pub mod bindings;
pub mod chain;
pub mod client;
pub mod node;
pub mod time;
