//! Cross-layer Rust↔contract end-to-end test fixtures for deCDN.
//!
//! Reusable building blocks for journeys that span *both* daemon behavior and
//! on-chain state. Compose:
//!
//! - [`chain::ChainFixture`] — spin up `anvil`, deploy the full protocol via the
//!   production `DeployProtocol` script, expose typed `alloy` handles, and the
//!   helper verbs (fund / mint / onboard an operator / read served bytes).
//! - [`node::NodeFixture`] — a `decdn-node` daemon (spawned as a subprocess)
//!   wired to the chain, with its admin RPC client and graceful teardown.
//! - [`client::ClientFixture`] — drive the real paid client path against a node
//!   and verify delivered bytes.
//! - [`cli::decdn_command`] — spawn the user-facing `decdn` binary under an
//!   isolated `HOME`, so a journey never touches the developer's `~/.decdn`.
//! - [`ports`] — loopback ports a spawned process binds without a race.
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
pub mod cli;
pub mod client;
pub mod node;
mod poll;
pub mod ports;
pub mod time;
pub mod timeout;

pub use poll::poll;

/// Bail if a mined transaction reverted. alloy's `get_receipt()` resolves `Ok`
/// for a transaction that was *mined but reverted* (`status == false`), so every
/// write helper must inspect the status or a revert passes silently. `what`
/// names the call for the error message.
pub fn ensure_mined(
    receipt: &alloy::rpc::types::TransactionReceipt,
    what: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(receipt.status(), "{what} reverted on-chain");
    Ok(())
}
