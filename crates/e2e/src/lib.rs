//! Cross-layer Rust↔contract end-to-end test fixtures for deCDN (issue #1028).
//!
//! Reusable building blocks for journeys that span *both* daemon behavior and
//! on-chain state — the home the self-contained
//! `crates/node/tests/anvil_settlement_e2e.rs` monolith never had. Compose:
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
pub mod time;

pub use poll::poll;

/// Grab `N` distinct ephemeral TCP ports, then release them for spawned
/// processes (anvil or the daemon) to claim. Shared by [`chain`] and [`node`] so
/// the fixtures derive ports the same way.
///
/// The `N` listeners are bound *simultaneously* and dropped together, so the OS
/// is forced to hand back `N` distinct ports — `N` sequential single-port grabs
/// can (and under parallel load do) repeat a number.
///
/// A residual TOCTOU remains: between releasing a port here and the child
/// binding it, another process can claim it. Callers that can detect the
/// resulting failure should retry — [`chain::ChainFixture::launch`] re-picks its
/// port when anvil dies at startup. The fully race-free alternative (hold the
/// listener and hand its fd to the child via `SO_REUSEADDR`) isn't available:
/// neither `anvil` nor `decdn-node` accepts an inherited listener.
pub(crate) fn free_ports<const N: usize>() -> anyhow::Result<[u16; N]> {
    use anyhow::Context;
    let listeners: Vec<std::net::TcpListener> = (0..N)
        .map(|_| std::net::TcpListener::bind(("127.0.0.1", 0)).context("bind ephemeral port"))
        .collect::<anyhow::Result<_>>()?;
    let mut ports = [0u16; N];
    for (slot, l) in ports.iter_mut().zip(&listeners) {
        *slot = l.local_addr().context("local_addr")?.port();
    }
    Ok(ports)
    // `listeners` drops here, freeing all N ports at once.
}

/// Grab a single ephemeral TCP port. See [`free_ports`] for the TOCTOU caveat.
pub(crate) fn free_port() -> anyhow::Result<u16> {
    let [port] = free_ports::<1>()?;
    Ok(port)
}

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
