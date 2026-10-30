//! Live anvil-backed contract run of the Python SDK
//! ([decdn/sdk-python](https://github.com/decdn/sdk-python)) against a real
//! `decdn gateway`.
//!
//! The SDK's contract suite (`tests/contract/test_*.py` in that repository)
//! runs against its fake gateway in the SDK's own CI. Here the same files run
//! against the binary, so a drift between the fake and the gateway fails.
//! The setup (chain, node, publisher or owner, gateway, fixture, the checks
//! after the suite) lives in `decdn_e2e::sdk_contract`; this target runs it
//! once per mode with [`Python`], which runs pytest with `uv` and names the
//! fixture file in `DECDN_CONTRACT`.
//!
//! Gated behind the `anvil-e2e` and `sdk-e2e` features (off by default).
//! Requires `anvil` + `forge` + `uv` on `PATH`, built `decdn` and
//! `decdn-node` binaries, and `DECDN_SDK_PYTHON_DIR` naming, by absolute
//! path, a checkout of decdn/sdk-python synced with `uv sync --frozen`.
//! The commands are in CONTRIBUTING.md § SDK contract runs.

#![cfg(all(feature = "anvil-e2e", feature = "sdk-e2e"))]

use decdn_e2e::sdk_contract::{Mode, Python};

#[tokio::test(flavor = "multi_thread")]
async fn the_sdk_contract_holds_on_a_public_gateway() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    decdn_e2e::sdk_contract::run(&Python::from_env()?, Mode::Public).await
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sdk_contract_holds_on_a_loopback_gateway() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    decdn_e2e::sdk_contract::run(&Python::from_env()?, Mode::Loopback).await
}
