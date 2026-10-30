//! Live anvil-backed contract run of the TypeScript SDK
//! ([decdn/sdk-typescript](https://github.com/decdn/sdk-typescript)) against
//! a real `decdn gateway`.
//!
//! The SDK's contract suite (`test/contract/*.test.ts` in that repository)
//! runs against its fake gateway in the SDK's own CI. Here the same files run
//! against the binary, so a drift between the fake and the gateway fails.
//! The setup (chain, node, publisher or owner, gateway, fixture, the checks
//! after the suite) lives in `decdn_e2e::sdk_contract`; this target runs it
//! once per mode with [`TypeScript`], which runs vitest with `node` and
//! names the fixture file in `DECDN_CONTRACT`.
//!
//! Gated behind the `anvil-e2e` and `sdk-e2e` features (off by default).
//! Requires `anvil` + `forge` + `node` on `PATH`, built `decdn` and
//! `decdn-node` binaries, and `DECDN_SDK_DIR` naming, by absolute path, a
//! checkout of decdn/sdk-typescript with its dependencies installed. The
//! commands are in CONTRIBUTING.md § SDK contract runs. The Python SDK has its
//! own target, `gateway_sdk_python`.

#![cfg(all(feature = "anvil-e2e", feature = "sdk-e2e"))]

use decdn_e2e::sdk_contract::{Mode, TypeScript};

#[tokio::test(flavor = "multi_thread")]
async fn the_sdk_contract_holds_on_a_public_gateway() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    decdn_e2e::sdk_contract::run(&TypeScript::from_env()?, Mode::Public).await
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sdk_contract_holds_on_a_loopback_gateway() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    decdn_e2e::sdk_contract::run(&TypeScript::from_env()?, Mode::Loopback).await
}
