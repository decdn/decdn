//! The payment-chain half of the anvil e2e deploys: one anvil chain is both
//! the governance chain and the payment chain, as `contracts/dev-deploy.sh`
//! sets it up. A `LoopbackCctp` plays CCTP V2 on both sides (domain 0),
//! `DeployProtocol` takes it as its messenger and transmitter, and
//! `DeployPaymentPool` deploys the `PaymentPool` that sweeps through it to the
//! governance suite's `CctpSweepReceiver`.
//!
//! Included with `mod anvil_deploy;` by each anvil test binary; a directory
//! under `tests/` is not compiled as its own test binary.

use std::path::Path;
use std::time::Duration;

use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, Bytes};
use alloy::providers::Provider;
use alloy::sol_types::SolValue;
use anyhow::Context;

/// How long the `DeployPaymentPool` script may run.
const POOL_DEPLOY_TIMEOUT: Duration = Duration::from_secs(60);

/// Deploy contract `name` from its compiled artifact
/// (`out/{name}.sol/{name}.json`) with the ABI-encoded `ctor_args` appended to
/// its creation code, and return its address.
pub(crate) async fn deploy_artifact<P: Provider>(
    provider: &P,
    contracts: &Path,
    name: &str,
    ctor_args: &[u8],
) -> anyhow::Result<Address> {
    let artifact = contracts.join(format!("out/{name}.sol/{name}.json"));
    let bytes = std::fs::read(&artifact)
        .with_context(|| format!("read {name} artifact at {}", artifact.display()))?;
    let json: serde_json::Value = serde_json::from_slice(&bytes)?;
    let code: Bytes = json
        .get("bytecode")
        .and_then(|b| b.get("object"))
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("{name} artifact missing bytecode.object"))?
        .parse()
        .with_context(|| format!("parse {name} bytecode"))?;
    let receipt = provider
        .send_transaction(
            alloy::rpc::types::TransactionRequest::default()
                .with_deploy_code([code.as_ref(), ctor_args].concat()),
        )
        .await
        .with_context(|| format!("{name} deploy send"))?
        .get_receipt()
        .await
        .with_context(|| format!("{name} deploy receipt"))?;
    anyhow::ensure!(receipt.status(), "{name} deploy reverted");
    receipt
        .contract_address
        .with_context(|| format!("{name} deploy produced no contract address"))
}

/// Deploy the `LoopbackCctp` that plays CCTP V2 on both sides of the chain, at
/// domain 0.
pub(crate) async fn deploy_loopback_cctp<P: Provider>(
    provider: &P,
    contracts: &Path,
) -> anyhow::Result<Address> {
    deploy_artifact(
        provider,
        contracts,
        "LoopbackCctp",
        &(0u32,).abi_encode_params(),
    )
    .await
}

/// The `contracts.<name>` address the manifest at `path` records.
pub(crate) fn manifest_contract(path: &Path, name: &str) -> anyhow::Result<Address> {
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    json.get("contracts")
        .and_then(|contracts| contracts.get(name))
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("manifest {} missing contracts.{name}", path.display()))?
        .parse()
        .with_context(|| format!("parse manifest address contracts.{name}"))
}

/// Run `forge script DeployPaymentPool.s.sol` from `deployer` (`deployer_key`)
/// against `rpc_url`: one `PaymentPool` settling in `usdc` and sweeping
/// through `cctp` to `receiver`. Returns the pool `payment_manifest` records.
pub(crate) async fn deploy_payment_pool(
    contracts: &Path,
    rpc_url: &str,
    (deployer_key, deployer): (&str, &str),
    (usdc, cctp, receiver): (Address, Address, Address),
    payment_manifest: &Path,
) -> anyhow::Result<Address> {
    let mut cmd = tokio::process::Command::new("forge");
    cmd.current_dir(contracts)
        .args([
            "script",
            "script/DeployPaymentPool.s.sol:DeployPaymentPool",
            "--rpc-url",
            rpc_url,
            "--broadcast",
            "--private-key",
            deployer_key,
            "--sender",
            deployer,
        ])
        .env("USDC_ADDRESS", usdc.to_string())
        .env("CCTP_TOKEN_MESSENGER", cctp.to_string())
        .env("SWEEP_RECIPIENT", receiver.to_string())
        .env("PAUSER", deployer)
        .env("FORCE_OVERWRITE_MANIFEST", "true")
        .kill_on_drop(true);
    let out = tokio::time::timeout(POOL_DEPLOY_TIMEOUT, cmd.output())
        .await
        .context("forge script DeployPaymentPool timed out")?
        .context("spawn `forge script DeployPaymentPool` (is foundry installed?)")?;
    anyhow::ensure!(
        out.status.success(),
        "forge script DeployPaymentPool failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    manifest_contract(payment_manifest, "PaymentPool")
}
