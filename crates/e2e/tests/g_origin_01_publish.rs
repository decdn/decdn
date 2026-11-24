//! G-ORIGIN-01 — namespace creation, driven through the `decdn publish` CLI
//! (issue #1038), against a real anvil deployment of `PublisherRegistry`.
//!
//! A publisher with a real encrypted keystore runs `decdn publish namespace
//! create` as a subprocess, and the on-chain `ownerOf(id)` is asserted to be
//! the signer. A second create mints a distinct id, also owned by the signer,
//! and the reserved namespace 0 is confirmed unowned. There is no per-hash
//! on-chain claim (ADR 002 § Hash-to-namespace association) — content is bound
//! to a namespace off-chain at fetch time. The namespace ownership-transfer
//! timelock is covered by the forge suite (`PublisherRegistry.t.sol`).
//!
//! Deliberately out of scope: proving content becomes servable *as an origin*.
//! That needs an `OriginAssignment` publisher vetting, a seated origin, and a
//! bonded operator to serve it — G-NODE-08's setup — so this file stops at the
//! namespace lifecycle.
//!
//! ```bash
//! cargo build -p decdn-cli   # the tests exec the built binary
//! cargo nextest run -p decdn-e2e --features anvil-e2e g_origin_01
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units
)]

use std::path::PathBuf;
use std::time::Duration;

use alloy::primitives::{Address, U256};
use anyhow::Context;
use decdn_e2e::bindings::PublisherRegistry;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::decdn_command;
use decdn_incentive::eth_identity;

/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
/// Cleanup (anvil kill) runs on drop even on timeout.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Password for the publisher's throwaway keystore; passed to the CLI through
/// `DECDN_KEYSTORE_PASSWORD` so no prompt is ever reached.
const KEYSTORE_PASSWORD: &str = "e2e-publisher-password";

#[tokio::test(flavor = "multi_thread")]
async fn namespace_create_mints_owned_ids() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_publish()))
        .await
        .context("publish e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run_publish() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let publisher = Publisher::new(&chain).await?;
    let registry = PublisherRegistry::new(chain.addrs().publisher_registry, chain.admin());

    // `decdn publish namespace create` mints a fresh namespace owned by the
    // signer. There is no per-hash on-chain claim (ADR 002 § Hash-to-namespace
    // association): content is bound to a namespace off-chain at fetch time.
    let ns1 = publisher.namespace_create(&chain)?;
    anyhow::ensure!(
        ns1 >= 1,
        "namespace ids start at 1 (0 is reserved), got {ns1}"
    );
    anyhow::ensure!(
        registry.ownerOf(U256::from(ns1)).call().await? == publisher.address,
        "the CLI signer must own the namespace it created"
    );

    // A second create mints a distinct id, also owned by the signer.
    let ns2 = publisher.namespace_create(&chain)?;
    anyhow::ensure!(
        ns2 != ns1,
        "createNamespace must mint a fresh id, got {ns2}"
    );
    anyhow::ensure!(
        registry.ownerOf(U256::from(ns2)).call().await? == publisher.address,
        "the CLI signer must own its second namespace"
    );

    // Namespace 0 is reserved — no publisher, no authorized origins (ADR 002 §
    // Namespace 0) — so it is never owned.
    anyhow::ensure!(
        registry.ownerOf(U256::ZERO).call().await? == Address::ZERO,
        "namespace 0 is reserved and must be unowned"
    );

    Ok(())
}

/// A publisher identity backed by a real encrypted keystore on disk — the same
/// artifact `decdn key-gen` produces, so the CLI subprocesses below decrypt it
/// exactly as an operator's would. The address is funded for gas.
struct Publisher {
    /// Holds the keystore; dropping it removes the directory.
    _dir: tempfile::TempDir,
    data_dir: PathBuf,
    address: Address,
}

impl Publisher {
    async fn new(chain: &ChainFixture) -> anyhow::Result<Self> {
        let dir = tempfile::tempdir().context("tempdir")?;
        // The keystore loader rejects a group/world-readable data dir.
        #[cfg(unix)]
        std::fs::set_permissions(
            dir.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .context("chmod data dir 0o700")?;
        let address = eth_identity::generate_and_persist(dir.path(), KEYSTORE_PASSWORD, false)
            .context("generate publisher keystore")?;
        chain.fund_eth(address, 10).await?;
        Ok(Self {
            data_dir: dir.path().to_path_buf(),
            _dir: dir,
            address,
        })
    }

    /// `decdn publish namespace create` → the minted namespace id.
    fn namespace_create(&self, chain: &ChainFixture) -> anyhow::Result<u64> {
        let out = self.run(chain, &["publish", "namespace", "create"])?;
        // `status` distinguishes a confirmed create from a broadcast whose
        // receipt could not be read; `submitted` is true for both.
        anyhow::ensure!(
            out["status"] == serde_json::json!("created"),
            "namespace create did not confirm: {out}"
        );
        out["namespace_id"]
            .as_u64()
            .context("namespace create output carried no numeric namespace_id")
    }

    /// Run the `decdn` binary with this publisher's keystore and the fixture's
    /// chain coordinates, returning the parsed `--json` stdout. A non-zero exit
    /// is an error carrying stderr (the CLI's `Context` chain).
    fn run(&self, chain: &ChainFixture, args: &[&str]) -> anyhow::Result<serde_json::Value> {
        // `decdn_command` pins `HOME` to this publisher's `0o700` tempdir. That
        // is load-bearing here, not hygiene: this is the one journey that
        // passes no `--config`, so without it the CLI reads `~/.decdn/node.toml`
        // from the machine running the test. Flags still win (chain_ctx
        // resolves flag > config > default), but the call below passes no
        // `--keystore` — so a real config's `blockchain.eth_keystore` fills that
        // gap and the CLI decrypts the developer's keystore with
        // `KEYSTORE_PASSWORD` instead of this tempdir's (#1332).
        let output = decdn_command(&self.data_dir, KEYSTORE_PASSWORD)?
            .args(args)
            .args([
                "--rpc-url",
                &chain.rpc_url(),
                "--chain-id",
                &chain.chain_id().to_string(),
                "--publisher-registry-address",
                &chain.addrs().publisher_registry.to_string(),
                "--data-dir",
            ])
            .arg(&self.data_dir)
            .arg("--json")
            .output()
            .context("spawn decdn CLI")?;
        anyhow::ensure!(
            output.status.success(),
            "`decdn {}` failed ({}):\n{}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).with_context(|| {
            format!(
                "parse `decdn {}` JSON output: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stdout)
            )
        })
    }
}
