//! G-ORIGIN-01 — namespace publish + claim, driven through the `decdn publish`
//! CLI (issue #1038).
//!
//! Two journeys against a real anvil deployment of `PublisherRegistry`:
//!
//! - **`namespace_create_claim_and_union`:** a publisher with a real encrypted
//!   keystore runs `decdn publish namespace create` and `decdn publish claim H`
//!   as subprocesses, and the on-chain `namespaceOf(H)` is asserted to contain
//!   the created id. The negatives ride the same fixture: re-claiming `H` into
//!   the *same* namespace fails (append-only — the contract reverts
//!   `AlreadyClaimed`) and leaves `namespaceOf(H)` a single entry, while
//!   claiming `H` into a *second* namespace resolves as a union (both ids
//!   present, in claim order).
//! - **`namespace_transfer_respects_timelock`:** the 2-step ownership transfer
//!   cannot be finalized before `namespaceTransferTimelock` (7 days by default)
//!   has elapsed, and does transfer ownership once the chain clock is warped
//!   past `pendingTransfer.readyAt`. There is no `publish namespace transfer`
//!   CLI subcommand, so this leg drives the contract bindings directly.
//!
//! Deliberately out of scope: proving `H` becomes servable *as an origin*. That
//! needs chain-backed origin recognition in the daemon (`ChainOriginDirectory`),
//! which does not exist — `crates/node/src/dht/origin.rs` ships only the trait
//! plus the TOML-driven `ConfigOriginDirectory`. That half belongs to G-NODE-08.
//!
//! ```bash
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

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_e2e::bindings::PublisherRegistry;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::time;
use decdn_incentive::eth_identity;

/// Overall ceiling so an unbounded await fails fast with a clear message.
/// Cleanup (anvil kill) runs on drop even on timeout.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(600);

/// Password for the publisher's throwaway keystore; passed to the CLI through
/// `DECDN_KEYSTORE_PASSWORD` so no prompt is ever reached.
const KEYSTORE_PASSWORD: &str = "e2e-publisher-password";

/// ADR 002 default `namespaceTransferTimelock`.
const SEVEN_DAYS: u64 = 7 * 24 * 60 * 60;

#[tokio::test(flavor = "multi_thread")]
async fn namespace_create_claim_and_union() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_publish()))
        .await
        .context("publish e2e exceeded the overall timeout")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn namespace_transfer_respects_timelock() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_transfer()))
        .await
        .context("namespace transfer e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run_publish() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let publisher = Publisher::new(&chain).await?;

    // The content hash `H` — a real BLAKE3 hash of a payload, as a publisher
    // would produce from `decdn bundle create`.
    let hash = Hash::new(b"g-origin-01 publishable content");
    let hash_hex = alloy::hex::encode(hash.as_bytes());
    let hash_key = B256::from(*hash.as_bytes());

    // Nothing has claimed H yet — the default-open case (empty, never reverts).
    let registry = PublisherRegistry::new(chain.addrs().publisher_registry, chain.admin());
    anyhow::ensure!(
        registry.namespaceOf(hash_key).call().await?.is_empty(),
        "namespaceOf must be empty before any claim"
    );

    // --- Happy path: create a namespace, claim H into it. -------------------
    let ns1 = publisher.namespace_create(&chain)?;
    anyhow::ensure!(
        ns1 >= 1,
        "namespace ids start at 1 (0 is reserved), got {ns1}"
    );
    anyhow::ensure!(
        registry.ownerOf(U256::from(ns1)).call().await? == publisher.address,
        "the CLI signer must own the namespace it created"
    );

    publisher.claim(&chain, &hash_hex, ns1)?;
    let claimed = registry.namespaceOf(hash_key).call().await?;
    anyhow::ensure!(
        claimed == vec![U256::from(ns1)],
        "namespaceOf(H) must be exactly [{ns1}] after the first claim, got {claimed:?}"
    );

    // --- Negative: append-only / idempotent. --------------------------------
    // Re-claiming H into the SAME namespace is rejected (`AlreadyClaimed`), and
    // — the property that actually matters — leaves the claim set unduplicated.
    let err = publisher
        .claim(&chain, &hash_hex, ns1)
        .err()
        .context("re-claiming the same hash into the same namespace must fail")?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("claimContent"),
        "the failure must come from claimContent, got: {msg}"
    );
    let after = registry.namespaceOf(hash_key).call().await?;
    anyhow::ensure!(
        after == vec![U256::from(ns1)],
        "a rejected re-claim must not duplicate the entry, got {after:?}"
    );

    // --- Negative: multi-claim union resolves. ------------------------------
    // A second namespace claiming the same H does NOT displace the first: both
    // ids are returned, in claim order (ADR 002 § Multi-claim semantics).
    let ns2 = publisher.namespace_create(&chain)?;
    anyhow::ensure!(ns2 != ns1, "createNamespace must mint a fresh id");
    publisher.claim(&chain, &hash_hex, ns2)?;
    let union = registry.namespaceOf(hash_key).call().await?;
    anyhow::ensure!(
        union == vec![U256::from(ns1), U256::from(ns2)],
        "namespaceOf(H) must union both claiming namespaces, got {union:?}"
    );
    for ns in [ns1, ns2] {
        anyhow::ensure!(
            registry.hasClaimed(U256::from(ns), hash_key).call().await?,
            "hasClaimed({ns}, H) must agree with namespaceOf"
        );
    }

    Ok(())
}

async fn run_transfer() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let owner = PrivateKeySigner::random();
    let recipient = PrivateKeySigner::random();
    chain.fund_eth(recipient.address(), 10).await?;

    // `create_namespace` funds the owner and mints the id.
    let ns = chain.create_namespace(&owner).await?;
    let registry_addr = chain.addrs().publisher_registry;
    let read = PublisherRegistry::new(registry_addr, chain.admin());
    anyhow::ensure!(
        read.ownerOf(ns).call().await? == owner.address(),
        "namespace must start owned by its creator"
    );

    // The timelock is the ADR 002 default: 7 days.
    let timelock = read.namespaceTransferTimelock().call().await?;
    anyhow::ensure!(
        timelock == SEVEN_DAYS,
        "expected the 7-day default transfer timelock, got {timelock}s"
    );

    // Owner queues the transfer.
    let owner_provider = chain.provider_for(&owner);
    let receipt = PublisherRegistry::new(registry_addr, &owner_provider)
        .initiateNamespaceTransfer(ns, recipient.address())
        .send()
        .await
        .context("initiateNamespaceTransfer send")?
        .get_receipt()
        .await
        .context("initiateNamespaceTransfer receipt")?;
    anyhow::ensure!(receipt.status(), "initiateNamespaceTransfer reverted");

    let pending = read.pendingTransfer(ns).call().await?;
    anyhow::ensure!(
        pending.newOwner == recipient.address(),
        "pendingTransfer must name the recipient"
    );
    let head = chain.head_timestamp().await?;
    anyhow::ensure!(
        pending.readyAt >= head + timelock - 5,
        "readyAt ({}) must be ~{timelock}s past the head ({head})",
        pending.readyAt
    );

    // --- Before the timelock elapses: finalization is rejected. -------------
    let recipient_registry = PublisherRegistry::new(registry_addr, chain.provider_for(&recipient));
    // Warp to one second short of `readyAt` — so the rejection is the timelock
    // gate specifically, not merely "no time has passed".
    time::advance_to(chain.admin(), pending.readyAt - 2).await?;
    anyhow::ensure!(
        recipient_registry
            .finalizeNamespaceTransfer(ns)
            .call()
            .await
            .is_err(),
        "finalizeNamespaceTransfer must revert before readyAt"
    );
    anyhow::ensure!(
        read.ownerOf(ns).call().await? == owner.address(),
        "ownership must not move before the timelock elapses"
    );

    // --- After the timelock elapses: the recipient can finalize. ------------
    time::advance_to(chain.admin(), pending.readyAt).await?;
    let finalize = recipient_registry
        .finalizeNamespaceTransfer(ns)
        .send()
        .await
        .context("finalizeNamespaceTransfer send")?
        .get_receipt()
        .await
        .context("finalizeNamespaceTransfer receipt")?;
    anyhow::ensure!(finalize.status(), "finalizeNamespaceTransfer reverted");
    anyhow::ensure!(
        read.ownerOf(ns).call().await? == recipient.address(),
        "ownership must move to the recipient once the timelock has elapsed"
    );
    let cleared = read.pendingTransfer(ns).call().await?;
    anyhow::ensure!(
        cleared.newOwner == Address::ZERO,
        "the pending transfer must be cleared on finalization"
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
        anyhow::ensure!(
            out["submitted"] == serde_json::json!(true),
            "namespace create reported submitted=false: {out}"
        );
        out["namespace_id"]
            .as_u64()
            .context("namespace create output carried no numeric namespace_id")
    }

    /// `decdn publish claim <hash> --namespace <id>`.
    fn claim(&self, chain: &ChainFixture, hash_hex: &str, namespace: u64) -> anyhow::Result<()> {
        let ns = namespace.to_string();
        let out = self.run(chain, &["publish", "claim", hash_hex, "--namespace", &ns])?;
        anyhow::ensure!(
            out["submitted"] == serde_json::json!(true),
            "claim reported submitted=false: {out}"
        );
        Ok(())
    }

    /// Run the `decdn` binary with this publisher's keystore and the fixture's
    /// chain coordinates, returning the parsed `--json` stdout. A non-zero exit
    /// is an error carrying stderr (the CLI's `Context` chain).
    fn run(&self, chain: &ChainFixture, args: &[&str]) -> anyhow::Result<serde_json::Value> {
        let output = Command::new(decdn_bin()?)
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
            .env("DECDN_KEYSTORE_PASSWORD", KEYSTORE_PASSWORD)
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

/// Locate the built `decdn` binary next to the test executable
/// (`target/<profile>/decdn`), falling back to `DECDN_BIN`. Mirrors
/// `decdn_e2e::node`'s `decdn-node` lookup.
fn decdn_bin() -> anyhow::Result<PathBuf> {
    if let Some(p) = std::env::var_os("DECDN_BIN") {
        return Ok(PathBuf::from(p));
    }
    let exe = std::env::current_exe().context("current_exe")?;
    let profile_dir: &Path = exe
        .parent()
        .and_then(Path::parent)
        .context("resolve target profile dir")?;
    let bin = profile_dir.join(if cfg!(windows) { "decdn.exe" } else { "decdn" });
    anyhow::ensure!(
        bin.exists(),
        "decdn binary not found at {}; run `cargo build -p decdn-cli` first (or set DECDN_BIN)",
        bin.display()
    );
    Ok(bin)
}
