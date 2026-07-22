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
//! Deliberately out of scope: proving `H` becomes servable *as an origin*. The
//! daemon can already do it — `crates/node/src/dht/chain_origin_directory.rs`
//! resolves `namespaceOf(H)` → assigned origins → operator, and the runtime
//! selects it whenever the registry addresses are configured. What is missing
//! is the *fixture*: driving it end-to-end needs an `OriginAssignment` proposal,
//! governance activation of that assignment, and a bonded operator to serve it.
//! That setup is G-NODE-08's, so this file stops at the on-chain claim state.
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

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_e2e::assert::expect_revert;
use decdn_e2e::bindings::PublisherRegistry;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::decdn_command;
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

/// How far short of `readyAt` the pre-timelock leg warps. Anvil derives block
/// timestamps from wall-clock, so this has to absorb the wall time a few RPC
/// round-trips take on a loaded CI runner — minutes of slack, not seconds.
const WARP_MARGIN: u64 = 600;

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
    // The CLI's error text alone cannot tell `AlreadyClaimed` apart from an RPC
    // hiccup or an out-of-gas publisher, so pin the *guard* down against the
    // binding: the same call must revert with exactly `AlreadyClaimed`.
    let publisher_registry =
        PublisherRegistry::new(chain.addrs().publisher_registry, publisher.provider(&chain));
    expect_revert::<_, PublisherRegistry::AlreadyClaimed>(
        publisher_registry
            .claimContent(U256::from(ns1), hash_key)
            .call()
            .await,
        "re-claiming H into the same namespace",
    )?;
    let after = registry.namespaceOf(hash_key).call().await?;
    anyhow::ensure!(
        after == vec![U256::from(ns1)],
        "a rejected re-claim must not duplicate the entry, got {after:?}"
    );

    // --- Negative: multi-claim union resolves. ------------------------------
    // A second namespace claiming the same H does NOT displace the first: both
    // ids are returned (ADR 002 § Multi-claim semantics), in claim order —
    // `claimContent` appends to `_claimingNamespaces` (`PublisherRegistry.sol`).
    // The ADR mandates the union; the ordering is the contract's.
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

    // --- Negative: only the namespace owner may claim into it. --------------
    // Every claim above was made by the namespace's own owner, so without this
    // a registry that dropped the owner check entirely would still pass.
    // A bare signer, not a `Publisher`: this identity only makes direct contract
    // calls, and standing up a second encrypted keystore costs seconds of scrypt.
    let stranger = PrivateKeySigner::random();
    chain.fund_eth(stranger.address(), 10).await?;
    let other_hash = B256::from(*Hash::new(b"g-origin-01 someone else's content").as_bytes());
    expect_revert::<_, PublisherRegistry::NotNamespaceOwner>(
        PublisherRegistry::new(
            chain.addrs().publisher_registry,
            chain.provider_for(&stranger),
        )
        .claimContent(U256::from(ns1), other_hash)
        .call()
        .await,
        "a non-owner claiming into someone else's namespace",
    )?;
    anyhow::ensure!(
        registry.namespaceOf(other_hash).call().await?.is_empty(),
        "a rejected non-owner claim must not register the hash"
    );

    Ok(())
}

async fn run_transfer() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let owner = PrivateKeySigner::random();
    let recipient = PrivateKeySigner::random();
    let stranger = PrivateKeySigner::random();
    chain.fund_eth(recipient.address(), 10).await?;
    chain.fund_eth(stranger.address(), 10).await?;

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
    // `readyAt` is exactly `timelock` past the block that ran the initiate — the
    // receipt names that block, so this is an equality, not a slack window. A
    // one-sided `>= now + timelock` would also pass for `now + 700 days`.
    let init_block = receipt
        .block_number
        .context("initiateNamespaceTransfer receipt carried no block number")?;
    let init_ts = block_timestamp(chain.admin(), init_block).await?;
    anyhow::ensure!(
        pending.readyAt == init_ts + timelock,
        "readyAt ({}) must be exactly {timelock}s past the initiating block ({init_ts})",
        pending.readyAt
    );

    // --- Cancel is the owner's escape hatch, and it really un-queues. -------
    let owner_registry = PublisherRegistry::new(registry_addr, &owner_provider);
    let cancel = owner_registry
        .cancelNamespaceTransfer(ns)
        .send()
        .await
        .context("cancelNamespaceTransfer send")?
        .get_receipt()
        .await
        .context("cancelNamespaceTransfer receipt")?;
    anyhow::ensure!(cancel.status(), "cancelNamespaceTransfer reverted");
    anyhow::ensure!(
        read.pendingTransfer(ns).call().await?.newOwner == Address::ZERO,
        "cancel must clear the pending transfer"
    );
    let recipient_registry = PublisherRegistry::new(registry_addr, chain.provider_for(&recipient));
    expect_revert::<_, PublisherRegistry::NoPendingTransfer>(
        recipient_registry
            .finalizeNamespaceTransfer(ns)
            .call()
            .await,
        "finalizing a cancelled transfer",
    )?;

    // Re-queue it so the timelock legs below have something to finalize.
    let receipt = owner_registry
        .initiateNamespaceTransfer(ns, recipient.address())
        .send()
        .await
        .context("re-initiateNamespaceTransfer send")?
        .get_receipt()
        .await
        .context("re-initiateNamespaceTransfer receipt")?;
    anyhow::ensure!(receipt.status(), "re-initiateNamespaceTransfer reverted");
    let pending = read.pendingTransfer(ns).call().await?;

    // --- Before the timelock elapses: finalization is rejected. -------------
    // Warp to comfortably short of `readyAt` rather than shaving a second off
    // it: anvil derives block timestamps from wall-clock, so a loaded runner
    // can overshoot a tight margin and turn a real gate into a spurious pass.
    // The margin is then asserted, so an overshoot fails as an overshoot.
    time::increase_time(chain.admin(), timelock - WARP_MARGIN).await?;
    let head = chain.head_timestamp().await?;
    anyhow::ensure!(
        head < pending.readyAt,
        "warp overshot readyAt ({}) — head is {head}; widen WARP_MARGIN",
        pending.readyAt
    );
    expect_revert::<_, PublisherRegistry::TransferNotReady>(
        recipient_registry
            .finalizeNamespaceTransfer(ns)
            .call()
            .await,
        "finalizing before readyAt",
    )?;

    // --- Only the pending owner may finalize, ever. -------------------------
    expect_revert::<_, PublisherRegistry::NotPendingOwner>(
        PublisherRegistry::new(registry_addr, chain.provider_for(&stranger))
            .finalizeNamespaceTransfer(ns)
            .call()
            .await,
        "a third party finalizing someone else's transfer",
    )?;

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
    /// The same key the keystore holds, for the assertions that go straight to
    /// the contract instead of through the CLI.
    signer: PrivateKeySigner,
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
        // Load the key back out of the keystore the CLI will itself decrypt, so
        // the direct-contract assertions sign as exactly the same identity.
        let signer =
            eth_identity::load_signer(&eth_identity::keystore_path(dir.path()), KEYSTORE_PASSWORD)
                .context("load publisher keystore")?;
        chain.fund_eth(address, 10).await?;
        Ok(Self {
            data_dir: dir.path().to_path_buf(),
            _dir: dir,
            address,
            signer,
        })
    }

    /// A provider signing as this publisher, for direct contract calls.
    fn provider(&self, chain: &ChainFixture) -> alloy::providers::DynProvider {
        chain.provider_for(&self.signer)
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

/// The timestamp of block `number`.
async fn block_timestamp<P: Provider>(provider: &P, number: u64) -> anyhow::Result<u64> {
    Ok(provider
        .get_block(alloy::eips::BlockId::number(number))
        .await
        .context("get block by number")?
        .with_context(|| format!("block {number} not found"))?
        .header
        .timestamp)
}
