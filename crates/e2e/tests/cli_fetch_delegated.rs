//! Cross-layer proof of the capability-DELEGATION settlement path
//! (publisher-pays, ADR 003 §Capability delegation), driven entirely through the
//! shipped `decdn` binary:
//!
//!   1. an OWNER opens and funds a `PaymentPool` (`decdn pool open`),
//!   2. the OWNER delegates a bounded, time-boxed spend on it to a SEPARATE
//!      signer key via an owner-signed EIP-712 capability (`decdn pool assign`,
//!      whose `dcap1:` token is captured), and
//!   3. the DELEGATE — a distinct keystore that owns no pool and never opens or
//!      tops one up — fetches a blob with that capability (`decdn fetch
//!      --capability`), paying voucher-only from the owner's deposit.
//!
//! It is the delegated twin of `anvil_pool_redeem` (the self-owned journey where
//! `signer == owner` and the self-capability's cap is `U256::MAX`). The point of
//! the test is the delegation-SPECIFIC on-chain surface that self-owned redeem
//! cannot reach: on its first redemption the node registers the DELEGATE signer
//! from the owner's capability, so `getAuthorization(pool, delegate).cap` fixes at
//! the assigned FINITE cap (never `U256::MAX`) and `.expiry` at the assigned
//! expiry; the delegate's `(pool, delegate, operator)` lane watermark advances and
//! a `PoolRedeemed` event names the delegate; and the OWNER address is never a
//! signer on that pool — owner ≠ signer delegation, on-chain.
//!
//! CLI-driven rather than a programmatic `ClientFixture` journey because the
//! fixture's client is hardwired to `signer == owner` (it self-issues the
//! `U256::MAX` capability), so it cannot express a distinct delegate; and because
//! the newly-added `pool assign` / `fetch --capability` surface is exactly what
//! the delegation flow ships as, so driving the real subprocesses validates the
//! token round-trip end to end (owner signs → `dcap1:` token → delegate adopts).
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a built `decdn` binary:
//!
//! ```bash
//! cargo build -p decdn-cli
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_fetch_delegated
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

use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::eth::Filter;
use alloy::sol_types::SolEvent;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_common::admin::AdminRpcClient;
use decdn_e2e::assert as e2e_assert;
use decdn_e2e::bindings::PaymentPool;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_e2e::poll;
use decdn_incentive::eth_identity;
use decdn_incentive::{CapabilityGrant, voucher_domain};

const MIB: usize = 1024 * 1024;
/// Owner pool deposit: 10 USDC (ADR 003 recommended minimum).
const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// The delegate's FINITE cumulative spend ceiling — 5 USDC. Deliberately not the
/// deposit and emphatically not `U256::MAX`: the on-chain registered cap must
/// read back as exactly this, proving the delegated capability (not a self-cap)
/// drove the registration. It dwarfs the ~20 `µUSDC` one 2 MiB delivery costs, so
/// it never binds during the fetch.
const DELEGATE_CAP_MICRO_USDC: u64 = 5_000_000;
/// Capability lifetime handed to `pool assign` (`--expiry-secs`). The exact
/// absolute expiry is read back off the decoded token, not recomputed here.
const EXPIRY_SECS: u64 = 3_600;
/// Shared keystore password for both throwaway keystores (owner + delegate). The
/// password is per-invocation env, so one constant serves both roles.
const KEYSTORE_PASSWORD: &str = "delegated-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn delegated_capability_fetch_registers_the_delegate_signer_and_redeems() -> anyhow::Result<()>
{
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("delegated-capability fetch e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's pool / \
              capability / redemption state, so decomposing it would thread state through helpers \
              without reducing the journey's length or making it easier to follow"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // A 2 MiB blob: at the node's 10 µUSDC/MiB rate one delivery accrues ~20 µUSDC,
    // clearing the daemon's 10 µUSDC redeem threshold so a single fetch triggers an
    // on-chain redemption — the registration event this test asserts on.
    let blob = vec![0xD3u8; 2 * MIB];
    let blob_hash = Hash::new(&blob);
    let (node, hash) = NodeFixture::launch(&chain, "US", &blob).await?;
    anyhow::ensure!(
        hash == blob_hash,
        "seeded blob hash mismatch: {hash} vs {blob_hash}"
    );
    let operator = node.operator_addr();
    let payment_pool = chain.addrs().payment_pool;

    // ---- OWNER: a funded keystore that opens and funds the pool ----
    let owner_dir = tempfile::tempdir().context("owner tempdir")?;
    #[cfg(unix)]
    std::fs::set_permissions(
        owner_dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod owner dir 0o700")?;
    eth_identity::generate_and_persist(owner_dir.path(), KEYSTORE_PASSWORD, false)
        .context("generate owner keystore")?;
    let owner_keystore = eth_identity::keystore_path(owner_dir.path());
    let owner = eth_identity::load_signer(&owner_keystore, KEYSTORE_PASSWORD)
        .context("load owner signer")?;
    let owner_addr = owner.address();
    chain.fund_eth(owner_addr, 100).await?;
    chain
        .mint_usdc(
            owner_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await
        .context("mint owner USDC")?;

    // ---- DELEGATE: a DISTINCT keystore that owns no pool ----
    //
    // Deliberately UNFUNDED (no ETH, no USDC): the delegated fetch signs vouchers
    // and its client binding off-chain and issues no transaction of its own, so a
    // delegate that cannot pay gas is exactly the publisher-pays posture. If any
    // step tried to `openPool`/`topUp` as the delegate, it would fail here for lack
    // of gas — a second, structural guard that the delegate never funds anything.
    let delegate_dir = tempfile::tempdir().context("delegate tempdir")?;
    #[cfg(unix)]
    std::fs::set_permissions(
        delegate_dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod delegate dir 0o700")?;
    eth_identity::generate_and_persist(delegate_dir.path(), KEYSTORE_PASSWORD, false)
        .context("generate delegate keystore")?;
    let delegate_keystore = eth_identity::keystore_path(delegate_dir.path());
    let delegate = eth_identity::load_signer(&delegate_keystore, KEYSTORE_PASSWORD)
        .context("load delegate signer")?;
    let delegate_addr = delegate.address();
    anyhow::ensure!(
        delegate_addr != owner_addr,
        "the delegate must be a distinct key from the owner"
    );

    // ---- OWNER opens the pool (`decdn pool open`) → capture the poolId ----
    let open_out = run_cli_capture(
        owner_dir.path(),
        &pool_open_argv(
            &chain,
            DEPOSIT_MICRO_USDC,
            &owner_keystore,
            owner_dir.path(),
        ),
        "decdn pool open",
    )
    .await?;
    let pool_id = parse_pool_id(&open_out).context("parse poolId from `pool open` output")?;

    // ---- OWNER delegates a bounded spend (`decdn pool assign`) → capture token ----
    let assign_out = run_cli_capture(
        owner_dir.path(),
        &pool_assign_argv(
            &chain,
            pool_id,
            delegate_addr,
            DELEGATE_CAP_MICRO_USDC,
            EXPIRY_SECS,
            &owner_keystore,
            owner_dir.path(),
        ),
        "decdn pool assign",
    )
    .await?;
    let token = extract_dcap1_token(&assign_out)
        .context("extract the dcap1: token from `pool assign` output")?;

    // Decode the token in-test: it is the source of truth for what the owner
    // signed, so the on-chain registration is asserted against the token's own
    // fields rather than against constants the CLI might have transformed. This
    // also re-checks the `dcap1:` codec round-trip the delegate relies on.
    let grant = CapabilityGrant::from_token(&token).context("decode captured dcap1 token")?;
    anyhow::ensure!(
        grant.pool_id == pool_id,
        "token pool {} does not match the opened pool {pool_id}",
        grant.pool_id
    );
    anyhow::ensure!(
        grant.signer == delegate_addr,
        "token authorizes {} but the delegate key is {delegate_addr}",
        grant.signer
    );
    anyhow::ensure!(
        grant.spending_cap == DELEGATE_CAP_MICRO_USDC,
        "token cap {} does not match the assigned finite cap {DELEGATE_CAP_MICRO_USDC}",
        grant.spending_cap
    );
    // The owner recovered from the token's signature must be the pool owner — the
    // node enforces exactly this against the pool's on-chain owner at redemption.
    let voucher_dom = voucher_domain(chain.chain_id(), payment_pool);
    anyhow::ensure!(
        grant.owner(&voucher_dom).context("recover token owner")? == owner_addr,
        "token owner does not recover to the pool owner {owner_addr}"
    );

    // ---- DELEGATE fetches with the capability (`decdn fetch --capability`) ----
    //
    // Pin the log scan's lower bound BEFORE the fetch. The redemption this journey
    // asserts on lands in some block after this one, and an `eth_getLogs` filter
    // that names no `fromBlock` is not a full-history scan: alloy omits the field
    // when it is `None` and anvil then reads the range as `latest..latest` — the
    // head block alone. Anchoring here keeps the scan bounded to this fetch while
    // still covering every block the redemption can land in.
    let fetch_from_block = chain
        .admin()
        .get_block_number()
        .await
        .context("read the pre-fetch block number")?;
    let out = delegate_dir.path().join("blob.bin");
    let fetch_args = delegate_fetch_argv(
        &chain,
        &node,
        &blob_hash,
        &token,
        delegate_dir.path(),
        &delegate_keystore,
        &out,
    );
    run_fetch_until_ready(delegate_dir.path(), &fetch_args).await?;

    let got = std::fs::read(&out).context("read delegated fetch output")?;
    anyhow::ensure!(
        got == blob,
        "delegated fetch produced {} bytes, expected {}",
        got.len(),
        blob.len()
    );

    // ---- Delegation-specific on-chain assertions ----
    //
    // First redemption registers the DELEGATE signer: `getAuthorization.cap` flips
    // from the unregistered zero sentinel to the granted FINITE cap.
    let auth = poll(Duration::from_secs(90), || async {
        let auth =
            e2e_assert::read_authorization(chain.admin(), payment_pool, pool_id, delegate_addr)
                .await?;
        Ok((auth.cap > 0).then_some(auth))
    })
    .await?
    .context("delegate signer was never registered on-chain (getAuthorization.cap stayed zero)")?;
    anyhow::ensure!(
        auth.cap == grant.spending_cap,
        "registered cap {} must equal the delegated capability's FINITE cap {}",
        auth.cap,
        grant.spending_cap
    );
    anyhow::ensure!(
        auth.cap != u64::MAX,
        "the registered cap must be the finite delegated cap, not the self-owned u64::MAX sentinel"
    );
    anyhow::ensure!(
        auth.expiry == grant.expiry,
        "registered expiry {} must equal the capability's expiry {}",
        auth.expiry,
        grant.expiry
    );
    anyhow::ensure!(
        auth.spent > 0 && auth.spent <= auth.cap,
        "the delegate's spent-so-far ({}) must be positive and within the cap ({})",
        auth.spent,
        auth.cap
    );

    // The delegate's own lane watermark advanced — the on-chain paid cumulative a
    // `PoolRedeemed` wrote for `(pool, delegate, operator)`.
    let lane = e2e_assert::read_watermark(
        chain.admin(),
        payment_pool,
        pool_id,
        delegate_addr,
        operator,
    )
    .await?;
    anyhow::ensure!(
        lane.amount > 0 && lane.bytesDelivered > 0,
        "the delegate's lane must have advanced (amount={}, bytesDelivered={})",
        lane.amount,
        lane.bytesDelivered
    );

    // A `PoolRedeemed` event names the DELEGATE as the signer (not the owner).
    // Polled rather than read once: the reads above prove the redemption's STATE
    // landed, which does not by itself make its log queryable, so a one-shot miss
    // here is ambiguous between "no such event" and "not indexed yet".
    let names_delegate = poll(Duration::from_secs(30), || async {
        let filter = Filter::new()
            .address(payment_pool)
            .event_signature(PaymentPool::PoolRedeemed::SIGNATURE_HASH)
            .from_block(fetch_from_block);
        let logs = chain
            .admin()
            .get_logs(&filter)
            .await
            .context("get PoolRedeemed logs")?;
        Ok(logs
            .iter()
            .any(|log| {
                PaymentPool::PoolRedeemed::decode_log_data(&log.inner.data).is_ok_and(|e| {
                    e.poolId == pool_id
                        && e.provider == operator
                        && e.lanes.iter().any(|lane| lane.signer == delegate_addr)
                })
            })
            .then_some(()))
    })
    .await?;
    anyhow::ensure!(
        names_delegate.is_some(),
        "no PoolRedeemed event named the delegate signer {delegate_addr} on pool {pool_id} \
         from block {fetch_from_block}"
    );

    // Owner ≠ signer, on-chain: the OWNER is never a signer on this pool. It funded
    // the deposit but signed no voucher, so it registered no authorization and holds
    // no lane — the whole point of delegation.
    let owner_auth =
        e2e_assert::read_authorization(chain.admin(), payment_pool, pool_id, owner_addr).await?;
    anyhow::ensure!(
        owner_auth.cap == 0 && owner_auth.spent == 0,
        "the owner must never be registered as a signer (cap={}, spent={})",
        owner_auth.cap,
        owner_auth.spent
    );
    let owner_lane =
        e2e_assert::read_watermark(chain.admin(), payment_pool, pool_id, owner_addr, operator)
            .await?;
    anyhow::ensure!(
        owner_lane.amount == 0 && owner_lane.bytesDelivered == 0,
        "the owner must hold no voucher lane on its own pool (amount={}, bytesDelivered={})",
        owner_lane.amount,
        owner_lane.bytesDelivered
    );

    // The delegate could not (and did not) top up: the deposit is exactly what the
    // owner escrowed at open. A delegate top-up is owner-only on-chain and the
    // delegated fetch path disables it, so the deposit never moved.
    let pool = e2e_assert::read_pool(chain.admin(), payment_pool, pool_id).await?;
    anyhow::ensure!(
        pool.owner == owner_addr,
        "on-chain pool owner {} must be the owner keystore {owner_addr}",
        pool.owner
    );
    anyhow::ensure!(
        pool.deposit == DEPOSIT_MICRO_USDC,
        "the deposit must be unchanged at {DEPOSIT_MICRO_USDC} µUSDC (a delegate cannot top up), \
         got {}",
        pool.deposit
    );

    // `NodeFixture` tears the daemon down on drop; there is nothing to await.
    drop(node);
    Ok(())
}

/// Parse the `{"poolId": "0x…"}` document `decdn pool open` prints to stdout.
fn parse_pool_id(stdout: &str) -> anyhow::Result<B256> {
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim())
        .with_context(|| format!("pool open output was not the expected JSON: {stdout:?}"))?;
    let raw = parsed
        .get("poolId")
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("pool open output had no string `poolId`: {stdout:?}"))?;
    B256::from_str(raw).with_context(|| format!("poolId {raw:?} is not a 32-byte hash"))
}

/// Pull the single `dcap1:` token line out of `decdn pool assign`'s human-readable
/// output (it is printed on its own line after a `Token …:` header).
fn extract_dcap1_token(stdout: &str) -> anyhow::Result<String> {
    stdout
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("dcap1:"))
        .map(str::to_string)
        .ok_or_else(|| {
            anyhow::anyhow!("no `dcap1:` token line in `pool assign` output: {stdout:?}")
        })
}

/// Run a `decdn` subcommand to completion, requiring success, and return its
/// captured stdout. For the deterministic (non-node-racing) `pool` subcommands.
async fn run_cli_capture(
    home: &std::path::Path,
    args: &[String],
    what: &str,
) -> anyhow::Result<String> {
    let output = tokio::process::Command::from(decdn_command(home, KEYSTORE_PASSWORD)?)
        .args(args)
        .output()
        .await
        .with_context(|| format!("spawn {what}"))?;
    anyhow::ensure!(
        output.status.success(),
        "{what} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).with_context(|| format!("{what} stdout was not UTF-8"))
}

/// Run `decdn fetch`, retrying until the node's chain watcher has observed the
/// owner's freshly-opened pool (`decdn fetch` has no internal retry for that
/// race). The only expected transient here is the readiness `NotFound`; the
/// delegated cap is far larger than one delivery costs, so a `SpendingCapExhausted` cannot
/// arise to be masked by the retry.
async fn run_fetch_until_ready(home: &std::path::Path, args: &[String]) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = tokio::process::Command::from(decdn_command(home, KEYSTORE_PASSWORD)?)
            .arg("fetch")
            .args(args)
            .output()
            .await
            .context("spawn decdn fetch")?;
        if output.status.success() {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "delegated decdn fetch never succeeded; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "delegated fetch not ready; retrying after watcher catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// `decdn pool open --deposit-micro-usdc …` argv (chain coordinates as flags so no
/// config file is needed).
fn pool_open_argv(
    chain: &ChainFixture,
    deposit_micro_usdc: u64,
    keystore: &std::path::Path,
    data_dir: &std::path::Path,
) -> Vec<String> {
    vec![
        "pool".into(),
        "open".into(),
        "--deposit-micro-usdc".into(),
        deposit_micro_usdc.to_string(),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
        "--chain-id".into(),
        chain.chain_id().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
    ]
}

/// `decdn pool assign …` argv: delegate a FINITE `--cap-micro-usdc` spend to
/// `signer`, expiring `--expiry-secs` from now, signed with the owner keystore.
fn pool_assign_argv(
    chain: &ChainFixture,
    pool_id: B256,
    signer: Address,
    cap_micro_usdc: u64,
    expiry_secs: u64,
    keystore: &std::path::Path,
    data_dir: &std::path::Path,
) -> Vec<String> {
    vec![
        "pool".into(),
        "assign".into(),
        "--pool".into(),
        format!("{pool_id:#x}"),
        "--signer".into(),
        format!("{signer}"),
        "--cap-micro-usdc".into(),
        cap_micro_usdc.to_string(),
        "--expiry-secs".into(),
        expiry_secs.to_string(),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
        "--chain-id".into(),
        chain.chain_id().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
    ]
}

/// The `decdn fetch --capability` argv (after the `fetch` subcommand) for the
/// DELEGATE: it adopts the owner's pool via the `dcap1:` token, opens nothing, and
/// never tops up. `--capacity-bond-address` is still required — it is the EIP-712
/// `verifyingContract` the delegate signs its ADR 005 client identity binding
/// against, and the node refuses a paid request that carries no verified binding
/// even for a blob it already holds.
fn delegate_fetch_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: &Hash,
    token: &str,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
    out: &std::path::Path,
) -> Vec<String> {
    vec![
        "--hash".into(),
        hash.to_hex(),
        "-o".into(),
        out.display().to_string(),
        "--capability".into(),
        token.to_string(),
        "--node-id".into(),
        node.node_id().to_string(),
        "--addr".into(),
        format!("127.0.0.1:{}", node.bind_port()),
        "--provider-address".into(),
        format!("{}", node.operator_addr()),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
        "--capacity-bond-address".into(),
        format!("{}", chain.addrs().capacity_bond),
        "--slash-judge-address".into(),
        format!("{}", chain.addrs().slash_judge),
        "--chain-id".into(),
        chain.chain_id().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
    ]
}

/// The registered capability A's cap in micro-USDC: four one-MiB voucher
/// intervals at the fixture node's rate of 10 per MiB. Enough to admit a stream,
/// far below what the 16 MiB blob costs.
const REGISTERED_CAP_MICRO_USDC: u64 = 40;
/// The presented capability B's cap: 5 USDC, far above the blob's cost.
const PRESENTED_CAP_MICRO_USDC: u64 = 5_000_000;
/// The fixture node's quoted rate, in micro-USDC per MiB.
const NODE_RATE_PER_MB: u64 = 10;

#[tokio::test(flavor = "multi_thread")]
async fn presented_capability_is_clamped_to_the_signers_registered_terms() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_clamp()))
        .await
        .context("capability-clamp e2e exceeded the overall timeout")??;
    Ok(())
}

/// The largest voucher cumulative the node has accepted on any lane of
/// `pool_id`, in micro-USDC, from its admin `lanes()` surface. `0` before any.
async fn accepted_on_pool(node: &NodeFixture, pool_id: B256) -> anyhow::Result<u64> {
    let wanted = format!("{pool_id}");
    let resp = node.admin_client()?.lanes().await.context("admin lanes")?;
    Ok(resp
        .lanes
        .iter()
        .filter(|l| l.pool_id.eq_ignore_ascii_case(&wanted))
        .map(|l| l.outstanding_micro_usdc)
        .max()
        .unwrap_or(0))
}

/// Whether the node has accepted a voucher on any lane of `pool_id` since it
/// started, from its admin `lanes()` surface.
async fn voucher_seen_on_pool(node: &NodeFixture, pool_id: B256) -> anyhow::Result<bool> {
    let wanted = format!("{pool_id}");
    let resp = node.admin_client()?.lanes().await.context("admin lanes")?;
    Ok(resp
        .lanes
        .iter()
        .any(|l| l.pool_id.eq_ignore_ascii_case(&wanted) && l.seconds_since_last_voucher.is_some()))
}

/// A fresh 0o700 keystore directory for one role, returning the directory, the
/// keystore path, and the key's address.
fn new_keystore(role: &str) -> anyhow::Result<(tempfile::TempDir, std::path::PathBuf, Address)> {
    let dir = tempfile::tempdir().with_context(|| format!("{role} tempdir"))?;
    #[cfg(unix)]
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .with_context(|| format!("chmod {role} dir 0o700"))?;
    eth_identity::generate_and_persist(dir.path(), KEYSTORE_PASSWORD, false)
        .with_context(|| format!("generate {role} keystore"))?;
    let keystore = eth_identity::keystore_path(dir.path());
    let addr = eth_identity::load_signer(&keystore, KEYSTORE_PASSWORD)
        .with_context(|| format!("load {role} signer"))?
        .address();
    Ok((dir, keystore, addr))
}

/// On-chain registration is write-once per `(pool, signer)` (ADR 003 §Capability
/// delegation). The owner registers the delegate with a small capability A, then
/// signs a second capability B for the same delegate with a far larger cap and a
/// later expiry. The delegate presents B to a node. The node must hold the
/// registered terms, not B's: every voucher it accepts must redeem on-chain.
#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey; each step depends on the previous step's \
              pool / registration / delivery state"
)]
async fn run_clamp() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // 16 MiB at 10 µUSDC/MiB costs ~160 µUSDC: four times the registered cap.
    let blob = vec![0xC1u8; 16 * MIB];
    let blob_hash = Hash::new(&blob);
    let (node, hash) = NodeFixture::launch(&chain, "US", &blob).await?;
    anyhow::ensure!(hash == blob_hash, "seeded blob hash mismatch");
    let operator = node.operator_addr();
    let payment_pool = chain.addrs().payment_pool;

    let (owner_dir, owner_keystore, owner_addr) = new_keystore("owner")?;
    chain.fund_eth(owner_addr, 100).await?;
    chain
        .mint_usdc(
            owner_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await
        .context("mint owner USDC")?;
    let (delegate_dir, delegate_keystore, delegate_addr) = new_keystore("delegate")?;

    let open_out = run_cli_capture(
        owner_dir.path(),
        &pool_open_argv(
            &chain,
            DEPOSIT_MICRO_USDC,
            &owner_keystore,
            owner_dir.path(),
        ),
        "decdn pool open",
    )
    .await?;
    let pool_id = parse_pool_id(&open_out).context("parse poolId from `pool open` output")?;

    let assign = |cap: u64, expiry_secs: u64| {
        pool_assign_argv(
            &chain,
            pool_id,
            delegate_addr,
            cap,
            expiry_secs,
            &owner_keystore,
            owner_dir.path(),
        )
    };
    let token_a = extract_dcap1_token(
        &run_cli_capture(
            owner_dir.path(),
            &assign(REGISTERED_CAP_MICRO_USDC, EXPIRY_SECS),
            "decdn pool assign (A)",
        )
        .await?,
    )?;
    let token_b = extract_dcap1_token(
        &run_cli_capture(
            owner_dir.path(),
            &assign(PRESENTED_CAP_MICRO_USDC, 2 * EXPIRY_SECS),
            "decdn pool assign (B)",
        )
        .await?,
    )?;
    let grant_a = CapabilityGrant::from_token(&token_a).context("decode token A")?;
    let grant_b = CapabilityGrant::from_token(&token_b).context("decode token B")?;
    anyhow::ensure!(
        grant_b.spending_cap > grant_a.spending_cap && grant_b.expiry > grant_a.expiry,
        "capability B must carry a higher cap and a later expiry than A"
    );

    // Register the delegate with capability A. `redeemMany` is permissionless and
    // a batch with a capability and no vouchers only registers, so the admin key
    // stands in for whichever provider landed the delegate's first redemption.
    let pool_contract = PaymentPool::new(payment_pool, chain.admin());
    let receipt = pool_contract
        .redeemMany(vec![PaymentPool::PoolBatch {
            poolId: pool_id,
            capabilities: vec![PaymentPool::CapabilityReg {
                signer: delegate_addr,
                spendingCap: grant_a.spending_cap,
                expiry: grant_a.expiry,
                ownerSig: grant_a.owner_signature.clone().into(),
            }],
            vouchers: vec![],
        }])
        .send()
        .await
        .context("send redeemMany registering capability A")?
        .get_receipt()
        .await
        .context("redeemMany receipt")?;
    decdn_e2e::ensure_mined(&receipt, "redeemMany registering capability A")?;
    let registered =
        e2e_assert::read_authorization(chain.admin(), payment_pool, pool_id, delegate_addr).await?;
    anyhow::ensure!(
        registered.cap == grant_a.spending_cap && registered.expiry == grant_a.expiry,
        "the delegate must be registered with capability A's terms"
    );

    // The delegate fetches presenting capability B. The fetch may fail once the
    // node stops at the registered cap; what matters is what the node accepted.
    let out = delegate_dir.path().join("blob.bin");
    let fetch_args = delegate_fetch_argv(
        &chain,
        &node,
        &blob_hash,
        &token_b,
        delegate_dir.path(),
        &delegate_keystore,
        &out,
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let fetched = tokio::time::timeout(
            Duration::from_secs(60),
            tokio::process::Command::from(decdn_command(delegate_dir.path(), KEYSTORE_PASSWORD)?)
                .arg("fetch")
                .args(&fetch_args)
                .kill_on_drop(true)
                .output(),
        )
        .await;
        let succeeded = matches!(&fetched, Ok(Ok(o)) if o.status.success());
        // Retry only while the node has accepted no voucher on the pool: a fetch
        // that ran before the node saw the fresh pool. Once the node accepted a
        // voucher, the outcome stands; a later fetch meets a drained signer,
        // which the node refuses at admission.
        if succeeded || voucher_seen_on_pool(&node, pool_id).await? {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the delegate fetch never reached the node's paid path: {fetched:?}"
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }

    // The node redeems what it accepted. Wait for that redemption to land, then
    // compare the node's accepted claim against what the chain pays.
    let accepted = accepted_on_pool(&node, pool_id).await?;
    let paid = poll(Duration::from_secs(90), || async {
        let lane = e2e_assert::read_watermark(
            chain.admin(),
            payment_pool,
            pool_id,
            delegate_addr,
            operator,
        )
        .await?;
        Ok((lane.amount > 0).then_some(lane.amount))
    })
    .await?
    .context("the node never redeemed the delegate's lane")?;
    anyhow::ensure!(
        accepted <= registered.cap,
        "the node accepted {accepted} µUSDC of vouchers under capability B, past the delegate's \
         registered cap of {} µUSDC; the chain pays {paid} µUSDC and the remaining {} µUSDC \
         redeems to 0",
        registered.cap,
        accepted.saturating_sub(paid)
    );
    let settled = poll(Duration::from_secs(90), || async {
        let lane = e2e_assert::read_watermark(
            chain.admin(),
            payment_pool,
            pool_id,
            delegate_addr,
            operator,
        )
        .await?;
        Ok((lane.amount >= accepted).then_some(lane.amount))
    })
    .await?;
    anyhow::ensure!(
        settled.is_some(),
        "every voucher the node accepted ({accepted} µUSDC) must redeem on-chain"
    );

    // The registration is write-once: B changed nothing on-chain.
    let after =
        e2e_assert::read_authorization(chain.admin(), payment_pool, pool_id, delegate_addr).await?;
    anyhow::ensure!(
        after.cap == grant_a.spending_cap && after.expiry == grant_a.expiry,
        "the registered terms must stay capability A's"
    );

    // A fetch on the spent signer stops at once with the new-key remedy. A
    // node that reads the signer as spent refuses it at admission `Unfunded`,
    // and a delegated client with no funder ends "funding needed" rather than
    // retrying until it gives up (#2338). A node whose projection has not
    // folded the last redemption yet admits the stream and rejects its first
    // voucher past the registered cap instead; that remedy names a new signer
    // key too.
    let headroom = after.cap.saturating_sub(after.spent);
    anyhow::ensure!(
        U256::from(headroom) < decdn_incentive::floor_micro(NODE_RATE_PER_MB),
        "precondition: the signer must be spent below one chunk, but {headroom} µUSDC of its \
         registered cap is left"
    );
    let started = tokio::time::Instant::now();
    let drained = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::from(decdn_command(delegate_dir.path(), KEYSTORE_PASSWORD)?)
            .arg("fetch")
            .args(&fetch_args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .context("a fetch on the spent signer must stop, not retry until its timeout")?
    .context("run the fetch on the spent signer")?;
    let stderr = String::from_utf8_lossy(&drained.stderr);
    anyhow::ensure!(
        !drained.status.success() && stderr.contains("new signer key"),
        "a fetch on a signer with {headroom} µUSDC of headroom must fail with the new-key \
         remedy (took {:?}): {stderr}",
        started.elapsed()
    );

    drop(node);
    Ok(())
}
