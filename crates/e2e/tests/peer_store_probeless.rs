//! Live anvil-backed e2e for the persisted [`decdn_client::PeerStore`]
//! (#1906): proves `decdn get` populates the store on a
//! normal auto-discovered fetch, and that a second fetch against the same
//! `data_dir` can be served straight from the store without a fresh
//! `CapacityBond` registry read or a `cdn/probe/v1` probe round.
//!
//! Why the whole binary rather than a unit test: `store_fast_path`'s own unit
//! tests (`crates/cli/src/commands/fetch.rs`) already prove the ranking and
//! `min_fresh_candidates` floor in isolation, and `PeerStore`'s unit tests
//! (`crates/client/src/peer_store.rs`) prove the on-disk record shape.
//! What neither can reach is that a REAL `decdn get` subprocess — driven by
//! `discover_provider` end to end — writes records a SEPARATE later
//! `decdn get` subprocess can read back and act on, against a real `CapacityBond`
//! registry and real iroh-dialed nodes. That cross-process round trip through
//! the filesystem is the thing this file exists to prove.
//!
//! **Why three holders.** `crate::PeerStore`'s `MIN_FRESH_CANDIDATES` (the
//! floor `store_fast_path` enforces before it will skip discovery) is `3`, and
//! `discovery::admit_sources` dedupes the ranked set to one node per
//! `eth_address` — so the fast path needs at least three *distinct-operator*
//! candidates the store holds a fresh latency sample for. A single-node
//! fixture (as `cli_fetch_resume`/`cli_fetch_multi_source` use) structurally
//! cannot reach `MIN_FRESH_CANDIDATES`; three independently-bonded holders of
//! the SAME blob is the minimum fixture shape that can.
//!
//! Three journeys, sharing one chain, three holders, and one funded buyer:
//!
//! 1. **Populate + probe-less repeat.** The first auto-discovered fetch probes
//!    all three holders (all report `has_blob:true`) and its off-critical-path
//!    harvest persists identity + a fresh latency sample for each of them —
//!    `<data_dir>/peers/` ends up with three selectable records. A second
//!    fetch (same `data_dir`, no `--rediscover`) then has enough fresh
//!    candidates for `store_fast_path` to engage, which — per its own doc
//!    comment — "takes no `Endpoint`, so it structurally issues no network
//!    probe": bytes still verify, without a probe round. The holders'
//!    `decdn_probe_requests_total` shows it: fetch #2 probes none of them,
//!    before the transfer or during it.
//!    The fast path knows no size, so fetch #2 takes its first size claim
//!    from a whole-blob open whose pull the download adopts as its first leg
//!    (#2633): the holders write the blob's wire bytes once, not once for
//!    the claim and again for the download. The lane that answered the open
//!    connects with no network work, so it is the only lane in the batch
//!    the download plans first, and that plan gives it the whole blob.
//! 2. **Cross-process persistence.** The two `decdn get` invocations above
//!    are separate OS processes sharing only `data_dir` on disk — a passing
//!    fetch #2 that reused the store already demonstrates this, and this
//!    journey additionally asserts the peer record files are present both
//!    right after fetch #1 and still present after fetch #2.
//! 3. **Probe-less stream.** A third fast-path fetch streams a blob longer
//!    than the read-ahead window to stdout (`-o -`). Its size open pulls the
//!    streamed drive's first leg, a read-ahead window of the blob, and the
//!    drive adopts that pull, so no leg the client opens starts at offset 0
//!    (#2644).
//!
//! **Registry-outage fallback is NOT covered here —
//! documented limitation, not a faked test.** `decdn get`'s CLI surface has
//! no way to break the `CapacityBond` registry read in isolation:
//! `--capacity-bond-address` is also the ADR 005 client-binding EIP-712
//! domain's `verifyingContract` (`crates/cli/src/commands/fetch.rs`'s
//! `attach_client_binding`: `bind_node_id_domain(chain.chain_id,
//! capacity_bond)`), computed identically on the node side from ITS OWN
//! (unchanged, correct) config — so pointing the flag anywhere else makes
//! every subsequent delivery request's binding fail `verify_binding` and the
//! node reset the stream (`APP_ERR_MALFORMED_MESSAGE`), regardless of whether
//! `resolve_bootstrap` correctly fell back to `Bootstrap::Cached` first
//! (confirmed empirically: the fallback warning prints, then delivery fails
//! against every cached candidate with `stream reset by peer: error 3`).
//! `--rpc-url` cannot be broken instead: `fetch()` unconditionally calls
//! `contract.usdc().call().await` right after target resolution
//! (`crates/cli/src/commands/fetch.rs::fetch`), before any pool-reuse check,
//! so an unreachable RPC aborts the whole command even when the buyer's
//! existing pool has ample deposit and would otherwise need no on-chain read
//! to be reused. Simulating the outage by pausing the fixture's own anvil
//! process hits the same wall — that same unconditional `usdc()` read would
//! also fail. Covering the fallback needs either a way to point discovery's registry read at
//! a different RPC/contract than the payment path uses, or a fixture seam
//! that fails `CapacityBond.getRegisteredNodes` in isolation (e.g. a
//! `NodeFixture`/`ChainFixture` hook to revert just that call) — neither
//! exists today. Items 1 and 2 are exercised in full below.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of both `decdn` and `decdn-node`:
//!
//! ```bash
//! cargo build -p decdn-cli -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e peer_store_probeless
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

use std::time::Duration;

use alloy::primitives::U256;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_client::{PeerStore, StoreConfig};
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_e2e::poll;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "peer-store-e2e-password";
/// Standard tier: three daemons plus a chain plus two sequential `decdn get`
/// journeys, each with its own retry ladder for the pool-watcher race.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`,
/// chunk-log 4 == 16 KiB chunk groups).
const CHUNK_GROUP: usize = 16 * 1024;
/// The bao wire bytes a node wrote on `cdn/client/v1`.
const SERVED: &str = "decdn_bytes_served_total";
/// A node's count of `cdn/probe/v1` requests it answered.
const PROBES: &str = "decdn_probe_requests_total";
/// A node's count of probes its per-peer bucket shed, which [`PROBES`] does
/// not count.
const PROBES_SHED: &str = "decdn_probe_rate_limit_rejected_per_peer_total";
/// What the holders may write past the wire bytes of the pull: a few parent
/// nodes a later leg repeats. Far below the blob's own wire bytes, which a
/// stream opened for the size and then dropped unread would cost at this
/// size.
const LEG_OVERHEAD_SLACK: u64 = 16 * 1024;

#[tokio::test(flavor = "multi_thread")]
async fn peer_store_probeless_second_fetch_and_cross_process_persistence() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("peer store e2e exceeded the overall timeout")??;
    Ok(())
}

/// Deterministic pseudo-random blob with a ragged final group.
fn make_blob() -> Vec<u8> {
    make_blob_of(12 * CHUNK_GROUP + 1234, 0xFEED_BEEF)
}

/// A blob longer than the CLI's default read-ahead (16 MiB), so a streamed
/// fetch's first leg is a window of it, not the whole blob (#2644).
fn make_streamed_blob() -> Vec<u8> {
    make_blob_of(17 * 1024 * 1024 + 1234, 0x2644_2644)
}

/// `len` deterministic pseudo-random bytes from xorshift `seed`.
fn make_blob_of(len: usize, seed: u32) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x: u32 = seed;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's fetch \
              state, so decomposing it would thread state through helpers without reducing the \
              journey's length or making it easier to follow"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli -p decdn-node`
    // hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // Three independently-bonded holders of the SAME blob — see the module
    // docs for why three is the minimum fixture shape that can reach
    // `MIN_FRESH_CANDIDATES`.
    let blob = make_blob();
    let blob_hash = Hash::new(&blob);
    let streamed = make_streamed_blob();
    let streamed_hash = Hash::new(&streamed);
    let (holder_a, hashes_a) =
        NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice(), streamed.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes_a.first() == Some(&blob_hash),
        "holder A hash mismatch"
    );
    let (holder_b, hashes_b) =
        NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice(), streamed.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes_b.first() == Some(&blob_hash),
        "holder B hash mismatch"
    );
    let (holder_c, hashes_c) =
        NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice(), streamed.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes_c.first() == Some(&blob_hash),
        "holder C hash mismatch"
    );
    anyhow::ensure!(
        holder_a.operator_addr() != holder_b.operator_addr()
            && holder_a.operator_addr() != holder_c.operator_addr()
            && holder_b.operator_addr() != holder_c.operator_addr(),
        "the three holders must be distinct operators — admit_sources dedupes per eth_address, \
         so `store_fast_path` could never see three fresh CANDIDATES from fewer than three \
         distinct operators"
    );

    // Funded buyer with an on-disk keystore under a `0o700` client data dir (the
    // `RedbBuyerPoolStore` the CLI opens enforces the mode; `tempdir` is
    // `0o755`).
    let client_dir = tempfile::tempdir().context("client tempdir")?;
    #[cfg(unix)]
    std::fs::set_permissions(
        client_dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod client dir 0o700")?;
    eth_identity::generate_and_persist(client_dir.path(), KEYSTORE_PASSWORD, false)
        .context("generate buyer keystore")?;
    let keystore = eth_identity::keystore_path(client_dir.path());
    let buyer = eth_identity::load_signer(&keystore, KEYSTORE_PASSWORD).context("load buyer")?;
    let buyer_addr = buyer.address();
    chain.fund_eth(buyer_addr, 100).await?;
    // One shared pool backs every lane this test's two sequential
    // single-source fetches may open, plus headroom for recovery top-ups.
    chain
        .mint_usdc(
            buyer_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(8u64),
        )
        .await
        .context("mint buyer USDC")?;

    let out = client_dir.path().join("blob.bin");
    let peers_dir = client_dir.path().join("peers");

    // ---- Journey 1 + 2: populate, then a probe-less second fetch ----
    //
    // Auto-discovery (no `--node-id`), so `discover_provider` reads the real
    // registry, probes the region-nearest candidates (all three holders here),
    // picks a holder, and off its critical path harvests identity + a fresh
    // latency sample for every probed candidate into the peer store.
    let args = fetch_argv(&chain, &blob_hash, client_dir.path(), &keystore, &out);
    run_fetch_until_ready(client_dir.path(), &args).await?;

    let got = std::fs::read(&out).context("read fetch #1 output")?;
    anyhow::ensure!(
        got == blob,
        "fetch #1 produced {} bytes, expected {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        peers_dir.is_dir(),
        "fetch #1 must create {} — nothing was harvested into the peer store",
        peers_dir.display()
    );

    // The harvest is spawned off the fetch's critical path (`spawn_harvest`'s
    // handle is dropped, not awaited, on the real CLI path) — poll rather than
    // assume it landed the instant the subprocess exited, though in practice
    // the paid transfer that follows discovery gives it ample wall-clock time.
    let selectable_after_fetch1 = poll(Duration::from_secs(30), || async {
        let cfg = StoreConfig::default();
        let now = now_secs();
        let store = PeerStore::open(client_dir.path());
        let n = store
            .load_all()
            .into_iter()
            .filter(|r| r.selectable(now, &cfg))
            .count();
        Ok((n >= 3).then_some(n))
    })
    .await?;
    anyhow::ensure!(
        selectable_after_fetch1.is_some(),
        "fetch #1's harvest never produced 3 fresh, selectable peer records — \
         store_fast_path's MIN_FRESH_CANDIDATES floor could never be reached; peers dir holds: \
         {:?}",
        std::fs::read_dir(&peers_dir).ok().map(|it| it
            .filter_map(|e| e.ok().map(|e| e.file_name()))
            .collect::<Vec<_>>())
    );

    // Fetch #2, same `data_dir`, no `--rediscover`: with >= MIN_FRESH_CANDIDATES
    // (3) selectable records now on disk, `discover_provider`'s `store_fast_path`
    // check engages BEFORE `discovery::bootstrap_nodes` (the registry read) or
    // `probe_and_order` (the network probe round) are ever reached — see
    // `crates/cli/src/commands/fetch.rs`'s `discover_provider`/`store_fast_path`.
    // The output is removed first so a byte-for-byte re-verify is meaningful
    // (an untouched file passing the equality check would prove nothing).
    std::fs::remove_file(&out).context("remove output before fetch #2")?;
    let holders = [&holder_a, &holder_b, &holder_c];
    let served_before = sum_metric(&holders, SERVED).await?;
    let probes_before =
        sum_metric(&holders, PROBES).await? + sum_metric(&holders, PROBES_SHED).await?;
    // The pool exists, so one attempt succeeds, and the counters cover it
    // alone.
    let (_, log) = run_fetch_logged(client_dir.path(), &args).await?;

    let got = std::fs::read(&out).context("read fetch #2 output")?;
    anyhow::ensure!(
        got == blob,
        "the probe-less second fetch produced {} bytes, expected {}",
        got.len(),
        blob.len()
    );
    let probes = (sum_metric(&holders, PROBES).await? + sum_metric(&holders, PROBES_SHED).await?)
        .saturating_sub(probes_before);
    // No probe round runs before the transfer, and none runs during it: the
    // lanes the store routes to leave no range uncovered while they still
    // build, so the acquire loop asks for no discovery (#2677).
    anyhow::ensure!(
        log.contains("fetch timings") && !log.contains("probe_start_ms"),
        "fetch #2 ran a probe round before the transfer:\n{log}"
    );
    anyhow::ensure!(
        probes == 0,
        "fetch #2 probed the holders {probes} time(s):\n{log}"
    );
    let wire = decdn_bao_range::align_range_clamped(0, 0, blob.len() as u64)?.wire_len();
    let served = sum_metric(&holders, SERVED)
        .await?
        .saturating_sub(served_before);
    anyhow::ensure!(
        (wire..=wire + LEG_OVERHEAD_SLACK).contains(&served),
        "the holders wrote {served} bytes for a {wire}-byte pull: the first size claim's \
         stream was not the download's first leg"
    );

    // ---- Journey 3: a probe-less streamed fetch (#2644) ----
    //
    // `-o -` with no size hint: the first size claim opens the streamed
    // drive's first leg, a read-ahead window of the blob, and the drive adopts
    // that pull. A whole-blob size open is not that leg, so the drive would
    // close it and open offset 0 again, after the node had written into it for
    // up to a round trip. On loopback the close lands before the node writes,
    // and a steal between the lanes stops a stream past which the node has
    // already written, so the served bytes tell the two apart neither way. The
    // legs the client opens do: an adopted first leg opens nothing itself.
    let mut stream_args = fetch_argv(&chain, &streamed_hash, client_dir.path(), &keystore, &out);
    if let Some(output) = stream_args.get_mut(2) {
        *output = "-".into();
    }
    let (got, log) = run_fetch_logged(client_dir.path(), &stream_args).await?;
    anyhow::ensure!(
        got == streamed,
        "the streamed fetch wrote {} bytes to stdout, expected {}",
        got.len(),
        streamed.len()
    );
    // The fetch timings name a probe round only when one ran, and a probe's
    // size hint would skip the size open.
    anyhow::ensure!(
        log.contains("fetch timings") && !log.contains("probe_start_ms"),
        "the streamed fetch probed, so no size open ran:\n{log}"
    );
    let legs: Vec<&str> = log
        .lines()
        .filter(|line| line.contains("opening a paid leg"))
        .collect();
    anyhow::ensure!(
        !legs.is_empty(),
        "the streamed fetch logged no leg open:\n{log}"
    );
    anyhow::ensure!(
        !legs.iter().any(|line| line.contains("byte_offset=0 ")),
        "the streamed drive opened offset 0 itself instead of adopting the size open's pull:\n{}",
        legs.join("\n")
    );

    // ---- Journey 2 (persistence half): the peer records outlive both ----
    //
    // Both `decdn get` invocations above are separate OS processes; the
    // store directory (and at least the 3 records fetch #1 wrote) must still
    // be on disk now that both have exited.
    let files_after_fetch2: Vec<_> = std::fs::read_dir(&peers_dir)
        .context("read peers dir after fetch #2")?
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("json"))
        .collect();
    anyhow::ensure!(
        files_after_fetch2.len() >= 3,
        "peer records did not persist across the two separate `decdn get` processes: found {} \
         record file(s) after fetch #2, expected >= 3",
        files_after_fetch2.len()
    );

    // `NodeFixture` tears each daemon down on drop; there is nothing to await.
    drop(holder_a);
    drop(holder_b);
    drop(holder_c);
    Ok(())
}

/// Wall-clock seconds since the Unix epoch, saturating to 0 rather than
/// panicking on a clock before the epoch (mirrors `decdn_client::targets::now_secs`).
fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The sum of metric `name` over `nodes`.
async fn sum_metric(nodes: &[&NodeFixture], name: &str) -> anyhow::Result<u64> {
    let mut sum = 0;
    for node in nodes {
        sum += node.scrape_metric(name).await?;
    }
    Ok(sum)
}

/// Run `decdn get` once with the client's leg opens logged, and fail with its
/// stderr unless it succeeds. Returns its stdout and its log, with the ANSI
/// styling stripped.
async fn run_fetch_logged(
    data_dir: &std::path::Path,
    args: &[String],
) -> anyhow::Result<(Vec<u8>, String)> {
    let output = tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
        .arg("get")
        .args(args)
        .env("RUST_LOG", "info,decdn_client::source=debug")
        .output()
        .await
        .context("spawn decdn get")?;
    let log = strip_ansi(&String::from_utf8_lossy(&output.stderr));
    anyhow::ensure!(output.status.success(), "decdn get failed:\n{log}");
    Ok((output.stdout, log))
}

/// `text` with its ANSI escape sequences (`ESC [ … letter`) removed.
fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Run `decdn get`, retrying until the node's chain watcher has observed the
/// freshly-opened pool (`decdn get` has no internal retry for that race).
/// Mirrors `cli_fetch_resume`/`cli_fetch_multi_source`'s helper of the same
/// name.
async fn run_fetch_until_ready(data_dir: &std::path::Path, args: &[String]) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
            .arg("get")
            .args(args)
            .output()
            .await
            .context("spawn decdn get")?;
        if output.status.success() {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "decdn get never succeeded; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "fetch not ready; retrying after watcher catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// The `decdn get` argv (after the `get` subcommand) to pull `hash` via
/// AUTO-DISCOVERY — deliberately no `--node-id`/`--addr`/`--provider-address`,
/// since the peer store only ever gets populated (and consulted) on the
/// auto-discovery path (`discover_provider`); the pinned `--node-id` path never
/// touches it.
fn fetch_argv(
    chain: &ChainFixture,
    hash: &Hash,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
    out: &std::path::Path,
) -> Vec<String> {
    vec![
        format!("decdn:{}", hash.to_hex()),
        "-o".into(),
        out.display().to_string(),
        "--capacity-bond-address".into(),
        format!("{}", chain.addrs().capacity_bond),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
        "--slash-judge-address".into(),
        format!("{}", chain.addrs().slash_judge),
        "--chain-id".into(),
        chain.chain_id().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
        "--working-deposit-micro-usdc".into(),
        DEPOSIT_MICRO_USDC.to_string(),
    ]
}
