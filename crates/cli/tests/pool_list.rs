//! Integration test for `decdn pool list`.
//!
//! Exercises the real store-open → `load_all` wiring the command depends on:
//! seed a `RedbBuyerPoolStore` in a temp data dir, drop it (releasing the redb
//! file lock), then drive `pool_dispatch` and assert the read path succeeds
//! for both the seeded and the empty case. The table/JSON *shape* is covered
//! by the unit tests next to the command impl (`commands::pool`); this file
//! owns the on-disk round-trip only.
//!
//! It also owns the node-data-dir regression (#2078): a data dir holding a
//! daemon's `buyer.redb` must not be read as if it were a client store, and
//! must not gain a `buyer-pools.redb` as a side effect of being looked at.

#![cfg(unix)] // The buyer store enforces POSIX `0o700` on its data dir.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::os::unix::fs::PermissionsExt;

mod common;

use alloy::primitives::{Address, B256, U256};
use decdn_cli::commands::pool::pool_dispatch;
use decdn_common::cli::{PoolArgs, PoolCommand, PoolListArgs};
use decdn_incentive::buyer_pool::{BuyerPoolState, BuyerPoolStore};
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;

/// The buyer store enforces `0o700` on its data dir (as `~/.decdn` must be);
/// `tempdir()` honours the umask (typically `0o775`), so tighten it first.
fn data_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

fn list_args(data_dir: &std::path::Path, json: bool) -> PoolArgs {
    PoolArgs {
        command: PoolCommand::List(PoolListArgs {
            data_dir: Some(data_dir.to_path_buf()),
            json,
            // Unused on a client data dir; only a node's routes to the daemon.
            admin_url: None,
            timeout_ms: 5_000,
        }),
    }
}

fn seed(data_dir: &std::path::Path, owner_byte: u8) {
    // Open, record one pool, then drop so the redb lock is released before
    // the command re-opens the same file (mirrors the real two-process flow).
    let store = RedbBuyerPoolStore::open(data_dir).unwrap();
    let state = BuyerPoolState::new(
        B256::repeat_byte(owner_byte),
        Address::repeat_byte(owner_byte),
        Address::repeat_byte(0xcd),
        U256::from(2_000_000u64),
    );
    store.record(&state).unwrap();
}

fn seed_corrupt(data_dir: &std::path::Path, pool_id: B256) {
    let store = RedbBuyerPoolStore::open(data_dir).unwrap();
    store.insert_raw_buyer_record(pool_id, &[0u8; 8]).unwrap();
}

/// Hermetic empty config so `load_file_config` never reads the developer's real
/// `~/.decdn/node.toml`; the command only needs the explicit `--data-dir` flag.
fn empty_config(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("node.toml");
    std::fs::write(&path, "").unwrap();
    path
}

#[tokio::test]
async fn lists_seeded_pools_as_table_and_json() {
    let dir = data_dir();
    let cfg = empty_config(dir.path());
    seed(dir.path(), 0x11);
    seed(dir.path(), 0x22);

    // Both render paths must open the store and read the two records cleanly.
    pool_dispatch(&list_args(dir.path(), false), Some(&cfg))
        .await
        .expect("table listing over a seeded store should succeed");
    pool_dispatch(&list_args(dir.path(), true), Some(&cfg))
        .await
        .expect("json listing over a seeded store should succeed");
}

#[tokio::test]
async fn empty_store_lists_without_error() {
    let dir = data_dir();
    let cfg = empty_config(dir.path());
    // No seed: opening a fresh data dir creates an empty store, and `list`
    // must succeed (the `(no tracked pools)` sentinel path).
    pool_dispatch(&list_args(dir.path(), false), Some(&cfg))
        .await
        .expect("listing an empty store should succeed");
}

#[test]
fn undecodable_row_is_named_on_stderr() {
    let dir = data_dir();
    let corrupt_pool_id = B256::repeat_byte(0x44);
    seed_corrupt(dir.path(), corrupt_pool_id);

    let output = common::decdn_command(dir.path())
        .args(["pool", "list", "--data-dir"])
        .arg(dir.path())
        .output()
        .expect("run decdn pool list");
    assert!(
        output.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!("{corrupt_pool_id:#x}")),
        "{stderr}"
    );
    assert!(stderr.contains("escrowed"), "{stderr}");
    // The store is not truly empty — a deposit is escrowed behind the skipped
    // row — so the empty sentinel must not claim otherwise.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("(no tracked pools)"), "{stdout}");
}

#[test]
fn json_lists_skipped_pools_in_band_alongside_healthy_pools() {
    let dir = data_dir();
    let healthy = Address::repeat_byte(0x11);
    let corrupt_pool_id = B256::repeat_byte(0x44);
    seed(dir.path(), 0x11);
    seed_corrupt(dir.path(), corrupt_pool_id);

    let output = common::decdn_command(dir.path())
        .args(["pool", "list", "--json", "--data-dir"])
        .arg(dir.path())
        .output()
        .expect("run decdn pool list --json");
    assert!(
        output.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // The skipped pool_id must reach the structured stdout payload, not only
    // the stderr warning — a machine consumer parsing stdout would otherwise be
    // blind to the escrowed-but-untracked deposit.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"skipped\""), "{stdout}");
    // The provenance fields are the machine-readable half of the fix: a script
    // must be able to tell WHICH store produced a listing, and the two sources
    // emit different pool-object shapes (#2078).
    assert!(stdout.contains("\"source\": \"client_store\""), "{stdout}");
    assert!(stdout.contains("buyer-pools.redb"), "{stdout}");
    assert!(
        stdout.contains(&format!("{corrupt_pool_id:#x}")),
        "{stdout}"
    );
    assert!(stdout.contains(&format!("{healthy:#x}")), "{stdout}");
}

#[tokio::test]
async fn list_with_data_dir_ignores_broken_config_env_expansion() {
    let dir = data_dir();
    // A config that would fail `load_file_config` env-expansion (an unset `${VAR}`
    // in an unrelated blockchain field). With `--data-dir` given, the read-only
    // `list` must not read/expand it at all.
    let cfg = dir.path().join("node.toml");
    std::fs::write(
        &cfg,
        "[blockchain]\nrpc_url = \"${DECDN_TEST_DEFINITELY_UNSET_VAR_XYZ}\"\n",
    )
    .unwrap();
    pool_dispatch(&list_args(dir.path(), false), Some(&cfg))
        .await
        .expect("list with --data-dir must not load or env-expand the config");
}

/// A data dir holding a daemon's `buyer.redb` is a node's, and `list` must say
/// so rather than reading the unrelated client store beside it. With no daemon
/// listening, the command fails — and, critically, leaves no `buyer-pools.redb`
/// behind. Creating that file and reporting its emptiness as the node's state
/// is the defect this pins (#2078).
/// A daemon's `buyer.redb`, written by the real daemon store and then closed.
/// Returns the pool id recorded into it.
fn seed_stopped_daemon_store(data_dir: &std::path::Path) -> B256 {
    use decdn_node::channel_store::{BuyerPoolStoreHandle, PersistentPoolStateStore};

    let pool_id = B256::repeat_byte(0x5a);
    let store = std::sync::Arc::new(PersistentPoolStateStore::open(data_dir).unwrap());
    let state = BuyerPoolState::new(
        pool_id,
        Address::repeat_byte(0x5a),
        Address::repeat_byte(0xcd),
        U256::from(7_000_000u64),
    );
    BuyerPoolStoreHandle::new(std::sync::Arc::clone(&store))
        .record(&state)
        .unwrap();
    // Dropping the last handle releases redb's process-exclusive lock — the
    // daemon exiting, in one line.
    drop(store);
    pool_id
}

/// #2084: with the daemon stopped, its `buyer.redb` is readable and nothing
/// else can show it. The listing must come from that file, and must say so —
/// the provenance is what keeps it distinguishable from the client store's
/// identically-shaped rows.
#[test]
fn a_stopped_daemons_store_is_read_from_disk_and_labelled() {
    let dir = data_dir();
    let pool_id = seed_stopped_daemon_store(dir.path());

    let output = common::decdn_command(dir.path())
        .args([
            "pool",
            "list",
            // Nothing listens here, so the admin call is refused.
            "--admin-url",
            "http://127.0.0.1:1",
            "--data-dir",
        ])
        .arg(dir.path())
        .output()
        .expect("run decdn pool list");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "a stopped daemon's store must read: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("buyer.redb"), "{stdout}");
    assert!(
        stdout.contains("read from disk; no daemon running"),
        "the listing must name where it came from: {stdout}"
    );
    assert!(stdout.contains("pools=1"), "{stdout}");
    assert!(
        !dir.path().join("buyer-pools.redb").exists(),
        "the offline read must not manufacture a client store"
    );

    // `--json` carries the same provenance in a field, not in prose.
    let json_out = common::decdn_command(dir.path())
        .args([
            "pool",
            "list",
            "--json",
            "--admin-url",
            "http://127.0.0.1:1",
            "--data-dir",
        ])
        .arg(dir.path())
        .output()
        .expect("run decdn pool list --json");
    let v: serde_json::Value = serde_json::from_slice(&json_out.stdout).unwrap();
    assert_eq!(v["source"], "node_store_offline");
    assert!(v["store"].as_str().unwrap().ends_with("buyer.redb"), "{v}");
    assert_eq!(v["pools"][0]["pool_id"], format!("{pool_id:#x}"));
}

/// A refused admin port with the store still write-locked means a daemon IS
/// running and the admin URL is wrong — the opposite diagnosis from "the node
/// is down", and the one the old message could not make.
#[test]
fn a_locked_store_says_the_admin_url_is_wrong_not_that_the_node_is_down() {
    use decdn_node::channel_store::PersistentPoolStateStore;

    let dir = data_dir();
    // Held for the duration of the command: the daemon is up, the admin port
    // in the flag is simply not its.
    let _daemon = PersistentPoolStateStore::open(dir.path()).unwrap();

    let output = common::decdn_command(dir.path())
        .args([
            "pool",
            "list",
            "--admin-url",
            "http://127.0.0.1:1",
            "--data-dir",
        ])
        .arg(dir.path())
        .output()
        .expect("run decdn pool list");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("admin URL") || stderr.contains("admin_port"),
        "the error must point at the admin URL: {stderr}"
    );
    assert!(
        !stderr.contains("Start decdn-node"),
        "a daemon holding the store is not a stopped daemon: {stderr}"
    );
}

#[test]
fn node_data_dir_is_not_read_as_a_client_store() {
    let dir = data_dir();
    let buyer_db = dir.path().join("buyer.redb");
    // Presence is the signal. This one is not a real redb file, so the offline
    // disk read (#2084) cannot salvage it either — and the command must still
    // never reach for the CLIENT store, which is the bug.
    std::fs::write(&buyer_db, b"not a real redb file").unwrap();

    let output = common::decdn_command(dir.path())
        .args([
            "pool",
            "list",
            // A port nothing listens on, so the admin call fails fast.
            "--admin-url",
            "http://127.0.0.1:1",
            "--data-dir",
        ])
        .arg(dir.path())
        .output()
        .expect("run decdn pool list");

    assert!(
        !output.status.success(),
        "an unreachable daemon must fail, not fall back to the client store: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("buyer.redb"),
        "the error must name the file that holds the pools: {stderr}"
    );
    assert!(
        !dir.path().join("buyer-pools.redb").exists(),
        "pool list must not create a client store inside a node data dir"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("pools=0"),
        "reporting an empty client store as the node's state is the bug: {stdout}"
    );
}

/// A client listing names the file it read, so `pools=0` is interpretable.
#[test]
fn client_listing_names_the_store_it_read() {
    let dir = data_dir();
    let output = common::decdn_command(dir.path())
        .args(["pool", "list", "--data-dir"])
        .arg(dir.path())
        .output()
        .expect("run decdn pool list");
    assert!(
        output.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("buyer-pools.redb"), "{stdout}");
    assert!(stdout.contains("pools=0"), "{stdout}");
}

/// The guards live at their call sites, so pin them THERE, not only on the
/// classifier. Each of these runs the real binary; deleting the one-line guard
/// from `open`, `top_up_cmd`, `close` or `reclaim` turns the corresponding
/// assertion red. The guard runs before the keystore prompt and before any
/// chain work, so none of these needs an RPC endpoint or a signer.
mod node_dir_guards {
    use super::{common, data_dir};

    /// Run `decdn pool <args>` in a data dir made to look like a daemon's, and
    /// return stderr. Asserts a non-zero exit and that no client store was
    /// manufactured — the side effect that started #2078.
    fn refused(args: &[&str]) -> String {
        let dir = data_dir();
        // Not the buyer store: `lanes.redb` alone marks a node data dir, which
        // is the mid-recovery window where the old classifier got it wrong.
        std::fs::write(dir.path().join("lanes.redb"), b"x").unwrap();

        let output = common::decdn_command(dir.path())
            .args(args)
            .arg("--data-dir")
            .arg(dir.path())
            .output()
            .expect("run decdn pool");

        assert!(
            !output.status.success(),
            "expected a refusal for {args:?}, got success: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(
            !dir.path().join("buyer-pools.redb").exists(),
            "a refused {args:?} must not create a client store in a node data dir"
        );
        String::from_utf8_lossy(&output.stderr).into_owned()
    }

    #[test]
    fn pool_open_is_refused() {
        let stderr = refused(&["pool", "open", "--deposit-micro-usdc", "1000000"]);
        assert!(stderr.contains("buyer.redb"), "{stderr}");
        assert!(stderr.contains("decdn node pools"), "{stderr}");
    }

    #[test]
    fn pool_top_up_is_refused() {
        let stderr = refused(&[
            "pool",
            "top-up",
            "--pool",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            "--amount-micro-usdc",
            "1000000",
        ]);
        assert!(stderr.contains("buyer.redb"), "{stderr}");
    }

    /// `--all` enumerates from chain by keystore address, so on a node host it
    /// would close the pool the daemon is paying from right now.
    #[test]
    fn close_all_is_refused_and_names_the_single_pool_alternative() {
        let stderr = refused(&["pool", "close", "--all"]);
        assert!(stderr.contains("--pool"), "{stderr}");
        assert!(stderr.contains("buyer.redb"), "{stderr}");
    }

    #[test]
    fn reclaim_all_is_refused() {
        let stderr = refused(&["pool", "reclaim", "--all"]);
        assert!(stderr.contains("--pool"), "{stderr}");
    }
}
