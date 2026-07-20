//! Integration test for `decdn channel list` (#1133).
//!
//! Exercises the real store-open → `load_all` wiring the command depends on:
//! seed a `RedbBuyerChannelStore` in a temp data dir, drop it (releasing the
//! redb file lock), then drive `channel_dispatch` and assert the read path
//! succeeds for both the seeded and the empty case. The table/JSON *shape* is
//! covered by the unit tests next to the command impl (`commands::channel`);
//! this file owns the on-disk round-trip only.

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
use decdn_cli::commands::channel::channel_dispatch;
use decdn_common::cli::{ChannelArgs, ChannelCommand, ChannelListArgs};
use decdn_incentive::buyer_channel::{BuyerChannelState, BuyerChannelStore};
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;

/// The buyer store enforces `0o700` on its data dir (as `~/.decdn` must be);
/// `tempdir()` honours the umask (typically `0o775`), so tighten it first.
fn data_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

fn list_args(data_dir: &std::path::Path, json: bool) -> ChannelArgs {
    ChannelArgs {
        command: ChannelCommand::List(ChannelListArgs {
            data_dir: Some(data_dir.to_path_buf()),
            json,
        }),
    }
}

fn seed(data_dir: &std::path::Path, provider_byte: u8) {
    // Open, record one channel, then drop so the redb lock is released before
    // the command re-opens the same file (mirrors the real two-process flow).
    let store = RedbBuyerChannelStore::open(data_dir).unwrap();
    let mut state = BuyerChannelState::new(
        B256::repeat_byte(0xab),
        Address::repeat_byte(provider_byte),
        Address::repeat_byte(0xcd),
        U256::from(2_000_000u64),
        0,
    );
    state.last_amount = U256::from(1_500_000u64);
    state.last_nonce = U256::from(4u64);
    state.last_bytes_delivered = U256::from(4096u64);
    store.record(&state).unwrap();
}

fn seed_corrupt(data_dir: &std::path::Path, provider: Address) {
    let store = RedbBuyerChannelStore::open(data_dir).unwrap();
    store.insert_raw_buyer_record(provider, &[0u8; 8]).unwrap();
}

/// Hermetic empty config so `load_file_config` never reads the developer's real
/// `~/.decdn/node.toml`; the command only needs the explicit `--data-dir` flag.
fn empty_config(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("node.toml");
    std::fs::write(&path, "").unwrap();
    path
}

#[tokio::test]
async fn lists_seeded_channels_as_table_and_json() {
    let dir = data_dir();
    let cfg = empty_config(dir.path());
    seed(dir.path(), 0x11);
    seed(dir.path(), 0x22);

    // Both render paths must open the store and read the two records cleanly.
    channel_dispatch(&list_args(dir.path(), false), Some(&cfg))
        .await
        .expect("table listing over a seeded store should succeed");
    channel_dispatch(&list_args(dir.path(), true), Some(&cfg))
        .await
        .expect("json listing over a seeded store should succeed");
}

#[tokio::test]
async fn empty_store_lists_without_error() {
    let dir = data_dir();
    let cfg = empty_config(dir.path());
    // No seed: opening a fresh data dir creates an empty store, and `list`
    // must succeed (the `(no tracked channels)` sentinel path).
    channel_dispatch(&list_args(dir.path(), false), Some(&cfg))
        .await
        .expect("listing an empty store should succeed");
}

#[test]
fn undecodable_row_is_named_on_stderr() {
    let dir = data_dir();
    let provider = Address::repeat_byte(0x44);
    seed_corrupt(dir.path(), provider);

    let output = common::decdn_command(dir.path())
        .args(["channel", "list", "--data-dir"])
        .arg(dir.path())
        .output()
        .expect("run decdn channel list");
    assert!(
        output.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(&format!("{provider:#x}")), "{stderr}");
    assert!(stderr.contains("escrowed"), "{stderr}");
    // The store is not truly empty — a deposit is escrowed behind the skipped
    // row — so the empty sentinel must not claim otherwise.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("(no tracked channels)"), "{stdout}");
}

#[test]
fn json_lists_skipped_providers_in_band_alongside_healthy_channels() {
    let dir = data_dir();
    let healthy = Address::repeat_byte(0x11);
    let corrupt = Address::repeat_byte(0x44);
    seed(dir.path(), 0x11);
    seed_corrupt(dir.path(), corrupt);

    let output = common::decdn_command(dir.path())
        .args(["channel", "list", "--json", "--data-dir"])
        .arg(dir.path())
        .output()
        .expect("run decdn channel list --json");
    assert!(
        output.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // The skipped provider must reach the structured stdout payload, not only
    // the stderr warning — a machine consumer parsing stdout would otherwise be
    // blind to the escrowed-but-untracked deposit.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"skipped\""), "{stdout}");
    assert!(stdout.contains(&format!("{corrupt:#x}")), "{stdout}");
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
    channel_dispatch(&list_args(dir.path(), false), Some(&cfg))
        .await
        .expect("list with --data-dir must not load or env-expand the config");
}
