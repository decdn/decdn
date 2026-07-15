//! Integration tests for `decdn channel close` / `settle` / `clean` (#1136).
//!
//! These exercise the pre-chain guard paths that are reachable without a live
//! node/anvil: an empty-store `clean` is a no-op success, and `close`/`settle`
//! for an untracked provider fail fast at the store lookup (before any signer
//! load or RPC dial). The on-chain state machine itself is unit-tested
//! exhaustively via `next_action` next to the command impl, and the underlying
//! `closeChannel`/`settleChannel`/`reclaimExpired` primitives are covered by the
//! node crate's anvil settlement e2e.

#![cfg(unix)] // The buyer store enforces POSIX `0o700` on its data dir.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use decdn_cli::commands::channel::channel_dispatch;
use decdn_common::cli::{
    ChannelArgs, ChannelChainArgs, ChannelCleanArgs, ChannelCloseArgs, ChannelCommand,
    ChannelSettleArgs,
};
use decdn_incentive::buyer_channel::{BuyerChannelState, BuyerChannelStore};
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;

/// The buyer store enforces `0o700` on its data dir; `tempdir()` honours the
/// umask, so tighten it first.
fn data_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

/// Hermetic empty config so `load_file_config` never reads the real
/// `~/.decdn/node.toml`.
fn empty_config(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("node.toml");
    std::fs::write(&path, "").unwrap();
    path
}

/// Chain flags that satisfy `resolve_chain` (rpc + payment-channel required) so
/// the guard tests reach the store lookup. The values are never dialed — every
/// case here returns before building a provider.
fn chain_args(dir: &Path) -> ChannelChainArgs {
    ChannelChainArgs {
        rpc_url: Some("http://127.0.0.1:1".to_string()),
        payment_channel_address: Some("0x00000000000000000000000000000000000000ab".to_string()),
        chain_id: None,
        keystore: None,
        data_dir: Some(dir.to_path_buf()),
    }
}

fn seed(dir: &Path, provider_byte: u8) {
    let store = RedbBuyerChannelStore::open(dir).unwrap();
    let mut state = BuyerChannelState::new(
        B256::repeat_byte(0xab),
        Address::repeat_byte(provider_byte),
        Address::repeat_byte(0xcd),
        U256::from(2_000_000u64),
        0,
    );
    state.last_nonce = U256::from(4u64);
    state.last_amount = U256::from(1_500_000u64);
    state.last_bytes_delivered = U256::from(4096u64);
    store.record(&state).unwrap();
}

#[tokio::test]
async fn clean_empty_store_is_a_noop_success() {
    let dir = data_dir();
    let cfg = empty_config(dir.path());
    let args = ChannelArgs {
        command: ChannelCommand::Clean(ChannelCleanArgs {
            chain: chain_args(dir.path()),
        }),
    };
    channel_dispatch(&args, Some(&cfg))
        .await
        .expect("clean over an empty store should be a no-op success");
}

#[tokio::test]
async fn close_unknown_provider_errors_before_touching_chain() {
    let dir = data_dir();
    let cfg = empty_config(dir.path());
    // Store exists (a different provider tracked) but lacks the requested one.
    seed(dir.path(), 0x22);
    let args = ChannelArgs {
        command: ChannelCommand::Close(ChannelCloseArgs {
            provider_address: "0x1111111111111111111111111111111111111111".to_string(),
            chain: chain_args(dir.path()),
        }),
    };
    let err = channel_dispatch(&args, Some(&cfg))
        .await
        .expect_err("close for an untracked provider must error");
    assert!(
        err.to_string().contains("no buyer channel tracked"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn settle_unknown_provider_errors_before_touching_chain() {
    let dir = data_dir();
    let cfg = empty_config(dir.path());
    seed(dir.path(), 0x22);
    let args = ChannelArgs {
        command: ChannelCommand::Settle(ChannelSettleArgs {
            provider_address: "0x1111111111111111111111111111111111111111".to_string(),
            chain: chain_args(dir.path()),
        }),
    };
    let err = channel_dispatch(&args, Some(&cfg))
        .await
        .expect_err("settle for an untracked provider must error");
    assert!(
        err.to_string().contains("no buyer channel tracked"),
        "unexpected error: {err}"
    );
}
