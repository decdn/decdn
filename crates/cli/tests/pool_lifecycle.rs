//! Integration tests for `decdn pool top-up` / `close` / `reclaim`.
//!
//! These exercise the guard paths that are reachable without a live
//! node/anvil: a malformed `--pool` id fails fast at parse time, before the
//! buyer-pool store is opened, the keystore is loaded, or any RPC is dialed;
//! and `reclaim --pool` reads the pool before it opens the store or unlocks the
//! keystore.
//! The on-chain `openPool`/`topUp`/`closePool`/`reclaim` primitives themselves
//! are covered by the node crate's anvil settlement e2e.

#![cfg(unix)] // The buyer store enforces POSIX `0o700` on its data dir.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use decdn_cli::commands::pool::pool_dispatch;
use decdn_common::cli::{
    PoolArgs, PoolChainArgs, PoolCloseArgs, PoolCommand, PoolReclaimArgs, PoolTopUpArgs,
};

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

/// Chain flags that satisfy `resolve_chain` (rpc + payment-pool required) so
/// the guard tests reach the `--pool` parse. Nothing listens on the RPC port,
/// so a case that does dial it fails at its first read.
fn chain_args(dir: &Path) -> PoolChainArgs {
    PoolChainArgs {
        rpc_url: Some("http://127.0.0.1:1".to_string()),
        payment_pool_address: Some("0x00000000000000000000000000000000000000ab".to_string()),
        chain_id: None,
        keystore: None,
        keystore_password_file: None,
        data_dir: Some(dir.to_path_buf()),
    }
}

#[tokio::test]
async fn top_up_with_unparseable_pool_id_errors_before_touching_chain() {
    let dir = data_dir();
    let cfg = empty_config(dir.path());
    let args = PoolArgs {
        command: PoolCommand::TopUp(PoolTopUpArgs {
            pool: "not-a-pool-id".to_string(),
            amount_micro_usdc: 1_000_000,
            chain: chain_args(dir.path()),
        }),
    };
    let err = pool_dispatch(&args, Some(&cfg))
        .await
        .expect_err("an unparseable --pool must error before touching chain");
    assert!(err.to_string().contains("invalid --pool"), "{err}");
}

#[tokio::test]
async fn close_with_unparseable_pool_id_errors_before_touching_chain() {
    let dir = data_dir();
    let cfg = empty_config(dir.path());
    let args = PoolArgs {
        command: PoolCommand::Close(PoolCloseArgs {
            pool: Some("not-a-pool-id".to_string()),
            all: false,
            chain: chain_args(dir.path()),
        }),
    };
    let err = pool_dispatch(&args, Some(&cfg))
        .await
        .expect_err("an unparseable --pool must error before touching chain");
    assert!(err.to_string().contains("invalid --pool"), "{err}");
}

#[tokio::test]
async fn reclaim_with_unparseable_pool_id_errors_before_touching_chain() {
    let dir = data_dir();
    let cfg = empty_config(dir.path());
    let args = PoolArgs {
        command: PoolCommand::Reclaim(PoolReclaimArgs {
            pool: Some("not-a-pool-id".to_string()),
            all: false,
            chain: chain_args(dir.path()),
        }),
    };
    let err = pool_dispatch(&args, Some(&cfg))
        .await
        .expect_err("an unparseable --pool must error before touching chain");
    assert!(err.to_string().contains("invalid --pool"), "{err}");
}

/// `reclaim --pool` reads the pool on a wallet-free provider before anything
/// else touches local state: with no keystore and an unreachable RPC, the
/// failure is the `getPool` read, not a keystore or password error, and the
/// buyer-pool store is never created.
#[tokio::test]
async fn reclaim_pool_reads_chain_before_store_or_keystore() {
    let dir = data_dir();
    let cfg = empty_config(dir.path());
    let args = PoolArgs {
        command: PoolCommand::Reclaim(PoolReclaimArgs {
            pool: Some(format!("0x{}", "11".repeat(32))),
            all: false,
            chain: chain_args(dir.path()),
        }),
    };
    let err = pool_dispatch(&args, Some(&cfg))
        .await
        .expect_err("an unreachable RPC must fail the pre-check");
    let msg = format!("{err:#}");
    assert!(msg.contains("getPool failed"), "{msg}");
    // The transport class stays in the chain beneath the context (#2277).
    assert!(
        msg.to_lowercase().contains("connection refused"),
        "the failure class must survive: {msg}"
    );
    assert!(
        !msg.contains("keystore") && !msg.contains("password"),
        "the pre-check must run before the keystore unlock: {msg}"
    );
    assert!(
        !dir.path().join("buyer-pools.redb").exists(),
        "the pre-check must run before the store opens"
    );
}
