//! `decdn fetch` and `decdn bundle pull` against a `decdn-node` daemon's data
//! dir (#2082).
//!
//! `decdn pool` refuses such a dir outright (#2078). These two are different:
//! a human fetching from a node host is a legitimately separate buyer, so an
//! explicit `--data-dir` is a real answer. What is not is *arriving* there —
//! `identity.data_dir` in the config file resolves to the node's dir with
//! nothing on the command line, and the keystore defaults to the same dir, so
//! the client pool is opened under the node's own operator address.
//!
//! The guard runs before the keystore prompt and before any chain or network
//! work, so none of these needs an RPC endpoint, a signer, or a peer.

#![cfg(unix)] // The buyer store enforces POSIX `0o700` on its data dir.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

mod common;

/// A hash the fetch never gets far enough to look for.
const SOME_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// An isolated HOME at `0o700`, which the buyer store requires of any data dir
/// beneath it.
fn home() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

/// Make `dir` look like a daemon's. Deliberately NOT `buyer.redb`: `lanes.redb`
/// alone marks a node data dir, which is the mid-recovery window where a
/// classifier keyed on the buyer store gets it wrong.
fn mark_as_node_dir(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(dir.join("lanes.redb"), b"x").unwrap();
}

/// A config whose `identity.data_dir` is the node's dir — the shape a node host
/// already has, and the one that makes the node's dir the *implicit* target.
fn config_pointing_at(home: &Path, data_dir: &Path) -> std::path::PathBuf {
    let path = home.join("node.toml");
    std::fs::write(
        &path,
        format!(
            "[identity]\ndata_dir = \"{}\"\n\n[blockchain]\nrpc_url = \
             \"http://127.0.0.1:8545\"\npayment_pool_address = \
             \"0x0000000000000000000000000000000000000001\"\nslash_judge_address = \
             \"0x0000000000000000000000000000000000000002\"\n",
            data_dir.display()
        ),
    )
    .unwrap();
    path
}

/// Run `decdn --config <cfg> <args>` and return (success, stderr).
fn run(home: &Path, cfg: &Path, args: &[&str]) -> (bool, String) {
    let output = common::decdn_command(home)
        .arg("--config")
        .arg(cfg)
        .args(args)
        .output()
        .expect("run decdn");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// The defect: a bare `decdn fetch` on a node host resolves to the daemon's
/// data dir through the config file and escrows there under the node's own
/// keystore. It must refuse, name both stores, and — the side effect that
/// started #2078 — manufacture no client store on the way out.
#[test]
fn fetch_refuses_a_node_data_dir_it_was_not_pointed_at() {
    let home = home();
    let data_dir = home.path().join("decdn-node");
    mark_as_node_dir(&data_dir);
    let cfg = config_pointing_at(home.path(), &data_dir);

    let (ok, stderr) = run(
        home.path(),
        &cfg,
        &[
            "fetch",
            "--hash",
            SOME_HASH,
            "-o",
            home.path().join("out").to_str().unwrap(),
        ],
    );

    assert!(!ok, "a bare fetch on a node data dir must refuse");
    assert!(stderr.contains("buyer.redb"), "{stderr}");
    assert!(stderr.contains("buyer-pools.redb"), "{stderr}");
    assert!(stderr.contains("--data-dir"), "{stderr}");
    assert!(
        !data_dir.join("buyer-pools.redb").exists(),
        "a refused fetch must not create a client store in a node data dir"
    );
}

/// `bundle pull` shares the resolver, so it shares the guard.
#[test]
fn bundle_pull_refuses_a_node_data_dir_it_was_not_pointed_at() {
    let home = home();
    let data_dir = home.path().join("decdn-node");
    mark_as_node_dir(&data_dir);
    let cfg = config_pointing_at(home.path(), &data_dir);

    let (ok, stderr) = run(
        home.path(),
        &cfg,
        &[
            "bundle",
            "pull",
            "--hash",
            SOME_HASH,
            "-o",
            home.path().join("out").to_str().unwrap(),
        ],
    );

    assert!(!ok, "a bare bundle pull on a node data dir must refuse");
    assert!(stderr.contains("buyer.redb"), "{stderr}");
    assert!(
        !data_dir.join("buyer-pools.redb").exists(),
        "a refused pull must not create a client store in a node data dir"
    );
}

/// Naming the dir is the operator saying they mean it, so the guard stands
/// aside. The command still fails — there is no chain and no peer behind that
/// config — but not with the refusal, and by then it is past the store open.
#[test]
fn an_explicit_data_dir_is_the_operators_call() {
    let home = home();
    let data_dir = home.path().join("decdn-node");
    mark_as_node_dir(&data_dir);
    let cfg = config_pointing_at(home.path(), &data_dir);

    let (ok, stderr) = run(
        home.path(),
        &cfg,
        &[
            "fetch",
            "--hash",
            SOME_HASH,
            "-o",
            home.path().join("out").to_str().unwrap(),
            "--data-dir",
            data_dir.to_str().unwrap(),
        ],
    );

    assert!(!ok, "there is no chain behind this config");
    assert!(
        !stderr.contains("refusing to fetch"),
        "an explicitly named data dir must not be refused: {stderr}"
    );
    assert!(
        data_dir.join("buyer-pools.redb").exists(),
        "past the guard, the client store opens as it always did: {stderr}"
    );
}
