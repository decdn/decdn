//! Integration tests for `decdn key-gen` covering the Ethereum keystore
//! extension (issue #406).
//!
//! These tests drive the public command handler directly rather than
//! shelling out the binary, so they don't depend on a built artifact —
//! `cargo test -p decdn-cli` is enough.

#![cfg(unix)] // Permission assertions are POSIX-specific.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use decdn_cli::commands::key_gen::key_gen;
use decdn_common::cli::KeyGenArgs;
use decdn_common::identity;
use decdn_incentive::eth_identity;
use tempfile::TempDir;

const TEST_PASSWORD: &str = "hunter2";

/// The `*.tmp.*` staging files currently in `dir`. The two-phase rotation must
/// never leave temp litter once the staged handles drop.
fn tmp_files(dir: &TempDir) -> Vec<PathBuf> {
    fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(".tmp."))
        })
        .collect()
}

fn tmp_file_count(dir: &TempDir) -> usize {
    tmp_files(dir).len()
}

fn make_args(dir: &TempDir, force: bool, pw_file: PathBuf) -> KeyGenArgs {
    KeyGenArgs {
        output_dir: Some(dir.path().to_path_buf()),
        force,
        password_file: Some(pw_file),
    }
}

fn write_password_file(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("pw.txt");
    fs::write(&path, TEST_PASSWORD).unwrap();
    path
}

#[test]
fn creates_both_files_in_empty_dir() {
    let tmp = TempDir::new().unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let pw_file = write_password_file(&tmp);
    let args = make_args(&tmp, false, pw_file);

    key_gen(&args).expect("key-gen succeeds");

    let key = tmp.path().join("node.secret");
    let keystore = tmp.path().join("keystore.json");
    assert!(key.exists(), "node.secret should be written");
    assert!(keystore.exists(), "keystore.json should be written");

    let key_mode = fs::metadata(&key).unwrap().permissions().mode() & 0o777;
    let ks_mode = fs::metadata(&keystore).unwrap().permissions().mode() & 0o777;
    assert_eq!(key_mode, 0o600, "node.secret mode {key_mode:#o}");
    assert_eq!(ks_mode, 0o600, "keystore.json mode {ks_mode:#o}");
}

#[test]
fn force_replaces_existing_keystore() {
    let tmp = TempDir::new().unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let pw_file = write_password_file(&tmp);
    key_gen(&make_args(&tmp, false, pw_file.clone())).unwrap();

    let first = fs::read_to_string(tmp.path().join("keystore.json")).unwrap();
    key_gen(&make_args(&tmp, true, pw_file)).expect("force regenerate");
    let second = fs::read_to_string(tmp.path().join("keystore.json")).unwrap();

    assert_ne!(
        first, second,
        "force should write a fresh keystore (different ciphertext / salt)"
    );
}

/// `--force` must archive both prior keys to `<name>.bak.<ts>` so the
/// runbook's offline-archive and rollback paths
/// (`appendix-operator-key-rotation.md` §1 step 9, §5) remain available.
#[test]
fn force_archives_both_prior_keys() {
    let tmp = TempDir::new().unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let pw_file = write_password_file(&tmp);
    key_gen(&make_args(&tmp, false, pw_file.clone())).unwrap();

    let original_node = fs::read(tmp.path().join("node.secret")).unwrap();
    let original_keystore = fs::read(tmp.path().join("keystore.json")).unwrap();

    key_gen(&make_args(&tmp, true, pw_file)).expect("force regenerate");

    let mut node_bak: Option<PathBuf> = None;
    let mut keystore_bak: Option<PathBuf> = None;
    for entry in fs::read_dir(tmp.path()).unwrap() {
        let p = entry.unwrap().path();
        let name = p
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_owned();
        if name.starts_with("node.secret.bak.") {
            node_bak = Some(p);
        } else if name.starts_with("keystore.json.bak.") {
            keystore_bak = Some(p);
        }
    }

    let node_bak = node_bak.expect("force should archive node.secret");
    let keystore_bak = keystore_bak.expect("force should archive keystore.json");
    assert_eq!(fs::read(&node_bak).unwrap(), original_node);
    assert_eq!(fs::read(&keystore_bak).unwrap(), original_keystore);
}

#[test]
fn errors_when_keystore_exists_without_force() {
    let tmp = TempDir::new().unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let pw_file = write_password_file(&tmp);
    key_gen(&make_args(&tmp, false, pw_file.clone())).unwrap();

    let err = key_gen(&make_args(&tmp, false, pw_file)).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("already exists"), "got: {msg}");
    assert!(msg.contains("--force"), "got: {msg}");
}

#[test]
fn errors_when_node_key_exists_without_force() {
    // Pre-populate node.secret only; keystore is missing. The pre-flight
    // should reject before any password is read or any work is done.
    let tmp = TempDir::new().unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let key = tmp.path().join("node.secret");
    fs::write(&key, [0u8; 32]).unwrap();
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    let pw_file = write_password_file(&tmp);

    let err = key_gen(&make_args(&tmp, false, pw_file)).unwrap_err();
    assert!(format!("{err}").contains("node key already exists"));
}

/// Core #844 guarantee: staging generates all key material into temp files and
/// mutates **neither** canonical file until `commit`. Dropping the staged
/// handles without committing leaves the prior `node.secret` / `keystore.json`
/// byte-for-byte intact and removes the temp files — so a failure generating
/// the second secret can never half-rotate the pair.
#[test]
fn staging_does_not_mutate_canonical_files_until_commit() {
    let tmp = TempDir::new().unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let pw_file = write_password_file(&tmp);
    // Establish a prior key pair to protect.
    key_gen(&make_args(&tmp, false, pw_file)).unwrap();

    let node_path = tmp.path().join("node.secret");
    let keystore_path = tmp.path().join("keystore.json");
    let original_node = fs::read(&node_path).unwrap();
    let original_keystore = fs::read(&keystore_path).unwrap();

    {
        // Stage both fresh secrets (the expensive, fallible work) — this is the
        // exact pair of calls `key-gen --force` makes before any commit.
        let staged_key = identity::stage_node_key(tmp.path()).unwrap();
        let staged_keystore = eth_identity::stage_keystore(tmp.path(), TEST_PASSWORD).unwrap();

        // The staged identities are readable, but nothing canonical moved yet.
        let _ = staged_key.public();
        let _ = staged_keystore.address();
        assert_eq!(
            fs::read(&node_path).unwrap(),
            original_node,
            "staging must not rewrite node.secret"
        );
        assert_eq!(
            fs::read(&keystore_path).unwrap(),
            original_keystore,
            "staging must not rewrite keystore.json"
        );
        // Two temp files are staged and waiting for commit.
        assert_eq!(tmp_file_count(&tmp), 2, "both stages should write a temp");
        // Staging now keeps key material on disk across a second fallible
        // operation, so the temps must already be 0o600 — never briefly
        // world-readable while waiting for commit.
        for tmp_file in tmp_files(&tmp) {
            let mode = fs::metadata(&tmp_file).unwrap().permissions().mode() & 0o777;
            assert_eq!(
                mode,
                0o600,
                "staged temp {} must be 0o600, was {mode:#o}",
                tmp_file.display()
            );
        }

        // Abandon both stages (e.g. an error aborted the run before commit).
    }

    // Canonical files are still the originals, and no temp litter remains.
    assert_eq!(
        fs::read(&node_path).unwrap(),
        original_node,
        "abandoned stage must leave node.secret untouched"
    );
    assert_eq!(
        fs::read(&keystore_path).unwrap(),
        original_keystore,
        "abandoned stage must leave keystore.json untouched"
    );
    assert_eq!(
        tmp_file_count(&tmp),
        0,
        "dropping uncommitted stages must clean up their temp files"
    );
}

/// Committing the staged handles installs exactly the identities the stages
/// reported pre-commit — the bytes that land are the ones that were generated,
/// with no swap between stage and commit.
#[test]
fn staged_commit_installs_the_staged_identity() {
    let tmp = TempDir::new().unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();

    let mut staged_key = identity::stage_node_key(tmp.path()).unwrap();
    let staged_keystore = eth_identity::stage_keystore(tmp.path(), TEST_PASSWORD).unwrap();
    let staged_node_id = staged_key.public();
    let staged_address = staged_keystore.address();

    // Fresh install: nothing to archive on either commit.
    assert!(staged_key.commit().unwrap().is_none());
    assert!(staged_keystore.commit().unwrap().is_none());

    // The committed node key reloads to the staged node id...
    let loaded = identity::load_or_generate(tmp.path()).unwrap();
    assert_eq!(
        loaded.public(),
        staged_node_id,
        "committed node.secret must be the staged key"
    );
    // ...and the committed keystore decrypts to the staged address.
    let signer = eth_identity::load_signer(&keystore_path_of(&tmp), TEST_PASSWORD).unwrap();
    assert_eq!(
        signer.address(),
        staged_address,
        "committed keystore must be the staged key"
    );
    assert_eq!(tmp_file_count(&tmp), 0, "commit must consume both temps");
}

fn keystore_path_of(dir: &TempDir) -> PathBuf {
    dir.path().join("keystore.json")
}

/// The `*.bak.*` archive files currently in `dir`.
fn bak_files(dir: &TempDir) -> Vec<PathBuf> {
    fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(".bak."))
        })
        .collect()
}

/// Delete the single `*.tmp.*` staging file whose name starts with `prefix`,
/// simulating the staged temp vanishing out from under a commit (so the install
/// rename fails with `ENOENT` while the directory stays writable — exercising
/// the fail-safe restore branch without an un-writable-dir trick that would also
/// break the restore).
fn delete_staged_temp(dir: &TempDir, prefix: &str) {
    let temp = tmp_files(dir)
        .into_iter()
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(prefix))
        })
        .expect("a staged temp should exist");
    fs::remove_file(&temp).unwrap();
}

/// Fail-safe commit (#844): if the install rename fails after `move_aside`
/// archived the prior `node.secret`, `commit` restores the archive so the old
/// key stays live — the canonical path is never left missing — and the error
/// reports that the restore happened.
#[test]
fn commit_restores_prior_node_key_when_install_rename_fails() {
    let tmp = TempDir::new().unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let pw_file = write_password_file(&tmp);
    key_gen(&make_args(&tmp, false, pw_file)).unwrap();

    let node_path = tmp.path().join("node.secret");
    let original_node = fs::read(&node_path).unwrap();

    let mut staged_key = identity::stage_node_key(tmp.path()).unwrap();
    // Make the install rename fail (ENOENT) without touching dir perms, so the
    // best-effort restore can still succeed. `errno`-agnostic: the restore branch
    // runs on any install-rename error, so this faithfully exercises it.
    delete_staged_temp(&tmp, "node.secret.tmp.");

    let err = staged_key.commit().unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("failed to rename"), "got: {msg}");
    assert!(
        msg.contains("stays live"),
        "error should report the prior key was restored: {msg}"
    );

    // The prior key is back in place (NOT missing), byte-for-byte.
    assert!(
        node_path.exists(),
        "node.secret must be restored, not missing"
    );
    assert_eq!(
        fs::read(&node_path).unwrap(),
        original_node,
        "the prior node key must be restored byte-for-byte"
    );
    // The restoring rename consumed the archive; no temp or bak litter remains.
    assert!(
        bak_files(&tmp).is_empty(),
        "restore must consume the .bak archive"
    );
    assert_eq!(tmp_file_count(&tmp), 0, "no staged temp should remain");
}

/// Fail-safe commit (#844), eth-keystore side: a failed install rename after the
/// prior `keystore.json` was archived restores it in place rather than leaving
/// the canonical path missing.
#[test]
fn commit_restores_prior_keystore_when_install_rename_fails() {
    let tmp = TempDir::new().unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let pw_file = write_password_file(&tmp);
    key_gen(&make_args(&tmp, false, pw_file)).unwrap();

    let keystore_path = keystore_path_of(&tmp);
    let original_keystore = fs::read(&keystore_path).unwrap();

    let staged_keystore = eth_identity::stage_keystore(tmp.path(), TEST_PASSWORD).unwrap();
    delete_staged_temp(&tmp, "keystore.json.tmp.");

    let err = staged_keystore.commit().unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("failed to rename"), "got: {msg}");
    assert!(
        msg.contains("stays live"),
        "error should report the prior keystore was restored: {msg}"
    );

    assert!(
        keystore_path.exists(),
        "keystore.json must be restored, not missing"
    );
    assert_eq!(
        fs::read(&keystore_path).unwrap(),
        original_keystore,
        "the prior keystore must be restored byte-for-byte"
    );
    assert!(
        bak_files(&tmp).is_empty(),
        "restore must consume the .bak archive"
    );
    assert_eq!(tmp_file_count(&tmp), 0, "no staged temp should remain");
}
