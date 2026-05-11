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
use tempfile::TempDir;

const TEST_PASSWORD: &str = "hunter2";

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
