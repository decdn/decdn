//! `decdn key-gen` password-source behavior that only a real process can show.
//!
//! Set-versus-unset for `DECDN_KEYSTORE_PASSWORD` is invisible in-process: the
//! workspace forbids `unsafe`, and `std::env::set_var` is `unsafe` under
//! edition 2024. These tests therefore spawn the built binary through
//! `common::decdn_command`, which strips the whole `DECDN_*` namespace before
//! anything is set — so both the "set to empty" and the "unset" arm are
//! deterministic on a developer machine that exports the variable.
//!
//! `crates/cli/tests/key_gen_e2e.rs` drives the handler in-process by design
//! and stays that way. These tests close stdin, so the `Prompt` source always
//! falls through; the prompt itself needs a controlling terminal and is driven
//! in `key_gen_prompt_pty.rs`.

#![cfg(unix)] // Permission assertions are POSIX-specific.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use decdn_incentive::eth_identity;
use tempfile::TempDir;

/// An isolated `HOME` plus a `0o700` output dir, the two things `key-gen`
/// needs before it will write key material.
fn dirs() -> (TempDir, TempDir) {
    let home = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    fs::set_permissions(out.path(), fs::Permissions::from_mode(0o700)).unwrap();
    (home, out)
}

fn key_gen(home: &Path, out: &Path) -> Command {
    let mut cmd = common::decdn_command(home);
    cmd.args(["key-gen", "--output-dir"])
        .arg(out)
        // No TTY, so the `Prompt` source falls through rather than blocking.
        .stdin(Stdio::null());
    cmd
}

fn run(cmd: &mut Command) -> Output {
    cmd.output().expect("spawn decdn")
}

fn keystore(out: &TempDir) -> PathBuf {
    eth_identity::keystore_path(out.path())
}

/// Creating under an empty `DECDN_KEYSTORE_PASSWORD` is refused (#1935): a
/// set-but-empty env var is almost always a shell expanding an unset variable,
/// and creation is the unverifiable direction. The error names the source and
/// nothing is written. A deliberate empty password keeps a reachable path
/// through an empty `--keystore-password-file` (covered below).
#[test]
fn creating_under_an_empty_env_password_is_refused_naming_the_source() {
    let (home, out) = dirs();
    let pw_file = out.path().join("pw.txt");
    fs::write(&pw_file, b"not-this-one").unwrap();

    let output = run(key_gen(home.path(), out.path())
        .arg("--keystore-password-file")
        .arg(&pw_file)
        .env(eth_identity::KEYSTORE_PASSWORD_ENV, ""));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "an empty env password must not create a keystore: {stderr}"
    );
    assert!(
        stderr.contains(eth_identity::KEYSTORE_PASSWORD_ENV),
        "the error must name the env source: {stderr}"
    );
    assert!(
        !keystore(&out).exists(),
        "no key material may be written when an empty env password is refused"
    );
    assert!(
        !decdn_common::identity::key_path(out.path()).exists(),
        "node.secret must not be half-written either"
    );
}

/// A `--keystore-password-file` shadowed by a set `DECDN_KEYSTORE_PASSWORD`
/// warns that the file was not used (#1934) — a previously silent inert flag —
/// while the env var still wins per precedence.
#[test]
fn a_shadowed_password_file_warns_and_env_still_wins() {
    let (home, out) = dirs();
    let pw_file = out.path().join("pw.txt");
    fs::write(&pw_file, b"file-password").unwrap();

    let output = run(key_gen(home.path(), out.path())
        .arg("--keystore-password-file")
        .arg(&pw_file)
        .env(eth_identity::KEYSTORE_PASSWORD_ENV, "env-password"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "key-gen failed: {stderr}");

    eth_identity::load_signer(&keystore(&out), "env-password")
        .expect("the env var must win over the shadowed file");
    assert!(
        stderr.contains(&pw_file.display().to_string()) && stderr.contains("was not used"),
        "a shadowed password file must be named in a warning: {stderr}"
    );
}

/// The other half of the presence rule: an unset variable still falls through
/// to `--keystore-password-file`, which is what every other `key-gen` test assumes.
#[test]
fn an_unset_env_password_falls_through_to_the_password_file() {
    let (home, out) = dirs();
    let pw_file = out.path().join("pw.txt");
    fs::write(&pw_file, b"hunter2\n").unwrap();

    let output = run(key_gen(home.path(), out.path())
        .arg("--keystore-password-file")
        .arg(&pw_file));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "key-gen failed: {stderr}");
    eth_identity::load_signer(&keystore(&out), "hunter2").expect("the file password must open it");
    assert!(
        !stderr.contains("EMPTY password"),
        "a non-empty password must not warn: {stderr}"
    );
}

/// An empty `--keystore-password-file` is a file that exists, so it supplies the empty
/// password rather than falling through to a prompt no daemon can answer.
#[test]
fn an_empty_password_file_creates_an_empty_password_keystore() {
    let (home, out) = dirs();
    let pw_file = out.path().join("empty.pw");
    fs::write(&pw_file, b"").unwrap();

    let output = run(key_gen(home.path(), out.path())
        .arg("--keystore-password-file")
        .arg(&pw_file));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "key-gen failed: {stderr}");
    eth_identity::load_signer(&keystore(&out), "").expect("the empty password must open it");
    assert!(stderr.contains("EMPTY password"), "must warn: {stderr}");
}

/// A variable that is SET but not valid UTF-8 is present-and-unreadable, so it
/// is fatal rather than a fall-through to the password file beside it. Without
/// this, a revert to a catch-all `Err(_) => skipped.push(...)` would silently
/// restore the old fall-through with a green suite.
#[test]
fn a_non_utf8_env_password_is_fatal_and_does_not_echo_the_value() {
    let (home, out) = dirs();
    let pw_file = out.path().join("pw.txt");
    fs::write(&pw_file, b"not-this-one").unwrap();
    let bad = OsString::from_vec(vec![0x70, 0xff, 0x77]);

    let output = run(key_gen(home.path(), out.path())
        .arg("--keystore-password-file")
        .arg(&pw_file)
        .env(eth_identity::KEYSTORE_PASSWORD_ENV, &bad));
    assert!(
        !output.status.success(),
        "a set-but-unreadable env var must not fall through to the file"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("is not valid UTF-8"),
        "the error must name the cause: {stderr}"
    );
    assert!(
        !stderr.contains("\u{fffd}") && !stderr.contains("pw"),
        "the error must not echo the value: {stderr}"
    );
    assert!(
        !keystore(&out).exists(),
        "no key material may be written on a fatal password error"
    );
}

/// `--keystore-password-file '~/pw.txt'` reaches the process literally when the shell
/// quotes it, so the CLI expands it. Under the presence rule an unexpanded
/// `~/pw.txt` would fall through as a missing file instead of erroring, so
/// nothing else would notice the expansion being dropped.
#[test]
fn a_tilde_password_file_is_expanded_against_home() {
    let (home, out) = dirs();
    fs::write(home.path().join("pw.txt"), b"hunter2\n").unwrap();

    let output = run(key_gen(home.path(), out.path())
        .arg("--keystore-password-file")
        .arg("~/pw.txt"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "key-gen failed: {stderr}");
    eth_identity::load_signer(&keystore(&out), "hunter2")
        .expect("the tilde path must resolve to the file under HOME");
}

/// A `--keystore-password-file` that does not exist falls through rather than erroring
/// at the read. With no TTY and no env var every source is exhausted, and the
/// resulting error names the path that was tried — which is what keeps a typo
/// diagnosable.
#[test]
fn a_missing_password_file_fails_naming_the_path() {
    let (home, out) = dirs();
    let missing = out.path().join("nope.pw");

    let output = run(key_gen(home.path(), out.path())
        .arg("--keystore-password-file")
        .arg(&missing));
    assert!(!output.status.success(), "a missing file must not succeed");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no keystore password source"),
        "got: {stderr}"
    );
    assert!(
        stderr.contains(&missing.display().to_string()),
        "the error must name the path tried: {stderr}"
    );
    assert!(
        !keystore(&out).exists(),
        "no key material may be written when the password cannot be sourced"
    );
    assert!(
        !decdn_common::identity::key_path(out.path()).exists(),
        "node.secret must not be half-written either — the password is sourced \
         before any disk write"
    );
}
