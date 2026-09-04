//! `decdn key-gen`'s interactive keystore-password prompt, driven over a pty.
//!
//! `key-gen` is the only command that CREATES a keystore, so it is the only one
//! that asks for the password twice and requires the entries to match
//! ([`decdn_incentive::eth_identity::PasswordUse::Create`]). A password typed
//! once has nothing to check it against: a typo would be scrypt-encrypted into
//! `keystore.json` and stay undiscovered until the next unlock fails, by which
//! point the key is unrecoverable.
//!
//! That double entry is reachable only through a controlling terminal —
//! `rpassword` reads and writes `/dev/tty`, not stdin, and
//! `eth_identity::read_password` skips the prompt source entirely when stdin is
//! not a TTY. Every headless test therefore short-circuits at the env var or
//! the password file before the prompt is consulted. `rexpect` forks the child
//! onto a pty and makes it the session leader, which is what puts the prompt
//! within reach of an assertion.
//!
//! These tests spawn the real `decdn` binary rather than calling the `key_gen`
//! handler in-process (which is what `key_gen_e2e.rs` does): the prompt needs a
//! process whose controlling terminal the test owns, and the test harness's own
//! terminal is not that.

#![cfg(unix)] // pty forking and the permission-sensitive data dir are POSIX-only.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use decdn_incentive::eth_identity;
use rexpect::session::PtySession;
use tempfile::TempDir;

/// Every `exp_*` call is bounded so a child that never prompts fails the test
/// instead of wedging CI. Generous next to the sub-second prompt round trip,
/// but `key-gen` also runs scrypt on the success path.
const TIMEOUT_MS: u64 = 30_000;

const PROMPT: &str = "eth keystore password: ";
const CONFIRM_PROMPT: &str = "eth keystore password (confirm): ";

/// A data dir `key-gen` will accept. `TempDir` is usually `0o700` already, but
/// CI can run under umask 002, and `ensure_data_dir` rejects any group/world
/// bit — so set the mode rather than depend on the ambient umask.
fn data_dir() -> TempDir {
    let dir = TempDir::new().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

/// Spawn `decdn key-gen --output-dir <dir>` on a fresh pty.
///
/// Both password sources that precede the prompt are removed from the child's
/// environment, and no `--keystore-password-file` is passed, so the prompt is
/// the only source `read_password` can reach. Removing them on the `Command`
/// keeps the parent's own environment untouched — `std::env::set_var` is unsafe
/// and forbidden workspace-wide.
fn spawn_key_gen(dir: &TempDir) -> PtySession {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_decdn"));
    cmd.arg("key-gen")
        .arg("--output-dir")
        .arg(dir.path())
        .env_remove("DECDN_KEYSTORE_PASSWORD")
        .env_remove("DECDN_KEYSTORE_PASSWORD_FILE");
    rexpect::session::spawn_command(cmd, Some(TIMEOUT_MS)).unwrap()
}

fn keystore_path(dir: &TempDir) -> std::path::PathBuf {
    eth_identity::keystore_path(dir.path())
}

fn node_key_path(dir: &TempDir) -> std::path::PathBuf {
    decdn_common::identity::key_path(dir.path())
}

/// The whole point of `PasswordUse::Create`: entries that disagree are refused,
/// and after the retries are spent nothing has been written.
///
/// This is the test that fails if `key_gen` is switched to
/// `PasswordUse::Unlock`: the first entry would then be taken as the password
/// and both files would land, mismatch and all.
#[test]
fn key_gen_refuses_mismatched_password_confirmation() {
    let dir = data_dir();
    let mut session = spawn_key_gen(&dir);

    // `prompt_password` allows 3 attempts before giving up. Each attempt is
    // driven to completion so the failure is "three genuine mismatches", not a
    // half-written line the child was still waiting on.
    for _ in 0..3 {
        session.exp_string(PROMPT).unwrap();
        session.send_line("correct horse").unwrap();
        session.exp_string(CONFIRM_PROMPT).unwrap();
        session.send_line("battery staple").unwrap();
    }

    session
        .exp_string("passwords did not match after 3 attempts")
        .unwrap();
    session.exp_eof().unwrap();
    assert_failed(&session);

    // The password is sourced before any disk write precisely so an abandoned
    // prompt leaves no half-written identity behind.
    assert!(
        !node_key_path(&dir).exists(),
        "node key written despite a refused password"
    );
    assert!(
        !keystore_path(&dir).exists(),
        "keystore written despite a refused password"
    );
}

/// The second read is a confirmation of the first, not a second password: the
/// keystore that lands decrypts with the entry that was typed twice, and the
/// confirmation line is not left behind to be read as something else.
#[test]
fn key_gen_accepts_a_matching_confirmation() {
    const PASSWORD: &str = "hunter2";

    let dir = data_dir();
    let mut session = spawn_key_gen(&dir);

    session.exp_string(PROMPT).unwrap();
    session.send_line(PASSWORD).unwrap();
    session.exp_string(CONFIRM_PROMPT).unwrap();
    session.send_line(PASSWORD).unwrap();

    session.exp_string("eth address: ").unwrap();
    session.exp_eof().unwrap();
    assert_succeeded(&session);

    assert!(node_key_path(&dir).exists(), "node key not written");
    let keystore = keystore_path(&dir);
    assert!(keystore.exists(), "keystore not written");
    // Decryption is the real assertion: it proves the committed file is sealed
    // with the entry the operator typed, not with the confirmation line read as
    // a password of its own.
    eth_identity::load_signer(&keystore, PASSWORD)
        .expect("keystore does not decrypt with the confirmed password");
}

fn assert_succeeded(session: &PtySession) {
    assert_eq!(exit_code(session), 0, "expected a successful exit");
}

fn assert_failed(session: &PtySession) {
    assert_ne!(exit_code(session), 0, "expected a nonzero exit");
}

/// Reap the child and report its exit code. Call only after `exp_eof`, which is
/// what guarantees the process has finished writing and is on its way out.
fn exit_code(session: &PtySession) -> i32 {
    match session.process().wait().unwrap() {
        rexpect::process::WaitStatus::Exited(_, code) => code,
        other => panic!("child did not exit normally: {other:?}"),
    }
}
