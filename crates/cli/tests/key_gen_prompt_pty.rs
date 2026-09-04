//! The interactive keystore-password prompt, driven over a pty.
//!
//! The prompt is out of reach of an ordinary integration test. `rpassword`
//! reads and writes `/dev/tty` rather than stdin, so redirecting a child's
//! stdin cannot drive it, and `eth_identity::read_password` skips the `Prompt`
//! source outright when stdin is not a TTY — a headless run either settles on
//! an earlier source or fails with `no keystore password source available`.
//! `rexpect` forks the child onto a pty and makes that pty its controlling
//! terminal, which is what puts the prompt within reach of an assertion.
//!
//! Two properties live here, one per
//! [`decdn_incentive::eth_identity::PasswordUse`] variant: `key-gen` confirms a
//! password it is about to seal into new ciphertext, and a command opening an
//! existing keystore asks once. The variant docs carry the reasoning; these
//! tests carry the evidence.
//!
//! `key_gen_e2e.rs` drives the `key_gen` handler in-process by design, and
//! `key_gen_password_sources.rs` spawns the binary with stdin closed. Only a
//! controlling terminal reaches this path, so it gets its own file.

#![cfg(unix)] // pty forking and the permission-sensitive data dir are POSIX-only.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use decdn_incentive::eth_identity;
use rexpect::session::PtySession;
use tempfile::TempDir;

/// Bound on every `exp_*` call, so a child that never prompts fails with
/// rexpect's "expected X, got Y" instead of stalling into nextest's own 300s
/// backstop, which reports far less. Ample: the workspace pins the keystore KDF
/// crates to `opt-level = 3` in dev, so scrypt is fast even here.
const TIMEOUT_MS: u64 = 10_000;

/// Sent over the pty in the clear, and echoed back into the captured output
/// whenever the test wins the race against `rpassword` disabling terminal echo.
/// Every password constant in this file stays obviously fake for that reason.
const PASSWORD: &str = "hunter2";

const PROMPT: &str = "eth keystore password: ";
const CONFIRM_PROMPT: &str = "eth keystore password (confirm): ";

/// An isolated `HOME` plus a `0o700` output dir — what `key-gen` needs before
/// it will write key material.
///
/// The mode is mandatory, not a hedge: `TempDir` creates with the ambient umask
/// (`0o755` under the common default), and `ensure_data_dir` rejects any group
/// or other bit. Mirrors `dirs()` in `key_gen_password_sources.rs`.
fn dirs() -> (TempDir, TempDir) {
    let home = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    fs::set_permissions(out.path(), fs::Permissions::from_mode(0o700)).unwrap();
    (home, out)
}

/// Spawn the built `decdn` binary on a fresh pty with the developer's
/// environment sealed off.
///
/// `common::decdn_command` strips the whole `DECDN_*` namespace and pins
/// `HOME`, which is what leaves the prompt as the only password source
/// `read_password` can reach — `DECDN_KEYSTORE_PASSWORD` and the
/// `--keystore-password-file` env fallback both precede it.
fn spawn(home: &Path, args: &[&str]) -> PtySession {
    let mut cmd = common::decdn_command(home);
    cmd.args(args);
    rexpect::session::spawn_command(cmd, Some(TIMEOUT_MS)).unwrap()
}

fn spawn_key_gen(home: &TempDir, out: &TempDir) -> PtySession {
    spawn(
        home.path(),
        &["key-gen", "--output-dir", &out.path().to_string_lossy()],
    )
}

fn keystore_path(out: &TempDir) -> PathBuf {
    eth_identity::keystore_path(out.path())
}

fn node_key_path(out: &TempDir) -> PathBuf {
    decdn_common::identity::key_path(out.path())
}

/// Bind then drop an ephemeral loopback port. The OS just confirmed it free, so
/// a connect refuses fast and deterministically. Same helper, same reasoning as
/// `setup_redacts_rpc_error.rs`.
fn refused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Reap the child and report its exit code. Call only after `exp_eof`, which is
/// what guarantees the process has finished writing and is on its way out —
/// `wait()` itself is unbounded.
fn exit_code(session: &PtySession) -> i32 {
    match session.process().wait().unwrap() {
        rexpect::process::WaitStatus::Exited(_, code) => code,
        other => panic!("child did not exit normally: {other:?}"),
    }
}

/// The whole point of `PasswordUse::Create`: entries that disagree are refused,
/// and once the retries are spent nothing has been written.
///
/// Switching `key_gen` to `PasswordUse::Unlock` would break this test — the
/// child would never ask for a confirmation, and would seal the keystore with
/// the first entry.
///
/// The three pairs differ by shrinking margins, ending on a lone trailing
/// space. A comparison narrowed to `trim()`, to ASCII case, or to a prefix
/// would still reject two strangers, so a far miss alone would leave the
/// equality check effectively unasserted — and a near miss is the typo this
/// prompt exists to catch.
#[test]
fn key_gen_refuses_mismatched_password_confirmation() {
    const PAIRS: [(&str, &str); 3] = [
        ("correct horse", "battery staple"),
        ("hunter2", "Hunter2"),
        ("hunter2", "hunter2 "),
    ];

    let (home, out) = dirs();
    let mut session = spawn_key_gen(&home, &out);

    // `prompt_password` allows 3 attempts before giving up. Each attempt is
    // driven to completion so the failure is three genuine mismatches, not a
    // half-written line the child was still waiting on.
    for (first, second) in PAIRS {
        session.exp_string(PROMPT).unwrap();
        session.send_line(first).unwrap();
        session.exp_string(CONFIRM_PROMPT).unwrap();
        session.send_line(second).unwrap();
    }

    session
        .exp_string("passwords did not match after 3 attempts")
        .unwrap();
    session.exp_eof().unwrap();
    assert_ne!(exit_code(&session), 0, "expected a nonzero exit");

    // The password is sourced before any disk write precisely so an abandoned
    // prompt leaves no half-written identity behind.
    assert!(
        !node_key_path(&out).exists(),
        "node key written despite a refused password"
    );
    assert!(
        !keystore_path(&out).exists(),
        "keystore written despite a refused password"
    );
}

/// A confirmed entry is accepted, and the keystore that lands decrypts with it:
/// the password survives the double read intact rather than arriving mangled,
/// truncated, or carrying the line ending the pty discipline added.
#[test]
fn key_gen_accepts_a_matching_confirmation() {
    let (home, out) = dirs();
    let mut session = spawn_key_gen(&home, &out);

    session.exp_string(PROMPT).unwrap();
    session.send_line(PASSWORD).unwrap();
    session.exp_string(CONFIRM_PROMPT).unwrap();
    session.send_line(PASSWORD).unwrap();

    session.exp_string("eth address: ").unwrap();
    session.exp_eof().unwrap();
    assert_eq!(exit_code(&session), 0, "expected a successful exit");

    assert!(node_key_path(&out).exists(), "node key not written");
    let keystore = keystore_path(&out);
    assert!(keystore.exists(), "keystore not written");
    eth_identity::load_signer(&keystore, PASSWORD)
        .expect("keystore does not decrypt with the confirmed password");
}

/// A password file is consulted before the prompt even when a terminal is
/// available. `standard_sources` pins that order structurally, but every
/// headless test asserts it with stdin closed, where the prompt is skipped
/// whatever the order — so a `read_password` loop that reached the prompt first
/// would hang an interactive `key-gen --keystore-password-file` with nothing to
/// catch it. Arriving at `eth address:` is the assertion: a prompt here blocks,
/// and the child never gets there.
#[test]
fn a_password_file_wins_over_an_available_prompt() {
    let (home, out) = dirs();
    let pw_file = out.path().join("pw.txt");
    fs::write(&pw_file, PASSWORD).unwrap();

    let mut session = spawn(
        home.path(),
        &[
            "key-gen",
            "--output-dir",
            &out.path().to_string_lossy(),
            "--keystore-password-file",
            &pw_file.to_string_lossy(),
        ],
    );

    session.exp_string("eth address: ").unwrap();
    session.exp_eof().unwrap();
    assert_eq!(exit_code(&session), 0, "expected a successful exit");
    eth_identity::load_signer(&keystore_path(&out), PASSWORD)
        .expect("keystore does not decrypt with the password from the file");
}

/// The `PasswordUse::Unlock` half: a command opening an existing keystore asks
/// once and never confirms.
///
/// Untested, this is #1941 in the other direction — deleting the `Unlock` arm
/// of `prompt_password` would make every interactive unlock prompt twice and
/// reject a mismatch, and no other test in the workspace reaches that branch.
///
/// `pool assign` is the cheapest command that loads the operator keystore: it
/// signs the capability offline and only then reads the chain, so an
/// unreachable RPC still exercises the whole password path. Whether it then
/// fails on that read or warns and issues the token is beside the point — the
/// assertion is that no confirmation prompt appeared before the child exited.
#[test]
fn unlocking_an_existing_keystore_asks_once() {
    let (home, out) = dirs();
    // Written directly rather than through `key-gen`, so the prompt this test
    // drives is unambiguously the unlock one.
    eth_identity::generate_and_persist(out.path(), PASSWORD, false).unwrap();

    let rpc_url = format!("http://127.0.0.1:{}", refused_port());
    let mut session = spawn(
        home.path(),
        &[
            "pool",
            "assign",
            "--pool",
            &format!("0x{}", "11".repeat(32)),
            "--signer",
            "0x000000000000000000000000000000000000dEaD",
            "--cap-micro-usdc",
            "1",
            "--expiry-secs",
            "3600",
            "--rpc-url",
            &rpc_url,
            "--chain-id",
            "421614",
            "--payment-pool-address",
            "0x00000000000000000000000000000000000000A1",
            "--keystore",
            &keystore_path(&out).to_string_lossy(),
            "--data-dir",
            &out.path().to_string_lossy(),
        ],
    );

    session.exp_string(PROMPT).unwrap();
    session.send_line(PASSWORD).unwrap();

    // Everything the child writes from here until it exits. A second prompt
    // would instead block on input the test never sends, failing on timeout.
    let rest = session.exp_eof().unwrap();
    assert!(
        !rest.contains("(confirm)"),
        "unlock asked for a confirmation: {rest}"
    );
}
