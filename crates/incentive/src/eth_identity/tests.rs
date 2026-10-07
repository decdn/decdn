use super::*;
use alloy::primitives::{B256, keccak256};
use alloy::signers::{Signer, SignerSync};
use alloy::sol;
use alloy::sol_types::{SolStruct, eip712_domain};
use std::os::unix::fs::PermissionsExt;
use tempfile::TempDir;

const TEST_PASSWORD: &str = "hunter2";

fn make_data_dir() -> TempDir {
    let tmp = TempDir::new().unwrap();
    // TempDir defaults are usually 0o700 already, but CI may run under
    // umask 002 — force the mode explicitly so the validation passes.
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    tmp
}

/// The builder every binary routes through: `Env` first, the `File` only
/// when the caller passed a path, `Prompt` last carrying `usage`.
/// Dropping the `Prompt` push would make every interactive command
/// headless-only, and pushing `File` ahead of `Env` would invert the
/// documented precedence. Neither shows up in the [`read_password`] tests
/// below: those drive a list handed to them, so they pin how a list is
/// consumed, never how this one is built.
#[test]
fn password_sources_orders_env_then_file_then_prompt() {
    let with_file = standard_sources(Some(PathBuf::from("/abs/pw.txt")), PasswordUse::Unlock);
    assert!(
        matches!(
            with_file.as_slice(),
            [
                PasswordSource::Env(name),
                PasswordSource::File(p),
                PasswordSource::Prompt {
                    usage: PasswordUse::Unlock
                },
            ] if *name == KEYSTORE_PASSWORD_ENV
                && p == Path::new("/abs/pw.txt")
        ),
        "got: {with_file:?}"
    );

    // No path => no `File` entry at all, so an operator who passed no flag
    // never sees a missing-file skip reason.
    let without = standard_sources(None, PasswordUse::Create);
    assert!(
        matches!(
            without.as_slice(),
            [
                PasswordSource::Env(_),
                PasswordSource::Prompt {
                    usage: PasswordUse::Create
                },
            ]
        ),
        "got: {without:?}"
    );
}

#[test]
fn generates_keystore_with_secure_perms() {
    let tmp = make_data_dir();
    let _addr = generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
    let path = keystore_path(tmp.path());
    let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "keystore file should be 0o600, was {mode:#o}");
    let dir_mode = fs::metadata(tmp.path()).unwrap().permissions().mode() & 0o777;
    assert_eq!(
        dir_mode, 0o700,
        "data_dir should be 0o700, was {dir_mode:#o}"
    );
}

#[test]
fn skips_when_keystore_exists_and_force_false() {
    let tmp = make_data_dir();
    generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
    let err = generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("already exists"), "got: {msg}");
    assert!(msg.contains("--force"), "got: {msg}");
}

#[test]
fn force_overwrites_existing_keystore() {
    let tmp = make_data_dir();
    let first = generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
    let second = generate_and_persist(tmp.path(), TEST_PASSWORD, true).unwrap();
    assert_ne!(
        first, second,
        "force should produce a fresh key with a different address"
    );
}

/// `--force` must archive (not delete) the prior keystore so the
/// operator key-rotation runbook (`appendix-operator-key-rotation.md`
/// §1 step 9, §5 rollback) has the previous ciphertext to fall back on.
#[test]
fn force_archives_old_keystore_to_bak() {
    let tmp = make_data_dir();
    generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
    let original = fs::read(keystore_path(tmp.path())).unwrap();

    generate_and_persist(tmp.path(), TEST_PASSWORD, true).unwrap();

    let mut bak: Option<PathBuf> = None;
    for entry in fs::read_dir(tmp.path()).unwrap() {
        let p = entry.unwrap().path();
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name.starts_with("keystore.json.bak.") {
            bak = Some(p);
            break;
        }
    }
    let bak = bak.expect("force should produce a `keystore.json.bak.<ts>` file");
    let archived = fs::read(&bak).unwrap();
    assert_eq!(
        archived, original,
        "bak file must hold the byte-for-byte prior keystore"
    );
}

#[test]
fn round_trip_address_recovery() {
    let tmp = make_data_dir();
    let written = generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
    let signer = load_signer(&keystore_path(tmp.path()), TEST_PASSWORD).unwrap();
    assert_eq!(
        signer.address(),
        written,
        "loaded signer must derive to the address returned by generate"
    );
}

/// The empty-password twin of [`round_trip_address_recovery`]: a keystore
/// created with `""` opens with `""` and derives the same address (#1931).
#[test]
fn empty_password_round_trip() {
    let tmp = make_data_dir();
    let written = generate_and_persist(tmp.path(), "", false).unwrap();
    let signer = load_signer(&keystore_path(tmp.path()), "").unwrap();
    assert_eq!(
        signer.address(),
        written,
        "an empty password must round-trip like any other"
    );
}

/// Proves the empty password is really applied to the KDF rather than
/// treated as "no password set": a non-empty guess must not open it.
#[test]
fn wrong_password_rejected_for_empty_keystore() {
    let tmp = make_data_dir();
    generate_and_persist(tmp.path(), "", false).unwrap();
    let err = load_signer(&keystore_path(tmp.path()), TEST_PASSWORD).unwrap_err();
    assert!(format!("{err:#}").contains("decrypt"), "got: {err:#}");
}

#[test]
fn wrong_password_rejected() {
    let tmp = make_data_dir();
    generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
    let err = load_signer(&keystore_path(tmp.path()), "definitely-wrong").unwrap_err();
    let chain = format!("{err:#}");
    assert!(
        chain.contains("decrypt"),
        "error should mention decrypt failure, got: {chain}"
    );
    assert!(
        !chain.contains(TEST_PASSWORD),
        "error must not echo the correct password, got: {chain}"
    );
    assert!(
        !chain.contains("definitely-wrong"),
        "error must not echo the attempted password, got: {chain}"
    );
}

#[test]
fn missing_keystore_path_rejected() {
    let tmp = make_data_dir();
    let path = tmp.path().join("does-not-exist.json");
    let err = load_signer(&path, TEST_PASSWORD).unwrap_err();
    assert!(format!("{err:#}").contains("cannot access"), "got: {err:#}");
}

#[test]
fn malformed_json_rejected() {
    let tmp = make_data_dir();
    let path = keystore_path(tmp.path());
    fs::write(&path, b"{not json").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let err = load_signer(&path, TEST_PASSWORD).unwrap_err();
    assert!(format!("{err:#}").contains("decrypt"), "got: {err:#}");
}

#[test]
fn non_v3_keystore_rejected() {
    let tmp = make_data_dir();
    let path = keystore_path(tmp.path());
    fs::write(&path, br#"{"version":1,"address":"0x0"}"#).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let err = load_signer(&path, TEST_PASSWORD).unwrap_err();
    assert!(format!("{err:#}").contains("decrypt"), "got: {err:#}");
}

#[test]
fn rejects_keystore_with_group_perms() {
    let tmp = make_data_dir();
    generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
    let path = keystore_path(tmp.path());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
    let err = load_signer(&path, TEST_PASSWORD).unwrap_err();
    assert!(
        format!("{err:#}").contains("insecure permissions"),
        "got: {err:#}"
    );
}

#[test]
fn rejects_symlinked_keystore() {
    let tmp = make_data_dir();
    // Generate a real keystore, then point a symlink at it.
    let real = tmp.path().join("real.json");
    fs::write(&real, b"placeholder").unwrap();
    fs::set_permissions(&real, fs::Permissions::from_mode(0o600)).unwrap();
    let link = keystore_path(tmp.path());
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let err = load_signer(&link, TEST_PASSWORD).unwrap_err();
    assert!(
        format!("{err:#}").contains("not a regular file"),
        "got: {err:#}"
    );
}

#[test]
fn first_matching_source_wins() {
    // `read_password` stops at the first PRESENT source. Using two
    // `File` sources exercises the same precedence loop as `Env` would; the
    // `unsafe_code = "forbid"` workspace lint blocks
    // `std::env::set_var` (edition 2024), so we drive the loop via
    // files instead. The "Env > File > Prompt" priority is a property
    // of how callers construct the slice, not of `read_password`.
    let tmp = make_data_dir();
    let first = tmp.path().join("first.txt");
    let second = tmp.path().join("second.txt");
    fs::write(&first, b"first-wins").unwrap();
    fs::write(&second, b"second-loses").unwrap();
    let pw = read_password(
        &[PasswordSource::File(first), PasswordSource::File(second)],
        "ignored",
    )
    .unwrap();
    assert_eq!(pw.secret(), "first-wins");
}

#[test]
fn missing_file_source_falls_through() {
    // First source is a path that does not exist (an absent source),
    // second has content.
    let tmp = make_data_dir();
    let missing = tmp.path().join("not-there.txt");
    let real = tmp.path().join("real.txt");
    fs::write(&real, b"actual-pw").unwrap();
    let pw = read_password(
        &[PasswordSource::File(missing), PasswordSource::File(real)],
        "ignored",
    )
    .unwrap();
    assert_eq!(pw.secret(), "actual-pw");
}

/// A file that fell through as not-found before a later source won is worth
/// a warning naming the path (#1934): the usual cause is a typo that looks
/// fine at the terminal and fails headless. The warning names the skipped
/// path and the source that actually supplied the password.
#[test]
fn a_skipped_file_before_the_winner_warns_naming_the_path() {
    let tmp = make_data_dir();
    let missing = tmp.path().join("typo.txt");
    let real = tmp.path().join("real.txt");
    fs::write(&real, b"actual-pw").unwrap();
    let resolved = read_password(
        &[
            PasswordSource::File(missing.clone()),
            PasswordSource::File(real.clone()),
        ],
        "ignored",
    )
    .unwrap();
    assert_eq!(resolved.warnings().len(), 1, "{:?}", resolved.warnings());
    let warning = resolved.warnings().first().unwrap();
    assert!(
        warning.contains(&missing.display().to_string()),
        "{warning}"
    );
    assert!(warning.contains(&real.display().to_string()), "{warning}");
    assert!(warning.contains("not found"), "{warning}");
    assert!(
        !warning.contains("actual-pw"),
        "must not echo the password: {warning}"
    );
}

/// A file configured after the source that won was never consulted, so its
/// flag is inert — warn that it was not used (#1934, the shadowed case).
#[test]
fn a_file_after_the_winner_warns_it_was_unused() {
    let tmp = make_data_dir();
    let winner = tmp.path().join("winner.txt");
    let shadowed = tmp.path().join("shadowed.txt");
    fs::write(&winner, b"winning-pw").unwrap();
    fs::write(&shadowed, b"never-read").unwrap();
    let resolved = read_password(
        &[
            PasswordSource::File(winner),
            PasswordSource::File(shadowed.clone()),
        ],
        "ignored",
    )
    .unwrap();
    let warning = resolved.warnings().first().expect("one warning");
    assert!(
        warning.contains(&shadowed.display().to_string()),
        "{warning}"
    );
    assert!(warning.contains("was not used"), "{warning}");
}

/// The common case: the single source that wins produces no warning and
/// reports itself as the origin.
#[test]
fn a_clean_single_source_resolve_has_no_warnings() {
    let tmp = make_data_dir();
    let pw_file = tmp.path().join("pw.txt");
    fs::write(&pw_file, b"hunter2").unwrap();
    let resolved = read_password(&[PasswordSource::File(pw_file.clone())], "ignored").unwrap();
    assert!(resolved.warnings().is_empty(), "{:?}", resolved.warnings());
    assert_eq!(resolved.origin(), &PasswordOrigin::File(pw_file));
}

#[test]
fn password_file_trailing_newline_stripped() {
    let tmp = make_data_dir();
    let pw_file = tmp.path().join("pw.txt");
    fs::write(&pw_file, b"hunter2\n").unwrap();
    let pw = read_password(&[PasswordSource::File(pw_file)], "ignored").unwrap();
    assert_eq!(pw.secret(), "hunter2");
}

#[test]
fn password_file_preserves_internal_newlines() {
    let tmp = make_data_dir();
    let pw_file = tmp.path().join("pw.txt");
    fs::write(&pw_file, b"line1\nline2").unwrap();
    let pw = read_password(&[PasswordSource::File(pw_file)], "ignored").unwrap();
    // No trailing newline to strip; internal newline preserved.
    assert_eq!(pw.secret(), "line1\nline2");
}

#[test]
fn password_file_strips_crlf() {
    let tmp = make_data_dir();
    let pw_file = tmp.path().join("pw.txt");
    fs::write(&pw_file, b"hunter2\r\n").unwrap();
    let pw = read_password(&[PasswordSource::File(pw_file)], "ignored").unwrap();
    assert_eq!(pw.secret(), "hunter2");
}

#[test]
fn prompt_skipped_when_not_a_tty() {
    // Under `cargo test` / `cargo nextest` stdin is not a TTY, so
    // `Prompt` should fall through and the empty source list path
    // surfaces the standard error.
    let err = read_password(
        &[PasswordSource::Prompt {
            usage: PasswordUse::Unlock,
        }],
        "ignored",
    )
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("no keystore password source") && msg.contains("TTY"),
        "got: {msg}"
    );
}

/// An empty file is a file that is there, so it supplies an empty password
/// rather than falling through. This is what lets an operator load a
/// keystore written with no password without a TTY (#1931).
#[test]
fn empty_password_file_is_an_empty_password() {
    let tmp = make_data_dir();
    let empty = tmp.path().join("empty.txt");
    fs::write(&empty, b"").unwrap();
    let pw = read_password(&[PasswordSource::File(empty)], "ignored").unwrap();
    assert_eq!(pw.secret(), "");
}

/// Presence, not content, decides precedence: an empty file earlier in the
/// slice beats a populated one later rather than falling through to it.
#[test]
fn empty_password_file_beats_a_later_populated_source() {
    let tmp = make_data_dir();
    let empty = tmp.path().join("empty.txt");
    let real = tmp.path().join("real.txt");
    fs::write(&empty, b"").unwrap();
    fs::write(&real, b"never-reached").unwrap();
    let pw = read_password(
        &[PasswordSource::File(empty), PasswordSource::File(real)],
        "ignored",
    )
    .unwrap();
    assert_eq!(pw.secret(), "");
}

/// A path that does not exist falls through, and the exhausted-sources
/// error names it — the only thing that keeps a typo'd
/// `--keystore-password-file` diagnosable, because a missing path falls
/// through rather than erroring at the read.
/// Every skipped source reaches the error, not just the last one. With a
/// single source this cannot be told apart from keeping only the last, so
/// the test drives all three fall-through legs at once.
#[test]
fn exhausted_sources_error_lists_every_skip_reason() {
    // A name no environment sets, rather than `KEYSTORE_PASSWORD_ENV`: a
    // developer who exports the real variable would otherwise fail this
    // test for a reason that has nothing to do with accumulation.
    const UNSET: &str = "DECDN_KEYSTORE_PASSWORD_ABSENT_IN_TESTS";
    let tmp = make_data_dir();
    let missing = tmp.path().join("nope.txt");
    // Stdin is not a TTY under the test harness, so all three fall through.
    let err = read_password(
        &[
            PasswordSource::Env(UNSET),
            PasswordSource::File(missing.clone()),
            PasswordSource::Prompt {
                usage: PasswordUse::Unlock,
            },
        ],
        "ignored",
    )
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains(UNSET), "got: {msg}");
    assert!(msg.contains(&missing.display().to_string()), "got: {msg}");
    assert!(msg.contains("TTY"), "got: {msg}");
}

#[test]
fn missing_password_file_falls_through_and_names_the_path() {
    let tmp = make_data_dir();
    let missing = tmp.path().join("nope.txt");
    let err = read_password(&[PasswordSource::File(missing.clone())], "ignored").unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("no keystore password source"), "got: {msg}");
    assert!(msg.contains(&missing.display().to_string()), "got: {msg}");
}

#[test]
fn password_file_with_only_a_newline_is_an_empty_password() {
    let tmp = make_data_dir();
    let pw_file = tmp.path().join("pw.txt");
    fs::write(&pw_file, b"\n").unwrap();
    let pw = read_password(&[PasswordSource::File(pw_file)], "ignored").unwrap();
    assert_eq!(pw.secret(), "");
}

/// A file that exists but cannot be read names a real misconfiguration, so
/// it is fatal — silently selecting the next source would hide it. A
/// directory is the portable way to provoke a non-`NotFound` read error: a
/// mode-000 file is still readable by root, and CI may run as root.
#[test]
fn unreadable_password_file_is_fatal() {
    let tmp = make_data_dir();
    let dir = tmp.path().join("a-directory");
    fs::create_dir(&dir).unwrap();
    let fallback = tmp.path().join("fallback.txt");
    fs::write(&fallback, b"never-reached").unwrap();
    let err = read_password(
        &[
            PasswordSource::File(dir.clone()),
            PasswordSource::File(fallback),
        ],
        "ignored",
    )
    .unwrap_err();
    let chain = format!("{err:#}");
    assert!(
        chain.contains("failed to read password file"),
        "got: {chain}"
    );
    assert!(
        chain.contains(&dir.display().to_string()),
        "error must name the offending path, got: {chain}"
    );
    assert!(
        !chain.contains("no keystore password source"),
        "an unreadable file must not fall through to the next source: {chain}"
    );
}

// Canonical EIP-712 schema from ADR 003 §EIP-712 NodeId-to-Ethereum
// Binding (`BindNodeId(bytes32 nodeId,uint64 nonce)`). Inlined rather
// than imported from another module: this test asserts the schema we
// build against the keystore-loaded signer matches the ADR exactly,
// so any drift here surfaces independently of the on-chain contract
// wiring (#327).
sol! {
    #[allow(missing_docs)]
    struct BindNodeId {
        bytes32 nodeId;
        uint64 nonce;
    }
}

#[test]
fn eip712_loopback_sign_and_recover() {
    let tmp = make_data_dir();
    let written = generate_and_persist(tmp.path(), TEST_PASSWORD, false).unwrap();
    let signer = load_signer(&keystore_path(tmp.path()), TEST_PASSWORD)
        .unwrap()
        .with_chain_id(Some(421_614));

    let domain = eip712_domain! {
        name: "decdn-test",
        version: "1",
        chain_id: 421_614,
        verifying_contract: Address::ZERO,
    };
    let binding = BindNodeId {
        nodeId: B256::from([0xaau8; 32]),
        nonce: 1,
    };
    let signing_hash: B256 = binding.eip712_signing_hash(&domain);

    let sig = signer.sign_hash_sync(&signing_hash).unwrap();
    let recovered = sig.recover_address_from_prehash(&signing_hash).unwrap();

    assert_eq!(
        recovered, written,
        "recovered EIP-712 signer must match the keystore-derived address"
    );
}

/// Lock the EIP-712 type hash to ADR 003's canonical wording. If this
/// breaks, either the ADR changed or the `sol!` macro's canonical
/// encoding shifted — both warrant a coordinated update with the
/// Solidity contract. Mirrors the equivalent test in
/// `voucher::voucher_type_hash_matches_adr_003`.
#[test]
fn bind_node_id_type_hash_matches_adr_003() {
    // Single space between Solidity type and field name; no other
    // whitespace; fields in declaration order. Per ADR 003 §606.
    let canonical: &[u8] = b"BindNodeId(bytes32 nodeId,uint64 nonce)";
    let expected = keccak256(canonical);
    let actual = keccak256(BindNodeId::eip712_root_type().as_bytes());
    assert_eq!(
        actual, expected,
        "BindNodeId type hash drifted from ADR 003 §EIP-712 NodeId-to-Ethereum Binding"
    );
}
