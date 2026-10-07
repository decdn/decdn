use super::*;
use decdn_incentive::eth_identity::generate_and_persist;

const TEST_PASSWORD: &str = "hunter2";

/// Write a `node.toml` with the given `identity.data_dir` and return its
/// path (plus the owning tempdir, which the caller must keep alive).
fn node_toml_with_data_dir(data_dir: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(&path, format!("[identity]\ndata_dir = \"{data_dir}\"\n")).unwrap();
    (dir, path)
}

/// The config file's `identity.data_dir` is honored when no `--output-dir`
/// is given. This is the reported bug: a node whose `data_dir` lives in
/// `node.toml` must not resolve to the `~/.decdn` default.
#[test]
fn resolve_data_dir_honors_config_file() {
    let (_dir, path) = node_toml_with_data_dir("/var/lib/decdn");
    assert_eq!(
        resolve_data_dir(None, Some(&path)).unwrap(),
        Some(PathBuf::from("/var/lib/decdn"))
    );
}

/// An explicit `--output-dir` wins over the config file's `identity.data_dir`.
#[test]
fn resolve_data_dir_output_dir_overrides_config_file() {
    let (_dir, path) = node_toml_with_data_dir("/var/lib/decdn");
    assert_eq!(
        resolve_data_dir(Some(Path::new("/opt/keys")), Some(&path)).unwrap(),
        Some(PathBuf::from("/opt/keys"))
    );
}

/// An explicit `--output-dir` must not read the config file at all, so a
/// broken `node.toml` it would override never turns into an error.
#[test]
fn resolve_data_dir_output_dir_ignores_broken_config() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(&path, "this is not = valid = toml\n").unwrap();
    assert_eq!(
        resolve_data_dir(Some(Path::new("/opt/keys")), Some(&path)).unwrap(),
        Some(PathBuf::from("/opt/keys"))
    );
}

/// An explicit but broken `--config` still errors when it is actually
/// consulted (no `--output-dir` to short-circuit it).
#[test]
fn resolve_data_dir_errors_on_broken_config_without_output_dir() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(&path, "this is not = valid = toml\n").unwrap();
    assert!(resolve_data_dir(None, Some(&path)).is_err());
}

#[cfg(unix)]
fn secure_tempdir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

#[cfg(not(unix))]
fn secure_tempdir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// The set of filenames in `dir`, to prove `whoami` mutates nothing.
fn file_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// A supplied password yields the eth address `key-gen` persisted, and
/// reads — not writes — the keystore (whoami must mutate nothing).
#[test]
fn address_line_shows_generated_address_and_mutates_nothing() {
    let dir = secure_tempdir();
    identity::load_or_generate(dir.path()).unwrap();
    let address = generate_and_persist(dir.path(), TEST_PASSWORD, false).unwrap();
    let keystore = eth_identity::keystore_path(dir.path());

    let before = file_names(dir.path());
    let attempt = eth_address_line(
        &keystore,
        &KeystoreState::Present,
        &KeystorePassword::Supplied(Zeroizing::new(TEST_PASSWORD.to_owned())),
    );

    assert_eq!(attempt.line, format!("eth address: {address}"));
    assert!(!attempt.retryable, "a decrypted keystore needs no retry");
    assert_eq!(
        before,
        file_names(dir.path()),
        "whoami must not add or remove files"
    );
}

/// With no password available, the eth line is a note rather than an error.
#[test]
fn address_line_without_password_notes_one_is_needed() {
    let keystore = Path::new("/data/keystore.json");
    let attempt = eth_address_line(
        keystore,
        &KeystoreState::Present,
        &KeystorePassword::Unavailable,
    );
    let line = attempt.line;
    assert!(
        line.starts_with("eth address:") && line.contains("password"),
        "expected a password note, got {line:?}"
    );
    assert!(
        !attempt.retryable,
        "no password source was present, so there is nothing to retry with"
    );
}

/// No keystore at all is reported on the eth line, not treated as an error.
#[test]
fn address_line_without_keystore_notes_it_is_absent() {
    let keystore = Path::new("/data/keystore.json");
    let attempt = eth_address_line(
        keystore,
        &KeystoreState::Absent,
        &KeystorePassword::Unavailable,
    );
    let line = attempt.line;
    assert!(
        line.starts_with("eth address:") && line.contains("no keystore"),
        "expected a 'no keystore' note, got {line:?}"
    );
    assert!(!attempt.retryable, "there is no keystore to retry against");
}

/// A wrong password degrades its own line to a note naming the keystore and
/// the reason, and marks the attempt retryable — it must not abort the
/// report, because the other keystore's password may well be correct
/// (#2008).
#[test]
fn address_line_with_wrong_password_notes_and_asks_for_a_retry() {
    let dir = secure_tempdir();
    generate_and_persist(dir.path(), TEST_PASSWORD, false).unwrap();
    let keystore = eth_identity::keystore_path(dir.path());

    let attempt = eth_address_line(
        &keystore,
        &KeystoreState::Present,
        &KeystorePassword::Supplied(Zeroizing::new("definitely-wrong".to_owned())),
    );
    assert!(
        attempt.line.starts_with("eth address: (keystore at "),
        "got: {:?}",
        attempt.line
    );
    assert!(
        attempt.line.contains("did not open with this password"),
        "the line must say why there is no address: {:?}",
        attempt.line
    );
    assert!(
        attempt.retryable,
        "a present keystore that did not open is exactly the retry case"
    );
}

/// `keystore_state` classifies an absent path as `Absent` (not an error)
/// and a present one as `Present`.
#[test]
fn keystore_state_classifies_present_and_absent() {
    let dir = secure_tempdir();
    let keystore = eth_identity::keystore_path(dir.path());
    assert!(
        matches!(keystore_state(&keystore).unwrap(), KeystoreState::Absent),
        "absent keystore must classify as Absent"
    );
    generate_and_persist(dir.path(), TEST_PASSWORD, false).unwrap();
    assert!(
        matches!(keystore_state(&keystore).unwrap(), KeystoreState::Present),
        "present keystore must classify as Present"
    );
}

/// The node id comes from `identity::load`, which errors (never generates)
/// on an absent key — so a `whoami` against an empty dir never mints one.
#[test]
fn node_id_load_errors_on_absent_key_without_writing() {
    let dir = secure_tempdir();
    let err = identity::load(dir.path()).expect_err("absent node key must error");
    assert!(
        format!("{err:#}").contains("key-gen"),
        "error should point at key-gen: {err:#}"
    );
    assert!(
        !identity::key_path(dir.path()).exists(),
        "a failed whoami must not create node.secret"
    );
}

/// A client-only install (a keystore under `client/`, no node key) reports
/// the client eth keystore instead of erroring, and the node id degrades to
/// a note. `report` mutates nothing — it never mints the missing node key.
#[test]
fn report_notes_absent_node_key_and_reports_client_keystore() {
    let dir = secure_tempdir();
    let client_dir = dir.path().join("client");
    // `generate_and_persist` creates the `client/` subdir at 0o700.
    generate_and_persist(&client_dir, TEST_PASSWORD, false).unwrap();

    // No password source (no env, no TTY under the test runner), so the
    // client eth line is the "needs a password" note — but it is present,
    // which is the point: the client keystore is reported at all.
    let lines = report(dir.path(), None).unwrap();

    assert!(
        lines.iter().any(|l| l.starts_with("node id:")
            && l.contains("no node key")
            && l.contains("key-gen")),
        "absent node key must be a note, got {lines:?}"
    );
    let client_keystore = eth_identity::keystore_path(&client_dir);
    assert!(
        lines.contains(&format!(
            "client keystore path: {}",
            client_keystore.display()
        )),
        "the client keystore path must be reported, got {lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.starts_with("client eth address:")),
        "the client eth address line must be reported, got {lines:?}"
    );

    assert!(
        !identity::key_path(dir.path()).exists(),
        "report must not mint a node key"
    );
    assert!(
        !eth_identity::keystore_path(dir.path()).exists(),
        "report must not create a node keystore"
    );
}

/// The #2008 repro: the node and client keystores were created at
/// different times under separate prompts, so they hold different
/// passwords. One password file cannot open both, and one `Mac Mismatch`
/// must not abort the report — that would print nothing at all, including
/// for the keystore whose password was correct. Both lines print, the one
/// that opened shows its address, and `report` does not error.
#[test]
fn report_survives_two_keystores_with_two_passwords() {
    let dir = secure_tempdir();
    let secret = identity::load_or_generate(dir.path()).unwrap();
    let node_address = generate_and_persist(dir.path(), TEST_PASSWORD, false).unwrap();
    let client_dir = dir.path().join("client");
    generate_and_persist(&client_dir, "a-different-password", false).unwrap();

    // A password file is the only source a test can supply: the env var
    // would need `set_var`, and stdin is not a TTY under the runner — which
    // also means the failing keystore gets no retry prompt here.
    let pw_file = dir.path().join("password");
    std::fs::write(&pw_file, TEST_PASSWORD).unwrap();

    let lines = report(dir.path(), Some(&pw_file)).unwrap();

    assert!(
        lines.contains(&format!("node id: {}", secret.public())),
        "the node id must print, got {lines:?}"
    );
    assert!(
        lines.contains(&format!("eth address: {node_address}")),
        "the keystore this password DOES open must show its address, got {lines:?}"
    );
    let client_line = lines
        .iter()
        .find(|l| l.starts_with("client eth address:"))
        .expect("the client eth line must print");
    assert!(
        client_line.contains("did not open with this password"),
        "the keystore this password does not open degrades to a note: {client_line:?}"
    );
}

/// A node install (node key + node keystore, no `client/`) reports the node
/// identity and omits the client section entirely.
#[test]
fn report_shows_node_identity_and_omits_absent_client() {
    let dir = secure_tempdir();
    let secret = identity::load_or_generate(dir.path()).unwrap();
    generate_and_persist(dir.path(), TEST_PASSWORD, false).unwrap();

    let lines = report(dir.path(), None).unwrap();

    assert!(
        lines.contains(&format!("node id: {}", secret.public())),
        "the real node id must print, got {lines:?}"
    );
    assert!(
        !lines.iter().any(|l| l.starts_with("client ")),
        "no client section without a client keystore, got {lines:?}"
    );
}
