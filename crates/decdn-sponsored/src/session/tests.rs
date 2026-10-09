use super::*;

const HEX: &str = "9194000d7b356650e6924a7746ec4afb0b705838b65913c0e46cba6b15af69e5";

#[test]
fn key_is_generated_once_and_reused() {
    let root = tempfile::tempdir().unwrap();
    let session = Session::open(root.path(), HEX).unwrap();
    let first = session.ensure_key().unwrap();
    let again = Session::open(root.path(), HEX)
        .unwrap()
        .ensure_key()
        .unwrap();
    assert_eq!(first, again);
    let pw = std::fs::read_to_string(session.password_path()).unwrap();
    assert_eq!(pw.len(), 64);
}

#[test]
fn key_without_password_is_replaced_with_its_capability() {
    let root = tempfile::tempdir().unwrap();
    let session = Session::open(root.path(), HEX).unwrap();
    let first = session.ensure_key().unwrap();
    session.save_capability("dcap1:bound-to-first").unwrap();
    std::fs::remove_file(session.password_path()).unwrap();

    let second = session.ensure_key().unwrap();
    assert_ne!(first, second);
    assert!(session.password_path().is_file());
    assert!(!session.capability_path().exists());
}

#[cfg(unix)]
#[test]
fn state_dirs_are_created_private() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let session = Session::open(root.path(), HEX).unwrap();
    let shared = session.reserve_decdn_data_dir().unwrap();
    for dir in [
        session.dir(),
        shared.path(),
        root.path().join("downloads").as_path(),
    ] {
        let mode = std::fs::metadata(dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "{}", dir.display());
    }
}

#[test]
fn malformed_capability_reads_as_none_and_discard_removes_state() {
    let root = tempfile::tempdir().unwrap();
    let session = Session::open(root.path(), HEX).unwrap();
    assert!(session.capability().unwrap().is_none());
    session.save_capability("dcap1:not-a-token").unwrap();
    assert!(session.capability().unwrap().is_none());
    let dir = session.dir().to_path_buf();
    session.discard().unwrap();
    assert!(!dir.exists());
}

#[test]
fn discard_keeps_the_shared_decdn_data_dir() {
    let root = tempfile::tempdir().unwrap();
    let session = Session::open(root.path(), HEX).unwrap();
    session.ensure_key().unwrap();
    let reserved = session.reserve_decdn_data_dir().unwrap();
    assert!(reserved.is_shared());
    let shared = reserved.path().to_path_buf();
    assert_eq!(shared, root.path().join(DECDN_DATA_DIR));
    assert!(!shared.starts_with(session.dir()));
    std::fs::create_dir(shared.join("peers")).unwrap();
    drop(reserved);

    let dir = session.dir().to_path_buf();
    session.discard().unwrap();
    assert!(!dir.exists());
    assert!(shared.join("peers").is_dir());
}

#[test]
fn downloads_in_turn_share_the_data_dir_but_not_the_key() {
    let root = tempfile::tempdir().unwrap();
    let a = Session::open(root.path(), HEX).unwrap();
    let b = Session::open(root.path(), &"ab".repeat(32)).unwrap();
    let a_dir = a.reserve_decdn_data_dir().unwrap().path().to_path_buf();
    let b_dir = b.reserve_decdn_data_dir().unwrap().path().to_path_buf();
    assert_eq!(a_dir, b_dir);
    assert_ne!(a.keystore_path(), b.keystore_path());
    assert_ne!(a.ensure_key().unwrap(), b.ensure_key().unwrap());

    // A fresh key after discard (success or near expiry) is a new signer.
    let first = a.ensure_key().unwrap();
    a.discard().unwrap();
    let again = Session::open(root.path(), HEX).unwrap();
    assert_ne!(again.ensure_key().unwrap(), first);
}

#[test]
fn a_concurrent_download_runs_in_its_own_dir() {
    let root = tempfile::tempdir().unwrap();
    let a = Session::open(root.path(), HEX).unwrap();
    let b = Session::open(root.path(), &"ab".repeat(32)).unwrap();
    let held = a.reserve_decdn_data_dir().unwrap();
    assert!(held.is_shared());

    let busy = b.reserve_decdn_data_dir().unwrap();
    assert!(!busy.is_shared());
    assert_eq!(busy.path(), b.dir());

    drop(held);
    assert!(b.reserve_decdn_data_dir().unwrap().is_shared());
}

#[test]
fn key_without_its_address_file_is_replaced() {
    let root = tempfile::tempdir().unwrap();
    let session = Session::open(root.path(), HEX).unwrap();
    let first = session.ensure_key().unwrap();
    assert_eq!(
        std::fs::read_to_string(session.dir().join(ADDRESS_FILE)).unwrap(),
        first.to_string()
    );
    std::fs::remove_file(session.dir().join(ADDRESS_FILE)).unwrap();
    assert_ne!(session.ensure_key().unwrap(), first);
}

#[test]
fn profile_round_trips() {
    let root = tempfile::tempdir().unwrap();
    let session = Session::open(root.path(), HEX).unwrap();
    assert!(session.profile().unwrap().is_none());
    let profile = Profile {
        chain_id: 1,
        rpc_url: "https://rpc".into(),
        payment_pool: Address::repeat_byte(1),
        capacity_bond: Address::repeat_byte(2),
        slash_judge: None,
        min_cli_version: None,
    };
    session.save_profile(&profile).unwrap();
    assert_eq!(session.profile().unwrap(), Some(profile));
}
