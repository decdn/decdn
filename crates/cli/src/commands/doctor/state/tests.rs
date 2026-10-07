use super::*;
use crate::commands::doctor::Severity;

#[test]
fn absent_secret_passes_with_first_boot_note() {
    let f = evaluate_secret(
        &FileFacts {
            exists: false,
            is_file: false,
            len: 0,
            mode: None,
            stat_error: None,
        },
        Path::new("/data/node.secret"),
    );
    assert_eq!(f.severity, Severity::Pass);
    assert!(f.title.to_lowercase().contains("generated") || f.detail.is_some());
}

#[test]
fn wrong_size_secret_fails() {
    let f = evaluate_secret(
        &FileFacts {
            exists: true,
            is_file: true,
            len: 10,
            mode: Some(0o600),
            stat_error: None,
        },
        Path::new("/data/node.secret"),
    );
    assert_eq!(f.severity, Severity::Fail);
}

#[test]
fn loose_perms_secret_fails() {
    let f = evaluate_secret(
        &FileFacts {
            exists: true,
            is_file: true,
            len: 32,
            mode: Some(0o644),
            stat_error: None,
        },
        Path::new("/data/node.secret"),
    );
    assert_eq!(f.severity, Severity::Fail);
}

#[test]
fn good_secret_passes() {
    let f = evaluate_secret(
        &FileFacts {
            exists: true,
            is_file: true,
            len: 32,
            mode: Some(0o600),
            stat_error: None,
        },
        Path::new("/data/node.secret"),
    );
    assert_eq!(f.severity, Severity::Pass);
}

#[test]
fn stat_error_on_secret_warns_instead_of_pass() {
    let f = evaluate_secret(
        &FileFacts {
            exists: false,
            is_file: false,
            len: 0,
            mode: None,
            stat_error: Some(std::io::ErrorKind::PermissionDenied),
        },
        Path::new("/data/node.secret"),
    );
    assert_eq!(f.severity, Severity::Warn);
    assert!(f.title.to_lowercase().contains("cannot stat"));
    assert!(f.remediation.is_some());
}

// --- evaluate_receipts ---------------------------------------------

#[test]
fn absent_receipt_log_passes() {
    let facts = FileFacts {
        exists: false,
        is_file: false,
        len: 0,
        mode: None,
        stat_error: None,
    };
    let f = evaluate_receipts(&facts, Path::new("/data/receipts.jsonl"));
    assert_eq!(f.id, "state.receipts");
    assert_eq!(f.severity, Severity::Pass);
    assert!(f.title.to_lowercase().contains("absent"));
}

#[test]
fn present_regular_receipt_log_passes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("receipts.jsonl");
    std::fs::write(&path, "").unwrap();
    let facts = FileFacts::read(&path);
    let f = evaluate_receipts(&facts, &path);
    assert_eq!(f.id, "state.receipts");
    assert_eq!(f.severity, Severity::Pass);
    assert!(f.title.to_lowercase().contains("present"));
}

#[test]
fn receipt_log_path_not_a_regular_file_warns() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("receipts.jsonl");
    std::fs::create_dir(&path).unwrap();
    let facts = FileFacts::read(&path);
    let f = evaluate_receipts(&facts, &path);
    assert_eq!(f.id, "state.receipts");
    assert_eq!(f.severity, Severity::Warn);
    assert!(f.remediation.is_some());
}

// --- evaluate_redb ----------------------------------------------------

#[test]
fn absent_redb_passes() {
    let facts = FileFacts {
        exists: false,
        is_file: false,
        len: 0,
        mode: None,
        stat_error: None,
    };
    let f = evaluate_redb("lanes.redb", &facts, Path::new("/data/lanes.redb"));
    assert_eq!(f.id, "state.redb");
    assert_eq!(f.severity, Severity::Pass);
    assert!(f.title.contains("absent"));
}

#[test]
fn zero_length_redb_fails_as_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lanes.redb");
    std::fs::write(&path, "").unwrap();
    let facts = FileFacts::read(&path);
    let f = evaluate_redb("lanes.redb", &facts, &path);
    assert_eq!(f.id, "state.redb");
    assert_eq!(f.severity, Severity::Fail);
    assert!(f.title.to_lowercase().contains("corrupt"));
}

#[test]
fn present_redb_with_data_passes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lanes.redb");
    std::fs::write(&path, b"some bytes").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let facts = FileFacts::read(&path);
    let f = evaluate_redb("lanes.redb", &facts, &path);
    assert_eq!(f.id, "state.redb");
    assert_eq!(f.severity, Severity::Pass);
}

#[cfg(unix)]
#[test]
fn loose_perms_redb_warns() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lanes.redb");
    std::fs::write(&path, b"some bytes").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let facts = FileFacts::read(&path);
    let f = evaluate_redb("lanes.redb", &facts, &path);
    assert_eq!(f.id, "state.redb");
    assert_eq!(f.severity, Severity::Warn);
}

// --- evaluate_keystore --------------------------------------------

#[test]
fn present_regular_keystore_passes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keystore.json");
    std::fs::write(&path, "{}").unwrap();
    let facts = FileFacts::read(&path);
    let f = evaluate_keystore(&facts, &path);
    assert_eq!(f.id, "state.keystore");
    assert_eq!(f.severity, Severity::Pass);
}

#[test]
fn keystore_path_is_a_directory_warns() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keystore.json");
    std::fs::create_dir(&path).unwrap();
    let facts = FileFacts::read(&path);
    let f = evaluate_keystore(&facts, &path);
    assert_eq!(f.id, "state.keystore");
    assert_eq!(f.severity, Severity::Warn);
    assert!(f.title.to_lowercase().contains("not a regular file"));
}

#[test]
fn missing_keystore_warns() {
    let facts = FileFacts {
        exists: false,
        is_file: false,
        len: 0,
        mode: None,
        stat_error: None,
    };
    let f = evaluate_keystore(&facts, Path::new("/data/keystore.json"));
    assert_eq!(f.id, "state.keystore");
    assert_eq!(f.severity, Severity::Warn);
    assert!(f.title.to_lowercase().contains("missing"));
}

#[test]
fn stat_error_on_keystore_warns_with_cannot_stat() {
    let facts = FileFacts {
        exists: false,
        is_file: false,
        len: 0,
        mode: None,
        stat_error: Some(std::io::ErrorKind::PermissionDenied),
    };
    let f = evaluate_keystore(&facts, Path::new("/data/keystore.json"));
    assert_eq!(f.id, "state.keystore");
    assert_eq!(f.severity, Severity::Warn);
    assert!(f.title.to_lowercase().contains("cannot stat"));
}

#[test]
fn stat_error_on_redb_warns_with_cannot_stat() {
    let facts = FileFacts {
        exists: false,
        is_file: false,
        len: 0,
        mode: None,
        stat_error: Some(std::io::ErrorKind::PermissionDenied),
    };
    let f = evaluate_redb("lanes.redb", &facts, Path::new("/data/lanes.redb"));
    assert_eq!(f.id, "state.redb");
    assert_eq!(f.severity, Severity::Warn);
    assert!(f.title.to_lowercase().contains("cannot stat"));
}

#[test]
fn stat_error_on_receipts_warns_with_cannot_stat() {
    let facts = FileFacts {
        exists: false,
        is_file: false,
        len: 0,
        mode: None,
        stat_error: Some(std::io::ErrorKind::PermissionDenied),
    };
    let f = evaluate_receipts(&facts, Path::new("/data/receipts.jsonl"));
    assert_eq!(f.id, "state.receipts");
    assert_eq!(f.severity, Severity::Warn);
    assert!(f.title.to_lowercase().contains("cannot stat"));
}

/// The doctor's list is the shared one, so a store added to
/// `DAEMON_STORE_FILES` is checked here without a second edit — and a name
/// nothing creates cannot linger in it.
#[test]
fn checked_files_are_the_shared_store_names() {
    let checked: Vec<&str> = checked_store_files().collect();
    for name in decdn_common::data_dir::DAEMON_STORE_FILES {
        assert!(checked.contains(name), "{name} is unchecked");
    }
    assert!(checked.contains(&decdn_common::data_dir::CLIENT_BUYER_DB_FILE));
    assert_eq!(
        checked.len(),
        decdn_common::data_dir::DAEMON_STORE_FILES.len() + 1,
        "the doctor checks a file no store owns: {checked:?}"
    );
}
