//! On-disk state group. All checks stat / read metadata only — the doctor
//! never generates `node.secret` and never opens a redb store for write.

use std::path::Path;

use decdn_common::config::{RECEIPT_LOG_FILE, ResolvedConfig};

use super::{Finding, Report, Severity};

/// Metadata facts about a file, gathered by I/O and evaluated purely.
pub struct FileFacts {
    pub exists: bool,
    pub is_file: bool,
    pub len: u64,
    /// Unix permission bits (`st_mode & 0o777`), or `None` on non-Unix.
    pub mode: Option<u32>,
}

impl FileFacts {
    fn read(path: &Path) -> Self {
        match std::fs::symlink_metadata(path) {
            Ok(m) => {
                #[cfg(unix)]
                let mode = {
                    use std::os::unix::fs::PermissionsExt;
                    Some(m.permissions().mode() & 0o777)
                };
                #[cfg(not(unix))]
                let mode = None;
                FileFacts {
                    exists: true,
                    is_file: m.is_file(),
                    len: m.len(),
                    mode,
                }
            }
            Err(_) => FileFacts {
                exists: false,
                is_file: false,
                len: 0,
                mode: None,
            },
        }
    }
}

/// Evaluate `node.secret`: absent is fine (first boot generates it); present
/// must be a 32-byte regular file with `0o600` (no group/other bits).
pub fn evaluate_secret(facts: &FileFacts) -> Finding {
    let base = |severity, title: String, remediation| Finding {
        group: "State",
        id: "state.node_secret",
        severity,
        title,
        detail: None,
        remediation,
    };
    if !facts.exists {
        return base(
            Severity::Pass,
            "node.secret absent (generated on first boot)".into(),
            None,
        );
    }
    if !facts.is_file {
        return base(
            Severity::Fail,
            "node.secret is not a regular file".into(),
            Some("remove the non-file at the node.secret path".into()),
        );
    }
    if facts.len != 32 {
        return base(
            Severity::Fail,
            format!("node.secret is {} bytes, expected 32", facts.len),
            Some(
                "restore the correct node.secret or remove it to regenerate a new identity".into(),
            ),
        );
    }
    if let Some(mode) = facts.mode
        && mode & 0o077 != 0
    {
        return base(
            Severity::Fail,
            format!("node.secret permissions are too open ({mode:#o})"),
            Some("run: chmod 600 <data_dir>/node.secret".into()),
        );
    }
    base(
        Severity::Pass,
        "node.secret present and secure".into(),
        None,
    )
}

/// Evaluate the eth keystore: only meaningful when blockchain is configured.
/// The resolver already stat-checks `eth_keystore`; this surfaces
/// readability in the report too. Absent is a `Warn` (not a `Fail`) because
/// not every deployment signs on-chain from this node.
pub fn evaluate_keystore(facts: &FileFacts, path: &Path) -> Finding {
    Finding {
        group: "State",
        id: "state.keystore",
        severity: if facts.exists && facts.is_file {
            Severity::Pass
        } else {
            Severity::Warn
        },
        title: if facts.exists && facts.is_file {
            "eth keystore present".into()
        } else if facts.exists {
            "keystore path exists but is not a regular file".into()
        } else {
            "eth keystore missing".into()
        },
        detail: Some(format!("path={}", path.display())),
        remediation: if facts.exists && facts.is_file {
            None
        } else if facts.exists {
            Some("remove the non-file at the keystore path".into())
        } else {
            Some(
                "create the keystore (decdn key-gen / your provisioning) before the node signs"
                    .into(),
            )
        },
    }
}

/// Evaluate one redb store: presence + `0o600` + non-zero-length tripwire.
/// Deeper open integrity is deferred to the daemon-side doctor (the store is
/// locked while the daemon runs).
pub fn evaluate_redb(name: &str, facts: &FileFacts, path: &Path) -> Finding {
    if !facts.exists {
        // Absent is normal before first run.
        return Finding {
            group: "State",
            id: "state.redb",
            severity: Severity::Pass,
            title: format!("{name} absent (created on first use)"),
            detail: None,
            remediation: None,
        };
    }
    if facts.len == 0 {
        return Finding {
            group: "State",
            id: "state.redb",
            severity: Severity::Fail,
            title: format!("{name} is zero-length (corrupt)"),
            detail: Some(format!("path={}", path.display())),
            remediation: Some(format!("stop the node and restore or remove {name}")),
        };
    }
    if facts.mode.is_some_and(|m| m & 0o077 != 0) {
        return Finding {
            group: "State",
            id: "state.redb",
            severity: Severity::Warn,
            title: format!("{name} permissions are too open"),
            detail: Some(format!("mode={:#o}", facts.mode.unwrap_or(0))),
            remediation: Some(format!("run: chmod 600 <data_dir>/{name}")),
        };
    }
    Finding {
        group: "State",
        id: "state.redb",
        severity: Severity::Pass,
        title: format!("{name} present"),
        detail: None,
        remediation: None,
    }
}

/// Evaluate the receipt log: presence is optional before the first receipt.
pub fn evaluate_receipts(facts: &FileFacts, path: &Path) -> Finding {
    if !facts.exists {
        return Finding {
            group: "State",
            id: "state.receipts",
            severity: Severity::Pass,
            title: "receipt log absent (created on first receipt)".into(),
            detail: None,
            remediation: None,
        };
    }
    if facts.is_file {
        return Finding {
            group: "State",
            id: "state.receipts",
            severity: Severity::Pass,
            title: "receipt log present".into(),
            detail: Some(format!("path={}", path.display())),
            remediation: None,
        };
    }
    Finding {
        group: "State",
        id: "state.receipts",
        severity: Severity::Warn,
        title: "receipt log path exists but is not a regular file".into(),
        detail: Some(format!("path={}", path.display())),
        remediation: Some("remove the non-file at the receipt log path".into()),
    }
}

/// Push all state-group findings.
pub fn check_state(report: &mut Report, cfg: &ResolvedConfig, daemon_running: bool) {
    const REDB: &[&str] = &[
        "lanes.redb",
        "settle.redb",
        "floor-loss.redb",
        "checkpoint.redb",
        "buyer.redb",
        "buyer-pools.redb",
    ];

    let data_dir = &cfg.identity.data_dir;

    report.push(evaluate_secret(&FileFacts::read(
        &data_dir.join("node.secret"),
    )));

    let keystore = &cfg.blockchain.eth_keystore;
    report.push(evaluate_keystore(&FileFacts::read(keystore), keystore));

    // redb stores.
    for name in REDB {
        let path = data_dir.join(name);
        let facts = FileFacts::read(&path);
        report.push(evaluate_redb(name, &facts, &path));
    }

    // Receipt log.
    let receipt_path = data_dir.join(RECEIPT_LOG_FILE);
    let receipts = FileFacts::read(&receipt_path);
    report.push(evaluate_receipts(&receipts, &receipt_path));

    if daemon_running {
        report.push(Finding {
            group: "State",
            id: "state.redb_integrity",
            severity: Severity::Pass,
            title: "redb open-integrity check skipped (held by running daemon)".into(),
            detail: None,
            remediation: None,
        });
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;
    use crate::commands::doctor::Severity;

    #[test]
    fn absent_secret_passes_with_first_boot_note() {
        let f = evaluate_secret(&FileFacts {
            exists: false,
            is_file: false,
            len: 0,
            mode: None,
        });
        assert_eq!(f.severity, Severity::Pass);
        assert!(f.title.to_lowercase().contains("generated") || f.detail.is_some());
    }

    #[test]
    fn wrong_size_secret_fails() {
        let f = evaluate_secret(&FileFacts {
            exists: true,
            is_file: true,
            len: 10,
            mode: Some(0o600),
        });
        assert_eq!(f.severity, Severity::Fail);
    }

    #[test]
    fn loose_perms_secret_fails() {
        let f = evaluate_secret(&FileFacts {
            exists: true,
            is_file: true,
            len: 32,
            mode: Some(0o644),
        });
        assert_eq!(f.severity, Severity::Fail);
    }

    #[test]
    fn good_secret_passes() {
        let f = evaluate_secret(&FileFacts {
            exists: true,
            is_file: true,
            len: 32,
            mode: Some(0o600),
        });
        assert_eq!(f.severity, Severity::Pass);
    }

    // --- evaluate_receipts ---------------------------------------------

    #[test]
    fn absent_receipt_log_passes() {
        let facts = FileFacts {
            exists: false,
            is_file: false,
            len: 0,
            mode: None,
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
        };
        let f = evaluate_keystore(&facts, Path::new("/data/keystore.json"));
        assert_eq!(f.id, "state.keystore");
        assert_eq!(f.severity, Severity::Warn);
        assert!(f.title.to_lowercase().contains("missing"));
    }
}
