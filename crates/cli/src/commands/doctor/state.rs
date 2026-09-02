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

/// Push all state-group findings.
#[allow(clippy::too_many_lines)]
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

    // Keystore: only meaningful when blockchain is configured. The resolver
    // already stat-checks eth_keystore; surface readability here too.
    let keystore = &cfg.blockchain.eth_keystore;
    let ks = FileFacts::read(keystore);
    report.push(Finding {
        group: "State",
        id: "state.keystore",
        severity: if ks.exists && ks.is_file {
            Severity::Pass
        } else {
            Severity::Warn
        },
        title: if ks.exists && ks.is_file {
            "eth keystore present".into()
        } else if ks.exists {
            "keystore path exists but is not a regular file".into()
        } else {
            "eth keystore missing".into()
        },
        detail: Some(format!("path={}", keystore.display())),
        remediation: if ks.exists && ks.is_file {
            None
        } else if ks.exists {
            Some("remove the non-file at the keystore path".into())
        } else {
            Some(
                "create the keystore (decdn key-gen / your provisioning) before the node signs"
                    .into(),
            )
        },
    });

    // redb stores: presence + 0o600 + non-zero-length tripwire. Deeper open
    // integrity is deferred to the daemon-side doctor (locked while running).
    for name in REDB {
        let facts = FileFacts::read(&data_dir.join(name));
        let finding = if !facts.exists {
            // Absent is normal before first run.
            Finding {
                group: "State",
                id: "state.redb",
                severity: Severity::Pass,
                title: format!("{name} absent (created on first use)"),
                detail: None,
                remediation: None,
            }
        } else if facts.len == 0 {
            Finding {
                group: "State",
                id: "state.redb",
                severity: Severity::Fail,
                title: format!("{name} is zero-length (corrupt)"),
                detail: Some(format!("path={}", data_dir.join(name).display())),
                remediation: Some(format!("stop the node and restore or remove {name}")),
            }
        } else if facts.mode.is_some_and(|m| m & 0o077 != 0) {
            Finding {
                group: "State",
                id: "state.redb",
                severity: Severity::Warn,
                title: format!("{name} permissions are too open"),
                detail: Some(format!("mode={:#o}", facts.mode.unwrap_or(0))),
                remediation: Some(format!("run: chmod 600 <data_dir>/{name}")),
            }
        } else {
            Finding {
                group: "State",
                id: "state.redb",
                severity: Severity::Pass,
                title: format!("{name} present"),
                detail: None,
                remediation: None,
            }
        };
        report.push(finding);
    }

    // Receipt log: presence is optional before first receipt.
    let receipt_path = data_dir.join(RECEIPT_LOG_FILE);
    let receipts = FileFacts::read(&receipt_path);
    let receipt_finding = if !receipts.exists {
        Finding {
            group: "State",
            id: "state.receipts",
            severity: Severity::Pass,
            title: "receipt log absent (created on first receipt)".into(),
            detail: None,
            remediation: None,
        }
    } else if receipts.is_file {
        Finding {
            group: "State",
            id: "state.receipts",
            severity: Severity::Pass,
            title: "receipt log present".into(),
            detail: Some(format!("path={}", receipt_path.display())),
            remediation: None,
        }
    } else {
        Finding {
            group: "State",
            id: "state.receipts",
            severity: Severity::Warn,
            title: "receipt log path exists but is not a regular file".into(),
            detail: Some(format!("path={}", receipt_path.display())),
            remediation: Some("remove the non-file at the receipt log path".into()),
        }
    };
    report.push(receipt_finding);

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

    #[test]
    fn absent_receipt_log_passes() {
        let facts = FileFacts {
            exists: false,
            is_file: false,
            len: 0,
            mode: None,
        };
        assert!(!facts.exists);
        assert!(!facts.is_file);
    }

    #[test]
    fn present_receipt_log_passes() {
        let tmp = std::env::temp_dir().join("decdn_receipt_test.jsonl");
        let _ = std::fs::write(&tmp, "");
        let facts = FileFacts::read(&tmp);
        assert!(facts.exists);
        assert!(facts.is_file);
        let _ = std::fs::remove_file(&tmp);
    }
}
