//! On-disk state group. All checks stat / read metadata only — the doctor
//! never generates `node.secret` and never opens a redb store for write.

use std::path::Path;

use decdn_common::config::{RECEIPT_LOG_FILE, ResolvedConfig};

use super::{Finding, Report, Severity};

/// Metadata facts about a file, gathered by I/O and evaluated purely.
pub(crate) struct FileFacts {
    /// True when a filesystem entry exists at the path.
    pub(crate) exists: bool,
    /// True when the entry exists and is a regular file.
    pub(crate) is_file: bool,
    /// Entry length in bytes, or 0 when it does not exist.
    pub(crate) len: u64,
    /// Unix permission bits (`st_mode & 0o777`), or `None` on non-Unix.
    pub(crate) mode: Option<u32>,
    /// `Some(kind)` when `symlink_metadata` failed for a reason other than
    /// the entry being absent (e.g. a permission error); `None` when the
    /// entry does not exist or was read cleanly.
    pub(crate) stat_error: Option<std::io::ErrorKind>,
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
                    stat_error: None,
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => FileFacts {
                exists: false,
                is_file: false,
                len: 0,
                mode: None,
                stat_error: None,
            },
            Err(e) => FileFacts {
                exists: false,
                is_file: false,
                len: 0,
                mode: None,
                stat_error: Some(e.kind()),
            },
        }
    }
}

/// Evaluate `node.secret`: absent is fine (first boot generates it); present
/// must be a 32-byte regular file with `0o600` (no group/other bits). A
/// non-`NotFound` stat error (e.g. permission denied) is reported as a
/// `Warn` rather than treated as absent, since that would otherwise mask
/// a real access problem behind a clean `Pass`.
pub(crate) fn evaluate_secret(facts: &FileFacts, path: &Path) -> Finding {
    let base = |severity, title: String, remediation| Finding {
        group: "State",
        id: "state.node_secret",
        severity,
        title,
        detail: None,
        remediation,
    };
    if let Some(kind) = facts.stat_error {
        return Finding {
            group: "State",
            id: "state.node_secret",
            severity: Severity::Warn,
            title: format!("cannot stat {}: {kind}", path.display()),
            detail: None,
            remediation: Some("check permissions/ownership".into()),
        };
    }
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
/// Config resolution (`resolve_config`) already validated `eth_keystore`
/// readability before doctor ran, so a healthy config implies this file
/// exists and is readable at that point; this check re-stats it and
/// surfaces that already-validated state in the report — it does not
/// re-derive the invariant. Absent is a `Warn` (not a `Fail`) because not
/// every deployment signs on-chain from this node, and the file could have
/// been removed between config resolution and this check.
pub(crate) fn evaluate_keystore(facts: &FileFacts, path: &Path) -> Finding {
    if let Some(kind) = facts.stat_error {
        return Finding {
            group: "State",
            id: "state.keystore",
            severity: Severity::Warn,
            title: format!("cannot stat {}: {kind}", path.display()),
            detail: Some(format!("path={}", path.display())),
            remediation: Some("check permissions/ownership".into()),
        };
    }
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
pub(crate) fn evaluate_redb(name: &str, facts: &FileFacts, path: &Path) -> Finding {
    if let Some(kind) = facts.stat_error {
        return Finding {
            group: "State",
            id: "state.redb",
            severity: Severity::Warn,
            title: format!("cannot stat {}: {kind}", path.display()),
            detail: Some(format!("path={}", path.display())),
            remediation: Some("check permissions/ownership".into()),
        };
    }
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
pub(crate) fn evaluate_receipts(facts: &FileFacts, path: &Path) -> Finding {
    if let Some(kind) = facts.stat_error {
        return Finding {
            group: "State",
            id: "state.receipts",
            severity: Severity::Warn,
            title: format!("cannot stat {}: {kind}", path.display()),
            detail: Some(format!("path={}", path.display())),
            remediation: Some("check permissions/ownership".into()),
        };
    }
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

/// Every store file the doctor checks in a data dir: the daemon's own set,
/// plus the client store a `decdn fetch` / `decdn pool` invocation leaves
/// behind when it is pointed at the same directory.
///
/// Sourced from [`decdn_common::data_dir`] rather than spelled here, so a fifth
/// daemon store reaches the permission and zero-length checks with the same
/// edit that teaches `daemon_marker` about it. A private copy drifts into
/// naming files nothing creates; `checked_files_are_the_shared_store_names`
/// holds the count.
fn checked_store_files() -> impl Iterator<Item = &'static str> {
    decdn_common::data_dir::DAEMON_STORE_FILES
        .iter()
        .copied()
        .chain(std::iter::once(
            decdn_common::data_dir::CLIENT_BUYER_DB_FILE,
        ))
}

/// Push all state-group findings.
pub(crate) fn check_state(report: &mut Report, cfg: &ResolvedConfig, daemon_running: bool) {
    let data_dir = &cfg.identity.data_dir;

    let secret_path = data_dir.join("node.secret");
    report.push(evaluate_secret(
        &FileFacts::read(&secret_path),
        &secret_path,
    ));

    let keystore = &cfg.blockchain.eth_keystore;
    report.push(evaluate_keystore(&FileFacts::read(keystore), keystore));

    // redb stores.
    for name in checked_store_files() {
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
mod tests;
