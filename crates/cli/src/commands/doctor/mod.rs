//! `decdn node doctor` — read-only diagnosis of node config, on-disk state,
//! disk-vs-budget, and network reachability. Runs pre-boot and against a live
//! daemon; mutates nothing.

use std::path::Path;

use serde::Serialize;

mod report;

/// A single diagnostic outcome. Every `Warn`/`Fail` carries a one-line
/// `remediation`; `detail` holds a grep-friendly `key=value` tail.
#[derive(Debug, Serialize)]
pub struct Finding {
    pub group: &'static str,
    pub id: &'static str,
    pub severity: Severity,
    pub title: String,
    pub detail: Option<String>,
    pub remediation: Option<String>,
}

/// Diagnostic severity. Only `Fail` (and `Warn` under `--strict`) drives a
/// nonzero exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Pass,
    Warn,
    Fail,
}

/// The collected findings of one doctor run.
#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub findings: Vec<Finding>,
}

impl Report {
    pub fn push(&mut self, finding: Finding) {
        self.findings.push(finding);
    }

    /// Returns `(pass, warn, fail)` counts.
    pub fn counts(&self) -> (usize, usize, usize) {
        self.findings
            .iter()
            .fold((0, 0, 0), |(p, w, f), finding| match finding.severity {
                Severity::Pass => (p + 1, w, f),
                Severity::Warn => (p, w + 1, f),
                Severity::Fail => (p, w, f + 1),
            })
    }

    pub fn has_fail(&self) -> bool {
        self.findings.iter().any(|x| x.severity == Severity::Fail)
    }

    pub fn has_warn(&self) -> bool {
        self.findings.iter().any(|x| x.severity == Severity::Warn)
    }
}

/// Sentinel error returned by [`run`] when the report contains failing checks
/// (or warnings under `--strict`). `main` recognizes it to set a nonzero exit
/// **without** printing an `Error:` line — the report is already on stdout.
#[derive(Debug)]
pub struct DoctorFailed;

impl std::fmt::Display for DoctorFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "doctor found failing checks")
    }
}

impl std::error::Error for DoctorFailed {}

/// Run every diagnostic group, render the report, and map severity to exit.
#[allow(clippy::unused_async)] // Future-shaped: later tasks add `.await`ed checks here.
// `async` is preserved even though no body is currently `.await`ed: `node_dispatch`
// awaits this future the same way it awaits every other subcommand handler, so the
// signature is part of the dispatch contract. Task 2+ add real network/admin-RPC
// awaits inside this body.
pub async fn run(
    args: &decdn_common::cli::DoctorArgs,
    _global_config: Option<&Path>,
) -> anyhow::Result<()> {
    let report = Report::default();
    // Later tasks populate `report` here.

    let mut stdout = std::io::stdout().lock();
    report::render(&mut stdout, &report, args.json)
        .map_err(|e| anyhow::anyhow!("failed to write doctor report: {e}"))?;

    let fail = report.has_fail() || (args.strict && report.has_warn());
    if fail {
        return Err(DoctorFailed.into());
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;

    fn f(sev: Severity) -> Finding {
        Finding {
            group: "G",
            id: "x",
            severity: sev,
            title: "t".into(),
            detail: None,
            remediation: None,
        }
    }

    #[test]
    fn counts_and_flags() {
        let mut r = Report::default();
        r.push(f(Severity::Pass));
        r.push(f(Severity::Warn));
        r.push(f(Severity::Fail));
        assert_eq!(r.counts(), (1, 1, 1));
        assert!(r.has_fail());
        assert!(r.has_warn());
    }

    #[test]
    fn clean_report_has_no_fail_or_warn() {
        let mut r = Report::default();
        r.push(f(Severity::Pass));
        assert_eq!(r.counts(), (1, 0, 0));
        assert!(!r.has_fail());
        assert!(!r.has_warn());
    }
}
