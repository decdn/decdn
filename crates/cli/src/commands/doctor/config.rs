//! Config group: run the daemon resolver and surface its aggregated result.

use std::path::Path;

use decdn_common::config::{ConfigNotice, ConfigNoticeLevel, ResolvedConfig, resolve_config};

use super::{Finding, Report, Severity};

/// Resolve config exactly as the daemon would. On success push a `Pass` and
/// return the resolved config for the other groups; on failure push a `Fail`
/// carrying the aggregated error text and return `None`.
///
/// A successful resolve also pushes one finding per resolve-time notice. This
/// is the reachability half: the daemon replays those through `tracing`, but
/// `decdn` links no subscriber, so `doctor` is where an operator sees them
/// without reading the node's log stream.
pub(crate) fn check_config(
    report: &mut Report,
    args: &decdn_common::cli::DoctorArgs,
    global_config: Option<&Path>,
) -> Option<ResolvedConfig> {
    match resolve_config(global_config, &args.run) {
        Ok((resolved, notices)) => {
            report.push(Finding {
                group: "Config",
                id: "config.resolves",
                severity: Severity::Pass,
                title: "config resolves".into(),
                detail: None,
                remediation: None,
            });
            push_notice_findings(report, &notices);
            Some(resolved)
        }
        Err(e) => {
            report.push(Finding {
                group: "Config",
                id: "config.resolves",
                severity: Severity::Fail,
                title: "config does not resolve".into(),
                detail: Some(decdn_common::redact::sanitize_err_chain(&e)),
                remediation: Some(
                    "fix the reported config error(s); run `decdn config validate` for detail"
                        .into(),
                ),
            });
            None
        }
    }
}

/// Turn each resolve-time notice into a finding.
///
/// `Warn` maps to [`Severity::Warn`] and `Info` to [`Severity::Pass`], so a
/// deliberate opt-out that is working as configured (a rate limit switched off)
/// stays visible without ever reaching the exit status, while a notice worth
/// attention rides the same `--strict` gate as every other `Warn` finding.
///
/// `field` goes in `detail` rather than the title: the message already reads as
/// a sentence about the value, and a `field=` tail is what a report consumer
/// greps. All notices share one `id` — the field is the discriminator, the same
/// way `origin.http` repeats per origin.
fn push_notice_findings(report: &mut Report, notices: &[ConfigNotice]) {
    for notice in notices {
        let severity = match notice.level {
            ConfigNoticeLevel::Warn => Severity::Warn,
            ConfigNoticeLevel::Info => Severity::Pass,
        };
        // `field` is a dotted config label or a bare env var name, and that
        // decides the fix: one is edited in the file, the other unset in the
        // environment. Nothing else about the notice says which.
        let remediation = (severity == Severity::Warn).then(|| {
            if notice.field.contains('.') {
                format!("review {} in the config file", notice.field)
            } else {
                format!("unset {} in the daemon's environment", notice.field)
            }
        });
        report.push(Finding {
            group: "Config",
            id: "config.notice",
            severity,
            title: notice.message.clone(),
            detail: Some(format!("field={}", notice.field)),
            remediation,
        });
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests
mod tests {
    use super::*;
    use crate::commands::doctor::{Report, Severity};

    fn args_for() -> decdn_common::cli::DoctorArgs {
        // Build DoctorArgs via clap from an argv so RunArgs defaults are populated.
        use clap::Parser;
        #[derive(clap::Parser)]
        struct W {
            #[command(flatten)]
            a: decdn_common::cli::DoctorArgs,
        }
        W::parse_from(["x", "--offline"]).a
    }

    #[test]
    fn invalid_config_is_a_fail_and_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("node.toml");
        // Missing required blockchain fields => resolve_config errors.
        std::fs::write(&cfg, "[cache]\ncache_size_mb = 1\n").unwrap();
        let mut report = Report::default();
        let resolved = check_config(&mut report, &args_for(), Some(&cfg));
        assert!(resolved.is_none());
        let last = report.findings.last().unwrap();
        assert_eq!(last.id, "config.resolves");
        assert_eq!(last.severity, Severity::Fail);
    }

    /// A `Warn` notice must reach `has_warn`, which is what `--strict` gates
    /// the exit status on: an unbounded rate-limit bookkeeping map is exactly
    /// the kind of thing an operator runs `doctor` to find, and it is
    /// invisible in the resolved values themselves.
    #[test]
    fn warn_notice_becomes_a_warn_finding() {
        let mut report = Report::default();
        push_notice_findings(
            &mut report,
            &[ConfigNotice {
                level: ConfigNoticeLevel::Warn,
                field: "security.max_tracked_sources".into(),
                message: "0: rate-limit bookkeeping map is unbounded".into(),
            }],
        );
        let finding = report.findings.last().unwrap();
        assert_eq!(finding.id, "config.notice");
        assert_eq!(finding.severity, Severity::Warn);
        assert!(finding.title.contains("unbounded"));
        // The field is the grep-friendly tail, not part of the sentence —
        // a title of "field: message" reads as a stutter when the message
        // already opens with the offending value.
        assert_eq!(
            finding.detail.as_deref(),
            Some("field=security.max_tracked_sources")
        );
        assert!(!finding.title.contains("security.max_tracked_sources"));
        assert_eq!(
            finding.remediation.as_deref(),
            Some("review security.max_tracked_sources in the config file")
        );
        assert!(report.has_warn());
    }

    /// A notice about a retired env var carries a bare var name, not a dotted
    /// key, and the fix is in the environment rather than the file. Doctor has
    /// only `field`'s shape to tell the two apart.
    #[test]
    fn env_var_notice_points_at_the_environment_not_the_file() {
        let mut report = Report::default();
        push_notice_findings(
            &mut report,
            &[ConfigNotice {
                level: ConfigNoticeLevel::Warn,
                field: "DECDN_DELIVERY_CEILING".into(),
                message: "set but no longer does anything".into(),
            }],
        );
        let finding = report.findings.last().unwrap();
        assert_eq!(
            finding.remediation.as_deref(),
            Some("unset DECDN_DELIVERY_CEILING in the daemon's environment")
        );
    }

    /// An `Info` notice stays visible but must not move the exit status: a
    /// deliberately disabled rate limit is working as configured, and a doctor
    /// that warns about it trains the operator to ignore doctor.
    #[test]
    fn info_notice_is_visible_without_moving_exit_status() {
        let mut report = Report::default();
        push_notice_findings(
            &mut report,
            &[ConfigNotice {
                level: ConfigNoticeLevel::Info,
                field: "security.per_source_rate_per_sec".into(),
                message: "0: per-source rate-limit disabled".into(),
            }],
        );
        let finding = report.findings.last().unwrap();
        assert_eq!(finding.severity, Severity::Pass);
        assert!(finding.title.contains("per-source rate-limit disabled"));
        // Nothing to remediate — the operator configured this on purpose.
        assert!(finding.remediation.is_none());
        assert!(!report.has_warn());
        assert!(!report.has_fail());
    }
}
