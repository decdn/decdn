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
mod tests;
