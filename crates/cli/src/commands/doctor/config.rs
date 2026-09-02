//! Config group: run the daemon resolver and surface its aggregated result.

use std::path::Path;

use decdn_common::config::{ResolvedConfig, resolve_config};

use super::{Finding, Report, Severity};

/// Resolve config exactly as the daemon would. On success push a `Pass` and
/// return the resolved config for the other groups; on failure push a `Fail`
/// carrying the aggregated error text and return `None`.
pub fn check_config(
    report: &mut Report,
    args: &decdn_common::cli::DoctorArgs,
    global_config: Option<&Path>,
) -> Option<ResolvedConfig> {
    match resolve_config(global_config, &args.run) {
        Ok(resolved) => {
            report.push(Finding {
                group: "Config",
                id: "config.resolves",
                severity: Severity::Pass,
                title: "config resolves".into(),
                detail: None,
                remediation: None,
            });
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
}
