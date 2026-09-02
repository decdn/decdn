//! Origin group. HTTP origins get a live `HEAD` reachability probe; fs
//! origins are stat-checked; S3 origins are validated for config shape
//! only — a live S3 dial needs the AWS SDK, which must not enter this
//! binary (#578).

use std::path::Path;
use std::time::Duration;

use decdn_common::config::{ResolvedConfig, ResolvedOrigin};
use decdn_common::redact::{redact_userinfo, sanitize_rpc_display};

use super::{Finding, Report, Severity};

pub fn evaluate_fs_origin(index: usize, path: &Path, exists: bool, is_dir: bool) -> Finding {
    let ok = exists && is_dir;
    Finding {
        group: "Origins",
        id: "origin.fs",
        severity: if ok { Severity::Pass } else { Severity::Fail },
        title: if ok {
            format!("fs origin #{index} readable")
        } else {
            format!("fs origin #{index} missing or not a directory")
        },
        detail: Some(format!("path={}", path.display())),
        remediation: if ok {
            None
        } else {
            Some("create the origin directory or fix cache.origin path".into())
        },
    }
}

/// Probe every configured origin backend and push one finding per origin.
/// A node with no origin configured (a pure relay/edge node) is not an
/// error — that gets a single `Pass`.
pub async fn check_origins(report: &mut Report, cfg: &ResolvedConfig, timeout_ms: u64) {
    if cfg.cache.origins.is_empty() {
        report.push(Finding {
            group: "Origins",
            id: "origin.none",
            severity: Severity::Pass,
            title: "no origin backend configured (relay/edge node)".into(),
            detail: None,
            remediation: None,
        });
        return;
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .build();
    let client = match client {
        Ok(c) => c,
        Err(e) => {
            report.push(Finding {
                group: "Origins",
                id: "origin.http",
                severity: Severity::Warn,
                title: "could not build HTTP client for origin probe".into(),
                detail: Some(format!("err={e}")),
                remediation: None,
            });
            return;
        }
    };

    for (i, origin) in cfg.cache.origins.iter().enumerate() {
        match origin {
            ResolvedOrigin::Http { url, .. } => {
                let target = url.as_url().as_str();
                // Origin URLs may carry basic-auth-in-URL credentials
                // (operators do this for internal origins); strip
                // userinfo before it reaches the report.
                let logged = redact_userinfo(target);
                match client.head(target).send().await {
                    Ok(resp) => report.push(Finding {
                        group: "Origins",
                        id: "origin.http",
                        severity: Severity::Pass,
                        title: format!("http origin #{i} reachable"),
                        detail: Some(format!("url={logged} status={}", resp.status().as_u16())),
                        remediation: None,
                    }),
                    Err(e) => report.push(Finding {
                        group: "Origins",
                        id: "origin.http",
                        severity: Severity::Warn,
                        title: format!("http origin #{i} unreachable"),
                        detail: Some(format!("url={logged} err={}", sanitize_rpc_display(e))),
                        remediation: Some(
                            "verify the origin host is reachable from this node".into(),
                        ),
                    }),
                }
            }
            ResolvedOrigin::Fs { path } => {
                let meta = std::fs::metadata(path);
                let (exists, is_dir) = match &meta {
                    Ok(m) => (true, m.is_dir()),
                    Err(_) => (false, false),
                };
                report.push(evaluate_fs_origin(i, path, exists, is_dir));
            }
            ResolvedOrigin::S3(_) => report.push(Finding {
                group: "Origins",
                id: "origin.s3",
                severity: Severity::Pass,
                title: format!("s3 origin #{i} config valid (live reachability needs the daemon)"),
                detail: None,
                remediation: None,
            }),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;
    use crate::commands::doctor::Severity;
    use std::path::Path;

    #[test]
    fn fs_origin_dir_passes_missing_fails() {
        assert_eq!(
            evaluate_fs_origin(0, Path::new("/x"), true, true).severity,
            Severity::Pass
        );
        assert_eq!(
            evaluate_fs_origin(0, Path::new("/x"), false, false).severity,
            Severity::Fail
        );
        assert_eq!(
            evaluate_fs_origin(0, Path::new("/x"), true, false).severity,
            Severity::Fail
        );
    }
}
