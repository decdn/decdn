//! Live-enrichment group. Best-effort: a reachable daemon adds a health
//! finding and a real cache-footprint reading; nothing here fails the run on
//! its own beyond a binding mismatch.

use std::time::Duration;

use decdn_common::admin::{AdminRpcClient, BindingStatus};
use decdn_common::config::ResolvedConfig;
use jsonrpsee::http_client::HttpClientBuilder;

use super::{Finding, Report, Severity};

/// What the live probe learned, threaded back into other groups.
pub struct LiveInfo {
    pub daemon_running: bool,
    pub cache_bytes: Option<u64>,
}

/// Extract the `decdn_cache_bytes` gauge value from Prometheus text.
pub fn parse_cache_bytes(metrics_body: &str) -> Option<u64> {
    metrics_body.lines().find_map(|line| {
        let line = line.trim_start();
        if line.starts_with('#') {
            return None;
        }
        let rest = line.strip_prefix("decdn_cache_bytes")?;
        let value = rest.trim();
        // A Prometheus gauge value fits comfortably in u64; clamp negatives
        // (never expected for a byte count) to 0 rather than propagating NaN
        // via `as` truncation semantics.
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        value
            .parse::<f64>()
            .ok()
            .map(|v| if v < 0.0 { 0 } else { v as u64 })
    })
}

/// Probe the admin RPC and metrics endpoints for a live daemon. Best-effort:
/// an unreachable admin server is reported as a `Pass` ("offline diagnosis"),
/// not a failure — the config/disk/state groups still stand on their own.
pub async fn probe_live(
    report: &mut Report,
    cfg: &ResolvedConfig,
    args: &decdn_common::cli::DoctorArgs,
    global_config: Option<&std::path::Path>,
) -> LiveInfo {
    let dur = Duration::from_millis(args.timeout_ms);
    let Ok(admin_url) =
        crate::commands::node::resolve_admin_url(args.admin_url.as_deref(), global_config)
    else {
        report.push(Finding {
            group: "Live",
            id: "live.health",
            severity: Severity::Pass,
            title: "admin endpoint not resolvable — live checks skipped".into(),
            detail: None,
            remediation: None,
        });
        return LiveInfo {
            daemon_running: false,
            cache_bytes: None,
        };
    };

    let Ok(client) = HttpClientBuilder::default()
        .request_timeout(dur)
        .build(&admin_url)
    else {
        report.push(Finding {
            group: "Live",
            id: "live.health",
            severity: Severity::Pass,
            title: "admin client could not be built — live checks skipped".into(),
            detail: None,
            remediation: None,
        });
        return LiveInfo {
            daemon_running: false,
            cache_bytes: None,
        };
    };

    let Ok(health) = client.health().await else {
        // No daemon (or admin disabled) — not an error; the offline checks stand.
        report.push(Finding {
            group: "Live",
            id: "live.health",
            severity: Severity::Pass,
            title: "no running daemon detected (offline diagnosis)".into(),
            detail: None,
            remediation: None,
        });
        return LiveInfo {
            daemon_running: false,
            cache_bytes: None,
        };
    };

    let mismatch = matches!(health.binding, BindingStatus::Mismatch);
    report.push(Finding {
        group: "Live",
        id: "live.health",
        severity: if mismatch { Severity::Fail } else { Severity::Pass },
        title: if mismatch {
            "daemon key/binding mismatch".into()
        } else {
            "daemon healthy".into()
        },
        detail: Some(format!(
            "binding={:?} registry_active={}",
            health.binding, health.registry_active
        )),
        remediation: if mismatch {
            Some(
                "the running key does not match the bound identity; check node.secret / registration"
                    .into(),
            )
        } else {
            None
        },
    });
    if !health.registry_active {
        report.push(Finding {
            group: "Live",
            id: "live.registry",
            severity: Severity::Warn,
            title: "node is not active in the on-chain registry".into(),
            detail: None,
            remediation: Some("run `decdn node register` / check bond & capacity".into()),
        });
    }

    // Scrape decdn_cache_bytes from the metrics endpoint (best-effort).
    let cache_bytes = scrape_cache_bytes(cfg, dur).await;

    LiveInfo {
        daemon_running: true,
        cache_bytes,
    }
}

/// Fetch and parse `decdn_cache_bytes` from the node's metrics endpoint.
/// Best-effort: any failure (connection, timeout, parse) silently yields
/// `None` rather than a Finding — a healthy daemon with an unreachable
/// metrics port is not itself diagnostic-worthy here, and the failure detail
/// could otherwise leak the metrics URL into the report.
async fn scrape_cache_bytes(cfg: &ResolvedConfig, dur: Duration) -> Option<u64> {
    let url = format!(
        "http://{}:{}/metrics",
        cfg.observability.metrics_bind, cfg.observability.metrics_port
    );
    let client = reqwest::Client::builder().timeout(dur).build().ok()?;
    let body = client.get(&url).send().await.ok()?.text().await.ok()?;
    parse_cache_bytes(&body)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;

    #[test]
    fn parses_gauge_line() {
        let body = "# HELP decdn_cache_bytes bytes\n# TYPE decdn_cache_bytes gauge\ndecdn_cache_bytes 1048576\n";
        assert_eq!(parse_cache_bytes(body), Some(1_048_576));
    }

    #[test]
    fn ignores_comments_and_missing() {
        assert_eq!(parse_cache_bytes("# decdn_cache_bytes 5\nother 3\n"), None);
    }

    #[test]
    fn handles_float_value() {
        assert_eq!(parse_cache_bytes("decdn_cache_bytes 2.0\n"), Some(2));
    }
}
