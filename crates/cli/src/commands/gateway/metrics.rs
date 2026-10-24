//! The public gateway's metrics: Prometheus text exposition of what it
//! served, refused and failed, hand-rolled as `sponsord`'s are (a registry
//! crate would add a shape of its own for eight series).
//!
//! One unlabeled sibling counter per outcome, as `adr/appendix-observability.md`
//! § Reason splits prefers: each outcome has its own operator remedy (a
//! spent allowance, a pool that cannot pay, an upstream fault), so no alert
//! spans the family. Every series exports from the start, at zero.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use hyper::StatusCode;

/// What the gateway counted since it started.
#[derive(Debug, Default)]
pub(super) struct Metrics {
    bytes_served: AtomicU64,
    requests_ok: AtomicU64,
    requests_not_found: AtomicU64,
    requests_refused: AtomicU64,
    requests_allowance_exhausted: AtomicU64,
    pull_failures_funding: AtomicU64,
    pull_failures_other: AtomicU64,
    pulls_active: AtomicU64,
}

impl Metrics {
    /// Count a request answered with `status`: success and redirects as
    /// served, 404 as not found, 402 as the allowance spent, 503 as a pool
    /// that cannot pay, 502 as an upstream fault, every other status as
    /// refused.
    pub(super) fn request(&self, status: StatusCode) {
        let counter = match status {
            s if s.is_success() || s.is_redirection() => &self.requests_ok,
            StatusCode::NOT_FOUND => &self.requests_not_found,
            StatusCode::PAYMENT_REQUIRED => &self.requests_allowance_exhausted,
            StatusCode::SERVICE_UNAVAILABLE => &self.pull_failures_funding,
            StatusCode::BAD_GATEWAY => &self.pull_failures_other,
            _ => &self.requests_refused,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Count `bytes` of content handed to a response.
    pub(super) fn bytes(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.bytes_served.fetch_add(bytes, Ordering::Relaxed);
    }

    /// A pull was handed to the worker.
    pub(super) fn pull_started(&self) {
        self.pulls_active.fetch_add(1, Ordering::Relaxed);
    }

    /// A pull ended, whole or not.
    pub(super) fn pull_ended(&self) {
        // Paired with `pull_started` by the pull's guard, so it never
        // underflows.
        self.pulls_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// The Prometheus text exposition.
    pub(super) fn render(&self) -> String {
        let series: [(&str, &str, &str, &AtomicU64); 8] = [
            (
                "decdn_gateway_bytes_served_total",
                "counter",
                "Content bytes handed to responses.",
                &self.bytes_served,
            ),
            (
                "decdn_gateway_requests_ok_total",
                "counter",
                "Requests answered with success or a redirect.",
                &self.requests_ok,
            ),
            (
                "decdn_gateway_requests_not_found_total",
                "counter",
                "Requests for a path, entry or manifest that does not exist.",
                &self.requests_not_found,
            ),
            (
                "decdn_gateway_requests_refused_total",
                "counter",
                "Requests refused before any pull: an unknown host, a bad URI, a method, a range \
                 past the end.",
                &self.requests_refused,
            ),
            (
                "decdn_gateway_requests_allowance_exhausted_total",
                "counter",
                "Requests answered 402: the day's allowance is spent.",
                &self.requests_allowance_exhausted,
            ),
            (
                "decdn_gateway_pull_failures_funding_total",
                "counter",
                "Requests answered 503: the gateway's pool cannot pay.",
                &self.pull_failures_funding,
            ),
            (
                "decdn_gateway_pull_failures_other_total",
                "counter",
                "Requests answered 502: the upstream pull failed.",
                &self.pull_failures_other,
            ),
            (
                "decdn_gateway_pulls_active",
                "gauge",
                "Pulls handed to the worker and not yet ended.",
                &self.pulls_active,
            ),
        ];
        let mut out = String::new();
        for (name, kind, help, value) in series {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} {kind}");
            let _ = writeln!(out, "{name} {}", value.load(Ordering::Relaxed));
        }
        out
    }
}

#[cfg(test)]
mod tests;
