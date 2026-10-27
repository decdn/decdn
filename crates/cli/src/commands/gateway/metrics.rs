//! The gateway's metrics: what it served, refused and failed, as Prometheus
//! text exposition and as the `counters` of `/_status`. Hand-rolled as
//! `sponsord`'s are (a registry crate would add a shape of its own for nine
//! series).
//!
//! One unlabeled sibling counter per outcome, as `adr/appendix-observability.md`
//! § Reason splits prefers: each outcome has its own operator remedy (a
//! spent allowance or a page no capability covers, a pool that cannot pay,
//! an upstream fault), so no alert spans the family. Every series exports
//! from the start, at zero.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use hyper::StatusCode;
use serde::Serialize;

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
    pointer_updates: AtomicU64,
}

/// One read of every counter in [`Metrics`]. Each field has the name of its
/// series without the `decdn_gateway_` prefix and the `_total` suffix.
#[derive(Debug, Default, Serialize, Clone, Copy, PartialEq, Eq)]
pub(super) struct Counters {
    /// Content bytes handed to responses.
    pub(super) bytes_served: u64,
    /// Requests answered with success or a redirect.
    pub(super) requests_ok: u64,
    /// Requests for a path, entry or manifest that does not exist.
    pub(super) requests_not_found: u64,
    /// Requests refused before any pull, a missing token's 401 among them.
    pub(super) requests_refused: u64,
    /// Requests answered 402: a spent allowance, or a page no capability
    /// covers.
    pub(super) requests_allowance_exhausted: u64,
    /// Requests answered 503.
    pub(super) pull_failures_funding: u64,
    /// Requests answered 502.
    pub(super) pull_failures_other: u64,
    /// Pulls handed to the worker and not yet ended.
    pub(super) pulls_active: u64,
    /// Namespace pointers stored.
    pub(super) pointer_updates: u64,
}

impl Metrics {
    /// Count a request answered with `status`: success and redirects as
    /// served, 404 as not found, 402 as payment required (a public gateway's
    /// spent allowance, or a page no capability of a loopback gateway
    /// covers), 503 as a pool that cannot pay, 502 as an upstream fault, every
    /// other status (a missing token's 401 among them) as refused.
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

    /// A namespace pointer was stored.
    pub(super) fn pointer_updated(&self) {
        self.pointer_updates.fetch_add(1, Ordering::Relaxed);
    }

    /// Read every counter.
    pub(super) fn snapshot(&self) -> Counters {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        Counters {
            bytes_served: read(&self.bytes_served),
            requests_ok: read(&self.requests_ok),
            requests_not_found: read(&self.requests_not_found),
            requests_refused: read(&self.requests_refused),
            requests_allowance_exhausted: read(&self.requests_allowance_exhausted),
            pull_failures_funding: read(&self.pull_failures_funding),
            pull_failures_other: read(&self.pull_failures_other),
            pulls_active: read(&self.pulls_active),
            pointer_updates: read(&self.pointer_updates),
        }
    }

    /// The Prometheus text exposition, read through [`Self::snapshot`] so it
    /// and `/_status` cannot disagree.
    pub(super) fn render(&self) -> String {
        let c = self.snapshot();
        let series: [(&str, &str, &str, u64); 9] = [
            (
                "decdn_gateway_bytes_served_total",
                "counter",
                "Content bytes handed to responses.",
                c.bytes_served,
            ),
            (
                "decdn_gateway_requests_ok_total",
                "counter",
                "Requests answered with success or a redirect.",
                c.requests_ok,
            ),
            (
                "decdn_gateway_requests_not_found_total",
                "counter",
                "Requests for a path, entry or manifest that does not exist.",
                c.requests_not_found,
            ),
            (
                "decdn_gateway_requests_refused_total",
                "counter",
                "Requests refused before any pull: an unknown host, a missing token, a bad URI, a \
                 method, a range past the end.",
                c.requests_refused,
            ),
            (
                "decdn_gateway_requests_allowance_exhausted_total",
                "counter",
                "Requests answered 402: on a public gateway the day's allowance is spent; on a \
                 loopback gateway no capability covers the page.",
                c.requests_allowance_exhausted,
            ),
            (
                "decdn_gateway_pull_failures_funding_total",
                "counter",
                "Requests answered 503: the gateway's pool cannot pay.",
                c.pull_failures_funding,
            ),
            (
                "decdn_gateway_pull_failures_other_total",
                "counter",
                "Requests answered 502: the upstream pull failed.",
                c.pull_failures_other,
            ),
            (
                "decdn_gateway_pulls_active",
                "gauge",
                "Pulls handed to the worker and not yet ended.",
                c.pulls_active,
            ),
            (
                "decdn_gateway_pointer_updates_total",
                "counter",
                "Namespace pointers stored through PUT /_pointer.",
                c.pointer_updates,
            ),
        ];
        let mut out = String::new();
        for (name, kind, help, value) in series {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} {kind}");
            let _ = writeln!(out, "{name} {value}");
        }
        out
    }
}

#[cfg(test)]
mod tests;
