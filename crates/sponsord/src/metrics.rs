//! `GET /metrics`: Prometheus text exposition of what the daemon has issued
//! and refused, and of the pool keeper's view of the pool.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use sponsord_api::ErrorCode;
use sponsord_core::KeeperSnapshot;

/// Counters the HTTP layer updates.
#[derive(Debug, Default)]
pub struct Metrics {
    issued_fresh: AtomicU64,
    issued_registered: AtomicU64,
    errors: Mutex<BTreeMap<&'static str, u64>>,
}

impl Metrics {
    /// Count a capability handed out; `registered` if its signer was already
    /// registered on-chain.
    pub fn issued(&self, registered: bool) {
        let counter = if registered {
            &self.issued_registered
        } else {
            &self.issued_fresh
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a request answered with the error `code`.
    pub fn error(&self, code: ErrorCode) {
        let mut errors = self
            .errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *errors.entry(code.as_str()).or_insert(0) += 1;
    }

    /// The exposition, with the keeper's `pool` snapshot.
    #[must_use]
    pub fn render(&self, pool: &KeeperSnapshot) -> String {
        let mut out = String::new();
        family(
            &mut out,
            "sponsord_capabilities_issued_total",
            "counter",
            "Capabilities handed out, by whether the signer was already registered on-chain.",
        );
        let _ = writeln!(
            out,
            "sponsord_capabilities_issued_total{{registered=\"false\"}} {}\n\
             sponsord_capabilities_issued_total{{registered=\"true\"}} {}",
            self.issued_fresh.load(Ordering::Relaxed),
            self.issued_registered.load(Ordering::Relaxed),
        );
        family(
            &mut out,
            "sponsord_request_errors_total",
            "counter",
            "Requests answered with an error, by error code.",
        );
        {
            let errors = self
                .errors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (code, n) in errors.iter() {
                let _ = writeln!(out, "sponsord_request_errors_total{{code=\"{code}\"}} {n}");
            }
        }
        let gauges: [(&str, &str, &str, u64); 6] = [
            (
                "sponsord_pool_remaining_micro_usdc",
                "gauge",
                "The pool's remaining balance at the keeper's last read, in micro-USDC.",
                pool.remaining.0,
            ),
            (
                "sponsord_pool_last_check_unix",
                "gauge",
                "Unix time of the keeper's last successful balance read (0: never).",
                pool.last_check_unix,
            ),
            (
                "sponsord_pool_last_topup_unix",
                "gauge",
                "Unix time of the keeper's last successful top-up (0: never).",
                pool.last_topup_unix,
            ),
            (
                "sponsord_pool_topups_total",
                "counter",
                "Successful pool top-ups.",
                pool.topups,
            ),
            (
                "sponsord_pool_keeper_failures_total",
                "counter",
                "Failed chain reads and top-ups.",
                pool.failures,
            ),
            (
                "sponsord_pool_topup_unconfirmed_since_unix",
                "gauge",
                "Unix time a top-up came back unconfirmed, while further top-ups are held \
                 for it (0: none held).",
                pool.topup_unconfirmed_since_unix,
            ),
        ];
        for (name, kind, help, value) in gauges {
            family(&mut out, name, kind, help);
            let _ = writeln!(out, "{name} {value}");
        }
        out
    }
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}
