//! Bound on the requester-generated `timestamp_us` a handler accepts.
//!
//! The signed response body ([`decdn_protocol::client::StreamResponseBody`])
//! echoes the request's `timestamp_us` into the EIP-712 signed set (ADR 005
//! § Signer binding). Other parties read that signed timestamp, and a pull
//! receipt carries it as the approximate time of each serve. A node that signs
//! whatever timestamp a requester chose therefore attests to a serve time of
//! the requester's choosing. The client handler refuses a skewed request
//! before anything is signed, so every `slash_sig` this node emits carries a
//! timestamp consistent with its clock at admission. A long serve only ages
//! the admitted timestamp: the window tiers and the closing response sign it
//! later, and signing never checks the bound again. An honest requester stamps
//! the request from its own wall clock, so it only trips the bound with a
//! misconfigured clock. The refusal carries
//! [`decdn_protocol::APP_ERR_TIMESTAMP_SKEW`] so the requester's own tooling
//! names the clock, and the throttled `warn!` at each refusal site tells this
//! node's operator. That includes the case where this node's clock is the
//! wrong one and every inbound request refuses.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::warn_throttle::WarnThrottle;

/// Largest accepted difference, in microseconds, between a request's
/// `timestamp_us` and this node's clock: 30 seconds either way. Wide enough
/// that NTP-disciplined clocks never come close.
pub const MAX_REQUEST_TIMESTAMP_SKEW_US: u64 = 30_000_000;

/// Throttle for the pre-epoch clock `warn!`. [`now_us`] runs once per inbound
/// request, so an unthrottled line would repeat for every request.
static PRE_EPOCH_WARN: WarnThrottle = WarnThrottle::new(Duration::from_secs(60));

/// Wall-clock now in microseconds since `UNIX_EPOCH`.
///
/// A host clock set before the epoch reads as 0 (panic-free per the workspace
/// anti-panic policy). That fails closed: it refuses every request that
/// carries a current timestamp. It is a node-local fault, so it gets its own
/// unambiguous `warn!`, at most once a minute. The refusal sites' throttled
/// `warn!` lines carry `now_us = 0` too.
pub fn now_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        // Truncation unreachable until year ~586524; saturate rather than wrap.
        Ok(d) => u64::try_from(d.as_micros()).unwrap_or(u64::MAX),
        Err(err) => {
            if let Some(suppressed) = PRE_EPOCH_WARN.admit() {
                tracing::warn!(
                    error = %err,
                    suppressed,
                    "wall-clock before UNIX_EPOCH; request timestamps read against 0, so \
                     every inbound stream request will be refused for clock skew. Set \
                     the host clock."
                );
            }
            0
        }
    }
}

/// The distance between a request's `timestamp_us` and `now_us` when it
/// exceeds [`MAX_REQUEST_TIMESTAMP_SKEW_US`]; `None` for an acceptable
/// timestamp. Symmetric: a stale and a future timestamp misstate the serve
/// time alike.
pub fn excessive_skew_us(timestamp_us: u64, now_us: u64) -> Option<u64> {
    let skew = now_us.abs_diff(timestamp_us);
    (skew > MAX_REQUEST_TIMESTAMP_SKEW_US).then_some(skew)
}

#[cfg(test)]
mod tests;
