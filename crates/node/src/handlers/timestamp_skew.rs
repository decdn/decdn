//! Bound on the requester-generated `timestamp_us` a handler accepts.
//!
//! Every signed response body ([`decdn_protocol::ProbeResponseBody`],
//! [`decdn_protocol::client::StreamResponseBody`]) echoes the request's
//! `timestamp_us` into the EIP-712 slash-evidence set (ADR 014 §1), and
//! `SlashJudge` reads that field as the serve time: it pairs evidence inside a
//! window keyed on it, applies blacklist effective-time comparisons to it, and
//! rejects evidence stamped more than 60 seconds ahead of block time. A node
//! that signs whatever timestamp a requester chose therefore attests to a
//! serve time of the requester's choosing. Both handlers refuse a skewed
//! request before anything is signed, so every `slash_sig` this node emits
//! carries a timestamp consistent with its clock at admission. A long serve
//! only ages the admitted timestamp: the window tiers and the closing response
//! sign it later, and signing never checks the bound again. ADR 005's RTT
//! computation (`receive_time - timestamp_us`) makes `timestamp_us` a fresh
//! wall-clock read on the requester, so an honest requester only trips the
//! bound with a misconfigured clock. The refusal carries
//! [`decdn_protocol::APP_ERR_TIMESTAMP_SKEW`] so the requester's own tooling
//! names the clock, and the throttled `warn!` at each refusal site tells this
//! node's operator. That includes the case where this node's clock is the
//! wrong one and every inbound request refuses.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::warn_throttle::WarnThrottle;

/// Largest accepted difference, in microseconds, between a request's
/// `timestamp_us` and this node's clock: 30 seconds either way.
///
/// Half of `SlashJudge`'s 60-second future-timestamp bound
/// (`MAX_FUTURE_SKEW_US` in `contracts/src/SlashJudge.sol`). While this node's
/// clock runs less than 30 seconds ahead of chain time, every timestamp it
/// admits stays inside that bound, so its signed evidence never reverts
/// `EvidenceInFuture`. Wide enough that NTP-disciplined clocks never come
/// close.
///
/// It also sets how long an upward reprice waits. A probe signed at the old
/// rate can carry a timestamp up to this bound behind real time, and a stream
/// signed at the new rate one up to this bound ahead. So a node is slashable
/// for the 30-second pairing window plus twice this bound, 90 seconds of real
/// time after its last probe at the old rate.
pub const MAX_REQUEST_TIMESTAMP_SKEW_US: u64 = 30_000_000;

/// `SlashJudge`'s `MAX_FUTURE_SKEW_US`: the furthest ahead of block time a
/// signed evidence timestamp may sit.
const SLASH_JUDGE_MAX_FUTURE_SKEW_US: u64 = 60_000_000;

// An admitted timestamp sits at most the bound ahead of this node's clock.
// Twice the bound leaves the other half of the judge's allowance for this
// node's clock to run ahead of chain time.
const _: () = assert!(2 * MAX_REQUEST_TIMESTAMP_SKEW_US <= SLASH_JUDGE_MAX_FUTURE_SKEW_US);

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
                     every inbound probe/stream request will be refused for clock skew. Set \
                     the host clock."
                );
            }
            0
        }
    }
}

/// The distance between a request's `timestamp_us` and `now_us` when it
/// exceeds [`MAX_REQUEST_TIMESTAMP_SKEW_US`]; `None` for an acceptable
/// timestamp. Symmetric: a stale and a future timestamp are refused alike,
/// since `SlashJudge` reads signed timestamps on both sides of its pairing
/// window.
pub fn excessive_skew_us(timestamp_us: u64, now_us: u64) -> Option<u64> {
    let skew = now_us.abs_diff(timestamp_us);
    (skew > MAX_REQUEST_TIMESTAMP_SKEW_US).then_some(skew)
}

#[cfg(test)]
mod tests;
