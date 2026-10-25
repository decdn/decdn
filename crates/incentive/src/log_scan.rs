//! Bounded `eth_getLogs` windows that learn the provider's range cap.
//!
//! Providers cap the block range one `eth_getLogs` may span, each with its own
//! error code and limit, and the free tiers sit far below the advertised number
//! (dRPC's free plan rejects above about 100 blocks). Every scan here walks `[from, to]`
//! one window at a time, halves a window the provider rejects as too wide
//! ([`is_range_rejection`]) and doubles it back after a run of accepted
//! windows ([`WindowSpan`]). The daemon's chain poller drives the same
//! primitives inside its tick loop; [`scan_logs`] is the standalone driver for
//! a one-shot scan such as `decdn node earnings --address`.

use std::num::NonZeroU64;
use std::time::Duration;

use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::transports::TransportError;
use anyhow::Context;

/// The inclusive end of the `eth_getLogs` window that starts at `start`: at
/// most `span` blocks, clamped to `to`. A scan walks `[from, to]` with it one
/// window at a time, because the span can shrink between windows when the
/// provider rejects a range. A `span` of `0` is treated as `1`.
#[must_use]
pub fn window_end(start: u64, to: u64, span: u64) -> u64 {
    start.saturating_add(span.max(1) - 1).min(to)
}

/// The span to retry with after the provider rejects the inclusive window
/// `[start, end]`: half its length, rounded down. `None` for a one-block window,
/// which cannot shrink. Computed from `end - start`, so a window that spans all
/// of `u64` cannot overflow.
#[must_use]
pub const fn halved_span(start: u64, end: u64) -> Option<u64> {
    let last = end.saturating_sub(start);
    if last == 0 {
        return None;
    }
    // (last + 1) / 2 without the `+ 1` overflow.
    Some(last / 2 + last % 2)
}

/// Accepted windows since the last shrink or regrow before [`WindowSpan`]
/// doubles its span. Against a provider whose real cap sits below the span,
/// a scan then pays about one rejected request per this many accepted
/// windows.
pub const SPAN_REGROW_AFTER_WINDOWS: u32 = 32;

/// The block span of the next `eth_getLogs` window, learned from the
/// provider. It starts at the configured ceiling. A rejected window sets it to
/// half that window's length ([`halved_span`]), never above the current span.
/// After [`SPAN_REGROW_AFTER_WINDOWS`] accepted windows it doubles, up to the
/// ceiling. Every accepted window counts, short ones too: a caught-up poller
/// therefore climbs back to the ceiling after a transient or misclassified
/// rejection, and a real cap shows up as the next backfill's rejections
/// instead of as a span that stays low for good.
#[derive(Debug, Clone)]
pub struct WindowSpan {
    current: u64,
    ceiling: u64,
    /// The span before the latest regrow, while no shrink has followed it: a
    /// shrink back to this level is the regrow's probe being rejected.
    probing_from: Option<u64>,
    streak: u32,
}

/// What a [`WindowSpan::shrink`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shrunk {
    /// The new span.
    pub span: u64,
    /// The shrink undid the latest regrow: the provider rejected the doubled
    /// span and the span is back at (or above) where the regrow started. A
    /// shrink that is not a rejected probe is news about the provider.
    pub rejected_probe: bool,
}

impl WindowSpan {
    /// Start at `ceiling`.
    #[must_use]
    pub const fn new(ceiling: NonZeroU64) -> Self {
        Self {
            current: ceiling.get(),
            ceiling: ceiling.get(),
            probing_from: None,
            streak: 0,
        }
    }

    /// The span of the next window, always `>= 1`.
    #[must_use]
    pub const fn current(&self) -> u64 {
        self.current
    }

    /// Shrink after the provider rejected the window `[start, end]`, to half
    /// the window's length and never above the current span. `None` when the
    /// window is one block and cannot shrink (the span is then unchanged).
    #[must_use]
    pub fn shrink(&mut self, start: u64, end: u64) -> Option<Shrunk> {
        let span = halved_span(start, end)?.min(self.current);
        let rejected_probe = self.probing_from.is_some_and(|from| span >= from);
        self.current = span;
        self.probing_from = None;
        self.streak = 0;
        Some(Shrunk {
            span,
            rejected_probe,
        })
    }

    /// Record an accepted window. Returns the new span when this window
    /// completes a regrow streak.
    #[must_use]
    pub fn record_success(&mut self) -> Option<u64> {
        if self.current >= self.ceiling {
            return None;
        }
        self.streak = self.streak.saturating_add(1);
        if self.streak < SPAN_REGROW_AFTER_WINDOWS {
            return None;
        }
        self.streak = 0;
        self.probing_from = Some(self.current);
        self.current = self.current.saturating_mul(2).min(self.ceiling);
        Some(self.current)
    }
}

/// Whether a failed `eth_getLogs` is the provider refusing the window's block
/// range (or its result count), so a smaller window can succeed. Providers use
/// different codes for this: dRPC `35 "ranges over 10000 blocks are not
/// supported"`, Alchemy `-32600 "… up to a 10 block range"`, Infura `-32005
/// "query returned more than 10000 results"` (also its rate-limit code),
/// Quicknode `-32614 "eth_getLogs is limited to a 10,000 range"`, Ankr
/// `"block range is too wide"`. The codes do not agree, so the test is the
/// message: a JSON-RPC error response that names a range or a result count
/// *and* a limit (`LIMIT_WORDS`). Both parts are required because a false
/// match still costs throughput until the span doubles back: a load-balanced
/// provider whose backend lags head reports a `toBlock` past that backend's
/// head with range wording but no limit ("block range extends beyond current
/// head block", "block number is out of range"), and a smaller window is not
/// the fix for that. Any other error (a timeout, a transport failure, a rate
/// limit, an unknown block) leaves the window span alone, so a transient fault
/// cannot shrink it.
#[must_use]
pub fn is_range_rejection(err: &anyhow::Error) -> bool {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<TransportError>())
        .and_then(TransportError::as_error_resp)
        .is_some_and(|resp| {
            let message = resp.message.to_ascii_lowercase();
            (message.contains("range") || message.contains("results"))
                && !message.contains("out of range")
                && LIMIT_WORDS.iter().any(|word| message.contains(word))
        })
}

/// Words that mark a range or result-count message as a limit, not a
/// head-lag or lookup error. See [`is_range_rejection`].
const LIMIT_WORDS: [&str; 10] = [
    "limit",
    "max",
    "exceed",
    "up to",
    " over ",
    "more than",
    "too many",
    "too wide",
    "too large",
    "not supported",
];

/// How many times [`scan_logs`] retries a window the provider failed for a
/// reason other than its range: a timeout, a rate limit, a transport fault.
/// A long scan makes hundreds of requests against a public endpoint, and one
/// refused request must not discard every window before it.
pub const SCAN_WINDOW_RETRIES: u32 = 3;

/// The sleep before the first retry of a failed window; each further retry
/// doubles it (2 s, 4 s, 8 s), which outlasts the per-minute quota windows of
/// the free tiers.
pub const SCAN_WINDOW_RETRY_DELAY: Duration = Duration::from_secs(2);

/// Fetch every log `filter` matches in `[from, to]`, one window at a time,
/// starting at `ceiling` blocks per window. A window the provider rejects as
/// too wide is retried at half its length; after enough accepted windows the
/// span doubles back. Any other failure of a window is retried
/// [`SCAN_WINDOW_RETRIES`] times with a doubling delay. `on_window` runs after
/// each accepted window with the window's last block and `to`, for progress
/// output. The logs come back in request order, which is chain order.
///
/// # Errors
///
/// A window that fails every retry, and a range rejection of a one-block
/// window, which no smaller window can fix. The error names the window, so a
/// caller can resume from it.
pub async fn scan_logs<P: Provider>(
    provider: &P,
    filter: &Filter,
    from: u64,
    to: u64,
    ceiling: NonZeroU64,
    mut on_window: impl FnMut(u64, u64),
) -> anyhow::Result<Vec<Log>> {
    let mut span = WindowSpan::new(ceiling);
    let mut logs = Vec::new();
    let mut start = from;
    let mut retries = 0u32;
    while start <= to {
        let end = window_end(start, to, span.current());
        let window = filter.clone().from_block(start).to_block(end);
        match provider
            .get_logs(&window)
            .await
            .with_context(|| format!("eth_getLogs [{start}, {end}]"))
        {
            Ok(batch) => {
                logs.extend(batch);
                let _regrown = span.record_success();
                retries = 0;
                on_window(end, to);
                start = end.saturating_add(1);
                if end == u64::MAX {
                    break;
                }
            }
            Err(err) if is_range_rejection(&err) => {
                if span.shrink(start, end).is_none() {
                    return Err(err.context(
                        "the RPC provider rejects even a one-block eth_getLogs window (a range \
                         cap, or more results in one block than it returns); use another \
                         provider",
                    ));
                }
            }
            Err(_) if retries < SCAN_WINDOW_RETRIES => {
                tokio::time::sleep(SCAN_WINDOW_RETRY_DELAY * 2u32.pow(retries)).await;
                retries += 1;
            }
            Err(err) => {
                return Err(err.context(format!(
                    "gave up after {SCAN_WINDOW_RETRIES} retries; blocks below {start} were \
                     scanned"
                )));
            }
        }
    }
    Ok(logs)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
