//! Windowing + reorg-margin primitives shared by every on-chain watcher.
//!
//! `resumable_watcher::run` walks a `[cursor, head]` gap in bounded
//! `eth_getLogs` windows and rewinds a shallow reorg margin on resume; the
//! buyer-side bootstrap reconciliation scan ([`crate::buyer_channel`], #763)
//! reuses the same math. Kept here (rather than in any one watcher) so a new
//! consumer picks up the shared span instead of forking a fresh one (#1092).
//! The origin directory's genesis `ContentClaimed` replay
//! ([`crate::dht::chain_origin_directory`], #651) also windows through
//! [`backfill_windows`], passing its own deliberate `REPLAY_WINDOW_BLOCKS = 9_000`
//! span (a margin under the provider 10k `eth_getLogs` cap) rather than
//! [`MAX_BACKFILL_BLOCK_SPAN`] (#1139).

use anyhow::Result;

/// How many blocks the persisted scan checkpoint is rewound before the
/// resume backfill (#751), absorbing a shallow reorg between the last scanned
/// block and the next boot: a `ChannelOpened` re-mined at a slightly different
/// height after a reorg is still inside the rescanned window.
/// `register_open_channel` is idempotent, so the only cost of the margin is a
/// few extra blocks of `eth_getLogs`. Sized for the shallow reorgs of an
/// Arbitrum-Sepolia-class L2.
///
/// Reachable only from [`resumable_watcher::CursorPolicy::Persisted`], the
/// variant that carries it (#1227) — it is the rewind applied to a *durable*
/// cursor, and the `HeadMinusWindow` / `FullReplay` watchers re-derive their
/// floor from head on every boot, so there is nothing to rewind.
///
/// Its one live consumer is the settlement watcher
/// ([`crate::payment_settlement`], #751). The origin directory
/// ([`crate::dht::chain_origin_directory`]) also constructs a `Persisted` policy
/// carrying this value, but never reads it: it seeds its cursor from its
/// bootstrap snapshot block, and a seeded cursor bypasses floor derivation
/// entirely (see `WatcherConfig::seed_cursor`), so its resume floor is the raw
/// checkpoint with no rewind. That dead field is pre-existing and needs
/// `CursorPolicy`'s persistence and floor-derivation axes split apart to remove
/// (#1238); do not read it as evidence the margin applies there.
///
/// [`resumable_watcher::CursorPolicy::Persisted`]: super::resumable_watcher::CursorPolicy
pub(crate) const REORG_MARGIN_BLOCKS: u64 = 128;

/// Maximum block span scanned per `eth_getLogs` during the resume backfill
/// (#751). A node down for a long time resumes from a checkpoint many thousands
/// of blocks behind head; a single unbounded `eth_getLogs` over that gap would
/// exceed the range/result caps most RPC providers enforce. The backfill walks
/// the gap in windows of this size instead.
///
/// This bounds the *block span* per request, not the *result count* — some
/// providers cap `eth_getLogs` by number of matched logs (or a lower block span)
/// rather than range, so a dense 10k-block window could still trip a result-count
/// limit. 10k is a conservative default that clears the common range caps; tie it
/// to the deployment provider's documented `eth_getLogs` limit (and make it
/// configurable) if a target RPC enforces a tighter or result-count-based cap.
///
/// Shared so the buyer-side bootstrap reconciliation scan
/// ([`crate::buyer_channel`], #763) uses the same window cap.
pub(crate) const MAX_BACKFILL_BLOCK_SPAN: u64 = 10_000;

/// Split an inclusive `[from, to]` block range into successive inclusive windows
/// of at most `span` blocks (#751), so a long resume backfill issues bounded
/// `eth_getLogs` calls. Pure and allocation-light (one entry per window); a
/// `from > to` range yields no windows (the caller validates that separately via
/// [`check_backfill_range`]). Unit-tested for the window math.
///
/// Shared so the buyer-side bootstrap reconciliation scan
/// ([`crate::buyer_channel`], #763) reuses the same windowing rather than forking it.
pub(crate) fn backfill_windows(from: u64, to: u64, span: u64) -> Vec<(u64, u64)> {
    let mut windows = Vec::new();
    if from > to || span == 0 {
        return windows;
    }
    let mut start = from;
    loop {
        let end = start.saturating_add(span - 1).min(to);
        windows.push((start, end));
        if end >= to {
            return windows;
        }
        start = end.saturating_add(1);
    }
}

/// Validate the bring-up backfill range `[from, to]` (#762). On a consistent
/// chain the head is monotonic, so `to` (read on the first watcher cycle) is
/// always `>=` `from` (the head captured at bootstrap); `from == to` is a valid
/// single-block range that must still be scanned (a `ChannelOpened` can sit in
/// that exact block). `from > to` is an anomaly — RPC replication lag (a
/// load-balanced endpoint answering from a stale node) or a reorg — returned as
/// an `Err` so the caller retries via the watcher backoff rather than skipping
/// the backfill (which would permanently reopen the race once the lagging node
/// catches up).
///
/// Shared so the buyer-side bootstrap reconciliation scan
/// ([`crate::buyer_channel`], #763) shares the same range validation.
pub(crate) fn check_backfill_range(from: u64, to: u64) -> Result<()> {
    if from > to {
        anyhow::bail!(
            "backfill range invalid: from_block ({from}) > to_block ({to}); \
             likely RPC replication lag or a reorg — retrying via watcher backoff"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backfill_windows_split_the_range() {
        // Exact multiple: two full windows.
        assert_eq!(
            backfill_windows(0, 19, 10),
            vec![(0, 9), (10, 19)],
            "two contiguous inclusive windows"
        );
        // Remainder: a short final window.
        assert_eq!(
            backfill_windows(0, 25, 10),
            vec![(0, 9), (10, 19), (20, 25)]
        );
        // Single block and single-window-fits cases.
        assert_eq!(backfill_windows(1_000, 1_000, 10), vec![(1_000, 1_000)]);
        assert_eq!(backfill_windows(1_000, 1_005, 10), vec![(1_000, 1_005)]);
        // Non-zero start offset: windows stay aligned to `from`, not to 0.
        assert_eq!(backfill_windows(100, 119, 10), vec![(100, 109), (110, 119)]);
        // Degenerate inputs yield no windows (caller validates separately).
        assert!(backfill_windows(10, 5, 10).is_empty());
        assert!(backfill_windows(0, 10, 0).is_empty());
    }

    #[test]
    fn backfill_windows_cover_a_large_gap_without_overlap_or_gap() {
        // A long downtime gap: every block in [from, to] is covered exactly once
        // and no window exceeds the span.
        let (from, to, span) = (1_000u64, 55_321u64, MAX_BACKFILL_BLOCK_SPAN);
        let windows = backfill_windows(from, to, span);
        let mut expected_next = from;
        for (start, end) in &windows {
            assert_eq!(*start, expected_next, "windows must be contiguous");
            assert!(end >= start, "window end >= start");
            // Window length (end - start + 1) must not exceed the span cap;
            // written as `< span` to avoid clippy's int_plus_one.
            assert!(
                end - start < span,
                "window span {} exceeds cap {span}",
                end - start + 1
            );
            expected_next = end.saturating_add(1);
        }
        assert_eq!(
            windows.last().map(|w| w.1),
            Some(to),
            "the final window must reach `to`"
        );
    }

    #[test]
    fn check_backfill_range_boundary() {
        // Empty range (`from > to`): an RPC-lag / reorg anomaly, not "no blocks
        // elapsed" — returned as a retryable `Err` so the caller retries rather
        // than silently skipping (which would reopen the race).
        assert!(check_backfill_range(1_001, 1_000).is_err());
        // Single block (`from == to`): valid and must be scanned — a
        // ChannelOpened can sit in the exact block bootstrap read the head at.
        assert!(check_backfill_range(1_000, 1_000).is_ok());
        // Normal forward range.
        assert!(check_backfill_range(1_000, 1_005).is_ok());
        // Genesis / zero head is a valid single-block range.
        assert!(check_backfill_range(0, 0).is_ok());
    }
}
