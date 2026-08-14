//! Windowing + reorg-margin primitives shared by every on-chain watcher.
//!
//! `resumable_watcher::run` walks a `[cursor, head]` gap in bounded
//! `eth_getLogs` windows and rewinds a shallow reorg margin on resume. Kept here
//! (rather than in any one watcher) so a new consumer picks up the shared span
//! instead of forking a fresh one (#1092).
//!
//! This provides the live-tail windowing every watcher shares, plus the
//! durable-cursor rewind that only the settlement watcher resumes from. The
//! genesis-replay watchers (origin directory, blacklist, buyer-reconcile)
//! enumerate their state from a contract view at a pinned block and use neither.

/// How many blocks the persisted scan checkpoint is rewound before the
/// resume backfill (#751), absorbing a shallow reorg between the last scanned
/// block and the next boot: a `ChannelOpened` re-mined at a slightly different
/// height after a reorg is still inside the rescanned window.
/// `register_open_channel` is idempotent, so the only cost of the margin is a
/// few extra blocks of `eth_getLogs`. Sized for the shallow reorgs of an
/// Arbitrum-Sepolia-class L2.
///
/// The rewind applies only where a *durable* cursor is resumed — a
/// `HeadMinusWindow` start re-derives its floor from head on every boot, so
/// there is nothing to rewind.
///
/// One live consumer: the settlement watcher ([`crate::payment_settlement`], #751),
/// through [`super::resumable_watcher::CursorStart::FromCheckpoint`]'s `reorg_margin`.
/// The origin directory seeds its tail at the enumeration block and persists
/// nothing, so it has no cursor to rewind; a reorg below that block is corrected
/// by the next boot's enumeration.
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
/// `from > to` range yields no windows (the caller validates that separately).
/// Unit-tested for the window math.
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
}
