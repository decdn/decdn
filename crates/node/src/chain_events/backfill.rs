//! Windowing + reorg-margin primitives shared by every on-chain watcher.
//!
//! The `multiplexed_poller` loop walks each route's `[cursor, head]` gap in
//! bounded `eth_getLogs` windows and rewinds a shallow reorg margin on resume. Kept here
//! (rather than in any one watcher) so a new consumer picks up the shared windowing
//! instead of forking a fresh one (#1092).
//!
//! Every watcher shares the live-tail windowing. Only the settlement watcher
//! resumes a durable cursor, so only it uses the reorg rewind. The others seed
//! their cursor at a pinned enumeration block (blacklist, slash, staker set) or
//! at head (fee shares) and persist nothing.

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
/// The other watchers seed their tail at an enumeration block or at head and
/// persist nothing, so they have no cursor to rewind; a reorg below that block
/// is corrected by the next boot's enumeration.
pub(crate) const REORG_MARGIN_BLOCKS: u64 = 128;

/// The inclusive end of the `eth_getLogs` window that starts at `start`: at
/// most `span` blocks, clamped to `to`. The poller walks `[from, to]` with it
/// one window at a time, because the span can shrink between windows when the
/// provider rejects a range (see `multiplexed_poller::run_tick`). A `span` of
/// `0` is treated as `1`; the poller never passes it.
pub(crate) fn window_end(start: u64, to: u64, span: u64) -> u64 {
    start.saturating_add(span.max(1) - 1).min(to)
}

/// The span to retry with after the provider rejects the inclusive window
/// `[start, end]`: half its length, rounded down. `None` for a one-block window,
/// which cannot shrink. Computed from `end - start`, so a window that spans all
/// of `u64` cannot overflow.
pub(crate) const fn halved_span(start: u64, end: u64) -> Option<u64> {
    let last = end.saturating_sub(start);
    if last == 0 {
        return None;
    }
    // (last + 1) / 2 without the `+ 1` overflow.
    Some(last / 2 + last % 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walk `[from, to]` the way the poller does with a fixed span.
    fn windows(from: u64, to: u64, span: u64) -> Vec<(u64, u64)> {
        let mut out = Vec::new();
        let mut start = from;
        while start <= to {
            let end = window_end(start, to, span);
            out.push((start, end));
            start = end + 1;
        }
        out
    }

    #[test]
    fn window_end_splits_the_range() {
        // Exact multiple: two full windows.
        assert_eq!(windows(0, 19, 10), vec![(0, 9), (10, 19)]);
        // Remainder: a short final window.
        assert_eq!(windows(0, 25, 10), vec![(0, 9), (10, 19), (20, 25)]);
        // Single block and single-window-fits cases.
        assert_eq!(windows(1_000, 1_000, 10), vec![(1_000, 1_000)]);
        assert_eq!(windows(1_000, 1_005, 10), vec![(1_000, 1_005)]);
        // Non-zero start offset: windows stay aligned to `from`, not to 0.
        assert_eq!(windows(100, 119, 10), vec![(100, 109), (110, 119)]);
        // A zero span is a one-block window, never an empty scan.
        assert_eq!(window_end(7, 100, 0), 7);
        // A span near u64::MAX saturates instead of overflowing.
        assert_eq!(window_end(5, 9, u64::MAX), 9);
    }

    #[test]
    fn halved_span_halves_the_window_length() {
        assert_eq!(halved_span(1, 20), Some(10), "20 blocks -> 10");
        assert_eq!(halved_span(0, 20), Some(10), "21 blocks -> 10");
        assert_eq!(halved_span(5, 7), Some(1), "3 blocks -> 1");
        assert_eq!(halved_span(5, 6), Some(1), "2 blocks -> 1");
        assert_eq!(halved_span(9, 9), None, "a one-block window cannot shrink");
        assert_eq!(
            halved_span(0, u64::MAX),
            Some(1 << 63),
            "the full u64 range does not overflow"
        );
    }

    #[test]
    fn windows_cover_a_large_gap_without_overlap_or_gap() {
        let (from, to, span) = (1_000u64, 55_321u64, 10_000u64);
        let windows = windows(from, to, span);
        let mut expected_next = from;
        for (start, end) in &windows {
            assert_eq!(*start, expected_next, "windows must be contiguous");
            assert!(end >= start, "window end >= start");
            // Written as `< span` to avoid clippy's int_plus_one.
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
