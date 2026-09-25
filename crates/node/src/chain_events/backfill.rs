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

/// Accepted span-limited windows in a row before [`WindowSpan`] doubles its
/// span. Against a provider whose real cap sits between two spans, the poller
/// then pays about one rejected request per this many accepted ones.
pub(crate) const SPAN_REGROW_AFTER_WINDOWS: u32 = 32;

/// The block span of the poller's next `eth_getLogs` window, learned from the
/// provider. It starts at the configured ceiling. A rejected window sets it to
/// half that window's length ([`halved_span`]). After
/// [`SPAN_REGROW_AFTER_WINDOWS`] accepted windows in a row that used the whole
/// span, it doubles, up to the ceiling. So a transient or misclassified
/// rejection costs throughput for a while, not until restart. A window shorter
/// than the span (the live tail) does not count toward the regrow: it does not
/// test the span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WindowSpan {
    current: u64,
    ceiling: u64,
    /// The smallest span reached so far, so a caller can tell a new low from
    /// the shrink-regrow cycle against a provider cap.
    low: u64,
    streak: u32,
}

impl WindowSpan {
    /// Start at `ceiling` (at least one block).
    pub(crate) const fn new(ceiling: u64) -> Self {
        let ceiling = if ceiling == 0 { 1 } else { ceiling };
        Self {
            current: ceiling,
            ceiling,
            low: ceiling,
            streak: 0,
        }
    }

    /// The span of the next window.
    pub(crate) const fn current(&self) -> u64 {
        self.current
    }

    /// Shrink after the provider rejected the window `[start, end]`. Returns
    /// the new span and whether it is a new low, or `None` when the window is
    /// one block and cannot shrink (the span is then unchanged).
    pub(crate) fn shrink(&mut self, start: u64, end: u64) -> Option<(u64, bool)> {
        let span = halved_span(start, end)?;
        self.current = span;
        self.streak = 0;
        let new_low = span < self.low;
        if new_low {
            self.low = span;
        }
        Some((span, new_low))
    }

    /// Record the accepted window `[start, end]`. Returns the new span when
    /// this window completes a regrow streak.
    pub(crate) fn record_success(&mut self, start: u64, end: u64) -> Option<u64> {
        let window = end.saturating_sub(start).saturating_add(1);
        if window < self.current || self.current >= self.ceiling {
            return None;
        }
        self.streak = self.streak.saturating_add(1);
        if self.streak < SPAN_REGROW_AFTER_WINDOWS {
            return None;
        }
        self.streak = 0;
        self.current = self.current.saturating_mul(2).min(self.ceiling);
        Some(self.current)
    }
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

    /// Accept `n` span-limited windows starting at block 0.
    fn accept_full_windows(span: &mut WindowSpan, n: u32) -> Option<u64> {
        let mut regrown = None;
        let mut start = 0;
        for _ in 0..n {
            let end = start + span.current() - 1;
            if let Some(new) = span.record_success(start, end) {
                regrown = Some(new);
            }
            start = end + 1;
        }
        regrown
    }

    #[test]
    fn window_span_shrinks_to_half_the_rejected_window() {
        let mut span = WindowSpan::new(10_000);
        // A 30-block tail window is rejected: 15, not 5 000.
        assert_eq!(span.shrink(100, 129), Some((15, true)));
        assert_eq!(span.current(), 15);
        // A one-block window cannot shrink and leaves the span alone.
        assert_eq!(span.shrink(7, 7), None);
        assert_eq!(span.current(), 15);
    }

    #[test]
    fn window_span_regrows_after_a_streak_up_to_the_ceiling() {
        let mut span = WindowSpan::new(100);
        assert_eq!(span.shrink(0, 99), Some((50, true)));
        assert_eq!(
            accept_full_windows(&mut span, SPAN_REGROW_AFTER_WINDOWS - 1),
            None
        );
        assert_eq!(accept_full_windows(&mut span, 1), Some(100));
        // At the ceiling it never grows further.
        assert_eq!(
            accept_full_windows(&mut span, SPAN_REGROW_AFTER_WINDOWS * 2),
            None
        );
        assert_eq!(span.current(), 100);
    }

    #[test]
    fn window_span_regrow_counts_only_span_limited_windows() {
        let mut span = WindowSpan::new(1_000);
        assert_eq!(span.shrink(0, 99), Some((50, true)));
        // Live-tail windows shorter than the span do not test it.
        for i in 0..u64::from(SPAN_REGROW_AFTER_WINDOWS) * 4 {
            assert_eq!(span.record_success(i * 10, i * 10 + 9), None);
        }
        assert_eq!(span.current(), 50);
    }

    #[test]
    fn window_span_rejection_resets_the_streak_and_repeat_lows_are_not_new() {
        let mut span = WindowSpan::new(200);
        assert_eq!(span.shrink(0, 199), Some((100, true)));
        assert_eq!(
            accept_full_windows(&mut span, SPAN_REGROW_AFTER_WINDOWS - 1),
            None
        );
        // Rejected again at the same span: back to 50 (a new low), streak reset.
        assert_eq!(span.shrink(0, 99), Some((50, true)));
        assert_eq!(
            accept_full_windows(&mut span, SPAN_REGROW_AFTER_WINDOWS - 1),
            None
        );
        assert_eq!(accept_full_windows(&mut span, 1), Some(100));
        // The regrown span is rejected: 50 again, which is not a new low.
        assert_eq!(span.shrink(0, 99), Some((50, false)));
    }

    #[test]
    fn window_span_treats_a_zero_ceiling_as_one_block() {
        assert_eq!(WindowSpan::new(0).current(), 1);
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
