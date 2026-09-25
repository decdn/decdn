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

use std::num::NonZeroU64;

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

/// Accepted windows since the last shrink or regrow before [`WindowSpan`]
/// doubles its span. Against a provider whose real cap sits below the span,
/// the poller then pays about one rejected request per this many accepted
/// windows.
pub(crate) const SPAN_REGROW_AFTER_WINDOWS: u32 = 32;

/// The block span of the poller's next `eth_getLogs` window, learned from the
/// provider. It starts at the configured ceiling. A rejected window sets it to
/// half that window's length ([`halved_span`]), never above the current span.
/// After [`SPAN_REGROW_AFTER_WINDOWS`] accepted windows it doubles, up to the
/// ceiling. Every accepted window counts, the short live-tail ones too: a
/// caught-up node therefore climbs back to the ceiling after a transient or
/// misclassified rejection, and a real cap shows up as the next backfill's
/// rejections instead of as a span that stays low for good.
#[derive(Debug, Clone)]
pub(crate) struct WindowSpan {
    current: u64,
    ceiling: u64,
    /// The span before the latest regrow, while no shrink has followed it: a
    /// shrink back to this level is the regrow's probe being rejected.
    probing_from: Option<u64>,
    streak: u32,
}

/// What a [`WindowSpan::shrink`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Shrunk {
    /// The new span.
    pub(crate) span: u64,
    /// The shrink undid the latest regrow: the provider rejected the doubled
    /// span and the span is back at (or above) where the regrow started. A
    /// shrink that is not a rejected probe is news about the provider.
    pub(crate) rejected_probe: bool,
}

impl WindowSpan {
    /// Start at `ceiling`.
    pub(crate) const fn new(ceiling: NonZeroU64) -> Self {
        Self {
            current: ceiling.get(),
            ceiling: ceiling.get(),
            probing_from: None,
            streak: 0,
        }
    }

    /// The span of the next window, always `>= 1`.
    pub(crate) const fn current(&self) -> u64 {
        self.current
    }

    /// Shrink after the provider rejected the window `[start, end]`, to half
    /// the window's length and never above the current span. `None` when the
    /// window is one block and cannot shrink (the span is then unchanged).
    #[must_use]
    pub(crate) fn shrink(&mut self, start: u64, end: u64) -> Option<Shrunk> {
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
    pub(crate) fn record_success(&mut self) -> Option<u64> {
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

    fn span(ceiling: u64) -> WindowSpan {
        WindowSpan::new(NonZeroU64::new(ceiling).unwrap_or(NonZeroU64::MIN))
    }

    /// Record `n` accepted windows; the last regrow, if any.
    fn accept(span: &mut WindowSpan, n: u32) -> Option<u64> {
        (0..n).filter_map(|_| span.record_success()).last()
    }

    #[test]
    fn window_span_shrinks_to_half_the_rejected_window() {
        let mut span = span(10_000);
        // A 30-block tail window is rejected: 15, not 5 000.
        assert_eq!(
            span.shrink(100, 129),
            Some(Shrunk {
                span: 15,
                rejected_probe: false
            })
        );
        assert_eq!(span.current(), 15);
        // A one-block window cannot shrink and leaves the span alone.
        assert_eq!(span.shrink(7, 7), None);
        assert_eq!(span.current(), 15);
    }

    #[test]
    fn window_span_never_shrinks_above_the_current_span() {
        let mut span = span(100);
        assert!(span.shrink(0, 19).is_some(), "20 blocks -> 10");
        // A caller passing a window wider than the span cannot grow it.
        assert_eq!(span.shrink(0, 99).map(|s| s.span), Some(10));
        assert_eq!(span.current(), 10);
    }

    #[test]
    fn window_span_regrows_after_a_streak_up_to_the_ceiling() {
        let mut span = span(100);
        assert!(span.shrink(0, 99).is_some(), "100 blocks -> 50");
        assert_eq!(accept(&mut span, SPAN_REGROW_AFTER_WINDOWS - 1), None);
        assert_eq!(accept(&mut span, 1), Some(100));
        // At the ceiling it never grows further.
        assert_eq!(accept(&mut span, SPAN_REGROW_AFTER_WINDOWS * 2), None);
        assert_eq!(span.current(), 100);
    }

    #[test]
    fn window_span_climbs_back_to_the_ceiling_on_short_windows() {
        // A transient rejection of a 30-block live-tail window drops the span to
        // 15; short accepted windows keep counting, so it climbs all the way back.
        let mut span = span(10_000);
        assert!(span.shrink(0, 29).is_some());
        let mut last = None;
        for _ in 0..20 {
            last = accept(&mut span, SPAN_REGROW_AFTER_WINDOWS).or(last);
        }
        assert_eq!(last, Some(10_000));
        assert_eq!(span.current(), 10_000);
    }

    #[test]
    fn window_span_flags_a_rejected_regrow_probe() {
        let mut span = span(200);
        let first = span.shrink(0, 199);
        assert_eq!(
            first.map(|s| (s.span, s.rejected_probe)),
            Some((100, false))
        );
        assert_eq!(accept(&mut span, SPAN_REGROW_AFTER_WINDOWS), Some(200));
        // The doubled span is rejected: back to 100, a rejected probe.
        let probe = span.shrink(0, 199);
        assert_eq!(probe.map(|s| (s.span, s.rejected_probe)), Some((100, true)));
        // A further shrink below the probe's start is news again.
        let deeper = span.shrink(0, 99);
        assert_eq!(
            deeper.map(|s| (s.span, s.rejected_probe)),
            Some((50, false))
        );
    }

    #[test]
    fn window_span_rejection_resets_the_streak() {
        let mut span = span(200);
        assert!(span.shrink(0, 199).is_some());
        assert_eq!(accept(&mut span, SPAN_REGROW_AFTER_WINDOWS - 1), None);
        assert!(span.shrink(0, 99).is_some(), "100 -> 50, streak reset");
        assert_eq!(accept(&mut span, SPAN_REGROW_AFTER_WINDOWS - 1), None);
        assert_eq!(accept(&mut span, 1), Some(100));
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
