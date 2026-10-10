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
/// block and the next boot: a `PoolOpened` re-mined at a slightly different
/// height after a reorg is still inside the rescanned window.
/// `PoolProjection::record_opened` is absolute and so idempotent, so the only
/// cost of the margin is a few extra blocks of `eth_getLogs`. Sized for the
/// shallow reorgs of an Arbitrum-Sepolia-class L2.
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
mod tests;
