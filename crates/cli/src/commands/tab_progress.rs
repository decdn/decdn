//! The terminal's own progress indicator (OSC 9;4) for `decdn fetch` and
//! `decdn bundle pull`.
//!
//! Terminals that support the sequence (iTerm2, Ghostty, `WezTerm`, Windows
//! Terminal, and others) show a command's percent in the tab bar or dock, as they
//! do for `cargo build`. Support is detected with `anstyle-progress`, the crate
//! Cargo uses; every other terminal, and a stderr that is not a terminal, gets no
//! escape codes.

use std::io::{IsTerminal, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

/// Where [`TabProgress`] writes each OSC 9;4 sequence: stderr in production, a
/// recorder in tests.
type TabSink = Box<dyn Fn(anstyle_progress::TermProgress) + Send + Sync>;

/// Whether stderr is a terminal that shows OSC 9;4 progress.
fn supported() -> bool {
    anstyle_progress::supports_term_progress(std::io::stderr().is_terminal())
}

/// Write one sequence to stderr. indicatif flushes each frame in one buffered
/// write, and this is one `write_all` under the stderr lock, so the two never
/// split each other's escape sequences.
fn write_stderr(p: anstyle_progress::TermProgress) {
    let _ = std::io::stderr().lock().write_all(p.to_string().as_bytes());
}

/// Remove any indicator from the terminal, for an exit that skips destructors.
/// A no-op on a terminal without OSC 9;4.
pub(crate) fn remove_now() {
    if supported() {
        write_stderr(anstyle_progress::TermProgress::remove());
    }
}

/// One command's tab-bar indicator. It only moves forward, and it writes only
/// when the whole-number percent changes, so a run emits at most about a hundred
/// sequences. Dropping it removes the indicator, so an early return never leaves
/// a stale percent in the tab.
pub(crate) struct TabProgress {
    /// The last percent sent, or [`TabProgress::CLEARED`] once removed.
    last: AtomicU8,
    /// The sequence writer.
    sink: TabSink,
}

impl TabProgress {
    /// The `last` value after the indicator is removed; no percent follows it.
    const CLEARED: u8 = u8::MAX;

    /// The indicator on stderr, or `None` when stderr is not a terminal with
    /// OSC 9;4 support. It starts at 0% when `determinate`, else as an
    /// indeterminate (busy) indicator.
    pub(crate) fn detect(determinate: bool) -> Option<Arc<Self>> {
        supported().then(|| Self::start(determinate, Box::new(write_stderr)))
    }

    /// Show the indicator through `sink`: at 0% when `determinate`, else as an
    /// indeterminate (busy) indicator.
    fn start(determinate: bool, sink: TabSink) -> Arc<Self> {
        let start = anstyle_progress::TermProgress::start();
        sink(if determinate { start.percent(0) } else { start });
        Arc::new(Self {
            last: AtomicU8::new(0),
            sink,
        })
    }

    /// Show `position` of `length` as a percent, if that is ahead of the last
    /// percent sent and the indicator is not yet removed.
    pub(crate) fn update(&self, position: u64, length: u64) {
        let pct = (u128::from(position.min(length)) * 100)
            .checked_div(u128::from(length))
            .and_then(|p| u8::try_from(p).ok())
            .unwrap_or(100);
        let advanced = self
            .last
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                (cur != Self::CLEARED && pct > cur).then_some(pct)
            })
            .is_ok();
        if advanced {
            (self.sink)(anstyle_progress::TermProgress::start().percent(pct));
        }
    }

    /// Remove the indicator. Only the first call writes.
    pub(crate) fn clear(&self) {
        if self.last.swap(Self::CLEARED, Ordering::AcqRel) != Self::CLEARED {
            (self.sink)(anstyle_progress::TermProgress::remove());
        }
    }

    /// An indicator whose sequences land in the returned log.
    #[cfg(test)]
    #[allow(clippy::unwrap_used)]
    pub(crate) fn recorded(determinate: bool) -> (Arc<Self>, Arc<std::sync::Mutex<Vec<String>>>) {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink_log = Arc::clone(&log);
        let tab = Self::start(
            determinate,
            Box::new(move |p| sink_log.lock().unwrap().push(p.to_string())),
        );
        (tab, log)
    }
}

impl Drop for TabProgress {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn writes_each_new_percent_once_and_never_backwards() {
        let (tab, log) = TabProgress::recorded(true);
        tab.update(10, 1000); // 1%
        tab.update(19, 1000); // still 1%
        tab.update(500, 1000); // 50%
        tab.update(400, 1000); // behind: a racing thread's stale sample
        tab.update(2000, 1000); // past the end caps at 100%
        assert_eq!(
            *log.lock().unwrap(),
            [
                "\x1b]9;4;1;0\x1b\\",
                "\x1b]9;4;1;1\x1b\\",
                "\x1b]9;4;1;50\x1b\\",
                "\x1b]9;4;1;100\x1b\\",
            ]
        );
    }

    #[test]
    fn a_zero_length_reads_as_done() {
        let (tab, log) = TabProgress::recorded(true);
        tab.update(0, 0);
        assert_eq!(log.lock().unwrap().last().unwrap(), "\x1b]9;4;1;100\x1b\\");
    }

    #[test]
    fn clear_removes_once_and_stops_updates() {
        let (tab, log) = TabProgress::recorded(true);
        tab.clear();
        tab.update(500, 1000);
        drop(tab);
        assert_eq!(
            *log.lock().unwrap(),
            ["\x1b]9;4;1;0\x1b\\", "\x1b]9;4;0;\x1b\\"]
        );
    }

    #[test]
    fn dropping_removes_the_indicator() {
        // An early return drops the indicator without a `clear`; the tab must not
        // keep showing a stale percent.
        let (tab, log) = TabProgress::recorded(false);
        drop(tab);
        assert_eq!(
            *log.lock().unwrap(),
            ["\x1b]9;4;3;\x1b\\", "\x1b]9;4;0;\x1b\\"]
        );
    }
}
