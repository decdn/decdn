//! Multi-bar progress for `decdn bundle pull`.
//!
//! A `bundle pull` run fetches many blobs concurrently, so it renders a stack of
//! per-file bars — one per in-flight pull, bounded by `--jobs` — above one bottom
//! **total** bar, all sharing an [`indicatif::MultiProgress`]. A finished file's
//! bar clears, so only active pulls stay on screen.
//!
//! Everything is measured in **wire** bytes — bao content plus interleaved proof
//! nodes — because that is the only unit the delivery [`ProgressCallback`] reports
//! (a blob's aligned wire length is known from its signed `StreamResponse`, its
//! content size is not). Each bar's length and position share that one unit, so a
//! bar fills to
//! exactly 100% and never overshoots. The total bar's length is **not** known up
//! front (the manifest carries content sizes, not wire lengths); it grows as each
//! pull's wire length is learned from its first delivered chunk, and its position
//! climbs the same deltas — so it, too, ends at exactly 100%.
//!
//! The whole renderer is silent — every bar a no-op, every file's delivery
//! callback `None` — when stderr is not a terminal or the run is `--json`, so
//! piped and scripted output is byte-for-byte what it was before per-file bars.

use std::io::IsTerminal;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use decdn_client_pull::ProgressCallback;

use super::fetch;

/// The label for a pull's bar: its first destination path, plus a `(+k more)`
/// tail when the same blob lands at more than one path (a fetch-once hash group).
pub(crate) fn file_label(paths: &[String]) -> String {
    match paths {
        [] => "(entry)".to_string(),
        [only] => only.clone(),
        [first, rest @ ..] => format!("{first} (+{} more)", rest.len()),
    }
}

/// The multi-bar renderer for one `bundle pull` run. Disabled variants (no inner
/// state) make every method a no-op and every bar handle silent.
pub(crate) struct PullProgress {
    inner: Option<Inner>,
}

/// The live rendering state, present only when bars are enabled.
struct Inner {
    /// The shared container every bar draws into — the process-global one when
    /// logging is on (so bars and log lines coexist), else a standalone one.
    mp: indicatif::MultiProgress,
    /// The bottom total bar; per-file bars are inserted before it. Its length
    /// starts at zero and grows as each pull's wire size is learned.
    total: indicatif::ProgressBar,
}

impl PullProgress {
    /// Build the renderer. Returns a disabled renderer — no bars, no callbacks —
    /// when stderr is not a terminal or the run is `--json`, so non-interactive
    /// output is unchanged.
    pub(crate) fn new(json: bool) -> Self {
        if json || !std::io::stderr().is_terminal() {
            return Self::disabled();
        }
        let mp = crate::logging::progress_container();
        let total = indicatif::ProgressBar::new(0);
        let style = indicatif::ProgressStyle::with_template(
            "{prefix:.bold} {bytes}/{total_bytes} [{wide_bar:.green}]",
        )
        .unwrap_or_else(|_| indicatif::ProgressStyle::default_bar())
        .progress_chars("=>-");
        total.set_style(style);
        total.set_prefix("total");
        total.enable_steady_tick(Duration::from_millis(120));
        let total = mp.add(total);
        Self {
            inner: Some(Inner { mp, total }),
        }
    }

    /// A renderer that draws nothing and hands out silent bar handles.
    pub(crate) const fn disabled() -> Self {
        Self { inner: None }
    }

    /// A per-file bar for a whole-blob pull, labeled `label` and inserted above the
    /// total bar. `size_estimate` (the manifest's content size, when declared) sets
    /// an initial length so the bar reads sensibly during the pre-byte handshake;
    /// the first delivered chunk replaces it with the authoritative wire length.
    ///
    /// When disabled the returned [`FileBar`] is silent and its
    /// [`FileBar::callback`] is `None`, so the fetch path runs byte-bar-free
    /// exactly as it did before per-file bars.
    pub(crate) fn file_bar(&self, label: String, size_estimate: Option<u64>) -> FileBar {
        let Some(i) = &self.inner else {
            return FileBar::disabled();
        };
        let bar = fetch::labeled_delivery_bar();
        bar.set_prefix(label);
        if let Some(n) = size_estimate {
            bar.set_length(n);
        }
        let bar = i.mp.insert_before(&i.total, bar);
        let (cb, _meter) = fetch::bar_callback(bar.clone(), Some(total_sink(&i.total)));
        FileBar {
            bar: Some(bar),
            cb: Some(Box::new(cb)),
        }
    }

    /// A chunked file's bar (summed across its chunk pulls), inserted above the
    /// total bar. Its length is not preset — a chunked file's whole-file size is a
    /// content size, and the bar is metered in wire bytes — so the length grows as
    /// each chunk's wire size is learned. Silent when disabled.
    pub(crate) fn chunked_file(&self, label: String) -> ChunkedFile {
        let Some(i) = &self.inner else {
            return ChunkedFile::disabled();
        };
        let bar = fetch::labeled_delivery_bar();
        bar.set_prefix(label);
        let bar = i.mp.insert_before(&i.total, bar);
        ChunkedFile {
            bar: Some(bar),
            total: Some(total_sink(&i.total)),
        }
    }

    /// Clear the total bar at the end of the run; the command then prints its own
    /// summary line. A no-op when disabled.
    pub(crate) fn finish(&self) {
        if let Some(i) = &self.inner {
            i.total.finish_and_clear();
        }
    }
}

/// One file's (or fetch-once hash group's) progress bar plus the delivery callback
/// that drives it. Silent when the renderer is disabled.
pub(crate) struct FileBar {
    /// The bar to clear when the pull settles; `None` when disabled.
    bar: Option<indicatif::ProgressBar>,
    /// The delivery callback handed to the fetch path; `None` when disabled.
    cb: Option<Box<ProgressCallback>>,
}

impl FileBar {
    /// A silent bar: no rendering, no callback.
    fn disabled() -> Self {
        Self {
            bar: None,
            cb: None,
        }
    }

    /// The delivery callback to hand the fetch path, or `None` when disabled (the
    /// fetch path then runs byte-bar-free, as before per-file bars).
    pub(crate) fn callback(&self) -> Option<&ProgressCallback> {
        self.cb.as_deref()
    }

    /// Clear the bar once the file's pull settles (success or failure).
    pub(crate) fn finish(self) {
        if let Some(bar) = self.bar {
            bar.finish_and_clear();
        }
    }
}

/// A chunked file's bar, advanced across its several chunk pulls. Unlike a
/// whole-file [`FileBar`] — one pull, one `set_position` callback — a chunked file
/// sums many chunk pulls, so its callbacks *increment* the bar and grow its length
/// as each chunk's wire size is learned. Silent when the renderer is disabled.
pub(crate) struct ChunkedFile {
    /// The file's bar; `None` when disabled.
    bar: Option<indicatif::ProgressBar>,
    /// The fold into the run total bar (wire bytes); `None` when disabled. Only a
    /// *fetched* chunk folds into it — a reused chunk was already counted by the
    /// file that fetched it.
    total: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
}

impl ChunkedFile {
    /// A silent chunked-file bar.
    fn disabled() -> Self {
        Self {
            bar: None,
            total: None,
        }
    }

    /// A fresh delivery callback for ONE chunk pull: it grows this file's bar
    /// length by the chunk's expected wire size and advances its position by each
    /// received-wire delta, and folds the same deltas into the run total. `None`
    /// when disabled (the chunk fetch then runs bar-free). Built per chunk, since
    /// each chunk's callback tracks its own cumulative from zero.
    pub(crate) fn chunk_callback(&self) -> Option<Box<ProgressCallback>> {
        let bar = self.bar.clone()?;
        let total = self.total.clone();
        let prev = AtomicU64::new(0);
        let prev_expected = AtomicU64::new(0);
        Some(Box::new(move |received: u64, expected: u64| {
            let received_delta = received.saturating_sub(prev.swap(received, Ordering::Relaxed));
            let expected_delta =
                expected.saturating_sub(prev_expected.swap(expected, Ordering::Relaxed));
            bar.inc_length(expected_delta);
            bar.inc(received_delta);
            if let Some(sink) = &total {
                sink(received_delta, expected_delta);
            }
        }))
    }

    /// Advance the file bar by a whole chunk that another file already fetched (so
    /// no callback fired for it here), growing the length by the same amount so the
    /// chunk reads as complete. `chunk_size` is the staged content size — the file
    /// bar is internally consistent because it grows length and position together —
    /// and the run total is left untouched, since the fetching file already counted
    /// this chunk's wire bytes there.
    pub(crate) fn advance_reused(&self, chunk_size: u64) {
        if let Some(bar) = &self.bar {
            bar.inc_length(chunk_size);
            bar.inc(chunk_size);
        }
    }

    /// Clear the bar once the file's chunks are fetched and it is assembled.
    pub(crate) fn finish(self) {
        if let Some(bar) = self.bar {
            bar.finish_and_clear();
        }
    }
}

/// The fold that grows the `total` bar: add each pull's `expected_delta` to its
/// length and `received_delta` to its position, both in wire bytes, so the total
/// stays wire-consistent and ends at exactly 100%.
fn total_sink(total: &indicatif::ProgressBar) -> Arc<dyn Fn(u64, u64) + Send + Sync> {
    let total = total.clone();
    Arc::new(move |received_delta: u64, expected_delta: u64| {
        total.inc_length(expected_delta);
        total.inc(received_delta);
    })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    #[test]
    fn file_label_single_path_is_the_path() {
        assert_eq!(file_label(&["models/a.bin".to_string()]), "models/a.bin");
    }

    #[test]
    fn file_label_multi_path_appends_more_count() {
        let paths = [
            "models/a.bin".to_string(),
            "b.bin".to_string(),
            "c.bin".to_string(),
        ];
        assert_eq!(file_label(&paths), "models/a.bin (+2 more)");
    }

    #[test]
    fn file_label_empty_is_placeholder() {
        assert_eq!(file_label(&[]), "(entry)");
    }

    // A hidden but length-bounded bar (as production builds via `ProgressBar::new(0)`)
    // tracks position/length, so the wire-byte accounting is testable without a
    // terminal. `ProgressBar::hidden()` starts unbounded (length `None`), where
    // `inc_length` is a no-op — so tests must start from `Some(0)`.
    fn test_bar() -> indicatif::ProgressBar {
        indicatif::ProgressBar::with_draw_target(Some(0), indicatif::ProgressDrawTarget::hidden())
    }

    #[test]
    fn total_sink_grows_length_by_expected_and_position_by_received() {
        let total = test_bar();
        let sink = total_sink(&total);
        // First update learns the pull's full wire length; later updates only move
        // the position.
        sink(0, 100);
        assert_eq!(total.length(), Some(100));
        assert_eq!(total.position(), 0);
        sink(40, 0);
        sink(60, 0);
        // Ends at exactly 100% — position caught up to length, no overshoot.
        assert_eq!(total.position(), 100);
        assert_eq!(total.length(), Some(100));
    }

    #[test]
    fn chunked_file_sums_chunks_and_folds_into_total() {
        let total_bar = test_bar();
        let cf = ChunkedFile {
            bar: Some(test_bar()),
            total: Some(total_sink(&total_bar)),
        };

        // Chunk 1: 50 wire bytes, delivered in two updates.
        let cb1 = cf.chunk_callback().expect("enabled -> Some callback");
        cb1(0, 50);
        cb1(30, 50);
        cb1(50, 50);
        // Chunk 2: 40 wire bytes. A fresh callback tracks its own cumulative.
        let cb2 = cf.chunk_callback().expect("enabled -> Some callback");
        cb2(0, 40);
        cb2(40, 40);

        let bar = cf.bar.as_ref().expect("bar present");
        assert_eq!(bar.length(), Some(90));
        assert_eq!(bar.position(), 90);
        // The total folds the same wire deltas.
        assert_eq!(total_bar.length(), Some(90));
        assert_eq!(total_bar.position(), 90);
    }

    #[test]
    fn advance_reused_completes_file_bar_without_touching_total() {
        let total_bar = test_bar();
        let cf = ChunkedFile {
            bar: Some(test_bar()),
            total: Some(total_sink(&total_bar)),
        };

        // A chunk another file already fetched: length and position grow together
        // so it reads as complete, and the total is untouched (already counted).
        cf.advance_reused(25);

        let bar = cf.bar.as_ref().expect("bar present");
        assert_eq!(bar.length(), Some(25));
        assert_eq!(bar.position(), 25);
        assert_eq!(total_bar.position(), 0);
        assert_eq!(total_bar.length(), Some(0));
    }

    #[test]
    fn disabled_chunked_file_has_no_callback() {
        let cf = ChunkedFile::disabled();
        assert!(cf.chunk_callback().is_none());
        // A silent advance is a no-op, not a panic.
        cf.advance_reused(10);
    }

    #[test]
    fn disabled_file_bar_has_no_callback() {
        let fb = FileBar::disabled();
        assert!(fb.callback().is_none());
    }
}
