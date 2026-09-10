//! Multi-bar progress for `decdn bundle pull`.
//!
//! A `bundle pull` run fetches many blobs concurrently, so it renders a stack of
//! per-file bars — one per in-flight pull, bounded by `--jobs` — above one bottom
//! **total** bar, all sharing an [`indicatif::MultiProgress`]. A finished file's
//! bar clears, so only active pulls stay on screen. The total bar counts
//! delivered content bytes when every kept entry declared a `size`, and completed
//! files otherwise (a chunk blob carries no size, so a chunked bundle with any
//! unsized entry falls back to the count).
//!
//! The whole renderer is silent — every bar a no-op, every file's delivery
//! callback `None` — when stderr is not a terminal or the run is `--json`, so
//! piped and scripted output is byte-for-byte what it was before per-file bars.

use std::io::IsTerminal;
use std::sync::Arc;
use std::time::Duration;

use decdn_client_pull::ProgressCallback;

use super::fetch;

/// How the bottom total bar measures the run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TotalMode {
    /// Every kept entry declared a `size`: the total counts delivered content
    /// bytes toward the summed size of the **distinct** blobs — a blob pulled
    /// once and materialized to several paths contributes its size once.
    Bytes(u64),
    /// At least one entry declared no `size`: the total counts completed files
    /// (every manifest path, duplicates included) toward this count.
    Count(u64),
}

/// Decide the total-bar mode from the kept entries' `(hash, size)` pairs.
///
/// Byte mode needs a `size` on every entry: one missing size makes a byte total
/// unreliable (chunk blobs carry none), so the run falls back to a files count.
/// The byte denominator dedups by hash, matching the fetch-once grouping — a blob
/// pulled once but written to N paths is one blob's worth of bytes, not N.
pub(crate) fn total_mode<'a>(entries: impl Iterator<Item = (&'a str, Option<u64>)>) -> TotalMode {
    let mut count: u64 = 0;
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut bytes: u64 = 0;
    let mut all_sized = true;
    for (hash, size) in entries {
        count = count.saturating_add(1);
        match size {
            Some(n) if seen.insert(hash) => bytes = bytes.saturating_add(n),
            Some(_) => {}
            None => all_sized = false,
        }
    }
    if all_sized {
        TotalMode::Bytes(bytes)
    } else {
        TotalMode::Count(count)
    }
}

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
/// state) make every method a no-op and every [`FileBar`] silent.
pub(crate) struct PullProgress {
    inner: Option<Inner>,
}

/// The live rendering state, present only when bars are enabled.
struct Inner {
    /// The shared container every bar draws into — the process-global one when
    /// logging is on (so bars and log lines coexist), else a standalone one.
    mp: indicatif::MultiProgress,
    /// The bottom total bar; per-file bars are inserted before it.
    total: indicatif::ProgressBar,
    /// How `total` advances (bytes folded from file deltas, or completed files).
    mode: TotalMode,
}

impl PullProgress {
    /// Build the renderer for a run whose total bar uses `mode`. Returns a
    /// disabled renderer — no bars, no callbacks — when stderr is not a terminal
    /// or the run is `--json`, so non-interactive output is unchanged.
    pub(crate) fn new(mode: TotalMode, json: bool) -> Self {
        if json || !std::io::stderr().is_terminal() {
            return Self::disabled();
        }
        let mp = crate::logging::progress_container();
        let total = indicatif::ProgressBar::new(0);
        let (template, length) = match mode {
            TotalMode::Bytes(n) => (
                "{prefix:.bold} {bytes}/{total_bytes} [{wide_bar:.green}]",
                n,
            ),
            TotalMode::Count(n) => ("{prefix:.bold} {pos}/{len} files [{wide_bar:.green}]", n),
        };
        let style = indicatif::ProgressStyle::with_template(template)
            .unwrap_or_else(|_| indicatif::ProgressStyle::default_bar())
            .progress_chars("=>-");
        total.set_style(style);
        total.set_length(length);
        total.set_prefix("total");
        total.enable_steady_tick(Duration::from_millis(120));
        let total = mp.add(total);
        Self {
            inner: Some(Inner { mp, total, mode }),
        }
    }

    /// A renderer that draws nothing and hands out silent [`FileBar`]s.
    pub(crate) const fn disabled() -> Self {
        Self { inner: None }
    }

    /// Run `f` with the bars cleared for the duration of its stderr write, so a
    /// fail-over notice printed mid-run does not tear the bars. A plain
    /// passthrough when disabled.
    pub(crate) fn suspend<F: FnOnce() -> R, R>(&self, f: F) -> R {
        match &self.inner {
            Some(i) => i.mp.suspend(f),
            None => f(),
        }
    }

    /// A per-file bar labeled `label`, inserted above the total bar.
    /// `content_size`, when known, sets the bar length up front (else it is learned
    /// from the first delivery callback). In byte-total mode the bar's byte deltas
    /// are folded into the total through the callback; in count mode the total
    /// advances on file completion via [`Self::advance_files`] instead.
    ///
    /// When disabled the returned [`FileBar`] is silent and its
    /// [`FileBar::callback`] is `None`, so the fetch path runs byte-bar-free
    /// exactly as it did before per-file bars.
    pub(crate) fn file_bar(&self, label: String, content_size: Option<u64>) -> FileBar {
        let Some(i) = &self.inner else {
            return FileBar::disabled();
        };
        let bar = fetch::labeled_delivery_bar();
        bar.set_prefix(label);
        if let Some(n) = content_size {
            bar.set_length(n);
        }
        let bar = i.mp.insert_before(&i.total, bar);
        // Byte mode: each delivery delta also advances the shared total. Count
        // mode: the total is driven by completed-file increments, not bytes.
        let sink: Option<Arc<dyn Fn(u64) + Send + Sync>> = match i.mode {
            TotalMode::Bytes(_) => {
                let total = i.total.clone();
                Some(Arc::new(move |delta: u64| total.inc(delta)))
            }
            TotalMode::Count(_) => None,
        };
        let (cb, _meter) = fetch::bar_callback(bar.clone(), sink);
        FileBar {
            bar: Some(bar),
            cb: Some(Box::new(cb)),
        }
    }

    /// A chunked file's bar (summed across its chunk pulls), inserted above the
    /// total bar. `size_hint` is the entry's declared whole-file size when the
    /// manifest gave one — it sets the bar length up front; otherwise the length
    /// grows as each chunk's size is learned. Silent when disabled.
    pub(crate) fn chunked_file(&self, label: String, size_hint: Option<u64>) -> ChunkedFile {
        let Some(i) = &self.inner else {
            return ChunkedFile::disabled();
        };
        let bar = fetch::labeled_delivery_bar();
        bar.set_prefix(label);
        if let Some(n) = size_hint {
            bar.set_length(n);
        }
        let bar = i.mp.insert_before(&i.total, bar);
        ChunkedFile {
            bar: Some(bar),
            total: match i.mode {
                TotalMode::Bytes(_) => Some(i.total.clone()),
                TotalMode::Count(_) => None,
            },
            size_known: size_hint.is_some(),
        }
    }

    /// Advance the total by `n` completed files. Only meaningful in count mode;
    /// in byte mode the total already tracks delivered bytes, so this is a no-op.
    pub(crate) fn advance_files(&self, n: u64) {
        if let Some(i) = &self.inner
            && matches!(i.mode, TotalMode::Count(_))
        {
            i.total.inc(n);
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

/// One file's (or fetch-once hash group's) progress bar plus the delivery
/// callback that drives it. Silent when the renderer is disabled.
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
/// sums many chunk pulls, so its callbacks *increment* the bar, and it grows its
/// own length as each chunk's size is learned when the entry declared no
/// whole-file size. Silent when the renderer is disabled.
pub(crate) struct ChunkedFile {
    /// The file's bar; `None` when disabled.
    bar: Option<indicatif::ProgressBar>,
    /// The run total bar to fold *fetched* bytes into (byte mode only); `None` in
    /// count mode or when disabled. A reused chunk never touches this — the file
    /// that fetched it already counted it.
    total: Option<indicatif::ProgressBar>,
    /// Whether the bar's length was set up front (the entry declared a size). When
    /// false the length grows as each chunk's expected size arrives.
    size_known: bool,
}

impl ChunkedFile {
    /// A silent chunked-file bar.
    const fn disabled() -> Self {
        Self {
            bar: None,
            total: None,
            size_known: false,
        }
    }

    /// A fresh delivery callback for ONE chunk pull: it increments this file's bar
    /// (and, in byte mode, the run total) by each byte delta, and — when the file
    /// declared no size — grows the bar's length by the chunk's expected size.
    /// `None` when disabled (the chunk fetch then runs bar-free). Built per chunk,
    /// since each chunk's callback tracks its own cumulative from zero.
    pub(crate) fn chunk_callback(&self) -> Option<Box<ProgressCallback>> {
        let bar = self.bar.clone()?;
        let total = self.total.clone();
        let grow_len = !self.size_known;
        let prev = std::sync::atomic::AtomicU64::new(0);
        let prev_expected = std::sync::atomic::AtomicU64::new(0);
        Some(Box::new(move |received: u64, expected: u64| {
            use std::sync::atomic::Ordering::Relaxed;
            if grow_len {
                let pe = prev_expected.swap(expected, Relaxed);
                bar.inc_length(expected.saturating_sub(pe));
            }
            let previous = prev.swap(received, Relaxed);
            bar.inc(received.saturating_sub(previous));
            if let Some(t) = &total {
                t.inc(received.saturating_sub(previous));
            }
        }))
    }

    /// Advance the file bar by a whole chunk that another file already fetched (so
    /// no callback fired for it here), growing the length too when the file had no
    /// declared size. Never touches the run total — the fetching file counted it.
    pub(crate) fn advance_reused(&self, chunk_size: u64) {
        if let Some(bar) = &self.bar {
            if !self.size_known {
                bar.inc_length(chunk_size);
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_mode_is_bytes_when_every_entry_is_sized() {
        let entries = [("a", Some(10u64)), ("b", Some(20u64))];
        assert_eq!(
            total_mode(entries.iter().map(|(h, s)| (*h, *s))),
            TotalMode::Bytes(30)
        );
    }

    #[test]
    fn total_mode_dedups_bytes_by_hash() {
        // A blob at two paths (same hash) is fetched once — its size counts once.
        let entries = [("a", Some(10u64)), ("a", Some(10u64)), ("b", Some(5u64))];
        assert_eq!(
            total_mode(entries.iter().map(|(h, s)| (*h, *s))),
            TotalMode::Bytes(15)
        );
    }

    #[test]
    fn total_mode_falls_back_to_count_on_any_missing_size() {
        // Count is every path (duplicates included), not distinct hashes.
        let entries = [("a", Some(10u64)), ("b", None), ("a", Some(10u64))];
        assert_eq!(
            total_mode(entries.iter().map(|(h, s)| (*h, *s))),
            TotalMode::Count(3)
        );
    }

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
}
