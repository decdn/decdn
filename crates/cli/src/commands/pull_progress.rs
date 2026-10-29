//! Multi-bar progress for a bundle `decdn get`.
//!
//! A bundle pull fetches many blobs concurrently, so it renders a stack of
//! per-file bars — one per pull that holds a `--jobs` slot, plus any that wait
//! for a sibling's donor chunks — above one bottom **total** bar, all sharing
//! an [`indicatif::MultiProgress`], under a one-line
//! header naming the run and its download vs on-disk sizes. A finished file's bar
//! clears, so only active pulls stay on screen. A blank **pad** bar under the
//! total gains a row for each cleared file bar and gives one back for each new
//! one, so the stack never shrinks: the container redraws in place, and an
//! in-place redraw never erases the rows a shrunk stack leaves below it.
//!
//! The **total** bar meters the run's **download** — the bytes actually fetched
//! after shared-chunk dedup (`download_bytes`), fixed up front as its denominator so
//! it reads "downloaded of the whole download" from the start. Only delivered bytes
//! fold into it (each file's delivery callback, capped at that file's download
//! size); bytes spliced from disk are free and never inflate it, so its rate and ETA
//! describe real transfer, not reconstruction. The header shows the download total
//! beside the whole on-disk content size (`total_content_bytes`), so the dedup
//! saving is visible.
//!
//! A **per-file** bar reads `downloaded/download-total (reconstructed)` plus a phase
//! word. During the download it tracks the pull's delivered content against the
//! blob's download size; when the download is done but a deferred chunk is still
//! being fetched by a sibling it shows `pending siblings…`; during the whole-file
//! verify it shows `reconstructing…` and fills with bytes hashed, so a multi-GB
//! reconstruction never sits as a frozen row; before the first byte it shows
//! `discovering…`. A per-file bar carries no rate/ETA: with `--jobs` pulls sharing
//! one link, a single file's rate is only its share of the bandwidth and reads as
//! stuck whenever another file has the bandwidth, while the total bar keeps moving.
//!
//! The total bar and header are shown only when the manifest declares sizes. A blob
//! with no declared size contributes nothing to the denominator. If no kept entry
//! (one that survives the include/exclude filters) declares a size, the total bar is
//! omitted and only per-file bars render.
//!
//! In a terminal that supports the OSC 9;4 progress sequence (iTerm2, Ghostty,
//! `WezTerm`, Windows Terminal, and others; detected by `anstyle-progress`, as Cargo
//! does), the total bar's percent also drives the terminal's own progress
//! indicator in the tab bar or dock. A run with no total bar shows it as
//! indeterminate. The indicator is removed when the run finishes or the renderer
//! drops.
//!
//! The whole renderer is silent — every bar a no-op, every file's delivery
//! callback `None` — when stderr is not a terminal or the run is `--json`, so
//! piped and scripted output carries no progress-bar bytes.

use std::borrow::Cow;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use decdn_client::ProgressCallback;

use super::fetch;
use super::tab_progress::TabProgress;

/// The label for a pull's bar: its first destination path, plus a `(+k more)`
/// tail when the same blob lands at more than one path (a fetch-once hash group).
pub(crate) fn file_label(paths: &[String]) -> String {
    match paths {
        [] => "(entry)".to_string(),
        [only] => terminal_safe(only).into_owned(),
        [first, rest @ ..] => format!("{} (+{} more)", terminal_safe(first), rest.len()),
    }
}

/// `path` with each control character escaped (`\u{1b}`, `\n`), so a path a
/// publisher wrote cannot drive the terminal that prints it. Every other
/// character, quotes and non-ASCII included, prints as it is.
pub(crate) fn terminal_safe(path: &str) -> Cow<'_, str> {
    if !path.chars().any(char::is_control) {
        return Cow::Borrowed(path);
    }
    Cow::Owned(
        path.chars()
            .map(|c| {
                if c.is_control() {
                    c.escape_default().to_string()
                } else {
                    c.to_string()
                }
            })
            .collect(),
    )
}

/// Steady-tick cadence shared by every bar.
///
/// Every bar here is born inside the [`indicatif::MultiProgress`] — `mp.add(..)`
/// or `mp.insert_before(.., ProgressBar::new(..))` as one call — and is styled,
/// labeled, sized, and ticked only afterwards. A `ProgressBar` outside a container
/// draws itself straight to stderr on `set_prefix`, `set_length`, and every tick;
/// the container has no record of that orphan line, so each later redraw moves the
/// cursor up too few rows and the stale line scrolls into history instead of being
/// overwritten. Nothing touches a bar before the container owns it.
const TICK: Duration = Duration::from_millis(120);

/// A bar of length `len` that clears its line when dropped unfinished, as the
/// in-flight bars are when a Ctrl-C stops the run. Setting the finish mode draws
/// nothing, so the bar is still untouched when the container takes it (see
/// [`TICK`]).
fn new_bar(len: u64) -> indicatif::ProgressBar {
    indicatif::ProgressBar::new(len).with_finish(indicatif::ProgressFinish::AndClear)
}

/// The multi-bar renderer for one bundle pull. Disabled variants (no inner
/// state) make every method a no-op and every bar handle silent.
pub(crate) struct PullProgress {
    inner: Option<Inner>,
}

/// The live rendering state, present only when bars are enabled.
struct Inner {
    /// The shared container every bar draws into — the process-global one when
    /// logging is on (so bars and log lines coexist), else a standalone one.
    mp: indicatif::MultiProgress,
    /// The bottom total bar, in download bytes with a fixed denominator, carrying
    /// the run's rate/ETA; per-file bars insert before it. `None` when the manifest
    /// declares no sizes, in which case only per-file bars render.
    total: Option<TotalBar>,
    /// The blank bar under every other bar that holds the stack's height.
    pad: Arc<PadBar>,
    /// The terminal's tab-bar indicator; `None` when the terminal lacks OSC 9;4.
    tab: Option<Arc<TabProgress>>,
}

/// The blank bottom bar that keeps the stack from shrinking. It renders one
/// blank line plus one per row a cleared file bar freed, and a new file bar
/// takes a freed row back. Its blank lines are written full width, so they
/// overwrite whatever a taller stack left in those rows.
struct PadBar {
    /// The bar, styled to render only its `{msg}` of newlines.
    bar: indicatif::ProgressBar,
    /// The rows freed and not yet taken back. The lock also orders the
    /// re-renders, so the last render shows the last count.
    rows: Mutex<usize>,
}

impl PadBar {
    /// Add the pad at the bottom of `mp` (see [`TICK`] for why the add comes
    /// first).
    fn add(mp: &indicatif::MultiProgress) -> Arc<Self> {
        let bar = mp.add(new_bar(1));
        bar.set_style(
            indicatif::ProgressStyle::with_template("{msg}")
                .unwrap_or_else(|_| indicatif::ProgressStyle::default_bar()),
        );
        Arc::new(Self {
            bar,
            rows: Mutex::new(0),
        })
    }

    /// Hold the row of a file bar about to clear. Called before the clear, so
    /// no frame draws the shorter stack.
    fn free_row(&self) {
        self.set_rows(|rows| rows + 1);
    }

    /// Give a freed row to a file bar just inserted. Called after the insert,
    /// so no frame draws the shorter stack.
    fn take_row(&self) {
        self.set_rows(|rows| rows.saturating_sub(1));
    }

    fn set_rows(&self, f: impl FnOnce(usize) -> usize) {
        if let Ok(mut rows) = self.rows.lock() {
            *rows = f(*rows);
            self.bar.set_message("\n".repeat(*rows));
        }
    }
}

/// A file bar's claim on one row of the stack. Releasing it, explicitly when
/// the file finishes or on drop when the pull stops early, hands the row to
/// the pad before the file bar clears.
struct PadRow {
    pad: Arc<PadBar>,
    held: AtomicBool,
}

impl PadRow {
    fn release(&self) {
        if self.held.swap(false, Ordering::Relaxed) {
            self.pad.free_row();
        }
    }
}

impl Drop for PadRow {
    fn drop(&mut self) {
        self.release();
    }
}

impl Drop for Inner {
    /// Remove the tab-bar indicator when the renderer drops, even if a per-file
    /// callback still holds a clone of it: an early return must not leave a
    /// stale percent in the tab, and a late update after the clear writes
    /// nothing.
    fn drop(&mut self) {
        if let Some(tab) = &self.tab {
            tab.clear();
        }
    }
}

/// The bottom total bar plus the rate meter behind its `{msg}`. Every downloaded
/// byte folded into the total passes through [`inc`](Self::inc), which samples the
/// meter; bytes spliced from disk never reach it, so its rate is a transfer rate.
/// Cloning shares the same bar and meter.
#[derive(Clone)]
pub(crate) struct TotalBar {
    /// The `indicatif` bar, in download bytes with a fixed length.
    bar: indicatif::ProgressBar,
    /// The whole-download rate estimate, sampled on every transferred increment.
    speed: Arc<Mutex<fetch::SpeedState>>,
    /// The terminal's tab-bar indicator, fed the bar's percent on every increment.
    tab: Option<Arc<TabProgress>>,
}

impl TotalBar {
    /// The total bar's style: byte counts, the rate/ETA `{msg}`, and a green bar.
    fn style() -> indicatif::ProgressStyle {
        indicatif::ProgressStyle::with_template(
            // Rate/ETA come from `{msg}` (see `refresh_rate`), not the built-in
            // `{bytes_per_sec}`/`{eta}` — those swing wildly on bursty arrival.
            "{prefix:.bold} {bytes}/{total_bytes} {msg}[{wide_bar:.green}]",
        )
        .unwrap_or_else(|_| indicatif::ProgressStyle::default_bar())
        .progress_chars("=>-")
    }

    /// Wrap an existing bar with a fresh meter, mirroring its percent into `tab`.
    fn wrap(bar: indicatif::ProgressBar, tab: Option<Arc<TabProgress>>) -> Self {
        Self {
            bar,
            speed: Arc::new(Mutex::new(fetch::SpeedState::default())),
            tab,
        }
    }

    /// Advance the total by `bytes` of transferred content, refresh the
    /// rate/ETA from the new position, and update the tab-bar percent.
    fn inc(&self, bytes: u64) {
        self.bar.inc(bytes);
        self.refresh_rate();
        if let Some(tab) = &self.tab {
            let position = self.bar.position();
            tab.update(position, self.bar.length().unwrap_or(position));
        }
    }

    /// Sample the meter at the bar's current position and rewrite the `{msg}` as
    /// `(rate, ETA) `. A poisoned lock only costs this one refresh.
    fn refresh_rate(&self) {
        let position = self.bar.position();
        if let Ok(mut s) = self.speed.lock() {
            let bps = s.observe(Instant::now(), position);
            let remaining = self
                .bar
                .length()
                .unwrap_or(position)
                .saturating_sub(position);
            self.bar.set_message(format!(
                "({}, {}) ",
                fetch::fmt_rate(bps),
                fetch::fmt_eta(remaining, bps)
            ));
        }
    }

    /// Clear the bar at the end of the run.
    fn finish_and_clear(&self) {
        self.bar.finish_and_clear();
    }

    /// The bar's content-byte position.
    #[cfg(test)]
    fn position(&self) -> u64 {
        self.bar.position()
    }
}

impl PullProgress {
    /// Build the renderer. Returns a disabled renderer — no bars, no callbacks —
    /// when stderr is not a terminal or the run is `--json`, so non-interactive
    /// output is unchanged.
    ///
    /// `download_total` is the run's **download** size — the bytes actually fetched
    /// after shared-chunk dedup (`download_bytes`) and the fixed total-bar
    /// denominator. `content_total` is the whole on-disk size (`total_content_bytes`),
    /// shown alongside in the header so the dedup saving is visible. `label` names
    /// the run (the output directory). `download_total` `None`/0 (the manifest
    /// declared no sizes) omits the total bar and renders only per-file bars.
    pub(crate) fn new(
        json: bool,
        download_total: Option<u64>,
        content_total: Option<u64>,
        label: &str,
    ) -> Self {
        if json || !std::io::stderr().is_terminal() {
            return Self::disabled();
        }
        let mp = crate::logging::progress_container();
        // Header: what the run downloads vs what lands on disk after dedup. Printed
        // once above the live bars; only when a download size is known and dedup
        // actually saves bytes is the "on disk" figure worth showing. It goes out
        // through `suspend` with its own newline: the container redraws in place
        // (`logging::new_container`), and an in-place redraw returns to the start
        // of a `println` line that has no bar under it yet and overwrites it.
        if let Some(dl) = download_total.filter(|n| *n > 0) {
            let header = match content_total.filter(|c| *c > dl) {
                Some(content) => format!(
                    "{label} · {} to download · {} on disk",
                    indicatif::HumanBytes(dl),
                    indicatif::HumanBytes(content),
                ),
                None => format!("{label} · {} to download", indicatif::HumanBytes(dl)),
            };
            mp.suspend(|| eprintln!("{header}"));
        }
        let download_total = download_total.filter(|n| *n > 0);
        let tab = TabProgress::detect(download_total.is_some());
        let total = download_total.map(|len| Self::add_total_bar(&mp, len, tab.clone()));
        let pad = PadBar::add(&mp);
        Self {
            inner: Some(Inner {
                mp,
                total,
                pad,
                tab,
            }),
        }
    }

    /// Build the bottom total bar with a fixed content-byte length inside the
    /// container, then style, label, and tick it (see [`TICK`] for why the add
    /// comes first).
    fn add_total_bar(
        mp: &indicatif::MultiProgress,
        len: u64,
        tab: Option<Arc<TabProgress>>,
    ) -> TotalBar {
        let bar = mp.add(new_bar(len));
        bar.set_style(TotalBar::style());
        bar.set_prefix("total");
        bar.enable_steady_tick(TICK);
        TotalBar::wrap(bar, tab)
    }

    /// A renderer that draws nothing and hands out silent bar handles.
    pub(crate) const fn disabled() -> Self {
        Self { inner: None }
    }

    /// The per-file bar style: a bold label, a free-form `{msg}` (the file's
    /// `downloaded/download-total (reconstructed)` counts plus a phase word, all
    /// formatted by [`FilePhase`]), and the wide bar. The counts live in `{msg}`
    /// rather than the built-in `{bytes}/{total_bytes}` so the same row can read as
    /// download progress and then as reconstruction without the bar's own length
    /// having to mean two different things.
    fn file_style() -> indicatif::ProgressStyle {
        indicatif::ProgressStyle::with_template("{prefix:.bold} {msg}[{wide_bar:.cyan/blue}]")
            .unwrap_or_else(|_| indicatif::ProgressStyle::default_bar())
            .progress_chars("=>-")
    }

    /// Insert a new per-file bar above the total bar (or above the pad when there
    /// is no total bar), style and label it, set its length to the file's download
    /// size (the download-phase denominator), and start its steady tick — all
    /// after the insert (see [`TICK`] for why nothing touches the bar before the
    /// container owns it). The bar then takes a freed row back from the pad.
    fn insert_file_bar(
        i: &Inner,
        label: &str,
        download_total: u64,
    ) -> (indicatif::ProgressBar, PadRow) {
        let above = i.total.as_ref().map_or(&i.pad.bar, |total| &total.bar);
        let bar = i.mp.insert_before(above, new_bar(download_total));
        bar.set_style(Self::file_style());
        bar.set_prefix(label.to_string());
        bar.enable_steady_tick(TICK);
        i.pad.take_row();
        let row = PadRow {
            pad: Arc::clone(&i.pad),
            held: AtomicBool::new(true),
        };
        (bar, row)
    }

    /// A per-file bar for a whole-blob pull, labeled `label` and inserted above the
    /// total bar. `download_total` is the blob's download (pay-now) bytes — the
    /// bar's download-phase length and the `x/y` denominator; `reconstruct_total` is
    /// the bytes it splices from disk — shown in parentheses and, during the verify,
    /// the extra span the bar fills. Their sum is the blob's whole size. The bar's
    /// download progress also folds into the total bar's download meter.
    ///
    /// When disabled the returned [`FileBar`] is silent: it has no delivery
    /// callback, and ignores every report.
    pub(crate) fn file_bar(
        &self,
        label: &str,
        download_total: u64,
        reconstruct_total: u64,
    ) -> FileBar {
        let Some(i) = &self.inner else {
            return FileBar::disabled();
        };
        let (bar, row) = Self::insert_file_bar(i, label, download_total);
        let phase = FilePhase {
            bar,
            download_total,
            reconstruct_total,
        };
        phase.render(0, download_total, "");
        // The delivery callback advances this file's download and folds the same
        // downloaded bytes into the total bar's download meter (capped at the file's
        // download size — a dedup entry's driver never delivers past its pay-now
        // ranges, but the cap keeps the total honest regardless; an unsized entry's
        // cap is 0, so it displays progress from `expected` but adds nothing).
        let total = i.total.clone();
        let cb_phase = phase.clone();
        // The file's download bytes the total already holds: every fetch of the
        // entry reports its whole position, so only the rise past it counts.
        let prev = AtomicU64::new(0);
        let cb = move |received: u64, expected: u64| {
            cb_phase.download(received, expected);
            if let Some(t) = &total {
                let capped = received.min(cb_phase.download_total);
                let last = prev.fetch_max(capped, Ordering::Relaxed);
                if capped > last {
                    t.inc(capped - last);
                }
            }
        };
        FileBar {
            row: Some(row),
            phase: Some(phase),
            cb: Some(Box::new(cb)),
        }
    }

    /// Clear the total bar and remove the tab-bar indicator at the end of the run;
    /// the command then prints its own summary line. A no-op when disabled.
    pub(crate) fn finish(&self) {
        let Some(i) = &self.inner else {
            return;
        };
        if let Some(total) = &i.total {
            total.finish_and_clear();
        }
        i.pad.bar.finish_and_clear();
        if let Some(tab) = &i.tab {
            tab.clear();
        }
    }
}

/// One file's (or fetch-once hash group's) live bar state: the bar, its download
/// and reconstruct sizes, and the formatting of its `x/y (z) phase-word` message.
/// Cloning shares the same underlying bar.
#[derive(Clone)]
struct FilePhase {
    bar: indicatif::ProgressBar,
    /// Download (pay-now) bytes — the `x/y` denominator and download-phase length.
    download_total: u64,
    /// Bytes spliced from disk — shown in `(…)` and, during verify, the extra span.
    reconstruct_total: u64,
}

impl FilePhase {
    /// Set the message to `downloaded/display-total[ (reconstructed)][ word]`, in
    /// human byte units. `display_total` is the denominator to show — the download
    /// total for a sized entry, or `0` for an entry with no known size yet, in which
    /// case only the delivered count renders. `word` is a phase hint
    /// (`pending siblings…`, `reconstructing…`, `discovering…`) or empty during a
    /// plain download.
    fn render(&self, downloaded: u64, display_total: u64, word: &str) {
        let counts = if display_total > 0 {
            format!(
                "{}/{}",
                indicatif::HumanBytes(downloaded.min(display_total)),
                indicatif::HumanBytes(display_total),
            )
        } else {
            indicatif::HumanBytes(downloaded).to_string()
        };
        let recon = if self.reconstruct_total > 0 {
            format!(" ({})", indicatif::HumanBytes(self.reconstruct_total))
        } else {
            String::new()
        };
        let tail = if word.is_empty() {
            String::new()
        } else {
            format!(" {word}")
        };
        self.bar.set_message(format!("{counts}{recon}{tail} "));
    }

    /// Advance the download: position tracks the delivered bytes and the counts
    /// re-render. A sized entry meters against its download total; an entry with no
    /// declared size (allowed by the manifest) has no download total, so it borrows
    /// the stream's `expected` length for display only — it still contributes
    /// nothing to the total bar (the fold is capped at the download total, which is
    /// `0` here).
    fn download(&self, received: u64, expected: u64) {
        let display_total = if self.download_total > 0 {
            self.download_total
        } else {
            expected
        };
        if display_total > 0 {
            self.bar.set_length(display_total);
            self.bar.set_position(received.min(display_total));
        } else {
            self.bar.set_position(received);
        }
        self.render(received, display_total, "");
    }

    /// Enter the `pending siblings…` wait: the download is done (bar full), and the
    /// blob is now waiting for a sibling to register a deferred chunk.
    fn pending(&self) {
        self.bar.set_position(self.download_total);
        self.render(
            self.download_total,
            self.download_total,
            "pending siblings…",
        );
    }

    /// Enter the `discovering…` phase: probing holders before any byte arrives.
    fn discovering(&self) {
        self.render(0, self.download_total, "discovering…");
    }

    /// Enter the `reconstructing…` verify: the bar now spans the whole blob size
    /// (download + reconstruct) and fills with bytes hashed.
    fn start_reconstructing(&self) {
        self.bar
            .set_length(self.download_total.saturating_add(self.reconstruct_total));
        self.bar.set_position(0);
        self.render(self.download_total, self.download_total, "reconstructing…");
    }
}

/// One file's (or fetch-once hash group's) progress handle: its [`FilePhase`] plus
/// the delivery callback that drives the download. Silent when the renderer is
/// disabled.
pub(crate) struct FileBar {
    /// The bar's row in the stack; `None` when disabled. Declared first so a
    /// drop hands the row to the pad before the bar's own drop clears it.
    row: Option<PadRow>,
    /// The live bar state; `None` when disabled.
    phase: Option<FilePhase>,
    /// The delivery callback handed to the fetch path; `None` when disabled.
    cb: Option<Box<ProgressCallback>>,
}

impl FileBar {
    /// A silent bar: no rendering, no callback.
    fn disabled() -> Self {
        Self {
            row: None,
            phase: None,
            cb: None,
        }
    }

    /// Show the `discovering…` phase (probing holders before any byte arrives).
    pub(crate) fn set_discovering(&self) {
        if let Some(p) = &self.phase {
            p.discovering();
        }
    }

    /// Show the `pending siblings…` phase (download done, waiting on a sibling to
    /// register a deferred chunk).
    pub(crate) fn set_pending(&self) {
        if let Some(p) = &self.phase {
            p.pending();
        }
    }
}

/// A file bar follows one blob of a [`decdn_client::bundle::BundlePuller`]
/// pull.
impl decdn_client::bundle::FileObserver for FileBar {
    fn delivered(&self, received: u64, expected: u64) {
        if let Some(cb) = &self.cb {
            cb(received, expected);
        }
    }

    fn discovering(&self) {
        self.set_discovering();
    }

    fn pending(&self) {
        self.set_pending();
    }

    fn reconstructing(&self) {
        if let Some(p) = &self.phase {
            p.start_reconstructing();
        }
    }

    fn hashed(&self, bytes: u64) {
        if let Some(p) = &self.phase {
            p.bar.set_position(bytes);
        }
    }

    fn finished(&self) {
        if let Some(row) = &self.row {
            row.release();
        }
        if let Some(p) = &self.phase {
            p.bar.finish_and_clear();
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
