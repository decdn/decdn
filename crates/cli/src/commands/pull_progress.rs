//! Multi-bar progress for `decdn bundle pull`.
//!
//! A `bundle pull` run fetches many blobs concurrently, so it renders a stack of
//! per-file bars — one per in-flight pull, bounded by `--jobs` — above one bottom
//! **total** bar, all sharing an [`indicatif::MultiProgress`], under a one-line
//! header naming the run and its download vs on-disk sizes. A finished file's bar
//! clears, so only active pulls stay on screen.
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
//! piped and scripted output is byte-for-byte what it was before per-file bars.

use std::collections::HashMap;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicU64, Ordering};
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
        [only] => only.clone(),
        [first, rest @ ..] => format!("{first} (+{} more)", rest.len()),
    }
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
    /// The bottom total bar, in download bytes with a fixed denominator, carrying
    /// the run's rate/ETA; per-file bars insert before it. `None` when the manifest
    /// declares no sizes, in which case only per-file bars render.
    total: Option<TotalBar>,
    /// The terminal's tab-bar indicator; `None` when the terminal lacks OSC 9;4.
    tab: Option<Arc<TabProgress>>,
    /// How many of each file's download bytes the total bar already holds, by bar
    /// label. A retry round builds a new bar for a file whose earlier bar already
    /// folded its landed prefix into the total, and its first callback reports
    /// that same prefix again as it resumes; the new bar picks up the old
    /// high-water mark so those bytes are not counted twice.
    folded: Mutex<HashMap<String, Arc<AtomicU64>>>,
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
        // actually saves bytes is the "on disk" figure worth showing.
        if let Some(dl) = download_total.filter(|n| *n > 0) {
            let header = match content_total.filter(|c| *c > dl) {
                Some(content) => format!(
                    "{label} · {} to download · {} on disk",
                    indicatif::HumanBytes(dl),
                    indicatif::HumanBytes(content),
                ),
                None => format!("{label} · {} to download", indicatif::HumanBytes(dl)),
            };
            let _ = mp.println(header);
        }
        let download_total = download_total.filter(|n| *n > 0);
        let tab = TabProgress::detect(download_total.is_some());
        let total = download_total.map(|len| Self::add_total_bar(&mp, len, tab.clone()));
        Self {
            inner: Some(Inner {
                mp,
                total,
                tab,
                folded: Mutex::new(HashMap::new()),
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

    /// Insert a new per-file bar above the total bar (or at the bottom when there is
    /// no total bar), style and label it, set its length to the file's download size
    /// (the download-phase denominator), and start its steady tick — all after the
    /// insert (see [`TICK`] for why nothing touches the bar before the container
    /// owns it).
    fn insert_file_bar(i: &Inner, label: &str, download_total: u64) -> indicatif::ProgressBar {
        let bar = match &i.total {
            Some(total) => i.mp.insert_before(&total.bar, new_bar(download_total)),
            None => i.mp.add(new_bar(download_total)),
        };
        bar.set_style(Self::file_style());
        bar.set_prefix(label.to_string());
        bar.enable_steady_tick(TICK);
        bar
    }

    /// A per-file bar for a whole-blob pull, labeled `label` and inserted above the
    /// total bar. `download_total` is the blob's download (pay-now) bytes — the
    /// bar's download-phase length and the `x/y` denominator; `reconstruct_total` is
    /// the bytes it splices from disk — shown in parentheses and, during the verify,
    /// the extra span the bar fills. Their sum is the blob's whole size. The bar's
    /// download progress also folds into the total bar's download meter.
    ///
    /// When disabled the returned [`FileBar`] is silent and its
    /// [`FileBar::callback`] is `None`, so the fetch path runs byte-bar-free exactly
    /// as it did before per-file bars.
    pub(crate) fn file_bar(
        &self,
        label: &str,
        download_total: u64,
        reconstruct_total: u64,
    ) -> FileBar {
        let Some(i) = &self.inner else {
            return FileBar::disabled();
        };
        let bar = Self::insert_file_bar(i, label, download_total);
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
        let prev = i.folded.lock().map_or_else(
            |_| Arc::new(AtomicU64::new(0)),
            |mut map| Arc::clone(map.entry(label.to_string()).or_default()),
        );
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
    /// The live bar state; `None` when disabled.
    phase: Option<FilePhase>,
    /// The delivery callback handed to the fetch path; `None` when disabled.
    cb: Option<Box<ProgressCallback>>,
}

impl FileBar {
    /// A silent bar: no rendering, no callback.
    fn disabled() -> Self {
        Self {
            phase: None,
            cb: None,
        }
    }

    /// The delivery callback to hand the fetch path, or `None` when disabled (the
    /// fetch path then runs byte-bar-free, as before per-file bars).
    pub(crate) fn callback(&self) -> Option<&ProgressCallback> {
        self.cb.as_deref()
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

    /// Enter the `reconstructing…` verify and return a progress reporter to feed the
    /// running byte count of the whole-file hash into, so the bar keeps moving
    /// through the verify instead of freezing. `None` when disabled — the hash then
    /// runs without a progress callback, as before.
    pub(crate) fn reconstruct_reporter(&self) -> Option<Box<dyn Fn(u64) + Send + Sync>> {
        let p = self.phase.as_ref()?;
        p.start_reconstructing();
        let bar = p.bar.clone();
        Some(Box::new(move |hashed: u64| bar.set_position(hashed)))
    }

    /// Clear the bar once the file's pull settles (success or failure).
    pub(crate) fn finish(self) {
        if let Some(p) = self.phase {
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

    /// A hidden total bar of fixed content length `len`, with its own rate meter.
    fn hidden_total(len: u64) -> TotalBar {
        TotalBar::wrap(
            indicatif::ProgressBar::with_draw_target(
                Some(len),
                indicatif::ProgressDrawTarget::hidden(),
            ),
            None,
        )
    }

    #[test]
    fn disabled_file_bar_has_no_callback() {
        let fb = FileBar::disabled();
        assert!(fb.callback().is_none());
    }

    /// A hidden [`PullProgress`] whose total bar denominates the run's download. The
    /// total bar is added to the container so a per-file `insert_before` has an
    /// anchor, exactly as production builds it.
    fn hidden_pp(download_total: u64) -> (PullProgress, TotalBar) {
        let mp =
            indicatif::MultiProgress::with_draw_target(indicatif::ProgressDrawTarget::hidden());
        let total = TotalBar::wrap(mp.add(indicatif::ProgressBar::new(download_total)), None);
        let pp = PullProgress {
            inner: Some(Inner {
                mp,
                total: Some(total.clone()),
                tab: None,
                folded: Mutex::new(HashMap::new()),
            }),
        };
        (pp, total)
    }

    #[test]
    fn file_bar_download_folds_into_the_total_capped_at_download_bytes() {
        // A blob that downloads 1000 and reconstructs 500 from disk. Its delivery
        // callback advances the total's DOWNLOAD meter by the delivered bytes only,
        // capped at the download size — a splice never inflates the download total.
        let (pp, total) = hidden_pp(1000);
        let fb = pp.file_bar("m", 1000, 500);
        let cb = fb.callback().expect("enabled");
        cb(400, 9999);
        assert_eq!(total.position(), 400);
        cb(1000, 9999);
        assert_eq!(total.position(), 1000);
        // Delivery past the download size (should not happen, but be safe) does not
        // push the total past the download denominator.
        cb(1200, 9999);
        assert_eq!(total.position(), 1000);
    }

    /// A retry round builds a fresh bar for the same file, and its resumed drive
    /// reports the already-landed prefix again. The total must count that prefix
    /// once, and still count the bytes the retry adds.
    #[test]
    fn a_retried_file_bar_does_not_recount_its_landed_prefix() {
        let (pp, total) = hidden_pp(1000);
        let first = pp.file_bar("m", 1000, 0);
        first.callback().expect("enabled")(600, 1000);
        first.finish();
        assert_eq!(total.position(), 600);

        let retry = pp.file_bar("m", 1000, 0);
        let cb = retry.callback().expect("enabled");
        cb(600, 1000);
        assert_eq!(
            total.position(),
            600,
            "the resumed prefix is already counted"
        );
        cb(1000, 1000);
        assert_eq!(total.position(), 1000);

        // A different file keeps its own mark.
        pp.file_bar("other", 1000, 0).callback().expect("enabled")(0, 1000);
        assert_eq!(total.position(), 1000);
    }

    #[test]
    fn unsized_file_bar_shows_stream_length_but_adds_nothing_to_the_total() {
        // An entry the manifest declares no size for has download_total 0. Its bar
        // borrows the stream's `expected` for display so the row still moves, but it
        // contributes nothing to the total bar's download meter.
        let (pp, total) = hidden_pp(1000);
        let fb = pp.file_bar("m", 0, 0);
        let cb = fb.callback().expect("enabled");
        cb(500, 2000);
        let bar = &fb.phase.as_ref().expect("enabled").bar;
        assert_eq!(bar.length(), Some(2000), "borrows the stream length");
        assert_eq!(bar.position(), 500);
        assert_eq!(
            total.position(),
            0,
            "an unsized entry adds nothing to the total"
        );
        cb(1500, 2000);
        assert_eq!(bar.position(), 1500);
        assert_eq!(total.position(), 0);
    }

    #[test]
    fn file_bar_message_shows_download_counts_and_reconstruct_size() {
        let (pp, _total) = hidden_pp(1000);
        let fb = pp.file_bar("m", 1000, 500);
        let bar = &fb.phase.as_ref().expect("enabled").bar;
        fb.callback().expect("enabled")(400, 0);
        let msg = bar.message();
        // "<downloaded>/<download-total> (<reconstruct>)".
        assert!(msg.contains('/'), "{msg}");
        assert!(msg.contains('('), "{msg}");
    }

    #[test]
    fn file_bar_reconstructing_spans_the_whole_blob_and_tracks_hashed_bytes() {
        let (pp, _total) = hidden_pp(1000);
        let fb = pp.file_bar("m", 1000, 500);
        let reporter = fb.reconstruct_reporter().expect("enabled");
        let bar = &fb.phase.as_ref().expect("enabled").bar;
        // The bar now spans the whole blob (download + reconstruct) and fills with
        // bytes hashed, so a multi-GB verify is never a frozen row.
        assert_eq!(bar.length(), Some(1500));
        reporter(750);
        assert_eq!(bar.position(), 750);
        assert!(
            bar.message().contains("reconstructing"),
            "{}",
            bar.message()
        );
    }

    #[test]
    fn file_bar_pending_marks_the_download_done_and_waiting() {
        let (pp, _total) = hidden_pp(1000);
        let fb = pp.file_bar("m", 1000, 500);
        fb.set_pending();
        let bar = &fb.phase.as_ref().expect("enabled").bar;
        assert_eq!(bar.position(), 1000, "download is complete while pending");
        assert!(
            bar.message().contains("pending siblings"),
            "{}",
            bar.message()
        );
    }

    #[test]
    fn total_bar_inc_writes_rate_and_eta_message() {
        let total = hidden_total(1000);
        assert_eq!(total.bar.message(), "", "nothing until a byte moves");
        total.inc(100);
        assert_eq!(total.position(), 100);
        let msg = total.bar.message();
        assert!(msg.starts_with('('), "{msg}");
        assert!(msg.contains("ETA"), "{msg}");
    }

    #[test]
    fn total_bar_inc_drives_the_tab_percent() {
        let (tab, log) = TabProgress::recorded(true);
        let total = TotalBar::wrap(
            indicatif::ProgressBar::with_draw_target(
                Some(1000),
                indicatif::ProgressDrawTarget::hidden(),
            ),
            Some(tab),
        );
        total.inc(250);
        assert_eq!(log.lock().unwrap().last().unwrap(), "\x1b]9;4;1;25\x1b\\");
    }
}
