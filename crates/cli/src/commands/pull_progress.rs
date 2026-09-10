//! Multi-bar progress for `decdn bundle pull`.
//!
//! A `bundle pull` run fetches many blobs concurrently, so it renders a stack of
//! per-file bars — one per in-flight pull, bounded by `--jobs` — above one bottom
//! **total** bar, all sharing an [`indicatif::MultiProgress`]. A finished file's
//! bar clears, so only active pulls stay on screen.
//!
//! The two rows measure different units on purpose. A per-file bar is in **wire**
//! bytes — bao content plus interleaved proof nodes — because that is the only
//! unit a single pull's delivery [`ProgressCallback`] reports (a blob's aligned
//! wire length is known from its signed `StreamResponse`, its content size is
//! not). The **total** bar is in **content** bytes, with its denominator fixed on
//! the first frame from the manifest's declared sizes (`total_content_bytes`):
//! the run knows up front exactly how many content bytes it will deliver, so the
//! total reads "delivered of the whole download" from the start rather than
//! growing as pulls begin. Each pull folds its wire progress into the total scaled
//! to its content size — `size × received_wire / expected_wire` — a monotonic
//! value that lands on exactly the pull's content size at completion, so the total
//! ends at exactly 100%.
//!
//! The total bar is shown only when the manifest declares sizes. A blob with no
//! declared size contributes nothing to the denominator and moves the total not at
//! all; if nothing kept declares a size the total bar is omitted and only per-file
//! bars render.
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

/// Steady-tick cadence shared by every bar. Enabled only after a bar joins the
/// [`indicatif::MultiProgress`]: a detached bar draws straight to stderr, so
/// ticking first paints an orphan line the container never accounts for, and every
/// later redraw scrolls instead of overwriting it.
const TICK: Duration = Duration::from_millis(120);

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
    /// The bottom total bar, in content bytes with a fixed denominator; per-file
    /// bars insert before it. `None` when the manifest declares no sizes, in which
    /// case only per-file bars render.
    total: Option<indicatif::ProgressBar>,
}

impl PullProgress {
    /// Build the renderer. Returns a disabled renderer — no bars, no callbacks —
    /// when stderr is not a terminal or the run is `--json`, so non-interactive
    /// output is unchanged.
    ///
    /// `total_content` is the run's whole-download content size — the fixed total
    /// bar denominator (`total_content_bytes`). `None` (the manifest declared no
    /// sizes) omits the total bar and renders only per-file bars.
    pub(crate) fn new(json: bool, total_content: Option<u64>) -> Self {
        if json || !std::io::stderr().is_terminal() {
            return Self::disabled();
        }
        let mp = crate::logging::progress_container();
        let total = total_content
            .filter(|n| *n > 0)
            .map(|len| Self::add_total_bar(&mp, len));
        Self {
            inner: Some(Inner { mp, total }),
        }
    }

    /// Build the bottom total bar with a fixed content-byte length and add it to
    /// the container. Its steady tick is enabled after the add, never before.
    fn add_total_bar(mp: &indicatif::MultiProgress, len: u64) -> indicatif::ProgressBar {
        let total = indicatif::ProgressBar::new(len);
        let style = indicatif::ProgressStyle::with_template(
            "{prefix:.bold} {bytes}/{total_bytes} [{wide_bar:.green}]",
        )
        .unwrap_or_else(|_| indicatif::ProgressStyle::default_bar())
        .progress_chars("=>-");
        total.set_style(style);
        total.set_prefix("total");
        let total = mp.add(total);
        total.enable_steady_tick(TICK);
        total
    }

    /// A renderer that draws nothing and hands out silent bar handles.
    pub(crate) const fn disabled() -> Self {
        Self { inner: None }
    }

    /// Insert a per-file bar above the total bar (or at the bottom when there is no
    /// total bar) and start its steady tick.
    fn insert_file_bar(&self, bar: indicatif::ProgressBar) -> indicatif::ProgressBar {
        let Some(i) = &self.inner else { return bar };
        let bar = match &i.total {
            Some(total) => i.mp.insert_before(total, bar),
            None => i.mp.add(bar),
        };
        bar.enable_steady_tick(TICK);
        bar
    }

    /// A per-file bar for a whole-blob pull, labeled `label` and inserted above the
    /// total bar. `size` (the manifest's content size, when declared) sets an
    /// initial bar length so it reads sensibly during the pre-byte handshake; the
    /// first delivered chunk replaces it with the authoritative wire length. It is
    /// also the pull's scaled contribution to the total bar.
    ///
    /// When disabled the returned [`FileBar`] is silent and its
    /// [`FileBar::callback`] is `None`, so the fetch path runs byte-bar-free
    /// exactly as it did before per-file bars.
    pub(crate) fn file_bar(&self, label: String, size: Option<u64>) -> FileBar {
        let Some(i) = &self.inner else {
            return FileBar::disabled();
        };
        let bar = fetch::labeled_delivery_bar();
        bar.set_prefix(label);
        if let Some(n) = size {
            bar.set_length(n);
        }
        let bar = self.insert_file_bar(bar);
        // The file bar tracks wire bytes; the total bar gets this pull's wire
        // progress scaled to its content `size`. A pull with no declared size
        // still shows its own bar but adds nothing to the total.
        let (file_cb, _meter) = fetch::bar_callback(bar.clone(), None);
        let contrib = i
            .total
            .as_ref()
            .zip(size)
            .map(|(total, s)| content_contributor(total.clone(), s));
        let cb = move |received: u64, expected: u64| {
            file_cb(received, expected);
            if let Some(c) = &contrib {
                c(received, expected);
            }
        };
        FileBar {
            bar: Some(bar),
            cb: Some(Box::new(cb)),
        }
    }

    /// A chunked file's bar (summed across its chunk pulls), inserted above the
    /// total bar. `size` (the manifest's whole-file content size) serves two roles.
    /// When declared it presets the bar length so it fills smoothly 0→100% across
    /// the file's chunks; without a preset length the bar grows chunk-by-chunk,
    /// which reads as complete at every chunk boundary (a file's chunks are fetched
    /// in sequence, so each finished chunk momentarily fills the bar). The manifest
    /// normally declares `size` for a chunked entry, but it is optional on the wire,
    /// so the grow-per-chunk path remains the fallback. `size` is also the file's
    /// scaled contribution to the total bar (`None` adds nothing). Silent when
    /// disabled.
    pub(crate) fn chunked_file(&self, label: String, size: Option<u64>) -> ChunkedFile {
        let Some(i) = &self.inner else {
            return ChunkedFile::disabled();
        };
        let bar = fetch::labeled_delivery_bar();
        bar.set_prefix(label);
        if let Some(n) = size {
            bar.set_length(n);
        }
        let bar = self.insert_file_bar(bar);
        ChunkedFile {
            bar: Some(bar),
            total: i.total.clone(),
            size,
            prev: Arc::new(AtomicU64::new(0)),
            preset_length: size.is_some(),
        }
    }

    /// Credit an already-present unit's content `size` straight to the total bar.
    /// A skipped file (every destination on disk) does no fetch and drives no
    /// delivery callback, but its content is part of the whole-download total
    /// (`total_content_bytes` counts it), so without this credit the total could
    /// never reach 100% on a resumed or already-present run. A no-op when disabled,
    /// when there is no total bar, or when `size` is absent (then it is not in the
    /// denominator either). Shows no per-file bar — a skip is instantaneous.
    pub(crate) fn credit_skipped(&self, size: Option<u64>) {
        if let Some(i) = &self.inner
            && let (Some(total), Some(s)) = (&i.total, size)
        {
            total.inc(s);
        }
    }

    /// Clear the total bar at the end of the run; the command then prints its own
    /// summary line. A no-op when disabled or when there is no total bar.
    pub(crate) fn finish(&self) {
        if let Some(i) = &self.inner
            && let Some(total) = &i.total
        {
            total.finish_and_clear();
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
    /// The run total bar this file folds its scaled content progress into; `None`
    /// when disabled or when the run has no total bar.
    total: Option<indicatif::ProgressBar>,
    /// The file's declared content size — its full contribution to the total bar
    /// and, when present, the preset denominator of the file bar. `None` adds
    /// nothing to the total.
    size: Option<u64>,
    /// Content bytes this file has already added to the total, so each update adds
    /// only the increment and the fold stays monotonic when a new chunk grows the
    /// file bar's length ahead of its bytes.
    prev: Arc<AtomicU64>,
    /// Whether the file bar's length was preset to the whole-file `size`. When true
    /// the bar advances position only (its denominator is already the whole file);
    /// when false the length grows per chunk as a fallback.
    preset_length: bool,
}

impl ChunkedFile {
    /// A silent chunked-file bar.
    fn disabled() -> Self {
        Self {
            bar: None,
            total: None,
            size: None,
            prev: Arc::new(AtomicU64::new(0)),
            preset_length: false,
        }
    }

    /// Fold this file's current progress into the total, scaled to its content
    /// `size`: `size × position / length`, advanced only upward. A no-op with no
    /// total bar, no size, or an unbounded bar.
    fn bump_total(&self) {
        bump_chunked_total(
            self.total.as_ref(),
            self.size,
            self.bar.as_ref(),
            &self.prev,
        );
    }

    /// A fresh delivery callback for ONE chunk pull: it advances this file's bar
    /// position by each received delta and folds the file's scaled content progress
    /// into the run total. When the bar length was not preset to the whole-file
    /// `size` it also grows the length by the expected delta (the fallback path).
    /// `None` when disabled (the chunk fetch then runs bar-free). Built per chunk,
    /// since each chunk's callback tracks its own cumulative from zero.
    pub(crate) fn chunk_callback(&self) -> Option<Box<ProgressCallback>> {
        let bar = self.bar.clone()?;
        let total = self.total.clone();
        let size = self.size;
        let file_prev = Arc::clone(&self.prev);
        let preset_length = self.preset_length;
        let prev = AtomicU64::new(0);
        let prev_expected = AtomicU64::new(0);
        Some(Box::new(move |received: u64, expected: u64| {
            let received_delta = received.saturating_sub(prev.swap(received, Ordering::Relaxed));
            let expected_delta =
                expected.saturating_sub(prev_expected.swap(expected, Ordering::Relaxed));
            if !preset_length {
                bar.inc_length(expected_delta);
            }
            bar.inc(received_delta);
            bump_chunked_total(total.as_ref(), size, Some(&bar), &file_prev);
        }))
    }

    /// Advance the file bar by a whole chunk that another file already fetched (so
    /// no callback fired for it here), and fold the file's scaled content progress
    /// into the total. `chunk_size` is the staged content size. The position
    /// advances by it; the length grows too only when it was not preset to the
    /// whole-file `size` (the fallback path), so the bar stays internally consistent
    /// either way.
    pub(crate) fn advance_reused(&self, chunk_size: u64) {
        if let Some(bar) = &self.bar {
            if !self.preset_length {
                bar.inc_length(chunk_size);
            }
            bar.inc(chunk_size);
        }
        self.bump_total();
    }

    /// Clear the bar once the file's chunks are fetched and it is assembled, and
    /// true up its total contribution to exactly its content `size` (the wire
    /// fraction lands there, but round-off could leave a byte or two short).
    pub(crate) fn finish(self) {
        if let (Some(total), Some(size)) = (&self.total, self.size) {
            let last = self.prev.fetch_max(size, Ordering::Relaxed);
            total.inc(size.saturating_sub(last));
        }
        if let Some(bar) = self.bar {
            bar.finish_and_clear();
        }
    }
}

/// `size × num / den`, clamped so `num ≤ den`, as a `u64`. The result is at most
/// `size`, so the narrowing back from `u128` cannot lose data.
fn scaled(size: u64, num: u64, den: u64) -> u64 {
    if den == 0 {
        return 0;
    }
    let v = u128::from(size) * u128::from(num.min(den)) / u128::from(den);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "v <= size, which is a u64, so the value fits"
    )]
    {
        v as u64
    }
}

/// A whole-file pull's fold into the total bar: it maps the pull's cumulative wire
/// `(received, expected)` to content bytes (`size × received / expected`) and adds
/// only the increase since the previous update. `expected` (the pull's wire
/// length) is constant, so the mapped value is monotonic and reaches exactly
/// `size` when the pull completes.
fn content_contributor(total: indicatif::ProgressBar, size: u64) -> impl Fn(u64, u64) {
    let prev = AtomicU64::new(0);
    move |received: u64, expected: u64| {
        if expected == 0 {
            return;
        }
        let content = scaled(size, received, expected);
        let last = prev.fetch_max(content, Ordering::Relaxed);
        if content > last {
            total.inc(content - last);
        }
    }
}

/// Fold a chunked file's current wire progress into `total`, scaled to its content
/// `size`: `size × bar.position / bar.length`, advanced only upward. The file
/// bar's length grows as chunks start, so the ratio can dip momentarily — the
/// upward-only [`AtomicU64::fetch_max`] keeps the total from regressing. A no-op
/// with no total bar, no size, or an unbounded bar.
fn bump_chunked_total(
    total: Option<&indicatif::ProgressBar>,
    size: Option<u64>,
    bar: Option<&indicatif::ProgressBar>,
    prev: &AtomicU64,
) {
    let (Some(total), Some(size), Some(bar)) = (total, size, bar) else {
        return;
    };
    let Some(len) = bar.length() else { return };
    let content = scaled(size, bar.position(), len);
    let last = prev.fetch_max(content, Ordering::Relaxed);
    if content > last {
        total.inc(content - last);
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

    #[test]
    fn scaled_is_proportional_and_capped() {
        assert_eq!(scaled(1000, 0, 4000), 0);
        assert_eq!(scaled(1000, 1000, 4000), 250);
        assert_eq!(scaled(1000, 4000, 4000), 1000);
        // `received` past `expected` (wire overshoot) is clamped, never over 100%.
        assert_eq!(scaled(1000, 5000, 4000), 1000);
        // A zero denominator (no wire length yet) is zero, not a divide-by-zero.
        assert_eq!(scaled(1000, 10, 0), 0);
    }

    // A hidden but length-bounded bar (as production builds via `ProgressBar::new`)
    // tracks position/length, so the content accounting is testable without a
    // terminal. `ProgressBar::hidden()` starts unbounded (length `None`), where
    // `inc_length` is a no-op — so tests must start from `Some(0)`.
    fn test_bar() -> indicatif::ProgressBar {
        indicatif::ProgressBar::with_draw_target(Some(0), indicatif::ProgressDrawTarget::hidden())
    }

    #[test]
    fn content_contributor_scales_wire_progress_to_content_size() {
        // A pull of 1000 content bytes whose wire length is 4000 (bao overhead):
        // the total advances in content bytes and ends on exactly 1000.
        let total = indicatif::ProgressBar::with_draw_target(
            Some(1000),
            indicatif::ProgressDrawTarget::hidden(),
        );
        let contrib = content_contributor(total.clone(), 1000);
        contrib(0, 4000);
        assert_eq!(total.position(), 0);
        contrib(2000, 4000);
        assert_eq!(total.position(), 500);
        contrib(4000, 4000);
        assert_eq!(total.position(), 1000);
    }

    #[test]
    fn content_contributor_without_expected_is_inert() {
        let total = test_bar();
        let contrib = content_contributor(total.clone(), 1000);
        // Pre-byte handshake: expected not yet known, so nothing folds in.
        contrib(0, 0);
        assert_eq!(total.position(), 0);
    }

    #[test]
    fn chunked_file_folds_scaled_content_into_total_and_trues_up() {
        let total = indicatif::ProgressBar::with_draw_target(
            Some(1000),
            indicatif::ProgressDrawTarget::hidden(),
        );
        // Fallback (no preset length): the denominator grows per chunk.
        let cf = ChunkedFile {
            bar: Some(test_bar()),
            total: Some(total.clone()),
            size: Some(1000),
            prev: Arc::new(AtomicU64::new(0)),
            preset_length: false,
        };

        // Chunk 1: 50 content bytes delivered in two updates.
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
        // 90/90 of the wire → the file's full 1000 content bytes fold into the total.
        assert_eq!(total.position(), 1000);
        // A completed file trues up to exactly its size (idempotent here).
        cf.finish();
        assert_eq!(total.position(), 1000);
    }

    #[test]
    fn preset_length_bar_climbs_to_full_without_growing_denominator() {
        // The whole-file content size (90) is known up front, as the manifest
        // normally declares for a chunked entry, so the bar length is preset. The
        // total bar is content-metered.
        let total = indicatif::ProgressBar::with_draw_target(
            Some(90),
            indicatif::ProgressDrawTarget::hidden(),
        );
        let whole = test_bar();
        whole.set_length(90);
        let cf = ChunkedFile {
            bar: Some(whole),
            total: Some(total.clone()),
            size: Some(90),
            prev: Arc::new(AtomicU64::new(0)),
            preset_length: true,
        };

        // Chunk 1 of 2 (50 of 90 bytes) fully delivered. The bug was the bar — and
        // its total contribution — reading full at this boundary because the length
        // grew chunk-by-chunk. With a preset denominator it reads 50/90.
        let cb1 = cf.chunk_callback().expect("enabled -> Some callback");
        cb1(0, 50);
        cb1(50, 50);
        let bar = cf.bar.as_ref().expect("bar present");
        assert_eq!(bar.length(), Some(90), "denominator stays the whole file");
        assert_eq!(bar.position(), 50, "half done, not full");
        assert_eq!(total.position(), 50, "total folds 50 content bytes, not 90");

        // Chunk 2 (40 bytes) finishes the file at exactly 100%.
        let cb2 = cf.chunk_callback().expect("enabled -> Some callback");
        cb2(0, 40);
        cb2(40, 40);
        assert_eq!(bar.position(), 90);
        assert_eq!(total.position(), 90);
        cf.finish();
        assert_eq!(total.position(), 90);
    }

    #[test]
    fn chunked_total_never_regresses_when_length_grows() {
        let total = indicatif::ProgressBar::with_draw_target(
            Some(1000),
            indicatif::ProgressDrawTarget::hidden(),
        );
        // Fallback (no preset length): the denominator grows per chunk.
        let cf = ChunkedFile {
            bar: Some(test_bar()),
            total: Some(total.clone()),
            size: Some(1000),
            prev: Arc::new(AtomicU64::new(0)),
            preset_length: false,
        };
        // First chunk completes: 50/50 → the whole content size folds in early
        // (only one chunk is known so far).
        let cb1 = cf.chunk_callback().expect("Some");
        cb1(50, 50);
        assert_eq!(total.position(), 1000);
        // A second chunk starts, growing the file bar's length ahead of its bytes.
        // The scaled value dips, but the total must not go backwards.
        let cb2 = cf.chunk_callback().expect("Some");
        cb2(0, 50);
        assert_eq!(total.position(), 1000);
    }

    #[test]
    fn advance_reused_folds_into_total_without_double_counting() {
        let total = indicatif::ProgressBar::with_draw_target(
            Some(50),
            indicatif::ProgressDrawTarget::hidden(),
        );
        // Fallback (no preset length): length and position grow together.
        let cf = ChunkedFile {
            bar: Some(test_bar()),
            total: Some(total.clone()),
            size: Some(50),
            prev: Arc::new(AtomicU64::new(0)),
            preset_length: false,
        };
        // A single reused chunk that is the whole file: the bar reads complete and
        // the total gains the file's content size once.
        cf.advance_reused(25);
        let bar = cf.bar.as_ref().expect("bar present");
        assert_eq!(bar.length(), Some(25));
        assert_eq!(bar.position(), 25);
        assert_eq!(total.position(), 50);
    }

    #[test]
    fn advance_reused_on_preset_bar_advances_position_only() {
        let total = indicatif::ProgressBar::with_draw_target(
            Some(60),
            indicatif::ProgressDrawTarget::hidden(),
        );
        let whole = test_bar();
        whole.set_length(60);
        let cf = ChunkedFile {
            bar: Some(whole),
            total: Some(total.clone()),
            size: Some(60),
            prev: Arc::new(AtomicU64::new(0)),
            preset_length: true,
        };

        // A reused chunk (25 of 60) of a preset-length bar advances the position
        // without growing the whole-file denominator, folding its scaled content
        // into the total.
        cf.advance_reused(25);

        let bar = cf.bar.as_ref().expect("bar present");
        assert_eq!(bar.length(), Some(60), "denominator unchanged");
        assert_eq!(bar.position(), 25);
        assert_eq!(total.position(), 25);
    }

    #[test]
    fn disabled_chunked_file_has_no_callback() {
        let cf = ChunkedFile::disabled();
        assert!(cf.chunk_callback().is_none());
        // A silent advance is a no-op, not a panic.
        cf.advance_reused(10);
        cf.finish();
    }

    #[test]
    fn disabled_file_bar_has_no_callback() {
        let fb = FileBar::disabled();
        assert!(fb.callback().is_none());
    }

    #[test]
    fn credit_skipped_advances_total_by_the_skipped_content_size() {
        // An already-present file drives no delivery callback, so its content is
        // credited straight to the total; a run of all-skipped files still reaches
        // 100%.
        let total = indicatif::ProgressBar::with_draw_target(
            Some(300),
            indicatif::ProgressDrawTarget::hidden(),
        );
        let pp = PullProgress {
            inner: Some(Inner {
                mp: indicatif::MultiProgress::with_draw_target(
                    indicatif::ProgressDrawTarget::hidden(),
                ),
                total: Some(total.clone()),
            }),
        };
        pp.credit_skipped(Some(100));
        pp.credit_skipped(Some(200));
        assert_eq!(total.position(), 300);
        // A sizeless skip is inert (it is not in the denominator either).
        pp.credit_skipped(None);
        assert_eq!(total.position(), 300);
    }

    #[test]
    fn credit_skipped_is_a_no_op_when_disabled() {
        // No total bar, no panic.
        PullProgress::disabled().credit_skipped(Some(100));
    }
}
