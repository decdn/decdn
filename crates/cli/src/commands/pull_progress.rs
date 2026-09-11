//! Multi-bar progress for `decdn bundle pull`.
//!
//! A `bundle pull` run fetches many blobs concurrently, so it renders a stack of
//! per-file bars — one per in-flight pull, bounded by `--jobs` — above one bottom
//! **total** bar, all sharing an [`indicatif::MultiProgress`]. A finished file's
//! bar clears, so only active pulls stay on screen.
//!
//! Both rows are in **content** bytes. A per-file bar tracks the `(received,
//! expected)` pair a single pull's delivery [`ProgressCallback`] reports — the
//! verified content position against the blob's `total_bytes`, known from its
//! signed `StreamResponse`. The **total** bar has its denominator fixed on the
//! first frame from the manifest's declared sizes (`total_content_bytes`): the run
//! knows up front exactly how many content bytes it will deliver, so the total
//! reads "delivered of the whole download" from the start rather than growing as
//! pulls begin. Each pull folds its progress into the total scaled to the
//! manifest's declared size — `size × received / expected`, which pins the
//! contribution to the manifest's figure even if the delivered blob's
//! `total_bytes` differs from it — a monotonic value that lands on exactly the
//! declared size at completion, so the total ends at exactly 100%.
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
    /// first progress callback (the driver's pre-stream `base_present` report)
    /// replaces it with the authoritative `total_bytes`. It is also the pull's
    /// scaled contribution to the total bar.
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
        // The file bar tracks the pull's own progress; the total bar gets that
        // progress scaled to the manifest's declared `size`. A pull with no
        // declared size still shows its own bar but adds nothing to the total.
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

/// A whole-file pull's fold into the total bar: it maps the pull's cumulative
/// `(received, expected)` onto the manifest's declared size (`size × received /
/// expected`) and adds only the increase since the previous update. `expected`
/// (the pull's `total_bytes`) is constant, so the mapped value is monotonic and
/// reaches exactly `size` when the pull completes.
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
        // `received` past `expected` (overshoot) is clamped, never over 100%.
        assert_eq!(scaled(1000, 5000, 4000), 1000);
        // A zero denominator (no length yet) is zero, not a divide-by-zero.
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
    fn content_contributor_scales_progress_to_declared_size() {
        // A pull reporting progress against an `expected` of 4000 for a declared
        // size of 1000: the total advances in declared bytes and ends on exactly 1000.
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
