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
//! Rate and ETA are shown on the **total** bar only, computed from total content
//! bytes with the same time-weighted moving average `decdn fetch` uses. A per-file
//! bar shows just its byte counts: with `--jobs` pulls sharing one link, a single
//! file's rate is only its share of the bandwidth and its ETA reads as stuck
//! whenever another file is being served, while the whole download keeps moving.
//! Bytes credited for already-present files shift the meter's baseline without
//! feeding the rate, so a resumed run reports transfer speed, not disk speed.
//!
//! The total bar is shown only when the manifest declares sizes. A blob with no
//! declared size contributes nothing to the denominator and moves the total not at
//! all. If no kept entry (one that survives the include/exclude filters) declares
//! a size, the total bar is omitted and only per-file bars render.
//!
//! The whole renderer is silent — every bar a no-op, every file's delivery
//! callback `None` — when stderr is not a terminal or the run is `--json`, so
//! piped and scripted output is byte-for-byte what it was before per-file bars.

use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
    /// The bottom total bar, in content bytes with a fixed denominator, carrying
    /// the run's rate/ETA; per-file bars insert before it. `None` when the manifest
    /// declares no sizes, in which case only per-file bars render.
    total: Option<TotalBar>,
}

/// The bottom total bar plus the rate meter behind its `{msg}`. Every content
/// byte folded into the total passes through [`inc`](Self::inc), which samples
/// the meter; already-present bytes pass through [`credit`](Self::credit), which
/// moves the bar without feeding the rate. Cloning shares the same bar and meter.
#[derive(Clone)]
pub(crate) struct TotalBar {
    /// The `indicatif` bar, in content bytes with a fixed length.
    bar: indicatif::ProgressBar,
    /// The whole-download rate estimate, sampled on every transferred increment.
    speed: Arc<Mutex<fetch::SpeedState>>,
}

impl TotalBar {
    /// A styled total bar of fixed content length `len`, not yet attached to any
    /// [`indicatif::MultiProgress`].
    fn new(len: u64) -> Self {
        let style = indicatif::ProgressStyle::with_template(
            // Rate/ETA come from `{msg}` (see `refresh_rate`), not the built-in
            // `{bytes_per_sec}`/`{eta}` — those swing wildly on bursty arrival.
            "{prefix:.bold} {bytes}/{total_bytes} {msg}[{wide_bar:.green}]",
        )
        .unwrap_or_else(|_| indicatif::ProgressStyle::default_bar())
        .progress_chars("=>-");
        let bar = indicatif::ProgressBar::new(len);
        bar.set_style(style);
        bar.set_prefix("total");
        Self::wrap(bar)
    }

    /// Wrap an existing bar with a fresh meter.
    fn wrap(bar: indicatif::ProgressBar) -> Self {
        Self {
            bar,
            speed: Arc::new(Mutex::new(fetch::SpeedState::default())),
        }
    }

    /// Advance the total by `bytes` of transferred content and refresh the
    /// rate/ETA from the new position.
    fn inc(&self, bytes: u64) {
        self.bar.inc(bytes);
        self.refresh_rate();
    }

    /// Advance the total by `bytes` that were not transferred (already on disk)
    /// without feeding the rate: the meter's baseline shifts past them so the
    /// next transferred increment is measured on its own.
    fn credit(&self, bytes: u64) {
        self.bar.inc(bytes);
        if let Ok(mut s) = self.speed.lock() {
            s.shift(bytes);
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
    fn add_total_bar(mp: &indicatif::MultiProgress, len: u64) -> TotalBar {
        let total = TotalBar::new(len);
        let bar = mp.add(total.bar.clone());
        bar.enable_steady_tick(TICK);
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
            Some(total) => i.mp.insert_before(&total.bar, bar),
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
        let file_cb = file_position_callback(bar.clone());
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

    /// Credit an already-present file's content `size` straight to the total bar.
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
            total.credit(s);
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

/// The delivery callback for one whole-file bar: it sets the bar length to the
/// pull's `total_bytes` once and advances the position to the cumulative verified
/// content-byte count. No rate — that lives on the total bar.
fn file_position_callback(bar: indicatif::ProgressBar) -> impl Fn(u64, u64) {
    // `expected` is constant across the pull, so set the length once (it takes a
    // write lock) rather than on every chunk in the hot receive loop.
    let length_set = AtomicBool::new(false);
    move |received: u64, expected: u64| {
        if !length_set.swap(true, Ordering::Relaxed) {
            bar.set_length(expected);
        }
        bar.set_position(received);
    }
}

/// A whole-file pull's fold into the total bar: it maps the pull's cumulative
/// `(received, expected)` onto the manifest's declared size (`size × received /
/// expected`) and adds only the increase since the previous update. `expected`
/// (the pull's `total_bytes`) is constant, so the mapped value is monotonic and
/// reaches exactly `size` when the pull completes.
fn content_contributor(total: TotalBar, size: u64) -> impl Fn(u64, u64) {
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

    /// A hidden total bar of fixed content length `len`, with its own rate meter.
    fn hidden_total(len: u64) -> TotalBar {
        TotalBar::wrap(indicatif::ProgressBar::with_draw_target(
            Some(len),
            indicatif::ProgressDrawTarget::hidden(),
        ))
    }

    #[test]
    fn content_contributor_scales_progress_to_declared_size() {
        // A pull reporting progress against an `expected` of 4000 for a declared
        // size of 1000: the total advances in declared bytes and ends on exactly 1000.
        let total = hidden_total(1000);
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
        let total = hidden_total(1000);
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
        let total = hidden_total(300);
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

    #[test]
    fn file_position_callback_sets_length_once_and_tracks_position() {
        let bar = test_bar();
        let cb = file_position_callback(bar.clone());
        cb(0, 400);
        assert_eq!(bar.length(), Some(400));
        assert_eq!(bar.position(), 0);
        cb(250, 400);
        assert_eq!(bar.position(), 250);
        // No rate/ETA message on a per-file bar.
        assert_eq!(bar.message(), "");
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
    fn total_bar_credit_moves_the_bar_but_not_the_rate() {
        let t0 = Instant::now();
        let total = hidden_total(1000);
        // Two transferred samples one second apart establish a rate.
        total.inc(100);
        let before = total
            .speed
            .lock()
            .map(|mut s| s.observe(t0 + Duration::from_secs(1), 200))
            .expect("unpoisoned");
        assert!(before > 0.0);
        // A credited (already-present) chunk moves the bar only: the next sample
        // one second later sees just its own 100 transferred bytes.
        total.credit(500);
        let after = total
            .speed
            .lock()
            .map(|mut s| s.observe(t0 + Duration::from_secs(2), 800))
            .expect("unpoisoned");
        assert_eq!(total.position(), 600, "inc(100) + credit(500)");
        // 100 B/s both times, smoothed toward the same value: had the credit fed
        // the rate, the second sample would have read a 600 B/s burst.
        assert!(
            after < 2.0 * before,
            "credit leaked into the rate: {before} -> {after}"
        );
    }
}
