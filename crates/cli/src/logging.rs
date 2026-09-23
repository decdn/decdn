//! Opt-in client-side tracing for the `decdn` CLI.
//!
//! The CLI is a terminal UI: by default it installs no tracing subscriber, so
//! its output is only the command's own stdout result and the stderr progress
//! bar. That keeps the common case clean, but it also means every
//! `tracing::debug!/info!/warn!` in `decdn-client`, `decdn-incentive`, and
//! `iroh` goes to a no-op collector — a stalled `decdn fetch` is silent on the
//! side that is actually failing.
//!
//! [`init`] turns logging on only when the operator asks for it, through
//! `-v`/`-vv`/`-vvv`, `--log-level`, or the `RUST_LOG` environment variable.
//! `RUST_LOG` wins (the same precedence the daemon uses), then `--log-level`,
//! then the `-v` count ([`requested_level`]). With none of them set the CLI
//! installs no subscriber and stays silent.
//!
//! # Progress-bar coexistence
//!
//! `decdn fetch` draws an [`indicatif`] progress bar to stderr, and a plain
//! stderr subscriber would shred it. When [`init`] installs a subscriber it
//! also creates one process-global [`indicatif::MultiProgress`]: log lines are
//! written through [`indicatif::MultiProgress::suspend`], and the fetch bar is
//! registered with the same `MultiProgress` (see [`attach_progress_bar`]), so
//! the two share stderr without corrupting each other. When no subscriber is
//! installed the bar draws straight to stderr as it always has.

use std::sync::OnceLock;

use decdn_common::cli::common::LogLevel;

/// The process-global [`indicatif::MultiProgress`] that log output and the
/// fetch progress bar share, set once by [`init`] when — and only when — a
/// subscriber is installed. Empty on the default (no-verbosity) path.
static PROGRESS: OnceLock<indicatif::MultiProgress> = OnceLock::new();

/// The level the command line asks for: an explicit `--log-level` wins over
/// the `-v` count, and a zero count with no `--log-level` asks for nothing.
#[must_use]
pub const fn requested_level(log_level: Option<LogLevel>, verbose: u8) -> Option<LogLevel> {
    if log_level.is_some() {
        return log_level;
    }
    match verbose {
        0 => None,
        1 => Some(LogLevel::Info),
        2 => Some(LogLevel::Debug),
        _ => Some(LogLevel::Trace),
    }
}

/// Decide the tracing filter directive from `RUST_LOG` and the command-line
/// level (`--log-level` or `-v`, see [`requested_level`]), or `None` to stay
/// silent.
///
/// `RUST_LOG` (when present and not blank) takes precedence over the
/// command-line level, matching the daemon. With neither source set the CLI installs no subscriber
/// and its output is identical to a build without this module.
fn filter_directive(rust_log: Option<&str>, log_level: Option<LogLevel>) -> Option<String> {
    match rust_log {
        Some(s) if !s.trim().is_empty() => Some(s.to_string()),
        _ => log_level.map(|level| level.to_string()),
    }
}

/// Level to use when the chosen directive is `RUST_LOG` but it fails to parse:
/// the operator's `--log-level` if they passed one, otherwise `info`. Mirrors
/// the daemon, so a broken `RUST_LOG` exported in the shell does not defeat an
/// explicit `--log-level` on the invocation.
fn malformed_rust_log_fallback(log_level: Option<LogLevel>) -> LogLevel {
    log_level.unwrap_or(LogLevel::Info)
}

/// Install the CLI tracing subscriber when the operator opts in.
///
/// Reads `RUST_LOG` and the parsed `--log-level`; if neither requests logging
/// this returns without touching global state, so the default terminal UX is
/// unchanged. Otherwise it installs a `fmt` subscriber whose writer routes
/// every line through a shared [`indicatif::MultiProgress`] so the `decdn
/// fetch` bar survives.
///
/// Call once, before command dispatch, so the subscriber covers every
/// subcommand.
pub fn init(log_level: Option<LogLevel>) {
    let rust_log = std::env::var("RUST_LOG").ok();
    let Some(directive) = filter_directive(rust_log.as_deref(), log_level) else {
        return;
    };

    // `EnvFilter::try_new` reports a malformed directive instead of panicking.
    // Only a `RUST_LOG` value can be malformed here — a `--log-level` directive
    // is always a valid level name — so fall back to the operator's
    // `--log-level` (or `info`), like the daemon, rather than dropping to a
    // fixed level or going silent.
    let filter = match tracing_subscriber::EnvFilter::try_new(&directive) {
        Ok(f) => f,
        Err(e) => {
            let fallback = malformed_rust_log_fallback(log_level);
            eprintln!(
                "warning: ignoring malformed RUST_LOG '{directive}'; \
                 falling back to log level '{fallback}': {e}"
            );
            tracing_subscriber::EnvFilter::new(fallback.to_string())
        }
    };

    let progress = indicatif::MultiProgress::new();
    // A second `init` call would be a bug (dispatch runs once), but guard
    // against clobbering the shared handle regardless.
    let _ = PROGRESS.set(progress.clone());

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(SuspendingMakeWriter { progress })
        .init();
}

/// Register a freshly built progress bar with the shared
/// [`indicatif::MultiProgress`] when a subscriber is installed, so log lines
/// and the bar coexist on stderr.
///
/// On the default path (no subscriber) the bar is returned untouched and draws
/// straight to stderr exactly as before.
pub fn attach_progress_bar(bar: indicatif::ProgressBar) -> indicatif::ProgressBar {
    match PROGRESS.get() {
        Some(progress) => progress.add(bar),
        None => bar,
    }
}

/// The [`indicatif::MultiProgress`] a multi-bar command (`bundle pull`) draws its
/// bars into: the process-global one when a subscriber is installed — so its bars
/// share stderr with log lines through [`indicatif::MultiProgress::suspend`] — or
/// a fresh standalone container on the default silent path, where there are no log
/// lines to coordinate with. Either way the bars stack coherently, and a
/// non-terminal stderr auto-hides them.
pub fn progress_container() -> indicatif::MultiProgress {
    match PROGRESS.get() {
        Some(progress) => progress.clone(),
        None => indicatif::MultiProgress::new(),
    }
}

/// [`tracing_subscriber::fmt::MakeWriter`] that emits each log line through
/// [`indicatif::MultiProgress::suspend`], so writing never overlaps a redraw of
/// the fetch progress bar.
struct SuspendingMakeWriter {
    /// Shared handle to the bar container; a clone of the one stored in
    /// [`PROGRESS`].
    progress: indicatif::MultiProgress,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SuspendingMakeWriter {
    type Writer = SuspendingWriter;

    fn make_writer(&'a self) -> Self::Writer {
        SuspendingWriter {
            progress: self.progress.clone(),
        }
    }
}

/// Per-event writer returned by [`SuspendingMakeWriter`]. Each `write` suspends
/// the shared [`indicatif::MultiProgress`] for the duration of the stderr write.
struct SuspendingWriter {
    /// Shared bar container to suspend while writing.
    progress: indicatif::MultiProgress,
}

impl std::io::Write for SuspendingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // With no active bar this is a plain stderr write; with the fetch bar
        // present, `suspend` clears it, writes the line, and redraws it.
        self.progress.suspend(|| std::io::stderr().write(buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stderr().flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_sources_stays_silent() {
        assert_eq!(filter_directive(None, None), None);
    }

    #[test]
    fn log_level_alone_builds_directive() {
        assert_eq!(
            filter_directive(None, Some(LogLevel::Debug)),
            Some("debug".to_string())
        );
    }

    #[test]
    fn rust_log_wins_over_log_level() {
        assert_eq!(
            filter_directive(Some("decdn_client=debug"), Some(LogLevel::Warn)),
            Some("decdn_client=debug".to_string())
        );
    }

    #[test]
    fn blank_rust_log_falls_back_to_log_level() {
        assert_eq!(
            filter_directive(Some("   "), Some(LogLevel::Info)),
            Some("info".to_string())
        );
    }

    #[test]
    fn blank_rust_log_with_no_level_stays_silent() {
        assert_eq!(filter_directive(Some(""), None), None);
    }

    #[test]
    fn malformed_rust_log_prefers_explicit_log_level() {
        assert_eq!(
            malformed_rust_log_fallback(Some(LogLevel::Debug)),
            LogLevel::Debug
        );
    }

    #[test]
    fn verbose_count_maps_to_levels() {
        assert_eq!(requested_level(None, 0), None);
        assert_eq!(requested_level(None, 1), Some(LogLevel::Info));
        assert_eq!(requested_level(None, 2), Some(LogLevel::Debug));
        assert_eq!(requested_level(None, 3), Some(LogLevel::Trace));
        assert_eq!(requested_level(None, 9), Some(LogLevel::Trace));
    }

    #[test]
    fn explicit_log_level_wins_over_verbose_count() {
        assert_eq!(
            requested_level(Some(LogLevel::Warn), 2),
            Some(LogLevel::Warn)
        );
    }

    #[test]
    fn rust_log_wins_over_verbose_count() {
        assert_eq!(
            filter_directive(Some("iroh=trace"), requested_level(None, 1)),
            Some("iroh=trace".to_string())
        );
    }

    #[test]
    fn malformed_rust_log_without_level_falls_back_to_info() {
        assert_eq!(malformed_rust_log_fallback(None), LogLevel::Info);
    }
}
