//! Structured config-resolution diagnostics: fatal problems and non-fatal
//! notices.
//!
//! [`ConfigDiagnostics`] is threaded through every section resolver during a
//! `resolve_config` pass — and through every reloadable section during a
//! SIGHUP reload (`runtime::reload`) — so every problem an operator made
//! surfaces in one bulleted `anyhow::Error`, not one per re-run.
//!
//! Each problem is recorded under a dotted field label (`blockchain.rpc_url`,
//! `cache.origins[2]`, ...) with the resolver's message text. Single-line
//! messages are reproduced verbatim; multi-line messages have their
//! continuation lines indented to stay under their bullet (see
//! [`ConfigDiagnostics::into_result`]).
//!
//! [`ConfigNotice`] carries the other half: a value that resolves fine but
//! that the operator should know about (a disabled cap, an unbounded map, a
//! retired env var). A notice never fails resolution. It rides the same bag
//! because the bag already reaches every resolver on both the startup and the
//! SIGHUP path, so the caller — which is the only layer that knows whether a
//! `tracing` subscriber exists yet — decides how to render it.

/// Field labels that participate in a `has_field` cascade-suppression guard.
///
/// These are the only labels whose exact string is load-bearing for
/// *correctness*: a producing-site `push`/`try_with` and a guarding-site
/// `has_field` (e.g. `resolve_identity_into` and
/// `ensure_region_when_publishing_global_into`) must agree byte-for-byte, or
/// the cascade-suppression guard silently degrades and a misleading second
/// error bubbles up. Naming them as constants makes that coupling
/// refactor-safe.
///
/// Add a new constant here only when introducing a new `has_field` guard.
/// The other ~60 non-guarded labels stay inline literals — they appear only
/// in operator output, never in a guard, so promoting them all would defeat
/// the point of singling these two out.
pub(crate) const IDENTITY_REGION: &str = "identity.region";
pub(crate) const IDENTITY_DATA_DIR: &str = "identity.data_dir";

/// How loud a [`ConfigNotice`] is.
///
/// The split is the operator's, not the resolver's: `Warn` marks a value that
/// weakens a safety property (an unbounded map, a collapsed failover list),
/// `Info` marks a deliberate opt-out that is working as configured (a rate
/// limit switched off). A consumer that gates an exit status or an alert on
/// severity keys off this rather than parsing the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigNoticeLevel {
    /// A deliberate, documented opt-out. Nothing is wrong.
    Info,
    /// The configured value weakens a safety property. Resolution still
    /// succeeds — the operator asked for it — but it warrants attention.
    Warn,
}

/// One non-fatal resolve-time notice: a severity, the label of whatever the
/// operator wrote, and the operator-facing message text.
///
/// `field` names the **source**, not always a config key: usually a dotted
/// config label (`security.max_tracked_sources`), matching the problem-label
/// convention so a notice and a problem about the same field read the same
/// way, but a notice about a retired environment variable carries the bare var
/// name (`DECDN_DELIVERY_CEILING`) because that is the thing the operator set
/// and must unset. A consumer that filters on `field` — the JSON log stream,
/// a `doctor` finding — must not assume a dotted shape.
///
/// Either way `field` is rendered as a structured value rather than embedded
/// in prose, so the message does not repeat the label.
#[derive(Debug, Clone)]
pub struct ConfigNotice {
    /// How loud this notice is.
    pub level: ConfigNoticeLevel,
    /// What the operator wrote: a dotted config label, or an environment
    /// variable name for a notice about one.
    pub field: String,
    /// Operator-facing message text, without a severity prefix.
    pub message: String,
}

/// One resolved-config problem: a dotted field label (`blockchain.rpc_url`)
/// plus the resolver's message text.
#[derive(Debug)]
struct ConfigProblem {
    field: String,
    message: String,
}

/// Accumulates every problem and every non-fatal notice found during a single
/// `resolve_config` pass.
///
/// Threaded by `&mut` through the `*_into` section workers. Callers record
/// problems via `check` (the `anyhow::ensure!` replacement) and
/// [`try_with`](Self::try_with) (the `?`/`.context()` replacement),
/// substituting a placeholder for any value they could not resolve so
/// later independent checks still run. They record notices via `warn` / `note`.
///
/// Insertion order is preserved end-to-end: [`into_result`](Self::into_result)
/// renders bullets in the same order they were `push`ed, so
/// resolver authors can rely on operator-facing problem order matching the
/// order of validation logic, and tests that pin specific output order keep
/// working through future refactors. [`take_notices`](Self::take_notices)
/// preserves notice order for the same reason.
///
/// Notices and problems are independent: a bag carrying only notices still
/// collapses to `Ok(())`, and a bag that fails resolution may still hold
/// notices its caller chooses not to render.
#[derive(Debug)]
pub struct ConfigDiagnostics {
    problems: Vec<ConfigProblem>,
    notices: Vec<ConfigNotice>,
}

impl Default for ConfigDiagnostics {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigDiagnostics {
    /// An empty bag. Resolution fills it and reports every problem at once,
    /// rather than failing on the first.
    pub const fn new() -> Self {
        Self {
            problems: Vec::new(),
            notices: Vec::new(),
        }
    }

    /// Record a problem under `field` with `message`.
    pub(crate) fn push(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.problems.push(ConfigProblem {
            field: field.into(),
            message: message.into(),
        });
    }

    /// Record a [`ConfigNoticeLevel::Warn`] notice under `field`.
    ///
    /// Non-fatal by construction — the bag still collapses to `Ok(())`. Use it
    /// where the operator asked for something legal that weakens a safety
    /// property, and refusing to boot over it would be the worse failure.
    pub(crate) fn warn(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.notices.push(ConfigNotice {
            level: ConfigNoticeLevel::Warn,
            field: field.into(),
            message: message.into(),
        });
    }

    /// Record a [`ConfigNoticeLevel::Info`] notice under `field`. The `Info`
    /// twin of [`warn`](Self::warn), for a deliberate opt-out that is working
    /// exactly as configured.
    pub(crate) fn note(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.notices.push(ConfigNotice {
            level: ConfigNoticeLevel::Info,
            field: field.into(),
            message: message.into(),
        });
    }

    /// Take every notice recorded so far, leaving the bag's notice list empty.
    ///
    /// Separate from [`into_result`](Self::into_result) — which consumes the
    /// bag — because the caller needs the notices whether resolution succeeded
    /// or not, and must drain them before collapsing the problems.
    pub fn take_notices(&mut self) -> Vec<ConfigNotice> {
        std::mem::take(&mut self.notices)
    }

    /// `anyhow::ensure!` replacement for a static message: record `message`
    /// under `field` when `cond` is false. Returns `cond` so callers can
    /// branch and skip a dependent check or substitute a placeholder.
    pub(crate) fn check(
        &mut self,
        cond: bool,
        field: impl Into<String>,
        message: impl Into<String>,
    ) -> bool {
        if !cond {
            self.push(field, message);
        }
        cond
    }

    /// Lazy variant of [`check`](Self::check): the message closure runs only
    /// when `cond` is false, matching `anyhow::ensure!`'s format-on-failure
    /// behaviour so the happy path allocates nothing for `format!(...)`
    /// messages.
    pub(crate) fn check_with(
        &mut self,
        cond: bool,
        field: impl Into<String>,
        message: impl FnOnce() -> String,
    ) -> bool {
        if !cond {
            self.push(field, message());
        }
        cond
    }

    /// `?` / `.context()` replacement: on `Err`, record the full `{e:#}`
    /// alternate-form context chain under `field` and return `None`; on
    /// `Ok`, return `Some(value)`.
    pub fn try_with<T>(
        &mut self,
        field: impl Into<String>,
        result: anyhow::Result<T>,
    ) -> Option<T> {
        match result {
            Ok(v) => Some(v),
            Err(e) => {
                self.push(field, format!("{e:#}"));
                None
            }
        }
    }

    /// Whether any problem has already been recorded for `field`.
    ///
    /// Match is exact-string, not prefix: a problem recorded under
    /// `cache.origins[0]` does *not* make `has_field("cache.origins")`
    /// true. Cascade guards must use the same label the producing site
    /// used (see [`IDENTITY_REGION`] / [`IDENTITY_DATA_DIR`] for the
    /// labels currently participating in guards).
    pub(crate) fn has_field(&self, field: &str) -> bool {
        self.problems.iter().any(|p| p.field == field)
    }

    /// Number of problems recorded. Read before
    /// [`into_result`](Self::into_result) when the count is needed
    /// alongside the consumed error (e.g. as a structured
    /// `problem_count` log field).
    pub const fn problem_count(&self) -> usize {
        self.problems.len()
    }

    /// Collapse the bag into a single `anyhow::Error` listing every problem
    /// as a `  - <field>: <message>` bullet, or `Ok(())` when empty.
    /// Single-line messages are reproduced verbatim; continuation lines of
    /// a multi-line message are indented to stay under their bullet.
    ///
    /// Only `\n` is replaced — `\r\n` continuations have their `\n` re-indented
    /// but the carriage return is left in place. An empty `message` renders as
    /// `  - <field>: `, and a trailing `\n` becomes a dangling-indent blank
    /// line; neither shape is produced by the current resolvers but a future
    /// `{e:#}` source whose `Display` ends in a newline would surface it.
    pub fn into_result(self) -> anyhow::Result<()> {
        if self.problems.is_empty() {
            return Ok(());
        }
        let n = self.problems.len();
        let bullets = self
            .problems
            .iter()
            .map(|p| {
                // Indent any continuation lines so a multi-line message
                // (e.g. a future `{e:#}` chain that wraps) still reads as
                // one bullet rather than dedenting to the margin.
                let message = p.message.replace('\n', "\n    ");
                format!("  - {}: {}", p.field, message)
            })
            .collect::<Vec<_>>()
            .join("\n");
        Err(anyhow::anyhow!(
            "configuration has {n} problem(s):\n{bullets}"
        ))
    }
}

/// Wraps a section `*_into` worker so a single-section caller (a
/// `runtime::reload` path or a `#[cfg(test)]` shim) gets back an
/// `anyhow::Result<T>`.
/// Cross-section aggregation lives in `resolve_config`, which runs the
/// workers against one shared bag instead.
///
/// Notices are dropped: every caller of this wrapper is a `#[cfg(test)]` shim
/// or an internal shape check, none of which is the operator-facing path a
/// notice exists for. The two paths that do render notices —
/// `resolve_config` and `runtime::reload` — drive the workers against their
/// own bag and drain it with [`ConfigDiagnostics::take_notices`].
pub(crate) fn one_section<T>(f: impl FnOnce(&mut ConfigDiagnostics) -> T) -> anyhow::Result<T> {
    let mut bag = ConfigDiagnostics::new();
    let value = f(&mut bag);
    bag.into_result()?;
    Ok(value)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    /// Notices and problems are independent channels. A bag holding only
    /// notices must still resolve — the whole point of the notice channel is
    /// that it never fails a config an operator deliberately asked for.
    #[test]
    fn notices_alone_do_not_fail_resolution() {
        let mut bag = ConfigDiagnostics::new();
        bag.warn("security.max_tracked_sources", "0: unbounded");
        bag.note("security.per_source_rate_per_sec", "0: disabled");
        assert_eq!(bag.problem_count(), 0, "a notice is not a problem");
        assert!(bag.into_result().is_ok());
    }

    /// Notice order is load-bearing for the same reason problem order is: an
    /// operator reads them against the order of the validation logic.
    #[test]
    fn take_notices_drains_in_insertion_order() {
        let mut bag = ConfigDiagnostics::new();
        bag.warn("b.second", "second");
        bag.note("a.first", "first");
        bag.warn("c.third", "third");

        let notices = bag.take_notices();
        let fields: Vec<&str> = notices.iter().map(|n| n.field.as_str()).collect();
        assert_eq!(fields, ["b.second", "a.first", "c.third"]);
        assert_eq!(
            notices.iter().map(|n| n.level).collect::<Vec<_>>(),
            [
                ConfigNoticeLevel::Warn,
                ConfigNoticeLevel::Info,
                ConfigNoticeLevel::Warn
            ]
        );
        assert!(
            bag.take_notices().is_empty(),
            "take must drain, not clone — a second caller would double-report"
        );
    }

    /// Draining notices must not disturb the problem channel: `resolve_config`
    /// and `runtime::reload` both `take_notices()` immediately before
    /// `into_result()`, so a bag that fails must still fail identically.
    #[test]
    fn taking_notices_leaves_problems_intact() {
        let mut bag = ConfigDiagnostics::new();
        bag.push("blockchain.rpc_url", "missing");
        bag.warn("security.max_tracked_sources", "0: unbounded");

        assert_eq!(bag.take_notices().len(), 1);
        assert_eq!(bag.problem_count(), 1);
        let err = bag.into_result().expect_err("the problem still fails");
        assert!(format!("{err:#}").contains("blockchain.rpc_url"));
    }

    #[test]
    fn empty_bag_is_ok() {
        assert!(ConfigDiagnostics::new().into_result().is_ok());
    }

    #[test]
    fn check_records_only_on_false_and_returns_cond() {
        let mut bag = ConfigDiagnostics::new();
        assert!(bag.check(true, "a.b", "should not appear"));
        assert!(!bag.check(false, "a.b", "boom"));
        let msg = format!("{:#}", bag.into_result().unwrap_err());
        // Exactly one problem — proves the `true` branch did not push.
        assert!(msg.contains("1 problem(s):"), "{msg}");
        assert!(!msg.contains("should not appear"), "{msg}");
        assert!(msg.contains("a.b: boom"), "{msg}");
    }

    #[test]
    fn try_with_preserves_context_chain() {
        let mut bag = ConfigDiagnostics::new();
        let r: anyhow::Result<u8> =
            Err(anyhow::anyhow!("root cause")).map_err(|e| e.context("outer context"));
        assert_eq!(bag.try_with("x.y", r), None);
        let msg = format!("{:#}", bag.into_result().unwrap_err());
        assert!(msg.contains("outer context"), "{msg}");
        assert!(msg.contains("root cause"), "{msg}");
    }

    #[test]
    fn aggregates_all_problems_with_count_and_bullets() {
        let mut bag = ConfigDiagnostics::new();
        bag.push("one.a", "first problem");
        bag.push("two.b", "second problem");
        let msg = format!("{:#}", bag.into_result().unwrap_err());
        assert!(msg.contains("configuration has 2 problem(s):"), "{msg}");
        assert!(msg.contains("  - one.a: first problem"), "{msg}");
        assert!(msg.contains("  - two.b: second problem"), "{msg}");
    }

    #[test]
    fn check_with_runs_closure_only_on_failure() {
        let mut bag = ConfigDiagnostics::new();
        let mut calls = 0;
        assert!(bag.check_with(true, "a.b", || {
            calls += 1;
            "unreachable".to_string()
        }));
        assert_eq!(calls, 0);
        assert!(!bag.check_with(false, "a.b", || {
            calls += 1;
            format!("boom {}", 42)
        }));
        assert_eq!(calls, 1);
        let msg = format!("{:#}", bag.into_result().unwrap_err());
        assert!(msg.contains("a.b: boom 42"), "{msg}");
    }

    #[test]
    fn into_result_indents_multiline_message_continuations() {
        let mut bag = ConfigDiagnostics::new();
        bag.push("x.y", "line one\nline two");
        let msg = format!("{:#}", bag.into_result().unwrap_err());
        assert!(msg.contains("  - x.y: line one\n    line two"), "{msg}");
    }

    #[test]
    fn has_field_matches_exact_label() {
        let mut bag = ConfigDiagnostics::new();
        bag.push("identity.region", "bad region");
        assert!(bag.has_field("identity.region"));
        assert!(!bag.has_field("identity"));
        assert!(!bag.has_field("identity.region.extra"));
    }

    #[test]
    fn one_section_returns_value_when_clean_and_error_when_not() {
        let ok: anyhow::Result<u32> = one_section(|_bag| 42);
        assert_eq!(ok.unwrap(), 42);

        let bad: anyhow::Result<u32> = one_section(|bag| {
            bag.push("s.f", "nope");
            7
        });
        let msg = format!("{:#}", bad.unwrap_err());
        assert!(msg.contains("s.f"), "{msg}");
        assert!(msg.contains("nope"), "{msg}");
    }
}
