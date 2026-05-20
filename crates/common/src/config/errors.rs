//! Structured config-validation error accumulator.
//!
//! [`ConfigErrorBag`] is threaded through every section resolver during a
//! `resolve_config` pass — and through every reloadable section during a
//! SIGHUP reload (`runtime::reload`) — so every problem an operator made
//! surfaces in one bulleted `anyhow::Error`, not one per re-run.
//!
//! Each problem is recorded under a dotted field label (`blockchain.rpc_url`,
//! `cache.origins[2]`, ...) with the resolver's message text. Single-line
//! messages are reproduced verbatim; multi-line messages have their
//! continuation lines indented to stay under their bullet (see
//! [`ConfigErrorBag::into_result`]).

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

/// One resolved-config problem: a dotted field label (`blockchain.rpc_url`)
/// plus the resolver's message text.
#[derive(Debug)]
struct ConfigProblem {
    field: String,
    message: String,
}

/// Accumulates every problem found during a single `resolve_config` pass.
///
/// Threaded by `&mut` through the `*_into` section workers. Callers record
/// problems via [`check`](Self::check) (the `anyhow::ensure!` replacement)
/// and [`try_with`](Self::try_with) (the `?`/`.context()` replacement),
/// substituting a placeholder for any value they could not resolve so
/// later independent checks still run.
///
/// Insertion order is preserved end-to-end: [`into_result`](Self::into_result)
/// renders bullets in the same order they were [`push`](Self::push)ed, so
/// resolver authors can rely on operator-facing problem order matching the
/// order of validation logic, and tests that pin specific output order keep
/// working through future refactors.
#[derive(Debug)]
pub struct ConfigErrorBag {
    problems: Vec<ConfigProblem>,
}

impl Default for ConfigErrorBag {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigErrorBag {
    pub const fn new() -> Self {
        Self {
            problems: Vec::new(),
        }
    }

    /// Record a problem under `field` with `message`.
    pub fn push(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.problems.push(ConfigProblem {
            field: field.into(),
            message: message.into(),
        });
    }

    /// `anyhow::ensure!` replacement for a static message: record `message`
    /// under `field` when `cond` is false. Returns `cond` so callers can
    /// branch and skip a dependent check or substitute a placeholder.
    pub fn check(
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
    pub fn check_with(
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
    /// used (see `IDENTITY_REGION` / `IDENTITY_DATA_DIR` for the
    /// labels currently participating in guards).
    pub fn has_field(&self, field: &str) -> bool {
        self.problems.iter().any(|p| p.field == field)
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
pub(crate) fn one_section<T>(f: impl FnOnce(&mut ConfigErrorBag) -> T) -> anyhow::Result<T> {
    let mut bag = ConfigErrorBag::new();
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

    #[test]
    fn empty_bag_is_ok() {
        assert!(ConfigErrorBag::new().into_result().is_ok());
    }

    #[test]
    fn check_records_only_on_false_and_returns_cond() {
        let mut bag = ConfigErrorBag::new();
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
        let mut bag = ConfigErrorBag::new();
        let r: anyhow::Result<u8> =
            Err(anyhow::anyhow!("root cause")).map_err(|e| e.context("outer context"));
        assert_eq!(bag.try_with("x.y", r), None);
        let msg = format!("{:#}", bag.into_result().unwrap_err());
        assert!(msg.contains("outer context"), "{msg}");
        assert!(msg.contains("root cause"), "{msg}");
    }

    #[test]
    fn aggregates_all_problems_with_count_and_bullets() {
        let mut bag = ConfigErrorBag::new();
        bag.push("one.a", "first problem");
        bag.push("two.b", "second problem");
        let msg = format!("{:#}", bag.into_result().unwrap_err());
        assert!(msg.contains("configuration has 2 problem(s):"), "{msg}");
        assert!(msg.contains("  - one.a: first problem"), "{msg}");
        assert!(msg.contains("  - two.b: second problem"), "{msg}");
    }

    #[test]
    fn check_with_runs_closure_only_on_failure() {
        let mut bag = ConfigErrorBag::new();
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
        let mut bag = ConfigErrorBag::new();
        bag.push("x.y", "line one\nline two");
        let msg = format!("{:#}", bag.into_result().unwrap_err());
        assert!(msg.contains("  - x.y: line one\n    line two"), "{msg}");
    }

    #[test]
    fn has_field_matches_exact_label() {
        let mut bag = ConfigErrorBag::new();
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
