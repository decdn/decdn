//! Structured config-validation error accumulator (issue #222).
//!
//! `resolve_config` used to fail fast on the first problem: an operator
//! with three mistakes fixed one, re-ran, hit the next, re-ran again.
//! [`ConfigErrorBag`] collects every problem found in a single resolution
//! pass so the whole list is reported at once.
//!
//! The bag preserves each fail-fast resolver's original message text
//! verbatim (as a bullet under a dotted field label), so existing
//! `.contains("substring")` assertions keep matching.

/// One resolved-config problem: a dotted field label (`blockchain.rpc_url`)
/// plus the verbatim message the fail-fast resolver used to return.
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
pub(crate) struct ConfigErrorBag {
    problems: Vec<ConfigProblem>,
}

impl ConfigErrorBag {
    pub(crate) const fn new() -> Self {
        Self {
            problems: Vec::new(),
        }
    }

    /// Record a problem under `field` with `message`.
    pub(crate) fn push(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.problems.push(ConfigProblem {
            field: field.into(),
            message: message.into(),
        });
    }

    /// `anyhow::ensure!` replacement: record `message` under `field` when
    /// `cond` is false. Returns `cond` so callers can branch and skip a
    /// dependent check or substitute a placeholder.
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

    /// `?` / `.context()` replacement: on `Err`, record the full `{e:#}`
    /// context chain under `field` and return `None`; on `Ok`, return
    /// `Some(value)`. The `{e:#}` form matches what `?` propagation +
    /// `format!("{err:#}")` produced before, so message text is unchanged.
    pub(crate) fn try_with<T>(
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

    /// Whether any problem has already been recorded for `field` (exact
    /// match). Used to suppress a misleading cascade error on a field that
    /// already failed an earlier check (e.g. region present-but-invalid
    /// must not also trigger "region must be set when `subscribe_global`").
    pub(crate) fn has_field(&self, field: &str) -> bool {
        self.problems.iter().any(|p| p.field == field)
    }

    #[cfg(test)]
    pub(crate) const fn is_empty(&self) -> bool {
        self.problems.is_empty()
    }

    /// Collapse the bag into a single `anyhow::Error` listing every problem
    /// as a `  - <field>: <message>` bullet, or `Ok(())` when empty. Each
    /// original message appears verbatim so existing substring assertions
    /// keep matching.
    pub(crate) fn into_result(self) -> anyhow::Result<()> {
        if self.problems.is_empty() {
            return Ok(());
        }
        let n = self.problems.len();
        let bullets = self
            .problems
            .iter()
            .map(|p| format!("  - {}: {}", p.field, p.message))
            .collect::<Vec<_>>()
            .join("\n");
        Err(anyhow::anyhow!(
            "configuration has {n} problem(s):\n{bullets}"
        ))
    }
}

/// Run a single-section resolver worker with its own private bag,
/// preserving the existing `anyhow::Result` public signature that
/// `runtime::reload` and the unit tests call directly. Cross-section
/// aggregation happens in `resolve_config`, which drives the `*_into`
/// workers with one shared bag instead of going through this shim.
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
        let bag = ConfigErrorBag::new();
        assert!(bag.is_empty());
        assert!(bag.into_result().is_ok());
    }

    #[test]
    fn check_records_only_on_false_and_returns_cond() {
        let mut bag = ConfigErrorBag::new();
        assert!(bag.check(true, "a.b", "should not appear"));
        assert!(bag.is_empty());
        assert!(!bag.check(false, "a.b", "boom"));
        assert!(!bag.is_empty());
        let msg = format!("{:#}", bag.into_result().unwrap_err());
        assert!(msg.contains("a.b"), "{msg}");
        assert!(msg.contains("boom"), "{msg}");
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
