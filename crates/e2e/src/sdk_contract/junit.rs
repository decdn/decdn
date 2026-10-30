//! The counts in a JUnit XML report, as pytest writes it with
//! `-o junit_family=xunit2`.
//!
//! The harness reads only the four counts on the first `<testsuite>` start
//! tag. That needs no XML parser: inside a tag, every `"` delimits a value,
//! because the writer escapes a quote in a value as `&quot;`.

use anyhow::Context;

/// The counts on a report's first `<testsuite>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Counts {
    /// The tests the suite collected.
    pub tests: u64,
    /// The tests whose assertions failed.
    pub failures: u64,
    /// The tests that raised outside an assertion, in a fixture for example.
    pub errors: u64,
    /// The tests the suite skipped.
    pub skipped: u64,
}

impl Counts {
    /// The tests that passed: the collected tests less the failed, errored
    /// and skipped ones, and 0 when those exceed the collected tests.
    pub(super) const fn passed(self) -> u64 {
        self.tests.saturating_sub(
            self.failures
                .saturating_add(self.errors)
                .saturating_add(self.skipped),
        )
    }
}

/// Read the counts on the first `<testsuite>` start tag in `xml`.
///
/// # Errors
///
/// Fails when `xml` has no `<testsuite>` start tag, when the tag has no
/// `>`, or when one of the four counts is missing or is not a number.
pub(super) fn counts(xml: &str) -> anyhow::Result<Counts> {
    // The trailing space keeps `<testsuites>` from matching. The tag body
    // keeps that space, so each attribute name follows a space.
    let rest = xml
        .find("<testsuite ")
        .and_then(|at| xml.get(at + "<testsuite".len()..))
        .context("the JUnit report has no <testsuite> element")?;
    let (tag, _) = rest
        .split_once('>')
        .context("the JUnit report's <testsuite> start tag has no `>`")?;
    let tag = tag.strip_suffix('/').unwrap_or(tag);
    Ok(Counts {
        tests: attr(tag, "tests")?,
        failures: attr(tag, "failures")?,
        errors: attr(tag, "errors")?,
        skipped: attr(tag, "skipped")?,
    })
}

/// The value of the ` name="digits"` attribute in `tag`, a start tag's body
/// after the element name.
fn attr(tag: &str, name: &str) -> anyhow::Result<u64> {
    // The leading space keeps `tests` from matching inside a longer name.
    let (_, rest) = tag
        .split_once(&format!(" {name}=\""))
        .with_context(|| format!("the JUnit <testsuite> has no {name} attribute"))?;
    let (value, _) = rest.split_once('"').with_context(|| {
        format!("the JUnit <testsuite>'s {name} attribute has no closing quote")
    })?;
    value
        .parse()
        .with_context(|| format!("the JUnit <testsuite>'s {name} is not a count: {value:?}"))
}
