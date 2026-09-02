//! Pure rendering for `decdn node doctor` — grouped checklist and JSON.

use serde::Serialize;

use super::{Report, Severity};

/// Render `report` to `w`. `json = true` emits a single machine-readable
/// object; otherwise a grouped human checklist.
pub fn render<W: std::io::Write>(w: &mut W, report: &Report, json: bool) -> std::io::Result<()> {
    if json {
        render_json(w, report)
    } else {
        render_human(w, report)
    }
}

const fn symbol(sev: Severity) -> char {
    match sev {
        Severity::Pass => '\u{2714}', // ✔
        Severity::Warn => '\u{26a0}', // ⚠
        Severity::Fail => '\u{2716}', // ✖
    }
}

fn render_human<W: std::io::Write>(w: &mut W, report: &Report) -> std::io::Result<()> {
    writeln!(w, "decdn node doctor")?;
    let mut current: Option<&str> = None;
    for finding in &report.findings {
        if current != Some(finding.group) {
            writeln!(w, "\n{}", finding.group)?;
            current = Some(finding.group);
        }
        writeln!(w, "  {} {}", symbol(finding.severity), finding.title)?;
        if let Some(detail) = &finding.detail {
            writeln!(w, "        {detail}")?;
        }
        if let Some(rem) = &finding.remediation {
            writeln!(w, "        fix: {rem}")?;
        }
    }
    let (p, warn, f) = report.counts();
    writeln!(w, "\nSummary: {p} passed, {warn} warnings, {f} failed")
}

#[derive(Serialize)]
struct JsonReport<'a> {
    findings: &'a [super::Finding],
    summary: JsonSummary,
    ok: bool,
}

#[derive(Serialize)]
struct JsonSummary {
    pass: usize,
    warn: usize,
    fail: usize,
}

fn render_json<W: std::io::Write>(w: &mut W, report: &Report) -> std::io::Result<()> {
    let (pass, warn, fail) = report.counts();
    let doc = JsonReport {
        findings: &report.findings,
        summary: JsonSummary { pass, warn, fail },
        ok: fail == 0,
    };
    let text = serde_json::to_string_pretty(&doc).map_err(std::io::Error::other)?;
    writeln!(w, "{text}")
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests
mod tests {
    use super::*;
    use crate::commands::doctor::{Finding, Report, Severity};

    fn sample() -> Report {
        let mut r = Report::default();
        r.push(Finding {
            group: "Disk & cache",
            id: "disk.budget_vs_free",
            severity: Severity::Fail,
            title: "cache budget exceeds free disk".into(),
            detail: Some("cache_size_mb=10240 free_gib=3.1".into()),
            remediation: Some(
                "lower cache.cache_size_mb to <= 2700, or move cache.cache_dir".into(),
            ),
        });
        r
    }

    #[test]
    fn human_groups_and_summary() {
        let mut buf = Vec::new();
        render(&mut buf, &sample(), false).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("Disk & cache"));
        assert!(out.contains("cache budget exceeds free disk"));
        assert!(out.contains("cache_size_mb=10240"));
        assert!(out.contains("lower cache.cache_size_mb"));
        assert!(out.contains("Summary: 0 passed, 0 warnings, 1 failed"));
    }

    #[test]
    fn json_is_machine_readable() {
        let mut buf = Vec::new();
        render(&mut buf, &sample(), true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["summary"]["fail"], 1);
        assert_eq!(v["ok"], false);
        assert_eq!(v["findings"][0]["id"], "disk.budget_vs_free");
        assert_eq!(v["findings"][0]["severity"], "fail");
    }
}
