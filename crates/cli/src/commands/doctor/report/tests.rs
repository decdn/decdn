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
        remediation: Some("lower cache.cache_size_mb to <= 2700, or move cache.cache_dir".into()),
    });
    r
}

#[test]
fn human_groups_and_summary() {
    let mut buf = Vec::new();
    render(&mut buf, &sample(), false, false).unwrap();
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
    render(&mut buf, &sample(), true, false).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["summary"]["fail"], 1);
    assert_eq!(v["ok"], false);
    assert_eq!(v["findings"][0]["id"], "disk.budget_vs_free");
    assert_eq!(v["findings"][0]["severity"], "fail");
}

fn warn_only_report() -> Report {
    let mut r = Report::default();
    r.push(Finding {
        group: "Disk & cache",
        id: "disk.thin",
        severity: Severity::Warn,
        title: "free disk is low".into(),
        detail: None,
        remediation: None,
    });
    r
}

#[test]
fn json_ok_reflects_strict_on_warn_only_report() {
    let report = warn_only_report();

    let mut buf = Vec::new();
    render(&mut buf, &report, true, false).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["ok"], true, "warn-only report is ok without --strict");

    let mut buf = Vec::new();
    render(&mut buf, &report, true, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["ok"], false, "warn-only report is not ok under --strict");
}
