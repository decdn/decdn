use super::*;

fn f(sev: Severity) -> Finding {
    Finding {
        group: "G",
        id: "x",
        severity: sev,
        title: "t".into(),
        detail: None,
        remediation: None,
    }
}

#[test]
fn counts_and_flags() {
    let mut r = Report::default();
    r.push(f(Severity::Pass));
    r.push(f(Severity::Warn));
    r.push(f(Severity::Fail));
    assert_eq!(r.counts(), (1, 1, 1));
    assert!(r.has_fail());
    assert!(r.has_warn());
}

#[test]
fn clean_report_has_no_fail_or_warn() {
    let mut r = Report::default();
    r.push(f(Severity::Pass));
    assert_eq!(r.counts(), (1, 0, 0));
    assert!(!r.has_fail());
    assert!(!r.has_warn());
}
