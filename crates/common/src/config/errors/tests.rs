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
