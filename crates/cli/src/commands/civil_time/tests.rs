use super::*;

#[test]
fn civil_round_trips_across_epochs_and_leap_days() {
    for (y, m, d) in [
        (1970, 1, 1),
        (1969, 12, 31),
        (2000, 2, 29),
        (2024, 2, 29),
        (2026, 10, 25),
        (2038, 1, 19),
        (2100, 12, 31),
        (1600, 3, 1),
    ] {
        let days = days_from_civil(y, m, d);
        assert_eq!(civil_from_days(days), (y, m, d), "{y}-{m}-{d}");
    }
    assert_eq!(days_from_civil(1970, 1, 1), 0);
    assert_eq!(civil_from_days(0), (1970, 1, 1));
    assert_eq!(civil_from_days(-1), (1969, 12, 31));
}

#[test]
fn format_utc_renders_date_and_minutes() {
    assert_eq!(format_utc(0), "1970-01-01 00:00 UTC");
    // 2026-10-25 11:02:30 UTC
    assert_eq!(format_utc(1_792_926_150), "2026-10-25 11:02 UTC");
    assert_eq!(format_date(1_792_926_150), "2026-10-25");
}

#[test]
fn format_utc_token_has_no_spaces() {
    assert_eq!(format_utc_token(1_792_926_150), "2026-10-25T11:02Z");
    assert_eq!(format_utc_token(0), "1970-01-01T00:00Z");
}

#[test]
fn format_rfc3339_carries_seconds() {
    assert_eq!(format_rfc3339(1_792_926_150), "2026-10-25T11:02:30Z");
    assert_eq!(format_rfc3339(59), "1970-01-01T00:00:59Z");
}

#[test]
fn parse_rfc3339_reads_what_format_rfc3339_writes() {
    for unix in [0, 59, 1_792_926_150] {
        assert_eq!(parse_rfc3339(&format_rfc3339(unix)), Some(unix));
    }
    for bad in [
        "2026-10-25 11:02:30Z",
        "2026-10-25T11:02:30",
        "2026-02-30T00:00:00Z",
        "2026-10-25T24:00:00Z",
        "2026-10-25T11:02Z",
        "2026-10-25T-1:02:30Z",
        "2026-10-25T+1:02:30Z",
        "2026-10-25T11:02:60Z",
        "+202-10-25T11:02:30Z",
        "1969-12-31T23:59:59Z",
        "2026-02-29T00:00:00Z",
    ] {
        assert_eq!(parse_rfc3339(bad), None, "{bad}");
    }
    assert_eq!(
        parse_rfc3339("2028-02-29T00:00:00Z").map(format_rfc3339),
        Some("2028-02-29T00:00:00Z".to_owned()),
        "a leap day"
    );
}

#[test]
fn parse_since_refuses_a_signed_date() {
    assert!(parse_since("+202-10-25").is_err());
}

#[test]
fn parse_since_takes_a_date_or_unix_seconds() {
    assert_eq!(parse_since("1700000000").unwrap(), 1_700_000_000);
    assert_eq!(parse_since("1970-01-02").unwrap(), 86_400);
    assert_eq!(parse_since(" 2026-10-01 ").unwrap(), 1_790_812_800);
    assert_eq!(
        format_date(parse_since("2026-10-01").unwrap()),
        "2026-10-01"
    );
}

#[test]
fn parse_since_refuses_what_is_neither() {
    for bad in [
        "yesterday",
        "2026-13-01",
        "2026-02-30",
        "26-10-01",
        "1969-12-31",
        "-5",
        "20261001",
    ] {
        assert!(parse_since(bad).is_err(), "{bad}");
    }
}
