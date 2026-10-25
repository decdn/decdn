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
