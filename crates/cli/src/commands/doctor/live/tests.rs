use super::*;

#[test]
fn parses_gauge_line() {
    let body = "# HELP decdn_cache_bytes bytes\n# TYPE decdn_cache_bytes gauge\ndecdn_cache_bytes 1048576\n";
    assert_eq!(parse_cache_bytes(body), Some(1_048_576));
}

#[test]
fn ignores_comments_and_missing() {
    assert_eq!(parse_cache_bytes("# decdn_cache_bytes 5\nother 3\n"), None);
}

#[test]
fn handles_float_value() {
    assert_eq!(parse_cache_bytes("decdn_cache_bytes 2.0\n"), Some(2));
}

#[test]
fn handles_labelled_line() {
    assert_eq!(
        parse_cache_bytes("decdn_cache_bytes{node=\"a\"} 123\n"),
        Some(123)
    );
}

#[test]
fn handles_trailing_timestamp() {
    assert_eq!(
        parse_cache_bytes("decdn_cache_bytes 123 1712345\n"),
        Some(123)
    );
}

#[test]
fn ignores_prefix_collision_series() {
    assert_eq!(
        parse_cache_bytes("decdn_cache_bytes_total 999\ndecdn_cache_bytes_returned_total 5\n"),
        None
    );
}
