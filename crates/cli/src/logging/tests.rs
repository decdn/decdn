use super::*;

#[test]
fn no_sources_stays_silent() {
    assert_eq!(filter_directive(None, None), None);
}

#[test]
fn log_level_alone_builds_directive() {
    assert_eq!(
        filter_directive(None, Some(LogLevel::Debug)),
        Some("debug".to_string())
    );
}

#[test]
fn rust_log_wins_over_log_level() {
    assert_eq!(
        filter_directive(Some("decdn_client=debug"), Some(LogLevel::Warn)),
        Some("decdn_client=debug".to_string())
    );
}

#[test]
fn blank_rust_log_falls_back_to_log_level() {
    assert_eq!(
        filter_directive(Some("   "), Some(LogLevel::Info)),
        Some("info".to_string())
    );
}

#[test]
fn blank_rust_log_with_no_level_stays_silent() {
    assert_eq!(filter_directive(Some(""), None), None);
}

#[test]
fn malformed_rust_log_prefers_explicit_log_level() {
    assert_eq!(
        malformed_rust_log_fallback(Some(LogLevel::Debug)),
        LogLevel::Debug
    );
}

#[test]
fn verbose_count_maps_to_levels() {
    assert_eq!(requested_level(None, 0), None);
    assert_eq!(requested_level(None, 1), Some(LogLevel::Info));
    assert_eq!(requested_level(None, 2), Some(LogLevel::Debug));
    assert_eq!(requested_level(None, 3), Some(LogLevel::Trace));
    assert_eq!(requested_level(None, 9), Some(LogLevel::Trace));
}

#[test]
fn explicit_log_level_wins_over_verbose_count() {
    assert_eq!(
        requested_level(Some(LogLevel::Warn), 2),
        Some(LogLevel::Warn)
    );
}

#[test]
fn rust_log_wins_over_verbose_count() {
    assert_eq!(
        filter_directive(Some("iroh=trace"), requested_level(None, 1)),
        Some("iroh=trace".to_string())
    );
}

#[test]
fn malformed_rust_log_without_level_falls_back_to_info() {
    assert_eq!(malformed_rust_log_fallback(None), LogLevel::Info);
}
