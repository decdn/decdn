use super::*;

#[test]
fn parses_bare_bytes() {
    assert_eq!(parse_byte_size("1048576").unwrap(), 1_048_576);
}

#[test]
fn parses_binary_units() {
    assert_eq!(parse_byte_size("4MiB").unwrap(), 4 * 1024 * 1024);
    assert_eq!(parse_byte_size("1M").unwrap(), 1024 * 1024);
    assert_eq!(parse_byte_size("2KB").unwrap(), 2048);
    assert_eq!(parse_byte_size("1G").unwrap(), 1024 * 1024 * 1024);
}

#[test]
fn parses_with_internal_whitespace_trimmed() {
    assert_eq!(parse_byte_size(" 4 MiB ").unwrap(), 4 * 1024 * 1024);
}

#[test]
fn rejects_empty() {
    assert!(parse_byte_size("").is_err());
    assert!(parse_byte_size("   ").is_err());
}

#[test]
fn rejects_garbage() {
    assert!(parse_byte_size("abc").is_err());
    assert!(parse_byte_size("4XiB").is_err());
    assert!(parse_byte_size("4.5MiB").is_err());
}

#[test]
fn rejects_overflow() {
    assert!(parse_byte_size("99999999999999999999G").is_err());
}
