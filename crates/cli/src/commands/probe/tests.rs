use super::*;
use decdn_protocol::SLASH_SIG_LEN;
use decdn_protocol::message::{ProbeResponse, ProbeResponseBody};
use std::assert_matches;

fn fixture(rate_per_mb: u64, has_blob: bool) -> ProbeResponse {
    ProbeResponse {
        body: ProbeResponseBody {
            hash: [0xAB; 32],
            has_blob,
            rate_per_mb,
            timestamp_us: 1_700_000_000_000_000,
        },
        slash_sig: vec![0xCD; SLASH_SIG_LEN],
    }
}

/// #252: the requester gate (`ProbeResponse::validate`, invoked on the
/// receive path in `run_probe`) MUST reject a zero `rate_per_mb` — a
/// zero-rate node trivially wins selection while earning nothing, so it is
/// an obvious misconfiguration the client refuses. The upper `MAX_RATE_PER_MB`
/// bound is enforced at decode time; zero is the requester-side obligation.
#[test]
fn validate_rejects_zero_rate_response() {
    let resp = fixture(0, true);
    let err = resp.validate().expect_err("zero rate must be rejected");
    assert_matches!(
        err,
        decdn_protocol::MessageValidationError::RateIsZero,
        "expected RateIsZero, got {err:?}"
    );
}

/// JSON wire shape: every key the operator-facing `--json` contract
/// guarantees. Catches a renamed key, a dropped field, or a type drift. A
/// probe carries no blob size, so neither does the output.
#[test]
fn json_output_keys_and_types() {
    let resp = fixture(10, true);
    let mut buf: Vec<u8> = Vec::new();
    write_probe_response(&mut buf, &resp, 12.5, true).unwrap();
    let text = std::str::from_utf8(&buf).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(text).unwrap();

    assert!(parsed.is_object(), "top level must be an object");
    let obj = parsed.as_object().unwrap();
    assert_eq!(
        obj.len(),
        6,
        "six keys: hash/has_blob/rate_per_mb/timestamp_us/rtt_ms/slash_sig"
    );
    assert_eq!(obj.get("hash").unwrap().as_str().map(str::len), Some(64));
    assert_eq!(obj.get("has_blob").unwrap().as_bool(), Some(true));
    assert_eq!(obj.get("rate_per_mb").unwrap().as_u64(), Some(10));
    assert!(obj.get("total_bytes").is_none(), "no size is printed");
    assert_eq!(
        obj.get("timestamp_us").unwrap().as_u64(),
        Some(1_700_000_000_000_000)
    );
    assert!(obj.get("rtt_ms").unwrap().is_number());
    assert_eq!(
        obj.get("slash_sig").unwrap().as_str().map(str::len),
        Some(SLASH_SIG_LEN * 2),
        "slash_sig is hex-encoded (2 chars/byte)"
    );
}

/// `rtt_ms` is quantized to ms precision. `12.5009` rounds to `12.501`.
#[test]
fn json_rtt_ms_quantized_to_ms_precision() {
    let resp = fixture(10, true);
    let mut buf: Vec<u8> = Vec::new();
    write_probe_response(&mut buf, &resp, 12.500_9, true).unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    let rtt = parsed["rtt_ms"].as_f64().unwrap();
    assert!(
        (rtt - 12.501).abs() < 1e-9,
        "expected rtt_ms ≈ 12.501, got {rtt}"
    );
}

/// `--json` output is single-line — operator scripts pipe through `jq`
/// without `-s`/slurp.
#[test]
fn json_output_is_single_line() {
    let resp = fixture(10, true);
    let mut buf: Vec<u8> = Vec::new();
    write_probe_response(&mut buf, &resp, 1.0, true).unwrap();
    let text = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        text.lines().count(),
        1,
        "expected one line of JSON, got {text:?}"
    );
}

/// Pretty output emits six labelled lines in a stable order — operator
/// scripts grep for `has_blob:` / `rtt:` etc. without `--json`.
#[test]
fn pretty_output_emits_labelled_lines_in_stable_order() {
    let resp = fixture(7, true);
    let mut buf: Vec<u8> = Vec::new();
    write_probe_response(&mut buf, &resp, 1.234, false).unwrap();
    let text = std::str::from_utf8(&buf).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 6);
    assert!(lines[0].starts_with("hash:"));
    assert!(lines[1].starts_with("has_blob:"));
    assert!(lines[2].starts_with("rate_per_mb:"));
    assert!(lines[3].starts_with("timestamp_us:"));
    assert!(lines[4].starts_with("rtt:"));
    assert!(lines[5].starts_with("slash_sig:"));
    assert!(
        lines[4].contains("1.234 ms"),
        "pretty rtt: line keeps 3-decimal format, got {:?}",
        lines[4],
    );
}

#[test]
fn parse_hash_accepts_hex_with_and_without_prefix() {
    let hex = "ab".repeat(32);
    let a = parse_hash(&hex).unwrap();
    let b = parse_hash(&format!("0x{hex}")).unwrap();
    assert_eq!(a, b);
    assert_eq!(a, [0xABu8; 32]);
}

#[test]
fn parse_hash_rejects_wrong_length() {
    assert!(parse_hash("abc").is_err());
    assert!(parse_hash(&"ab".repeat(33)).is_err());
}
