use super::*;
use decdn_protocol::SLASH_SIG_LEN;

fn fixture(ok: bool, total_bytes: Option<u64>) -> ProbeAnswer {
    ProbeAnswer {
        hash: [0xAB; 32],
        ok,
        rate_per_mb: 10,
        total_bytes,
        coverage_blocks: usize::from(ok),
        timestamp_us: 1_700_000_000_000_000,
        rtt_ms: 12.5,
        slash_sig: vec![0xCD; SLASH_SIG_LEN],
    }
}

/// JSON wire shape: every key the operator-facing `--json` contract
/// guarantees. Catches a renamed key, a dropped field, or a type drift.
#[test]
fn json_output_keys_and_types() {
    let mut buf: Vec<u8> = Vec::new();
    write_probe_answer(&mut buf, &fixture(true, Some(4096)), true).unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();

    let obj = parsed.as_object().expect("top level must be an object");
    assert_eq!(
        obj.len(),
        8,
        "eight keys: hash/ok/rate_per_mb/total_bytes/coverage_blocks/timestamp_us/rtt_ms/\
         slash_sig"
    );
    assert_eq!(obj["hash"].as_str().map(str::len), Some(64));
    assert_eq!(obj["ok"].as_bool(), Some(true));
    assert!(obj.get("has_blob").is_none());
    assert_eq!(obj["rate_per_mb"].as_u64(), Some(10));
    assert_eq!(obj["total_bytes"].as_u64(), Some(4096));
    assert_eq!(obj["coverage_blocks"].as_u64(), Some(1));
    assert_eq!(obj["timestamp_us"].as_u64(), Some(1_700_000_000_000_000));
    assert!(obj["rtt_ms"].is_number());
    assert_eq!(
        obj["slash_sig"].as_str().map(str::len),
        Some(SLASH_SIG_LEN * 2),
        "slash_sig is hex-encoded (2 chars/byte)"
    );
}

/// `total_bytes` serializes as JSON `null` for a refusal, which proves no
/// size.
#[test]
fn json_total_bytes_null_for_a_refusal() {
    let mut buf: Vec<u8> = Vec::new();
    write_probe_answer(&mut buf, &fixture(false, None), true).unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert!(parsed["total_bytes"].is_null());
    assert_eq!(parsed["ok"].as_bool(), Some(false));
    assert_eq!(parsed["coverage_blocks"].as_u64(), Some(0));
}

/// `rtt_ms` is quantized to microsecond precision. `12.5009` rounds to
/// `12.501`.
#[test]
fn json_rtt_ms_quantized() {
    let mut answer = fixture(true, None);
    answer.rtt_ms = 12.500_9;
    let mut buf: Vec<u8> = Vec::new();
    write_probe_answer(&mut buf, &answer, true).unwrap();
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
    let mut buf: Vec<u8> = Vec::new();
    write_probe_answer(&mut buf, &fixture(true, Some(1)), true).unwrap();
    let text = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        text.lines().count(),
        1,
        "expected one line of JSON, got {text:?}"
    );
}

/// Pretty output emits eight labelled lines in a stable order — operator
/// scripts grep for `ok:` / `rtt:` etc. without `--json`.
#[test]
fn pretty_output_emits_labelled_lines_in_stable_order() {
    let mut answer = fixture(true, Some(2048));
    answer.rtt_ms = 1.234;
    let mut buf: Vec<u8> = Vec::new();
    write_probe_answer(&mut buf, &answer, false).unwrap();
    let text = std::str::from_utf8(&buf).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 8);
    assert!(lines[0].starts_with("hash:"));
    assert!(lines[1].starts_with("ok:"));
    assert!(lines[2].starts_with("rate_per_mb:"));
    assert!(lines[3].starts_with("total_bytes:"));
    assert!(lines[4].starts_with("coverage_blocks:"));
    assert!(lines[5].starts_with("timestamp_us:"));
    assert!(lines[6].starts_with("rtt:"));
    assert!(lines[7].starts_with("slash_sig:"));
    assert!(
        lines[6].contains("1.234 ms"),
        "pretty rtt: line keeps 3-decimal format, got {:?}",
        lines[6],
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

/// `decdn probe` names one node: without `--node-id` it stops before it
/// resolves a chain or binds an endpoint.
#[tokio::test]
async fn a_probe_needs_a_node_id() {
    use clap::Parser as _;
    let cli =
        decdn_common::cli::Cli::try_parse_from(["decdn", "probe", "--hash", &"ab".repeat(32)])
            .unwrap();
    let decdn_common::cli::Command::Probe(args) = cli.command else {
        panic!("a probe command");
    };
    let err = probe(&args, None).await.unwrap_err();
    assert!(err.to_string().contains("--node-id"), "{err:#}");
}
