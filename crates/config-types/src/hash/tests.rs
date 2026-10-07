use super::*;

#[test]
fn hex_round_trips() {
    let h = Hash::from_bytes([0xab; 32]);
    let hex = h.to_hex();
    assert_eq!(hex.len(), 64);
    assert!(hex.chars().all(|c| c == 'a' || c == 'b'));
    let back: Hash = hex.parse().expect("round-trip");
    assert_eq!(h, back);
}

#[test]
fn display_and_debug_match_to_hex() {
    // `Display`/`Debug` format hex independently of `to_hex()` (the
    // serde path) — pin their equivalence so the two impls cannot
    // silently drift. A divergence would split the operator-facing
    // wire form: admin JSON-RPC (serde → `to_hex`) vs tracing
    // `%hash` (`Display`). Cover a leading-zero byte and `0xff`.
    let mut bytes = [0xffu8; 32];
    if let Some(first) = bytes.first_mut() {
        *first = 0x00;
    }
    for h in [
        Hash::from_bytes([0u8; 32]),
        Hash::from_bytes([0xab; 32]),
        Hash::from_bytes(bytes),
    ] {
        assert_eq!(h.to_string(), h.to_hex(), "Display must equal to_hex()");
        assert_eq!(format!("{h:?}"), h.to_hex(), "Debug must equal to_hex()");
    }
}

#[test]
fn hex_is_lowercase_and_full_width() {
    // Leading zero byte must still produce two chars (no trimming).
    let mut bytes = [0u8; 32];
    if let Some(last) = bytes.last_mut() {
        *last = 0x0f;
    }
    let h = Hash::from_bytes(bytes);
    let hex = h.to_hex();
    assert_eq!(hex.len(), 64);
    assert!(hex.starts_with("00"));
    assert!(hex.ends_with("0f"));
}

#[test]
fn serde_is_lowercase_hex_string() {
    let h = Hash::from_bytes([0xab; 32]);
    let json = serde_json::to_string(&h).expect("serialise");
    assert_eq!(json, format!("\"{}\"", "ab".repeat(32)));
    let back: Hash = serde_json::from_str(&json).expect("deserialise");
    assert_eq!(h, back);
}

#[test]
fn from_str_accepts_mixed_case() {
    let lower = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
    let upper = lower.to_uppercase();
    assert_eq!(
        lower.parse::<Hash>().expect("lower"),
        upper.parse::<Hash>().expect("upper")
    );
}

#[test]
fn from_str_rejects_bad_length() {
    assert_eq!(
        "abcd".parse::<Hash>().unwrap_err(),
        HashParseError::BadLength { got: 4 }
    );
    assert!(matches!(
        "".parse::<Hash>(),
        Err(HashParseError::BadLength { got: 0 })
    ));
}

#[test]
fn from_str_rejects_non_hex() {
    let bad = "z".repeat(64);
    assert_eq!(bad.parse::<Hash>().unwrap_err(), HashParseError::NonHexChar);
}

#[test]
fn pinned_hashes_diff_counts_added_and_removed() {
    // Direct unit test of the diff helper, independent of any engine
    // swap path. Locks the API: a future caller stitching log
    // messages from `PinDiff` shouldn't break silently if the
    // counting changes shape.
    let h1 = Hash::from_bytes([1; 32]);
    let h2 = Hash::from_bytes([2; 32]);
    let h3 = Hash::from_bytes([3; 32]);

    let prev = PinnedHashes::new([h1, h2].into_iter().collect());
    let new = PinnedHashes::new([h2, h3].into_iter().collect());

    let diff = new.diff(&prev);
    assert!(diff.added == 1 && diff.removed == 1, "got {diff:?}");

    let no_change = new.diff(&new);
    assert!(no_change.added == 0 && no_change.removed == 0);
}

#[test]
fn pinned_hashes_empty_and_contains() {
    let empty = PinnedHashes::empty();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    let h = Hash::from_bytes([7; 32]);
    let set = PinnedHashes::new([h].into_iter().collect());
    assert!(set.contains(&h));
    assert_eq!(set.len(), 1);
}
