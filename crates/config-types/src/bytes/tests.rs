use super::*;

#[test]
fn new_and_get_round_trip() {
    assert_eq!(Bytes::new(1_048_576).get(), 1_048_576);
    assert_eq!(Bytes::default().get(), 0);
}

#[test]
fn serde_is_a_bare_integer() {
    // Transparent: serializes to a bare integer, not a wrapper object.
    let json = serde_json::to_string(&Bytes::new(2_097_152)).expect("serialize");
    assert_eq!(json, "2097152");
    let back: Bytes = serde_json::from_str("2097152").expect("deserialize");
    assert_eq!(back, Bytes::new(2_097_152));
}
