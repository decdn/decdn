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

#[test]
fn display_delegates_to_inner() {
    assert_eq!(Bytes::new(256).to_string(), "256");
}

#[test]
fn saturating_add_saturates_at_max() {
    assert_eq!(Bytes::new(2).saturating_add(Bytes::new(3)), Bytes::new(5));
    assert_eq!(
        Bytes::new(u64::MAX).saturating_add(Bytes::new(1)),
        Bytes::new(u64::MAX)
    );
}

#[test]
fn saturating_sub_saturates_at_zero() {
    assert_eq!(Bytes::new(5).saturating_sub(Bytes::new(3)), Bytes::new(2));
    assert_eq!(Bytes::new(3).saturating_sub(Bytes::new(5)), Bytes::new(0));
}

#[test]
fn is_zero_tracks_the_sentinel() {
    assert!(Bytes::new(0).is_zero());
    assert!(Bytes::default().is_zero());
    assert!(!Bytes::new(1).is_zero());
}
