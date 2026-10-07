use super::*;

const LOCAL: B256 = B256::repeat_byte(0xAA);

#[test]
fn matching_binding_is_bound_and_echoes_the_id() {
    let r = classify(LOCAL, LOCAL);
    assert_eq!(r.status, BindingStatus::Bound);
    assert_eq!(r.bound_node_id, Some(LOCAL.into()));
}

/// The whole point of the check: a different bound id is the unslashable
/// state, and the report has to carry the id to restore.
#[test]
fn different_binding_is_a_mismatch_that_names_the_bound_id() {
    let other = B256::repeat_byte(0xBB);
    let r = classify(other, LOCAL);
    assert_eq!(r.status, BindingStatus::Mismatch);
    assert_eq!(
        r.bound_node_id,
        Some(other.into()),
        "the operator needs the id they are bound to, not the one they are running"
    );
}

/// A zero binding is `Unbound`, never `Mismatch`: reporting it as a
/// mismatch would send the operator hunting for a key to restore that was
/// never bound.
#[test]
fn zero_binding_is_unbound_with_no_id() {
    let r = classify(B256::ZERO, LOCAL);
    assert_eq!(r.status, BindingStatus::Unbound);
    assert_eq!(r.bound_node_id, None);
}

/// `Unknown` must not be reachable from a successful read — it means "we
/// could not check", and conflating it with a checked state is exactly the
/// ambiguity the status enum exists to remove.
#[test]
fn classify_never_reports_unknown() {
    for bound in [B256::ZERO, LOCAL, B256::repeat_byte(0x01)] {
        assert_ne!(classify(bound, LOCAL).status, BindingStatus::Unknown);
    }
    assert_eq!(BindingReport::unknown().status, BindingStatus::Unknown);
}
