use super::*;
use crate::commands::doctor::Severity;

#[test]
fn chain_id_match_passes_mismatch_fails() {
    assert_eq!(classify_chain_id(421_614, 421_614).severity, Severity::Pass);
    assert_eq!(classify_chain_id(421_614, 1).severity, Severity::Fail);
}

#[test]
fn empty_code_fails_nonempty_passes() {
    assert_eq!(
        classify_code("payment_pool", "0xabc", 0).severity,
        Severity::Fail
    );
    assert_eq!(
        classify_code("payment_pool", "0xabc", 1234).severity,
        Severity::Pass
    );
}
