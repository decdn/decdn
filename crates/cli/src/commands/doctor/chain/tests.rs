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

/// A payment chain whose RPC serves another chain fails, naming the entry.
#[test]
fn payment_chain_id_mismatch_fails() {
    let ok = classify_payment_chain_id(84_532, 84_532);
    assert_eq!(ok.severity, Severity::Pass);
    let bad = classify_payment_chain_id(84_532, 1);
    assert_eq!(bad.severity, Severity::Fail);
    assert!(
        bad.remediation
            .unwrap()
            .contains("[[blockchain.payment_chains]]")
    );
}

/// A pool sweeping to another receiver than the configured one fails: its
/// sweeps would land where the governance chain's `FeeRouter` never sees them.
#[test]
fn sweep_recipient_mismatch_fails_and_match_passes() {
    let configured = Address::repeat_byte(0xC5);
    let pool = "eip155:84532:0xb0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0";
    assert_eq!(
        classify_sweep_recipient(pool, Some(configured), configured).severity,
        Severity::Pass
    );
    let other = Address::repeat_byte(0x99);
    let mismatch = classify_sweep_recipient(pool, Some(configured), other);
    assert_eq!(mismatch.severity, Severity::Fail);
    assert!(mismatch.detail.unwrap().contains(&other.to_string()));
    // Nothing configured: report what the pool names, no verdict to give.
    assert_eq!(
        classify_sweep_recipient(pool, None, other).severity,
        Severity::Pass
    );
}

/// A contract that does not answer the pool's identity reads is not a
/// `PaymentPool`.
#[test]
fn a_pool_that_does_not_answer_its_identity_fails() {
    let pool = "eip155:84532:0xb0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0";
    assert_eq!(
        classify_pool_identity(pool, Ok((6_000, 0))).severity,
        Severity::Pass
    );
    let bad = classify_pool_identity(pool, Err("execution reverted".to_owned()));
    assert_eq!(bad.severity, Severity::Fail);
}
