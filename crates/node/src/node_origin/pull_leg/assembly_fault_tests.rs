use decdn_client::{HealExhausted, PoolExhausted, UpstreamRefused, UpstreamVoucherRejected};
use decdn_protocol::client::{StreamError, VoucherRejectReason};

use super::ends_the_assembly;

fn refusal(error: StreamError) -> anyhow::Error {
    anyhow::Error::new(UpstreamRefused::mid_stream(error))
}

/// A holder's refusal or voucher rejection scopes to that holder (ADR 039
/// §Failure handling): its range moves to another holder, which may
/// reserve less or accept the voucher. Only this node's dry pool ends the
/// assembly: it funds every holder.
#[test]
fn a_refusal_reassigns_and_a_dry_pool_ends_the_assembly() {
    assert!(!ends_the_assembly(&refusal(StreamError::Unfunded)));
    assert!(ends_the_assembly(&anyhow::Error::new(PoolExhausted {
        gap_start: 0,
        gap_len: 1 << 20,
    })));
    let rejected = |reason| {
        anyhow::Error::new(UpstreamVoucherRejected {
            reason,
            bundle: None,
            proof_generation: None,
        })
    };
    assert!(!ends_the_assembly(&rejected(
        VoucherRejectReason::CapabilityExpired
    )));
    assert!(!ends_the_assembly(&rejected(
        VoucherRejectReason::UnderFold
    )));
    assert!(!ends_the_assembly(
        &rejected(VoucherRejectReason::UnderFold).context(HealExhausted)
    ));
    assert!(!ends_the_assembly(&refusal(StreamError::Declined)));
    assert!(!ends_the_assembly(&refusal(StreamError::NotFound)));
    assert!(!ends_the_assembly(&anyhow::anyhow!("connection reset")));
}
