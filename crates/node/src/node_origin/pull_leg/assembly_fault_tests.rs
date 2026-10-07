use decdn_client::{HealExhausted, PoolExhausted, UpstreamRefused, UpstreamVoucherRejected};
use decdn_protocol::client::{StreamError, VoucherRejectReason};

use super::ends_the_assembly;

fn refusal(error: StreamError) -> anyhow::Error {
    anyhow::Error::new(UpstreamRefused::mid_stream(error))
}

/// A holder's reservation floor above the pool moves the range to another
/// holder, which may reserve less, and so does a rejection healed past the
/// resume budget; a dry pool, a fatal fault, and a rejection no heal took
/// end it.
#[test]
fn insufficient_deposit_reassigns_and_a_dry_pool_ends_the_assembly() {
    assert!(!ends_the_assembly(&refusal(
        StreamError::InsufficientDeposit
    )));
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
    assert!(ends_the_assembly(&rejected(
        VoucherRejectReason::CapabilityExpired
    )));
    assert!(ends_the_assembly(&rejected(VoucherRejectReason::UnderFold)));
    assert!(!ends_the_assembly(
        &rejected(VoucherRejectReason::UnderFold).context(HealExhausted)
    ));
    assert!(ends_the_assembly(&refusal(StreamError::OriginBlacklisted)));
    assert!(!ends_the_assembly(&refusal(StreamError::NotFound)));
    assert!(!ends_the_assembly(&anyhow::anyhow!("connection reset")));
}
