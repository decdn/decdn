use super::{FatalScope, Fault, HealExhausted, LaneBuildFault, classify};
use crate::driver::PoolExhausted;
use crate::{BlobTooLarge, LocalPullFault, UpstreamRefused, UpstreamVoucherRejected};
use decdn_protocol::client::{StreamError, VoucherRejectReason};

fn refusal(error: StreamError) -> anyhow::Error {
    anyhow::Error::new(UpstreamRefused::mid_stream(error))
}

fn rejected(reason: VoucherRejectReason) -> anyhow::Error {
    anyhow::Error::new(UpstreamVoucherRejected {
        reason,
        bundle: None,
        proof_generation: None,
    })
}

/// Only a local fault ends the command; no node's refusal does.
#[test]
fn a_local_fault_ends_the_command() {
    let local = anyhow::anyhow!("store write").context(LocalPullFault);
    assert_eq!(classify(&local), Fault::Fatal(FatalScope::Command));
}

/// An unhealed funding rejection acts as `Unfunded` from its source; every
/// other unhealed rejection acts as `Declined` (ADR 005 §`VoucherRejected`
/// semantics). Neither ends the fetch on its own.
#[test]
fn an_unhealed_rejection_scopes_to_its_source() {
    for reason in [
        VoucherRejectReason::SpendingCapExhausted,
        VoucherRejectReason::CapabilityExpired,
        VoucherRejectReason::PoolExhausted,
        VoucherRejectReason::SignerCapExhausted,
    ] {
        assert_eq!(
            classify(&rejected(reason)),
            Fault::Unaffordable,
            "{reason:?}"
        );
        assert_eq!(super::declining_rejection(&rejected(reason)), None);
    }
    for reason in [
        VoucherRejectReason::BadSignature,
        VoucherRejectReason::WrongSigner,
        VoucherRejectReason::BytesRegression,
        VoucherRejectReason::Underpaid,
        VoucherRejectReason::UnderFold,
        VoucherRejectReason::AmountRegression,
    ] {
        assert_eq!(classify(&rejected(reason)), Fault::Source, "{reason:?}");
        assert_eq!(
            super::declining_rejection(&rejected(reason)),
            Some(reason),
            "{reason:?}"
        );
        let mid_stream = refusal(StreamError::VoucherRejected {
            reason,
            bundle: None,
        });
        assert_eq!(super::declining_rejection(&mid_stream), Some(reason));
    }
    let stop = anyhow::Error::new(crate::source_set::NoNodeWillServe { reasons: vec![] });
    assert_eq!(classify(&stop), Fault::Fatal(FatalScope::Item));
}

/// A watermark rejection that healed the lane ledger after the lane spent
/// its resume budget (#2257) only cools its source: the range moves on,
/// and the source does not decline the fetch.
#[test]
fn a_rejection_healed_past_the_resume_budget_is_the_sources() {
    for reason in [
        VoucherRejectReason::UnderFold,
        VoucherRejectReason::AmountRegression,
        VoucherRejectReason::Underpaid,
    ] {
        let err = rejected(reason).context(HealExhausted);
        assert_eq!(classify(&err), Fault::Source, "{reason:?}");
        assert_eq!(super::declining_rejection(&err), None, "{reason:?}");
        assert!(
            err.downcast_ref::<UpstreamVoucherRejected>().is_some(),
            "the marker keeps the rejection in the chain"
        );
    }
}

#[test]
fn a_full_disk_ends_the_command() {
    let io = std::io::Error::from(std::io::ErrorKind::StorageFull);
    let err = anyhow::Error::new(io).context("write .partial");
    assert_eq!(classify(&err), Fault::Fatal(FatalScope::Command));
}

#[test]
fn an_over_cap_blob_ends_only_its_item() {
    let err = anyhow::Error::new(BlobTooLarge {
        reached: 1 << 40,
        ceiling: 1 << 20,
    });
    assert_eq!(classify(&err), Fault::Fatal(FatalScope::Item));
}

#[test]
fn a_dry_pool_marks_the_source_unaffordable() {
    let dry = anyhow::Error::new(PoolExhausted {
        gap_start: 0,
        gap_len: 1 << 20,
    });
    assert_eq!(classify(&dry), Fault::Unaffordable);
    assert_eq!(
        classify(&refusal(StreamError::Unfunded)),
        Fault::Unaffordable
    );
}

#[test]
fn a_lane_build_error_is_transient() {
    let err = anyhow::Error::new(LaneBuildFault(anyhow::anyhow!("rpc timed out")));
    assert_eq!(classify(&err), Fault::Transient);
}

/// A wallet short of USDC is not fixed by a retry, so it reads like a dry
/// pool: the funding recovery step that meets it ends "funding needed".
#[test]
fn a_wallet_shortfall_is_unaffordable() {
    let short = anyhow::anyhow!("submit topUp: transfer amount exceeds balance")
        .context(crate::buyer_pool::WalletShortfall);
    assert_eq!(classify(&short), Fault::Unaffordable);
}

/// A recovery step that opened a new pool ends this pass of the command: no
/// lane can pay from the new pool, and the caller runs the remaining work
/// again against it.
#[test]
fn a_replaced_pool_ends_the_command_pass() {
    let replaced = anyhow::Error::new(crate::PoolReplaced {
        closed: alloy::primitives::B256::repeat_byte(1),
        opened: alloy::primitives::B256::repeat_byte(2),
    });
    assert_eq!(classify(&replaced), Fault::Fatal(FatalScope::Command));
}

/// The pacer's refusal of a lane whose deposit cannot cover the next voucher
/// prices the source out at the current deposit, like a node's `Unfunded`
/// refusal: it never cools the source as a delivery fault.
#[test]
fn a_dry_lane_is_unaffordable() {
    let dry = anyhow::Error::new(crate::PoolExhausted {
        gap_start: 0,
        gap_len: 1,
    });
    assert_eq!(classify(&dry), Fault::Unaffordable);
}

/// A lane build never blames its source and retries, unless what failed
/// it is fatal on its own: a pool that cannot pay and cannot be funded.
#[test]
fn a_lane_build_takes_its_cause_only_when_fatal() {
    let unaffordable = anyhow::Error::new(crate::source_set::NoAffordableSource {
        deposit: alloy::primitives::U256::ZERO,
    })
    .context("buyer pool has no unspent deposit, and the wallet cannot fund a top-up");
    assert_eq!(
        classify(&anyhow::Error::new(LaneBuildFault(unaffordable))),
        Fault::Fatal(FatalScope::Command)
    );
    let short = anyhow::anyhow!("no USDC").context(crate::buyer_pool::WalletShortfall);
    assert_eq!(
        classify(&anyhow::Error::new(LaneBuildFault(short))),
        Fault::Transient
    );
}

/// A `topUp` or `openPool` that may have escrowed USDC no record credits
/// ends the command, whether it surfaces from a lane build (which otherwise
/// retries) or from a funding recovery step: a retry escrows again.
#[test]
fn a_possibly_escrowed_deposit_ends_the_command() {
    let tx = alloy::primitives::TxHash::repeat_byte(0xab);
    let untracked =
        || crate::buyer_pool::escrowed_but_untracked("pool 0x01 topped up by 5 µUSDC", tx, "disk");
    let unconfirmed = || {
        anyhow::anyhow!("receipt timed out").context(crate::buyer_pool::TopUpUnconfirmed {
            tx: Some(tx),
            nonce: 7,
        })
    };
    let maybe_broadcast = anyhow::anyhow!("submit topUp: connection reset")
        .context(crate::buyer_pool::TopUpUnconfirmed { tx: None, nonce: 7 });
    let open_unconfirmed =
        anyhow::anyhow!("await openPool receipt").context(crate::buyer_pool::OpenUnconfirmed {
            tx: Some(tx),
            nonce: 7,
        });
    for (name, err) in [
        ("untracked", untracked()),
        ("unconfirmed", unconfirmed()),
        ("maybe broadcast", maybe_broadcast),
        ("open unconfirmed", open_unconfirmed),
        (
            "untracked lane build",
            anyhow::Error::new(LaneBuildFault(untracked())),
        ),
        (
            "unconfirmed lane build",
            anyhow::Error::new(LaneBuildFault(unconfirmed())),
        ),
    ] {
        assert_eq!(
            classify(&err),
            Fault::Fatal(FatalScope::Command),
            "{name}: {err:#}"
        );
    }
}

#[test]
fn delivery_faults_are_the_sources() {
    for error in [StreamError::NotFound, StreamError::Declined] {
        assert_eq!(
            classify(&refusal(error.clone())),
            Fault::Source,
            "{error:?}"
        );
    }
    assert_eq!(
        classify(&anyhow::anyhow!("connection reset")),
        Fault::Source
    );
}

#[test]
fn not_found_is_a_source_fault_that_says_absent() {
    let err = refusal(StreamError::NotFound);
    assert_eq!(classify(&err), Fault::Source);
    assert!(super::says_absent(&err));
    assert!(!super::says_absent(&refusal(StreamError::Unfunded)));
}

#[test]
fn unanimous_stops_are_fatal_with_their_scope() {
    use crate::source_set::NoAffordableSource;
    let dry = anyhow::Error::new(NoAffordableSource {
        deposit: alloy::primitives::U256::ZERO,
    });
    assert_eq!(classify(&dry), Fault::Fatal(FatalScope::Command));
    let absent = anyhow::Error::new(crate::source_set::NoSourceHasBlob);
    assert_eq!(classify(&absent), Fault::Fatal(FatalScope::Item));
}

/// `classify` reads the markers in a fixed order, and the order is the
/// meaning when an error carries more than one: a local fault outranks any
/// node's refusal, a heal past the resume budget outranks the funding reason
/// of the rejection it carries, and a voucher rejection's reason outranks the
/// refusal class around it.
#[test]
fn stacked_markers_classify_in_a_fixed_order() {
    let local = refusal(StreamError::Unfunded).context(LocalPullFault);
    assert_eq!(classify(&local), Fault::Fatal(FatalScope::Command));

    let healed = rejected(VoucherRejectReason::SpendingCapExhausted).context(HealExhausted);
    assert_eq!(classify(&healed), Fault::Source);

    let funding_inside_a_decline = rejected(VoucherRejectReason::SignerCapExhausted)
        .context(UpstreamRefused::mid_stream(StreamError::Declined));
    assert_eq!(classify(&funding_inside_a_decline), Fault::Unaffordable);
}

/// A source failing or a stop only the human can fix `warns()`; a deposit wait
/// and a chain-side retry do not (#2331).
#[test]
fn only_source_and_fatal_faults_warn() {
    assert!(Fault::Source.warns());
    assert!(Fault::Fatal(FatalScope::Command).warns());
    assert!(Fault::Fatal(FatalScope::Item).warns());
    assert!(!Fault::Unaffordable.warns());
    assert!(!Fault::Transient.warns());
}
