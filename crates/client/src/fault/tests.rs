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

#[test]
fn payment_and_blacklist_faults_end_the_command() {
    for reason in [
        VoucherRejectReason::SpendingCapExhausted,
        VoucherRejectReason::CapabilityExpired,
        VoucherRejectReason::BadSignature,
        VoucherRejectReason::WrongSigner,
    ] {
        assert_eq!(
            classify(&rejected(reason)),
            Fault::Fatal(FatalScope::Command),
            "{reason:?}"
        );
    }
    assert_eq!(
        classify(&refusal(StreamError::OriginBlacklisted)),
        Fault::Fatal(FatalScope::Command)
    );
    let local = anyhow::anyhow!("store write").context(LocalPullFault);
    assert_eq!(classify(&local), Fault::Fatal(FatalScope::Command));
}

fn drained(remaining: u64, expired: bool) -> crate::SignerCapDrained {
    crate::SignerCapDrained {
        pool_id: alloy::primitives::B256::ZERO,
        signer: alloy::primitives::Address::ZERO,
        provider: alloy::primitives::Address::ZERO,
        remaining,
        rate_per_mb: 10,
        expired,
    }
}

/// A drained signer ends the command only when no provider at any rate
/// can serve it: its registration expired, or nothing is left of its cap.
/// With some headroom left, a cheaper provider still can, so only the
/// refusing source is barred, and the stop once every source is ends the
/// item (#2338).
#[test]
fn a_drained_signer_ends_the_command_only_at_every_rate() {
    for (remaining, expired) in [(0, false), (5, true)] {
        assert_eq!(
            classify(&anyhow::Error::new(drained(remaining, expired))),
            Fault::Fatal(FatalScope::Command),
            "remaining {remaining}, expired {expired}"
        );
    }
    assert_eq!(
        classify(&anyhow::Error::new(drained(5, false))),
        Fault::Source
    );
    let stop =
        anyhow::Error::new(drained(5, false)).context(crate::source_set::NoSourceServesSigner);
    assert_eq!(classify(&stop), Fault::Fatal(FatalScope::Item));
}

/// A watermark rejection that healed the lane ledger after the lane spent
/// its resume budget (#2257) is this source's, so the range moves on and
/// the rest of the bundle continues.
#[test]
fn a_rejection_healed_past_the_resume_budget_is_the_sources() {
    for reason in [
        VoucherRejectReason::UnderFold,
        VoucherRejectReason::AmountRegression,
        VoucherRejectReason::Underpaid,
    ] {
        let err = rejected(reason).context(HealExhausted);
        assert_eq!(classify(&err), Fault::Source, "{reason:?}");
        assert!(
            err.downcast_ref::<UpstreamVoucherRejected>().is_some(),
            "the marker keeps the rejection in the chain"
        );
    }
}

/// A watermark rejection that no heal took ends the command, as ADR 005
/// says: a `BytesRegression` with no bundle is a single-signer fault, and
/// an `Underpaid` or a trailing proof that no bundle heals has nothing to
/// retry from.
#[test]
fn a_rejection_no_heal_took_ends_the_command() {
    for reason in [
        VoucherRejectReason::BytesRegression,
        VoucherRejectReason::Underpaid,
        VoucherRejectReason::UnderFold,
        VoucherRejectReason::AmountRegression,
    ] {
        assert_eq!(
            classify(&rejected(reason)),
            Fault::Fatal(FatalScope::Command),
            "{reason:?}"
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
        classify(&refusal(StreamError::InsufficientDeposit)),
        Fault::Unaffordable
    );
}

#[test]
fn a_lane_build_error_is_transient() {
    let err = anyhow::Error::new(LaneBuildFault(anyhow::anyhow!("rpc timed out")));
    assert_eq!(classify(&err), Fault::Transient);
}

/// A failed reactive top-up is the buyer's funding, never the source's
/// delivery: it is transient, so the source keeps its health and the loop
/// retries. A wallet short of USDC is not fixed by a retry, so the source
/// waits for the deposit like a dry pool.
#[test]
fn a_failed_reactive_top_up_is_transient_unless_the_wallet_is_short() {
    let failed =
        || anyhow::anyhow!("submit topUp: rpc timed out").context(crate::driver::TopUpFailed);
    assert_eq!(classify(&failed()), Fault::Transient);
    let short = failed().context(crate::buyer_pool::WalletShortfall);
    assert_eq!(classify(&short), Fault::Unaffordable);
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

/// A `topUp` that may have escrowed USDC no record credits ends the
/// command, whether it surfaces from a lane build (which otherwise retries)
/// or from a reactive top-up (which is otherwise unaffordable): a retry
/// escrows again.
#[test]
fn a_possibly_escrowed_top_up_ends_the_command() {
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
    for (name, err) in [
        ("untracked", untracked()),
        ("unconfirmed", unconfirmed()),
        ("maybe broadcast", maybe_broadcast),
        (
            "untracked lane build",
            anyhow::Error::new(LaneBuildFault(untracked())),
        ),
        (
            "unconfirmed lane build",
            anyhow::Error::new(LaneBuildFault(unconfirmed())),
        ),
        (
            "untracked reactive",
            untracked().context(crate::driver::TopUpFailed),
        ),
        (
            "unconfirmed reactive",
            unconfirmed().context(crate::driver::TopUpFailed),
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
    for error in [
        StreamError::NotFound,
        StreamError::Overloaded,
        StreamError::BlobTooLarge,
        StreamError::InternalError,
        StreamError::EvictedSinceProbe,
        StreamError::HashBlacklisted,
    ] {
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
    assert!(!super::says_absent(&refusal(StreamError::Overloaded)));
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
