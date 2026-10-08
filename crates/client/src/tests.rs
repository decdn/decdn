use bao_tree::io::outboard::PreOrderMemOutboard;
use decdn_bao_range::{IROH_BLOCK_SIZE, align_range, encode_verified_range};

use bytes::Bytes;

use super::{
    Cumulative, HashMismatch, Healed, LocalPullFault, PoolContext, PoolLedger, U256,
    UpstreamVoucherRejected, Voucher, VoucherRejectReason, WatermarkBundle, aligned_wire_len,
    decode_to_vec, heal_watermark_desync, rejection_watermark, resumable_watermark,
};

/// The `LocalPullFault` marker must ride out on the errors the range helpers ACTUALLY
/// raise — not on one a test hand-built (#1145 review).
///
/// This distinction is the whole point of the test: a test that calls the real
/// `sign_client_binding`, throws away its `Ok` result, and hand-builds
/// `anyhow!("...").context(LocalPullFault)` before asserting the ladder finds
/// `LocalPullFault` in it is true by construction. Attaching the marker and then
/// finding it proves nothing — such a test cannot fail even if every
/// `.context(LocalPullFault)` call site is stripped from the crate, so a synthetic
/// `anyhow!(...)` passes it while production silently fails to score the peer.
///
/// So: real functions, real errors, marker never touched by the test. `align_range`
/// rejects an offset at or past the end of the blob (never clamps — ADR 005), which is
/// the one local-fault trigger reachable without mocking a signer, and
/// `aligned_wire_len` is the one site every pull passes it through.
///
/// The stakes, and why an unguarded marker here is not cosmetic: every arm BELOW
/// `LocalPullFault` in the ladder blames the peer to some degree, and the catch-all
/// scores `Unreachable` in the local per-peer EWMA (ADR 008). A fault in this
/// node is not evidence about a provider, and a node in this state meets every candidate
/// in turn, so losing the marker does not mis-score one peer: it defames the whole
/// candidate list on the strength of our own defect.
/// `capped` must refuse a cap that cannot outlast its own stages (#1145 review).
///
/// This is the enforcement of record. The CLI's `ClientFetchArgs::validate` restates the
/// rule as `timeout > 2 × stall` to give the user an early error in their own flags, but
/// that `2 ×` is only correct because both call sites set the open bound from the stall
/// knob — an assumption the compiler does not hold and an `--open-timeout-ms` flag would
/// break. Here the three values are all in hand, so the real relation can be checked, and
/// a `PullDeadlines` whose stall bound could never fire simply cannot be constructed.
#[test]
fn a_cap_that_cannot_outlast_its_stages_is_refused() {
    use super::{DeadlineError, PullDeadlines};
    use std::time::Duration;

    let open = Duration::from_secs(5);
    let window = Duration::from_secs(5);
    let floor = 4096;

    // At and below `open + window` the cap always wins the race, so the throughput floor
    // — the signal that a healthy stream is still making progress — could never fire.
    for cap in [Duration::from_secs(1), window, open + window] {
        assert!(
            matches!(
                PullDeadlines::capped(open, window, floor, cap),
                Err(DeadlineError::CapCannotOutlastItsStages { .. })
            ),
            "a cap of {cap:?} against open {open:?} + window {window:?} leaves the floor \
             unable to fire, and must not be constructible"
        );
    }

    // One tick past it, the throughput floor can actually fire.
    assert!(
        PullDeadlines::capped(
            open,
            window,
            floor,
            open + window + Duration::from_millis(1)
        )
        .is_ok(),
        "past open + window the floor can fire, so this is a legitimate pull"
    );

    // A zero budget elapses on its first poll: the stage it bounds can never run.
    assert!(matches!(
        PullDeadlines::capped(Duration::ZERO, window, floor, Duration::from_mins(1)),
        Err(DeadlineError::ZeroBudget)
    ));
    assert!(matches!(
        PullDeadlines::capped(open, Duration::ZERO, floor, Duration::from_mins(1)),
        Err(DeadlineError::ZeroBudget)
    ));
}

/// `new` must refuse a zero budget too — and it is the constructor that MATTERS, because
/// it is the one every production pull takes (#1145 review).
///
/// A zero `window` makes the throughput floor demand progress over no time at all, so the
/// streaming stage can never satisfy it and every honest read aborts on its first poll.
/// The floor rate itself may legitimately be zero — that is idle-detection mode (one byte
/// per window) — so only the durations are checked.
///
/// The invariant belongs to the type, not to a resolver in another crate that a caller
/// has to remember to run.
#[test]
fn new_refuses_a_zero_budget_on_the_path_every_production_pull_takes() {
    use super::{DeadlineError, PullDeadlines};
    use std::time::Duration;

    assert!(
        matches!(
            PullDeadlines::new(Duration::ZERO, Duration::from_secs(20), 4096),
            Err(DeadlineError::ZeroBudget)
        ),
        "a zero open bound means the open stage can never complete"
    );
    assert!(
        matches!(
            PullDeadlines::new(Duration::from_secs(20), Duration::ZERO, 4096),
            Err(DeadlineError::ZeroBudget)
        ),
        "a zero window makes the throughput floor unsatisfiable on every read"
    );
    assert!(PullDeadlines::new(Duration::from_secs(20), Duration::from_secs(20), 4096).is_ok());
    assert!(
        PullDeadlines::new(Duration::from_secs(20), Duration::from_secs(20), 0).is_ok(),
        "a zero floor rate is idle-detection mode, not an invalid budget"
    );
}

/// `aligned_wire_len`'s new `byte_len` parameter must actually bound the
/// quoted wire cost — not just be accepted and ignored. This is the
/// construction-level proof that `open_progressive_pull`'s `byte_len`
/// threads all the way to the wire-byte bound `PeerSource`'s callers price
/// vouchers from (#1608): a middle-gap request must quote strictly less
/// than the whole tail, and must match `align_range`'s own `wire_len` for the
/// identical bounded span so the two can never drift.
#[test]
fn aligned_wire_len_is_bounded_by_the_requested_byte_len() {
    let total = 10 * decdn_bao_range::CHUNK_GROUP_BYTES;
    let whole_tail = aligned_wire_len(0, 0, total).unwrap_or(0);
    let one_group = aligned_wire_len(0, decdn_bao_range::CHUNK_GROUP_BYTES, total).unwrap_or(0);
    assert!(
        one_group > 0 && one_group < whole_tail,
        "a one-group byte_len must quote less than the whole 10-group tail: \
         one_group={one_group}, whole_tail={whole_tail}"
    );
    let via_align_range =
        align_range(0, decdn_bao_range::CHUNK_GROUP_BYTES, total).map_or(0, |r| r.wire_len());
    assert_eq!(
        one_group, via_align_range,
        "aligned_wire_len must reproduce align_range's own wire_len for the same \
         bounded span, or the two can silently drift"
    );
}

/// Every input to `aligned_wire_len` but the offset is the peer's signed
/// size or our own cap, so a range it cannot align is a source fault, never
/// ours: one node that signs a bad size must not end the command.
#[test]
fn a_range_the_signed_size_cannot_hold_is_a_source_fault() {
    for (offset, len, total) in [(8192, 0, 4096), (0, 16_384, 0)] {
        let aligned = aligned_wire_len(offset, len, total).err();
        assert!(
            aligned.as_ref().is_some_and(|e| {
                e.downcast_ref::<LocalPullFault>().is_none()
                    && crate::classify(e) == crate::Fault::Source
            }),
            "({offset}, {len}) against a signed {total} is the source's fault: {aligned:?}"
        );
        // It names a range past the blob's end, so a caller that cut the
        // range from an unsigned size can tell it from any other failure.
        assert!(aligned.as_ref().is_some_and(super::is_range_past_end));
    }
    assert!(super::is_range_past_end(&anyhow::Error::new(
        super::ResumeOffsetPastEnd {
            total_bytes: 4096,
            byte_offset: 8192,
        }
    )));
    assert!(!super::is_range_past_end(&anyhow::Error::new(
        super::UpstreamRefused::mid_stream(decdn_protocol::client::StreamError::NotFound)
    )));
    assert!(!super::is_range_past_end(
        &anyhow::anyhow!("dial timed out").context(LocalPullFault)
    ));
}

/// A node that signs a size of zero for a bounded open is refused with the
/// typed `ResumeOffsetPastEnd`, a source fault. The open of the empty blob,
/// `(0, 0)` against zero, still passes.
#[test]
fn a_zero_size_signed_for_a_bounded_open_is_refused_as_the_sources_fault() {
    let err = super::served_wire_len(0, 16_384, 0).err();
    assert!(
        err.as_ref().is_some_and(|e| {
            e.downcast_ref::<super::ResumeOffsetPastEnd>().is_some()
                && crate::classify(e) == crate::Fault::Source
        }),
        "a zero size for a bounded open is the source's fault: {err:?}"
    );
    assert!(
        super::served_wire_len(8192, 0, 4096)
            .err()
            .is_some_and(|e| e.downcast_ref::<super::ResumeOffsetPastEnd>().is_some()),
        "an offset past the signed end is refused the same way"
    );
    assert!(
        super::served_wire_len(0, 0, 0).is_ok(),
        "the empty blob opens"
    );
    assert!(
        super::served_wire_len(0, 16_384, 4096).is_ok(),
        "an end past the size clamps"
    );
}

/// A response header whose `total_bytes` is smaller than the requested end
/// is served, and priced, up to the blob's end. `aligned_wire_len` must
/// clamp rather than wrap the clamp in `LocalPullFault`, and the clamped
/// wire length must equal the whole blob's.
#[test]
fn aligned_wire_len_clamps_when_the_response_total_is_smaller_than_the_requested_end()
-> anyhow::Result<()> {
    let total = 4096;
    let requested_end_past_total = total + 8192;
    let clamped = aligned_wire_len(0, requested_end_past_total, total);
    assert!(
        !clamped
            .as_ref()
            .is_err_and(|e| e.downcast_ref::<LocalPullFault>().is_some()),
        "a clamped end must never be marked LocalPullFault: {clamped:?}"
    );
    let whole = aligned_wire_len(0, 0, total)?;
    assert_eq!(
        clamped?, whole,
        "the clamped wire length must equal the whole blob's"
    );
    Ok(())
}

/// `client_binding_ext` maps an unbound context to `None` (so
/// `encode_stream_request` appends no ext bytes — byte-for-byte the pre-#1115
/// wire) and a bound one to `Some` carrying exactly the binding at the default
/// voucher cadence. This is the mapping the single request site
/// (`open_stream`) relies on, so it guards a refactor that would silently
/// drop the ext (#1115).
#[test]
fn client_binding_ext_reflects_binding_presence() -> anyhow::Result<()> {
    use std::sync::Arc;

    use alloy::primitives::{Address, B256, U256};
    use alloy::signers::local::PrivateKeySigner;

    use super::{PoolContext, client_binding_ext, sign_client_binding};

    let signer = PrivateKeySigner::random();
    let domain = decdn_incentive::bind_node_id_domain(1, Address::ZERO);
    let ctx = PoolContext {
        pool_id: B256::ZERO,
        provider: Address::ZERO,
        deposit: U256::ZERO,
        client_signer: Arc::new(signer.clone()),
        voucher_domain: domain.clone(),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
        capability: None,
    };
    // Unbound ⇒ no ext.
    anyhow::ensure!(
        client_binding_ext(&ctx).is_none(),
        "unbound ctx must yield no ext"
    );

    // Bound ⇒ ext carries exactly the binding.
    let binding = sign_client_binding(&signer, B256::repeat_byte(0xAB), &domain)?;
    let ctx = ctx.with_client_binding(binding.clone());
    let ext =
        client_binding_ext(&ctx).ok_or_else(|| anyhow::anyhow!("bound ctx must yield an ext"))?;
    anyhow::ensure!(
        ext.binding == Some(binding),
        "ext must carry the exact binding"
    );
    anyhow::ensure!(
        ext.capability.is_none(),
        "no capability attached ⇒ ext must carry none"
    );

    // Capability attached ⇒ ext carries the wire-mapped capability, alongside
    // the still-present binding — the two fields are independent.
    let owner = PrivateKeySigner::random();
    let capability = decdn_incentive::Capability {
        signer: signer.address(),
        spending_cap: 10_000_000u64,
        pool_id: B256::ZERO,
        expiry: 1_900_000_000,
    }
    .sign(&owner, &domain)?;
    let ctx = ctx.with_capability(capability.clone());
    let ext = client_binding_ext(&ctx)
        .ok_or_else(|| anyhow::anyhow!("ctx with capability must yield an ext"))?;
    let wire_cap = ext
        .capability
        .ok_or_else(|| anyhow::anyhow!("ext must carry the capability"))?;
    anyhow::ensure!(
        wire_cap.spending_cap == capability.capability.spending_cap,
        "spending_cap must round-trip to wire form"
    );
    anyhow::ensure!(
        wire_cap.expiry == capability.capability.expiry,
        "expiry must round-trip unchanged"
    );
    anyhow::ensure!(
        wire_cap.owner_signature == capability.signature.as_bytes().to_vec(),
        "owner_signature must round-trip to wire bytes"
    );
    Ok(())
}

/// Build a test [`PoolContext`] signing with `signer` over the
/// `(pool_id, provider)` lane, sharing the shape
/// `client_binding_ext_reflects_binding_presence` already uses.
fn resume_test_ctx(
    pool_id: alloy::primitives::B256,
    provider: alloy::primitives::Address,
    signer: &std::sync::Arc<alloy::signers::local::PrivateKeySigner>,
    domain: &alloy::dyn_abi::Eip712Domain,
) -> PoolContext {
    PoolContext {
        pool_id,
        provider,
        deposit: U256::ZERO,
        client_signer: std::sync::Arc::clone(signer),
        voucher_domain: domain.clone(),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
        capability: None,
    }
}

/// Build a `WatermarkBundle` whose `last_signature` is `signer`'s real EIP-712 voucher
/// signature over `(pool_id, signer, provider, amount, bytes_delivered)` — i.e. a
/// genuinely-signed bundle, the shape a caller must construct one from.
fn signed_bundle(
    pool_id: alloy::primitives::B256,
    provider: alloy::primitives::Address,
    signer: &alloy::signers::local::PrivateKeySigner,
    domain: &alloy::dyn_abi::Eip712Domain,
    amount: U256,
    bytes_delivered: U256,
) -> anyhow::Result<WatermarkBundle> {
    // A SEALED watermark: zero root, zero price. The bundle's authentication
    // gate recovers `last_signature` over the full seven-field voucher, so
    // the chain half has to be exactly what was signed — building the bundle
    // and the signature from one shape is what keeps that honest.
    let voucher_signature = Voucher {
        pool_id,
        signer: signer.address(),
        provider,
        amount,
        bytes_delivered,
        chain_root: alloy::primitives::B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(signer, domain)
    .map_err(|e| anyhow::anyhow!("voucher signing failed: {e}"))?;
    Ok(WatermarkBundle {
        amount: u64::try_from(amount)?,
        bytes_delivered: u64::try_from(bytes_delivered)?,
        chain_root: [0u8; 32],
        verified_index: 0,
        tip: [0u8; 32],
        chunk_price: 0,
        last_signature: voucher_signature.signature.as_bytes().to_vec(),
    })
}

/// A metered bundle: the same seven-field voucher, but committing a real chain root and
/// echoing the frontier the node claims under it. `released` is the depth the client
/// actually gave the node; `claimed` is the depth the node reports. An honest node sets
/// them equal.
fn metered_bundle(
    pool_id: alloy::primitives::B256,
    provider: alloy::primitives::Address,
    signer: &alloy::signers::local::PrivateKeySigner,
    domain: &alloy::dyn_abi::Eip712Domain,
    seed: alloy::primitives::B256,
    released: u8,
    claimed: u8,
) -> anyhow::Result<WatermarkBundle> {
    let chain_root = decdn_incentive::chain::root_from_seed(seed);
    let amount = U256::from(500u64);
    let bytes_delivered = U256::from(4096u64);
    let voucher_signature = Voucher {
        pool_id,
        signer: signer.address(),
        provider,
        amount,
        bytes_delivered,
        chain_root,
        chunk_price: U256::from(10u64),
    }
    .sign(signer, domain)
    .map_err(|e| anyhow::anyhow!("voucher signing failed: {e}"))?;
    Ok(WatermarkBundle {
        amount: u64::try_from(amount)?,
        bytes_delivered: u64::try_from(bytes_delivered)?,
        chain_root: chain_root.into(),
        verified_index: claimed,
        // The deepest preimage the node was actually handed. It cannot fabricate a deeper
        // one, so this is the whole of what it can prove.
        tip: decdn_incentive::chain::preimage_at(seed, released).into(),
        chunk_price: 10,
        last_signature: voucher_signature.signature.as_bytes().to_vec(),
    })
}

/// The chain half of a bundle is covered by NO signature: `verified_index` is a number the
/// node writes, and the voucher whose signature it echoes carries no index. So a node can
/// claim any depth it likes — and the resuming client folds `verified_index × chunk_price`
/// into the amount it re-signs. Here the client released 3 chunks and the node reports 255;
/// without the tip check the client would sign away 252 chunks it never received.
#[test]
fn resumable_watermark_rejects_a_frontier_the_tip_does_not_prove() -> anyhow::Result<()> {
    use alloy::primitives::{Address, B256};
    use alloy::signers::local::PrivateKeySigner;

    let channel_id = B256::repeat_byte(0x11);
    let token = Address::repeat_byte(0x22);
    let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
    let our_signer = std::sync::Arc::new(PrivateKeySigner::random());
    let ctx = resume_test_ctx(channel_id, token, &our_signer, &domain);

    let bundle = metered_bundle(
        channel_id,
        token,
        &our_signer,
        &domain,
        B256::repeat_byte(0x5E),
        3,
        255,
    )?;
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: VoucherRejectReason::AmountRegression,
        bundle: Some(bundle),
        proof_generation: None,
    });

    anyhow::ensure!(
        resumable_watermark(&err, &ctx).is_none(),
        "a bundle claiming a depth its tip does not reach must never be folded into money"
    );
    Ok(())
}

/// The positive twin: the node reports exactly the depth it was given, its tip hashes
/// forward to the root the client's own signature commits to, and the frontier is folded.
#[test]
fn resumable_watermark_accepts_a_frontier_the_tip_proves() -> anyhow::Result<()> {
    use alloy::primitives::{Address, B256};
    use alloy::signers::local::PrivateKeySigner;

    let channel_id = B256::repeat_byte(0x11);
    let token = Address::repeat_byte(0x22);
    let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
    let our_signer = std::sync::Arc::new(PrivateKeySigner::random());
    let ctx = resume_test_ctx(channel_id, token, &our_signer, &domain);

    let bundle = metered_bundle(
        channel_id,
        token,
        &our_signer,
        &domain,
        B256::repeat_byte(0x5E),
        3,
        3,
    )?;
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: VoucherRejectReason::AmountRegression,
        bundle: Some(bundle),
        proof_generation: None,
    });

    let got = resumable_watermark(&err, &ctx)
        .ok_or_else(|| anyhow::anyhow!("a genuinely proved frontier must be resumable"))?;
    // 500 + 3 × 10: the anchor plus the three chunks the tip proves.
    anyhow::ensure!(Cumulative::from(got).amount == U256::from(530u64));
    Ok(())
}

/// The security property this module exists to guard (post-review-round-2, #1481 §5): a
/// mid-stream `StreamError` carries no signature of its own, so `WatermarkBundle.amount`/
/// `nonce`/`bytes_delivered` are otherwise attacker-controllable by the upstream node.
/// `resumable_watermark` MUST refuse to reseed the ledger from a bundle whose
/// `last_signature` does not recover to THIS client's own `ctx.client_signer` — otherwise a
/// malicious/buggy upstream could hand back an inflated watermark and have this client sign
/// (and the node redeem) a voucher for money it never delivered.
#[test]
fn resumable_watermark_rejects_a_bundle_not_signed_by_our_own_key() -> anyhow::Result<()> {
    use alloy::primitives::{Address, B256};
    use alloy::signers::local::PrivateKeySigner;

    let channel_id = B256::repeat_byte(0x11);
    let token = Address::repeat_byte(0x22);
    let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
    let our_signer = std::sync::Arc::new(PrivateKeySigner::random());
    let attacker_signer = PrivateKeySigner::random();
    let ctx = resume_test_ctx(channel_id, token, &our_signer, &domain);

    // The upstream (or an attacker impersonating it) signs the SAME tuple with a
    // DIFFERENT key — exactly what a malicious node echoing a fabricated watermark
    // would have to do, since it does not hold our key.
    let bundle = signed_bundle(
        channel_id,
        token,
        &attacker_signer,
        &domain,
        U256::from(1_000_000u64), // an inflated amount our ledger never earned
        U256::from(4096u64),
    )?;
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: VoucherRejectReason::AmountRegression,
        bundle: Some(bundle),
        proof_generation: None,
    });

    anyhow::ensure!(
        resumable_watermark(&err, &ctx).is_none(),
        "a bundle signed by a key other than ours must never be treated as resumable"
    );
    Ok(())
}

/// The companion attack shape: the SAME rejected-voucher signature bytes replayed
/// alongside a TAMPERED `amount` field. Recovery is over the whole tuple, so any field
/// mismatch (not just a wrong key) must also fail the check — `last_signature` binds the
/// exact `(amount, nonce, bytes_delivered)` triple, not just "some voucher we once signed".
#[test]
fn resumable_watermark_rejects_a_bundle_with_a_tampered_amount() -> anyhow::Result<()> {
    use alloy::primitives::{Address, B256};
    use alloy::signers::local::PrivateKeySigner;

    let channel_id = B256::repeat_byte(0x11);
    let token = Address::repeat_byte(0x22);
    let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
    let our_signer = std::sync::Arc::new(PrivateKeySigner::random());
    let ctx = resume_test_ctx(channel_id, token, &our_signer, &domain);

    // Genuinely our own signature — but over amount 100, not the 1_000_000 the bundle
    // claims. A node that recorded 100 and echoes 1_000_000 (bug or malice) must not
    // slip through just because SOME real signature accompanies it.
    let mut bundle = signed_bundle(
        channel_id,
        token,
        &our_signer,
        &domain,
        U256::from(100u64),
        U256::from(4096u64),
    )?;
    bundle.amount = 1_000_000u64;
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: VoucherRejectReason::AmountRegression,
        bundle: Some(bundle),
        proof_generation: None,
    });

    anyhow::ensure!(
        resumable_watermark(&err, &ctx).is_none(),
        "a bundle whose signature does not cover the claimed amount must never be treated \
         as resumable"
    );
    Ok(())
}

/// The positive twin: a bundle genuinely signed by OUR OWN key, over the tuple it claims,
/// for a gated reason, passes every check and is returned so the caller can reseed.
#[test]
fn resumable_watermark_accepts_a_bundle_genuinely_signed_by_our_own_key() -> anyhow::Result<()> {
    use alloy::primitives::{Address, B256};
    use alloy::signers::local::PrivateKeySigner;

    let channel_id = B256::repeat_byte(0x11);
    let token = Address::repeat_byte(0x22);
    let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
    let our_signer = std::sync::Arc::new(PrivateKeySigner::random());
    let ctx = resume_test_ctx(channel_id, token, &our_signer, &domain);

    let bundle = signed_bundle(
        channel_id,
        token,
        &our_signer,
        &domain,
        U256::from(500u64),
        U256::from(4096u64),
    )?;
    let expected_bytes = bundle.bytes_delivered;
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: VoucherRejectReason::AmountRegression,
        bundle: Some(bundle),
        proof_generation: None,
    });

    let got = resumable_watermark(&err, &ctx).ok_or_else(|| {
        anyhow::anyhow!(
            "a bundle genuinely signed by our own key over the claimed tuple must resolve"
        )
    })?;
    assert_eq!(got.bytes_delivered, expected_bytes);
    Ok(())
}

/// Build an `UpstreamVoucherRejected` carrying a bundle genuinely signed by
/// `signer` at `(amount, bytes)`, for a voucher signed under `proof_generation`.
fn rejected_with_bundle(
    reason: VoucherRejectReason,
    ctx: &PoolContext,
    signer: &alloy::signers::local::PrivateKeySigner,
    (amount, bytes): (u64, u64),
    proof_generation: Option<u64>,
) -> anyhow::Result<anyhow::Error> {
    let bundle = signed_bundle(
        ctx.pool_id,
        ctx.provider,
        signer,
        &ctx.voucher_domain,
        U256::from(amount),
        U256::from(bytes),
    )?;
    Ok(anyhow::Error::new(UpstreamVoucherRejected {
        reason,
        bundle: Some(bundle),
        proof_generation,
    }))
}

fn heal_test_ctx() -> (
    PoolContext,
    std::sync::Arc<alloy::signers::local::PrivateKeySigner>,
) {
    use alloy::primitives::{Address, B256};
    let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
    let signer = std::sync::Arc::new(alloy::signers::local::PrivateKeySigner::random());
    let ctx = resume_test_ctx(
        B256::repeat_byte(0x11),
        Address::repeat_byte(0x22),
        &signer,
        &domain,
    );
    (ctx, signer)
}

/// Extract the rejection's watermark and heal from it, the way both resume
/// loops do.
async fn heal(err: &anyhow::Error, ctx: &PoolContext, ledger: &PoolLedger) -> Option<Healed> {
    let watermark = rejection_watermark(err, ctx)?;
    heal_watermark_desync(err, watermark, ledger).await
}

/// An authenticated `Underpaid` bundle BEHIND the ledger rebases it down to the
/// node's watermark. A later `Underpaid` for a voucher signed before that
/// rebase is stale: it retries and leaves the ledger alone.
#[tokio::test]
async fn heal_rebases_on_an_underpaid_bundle_behind_the_ledger() -> anyhow::Result<()> {
    let (ctx, signer) = heal_test_ctx();
    let ledger = PoolLedger::new(Cumulative {
        bytes: U256::from(9_000u64),
        amount: U256::from(90u64),
    });
    let err = rejected_with_bundle(
        VoucherRejectReason::Underpaid,
        &ctx,
        &signer,
        (60, 5_000),
        Some(0),
    )?;
    assert_eq!(heal(&err, &ctx, &ledger).await, Some(Healed::Rebased));
    assert_eq!(
        ledger.committed(),
        Cumulative {
            bytes: U256::from(5_000u64),
            amount: U256::from(60u64),
        }
    );

    let stale = rejected_with_bundle(
        VoucherRejectReason::Underpaid,
        &ctx,
        &signer,
        (50, 4_000),
        Some(0),
    )?;
    assert_eq!(heal(&stale, &ctx, &ledger).await, Some(Healed::Stale));
    assert_eq!(ledger.committed().amount, U256::from(60u64));
    Ok(())
}

/// Only `Underpaid` rebases down. An `AmountRegression` bundle behind the
/// ledger means the ledger already moved past the proof the node refused:
/// the pull retries and the ledger stays where it is.
#[tokio::test]
async fn an_amount_regression_behind_the_ledger_retries_without_a_rebase() -> anyhow::Result<()> {
    let (ctx, signer) = heal_test_ctx();
    let seed = Cumulative {
        bytes: U256::from(9_000u64),
        amount: U256::from(90u64),
    };
    let ledger = PoolLedger::new(seed);
    let err = rejected_with_bundle(
        VoucherRejectReason::AmountRegression,
        &ctx,
        &signer,
        (60, 5_000),
        Some(0),
    )?;
    assert_eq!(heal(&err, &ctx, &ledger).await, Some(Healed::Stale));
    assert_eq!(ledger.generation(), 0);
    assert_eq!(ledger.committed(), seed);
    Ok(())
}

/// An `UnderFold` bundle ahead of the ledger reseeds it: the fold the node
/// states is what the payer owes, and it resumes from there.
#[tokio::test]
async fn heal_reseeds_on_an_under_fold_bundle_ahead_of_the_ledger() -> anyhow::Result<()> {
    let (ctx, signer) = heal_test_ctx();
    let ledger = PoolLedger::new(Cumulative {
        bytes: U256::from(5_000u64),
        amount: U256::from(60u64),
    });
    let err = rejected_with_bundle(
        VoucherRejectReason::UnderFold,
        &ctx,
        &signer,
        (90, 9_000),
        Some(0),
    )?;
    assert_eq!(heal(&err, &ctx, &ledger).await, Some(Healed::Reseeded));
    assert_eq!(ledger.committed().amount, U256::from(90u64));
    Ok(())
}

/// Concurrent streams on one lane take the same `UnderFold` with the same
/// bundle. The first heal reseeds; the second finds the ledger already at
/// the bundle and retries rather than failing the stream.
#[tokio::test]
async fn a_sibling_under_fold_after_the_reseed_is_stale() -> anyhow::Result<()> {
    let (ctx, signer) = heal_test_ctx();
    let ledger = PoolLedger::new(Cumulative {
        bytes: U256::from(5_000u64),
        amount: U256::from(60u64),
    });
    let first = rejected_with_bundle(
        VoucherRejectReason::UnderFold,
        &ctx,
        &signer,
        (90, 9_000),
        Some(0),
    )?;
    let second = rejected_with_bundle(
        VoucherRejectReason::UnderFold,
        &ctx,
        &signer,
        (90, 9_000),
        Some(0),
    )?;
    assert_eq!(heal(&first, &ctx, &ledger).await, Some(Healed::Reseeded));
    assert_eq!(heal(&second, &ctx, &ledger).await, Some(Healed::Stale));
    assert_eq!(
        ledger.committed(),
        Cumulative {
            bytes: U256::from(9_000u64),
            amount: U256::from(90u64),
        }
    );
    Ok(())
}

/// A restarted payer's concurrent streams each carry a stale proof, and the
/// node rejects each `AmountRegression` with the same bundle. The first heal
/// reseeds; the second finds the ledger already at the bundle and retries
/// rather than failing the stream (#2173).
#[tokio::test]
async fn a_sibling_amount_regression_after_the_reseed_is_stale() -> anyhow::Result<()> {
    let (ctx, signer) = heal_test_ctx();
    let ledger = PoolLedger::new(Cumulative::default());
    let rejected = || {
        rejected_with_bundle(
            VoucherRejectReason::AmountRegression,
            &ctx,
            &signer,
            (90, 9_000),
            None,
        )
    };
    assert_eq!(
        heal(&rejected()?, &ctx, &ledger).await,
        Some(Healed::Reseeded)
    );
    assert_eq!(heal(&rejected()?, &ctx, &ledger).await, Some(Healed::Stale));
    assert_eq!(
        ledger.committed(),
        Cumulative {
            bytes: U256::from(9_000u64),
            amount: U256::from(90u64),
        }
    );
    Ok(())
}

/// An `UnderFold` bundle that does not advance the ledger's amount cannot
/// heal it, even when it is ahead on bytes: reseeding would re-sign an amount
/// already spent. The rejection is terminal and the ledger is untouched.
#[tokio::test]
async fn heal_leaves_a_bytes_only_under_fold_terminal() -> anyhow::Result<()> {
    let (ctx, signer) = heal_test_ctx();
    let seed = Cumulative {
        bytes: U256::from(5_000u64),
        amount: U256::from(90u64),
    };
    let ledger = PoolLedger::new(seed);
    let err = rejected_with_bundle(
        VoucherRejectReason::UnderFold,
        &ctx,
        &signer,
        (90, 9_000),
        Some(0),
    )?;
    assert_eq!(heal(&err, &ctx, &ledger).await, None);
    assert_eq!(ledger.committed(), seed);
    assert_eq!(ledger.generation(), 0);
    Ok(())
}

/// A lane seeded below the node's anchor prices its spans from another base,
/// so its amount passes the anchor while its bytes still trail it, and the
/// node rejects `BytesRegression`. The bundle is our own voucher, behind on
/// amount and ahead on bytes: the ledger rebases to it, and hands the
/// anchor to the next persist once.
#[tokio::test]
async fn heal_rebases_on_a_bytes_regression_bundle_ahead_on_bytes() -> anyhow::Result<()> {
    let (ctx, signer) = heal_test_ctx();
    let ledger = PoolLedger::new(Cumulative {
        bytes: U256::from(5_000u64),
        amount: U256::from(90u64),
    });
    let err = rejected_with_bundle(
        VoucherRejectReason::BytesRegression,
        &ctx,
        &signer,
        (80, 9_000),
        Some(0),
    )?;
    let anchor = Cumulative {
        bytes: U256::from(9_000u64),
        amount: U256::from(80u64),
    };
    assert_eq!(heal(&err, &ctx, &ledger).await, Some(Healed::Rebased));
    assert_eq!(ledger.committed(), anchor);
    assert_eq!(ledger.generation(), 1);
    assert_eq!(ledger.take_unsaved_rebase(), Some(anchor));
    Ok(())
}

/// Concurrent streams on one lane take the same `BytesRegression` with the
/// same bundle. The first heal rebases. The rest find the ledger already
/// at that bundle and retry, whether or not the rejection names the
/// generation its voucher was signed under.
#[tokio::test]
async fn a_sibling_bytes_regression_after_the_rebase_is_stale() -> anyhow::Result<()> {
    let (ctx, signer) = heal_test_ctx();
    let ledger = PoolLedger::new(Cumulative {
        bytes: U256::from(5_000u64),
        amount: U256::from(90u64),
    });
    let rejected = |generation| {
        rejected_with_bundle(
            VoucherRejectReason::BytesRegression,
            &ctx,
            &signer,
            (80, 9_000),
            generation,
        )
    };
    assert_eq!(
        heal(&rejected(Some(0))?, &ctx, &ledger).await,
        Some(Healed::Rebased)
    );
    assert_eq!(
        heal(&rejected(Some(0))?, &ctx, &ledger).await,
        Some(Healed::Stale)
    );
    assert_eq!(
        heal(&rejected(None)?, &ctx, &ledger).await,
        Some(Healed::Stale)
    );
    assert_eq!(ledger.generation(), 1);
    Ok(())
}

/// A `BytesRegression` whose bundle the ledger covers on both axes says the
/// rejected voucher trailed the ledger, for example a sibling voucher
/// committed past the node's anchor first. The pull retries from the
/// ledger, which signs at or above the anchor, and nothing moves. A bundle
/// equal to the ledger on bytes is not ahead of it and does not rebase.
#[tokio::test]
async fn a_bytes_regression_the_ledger_covers_retries_without_a_rebase() -> anyhow::Result<()> {
    let (ctx, signer) = heal_test_ctx();
    let seed = Cumulative {
        bytes: U256::from(9_000u64),
        amount: U256::from(90u64),
    };
    for bundle in [(80, 5_000), (80, 9_000)] {
        let ledger = PoolLedger::new(seed);
        let err = rejected_with_bundle(
            VoucherRejectReason::BytesRegression,
            &ctx,
            &signer,
            bundle,
            Some(0),
        )?;
        assert_eq!(
            heal(&err, &ctx, &ledger).await,
            Some(Healed::Stale),
            "{bundle:?}"
        );
        assert_eq!(ledger.committed(), seed, "{bundle:?}");
        assert_eq!(ledger.generation(), 0, "{bundle:?}");
        assert_eq!(ledger.take_unsaved_rebase(), None, "{bundle:?}");
    }
    Ok(())
}

/// A `BytesRegression` for a voucher signed before the latest rebase is
/// stale even when its bundle is ahead of the ledger on bytes: it measured
/// its span from the anchor the ledger has left. One from the current
/// generation rebases again.
#[tokio::test]
async fn a_bytes_regression_from_an_earlier_generation_is_stale() -> anyhow::Result<()> {
    let (ctx, signer) = heal_test_ctx();
    let ledger = PoolLedger::new(Cumulative {
        bytes: U256::from(5_000u64),
        amount: U256::from(90u64),
    });
    let rejected = |bundle, generation| {
        rejected_with_bundle(
            VoucherRejectReason::BytesRegression,
            &ctx,
            &signer,
            bundle,
            generation,
        )
    };
    assert_eq!(
        heal(&rejected((80, 9_000), Some(0))?, &ctx, &ledger).await,
        Some(Healed::Rebased)
    );
    // The healed lane pays on.
    let progressed = Cumulative {
        bytes: U256::from(9_500u64),
        amount: U256::from(100u64),
    };
    assert!(ledger.reseed(progressed));

    assert_eq!(
        heal(&rejected((95, 9_800), Some(0))?, &ctx, &ledger).await,
        Some(Healed::Stale)
    );
    assert_eq!(ledger.committed(), progressed);
    assert_eq!(ledger.generation(), 1);

    assert_eq!(
        heal(&rejected((95, 9_800), Some(1))?, &ctx, &ledger).await,
        Some(Healed::Rebased)
    );
    assert_eq!(ledger.generation(), 2);
    assert_eq!(
        ledger.committed(),
        Cumulative {
            bytes: U256::from(9_800u64),
            amount: U256::from(95u64),
        }
    );
    Ok(())
}

/// A bundle not signed by our own key heals nothing, on `Underpaid` or on a
/// `BytesRegression` ahead of us on bytes: a node cannot move the payer's
/// ledger to a watermark it never signed.
#[tokio::test]
async fn heal_refuses_a_rebase_bundle_we_did_not_sign() -> anyhow::Result<()> {
    let (ctx, _signer) = heal_test_ctx();
    let stranger = alloy::signers::local::PrivateKeySigner::random();
    let seed = Cumulative {
        bytes: U256::from(9_000u64),
        amount: U256::from(90u64),
    };
    for (reason, bundle) in [
        (VoucherRejectReason::Underpaid, (60, 5_000)),
        (VoucherRejectReason::BytesRegression, (80, 12_000)),
    ] {
        let ledger = PoolLedger::new(seed);
        let err = rejected_with_bundle(reason, &ctx, &stranger, bundle, Some(0))?;
        assert_eq!(heal(&err, &ctx, &ledger).await, None, "{reason:?}");
        assert_eq!(ledger.committed(), seed, "{reason:?}");
    }
    Ok(())
}

/// Deterministic pseudo-random blob spanning several 16 KiB chunk groups.
fn make_blob(len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

fn sub(data: &[u8], start: u64, end: u64) -> anyhow::Result<Vec<u8>> {
    let s = usize::try_from(start)?;
    let e = usize::try_from(end)?;
    data.get(s..e)
        .map(<[u8]>::to_vec)
        .ok_or_else(|| anyhow::anyhow!("range [{start}, {end}) out of bounds"))
}

/// Produce the **header-less** bao wire stream a server emits for
/// `[byte_offset, byte_offset + byte_len)` of `blob` (`byte_len == 0` ⇒ to
/// end), plus the content root. `encode_verified_range` yields the combined
/// form (8-byte size header + interleaved stream); the wire drops the header.
fn wire_for(blob: &[u8], byte_offset: u64, byte_len: u64) -> anyhow::Result<([u8; 32], Vec<u8>)> {
    let ob = PreOrderMemOutboard::create(blob, IROH_BLOCK_SIZE);
    let root = *ob.root.as_bytes();
    let blob_size = u64::try_from(blob.len())?;
    let aligned = align_range(byte_offset, byte_len, blob_size)?;
    let data = sub(blob, aligned.fetch_start(), aligned.fetch_end())?;
    let combined = encode_verified_range(root, &aligned, &data, ob.data.clone().into())?;
    let wire = combined
        .get(8..)
        .ok_or_else(|| anyhow::anyhow!("combined shorter than 8-byte header"))?
        .to_vec();
    Ok((root, wire))
}

/// Drive `decode_to_vec` over a fixed wire buffer (a `Bytes` reader stashes
/// no fault, so the decoder's verdict is the whole story) and trim the
/// aligned superset back to `[byte_offset, total_bytes)` — the receive side
/// of `fetch_in_memory_once` with the live pull swapped for memory.
async fn decode_wire(
    root: [u8; 32],
    total_bytes: u64,
    byte_offset: u64,
    wire: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let aligned = align_range(byte_offset, 0, total_bytes)?;
    let (out, _reader) = decode_to_vec(
        root,
        total_bytes,
        &aligned,
        Bytes::copy_from_slice(wire),
        None,
    )
    .await?;
    let lead = usize::try_from(byte_offset.saturating_sub(aligned.fetch_start()))?;
    out.get(lead..)
        .map(<[u8]>::to_vec)
        .ok_or_else(|| anyhow::anyhow!("decoded range shorter than requested span"))
}

#[tokio::test]
async fn decode_to_vec_round_trips_whole_blob() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024 + 777);
    let (root, wire) = wire_for(&blob, 0, 0)?;
    let out = decode_wire(root, u64::try_from(blob.len())?, 0, &wire).await?;
    anyhow::ensure!(out == blob, "whole-blob round-trip");
    Ok(())
}

/// A resumed fetch at a group-aligned offset self-verifies against the root —
/// no dependency on the bytes before the offset (the old gap is closed).
#[tokio::test]
async fn decode_to_vec_resumed_group_aligned_offset() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024 + 777);
    let off = 64 * 1024; // 16 KiB-group aligned
    let (root, wire) = wire_for(&blob, off, 0)?;
    let out = decode_wire(root, u64::try_from(blob.len())?, off, &wire).await?;
    let want = sub(&blob, off, u64::try_from(blob.len())?)?;
    anyhow::ensure!(out == want, "resumed tail self-verifies");
    Ok(())
}

/// A non-group-aligned resume offset: the server serves the aligned superset
/// and the receiver trims the leading bytes back to the exact requested span.
#[tokio::test]
async fn decode_to_vec_trims_non_aligned_offset() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024 + 777);
    let off = 70 * 1024; // inside a group, not on a boundary
    let (root, wire) = wire_for(&blob, off, 0)?;
    let out = decode_wire(root, u64::try_from(blob.len())?, off, &wire).await?;
    let want = sub(&blob, off, u64::try_from(blob.len())?)?;
    anyhow::ensure!(out == want, "trimmed to requested offset");
    Ok(())
}

/// A corrupt tail byte is rejected at its chunk group with the typed
/// `HashMismatch` — even on a resumed fetch with no earlier bytes (ADR 038 #1).
#[tokio::test]
async fn decode_to_vec_rejects_corrupt_tail() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024 + 777);
    let off = 64 * 1024;
    let (root, mut wire) = wire_for(&blob, off, 0)?;
    // Flip a byte near the end of the stream — inside the final leaf's data.
    let last = wire
        .len()
        .checked_sub(8)
        .ok_or_else(|| anyhow::anyhow!("wire too short"))?;
    if let Some(b) = wire.get_mut(last) {
        *b ^= 0xff;
    }
    let err = decode_wire(root, u64::try_from(blob.len())?, off, &wire)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected corrupt tail to be rejected"))?;
    anyhow::ensure!(
        err.downcast_ref::<HashMismatch>().is_some(),
        "corrupt tail must surface HashMismatch, got: {err}"
    );
    Ok(())
}

/// ADR 038 AC#2 (early rejection at the offending group): a corrupt MIDDLE
/// group is rejected as `HashMismatch` even when everything AFTER it is
/// missing — detection needs no tail, which is what lets the live receive
/// loop stop pulling (and paying) at group *k*.
#[tokio::test]
async fn decode_to_vec_rejects_corrupt_middle_group_without_tail() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024 + 777);
    let (root, mut wire) = wire_for(&blob, 0, 0)?;
    // Corrupt a byte ~55% in (inside a middle group's data), then TRUNCATE
    // everything after ~70% — the decoder must fail on the corrupt group,
    // never reaching (or needing) the missing tail.
    let corrupt_at = wire.len() * 55 / 100;
    let truncate_at = wire.len() * 70 / 100;
    if let Some(b) = wire.get_mut(corrupt_at) {
        *b ^= 0xff;
    }
    wire.truncate(truncate_at);
    let err = decode_wire(root, u64::try_from(blob.len())?, 0, &wire)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected corrupt middle group to be rejected"))?;
    anyhow::ensure!(
        err.downcast_ref::<HashMismatch>().is_some(),
        "corrupt middle group must surface HashMismatch (early rejection), got: {err}"
    );
    Ok(())
}

/// A truncated-but-clean stream is a transport-class failure, NOT
/// corruption: it must NOT downcast to `HashMismatch`, because callers use
/// that sentinel to score the provider `Corruption` (tarring a peer for a
/// dropped connection would misattribute blame — #915 review).
#[tokio::test]
async fn decode_to_vec_truncation_is_not_hash_mismatch() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024 + 777);
    let (root, mut wire) = wire_for(&blob, 0, 0)?;
    wire.truncate(wire.len() * 60 / 100);
    let err = decode_wire(root, u64::try_from(blob.len())?, 0, &wire)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected truncated stream to be rejected"))?;
    anyhow::ensure!(
        err.downcast_ref::<HashMismatch>().is_none(),
        "clean truncation must NOT be classified as corruption, got HashMismatch: {err}"
    );
    Ok(())
}

/// Decoding an honest stream against the WRONG root fails closed (the range
/// can't be re-anchored), so a source serving a different blob is rejected.
#[tokio::test]
async fn decode_to_vec_rejects_wrong_root() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024 + 777);
    let (_root, wire) = wire_for(&blob, 0, 0)?;
    let wrong = [0xABu8; 32];
    let err = decode_wire(wrong, u64::try_from(blob.len())?, 0, &wire)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected wrong-root rejection"))?;
    anyhow::ensure!(
        err.downcast_ref::<HashMismatch>().is_some(),
        "wrong root must surface HashMismatch, got: {err}"
    );
    Ok(())
}

/// An open-stage refusal must carry the upstream's **signed** `StreamResponse`
/// out with it, not just the wire code (#1042).
///
/// This is the seam that makes real daemon output admissible on-chain. The
/// `slash_sig` here is produced by the same `StreamSlashData` signer the node
/// uses and covers the same EIP-712 digest `SlashJudge._verifyPair` checks, so
/// what the assertion actually proves is that the operator's self-incriminating
/// attestation survives the client's error path intact — byte-for-byte, still
/// recovering to the signer. Before #1042 `refusal()` took only
/// `response.error` and dropped the body and signature on the floor, leaving
/// `SlashJudge`'s rate path reachable only from a test that holds the
/// operator's key and synthesises its own evidence.
///
/// Deliberately routed through the private [`UpstreamRefused::open`] — the one
/// constructor the open stage uses — rather
/// than a hand-built value, which #1377's newtype now makes impossible outside
/// this crate anyway.
#[test]
fn an_open_stage_refusal_preserves_the_signed_stream_response() -> anyhow::Result<()> {
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::slash_judge_domain;
    use decdn_incentive::stream_sig::StreamSlashData;
    use decdn_protocol::client::{StreamError, StreamResponse, StreamResponseBody};

    use super::UpstreamRefused;

    let operator = PrivateKeySigner::random();
    let domain = slash_judge_domain(31_337, alloy::primitives::Address::repeat_byte(0x11));
    // The wire shape of a signed refusal: the node signed `ok = false` for a
    // hash it had just announced (now inert as slash evidence).
    let body = StreamResponseBody {
        hash: [0x5Au8; 32],
        ok: false,
        rate_per_mb: 10,
        total_bytes: 0,
        pool_id: [0x77u8; 32],
        timestamp_us: 1_700_000_000_000_000,
    };
    let sig = StreamSlashData::from_response_body(&body).sign(&operator, &domain)?;
    let response = StreamResponse {
        body: body.clone(),
        slash_sig: sig.as_bytes().to_vec(),
    };
    let response_ext = decdn_protocol::StreamResponseExt {
        error: Some(StreamError::Declined),
    };
    // Preconditions the real open stage enforces before ever calling `open`.
    response.validate()?;
    response_ext.validate(response.body.ok)?;

    let err = UpstreamRefused::open(response, &response_ext);
    let refused = err
        .downcast_ref::<UpstreamRefused>()
        .ok_or_else(|| anyhow::anyhow!("open() must stay a typed UpstreamRefused: {err:#}"))?;
    anyhow::ensure!(
        *refused.error() == StreamError::Declined,
        "the wire code must survive unchanged, got {:?}",
        refused.error()
    );
    let preserved = refused.evidence().ok_or_else(|| {
        anyhow::anyhow!("the signed StreamResponse must survive on the refusal (#1042)")
    })?;
    anyhow::ensure!(
        preserved.body == body,
        "the preserved body must be the signed body verbatim"
    );
    // #1377: `error()` is now DERIVED from the evidence by `open()`, so the two
    // legs cannot desync by construction. This pins that they agree.
    anyhow::ensure!(
        response_ext.error.as_ref() == Some(refused.error()),
        "the derived wire code {:?} must match the code the extension carried {:?}",
        refused.error(),
        response_ext.error,
    );
    // The whole point: the surviving signature still recovers to the operator,
    // so it can be replayed to `SlashJudge` with no re-signing by the observer.
    let recovered = alloy::primitives::Signature::try_from(preserved.slash_sig.as_slice())?;
    StreamSlashData::from_response_body(&preserved.body).verify_signer(
        &recovered,
        operator.address(),
        &domain,
    )?;
    Ok(())
}

/// #1377: `open()` on a `body.ok == false` response that carries no error code
/// is a protocol violation (the `validate` invariant was bypassed), so it does
/// NOT produce a typed `UpstreamRefused` that a challenger could act on — it
/// surfaces as a plain error instead of laundering a malformed refusal.
#[test]
fn open_on_a_response_without_an_error_code_is_not_a_typed_refusal() {
    use decdn_protocol::client::{StreamResponse, StreamResponseBody};

    use super::UpstreamRefused;

    let response = StreamResponse {
        body: StreamResponseBody {
            hash: [0x5Au8; 32],
            ok: false,
            rate_per_mb: 10,
            total_bytes: 0,
            pool_id: [0x77u8; 32],
            timestamp_us: 1_700_000_000_000_000,
        },
        slash_sig: vec![0u8; decdn_protocol::message::SLASH_SIG_LEN],
    };
    let err = UpstreamRefused::open(response, &decdn_protocol::StreamResponseExt::default());
    assert!(
        err.downcast_ref::<UpstreamRefused>().is_none(),
        "a response with no error code must not become a typed refusal"
    );
}

/// #1375: the two buyer ceilings combine as a min with `0` meaning "unbounded"
/// on each input, so a completed pull is always bounded by the LOWER of the
/// probe-relative and absolute bounds — and unbounded only when both are.
#[test]
fn effective_rate_ceiling_is_min_with_zero_as_unbounded() {
    use super::effective_rate_ceiling;
    assert_eq!(
        effective_rate_ceiling(0, 0),
        0,
        "both unbounded => unbounded"
    );
    assert_eq!(
        effective_rate_ceiling(10, 0),
        10,
        "config unbounded => probe"
    );
    assert_eq!(
        effective_rate_ceiling(0, 900),
        900,
        "probe unbounded => config"
    );
    assert_eq!(effective_rate_ceiling(10, 900), 10, "min: probe binds");
    assert_eq!(effective_rate_ceiling(900, 10), 10, "min: config binds");
    assert_eq!(effective_rate_ceiling(42, 42), 42, "equal bounds");
}

/// A **mid-stream** refusal carries no signature, so it must never carry a
/// `StreamResponse` either: the evidence contract promises a present value
/// always recovers to the delivering node, and a synthesised one would hand an
/// observer an unsigned artifact that breaks that promise (#1378). Routing all
/// four mid-stream sites through [`UpstreamRefused::mid_stream`] makes
/// `evidence() == None` a one-place decision; #1377 makes it a type property.
#[test]
fn a_mid_stream_refusal_never_carries_a_response() {
    use decdn_protocol::client::StreamError;

    use super::UpstreamRefused;

    let refused = UpstreamRefused::mid_stream(StreamError::Declined);
    assert!(
        refused.evidence().is_none(),
        "a mid-stream refusal has no signed response to carry"
    );
    assert_eq!(
        *refused.error(),
        StreamError::Declined,
        "the mid-stream wire code must survive unchanged"
    );
}
