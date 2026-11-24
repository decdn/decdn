use super::*;
use crate::voucher::{Voucher, voucher_domain};
use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{address, b256};
use alloy::signers::local::PrivateKeySigner;

#[test]
fn hydrate_seeds_registered_until_zero() {
    let st = LaneState::hydrate(
        B256::ZERO,
        Address::ZERO,
        Address::ZERO,
        U256::MAX,
        u64::MAX,
        U256::ZERO,
        U256::ZERO,
        None,
        LaneChain::NONE,
    );
    assert_eq!(st.registered_until, 0, "hydrate seeds unknown");
}

fn lane_with_terms(cap: u64, expiry: u64) -> LaneState {
    LaneState::hydrate(
        B256::ZERO,
        Address::ZERO,
        Address::ZERO,
        U256::from(cap),
        expiry,
        U256::ZERO,
        U256::ZERO,
        None,
        LaneChain::NONE,
    )
}

/// A presented capability wider than the registration holds the registered
/// terms; one narrower keeps its own (#2265).
#[test]
fn clamp_to_registration_holds_the_lower_terms() {
    let mut wider = lane_with_terms(5_000_000, 2_000);
    assert!(wider.clamp_to_registration(40, 1_000));
    assert_eq!(wider.cap, U256::from(40u64));
    assert_eq!(wider.expiry, 1_000);
    assert_eq!(wider.registered_until, 1_000);

    let mut narrower = lane_with_terms(10, 500);
    assert!(narrower.clamp_to_registration(40, 1_000));
    assert_eq!(narrower.cap, U256::from(10u64));
    assert_eq!(narrower.expiry, 500);
    assert_eq!(
        narrower.registered_until, 1_000,
        "the registration is recorded"
    );

    assert!(
        !narrower.clamp_to_registration(40, 1_000),
        "the same registration again changes nothing"
    );
}

/// A held expiry of `0` takes the registered expiry, the term the chain
/// redeems under. A registered expiry of `0` is already expired on-chain, so
/// the lane holds the past moment `1`.
#[test]
fn clamp_to_registration_maps_the_zero_expiries() {
    let mut held_zero = lane_with_terms(100, 0);
    held_zero.clamp_to_registration(100, 1_000);
    assert_eq!(held_zero.expiry, 1_000);

    let mut expired = lane_with_terms(100, 0);
    expired.clamp_to_registration(100, 0);
    assert_eq!(expired.expiry, 1, "a zero registered expiry holds as past");
    assert_eq!(expired.registered_until, 0);
}

const CHAIN_ID: u64 = 421_614;
const VERIFYING: Address = address!("0000000000000000000000000000000000001234");
const PROVIDER: Address = address!("00000000000000000000000000000000000000b2");
const POOL_ID: B256 = b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff");

/// Shared test fixture: a fresh keypair, an empty `LaneState` (cap 10 USDC),
/// and the voucher EIP-712 domain.
fn fixture() -> (PrivateKeySigner, LaneState, Eip712Domain) {
    let signer = PrivateKeySigner::random();
    let state = LaneState::hydrate(
        POOL_ID,
        signer.address(),
        PROVIDER,
        U256::from(10_000_000u64), // 10 USDC cap
        u64::MAX,
        U256::ZERO,
        U256::ZERO,
        None,
        LaneChain::NONE,
    );
    let domain = voucher_domain(CHAIN_ID, VERIFYING);
    (signer, state, domain)
}

/// Advance `state` by `signed` the way the serve path does, and swap the
/// successor in.
fn advance(state: &mut LaneState, signed: &SignedVoucher) -> Result<VoucherApplied, PoolError> {
    let (next, applied) = state.advance_presigned(signed)?;
    *state = next;
    Ok(applied)
}

/// Fold a reveal with no off-lock walk, so the lane hashes it against its own
/// live tip.
fn fold(
    state: &LaneState,
    root: B256,
    index: u8,
    preimage: B256,
) -> Result<(LaneState, PreimageApplied), PoolError> {
    state.advance_preimage_verified(root, index, preimage, None, false)
}

fn build(
    pool_id: B256,
    signer: Address,
    provider: Address,
    amount: u64,
    bytes_delivered: u64,
) -> Voucher {
    Voucher {
        pool_id,
        signer,
        provider,
        amount: U256::from(amount),
        bytes_delivered: U256::from(bytes_delivered),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
}

/// A metering voucher: opens (or re-asserts) an epoch on `chain_root` at
/// `chunk_price`.
fn build_metering(
    signer: Address,
    amount: u64,
    bytes_delivered: u64,
    chain_root: B256,
    chunk_price: u64,
) -> Voucher {
    Voucher {
        pool_id: POOL_ID,
        signer,
        provider: PROVIDER,
        amount: U256::from(amount),
        bytes_delivered: U256::from(bytes_delivered),
        chain_root,
        chunk_price: U256::from(chunk_price),
    }
}

const PRICE: u64 = 10;
/// A payer's chain seed, fixed here so the ladder is reproducible in tests;
/// a real one is drawn by `chain::random_seed`.
const SEED: B256 = B256::repeat_byte(0x5E);

fn root() -> B256 {
    crate::chain::root_from_seed(SEED)
}

fn reveal(index: u8) -> B256 {
    crate::chain::preimage_at(SEED, index)
}

fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
    r.err()
        .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
}

#[test]
fn first_voucher_advances_state() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    let signed =
        build(POOL_ID, signer.address(), PROVIDER, 1_000, 1_048_576).sign(&signer, &domain)?;

    let applied = advance(&mut state, &signed)?;
    anyhow::ensure!(state.last_amount == U256::from(1_000u64));
    anyhow::ensure!(state.last_bytes_delivered == U256::from(1_048_576u64));
    anyhow::ensure!(applied.amount_delta() == U256::from(1_000u64));
    anyhow::ensure!(applied.bytes_delta() == U256::from(1_048_576u64));
    Ok(())
}

#[test]
fn monotonic_progression_accepted() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    for (amount, bytes) in [(1_000u64, 1_048_576u64), (2_500, 2_621_440)] {
        let signed =
            build(POOL_ID, signer.address(), PROVIDER, amount, bytes).sign(&signer, &domain)?;
        advance(&mut state, &signed)?;
    }
    anyhow::ensure!(state.last_amount == U256::from(2_500u64));
    Ok(())
}

#[test]
fn equal_amount_rejected() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    let v1 = build(POOL_ID, signer.address(), PROVIDER, 1_000, 1).sign(&signer, &domain)?;
    advance(&mut state, &v1)?;

    let v2 = build(POOL_ID, signer.address(), PROVIDER, 1_000, 2).sign(&signer, &domain)?;
    let err = err_of(advance(&mut state, &v2))?;
    anyhow::ensure!(matches!(err, PoolError::AmountRegression { .. }), "{err:?}");
    Ok(())
}

#[test]
fn lower_amount_rejected() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    let v1 = build(POOL_ID, signer.address(), PROVIDER, 5_000, 1).sign(&signer, &domain)?;
    advance(&mut state, &v1)?;

    let v2 = build(POOL_ID, signer.address(), PROVIDER, 4_000, 2).sign(&signer, &domain)?;
    let err = err_of(advance(&mut state, &v2))?;
    anyhow::ensure!(matches!(err, PoolError::AmountRegression { .. }), "{err:?}");
    Ok(())
}

#[test]
fn bytes_decrease_rejected() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    let v1 = build(POOL_ID, signer.address(), PROVIDER, 1_000, 1_048_576).sign(&signer, &domain)?;
    advance(&mut state, &v1)?;

    let v2 = build(POOL_ID, signer.address(), PROVIDER, 2_000, 524_288).sign(&signer, &domain)?;
    let err = err_of(advance(&mut state, &v2))?;
    anyhow::ensure!(matches!(err, PoolError::BytesRegression { .. }), "{err:?}");
    Ok(())
}

#[test]
fn amount_exceeds_cap_rejected() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    // cap is 10_000_000 (10 USDC); attempt 11 USDC.
    let signed =
        build(POOL_ID, signer.address(), PROVIDER, 11_000_000, 1).sign(&signer, &domain)?;
    let err = err_of(advance(&mut state, &signed))?;
    anyhow::ensure!(matches!(err, PoolError::CapExceeded { .. }), "{err:?}");
    anyhow::ensure!(
        state.last_amount == U256::ZERO,
        "a rejection advances nothing"
    );
    Ok(())
}

// --- PayWord hash chain (ADR 003 §Hash-chain metering) -------------------

/// The base case: a voucher opens an epoch, and the lane starts metering
/// at index 0 with the root as its own tip — so a claim at exactly the
/// signed `amount` walks nothing.
#[test]
fn a_metering_voucher_opens_an_epoch_at_index_zero() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    let signed =
        build_metering(signer.address(), 1_000, 1_048_576, root(), PRICE).sign(&signer, &domain)?;
    advance(&mut state, &signed)?;

    let chain = state.chain();
    anyhow::ensure!(chain.chain_root == root());
    anyhow::ensure!(chain.chunk_price == U256::from(PRICE));
    anyhow::ensure!(chain.verified_index == 0);
    anyhow::ensure!(chain.tip == root(), "the root is its own tip at index 0");
    anyhow::ensure!(state.owed() == U256::from(1_000u64));
    Ok(())
}

/// One tick: a reveal at index 1 adds exactly one `chunk_price` over the
/// anchor and one `CHUNK_BYTES` on the byte axis.
#[test]
fn one_reveal_adds_one_chunk_price_over_the_anchor() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 1_048_576, root(), PRICE)
            .sign(&signer, &domain)?,
    )?;

    let (next, applied) = fold(&state, root(), 1, reveal(1))?;
    anyhow::ensure!(applied.advanced());
    anyhow::ensure!(applied.amount_delta() == U256::from(PRICE));
    anyhow::ensure!(applied.bytes_delta() == U256::from(crate::chain::CHUNK_BYTES));
    anyhow::ensure!(next.chain().verified_index == 1);
    anyhow::ensure!(next.chain().tip == reveal(1));
    anyhow::ensure!(next.owed() == U256::from(1_000 + PRICE));
    Ok(())
}

/// A fast stream may skip indices a slower one has not reached, so the walk
/// must span an arbitrary gap and credit every step it crossed.
#[test]
fn a_skipped_gap_credits_every_step_it_crossed() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;

    let (next, applied) = fold(&state, root(), 7, reveal(7))?;
    anyhow::ensure!(applied.amount_delta() == U256::from(7 * PRICE));
    anyhow::ensure!(next.owed() == U256::from(1_000 + 7 * PRICE));
    Ok(())
}

// --- Optimistic off-lock PayWord walk (issue #1792 item 5) ---------------

/// `preimage_frontier` names the `(verified_index, tip)` a reveal must hash
/// to, and reports `None` for anything that folds nothing — a covered index,
/// or a root the lane does not meter.
#[test]
fn preimage_frontier_names_the_walk_target_or_none() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    // Fresh epoch: index 3 walks from the root at index 0.
    anyhow::ensure!(state.preimage_frontier(root(), 3) == Some((0, root())));
    // A root this lane does not track folds nothing.
    anyhow::ensure!(
        state
            .preimage_frontier(B256::repeat_byte(0xEE), 3)
            .is_none()
    );

    let (state, _) = fold(&state, root(), 5, reveal(5))?;
    // At/below the frontier is covered — no walk.
    anyhow::ensure!(state.preimage_frontier(root(), 5).is_none());
    anyhow::ensure!(state.preimage_frontier(root(), 3).is_none());
    // Above it walks from the live tip at the live index.
    anyhow::ensure!(state.preimage_frontier(root(), 9) == Some((5, reveal(5))));
    Ok(())
}

/// The happy path: a walk run against the CURRENT frontier is trusted, and
/// yields exactly what a fold that re-hashes under the lock does.
#[test]
fn a_verified_walk_against_the_live_frontier_matches_an_in_lock_walk() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let walked = state.preimage_frontier(root(), 4);
    let Some((wv, wtip)) = walked else {
        anyhow::bail!("index 4 needs a walk on a fresh epoch");
    };
    let ok = crate::chain::verify_forward(reveal(4), 4 - wv, wtip);
    anyhow::ensure!(ok, "the honest reveal must verify");

    let (opt_next, opt_applied) =
        state.advance_preimage_verified(root(), 4, reveal(4), walked, ok)?;
    let (ref_next, ref_applied) = fold(&state, root(), 4, reveal(4))?;
    anyhow::ensure!(
        opt_next == ref_next,
        "optimistic apply diverged from the in-lock walk"
    );
    anyhow::ensure!(opt_applied.amount_delta() == ref_applied.amount_delta());
    anyhow::ensure!(opt_next.chain().verified_index == 4);
    Ok(())
}

/// The race: a sibling advanced the lane while this reveal was hashing, so the
/// snapshot `walked` no longer matches the live frontier. The stale walk is
/// discarded and re-hashed under the lock against the live tip, so the reveal
/// still lands correctly — the apply is never wrong, only occasionally re-walks.
#[test]
fn a_stale_walk_is_re_hashed_against_the_live_frontier() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    // This stream snapshots the fresh frontier for a walk to index 5.
    let stale = state.preimage_frontier(root(), 5);
    anyhow::ensure!(stale == Some((0, root())));
    let stale_ok = crate::chain::verify_forward(reveal(5), 5, root());

    // A sibling advances the lane to index 3 before this stream re-locks.
    let (advanced, _) = fold(&state, root(), 3, reveal(3))?;

    // Applied against the ADVANCED lane, the stale snapshot no longer matches
    // the live frontier `(3, reveal(3))`, so the value is re-hashed under the
    // lock and the reveal still lands at index 5.
    let (next, applied) =
        advanced.advance_preimage_verified(root(), 5, reveal(5), stale, stale_ok)?;
    anyhow::ensure!(next.chain().verified_index == 5);
    anyhow::ensure!(next.chain().tip == reveal(5));
    // It credits only the 5→3 = 2 steps the lane had not yet covered.
    anyhow::ensure!(applied.amount_delta() == U256::from(2 * PRICE));
    Ok(())
}

/// Safety of the trust gate: a `walked_ok = true` claimed against a STALE
/// frontier cannot smuggle in a bad preimage. Because the snapshot no longer
/// matches the live frontier, the caller's word is ignored and the value is
/// re-hashed under the lock — where the wrong preimage is caught.
#[test]
fn a_true_verdict_on_a_stale_frontier_cannot_bypass_the_walk() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let stale = state.preimage_frontier(root(), 5);
    let (advanced, _) = fold(&state, root(), 3, reveal(3))?;

    // A wrong preimage, but the caller lies that it verified — against the now
    // stale snapshot. The re-hash under the lock rejects it anyway.
    let foreign = B256::repeat_byte(0xAB);
    let err = err_of(advanced.advance_preimage_verified(root(), 5, foreign, stale, true))?;
    anyhow::ensure!(
        matches!(err, PoolError::BadPreimage { .. }),
        "a stale true verdict must not bypass the walk: {err:?}"
    );
    Ok(())
}

/// A covered reveal folds nothing regardless of what the caller walked — the
/// tracked-root and at-or-below-frontier gates run before `walked_ok` is
/// consulted, so a `None` walk with `false` verdict is still benign.
#[test]
fn a_covered_reveal_ignores_the_walk_verdict() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (advanced, _) = fold(&state, root(), 5, reveal(5))?;
    let (next, applied) = advanced.advance_preimage_verified(root(), 3, reveal(3), None, false)?;
    anyhow::ensure!(!applied.advanced(), "a covered reveal folds nothing");
    anyhow::ensure!(
        next.chain().verified_index == 5,
        "the frontier is untouched"
    );
    Ok(())
}

/// Deepest wins: a reveal at or below the frontier is already covered. It
/// advances nothing and is NOT an error — a duplicate or out-of-order
/// reveal is ordinary once several streams share one lane.
#[test]
fn a_reveal_at_or_below_the_frontier_is_already_covered() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (state, _) = fold(&state, root(), 5, reveal(5))?;

    for index in [1u8, 4, 5] {
        let (next, applied) = fold(&state, root(), index, reveal(index))?;
        anyhow::ensure!(!applied.advanced(), "index {index} must advance nothing");
        anyhow::ensure!(next.chain().verified_index == 5, "frontier must hold");
    }
    Ok(())
}

/// A value from another chain never reaches the tip, which is the whole
/// basis of the `BadPreimage` rejection.
#[test]
fn a_foreign_preimage_is_rejected() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;

    let foreign = crate::chain::preimage_at(B256::repeat_byte(0x5F), 3);
    let err = err_of(fold(&state, root(), 3, foreign))?;
    anyhow::ensure!(
        matches!(
            err,
            PoolError::BadPreimage {
                index: 3,
                verified: 0
            }
        ),
        "expected BadPreimage, got: {err:?}"
    );
    anyhow::ensure!(
        state.chain().verified_index == 0,
        "a rejection advances nothing"
    );
    Ok(())
}

/// Nothing hashes to zero, so a sealed voucher is sealed at exactly its
/// `amount`: no reveal can extend it, at any index.
#[test]
fn a_sealed_voucher_cannot_be_extended() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build(POOL_ID, signer.address(), PROVIDER, 1_000, 0).sign(&signer, &domain)?,
    )?;
    anyhow::ensure!(state.chain().chain_root.is_zero());

    for index in [1u8, 255] {
        let (next, applied) = fold(&state, B256::ZERO, index, reveal(index))?;
        anyhow::ensure!(!applied.advanced());
        anyhow::ensure!(next.owed() == U256::from(1_000u64));
    }
    Ok(())
}

/// The cooperative rollover: the payer folds the frontier the node actually
/// proved into the new voucher's `amount`, so the lane's total is unchanged
/// by the roll itself and the new epoch starts clean at index 0.
#[test]
fn a_correctly_folded_rollover_preserves_the_total() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (state_at_3, _) = fold(&state, root(), 3, reveal(3))?;
    let owed_before = state_at_3.owed();
    anyhow::ensure!(owed_before == U256::from(1_000 + 3 * PRICE));

    let next_seed = B256::repeat_byte(0x6E);
    let next_root = crate::chain::root_from_seed(next_seed);
    let folded = build_metering(
        signer.address(),
        1_000 + 3 * PRICE,
        3 * crate::chain::CHUNK_BYTES,
        next_root,
        PRICE,
    )
    .sign(&signer, &domain)?;
    let (rolled, _) = state_at_3.advance_presigned(&folded)?;

    anyhow::ensure!(rolled.owed() == owed_before, "the fold must lose nothing");
    anyhow::ensure!(rolled.chain().chain_root == next_root);
    anyhow::ensure!(rolled.chain().verified_index == 0);
    Ok(())
}

/// ADR 003 §Rollover: a payer that folds LESS than the frontier the node
/// proved is REFUSED. Adopting the new root would retire the old chain and
/// discard the difference, so the voucher is rejected before anything is
/// adopted and the lane's claim is exactly as strong afterwards as before.
///
/// The reason is watermark-gated, so the rejection carries the bundle that
/// tells the payer the fold it owes. A payer that resumes from a watermark
/// behind the node's frontier reaches it, and folds the bundle to recover.
#[test]
fn an_under_folded_rollover_is_refused() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (state_at_9, _) = fold(&state, root(), 9, reveal(9))?;
    let owed_before = state_at_9.owed();

    // Folds only 2 of the 9 chunks the node holds a preimage for.
    let next_root = crate::chain::root_from_seed(B256::repeat_byte(0x6E));
    let stingy = build_metering(
        signer.address(),
        1_000 + 2 * PRICE,
        2 * crate::chain::CHUNK_BYTES,
        next_root,
        PRICE,
    )
    .sign(&signer, &domain)?;

    anyhow::ensure!(
        matches!(
            state_at_9.advance_presigned(&stingy),
            Err(PoolError::UnderFold { axis: FoldAxis::Amount, owed, got })
                if owed == owed_before && got == U256::from(1_000 + 2 * PRICE)
        ),
        "an under-folding rollover must be refused, naming the fold it owed"
    );
    anyhow::ensure!(
        state_at_9.owed() == owed_before,
        "and the lane must be untouched by the refusal"
    );
    anyhow::ensure!(state_at_9.chain().chain_root == root());
    anyhow::ensure!(state_at_9.chain().verified_index == 9);
    Ok(())
}

/// A rollover at EXACTLY the signed anchor folds none of the proved
/// frontier. No sibling settled it under that root, so it is an under-fold,
/// not an ordering regression that the node would treat as already
/// satisfied. A payer process that sent reveals and exited before it
/// persisted them resumes here.
#[test]
fn a_rollover_at_the_anchor_over_a_proved_frontier_is_an_under_fold() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (state_at_1, _) = fold(&state, root(), 1, reveal(1))?;

    let next_root = crate::chain::root_from_seed(B256::repeat_byte(0x6E));
    let at_anchor =
        build_metering(signer.address(), 1_000, 0, next_root, PRICE).sign(&signer, &domain)?;
    anyhow::ensure!(
        matches!(
            state_at_1.advance_presigned(&at_anchor),
            Err(PoolError::UnderFold { axis: FoldAxis::Amount, owed, got })
                if owed == state_at_1.owed() && got == U256::from(1_000u64)
        ),
        "a rollover at the anchor over a proved frontier must be an under-fold"
    );

    // The same voucher over a frontier with nothing proved is an ordinary
    // ordering regression: the live claim is the anchor, and the node may
    // adopt the new root.
    let at_zero =
        build_metering(signer.address(), 1_000, 0, next_root, PRICE).sign(&signer, &domain)?;
    anyhow::ensure!(
        matches!(
            state.advance_presigned(&at_zero),
            Err(PoolError::AmountRegression { .. })
        ),
        "with nothing proved, the anchor voucher stays an ordering regression"
    );
    anyhow::ensure!(state.adopt_chain(&at_zero).is_some());
    Ok(())
}

/// A sealed voucher at the anchor is a sibling's closing voucher, not a
/// restart. One payer signs a sealed close at amount C, then a sibling opens
/// a chain at the same C and reveals on it. When the open and the reveal
/// reach the node first, the late sealed close sits at the anchor under a
/// root the lane does not meter. The payer already covered it, so it stays
/// an ordering regression, which the node treats as already satisfied.
#[test]
fn a_late_sealed_close_at_the_anchor_is_not_an_under_fold() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    let sealed_close =
        build_metering(signer.address(), 1_000, 0, B256::ZERO, 0).sign(&signer, &domain)?;
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (state_at_1, _) = fold(&state, root(), 1, reveal(1))?;

    anyhow::ensure!(
        matches!(
            state_at_1.advance_presigned(&sealed_close),
            Err(PoolError::AmountRegression { .. })
        ),
        "a late sealed close at the anchor must stay an ordering regression"
    );
    Ok(())
}

/// The fold binds the byte axis too. A rollover that pays the whole claim
/// but signs fewer bytes than the frontier proved is refused the same way:
/// adopting it would retire proved chunks from the lane's byte claim.
#[test]
fn a_rollover_folding_short_on_bytes_is_refused() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (state_at_9, _) = fold(&state, root(), 9, reveal(9))?;
    let bytes_before = state_at_9.owed_bytes();

    // Folds all 9 chunks on the money axis, only 2 on the byte axis.
    let next_root = crate::chain::root_from_seed(B256::repeat_byte(0x6E));
    let short_bytes = build_metering(
        signer.address(),
        1_000 + 9 * PRICE,
        2 * crate::chain::CHUNK_BYTES,
        next_root,
        PRICE,
    )
    .sign(&signer, &domain)?;

    anyhow::ensure!(
        matches!(
            state_at_9.advance_presigned(&short_bytes),
            Err(PoolError::UnderFold { axis: FoldAxis::Bytes, owed, got })
                if owed == bytes_before && got == U256::from(2 * crate::chain::CHUNK_BYTES)
        ),
        "a rollover short on bytes must be refused, naming the bytes it owed"
    );
    anyhow::ensure!(state_at_9.owed_bytes() == bytes_before);
    anyhow::ensure!(state_at_9.chain().chain_root == root());
    Ok(())
}

/// The refusal is watermark-gated, which is what makes it recoverable: the
/// wire reason a rejected under-fold maps to is the one the node attaches a
/// resume bundle to, so the payer learns the frontier it has to fold.
#[test]
fn the_under_fold_refusal_carries_a_resume_bundle() {
    let reason = crate::client_bridge::voucher_reject_reason(&PoolError::UnderFold {
        axis: FoldAxis::Amount,
        owed: U256::from(1_090u64),
        got: U256::from(1_020u64),
    });
    assert_eq!(
        reason,
        decdn_protocol::client::VoucherRejectReason::UnderFold
    );
    assert!(
        decdn_protocol::client::VoucherRejectReason::UnderFold.is_watermark_gated(),
        "the payer cannot fold correctly without the bundle that states the frontier"
    );
}

/// A reveal naming an epoch the lane no longer tracks is real but worth
/// nothing: a signature has since folded a frontier at least as deep.
#[test]
fn a_reveal_for_a_superseded_epoch_folds_nothing() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (state_at_3, _) = fold(&state, root(), 3, reveal(3))?;
    let next_root = crate::chain::root_from_seed(B256::repeat_byte(0x6E));
    let (rolled, _) = state_at_3.advance_presigned(
        &build_metering(
            signer.address(),
            1_000 + 3 * PRICE,
            3 * crate::chain::CHUNK_BYTES,
            next_root,
            PRICE,
        )
        .sign(&signer, &domain)?,
    )?;
    let (after, applied) = fold(&rolled, root(), 4, reveal(4))?;
    anyhow::ensure!(!applied.advanced());
    anyhow::ensure!(after.owed() == rolled.owed());
    Ok(())
}

/// Re-sending the epoch's root voucher is free and MUST NOT reset the
/// frontier — every stream emits it before its own first reveal of an
/// epoch, so a reset here would silently discard proved chunks.
#[test]
fn re_asserting_the_same_root_holds_the_frontier() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (state_at_6, _) = fold(&state, root(), 6, reveal(6))?;

    // A later voucher on the SAME epoch (a partial-chunk settlement, say).
    let same_epoch =
        build_metering(signer.address(), 1_500, 0, root(), PRICE).sign(&signer, &domain)?;
    let (after, _) = state_at_6.advance_presigned(&same_epoch)?;
    anyhow::ensure!(after.chain().verified_index == 6, "frontier must survive");
    anyhow::ensure!(after.chain().tip == reveal(6));
    anyhow::ensure!(after.owed() == U256::from(1_500 + 6 * PRICE));
    Ok(())
}

/// The full-depth case the walk bound exists for: a payer that abandons a
/// stream mid-chain leaves the node holding a claim worth 255 chunks over
/// the anchor, and every one of them is provable.
#[test]
fn a_full_depth_chain_is_claimable() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (deep, applied) = fold(
        &state,
        root(),
        crate::chain::MAX_CHAIN_LENGTH,
        reveal(crate::chain::MAX_CHAIN_LENGTH),
    )?;
    anyhow::ensure!(applied.amount_delta() == U256::from(255 * PRICE));
    anyhow::ensure!(deep.owed() == U256::from(1_000 + 255 * PRICE));
    let claim = deep
        .live_claim()
        .ok_or_else(|| anyhow::anyhow!("expected a claim"))?;
    anyhow::ensure!(claim.value() == deep.owed());
    anyhow::ensure!(
        claim.bytes_value() == U256::from(255u64) * U256::from(crate::chain::CHUNK_BYTES)
    );
    Ok(())
}

/// A second transfer on a lane whose previous one closed on a rollover
/// opens a fresh epoch through the ALREADY-SATISFIED path: the opening
/// voucher re-asserts the cumulative the lane already holds, so it advances
/// no money, but it still has to install the root it names. Refusing would
/// leave the lane metering the abandoned root, and every reveal that
/// followed would fold nothing — the delivery stalls with the node holding
/// a chain the payer no longer has a seed for.
#[test]
fn a_non_advancing_voucher_replaces_a_chain_that_proved_nothing() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;

    // A fresh root at the SAME cumulative — nothing was metered under the
    // old one, so there is nothing to lose by retiring it.
    let next_root = crate::chain::root_from_seed(B256::repeat_byte(0x6E));
    let opening =
        build_metering(signer.address(), 1_000, 0, next_root, PRICE).sign(&signer, &domain)?;
    let adopted = state
        .adopt_chain(&opening)
        .ok_or_else(|| anyhow::anyhow!("an unproved chain must yield to a fresh root"))?;
    anyhow::ensure!(adopted.chain().chain_root == next_root);
    anyhow::ensure!(adopted.chain().verified_index == 0);
    // The root and the signature MUST come from the same voucher: the
    // contract rebuilds the digest from both, so a claim pairing a fresh
    // root with the previous voucher's signature recovers the wrong signer
    // and reverts `InvalidVoucherSignature` on-chain.
    let claim = adopted
        .live_claim()
        .ok_or_else(|| anyhow::anyhow!("an adopted anchor is a claim"))?;
    let rebuilt = SignedVoucher {
        voucher: Voucher {
            pool_id: POOL_ID,
            signer: signer.address(),
            provider: PROVIDER,
            amount: claim.amount,
            bytes_delivered: claim.bytes_delivered,
            chain_root: claim.chain.chain_root,
            chunk_price: claim.chain.chunk_price,
        },
        signature: alloy::primitives::Signature::from_raw(&claim.signature)
            .map_err(|e| anyhow::anyhow!("claim signature is malformed: {e}"))?,
    };
    rebuilt.verify_signer(signer.address(), &domain)?;

    // And the new epoch meters for real.
    let (next, applied) = fold(
        &adopted,
        next_root,
        1,
        crate::chain::preimage_at(B256::repeat_byte(0x6E), 1),
    )?;
    anyhow::ensure!(applied.advanced());
    anyhow::ensure!(next.owed() == U256::from(1_000 + PRICE));
    Ok(())
}

/// A re-asserting voucher must not move the price out from under the
/// signature that covers it.
///
/// The trigger is ordinary: the node's quoted rate changes, and a payer
/// re-states the live root at the new price. The voucher passes the quote
/// check and signature recovery, and lands on the already-satisfied path. If
/// the price were refreshed in place, the lane would then pair the OLD
/// voucher's signature with the NEW price — and redemption rebuilds the
/// EIP-712 digest from both, so it recovers the wrong signer and reverts
/// `InvalidVoucherSignature`. That is a revert, not a zero-pay skip, so every
/// `redeemMany` batch carrying this lane fails wholesale and the node can
/// never collect the anchor at all.
#[test]
fn a_re_asserting_voucher_cannot_reprice_the_lane() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (proved, _) = fold(&state, root(), 4, reveal(4))?;

    // The same root, re-asserted at a higher price after a quote move.
    let repriced =
        build_metering(signer.address(), 1_000, 0, root(), PRICE * 2).sign(&signer, &domain)?;
    anyhow::ensure!(
        proved.adopt_chain(&repriced).is_none(),
        "an already-satisfied voucher pays for nothing and may not reprice the frontier"
    );

    // The claim the lane still holds is redeemable: its price is the one its
    // own signature was taken over.
    let claim = proved
        .live_claim()
        .ok_or_else(|| anyhow::anyhow!("a lane with a signature has a claim"))?;
    let rebuilt = SignedVoucher {
        voucher: Voucher {
            pool_id: POOL_ID,
            signer: signer.address(),
            provider: PROVIDER,
            amount: claim.amount,
            bytes_delivered: claim.bytes_delivered,
            chain_root: claim.chain.chain_root,
            chunk_price: claim.chain.chunk_price,
        },
        signature: alloy::primitives::Signature::from_raw(&claim.signature)
            .map_err(|e| anyhow::anyhow!("claim signature is malformed: {e}"))?,
    };
    rebuilt.verify_signer(signer.address(), &domain)?;
    Ok(())
}

/// The other side of that rule: a chain with reveals under it is worth more
/// than its anchor, and a voucher that did not pay for the difference has no
/// authority to retire it. Otherwise a stale or replayed voucher could strand
/// a frontier the node has already proved.
#[test]
fn a_non_advancing_voucher_cannot_retire_a_proved_chain() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (proved, _) = fold(&state, root(), 4, reveal(4))?;

    let next_root = crate::chain::root_from_seed(B256::repeat_byte(0x6E));
    let stale =
        build_metering(signer.address(), 1_000, 0, next_root, PRICE).sign(&signer, &domain)?;
    anyhow::ensure!(
        proved.adopt_chain(&stale).is_none(),
        "a proved frontier must survive a voucher that paid nothing for it"
    );
    anyhow::ensure!(proved.owed() == U256::from(1_000 + 4 * PRICE));
    Ok(())
}

/// The escape from that rule, and the one a resuming payer must take.
///
/// A node that rejects mid-chain reports its anchor AND the frontier its chain
/// has proved, and the payer's side of the bargain is to fold
/// `verified_index × chunk_price` into the amount it re-signs (ADR 005
/// §Watermark bundle). A voucher that does fold is no longer stale: it pays
/// for every chunk the frontier proved, so it may retire the chain and open a
/// fresh one — and this is the ONLY way out, since a payer that re-signed the
/// anchor alone would be refused by the test above and every reveal it sent
/// afterwards would name a root the lane never adopted.
#[test]
fn a_voucher_that_folds_the_proved_frontier_may_open_a_fresh_chain() -> anyhow::Result<()> {
    let (signer, mut state, domain) = fixture();
    advance(
        &mut state,
        &build_metering(signer.address(), 1_000, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;
    let (proved, _) = fold(&state, root(), 4, reveal(4))?;
    let folded_amount = 1_000 + 4 * PRICE;
    anyhow::ensure!(proved.owed() == U256::from(folded_amount));

    // Exactly what `Cumulative::from(&WatermarkBundle)` now hands a resuming
    // payer: the anchor with the frontier folded in, under a fresh root.
    let next_root = crate::chain::root_from_seed(B256::repeat_byte(0x6E));
    let resumed = build_metering(
        signer.address(),
        folded_amount,
        4 * crate::chain::CHUNK_BYTES,
        next_root,
        PRICE,
    )
    .sign(&signer, &domain)?;
    let (healed, _) = proved.advance_presigned(&resumed)?;

    anyhow::ensure!(
        healed.chain().chain_root == next_root,
        "a folding voucher installs the chain it names"
    );
    anyhow::ensure!(
        healed.chain().verified_index == 0,
        "the fresh chain starts at its own root, with nothing proved under it"
    );
    anyhow::ensure!(
        healed.owed() == U256::from(folded_amount),
        "the fold is exact: retiring the old chain strands none of its value \
         and duplicates none of it either"
    );
    Ok(())
}

/// A reveal answers for the capability's spending cap exactly as a voucher
/// does. Without this the chain would be a way around the cap: it advances the
/// claim with no new signature, so a cap checked only when a voucher arrives is
/// one the chain walks straight past — and the contract clamps payment at
/// `cap - spent` rather than reverting, so the node would deliver bytes it can
/// never collect for and simply eat the difference.
#[test]
fn a_reveal_past_the_spending_cap_is_refused() -> anyhow::Result<()> {
    let (signer, _, domain) = fixture();
    // A cap two chunks above the anchor, so the third reveal is the one that
    // cannot be paid for.
    let anchor = 1_000u64;
    let cap = U256::from(anchor + 2 * PRICE);
    let mut state = LaneState::hydrate(
        POOL_ID,
        signer.address(),
        PROVIDER,
        cap,
        u64::MAX,
        U256::ZERO,
        U256::ZERO,
        None,
        LaneChain::NONE,
    );
    advance(
        &mut state,
        &build_metering(signer.address(), anchor, 0, root(), PRICE).sign(&signer, &domain)?,
    )?;

    let (at_cap, _) = fold(&state, root(), 2, reveal(2))?;
    anyhow::ensure!(
        at_cap.owed() == cap,
        "a reveal landing exactly ON the cap is still payable"
    );
    anyhow::ensure!(
        matches!(
            fold(&at_cap, root(), 3, reveal(3)),
            Err(PoolError::CapExceeded { .. })
        ),
        "the reveal that would cross the cap must be refused, not credited"
    );
    Ok(())
}

/// A voucher that repeats the live root keeps the frontier, so its claim is
/// `amount + verified_index × chunk_price`, and the cap binds that whole claim,
/// as it binds a reveal. An amount bump under the cap whose frontier carries the
/// claim past it is refused; the same bump that lands exactly on the cap is not.
#[test]
fn a_same_root_voucher_whose_frontier_crosses_the_cap_is_refused() -> anyhow::Result<()> {
    let (signer, _, domain) = fixture();
    let anchor = 1_000u64;
    // Two reveals on top of the anchor, then a bump of `PRICE`: the claim is
    // `anchor + PRICE + 2 × PRICE`.
    let bumped = anchor + PRICE;
    let claim = bumped + 2 * PRICE;
    let lane = |cap: u64| -> anyhow::Result<LaneState> {
        let mut state = LaneState::hydrate(
            POOL_ID,
            signer.address(),
            PROVIDER,
            U256::from(cap),
            u64::MAX,
            U256::ZERO,
            U256::ZERO,
            None,
            LaneChain::NONE,
        );
        advance(
            &mut state,
            &build_metering(signer.address(), anchor, 0, root(), PRICE).sign(&signer, &domain)?,
        )?;
        Ok(fold(&state, root(), 2, reveal(2))?.0)
    };
    let bump = build_metering(signer.address(), bumped, 0, root(), PRICE).sign(&signer, &domain)?;

    let (at_cap, _) = lane(claim)?.advance_presigned(&bump)?;
    anyhow::ensure!(
        at_cap.owed() == U256::from(claim),
        "a bump whose claim lands exactly ON the cap is still payable"
    );

    let err = err_of(lane(claim - 1)?.advance_presigned(&bump))?;
    anyhow::ensure!(
        matches!(
            err,
            PoolError::CapExceeded { cap, got }
                if cap == U256::from(claim - 1) && got == U256::from(claim)
        ),
        "an amount under the cap must not carry the frontier past it: {err:?}"
    );

    // The adopted price counts: the same amount at a higher price re-values
    // the kept frontier past the cap.
    let repriced =
        build_metering(signer.address(), bumped, 0, root(), PRICE + 1).sign(&signer, &domain)?;
    let err = err_of(lane(claim)?.advance_presigned(&repriced))?;
    anyhow::ensure!(
        matches!(err, PoolError::CapExceeded { .. }),
        "a price bump that re-values the frontier past the cap must be refused: {err:?}"
    );
    Ok(())
}

/// A lane that has never accepted a voucher holds no claim and is owed
/// nothing — there is no signature to submit, whatever reveals arrive.
#[test]
fn a_lane_with_no_voucher_holds_no_claim() {
    let (_, state, _) = fixture();
    assert!(state.live_claim().is_none());
    assert_eq!(state.owed(), U256::ZERO);
}
