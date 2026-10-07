use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::store::PoolStateStore;
use decdn_incentive::{LaneKey, LaneState};
use tokio::sync::Mutex;

use super::super::{LaneDeliveryState, handler_over_store};
use crate::metrics::Metrics;

/// A well-formed voucher verifies against a fresh lane and advances the
/// candidate state to the voucher's cumulative amount/bytes — the pure,
/// no-I/O half of [`super::ClientHandler::commit_one_proof`]. The
/// read→verify→record path over real streams (durability included) is
/// covered end to end by the `client_loopback` integration family.
#[tokio::test]
async fn verify_voucher_advances_candidate() {
    let metrics = Arc::new(Metrics::new());
    let store =
        Arc::new(decdn_incentive::store::MemoryPoolStateStore::new()) as Arc<dyn PoolStateStore>;
    let (handler, _dir) = handler_over_store(&metrics, store).await;

    // `handler_over_store` builds all three EIP-712 domains from this literal,
    // so the voucher signer must sign over the same one.
    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let signer_key = PrivateKeySigner::random();
    let signer = signer_key.address();
    let pool_id = B256::repeat_byte(0x21);
    let provider = Address::repeat_byte(0x55);

    // Seed a fresh lane at a zero watermark with an ample cap.
    let lane_key = LaneKey {
        pool_id,
        signer,
        provider,
    };
    let seed = LaneState::hydrate(
        pool_id,
        signer,
        provider,
        U256::MAX,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    );
    let lane = Arc::new(Mutex::new(LaneDeliveryState {
        state: seed,
        bytes_delivered_cumulative: U256::ZERO,
        paid_credited: U256::ZERO,
        last_voucher_at: AtomicU64::new(0),
        ramp_pool: Arc::default(),
    }));
    handler.lanes.insert(lane_key, Arc::clone(&lane));

    // A cumulative voucher paying exactly one MB from zero, signed by the
    // lane's pinned signer over the lane context.
    let rate_per_mb = 1_000_000u64;
    let delta = decdn_incentive::rate::BYTES_PER_MB;
    let new_bytes = U256::from(delta);
    let amount = decdn_incentive::min_payment(delta, rate_per_mb);
    let signed_voucher = decdn_incentive::Voucher {
        pool_id,
        signer,
        provider,
        amount,
        bytes_delivered: new_bytes,
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(&signer_key, &domain)
    .expect("sign voucher");
    let wire = decdn_protocol::client::Voucher {
        signature: signed_voucher.signature.as_bytes().to_vec(),
        amount: u64::try_from(amount).expect("amount fits u64 in this test"),
        bytes_delivered: u64::try_from(new_bytes).expect("bytes fit u64 in this test"),
        chain_root: [0u8; 32],
        chunk_price: 0,
    };

    let snapshot = lane.lock().await.state.clone();
    let verified = super::ClientHandler::verify_voucher(
        &snapshot,
        U256::ZERO,
        &signed_voucher,
        &wire,
        rate_per_mb,
    )
    .expect("a well-formed voucher verifies against a fresh lane");
    assert_eq!(
        verified.new_bytes, new_bytes,
        "the candidate advances to the voucher's cumulative bytes"
    );
    assert_eq!(
        verified.next_state.last_amount(),
        amount,
        "the candidate advances to the voucher's cumulative amount"
    );
    assert_eq!(
        verified.amount,
        u64::try_from(amount).expect("amount fits u64 in this test"),
        "the receipt amount matches the voucher's cumulative amount"
    );
}

/// Rule #1 credit cap: a benign already-satisfied voucher (watermark
/// unchanged) on a lane with no slack credits ZERO — this is what blocks the
/// free-download leech (a client that delivers a window then pays only (0,0)
/// vouchers must not have its window reopened).
#[test]
fn credit_advance_benign_credits_zero() {
    let (new_credited, credited) = super::credit_advance(U256::ZERO, 500, U256::ZERO)
        .expect("credit_advance is infallible for in-range values");
    assert_eq!(new_credited, U256::ZERO);
    assert_eq!(
        credited, 0,
        "a benign voucher with no watermark slack must credit ZERO (leech block)"
    );
}

/// Rule #1 credit cap: an advance whose watermark grew to cover the delta
/// credits the full delta — no behavior change for honest flows.
#[test]
fn credit_advance_full_delta() {
    let (new_credited, credited) = super::credit_advance(U256::ZERO, 500, U256::from(500u64))
        .expect("credit_advance is infallible for in-range values");
    assert_eq!(new_credited, U256::from(500u64));
    assert_eq!(credited, 500, "an advance credits the full delivered delta");
}

/// Rule #1 credit cap: a voucher whose watermark is already fully credited
/// credits nothing further, and `paid_credited` does not advance past it.
#[test]
fn credit_advance_already_credited() {
    let (new_credited, credited) =
        super::credit_advance(U256::from(500u64), 500, U256::from(500u64))
            .expect("credit_advance is infallible for in-range values");
    assert_eq!(new_credited, U256::from(500u64));
    assert_eq!(
        credited, 0,
        "an already fully credited watermark credits nothing further"
    );
}

/// A wire voucher naming `chain_root`, for the credit-share tests. Only the
/// root matters to [`super::voucher_credit_delta`]; the other fields are
/// placeholders.
fn wire_voucher_with_root(chain_root: [u8; 32]) -> decdn_protocol::client::Voucher {
    decdn_protocol::client::Voucher {
        signature: vec![0u8; 65],
        amount: 0,
        bytes_delivered: 0,
        chain_root,
        chunk_price: 0,
    }
}

/// A stale proof is credited only when lane headroom pays all of its chunk.
/// The credit it would take stays uncommitted on a short headroom, so a stream
/// the caller then rejects consumes nothing (#2173).
#[test]
fn a_stale_proof_is_credited_only_in_full() -> anyhow::Result<()> {
    let chunk = super::super::CHUNK_BYTES;
    assert_eq!(
        super::credit_stale(U256::ZERO, chunk, U256::from(2 * chunk))?,
        Some((U256::from(chunk), chunk)),
        "headroom covering the chunk pays it"
    );
    assert_eq!(
        super::credit_stale(U256::from(chunk), chunk, U256::from(chunk + 1))?,
        None,
        "one byte of headroom does not pay a chunk"
    );
    assert_eq!(
        super::credit_stale(U256::from(chunk), chunk, U256::from(chunk))?,
        None,
        "no headroom pays nothing"
    );
    Ok(())
}

/// A metering voucher (live root) takes no whole chunk from lane headroom,
/// even when the headroom covers it: the reveal it precedes, or a sibling's
/// reveal it folded, pays that chunk.
#[test]
fn a_metering_voucher_credits_no_whole_chunk() {
    let chunk = super::super::CHUNK_BYTES;
    let wire = wire_voucher_with_root([0x5E; 32]);
    let delta = super::voucher_credit_delta(&wire, super::OwedChunk::new(chunk));
    assert_eq!(delta, 0, "a metering voucher pays no whole chunk");
    let (new_credited, credited) = super::credit_advance(U256::ZERO, delta, U256::from(4 * chunk))
        .expect("credit_advance is infallible for in-range values");
    assert_eq!(new_credited, U256::ZERO, "the headroom stays unclaimed");
    assert_eq!(credited, 0);
}

/// A metering voucher that settles a partial closing chunk is the payment
/// for those bytes, so it credits what the chunk still owes.
#[test]
fn a_metering_voucher_settles_a_closing_partial() {
    let partial = super::super::CHUNK_BYTES - 1;
    let wire = wire_voucher_with_root([0x5E; 32]);
    assert_eq!(
        super::voucher_credit_delta(&wire, super::OwedChunk::new(partial)),
        partial,
        "a closing residual under one chunk credits in full"
    );
}

/// A sealed voucher (zero root) meters no chain, so nothing else can pay the
/// chunk it answers: it credits what the chunk still owes.
#[test]
fn a_sealed_voucher_credits_a_whole_chunk() {
    let chunk = super::super::CHUNK_BYTES;
    let wire = wire_voucher_with_root([0u8; 32]);
    assert_eq!(
        super::voucher_credit_delta(&wire, super::OwedChunk::new(chunk)),
        chunk,
        "a sealed voucher pays its whole chunk"
    );
}

/// A proof that pays part of a chunk leaves the rest owed, and the chunk is
/// settled only when its remainder reaches zero (#2132).
#[test]
fn an_owed_chunk_settles_only_when_fully_paid() -> anyhow::Result<()> {
    let mut owed = super::OwedChunk::new(1000);
    assert!(!owed.settle(0)?, "a zero credit settles nothing");
    assert!(!owed.settle(400)?, "a partial credit leaves the rest owed");
    assert_eq!(owed.remaining(), 600);
    assert_eq!(owed.len(), 1000, "the delivered length does not change");
    assert!(owed.settle(600)?, "paying the remainder settles the chunk");
    assert_eq!(owed.remaining(), 0);
    Ok(())
}

/// A credit larger than what the chunk still owes is a node accounting fault,
/// never a quiet settle: the serve loop has already added it to `paid`.
#[test]
fn an_over_credit_is_refused() {
    let mut owed = super::OwedChunk::new(10);
    assert!(owed.settle(11).is_err(), "an over-credit must not settle");
}

/// The post-recoup accounting check passes only when the unpaid bytes are
/// exactly the ones not yet cut into a chunk.
#[test]
fn unpaid_bytes_must_all_be_tracked() {
    assert!(super::ensure_unpaid_bytes_tracked(1000, 900, 100).is_ok());
    assert!(
        super::ensure_unpaid_bytes_tracked(1000, 800, 100).is_err(),
        "a shortfall nothing tracks is refused"
    );
    assert!(
        super::ensure_unpaid_bytes_tracked(1000, 1001, 0).is_err(),
        "paid running ahead of delivered is refused"
    );
}

/// A whole chunk that a proof paid in part is still a whole chunk: a metering
/// voucher takes nothing from its remainder, so the remainder cannot eat the
/// headroom a sibling's reveal needs. A sealed voucher takes exactly the
/// remainder, never the chunk's full length.
#[test]
fn a_partly_paid_whole_chunk_keeps_its_whole_chunk_rule() {
    let chunk = super::super::CHUNK_BYTES;
    let mut owed = super::OwedChunk::new(chunk);
    assert!(
        !owed
            .settle(chunk / 4)
            .expect("a partial credit fits the chunk")
    );
    let remainder = chunk - chunk / 4;

    let metering = wire_voucher_with_root([0x5E; 32]);
    assert_eq!(
        super::voucher_credit_delta(&metering, owed),
        0,
        "a metering voucher pays no part of a whole chunk"
    );
    let sealed = wire_voucher_with_root([0u8; 32]);
    assert_eq!(
        super::voucher_credit_delta(&sealed, owed),
        remainder,
        "a sealed voucher pays what the chunk still owes"
    );
}

/// A partly paid closing partial chunk credits only its remainder, from any
/// voucher.
#[test]
fn a_partly_paid_closing_partial_credits_its_remainder() {
    let partial = super::super::CHUNK_BYTES / 2;
    let mut owed = super::OwedChunk::new(partial);
    assert!(!owed.settle(100).expect("a partial credit fits the chunk"));
    let metering = wire_voucher_with_root([0x5E; 32]);
    assert_eq!(super::voucher_credit_delta(&metering, owed), partial - 100);
}

/// A voucher at-or-below the lane watermark — a concurrent sibling raced ahead
/// — is `AlreadySatisfied`: `verify_voucher` returns Ok WITHOUT advancing the
/// candidate watermark. It must NOT reject (that would kill an honest lagging
/// stream, #1699).
#[test]
fn stale_voucher_is_benign_and_does_not_regress_watermark() {
    use alloy::primitives::B256;
    use alloy::signers::local::PrivateKeySigner;

    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let signer_key = PrivateKeySigner::random();
    let signer = signer_key.address();
    let pool_id = B256::repeat_byte(0x21);
    let provider = Address::repeat_byte(0x55);

    let rate_per_mb = 1_000_000u64;
    // Seed the lane already advanced to 2 MB (a sibling settled it).
    let two_mb = decdn_incentive::rate::BYTES_PER_MB * 2;
    let high_amount = decdn_incentive::min_payment(two_mb, rate_per_mb);
    let seed = LaneState::hydrate(
        pool_id,
        signer,
        provider,
        U256::MAX,
        0,
        high_amount,
        U256::from(two_mb),
        Some([9u8; 65]),
        decdn_incentive::LaneChain::NONE,
    );

    // A LOWER cumulative voucher: 1 MB. Signed correctly by the lane signer.
    let one_mb = decdn_incentive::rate::BYTES_PER_MB;
    let low_amount = decdn_incentive::min_payment(one_mb, rate_per_mb);
    let signed_low = decdn_incentive::Voucher {
        pool_id,
        signer,
        provider,
        amount: low_amount,
        bytes_delivered: U256::from(one_mb),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(&signer_key, &domain)
    .expect("sign low voucher");
    let wire = decdn_protocol::client::Voucher {
        signature: signed_low.signature.as_bytes().to_vec(),
        amount: u64::try_from(low_amount).expect("amount fits u64 in this test"),
        bytes_delivered: one_mb,
        chain_root: [0u8; 32],
        chunk_price: 0,
    };

    // verify against the high watermark; the sibling's watermark already
    // covers this stream's delivered.
    let verified = super::ClientHandler::verify_voucher(
        &seed,
        U256::from(two_mb),
        &signed_low,
        &wire,
        rate_per_mb,
    )
    .expect("a superseded but well-signed voucher is benign, not a reject");
    assert_eq!(
        verified.new_bytes,
        U256::from(two_mb),
        "the candidate watermark must NOT regress to the stale voucher"
    );
    assert_eq!(
        verified.next_state.last_amount(),
        high_amount,
        "the candidate amount must stay at the sibling-settled watermark"
    );
}

/// The single-signer guard (#1699 rule 4): a voucher at the SAME amount but a
/// HIGHER `bytes_delivered` — same money, more bytes claimed — is a divergent
/// fault, not a benign supersede.
#[test]
fn divergent_voucher_at_equal_amount_is_rejected() {
    use alloy::primitives::B256;
    use alloy::signers::local::PrivateKeySigner;

    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let signer_key = PrivateKeySigner::random();
    let signer = signer_key.address();
    let pool_id = B256::repeat_byte(0x21);
    let provider = Address::repeat_byte(0x55);

    let rate_per_mb = 1_000_000u64;
    let one_mb = decdn_incentive::rate::BYTES_PER_MB;
    let amount = decdn_incentive::min_payment(one_mb, rate_per_mb);
    // Seed at (amount, 1 MB).
    let seed = LaneState::hydrate(
        pool_id,
        signer,
        provider,
        U256::MAX,
        0,
        amount,
        U256::from(one_mb),
        Some([9u8; 65]),
        decdn_incentive::LaneChain::NONE,
    );
    // Same amount, but claims 2 MB of bytes.
    let two_mb = one_mb * 2;
    let divergent_voucher = decdn_incentive::Voucher {
        pool_id,
        signer,
        provider,
        amount,
        bytes_delivered: U256::from(two_mb),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(&signer_key, &domain)
    .expect("sign divergent voucher");
    let wire = decdn_protocol::client::Voucher {
        signature: divergent_voucher.signature.as_bytes().to_vec(),
        amount: u64::try_from(amount).expect("amount fits u64 in this test"),
        bytes_delivered: two_mb,
        chain_root: [0u8; 32],
        chunk_price: 0,
    };

    let err = super::ClientHandler::verify_voucher(
        &seed,
        U256::from(one_mb),
        &divergent_voucher,
        &wire,
        rate_per_mb,
    )
    .expect_err("a divergent equal-amount voucher must be rejected");
    match err {
        super::VerifyStop::Reject(reason, _) => assert_eq!(
            reason,
            decdn_protocol::client::VoucherRejectReason::BytesRegression,
            "divergent equal-amount voucher rejects as BytesRegression"
        ),
        super::VerifyStop::Bail(e) => panic!("expected a Reject, got Bail({e})"),
    }
}

/// The divergence rule binds both directions (#2173): a voucher at the SAME
/// amount signing FEWER bytes is the same money for a different byte count,
/// and is rejected `BytesRegression`. On a chain with nothing proved, the
/// live claim is the anchor, so this is not an under-fold. Swallowing it
/// would anchor its stream to a root it cannot install, and every reveal
/// after it would fold nothing.
#[test]
fn a_voucher_at_the_amount_with_fewer_bytes_is_rejected() {
    let lane = LiveChainLane::proved_to(0);
    let verified = lane.verify(
        B256::repeat_byte(0x6E),
        lane.price,
        lane.anchor_amount,
        lane.anchor_bytes - 1,
    );
    let Err(super::VerifyStop::Reject(reason, _)) = verified else {
        panic!("expected a BytesRegression Reject");
    };
    assert_eq!(
        reason,
        decdn_protocol::client::VoucherRejectReason::BytesRegression
    );
}

/// A voucher past the signed anchor on amount but short of it on bytes —
/// a payer that resumed below the node's anchor and priced its spans from
/// there — rejects `BytesRegression` WITH the watermark bundle. The bundle
/// is the anchor the payer rebases to (ADR 005); without it the payer's
/// lane would have no way back.
#[test]
fn a_voucher_past_the_amount_but_short_on_bytes_carries_the_anchor() {
    let lane = LiveChainLane::proved_to(0);
    let verified = lane.verify(
        B256::ZERO,
        U256::ZERO,
        lane.anchor_amount + lane.price,
        lane.anchor_bytes - decdn_incentive::chain::CHUNK_BYTES,
    );
    let Err(super::VerifyStop::Reject(reason, bundle)) = verified else {
        panic!("expected a BytesRegression Reject");
    };
    assert_eq!(
        reason,
        decdn_protocol::client::VoucherRejectReason::BytesRegression
    );
    let bundle = bundle.expect("a lane-level BytesRegression carries the watermark bundle");
    assert_eq!(
        U256::from(bundle.amount),
        lane.anchor_amount,
        "the bundle states the signed anchor"
    );
    assert_eq!(bundle.bytes_delivered, lane.anchor_bytes);
}

/// A lane holding a live chain: a signed anchor at 5 chunks plus 9 chunks
/// proved on top of it, and the key that signs its vouchers (#2167).
struct LiveChainLane {
    signer_key: alloy::signers::local::PrivateKeySigner,
    domain: alloy::sol_types::Eip712Domain,
    rate_per_mb: u64,
    price: U256,
    anchor_amount: U256,
    anchor_bytes: u64,
    state: LaneState,
}

impl LiveChainLane {
    const VERIFIED_INDEX: u8 = 9;

    fn new() -> Self {
        Self::proved_to(Self::VERIFIED_INDEX)
    }

    /// The same lane with `verified_index` chunks proved on its chain.
    fn proved_to(verified_index: u8) -> Self {
        use decdn_incentive::chain::{CHUNK_BYTES, preimage_at, root_from_seed};

        let signer_key = alloy::signers::local::PrivateKeySigner::random();
        let rate_per_mb = 1_000_000u64;
        let price = decdn_incentive::min_payment(CHUNK_BYTES, rate_per_mb);
        let anchor_amount = price * U256::from(5u64);
        let anchor_bytes = 5 * CHUNK_BYTES;
        let chain_seed = B256::repeat_byte(0x5E);
        let state = LaneState::hydrate(
            B256::repeat_byte(0x21),
            signer_key.address(),
            Address::repeat_byte(0x55),
            U256::MAX,
            0,
            anchor_amount,
            U256::from(anchor_bytes),
            Some([9u8; 65]),
            decdn_incentive::LaneChain {
                chain_root: root_from_seed(chain_seed),
                chunk_price: price,
                verified_index,
                tip: preimage_at(chain_seed, verified_index),
            },
        );
        Self {
            signer_key,
            domain: alloy::sol_types::eip712_domain! { name: "t", version: "1", },
            rate_per_mb,
            price,
            anchor_amount,
            anchor_bytes,
            state,
        }
    }

    /// Verify a voucher at `amount` / `bytes` under `chain_root`, priced at
    /// `chunk_price`, against this lane.
    fn verify(
        &self,
        chain_root: B256,
        chunk_price: U256,
        amount: U256,
        bytes: u64,
    ) -> Result<super::VerifiedVoucher, super::VerifyStop> {
        let key = self.state.key();
        let voucher = decdn_incentive::Voucher {
            pool_id: key.pool_id,
            signer: key.signer,
            provider: key.provider,
            amount,
            bytes_delivered: U256::from(bytes),
            chain_root,
            chunk_price,
        }
        .sign(&self.signer_key, &self.domain)
        .expect("sign voucher");
        let wire = decdn_protocol::client::Voucher {
            signature: voucher.signature.as_bytes().to_vec(),
            amount: u64::try_from(amount).expect("amount fits u64 in this test"),
            bytes_delivered: bytes,
            chain_root: chain_root.into(),
            chunk_price: u64::try_from(chunk_price).expect("price fits u64 in this test"),
        };
        super::ClientHandler::verify_voucher(
            &self.state,
            self.state.owed_bytes(),
            &voucher,
            &wire,
            self.rate_per_mb,
        )
    }

    /// Verify a sealed voucher (`chain_root = 0`, so a rollover against the
    /// live root) at `amount` / `bytes`.
    fn verify_sealed(
        &self,
        amount: U256,
        bytes: u64,
    ) -> Result<super::VerifiedVoucher, super::VerifyStop> {
        self.verify(B256::ZERO, U256::ZERO, amount, bytes)
    }
}

/// Unwrap an `UnderFold` rejection and return the bundle it carries.
fn expect_under_fold(
    verified: Result<super::VerifiedVoucher, super::VerifyStop>,
) -> decdn_protocol::client::WatermarkBundle {
    let Err(super::VerifyStop::Reject(reason, bundle)) = verified else {
        panic!("expected an under-fold Reject");
    };
    assert_eq!(
        reason,
        decdn_protocol::client::VoucherRejectReason::UnderFold
    );
    bundle.expect("the rejection carries the resume bundle")
}

/// #2167: a voucher under a different root whose amount sits ABOVE the
/// signed watermark but BELOW the live chain's claim under-folds the chain.
/// No sibling settled it, so it is not the benign already-satisfied case:
/// crediting it zero would leave the node waiting on a proof the payer never
/// sends. It is rejected `UnderFold` with the bundle naming the frontier the
/// payer has to fold.
#[test]
fn an_under_folding_voucher_is_rejected_with_the_resume_bundle() {
    use decdn_incentive::chain::CHUNK_BYTES;

    let lane = LiveChainLane::new();
    let chain = lane.state.chain();

    // Folds 2 of the 9 proved chunks: above the anchor, short of the claim.
    let bundle = expect_under_fold(lane.verify_sealed(
        lane.anchor_amount + lane.price * U256::from(2u64),
        lane.anchor_bytes + 2 * CHUNK_BYTES,
    ));
    assert_eq!(
        U256::from(bundle.amount),
        lane.anchor_amount,
        "the bundle echoes the signed anchor"
    );
    assert_eq!(bundle.bytes_delivered, lane.anchor_bytes);
    assert_eq!(
        bundle.verified_index,
        LiveChainLane::VERIFIED_INDEX,
        "the bundle names the frontier the payer has to fold"
    );
    assert_eq!(U256::from(bundle.chunk_price), lane.price);
    assert_eq!(B256::from(bundle.chain_root), chain.chain_root);
    assert_eq!(B256::from(bundle.tip), chain.tip);
}

/// At EXACTLY the signed anchor, the root decides. A re-send under the live
/// root is what a sibling sends before its reveals, and a sealed voucher is
/// a sibling's late closing voucher: both are already satisfied, and the
/// lane is untouched. The same amount under a fresh root folds none of the
/// proved frontier: a payer process that sent reveals and exited before it
/// persisted them resumes with it, and it is rejected `UnderFold` with the
/// bundle.
#[test]
fn a_voucher_at_the_anchor_is_an_under_fold_only_under_a_fresh_root() {
    let lane = LiveChainLane::new();
    let live_root = lane.state.chain().chain_root;

    for (root, price, what) in [
        (live_root, lane.price, "a re-send of the live root voucher"),
        (B256::ZERO, U256::ZERO, "a late sealed close"),
    ] {
        let verified = lane
            .verify(root, price, lane.anchor_amount, lane.anchor_bytes)
            .unwrap_or_else(|_| panic!("{what} at the anchor is benign"));
        assert_eq!(
            verified.next_state, lane.state,
            "{what} leaves the lane untouched"
        );
        assert_eq!(verified.new_bytes, lane.state.owed_bytes());
    }

    let bundle = expect_under_fold(lane.verify(
        B256::repeat_byte(0x6E),
        lane.price,
        lane.anchor_amount,
        lane.anchor_bytes,
    ));
    assert_eq!(bundle.verified_index, LiveChainLane::VERIFIED_INDEX);
}

/// The fold binds the byte axis too: a rollover that pays the whole live
/// claim but signs only the anchor's bytes is refused `UnderFold` with the
/// bundle, the same as one short on the amount.
#[test]
fn a_rollover_short_on_bytes_is_rejected_with_the_resume_bundle() {
    let lane = LiveChainLane::new();
    let bundle = expect_under_fold(lane.verify_sealed(
        lane.anchor_amount + lane.price * U256::from(LiveChainLane::VERIFIED_INDEX),
        lane.anchor_bytes,
    ));
    assert_eq!(bundle.verified_index, LiveChainLane::VERIFIED_INDEX);
}

/// #1735: signature validity is hoisted OUT of the per-lane lock. A voucher
/// signed by the wrong key is rejected by the (lock-free) signature recovery
/// — `verify_signer` against the lane's pinned signer — while the inside-lock
/// `advance_presigned` no longer inspects the signature at all: it would
/// happily advance the same voucher. This is exactly what lets concurrent
/// same-lane streams recover in parallel and only briefly serialize on the
/// advance.
#[test]
fn wrong_signer_is_rejected_without_the_lane_lock() {
    use alloy::primitives::{Address, B256, U256};
    use alloy::signers::local::PrivateKeySigner;

    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let pinned_key = PrivateKeySigner::random();
    let pinned_signer = pinned_key.address();
    let wrong_key = PrivateKeySigner::random();
    let pool_id = B256::repeat_byte(0x21);
    let provider = Address::repeat_byte(0x55);

    // A fresh lane pinned to `pinned_signer` at a zero watermark.
    let seed = LaneState::hydrate(
        pool_id,
        pinned_signer,
        provider,
        U256::MAX,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    );

    // A well-formed, monotone voucher — but SIGNED BY THE WRONG KEY. Its
    // `signer` field still names the pinned signer; only the signature is
    // forged, so recovery lands on `wrong_key`'s address.
    let rate_per_mb = 1_000_000u64;
    let one_mb = decdn_incentive::rate::BYTES_PER_MB;
    let amount = decdn_incentive::min_payment(one_mb, rate_per_mb);
    let forged = decdn_incentive::Voucher {
        pool_id,
        signer: pinned_signer,
        provider,
        amount,
        bytes_delivered: U256::from(one_mb),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(&wrong_key, &domain)
    .expect("sign with the wrong key");

    // OUTSIDE the lock: the signature recovery rejects it as WrongSigner. This
    // is the reject the handler performs before ever taking the guard.
    let err = forged
        .verify_signer(pinned_signer, &domain)
        .expect_err("a wrong-key voucher must fail signature recovery");
    assert!(
        matches!(err, decdn_incentive::VoucherError::WrongSigner { .. }),
        "wrong-key voucher recovers to a different signer: {err:?}"
    );

    // INSIDE the (would-be) lock: `advance_presigned` does NOT re-check the
    // signature — it advances the same forged voucher against the live
    // watermark. The safety of moving recovery out rests on this: the accept
    // decision is fully made by the lock-free recovery above.
    let (next, _applied) = seed
        .advance_presigned(&forged)
        .expect("advance_presigned ignores the signature and advances");
    assert_eq!(
        next.last_bytes_delivered(),
        U256::from(one_mb),
        "advance_presigned advanced the watermark without inspecting the signature"
    );
}

/// #1735: the monotonicity check and the watermark advance stay atomic under
/// the lock. Two same-lane streams that both recovered their vouchers against
/// the SAME zero snapshot cannot both advance: once the first advances the
/// live watermark, the second — re-checked against that LIVE watermark inside
/// the lock, not against its stale snapshot — is a benign `AmountRegression`
/// rather than a second advance that would lose the first's update.
#[test]
fn concurrent_advances_recheck_the_live_watermark_no_lost_update() {
    use alloy::primitives::{Address, B256, U256};
    use alloy::signers::local::PrivateKeySigner;

    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let signer_key = PrivateKeySigner::random();
    let signer = signer_key.address();
    let pool_id = B256::repeat_byte(0x21);
    let provider = Address::repeat_byte(0x55);

    let rate_per_mb = 1_000_000u64;
    let one_mb = decdn_incentive::rate::BYTES_PER_MB;
    let two_mb = one_mb * 2;

    // Both streams see the same zero-watermark snapshot when they recover.
    let snapshot = LaneState::hydrate(
        pool_id,
        signer,
        provider,
        U256::MAX,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    );

    let mk = |bytes: u64| {
        let amount = decdn_incentive::min_payment(bytes, rate_per_mb);
        decdn_incentive::Voucher {
            pool_id,
            signer,
            provider,
            amount,
            bytes_delivered: U256::from(bytes),
            chain_root: B256::ZERO,
            chunk_price: U256::ZERO,
        }
        .sign(&signer_key, &domain)
        .expect("sign voucher")
    };
    let voucher_hi = mk(two_mb); // the winner: advances to 2 MB
    let voucher_lo = mk(one_mb); // the straggler: recovered against zero too

    // First stream advances the live watermark to 2 MB.
    let (after_hi, _) = snapshot
        .advance_presigned(&voucher_hi)
        .expect("the higher cumulative voucher advances from zero");
    assert_eq!(after_hi.last_bytes_delivered(), U256::from(two_mb));

    // Second stream re-checks against the LIVE (2 MB) watermark — NOT its own
    // zero snapshot — so its lower cumulative is a regression, not an advance.
    // Advancing it against the stale snapshot would regress the watermark and
    // lose the first stream's update.
    let err = after_hi
        .advance_presigned(&voucher_lo)
        .expect_err("a straggler below the live watermark must not advance");
    assert!(
        matches!(err, decdn_incentive::PoolError::AmountRegression { .. }),
        "straggler is a benign amount regression against the live watermark: {err:?}"
    );

    // Sanity: against its own stale snapshot the straggler WOULD have advanced
    // — proving the re-check against the live watermark is what prevents the
    // lost update.
    snapshot
        .advance_presigned(&voucher_lo)
        .expect("against the stale zero snapshot the straggler advances");
}

#[derive(Clone, Default)]
struct CapturedLog(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The one line `log` writes for a fixed lane, captured at INFO.
fn reject_line(log_fn: impl FnOnce(&LaneKey)) -> String {
    let log = CapturedLog::default();
    let sink = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();
    let lane_key = LaneKey {
        pool_id: B256::repeat_byte(0x21),
        signer: Address::repeat_byte(0x33),
        provider: Address::repeat_byte(0x55),
    };
    tracing::subscriber::with_default(subscriber, || {
        log_fn(&lane_key);
    });
    let text = String::from_utf8(
        log.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .expect("the fmt layer writes UTF-8");
    let mut lines = text.lines();
    let line = lines.next().expect("a rejection logs a line").to_owned();
    assert!(lines.next().is_none(), "one line per rejection: {text}");
    line
}

/// A proof rejection counts on `decdn_serve_stream_voucher_rejected_total`,
/// and the counter has no reason split. The line must reach an INFO node
/// with the cause: the proof kind, the reason, the lane, and whether the
/// payer got a watermark to rebase from (#2342).
#[test]
fn a_proof_rejection_logs_one_info_line_with_reason_lane_and_bundle_flag() {
    use decdn_protocol::client::VoucherRejectReason;

    let line = reject_line(|lane| {
        super::log_proof_reject("voucher", lane, VoucherRejectReason::AmountRegression, true);
    });
    assert!(line.contains(" INFO "), "{line}");
    assert!(line.contains("rejecting a payment proof"), "{line}");
    assert!(line.contains("proof=\"voucher\""), "{line}");
    assert!(line.contains("reason=AmountRegression"), "{line}");
    assert!(
        line.contains(&format!("pool_id={}", B256::repeat_byte(0x21))),
        "{line}"
    );
    assert!(
        line.contains(&format!("signer={}", Address::repeat_byte(0x33))),
        "{line}"
    );
    assert!(line.contains("with_watermark=true"), "{line}");

    let line = reject_line(|lane| {
        super::log_proof_reject("reveal", lane, VoucherRejectReason::BadSignature, false);
    });
    assert!(line.contains(" INFO "), "{line}");
    assert!(line.contains("proof=\"reveal\""), "{line}");
    assert!(line.contains("reason=BadSignature"), "{line}");
    assert!(line.contains("with_watermark=false"), "{line}");
}

/// A rate-check bail writes no reject frame but still counts on
/// `decdn_serve_stream_voucher_rejected_total`, so it logs under the same
/// message with `reason=RateCheck` and the error (#2342).
#[test]
fn a_rate_check_bail_logs_one_info_line_under_the_reject_message() {
    let error = anyhow::Error::new(crate::handlers::client::wire::ClientPaymentFault)
        .context("voucher fails rate check: zero bytes");
    let line = reject_line(|lane| super::log_rate_check_reject(lane, &error));
    assert!(line.contains(" INFO "), "{line}");
    assert!(line.contains("rejecting a payment proof"), "{line}");
    assert!(line.contains("proof=\"voucher\""), "{line}");
    assert!(line.contains("reason=RateCheck"), "{line}");
    assert!(line.contains("with_watermark=false"), "{line}");
    assert!(
        line.contains("error=voucher fails rate check: zero bytes"),
        "{line}"
    );
}
