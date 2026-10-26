use std::sync::Arc;

use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::chain::{CHUNK_BYTES, MAX_CHAIN_LENGTH, preimage_at, root_from_seed};
use decdn_incentive::stream_sig::StreamSlashData;
use decdn_incentive::{
    LaneKey, Voucher, signed_to_wire_voucher, slash_judge_domain, voucher_domain,
};
use decdn_protocol::client::{StreamResponse, StreamResponseBody, Voucher as WireVoucher};

use super::*;
use crate::ledger::{ChainCommit, Cumulative, EpochAction, Metered, PoolLedger, StreamProof};

const CHAIN_ID: u64 = 421_614;
const RATE: u64 = 500;

fn chain() -> ReceiptChain {
    ReceiptChain {
        chain_id: CHAIN_ID,
        payment_pool: Address::repeat_byte(0x11),
        slash_judge: Address::repeat_byte(0x22),
    }
}

fn chain_config() -> crate::session::ChainConfig {
    let c = chain();
    crate::session::ChainConfig {
        rpc_url: "http://127.0.0.1:1".to_owned(),
        chain_id: c.chain_id,
        payment_pool: c.payment_pool,
        slash_judge: c.slash_judge,
        capacity_bond: None,
    }
}

struct Keys {
    payer: PrivateKeySigner,
    node: PrivateKeySigner,
    pool_id: B256,
}

impl Keys {
    fn new() -> Self {
        Self {
            payer: PrivateKeySigner::random(),
            node: PrivateKeySigner::random(),
            pool_id: B256::repeat_byte(0x33),
        }
    }

    fn key(&self) -> LaneKey {
        LaneKey {
            pool_id: self.pool_id,
            signer: self.payer.address(),
            provider: self.node.address(),
        }
    }

    fn response(&self, hash: B256, total_bytes: u64) -> StreamResponse {
        let body = StreamResponseBody {
            hash: hash.into(),
            ok: true,
            rate_per_mb: RATE,
            total_bytes,
            pool_id: self.pool_id.into(),
            timestamp_us: 1_700_000_000_000_000,
        };
        let c = chain();
        let sig = StreamSlashData::from_response_body(&body)
            .sign(&self.node, &slash_judge_domain(c.chain_id, c.slash_judge))
            .unwrap();
        StreamResponse {
            body,
            slash_sig: sig.as_bytes().to_vec(),
        }
    }

    fn voucher(&self, next: Cumulative, commit: ChainCommit) -> WireVoucher {
        let c = chain();
        let signed = Voucher {
            pool_id: self.pool_id,
            signer: self.payer.address(),
            provider: self.node.address(),
            amount: next.amount,
            bytes_delivered: next.bytes,
            chain_root: commit.chain_root,
            chunk_price: commit.chunk_price,
        }
        .sign(&self.payer, &voucher_domain(c.chain_id, c.payment_pool))
        .unwrap();
        signed_to_wire_voucher(&signed).unwrap()
    }
}

fn cumulative(amount: u64, bytes: u64) -> Cumulative {
    Cumulative {
        amount: U256::from(amount),
        bytes: U256::from(bytes),
    }
}

/// Issue one voucher on `ledger` the way the pull does: sign, keep it in the
/// lane's evidence, then send. Returns the cumulative it signed.
async fn issue(ledger: &PoolLedger, keys: &Keys, delta: u64, epoch: EpochAction) -> Cumulative {
    let evidence = ledger.attach_evidence();
    ledger
        .issue(delta, RATE, epoch, |next, commit| {
            evidence.note_voucher(&keys.voucher(next, commit));
            async { Ok(()) }
        })
        .await
        .unwrap()
}

/// Issue one voucher whose send fails after signing, as a dropped write does:
/// the voucher is kept as evidence and left armed.
async fn issue_unsent(ledger: &PoolLedger, keys: &Keys, delta: u64, epoch: EpochAction) {
    let evidence = ledger.attach_evidence();
    let sent = ledger
        .issue(delta, RATE, epoch, |next, commit| {
            evidence.note_voucher(&keys.voucher(next, commit));
            async { Err(anyhow::anyhow!("write failed")) }
        })
        .await;
    assert!(sent.is_err());
}

async fn reveal(ledger: &PoolLedger) -> crate::ledger::Released {
    let root = ledger.chain_root();
    match ledger.meter(root, |_| async { Ok(()) }).await.unwrap() {
        Metered::Released(released) => released,
        other => panic!("expected a release, got {other:?}"),
    }
}

fn content(hash: B256, size: u64) -> ReceiptContent {
    ReceiptContent {
        hash,
        kind: ContentKind::Blob,
        namespace: Some(7),
        entries: vec![ReceiptEntry { hash, size }],
        unlanded: Vec::new(),
        complete: true,
    }
}

fn window() -> ReceiptWindow {
    ReceiptWindow {
        started: "2026-10-24T14:02:11Z".to_owned(),
        finished: "2026-10-24T14:09:48Z".to_owned(),
    }
}

/// A collector with one lane for `keys`, on a ledger seeded at `seed`.
fn collected(keys: &Keys, seed: Cumulative) -> (ReceiptEvidence, Arc<PoolLedger>) {
    let ledger = Arc::new(PoolLedger::new(seed));
    let collector = ReceiptEvidence::new();
    collector.attach(&chain_config(), keys.key(), &ledger);
    (collector, ledger)
}

/// The collector's one lane.
async fn only_lane(collector: &ReceiptEvidence) -> ReceiptLane {
    let lanes = collector.lanes().await;
    assert_eq!(lanes.len(), 1, "{lanes:?}");
    lanes.into_iter().next().unwrap()
}

fn receipt_of(lanes: Vec<ReceiptLane>, hash: B256, size: u64) -> PullReceipt {
    PullReceipt::new(chain(), content(hash, size), window(), lanes)
}

/// A receipt built from a real ledger that opened a chain, revealed three
/// chunks and stops there.
async fn chained_receipt(keys: &Keys, seed: Cumulative) -> PullReceipt {
    let hash = B256::repeat_byte(0x44);
    let (collector, ledger) = collected(keys, seed);
    ledger
        .attach_evidence()
        .note_response(&keys.response(hash, 3 * CHUNK_BYTES));
    issue(&ledger, keys, 0, EpochAction::Open).await;
    for _ in 0..3 {
        reveal(&ledger).await;
    }
    receipt_of(collector.lanes().await, hash, 3 * CHUNK_BYTES)
}

fn proof(lane: &ReceiptLane) -> &ReceiptProof {
    lane.proof.as_ref().expect("the lane carries a proof")
}

#[test]
fn evidence_keeps_the_last_signed_vouchers() {
    let keys = Keys::new();
    let evidence = LaneEvidence::default();
    for amount in 1..=6u64 {
        evidence.note_voucher(&keys.voucher(cumulative(amount, amount), ChainCommit::SEALED));
    }
    let kept: Vec<u64> = evidence.inner().signed.iter().map(|v| v.amount).collect();
    assert_eq!(kept, vec![3, 4, 5, 6]);
}

#[test]
fn evidence_keeps_the_first_acceptance_per_hash() {
    let keys = Keys::new();
    let evidence = LaneEvidence::default();
    let mut refused = keys.response(B256::repeat_byte(1), 10);
    refused.body.ok = false;
    evidence.note_response(&refused);
    let first = keys.response(B256::repeat_byte(1), 10);
    evidence.note_response(&first);
    evidence.note_response(&keys.response(B256::repeat_byte(1), 99));
    evidence.note_response(&keys.response(B256::repeat_byte(2), 20));
    let kept = evidence.responses();
    assert_eq!(kept.len(), 2);
    assert_eq!(kept.first(), Some(&first));
}

#[test]
fn proof_needs_a_voucher_signing_the_anchor() {
    let keys = Keys::new();
    let evidence = LaneEvidence::default();
    evidence.note_voucher(&keys.voucher(cumulative(5, 5), ChainCommit::SEALED));
    let frontier = ProvedFrontier {
        anchor: cumulative(9, 9),
        baseline: Cumulative::default(),
        reveal: None,
    };
    assert!(evidence.proof_for(frontier).is_none());
}

#[test]
fn proof_drops_a_reveal_under_another_root() {
    let keys = Keys::new();
    let evidence = LaneEvidence::default();
    evidence.note_voucher(&keys.voucher(cumulative(5, 5), ChainCommit::SEALED));
    let frontier = ProvedFrontier {
        anchor: cumulative(5, 5),
        baseline: Cumulative::default(),
        reveal: Some(crate::ledger::Released {
            preimage: B256::repeat_byte(9),
            index: 2,
            chain_root: B256::repeat_byte(8),
        }),
    };
    let proof = evidence.proof_for(frontier).unwrap();
    assert_eq!(proof.voucher.amount, 5);
    assert!(proof.reveal.is_none());
    assert_eq!(
        proof.claim,
        ReceiptAmount {
            amount: 5,
            bytes: 5
        }
    );
}

#[test]
fn proof_takes_the_voucher_on_the_reveals_chain_over_a_newer_one() {
    let keys = Keys::new();
    let evidence = LaneEvidence::default();
    let commit = |root: u8| ChainCommit {
        chain_root: B256::repeat_byte(root),
        chunk_price: U256::from(RATE),
    };
    evidence.note_voucher(&keys.voucher(cumulative(5, 5), commit(0xa0)));
    evidence.note_voucher(&keys.voucher(cumulative(5, 5), commit(0xb0)));
    let frontier = ProvedFrontier {
        anchor: cumulative(5, 5),
        baseline: Cumulative::default(),
        reveal: Some(crate::ledger::Released {
            preimage: B256::repeat_byte(9),
            index: 2,
            chain_root: B256::repeat_byte(0xa0),
        }),
    };
    let proof = evidence.proof_for(frontier).unwrap();
    assert_eq!(proof.voucher.chain_root, B256::repeat_byte(0xa0));
    assert_eq!(proof.reveal.map(|r| r.index), Some(2));
}

#[tokio::test]
async fn a_chained_lane_claims_its_reveals_and_verifies() {
    let keys = Keys::new();
    let receipt = chained_receipt(&keys, cumulative(500, 0)).await;
    assert_eq!(receipt.verify(), Vec::<ReceiptFault>::new());
    let lane = receipt.lanes.first().unwrap();
    assert_eq!(proof(lane).reveal.map(|r| r.index), Some(3));
    assert_eq!(
        proof(lane).claim,
        ReceiptAmount {
            amount: 500 + 3 * RATE,
            bytes: 3 * CHUNK_BYTES,
        }
    );
    assert_eq!(
        lane.prior,
        ReceiptAmount {
            amount: 500,
            bytes: 0
        }
    );
    let summary = receipt.summary();
    assert_eq!(summary.paid.amount, 3 * RATE);
    assert_eq!(summary.paid.bytes, 3 * CHUNK_BYTES);
    assert_eq!((summary.node_signatures, summary.payer_signatures), (1, 1));
}

#[tokio::test]
async fn a_closing_voucher_folds_the_chain_into_the_claim() {
    let keys = Keys::new();
    let (collector, ledger) = collected(&keys, Cumulative::default());
    issue(&ledger, &keys, 0, EpochAction::Open).await;
    reveal(&ledger).await;
    reveal(&ledger).await;
    issue(&ledger, &keys, 100, EpochAction::Keep).await;
    let lane = only_lane(&collector).await;
    assert!(
        proof(&lane).reveal.is_none(),
        "the fold rolled to a fresh chain"
    );
    assert_eq!(proof(&lane).claim.bytes, 2 * CHUNK_BYTES + 100);
    assert_eq!(
        proof(&lane).claim,
        ReceiptAmount::try_from(ledger.committed()).unwrap(),
        "the claim is the ledger's committed cumulative"
    );
}

#[tokio::test]
async fn a_lane_that_paid_nothing_is_left_out() {
    let keys = Keys::new();
    let (collector, _ledger) = collected(&keys, cumulative(40, 40));
    assert!(collector.lanes().await.is_empty());
}

#[tokio::test]
async fn a_collector_attaches_each_ledger_once() {
    let keys = Keys::new();
    let (collector, ledger) = collected(&keys, cumulative(3, 3));
    collector.attach(&chain_config(), keys.key(), &ledger);
    issue(&ledger, &keys, CHUNK_BYTES, EpochAction::Keep).await;
    let lane = only_lane(&collector).await;
    assert_eq!(
        lane.prior,
        ReceiptAmount {
            amount: 3,
            bytes: 3
        }
    );
}

#[tokio::test]
async fn a_rejected_voucher_rewinds_the_proof_to_the_one_before() {
    let keys = Keys::new();
    let (collector, ledger) = collected(&keys, Cumulative::default());
    let first = issue(&ledger, &keys, 0, EpochAction::Open).await;
    for _ in 0..2 {
        reveal(&ledger).await;
    }
    // A closing voucher folds the two reveals and rolls; the node refuses it.
    let refused = issue(&ledger, &keys, 100, EpochAction::Keep).await;
    assert!(ledger.resolve_reject(StreamProof::Voucher {
        amount: refused.amount,
        generation: 0,
    }));
    let lane = only_lane(&collector).await;
    let proof = proof(&lane);
    assert_eq!(proof.voucher.amount, u64::try_from(first.amount).unwrap());
    assert_eq!(
        proof.reveal.map(|r| r.index),
        Some(2),
        "the old chain is restored"
    );
    assert_eq!(proof.claim.amount, 2 * RATE);
    assert_eq!(receipt_of(vec![lane], B256::ZERO, 0).verify(), Vec::new());
}

#[tokio::test]
async fn a_rejected_reveal_leaves_the_reveal_one_shallower() {
    let keys = Keys::new();
    let (collector, ledger) = collected(&keys, Cumulative::default());
    issue(&ledger, &keys, 0, EpochAction::Open).await;
    reveal(&ledger).await;
    let refused = reveal(&ledger).await;
    assert!(ledger.resolve_reject(StreamProof::Reveal {
        chain_root: refused.chain_root,
        index: refused.index,
    }));
    let lane = only_lane(&collector).await;
    assert_eq!(proof(&lane).reveal.map(|r| r.index), Some(1));
    assert_eq!(receipt_of(vec![lane], B256::ZERO, 0).verify(), Vec::new());
}

#[tokio::test]
async fn an_unsent_voucher_proves_only_once_a_stream_end_confirms_it() {
    let keys = Keys::new();
    let (collector, ledger) = collected(&keys, Cumulative::default());
    let anchor = issue(&ledger, &keys, CHUNK_BYTES, EpochAction::Keep).await;
    issue_unsent(&ledger, &keys, CHUNK_BYTES, EpochAction::Keep).await;
    // Unconfirmed: the claim stays on the anchor the node holds.
    let lane = only_lane(&collector).await;
    assert_eq!(proof(&lane).claim, ReceiptAmount::try_from(anchor).unwrap());
    // Confirmed: the voucher signed before the failed write is the proof.
    let armed = ledger.settlement();
    assert!(ledger.confirm_armed(armed.amount).await.is_some());
    let lane = only_lane(&collector).await;
    assert_eq!(proof(&lane).claim, ReceiptAmount::try_from(armed).unwrap());
    assert_eq!(receipt_of(vec![lane], B256::ZERO, 0).verify(), Vec::new());
}

#[tokio::test]
async fn a_reseed_is_not_this_pulls_payment() {
    let keys = Keys::new();
    let (collector, ledger) = collected(&keys, cumulative(100, 100));
    let first = issue(&ledger, &keys, CHUNK_BYTES, EpochAction::Keep).await;
    // The node holds a higher watermark from another data dir's pulls.
    let jump = cumulative(10_000, 20 * CHUNK_BYTES);
    assert!(ledger.reseed(jump));
    let lane = only_lane(&collector).await;
    assert!(
        lane.proof.is_none(),
        "no signature of this pull states the jump"
    );
    let paid_before = u64::try_from(first.amount).unwrap() - 100;
    assert_eq!(
        lane.prior.amount,
        10_000 - paid_before,
        "the baseline rose by the jump"
    );
    let receipt = receipt_of(vec![lane], B256::ZERO, 0);
    assert_eq!(receipt.verify(), Vec::new());
    assert_eq!(receipt.summary().unproved_lanes, 1);
    assert_eq!(receipt.summary().paid, ReceiptAmount::default());

    // The next voucher proves the lane again, and only this pull's payments
    // count as paid.
    issue(&ledger, &keys, CHUNK_BYTES, EpochAction::Keep).await;
    let lane = only_lane(&collector).await;
    assert_eq!(lane.paid().amount, paid_before + RATE);
    assert_eq!(receipt_of(vec![lane], B256::ZERO, 0).verify(), Vec::new());
}

#[tokio::test]
async fn a_rebase_below_the_seed_counts_the_re_signed_claims() {
    let keys = Keys::new();
    let (collector, ledger) = collected(&keys, cumulative(5_000, 10 * CHUNK_BYTES));
    issue(&ledger, &keys, CHUNK_BYTES, EpochAction::Keep).await;
    // The node accepted less than the seed: everything above its watermark is
    // signed again by this ledger, and counts as this pull's payment.
    let floor = cumulative(2_000, 4 * CHUNK_BYTES);
    ledger.rebase(floor, None).await;
    issue(&ledger, &keys, CHUNK_BYTES, EpochAction::Keep).await;
    let lane = only_lane(&collector).await;
    assert_eq!(lane.prior, ReceiptAmount::try_from(floor).unwrap());
    assert_eq!(lane.paid().amount, RATE);
    assert_eq!(receipt_of(vec![lane], B256::ZERO, 0).verify(), Vec::new());
}

#[tokio::test]
async fn a_full_chain_and_its_rollover_verify() {
    let keys = Keys::new();
    let (collector, ledger) = collected(&keys, Cumulative::default());
    issue(&ledger, &keys, 0, EpochAction::Open).await;
    for _ in 0..MAX_CHAIN_LENGTH {
        reveal(&ledger).await;
    }
    let lane = only_lane(&collector).await;
    assert_eq!(proof(&lane).reveal.map(|r| r.index), Some(MAX_CHAIN_LENGTH));
    assert_eq!(
        proof(&lane).claim.amount,
        u64::from(MAX_CHAIN_LENGTH) * RATE
    );
    assert_eq!(receipt_of(vec![lane], B256::ZERO, 0).verify(), Vec::new());

    // The roll folds the spent chain; two reveals extend the fresh one.
    let rolled = issue(&ledger, &keys, 0, EpochAction::Roll).await;
    reveal(&ledger).await;
    reveal(&ledger).await;
    let lane = only_lane(&collector).await;
    assert_eq!(
        Some(proof(&lane).voucher.chain_root),
        ledger.chain_root(),
        "the proof is the roll voucher"
    );
    assert_eq!(
        proof(&lane).claim.amount,
        u64::try_from(rolled.amount).unwrap() + 2 * RATE
    );
    assert_eq!(receipt_of(vec![lane], B256::ZERO, 0).verify(), Vec::new());
}

#[tokio::test]
async fn an_unconfirmed_rollover_claims_only_the_old_anchor() {
    let keys = Keys::new();
    let (collector, ledger) = collected(&keys, Cumulative::default());
    let anchor = issue(&ledger, &keys, CHUNK_BYTES, EpochAction::Open).await;
    reveal(&ledger).await;
    // The roll's send fails: the chain it displaced keeps its reveal in the
    // armed fold, which nothing confirmed, so the proof is the old anchor.
    issue_unsent(&ledger, &keys, 0, EpochAction::Roll).await;
    let lane = only_lane(&collector).await;
    assert_eq!(proof(&lane).claim, ReceiptAmount::try_from(anchor).unwrap());
    assert!(proof(&lane).reveal.is_none());
    assert_eq!(receipt_of(vec![lane], B256::ZERO, 0).verify(), Vec::new());
}

#[tokio::test]
async fn two_ledgers_on_one_lane_merge() {
    let keys = Keys::new();
    let collector = ReceiptEvidence::new();
    let (low, high) = (
        Arc::new(PoolLedger::new(cumulative(10, 10))),
        Arc::new(PoolLedger::new(cumulative(50, 50))),
    );
    collector.attach(&chain_config(), keys.key(), &low);
    collector.attach(&chain_config(), keys.key(), &high);
    low.attach_evidence()
        .note_response(&keys.response(B256::repeat_byte(1), 1));
    high.attach_evidence()
        .note_response(&keys.response(B256::repeat_byte(2), 2));
    issue(&low, &keys, CHUNK_BYTES, EpochAction::Keep).await;
    let top = issue(&high, &keys, CHUNK_BYTES, EpochAction::Keep).await;
    let lane = only_lane(&collector).await;
    assert_eq!(proof(&lane).claim, ReceiptAmount::try_from(top).unwrap());
    assert_eq!(
        lane.prior,
        ReceiptAmount {
            amount: 10,
            bytes: 10
        }
    );
    assert_eq!(lane.responses.len(), 2);
}

#[test]
fn a_merge_prefers_a_lane_with_a_proof() {
    let keys = Keys::new();
    let bare = ReceiptLane {
        pool_id: keys.pool_id,
        signer: keys.payer.address(),
        provider: keys.node.address(),
        responses: Vec::new(),
        proof: None,
        prior: ReceiptAmount::default(),
        settlement: None,
    };
    let proved = ReceiptLane {
        proof: Some(ReceiptProof {
            voucher: ReceiptVoucher::from(&keys.voucher(cumulative(9, 9), ChainCommit::SEALED)),
            reveal: None,
            claim: ReceiptAmount {
                amount: 9,
                bytes: 9,
            },
        }),
        ..bare.clone()
    };
    for order in [
        [bare.clone(), proved.clone()],
        [proved.clone(), bare.clone()],
    ] {
        let mut out = Vec::new();
        for lane in order {
            merge_lane(&mut out, lane);
        }
        assert_eq!(out.len(), 1);
        assert!(out.first().unwrap().proof.is_some());
    }
}

fn lane0(r: &mut PullReceipt) -> &mut ReceiptLane {
    r.lanes.first_mut().unwrap()
}

fn proof0(r: &mut PullReceipt) -> &mut ReceiptProof {
    lane0(r).proof.as_mut().unwrap()
}

fn response0(r: &mut PullReceipt) -> &mut ReceiptResponse {
    lane0(r).responses.first_mut().unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one case per signed field, each a few lines
async fn tampering_with_each_signed_field_is_named() {
    let keys = Keys::new();
    let good = chained_receipt(&keys, Cumulative::default()).await;
    let hash = good.content.hash;
    let provider = keys.node.address();

    let mut r = good.clone();
    response0(&mut r).rate_per_mb += 1;
    assert_eq!(
        r.verify(),
        vec![ReceiptFault::ResponseSignature { provider, hash }]
    );

    let mut r = good.clone();
    response0(&mut r).total_bytes += 1;
    assert_eq!(
        r.verify(),
        vec![
            ReceiptFault::ResponseSignature { provider, hash },
            ReceiptFault::EntrySize {
                hash,
                stated: 3 * CHUNK_BYTES,
                signed: 3 * CHUNK_BYTES + 1,
            },
        ]
    );

    let mut r = good.clone();
    r.content.entries.first_mut().unwrap().size += 1;
    assert!(matches!(
        r.verify().as_slice(),
        [ReceiptFault::EntrySize { .. }]
    ));

    let mut r = good.clone();
    lane0(&mut r).provider = Address::repeat_byte(0x55);
    let other = Address::repeat_byte(0x55);
    assert_eq!(
        r.verify(),
        vec![
            ReceiptFault::ResponseSignature {
                provider: other,
                hash
            },
            ReceiptFault::VoucherSignature { provider: other },
        ]
    );

    let mut r = good.clone();
    lane0(&mut r).signer = Address::repeat_byte(0x56);
    assert_eq!(
        r.verify(),
        vec![ReceiptFault::VoucherSignature { provider }]
    );

    let mut r = good.clone();
    lane0(&mut r).pool_id = B256::repeat_byte(0x57);
    assert_eq!(
        r.verify(),
        vec![
            ReceiptFault::ResponsePool { provider, hash },
            ReceiptFault::VoucherSignature { provider },
        ]
    );

    let mut r = good.clone();
    proof0(&mut r).voucher.amount += 1;
    assert!(matches!(
        r.verify().as_slice(),
        [
            ReceiptFault::VoucherSignature { .. },
            ReceiptFault::Claim { .. }
        ]
    ));

    let mut r = good.clone();
    proof0(&mut r).reveal.as_mut().unwrap().index += 1;
    assert!(matches!(
        r.verify().as_slice(),
        [ReceiptFault::Reveal { .. }, ReceiptFault::Claim { .. }]
    ));

    let mut r = good.clone();
    proof0(&mut r).claim.amount += 1;
    assert!(matches!(
        r.verify().as_slice(),
        [ReceiptFault::Claim { .. }]
    ));

    let mut r = good.clone();
    lane0(&mut r).prior.amount = u64::MAX;
    assert_eq!(r.verify(), vec![ReceiptFault::ClaimBelowPrior { provider }]);

    let mut r = good.clone();
    lane0(&mut r).prior.bytes = u64::MAX;
    assert_eq!(r.verify(), vec![ReceiptFault::ClaimBelowPrior { provider }]);

    let mut r = good.clone();
    r.chain.chain_id += 1;
    assert_eq!(
        r.verify(),
        vec![
            ReceiptFault::ResponseSignature { provider, hash },
            ReceiptFault::VoucherSignature { provider },
        ]
    );

    let mut r = good.clone();
    r.content.entries.clear();
    r.content.hash = B256::repeat_byte(0x66);
    assert_eq!(
        r.verify(),
        vec![ReceiptFault::ResponseHash { provider, hash }]
    );

    let mut r = good;
    proof0(&mut r).voucher.chain_root = B256::ZERO;
    assert!(matches!(
        r.verify().as_slice(),
        [
            ReceiptFault::VoucherSignature { .. },
            ReceiptFault::Reveal { .. }
        ]
    ));
}

#[tokio::test]
async fn malformed_signatures_say_so() {
    let keys = Keys::new();
    let good = chained_receipt(&keys, Cumulative::default()).await;

    let mut r = good.clone();
    response0(&mut r).slash_sig = Bytes::from_static(&[1, 2, 3]);
    assert!(matches!(
        r.verify().as_slice(),
        [ReceiptFault::ResponseMalformed { .. }]
    ));

    let mut r = good.clone();
    response0(&mut r).rate_per_mb = 0;
    assert!(matches!(
        r.verify().as_slice(),
        [ReceiptFault::ResponseMalformed { .. }]
    ));

    let mut r = good;
    proof0(&mut r).voucher.signature = Bytes::from_static(&[1, 2, 3]);
    assert!(matches!(
        r.verify().as_slice(),
        [ReceiptFault::VoucherMalformed { .. }]
    ));
}

#[tokio::test]
async fn only_the_tampered_lane_is_named() {
    let (keys_a, keys_b) = (Keys::new(), Keys::new());
    let a = chained_receipt(&keys_a, Cumulative::default()).await;
    let b = chained_receipt(&keys_b, Cumulative::default()).await;
    let mut r = a.clone();
    r.lanes.extend(b.lanes);
    assert_eq!(r.verify(), Vec::new());
    r.lanes
        .last_mut()
        .unwrap()
        .responses
        .first_mut()
        .unwrap()
        .rate_per_mb += 1;
    assert_eq!(
        r.verify(),
        vec![ReceiptFault::ResponseSignature {
            provider: keys_b.node.address(),
            hash: a.content.hash,
        }]
    );
}

#[tokio::test]
async fn an_incomplete_receipt_lists_what_did_not_land_and_verifies() {
    let keys = Keys::new();
    let (collector, ledger) = collected(&keys, Cumulative::default());
    let (manifest, landed, started) = (
        B256::repeat_byte(0x01),
        B256::repeat_byte(0x02),
        B256::repeat_byte(0x03),
    );
    let evidence = ledger.attach_evidence();
    for (hash, size) in [
        (manifest, 10),
        (landed, CHUNK_BYTES),
        (started, CHUNK_BYTES),
    ] {
        evidence.note_response(&keys.response(hash, size));
    }
    issue(&ledger, &keys, CHUNK_BYTES, EpochAction::Keep).await;
    collector.note_landed(manifest.into(), 10);
    collector.note_landed(landed.into(), CHUNK_BYTES);
    let receipt = collector
        .receipt(manifest.into(), ContentKind::Bundle, None, false, window())
        .await
        .unwrap();
    assert_eq!(receipt.content.unlanded, vec![started]);
    assert_eq!(receipt.verify(), Vec::new());

    // A receipt marked complete may not list an unlanded hash.
    let mut complete = receipt;
    complete.content.complete = true;
    assert_eq!(
        complete.verify(),
        vec![ReceiptFault::Unlanded { hash: started }]
    );
}

#[tokio::test]
async fn the_collector_builds_the_whole_receipt() {
    let keys = Keys::new();
    let collector = ReceiptEvidence::new();
    assert!(
        collector
            .receipt([0; 32], ContentKind::Blob, None, true, window())
            .await
            .is_none(),
        "no lane, no deployment, no receipt"
    );
    let hash = B256::repeat_byte(0x44);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    collector.attach(&chain_config(), keys.key(), &ledger);
    ledger
        .attach_evidence()
        .note_response(&keys.response(hash, CHUNK_BYTES));
    issue(&ledger, &keys, CHUNK_BYTES, EpochAction::Keep).await;
    collector.note_landed(hash.into(), CHUNK_BYTES);
    collector.note_landed(hash.into(), CHUNK_BYTES);
    let receipt = collector
        .receipt(hash.into(), ContentKind::Blob, Some(3), true, window())
        .await
        .unwrap();
    assert_eq!(receipt.chain, chain());
    assert_eq!(
        receipt.content.entries,
        vec![ReceiptEntry {
            hash,
            size: CHUNK_BYTES,
        }]
    );
    assert!(receipt.content.unlanded.is_empty());
    assert_eq!(receipt.verify(), Vec::<ReceiptFault>::new());
    assert_eq!(receipt.summary().paid.bytes, CHUNK_BYTES);
}

#[test]
fn a_lane_is_settled_once_a_redemption_reaches_its_claim() {
    let keys = Keys::new();
    let mut lane = ReceiptLane {
        pool_id: keys.pool_id,
        signer: keys.payer.address(),
        provider: keys.node.address(),
        responses: Vec::new(),
        proof: Some(ReceiptProof {
            voucher: ReceiptVoucher::from(&keys.voucher(cumulative(9, 9), ChainCommit::SEALED)),
            reveal: None,
            claim: ReceiptAmount {
                amount: 9,
                bytes: 9,
            },
        }),
        prior: ReceiptAmount::default(),
        settlement: None,
    };
    assert!(!lane.is_settled());
    let at = |paid| ReceiptSettlement {
        tx_hash: B256::ZERO,
        block: 1,
        new_paid_cumulative: paid,
    };
    lane.settlement = Some(at(8));
    assert!(!lane.is_settled());
    lane.settlement = Some(at(9));
    assert!(lane.is_settled());
    lane.proof = None;
    assert!(!lane.is_settled(), "no claim, nothing to settle");
}

#[test]
fn a_reveal_verifies_against_the_chain_root() {
    let seed = B256::repeat_byte(0x77);
    let root = root_from_seed(seed);
    let voucher = ReceiptVoucher {
        amount: 10,
        bytes_delivered: 0,
        chain_root: root,
        chunk_price: RATE,
        signature: Bytes::new(),
    };
    let index = 4;
    assert!(verify_forward(
        preimage_at(seed, index),
        index,
        voucher.chain_root
    ));
    assert_eq!(
        voucher.claim(index),
        Some(ReceiptAmount {
            amount: 10 + 4 * RATE,
            bytes: 4 * CHUNK_BYTES,
        })
    );
}

#[test]
fn another_format_does_not_parse() {
    let receipt = PullReceipt::new(chain(), content(B256::ZERO, 0), window(), Vec::new());
    let mut json = serde_json::to_value(&receipt).unwrap();
    *json.get_mut("format").unwrap() = serde_json::json!("decdn-receipt/2");
    let err = serde_json::from_value::<PullReceipt>(json).unwrap_err();
    assert!(err.to_string().contains("decdn-receipt/2"), "{err}");
}

/// The JSON shape is the contract a published receipt is read by: pin it.
#[test]
#[allow(clippy::too_many_lines)] // one literal receipt, field by field
fn the_json_schema_is_pinned() {
    let lane = ReceiptLane {
        pool_id: B256::repeat_byte(0x01),
        signer: Address::repeat_byte(0x02),
        provider: Address::repeat_byte(0x03),
        responses: vec![ReceiptResponse {
            hash: B256::repeat_byte(0x04),
            ok: true,
            rate_per_mb: 10,
            total_bytes: 20,
            pool_id: B256::repeat_byte(0x01),
            timestamp_us: 30,
            slash_sig: Bytes::from_static(&[0xaa]),
        }],
        proof: Some(ReceiptProof {
            voucher: ReceiptVoucher {
                amount: 40,
                bytes_delivered: 50,
                chain_root: B256::repeat_byte(0x05),
                chunk_price: 10,
                signature: Bytes::from_static(&[0xbb]),
            },
            reveal: Some(ReceiptReveal {
                preimage: B256::repeat_byte(0x06),
                index: 2,
            }),
            claim: ReceiptAmount {
                amount: 60,
                bytes: 70,
            },
        }),
        prior: ReceiptAmount {
            amount: 1,
            bytes: 2,
        },
        settlement: Some(ReceiptSettlement {
            tx_hash: B256::repeat_byte(0x07),
            block: 80,
            new_paid_cumulative: 60,
        }),
    };
    let receipt = PullReceipt::new(
        chain(),
        ReceiptContent {
            kind: ContentKind::Bundle,
            unlanded: vec![B256::repeat_byte(0x08)],
            complete: false,
            ..content(B256::repeat_byte(0x04), 20)
        },
        window(),
        vec![lane],
    );
    let json = serde_json::to_value(&receipt).unwrap();
    let b = |byte: &str| format!("0x{}", byte.repeat(32));
    let a = |byte: &str| format!("0x{}", byte.repeat(20));
    let expected = serde_json::json!({
        "format": "decdn-receipt/1",
        "chain": {
            "chain_id": 421_614,
            "payment_pool": a("11"),
            "slash_judge": a("22"),
        },
        "content": {
            "hash": b("04"),
            "kind": "bundle",
            "namespace": 7,
            "entries": [{ "hash": b("04"), "size": 20 }],
            "unlanded": [b("08")],
            "complete": false,
        },
        "window": {
            "started": "2026-10-24T14:02:11Z",
            "finished": "2026-10-24T14:09:48Z",
        },
        "lanes": [{
            "pool_id": b("01"),
            "signer": a("02"),
            "provider": a("03"),
            "responses": [{
                "hash": b("04"),
                "ok": true,
                "rate_per_mb": 10,
                "total_bytes": 20,
                "pool_id": b("01"),
                "timestamp_us": 30,
                "slash_sig": "0xaa",
            }],
            "proof": {
                "voucher": {
                    "amount": 40,
                    "bytes_delivered": 50,
                    "chain_root": b("05"),
                    "chunk_price": 10,
                    "signature": "0xbb",
                },
                "reveal": { "preimage": b("06"), "index": 2 },
                "claim": { "amount": 60, "bytes": 70 },
            },
            "prior": { "amount": 1, "bytes": 2 },
            "settlement": {
                "tx_hash": b("07"),
                "block": 80,
                "new_paid_cumulative": 60,
            },
        }],
    });
    assert_eq!(json, expected);
    let back: PullReceipt = serde_json::from_value(json).unwrap();
    assert_eq!(back, receipt);
}
