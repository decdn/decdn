use alloy::primitives::{Address, B256, Bytes, LogData};
use alloy::rpc::types::Log;
use decdn_client::receipt::{
    ContentKind, PullReceipt, ReceiptAmount, ReceiptChain, ReceiptContent, ReceiptEntry,
    ReceiptLane, ReceiptProof, ReceiptSettlement, ReceiptVoucher, ReceiptWindow,
};
use decdn_incentive::payment_pool::PaymentPool;

use super::*;

const PROVIDER_A: Address = Address::repeat_byte(0x2a);
const PROVIDER_B: Address = Address::repeat_byte(0x9c);
const SIGNER: Address = Address::repeat_byte(0x5e);
const POOL: B256 = B256::repeat_byte(0x7d);
const MB: u64 = 1_000_000;

fn proof(claim: u64, bytes: u64) -> ReceiptProof {
    ReceiptProof {
        voucher: ReceiptVoucher {
            amount: claim,
            bytes_delivered: bytes,
            chain_root: B256::ZERO,
            chunk_price: 0,
            signature: Bytes::new(),
        },
        reveal: None,
        claim: ReceiptAmount {
            amount: claim,
            bytes,
        },
    }
}

fn lane(provider: Address, prior: u64, claim: u64, bytes: u64) -> ReceiptLane {
    ReceiptLane {
        pool_id: POOL,
        signer: SIGNER,
        provider,
        responses: Vec::new(),
        proof: Some(proof(claim, bytes)),
        prior: ReceiptAmount {
            amount: prior,
            bytes: 0,
        },
        settlement: None,
    }
}

fn bundle_receipt(chain_id: u64) -> PullReceipt {
    let hash = B256::repeat_byte(0x91);
    PullReceipt::new(
        ReceiptChain {
            chain_id,
            payment_pool: Address::repeat_byte(0x11),
            slash_judge: Address::repeat_byte(0x22),
        },
        ReceiptContent {
            hash,
            kind: ContentKind::Bundle,
            namespace: Some(1),
            entries: vec![
                ReceiptEntry { hash, size: 2_000 },
                ReceiptEntry {
                    hash: B256::repeat_byte(0x01),
                    size: 11_200 * MB,
                },
                ReceiptEntry {
                    hash: B256::repeat_byte(0x02),
                    size: 9_800 * MB,
                },
            ],
            unlanded: vec![B256::repeat_byte(0x03)],
            complete: false,
        },
        ReceiptWindow {
            started: "2026-10-24T14:02:11Z".to_owned(),
            finished: "2026-10-24T14:09:48Z".to_owned(),
        },
        vec![
            lane(PROVIDER_B, 0, 98_000, 9_800 * MB),
            lane(PROVIDER_A, 1_000, 113_000, 11_200 * MB),
        ],
    )
}

/// A receipt with no lanes: nothing in it to verify, so it passes.
fn empty_receipt(chain_id: u64) -> PullReceipt {
    let mut receipt = bundle_receipt(chain_id);
    receipt.lanes.clear();
    receipt.content.unlanded.clear();
    receipt
}

fn rendered(receipt: &PullReceipt, file: ReceiptFile<'_>) -> String {
    let mut out = Vec::new();
    write_receipt(&mut out, receipt, file).unwrap();
    String::from_utf8(out).unwrap()
}

#[test]
fn grouped_separates_thousands() {
    assert_eq!(grouped(0), "0");
    assert_eq!(grouped(999), "999");
    assert_eq!(grouped(1_000), "1,000");
    assert_eq!(grouped(28_995_469_846), "28,995,469,846");
}

#[test]
fn hashes_and_addresses_print_cut() {
    assert_eq!(short_hex(PROVIDER_A.as_slice()), "0x2a2a…2a2a");
    assert_eq!(short_hash(B256::repeat_byte(0x91)), "91919191…9191");
    assert_eq!(short_hex(&[0xab]), "0xab");
}

#[test]
fn the_block_names_what_landed_and_what_it_paid() {
    let text = rendered(
        &bundle_receipt(31_337),
        ReceiptFile::Saved(Path::new("r.json")),
    );
    let expected = "\
decdn receipt v1
hash        91919191…9191  (bundle, 21.0 GB, 2 entries)
status      incomplete: the pull ended early; this is what it paid for (1 started blob(s) did not land)
namespace   1
bytes       21,000,002,000 verified · 21,000,000,000 paid
nodes       2   0x2a2a…2a2a 11.2 GB · 0x9c9c…9c9c 9.8 GB
paid        0.21 USDC from pool 0x7d7d…7d7d
started     2026-10-24T14:02:11Z   finished 2026-10-24T14:09:48Z
evidence    0 node + 2 payer signatures · decdn receipt verify r.json
";
    assert_eq!(text, expected);
}

#[test]
fn the_block_without_a_file_names_the_flag_and_counts_unproved_lanes() {
    let mut receipt = bundle_receipt(31_337);
    receipt.content.complete = true;
    receipt.content.unlanded.clear();
    receipt.content.namespace = None;
    if let Some(lane) = receipt.lanes.first_mut() {
        lane.proof = None;
    }
    let text = rendered(&receipt, ReceiptFile::Unrequested);
    assert!(!text.contains("status"));
    assert!(!text.contains("namespace"));
    assert!(text.ends_with(
        "evidence    0 node + 1 payer signatures · 1 lane without a voucher \
         (--receipt <FILE> keeps them)\n"
    ));
}

#[test]
fn the_block_says_when_the_file_was_not_written() {
    let text = rendered(
        &bundle_receipt(31_337),
        ReceiptFile::Failed(Path::new("/nope/r.json")),
    );
    assert!(
        text.ends_with(" · receipt NOT written to /nope/r.json\n"),
        "{text}"
    );
}

#[test]
fn the_block_of_a_blob_with_two_pools_and_a_settled_lane() {
    let mut receipt = bundle_receipt(31_337);
    receipt.content.kind = ContentKind::Blob;
    receipt.content.complete = true;
    receipt.content.unlanded.clear();
    receipt.content.entries = vec![ReceiptEntry {
        hash: receipt.content.hash,
        size: 5 * MB,
    }];
    let mut second = lane(PROVIDER_A, 0, 1_000, MB);
    second.pool_id = B256::repeat_byte(0x7e);
    second.settlement = Some(ReceiptSettlement {
        tx_hash: B256::ZERO,
        block: 1,
        new_paid_cumulative: 1_000,
    });
    receipt.lanes.push(second);
    let text = rendered(&receipt, ReceiptFile::Unrequested);
    assert!(text.contains("(blob, 5.0 MB)"), "{text}");
    assert!(text.contains("from 2 pools"), "{text}");
    assert!(text.contains("settled     1 of 3 lanes"), "{text}");
    // Both of provider A's lanes add up on one line.
    assert!(
        text.contains("nodes       2   0x2a2a…2a2a 11.2 GB"),
        "{text}"
    );
}

#[test]
fn the_block_of_a_blob_that_did_not_land() {
    let mut receipt = bundle_receipt(31_337);
    receipt.content.kind = ContentKind::Blob;
    receipt.content.entries.clear();
    receipt.content.unlanded.clear();
    let text = rendered(&receipt, ReceiptFile::Unrequested);
    assert!(text.contains("(blob, no whole blob landed)"), "{text}");
    assert!(
        text.contains("bytes       0 verified · 21,000,000,000 paid"),
        "{text}"
    );
}

#[test]
fn a_saved_receipt_reads_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.json");
    let receipt = bundle_receipt(31_337);
    save(&path, &receipt).unwrap();
    assert_eq!(load(&path).unwrap(), receipt);
    // A second save replaces the file.
    let mut changed = receipt;
    changed.content.complete = true;
    save(&path, &changed).unwrap();
    assert_eq!(load(&path).unwrap(), changed);
}

#[test]
fn the_receipt_path_is_checked_before_the_pull() {
    let dir = tempfile::tempdir().unwrap();
    check_receipt_path(&dir.path().join("r.json")).unwrap();
    assert!(check_receipt_path(dir.path()).is_err(), "a directory");
    assert!(
        check_receipt_path(&dir.path().join("missing").join("r.json")).is_err(),
        "a directory that does not exist"
    );
    assert!(
        !dir.path().join("r.json").exists(),
        "the check leaves nothing behind"
    );
}

#[test]
fn a_run_that_paid_nothing_writes_no_receipt_and_removes_a_stale_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.json");
    std::fs::write(&path, "an earlier run's receipt").unwrap();
    let mut out = Vec::new();
    finish_with(None, Some(&path), Ok(()), &mut out).unwrap();
    assert!(out.is_empty(), "no block");
    assert!(!path.exists(), "the stale receipt is removed");
    // The pull's own error passes through unchanged, and a failed run leaves
    // the file alone.
    std::fs::write(&path, "an earlier run's receipt").unwrap();
    let err = finish_with(None, Some(&path), Err(anyhow::anyhow!("boom")), &mut out).unwrap_err();
    assert_eq!(err.to_string(), "boom");
    assert!(path.exists(), "a failed run removes nothing");
}

#[test]
fn a_receipt_may_not_name_the_output() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("blob.bin");
    assert!(refuse_collision(&blob, &blob, "the blob lands").is_err());
    std::fs::write(&blob, b"x").unwrap();
    let dotted = dir.path().join(".").join("blob.bin");
    let err = refuse_collision(&dotted, &blob, "the blob lands").unwrap_err();
    assert!(err.to_string().contains("is where the blob lands"), "{err}");
    refuse_collision(&dir.path().join("r.json"), &blob, "the blob lands").unwrap();
}

#[test]
fn a_failed_pull_still_writes_its_receipt_and_keeps_its_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.json");
    let receipt = bundle_receipt(31_337);
    let mut out = Vec::new();
    let err = finish_with(
        Some(&receipt),
        Some(&path),
        Err(anyhow::anyhow!("pull failed")),
        &mut out,
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "pull failed");
    assert_eq!(load(&path).unwrap(), receipt);
    assert!(
        String::from_utf8(out)
            .unwrap()
            .contains("decdn receipt verify")
    );
}

#[test]
fn a_receipt_that_cannot_be_written_fails_a_good_pull_and_not_a_failed_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing").join("r.json");
    let receipt = bundle_receipt(31_337);
    let mut out = Vec::new();
    let err = finish_with(Some(&receipt), Some(&path), Ok(()), &mut out).unwrap_err();
    assert!(format!("{err:#}").contains("temp file"), "{err:#}");
    assert!(
        String::from_utf8(out)
            .unwrap()
            .contains("receipt NOT written")
    );
    let err = finish_with(
        Some(&receipt),
        Some(&path),
        Err(anyhow::anyhow!("pull failed")),
        &mut Vec::new(),
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "pull failed");
}

#[test]
fn verify_fails_on_a_tampered_file_and_passes_an_empty_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.json");
    save(&path, &bundle_receipt(31_337)).unwrap();
    let args = ReceiptVerifyArgs {
        file: path.clone(),
        json: true,
    };
    assert!(
        verify_file(&args).is_err(),
        "unsigned vouchers do not verify"
    );
    save(&path, &empty_receipt(31_337)).unwrap();
    verify_file(&args).unwrap();
}

#[test]
fn a_known_chain_pins_the_contract_addresses() {
    let chain = known_chains::by_chain_id(421_614).unwrap();
    let addresses = chain.addresses().unwrap();
    let mut receipt = bundle_receipt(421_614);
    let ChainCheck::Known(faults) = check_chain(&receipt.chain).unwrap() else {
        panic!("421614 is a known chain");
    };
    assert_eq!(faults.len(), 2);
    receipt.chain.payment_pool = addresses.payment_pool.parse().unwrap();
    receipt.chain.slash_judge = addresses.slash_judge.parse().unwrap();
    assert_eq!(
        check_chain(&receipt.chain).unwrap(),
        ChainCheck::Known(Vec::new())
    );
    assert_eq!(
        check_chain(&bundle_receipt(31_337).chain).unwrap(),
        ChainCheck::Unknown
    );
}

#[tokio::test]
async fn settle_refuses_a_receipt_that_does_not_verify() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.json");
    save(&path, &bundle_receipt(31_337)).unwrap();
    let before = std::fs::read(&path).unwrap();
    let args = ReceiptSettleArgs {
        file: path.clone(),
        // Never dialled: the check runs first.
        rpc_url: Some("http://127.0.0.1:1".to_owned()),
        from_block: Some(0),
        json: false,
    };
    let err = settle_file(&args, Some(&dir.path().join("none.toml")))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("does not verify"), "{err:#}");
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "the file is untouched"
    );
}

fn redemption_in(
    pool: B256,
    block: u64,
    provider: Address,
    signer: Address,
    paid: u64,
) -> Redemption {
    Redemption {
        tx_hash: B256::from(U256::from(block)),
        block,
        event: PaymentPool::PoolRedeemed {
            poolId: pool,
            provider,
            lanes: vec![PaymentPool::LaneSettled {
                signer,
                newPaidCumulative: paid,
                bytesPaid: 0,
            }],
        },
    }
}

fn redemption(block: u64, provider: Address, signer: Address, paid: u64) -> Redemption {
    redemption_in(POOL, block, provider, signer, paid)
}

fn settlement_of(receipt: &PullReceipt, provider: Address) -> Option<ReceiptSettlement> {
    receipt
        .lanes
        .iter()
        .find(|l| l.provider == provider)
        .and_then(|l| l.settlement)
}

#[test]
fn the_first_covering_redemption_settles_a_lane() {
    let mut receipt = bundle_receipt(31_337);
    apply_redemptions(
        &mut receipt,
        &[
            // Below the prior watermark: an earlier pull's redemption.
            redemption(10, PROVIDER_A, SIGNER, 900),
            // Another signer's lane, another pool's, and another node's.
            redemption(11, PROVIDER_A, Address::repeat_byte(1), 200_000),
            redemption_in(B256::repeat_byte(0x01), 11, PROVIDER_A, SIGNER, 200_000),
            redemption(12, PROVIDER_B, SIGNER, 50_000),
            redemption(13, PROVIDER_A, SIGNER, 60_000),
            redemption(14, PROVIDER_A, SIGNER, 113_000),
            redemption(15, PROVIDER_A, SIGNER, 150_000),
        ],
    );
    assert_eq!(
        settlement_of(&receipt, PROVIDER_A),
        Some(ReceiptSettlement {
            tx_hash: B256::from(U256::from(14)),
            block: 14,
            new_paid_cumulative: 113_000,
        })
    );
    assert_eq!(
        settlement_of(&receipt, PROVIDER_B).map(|s| s.block),
        Some(12)
    );
    assert_eq!(receipt.summary().settled_lanes, 1);

    // A rescan rebuilds every settlement from the events it read: one it no
    // longer sees (a reorganization, or a hand-written one) is gone.
    apply_redemptions(&mut receipt, &[redemption(20, PROVIDER_B, SIGNER, 98_000)]);
    assert_eq!(receipt.summary().settled_lanes, 1);
    assert_eq!(settlement_of(&receipt, PROVIDER_A), None);
    assert_eq!(
        settlement_of(&receipt, PROVIDER_B).map(|s| s.block),
        Some(20)
    );
}

#[test]
fn a_settlement_the_scan_does_not_find_is_dropped() {
    let mut receipt = bundle_receipt(31_337);
    if let Some(lane) = receipt.lanes.first_mut() {
        lane.settlement = Some(ReceiptSettlement {
            tx_hash: B256::repeat_byte(0xee),
            block: 99,
            new_paid_cumulative: u64::MAX,
        });
    }
    assert_eq!(receipt.summary().settled_lanes, 1);
    apply_redemptions(&mut receipt, &[]);
    assert_eq!(receipt.summary().settled_lanes, 0);
    assert!(receipt.lanes.iter().all(|l| l.settlement.is_none()));
}

#[test]
fn a_partial_lane_keeps_the_highest_redemption_and_a_bare_lane_none() {
    let mut receipt = bundle_receipt(31_337);
    if let Some(lane) = receipt.lanes.first_mut() {
        lane.proof = None;
    }
    apply_redemptions(
        &mut receipt,
        &[
            redemption(12, PROVIDER_A, SIGNER, 60_000),
            redemption(13, PROVIDER_A, SIGNER, 50_000),
            redemption(14, PROVIDER_B, SIGNER, 98_000),
        ],
    );
    assert_eq!(
        settlement_of(&receipt, PROVIDER_A).map(|s| s.block),
        Some(12)
    );
    assert_eq!(
        settlement_of(&receipt, PROVIDER_B),
        None,
        "no proof, no settlement"
    );
}

#[test]
fn a_redemption_log_without_a_position_or_with_bad_data_fails() {
    let log = |tx: Option<B256>, data: LogData| Log {
        inner: alloy::primitives::Log {
            address: Address::ZERO,
            data,
        },
        block_hash: None,
        block_number: Some(1),
        block_timestamp: None,
        transaction_hash: tx,
        transaction_index: None,
        log_index: Some(0),
        removed: false,
    };
    let bad = LogData::new_unchecked(
        vec![PaymentPool::PoolRedeemed::SIGNATURE_HASH],
        Bytes::new(),
    );
    assert!(decode_redemptions(&[log(None, bad.clone())]).is_err());
    assert!(decode_redemptions(&[log(Some(B256::ZERO), bad)]).is_err());
}

#[test]
fn settlement_lines_name_each_state() {
    let mut lane = lane(PROVIDER_A, 0, 100_000, MB);
    assert!(settlement_line(&lane).ends_with("not redeemed yet"));
    lane.settlement = Some(ReceiptSettlement {
        tx_hash: B256::repeat_byte(0xab),
        block: 7,
        new_paid_cumulative: 50_000,
    });
    assert_eq!(
        settlement_line(&lane),
        "0x2a2a…2a2a  partly redeemed: 0.050 of 0.10 USDC claimed by block 7 (tx 0xabab…abab)"
    );
    lane.proof = None;
    assert!(settlement_line(&lane).ends_with("no voucher, so no claim to settle"));
}
