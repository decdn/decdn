use super::*;
use std::assert_matches;

#[test]
fn resolve_expiry_absolute_and_relative_agree() {
    let now = 1_000_000u64;
    // Relative: now + secs.
    assert_eq!(resolve_expiry(now, Some(3_600), None).unwrap(), now + 3_600);
    // Absolute: used verbatim when in the future.
    assert_eq!(resolve_expiry(now, None, Some(now + 10)).unwrap(), now + 10);
}

#[test]
fn resolve_expiry_rejects_missing_and_past() {
    let now = 1_000_000u64;
    // Neither flag: clap normally guards this, but the handler still refuses.
    let missing = resolve_expiry(now, None, None).unwrap_err();
    assert!(missing.to_string().contains("exactly one"), "{missing}");
    // An absolute expiry at/behind now is dead on arrival.
    let past = resolve_expiry(now, None, Some(now)).unwrap_err();
    assert!(past.to_string().contains("not in the future"), "{past}");
}

/// `assign` warns on a lifetime at or under the default node margin, and not
/// on a longer one.
#[test]
fn expires_within_default_node_margin_matches_the_node_margin() {
    let now = 1_000_000u64;
    let margin = DEFAULT_REDEEM_INTERVAL_SECS + decdn_common::config::REDEEM_LANDING_SLACK_SECS;
    assert_eq!(DEFAULT_NODE_EXPIRY_MARGIN_SECS, margin);
    assert!(expires_within_default_node_margin(now + 1, now));
    assert!(expires_within_default_node_margin(now + margin, now));
    assert!(!expires_within_default_node_margin(now + margin + 1, now));
    assert!(
        expires_within_default_node_margin(now - 1, now),
        "a past expiry saturates to 0 s left"
    );
}

/// An RPC that cannot be read leaves `assign` on the local clock rather than
/// failing: offline issuance stays valid.
#[tokio::test]
async fn issuance_now_falls_back_to_the_local_clock() {
    let before = unix_now().unwrap();
    let now = issuance_now("not a url", HEAD_READ_TIMEOUT).await.unwrap();
    let after = unix_now().unwrap();
    assert!(
        (before..=after).contains(&now),
        "{before} <= {now} <= {after}"
    );
}

/// An RPC endpoint that accepts the connection and never answers leaves
/// `assign` on the local clock once the head read times out.
#[tokio::test]
async fn issuance_now_falls_back_when_the_rpc_never_answers() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let silent = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((conn, _)) = listener.accept().await {
            held.push(conn);
        }
    });
    let before = unix_now().unwrap();
    let now = tokio::time::timeout(
        Duration::from_secs(10),
        issuance_now(&format!("http://{addr}"), Duration::from_millis(200)),
    )
    .await
    .expect("the head read must time out, not hang")
    .unwrap();
    let after = unix_now().unwrap();
    silent.abort();
    assert!(
        (before..=after).contains(&now),
        "{before} <= {now} <= {after}"
    );
}

#[test]
fn format_expiry_breaks_out_days_hours_minutes() {
    let now = 1_000_000u64;
    // 1 day + 2 hours + 3 minutes ahead.
    let expiry = now + 86_400 + 2 * 3_600 + 3 * 60;
    let rendered = format_expiry(expiry, now);
    assert!(rendered.contains(&format!("Unix {expiry}")), "{rendered}");
    assert!(rendered.contains("in 1d 2h 3m"), "{rendered}");
}

fn args() -> cli::PoolChainArgs {
    cli::PoolChainArgs {
        rpc_url: None,
        payment_pool_address: None,
        chain_id: None,
        keystore: None,
        keystore_password_file: None,
        data_dir: Some(PathBuf::from("/tmp/d")),
    }
}

fn config(body: &str) -> FileConfig {
    toml::from_str(body).unwrap()
}

#[test]
fn flags_override_config() {
    let mut a = args();
    a.rpc_url = Some("http://flag:8545".into());
    a.chain_id = Some(99);
    let pp = "0x1111111111111111111111111111111111111111";
    a.payment_pool_address = Some(pp.into());
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\n\
         payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n",
    );
    let r = resolve_chain(&a, &file).unwrap();
    assert_eq!(r.rpc_url, "http://flag:8545");
    assert_eq!(r.chain_id, 99);
    assert_eq!(r.payment_pool, Address::from_str(pp).unwrap());
}

#[test]
fn config_fills_unset_flags_and_chain_id_defaults() {
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\n\
         payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n",
    );
    let r = resolve_chain(&args(), &file).unwrap();
    assert_eq!(r.rpc_url, "http://config:8545");
    assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
    assert_eq!(
        r.keystore,
        eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
    );
}

/// The password file is CLI/env-only — absent unless the operator passes
/// the flag, and carried through verbatim when they do. An absolute path
/// keeps the assertion off the ambient `$HOME` that `expand_tilde` reads.
#[test]
fn keystore_password_file_flows_through_and_defaults_to_none() {
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\n\
         payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n",
    );
    assert!(
        resolve_chain(&args(), &file)
            .unwrap()
            .keystore_password_file
            .is_none()
    );

    let mut a = args();
    a.keystore_password_file = Some(PathBuf::from("/abs/pw.txt"));
    assert_eq!(
        resolve_chain(&a, &file).unwrap().keystore_password_file,
        Some(PathBuf::from("/abs/pw.txt"))
    );
}

#[test]
fn explicit_data_dir_not_client_scoped() {
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\n\
         payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n",
    );
    let r = resolve_chain(&args(), &file).unwrap();
    assert_eq!(r.data_dir, PathBuf::from("/tmp/d"));
    assert_eq!(
        r.keystore,
        eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
    );
}

#[test]
fn missing_payment_pool_errors() {
    let file = config("[blockchain]\nrpc_url = \"http://config:8545\"\n");
    let err = resolve_chain(&args(), &file).unwrap_err();
    assert!(
        err.to_string().contains("payment_pool_address not set"),
        "{err}"
    );
}

/// A present-but-zero `payment_pool_address` fails fast via the shared
/// `parse_nonzero_address` guard rather than as an opaque on-chain revert.
#[test]
fn rejects_zero_payment_pool() {
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\n\
         payment_pool_address = \"0x0000000000000000000000000000000000000000\"\n",
    );
    let err = resolve_chain(&args(), &file).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("payment_pool_address"), "{err}");
    assert!(msg.contains("must not be the zero address"), "{err}");
}

#[test]
fn parse_pool_id_accepts_hex_and_rejects_garbage() {
    let id = "0x1111111111111111111111111111111111111111111111111111111111111111";
    assert_eq!(parse_pool_id(id).unwrap(), B256::from_str(id).unwrap());
    assert!(parse_pool_id("not-a-hash").is_err());
}

#[test]
fn ensure_owned_accepts_matching_and_rejects_mismatch() {
    let ours = Address::repeat_byte(1);
    let pool_id = B256::repeat_byte(0xab);
    assert!(ensure_owned(ours, ours, pool_id).is_ok());
    let err = ensure_owned(Address::repeat_byte(2), ours, pool_id).unwrap_err();
    assert!(err.to_string().contains("not this keystore's address"));
}

/// The deployment every fixture row lives on.
const DEPLOYMENT: Deployment = Deployment {
    chain_id: 421_614,
    payment_pool: Address::repeat_byte(0x9c),
};

fn mk_state(byte: u8, deposit_micro: u64) -> BuyerPoolState {
    BuyerPoolState::new(
        B256::repeat_byte(byte),
        DEPLOYMENT,
        Address::repeat_byte(byte),
        Address::repeat_byte(0xcd),
        U256::from(deposit_micro),
    )
}

/// A landed close on a node data dir does not claim the row was cleared —
/// it reports that the daemon's row survives. The `Ok` here is the
/// on-chain leg succeeding, which it did.
#[test]
fn daemon_owned_bookkeeping_does_not_claim_a_clean_close() {
    let books = LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb"));
    books
        .forget_after_close(
            Address::repeat_byte(0x11),
            B256::repeat_byte(0x22),
            TxHash::repeat_byte(0x33),
            "run `decdn pool reclaim` later",
        )
        .expect("the on-chain close landed; the local row is reported, not graded");
}

/// `--all` renders every lifecycle state, and `TRACKED` distinguishes
/// "the store does not have this pool" from "the store could not be read"
/// — the second is the case the flag exists for, and reporting it as the
/// first would be a false claim about the pool.
#[test]
fn write_chain_pools_separates_untracked_from_unreadable() {
    const NOW: u64 = 1_000_000;
    let rows = vec![
        ChainPoolRow {
            pool_id: B256::repeat_byte(0x11),
            status: PaymentPool::Status::Open,
            deposit: U256::from(2_500_000u64),
            total_redeemed: U256::from(1_000_000u64),
            dispute_deadline: 0,
            tracked: Some(true),
        },
        ChainPoolRow {
            pool_id: B256::repeat_byte(0x22),
            status: PaymentPool::Status::Closing,
            deposit: U256::from(1_000_000u64),
            total_redeemed: U256::ZERO,
            dispute_deadline: NOW + 3_600,
            tracked: Some(false),
        },
        ChainPoolRow {
            pool_id: B256::repeat_byte(0x33),
            status: PaymentPool::Status::Closed,
            deposit: U256::ZERO,
            total_redeemed: U256::from(1_000_000u64),
            dispute_deadline: NOW - 1,
            tracked: None,
        },
    ];

    let mut buf = Vec::new();
    write_chain_pools(&mut buf, Address::repeat_byte(0xab), 42, &rows, NOW, true).unwrap();
    let out = String::from_utf8(buf).unwrap();

    assert!(out.contains("chain_id=42"), "{out}");
    assert!(out.contains("pools=3"), "{out}");
    assert!(out.contains("open"), "{out}");
    assert!(out.contains("closing"), "{out}");
    assert!(out.contains("closed"), "{out}");
    assert!(out.contains("2.500000"), "{out}");
    assert!(out.contains("close it first"), "{out}");
    assert!(out.contains("already reclaimed"), "{out}");

    let row_for = |byte: u8| {
        let tag = short_hex(&format!("{:#x}", B256::repeat_byte(byte)));
        out.lines()
            .find(|l| l.starts_with(&tag))
            .map(str::to_owned)
            .expect("every row renders")
    };
    let untracked = row_for(0x22);
    assert!(untracked.contains(" no "), "{untracked}");
    let unreadable = row_for(0x33);
    assert!(unreadable.contains(" ? "), "{unreadable}");
}

/// A pool whose local row will not decode is NOT untracked: the store holds
/// a row for it, keyed by a `pool_id` that survives whatever corrupted the
/// value bytes. Reporting `no` would send the operator to `pool close`
/// (recover a stranded deposit) when the remedy is repairing a record. One
/// bad row must also not blank the verdict for the pools either side of it.
#[test]
fn an_undecodable_row_is_unknown_not_untracked() {
    let decoded = B256::repeat_byte(0x11);
    let corrupt = B256::repeat_byte(0x22);
    let absent = B256::repeat_byte(0x33);
    let local = TrackedPools(
        [
            (decoded, RowState::Decoded),
            (corrupt, RowState::Undecodable),
        ]
        .into_iter()
        .collect(),
    );

    assert_eq!(local.verdict(decoded), Some(true));
    assert_eq!(local.verdict(absent), Some(false), "no row means untracked");
    assert_eq!(
        local.verdict(corrupt),
        None,
        "a row that will not decode is unknown, not untracked"
    );
    assert_eq!(tracked_label(local.verdict(corrupt)), "?");
}

/// A `pool_id` spelling this binary cannot parse must make the whole answer
/// unknown. Dropping it silently renders the pool `no`, which tells an
/// operator to close a pool the daemon may be paying from right now.
///
/// The other half of the drift guard is in `decdn-node`, where the wire
/// spelling is produced: `build_buyer_pools_response_ids_parse_back`.
#[test]
fn a_wire_response_round_trips_and_an_unparseable_id_is_not_a_no() {
    let decoded = B256::repeat_byte(0x11);
    let corrupt = B256::repeat_byte(0x22);
    // The `{:#x}` spelling the daemon emits; `decdn-node`'s
    // `build_buyer_pools_response_ids_parse_back` pins that it still does.
    let resp = BuyerPoolsResponse {
        pools: vec![decdn_common::admin::BuyerPoolSnapshot {
            pool_id: format!("{decoded:#x}"),
            chain_id: 421_614,
            payment_pool: "0x00dd".to_string(),
            owner: format!("{:?}", Address::repeat_byte(0x11)),
            token: format!("{:?}", Address::repeat_byte(0xcd)),
            deposit_micro_usdc: 1,
            lanes: Vec::new(),
        }],
        skipped: vec![format!("{corrupt:#x}")],
    };
    let local = TrackedPools::from_wire(&resp).expect("the daemon's own spelling must parse");
    assert_eq!(local.verdict(decoded), Some(true));
    assert_eq!(local.verdict(corrupt), None);
    assert_eq!(local.verdict(B256::repeat_byte(0x33)), Some(false));

    let drifted = BuyerPoolsResponse {
        pools: Vec::new(),
        skipped: vec!["not-a-pool-id".to_owned()],
    };
    assert!(
        TrackedPools::from_wire(&drifted).is_none(),
        "a spelling this binary cannot parse makes the answer unknown, never `no`"
    );
}

/// `BuyerLoad`'s two halves land in the two sets — the conversion is where
/// `skipped` would otherwise be dropped, which is what made an undecodable
/// row read as `no`.
#[test]
fn a_buyer_load_keeps_its_skipped_rows() {
    let state = BuyerPoolState::new(
        B256::repeat_byte(0x11),
        DEPLOYMENT,
        Address::repeat_byte(0x11),
        Address::repeat_byte(0xcd),
        U256::from(1u64),
    );
    let local = TrackedPools::from(BuyerLoad {
        pools: vec![state],
        skipped: vec![B256::repeat_byte(0x22)],
    });
    assert_eq!(local.verdict(B256::repeat_byte(0x11)), Some(true));
    assert_eq!(local.verdict(B256::repeat_byte(0x22)), None);
}

/// An owner with nothing on chain gets a sentinel, not a bare header — the
/// same shape the store listings use, so one reader parses all of them.
#[test]
fn write_chain_pools_empty_emits_sentinel() {
    let mut buf = Vec::new();
    write_chain_pools(&mut buf, Address::repeat_byte(0xab), 1, &[], 0, true).unwrap();
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("pools=0"), "{out}");
    assert!(out.contains("owns no pools"), "{out}");
    assert!(!out.contains("POOL "), "no header without rows: {out}");
}

/// A `Closing` pool past its window reads as reclaimable in both renderings
/// from one `plan_reclaim` call — the table and the JSON must not be able
/// to disagree about it.
#[test]
fn reclaimable_now_agrees_between_table_and_json() {
    const NOW: u64 = 500;
    let row = ChainPoolRow {
        pool_id: B256::repeat_byte(0x44),
        status: PaymentPool::Status::Closing,
        deposit: U256::from(1u64),
        total_redeemed: U256::ZERO,
        dispute_deadline: NOW - 1,
        tracked: Some(true),
    };
    assert_eq!(reclaimable_label(&row, NOW), "now");
    assert!(ChainPoolJson::at(&row, NOW).reclaimable_now);

    let inside = ChainPoolRow {
        dispute_deadline: NOW + 60,
        ..row
    };
    assert!(reclaimable_label(&inside, NOW).starts_with("Unix "));
    assert!(!ChainPoolJson::at(&inside, NOW).reclaimable_now);
}

/// The daemon renderer shares [`write_pools`]'s columns and its empty
/// sentinel, so the two listings read as one view.
#[test]
fn write_buyer_pools_matches_the_client_table_shape() {
    let mut buf = Vec::new();
    write_buyer_pools(
        &mut buf,
        &BuyerPoolsResponse {
            pools: Vec::new(),
            skipped: Vec::new(),
        },
    )
    .unwrap();
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("pools=0"), "{out}");
    assert!(out.contains("(no tracked pools)"), "{out}");

    let mut buf = Vec::new();
    write_buyer_pools(
        &mut buf,
        &BuyerPoolsResponse {
            pools: vec![decdn_common::admin::BuyerPoolSnapshot {
                pool_id: "0xabcdef0123456789".to_string(),
                chain_id: 421_614,
                payment_pool: "0x00dd".to_string(),
                owner: "0x1111111111111111".to_string(),
                token: "0x2222222222222222".to_string(),
                deposit_micro_usdc: 1_500_000,
                lanes: Vec::new(),
            }],
            skipped: vec!["0x4444".to_string()],
        },
    )
    .unwrap();
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("1.500000"), "{out}");
    assert!(out.contains("pools=1"), "{out}");
    assert!(
        !out.contains("0x4444"),
        "undecodable rows belong on stderr, not in the table sink: {out}"
    );

    // …and they are still reported, on the stream the client path uses.
    let mut warnings = Vec::new();
    write_skipped_buyer_pools(&mut warnings, &["0x4444".to_string()]).unwrap();
    let warnings = String::from_utf8(warnings).unwrap();
    assert!(warnings.contains("0x4444"), "{warnings}");
    assert!(warnings.contains("escrowed"), "{warnings}");
}

#[test]
fn write_pools_empty_emits_sentinel() {
    let mut buf = Vec::new();
    write_pools(&mut buf, &[], &[]).unwrap();
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("pools=0"), "{out}");
    assert!(out.contains("(no tracked pools)"), "{out}");
}

#[test]
fn write_pools_renders_deposit_and_lane_count() {
    let state = mk_state(1, 1_500_000);
    let mut buf = Vec::new();
    write_pools(&mut buf, std::slice::from_ref(&state), &[]).unwrap();
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("1.500000"), "{out}");
    assert!(
        out.contains(" 0\n") || out.trim_end().ends_with(" 0"),
        "{out}"
    );
}

#[test]
fn format_usdc_u256_pads_fractional_digits() {
    assert_eq!(format_usdc_u256(U256::from(1_000_000u64)), "1.000000");
    assert_eq!(format_usdc_u256(U256::from(500_000u64)), "0.500000");
}

#[test]
fn short_hex_truncates_long_hashes() {
    let long = "0x1111111111111111111111111111111111111111";
    assert!(short_hex(long).ends_with('…'));
    assert_eq!(short_hex("0x01"), "0x01");
}

// ---- landed-on-chain, not-recorded-locally ----------------------------

fn a_pool() -> PoolId {
    PoolId::from([0x11; 32])
}

fn addr(byte: u8) -> Address {
    Address::from([byte; 20])
}

fn a_tx() -> TxHash {
    TxHash::from([0xab; 32])
}

const RECLAIM_NOTE: &str = "run `decdn pool reclaim --pool 0x11` after Unix 99";

/// A deleted row is the clean close: nothing maps this owner to the pool
/// that is now winding down.
#[test]
fn a_cleared_row_is_a_clean_close() {
    assert!(grade_local_forget(Ok(true), a_pool(), a_tx(), RECLAIM_NOTE).is_ok());
}

/// `forget_if_pool` is compare-and-delete, so `Ok(false)` means it found
/// nothing to delete — no row, or a row for a newer pool. Neither leaves
/// this owner pointing at the closed pool, so the close is clean. Closing a
/// pool this store never tracked (a second machine, a fresh `--data-dir`)
/// lands here, and failing it would both misreport a correct close and
/// invite the operator to delete a live replacement row.
#[test]
fn a_compare_and_delete_that_matched_nothing_is_still_a_clean_close() {
    assert!(grade_local_forget(Ok(false), a_pool(), a_tx(), RECLAIM_NOTE).is_ok());
}

/// Only the backend fault leaves the row's fate unknown, and the close has
/// already landed — so the error has to carry both the tx and the reclaim,
/// which is the operation that clears the row it warns about.
#[test]
fn a_faulted_clear_fails_and_keeps_the_reclaim_instruction() {
    let err = grade_local_forget(
        Err(decdn_incentive::StoreError::Backend("no space".into())),
        a_pool(),
        a_tx(),
        RECLAIM_NOTE,
    )
    .expect_err("a row of unknown fate must not read as a clean close");
    let msg = format!("{err:#}");
    assert!(msg.contains("closed on-chain"), "{msg}");
    assert!(msg.contains("no space"), "the cause must survive: {msg}");
    assert!(
        msg.contains(&format!("{}", a_tx())),
        "the tx is the handle an operator reconciles against: {msg}"
    );
    assert!(
        msg.contains("decdn pool reclaim"),
        "the close landed, so the reclaim deadline must survive the failure: {msg}"
    );
}

// ---- `--all` classification + summary --------------------------------

#[test]
fn close_plan_acts_only_on_open() {
    assert_eq!(plan_close(PaymentPool::Status::Open), ClosePlan::Close);
    assert_matches!(plan_close(PaymentPool::Status::Closing), ClosePlan::Skip(_));
    assert_matches!(plan_close(PaymentPool::Status::Closed), ClosePlan::Skip(_));
}

#[test]
fn reclaim_plan_respects_status_and_deadline() {
    // Open must be closed first.
    assert_eq!(
        plan_reclaim(PaymentPool::Status::Open, 100, 200),
        ReclaimPlan::SkipOpen
    );
    // Closing, still inside the window: not yet, and it carries the deadline.
    assert_eq!(
        plan_reclaim(PaymentPool::Status::Closing, 200, 100),
        ReclaimPlan::SkipInWindow(200)
    );
    // Closing, window elapsed (now == deadline is elapsed): reclaim.
    assert_eq!(
        plan_reclaim(PaymentPool::Status::Closing, 200, 200),
        ReclaimPlan::Reclaim
    );
    assert_eq!(
        plan_reclaim(PaymentPool::Status::Closing, 200, 300),
        ReclaimPlan::Reclaim
    );
    // Already reclaimed.
    assert_eq!(
        plan_reclaim(PaymentPool::Status::Closed, 0, 300),
        ReclaimPlan::SkipClosed
    );
}

/// The `--pool` gate refuses each non-reclaimable pool with its own cause,
/// never with the post-send `reclaim_reverted` text.
#[test]
fn single_reclaim_gate_names_the_cause() {
    let owner = Address::repeat_byte(0x22);
    let gate = |owner, status, deadline, now| {
        single_reclaim_gate(a_pool(), owner, status, deadline, now)
            .err()
            .map(|e| e.to_string())
    };

    // Past (or at) the deadline: send.
    assert_eq!(gate(owner, PaymentPool::Status::Closing, 200, 200), None);
    assert_eq!(gate(owner, PaymentPool::Status::Closing, 200, 300), None);

    let refusals = [
        (
            gate(Address::ZERO, PaymentPool::Status::Open, 0, 300),
            "does not exist",
        ),
        (
            gate(owner, PaymentPool::Status::Open, 0, 300),
            "decdn pool close --pool",
        ),
        (
            gate(
                owner,
                PaymentPool::Status::Closing,
                1_790_875_000,
                1_790_700_000,
            ),
            "dispute window; reclaimable after Unix 1790875000 (in 2d 0h 36m)",
        ),
        (
            gate(owner, PaymentPool::Status::Closed, 200, 300),
            "already reclaimed",
        ),
    ];
    for (msg, cause) in refusals {
        let msg = msg.unwrap_or_default();
        assert!(msg.contains(cause), "expected {cause:?} in {msg:?}");
        assert!(
            !msg.contains("reclaim reverted"),
            "must name its own cause: {msg}"
        );
    }
}

#[test]
fn batch_result_is_ok_unless_something_failed() {
    // Nothing eligible is success (exit 0) — the summary line said so.
    assert!(batch_result(&BatchTally::default(), "close").is_ok());
    // Acted + skipped, none failed: still success.
    let clean = BatchTally {
        acted: 3,
        skipped: 2,
        failed: 0,
    };
    assert!(batch_result(&clean, "reclaim").is_ok());
    // Any failure is a nonzero exit, and the verb reaches the message.
    let broke = BatchTally {
        acted: 1,
        skipped: 0,
        failed: 2,
    };
    let err = batch_result(&broke, "close").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains('2'), "the failure count must show: {msg}");
    assert!(msg.contains("close"), "the verb must show: {msg}");
}

#[test]
fn the_owner_check_passes_when_the_chain_agrees() {
    assert_eq!(
        grade_on_chain_owner(addr(0xaa), addr(0xaa)),
        OwnerVerdict::Owned
    );
}

/// A successful read that disagrees proves the token is dead at redemption.
/// Emitting it anyway is how `decdn pool assign … > delegate.token` writes a
/// file that looks valid and is not.
#[test]
fn a_disagreeing_owner_read_refuses_to_issue() {
    let verdict = grade_on_chain_owner(addr(0xbb), addr(0xaa));
    assert_eq!(verdict, OwnerVerdict::OwnedByOther(addr(0xbb)));

    let err = verdict
        .into_result(a_pool(), addr(0xaa))
        .expect_err("a proven-dead capability must not be issued");
    let msg = format!("{err:#}");
    assert!(msg.contains("rejected at redemption"), "{msg}");
    assert!(
        msg.contains("no token was issued"),
        "the operator must know nothing usable reached stdout: {msg}"
    );
    // The two addresses are the same type and the verdict is symmetric, so
    // only the message distinguishes them. Swapping them would send the
    // operator to check the wrong key.
    let (on_chain, keystore) = (format!("{}", addr(0xbb)), format!("{}", addr(0xaa)));
    let on_chain_at = msg.find(&on_chain).expect("names the on-chain owner");
    let keystore_at = msg.find(&keystore).expect("names the keystore address");
    assert!(
        on_chain_at < keystore_at,
        "the on-chain owner must be named as the owner, not as the keystore: {msg}"
    );
}

/// `getPool` zero-fills an unknown key rather than reverting, so a wrong
/// `--pool`, `--payment-pool-address` or `--chain-id` arrives as a
/// *successful* read of a zero owner. That is not an ownership dispute, and
/// telling the operator to "sign with the owner keystore" sends them after
/// the wrong fault.
#[test]
fn an_unopened_pool_is_diagnosed_as_missing_not_as_a_wrong_keystore() {
    let verdict = grade_on_chain_owner(Address::ZERO, addr(0xaa));
    assert_eq!(verdict, OwnerVerdict::NoSuchPool);

    let err = verdict
        .into_result(a_pool(), addr(0xaa))
        .expect_err("a capability for a pool that does not exist must not be issued");
    let msg = format!("{err:#}");
    assert!(msg.contains("does not exist"), "{msg}");
    assert!(
        msg.contains("--payment-pool-address"),
        "the likely fault is a misconfigured contract or chain, so name them: {msg}"
    );
    assert!(
        !msg.contains("sign with the owner keystore"),
        "a missing pool is not an ownership dispute: {msg}"
    );
}

/// The other half of the split: a read that could not be performed proves
/// nothing, so offline issuance stays a warning and exits 0. Pinned against
/// a port nothing listens on, so it needs no chain and no provider mock.
#[tokio::test]
async fn an_unreachable_rpc_warns_and_still_issues() {
    let signer = PrivateKeySigner::random();
    let rpc = provider::build_provider("http://127.0.0.1:1", &signer)
        .expect("a well-formed URL builds a provider without dialing");
    let contract = PaymentPool::new(Address::ZERO, rpc);
    let owner = signer.address();

    ensure_on_chain_owner(&contract, a_pool(), owner)
        .await
        .expect("offline issuance is valid by design — an unreachable RPC must not fail it");
}

/// A `PaymentPool` on a filler-free provider that answers the
/// `eth_sendTransaction` with `hash` and then fails every call, so
/// `get_receipt` cannot read the receipt of a broadcast tx.
fn pool_whose_receipt_is_unreadable(
    hash: B256,
) -> PaymentPool::PaymentPoolInstance<impl alloy::providers::Provider + Clone> {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&hash);
    PaymentPool::new(
        Address::repeat_byte(0x01),
        alloy::providers::ProviderBuilder::default().connect_mocked_client(asserter),
    )
}

/// A `closePool` whose receipt cannot be read is in flight, so the error names
/// its tx hash (#2413).
#[tokio::test]
async fn an_unreadable_close_receipt_names_the_tx() {
    let hash = B256::repeat_byte(0xab);
    let err = close_and_forget(
        &pool_whose_receipt_is_unreadable(hash),
        LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
    )
    .await
    .unwrap_err();
    assert!(
        format!("{err:#}").contains(&format!("{hash:#x}")),
        "{err:#}"
    );
}

/// A `reclaim` whose receipt cannot be read is in flight, so the error names
/// its tx hash (#2413).
#[tokio::test]
async fn an_unreadable_reclaim_receipt_names_the_tx() {
    let hash = B256::repeat_byte(0xab);
    let err = reclaim_and_forget(
        &pool_whose_receipt_is_unreadable(hash),
        LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
    )
    .await
    .unwrap_err();
    assert!(
        format!("{err:#}").contains(&format!("{hash:#x}")),
        "{err:#}"
    );
}
