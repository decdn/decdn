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
/// on-chain leg succeeding, which it did. A status refusal on such a dir adds
/// no "cleared" claim to its message.
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
    let cleared = books.forget_not_open(
        Address::repeat_byte(0x11),
        B256::repeat_byte(0x22),
        PaymentPool::Status::Closed,
    );
    assert_eq!(cleared, NotOpenClear::Untouched);
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
/// already landed — so the error has to carry the tx and the commands that
/// clear the row it warns about, and mark the close as landed.
#[test]
fn a_faulted_clear_fails_and_keeps_the_reclaim_instruction() {
    let err = grade_local_forget(
        Err(decdn_incentive::StoreError::Backend("no space".into())),
        a_pool(),
        a_tx(),
        RECLAIM_NOTE,
    )
    .expect_err("a row of unknown fate must not read as a clean close");
    assert!(
        err.downcast_ref::<CloseUnrecorded>().is_some(),
        "a --all sweep must see that the close landed: {err:#}"
    );
    // The cause sits next to "failed" and survives a plain `{}` rendering,
    // rather than trailing the remedy as a chained source.
    let msg = err.to_string();
    assert!(
        msg.contains("failed: pool store backend: no space"),
        "{msg}"
    );
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
    assert!(
        msg.contains(&format!("decdn pool close --pool {}", a_pool())),
        "a re-run of the close clears the row, so the warning names it: {msg}"
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
        ..BatchTally::default()
    };
    assert!(batch_result(&clean, "reclaim").is_ok());
    // Any failure is a nonzero exit, and the verb reaches the message.
    let broke = BatchTally {
        acted: 1,
        failed: 2,
        ..BatchTally::default()
    };
    let err = batch_result(&broke, "close").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("2 pool(s) failed to close"), "{msg}");
    assert!(!msg.contains("may have broadcast"), "{msg}");
}

/// A pool whose tx may still mine is not reported as a definite failure, but
/// it still exits nonzero and tells the operator to check before a re-run.
#[test]
fn batch_result_names_unconfirmed_pools_apart_from_failed_ones() {
    let unconfirmed = BatchTally {
        unconfirmed: 1,
        ..BatchTally::default()
    };
    let msg = batch_result(&unconfirmed, "reclaim")
        .unwrap_err()
        .to_string();
    assert!(
        msg.contains("1 pool(s) may have broadcast a reclaim"),
        "{msg}"
    );
    assert!(msg.contains("before re-running"), "{msg}");
    assert!(!msg.contains("failed"), "{msg}");

    let both = BatchTally {
        failed: 2,
        unconfirmed: 3,
        ..BatchTally::default()
    };
    let msg = batch_result(&both, "close").unwrap_err().to_string();
    assert!(msg.contains("2 pool(s) failed to close"), "{msg}");
    assert!(
        msg.contains("3 pool(s) may have broadcast a close"),
        "{msg}"
    );
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

/// A `PaymentPool` on a filler-free provider whose RPC calls `asserter` answers
/// in order; a call past the queue fails in transport. The provider carries
/// the send-leg marker, as `decdn_client::provider::build_provider` does, so
/// the one request `send()` makes is read as the send.
fn pool_on(
    asserter: alloy::providers::mock::Asserter,
) -> PaymentPool::PaymentPoolInstance<impl alloy::providers::Provider + Clone> {
    PaymentPool::new(
        Address::repeat_byte(0x01),
        alloy::providers::ProviderBuilder::default()
            .layer(decdn_incentive::tx::SendLegLayer)
            .connect_mocked_client(asserter),
    )
}

/// [`pool_on`] with the nonce and gas fillers, so `send()` reads the chain
/// before it sends: an empty queue then fails the gas estimate (the first
/// read the fillers poll), not the send request.
fn filled_pool_on(
    asserter: alloy::providers::mock::Asserter,
) -> PaymentPool::PaymentPoolInstance<impl alloy::providers::Provider + Clone> {
    PaymentPool::new(
        Address::repeat_byte(0x01),
        alloy::providers::ProviderBuilder::default()
            .layer(decdn_incentive::tx::SendLegLayer)
            .with_simple_nonce_management()
            .with_gas_estimation()
            .connect_mocked_client(asserter),
    )
}

/// A `PaymentPool` on a filler-free provider that answers the
/// `eth_sendTransaction` with `hash` and then fails every call, so
/// `get_receipt` cannot read the receipt of a broadcast tx.
fn pool_whose_receipt_is_unreadable(
    hash: B256,
) -> PaymentPool::PaymentPoolInstance<impl alloy::providers::Provider + Clone> {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&hash);
    pool_on(asserter)
}

/// A `closePool` whose receipt cannot be read may still be in flight, so the
/// error names its tx hash (#2413).
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
    assert_eq!(
        err.downcast_ref::<TxUnconfirmed>(),
        Some(&TxUnconfirmed {
            label: "closePool",
            tx: Some(hash)
        })
    );
}

/// A `reclaim` whose receipt cannot be read may still be in flight, so the
/// error names its tx hash (#2413).
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
    assert_eq!(
        err.downcast_ref::<TxUnconfirmed>(),
        Some(&TxUnconfirmed {
            label: "reclaim",
            tx: Some(hash)
        })
    );
}

/// A `PaymentPool` whose send is broadcast as
/// [`SCRIPTED_TX`](decdn_incentive::test_rpc::SCRIPTED_TX) and whose receipt
/// never arrives: a tx dropped from the mempool.
fn pool_whose_tx_never_mines()
-> PaymentPool::PaymentPoolInstance<impl alloy::providers::Provider + Clone> {
    PaymentPool::new(
        Address::repeat_byte(0x01),
        decdn_incentive::test_rpc::ScriptedRpc::parked_receipt().provider(),
    )
}

/// A `closePool` whose receipt never arrives ends at [`RECEIPT_TIMEOUT`] as
/// unconfirmed, naming its tx, so a `--all` sweep moves on to the next pool
/// (#2433).
#[tokio::test(start_paused = true)]
async fn a_close_that_never_mines_lapses_unconfirmed() {
    let err = tokio::time::timeout(
        RECEIPT_TIMEOUT * 2,
        close_and_forget(
            &pool_whose_tx_never_mines(),
            LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
            Address::repeat_byte(0x22),
            B256::repeat_byte(0x33),
        ),
    )
    .await
    .expect("the receipt wait must lapse within RECEIPT_TIMEOUT")
    .unwrap_err();
    assert_eq!(
        err.downcast_ref::<TxUnconfirmed>(),
        Some(&TxUnconfirmed {
            label: "closePool",
            tx: Some(decdn_incentive::test_rpc::SCRIPTED_TX)
        })
    );
    assert!(format!("{err:#}").contains("no receipt within"), "{err:#}");
}

/// A `reclaim` whose receipt never arrives ends at [`RECEIPT_TIMEOUT`] as
/// unconfirmed, naming its tx (#2433).
#[tokio::test(start_paused = true)]
async fn a_reclaim_that_never_mines_lapses_unconfirmed() {
    let err = tokio::time::timeout(
        RECEIPT_TIMEOUT * 2,
        reclaim_and_forget(
            &pool_whose_tx_never_mines(),
            LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
            Address::repeat_byte(0x22),
            B256::repeat_byte(0x33),
        ),
    )
    .await
    .expect("the receipt wait must lapse within RECEIPT_TIMEOUT")
    .unwrap_err();
    assert_eq!(
        err.downcast_ref::<TxUnconfirmed>(),
        Some(&TxUnconfirmed {
            label: "reclaim",
            tx: Some(decdn_incentive::test_rpc::SCRIPTED_TX)
        })
    );
    assert!(format!("{err:#}").contains("no receipt within"), "{err:#}");
}

/// The `send_failed` note that a send whose broadcast is unknown may still mine.
const MAYBE_BROADCAST: &str = "may be in a mempool and may still mine";

/// Run [`close_and_forget`] against a pool whose RPC calls `asserter` answers.
async fn close_on(asserter: alloy::providers::mock::Asserter) -> anyhow::Error {
    close_and_forget(
        &pool_on(asserter),
        LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
    )
    .await
    .unwrap_err()
}

/// Run [`reclaim_and_forget`] against a pool whose RPC calls `asserter` answers.
async fn reclaim_on(asserter: alloy::providers::mock::Asserter) -> anyhow::Error {
    reclaim_and_forget(
        &pool_on(asserter),
        LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
    )
    .await
    .unwrap_err()
}

/// A `closePool` send that fails in transport may have broadcast the tx with no
/// hash returned, so the error warns before the operator re-runs (#2416).
#[tokio::test]
async fn a_transport_failed_close_send_warns_it_may_have_broadcast() {
    let err = close_on(alloy::providers::mock::Asserter::new()).await;
    assert_eq!(
        err.downcast_ref::<TxUnconfirmed>(),
        Some(&TxUnconfirmed {
            label: "closePool",
            tx: None
        })
    );
    let msg = format!("{err:#}");
    assert!(msg.contains("closePool send failed"), "{msg}");
    assert!(msg.contains(MAYBE_BROADCAST), "{msg}");
    // The failure is the send itself, not a fill-stage read before it.
    assert!(msg.contains("eth_sendTransaction"), "{msg}");
}

/// A `closePool` whose chain reads fail before the send signed nothing, so
/// it carries no [`TxUnconfirmed`] and no broadcast warning (#2431).
#[tokio::test]
async fn a_close_whose_reads_fail_before_the_send_is_not_unconfirmed() {
    let err = close_and_forget(
        &filled_pool_on(alloy::providers::mock::Asserter::new()),
        LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
    )
    .await
    .unwrap_err();
    assert!(err.downcast_ref::<TxUnconfirmed>().is_none(), "{err:#}");
    let msg = format!("{err:#}");
    assert!(msg.contains("closePool send failed"), "{msg}");
    assert!(!msg.contains(MAYBE_BROADCAST), "{msg}");
    assert!(msg.contains("eth_estimateGas"), "{msg}");
    assert!(!msg.contains("eth_sendTransaction"), "{msg}");
}

/// A `reclaim` whose chain reads fail before the send signed nothing, so it
/// carries no [`TxUnconfirmed`] and no broadcast warning (#2431).
#[tokio::test]
async fn a_reclaim_whose_reads_fail_before_the_send_is_not_unconfirmed() {
    let err = reclaim_and_forget(
        &filled_pool_on(alloy::providers::mock::Asserter::new()),
        LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
    )
    .await
    .unwrap_err();
    assert!(err.downcast_ref::<TxUnconfirmed>().is_none(), "{err:#}");
    let msg = format!("{err:#}");
    assert!(msg.contains("reclaim send failed"), "{msg}");
    assert!(!msg.contains(MAYBE_BROADCAST), "{msg}");
    assert!(msg.contains("eth_estimateGas"), "{msg}");
    assert!(!msg.contains("eth_sendTransaction"), "{msg}");
}

/// `reclaim --all` counts a pool whose `reclaim` reads failed before the send
/// as failed, not as one that may have broadcast (#2431).
#[tokio::test]
async fn reclaim_all_counts_a_failed_read_before_the_send_as_failed() {
    let owner = Address::repeat_byte(0x22);
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&owned_pools(2));
    let mut head = alloy::rpc::types::Block::<alloy::rpc::types::Transaction>::default();
    head.header.inner.timestamp = 1_000;
    asserter.push_success(&head);
    for _ in 0..2 {
        asserter.push_success(&pool_in(owner, PaymentPool::Status::Closing));
    }

    let err = reclaim_all(
        &filled_pool_on(asserter),
        LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
        owner,
    )
    .await
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("2 pool(s) failed to reclaim"), "{msg}");
    assert!(!msg.contains("may have broadcast"), "{msg}");
}

/// `close --all` counts a pool whose `closePool` reads failed before the send
/// as failed, not as one that may have broadcast (#2431): every send in the
/// sweep fails on the first chain read, so no pool is unconfirmed.
#[tokio::test]
async fn close_all_counts_a_failed_read_before_the_send_as_failed() {
    let owner = Address::repeat_byte(0x22);
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&owned_pools(2));
    for _ in 0..2 {
        asserter.push_success(&pool_in(owner, PaymentPool::Status::Open));
    }

    let err = close_all(
        &filled_pool_on(asserter),
        LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
        owner,
    )
    .await
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("2 pool(s) failed to close"), "{msg}");
    assert!(!msg.contains("may have broadcast"), "{msg}");
}

/// A `closePool` send the RPC node rejects (other than `already known`)
/// broadcast nothing, so it carries no broadcast warning.
#[tokio::test]
async fn a_rejected_close_send_does_not_warn() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_failure_msg("insufficient funds for gas");
    let err = close_on(asserter).await;
    assert!(err.downcast_ref::<TxUnconfirmed>().is_none(), "{err:#}");
    let msg = format!("{err:#}");
    assert!(msg.contains("closePool send failed"), "{msg}");
    assert!(!msg.contains(MAYBE_BROADCAST), "{msg}");
}

/// A `closePool` send the RPC node answers with `already known` may still
/// mine: an earlier copy of it reached a mempool (#2432). It carries
/// [`TxUnconfirmed`] with no hash, so `--all` counts it as unconfirmed.
#[tokio::test]
async fn an_already_known_close_send_warns_it_may_have_broadcast() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_failure_msg("already known");
    let err = close_on(asserter).await;
    assert_eq!(
        err.downcast_ref::<TxUnconfirmed>(),
        Some(&TxUnconfirmed {
            label: "closePool",
            tx: None
        })
    );
    assert!(format!("{err:#}").contains(MAYBE_BROADCAST), "{err:#}");
    let mut tally = BatchTally::default();
    assert_eq!(tally.record_error(&err), ErrorClass::Unconfirmed);
}

/// A `reclaim` send that fails in transport may have broadcast the tx with no
/// hash returned, so the error warns before the operator re-runs (#2416).
#[tokio::test]
async fn a_transport_failed_reclaim_send_warns_it_may_have_broadcast() {
    let err = reclaim_on(alloy::providers::mock::Asserter::new()).await;
    assert_eq!(
        err.downcast_ref::<TxUnconfirmed>(),
        Some(&TxUnconfirmed {
            label: "reclaim",
            tx: None
        })
    );
    let msg = format!("{err:#}");
    assert!(msg.contains("reclaim send failed"), "{msg}");
    assert!(msg.contains(MAYBE_BROADCAST), "{msg}");
    // The failure is the send itself, not a fill-stage read before it.
    assert!(msg.contains("eth_sendTransaction"), "{msg}");
}

/// A `reclaim` send the RPC node rejects (other than `already known`)
/// broadcast nothing, so it carries no broadcast warning.
#[tokio::test]
async fn a_rejected_reclaim_send_does_not_warn() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_failure_msg("insufficient funds for gas");
    let err = reclaim_on(asserter).await;
    assert!(err.downcast_ref::<TxUnconfirmed>().is_none(), "{err:#}");
    let msg = format!("{err:#}");
    assert!(msg.contains("reclaim send failed"), "{msg}");
    assert!(!msg.contains(MAYBE_BROADCAST), "{msg}");
}

/// `record_error` counts an error that carries [`TxUnconfirmed`] as
/// unconfirmed, and any other error as failed.
#[test]
fn record_error_splits_unconfirmed_from_failed() {
    let mut tally = BatchTally::default();
    let rejected = anyhow::anyhow!("insufficient funds").context("closePool send failed");
    assert_eq!(tally.record_error(&rejected), ErrorClass::Failed);
    let unconfirmed = anyhow::anyhow!("timed out").context(TxUnconfirmed {
        label: "closePool",
        tx: None,
    });
    assert_eq!(tally.record_error(&unconfirmed), ErrorClass::Unconfirmed);
    assert_eq!(tally.record_error(&unconfirmed), ErrorClass::Unconfirmed);
    assert_eq!(
        tally,
        BatchTally {
            failed: 1,
            unconfirmed: 2,
            ..BatchTally::default()
        }
    );
}

/// The `getPools` answer for `n` owned pools, as one page.
fn owned_pools(n: u8) -> alloy::primitives::Bytes {
    use alloy::sol_types::SolCall;

    let ids: Vec<PoolId> = (1..=n).map(B256::repeat_byte).collect();
    PaymentPool::getPoolsCall::abi_encode_returns(&ids).into()
}

/// The `getPool` answer for a pool `owner` holds in `status`.
fn pool_in(owner: Address, status: PaymentPool::Status) -> alloy::primitives::Bytes {
    use alloy::sol_types::SolCall;

    PaymentPool::getPoolCall::abi_encode_returns(&PaymentPool::Pool {
        owner,
        status,
        disputeDeadline: 0,
        deposit: 1_000_000,
        totalRedeemed: 0,
    })
    .into()
}

/// Queue a sweep over three pools in `status`: the RPC node rejects the first
/// two sends, and the third fails in transport because the queue runs out.
/// The counts are asymmetric, so a swapped classification changes them.
fn two_rejected_then_one_lost(
    asserter: &alloy::providers::mock::Asserter,
    owner: Address,
    status: PaymentPool::Status,
) {
    for _ in 0..2 {
        asserter.push_success(&pool_in(owner, status));
        asserter.push_failure_msg("insufficient funds for gas");
    }
    asserter.push_success(&pool_in(owner, status));
}

/// `close --all` reports a rejected `closePool` as failed and a transport-failed
/// one as unconfirmed, not as a failure (#2427).
#[tokio::test]
async fn close_all_counts_a_transport_failed_send_as_unconfirmed() {
    let owner = Address::repeat_byte(0x22);
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&owned_pools(3));
    two_rejected_then_one_lost(&asserter, owner, PaymentPool::Status::Open);

    let err = close_all(
        &pool_on(asserter),
        LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
        owner,
    )
    .await
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("2 pool(s) failed to close"), "{msg}");
    assert!(
        msg.contains("1 pool(s) may have broadcast a close"),
        "{msg}"
    );
}

/// `reclaim --all` reports a rejected `reclaim` as failed and a
/// transport-failed one as unconfirmed, not as a failure (#2427).
#[tokio::test]
async fn reclaim_all_counts_a_transport_failed_send_as_unconfirmed() {
    let owner = Address::repeat_byte(0x22);
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&owned_pools(3));
    // The head block `reclaim_all` reads once before the loop. Every pool's
    // dispute deadline is 0, so each one is reclaimable.
    let mut head = alloy::rpc::types::Block::<alloy::rpc::types::Transaction>::default();
    head.header.inner.timestamp = 1_000;
    asserter.push_success(&head);
    two_rejected_then_one_lost(&asserter, owner, PaymentPool::Status::Closing);

    let err = reclaim_all(
        &pool_on(asserter),
        LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb")),
        owner,
    )
    .await
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("2 pool(s) failed to reclaim"), "{msg}");
    assert!(
        msg.contains("1 pool(s) may have broadcast a reclaim"),
        "{msg}"
    );
}

/// A mined reclaim revert names its tx.
#[test]
fn reclaim_reverted_names_the_mined_tx() {
    let hash = B256::repeat_byte(0xab);
    let msg = reclaim_reverted(B256::repeat_byte(0x33), hash);
    assert!(msg.contains(&format!("(tx {hash:#x})")), "{msg}");
    assert!(msg.contains(RECLAIM_REVERT_GUESS), "{msg}");
}

/// An RPC that answers the filler-free `eth_sendTransaction` with revert data
/// `selector`, standing in for the `eth_estimateGas` rejection a filled
/// provider gets for a call that would revert.
fn reverting_with(selector: [u8; 4]) -> alloy::providers::mock::Asserter {
    let data = format!("\"{}\"", alloy::primitives::hex::encode_prefixed(selector));
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_failure(alloy_json_rpc::ErrorPayload {
        code: 3,
        message: "execution reverted".into(),
        data: Some(serde_json::value::RawValue::from_string(data).unwrap()),
    });
    asserter
}

/// Assert that `msg` reports a send that gas estimation rejected: nothing was
/// sent, so it says neither "on-chain" nor that the tx may have broadcast.
fn assert_rejected_before_send(msg: &str, label: &str) {
    assert!(
        msg.contains(&format!("{label} would revert for pool")),
        "{msg}"
    );
    assert!(
        msg.contains("rejected at gas estimation; nothing was sent"),
        "{msg}"
    );
    assert!(!msg.contains("on-chain"), "{msg}");
    assert!(!msg.contains(MAYBE_BROADCAST), "{msg}");
}

/// A `closePool` gas estimation that reverts `PoolNotOpen` names the error and
/// the likely concurrent close, and says nothing was sent (#2426).
#[tokio::test]
async fn a_close_estimation_revert_names_pool_not_open() {
    use alloy::sol_types::SolError;
    use decdn_incentive::pool_open_error::PoolNotOpen;
    let msg = format!(
        "{:#}",
        close_on(reverting_with(PoolNotOpen::SELECTOR)).await
    );
    assert_rejected_before_send(&msg, "closePool");
    assert!(
        msg.contains("PoolNotOpen() — the pool is no longer Open"),
        "{msg}"
    );
    assert!(msg.contains("concurrent close"), "{msg}");
}

/// A `closePool` gas estimation that reverts `NotPoolOwner` names the error.
#[tokio::test]
async fn a_close_estimation_revert_names_not_pool_owner() {
    use alloy::sol_types::SolError;
    use decdn_incentive::pool_open_error::NotPoolOwner;
    let msg = format!(
        "{:#}",
        close_on(reverting_with(NotPoolOwner::SELECTOR)).await
    );
    assert_rejected_before_send(&msg, "closePool");
    assert!(
        msg.contains("NotPoolOwner() — this keystore does not own the pool"),
        "{msg}"
    );
}

/// A `reclaim` gas estimation that reverts `PoolNotClosing` names the error
/// in place of the generic guess (#2426).
#[tokio::test]
async fn a_reclaim_estimation_revert_names_pool_not_closing() {
    use alloy::sol_types::SolError;
    use decdn_incentive::pool_open_error::PoolNotClosing;
    let msg = format!(
        "{:#}",
        reclaim_on(reverting_with(PoolNotClosing::SELECTOR)).await
    );
    assert_rejected_before_send(&msg, "reclaim");
    assert!(
        msg.contains("PoolNotClosing() — the pool is not Closing"),
        "{msg}"
    );
    assert!(!msg.contains(RECLAIM_REVERT_GUESS), "{msg}");
}

/// A `reclaim` gas estimation that reverts `GraceWindowActive` names the error.
#[tokio::test]
async fn a_reclaim_estimation_revert_names_grace_window_active() {
    use alloy::sol_types::SolError;
    use decdn_incentive::pool_open_error::GraceWindowActive;
    let msg = format!(
        "{:#}",
        reclaim_on(reverting_with(GraceWindowActive::SELECTOR)).await
    );
    assert_rejected_before_send(&msg, "reclaim");
    assert!(
        msg.contains("GraceWindowActive() — the dispute window has not elapsed"),
        "{msg}"
    );
}

/// A `reclaim` gas estimation that reverts with an unknown selector points at
/// the refund transfer, not a concurrent reclaim (which reverts
/// `PoolNotClosing`), and keeps the raw revert data from the source error.
#[tokio::test]
async fn a_reclaim_estimation_revert_with_an_unknown_selector_blames_the_refund() {
    let msg = format!(
        "{:#}",
        reclaim_on(reverting_with([0xde, 0xad, 0xbe, 0xef])).await
    );
    assert_rejected_before_send(&msg, "reclaim");
    assert!(msg.contains(RECLAIM_UNKNOWN_REVERT), "{msg}");
    assert!(!msg.contains("concurrent reclaim"), "{msg}");
    assert!(msg.contains("0xdeadbeef"), "{msg}");
}

/// A `closePool` gas estimation that reverts with an unknown selector names no
/// cause and keeps the raw revert data from the source error.
#[tokio::test]
async fn a_close_estimation_revert_with_an_unknown_selector_keeps_the_revert_data() {
    let msg = format!(
        "{:#}",
        close_on(reverting_with([0xde, 0xad, 0xbe, 0xef])).await
    );
    assert_rejected_before_send(&msg, "closePool");
    assert!(!msg.contains(RECLAIM_UNKNOWN_REVERT), "{msg}");
    assert!(!msg.contains(RECLAIM_REVERT_GUESS), "{msg}");
    assert!(msg.contains("0xdeadbeef"), "{msg}");
}

// ---- clearing the row of a pool that is no longer Open (#2425) ---------

/// A row that maps `owner` to pool `pool_byte`.
fn row(pool_byte: u8, owner: Address) -> BuyerPoolState {
    BuyerPoolState::new(
        B256::repeat_byte(pool_byte),
        DEPLOYMENT,
        owner,
        Address::repeat_byte(0xcd),
        U256::from(1_000_000_u64),
    )
}

/// Bookkeeping on `store` for [`DEPLOYMENT`].
fn books(store: &dyn BuyerPoolStore) -> LocalBookkeeping<'_> {
    LocalBookkeeping::Store {
        store,
        deployment: DEPLOYMENT,
    }
}

/// A store holding `rows`.
fn store_with(rows: &[BuyerPoolState]) -> decdn_incentive::MemoryBuyerPoolStore {
    let store = decdn_incentive::MemoryBuyerPoolStore::new();
    for state in rows {
        store
            .record(state)
            .expect("an in-memory record cannot fail");
    }
    store
}

/// The pool `owner`'s row points at, if any.
fn tracked(store: &dyn BuyerPoolStore, owner: Address) -> Option<PoolId> {
    store
        .get_by_owner(owner)
        .expect("an in-memory read cannot fail")
        .map(|state| state.pool_id)
}

/// The `getPool` answer for a pool `owner` holds in `status`, with a dispute
/// deadline of `deadline`.
fn pool_until(
    owner: Address,
    status: PaymentPool::Status,
    deadline: u64,
) -> alloy::primitives::Bytes {
    use alloy::sol_types::SolCall;

    PaymentPool::getPoolCall::abi_encode_returns(&PaymentPool::Pool {
        owner,
        status,
        disputeDeadline: deadline,
        deposit: 1_000_000,
        totalRedeemed: 0,
    })
    .into()
}

/// A head block at Unix `timestamp`, the read `reclaim_all` makes once.
fn head_at(timestamp: u64) -> alloy::rpc::types::Block<alloy::rpc::types::Transaction> {
    let mut head = alloy::rpc::types::Block::<alloy::rpc::types::Transaction>::default();
    head.header.inner.timestamp = timestamp;
    head
}

/// Only `Closing` and `Closed` mark a row stale; `Open` never touches it.
#[test]
fn only_a_closing_or_closed_pool_is_no_longer_open() {
    assert!(!no_longer_open(PaymentPool::Status::Open));
    assert!(no_longer_open(PaymentPool::Status::Closing));
    assert!(no_longer_open(PaymentPool::Status::Closed));
}

/// `close --all` skips a `Closing` pool and clears the owner's row for it, so
/// a close whose first run could not clear the row is repaired on a re-run. A
/// row for another owner stays.
#[tokio::test]
async fn close_all_clears_the_row_of_a_skipped_closing_pool() {
    let owner = Address::repeat_byte(0x22);
    let other = Address::repeat_byte(0x44);
    let store = store_with(&[row(0x02, owner), row(0x09, other)]);
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&owned_pools(2));
    asserter.push_success(&pool_in(owner, PaymentPool::Status::Closed));
    asserter.push_success(&pool_in(owner, PaymentPool::Status::Closing));

    close_all(&pool_on(asserter), books(&store), owner)
        .await
        .expect("two skipped pools are a clean sweep");
    assert_eq!(tracked(&store, owner), None);
    assert_eq!(tracked(&store, other), Some(B256::repeat_byte(0x09)));
}

/// The clear is compare-and-delete: an owner whose row already points at a
/// newer pool keeps it when an older pool is skipped as `Closing`.
#[tokio::test]
async fn close_all_keeps_a_row_for_a_newer_pool() {
    let owner = Address::repeat_byte(0x22);
    let store = store_with(&[row(0x05, owner)]);
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&owned_pools(1));
    asserter.push_success(&pool_in(owner, PaymentPool::Status::Closing));

    close_all(&pool_on(asserter), books(&store), owner)
        .await
        .expect("a skipped pool is a clean sweep");
    assert_eq!(tracked(&store, owner), Some(B256::repeat_byte(0x05)));
}

/// `reclaim --all` clears the row of a skipped `Closed` pool (the reclaim
/// already ran) and of a `Closing` pool still in its dispute window. A row
/// for another owner stays.
#[tokio::test]
async fn reclaim_all_clears_the_row_of_a_skipped_closing_or_closed_pool() {
    let owner = Address::repeat_byte(0x22);
    let other = Address::repeat_byte(0x44);
    for (status, deadline) in [
        (PaymentPool::Status::Closed, 0),
        (PaymentPool::Status::Closing, 5_000),
    ] {
        let store = store_with(&[row(0x01, owner), row(0x09, other)]);
        let asserter = alloy::providers::mock::Asserter::new();
        asserter.push_success(&owned_pools(1));
        asserter.push_success(&head_at(1_000));
        asserter.push_success(&pool_until(owner, status, deadline));

        reclaim_all(&pool_on(asserter), books(&store), owner)
            .await
            .expect("a skipped pool is a clean sweep");
        assert_eq!(tracked(&store, owner), None, "{}", status_label(status));
        assert_eq!(
            tracked(&store, other),
            Some(B256::repeat_byte(0x09)),
            "{}",
            status_label(status)
        );
    }
}

/// `close --pool` on a pool that is no longer `Open` still refuses, and clears
/// the row first; the refusal says so.
#[test]
fn a_close_refusal_clears_the_row_and_says_so() {
    let owner = Address::repeat_byte(0x22);
    let other = Address::repeat_byte(0x44);
    for status in [PaymentPool::Status::Closing, PaymentPool::Status::Closed] {
        let store = store_with(&[row(0x01, owner), row(0x09, other)]);
        let msg = refuse_close(books(&store), owner, B256::repeat_byte(0x01), status).to_string();
        assert!(msg.contains("is not Open — nothing to close"), "{msg}");
        assert!(
            msg.contains("cleared its row from the local store"),
            "{msg}"
        );
        assert_eq!(tracked(&store, owner), None, "{}", status_label(status));
        assert_eq!(
            tracked(&store, other),
            Some(B256::repeat_byte(0x09)),
            "only the caller's row is cleared: {}",
            status_label(status)
        );
    }
}

/// A refusal that cleared nothing does not claim a clear.
#[test]
fn a_close_refusal_with_no_row_claims_no_clear() {
    let store = store_with(&[]);
    let msg = refuse_close(
        books(&store),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x01),
        PaymentPool::Status::Closing,
    )
    .to_string();
    assert!(msg.contains("is not Open"), "{msg}");
    assert!(!msg.contains("cleared"), "{msg}");
}

/// `reclaim --pool` on a `Closed` pool keeps the gate's refusal, and clears
/// the row first — the re-run the `forget_after_reclaim` warning names.
#[test]
fn a_reclaim_refusal_clears_the_row_and_keeps_the_cause() {
    let owner = Address::repeat_byte(0x22);
    let pool_id = B256::repeat_byte(0x01);
    let store = store_with(&[row(0x01, owner)]);
    let refusal = single_reclaim_gate(pool_id, owner, PaymentPool::Status::Closed, 0, 300)
        .expect_err("a Closed pool has nothing to reclaim");
    let msg = refuse_reclaim(
        &refusal,
        Ok(books(&store)),
        owner,
        pool_id,
        PaymentPool::Status::Closed,
    )
    .to_string();
    assert!(msg.contains("already reclaimed"), "{msg}");
    assert!(
        msg.contains("cleared its row from the local store"),
        "{msg}"
    );
    assert_eq!(tracked(&store, owner), None);
}

/// A store that will not open (a concurrent fetch holds it) does not hide the
/// refusal: the operator still learns why there is nothing to reclaim, and
/// the line says the row was not cleared.
#[test]
fn a_reclaim_refusal_survives_a_store_that_will_not_open() {
    let pool_id = B256::repeat_byte(0x01);
    let refusal = single_reclaim_gate(
        pool_id,
        Address::repeat_byte(0x22),
        PaymentPool::Status::Closing,
        5_000,
        1_000,
    )
    .expect_err("an in-window pool has nothing to reclaim yet");
    let open_fault = anyhow::anyhow!("another decdn process is using the pool store");
    let msg = refuse_reclaim(
        &refusal,
        Err(&open_fault),
        Address::repeat_byte(0x22),
        pool_id,
        PaymentPool::Status::Closing,
    )
    .to_string();
    assert!(msg.contains("reclaimable after"), "{msg}");
    assert!(msg.contains("could not be cleared"), "{msg}");
}

/// An `Open` pool's row is the one fetch pays from, so no status refusal
/// clears it, whatever the caller checked.
#[test]
fn an_open_pool_never_has_its_row_cleared() {
    let owner = Address::repeat_byte(0x22);
    let store = store_with(&[row(0x01, owner)]);
    let cleared =
        books(&store).forget_not_open(owner, B256::repeat_byte(0x01), PaymentPool::Status::Open);
    assert_eq!(cleared, NotOpenClear::Untouched);
    assert_eq!(tracked(&store, owner), Some(B256::repeat_byte(0x01)));
}

/// A pool id repeats across `PaymentPool` deployments, so a clear against one
/// deployment leaves a row for the same id on another deployment alone.
#[test]
fn a_row_on_another_deployment_survives() {
    let owner = Address::repeat_byte(0x22);
    let store = store_with(&[row(0x01, owner)]);
    let elsewhere = LocalBookkeeping::Store {
        store: &store,
        deployment: Deployment {
            chain_id: DEPLOYMENT.chain_id,
            payment_pool: Address::repeat_byte(0x9d),
        },
    };
    let cleared =
        elsewhere.forget_not_open(owner, B256::repeat_byte(0x01), PaymentPool::Status::Closed);
    assert_eq!(cleared, NotOpenClear::Untouched);
    // The landed paths scope the clear the same way.
    elsewhere
        .forget_after_close(owner, B256::repeat_byte(0x01), a_tx(), RECLAIM_NOTE)
        .expect("no row on this deployment is a clean close");
    elsewhere.forget_after_reclaim(owner, B256::repeat_byte(0x01));
    assert_eq!(tracked(&store, owner), Some(B256::repeat_byte(0x01)));
}

/// A store whose `forget_if_pool` faults; every other method delegates to an
/// in-memory store.
struct ForgetFaults(decdn_incentive::MemoryBuyerPoolStore);

impl BuyerPoolStore for ForgetFaults {
    fn load_all(&self) -> Result<BuyerLoad, decdn_incentive::StoreError> {
        self.0.load_all()
    }
    fn record(&self, state: &BuyerPoolState) -> Result<(), decdn_incentive::StoreError> {
        self.0.record(state)
    }
    fn forget(&self, owner: Address) -> Result<(), decdn_incentive::StoreError> {
        self.0.forget(owner)
    }
    fn get_by_pool_id(
        &self,
        pool_id: PoolId,
    ) -> Result<Option<BuyerPoolState>, decdn_incentive::StoreError> {
        self.0.get_by_pool_id(pool_id)
    }
    fn forget_if_pool(
        &self,
        _owner: Address,
        _pool_id: PoolId,
    ) -> Result<bool, decdn_incentive::StoreError> {
        Err(decdn_incentive::StoreError::Backend("no space".into()))
    }
    fn get_by_owner(
        &self,
        owner: Address,
    ) -> Result<Option<BuyerPoolState>, decdn_incentive::StoreError> {
        self.0.get_by_owner(owner)
    }
    fn advance_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: decdn_incentive::LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<decdn_incentive::AdvanceOutcome, decdn_incentive::StoreError> {
        self.0.advance_progress(owner, pool_id, lane, bytes, amount)
    }
    fn rebase_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: decdn_incentive::LaneKey,
        anchor: decdn_incentive::BuyerLaneProgress,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<decdn_incentive::AdvanceOutcome, decdn_incentive::StoreError> {
        self.0.rebase_progress(owner, pool_id, lane, anchor, totals)
    }
    fn seed_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: decdn_incentive::LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<decdn_incentive::AdvanceOutcome, decdn_incentive::StoreError> {
        self.0.seed_progress(owner, pool_id, lane, bytes, amount)
    }
    fn add_deposit(
        &self,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> Result<decdn_incentive::DepositOutcome, decdn_incentive::StoreError> {
        self.0.add_deposit(owner, pool_id, additional)
    }
    fn delegate_lanes(
        &self,
        deployment: Deployment,
        pool_id: PoolId,
        signer: Address,
    ) -> Result<Vec<(Address, decdn_incentive::BuyerLaneProgress)>, decdn_incentive::StoreError>
    {
        self.0.delegate_lanes(deployment, pool_id, signer)
    }
    fn advance_delegate_progress(
        &self,
        deployment: Deployment,
        lane: decdn_incentive::LaneKey,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<decdn_incentive::AdvanceOutcome, decdn_incentive::StoreError> {
        self.0.advance_delegate_progress(deployment, lane, totals)
    }
    fn rebase_delegate_progress(
        &self,
        deployment: Deployment,
        lane: decdn_incentive::LaneKey,
        anchor: decdn_incentive::BuyerLaneProgress,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<decdn_incentive::AdvanceOutcome, decdn_incentive::StoreError> {
        self.0
            .rebase_delegate_progress(deployment, lane, anchor, totals)
    }

    fn lane_salt(
        &self,
    ) -> Result<decdn_incentive::lane_signer::LaneSalt, decdn_incentive::StoreError> {
        self.0.lane_salt()
    }
}

/// A faulted row-clear on a skipped pool fails the sweep as "row not cleared",
/// the same as on the run whose close landed: a repair that did not happen
/// must not read as success.
#[tokio::test]
async fn a_faulted_clear_on_a_skipped_pool_fails_the_sweep() {
    let owner = Address::repeat_byte(0x22);
    let store = ForgetFaults(store_with(&[row(0x01, owner)]));
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&owned_pools(1));
    asserter.push_success(&pool_in(owner, PaymentPool::Status::Closing));
    let msg = close_all(&pool_on(asserter), books(&store), owner)
        .await
        .expect_err("a row that may survive is not a clean sweep")
        .to_string();
    assert!(msg.contains("1 pool(s) are no longer Open"), "{msg}");
    assert!(!msg.contains("failed to close"), "{msg}");

    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&owned_pools(1));
    asserter.push_success(&head_at(1_000));
    asserter.push_success(&pool_in(owner, PaymentPool::Status::Closed));
    let msg = reclaim_all(&pool_on(asserter), books(&store), owner)
        .await
        .expect_err("a row that may survive is not a clean sweep")
        .to_string();
    assert!(msg.contains("1 pool(s) are no longer Open"), "{msg}");
}

/// A close that landed but whose row-clear faulted counts as closed, not as
/// "failed to close", and the sweep still exits nonzero: the money moved and
/// the local record did not follow (#2425).
#[test]
fn a_landed_close_with_a_faulted_clear_counts_as_closed_and_still_fails_the_sweep() {
    let err = grade_local_forget(
        Err(decdn_incentive::StoreError::Backend("no space".into())),
        a_pool(),
        a_tx(),
        RECLAIM_NOTE,
    )
    .expect_err("a faulted clear is an error");
    let mut tally = BatchTally::default();
    assert_eq!(tally.record_error(&err), ErrorClass::LandedUnrecorded);
    assert_eq!(
        tally,
        BatchTally {
            acted: 1,
            unrecorded: 1,
            ..BatchTally::default()
        }
    );
    let msg = batch_result(&tally, "close").unwrap_err().to_string();
    assert!(
        msg.contains("1 pool(s) are no longer Open on-chain but kept a local row"),
        "{msg}"
    );
    assert!(msg.contains("re-run the same command"), "{msg}");
    assert!(!msg.contains("failed to close"), "{msg}");
}

/// Every cause in a mixed sweep reaches the final error.
#[test]
fn batch_result_names_every_cause() {
    let all = BatchTally {
        acted: 1,
        failed: 1,
        unconfirmed: 1,
        unrecorded: 1,
        ..BatchTally::default()
    };
    let msg = batch_result(&all, "close").unwrap_err().to_string();
    assert!(msg.contains("1 pool(s) failed to close"), "{msg}");
    assert!(
        msg.contains("1 pool(s) may have broadcast a close"),
        "{msg}"
    );
    assert!(
        msg.contains("1 pool(s) are no longer Open on-chain"),
        "{msg}"
    );
}

// ---- the escrow stays named during the record write (#2515, #2521) -----

/// An in-memory store that notes, at each pool record and deposit credit,
/// whether `escrows` names [`SCRIPTED_TX`](decdn_incentive::test_rpc::SCRIPTED_TX)
/// as the write runs.
/// A failing one faults every pool record and deposit credit.
struct WatchingStore {
    inner: decdn_incentive::MemoryBuyerPoolStore,
    escrows: UnconfirmedEscrows,
    writes: std::sync::Mutex<Vec<bool>>,
    failing: bool,
}

impl WatchingStore {
    fn new(inner: decdn_incentive::MemoryBuyerPoolStore, escrows: UnconfirmedEscrows) -> Self {
        Self {
            inner,
            escrows,
            writes: std::sync::Mutex::default(),
            failing: false,
        }
    }

    /// A store whose every pool record and deposit credit faults.
    fn failing(inner: decdn_incentive::MemoryBuyerPoolStore, escrows: UnconfirmedEscrows) -> Self {
        Self {
            failing: true,
            ..Self::new(inner, escrows)
        }
    }

    /// The fault a failing write returns.
    fn fault(&self) -> Result<(), decdn_incentive::StoreError> {
        if self.failing {
            return Err(decdn_incentive::StoreError::Backend("disk full".into()));
        }
        Ok(())
    }

    /// Note whether the handle names the scripted tx now.
    fn note_write(&self) {
        use decdn_client::buyer_pool::EscrowUnconfirmed;
        use decdn_incentive::test_rpc::SCRIPTED_TX;

        let pending = self.escrows.pending().iter().any(|escrow| match escrow {
            EscrowUnconfirmed::Open(open) => open.tx == Some(SCRIPTED_TX),
            EscrowUnconfirmed::TopUp(top_up) => top_up.tx == Some(SCRIPTED_TX),
        });
        let untracked = self
            .escrows
            .untracked()
            .iter()
            .any(|held| held.tx == SCRIPTED_TX);
        self.writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(pending || untracked);
    }

    /// For each pool record and deposit credit, whether the handle named the
    /// scripted tx as the write ran.
    fn named_writes(&self) -> Vec<bool> {
        self.writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl BuyerPoolStore for WatchingStore {
    fn load_all(&self) -> Result<BuyerLoad, decdn_incentive::StoreError> {
        self.inner.load_all()
    }
    fn record(&self, state: &BuyerPoolState) -> Result<(), decdn_incentive::StoreError> {
        self.note_write();
        self.fault()?;
        self.inner.record(state)
    }
    fn forget(&self, owner: Address) -> Result<(), decdn_incentive::StoreError> {
        self.inner.forget(owner)
    }
    fn get_by_pool_id(
        &self,
        pool_id: PoolId,
    ) -> Result<Option<BuyerPoolState>, decdn_incentive::StoreError> {
        self.inner.get_by_pool_id(pool_id)
    }
    fn forget_if_pool(
        &self,
        owner: Address,
        pool_id: PoolId,
    ) -> Result<bool, decdn_incentive::StoreError> {
        self.inner.forget_if_pool(owner, pool_id)
    }
    fn get_by_owner(
        &self,
        owner: Address,
    ) -> Result<Option<BuyerPoolState>, decdn_incentive::StoreError> {
        self.inner.get_by_owner(owner)
    }
    fn advance_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: decdn_incentive::LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<decdn_incentive::AdvanceOutcome, decdn_incentive::StoreError> {
        self.inner
            .advance_progress(owner, pool_id, lane, bytes, amount)
    }
    fn rebase_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: decdn_incentive::LaneKey,
        anchor: decdn_incentive::BuyerLaneProgress,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<decdn_incentive::AdvanceOutcome, decdn_incentive::StoreError> {
        self.inner
            .rebase_progress(owner, pool_id, lane, anchor, totals)
    }
    fn seed_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: decdn_incentive::LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<decdn_incentive::AdvanceOutcome, decdn_incentive::StoreError> {
        self.inner
            .seed_progress(owner, pool_id, lane, bytes, amount)
    }
    fn add_deposit(
        &self,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> Result<decdn_incentive::DepositOutcome, decdn_incentive::StoreError> {
        self.note_write();
        self.fault()?;
        self.inner.add_deposit(owner, pool_id, additional)
    }
    fn delegate_lanes(
        &self,
        deployment: Deployment,
        pool_id: PoolId,
        signer: Address,
    ) -> Result<Vec<(Address, decdn_incentive::BuyerLaneProgress)>, decdn_incentive::StoreError>
    {
        self.inner.delegate_lanes(deployment, pool_id, signer)
    }
    fn advance_delegate_progress(
        &self,
        deployment: Deployment,
        lane: decdn_incentive::LaneKey,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<decdn_incentive::AdvanceOutcome, decdn_incentive::StoreError> {
        self.inner
            .advance_delegate_progress(deployment, lane, totals)
    }
    fn rebase_delegate_progress(
        &self,
        deployment: Deployment,
        lane: decdn_incentive::LaneKey,
        anchor: decdn_incentive::BuyerLaneProgress,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<decdn_incentive::AdvanceOutcome, decdn_incentive::StoreError> {
        self.inner
            .rebase_delegate_progress(deployment, lane, anchor, totals)
    }
    fn lane_salt(
        &self,
    ) -> Result<decdn_incentive::lane_signer::LaneSalt, decdn_incentive::StoreError> {
        self.inner.lane_salt()
    }
}

/// A `PaymentPool` at [`DEPLOYMENT`] whose every transaction mines with
/// `receipt`.
fn pool_that_mines(
    receipt: alloy::rpc::types::TransactionReceipt,
) -> PaymentPool::PaymentPoolInstance<impl alloy::providers::Provider + Clone> {
    PaymentPool::new(
        DEPLOYMENT.payment_pool,
        decdn_incentive::test_rpc::ScriptedRpc::mined(receipt).provider(),
    )
}

/// #2521: `pool open` keeps its mined `openPool` named on the handle while
/// the pool record is written, so a second Ctrl-C during the write still
/// names it, and releases it once the record commits.
#[tokio::test(start_paused = true)]
async fn pool_open_names_the_open_while_its_record_is_written() {
    let owner = Address::repeat_byte(0x22);
    let pool_id = B256::repeat_byte(0x33);
    let deposit = U256::from(1_000_000_u64);
    let receipt = mined_open(owner, pool_id, deposit);
    let escrows = UnconfirmedEscrows::default();
    let store = WatchingStore::new(store_with(&[]), escrows.clone());
    let opened = open_and_record(
        &pool_that_mines(receipt),
        &store,
        DEPLOYMENT,
        Address::repeat_byte(0xcd),
        owner,
        deposit,
        &escrows,
    )
    .await
    .expect("the open is recorded");
    assert_eq!(opened, pool_id);
    assert_eq!(
        store.named_writes(),
        [true],
        "the record write names the open"
    );
    assert_eq!(tracked(&store, owner), Some(pool_id));
    assert!(escrows.pending().is_empty(), "the record released the open");
    assert!(escrows.untracked().is_empty());
}

/// #2521: `pool top-up` keeps its mined `topUp` named on the handle while
/// the deposit credit is written, and releases it once the credit commits.
#[tokio::test(start_paused = true)]
async fn pool_top_up_names_the_top_up_while_its_credit_is_written() {
    let owner = Address::repeat_byte(0x22);
    let pool_id = B256::repeat_byte(0x33);
    let escrows = UnconfirmedEscrows::default();
    let store = WatchingStore::new(store_with(&[row(0x33, owner)]), escrows.clone());
    // No `PoolToppedUp` log, so the credit is the amount asked for.
    let receipt = decdn_incentive::test_rpc::mined_receipt(DEPLOYMENT.payment_pool, Vec::new());
    let (credited, deposit) = top_up_and_credit(
        &pool_that_mines(receipt),
        &store,
        owner,
        pool_id,
        U256::from(5_000_u64),
        &escrows,
    )
    .await
    .expect("the topUp is credited");
    assert_eq!(credited, U256::from(5_000_u64));
    assert_eq!(deposit, U256::from(1_005_000_u64));
    assert_eq!(store.named_writes(), [true], "the credit names the topUp");
    assert!(
        escrows.pending().is_empty(),
        "the credit released the topUp"
    );
    assert!(escrows.untracked().is_empty());
}

/// The receipt of an `openPool` that opened `pool_id` for `owner` at
/// `deposit`.
fn mined_open(
    owner: Address,
    pool_id: PoolId,
    deposit: U256,
) -> alloy::rpc::types::TransactionReceipt {
    use alloy::sol_types::SolEvent;

    let opened = PaymentPool::PoolOpened {
        poolId: pool_id,
        owner,
        deposit,
    };
    decdn_incentive::test_rpc::mined_receipt(
        DEPLOYMENT.payment_pool,
        vec![alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: DEPLOYMENT.payment_pool,
                data: opened.encode_log_data(),
            },
            ..Default::default()
        }],
    )
}

/// Assert that `escrows` holds the scripted tx as untracked, and nothing
/// pending: the command names it on every exit.
fn holds_the_escrow_untracked(err: &anyhow::Error, escrows: &UnconfirmedEscrows) {
    assert!(
        err.downcast_ref::<decdn_client::buyer_pool::EscrowUntracked>()
            .is_some(),
        "{err:#}"
    );
    let untracked = escrows.untracked();
    assert!(
        untracked
            .iter()
            .any(|held| held.tx == decdn_incentive::test_rpc::SCRIPTED_TX),
        "{untracked:?}"
    );
    assert!(escrows.pending().is_empty(), "{:?}", escrows.pending());
}

/// #2521: a `pool open` whose record write fails after the `openPool` mined
/// holds the deposit as untracked on the handle, so the command names the tx.
#[tokio::test(start_paused = true)]
async fn pool_open_whose_record_fails_holds_the_escrow_untracked() {
    let owner = Address::repeat_byte(0x22);
    let deposit = U256::from(1_000_000_u64);
    let escrows = UnconfirmedEscrows::default();
    let store = WatchingStore::failing(store_with(&[]), escrows.clone());
    let err = open_and_record(
        &pool_that_mines(mined_open(owner, B256::repeat_byte(0x33), deposit)),
        &store,
        DEPLOYMENT,
        Address::repeat_byte(0xcd),
        owner,
        deposit,
        &escrows,
    )
    .await
    .expect_err("the record fails");
    holds_the_escrow_untracked(&err, &escrows);
    assert_eq!(tracked(&store, owner), None);
}

/// #2521: a `pool top-up` whose credit does not land after the `topUp` mined,
/// on a fault or on a row that no longer tracks the pool, holds the escrow as
/// untracked on the handle.
#[tokio::test(start_paused = true)]
async fn pool_top_up_whose_credit_fails_holds_the_escrow_untracked() {
    let owner = Address::repeat_byte(0x22);
    let pool_id = B256::repeat_byte(0x33);
    for (store_rows, failing) in [(vec![row(0x33, owner)], true), (Vec::new(), false)] {
        let escrows = UnconfirmedEscrows::default();
        let inner = store_with(&store_rows);
        let store = if failing {
            WatchingStore::failing(inner, escrows.clone())
        } else {
            WatchingStore::new(inner, escrows.clone())
        };
        let receipt = decdn_incentive::test_rpc::mined_receipt(DEPLOYMENT.payment_pool, Vec::new());
        let err = top_up_and_credit(
            &pool_that_mines(receipt),
            &store,
            owner,
            pool_id,
            U256::from(5_000_u64),
            &escrows,
        )
        .await
        .expect_err("the credit does not land");
        holds_the_escrow_untracked(&err, &escrows);
    }
}
