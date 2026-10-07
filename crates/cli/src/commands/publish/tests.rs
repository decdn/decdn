use super::*;

#[test]
fn namespace_dry_run_and_submitted_formats() {
    // Dry run: no signer loaded, so no operator line is printed.
    let base = NamespaceOutcome {
        operator: None,
        registry: Address::repeat_byte(0x01),
        status: NamespaceStatus::DryRun,
    };
    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &base, false).unwrap();
    let dry = String::from_utf8(buf).unwrap();
    assert!(dry.contains("status=dry_run submitted=false"), "{dry}");
    assert!(!dry.contains("operator="), "{dry}");
    assert!(!dry.contains("namespace_id="), "{dry}");

    let done = NamespaceOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        status: NamespaceStatus::Created {
            tx: B256::repeat_byte(0x55),
            id: Some(9),
        },
        ..base
    };
    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &done, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("operator=0xcdcd"), "{s}");
    assert!(s.contains("namespace_id=9"), "{s}");
    assert!(s.contains("status=created tx=0x5555"), "{s}");
}

/// The enum makes an illegal pair unrepresentable: a `dry_run: true`
/// alongside a `tx: Some` cannot print `status=dry_run tx=…` or emit
/// `"submitted": true` next to `"status": "dry_run"`. `DryRun` carries no hash,
/// so `tx()` is `None` and text and JSON agree on both fields — which this pins.
#[test]
fn namespace_dry_run_carries_no_tx_and_is_not_submitted() {
    let dry = NamespaceOutcome {
        operator: None,
        registry: Address::repeat_byte(0x01),
        status: NamespaceStatus::DryRun,
    };
    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &dry, false).unwrap();
    let text = String::from_utf8(buf).unwrap();
    assert!(!text.contains("tx=0x"), "{text}");
    assert!(text.contains("status=dry_run submitted=false"), "{text}");

    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &dry, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("dry_run"));
    assert_eq!(v["submitted"], serde_json::json!(false));
    assert!(v["tx"].is_null(), "{v}");
}

/// The state this receipt exists for: `createNamespace` confirmed, so the
/// namespace is live and already counts against the publisher's cap, but its
/// id could not be decoded. The tx hash is then the operator's ONLY handle on
/// it — dropping the receipt here is what would make the id unrecoverable and
/// a retry quota-burning. It is also not a dry run, and must never say so.
///
/// Then walks the other two non-dry-run statuses off the same fixture: a
/// broadcast whose receipt could not be read is `unknown`, and no hash at all
/// is `failed`.
#[test]
fn namespace_output_distinguishes_created_unknown_and_failed() {
    let o = NamespaceOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        registry: Address::repeat_byte(0x01),
        status: NamespaceStatus::Created {
            tx: B256::repeat_byte(0x55),
            id: None,
        },
    };
    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &o, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("status=created tx=0x5555"), "{s}");
    assert!(!s.contains("dry_run"), "{s}");
    assert!(!s.contains("namespace_id="), "{s}");

    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &o, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    // `created` + a null id is what separates this from "not created": a
    // `--json` consumer switching on `status` cannot confuse the two.
    assert_eq!(v["status"], serde_json::json!("created"));
    assert_eq!(v["submitted"], serde_json::json!(true));
    assert!(v["namespace_id"].is_null(), "{v}");
    assert_eq!(
        v["tx"],
        serde_json::json!(format!("{:#x}", B256::repeat_byte(0x55)))
    );

    // A broadcast whose receipt could not be read is UNKNOWN, not created —
    // but it keeps its hash, because a namespace that may exist needs the
    // same handle as one that does.
    let unknown = NamespaceOutcome {
        status: NamespaceStatus::InFlight(B256::repeat_byte(0x55)),
        ..o
    };
    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &unknown, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("unknown"));
    assert_eq!(
        v["tx"],
        serde_json::json!(format!("{:#x}", B256::repeat_byte(0x55)))
    );

    // Nothing reached the chain: `failed`, and distinguishable from a dry run.
    let failed = NamespaceOutcome {
        status: NamespaceStatus::Failed,
        ..o
    };
    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &failed, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("failed"));
    assert_eq!(v["submitted"], serde_json::json!(false));
}

/// A mined revert (#1550) reports `reverted` and keeps its hash; a lost send
/// response (#1577) reports `maybe_broadcast` with a NULL `submitted` and no
/// hash. Neither may collapse into `failed` — the first has a gas-burning tx
/// to reconcile, the second may still mint a namespace against the cap.
#[test]
fn namespace_output_surfaces_reverted_and_maybe_broadcast() {
    let base = NamespaceOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        registry: Address::repeat_byte(0x01),
        status: NamespaceStatus::Reverted(B256::repeat_byte(0x55)),
    };
    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &base, false).unwrap();
    let text = String::from_utf8(buf).unwrap();
    assert!(text.contains("status=reverted tx=0x5555"), "{text}");

    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &base, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("reverted"));
    assert_eq!(v["submitted"], serde_json::json!(true));
    assert_eq!(
        v["tx"],
        serde_json::json!(format!("{:#x}", B256::repeat_byte(0x55)))
    );

    let maybe = NamespaceOutcome {
        status: NamespaceStatus::MaybeBroadcast,
        ..base
    };
    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &maybe, false).unwrap();
    let text = String::from_utf8(buf).unwrap();
    assert!(
        text.contains("status=maybe_broadcast submitted=unknown"),
        "{text}"
    );

    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &maybe, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("maybe_broadcast"));
    assert!(
        v["submitted"].is_null(),
        "submitted must be null, not false: {v}"
    );
    assert!(v["tx"].is_null(), "{v}");
}

/// A `created` with a decoded id prints and emits the id, distinguishing it
/// from the id-less `created` above — the fifth of the five states, and the
/// one whose `namespace_id` a `--json` consumer reads back.
#[test]
fn namespace_created_with_id_emits_the_id() {
    let o = NamespaceOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        registry: Address::repeat_byte(0x01),
        status: NamespaceStatus::Created {
            tx: B256::repeat_byte(0x55),
            id: Some(9),
        },
    };
    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &o, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("namespace_id=9"), "{s}");
    assert!(s.contains("status=created tx=0x5555"), "{s}");

    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &o, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("created"));
    assert_eq!(v["namespace_id"], serde_json::json!(9));
}

/// A `namespace create` failure must only warn about the burnt quota when a
/// namespace was actually minted. Claiming it on the path where the send was
/// rejected — the modal failure, since `NamespaceCapReached` surfaces from
/// the pre-flight gas estimate — sends the publisher hunting for a namespace
/// that does not exist, and contradicts the `status=failed` receipt printed
/// one line above.
#[test]
fn namespace_failure_context_only_claims_a_namespace_when_one_exists() {
    // Broadcast, receipt unreadable: it may exist, so say so.
    let in_flight =
        namespace_failure_context(NamespaceStatus::InFlight(B256::repeat_byte(0x55))).unwrap();
    assert!(in_flight.contains("was broadcast"), "{in_flight}");
    assert!(
        in_flight.contains("maxNamespacesPerPublisher"),
        "{in_flight}"
    );

    // Confirmed, but the id could not be decoded: the namespace IS owned.
    let landed = namespace_failure_context(NamespaceStatus::Created {
        tx: B256::repeat_byte(0x55),
        id: None,
    })
    .unwrap();
    assert!(landed.contains("exists and is owned"), "{landed}");
    // The recovery path has to work for every decode failure, including the
    // one where the receipt came back WITHOUT the log — so it points at an
    // owner-filtered log query (both event params are indexed), not at the
    // receipt's own log, and never at `ownerOf`, which maps id → owner and
    // so needs the id that was just lost.
    assert!(
        landed.contains("NamespaceCreated logs for this signer"),
        "{landed}"
    );
    assert!(!landed.contains("ownerOf"), "{landed}");

    // A lost send response may have minted too, but has no hash to point at,
    // so the note routes the operator to this signer's own transactions.
    let maybe = namespace_failure_context(NamespaceStatus::MaybeBroadcast).unwrap();
    assert!(maybe.contains("may have reached the node"), "{maybe}");
    assert!(maybe.contains("this signer's pending"), "{maybe}");
    assert!(maybe.contains("maxNamespacesPerPublisher"), "{maybe}");

    // Nothing reached the chain — nothing minted, and no hash to cite.
    assert!(
        namespace_failure_context(NamespaceStatus::Failed).is_none(),
        "a rejected send must not claim a created namespace"
    );
    // A confirmed revert minted nothing — the create had no effect.
    assert!(
        namespace_failure_context(NamespaceStatus::Reverted(B256::repeat_byte(0x55))).is_none(),
        "a reverted create minted nothing"
    );
    // A dry run minted nothing either.
    assert!(
        namespace_failure_context(NamespaceStatus::DryRun).is_none(),
        "a dry run must not claim a created namespace"
    );
}

/// A confirmed receipt carrying `logs`.
///
/// The decode helpers read only `inner.logs()`, so every other field is
/// filler. It exists because `TransactionReceipt` has no constructor, and
/// without it the decode failures below are reachable only against a chain
/// whose deployed ABI has drifted from this CLI's — which is to say, never
/// in a test.
fn receipt_with_logs(logs: Vec<alloy::rpc::types::Log>) -> alloy::rpc::types::TransactionReceipt {
    alloy::rpc::types::TransactionReceipt {
        inner: alloy::consensus::ReceiptEnvelope::Eip1559(alloy::consensus::ReceiptWithBloom {
            receipt: alloy::consensus::Receipt {
                status: alloy::consensus::Eip658Value::Eip658(true),
                cumulative_gas_used: 0,
                logs,
            },
            logs_bloom: alloy::primitives::Bloom::ZERO,
        }),
        transaction_hash: B256::repeat_byte(0x55),
        transaction_index: Some(0),
        block_hash: None,
        block_number: None,
        gas_used: 0,
        effective_gas_price: 0,
        blob_gas_used: None,
        blob_gas_price: None,
        from: Address::ZERO,
        to: None,
        contract_address: None,
    }
}

/// A log with `topics` and no data — enough to drive `topic0` matching and
/// the decode that follows it.
fn log_with_topics(topics: Vec<B256>) -> alloy::rpc::types::Log {
    alloy::rpc::types::Log {
        inner: alloy::primitives::Log {
            address: Address::repeat_byte(0x01),
            data: alloy::primitives::LogData::new_unchecked(
                topics,
                alloy::primitives::Bytes::new(),
            ),
        },
        block_hash: None,
        block_number: None,
        block_timestamp: None,
        transaction_hash: None,
        transaction_index: None,
        log_index: None,
        removed: false,
    }
}

/// A `NamespaceCreated` log as the registry actually emits it: both
/// parameters are `indexed`, so the id and the owner arrive as topics and
/// the data is empty.
fn namespace_created_log(id: U256, owner: Address) -> alloy::rpc::types::Log {
    log_with_topics(vec![
        PublisherRegistry::NamespaceCreated::SIGNATURE_HASH,
        B256::from(id),
        owner.into_word(),
    ])
}

#[test]
fn decode_created_namespace_reads_the_id_out_of_the_log() {
    let receipt = receipt_with_logs(vec![namespace_created_log(
        U256::from(9),
        Address::repeat_byte(0xCD),
    )]);
    assert_eq!(decode_created_namespace(&receipt).unwrap(), 9);
}

/// The failure that makes a live namespace's id unrecoverable, and so the
/// one that must reach the operator intact rather than as a bare `?`.
#[test]
fn decode_created_namespace_reports_a_receipt_with_no_matching_log() {
    // A log from some other event: the receipt is not empty, it just does
    // not carry the one being looked for.
    let receipt = receipt_with_logs(vec![log_with_topics(vec![B256::repeat_byte(0xAB)])]);
    let err = format!("{:#}", decode_created_namespace(&receipt).unwrap_err());
    assert!(err.contains("carried no NamespaceCreated"), "{err}");
}

/// `topic0` matches but the payload does not — the case the match-then-decode
/// ordering exists for. Collapsing the two steps into a single
/// `find_map(|l| l.log_decode().ok())` would report this as a missing log and
/// send the operator hunting for an event the chain did emit.
#[test]
fn decode_created_namespace_separates_an_undecodable_log_from_a_missing_one() {
    let receipt = receipt_with_logs(vec![log_with_topics(vec![
        PublisherRegistry::NamespaceCreated::SIGNATURE_HASH,
    ])]);
    let err = format!("{:#}", decode_created_namespace(&receipt).unwrap_err());
    assert!(err.contains("failed to decode"), "{err}");
    assert!(!err.contains("carried no NamespaceCreated"), "{err}");
}

/// Unreachable against this registry — it allocates from a `uint64` counter —
/// so this pins that an impossible id is reported rather than truncated into
/// a plausible one.
#[test]
fn decode_created_namespace_rejects_an_id_that_does_not_fit_u64() {
    let receipt = receipt_with_logs(vec![namespace_created_log(
        U256::from(u64::MAX) + U256::from(1),
        Address::repeat_byte(0xCD),
    )]);
    let err = format!("{:#}", decode_created_namespace(&receipt).unwrap_err());
    assert!(err.contains("exceeds u64"), "{err}");
}

#[test]
fn assign_output_lists_every_seat_with_its_tx() {
    let a = Address::repeat_byte(0x11);
    let b = Address::repeat_byte(0x22);
    let o = AssignOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        origin_assignment: Address::repeat_byte(0x02),
        namespace_id: 7,
        seats: vec![
            (a, SeatOutcome::Seated(B256::repeat_byte(0x55))),
            (b, SeatOutcome::Seated(B256::repeat_byte(0x66))),
        ],
        dry_run: false,
    };
    let mut buf = Vec::new();
    write_assign_outcome(&mut buf, &o, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("operators=2"), "{s}");
    assert!(s.contains("tx=0x5555"), "{s}");
    assert!(s.contains("tx=0x6666"), "{s}");
    assert!(s.contains("status=seated seated=2"), "{s}");
}

/// Seating is one transaction per operator, so a mid-run failure leaves some
/// operators live and the rest not. The receipt must say which — that is the
/// whole reason it is printed before the error propagates.
#[test]
fn assign_output_distinguishes_partial_from_seated() {
    let landed = Address::repeat_byte(0x11);
    let failed = Address::repeat_byte(0x22);
    let untried = Address::repeat_byte(0x33);
    let o = AssignOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        origin_assignment: Address::repeat_byte(0x02),
        namespace_id: 7,
        seats: vec![
            (landed, SeatOutcome::Seated(B256::repeat_byte(0x55))),
            (failed, SeatOutcome::Failed),
            (untried, SeatOutcome::NotAttempted),
        ],
        dry_run: false,
    };
    let mut buf = Vec::new();
    write_assign_outcome(&mut buf, &o, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains(&format!("  operator={landed:#x} tx=")), "{s}");
    assert!(
        s.contains(&format!("  operator={failed:#x} state=failed")),
        "{s}"
    );
    assert!(
        s.contains(&format!("  operator={untried:#x} state=not_attempted")),
        "{s}"
    );
    assert!(s.contains("status=partial seated=1"), "{s}");
}

/// Nothing landed is NOT "partial" — that would read as if a seat exists.
/// This is the modal failure (an unvetted signer), so the status a machine
/// consumer switches on has to be right for it.
#[test]
fn assign_output_reports_failed_when_no_seat_landed() {
    let o = AssignOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        origin_assignment: Address::repeat_byte(0x02),
        namespace_id: 7,
        seats: vec![
            (Address::repeat_byte(0x11), SeatOutcome::Failed),
            (Address::repeat_byte(0x22), SeatOutcome::NotAttempted),
        ],
        dry_run: false,
    };
    let mut buf = Vec::new();
    write_assign_outcome(&mut buf, &o, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("status=failed seated=0"), "{s}");

    let mut buf = Vec::new();
    write_assign_outcome(&mut buf, &o, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("failed"));
    assert_eq!(v["submitted"], serde_json::json!(false));
}

/// A transaction that was broadcast but whose receipt could not be read has
/// an UNKNOWN outcome. Reporting it as reverted is how an operator gets told
/// to re-send a transaction that is still pending, so it gets its own state,
/// keeps its hash, and outranks every other status.
#[test]
fn assign_output_surfaces_an_in_flight_transaction() {
    let landed = Address::repeat_byte(0x11);
    let unknown = Address::repeat_byte(0x22);
    let o = AssignOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        origin_assignment: Address::repeat_byte(0x02),
        namespace_id: 7,
        seats: vec![
            (landed, SeatOutcome::Seated(B256::repeat_byte(0x55))),
            (unknown, SeatOutcome::InFlight(B256::repeat_byte(0x66))),
        ],
        dry_run: false,
    };
    let mut buf = Vec::new();
    write_assign_outcome(&mut buf, &o, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(
        s.contains(&format!("  operator={unknown:#x} tx=0x6666")),
        "the in-flight hash must reach the receipt, not just the error chain: {s}"
    );
    assert!(s.contains("state=in_flight"), "{s}");
    assert!(s.contains("status=unknown"), "{s}");

    let mut buf = Vec::new();
    write_assign_outcome(&mut buf, &o, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("unknown"));
    assert_eq!(v["submitted"], serde_json::json!(true));
    let origins = v["origins"].as_array().unwrap();
    assert_eq!(origins[1]["state"], serde_json::json!("in_flight"));
    assert_eq!(
        origins[1]["tx"],
        serde_json::json!(format!("{:#x}", B256::repeat_byte(0x66)))
    );
}

/// A mined revert (#1550) keeps its hash as `reverted`; a lost send response
/// (#1577) reports `maybe_broadcast` with no hash. Neither collapses into a
/// bare `failed`. `maybe_broadcast` is uncertain, so it drives `status=unknown`.
#[test]
fn assign_seats_distinguish_reverted_and_maybe_broadcast_from_failed() {
    // A run whose only send reverted: the seat cites its hash, and with no
    // uncertain seat the run's status is the definite `failed` (0 seated).
    let reverted_only = AssignOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        origin_assignment: Address::repeat_byte(0x02),
        namespace_id: 7,
        seats: vec![(
            Address::repeat_byte(0x11),
            SeatOutcome::Reverted(B256::repeat_byte(0x66)),
        )],
        dry_run: false,
    };
    let mut buf = Vec::new();
    write_assign_outcome(&mut buf, &reverted_only, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("failed"));
    let origins = v["origins"].as_array().unwrap();
    assert_eq!(origins[0]["state"], serde_json::json!("reverted"));
    assert_eq!(
        origins[0]["tx"],
        serde_json::json!(format!("{:#x}", B256::repeat_byte(0x66))),
        "a mined revert must keep its hash: {v}"
    );

    // A lost send response: uncertain, no hash, so `status=unknown`.
    let maybe = AssignOutcome {
        seats: vec![(Address::repeat_byte(0x22), SeatOutcome::MaybeBroadcast)],
        ..reverted_only
    };
    let mut buf = Vec::new();
    write_assign_outcome(&mut buf, &maybe, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("unknown"));
    let origins = v["origins"].as_array().unwrap();
    assert_eq!(origins[0]["state"], serde_json::json!("maybe_broadcast"));
    assert!(origins[0]["tx"].is_null(), "no hash was captured: {v}");
}

/// `seated` means every requested operator landed. An empty `seats` satisfies
/// `seated_count == len` vacuously, so the zero case has to be answered first
/// or a receipt with no operators at all claims success.
#[test]
fn assign_status_does_not_call_an_empty_run_seated() {
    let empty = AssignOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        origin_assignment: Address::repeat_byte(0x02),
        namespace_id: 7,
        seats: Vec::new(),
        dry_run: false,
    };
    assert_eq!(empty.status(), "failed");
}

#[test]
fn assign_dry_run_omits_signer_and_txs() {
    let o = AssignOutcome {
        operator: None,
        origin_assignment: Address::repeat_byte(0x02),
        namespace_id: 7,
        seats: vec![(Address::repeat_byte(0x11), SeatOutcome::NotAttempted)],
        dry_run: true,
    };
    let mut buf = Vec::new();
    write_assign_outcome(&mut buf, &o, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("status=dry_run submitted=false"), "{s}");
    // The signer `operator=` line sits at column 0; the operator-set lines
    // are indented (`  operator=`). Only the signer line must be absent.
    assert!(!s.lines().any(|l| l.starts_with("operator=")), "{s}");
    assert!(!s.contains("tx="), "{s}");
}

/// The write error must never outrank — and so hide — a chain error the
/// operator has to act on. `| head -1` on a failed run is the real case.
#[test]
fn propagate_prefers_the_chain_error_over_a_broken_pipe() {
    let broken = || io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe");
    let err = propagate(
        Some(anyhow::anyhow!("addOrigin reverted")),
        Some(broken()),
        "w",
        "nothing was sent",
    )
    .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(rendered.contains("addOrigin reverted"), "{rendered}");
    assert!(rendered.contains("could not be written"), "{rendered}");

    // Write failure alone still surfaces, with its own context.
    let err = propagate(
        None,
        Some(broken()),
        "failed to write assign output",
        "nothing was sent",
    )
    .unwrap_err();
    assert!(
        format!("{err:#}").contains("failed to write assign output"),
        "{err:#}"
    );

    assert!(propagate(None, None, "w", "nothing was sent").is_ok());
}

/// A broken pipe must not be able to eat the receipt. Both write-failure arms
/// carry the facts into the error chain, which goes to stderr — a different
/// descriptor, still writable when stdout is a closed pipe or a full disk.
/// The success + broken-pipe arm is the sharp one: the namespace was minted
/// AND its id decoded, and without this the operator learns neither.
#[test]
fn propagate_carries_the_receipt_when_stdout_could_not_take_it() {
    let broken = || io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe");
    let receipt = single_tx_receipt(
        Some("namespace_id=9".to_string()),
        Some(B256::repeat_byte(0x55)),
    );

    let err = propagate(None, Some(broken()), "failed to write output", &receipt).unwrap_err();
    let rendered = format!("{err:#}");
    assert!(rendered.contains("namespace_id=9"), "{rendered}");
    assert!(rendered.contains("tx=0x5555"), "{rendered}");

    // And alongside a chain failure, where the receipt is the only handle on
    // a namespace whose id could not be decoded.
    let err = propagate(
        Some(anyhow::anyhow!("no NamespaceCreated log")),
        Some(broken()),
        "failed to write output",
        &single_tx_receipt(None, Some(B256::repeat_byte(0x55))),
    )
    .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(rendered.contains("no NamespaceCreated log"), "{rendered}");
    assert!(rendered.contains("tx=0x5555"), "{rendered}");
}

#[test]
fn single_tx_receipt_names_only_what_is_known() {
    let hash = B256::repeat_byte(0x55);
    assert_eq!(
        single_tx_receipt(Some("namespace_id=9".to_string()), Some(hash)),
        format!("namespace_id=9 tx={hash:#x}")
    );
    assert_eq!(single_tx_receipt(None, Some(hash)), format!("tx={hash:#x}"));
    // A rejected send: there is no hash, and saying so beats an empty note.
    assert_eq!(single_tx_receipt(None, None), "nothing was sent");
}

#[test]
fn revoke_failure_context_only_warns_once_something_was_broadcast() {
    let in_flight =
        revoke_failure_context(RevokeStatus::InFlight(B256::repeat_byte(0x55))).unwrap();
    assert!(in_flight.contains("was broadcast"), "{in_flight}");
    // The retry's revert reads like a permissions problem, so it has to be
    // named — that is the whole reason this note exists.
    assert!(in_flight.contains("NotAuthorizedOrigin"), "{in_flight}");

    // A lost send response warns too, but points at the signer's own txs
    // since there is no hash to cite.
    let maybe = revoke_failure_context(RevokeStatus::MaybeBroadcast).unwrap();
    assert!(maybe.contains("may have reached the node"), "{maybe}");
    assert!(maybe.contains("NotAuthorizedOrigin"), "{maybe}");

    assert!(
        revoke_failure_context(RevokeStatus::Failed).is_none(),
        "a rejected send unseated nothing and has no hash to cite"
    );
    assert!(
        revoke_failure_context(RevokeStatus::Reverted(B256::repeat_byte(0x55))).is_none(),
        "a confirmed revert unseated nothing"
    );
    assert!(
        revoke_failure_context(RevokeStatus::Revoked(B256::repeat_byte(0x55))).is_none(),
        "a confirmed removal needs no extra warning"
    );
    assert!(
        revoke_failure_context(RevokeStatus::DryRun).is_none(),
        "a dry run sent nothing"
    );
}

#[test]
fn revoke_output_names_the_unseated_operator() {
    let o = RevokeOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        origin_assignment: Address::repeat_byte(0x02),
        namespace_id: 7,
        revoked: Address::repeat_byte(0x11),
        status: RevokeStatus::Revoked(B256::repeat_byte(0x55)),
    };
    let mut buf = Vec::new();
    write_revoke_outcome(&mut buf, &o, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("namespace_id=7"), "{s}");
    assert!(s.contains("revoked_operator=0x1111"), "{s}");
    assert!(s.contains("status=revoked tx=0x5555"), "{s}");

    let dry = RevokeOutcome {
        operator: None,
        status: RevokeStatus::DryRun,
        ..o
    };
    let mut buf = Vec::new();
    write_revoke_outcome(&mut buf, &dry, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("status=dry_run submitted=false"), "{s}");
}

// JSON writers are the machine-consumable contract — round-trip each so a
// renamed key or wrong-shaped value fails loudly.
#[test]
fn namespace_json_round_trips() {
    let dry = NamespaceOutcome {
        operator: None,
        registry: Address::repeat_byte(0x01),
        status: NamespaceStatus::DryRun,
    };
    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &dry, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["submitted"], serde_json::json!(false));
    assert_eq!(v["status"], serde_json::json!("dry_run"));
    assert!(v["tx"].is_null(), "{v}");
    assert!(v["operator"].is_null(), "{v}");
    assert!(v["namespace_id"].is_null(), "{v}");

    let done = NamespaceOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        registry: Address::repeat_byte(0x01),
        status: NamespaceStatus::Created {
            tx: B256::repeat_byte(0x55),
            id: Some(9),
        },
    };
    let mut buf = Vec::new();
    write_namespace_outcome(&mut buf, &done, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["submitted"], serde_json::json!(true));
    assert_eq!(v["status"], serde_json::json!("created"));
    assert_eq!(v["namespace_id"], serde_json::json!(9)); // number, not string
    assert_eq!(
        v["operator"],
        serde_json::json!(format!("{:#x}", done.operator.unwrap()))
    );
}

#[test]
fn assign_json_round_trips() {
    let landed = Address::repeat_byte(0x11);
    let untried = Address::repeat_byte(0x22);
    let o = AssignOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        origin_assignment: Address::repeat_byte(0x02),
        namespace_id: 7,
        seats: vec![
            (landed, SeatOutcome::Seated(B256::repeat_byte(0x55))),
            (untried, SeatOutcome::NotAttempted),
        ],
        dry_run: false,
    };
    let mut buf = Vec::new();
    write_assign_outcome(&mut buf, &o, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("partial"));
    assert_eq!(v["submitted"], serde_json::json!(true));
    assert_eq!(v["operators"].as_array().unwrap().len(), 2);
    let origins = v["origins"].as_array().unwrap();
    // Every requested operator appears, in order, with its state — so a
    // consumer can never mistake "not tried" for "did not happen".
    assert_eq!(origins.len(), 2);
    assert_eq!(
        origins[0]["operator"],
        serde_json::json!(format!("{landed:#x}"))
    );
    assert_eq!(origins[0]["state"], serde_json::json!("seated"));
    assert_eq!(
        origins[0]["tx"],
        serde_json::json!(format!("{:#x}", B256::repeat_byte(0x55)))
    );
    assert_eq!(origins[1]["state"], serde_json::json!("not_attempted"));
    assert!(origins[1]["tx"].is_null(), "{v}");
}

/// A `removeOrigin` that was broadcast without a readable receipt must not
/// print the success label — the operator has to check the hash before
/// retrying, because a retry after a landed removal reverts.
#[test]
fn single_tx_commands_report_an_unreadable_broadcast_as_unknown() {
    let revoke = RevokeOutcome {
        operator: Some(Address::repeat_byte(0xCD)),
        origin_assignment: Address::repeat_byte(0x02),
        namespace_id: 7,
        revoked: Address::repeat_byte(0x11),
        status: RevokeStatus::InFlight(B256::repeat_byte(0x55)),
    };
    let mut buf = Vec::new();
    write_revoke_outcome(&mut buf, &revoke, false).unwrap();
    let text = String::from_utf8(buf).unwrap();
    assert!(text.contains("status=unknown tx=0x5555"), "{text}");
    assert!(!text.contains("status=revoked"), "{text}");

    let mut buf = Vec::new();
    write_revoke_outcome(&mut buf, &revoke, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("unknown"));
    // The hash still reaches the receipt — it is the operator's only handle.
    assert_eq!(
        v["tx"],
        serde_json::json!(format!("{:#x}", B256::repeat_byte(0x55)))
    );
}

/// The label/hash derivation for every `NamespaceStatus` state, walked once
/// so the two renderers can never disagree with each other about a state.
/// `dry_run` and `failed` are distinct, hash-free states the enum keeps apart.
#[test]
fn namespace_status_labels_and_hashes_match_their_states() {
    let hash = B256::repeat_byte(0x55);
    assert_eq!(NamespaceStatus::DryRun.label(), "dry_run");
    assert_eq!(NamespaceStatus::DryRun.tx(), None);
    assert_eq!(NamespaceStatus::DryRun.namespace_id(), None);

    assert_eq!(NamespaceStatus::Failed.label(), "failed");
    assert_eq!(NamespaceStatus::Failed.tx(), None);
    assert_eq!(NamespaceStatus::Failed.namespace_id(), None);

    assert_eq!(NamespaceStatus::InFlight(hash).label(), "unknown");
    assert_eq!(NamespaceStatus::InFlight(hash).tx(), Some(hash));
    // A broadcast whose receipt was unreadable never carries a decoded id.
    assert_eq!(NamespaceStatus::InFlight(hash).namespace_id(), None);

    let created = NamespaceStatus::Created {
        tx: hash,
        id: Some(9),
    };
    assert_eq!(created.label(), "created");
    assert_eq!(created.tx(), Some(hash));
    assert_eq!(created.namespace_id(), Some(9));
    // Confirmed but the id could not be decoded: still `created`, still keeps
    // its hash, but has no id — `Some(id)` therefore implies `created`.
    let created_no_id = NamespaceStatus::Created { tx: hash, id: None };
    assert_eq!(created_no_id.label(), "created");
    assert_eq!(created_no_id.tx(), Some(hash));
    assert_eq!(created_no_id.namespace_id(), None);
}

/// The `RevokeStatus` parallel: a send that never made it on-chain is
/// `failed`, not `dry_run` — the two were indistinguishable while the status
/// was derived from `tx` alone.
#[test]
fn revoke_status_labels_and_hashes_match_their_states() {
    let hash = B256::repeat_byte(0x55);
    assert_eq!(RevokeStatus::DryRun.label(), "dry_run");
    assert_eq!(RevokeStatus::DryRun.tx(), None);

    assert_eq!(RevokeStatus::Failed.label(), "failed");
    assert_eq!(RevokeStatus::Failed.tx(), None);

    assert_eq!(RevokeStatus::InFlight(hash).label(), "unknown");
    assert_eq!(RevokeStatus::InFlight(hash).tx(), Some(hash));

    assert_eq!(RevokeStatus::Revoked(hash).label(), "revoked");
    assert_eq!(RevokeStatus::Revoked(hash).tx(), Some(hash));

    // A mined revert keeps its hash (#1550); a lost send response has none
    // and reports `submitted: null` rather than a definite `false` (#1577).
    assert_eq!(RevokeStatus::Reverted(hash).label(), "reverted");
    assert_eq!(RevokeStatus::Reverted(hash).tx(), Some(hash));
    assert_eq!(RevokeStatus::Reverted(hash).submitted(), Some(true));

    assert_eq!(RevokeStatus::MaybeBroadcast.label(), "maybe_broadcast");
    assert_eq!(RevokeStatus::MaybeBroadcast.tx(), None);
    assert_eq!(RevokeStatus::MaybeBroadcast.submitted(), None);
}

#[test]
fn revoke_json_round_trip() {
    let revoke = RevokeOutcome {
        operator: None,
        origin_assignment: Address::repeat_byte(0x02),
        namespace_id: 7,
        revoked: Address::repeat_byte(0x11),
        status: RevokeStatus::DryRun,
    };
    let mut buf = Vec::new();
    write_revoke_outcome(&mut buf, &revoke, true).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(v["status"], serde_json::json!("dry_run"));
    assert_eq!(v["submitted"], serde_json::json!(false));
    assert!(v["tx"].is_null(), "{v}");
    assert_eq!(
        v["revoked_operator"],
        serde_json::json!(format!("{:#x}", revoke.revoked))
    );
}

#[test]
fn unique_operators_accepts_distinct_rejects_dupes() {
    let a = Address::repeat_byte(0x11);
    let b = Address::repeat_byte(0x22);
    assert!(ensure_unique_operators(&[a, b]).is_ok());
    // Same address, however the user spelled it, parses to one `Address`.
    let err = ensure_unique_operators(&[a, b, a]).unwrap_err().to_string();
    assert!(err.contains("duplicate operator address"), "{err}");
    assert!(err.contains(&format!("{a:#x}")), "{err}");
}

#[test]
fn chain_id_guard_matches_and_mismatches() {
    assert!(chain_id_guard(421_614, 421_614).is_ok());
    let err = chain_id_guard(421_614, 31_337).unwrap_err().to_string();
    assert!(err.contains("421614"), "{err}");
    assert!(err.contains("31337"), "{err}");
}
