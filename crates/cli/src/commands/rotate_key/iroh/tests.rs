use super::*;

/// Build a receipt carrying `logs`, so the gate can be driven without a
/// chain. Only the fields `confirm_bound` reads are meaningful.
fn receipt_with(logs: Vec<alloy::rpc::types::Log>) -> alloy::rpc::types::TransactionReceipt {
    use alloy::consensus::{Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom};

    alloy::rpc::types::TransactionReceipt {
        inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
            receipt: Receipt {
                status: Eip658Value::Eip658(true),
                cumulative_gas_used: 0,
                logs,
            },
            logs_bloom: alloy::primitives::Bloom::ZERO,
        }),
        transaction_hash: B256::repeat_byte(0xAB),
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

/// A `NodeIdBound` log as the contract would emit it.
fn bound_log(emitter: Address, operator: Address, node_id: B256) -> alloy::rpc::types::Log {
    use alloy::sol_types::SolEvent as _;

    let event = CapacityBond::NodeIdBound {
        ethAddress: operator,
        nodeId: node_id,
        bindingNonce: 3,
    };
    alloy::rpc::types::Log {
        inner: alloy::primitives::Log {
            address: emitter,
            data: event.encode_log_data(),
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

const CB: Address = Address::repeat_byte(0xCB);
const OP: Address = Address::repeat_byte(0x0E);
const NEW_ID: B256 = B256::repeat_byte(0x22);

/// The gate's whole purpose: a receipt that mined WITHOUT the binding event
/// must not green-light replacing `node.secret`.
#[test]
fn confirm_bound_rejects_a_receipt_with_no_log() {
    confirm_bound(&receipt_with(Vec::new()), OP, NEW_ID, CB)
        .expect_err("a mined receipt with no NodeIdBound must not confirm");
}

#[test]
fn confirm_bound_accepts_the_matching_log() {
    confirm_bound(
        &receipt_with(vec![bound_log(CB, OP, NEW_ID)]),
        OP,
        NEW_ID,
        CB,
    )
    .expect("the exact binding this run submitted must confirm");
}

/// Matching the event signature alone is not enough — this is what separates
/// a real gate from a ceremonial one. A regression relaxing the operator or
/// node-id check would install a key the chain does not point at.
#[test]
fn confirm_bound_rejects_a_log_for_another_operator() {
    let other = Address::repeat_byte(0x99);
    confirm_bound(
        &receipt_with(vec![bound_log(CB, other, NEW_ID)]),
        OP,
        NEW_ID,
        CB,
    )
    .expect_err("a NodeIdBound for a different operator is not evidence for this one");
}

#[test]
fn confirm_bound_rejects_a_log_for_another_node_id() {
    let other_id = B256::repeat_byte(0x77);
    confirm_bound(
        &receipt_with(vec![bound_log(CB, OP, other_id)]),
        OP,
        NEW_ID,
        CB,
    )
    .expect_err("a NodeIdBound for a different node id must not confirm this rotation");
}

/// The emitting contract is part of the claim: a well-formed event from some
/// other address says nothing about THIS deployment's registry.
#[test]
fn confirm_bound_rejects_a_log_from_a_foreign_contract() {
    let impostor = Address::repeat_byte(0x01);
    confirm_bound(
        &receipt_with(vec![bound_log(impostor, OP, NEW_ID)]),
        OP,
        NEW_ID,
        CB,
    )
    .expect_err("a NodeIdBound from another contract is not evidence about CapacityBond");
}

/// The real receipt shape: the matching log sits among unrelated ones.
#[test]
fn confirm_bound_finds_the_match_among_other_logs() {
    let logs = vec![
        bound_log(CB, Address::repeat_byte(0x99), NEW_ID),
        bound_log(CB, OP, NEW_ID),
    ];
    confirm_bound(&receipt_with(logs), OP, NEW_ID, CB)
        .expect("a matching log must be found even when others precede it");
}

/// `stage_node_key` enforces a `0o700` data dir, and `tempfile` honours the
/// ambient umask (commonly `0o755`), so tests must tighten it first.
fn secure_tempdir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("scratch dir");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("chmod 0700");
    }
    dir
}

/// The decision that governs whether irreplaceable key material survives.
/// Inverting it — or letting a future `SendOutcome` variant drift into
/// `maybe_effected` — silently destroys the key for a real broadcast.
#[test]
fn park_preserves_the_key_exactly_when_the_send_may_have_landed() {
    let h = B256::repeat_byte(0xAB);
    let park = [
        SendOutcome::MaybeBroadcast,
        SendOutcome::InFlight(h),
        SendOutcome::Confirmed(h),
    ];
    let discard = [
        SendOutcome::NotSent,
        SendOutcome::Rejected,
        SendOutcome::Reverted(h),
    ];

    for outcome in park {
        let tmp = secure_tempdir();
        let mut key = NewKey::Staged(Box::new(
            identity::stage_node_key(tmp.path()).expect("stage"),
        ));
        let parked = park_key_if_effected(&mut key, &outcome, &plan(true, true));
        assert!(
            parked.as_deref().is_some_and(Path::exists),
            "{outcome:?} may have landed; the key must be parked and on disk"
        );
    }

    for outcome in discard {
        let tmp = secure_tempdir();
        let mut key = NewKey::Staged(Box::new(
            identity::stage_node_key(tmp.path()).expect("stage"),
        ));
        assert!(
            park_key_if_effected(&mut key, &outcome, &plan(true, true)).is_none(),
            "{outcome:?} definitively did not take effect; the key is worthless"
        );
    }
}

/// Both non-staged arms have nothing to preserve, for different reasons —
/// and neither is a failure.
#[test]
fn park_is_a_noop_for_a_key_that_was_never_staged() {
    let mut existing = NewKey::Existing(Box::new(identity::fresh_secret_key()));
    assert!(existing.park().expect("no-op").is_none());
    let mut preview = NewKey::Preview(Box::new(identity::fresh_secret_key()));
    assert!(preview.park().expect("no-op").is_none());
}

/// A `--dry-run` preview of a fresh key must not create `data_dir`, let
/// alone write into it — the bug the FS side effect was: `stage_node_key`
/// eagerly creates the directory via `ensure_data_dir` even though its
/// temp file is removed on drop, so a preview against a data dir that
/// does not exist yet would otherwise leave it behind anyway.
#[test]
fn dry_run_acquire_creates_no_data_dir() {
    let tmp = tempfile::tempdir().expect("make a scratch dir");
    let data_dir = tmp.path().join("does-not-exist-yet");
    assert!(!data_dir.exists());

    let key = NewKey::acquire(&data_dir, false, true).expect("preview key must build");
    assert!(
        !data_dir.exists(),
        "a --dry-run preview must not create the data dir"
    );
    assert!(matches!(key, NewKey::Preview(_)));
}

/// Same guarantee restated at the type level: a preview can sign (the
/// receipt needs a real ownership proof) but must refuse to commit,
/// because nothing should ever call `commit` on one.
#[test]
fn preview_key_refuses_to_commit() {
    let mut key = NewKey::Preview(Box::new(identity::fresh_secret_key()));
    // Signing must still work — a dry run's printed signatures are real.
    let _ = key.sign(b"digest");
    key.commit().expect_err("a preview must never be committed");
}

/// `--bind-existing` is unaffected by `dry_run`: it only ever loads, and
/// `NewKey::acquire`'s existence check runs regardless.
#[test]
fn bind_existing_dry_run_still_requires_an_existing_key() {
    let tmp = tempfile::tempdir().expect("make a scratch dir");
    let result = NewKey::acquire(tmp.path(), true, true);
    let err = result
        .err()
        .expect("bind-existing with no key on disk must fail, dry-run or not");
    assert!(format!("{err}").contains("--bind-existing needs a node key"));
}

fn plan(generated: bool, active: bool) -> Plan {
    Plan {
        capacity_bond: Address::repeat_byte(0xCB),
        operator: Address::repeat_byte(0x0E),
        chain_id: 31337,
        old_node_id: B256::repeat_byte(0x11),
        active,
        new_node_id: B256::repeat_byte(0x22),
        generated,
        binding_nonce: 3,
        registration_nonce: 0,
        binding_sig: vec![0xAA; 65],
        ed25519_sig: vec![0xBB; 64],
    }
}

/// The happy-path shape: a run that reached a transaction persisted its key,
/// a preview did not. [`rendered_unpersisted`] covers the third case.
fn rendered(p: &Plan, json: bool, tx: Option<&B256>, dry_run: bool) -> String {
    rendered_with_persist(p, json, tx, dry_run, tx.is_some() && !dry_run)
}

/// A real run that reached the chain but did NOT end up installing the key —
/// the failed-send and failed-gate paths.
fn rendered_unpersisted(p: &Plan, tx: Option<&B256>) -> String {
    rendered_with_persist(p, false, tx, false, false)
}

fn rendered_with_persist(
    p: &Plan,
    json: bool,
    tx: Option<&B256>,
    dry_run: bool,
    key_persisted: bool,
) -> String {
    let mut buf = Vec::new();
    write_plan(
        &mut buf,
        p,
        json,
        &RunOutcome {
            tx,
            dry_run,
            key_installed: key_persisted,
            ..RunOutcome::default()
        },
    )
    .expect("write to a Vec cannot fail");
    String::from_utf8(buf).expect("output is ASCII")
}

fn disclosure(p: &Plan) -> String {
    let mut buf = Vec::new();
    write_disclosure(&mut buf, p).expect("write to a Vec cannot fail");
    String::from_utf8(buf).expect("output is ASCII")
}

fn args(key: cli::RotateKeyTarget) -> cli::RotateKeyArgs {
    cli::RotateKeyArgs {
        key,
        bind_existing: false,
        new_keystore: None,
        mbps: None,
        region: None,
        multiaddrs: Vec::new(),
        accept_terms: false,
        yes: false,
        chain: cli::ChainArgs {
            common: cli::CommonChainArgs {
                config: None,
                rpc_url: None,
                chain_id: None,
                keystore: None,
                data_dir: None,
                keystore_password_file: None,
                dry_run: false,
                json: false,
            },
            capacity_bond_address: None,
        },
    }
}

/// The reassurance is the disclosure's job — an operator who believes
/// rotating costs them their bond or their open payment pools will not rotate a
/// compromised key, which is strictly worse than rotating one.
#[test]
fn disclosure_says_the_bond_and_pools_survive() {
    let s = disclosure(&plan(true, true));
    assert!(s.contains("NOT affected"), "{s}");
    assert!(s.contains("still settle"), "names the voucher case: {s}");
    assert!(
        s.contains("no unslashable window"),
        "the atomicity is the point: {s}"
    );
}

/// The challenger-side hazard is not in the runbook and not enforced
/// anywhere in the CLI, so the disclosure is the only place an operator
/// with evidence in flight can learn it.
#[test]
fn disclosure_warns_about_evidence_citing_the_old_node_id() {
    let s = disclosure(&plan(true, true));
    assert!(s.contains("re-submitted against"), "{s}");
}

/// The archive note describes an effect that only happens on the generating
/// path; under `--bind-existing` nothing is moved aside, and claiming
/// otherwise would send an operator hunting for a file that is not there.
#[test]
fn disclosure_omits_the_archive_note_when_binding_an_existing_key() {
    let generated = disclosure(&plan(true, true));
    assert!(generated.contains("archived"), "{generated}");
    let existing = disclosure(&plan(false, true));
    assert!(!existing.contains("archived"), "{existing}");
}

/// `bindNodeId` only patches the registration record for an ACTIVE
/// operator, so an inactive one ends up with a binding and a record that
/// disagree. Silence there is how that becomes a mystery later.
#[test]
fn disclosure_flags_an_inactive_registration() {
    assert!(disclosure(&plan(true, false)).contains("not currently in the active set"));
    assert!(!disclosure(&plan(true, true)).contains("not currently in the active set"));
}

#[test]
fn dry_run_reports_no_tx_and_is_distinguishable_from_a_failed_send() {
    let dry = rendered(&plan(true, true), false, None, true);
    assert!(dry.contains("bind_tx=skipped"), "{dry}");
    assert!(dry.contains("submitted=false dry_run=true"), "{dry}");

    let failed = rendered(&plan(true, true), false, None, false);
    assert!(
        failed.contains("submitted=false dry_run=false"),
        "a failed send is not a preview: {failed}"
    );
}

/// A previewed generated key is discarded on drop, so the id it names is
/// not the id a later real run binds. Reporting it without that caveat
/// invites an operator to pre-authorize the wrong id somewhere.
#[test]
fn preview_key_is_flagged_only_when_a_generated_key_was_discarded() {
    assert!(rendered(&plan(true, true), false, None, true).contains("preview_key=true"));
    // `--bind-existing` previews a key that really is on disk.
    assert!(rendered(&plan(false, true), false, None, true).contains("preview_key=false"));
    // A committed key is not a preview.
    let tx = B256::repeat_byte(0xAB);
    assert!(rendered(&plan(true, true), false, Some(&tx), false).contains("preview_key=false"));
}

/// The regression this flag's derivation was changed for: a REAL run whose
/// send failed also leaves the generated key unpersisted. Keying
/// `preview_key` on `dry_run` reported `false` there — asserting the printed
/// `new_node_id` named a key on disk at exactly the moment it named one that
/// had just been discarded or parked.
#[test]
fn a_failed_real_run_flags_its_key_as_not_persisted() {
    // Send never landed: no tx, not a dry run.
    assert!(rendered_unpersisted(&plan(true, true), None).contains("preview_key=true"));
    // Broadcast, but the key was parked rather than installed.
    let tx = B256::repeat_byte(0xAB);
    assert!(rendered_unpersisted(&plan(true, true), Some(&tx)).contains("preview_key=true"));
    // `--bind-existing` is never a preview — the key is already `node.secret`
    // regardless of how the transaction went.
    assert!(rendered_unpersisted(&plan(false, true), Some(&tx)).contains("preview_key=false"));
}

#[test]
fn json_carries_both_ids_and_both_signatures() {
    let tx = B256::repeat_byte(0xAB);
    let s = rendered(&plan(true, true), true, Some(&tx), false);
    let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
    assert_eq!(
        v.get("key").and_then(serde_json::Value::as_str),
        Some("iroh")
    );
    assert_eq!(
        v.get("submitted").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        v.get("old_node_id").and_then(serde_json::Value::as_str),
        Some(format!("{:#x}", B256::repeat_byte(0x11)).as_str())
    );
    assert_eq!(
        v.get("new_node_id").and_then(serde_json::Value::as_str),
        Some(format!("{:#x}", B256::repeat_byte(0x22)).as_str())
    );
    assert!(
        v.get("ed25519_sig")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|s| s.starts_with("0xbbbb")),
        "{s}"
    );
    assert_eq!(
        v.get("bind_tx").and_then(serde_json::Value::as_str),
        Some(format!("{tx:#x}").as_str())
    );
}

#[test]
fn json_null_tx_and_archive_on_a_dry_run() {
    let s = rendered(&plan(true, true), true, None, true);
    let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
    assert!(
        v.get("bind_tx").is_some_and(serde_json::Value::is_null),
        "the key is present-and-null, not absent: {s}"
    );
    assert!(
        v.get("archived_key")
            .is_some_and(serde_json::Value::is_null),
        "a preview archives nothing: {s}"
    );
}

/// Ignoring an eth-only flag here would let an operator believe a tier or a
/// terms acceptance was applied by a command that does neither.
#[test]
fn eth_only_flags_are_refused_on_the_iroh_path() {
    let mut a = args(cli::RotateKeyTarget::Iroh);
    a.mbps = Some(1000);
    a.accept_terms = true;
    let err = reject_eth_only_flags(&a).expect_err("eth-only flags must not be ignored");
    let msg = format!("{err}");
    assert!(msg.contains("--mbps"), "{msg}");
    assert!(msg.contains("--accept-terms"), "{msg}");
    assert!(msg.contains("--key eth"), "names where they belong: {msg}");

    reject_eth_only_flags(&args(cli::RotateKeyTarget::Iroh))
        .expect("a bare iroh rotation is legal");
}
