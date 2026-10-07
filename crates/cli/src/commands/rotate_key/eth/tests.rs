use super::*;

fn plan(phase: Phase) -> Plan {
    Plan {
        capacity_bond: Address::repeat_byte(0xCB),
        old_operator: Address::repeat_byte(0x0E),
        chain_id: 31337,
        phase,
        active_bond: U256::from(50_000u64),
        unbonding_period: 14 * 86_400,
        first_bonded_at: 1_700_000_000,
    }
}

fn rendered(p: &Plan, json: bool, o: &Outcome, dry_run: bool) -> String {
    let mut buf = Vec::new();
    write_plan(&mut buf, p, json, o, dry_run).expect("write to a Vec cannot fail");
    String::from_utf8(buf).expect("output is ASCII")
}

fn disclosure(p: &Plan) -> String {
    let mut buf = Vec::new();
    write_disclosure(&mut buf, p).expect("write to a Vec cannot fail");
    String::from_utf8(buf).expect("output is ASCII")
}

fn reonboard_phase() -> Phase {
    Phase::Reonboard {
        new_operator: Address::repeat_byte(0x0F),
        mbps: 5_000,
        region: "DE".to_string(),
        multiaddrs: vec!["/ip4/203.0.113.10/udp/4433/quic-v1".to_string()],
    }
}

/// An unbound key is exactly the state a prior `reonboard` attempt leaves
/// behind when its `registerNode` never landed — this is the resumability
/// path the module doc promises.
#[test]
fn unbound_key_is_reusable() {
    assert!(key_is_reusable(Address::ZERO));
}

/// Bound to ANYONE — not just a different operator — must refuse reuse.
/// The realistic case is the OLD operator: `deregisterNode` never clears
/// `nodeIdToAddress`, so the pre-migration key reads as "claimed" here,
/// which is what forces a fresh mint on the very first `reonboard` call
/// rather than an attempt to re-register the old identity.
#[test]
fn a_key_bound_to_anyone_is_not_reusable() {
    assert!(!key_is_reusable(Address::repeat_byte(0xAB)));
}

/// The bug both reviewers found: reusing a key bound to someone else
/// would revert `NodeIdAlreadyBound` at best; the point of the guard is
/// that a retry never attempts it.
#[test]
fn same_address_migration_is_refused() {
    let addr = Address::repeat_byte(0xAA);
    let err = ensure_distinct_operator(addr, addr).expect_err("same address must be refused");
    assert!(
        format!("{err}").contains("SAME address"),
        "names the mistake: {err}"
    );
}

#[test]
fn distinct_address_migration_is_allowed() {
    ensure_distinct_operator(Address::repeat_byte(0xAA), Address::repeat_byte(0xBB))
        .expect("a genuinely different address is the whole point of this path");
}

/// The guard behind the confirmation prompt. Its message shipped once with
/// mangled whitespace precisely because nothing executed this branch.
#[test]
fn a_keystore_that_changed_after_the_plan_is_refused() {
    let planned = Address::repeat_byte(0xAA);
    let swapped = Address::repeat_byte(0xBB);
    let err = ensure_planned_operator(planned, swapped)
        .expect_err("an address the operator never saw must not transact");
    let msg = format!("{err}");
    assert!(msg.contains("planned and disclosed"), "{msg}");
    assert!(msg.contains("Nothing was submitted"), "{msg}");
    // The whitespace regression this test exists to catch.
    assert!(
        !msg.contains("  "),
        "message has collapsed indentation: {msg}"
    );
}

#[test]
fn the_planned_keystore_is_accepted() {
    let a = Address::repeat_byte(0xAA);
    ensure_planned_operator(a, a).expect("the disclosed address must proceed");
}

/// Every transaction slot must count toward `submitted` — a landed
/// `approve` reported next to `submitted=false` is a standing ERC-20
/// allowance the operator is told was never granted.
#[test]
fn submitted_is_true_for_every_transaction_slot() {
    let h = B256::repeat_byte(0xAB);
    let slots = [
        (
            "deregister",
            Outcome {
                deregister: Some(h),
                ..Outcome::default()
            },
        ),
        (
            "request",
            Outcome {
                request: Some(h),
                ..Outcome::default()
            },
        ),
        (
            "withdraw",
            Outcome {
                withdraw: Some(h),
                ..Outcome::default()
            },
        ),
        (
            "approve",
            Outcome {
                approve: Some(h),
                ..Outcome::default()
            },
        ),
        (
            "bond",
            Outcome {
                bond: Some(h),
                ..Outcome::default()
            },
        ),
        (
            "declare",
            Outcome {
                declare: Some(h),
                ..Outcome::default()
            },
        ),
        (
            "register",
            Outcome {
                register: Some(h),
                ..Outcome::default()
            },
        ),
    ];
    for (name, o) in slots {
        let s = rendered(&plan(reonboard_phase()), false, &o, false);
        assert!(
            s.contains("submitted=true"),
            "a landed {name} must count as submitted: {s}"
        );
    }
}

/// The opposite direction, and the more dangerous one: `new_node_id` is set
/// BEFORE anything is sent, so counting it would make a polling wrapper mark
/// a run that submitted nothing as done.
#[test]
fn a_minted_node_id_alone_is_not_a_submission() {
    let o = Outcome {
        new_node_id: Some(B256::repeat_byte(0xCD)),
        ..Outcome::default()
    };
    let s = rendered(&plan(reonboard_phase()), false, &o, false);
    assert!(
        s.contains("submitted=false"),
        "an identity is not a tx: {s}"
    );
}

/// The tier is destroyed by the call being confirmed and cannot be read
/// back, so the disclosure is the operator's only chance to record it.
#[test]
fn deregister_disclosure_prints_the_tier_to_carry_forward() {
    let s = disclosure(&plan(Phase::Deregister {
        declared_mbps: 5_000,
    }));
    assert!(s.contains("RECORD THIS"), "{s}");
    assert!(
        s.contains("--mbps 5000"),
        "names the exact flag to re-pass: {s}"
    );
    assert!(
        s.contains("NOT returned"),
        "the bond does not move here: {s}"
    );
}

/// The node id changing is the surprise of this path, and the workaround is
/// non-obvious enough that omitting it would strand anyone who cares about
/// reputation continuity.
#[test]
fn reonboard_disclosure_names_the_fresh_node_id_and_the_carry_over_route() {
    let s = disclosure(&plan(reonboard_phase()));
    assert!(s.contains("FRESH node id"), "{s}");
    assert!(s.contains("--key iroh"), "names the carry-over route: {s}");
    assert!(
        s.contains("firstBondedAt resets"),
        "names the real cost: {s}"
    );
    assert!(
        s.contains("voucherSigner"),
        "the old keystore retention obligation is easy to miss: {s}"
    );
}

/// Prompting on a phase that only returns the operator's own money — or
/// submits nothing at all — trains reflexive confirmation, which is how the
/// prompts that matter stop being read.
#[test]
fn only_the_costly_phases_prompt() {
    assert!(Phase::Deregister { declared_mbps: 1 }.needs_confirmation());
    assert!(
        Phase::Request {
            amount: U256::from(1u64)
        }
        .needs_confirmation()
    );
    assert!(reonboard_phase().needs_confirmation());

    assert!(
        !Phase::Withdraw {
            amount: U256::from(1u64)
        }
        .needs_confirmation()
    );
    assert!(
        !Phase::Waiting {
            amount: U256::from(1u64),
            unlock_at: 10,
            now: 1
        }
        .needs_confirmation()
    );
    assert!(
        !Phase::Complete {
            new_operator: Address::ZERO
        }
        .needs_confirmation()
    );
}

#[test]
fn waiting_reports_the_remaining_window_and_submits_nothing() {
    let s = rendered(
        &plan(Phase::Waiting {
            amount: U256::from(50_000u64),
            unlock_at: 1_000_000 + 3 * 86_400,
            now: 1_000_000,
        }),
        false,
        &Outcome::default(),
        false,
    );
    assert!(s.contains("phase=waiting"), "{s}");
    assert!(s.contains(&format!("remaining_secs={}", 3 * 86_400)), "{s}");
    assert!(s.contains("submitted=false"), "{s}");
}

/// `remaining_secs` describes the waiting phase only; a non-zero value on
/// any other phase would read as "still blocked" to a polling wrapper.
#[test]
fn remaining_secs_is_zero_off_the_waiting_phase() {
    for phase in [
        Phase::Deregister { declared_mbps: 1 },
        Phase::Withdraw {
            amount: U256::from(1u64),
        },
        reonboard_phase(),
    ] {
        let s = rendered(&plan(phase), false, &Outcome::default(), false);
        assert!(s.contains("remaining_secs=0"), "{s}");
    }
}

#[test]
fn json_carries_the_phase_and_every_tx_slot() {
    let outcome = Outcome {
        withdraw: Some(B256::repeat_byte(0xAB)),
        ..Outcome::default()
    };
    let s = rendered(
        &plan(Phase::Withdraw {
            amount: U256::from(50_000u64),
        }),
        true,
        &outcome,
        false,
    );
    let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
    assert_eq!(
        v.get("key").and_then(serde_json::Value::as_str),
        Some("eth")
    );
    assert_eq!(
        v.get("phase").and_then(serde_json::Value::as_str),
        Some("withdraw")
    );
    assert_eq!(
        v.get("submitted").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        v.get("withdraw_tx").and_then(serde_json::Value::as_str),
        Some(format!("{:#x}", B256::repeat_byte(0xAB)).as_str())
    );
    assert!(
        v.get("register_tx").is_some_and(serde_json::Value::is_null),
        "unreached steps are present-and-null, not absent: {s}"
    );
    assert_eq!(
        v.get("active_bond_base")
            .and_then(serde_json::Value::as_str),
        Some("50000"),
        "base units are a decimal STRING — 1e18-scaled values overflow a JSON number"
    );
}

/// A withdrawal that only returns money still has to be distinguishable
/// from a preview, the way every other command's receipt is.
#[test]
fn dry_run_is_distinguishable_from_a_failed_send() {
    let dry = rendered(
        &plan(Phase::Withdraw {
            amount: U256::from(1u64),
        }),
        false,
        &Outcome::default(),
        true,
    );
    assert!(dry.contains("submitted=false dry_run=true"), "{dry}");
    let failed = rendered(
        &plan(Phase::Withdraw {
            amount: U256::from(1u64),
        }),
        false,
        &Outcome::default(),
        false,
    );
    assert!(failed.contains("submitted=false dry_run=false"), "{failed}");
}
