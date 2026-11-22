use super::*;

fn plan(phase: Phase) -> Plan {
    Plan {
        capacity_bond: Address::repeat_byte(0xCB),
        old_operator: Address::repeat_byte(0x0E),
        chain_id: 31337,
        phase,
        active_bond: U256::from(50_000u64),
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
            "unbond",
            Outcome {
                unbond: Some(h),
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
    assert!(
        s.contains("clears this address's firstBondedAt (1700000000)"),
        "the tenure loss is disclosed at the first confirmation: {s}"
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

/// Every phase that submits asks first, the unbond phase included: the
/// whole-bond `unbond` ends the tenure (it clears `firstBondedAt` and the
/// activation floor) whether or not it clears a tier. `Complete` submits
/// nothing, and prompting there trains reflexive confirmation.
#[test]
fn every_submitting_phase_prompts() {
    assert!(Phase::Deregister { declared_mbps: 1 }.needs_confirmation());
    assert!(reonboard_phase().needs_confirmation());
    for clear_tier in [Some(5_000), None] {
        assert!(
            Phase::Unbond {
                amount: U256::from(1u64),
                clear_tier,
            }
            .needs_confirmation(),
            "{clear_tier:?}"
        );
    }

    assert!(
        !Phase::Complete {
            new_operator: Address::ZERO
        }
        .needs_confirmation()
    );
}

/// An operator that never deregistered sees no other confirmation before
/// its tier and its tenure go, so this disclosure names both.
#[test]
fn unbond_disclosure_records_the_tier_and_the_tenure_loss() {
    let s = disclosure(&plan(Phase::Unbond {
        amount: U256::from(50_000u64),
        clear_tier: Some(5_000),
    }));
    assert!(s.contains("50000 base units"), "{s}");
    assert!(
        s.contains(
            "RECORD THIS: the declared tier is 5000 Mbps; pass --mbps 5000 at re-onboarding"
        ),
        "{s}"
    );
    assert!(s.contains("declareMbps(0)"), "{s}");
    assert!(
        s.contains("clears firstBondedAt (1700000000)"),
        "the tenure loss is disclosed: {s}"
    );
    assert!(s.contains("`age_ramp_months` (6 by default)"), "{s}");
}

/// A deregistered operator has no tier left to record, but the whole-bond
/// unbond still ends its tenure.
#[test]
fn unbond_disclosure_without_a_tier_still_names_the_tenure_loss() {
    let s = disclosure(&plan(Phase::Unbond {
        amount: U256::from(50_000u64),
        clear_tier: None,
    }));
    assert!(!s.contains("RECORD THIS"), "{s}");
    assert!(!s.contains("declareMbps(0)"), "{s}");
    assert!(s.contains("clears firstBondedAt (1700000000)"), "{s}");

    let mut no_tenure = plan(Phase::Unbond {
        amount: U256::from(50_000u64),
        clear_tier: None,
    });
    no_tenure.first_bonded_at = 0;
    let s = disclosure(&no_tenure);
    assert!(s.contains("holds no tenure"), "{s}");
    assert!(!s.contains("clears firstBondedAt"), "{s}");
}

/// A receipt write that fails after a failed operation degrades to stderr,
/// and the cleared tier goes with it: the receipt is the only other place
/// it is written.
#[test]
fn a_lost_receipt_after_a_failed_unbond_keeps_the_tier_on_stderr() {
    let p = plan(Phase::Unbond {
        amount: U256::from(50_000u64),
        clear_tier: Some(5_000),
    });
    let mut stderr = Vec::new();
    let err = settle_receipt(
        &p,
        Err(anyhow::anyhow!("unbond reverted")),
        Err(io::Error::from(io::ErrorKind::BrokenPipe)),
        &mut stderr,
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "unbond reverted",
        "the operation error outranks the receipt"
    );
    let stderr = String::from_utf8(stderr).unwrap();
    assert!(
        stderr.contains("failed to write the result receipt"),
        "{stderr}"
    );
    assert!(stderr.contains("--mbps 5000"), "{stderr}");
}

/// A receipt write that fails after a completed unbond fails the run, and
/// its error carries the cleared tier.
#[test]
fn a_lost_receipt_after_a_completed_unbond_names_the_tier_in_the_error() {
    let p = plan(Phase::Unbond {
        amount: U256::from(50_000u64),
        clear_tier: Some(5_000),
    });
    let mut stderr = Vec::new();
    let err = settle_receipt(
        &p,
        Ok(()),
        Err(io::Error::from(io::ErrorKind::BrokenPipe)),
        &mut stderr,
    )
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("receipt could not be written"), "{msg}");
    assert!(
        msg.contains("RECORD THIS: the declared tier is 5000 Mbps; pass --mbps 5000"),
        "{msg}"
    );

    // No tier to clear: the error carries none.
    let p = plan(Phase::Unbond {
        amount: U256::from(50_000u64),
        clear_tier: None,
    });
    let err = settle_receipt(
        &p,
        Ok(()),
        Err(io::Error::from(io::ErrorKind::BrokenPipe)),
        &mut stderr,
    )
    .unwrap_err();
    assert!(!format!("{err:#}").contains("RECORD THIS"), "{err:#}");
}

/// For an operator that never deregistered, `declareMbps(0)` goes out before
/// `unbond`: the reverse order reverts `BondBelowCurve` against the standing
/// tier. Both sends land in the outcome.
#[tokio::test]
async fn the_unbond_phase_clears_the_tier_before_it_withdraws() {
    use alloy::sol_types::SolCall;
    use decdn_incentive::test_rpc::{ScriptedRpc, mined_receipt};

    let cb = Address::repeat_byte(0xCB);
    let rpc = ScriptedRpc::mined(mined_receipt(cb, Vec::new()));
    let bond_contract = CapacityBond::new(cb, rpc.clone().provider());
    let amount = U256::from(50_000u64);
    let mut outcome = Outcome::default();
    unbond_whole_bond(&bond_contract, amount, true, &mut outcome)
        .await
        .unwrap();

    let sent = rpc.sent_inputs();
    let expected = [
        CapacityBond::declareMbpsCall { mbps: U256::ZERO }.abi_encode(),
        CapacityBond::unbondCall { amount }.abi_encode(),
    ];
    assert_eq!(
        sent.iter().map(|b| b.to_vec()).collect::<Vec<_>>(),
        expected.to_vec()
    );
    assert!(outcome.declare.is_some(), "the clear is recorded");
    assert!(outcome.unbond.is_some(), "the withdrawal is recorded");

    // A cleared tier: the re-run only withdraws.
    let rpc = ScriptedRpc::mined(mined_receipt(cb, Vec::new()));
    let bond_contract = CapacityBond::new(cb, rpc.clone().provider());
    let mut outcome = Outcome::default();
    unbond_whole_bond(&bond_contract, amount, false, &mut outcome)
        .await
        .unwrap();
    assert_eq!(
        rpc.sent_inputs()
            .iter()
            .map(|b| b.to_vec())
            .collect::<Vec<_>>(),
        vec![CapacityBond::unbondCall { amount }.abi_encode()]
    );
    assert!(outcome.declare.is_none(), "{:?}", outcome.declare);
}

#[test]
fn json_carries_the_phase_and_every_tx_slot() {
    let outcome = Outcome {
        unbond: Some(B256::repeat_byte(0xAB)),
        ..Outcome::default()
    };
    let s = rendered(
        &plan(Phase::Unbond {
            amount: U256::from(50_000u64),
            clear_tier: None,
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
        Some("unbond")
    );
    assert_eq!(
        v.get("submitted").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        v.get("unbond_tx").and_then(serde_json::Value::as_str),
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

/// An unbond that only returns money still has to be distinguishable
/// from a preview, the way every other command's receipt is.
#[test]
fn dry_run_is_distinguishable_from_a_failed_send() {
    let dry = rendered(
        &plan(Phase::Unbond {
            amount: U256::from(1u64),
            clear_tier: None,
        }),
        false,
        &Outcome::default(),
        true,
    );
    assert!(dry.contains("submitted=false dry_run=true"), "{dry}");
    let failed = rendered(
        &plan(Phase::Unbond {
            amount: U256::from(1u64),
            clear_tier: None,
        }),
        false,
        &Outcome::default(),
        false,
    );
    assert!(failed.contains("submitted=false dry_run=false"), "{failed}");
}

/// `rotate-key --key eth` with no optional flags, for driving [`select_phase`].
fn eth_args() -> cli::RotateKeyArgs {
    cli::RotateKeyArgs {
        key: cli::RotateKeyTarget::Eth,
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

/// Run [`select_phase`] against a provider with no queued responses, so any
/// chain read the selection makes fails the test: the deregister and unbond
/// phases decide from the registry reads `build_plan` already made.
async fn phase_for(active: bool, active_bond: u64, declared_mbps: u64) -> Phase {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;

    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let bond_contract = CapacityBond::new(Address::repeat_byte(0xCB), provider);
    select_phase(
        &bond_contract,
        PhaseInputs {
            old_operator: Address::repeat_byte(0x0E),
            active,
            active_bond: U256::from(active_bond),
            declared_mbps,
            region_hint: "DE".to_string(),
            packed_multiaddrs: Vec::new(),
        },
        &eth_args(),
    )
    .await
    .expect("the phase is decidable from the registry reads alone")
}

/// An operator out of the active set without `deregisterNode` (ejected, or
/// displaced by `reclaimNodeId`) still has a declared tier, and `unbond` of
/// the whole bond reverts `BondBelowCurve` against it. The unbond phase must
/// clear that tier first, and name it, since nothing can read it back later.
#[tokio::test]
async fn an_inactive_operator_with_a_tier_clears_it_before_unbonding() {
    assert_eq!(
        phase_for(false, 50_000, 5_000).await,
        Phase::Unbond {
            amount: U256::from(50_000u64),
            clear_tier: Some(5_000),
        }
    );
}

/// The convergence case: a run that landed `declareMbps(0)` and lost the
/// `unbond` re-reads a zero tier, so the re-run only withdraws. A deregistered
/// operator reaches the same phase, since `deregisterNode` clears the tier.
#[tokio::test]
async fn a_cleared_tier_only_unbonds() {
    assert_eq!(
        phase_for(false, 50_000, 0).await,
        Phase::Unbond {
            amount: U256::from(50_000u64),
            clear_tier: None,
        }
    );
}

/// An active operator deregisters, whatever its tier: `declareMbps(0)` is
/// refused from a registered operator.
#[tokio::test]
async fn an_active_operator_deregisters_first() {
    assert_eq!(
        phase_for(true, 50_000, 5_000).await,
        Phase::Deregister {
            declared_mbps: 5_000
        }
    );
}

/// The cleared tier is the one input the re-onboarding phase cannot read
/// back, so the unbond receipt carries it in both output shapes.
#[test]
fn the_unbond_receipt_names_the_cleared_tier() {
    let p = plan(Phase::Unbond {
        amount: U256::from(50_000u64),
        clear_tier: Some(5_000),
    });
    let text = rendered(&p, false, &Outcome::default(), false);
    assert!(text.contains("clear_tier_mbps=5000"), "{text}");
    let json = rendered(&p, true, &Outcome::default(), false);
    let v: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
    assert_eq!(
        v.get("clear_tier_mbps").and_then(serde_json::Value::as_u64),
        Some(5_000)
    );

    let none = rendered(&plan(reonboard_phase()), false, &Outcome::default(), false);
    assert!(none.contains("clear_tier_mbps=none"), "{none}");
}
