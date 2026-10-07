use super::*;

fn plan(prior: u64, required: u64, target: u64, needs_declare: bool) -> Plan {
    Plan {
        mbps: 1000,
        token: Address::repeat_byte(0x11),
        capacity_bond: Address::repeat_byte(0x22),
        required: U256::from(required),
        target: U256::from(target),
        prior: U256::from(prior),
        shortfall: U256::from(target).saturating_sub(U256::from(prior)),
        needs_declare,
    }
}

#[test]
fn real_run_already_bonded_is_noop_not_dry_run() {
    // prior already at/above target, tier already declared → nothing to do.
    // A real run (dry_run=false) that submits nothing must report
    // `dry_run=false`, not masquerade as a dry-run preview (#934 review).
    let p = plan(60_000, 50_000, 60_000, false);
    assert_eq!(p.shortfall, U256::ZERO);
    let mut buf = Vec::new();
    write_plan(&mut buf, &p, false, &Outcome::default(), false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("bonded_base=0"), "{s}");
    assert!(s.contains("needs_declare=false"), "{s}");
    assert!(s.contains("submitted=false dry_run=false"), "{s}");
    assert!(s.contains("bond_tx=skipped"), "{s}");
}

#[test]
fn dry_run_reports_dry_run_true() {
    let p = plan(0, 50_000, 50_000, true);
    let mut buf = Vec::new();
    write_plan(&mut buf, &p, false, &Outcome::default(), true).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("submitted=false dry_run=true"), "{s}");
}

/// #1355 — the partial-failure shape `run` now prints before propagating:
/// `approve` + `bond` mined, `declareMbps` did not. The operator's TOKEN has
/// moved, so the report must say `submitted=true` and name the two landed
/// hashes; reporting `declare_tx` as skipped-vs-failed is not distinguished
/// here (the accompanying error is what says which), but the landed hashes
/// must be present either way.
#[test]
fn partial_outcome_reports_the_steps_that_landed() {
    let p = plan(0, 50_000, 50_000, true);
    let outcome = Outcome {
        approve: Some(B256::repeat_byte(0xa1)),
        bond: Some(B256::repeat_byte(0xb2)),
        declare: None,
    };

    let mut buf = Vec::new();
    write_plan(&mut buf, &p, false, &outcome, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("submitted=true dry_run=false"), "{s}");
    assert!(
        s.contains(&format!("approve_tx={:#x}", outcome.approve.unwrap())),
        "{s}"
    );
    assert!(
        s.contains(&format!("bond_tx={:#x}", outcome.bond.unwrap())),
        "{s}"
    );
    assert!(s.contains("declare_tx=skipped"), "{s}");

    let mut buf = Vec::new();
    write_plan(&mut buf, &p, true, &outcome, false).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(
        v.get("submitted").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        v.get("approve_tx").and_then(serde_json::Value::as_str),
        Some(format!("{:#x}", outcome.approve.unwrap()).as_str())
    );
    assert_eq!(
        v.get("bond_tx").and_then(serde_json::Value::as_str),
        Some(format!("{:#x}", outcome.bond.unwrap()).as_str())
    );
    assert!(
        v.get("declare_tx").is_some_and(serde_json::Value::is_null),
        "unlanded step must be present and null, got {v}"
    );
}

/// #1355 — the resume guidance is what an operator acts on after a partial
/// failure, and its arm ordering is load-bearing: putting an earlier step
/// first would tell someone whose TOKEN is already in `CapacityBond` that
/// only an allowance was granted. Pin which step each state names, over
/// every representable `Outcome`.
#[test]
fn resume_hint_names_the_latest_step_that_may_have_taken_effect() {
    let approve = B256::repeat_byte(0xa1);
    let bond = B256::repeat_byte(0xb2);
    let declare = B256::repeat_byte(0xc3);

    // Each row: the outcome, the hash it MUST name, and the hashes it must
    // not name (an earlier step is never the one still outstanding).
    let cases = [
        (
            Outcome {
                approve: Some(approve),
                bond: Some(bond),
                declare: Some(declare),
            },
            declare,
            vec![approve, bond],
        ),
        (
            Outcome {
                approve: Some(approve),
                bond: Some(bond),
                declare: None,
            },
            bond,
            vec![approve],
        ),
        // Allowance already sufficient, so no approve tx at all.
        (
            Outcome {
                approve: None,
                bond: Some(bond),
                declare: None,
            },
            bond,
            vec![],
        ),
        (
            Outcome {
                approve: Some(approve),
                bond: None,
                declare: None,
            },
            approve,
            vec![bond],
        ),
    ];

    for (outcome, must_name, must_not_name) in cases {
        let hint = resume_hint(&outcome);
        assert!(
            hint.contains(&format!("{must_name:#x}")),
            "must name {must_name:#x}: {hint}"
        );
        for other in must_not_name {
            assert!(
                !hint.contains(&format!("{other:#x}")),
                "must not name the earlier step {other:#x}: {hint}"
            );
        }
    }

    let nothing = resume_hint(&Outcome::default());
    assert!(
        nothing.contains("no transaction is recorded as having taken effect"),
        "{nothing}"
    );
}

/// #1355 review — the property, not a denylist of last release's phrasing.
///
/// `chain_ctx::send` sets its `landed` slot BEFORE awaiting the receipt, so
/// `Some` means "may have taken effect" — including the receipt-fetch
/// timeout where the tx is still in flight. Every arm that names a hash must
/// therefore hedge and must tell the operator to wait: advising an immediate
/// re-run while a bond is pending is what doubles it. The first version of
/// this test forbade four literal strings and missed a fifth arm that said
/// the same thing in different words, so assert the required content
/// instead.
#[test]
fn resume_hint_hedges_and_warns_whenever_a_step_may_be_outstanding() {
    let tx = B256::repeat_byte(0xd4);
    let outcomes = [
        Outcome {
            approve: Some(tx),
            bond: None,
            declare: None,
        },
        Outcome {
            approve: None,
            bond: Some(tx),
            declare: None,
        },
        Outcome {
            approve: None,
            bond: None,
            declare: Some(tx),
        },
    ];

    for outcome in outcomes {
        let hint = resume_hint(&outcome);
        assert!(
            hint.contains("may have taken effect"),
            "must hedge rather than assert an outcome: {hint}"
        );
        assert!(
            hint.contains("Do NOT re-run until"),
            "must warn against re-running while a tx is outstanding: {hint}"
        );
        // The unqualified negatives that made the first version unsafe.
        for forbidden in [
            "no TOKEN was transferred",
            "on-chain state is unchanged",
            "will not over-bond",
            "no transaction landed",
            "no bond transaction followed",
            "re-running is safe",
        ] {
            assert!(
                !hint.contains(forbidden),
                "must not assert `{forbidden}` — the tx may be in flight: {hint}"
            );
        }
    }

    // The all-absent state is the one place a negative is legitimate, and it
    // still must not claim the chain is untouched.
    let nothing = resume_hint(&Outcome::default());
    assert!(
        !nothing.contains("on-chain state is unchanged"),
        "{nothing}"
    );
    assert!(nothing.contains("confirmed reverted"), "{nothing}");
}

#[test]
fn shortfall_is_target_minus_prior() {
    // target = max(minBond, required); here required dominates.
    let p = plan(10_000, 50_000, 50_000, true);
    assert_eq!(p.shortfall, U256::from(40_000u64));
}

#[test]
fn target_floor_is_min_bond() {
    // required below minBond → target is the minBond floor, shortfall to it.
    let p = plan(0, 100, 200, true);
    assert_eq!(p.target, U256::from(200u64));
    assert_eq!(p.shortfall, U256::from(200u64));
}
