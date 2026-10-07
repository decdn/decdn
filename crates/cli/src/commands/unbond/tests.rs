use super::*;

fn plan(action: Action, below_min_bond: bool) -> Plan {
    Plan {
        capacity_bond: Address::repeat_byte(0x22),
        action,
        prior: U256::from(60_000u64),
        declared_mbps: 100,
        unbonding_period: 14 * 24 * 3600,
        below_min_bond,
    }
}

fn rendered(p: &Plan, json: bool, o: &Outcome) -> String {
    render(p, json, o, false)
}

fn render(p: &Plan, json: bool, o: &Outcome, dry_run: bool) -> String {
    let mut buf = Vec::new();
    write_plan(&mut buf, p, json, o, dry_run).unwrap();
    String::from_utf8(buf).unwrap()
}

#[test]
fn request_phase_reports_release_retained_and_declare() {
    let p = plan(
        Action::Request {
            release: U256::from(40_000u64),
            retained: U256::from(20_000u64),
            declare_to: Some(50),
        },
        false,
    );
    let s = rendered(&p, false, &Outcome::default());
    assert!(s.contains("phase=request"), "{s}");
    assert!(s.contains("release_base=40000"), "{s}");
    assert!(s.contains("retained_bond_base=20000"), "{s}");
    assert!(s.contains("declare_to_mbps=50"), "{s}");
    assert!(s.contains("unbonding_period=14d 0h"), "{s}");
    assert!(s.contains("below_min_bond=false"), "{s}");
    assert!(s.contains("submitted=false"), "{s}");
}

/// Rendering only — the `--all` floor arithmetic itself lives in
/// `plan_computation::all_retains_the_bare_curve_floor_not_min_bond`.
#[test]
fn write_plan_reports_a_below_min_bond_residual() {
    // The operator has to be able to see, in the receipt, that the node
    // won't come back without re-bonding.
    let p = plan(
        Action::Request {
            release: U256::from(59_000u64),
            retained: U256::from(1_000u64),
            declare_to: None,
        },
        true,
    );
    let s = rendered(&p, false, &Outcome::default());
    assert!(s.contains("declare_to_mbps=none"), "{s}");
    assert!(s.contains("below_min_bond=true"), "{s}");
}

#[test]
fn waiting_phase_reports_unlock_and_remaining() {
    let p = plan(
        Action::Waiting {
            amount: U256::from(40_000u64),
            unlock_at: 1_000_000 + 3 * 24 * 3600,
            now: 1_000_000,
        },
        false,
    );
    assert_eq!(p.remaining_secs(), 3 * 24 * 3600);
    let s = rendered(&p, false, &Outcome::default());
    assert!(s.contains("phase=waiting"), "{s}");
    assert!(s.contains("pending_base=40000"), "{s}");
    assert!(s.contains("remaining=3d 0h"), "{s}");
    // No release keys leak into a phase they don't describe.
    assert!(!s.contains("release_base"), "{s}");
}

#[test]
fn withdraw_phase_reports_the_tx() {
    let p = plan(
        Action::Withdraw {
            amount: U256::from(40_000u64),
        },
        false,
    );
    let o = Outcome {
        withdraw: Some(B256::repeat_byte(0xab)),
        ..Outcome::default()
    };
    let s = rendered(&p, false, &o);
    assert!(s.contains("phase=withdraw"), "{s}");
    assert!(s.contains("withdrawn_base=40000"), "{s}");
    assert!(s.contains("declare_tx=skipped"), "{s}");
    assert!(s.contains("submitted=true"), "{s}");
}

#[test]
fn json_carries_only_the_active_phase_keys() {
    let p = plan(
        Action::Request {
            release: U256::from(40_000u64),
            retained: U256::from(20_000u64),
            declare_to: Some(50),
        },
        false,
    );
    let s = rendered(&p, true, &Outcome::default());
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert_eq!(
        v.get("phase").and_then(serde_json::Value::as_str),
        Some("request")
    );
    assert_eq!(
        v.get("release_base").and_then(serde_json::Value::as_str),
        Some("40000")
    );
    assert_eq!(
        v.get("declare_to_mbps").and_then(serde_json::Value::as_u64),
        Some(50)
    );
    assert!(v.get("pending_base").is_none(), "{s}");
    assert!(
        v.get("withdraw_tx").is_some_and(serde_json::Value::is_null),
        "{s}"
    );
}

#[test]
fn remaining_secs_is_zero_off_the_waiting_phase() {
    let p = plan(
        Action::Withdraw {
            amount: U256::from(1u64),
        },
        false,
    );
    assert_eq!(p.remaining_secs(), 0);
}

/// The convergence case: `execute` sends `declareMbps` and `requestUnbond`
/// as two transactions, so a run that lands the first and loses the second
/// leaves `declared == target`. Re-running the identical command must pick
/// up where it left off — skipping the redundant declare and still
/// releasing the surplus — not refuse because the tier is no longer
/// strictly above the target.
#[test]
fn a_retry_after_the_declare_landed_still_unbonds() {
    assert_eq!(
        tier_step(50, 50).expect("an equal tier is a resumable retry, not an error"),
        None,
        "the declare already landed, so it must be skipped rather than re-sent"
    );
}

#[test]
fn tier_step_declares_down_and_rejects_raising() {
    assert_eq!(tier_step(50, 100).unwrap(), Some(50));
    let err = tier_step(200, 100).expect_err("raising capacity is not this command's job");
    let msg = format!("{err:#}");
    assert!(msg.contains("above the current declared tier"), "{msg}");
    assert!(msg.contains("decdn node bond --mbps"), "{msg}");
}

/// The gate exists so an operator cannot start a window-long outage by
/// accident, so the headless-and-unconfirmed case must REFUSE rather than
/// assume consent — the one wrong answer here is expensive to undo.
#[test]
fn headless_request_without_yes_refuses() {
    let request = Action::Request {
        release: U256::from(1u64),
        retained: U256::ZERO,
        declare_to: None,
    };
    assert_eq!(
        decide_confirmation(request, false, false),
        Confirmation::NeedFlag
    );
    assert_eq!(
        decide_confirmation(request, false, true),
        Confirmation::Prompt
    );
    assert_eq!(
        decide_confirmation(request, true, false),
        Confirmation::Bypassed
    );
}

/// A withdrawal only returns TOKEN and the maturing case submits nothing,
/// so neither may block on a prompt — that would wedge `--json` consumers
/// and cron-driven withdrawals.
#[test]
fn only_a_new_request_is_ever_confirmed() {
    for action in [
        Action::Withdraw {
            amount: U256::from(1u64),
        },
        Action::Waiting {
            amount: U256::from(1u64),
            unlock_at: 2,
            now: 1,
        },
    ] {
        for interactive in [true, false] {
            assert_eq!(
                decide_confirmation(action, false, interactive),
                Confirmation::NotNeeded,
                "{action:?} must not prompt"
            );
        }
    }
}

#[test]
fn format_remaining_units() {
    assert_eq!(format_remaining(0), "now");
    assert_eq!(format_remaining(45), "45s");
    assert_eq!(format_remaining(90), "1m");
    assert_eq!(format_remaining(3600 + 1800), "1h 30m");
    assert_eq!(format_remaining(14 * 24 * 3600), "14d 0h");
    assert_eq!(format_remaining(36 * 3600), "1d 12h");
    // Exact unit boundaries, where an off-by-one `<` would silently reclassify.
    assert_eq!(format_remaining(60), "1m");
    assert_eq!(format_remaining(3600), "1h 0m");
    assert_eq!(format_remaining(86_400), "1d 0h");
}

/// Plan-computation coverage against a mocked provider.
///
/// `resolve_release` decides how much TOKEN moves, and until this module
/// existed nothing exercised it: the `--all` arm was never executed by any
/// test (its only e2e use returns from `build_plan`'s pending-request branch
/// first), and `--to-mbps`'s `max(minBond, ·)` term was inert at the e2e's
/// chosen tiers — deleting it broke nothing.
///
/// `Asserter` serves `eth_call` responses FIFO, which works here because the
/// call order is static: `--to-mbps` reads the capacity band only when a
/// `declareMbps` is actually planned, then `bondRequired`; `--all` and
/// `--amount` read `bondRequired` alone. Queueing exactly the expected number
/// of responses is therefore itself an assertion about which reads happen —
/// a surplus read would fail with an empty-queue error.
mod plan_computation {
    use alloy::primitives::Bytes;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;

    use super::*;

    /// TOKEN base units, for readable fixtures.
    fn token(n: u64) -> U256 {
        U256::from(n) * U256::from(1_000_000_000_000_000_000_u64)
    }

    /// Queue one ABI-encoded `uint256` return for the next `eth_call`.
    fn push_u256(asserter: &Asserter, v: U256) {
        asserter.push_success(&Bytes::from(v.to_be_bytes::<32>().to_vec()));
    }

    /// Run `resolve_release` against a provider serving `calls` in order,
    /// for a REGISTERED operator — the state every flag was designed
    /// around. The inactive variant is [`resolve_inactive`]; it is a
    /// separate helper rather than a sixth parameter so the ten call sites
    /// below keep reading as "an ordinary operator lowering their bond".
    async fn resolve(
        calls: &[U256],
        request: AmountRequest,
        prior: U256,
        declared: u64,
        min_bond: U256,
    ) -> anyhow::Result<(U256, U256, Option<u64>)> {
        resolve_as(calls, request, prior, declared, min_bond, true).await
    }

    /// [`resolve`] for an operator that is NOT in the registered set —
    /// ejected, or bonded-and-declared but never registered (#1361).
    async fn resolve_inactive(
        calls: &[U256],
        request: AmountRequest,
        prior: U256,
        declared: u64,
        min_bond: U256,
    ) -> anyhow::Result<(U256, U256, Option<u64>)> {
        resolve_as(calls, request, prior, declared, min_bond, false).await
    }

    async fn resolve_as(
        calls: &[U256],
        request: AmountRequest,
        prior: U256,
        declared: u64,
        min_bond: U256,
        active: bool,
    ) -> anyhow::Result<(U256, U256, Option<u64>)> {
        let asserter = Asserter::new();
        for v in calls {
            push_u256(&asserter, *v);
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let addr = Address::repeat_byte(0x22);
        let bond = CapacityBond::new(addr, provider);
        resolve_release(
            &bond,
            addr,
            request,
            prior,
            U256::from(declared),
            min_bond,
            active,
        )
        .await
    }

    /// `--all` takes the BARE curve floor. If this ever grows a
    /// `min_bond.max(...)` — the obvious copy-paste from the `--to-mbps` arm
    /// one match arm up — it would retain more than promised and silently
    /// release less TOKEN than the operator asked for.
    #[tokio::test]
    async fn all_retains_the_bare_curve_floor_not_min_bond() {
        // Below the curve/minBond crossover: curve floor 100 < minBond 50_000.
        let (release, retained, declare_to) = resolve(
            &[token(100)],
            AmountRequest::All,
            token(60_000),
            10,
            token(50_000),
        )
        .await
        .expect("a surplus above the curve floor is releasable");
        assert_eq!(retained, token(100), "the bare curve floor, no minBond max");
        assert_eq!(release, token(59_900));
        assert_eq!(
            declare_to, None,
            "--all never moves an ACTIVE operator's declared tier"
        );
        assert!(
            retained < token(50_000),
            "this is the case where --all leaves the node inactive"
        );
    }

    /// The #1361 exit end-to-end through `resolve_release`. This pins the
    /// plumbing — the `active` flag reaches the `--all` arm, a declare is
    /// planned, the release figure is the whole bond — but NOT the retarget
    /// itself, which the mock cannot observe. See
    /// `all_target_retargets_the_curve_for_an_inactive_operator`.
    #[tokio::test]
    async fn all_releases_the_tier_and_everything_for_an_inactive_operator() {
        let (release, retained, declare_to) = resolve_inactive(
            &[U256::ZERO],
            AmountRequest::All,
            token(10_126),
            1000,
            token(50_000),
        )
        .await
        .expect("an inactive operator can release their tier");
        assert_eq!(
            declare_to,
            Some(0),
            "the run must send declareMbps(0) before requestUnbond"
        );
        assert_eq!(retained, U256::ZERO, "bondRequired(0) == 0 — a full exit");
        assert_eq!(
            release,
            token(10_126),
            "the whole remaining bond, including a slashed-below-floor residual"
        );
    }

    /// The retarget itself, tested where it is observable at all.
    ///
    /// `resolve_release` cannot pin this: the decision shows up only in the
    /// ARGUMENT to `bondRequired`, and `Asserter` answers by queue position,
    /// not by calldata. Mutating the retarget away leaves every mocked test
    /// in this module green — verified — while shipping a CLI that refuses
    /// the #1361 exit with "nothing to release", because `retained` would be
    /// `bondRequired(1000) > prior` and the release would floor to 0.
    #[test]
    fn all_target_retargets_the_curve_for_an_inactive_operator() {
        let declared = U256::from(1000u64);

        // Inactive with a standing tier: clear it, and evaluate the curve
        // against 0 — the pair the mock cannot distinguish.
        assert_eq!(all_target(false, declared), (Some(0), U256::ZERO));

        // Active: unchanged. `declareMbps(0)` reverts for them, and the
        // retained bond is their standing tier's floor.
        assert_eq!(all_target(true, declared), (None, declared));

        // Nothing declared: no redundant `declareMbps(0)` to pay gas for and
        // log as a 0 -> 0 change, on either side of the registry.
        assert_eq!(all_target(false, U256::ZERO), (None, U256::ZERO));
        assert_eq!(all_target(true, U256::ZERO), (None, U256::ZERO));
    }

    /// The release is scoped to a tier that actually stands: an inactive
    /// operator with nothing declared must not send a redundant
    /// `declareMbps(0)`, which costs gas and logs a 0 -> 0 tier change.
    #[tokio::test]
    async fn inactive_operator_with_no_tier_sends_no_declare() {
        let (release, retained, declare_to) = resolve_inactive(
            &[U256::ZERO],
            AmountRequest::All,
            token(60_000),
            0,
            token(50_000),
        )
        .await
        .expect("a never-declared operator can still release everything");
        assert_eq!(declare_to, None, "nothing to clear");
        assert_eq!(retained, U256::ZERO);
        assert_eq!(release, token(60_000));
    }

    /// A bonded-but-never-declared operator has `bondRequired(0) == 0`, so
    /// `--all` really does mean all. This is the maximal-consequence path
    /// through the command and had no coverage at all.
    #[tokio::test]
    async fn all_releases_everything_when_no_tier_was_ever_declared() {
        let (release, retained, _) = resolve(
            &[U256::ZERO],
            AmountRequest::All,
            token(60_000),
            0,
            token(50_000),
        )
        .await
        .expect("nothing is retained when no tier is declared");
        assert_eq!(retained, U256::ZERO);
        assert_eq!(release, token(60_000), "the entire bond");
    }

    /// The `max(minBond, ·)` term that distinguishes `--to-mbps` from
    /// `--all`. Below the crossover `minBond` is the operative floor, so
    /// deleting the `max` makes this fail — which is the point.
    #[tokio::test]
    async fn to_mbps_below_the_crossover_retains_min_bond() {
        // Band [10, 200_000], then bondRequired(10) = 100 TOKEN << minBond.
        let (release, retained, declare_to) = resolve(
            &[U256::from(10), U256::from(200_000), token(100)],
            AmountRequest::ToMbps(10),
            token(60_000),
            5_000,
            token(50_000),
        )
        .await
        .expect("a surplus above minBond is releasable");
        assert_eq!(
            retained,
            token(50_000),
            "minBond is the floor here, NOT the 100 TOKEN curve value"
        );
        assert_eq!(release, token(10_000));
        assert_eq!(declare_to, Some(10), "the tier must be declared down first");
    }

    /// Above the crossover the curve dominates and `minBond` is inert — the
    /// complement of the case above, so neither term can be dropped.
    #[tokio::test]
    async fn to_mbps_above_the_crossover_retains_the_curve() {
        let (_, retained, _) = resolve(
            &[U256::from(10), U256::from(200_000), token(115_000)],
            AmountRequest::ToMbps(2_000),
            token(346_000),
            5_000,
            token(50_000),
        )
        .await
        .expect("a surplus above the curve is releasable");
        assert_eq!(retained, token(115_000), "the curve dominates minBond here");
    }

    /// The resume path: tier already at the target, so no `declareMbps` is
    /// planned — and therefore the capacity band is never read. Queueing only
    /// the `bondRequired` response proves the band check is gated on the
    /// declare rather than run unconditionally; an ungated check would try to
    /// read from an empty queue and fail.
    #[tokio::test]
    async fn to_mbps_at_the_current_tier_skips_the_declare_and_the_band_read() {
        let (release, retained, declare_to) = resolve(
            &[token(115_000)],
            AmountRequest::ToMbps(2_000),
            token(346_000),
            2_000,
            token(50_000),
        )
        .await
        .expect("an equal tier is a resumable retry");
        assert_eq!(declare_to, None, "the declare already landed");
        assert_eq!(retained, token(115_000));
        assert_eq!(release, token(231_000), "the surplus is still released");
    }

    /// `--amount` is bounded by the active bond. The guard is structural, not
    /// cosmetic: `retained = prior - amount` is a raw `U256` subtraction and
    /// alloy's `Uint` panics on overflow, in a workspace that denies `panic`.
    #[tokio::test]
    async fn amount_above_the_active_bond_is_rejected_before_subtracting() {
        let err = resolve(
            &[],
            AmountRequest::Exact(token(60_001)),
            token(60_000),
            2_000,
            token(50_000),
        )
        .await
        .expect_err("releasing more than is bonded must be refused");
        let msg = format!("{err:#}");
        assert!(msg.contains("exceeds the active bond"), "{msg}");
    }

    /// The happy `--amount` path, and the boundary: releasing down to exactly
    /// the curve floor is legal.
    #[tokio::test]
    async fn amount_down_to_exactly_the_curve_floor_is_allowed() {
        let (release, retained, declare_to) = resolve(
            &[token(115_000)],
            AmountRequest::Exact(token(231_000)),
            token(346_000),
            2_000,
            token(50_000),
        )
        .await
        .expect("landing exactly on the curve floor is legal");
        assert_eq!(retained, token(115_000));
        assert_eq!(release, token(231_000));
        assert_eq!(declare_to, None, "--amount never moves the tier");
    }

    /// One base unit past the floor must be refused with the remedy, so
    /// `BondBelowCurve` never surfaces as a raw revert.
    #[tokio::test]
    async fn amount_one_unit_below_the_curve_floor_names_the_remedy() {
        let err = resolve(
            &[token(115_000)],
            AmountRequest::Exact(token(231_000) + U256::from(1u64)),
            token(346_000),
            2_000,
            token(50_000),
        )
        .await
        .expect_err("breaching the curve floor must be refused");
        let msg = format!("{err:#}");
        assert!(msg.contains("BondBelowCurve"), "{msg}");
        assert!(msg.contains("--to-mbps"), "{msg}");
    }

    /// "Nothing to release" rather than a `ZeroAmount` revert, for both
    /// curve-derived flags.
    #[tokio::test]
    async fn nothing_to_release_is_refused_locally() {
        let err = resolve(
            &[U256::from(10), U256::from(200_000), token(115_000)],
            AmountRequest::ToMbps(2_000),
            token(115_000),
            5_000,
            token(50_000),
        )
        .await
        .expect_err("already at the target");
        assert!(format!("{err:#}").contains("nothing to release"), "{err:#}");

        let err = resolve(
            &[token(115_000)],
            AmountRequest::All,
            token(115_000),
            2_000,
            token(50_000),
        )
        .await
        .expect_err("already at the curve floor");
        assert!(format!("{err:#}").contains("nothing to release"), "{err:#}");
    }
}
