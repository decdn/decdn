use super::*;
use decdn_protocol::{Coverage, VoucherRejectReason};

/// A hash stays pending while any open for it is in flight, and clears when
/// the last guard drops. Another hash is never pending (#2224).
#[test]
fn an_open_stays_pending_until_its_last_guard_drops() {
    let pending = Arc::new(PendingOpens::default());
    let (hash, other) = ([1u8; 32], [2u8; 32]);
    let first = PendingOpen::enter(&pending, hash);
    let second = PendingOpen::enter(&pending, hash);
    assert!(pending.contains(&hash));
    assert!(!pending.contains(&other));
    drop(first);
    assert!(pending.contains(&hash), "one open is still in flight");
    drop(second);
    assert!(!pending.contains(&hash), "the last guard clears the hash");
    assert!(
        pending.0.lock().unwrap().is_empty(),
        "a cleared hash leaves no entry behind"
    );
}

/// An unprovisioned origin refuses nothing, even with an open marked.
#[test]
fn an_unprovisioned_origin_refuses_no_whole_blob_request() {
    let origin = NodeOrigin::new();
    let hash = Hash::from_bytes([3u8; 32]);
    let _open = origin.enter_open(hash);
    assert!(!origin.refuses_whole_blob(hash, [4u8; 32]));
}

/// The wedge is a FIXED [`WEDGED_PROVIDER_SUPPRESSION_SECS`] window measured from the
/// rejection, not a channel deadline — the buyer pool is shared across every provider and
/// carries no per-provider expiry to key one on. Round-trips the write and the read halves
/// so the horizon's DERIVATION is pinned, not just its comparison: nothing else in the
/// workspace distinguishes 3600s from any other future instant, because both integration
/// tests observe the wedge milliseconds after it is recorded.
///
/// Fail-on-revert — each mutation run, with the assertion it actually trips:
/// - derive the horizon from anything but `WEDGED_PROVIDER_SUPPRESSION_SECS` in
///   `record_wedged_at` → "the horizon is wedge time + the window";
/// - flip `retain`'s `>` to `>=`, or drop the `retain` entirely → both trip "the elapsed
///   sibling must go in the same read", which reaches them before the boundary assertion
///   does because a stale entry survives that prune either way;
/// - swap `insert` for `entry().or_insert()` → "a re-wedge must restart the window".
///
/// The two map-state assertions pin what the booleans cannot: that the READ is what bounds
/// the map. There is deliberately no sweep task, so a prune that stopped happening would
/// leak an entry per wedged provider forever.
#[test]
fn a_wedge_lifts_when_its_fixed_suppression_window_elapses() {
    let peer = DhtNodeId::from_bytes([7u8; 32]);
    let other = DhtNodeId::from_bytes([9u8; 32]);
    let wedged_at = 1_000_000u64;
    let horizon = wedged_at + WEDGED_PROVIDER_SUPPRESSION_SECS;

    // Inside the window: suppressed, and the entry survives the read.
    let mut map = HashMap::new();
    record_wedged_at(&mut map, peer, wedged_at);
    assert_eq!(
        map.get(&peer),
        Some(&horizon),
        "the horizon is wedge time + the window"
    );
    assert!(
        prune_and_check_wedged(&mut map, &peer, horizon - 1),
        "a provider must stay suppressed for the whole window"
    );
    assert_eq!(map.len(), 1, "an unexpired horizon must survive the prune");

    // A live wedge is per-peer, and one read prunes every elapsed entry, not just the one
    // asked about. `other` elapsed a second ago; `peer` has not.
    record_wedged_at(&mut map, other, wedged_at - 1);
    assert!(
        prune_and_check_wedged(&mut map, &peer, horizon - 1),
        "a live horizon must survive a prune that drops a sibling"
    );
    assert_eq!(map.len(), 1, "the elapsed sibling must go in the same read");
    assert!(
        !prune_and_check_wedged(&mut map, &other, horizon - 1),
        "an elapsed peer must not inherit a live peer's suppression"
    );

    // ON the horizon second: rankable again, and the read pruned the entry.
    let mut map = HashMap::new();
    record_wedged_at(&mut map, peer, wedged_at);
    assert!(
        !prune_and_check_wedged(&mut map, &peer, horizon),
        "the boundary is exclusive: rankable ON the second the window ends"
    );
    assert!(map.is_empty(), "an elapsed horizon must be pruned on read");

    // A re-wedge restarts the window from the newer rejection rather than inheriting the
    // older horizon — reachable whenever a lifted provider is ranked and wedges again.
    let mut map = HashMap::new();
    record_wedged_at(&mut map, peer, wedged_at);
    record_wedged_at(&mut map, peer, wedged_at + 1_000);
    assert!(
        prune_and_check_wedged(&mut map, &peer, horizon),
        "a re-wedge must restart the window, not inherit the older horizon"
    );
}

/// The provider-wide window must outlive the per-`(peer, hash)` negative-cache entry the
/// same wedge writes. At or below it, `wedged_providers` buys nothing the negative cache
/// does not already give for the blob that wedged it, and the filter plus both integration
/// tests guarding it become dead weight while still passing green.
///
/// Bounding a policy constant is in-convention in this module — see
/// `only_a_durable_refusal_earns_the_full_suppression_ttl`.
#[test]
fn the_provider_wide_window_outlives_the_per_hash_one() {
    assert!(
        WEDGED_PROVIDER_SUPPRESSION_SECS > REFUSAL_SUPPRESSION_TTL.as_secs(),
        "the provider-wide window ({WEDGED_PROVIDER_SUPPRESSION_SECS}s) must outlive the \
         per-(peer, hash) one ({}s), or the wedge filter buys nothing",
        REFUSAL_SUPPRESSION_TTL.as_secs()
    );
}

#[test]
fn region_penalty_only_for_same_region_slow_peer() {
    // Same region, slow → penalized.
    assert!(region_latency_penalty_applies(
        Some("DE"),
        "DE",
        REGION_LATENCY_MAX_MS + 1
    ));
    // Same region but at/under the ceiling → not penalized (boundary is >).
    assert!(!region_latency_penalty_applies(
        Some("DE"),
        "DE",
        REGION_LATENCY_MAX_MS
    ));
    // Different region, however slow → not penalized (the claim is plausible).
    assert!(!region_latency_penalty_applies(Some("DE"), "US", 5000));
    // Own region unset → penalty disabled (nothing to compare against).
    assert!(!region_latency_penalty_applies(None, "DE", 5000));
    // Peer region unknown (not in the peer table) → no claim to contradict.
    assert!(!region_latency_penalty_applies(Some("DE"), "", 5000));
}

/// The failure-class `reason` (#966) the `open_channel` kernel attaches to
/// the `anyhow` error chain must survive the additional `.context(...)`
/// layers `open_and_persist` / `open_or_reuse_pool` wrap around it —
/// `record_pool_open_failure`'s `downcast_ref` walks the whole chain, so
/// the metric label is recovered regardless of how deep the reason sits.
#[test]
fn failure_reason_survives_context_wrapping() {
    for reason in [
        PoolOpenFailureReason::InsufficientDeposit,
        PoolOpenFailureReason::ContractRevert,
        PoolOpenFailureReason::RpcError,
    ] {
        // Approximate the real chain: a base error, the kernel's typed
        // reason, then the caller's wrapping `.context` layers. The exact
        // ordering differs from the submit path — there the kernel attaches
        // the reason *after* its own `.context("submit openChannel")` — but
        // `downcast_ref` walks the whole chain irrespective of layer order,
        // which is exactly what this test pins down.
        let err = anyhow::anyhow!("openChannel send failed: transport down")
            .context(reason)
            .context("submit openChannel")
            .context("persist newly-opened buyer channel");
        let recovered = err.downcast_ref::<PoolOpenFailureReason>().copied();
        assert_eq!(
            recovered,
            Some(reason),
            "reason {reason:?} must be recoverable from the wrapped chain"
        );
    }

    // An error with no attached reason (e.g. a pure store fault) downcasts
    // to `None`, so the helper logs `unclassified` and only the unlabeled
    // total moves.
    let storeless = anyhow::anyhow!("redb write failed").context("persist buyer channel");
    assert!(storeless.downcast_ref::<PoolOpenFailureReason>().is_none());
}

/// A failed `openPool` tx moves `node_pull_pool_open_failures_total` exactly
/// once, because the open task meters it and marks it [`OpenReported`] — so
/// the classifier's residual arm, which is the counter's other writer, never
/// sees it. Two writers for one failure double the counter against
/// `node_pull_attempts_total` (#2072).
#[test]
fn a_reported_open_failure_is_not_metered_a_second_time() {
    for reason in [
        PoolOpenFailureReason::ContractRevert,
        PoolOpenFailureReason::RpcError,
    ] {
        let err = anyhow::anyhow!("submit openPool: execution reverted")
            .context(reason)
            .context(OpenReported);
        assert_eq!(
            classify_pool_open_arm(&err),
            PoolOpenArm::Reported,
            "{reason:?} must land on the arm that meters nothing"
        );
    }
}

/// The node-wide faults carry both markers, and `LocalPullFault` must win: an
/// `OpenReported` verdict here would answer `NotFound` from a node that cannot
/// pay anyone (#1560).
#[test]
fn local_fault_outranks_open_reported() {
    let err = anyhow::anyhow!("buyer pool store read failed")
        .context(OpenReported)
        .context(LocalPullFault);
    assert_eq!(classify_pool_open_arm(&err), PoolOpenArm::LocalFault);

    // And in the other attachment order — `downcast_ref` walks the chain, so
    // the arm order is what decides this, not which context was applied last.
    let err = anyhow::anyhow!("buyer pool store read failed")
        .context(LocalPullFault)
        .context(OpenReported);
    assert_eq!(classify_pool_open_arm(&err), PoolOpenArm::LocalFault);
}

/// An unfundable wallet carries `LocalPullFault` and `OpenReported` like the
/// other node-wide faults, but its fill ends "funding needed": the arm is its
/// own, and its miss answers the client `NotFound`, not `Declined`.
#[test]
fn an_unfundable_wallet_is_funding_needed_and_answers_not_found() {
    let err = anyhow::anyhow!("openPool would revert: balance too low")
        .context(PoolOpenFailureReason::InsufficientDeposit)
        .context(OpenReported)
        .context(LocalPullFault);
    assert_eq!(classify_pool_open_arm(&err), PoolOpenArm::FundingNeeded);
    assert!(matches!(
        miss_answer(PullMiss::FundingNeeded),
        Ok(OriginFetch::NotFound)
    ));
    assert_eq!(
        PullMiss::Clean.or(PullMiss::FundingNeeded),
        PullMiss::FundingNeeded
    );
    assert_eq!(
        PullMiss::FundingNeeded.or(PullMiss::LocalFault),
        PullMiss::LocalFault
    );
    assert!(!PullMiss::FundingNeeded.is_local_fault());
}

/// A pending open outranks everything: it is not a failure at all, so it must
/// not reach a counter that a dashboard reads as one.
#[test]
fn pending_outranks_every_other_marker() {
    let err = anyhow::anyhow!("still opening")
        .context(LocalPullFault)
        .context(OpenReported)
        .context(PoolOpenPending {
            waited: std::time::Duration::from_secs(5),
        });
    assert_eq!(classify_pool_open_arm(&err), PoolOpenArm::Pending);
}

/// An unmarked failure is the residual the ladder counts itself — the only
/// arm that writes `node_pull_pool_open_failures_total` from the caller side.
#[test]
fn an_unmarked_failure_is_the_residual() {
    let err = anyhow::anyhow!("supervisor aborted at shutdown");
    assert_eq!(classify_pool_open_arm(&err), PoolOpenArm::Residual);
}

/// An unprovisioned `NodeOrigin` is a clean miss for any hash, so wiring it
/// into the engine chain before its dependencies exist (or with the feature
/// off) never disturbs the existing miss behaviour.
#[tokio::test]
async fn unprovisioned_fetch_is_not_found() {
    let origin = NodeOrigin::new();
    let got = origin.fetch(Hash::new(b"anything"), 1 << 20).await.unwrap();
    assert!(matches!(got, OriginFetch::NotFound));
    assert_eq!(origin.kind(), OriginKind::Peer);
}

#[test]
fn ms_to_u32_saturates_and_floors() {
    assert_eq!(ms_to_u32(-5.0), 0);
    assert_eq!(ms_to_u32(42.9), 42);
    assert_eq!(ms_to_u32(f64::from(u32::MAX) + 1.0), u32::MAX);
}

/// The whole #857 fix hinges on `pull_from_candidate` recovering the buyer-side
/// sentinels via `downcast_ref` after they round-trip through `anyhow::Error`
/// (the timeout path even double-wraps via `??`). Pin that contract at the
/// boundary — using the SAME `downcast_ref` call production uses — so a future
/// change to the sentinel type or a switch away from `downcast_ref` fails here,
/// a localized failure, rather than as a confusing "honest provider got tarred"
/// assertion three layers up. The last case proves `downcast_ref` still finds
/// the sentinel through a `.context()` layer (anyhow walks the chain), so a
/// future wrap in the propagation path would not silently break classification.
#[test]
fn buyer_side_sentinels_survive_anyhow_downcast() {
    let timeout: anyhow::Error = anyhow::Error::new(PullTimeout {
        after: Duration::from_secs(3),
    });
    assert!(timeout.downcast_ref::<PullTimeout>().is_some());
    assert!(timeout.downcast_ref::<HashMismatch>().is_none());

    let rejected: anyhow::Error = anyhow::Error::new(UpstreamVoucherRejected {
        reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
        bundle: None,
        proof_generation: None,
    });
    assert!(rejected.downcast_ref::<UpstreamVoucherRejected>().is_some());
    assert!(rejected.downcast_ref::<PullTimeout>().is_none());

    let shed: anyhow::Error = anyhow::Error::new(UpstreamRateLimited { label: None });
    assert!(shed.downcast_ref::<UpstreamRateLimited>().is_some());
    assert!(shed.downcast_ref::<PullStalled>().is_none());

    // Even with an added context layer, the plain `downcast_ref` the
    // orchestrator uses still recovers the sentinel (no `root_cause()` needed).
    let wrapped = timeout.context("added context in some future propagation path");
    assert!(wrapped.downcast_ref::<PullTimeout>().is_some());

    // `LocalPullFault` (#1145 review) is the one sentinel attached as a CONTEXT
    // layer rather than as the error itself — `anyhow!("voucher signing failed")
    // .context(LocalPullFault)` — and it is then wrapped again on the way up. If
    // this downcast ever stopped working, the exoneration arm would silently stop
    // firing and a node with a broken signer would go back to recording a local
    // `Unreachable` EWMA hit against every honest provider it tried. That failure is invisible
    // at the call site, so pin it here.
    let local = anyhow::anyhow!("voucher signing failed: bad key")
        .context(LocalPullFault)
        .context("self_pay");
    assert!(
        local.downcast_ref::<LocalPullFault>().is_some(),
        "a local fault must stay recoverable through the context layers above it"
    );
    assert!(
        local.downcast_ref::<PullStalled>().is_none(),
        "and must not be confused with a peer-attributable sentinel"
    );

    // The refusal sentinel (#1144) carries the wire code through the same
    // channel, so `classify_pull_failure` can read the refusal class instead
    // of folding every refusal to Unreachable.
    let refused: anyhow::Error =
        anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound));
    let recovered = refused
        .downcast_ref::<UpstreamRefused>()
        .map(|r| r.error().clone());
    assert_eq!(recovered, Some(StreamError::NotFound));
    assert!(refused.downcast_ref::<UpstreamVoucherRejected>().is_none());
}

/// Asserted on the real predicate `classify_pull_failure` consults: a refusal is
/// proof the peer ANSWERED, so no refusal class scores it. A healthy-but-empty
/// node must not take an `Unreachable` hit (local EWMA; ADR 008 has no
/// cross-node propagation) for honestly saying so.
#[test]
fn each_refusal_class_has_its_verdict() {
    assert_eq!(
        classify_refusal(&StreamError::NotFound),
        RefusalVerdict::Transient
    );
    assert_eq!(
        classify_refusal(&StreamError::Unfunded),
        RefusalVerdict::Transient
    );
    assert_eq!(
        classify_refusal(&StreamError::Declined),
        RefusalVerdict::DurableMiss
    );
    assert_eq!(
        classify_refusal(&StreamError::VoucherRejected {
            reason: VoucherRejectReason::PoolExhausted,
            bundle: None,
        }),
        RefusalVerdict::OurFault
    );
}

/// #2178: backpressure is a transport shed or an OPEN-stage refusal. A bare
/// mid-stream `NotFound` is not: the per-signer cap refuses
/// only at admission, and a source that serves some bytes and then refuses
/// must not reset the wait budget on every round. The open-stage `NotFound`
/// case needs a signed refusal, which only the client crate can build; the
/// `a_sole_coverer_backpressure_refusal_is_retried_not_dropped` integration
/// test drives it through a real signed `StreamResponse`.
#[test]
fn backpressure_is_a_transport_shed_or_an_open_stage_refusal() {
    assert_eq!(
        backpressure_verdict(&anyhow::Error::new(UpstreamRateLimited { label: None })),
        Some(PullVerdict::RateLimited)
    );
    for error in [
        StreamError::NotFound,
        StreamError::Unfunded,
        StreamError::Declined,
    ] {
        let mid_stream = anyhow::Error::new(UpstreamRefused::mid_stream(error.clone()));
        assert_eq!(
            backpressure_verdict(&mid_stream),
            None,
            "a mid-stream {error:?} is not backpressure"
        );
    }
    assert_eq!(
        backpressure_verdict(&anyhow::anyhow!("connection lost")),
        None
    );
}

/// An `Underpaid` with a watermark means our lane drifted and the resync budget
/// ran out: this lane is dead, the peer kept. Without a watermark the lane had
/// accepted nothing, so our own pricing is at fault on every candidate, and
/// the peer must not be suppressed for it.
#[test]
fn an_underpaid_rejection_blames_the_lane_or_our_pricing() {
    assert_eq!(
        voucher_verdict(VoucherRejectReason::Underpaid, true),
        PullVerdict::OurDeadLane(VoucherRejectReason::Underpaid)
    );
    assert_eq!(
        voucher_verdict(VoucherRejectReason::Underpaid, false),
        PullVerdict::OurLocalFault
    );
}

/// An `UnderFold` that reaches the classifier survived the drive loop's heal: it carried no
/// bundle, its bundle failed authentication or did not advance our ledger, or it ran out the
/// resume budget. Our lane to this peer is dead; the peer and the pool row are kept, as for the
/// other drifted-accounting reasons.
#[test]
fn an_under_fold_rejection_is_a_dead_lane() {
    for has_bundle in [true, false] {
        assert_eq!(
            voucher_verdict(VoucherRejectReason::UnderFold, has_bundle),
            PullVerdict::OurDeadLane(VoucherRejectReason::UnderFold)
        );
    }
}

/// A `PoolExhausted` rejection is a statement about OUR buyer pool, not the peer:
/// the deposit we fund the upstream from can no longer cover further credit. The
/// peer did nothing wrong, so this must be judged retryable with the peer KEPT —
/// not an `OurDeadLane` (which would suppress a healthy provider needlessly) nor
/// an `OurLocalFault` (which would tar the peer for our own funding gap). A top-up
/// and retry is the fix, which is exactly what `OurVoucherRetryable` drives.
#[test]
fn a_pool_exhaustion_is_a_retryable_topup_not_a_dead_channel() {
    assert_eq!(
        voucher_verdict(VoucherRejectReason::PoolExhausted, false),
        PullVerdict::OurVoucherRetryable(VoucherRejectReason::PoolExhausted),
        "our own drained pool keeps the healthy peer and retries after a top-up"
    );
}

/// The #1145-review refinement: exonerating a refusal is not the same as believing
/// it. How long we suppress a (peer, hash) must match how much the refusal actually
/// proves — and for the codes below it proves rather little.
#[test]
fn only_a_durable_refusal_earns_the_full_suppression_ttl() {
    // A peer that declines the blob will say the same thing in a minute. Worth
    // the full TTL.
    assert_eq!(
        classify_refusal(&StreamError::Declined),
        RefusalVerdict::DurableMiss,
        "Declined is a lasting fact about this (peer, hash)"
    );
    // `NotFound` is the one that matters. `wire_error` collapses `UnknownChannel`
    // (the window where the upstream's chain watcher has not yet seen the pool
    // WE just opened) onto it, so pool existence cannot be probed. At the full
    // TTL that blackholes a healthy peer for five minutes over a condition that
    // has already passed. `Unfunded` is our own upstream funding short at the
    // peer, likewise transient and no fault of the peer.
    for error in [StreamError::NotFound, StreamError::Unfunded] {
        assert_eq!(
            classify_refusal(&error),
            RefusalVerdict::Transient,
            "{error:?} is not durable evidence about this (peer, hash)"
        );
    }
    // And the brief suppression has to actually be brief — a `REFUSAL_SUPPRESSION_TTL`
    // raised to the cache's own TTL would silently restore the bug.
    assert!(
        REFUSAL_SUPPRESSION_TTL < Duration::from_mins(5),
        "the transient TTL must stay well under the negative cache's own"
    );
}

/// A refusal that is OUR fault must leave the peer entirely untouched — not scored,
/// and not suppressed either. It still holds the blob; the problem is our voucher.
#[test]
fn our_own_payment_fault_neither_scores_nor_suppresses_the_peer() {
    assert_eq!(
        classify_refusal(&StreamError::VoucherRejected {
            reason: VoucherRejectReason::BadSignature,
            bundle: None,
        }),
        RefusalVerdict::OurFault
    );
}

/// A fault in THIS node must never be scored against the peer we happened to be
/// talking to when it surfaced.
///
/// The stakes are why this is pinned rather than assumed: the buyer key that signs the
/// ADR 005 client binding is the same key that signs vouchers, and the binding is signed
/// BEFORE the stream opens, on every candidate. So a node whose signer is broken does not
/// mis-score one provider — it walks the entire candidate list handing out `Unreachable`
/// (a local EWMA hit; ADR 008 scoring is local-only) to every honest peer it meets, on the
/// strength of its own defect. The catch-all is only ever one misplaced arm away.
///
/// **What this test does and does not prove.** It pins the LADDER: that a `LocalPullFault`
/// buried under the context layers the real call stack adds still beats every arm below
/// it. It does NOT prove any production site attaches the marker — a pure function over
/// an `anyhow::Error` cannot, and the version of this test that pretended otherwise was
/// the reason the whole review round exists. That one hand-built the error WITH
/// `.context(LocalPullFault)` and then asserted the ladder found `LocalPullFault`: true
/// by construction, unfailable, and green even with every marker stripped from the crate.
///
/// The wiring is guarded where the wiring lives:
/// `node_origin_an_unverifiable_voucher_is_a_local_fault_not_a_payment_one` drives a
/// real pull whose signature the upstream cannot verify (the production shape of "our
/// buyer key is broken") and asserts `node_pull_local_fault_total` moves while the
/// peer is left unscored.
#[test]
fn a_local_fault_outranks_every_arm_that_blames_the_peer() {
    // Marker under the context layers the real call stack adds on the way out — the
    // shape production produces, though (necessarily) assembled here.
    let err = anyhow::anyhow!("voucher signing failed: signer unavailable")
        .context(LocalPullFault)
        .context("bind the upstream request")
        .context("pull from candidate");

    assert_eq!(
        pull_verdict(&err),
        PullVerdict::OurLocalFault,
        "a local fault must outrank the catch-all — reaching it tars every honest \
         provider as unreachable"
    );

    // And it must outrank the arm that sits directly below it. `UpstreamRefused` is the
    // one that would otherwise catch a local fault raised while a refusal was in flight,
    // and it exonerates the peer for the WRONG reason — quietly, and without the
    // `node_pull_local_fault_total` an operator needs to see that this node is broken.
    let refused_too = anyhow::anyhow!("encode failed")
        .context(LocalPullFault)
        .context(UpstreamRefused::mid_stream(StreamError::NotFound));
    assert_eq!(
        pull_verdict(&refused_too),
        PullVerdict::OurLocalFault,
        "a local fault must win over a refusal on the same chain: the refusal is a \
         symptom, the broken node is the cause"
    );
}

/// A clean leg that moved neither frontier (#2194) gets its own verdict, even under
/// context layers: the catch-all would score the upstream `Unreachable` for a leg it
/// served and was paid for.
#[test]
fn a_no_progress_leg_is_not_scored_unreachable() {
    let err = anyhow::Error::new(LegNoProgress {
        offset: 0,
        len: 16_384,
        paid_frontier: 0,
        delivered_frontier: 0,
        paid_wire: 0,
    })
    .context("pull from candidate");
    assert_eq!(pull_verdict(&err), PullVerdict::LegNoProgress);
}

/// Only a fault in THIS node may stop a failed pull answering `NotFound` (#1560).
///
/// The asymmetry is the whole point, and both halves of it can regress silently. Widen
/// it and a node with one wedged lane to one provider tells every client "do not
/// retry this node" — steering traffic off a node that is fine for every other provider
/// and every other blob. Narrow it (or let a future verdict fall into a catch-all) and
/// we are back to the bug: a broken buyer key signs a client a `NotFound` about content
/// that exists and is reachable, and the client caches OUR defect as a fact about the
/// blob.
///
/// The build-break guarantee lives in `PullMiss::for_verdict`'s catch-all-free `match`,
/// NOT here: this test iterates a hand-written list, so a new [`PullVerdict`] variant
/// would compile fine and simply go untested. What the test pins is the DECISION each
/// existing variant made — the thing a future refactor could flip without noticing.
///
/// Known edges, stated precisely because the guarantee is narrower than it looks:
/// `for_verdict` matches `OurDeadLane(_)` / `OurVoucherRetryable(_)` on their payloads,
/// so a new `VoucherRejectReason` inherits `Clean` without a build break — acceptable,
/// because `voucher_verdict` IS exhaustive over all ten and already routes the node-wide
/// reasons (`BadSignature`, `WrongSigner`) to `OurLocalFault` before this function sees
/// them. `RefusalVerdict`'s three discriminants are spelled out so a fourth does break
/// the build.
#[test]
fn only_our_own_fault_may_withhold_a_not_found() {
    let reason = VoucherRejectReason::SpendingCapExhausted;
    for verdict in [
        PullVerdict::Oversize,
        PullVerdict::RateCeiling,
        PullVerdict::OurDeadline,
        PullVerdict::Stalled,
        PullVerdict::OurDeadLane(reason),
        PullVerdict::OurVoucherRetryable(reason),
        PullVerdict::Refused(RefusalVerdict::Transient),
        PullVerdict::Refused(RefusalVerdict::OurFault),
        PullVerdict::Refused(RefusalVerdict::DurableMiss),
        PullVerdict::Corruption,
        PullVerdict::LegNoProgress,
        PullVerdict::Unreachable,
        PullVerdict::RateLimited,
    ] {
        assert_eq!(
            PullMiss::for_verdict(verdict),
            PullMiss::Clean,
            "{verdict:?} is not an UNEXPECTED failure of this node, so it must still \
             answer a clean miss"
        );
    }

    assert_eq!(
        PullMiss::for_verdict(PullVerdict::OurLocalFault),
        PullMiss::LocalFault,
        "a broken signer / encode / range is the one verdict that makes a `NotFound` a \
         false claim about the content"
    );
}

/// A [`PullMiss::BelowMargin`] answers the same wire code as [`PullMiss::Clean`]:
/// declining an unprofitable relay must not leak the serve-economics floor to the
/// client as a distinct signal (that would let a client infer this node's buy
/// ceiling by probing for the wire difference).
#[test]
fn below_margin_is_wire_identical_to_clean() {
    assert!(matches!(
        miss_answer(PullMiss::Clean),
        Ok(OriginFetch::NotFound)
    ));
    assert!(matches!(
        miss_answer(PullMiss::BelowMargin),
        Ok(OriginFetch::NotFound)
    ));
}

/// A [`PullMiss::BelowMargin`] never masks, and is never masked by, a
/// [`PullMiss::LocalFault`] in the fold: `LocalFault` outranks everything else
/// regardless of position, in both argument orders. A [`PullMiss::BelowMargin`]
/// DOES outrank a plain [`PullMiss::Clean`], again in both orders, because the
/// fold's job is to carry the strongest signal seen anywhere in the walk forward
/// — losing a below-margin classification to a later clean miss would report a
/// genuinely empty walk when an economics refusal actually happened partway
/// through it.
#[test]
fn below_margin_never_masks_or_is_masked_by_a_local_fault() {
    assert_eq!(
        PullMiss::LocalFault.or(PullMiss::BelowMargin),
        PullMiss::LocalFault,
        "a local fault must survive being folded against a later economics refusal"
    );
    assert_eq!(
        PullMiss::BelowMargin.or(PullMiss::LocalFault),
        PullMiss::LocalFault,
        "a local fault reached later in the walk must still win over an earlier \
         economics refusal"
    );
    assert_eq!(
        PullMiss::BelowMargin.or(PullMiss::Clean),
        PullMiss::BelowMargin,
        "an economics refusal must survive being folded against a later clean miss"
    );
    assert_eq!(
        PullMiss::Clean.or(PullMiss::BelowMargin),
        PullMiss::BelowMargin,
        "an economics refusal reached later in the walk must still win over an \
         earlier clean miss"
    );
}

/// A local fault LATCHES across a walk: one candidate's fault is not erased by the next
/// candidate's honest miss.
///
/// The order matters in both directions, which is why both are asserted. A walk folds
/// left-to-right over whatever order the ranker produced, so a fault at position 1
/// followed by clean misses, and clean misses followed by a fault at position 3, are the
/// same story and must reach the same answer — otherwise the wire code an operator sees
/// depends on where in the ranking the broken pull happened to land.
#[test]
fn a_local_fault_survives_the_rest_of_the_walk() {
    assert_eq!(
        PullMiss::Clean.or(PullMiss::Clean),
        PullMiss::Clean,
        "a walk of honest misses is an honest miss"
    );
    assert_eq!(
        PullMiss::LocalFault.or(PullMiss::Clean),
        PullMiss::LocalFault,
        "a later clean miss must not overwrite an earlier fault of ours"
    );
    assert_eq!(
        PullMiss::Clean.or(PullMiss::LocalFault),
        PullMiss::LocalFault,
        "a fault reached late in the walk counts the same as one reached first"
    );
    assert_eq!(
        PullMiss::LocalFault.or(PullMiss::LocalFault),
        PullMiss::LocalFault
    );
}

/// A refusal that arrives MID-STREAM carries the same wire code, and therefore the same
/// meaning, as one that arrives at the open. Stringifying it
/// (`bail!(\"stream failed: {e:?}\")`) would fall through every downcast to the
/// catch-all and score the peer `Unreachable` — the same mis-attribution #1144
/// forecloses at the open stage, one stage later.
#[test]
fn a_mid_stream_refusal_is_judged_by_its_wire_code_not_the_catch_all() {
    for (error, want) in [
        (StreamError::NotFound, RefusalVerdict::Transient),
        (StreamError::Unfunded, RefusalVerdict::Transient),
        (StreamError::Declined, RefusalVerdict::DurableMiss),
    ] {
        // Exactly what the receive loops now raise — wrapped, because a real one comes
        // up through the pull path's `.context` layers and `downcast_ref` must still
        // find it.
        let err = anyhow::Error::new(UpstreamRefused::mid_stream(error.clone()))
            .context("receive and pay")
            .context("pull from candidate");
        assert_eq!(
            pull_verdict(&err),
            PullVerdict::Refused(want),
            "a mid-stream {error:?} must be judged as a refusal, not fall to the catch-all"
        );
    }
}

/// The residual arm has to stay reachable — it is how a genuinely dead node gets
/// scored, and an over-eager sentinel above it would silently stop scoring anyone.
#[test]
fn an_unrecognised_failure_still_scores_the_peer() {
    let err = anyhow::anyhow!("connection refused").context("dial provider");
    assert_eq!(pull_verdict(&err), PullVerdict::Unreachable);
}

/// A store import failure as iroh-blobs reports a failed data-file write — a
/// full or read-only disk among them: a kind-less `io::Error` under a context,
/// inside [`decdn_cache::CacheError::Store`].
fn store_fault() -> decdn_cache::CacheError {
    let io = iroh_blobs::api::Error::from(std::io::Error::other("write batch failed"));
    decdn_cache::CacheError::Store(anyhow::Error::new(io).context("admit_bao_stream: store import"))
}

/// This node's own store failing under a pull is not the peer's fault (#2286): a
/// store fault on the write path (the `CacheError` is the anyhow root), the same
/// fault on a store query (boxed under `RangedStoreError::Backend`), and an
/// internal fault all rule `OurLocalFault`, never the `Unreachable` catch-all.
#[test]
fn a_local_store_fault_is_not_scored_against_the_peer() {
    let written = anyhow::Error::from(store_fault()).context("pull from candidate");
    assert_eq!(
        pull_verdict(&written),
        PullVerdict::OurLocalFault,
        "{written:#}"
    );

    let queried = anyhow::Error::new(decdn_bao_range::RangedStoreError::Backend(Box::new(
        store_fault(),
    )))
    .context("pull from candidate");
    assert_eq!(
        pull_verdict(&queried),
        PullVerdict::OurLocalFault,
        "{queried:#}"
    );

    let internal = anyhow::Error::from(decdn_cache::CacheError::Internal(anyhow::anyhow!(
        "reader invariant broken"
    )))
    .context("pull from candidate");
    assert_eq!(
        pull_verdict(&internal),
        PullVerdict::OurLocalFault,
        "{internal:#}"
    );
}

/// A stream the peer ended early fails the admit with a
/// [`decdn_cache::CacheError::Feed`]. That short delivery is the peer's, so the
/// store-fault arm must not excuse it.
#[test]
fn a_truncated_delivery_still_scores_the_peer() {
    let truncated = anyhow::Error::from(decdn_cache::CacheError::Feed(
        anyhow::Error::from(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "stream ended early",
        ))
        .context("admit_bao_stream: decode/feed failed"),
    ))
    .context("pull from candidate");
    assert_eq!(
        pull_verdict(&truncated),
        PullVerdict::Unreachable,
        "{truncated:#}"
    );
}

/// The ladder's ORDER is load-bearing and invisible to the compiler. A stall is the one
/// timeout that scores the peer; our own deadline is the one that must not.
#[test]
fn our_deadline_and_their_silence_get_opposite_verdicts() {
    let ours = anyhow::Error::new(PullTimeout {
        after: Duration::from_secs(20),
    })
    .context("pull from candidate");
    let theirs = anyhow::Error::new(PullStalled {
        after: Duration::from_secs(20),
    })
    .context("pull from candidate");
    assert_eq!(pull_verdict(&ours), PullVerdict::OurDeadline);
    assert_eq!(pull_verdict(&theirs), PullVerdict::Stalled);
}

/// A transport-level `APP_ERR_RATE_LIMITED` shed (#1986) is the same event as
/// a handler-level load-shed `NotFound`, and must get the same verdict: brief
/// suppression, no reputation. Scoring it `Unreachable` would record a 0.0 EWMA
/// sample for a peer that answered, and every prober in a `GlobalFull` window
/// would record one at once.
#[test]
fn a_rate_limit_shed_is_suppressed_not_scored() {
    let shed = anyhow::Error::new(UpstreamRateLimited {
        label: Some("global-full".to_owned()),
    })
    .context("open_bi failed")
    .context("pull from candidate");
    assert_eq!(pull_verdict(&shed), PullVerdict::RateLimited);
    assert_eq!(
        PullMiss::for_verdict(PullVerdict::RateLimited),
        PullMiss::Clean,
        "a peer shedding load says nothing about THIS node, so the client gets an \
         honest miss"
    );
}

/// Shared in-memory sink for the captured tracing output.
#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Every recorded outcome names its peer and the score it leaves. The
/// `node_pull_*` counters are unlabeled aggregates, so without this event an
/// `Unreachable` penalty is a counter tick that no operator can attribute.
#[test]
fn a_reputation_penalty_names_the_peer_and_its_new_score() -> anyhow::Result<()> {
    // `docs/runbook.md` names this filter string verbatim.
    assert_eq!(REPUTATION_LOG_TARGET, "decdn::reputation");
    let local_rep = LocalReputation::new(decdn_reputation::LocalReputationConfig::default())?;
    let metrics = Metrics::new();
    let pk = iroh::SecretKey::generate().public();

    let log = CapturedLog::default();
    let sink = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        fold_outcome(&local_rep, &metrics, pk, &Outcome::Unreachable);
    });

    let text = String::from_utf8(
        log.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )?;
    let line = text
        .lines()
        .find(|l| l.contains(REPUTATION_LOG_TARGET))
        .ok_or_else(|| anyhow::anyhow!("no `{REPUTATION_LOG_TARGET}` event; got:\n{text}"))?;
    assert!(line.contains(&format!("peer={pk}")), "{line}");
    assert!(line.contains("outcome=Unreachable"), "{line}");
    let logged: f64 = line
        .split_once("score=")
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .ok_or_else(|| anyhow::anyhow!("no score field: {line}"))?
        .parse()?;
    assert!(
        (logged - local_rep.score(pk)).abs() < 1e-3,
        "the event carries the post-fold score: {line}"
    );
    let scrape = metrics.encode()?;
    assert!(
        scrape
            .lines()
            .any(|l| l == "decdn_node_pull_unreachable_total 1"),
        "the aggregate still counts the penalty:\n{scrape}"
    );
    Ok(())
}

/// A probed holder with `coverage` over a blob of `total_bytes_hint` bytes.
fn holder(coverage: Coverage, total_bytes_hint: Option<u64>) -> Candidate {
    Candidate {
        node_id: [0; 32],
        rate_per_mb: 1,
        rtt_ms: 1,
        reputation: 1.0,
        region: String::new(),
        stake: 0,
        coverage,
        total_bytes_hint,
    }
}

/// #2195: the span check that decides whether the ranged path adds the
/// origin-directory candidates. Three blocks, sized by the holders' own
/// hints.
#[test]
fn coverage_spans_needs_every_block_from_a_sized_holder() {
    let size = 3 * decdn_protocol::DISCOVERY_BLOCK_BYTES;
    let blocks = |set: &[u32]| Coverage::from_block_indices(3, set.iter().copied());

    assert!(
        coverage_spans(&[holder(Coverage::full(3), Some(size))]),
        "one whole holder spans the blob"
    );
    assert!(
        coverage_spans(&[
            holder(blocks(&[0, 1]), Some(size)),
            holder(blocks(&[2]), Some(size)),
        ]),
        "partials whose union covers every block span the blob"
    );
    assert!(
        !coverage_spans(&[
            holder(blocks(&[0]), Some(size)),
            holder(blocks(&[0, 1]), Some(size)),
        ]),
        "partials that leave a block uncovered do not span the blob"
    );
    assert!(
        !coverage_spans(&[holder(blocks(&[0, 1]), None)]),
        "a holder that reports no size spans nothing"
    );
    assert!(!coverage_spans(&[]), "an empty candidate set spans nothing");
    // A two-block holder whose wire bitmap also sets bit 2 claims nothing
    // past its own size, so a third block stays uncovered.
    let spurious = holder(
        Coverage::full(3),
        Some(2 * decdn_protocol::DISCOVERY_BLOCK_BYTES),
    );
    assert!(
        !coverage_spans(&[spurious, holder(blocks(&[0]), Some(size))]),
        "a coverage bit past the holder's own block count does not count"
    );
}
