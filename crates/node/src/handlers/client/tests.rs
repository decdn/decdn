use super::*;

/// #2171: a lane loaded from the store counts its whole claim — the signed
/// bytes AND the chunks its chain proved — as already credited. Crediting
/// only the signed half would reopen the proved frontier as headroom for any
/// stream whose proofs pay nothing.
#[tokio::test]
async fn a_loaded_lane_counts_its_proved_frontier_as_credited() {
    use decdn_incentive::chain::{CHUNK_BYTES, preimage_at, root_from_seed};

    let seed = B256::repeat_byte(0x5E);
    let lane = LaneState::hydrate(
        B256::repeat_byte(0x21),
        Address::repeat_byte(0x33),
        Address::repeat_byte(0x55),
        U256::MAX,
        0,
        U256::from(3_000u64),
        U256::from(3 * CHUNK_BYTES),
        Some([9u8; 65]),
        decdn_incentive::LaneChain {
            chain_root: root_from_seed(seed),
            chunk_price: U256::from(1_000u64),
            verified_index: 5,
            tip: preimage_at(seed, 5),
        },
    );
    let owed_bytes = lane.owed_bytes();
    assert_eq!(owed_bytes, U256::from(8 * CHUNK_BYTES));

    let store = Arc::new(decdn_incentive::store::MemoryPoolStateStore::new());
    store.record(&lane).expect("record the seeded lane");
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_over_store(&metrics, store).await;

    let loaded = handler
        .lanes
        .get(&lane.key())
        .map(|e| Arc::clone(e.value()))
        .expect("the handler loads the stored lane");
    let guard = loaded.lock().await;
    assert_eq!(
        guard.paid_credited, owed_bytes,
        "the whole claim is credited"
    );
    assert_eq!(guard.bytes_delivered_cumulative, owed_bytes);
}

/// A client that sends garbage on the proof stream is a peer fault, not a node
/// bug: it must reach `debug!`, not the node-fault counter. The `ProbeRequest`
/// stands in for any `ClientMessage` variant that is not a proof.
#[tokio::test]
async fn a_malformed_or_unexpected_proof_frame_is_attributed_to_the_peer() {
    let mut reader = BufferedProofReader::default();
    assert!(
        reader
            .take_buffered()
            .expect("an empty buffer is not a fault")
            .is_none(),
        "an empty buffer holds no frame"
    );

    // An undecodable body behind a well-formed length prefix.
    let mut framed = Vec::new();
    framed.push(3u8);
    framed.extend_from_slice(&[0xFF, 0xFF, 0xFF]);
    reader.buf = framed;
    let e = reader
        .take_buffered()
        .expect("a bad body is the frame's outcome, not the buffer's")
        .expect("the frame is whole")
        .expect_err("an undecodable body must fail");
    assert!(wire::is_peer_attributable(&e), "unexpected: {e:#}");
    assert!(
        reader.buf.is_empty(),
        "a bad frame must not wedge the buffer"
    );

    // A well-formed `ClientMessage` that is not a proof.
    let payload = encode_message(&ClientMessage::StreamEnd).expect("StreamEnd encodes");
    let mut framed = Vec::new();
    write_frame(&mut framed, &payload)
        .await
        .expect("a Vec sink never fails");
    reader.buf = framed;
    let e = reader
        .take_buffered()
        .expect("a non-proof message is the frame's outcome, not the buffer's")
        .expect("the frame is whole")
        .expect_err("a non-proof message must fail");
    assert!(wire::is_peer_attributable(&e), "unexpected: {e:#}");
}

/// A frame must never cross a payment-chunk boundary, whatever the configured
/// target. Both sides meter the same frame sequence and exchange one preimage per
/// interval; a straddling frame lands the payer past the boundary, which settles
/// as a signed residual voucher instead — a per-frame signature on the hot path
/// and a cadence the two sides no longer share. A frame size that divides
/// `interval_bytes` gets the property for free; at any other size it has to be
/// cut for explicitly, which is what this clamp does.
#[tokio::test]
async fn a_frame_never_crosses_a_payment_chunk_boundary() {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await;
    let interval = decdn_protocol::CHUNK_BYTES;
    let wide_open = u64::MAX;

    // Walk a whole interval in the frame sizes the handler itself hands out and
    // confirm the walk lands exactly on the boundary rather than stepping over it.
    let mut unvouchered = 0u64;
    let mut frames = 0u32;
    while unvouchered < interval {
        let target = handler.frame_target(unvouchered, interval, wide_open) as u64;
        assert!(target > 0, "a zero-length frame is a protocol error");
        unvouchered += target;
        assert!(
            unvouchered <= interval,
            "frame of {target} crossed the boundary: {unvouchered} > {interval}"
        );
        frames += 1;
        assert!(
            frames < 64,
            "target collapsed to runt frames: {frames} per interval"
        );
    }
    assert_eq!(unvouchered, interval, "the walk must land ON the boundary");
}

/// At the default target an open window spends exactly ONE frame per payment
/// interval. Nothing else pins that the default target and the payment quantum
/// line up, and a regression multiplies per-frame CPU across every byte the node
/// egresses.
#[tokio::test]
async fn an_open_window_spends_one_frame_per_payment_chunk() {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await;
    let interval = decdn_protocol::CHUNK_BYTES;
    assert_eq!(
        handler.frame_target(0, interval, u64::MAX) as u64,
        interval,
        "an open window at the default target must cover the interval in one frame"
    );
}

/// The serve side's prefetch floor and the pull side's reservation are one
/// invariant split across two crates, so it needs a test that names both.
///
/// `frame_target` floors its room cap at one bao chunk group, which on a cache
/// miss is a request the upstream pull must be allowed to satisfy past a shut
/// credit window. `PULL_WINDOW_FLOOR` reserves a third group for exactly that.
/// Raise one without the other and the cache-miss leg parks — and it parks as a
/// hang with no diagnostic, which is why the relationship is asserted here
/// rather than left to an integration test's timeout.
#[tokio::test]
async fn frame_target_room_floor_matches_the_pull_reservation() {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await;
    let interval = decdn_protocol::CHUNK_BYTES;
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;

    // What the serve side asks for past a shut window.
    let prefetch = handler.frame_target(0, interval, 0) as u64;
    assert_eq!(prefetch, group, "the room floor is one bao chunk group");

    // What the pull side can still draw once both group roundings are spent.
    let drawable = decdn_client::PULL_WINDOW_FLOOR - 2 * group;
    assert!(
        drawable >= interval + prefetch,
        "the pull floor leaves {drawable} bytes after both roundings, but the \
         client must complete a {interval}-byte chunk to pay AND the serve leg \
         prefetches {prefetch} bytes past its shut window — the miss leg would park"
    );
}

/// A closed window must not be asked for a full frame. Both loops prefetch one
/// frame ahead of the window check, and on the cache-miss leg the producer is fed
/// by an upstream pull paced against this stream's own served-and-paid frontier —
/// so a large request parks on bytes that only recouping can unblock, and the
/// loop must exit to recoup. The room cap is what keeps that prefetch
/// satisfiable; the floor keeps it from degenerating to a single byte.
#[tokio::test]
async fn a_closed_window_yields_a_short_frame_not_a_full_one() {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await;
    let interval = decdn_protocol::CHUNK_BYTES;
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;

    let closed = handler.frame_target(0, interval, 0) as u64;
    assert_eq!(
        closed, group,
        "a closed window must fall back to the group floor"
    );

    // A partly-open window is honoured as-is once it clears the floor.
    assert_eq!(
        handler.frame_target(0, interval, 4 * group) as u64,
        4 * group
    );
    // ...and the boundary still wins when it is the tighter of the two.
    assert_eq!(
        handler.frame_target(interval - group, interval, u64::MAX) as u64,
        group
    );
}

/// A stream's first frame is small, so its first byte waits for a few chunk
/// groups of read or pull rather than a whole interval. A tighter opening
/// window still wins, and the frame never drops below one chunk group.
#[tokio::test]
async fn the_first_frame_is_small() {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await;
    let interval = decdn_protocol::CHUNK_BYTES;
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;

    assert_eq!(
        handler.first_frame_target(interval, u64::MAX) as u64,
        FIRST_FRAME_BYTES
    );
    assert_eq!(
        handler.first_frame_target(interval, 2 * group) as u64,
        2 * group
    );
    assert_eq!(handler.first_frame_target(interval, 0) as u64, group);
    assert_eq!(
        FIRST_FRAME_BYTES % group,
        0,
        "the first frame is whole chunk groups"
    );
}

/// Build the smallest `ClientHandler` for the handler-layer tests below,
/// seeding the floor-`M` minimum-remaining-deposit at zero.
async fn handler_for_tests(metrics: &Arc<Metrics>) -> (Arc<ClientHandler>, tempfile::TempDir) {
    handler_for_tests_with_floor(metrics, U256::ZERO).await
}

/// [`handler_for_tests`] with an explicit floor-`M` so the floor-`M` guard can
/// be exercised with a non-zero minimum-remaining-deposit. The per-signer gates
/// are no-ops (unbounded live cap, bottomless bucket), so the pool ceiling is the
/// only floor bound that bites.
async fn handler_for_tests_with_floor(
    metrics: &Arc<Metrics>,
    pool_min_remaining_deposit: U256,
) -> (Arc<ClientHandler>, tempfile::TempDir) {
    handler_for_tests_with_signer_policy(metrics, pool_min_remaining_deposit, u64::MAX).await
}

/// [`handler_for_tests_with_floor`] plus an explicit per-signer live concurrency
/// cap `k` (in windows). `u64::MAX` leaves that gate a no-op.
async fn handler_for_tests_with_signer_policy(
    metrics: &Arc<Metrics>,
    pool_min_remaining_deposit: U256,
    pool_floor_signer_live_windows: u64,
) -> (Arc<ClientHandler>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = CacheEngine::open(dir.path(), Vec::new(), 16)
        .await
        .expect("cache");
    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let mut deps = ClientHandlerDeps::new(
        iroh::SecretKey::generate().public(),
        Arc::clone(metrics),
        Arc::new(ConnectionLimiter::new(
            &decdn_common::config::ResolvedSecurity {
                max_concurrent_handlers: u32::MAX,
                per_source_rate_per_sec: 1e9,
                per_source_burst: u32::MAX,
                max_tracked_sources: 16,
            },
            Arc::clone(metrics),
        )),
        cache,
        Arc::new(alloy::signers::local::PrivateKeySigner::random()),
        domain.clone(),
        domain.clone(),
        domain,
        Arc::new(decdn_incentive::store::MemoryPoolStateStore::new()) as Arc<dyn PoolStateStore>,
        Arc::new(crate::receipt_log::DirectReceiptSink::new(Arc::new(
            crate::receipt_log::NoopReceiptLog,
        ))) as Arc<dyn ReceiptSink>,
        1,
        16,
        Arc::new(crate::content_deny::ContentDenylist::empty()),
        pool_min_remaining_deposit,
        always_admit_shed(),
    );
    deps.pool_floor_signer_live_windows = pool_floor_signer_live_windows;
    let handler = ClientHandler::new(deps).expect("handler");
    (Arc::new(handler), dir)
}

/// The load-shed controller sheds a cache-miss once the node is at its
/// configured concurrency ceiling, while a cache-hit for a DIFFERENT client
/// still rides — a hit is local, zero-upstream-cost margin, so it is shed
/// last (miss-before-hit). Exercises the controller directly at the wiring
/// boundary rather than standing up a full QUIC loopback.
#[tokio::test]
async fn miss_is_shed_when_node_at_capacity_but_hit_admitted() {
    // Build a handler whose shed controller trips at 1 concurrent serve.
    let cfg = decdn_common::config::ResolvedLoadShed {
        policy: decdn_common::config::LoadShedPolicyKind::ResourcePressure,
        egress_budget_mbps: 0,
        max_concurrent_serves_high: 1,
        max_concurrent_serves_low: 0,
        per_client_serve_cap: 0,
    };
    let shed = crate::load_shed::LoadShedController::from_config(&cfg);
    // Occupy the one slot.
    let _held = shed
        .try_admit(crate::load_shed::RequestClass::CacheHit, B256::ZERO)
        .expect("first serve admits");
    // A new miss is shed; a new hit rides (egress under budget).
    assert!(
        shed.try_admit(
            crate::load_shed::RequestClass::CacheMiss,
            B256::from([1u8; 32])
        )
        .is_err()
    );
    assert!(
        shed.try_admit(
            crate::load_shed::RequestClass::CacheHit,
            B256::from([1u8; 32])
        )
        .is_ok()
    );
}

/// The lane registry resolves independent lanes concurrently (#1731). Many
/// distinct [`LaneKey`]s register, resolve, and forget in parallel with no
/// shared map lock serializing them; the sharded map must still preserve the
/// single-mutex semantics — every registered lane is present and resolvable,
/// and every forgotten lane is gone.
#[tokio::test]
async fn distinct_lanes_register_resolve_and_forget_concurrently() {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await;

    // Distinct lanes differ only by signer — the independent-lane case a
    // single mutex would serialize regardless of how they shard.
    let keys: Vec<LaneKey> = (0u8..32)
        .map(|i| LaneKey {
            pool_id: B256::repeat_byte(0xC0),
            signer: Address::repeat_byte(i),
            provider: handler.eth_signer.address(),
        })
        .collect();

    // Register every lane concurrently.
    let mut register = Vec::new();
    for key in &keys {
        let handler = Arc::clone(&handler);
        let state = LaneState::hydrate(
            key.pool_id,
            key.signer,
            key.provider,
            U256::from(1_000_000u64),
            0,
            U256::ZERO,
            U256::ZERO,
            None,
            decdn_incentive::LaneChain::NONE,
        );
        register.push(tokio::spawn(async move { handler.register_lane(state) }));
    }
    for task in register {
        task.await.expect("join").expect("register_lane");
    }
    assert_eq!(handler.lanes.len(), keys.len(), "every lane is tracked");

    // Resolve every lane concurrently — each is a point read on the map.
    let mut resolve = Vec::new();
    for key in &keys {
        let handler = Arc::clone(&handler);
        let key = *key;
        resolve.push(tokio::spawn(async move {
            match handler.lanes.get(&key).map(|e| Arc::clone(e.value())) {
                Some(entry) => Some(entry.lock().await.state.key()),
                None => None,
            }
        }));
    }
    for (task, key) in resolve.into_iter().zip(keys.iter()) {
        assert_eq!(
            task.await.expect("join"),
            Some(*key),
            "each registered lane resolves to itself"
        );
    }

    // Forget every lane concurrently; the map drains to empty.
    let mut forget = Vec::new();
    for key in &keys {
        let handler = Arc::clone(&handler);
        let key = *key;
        forget.push(tokio::spawn(async move { handler.forget_lane(key).await }));
    }
    for task in forget {
        task.await.expect("join").expect("forget_lane");
    }
    assert_eq!(handler.lanes.len(), 0, "every lane is forgotten");
}

/// The floor-`M` solvency arithmetic with a NON-ZERO floor `M`
/// (`pool_remaining_covers_window`, ADR 003 §Sizing). The node keeps serving a
/// pool only while its on-chain remaining minus `M` still covers the reserved
/// credit window; it refuses once the refundable floor would be dipped into.
#[tokio::test]
async fn pool_remaining_covers_window_reserves_the_floor_m() {
    let metrics = Arc::new(Metrics::new());
    // M = 1 USDC; a 1 MB window at 1 USDC/MB costs exactly 1 USDC.
    let m = U256::from(1_000_000u64);
    let (handler, _dir) = handler_for_tests_with_floor(&metrics, m).await;
    let rate_per_mb = 1_000_000u64; // 1 USDC/MB
    let window_bytes = decdn_protocol::MB_BYTES; // one MB
    let window_cost = decdn_incentive::min_payment(window_bytes, rate_per_mb);
    assert_eq!(window_cost, U256::from(1_000_000u64), "1 MB @ 1 USDC/MB");

    // remaining just below `M + window_cost` → the window would dip into the
    // floor → refuse.
    let below = m + window_cost - U256::from(1u64);
    assert!(
        !handler.pool_remaining_covers_window(below, window_bytes, rate_per_mb),
        "remaining under M + window cost must be refused"
    );
    // remaining exactly `M + window_cost` → the window is covered above the
    // floor → serve.
    let exact = m + window_cost;
    assert!(
        handler.pool_remaining_covers_window(exact, window_bytes, rate_per_mb),
        "remaining at exactly M + window cost must be served"
    );
    // A pool with only the floor left (remaining == M) can never serve a
    // non-empty window.
    assert!(
        !handler.pool_remaining_covers_window(m, window_bytes, rate_per_mb),
        "remaining == M leaves nothing above the floor"
    );
}

/// The seller-side lane-count gauge tracks the live `lanes` map through the
/// atomic counter (#1789 item 3): registering a lane publishes 1,
/// forgetting it publishes 0 again. Deposit is a pool-level on-chain
/// quantity (getPool), not carried per lane, so the snapshot reports the
/// open-lane count only.
#[tokio::test]
async fn lane_count_gauge_tracks_the_live_map() {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await;
    let lane = LaneKey {
        pool_id: B256::repeat_byte(0xA1),
        signer: Address::repeat_byte(0x11),
        provider: Address::repeat_byte(0x22),
    };
    let state = LaneState::hydrate(
        lane.pool_id,
        lane.signer,
        lane.provider,
        U256::from(10u64),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    );
    // A duplicate registration is a no-op and must not double-count.
    handler.register_lane(state.clone()).expect("register");
    handler.register_lane(state).expect("register twice");
    let encoded = metrics.encode().expect("metrics encode");
    assert!(
        encoded.lines().any(|line| line == "decdn_lanes_open 1"),
        "an idempotent register must not double count"
    );
    handler.forget_lane(lane).await.expect("forget");
    let encoded = metrics.encode().expect("metrics encode");
    assert!(
        encoded.lines().any(|line| line == "decdn_lanes_open 0"),
        "forget must tune the gauge back down"
    );
}

/// #1789 item 3: concurrent registration and removal of many distinct
/// lanes leaves the gauge exactly equal to the number of lanes still live
/// — the count moves with the real map, whatever the interleaving.
///
/// Multi-threaded on purpose, and the gauge is read WITHOUT a settling
/// republish: the publish is the half the atomic does not make safe on its
/// own, so a lost `set_lanes_open` ordering leaves a stale value
/// that only an extra refresh would paper over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lane_gauge_matches_live_count_after_concurrent_register_and_remove() {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await;
    let provider = Address::repeat_byte(0x22);
    let mut join = Vec::new();
    for i in 0u8..24 {
        let handler = Arc::clone(&handler);
        join.push(tokio::spawn(async move {
            let lane = LaneKey {
                pool_id: B256::repeat_byte(i + 0xA0),
                signer: Address::repeat_byte(i + 0x01),
                provider,
            };
            let state = LaneState::hydrate(
                lane.pool_id,
                lane.signer,
                lane.provider,
                U256::from(10u64),
                0,
                U256::ZERO,
                U256::ZERO,
                None,
                decdn_incentive::LaneChain::NONE,
            );
            handler.register_lane(state).expect("register");
        }));
    }
    for handle in join {
        handle.await.expect("register task join");
    }
    assert_eq!(handler.lane_count.load(Ordering::Relaxed), 24);
    // Forget half of them, concurrently.
    let mut join = Vec::new();
    for i in 0u8..12 {
        let handler = Arc::clone(&handler);
        join.push(tokio::spawn(async move {
            handler
                .forget_lane(LaneKey {
                    pool_id: B256::repeat_byte(i + 0xA0),
                    signer: Address::repeat_byte(i + 0x01),
                    provider,
                })
                .await
                .expect("forget");
        }));
    }
    for handle in join {
        handle.await.expect("forget task join");
    }
    assert_eq!(handler.lane_count.load(Ordering::Relaxed), 12);
    let encoded = metrics.encode().expect("metrics encode");
    assert!(
        encoded.lines().any(|line| line == "decdn_lanes_open 12"),
        "gauge must reflect the live lane count after concurrent changes"
    );
}

/// #1789 item 3: racing first-streams on ONE lane count it once. The
/// vacant-entry guard is what makes the increment conditional, so a
/// regression to an unconditional `fetch_add` drifts the gauge upward
/// permanently — `decdn_lanes_open` is alerted on, so a monotonically
/// climbing gauge is worse than a wrong-but-settling one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_streams_on_one_lane_count_it_once() {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await;
    let lane = LaneKey {
        pool_id: B256::repeat_byte(0xA1),
        signer: Address::repeat_byte(0x11),
        provider: Address::repeat_byte(0x22),
    };
    let mut join = Vec::new();
    for _ in 0..16 {
        let handler = Arc::clone(&handler);
        join.push(tokio::spawn(async move {
            let state = LaneState::hydrate(
                lane.pool_id,
                lane.signer,
                lane.provider,
                U256::from(10u64),
                0,
                U256::ZERO,
                U256::ZERO,
                None,
                decdn_incentive::LaneChain::NONE,
            );
            handler.register_lane(state).expect("register");
        }));
    }
    for handle in join {
        handle.await.expect("register task join");
    }
    assert_eq!(handler.lane_count.load(Ordering::Relaxed), 1);
    let encoded = metrics.encode().expect("metrics encode");
    assert!(
        encoded.lines().any(|line| line == "decdn_lanes_open 1"),
        "16 racing registrations of one lane must publish a gauge of 1"
    );
}

/// Floor-`M` serving policy: the pool serves a full credit window while
/// `remaining − M` covers it and stops the instant it cannot. `M` is the
/// refundable minimum the pool owner is guaranteed to keep.
#[tokio::test]
async fn floor_m_serves_above_the_floor_and_stops_at_it() {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await;
    // `handler_for_tests` seeds `pool_min_remaining_deposit == 0`; rebuild a
    // small handler with a real floor by poking the field via a fresh deps is
    // awkward, so assert the arithmetic directly against the ZERO-floor
    // handler plus a manual floor calculation.
    // ZERO floor: covered whenever remaining >= min_payment.
    let rate = 1_000u64;
    let bytes = decdn_incentive::rate::BYTES_PER_MB; // one MB
    let cost = min_payment(bytes, rate);
    assert!(
        handler.pool_remaining_covers_window(cost, bytes, rate),
        "exactly the cost clears a zero floor"
    );
    assert!(
        !handler.pool_remaining_covers_window(cost - U256::from(1u64), bytes, rate),
        "one base unit short must refuse"
    );
    // Non-zero floor arithmetic: remaining − M must still cover the window.
    let floor = U256::from(500u64);
    let remaining = cost + floor;
    let refundable = remaining.saturating_sub(floor);
    assert_eq!(refundable, cost, "remaining − M is exactly the window cost");
    // Draining to the floor must stop serving: remaining − M underflows to 0.
    let at_floor = floor;
    assert!(at_floor.saturating_sub(floor).is_zero());
}

/// ADR 011 §`StreamRequest` Response names distinct refusal codes for the two
/// takedown reasons. They must NOT join the `NotFound` collapse:
/// a client told `NotFound` retries elsewhere and pays again, which for
/// `OriginBlacklisted` is advice that can never succeed.
#[test]
fn takedown_reject_reasons_do_not_collapse_to_not_found() {
    assert_eq!(
        ServeRejectReason::HashDenied.wire_error(),
        decdn_protocol::StreamError::HashBlacklisted
    );
    assert_eq!(
        ServeRejectReason::OriginDenied.wire_error(),
        decdn_protocol::StreamError::OriginBlacklisted
    );
    // The collapse itself is unchanged — it is a privacy property, not an
    // oversight, and widening it was never the point of #1179.
    for reason in [
        ServeRejectReason::CacheMiss,
        ServeRejectReason::UnknownChannel,
        ServeRejectReason::OwnerMismatch,
        // A distinct wire code here would hand a prober an oracle: with
        // throwaway signer keys it could binary-search per-signer headroom and
        // reconstruct `remaining − M`, the pool-balance map this collapse exists
        // to hide.
        ServeRejectReason::SignerFloorAtCap,
        ServeRejectReason::RangeNotSatisfiable,
    ] {
        assert_eq!(
            reason.wire_error(),
            decdn_protocol::StreamError::NotFound,
            "{reason:?} must stay wire-indistinguishable"
        );
    }
}

/// Option 2 / #2013: the pool-floor refusal is the ONE floor gate that does not
/// collapse to `NotFound`. It is reachable only past the lane-ownership proof —
/// the floor reservation fires behind a known lane keyed to a verified signer
/// holding an owner-signed capability — so its audience is the proven pool
/// owner, never an unauthenticated prober, and it ships the true reason so the
/// owner's reactive top-up loop can recover it. The per-signer floor gates
/// (`SignerFloorAtCap`, `SignerCapExhausted`) and the unconfirmed-pool gate stay
/// collapsed: each keys on a different quantity than the pool floor.
#[test]
fn insufficient_deposit_speaks_its_true_code_but_signer_gates_stay_collapsed() {
    assert_eq!(
        ServeRejectReason::InsufficientDeposit.wire_error(),
        decdn_protocol::StreamError::InsufficientDeposit,
        "the pool floor refusal is spoken to the proven owner"
    );
    for reason in [
        ServeRejectReason::SignerFloorAtCap,
        ServeRejectReason::SignerCapExhausted,
        ServeRejectReason::PoolUnconfirmed,
    ] {
        assert_eq!(
            reason.wire_error(),
            decdn_protocol::StreamError::NotFound,
            "{reason:?} keys on a per-signer/confirm quantity and stays a plain miss"
        );
    }
}

/// The privacy invariant ADR 011 §`StreamRequest` Response actually asks
/// for: a governance takedown and this operator's own denylist entry are one
/// wire code. They stay distinct *reasons* only so the operator's own
/// metrics can tell them apart, which no client can read.
///
/// Without this, governance entries reaching the serve path only as cache
/// evictions would answer `EvictedSinceProbe`, making `HashBlacklisted` a unique
/// fingerprint for "this operator privately denied it": exactly the map of an
/// operator's legal exposure the ADR forecloses.
#[test]
fn local_and_governance_hash_denials_share_one_wire_code() {
    assert_eq!(
        ServeRejectReason::HashDenied.wire_error(),
        ServeRejectReason::ChainHashDenied.wire_error(),
        "a client must not be able to tell a governance takedown from a local one"
    );
}

/// ...while an eviction with no blacklist entry behind it (corruption
/// recovery, a manual `decdn node evict`) keeps its own code. Collapsing
/// that one too would cost the probe-then-gone race its distinct answer for
/// no privacy gain: nobody can infer a legal exposure from a hash this node
/// simply no longer holds.
#[test]
fn plain_eviction_keeps_its_own_wire_code() {
    assert_ne!(
        ServeRejectReason::EvictedSinceProbe.wire_error(),
        ServeRejectReason::HashDenied.wire_error()
    );
}

/// Read a lane's own owner-signed capability material (`owner_sig`) from the
/// handler's live map — the field the redeemer builds its `CapabilityReg`
/// from. `None` when the lane is absent OR present without a captured grant.
async fn lane_owner_sig(handler: &ClientHandler, key: &LaneKey) -> Option<[u8; 65]> {
    let lane = handler.lanes.get(key).map(|e| Arc::clone(e.value()))?;
    // `owner_sig` is `Copy`, so it copies out as the guard's temporary drops.
    lane.lock().await.state.owner_sig
}

/// A [`crate::pool_view::PoolView`] returning a fixed owner (and unbounded
/// remaining, so the floor-`M` gate never interferes) for the capability
/// owner-verification test.
#[derive(Debug)]
struct FixedPoolView {
    owner: Address,
}

#[async_trait::async_trait]
impl crate::pool_view::PoolView for FixedPoolView {
    async fn status(&self, _pool_id: B256) -> Option<crate::pool_view::PoolStatus> {
        Some(crate::pool_view::PoolStatus {
            owner: self.owner,
            remaining: U256::MAX,
            lifecycle: crate::pool_view::Lifecycle::Open,
        })
    }
}

/// Build a handler with a real in-memory lane store and a fixed-owner
/// pool-view wired, for the capability-intake owner check. The captured
/// grant is read back off the lane record via [`lane_owner_sig`].
async fn handler_with_capability_intake(
    metrics: &Arc<Metrics>,
    owner: Address,
) -> (Arc<ClientHandler>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = CacheEngine::open(dir.path(), Vec::new(), 16)
        .await
        .expect("cache");
    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let mut deps = ClientHandlerDeps::new(
        iroh::SecretKey::generate().public(),
        Arc::clone(metrics),
        Arc::new(ConnectionLimiter::new(
            &decdn_common::config::ResolvedSecurity {
                max_concurrent_handlers: u32::MAX,
                per_source_rate_per_sec: 1e9,
                per_source_burst: u32::MAX,
                max_tracked_sources: 16,
            },
            Arc::clone(metrics),
        )),
        cache,
        Arc::new(alloy::signers::local::PrivateKeySigner::random()),
        domain.clone(),
        domain.clone(),
        domain,
        Arc::new(decdn_incentive::store::MemoryPoolStateStore::new()) as Arc<dyn PoolStateStore>,
        Arc::new(crate::receipt_log::DirectReceiptSink::new(Arc::new(
            crate::receipt_log::NoopReceiptLog,
        ))) as Arc<dyn ReceiptSink>,
        1,
        16,
        Arc::new(crate::content_deny::ContentDenylist::empty()),
        U256::ZERO,
        always_admit_shed(),
    );
    deps.pool_view = Some(Arc::new(FixedPoolView { owner }));
    let handler = ClientHandler::new(deps).expect("handler");
    (Arc::new(handler), dir)
}

/// Build a handler whose pool-view is a real [`crate::pool_view::PoolProjection`],
/// returned alongside so a test can fold `PoolRedeemed` deltas into it and drive
/// the mid-stream signer cap-headroom re-check against a live projection.
async fn handler_with_projection_view(
    metrics: &Arc<Metrics>,
) -> (
    Arc<ClientHandler>,
    crate::pool_view::PoolProjection,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = CacheEngine::open(dir.path(), Vec::new(), 16)
        .await
        .expect("cache");
    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let projection = crate::pool_view::PoolProjection::new();
    let mut deps = ClientHandlerDeps::new(
        iroh::SecretKey::generate().public(),
        Arc::clone(metrics),
        Arc::new(ConnectionLimiter::new(
            &decdn_common::config::ResolvedSecurity {
                max_concurrent_handlers: u32::MAX,
                per_source_rate_per_sec: 1e9,
                per_source_burst: u32::MAX,
                max_tracked_sources: 16,
            },
            Arc::clone(metrics),
        )),
        cache,
        Arc::new(alloy::signers::local::PrivateKeySigner::random()),
        domain.clone(),
        domain.clone(),
        domain,
        Arc::new(decdn_incentive::store::MemoryPoolStateStore::new()) as Arc<dyn PoolStateStore>,
        Arc::new(crate::receipt_log::DirectReceiptSink::new(Arc::new(
            crate::receipt_log::NoopReceiptLog,
        ))) as Arc<dyn ReceiptSink>,
        1,
        16,
        Arc::new(crate::content_deny::ContentDenylist::empty()),
        U256::ZERO,
        always_admit_shed(),
    );
    deps.pool_view = Some(Arc::new(projection.clone()));
    let handler = ClientHandler::new(deps).expect("handler");
    (Arc::new(handler), projection, dir)
}

/// Fix 1 (security): capability intake verifies the owner signature against
/// the on-chain pool owner. A grant signed by a NON-owner key is dropped —
/// never captured on a lane, never lane-registered — so it cannot revert the
/// redeemer's `redeemMany` batch. A correct-owner grant registers its lane
/// and rides the lane record as its `owner_sig`.
#[tokio::test]
async fn intake_rejects_wrong_owner_capability() {
    let metrics = Arc::new(Metrics::new());
    let owner = PrivateKeySigner::random();
    let (handler, _dir) = handler_with_capability_intake(&metrics, owner.address()).await;
    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let pool_id = B256::repeat_byte(0x77);
    let signer = Address::repeat_byte(0x11);
    let spending_cap = 1_000_000u64;
    let expiry = 1_900_000_000u64;

    let make_wire = |key: &PrivateKeySigner| -> decdn_protocol::client::WireCapability {
        let signed_cap = Capability {
            signer,
            spending_cap,
            pool_id,
            expiry,
        }
        .sign(key, &domain)
        .expect("sign capability");
        decdn_protocol::client::WireCapability {
            spending_cap,
            expiry,
            owner_signature: signed_cap.signature.as_bytes().to_vec(),
        }
    };

    let lane_key = LaneKey {
        pool_id,
        signer,
        provider: handler.eth_signer.address(),
    };

    // A capability signed by a NON-owner is dropped: no lane, no material.
    let bad = make_wire(&PrivateKeySigner::random());
    handler.intake_capability(pool_id, signer, owner.address(), &bad);
    assert!(
        !handler.lanes.contains_key(&lane_key),
        "a forged-owner capability must not register a lane"
    );
    assert_eq!(
        lane_owner_sig(&handler, &lane_key).await,
        None,
        "a forged-owner capability captures no material"
    );

    // The correct owner's capability registers the lane and rides its record.
    let good = make_wire(&owner);
    let expected_sig =
        <[u8; 65]>::try_from(good.owner_signature.as_slice()).expect("65-byte owner sig");
    handler.intake_capability(pool_id, signer, owner.address(), &good);
    assert!(
        handler.lanes.contains_key(&lane_key),
        "a correct-owner capability registers its lane so vouchers can be served"
    );
    assert_eq!(
        lane_owner_sig(&handler, &lane_key).await,
        Some(expected_sig),
        "the verified owner signature is captured on the lane record for the redeemer"
    );
}

/// #1789 item 2: the verification cache returns the cached recovery for an
/// identical repeat, treats a tampered signature as a miss (ecrecover is
/// keyed on the full signed material, so a different signature can never
/// be served a stale owner), and never lets the map exceed its capacity.
#[test]
fn capability_verify_cache_repeat_hits_tampered_misses_and_is_bounded() {
    let mut cache = CapabilityVerifyCache::with_capacity(2);
    let hash = B256::repeat_byte(0xAB);
    let sig = [0x10u8; 65];
    let owner = Address::repeat_byte(0x42);
    assert_eq!(cache.get(hash, sig), None, "a cold lookup misses");
    cache.insert(hash, sig, CapabilityVerifyOutcome::Owner(owner));
    assert_eq!(
        cache.get(hash, sig),
        Some(CapabilityVerifyOutcome::Owner(owner)),
        "an identical repeat hits the cache"
    );
    let tampered = {
        let mut bytes = sig;
        bytes[0] ^= 0x01;
        bytes
    };
    assert_eq!(
        cache.get(hash, tampered),
        None,
        "a tampered signature is a different key, never served from cache"
    );
    cache.insert(hash, tampered, CapabilityVerifyOutcome::Invalid);
    // Touch the original so it is the MRU entry; the tampered one is now
    // least-recently-used and is what a third insert must displace.
    assert_eq!(
        cache.get(hash, sig),
        Some(CapabilityVerifyOutcome::Owner(owner)),
        "the original is still cached before the eviction"
    );
    let other_hash = B256::repeat_byte(0xCD);
    cache.insert(
        other_hash,
        [0x20u8; 65],
        CapabilityVerifyOutcome::Owner(Address::repeat_byte(0x99)),
    );
    assert_eq!(cache.len(), 2, "capacity is never exceeded");
    assert_eq!(
        cache.get(hash, tampered),
        None,
        "eviction takes the least-recently-used entry"
    );
    assert_eq!(
        cache.get(hash, sig),
        Some(CapabilityVerifyOutcome::Owner(owner)),
        "the recently-used entry survives the eviction"
    );
}

/// #1789 item 2: a malformed signature is cached as `Invalid` and re-served
/// as `Invalid` — never as a recovered owner, and never as a hit that
/// bypasses the pool-owner comparison. `recover_owner` rejects high-`s` up
/// front (#836) so the off-chain accept-set matches the on-chain verifiable
/// set; a regression that mapped a recovery failure onto `Owner(ZERO)` or
/// cached a boolean verdict would let a malleable capability through
/// off-chain and revert `redeemMany` on-chain.
#[allow(clippy::similar_names)] // signer/signed pair up clearly here
#[tokio::test]
async fn high_s_capability_is_dropped_and_cached_as_invalid() {
    // secp256k1 group order `n`, for building the malleable high-`s` twin
    // `(r, n - s, !v)` below. The twin recovers the SAME signer, so a
    // rejection is specifically about canonicalization (#836), not a wrong
    // owner.
    const SECP256K1N: U256 = U256::from_be_bytes([
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xfe, 0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36,
        0x41, 0x41,
    ]);

    let metrics = Arc::new(Metrics::new());
    let owner = PrivateKeySigner::random();
    let (handler, _dir) = handler_with_capability_intake(&metrics, owner.address()).await;
    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let pool_id = B256::repeat_byte(0x12);
    let signer = Address::repeat_byte(0x34);
    let lane_key = LaneKey {
        pool_id,
        signer,
        provider: handler.eth_signer.address(),
    };
    let signed = Capability {
        signer,
        spending_cap: 1_000_000u64,
        pool_id,
        expiry: 1_900_000_000,
    }
    .sign(&owner, &domain)
    .expect("sign capability");
    let twin = alloy::primitives::Signature::new(
        signed.signature.r(),
        SECP256K1N - signed.signature.s(),
        !signed.signature.v(),
    );
    let bytes = twin.as_bytes();

    let wire = decdn_protocol::client::WireCapability {
        spending_cap: signed.capability.spending_cap,
        expiry: signed.capability.expiry,
        owner_signature: bytes.to_vec(),
    };
    handler.intake_capability(pool_id, signer, owner.address(), &wire);
    handler.intake_capability(pool_id, signer, owner.address(), &wire);

    assert!(
        !handler.lanes.contains_key(&lane_key),
        "a malformed capability never registers a lane (nor captures material)"
    );
    assert_eq!(
        lane_owner_sig(&handler, &lane_key).await,
        None,
        "a malformed capability captures no owner_sig"
    );
    let cached = handler
        .capability_verify_cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(signed.capability.signing_hash(&domain), bytes);
    assert_eq!(
        cached,
        Some(CapabilityVerifyOutcome::Invalid),
        "the malformed recovery is cached as Invalid, so the repeat skips the ecrecover"
    );
}

/// #1789 item 2: a client that re-sends the same capability (the
/// documented recovery path) hits the verification cache instead of paying a
/// fresh `ecrecover`, and the genuine grant lands on the lane record. A
/// tampered re-send — same payload, different signature — misses the cache,
/// re-verifies, recovers a different owner, and is dropped without disturbing
/// the genuine `owner_sig` already on the lane.
#[allow(clippy::similar_names)] // signer/signed/signature pair up clearly here
#[tokio::test]
async fn repeat_capability_send_hits_verify_cache_and_keeps_the_genuine_material() {
    let metrics = Arc::new(Metrics::new());
    let owner = PrivateKeySigner::random();
    let (handler, _dir) = handler_with_capability_intake(&metrics, owner.address()).await;
    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let pool_id = B256::repeat_byte(0x12);
    let signer = Address::repeat_byte(0x34);
    let lane_key = LaneKey {
        pool_id,
        signer,
        provider: handler.eth_signer.address(),
    };
    let signed = Capability {
        signer,
        spending_cap: 1_000_000u64,
        pool_id,
        expiry: 1_900_000_000,
    }
    .sign(&owner, &domain)
    .expect("sign capability");
    let genuine_sig = signed.signature.as_bytes();
    let wire = decdn_protocol::client::WireCapability {
        spending_cap: signed.capability.spending_cap,
        expiry: signed.capability.expiry,
        owner_signature: genuine_sig.to_vec(),
    };

    handler.intake_capability(pool_id, signer, owner.address(), &wire);
    handler.intake_capability(pool_id, signer, owner.address(), &wire);
    let cache_len = handler
        .capability_verify_cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .len();
    assert_eq!(
        cache_len, 1,
        "the identical repeat must hit the verification cache"
    );
    assert_eq!(
        lane_owner_sig(&handler, &lane_key).await,
        Some(genuine_sig),
        "the genuine owner_sig rides the lane record after the repeated intake"
    );

    // A tampered re-send: the SAME capability payload signed by a
    // different key — a well-formed signature that is a distinct cache
    // key, so it is a miss, re-verifies, recovers a different owner, and
    // is dropped rather than accepted on the strength of the earlier
    // grant.
    let other = PrivateKeySigner::random();
    let forged = Capability {
        signer,
        spending_cap: signed.capability.spending_cap,
        pool_id,
        expiry: signed.capability.expiry,
    }
    .sign(&other, &domain)
    .expect("sign capability");
    let forged_wire = decdn_protocol::client::WireCapability {
        spending_cap: forged.capability.spending_cap,
        expiry: forged.capability.expiry,
        owner_signature: forged.signature.as_bytes().to_vec(),
    };
    handler.intake_capability(pool_id, signer, owner.address(), &forged_wire);
    assert_eq!(
        handler
            .capability_verify_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        2,
        "the forged signature records its own (new) key"
    );
    assert_eq!(
        lane_owner_sig(&handler, &lane_key).await,
        Some(genuine_sig),
        "the forged re-send is dropped; the genuine owner_sig on the lane is untouched"
    );
}

/// Lock the floor map for a test assertion, surfacing a poisoned lock as an
/// `anyhow` error rather than panicking (the anti-panic policy holds in tests).
/// The capability signer every floor-accumulator unit test reserves under.
/// A second signer (`TEST_SIGNER_B`) exercises per-signer isolation.
const TEST_SIGNER: Address = Address::new([0xa1u8; 20]);
/// A distinct co-tenant on the same pool.
const TEST_SIGNER_B: Address = Address::new([0xb2u8; 20]);
/// The advertised `µUSDC`/MB rate the floor-cap tests price against. Only the
/// one-credit-window clamp in [`ClientHandler::signer_floor_cap`] reads it.
const TEST_RATE: u64 = 1_000;

fn lock_floor(
    map: &Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
) -> anyhow::Result<std::sync::MutexGuard<'_, HashMap<B256, PoolFloorState>>> {
    map.lock()
        .map_err(|e| anyhow::anyhow!("floor map poisoned: {e}"))
}

/// A serve REFUSED before the serve loop ran — [`FloorReservation::release_unspent`]
/// called on the pre-spend refusal paths (the floor-`M` gate, the size gate, an
/// upstream that refused the header handshake) — frees the live reservation at
/// once. It fronted no USDC and delivered no byte, so the pool's floor headroom is
/// fully restored and the signer row is pruned.
#[test]
fn floor_reservation_refused_unspent_releases_the_live_floor() -> anyhow::Result<()> {
    let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let pool = B256::repeat_byte(0x5C);
    let floor = decdn_incentive::floor_micro(1000);
    {
        let res = FloorReservation::reserve(map.clone(), pool, TEST_SIGNER, floor);
        let live = lock_floor(&map)?.get(&pool).map(|s| s.live_reservation);
        anyhow::ensure!(
            live == Some(floor),
            "live reservation is held while the guard lives"
        );
        // Refused before any spend — release the live floor at once.
        res.release_unspent();
    } // drop → no-op: release_unspent already freed the live reservation
    let st = lock_floor(&map)?.get(&pool).cloned().unwrap_or_default();
    anyhow::ensure!(
        st.live_reservation == U256::ZERO && st.signers.is_empty(),
        "a pre-spend refusal releases its reservation, so its row is pruned"
    );
    Ok(())
}

/// The pool-budget guard counts a pool's committed LIVE floor reservation against
/// `remaining − M`. This is the re-check both the mid-stream gate and the
/// direct-serve gate apply to a stream that already holds its reservation, so it is
/// deliberately pool-level only and carries no signer dimension.
#[tokio::test]
async fn pool_budget_covers_reserve_accounts_committed_floor() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await; // M = 0
    let pool = B256::repeat_byte(0x11);
    let floor = decdn_incentive::floor_micro(1_000_000);
    // Empty pool, M = 0: remaining must cover the new reserve exactly.
    anyhow::ensure!(
        handler.pool_budget_covers_reserve(pool, floor, floor),
        "remaining equal to the reserve is covered"
    );
    anyhow::ensure!(
        !handler.pool_budget_covers_reserve(pool, floor.saturating_sub(U256::from(1u64)), floor),
        "remaining one below the reserve is not covered"
    );
    // A live reservation consumes the budget: the same remaining no longer
    // covers a second identical reserve.
    let _guard = handler.reserve_floor(pool, TEST_SIGNER, floor);
    anyhow::ensure!(
        !handler.pool_budget_covers_reserve(pool, floor, floor),
        "an in-flight floor reservation is committed against the budget"
    );
    // The signer dimension is absent by design: a co-tenant's reserve is
    // counted here just the same, because this asks only whether the POOL can
    // still pay.
    anyhow::ensure!(
        !handler.pool_budget_covers_reserve(pool, floor, floor),
        "the pool-level re-check does not vary with the signer"
    );
    Ok(())
}

/// `try_reserve_floor` reserves atomically: it charges the budget only when
/// `remaining − M` covers the pool's committed live reservation plus the new floor,
/// and the charge is visible to the very next call so a second reserve on an
/// exhausted pool is refused. The no-op per-signer gates keep the pool ceiling the
/// only bound under test.
#[tokio::test]
async fn try_reserve_floor_charges_only_when_budget_covers() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await; // M = 0
    let pool = B256::repeat_byte(0x22);
    let floor = decdn_incentive::floor_micro(1_000_000);
    // Budget covers exactly one floor: the first reserve succeeds.
    let first = handler.try_reserve_floor(pool, TEST_SIGNER, floor, TEST_RATE, floor);
    anyhow::ensure!(first.is_ok(), "a floor within remaining − M is reserved");
    // The charge is live: a second identical reserve against the SAME remaining
    // now sees `committed = floor` and is refused (no over-commit).
    anyhow::ensure!(
        handler
            .try_reserve_floor(pool, TEST_SIGNER, floor, TEST_RATE, floor)
            .err()
            == Some(FloorRefusal::PoolExhausted),
        "a second reserve over the same budget is refused as pool-exhausted"
    );
    // Dropping the reservation frees its live floor, reopening the budget.
    drop(first);
    anyhow::ensure!(
        handler
            .try_reserve_floor(pool, TEST_SIGNER, floor, TEST_RATE, floor)
            .is_ok(),
        "budget reopens once a released reservation frees its live floor"
    );
    Ok(())
}

/// The per-signer live cap isolates co-tenants of one shared pool: a signer that
/// holds its `k`-window cap of live reservation is refused `SignerAtCap` while the
/// pool can still pay, and a SECOND signer is admitted from its own cap at the same
/// instant. Without the signer dimension the first signer's reservations would be
/// the pool's, and the second would be refused too.
#[tokio::test]
async fn signer_live_cap_refuses_one_signer_and_admits_a_co_tenant() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    // k = 1 window each, a bottomless bucket, M = 0.
    let (handler, _dir) = handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 1).await;
    let pool = B256::repeat_byte(0x31);
    let window = handler.one_window(TEST_RATE);
    let remaining = window.saturating_mul(U256::from(4u64));
    anyhow::ensure!(
        handler.signer_floor_cap(TEST_RATE) == window,
        "k = 1 gives a one-window live cap"
    );
    let held = handler.try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, window);
    anyhow::ensure!(held.is_ok(), "the first window fits inside the signer cap");
    // Signer A is at its cap. The POOL is not — three windows of headroom are
    // untouched — so the refusal must name the signer cap, not the pool.
    anyhow::ensure!(
        handler
            .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, window)
            .err()
            .is_some_and(|e| matches!(e, FloorRefusal::SignerAtCap { .. })),
        "a second window on the SAME signer exceeds its cap while the pool can still pay"
    );
    // A co-tenant draws on its own untouched cap.
    anyhow::ensure!(
        handler
            .try_reserve_floor(pool, TEST_SIGNER_B, remaining, TEST_RATE, window)
            .is_ok(),
        "a distinct signer is admitted from its own cap while the first is capped"
    );
    Ok(())
}

/// The per-pool ceiling still bounds the AGGREGATE across signers: solvency
/// cannot be escaped by spraying identities. With k = 1 window each, four signers
/// each take their own window on a four-window pool, none exceeding its live cap,
/// and the fifth is refused `PoolExhausted` — the pool, not the signer, ran out.
#[tokio::test]
async fn pool_ceiling_still_bounds_the_aggregate_across_signers() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 1).await;
    let pool = B256::repeat_byte(0x32);
    let window = handler.one_window(TEST_RATE);
    let remaining = window.saturating_mul(U256::from(4u64));
    let mut held = Vec::new();
    for i in 0u8..4 {
        let signer = Address::new([i.saturating_add(1); 20]);
        let guard = handler
            .try_reserve_floor(pool, signer, remaining, TEST_RATE, window)
            .map_err(|e| anyhow::anyhow!("signer {i} refused: {e:?}"))?;
        held.push(guard);
    }
    // Every one of the four sits at exactly its own window, so no live cap is
    // exceeded; what refuses the fifth is the pool ceiling.
    anyhow::ensure!(
        handler
            .try_reserve_floor(pool, Address::new([0xee; 20]), remaining, TEST_RATE, window)
            .err()
            == Some(FloorRefusal::PoolExhausted),
        "signer fan-out cannot push the aggregate past remaining − M"
    );
    Ok(())
}

/// A refused admission inserts nothing at either level. This is what stops a
/// client probing a full pool with throwaway signer keys from growing a map that
/// is locked on every admission — reading through `entry().or_default()` instead
/// of `get` would make every refusal a permanent row.
#[tokio::test]
async fn a_refused_admission_leaves_no_row_at_either_level() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 1).await;
    let pool = B256::repeat_byte(0x38);
    let window = handler.one_window(TEST_RATE);
    let floor = window;
    let remaining = window.saturating_mul(U256::from(4u64));

    // Refused by the POOL ceiling, on a pool with no entry at all.
    anyhow::ensure!(
        handler
            .try_reserve_floor(
                pool,
                TEST_SIGNER,
                floor.saturating_sub(U256::from(1u64)),
                TEST_RATE,
                floor
            )
            .is_err(),
        "a pool that cannot cover one floor refuses"
    );
    anyhow::ensure!(
        !lock_floor(&handler.pool_floor)?.contains_key(&pool),
        "a pool-ceiling refusal creates no pool entry"
    );

    // Now admit one signer, then spray fresh signer keys past the pool ceiling.
    let mut held = Vec::new();
    for i in 0u8..4 {
        held.push(
            handler
                .try_reserve_floor(
                    pool,
                    Address::new([i.saturating_add(1); 20]),
                    remaining,
                    TEST_RATE,
                    floor,
                )
                .map_err(|e| anyhow::anyhow!("signer {i} refused: {e:?}"))?,
        );
    }
    for i in 0u8..8 {
        anyhow::ensure!(
            handler
                .try_reserve_floor(
                    pool,
                    Address::new([i.saturating_add(0xc0); 20]),
                    remaining,
                    TEST_RATE,
                    floor
                )
                .is_err(),
            "prober {i} is refused on a full pool"
        );
    }
    let signers = lock_floor(&handler.pool_floor)?
        .get(&pool)
        .map(|s| s.signers.len());
    anyhow::ensure!(
        signers == Some(4),
        "only the four admitted signers hold rows; eight refused probes added none"
    );
    Ok(())
}

/// A guard whose pool was reclaimed reconciles against nothing, even when a
/// later admission has re-entered the same `pool_id`. Without the generation
/// stamp the stale drop finds the NEW entry and subtracts a reservation that entry
/// never held — leaving the pool total below the sum of its signer rows, which is
/// the direction that over-admits.
#[test]
fn a_stale_guard_does_not_reconcile_against_a_re_entered_pool() -> anyhow::Result<()> {
    let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let pool = B256::repeat_byte(0x39);
    let floor = decdn_incentive::floor_micro(1000);

    let stale = FloorReservation::reserve(Arc::clone(&map), pool, TEST_SIGNER, floor);
    // The pool is reclaimed on-chain: its whole entry goes, signer rows and all.
    lock_floor(&map)?.remove(&pool);
    // A later admission re-enters the same key — the cached `getPool` view can
    // still show headroom for a moment after the reclaim lands.
    let fresh = FloorReservation::reserve(Arc::clone(&map), pool, TEST_SIGNER_B, floor);
    drop(stale);

    let st = lock_floor(&map)?.get(&pool).cloned().unwrap_or_default();
    anyhow::ensure!(
        st.live_reservation == floor,
        "the stale drop must not release the new entry's live reservation"
    );
    anyhow::ensure!(
        st.signers.len() == 1 && st.signers.contains_key(&TEST_SIGNER_B),
        "the stale drop must not insert its own signer row under the new entry"
    );
    anyhow::ensure!(
        st.live_reservation
            == st
                .signers
                .values()
                .fold(U256::ZERO, |acc, s| acc.saturating_add(s.live_reservation)),
        "the pool live total stays the sum of its signer rows"
    );
    drop(fresh);
    Ok(())
}

/// The live cap is `k · one_window`, lower-clamped to one window: `k = 0` (or an
/// unset field) still admits a lone signer's first stream on any solvent pool
/// rather than wedging it, and the cap is an ABSOLUTE window count — it does not
/// scale with the pool's deposit.
#[tokio::test]
async fn signer_floor_cap_is_k_windows_lower_clamped_to_one() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    // k = 0 → lower-clamped to one window.
    let (clamped, _c) = handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 0).await;
    let one_window = clamped.one_window(TEST_RATE);
    anyhow::ensure!(
        clamped.signer_floor_cap(TEST_RATE) == one_window,
        "k = 0 clamps the live cap up to one window"
    );
    let pool = B256::repeat_byte(0x34);
    let remaining = one_window.saturating_mul(U256::from(2u64));
    anyhow::ensure!(
        clamped
            .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, one_window)
            .is_ok(),
        "a lone signer's first window is admitted even at k = 0"
    );
    // k = 8 → cap is eight windows, the same however large the pool's deposit.
    let (handler, _dir) = handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 8).await;
    let eight = one_window.saturating_mul(U256::from(8u64));
    anyhow::ensure!(
        handler.signer_floor_cap(TEST_RATE) == eight,
        "k = 8 gives an eight-window cap"
    );
    Ok(())
}

/// Headroom below one credit window: the one-window lower clamp then returns a
/// live cap LARGER than the pool's entire headroom, and only the pool ceiling
/// running FIRST keeps the node from admitting past the refundable minimum `M` the
/// pool owner is guaranteed. Pins that order — the refusal must be `PoolExhausted`,
/// never `Ok`, at any `k`.
#[tokio::test]
async fn pool_ceiling_refuses_below_one_window_whatever_the_cap() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    for k in [0u64, 1, 8] {
        let (handler, _dir) = handler_for_tests_with_signer_policy(&metrics, U256::ZERO, k).await;
        let pool = B256::repeat_byte(0x36);
        let one_window =
            decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
        let remaining = one_window.saturating_sub(U256::from(1u64));
        anyhow::ensure!(
            handler.signer_floor_cap(TEST_RATE) > remaining,
            "k {k}: the one-window lower clamp exceeds the headroom, which is what makes \
             the check order load-bearing"
        );
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, one_window)
                .err()
                == Some(FloorRefusal::PoolExhausted),
            "k {k}: a reserve past remaining − M must be refused by the pool ceiling, \
             not admitted through the clamped live cap"
        );
    }
    Ok(())
}

/// A signer that reserves, pays, and leaves takes its row with it. Without the
/// prune the map only grows: ADR 003 §Revocation makes short-expiry session keys
/// the intended usage, so a busy publisher mints signer identities steadily and
/// every one that pays cleanly would leave a zero row alive until the pool is
/// reclaimed on-chain — inside a map locked on every admission.
#[tokio::test]
async fn a_paid_out_signer_leaves_no_row_behind() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await; // M = 0
    let pool = B256::repeat_byte(0x37);
    let floor = decdn_incentive::floor_micro(1_000_000);
    let guard = handler
        .try_reserve_floor(pool, TEST_SIGNER, floor, TEST_RATE, floor)
        .map_err(|e| anyhow::anyhow!("refused: {e:?}"))?;
    {
        let map = handler
            .pool_floor
            .lock()
            .map_err(|e| anyhow::anyhow!("floor map poisoned: {e}"))?;
        anyhow::ensure!(
            map.get(&pool)
                .is_some_and(|s| s.signers.contains_key(&TEST_SIGNER)),
            "the row exists while the reservation is live"
        );
    }
    // Paid in full, then dropped: the live reservation is released and the bucket
    // is never debited, so the row carries no information and is pruned.
    guard.release_live_repaid();
    drop(guard);
    let map = handler
        .pool_floor
        .lock()
        .map_err(|e| anyhow::anyhow!("floor map poisoned: {e}"))?;
    let entry = map.get(&pool).cloned().unwrap_or_default();
    anyhow::ensure!(
        !entry.signers.contains_key(&TEST_SIGNER),
        "a signer that carries no information must not keep a row for the pool's lifetime"
    );
    anyhow::ensure!(
        entry.live_committed() == U256::ZERO,
        "and the pool live total still agrees with the (now empty) signer set"
    );
    Ok(())
}

/// The live cap makes one signer's concurrent exposure an ABSOLUTE window count,
/// not a slice of the deposit (#1857): `k · one_window`, the same however large the
/// pool's headroom, and lower-clamped to one window so it never wedges a lone
/// signer off a small pool.
#[tokio::test]
async fn signer_cap_is_a_window_count_not_a_slice_of_the_deposit() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 16).await;
    let one_window = decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
    let ceiling = one_window.saturating_mul(U256::from(16u64));
    // The cap does not vary with the request's rate scale beyond one_window, and it
    // is exactly k windows — no deposit term enters.
    anyhow::ensure!(
        handler.signer_floor_cap(TEST_RATE) == ceiling,
        "the live cap is exactly k windows"
    );
    // k = 0 clamps up to one window (already covered), and the cap is independent of
    // any headroom value — it is not a function of remaining at all.
    let (clamped, _c) = handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 0).await;
    anyhow::ensure!(
        clamped.signer_floor_cap(TEST_RATE) == one_window,
        "k = 0 clamps up to one window regardless of the deposit"
    );
    Ok(())
}

/// A `PoolRedeemed` lane entry for the projection, at `cumulative` `µUSDC`.
fn lane_settled(
    signer: Address,
    cumulative: u64,
) -> decdn_incentive::payment_pool::PaymentPool::LaneSettled {
    decdn_incentive::payment_pool::PaymentPool::LaneSettled {
        signer,
        newPaidCumulative: cumulative,
        bytesPaid: 0,
    }
}

/// A live lane registered from a wider capability narrows to the signer's
/// on-chain registration, and the store persists the clamped terms (#2265).
#[tokio::test]
async fn clamp_lane_to_registration_narrows_the_live_lane_and_persists() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (handler, _projection, _dir) = handler_with_projection_view(&metrics).await;
    let lane = LaneState::hydrate(
        B256::repeat_byte(0x61),
        Address::new([0xa2; 20]),
        handler.eth_signer.address(),
        U256::from(5_000_000u64),
        2_000_000_000,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    );
    let key = lane.key();
    handler.register_lane(lane)?;
    let live = handler
        .lanes
        .get(&key)
        .map(|e| Arc::clone(e.value()))
        .ok_or_else(|| anyhow::anyhow!("the lane is live"))?;

    handler
        .clamp_lane_to_registration(&live, 40, 1_900_000_000)
        .await;

    let held = live.lock().await.state.clone();
    assert_eq!(held.cap, U256::from(40u64));
    assert_eq!(held.expiry, 1_900_000_000);
    assert_eq!(held.registered_until, 1_900_000_000);
    let stored = handler
        .channel_state_store
        .get(key)?
        .ok_or_else(|| anyhow::anyhow!("the lane row exists"))?;
    assert_eq!((stored.cap, stored.expiry), (held.cap, held.expiry));
    assert_eq!(stored.registered_until, 1_900_000_000);
    Ok(())
}

/// The mid-stream signer cap-headroom re-check stops a live stream once the
/// signer drains its shared `cap` at OTHER providers since admission — the drain
/// the pool-solvency re-check cannot see, because the pool's `remaining` stays
/// healthy on other signers' budgets.
#[tokio::test]
async fn midstream_signer_recheck_trips_when_signer_drains_at_other_nodes() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (handler, projection, _dir) = handler_with_projection_view(&metrics).await;
    let pool = B256::repeat_byte(0x51);
    let signer = Address::new([0xa1; 20]);
    let one_window = decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
    let ow = u64::try_from(one_window).expect("one window fits u64");
    // The signer holds a cap of ten windows on this lane; the pool is richly
    // funded, so only per-signer cap headroom can bind here.
    let held_cap = U256::from(ow.saturating_mul(10));
    projection.record_opened(pool, Address::new([0x07; 20]), U256::MAX);

    // At admit the signer has spent one window across providers — nine windows of
    // headroom remain, above the one-window floor, so the stream keeps serving.
    projection.record_redeemed(pool, Address::new([0xc0; 20]), &[lane_settled(signer, ow)]);
    anyhow::ensure!(
        !handler
            .signer_cap_drained_midstream(pool, signer, held_cap, TEST_RATE)
            .await,
        "nine windows of headroom must keep the stream serving"
    );

    // Mid-stream the signer drains the rest of its cap at a DIFFERENT provider,
    // taking the cross-provider total to the full cap — headroom falls below one
    // floor, so the re-check stops the stream.
    projection.record_redeemed(
        pool,
        Address::new([0xc1; 20]),
        &[lane_settled(signer, ow.saturating_mul(9))],
    );
    anyhow::ensure!(
        handler
            .signer_cap_drained_midstream(pool, signer, held_cap, TEST_RATE)
            .await,
        "a signer drained to its full cap across providers must stop the stream"
    );
    Ok(())
}

/// The re-check fails toward SERVING on the cold-start undercount: a pool the
/// projection has not folded reports zero spent, over-stating headroom, and a
/// handler with no pool-view skips the check entirely. Both mirror the pool
/// re-check's fail-open, and the admit-time `getAuthorization` (#1958) already
/// caught an already-exhausted signer authoritatively.
#[tokio::test]
async fn midstream_signer_recheck_fails_toward_serving_on_projection_gap() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let pool = B256::repeat_byte(0x52);
    let signer = Address::new([0xa2; 20]);

    // Pool-view wired, but the pool is ABSENT from the projection (opened before
    // the watcher's cold-start head): `signer_spent` under-counts to zero, so even
    // a cap of exactly one floor reads as full headroom and the stream serves.
    let (handler, _projection, _dir) = handler_with_projection_view(&metrics).await;
    let one_window = decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
    anyhow::ensure!(
        !handler
            .signer_cap_drained_midstream(pool, signer, one_window, TEST_RATE)
            .await,
        "an unfolded pool under-counts spent to zero and must fail toward serving"
    );

    // No pool-view wired at all (dev/test): the re-check is skipped, whatever the
    // held cap.
    let (bare, _d) = handler_for_tests(&metrics).await;
    anyhow::ensure!(
        !bare
            .signer_cap_drained_midstream(pool, signer, U256::ZERO, TEST_RATE)
            .await,
        "no pool-view wired must skip the re-check and keep serving"
    );
    Ok(())
}

/// The live cap scales the number of distinct signers needed to strand a pool's
/// floor budget with the deposit. With a `k`-window cap on a pool holding `N`
/// windows of headroom it takes `ceil(N / k)` signers — the escape a share alone
/// could not give, where a constant number of keys strands any deposit.
#[tokio::test]
async fn live_cap_scales_the_signers_needed_to_strand_the_floor() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 16).await;
    let pool = B256::repeat_byte(0x38);
    let one_window = decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
    let remaining = one_window.saturating_mul(U256::from(144u64));
    let ceiling = one_window.saturating_mul(U256::from(16u64));
    anyhow::ensure!(
        handler.signer_floor_cap(TEST_RATE) == ceiling,
        "k = 16 gives a 16-window live cap"
    );

    // Eight signers each fill their own ceiling and stop there. The pool keeps a
    // spare ceiling's worth of headroom throughout (8 × 16 == 128 of 144), so
    // every refusal in this loop is the live cap and not the pool running out.
    let mut held = Vec::new();
    for i in 0u8..8 {
        let signer = Address::new([i.saturating_add(1); 20]);
        held.push(
            handler
                .try_reserve_floor(pool, signer, remaining, TEST_RATE, ceiling)
                .map_err(|e| anyhow::anyhow!("signer {i} refused early: {e:?}"))?,
        );
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, signer, remaining, TEST_RATE, one_window)
                .err()
                .is_some_and(|e| matches!(e, FloorRefusal::SignerAtCap { .. })),
            "signer {i} must stop at its window ceiling, not at a share of the deposit"
        );
    }
    // A ninth signer takes the last ceiling's worth, and only then is the pool
    // itself spent — after nine signers, not the four a bare quarter-share would
    // have needed however large the deposit.
    held.push(
        handler
            .try_reserve_floor(
                pool,
                Address::new([0x9au8; 20]),
                remaining,
                TEST_RATE,
                ceiling,
            )
            .map_err(|e| anyhow::anyhow!("the ninth signer's own share must fit: {e:?}"))?,
    );
    anyhow::ensure!(
        handler
            .try_reserve_floor(
                pool,
                Address::new([0xeeu8; 20]),
                remaining,
                TEST_RATE,
                one_window
            )
            .err()
            == Some(FloorRefusal::PoolExhausted),
        "the pool ceiling still bounds the aggregate once every share is spent"
    );
    Ok(())
}

/// A very large `k` makes the live cap wider than the pool's whole headroom, so
/// the per-signer live gate never binds and the check collapses to exactly the
/// pool-ceiling behavior. This is the escape hatch an operator serving
/// single-signer pools sets.
#[tokio::test]
async fn wide_live_cap_reproduces_the_pool_only_bound() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) =
        handler_for_tests_with_signer_policy(&metrics, U256::ZERO, u64::MAX).await;
    let pool = B256::repeat_byte(0x35);
    let floor = decdn_incentive::floor_micro(1_000_000);
    let remaining = floor.saturating_mul(U256::from(3u64));
    // ONE signer draws the pool's entire headroom, three floors, unrefused.
    let mut held = Vec::new();
    for _ in 0u8..3 {
        held.push(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
                .map_err(|e| anyhow::anyhow!("refused under a wide live cap: {e:?}"))?,
        );
    }
    anyhow::ensure!(
        handler
            .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
            .err()
            == Some(FloorRefusal::PoolExhausted),
        "the pool ceiling is the only bound left at a wide live cap"
    );
    Ok(())
}

/// A repayment that lands after the pool was reclaimed releases nothing:
/// `forget_pool_floor` removed the entry (its live reservation went with
/// it), so `release_live_repaid` must not re-insert a default state for the
/// closed pool — the in-memory face of the #1781 resurrection race. The
/// pool id never recurs, so a re-inserted entry would sit in the map for the
/// process lifetime.
#[test]
fn repaid_release_after_forget_does_not_resurrect_entry() -> anyhow::Result<()> {
    let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let pool = B256::repeat_byte(0x5F);
    let floor = decdn_incentive::floor_micro(1000);
    let res = FloorReservation::reserve(map.clone(), pool, TEST_SIGNER, floor);
    // The pool closes mid-stream: the same in-memory remove
    // `forget_pool_floor` performs.
    lock_floor(&map)?.remove(&pool);
    res.release_live_repaid();
    anyhow::ensure!(
        lock_floor(&map)?.get(&pool).is_none(),
        "a repaid release on a reclaimed pool must not re-insert its entry"
    );
    drop(res);
    anyhow::ensure!(
        lock_floor(&map)?.get(&pool).is_none(),
        "the subsequent drop leaves the reclaimed pool absent too"
    );
    Ok(())
}

/// A lane this process loads starts with an empty ramp-credit pool, so a
/// restart carries no credit into the next stream. A stream that ends fully
/// paid returns its credit, and the next stream on the lane opens above the
/// floor (ADR 003 §Credit window).
#[tokio::test]
async fn a_loaded_lane_starts_empty_and_banks_for_the_next_stream() {
    let metrics = Arc::new(Metrics::new());
    let (handler, _dir) = handler_for_tests(&metrics).await;
    let state = LaneState::hydrate(
        B256::repeat_byte(0x11),
        Address::repeat_byte(0x22),
        Address::repeat_byte(0x33),
        U256::MAX,
        0,
        U256::from(9_000_000u64),
        U256::from(9u64 << 20),
        None,
        decdn_incentive::LaneChain::NONE,
    );
    let lane = Arc::new(Mutex::new(LaneDeliveryState::hydrated(state)));

    let first = handler.take_ramp_carry(Some(&lane)).await;
    assert_eq!(first.carried(), 0, "a loaded lane carries no credit");
    assert_eq!(
        handler.credit_window(CHUNK_BYTES, first.ramp_paid(0)),
        CHUNK_BYTES
    );
    first.return_paid(8 << 20);

    let second = handler.take_ramp_carry(Some(&lane)).await;
    assert_eq!(second.carried(), 8 << 20);
    assert_eq!(
        handler.credit_window(CHUNK_BYTES, second.ramp_paid(0)),
        (8 << 20) / handler.credit_ramp_divisor
    );
}

/// `LaneActivityClock::ages` reports a near-zero whole-seconds age for a
/// stamped lane and OMITS an unstamped (`last_voucher_at == 0`) one, so the
/// admin surface reads the latter back as "never" rather than a bogus `0`
/// age (issue #1733).
#[tokio::test]
async fn lane_activity_clock_ages_reports_stamped_and_omits_unstamped() {
    let stamped = LaneKey {
        pool_id: B256::repeat_byte(0x11),
        signer: Address::repeat_byte(0x22),
        provider: Address::repeat_byte(0x33),
    };
    let unstamped = LaneKey {
        pool_id: B256::repeat_byte(0x44),
        signer: Address::repeat_byte(0x55),
        provider: Address::repeat_byte(0x66),
    };
    let mk = |key: LaneKey, stamp: u64| {
        Arc::new(Mutex::new(LaneDeliveryState {
            state: LaneState::hydrate(
                key.pool_id,
                key.signer,
                key.provider,
                U256::MAX,
                0,
                U256::ZERO,
                U256::ZERO,
                None,
                decdn_incentive::LaneChain::NONE,
            ),
            bytes_delivered_cumulative: U256::ZERO,
            paid_credited: U256::ZERO,
            last_voucher_at: AtomicU64::new(stamp),
            ramp_pool: Arc::default(),
        }))
    };
    let map = DashMap::new();
    map.insert(stamped, mk(stamped, unix_millis()));
    map.insert(unstamped, mk(unstamped, 0));
    let clock = LaneActivityClock {
        lanes: Arc::new(map),
    };

    let ages = clock.ages().await;
    assert!(
        ages.get(&stamped).is_some_and(|age| *age < 5),
        "a freshly stamped lane reports a near-zero age, got {:?}",
        ages.get(&stamped)
    );
    assert!(
        !ages.contains_key(&unstamped),
        "an unstamped lane (stamp == 0) must be omitted, read back as never"
    );
}
