use super::*;

fn boot_retries(metrics: &Metrics) -> u64 {
    let text = metrics.encode().unwrap_or_default();
    text.lines()
        .find_map(|l| l.strip_prefix("decdn_chain_boot_read_retries_total "))
        .and_then(|v| v.parse().ok())
        .unwrap_or(u64::MAX)
}

/// A transient error on the `usdc()` self-check is retried, not fatal.
#[tokio::test(start_paused = true)]
async fn a_transient_usdc_self_check_error_is_retried() {
    use crate::chain_events::boot_retry::BOOT_CHAIN_RETRY_BUDGET;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolValue;

    let usdc = Address::repeat_byte(0x33);
    let asserter = Asserter::new();
    asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
    asserter.push_success(&alloy::primitives::Bytes::from(usdc.abi_encode()));
    let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);
    let metrics = Arc::new(Metrics::new());

    let got = usdc_self_check(
        &PaymentPool::new(Address::repeat_byte(0x11), provider),
        &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
    )
    .await;

    assert_eq!(got.ok(), Some(usdc));
    assert_eq!(boot_retries(&metrics), 1);
}

/// No contract at the configured address fails the self-check at once.
#[tokio::test(start_paused = true)]
async fn a_missing_payment_pool_fails_the_self_check_at_once() {
    use crate::chain_events::boot_retry::BOOT_CHAIN_RETRY_BUDGET;
    use alloy::providers::mock::Asserter;

    let asserter = Asserter::new();
    asserter.push_success(&alloy::primitives::Bytes::new());
    let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);
    let metrics = Arc::new(Metrics::new());
    let start = tokio::time::Instant::now();

    let err = usdc_self_check(
        &PaymentPool::new(Address::repeat_byte(0x11), provider),
        &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
    )
    .await
    .err()
    .map(|e| format!("{e:#}"))
    .unwrap_or_default();

    assert!(err.contains("not retried"), "{err}");
    assert_eq!(boot_retries(&metrics), 0);
    assert_eq!(start.elapsed(), Duration::ZERO);
}

fn status(remaining: u64, lifecycle: Lifecycle) -> PoolStatus {
    PoolStatus {
        owner: Address::from([1u8; 20]),
        remaining: U256::from(remaining),
        lifecycle,
    }
}

#[test]
fn unknown_pool_fails_open() {
    assert!(pool_is_redeemable(None, 1_000));
}

fn pool(owner: Address, status: PaymentPool::Status, deadline: u64) -> PaymentPool::Pool {
    PaymentPool::Pool {
        owner,
        status,
        disputeDeadline: deadline,
        deposit: 1_000,
        totalRedeemed: 0,
    }
}

#[test]
fn resolved_lifecycle_open_pool_is_servable() {
    let p = pool(Address::from([7u8; 20]), PaymentPool::Status::Open, 0);
    assert_eq!(resolved_lifecycle(&p), Some(Lifecycle::Open));
}

#[test]
fn resolved_lifecycle_closing_pool_carries_its_deadline() {
    let p = pool(
        Address::from([7u8; 20]),
        PaymentPool::Status::Closing,
        1_900_000_000,
    );
    assert_eq!(
        resolved_lifecycle(&p),
        Some(Lifecycle::Closing {
            deadline: 1_900_000_000
        })
    );
}

/// An instant `age` in the past, for the freshness gate.
fn aged(age: Duration) -> Instant {
    Instant::now().checked_sub(age).unwrap_or_else(Instant::now)
}

/// An instant past the fault window but inside the verdict window.
fn between_windows() -> Instant {
    aged(RESOLVE_FAULT_TTL + Duration::from_secs(1))
}

/// An instant past both windows.
fn stale_instant() -> Instant {
    aged(RESOLVE_VERDICT_TTL + Duration::from_secs(1))
}

#[test]
fn negative_cache_hit_only_for_a_fresh_entry() {
    let mut cache = HashMap::new();
    let id = B256::repeat_byte(0x33);
    assert_eq!(
        negative_cache_hit(&cache, id),
        None,
        "an absent id is not a hit"
    );
    for reason in [NegativeReason::Verdict, NegativeReason::Fault] {
        remember_negative(&mut cache, id, reason);
        assert_eq!(
            negative_cache_hit(&cache, id),
            Some(reason),
            "a just-recorded id is a hit that names its reason"
        );
        cache.insert(id, (reason, stale_instant()));
        assert_eq!(
            negative_cache_hit(&cache, id),
            None,
            "an entry past its window is re-checked, not suppressed"
        );
    }
}

/// A fault lapses long before a verdict: an entry of the same age suppresses
/// a repeat `getPool` as a verdict but not as a fault.
#[test]
fn negative_cache_fault_lapses_before_a_verdict() {
    let mut cache = HashMap::new();
    let id = B256::repeat_byte(0x34);
    cache.insert(id, (NegativeReason::Fault, between_windows()));
    assert_eq!(
        negative_cache_hit(&cache, id),
        None,
        "a fault past its short window is re-checked"
    );
    cache.insert(id, (NegativeReason::Verdict, between_windows()));
    assert_eq!(
        negative_cache_hit(&cache, id),
        Some(NegativeReason::Verdict),
        "a verdict of the same age still suppresses"
    );
}

#[test]
fn remember_negative_prunes_expired_entries_at_the_cap() {
    let mut cache = HashMap::new();
    // Fill to the cap with stale entries, then record one more: the insert
    // prunes the expired ones instead of growing past the cap.
    for i in 0..RESOLVE_NEGATIVE_CACHE_MAX {
        let id = B256::from(U256::from(i).to_be_bytes::<32>());
        cache.insert(id, (NegativeReason::Verdict, stale_instant()));
    }
    assert_eq!(cache.len(), RESOLVE_NEGATIVE_CACHE_MAX);
    remember_negative(&mut cache, B256::repeat_byte(0xff), NegativeReason::Verdict);
    assert_eq!(
        cache.len(),
        1,
        "the cap-prune drops every expired entry, leaving only the fresh insert"
    );
}

/// The cap-prune applies each entry's own window: a fault past its short
/// window goes, a verdict of the same age stays.
#[test]
fn remember_negative_prunes_each_entry_by_its_reason() {
    let mut cache = HashMap::new();
    let mut verdicts = 0;
    for i in 0..RESOLVE_NEGATIVE_CACHE_MAX {
        let id = B256::from(U256::from(i).to_be_bytes::<32>());
        let reason = if i % 2 == 0 {
            verdicts += 1;
            NegativeReason::Verdict
        } else {
            NegativeReason::Fault
        };
        cache.insert(id, (reason, between_windows()));
    }
    remember_negative(&mut cache, B256::repeat_byte(0xff), NegativeReason::Fault);
    assert_eq!(
        cache.len(),
        verdicts + 1,
        "the prune keeps every in-window verdict and drops every lapsed fault"
    );
    assert!(
        cache
            .values()
            .filter(|(_, at)| at.elapsed() >= RESOLVE_FAULT_TTL)
            .all(|(reason, _)| *reason == NegativeReason::Verdict),
        "no lapsed fault survives the prune"
    );
}

/// A cache full of in-window entries skips a new insert rather than grow
/// past the cap; an existing entry is still refreshed.
#[test]
fn remember_negative_skips_the_insert_when_full_of_fresh_entries() {
    let mut cache = HashMap::new();
    for i in 0..RESOLVE_NEGATIVE_CACHE_MAX {
        let id = B256::from(U256::from(i).to_be_bytes::<32>());
        remember_negative(&mut cache, id, NegativeReason::Verdict);
    }
    let newcomer = B256::repeat_byte(0xff);
    remember_negative(&mut cache, newcomer, NegativeReason::Fault);
    assert_eq!(
        cache.len(),
        RESOLVE_NEGATIVE_CACHE_MAX,
        "the map stays at the cap"
    );
    assert_eq!(
        negative_cache_hit(&cache, newcomer),
        None,
        "the newcomer is not cached"
    );

    let resident = B256::from(U256::from(0u8).to_be_bytes::<32>());
    remember_negative(&mut cache, resident, NegativeReason::Fault);
    assert_eq!(
        negative_cache_hit(&cache, resident),
        Some(NegativeReason::Fault),
        "a resident entry is still overwritten"
    );
}

#[test]
fn resolved_lifecycle_skips_zero_owner_and_closed() {
    // A nonexistent pool (zero owner) and a reclaimed (Closed) pool both seed
    // nothing — the serve gate stays fail-open None rather than register a lane
    // against funds that cannot be redeemed.
    let no_owner = pool(Address::ZERO, PaymentPool::Status::Open, 0);
    assert_eq!(resolved_lifecycle(&no_owner), None);
    let closed = pool(Address::from([7u8; 20]), PaymentPool::Status::Closed, 0);
    assert_eq!(resolved_lifecycle(&closed), None);
}

/// A mocked provider whose `eth_call` queue returns one ABI-encoded `getPool`
/// result. `Pool` is a static tuple, so its `SolValue` encoding equals the
/// single-struct return `getPool` decodes.
fn mocked_getpool_view(
    response: Option<PaymentPool::Pool>,
) -> (
    ResolvingPoolView<impl Provider + Clone + 'static>,
    PoolProjection,
    alloy::providers::mock::Asserter,
) {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolValue;

    let asserter = Asserter::new();
    if let Some(pool) = response {
        asserter.push_success(&Bytes::from(pool.abi_encode()));
    }
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let contract = PaymentPool::new(Address::ZERO, provider);
    let projection = PoolProjection::new();
    let view = ResolvingPoolView::new(contract, projection.clone(), Arc::new(Metrics::new()));
    (view, projection, asserter)
}

/// Admit path (i): a pool the projection has not observed triggers ONE
/// `getPool`; a solvent pool is folded into the projection and admitted. A
/// second request is a projection hit and issues no further `getPool`.
#[tokio::test]
async fn resolving_status_does_getpool_on_miss_and_admits_solvent_pool() -> Result<()> {
    use crate::pool_view::PoolView;

    let owner = Address::from([7u8; 20]);
    let (view, projection, asserter) =
        mocked_getpool_view(Some(pool(owner, PaymentPool::Status::Open, 0)));
    let pool_id = B256::repeat_byte(0x44);

    let status = view
        .status(pool_id)
        .await
        .ok_or_else(|| anyhow::anyhow!("a solvent unknown pool must admit via getPool"))?;
    assert_eq!(status.owner, owner);
    // `pool()` seeds deposit 1000, totalRedeemed 0.
    assert_eq!(status.remaining, U256::from(1_000u64));
    assert!(
        projection.snapshot(pool_id).is_some(),
        "the resolved pool is folded into the projection"
    );
    assert_eq!(
        asserter.read_q().len(),
        0,
        "exactly one getPool was consumed"
    );

    // Admit path (iii): the second request hits the projection — no getPool.
    // The queue is empty, so any second eth_call would error and yield None;
    // a Some result therefore proves the projection served it.
    let again = view
        .status(pool_id)
        .await
        .ok_or_else(|| anyhow::anyhow!("a confirmed pool stays admitted from the projection"))?;
    assert_eq!(again.owner, owner);
    Ok(())
}

/// Admit path (ii, verdict): an absent (`owner == 0`) or `Closed` pool the
/// `getPool` returns is refused (`None`), never folded, and negative-cached as
/// a `Verdict`. The entry still suppresses a repeat `getPool` past the fault
/// window: a queued live answer is left unread and the pool stays refused.
#[tokio::test]
async fn resolving_status_refuses_absent_pool_and_caches_negative() -> Result<()> {
    use crate::pool_view::PoolView;
    use alloy::sol_types::SolValue;

    let owner = Address::from([7u8; 20]);
    for (case, dead) in [
        ("absent", pool(Address::ZERO, PaymentPool::Status::Open, 0)),
        ("closed", pool(owner, PaymentPool::Status::Closed, 0)),
    ] {
        let (view, projection, asserter) = mocked_getpool_view(Some(dead));
        let pool_id = B256::repeat_byte(0x55);

        assert!(view.status(pool_id).await.is_none(), "{case}: refused");
        assert!(
            projection.snapshot(pool_id).is_none(),
            "{case}: never folded into the projection"
        );
        let reason = view
            .negative
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&pool_id)
            .map(|(reason, _)| *reason);
        assert_eq!(reason, Some(NegativeReason::Verdict), "{case}: a verdict");

        // Age the entry past the fault window and queue a live answer: a
        // verdict still suppresses, so the answer is never read.
        view.negative
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(pool_id, (NegativeReason::Verdict, between_windows()));
        asserter.push_success(&Bytes::from(
            pool(owner, PaymentPool::Status::Open, 0).abi_encode(),
        ));
        assert!(
            view.status(pool_id).await.is_none(),
            "{case}: still refused"
        );
        assert_eq!(asserter.read_q().len(), 1, "{case}: no second getPool");
    }
    Ok(())
}

/// Admit path (ii, fault): a `getPool` RPC error refuses the pool (`None`) and
/// negative-caches it as a `Fault`, so a re-request flood inside
/// `RESOLVE_FAULT_TTL` cannot storm `getPool`.
#[tokio::test]
async fn resolving_status_refuses_on_getpool_error_and_caches_negative() -> Result<()> {
    use crate::pool_view::PoolView;

    // No response queued: the mocked eth_call errors.
    let (view, projection, _asserter) = mocked_getpool_view(None);
    let pool_id = B256::repeat_byte(0x66);

    assert!(
        view.status(pool_id).await.is_none(),
        "a getPool fault refuses the pool"
    );
    assert!(projection.snapshot(pool_id).is_none());
    let reason = view
        .negative
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&pool_id)
        .map(|(reason, _)| *reason);
    assert_eq!(
        reason,
        Some(NegativeReason::Fault),
        "an RPC error is negative-cached as a fault"
    );
    Ok(())
}

/// A live pool refused on a fault is admitted once the fault window lapses
/// and the RPC answers: the re-read folds it and drops the negative entry.
#[tokio::test]
async fn resolving_status_admits_a_faulted_pool_after_the_rpc_recovers() -> Result<()> {
    use crate::pool_view::PoolView;
    use alloy::sol_types::SolValue;

    let (view, projection, asserter) = mocked_getpool_view(None);
    let pool_id = B256::repeat_byte(0x67);
    let owner = Address::from([9u8; 20]);
    assert!(view.status(pool_id).await.is_none(), "the fault refuses");

    view.negative
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(pool_id, (NegativeReason::Fault, between_windows()));
    asserter.push_success(&Bytes::from(
        pool(owner, PaymentPool::Status::Open, 0).abi_encode(),
    ));
    assert!(view.status(pool_id).await.is_some(), "the re-read admits");
    assert_eq!(
        projection.snapshot(pool_id).map(|s| s.owner),
        Some(owner),
        "the pool is folded"
    );
    assert!(
        !view
            .negative
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&pool_id),
        "the resolve drops the negative entry"
    );
    Ok(())
}

/// `cached_status` never issues a `getPool`: it reads the projection only, so
/// an unknown pool returns `None` even though `status` would resolve it, and
/// the mock's response queue is left untouched.
#[tokio::test]
async fn resolving_cached_status_never_calls_getpool() -> Result<()> {
    use crate::pool_view::PoolView;

    let owner = Address::from([7u8; 20]);
    let (view, _projection, asserter) =
        mocked_getpool_view(Some(pool(owner, PaymentPool::Status::Open, 0)));
    let pool_id = B256::repeat_byte(0x77);

    assert!(
        view.cached_status(pool_id).await.is_none(),
        "cached_status does not resolve an unknown pool on-chain"
    );
    assert_eq!(
        asserter.read_q().len(),
        1,
        "the queued getPool response is untouched by cached_status"
    );
    Ok(())
}

const AUTH_EXPIRY: u64 = 1_900_000_000;

/// A registered authorization (`expiry` non-zero), or the unregistered
/// all-zero struct when `cap == 0`.
fn authz(cap: u64, spent: u64) -> PaymentPool::Authorization {
    PaymentPool::Authorization {
        cap,
        expiry: if cap == 0 { 0 } else { AUTH_EXPIRY },
        spent,
    }
}

const fn registered(cap: u64, spent: u64) -> SignerAuthorization {
    SignerAuthorization::Registered {
        cap,
        expiry: AUTH_EXPIRY,
        spent,
    }
}

/// A mocked provider whose `eth_call` queue returns one ABI-encoded
/// `getAuthorization` result per entry. `Authorization` is an all-static
/// uint64 tuple, so its `SolValue` encoding equals the single-struct return
/// `getAuthorization` decodes.
fn mocked_getauth_view(
    responses: &[PaymentPool::Authorization],
) -> (
    ResolvingPoolView<impl Provider + Clone + 'static>,
    alloy::providers::mock::Asserter,
) {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolValue;

    let asserter = Asserter::new();
    for auth in responses {
        asserter.push_success(&Bytes::from(auth.abi_encode()));
    }
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let contract = PaymentPool::new(Address::ZERO, provider);
    let view = ResolvingPoolView::new(contract, PoolProjection::new(), Arc::new(Metrics::new()));
    (view, asserter)
}

/// A registered signer reports its registered terms and `spent`; the admit
/// gate covers a floor up to `cap − spent`.
#[tokio::test]
async fn registered_signer_reports_terms_and_spent() -> Result<()> {
    use crate::pool_view::PoolView;

    let (view, _asserter) = mocked_getauth_view(&[authz(1_000, 300)]);
    let auth = view
        .signer_authorization(B256::repeat_byte(0x11), Address::from([2u8; 20]))
        .await
        .ok_or_else(|| anyhow::anyhow!("a registered signer reports its authorization"))?;
    assert_eq!(auth, registered(1_000, 300));
    assert!(
        auth.covers(U256::from(700u64), AUTH_EXPIRY - 1),
        "cap 1000 − spent 300"
    );
    assert!(!auth.covers(U256::from(701u64), AUTH_EXPIRY - 1));
    Ok(())
}

/// A signer that has spent its full `cap` covers no floor — the dispatch gate
/// then refuses it, but the view reports the truth.
#[tokio::test]
async fn exhausted_signer_covers_no_floor() -> Result<()> {
    use crate::pool_view::PoolView;

    let (view, _asserter) = mocked_getauth_view(&[authz(1_000, 1_000)]);
    let auth = view
        .signer_authorization(B256::repeat_byte(0x22), Address::from([3u8; 20]))
        .await
        .ok_or_else(|| anyhow::anyhow!("an exhausted signer still reports a value"))?;
    assert_eq!(auth, registered(1_000, 1_000));
    assert!(
        !auth.covers(U256::from(1u64), AUTH_EXPIRY - 1),
        "spent == cap"
    );
    Ok(())
}

/// A registered signer whose registration has expired covers no floor, even
/// with headroom left: the chain redeems nothing at or past the expiry.
#[test]
fn expired_registration_covers_no_floor() {
    let auth = registered(1_000, 0);
    assert!(auth.covers(U256::from(1u64), AUTH_EXPIRY - 1));
    assert!(!auth.covers(U256::from(1u64), AUTH_EXPIRY));
}

/// An unregistered signer (`cap == 0 && expiry == 0`) is unconstrained — it
/// has spent nothing on-chain and admits on its presented capability — and a
/// second call within [`UNREGISTERED_AUTH_TTL`] is served from cache with no
/// further `getAuthorization`.
#[tokio::test]
async fn unregistered_signer_is_unconstrained_and_cached() -> Result<()> {
    use crate::pool_view::PoolView;

    // Only ONE response queued: a second on-chain read would error → None.
    let (view, asserter) = mocked_getauth_view(&[authz(0, 0)]);
    let pool_id = B256::repeat_byte(0x33);
    let signer = Address::from([4u8; 20]);

    let first = view
        .signer_authorization(pool_id, signer)
        .await
        .ok_or_else(|| anyhow::anyhow!("an unregistered signer is unconstrained"))?;
    assert_eq!(first, SignerAuthorization::Unregistered);
    assert!(first.covers(U256::MAX, u64::MAX), "no on-chain constraint");
    assert_eq!(
        asserter.read_q().len(),
        0,
        "exactly one getAuthorization was consumed"
    );

    let second = view
        .signer_authorization(pool_id, signer)
        .await
        .ok_or_else(|| anyhow::anyhow!("a fresh cache entry serves the second call"))?;
    assert_eq!(
        second,
        SignerAuthorization::Unregistered,
        "the cached read is returned unchanged"
    );
    assert_eq!(
        asserter.read_q().len(),
        0,
        "no second getAuthorization was issued within the TTL"
    );
    Ok(())
}

/// A REGISTERED signer with a zero `spendingCap` (`cap == 0` but `expiry != 0`)
/// is NOT the all-zero unregistered struct: it is registered with no headroom,
/// so the dispatch gate refuses it — it is not misread as unconstrained, which
/// would fail open and admit an uncashable signer.
#[tokio::test]
async fn registered_zero_cap_signer_is_refused_not_unconstrained() -> Result<()> {
    use crate::pool_view::PoolView;

    let auth = PaymentPool::Authorization {
        cap: 0,
        expiry: AUTH_EXPIRY,
        spent: 0,
    };
    let (view, _asserter) = mocked_getauth_view(&[auth]);
    let auth = view
        .signer_authorization(B256::repeat_byte(0x44), Address::from([5u8; 20]))
        .await
        .ok_or_else(|| anyhow::anyhow!("a registered zero-cap signer reports a value"))?;
    assert_eq!(
        auth,
        registered(0, 0),
        "cap == 0 with expiry != 0 is a registered zero-cap signer, not unregistered"
    );
    assert!(!auth.covers(U256::from(1u64), AUTH_EXPIRY - 1));
    Ok(())
}

/// A `getAuthorization` RPC fault for a signer this node has never read
/// returns `None`, so the caller refuses rather than fail open.
#[tokio::test]
async fn getauthorization_fault_refuses_signer() -> Result<()> {
    use crate::pool_view::PoolView;

    // No response queued: the mocked eth_call errors.
    let (view, _asserter) = mocked_getauth_view(&[]);
    assert!(
        view.signer_authorization(B256::repeat_byte(0x44), Address::from([5u8; 20]))
            .await
            .is_none(),
        "a fault with no cached read refuses the signer"
    );
    Ok(())
}

/// A `Registered` read answers however old it is: the second call finds an
/// empty response queue, so a chain read would fault and refuse.
#[tokio::test]
async fn a_registered_read_never_ages_out() -> Result<()> {
    use crate::pool_view::PoolView;

    let pool_id = B256::repeat_byte(0x45);
    let signer = Address::from([6u8; 20]);
    let (view, _asserter) = mocked_getauth_view(&[authz(1_000_000, 200_000)]);
    assert_eq!(
        view.signer_authorization(pool_id, signer).await,
        Some(registered(1_000_000, 200_000))
    );
    age_auth_read(&view, pool_id, signer)?;
    assert_eq!(
        view.signer_authorization(pool_id, signer).await,
        Some(registered(1_000_000, 200_000)),
        "the old registered read answers with no chain read"
    );
    assert_eq!(signer_auth_counts(&view.metrics), (1, 1, 0));
    Ok(())
}

/// The admit signer-confirm counts as `(cached, first_read, reread)`, read
/// from the scrape text.
fn signer_auth_counts(metrics: &Metrics) -> (u64, u64, u64) {
    let text = metrics.encode().unwrap_or_default();
    let value = |name: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.parse::<u64>().ok())
            .unwrap_or(u64::MAX)
    };
    (
        value("decdn_serve_signer_auth_cached_total"),
        value("decdn_serve_signer_auth_first_read_total"),
        value("decdn_serve_signer_auth_reread_total"),
    )
}

/// Backdate the held read of `(pool_id, signer)` past
/// [`UNREGISTERED_AUTH_TTL`].
fn age_auth_read(
    view: &ResolvingPoolView<impl Provider + Clone>,
    pool_id: B256,
    signer: Address,
) -> Result<()> {
    let mut guard = view
        .auth_cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let entry = guard
        .get_mut(&(pool_id, signer))
        .ok_or_else(|| anyhow::anyhow!("the first read is cached"))?;
    entry.at = Instant::now()
        .checked_sub(UNREGISTERED_AUTH_TTL * 2)
        .ok_or_else(|| anyhow::anyhow!("clock too close to its epoch"))?;
    Ok(())
}

/// In-memory sink for the tracing output a test captures.
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

impl CapturedLog {
    /// Capture this thread's tracing output until the guard drops. A
    /// paused-clock test runs on one thread, so the thread default sees
    /// every event the view logs.
    fn install(&self) -> tracing::subscriber::DefaultGuard {
        let writer = self.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    /// Every captured line that contains `needle`, or an error that shows
    /// the whole log when none does.
    fn lines(&self, needle: &str) -> Result<Vec<String>> {
        let bytes = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let text = String::from_utf8(bytes)?;
        let found: Vec<String> = text
            .lines()
            .filter(|l| l.contains(needle))
            .map(str::to_owned)
            .collect();
        if found.is_empty() {
            anyhow::bail!("no captured line contains {needle:?}:\n{text}");
        }
        Ok(found)
    }
}

/// A view whose every chain read hangs forever. Pair with
/// `#[tokio::test(start_paused = true)]` so the `timed` bound fires on
/// virtual time.
fn hanging_view() -> ResolvingPoolView<impl Provider + Clone + 'static> {
    counting_hanging_view().0
}

/// A [`hanging_view`] plus a count of the chain reads dispatched to it.
fn counting_hanging_view() -> (
    ResolvingPoolView<impl Provider + Clone + 'static>,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let (provider, calls) = crate::chain_events::test_support::counting_hanging_provider();
    let view = ResolvingPoolView::new(
        PaymentPool::new(Address::ZERO, provider),
        PoolProjection::new(),
        Arc::new(Metrics::new()),
    );
    (view, calls)
}

/// A hung admit `getPool` times out and takes the fault path: the pool is
/// refused and negative-cached as a fault, so a request inside
/// `RESOLVE_FAULT_TTL` refuses at once instead of waiting again. The WARN
/// carries the timeout and the fault window.
#[tokio::test(start_paused = true)]
async fn admit_getpool_hang_refuses_within_the_bound() -> Result<()> {
    use crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT;
    use crate::chain_events::test_support::bounded;
    use crate::pool_view::PoolView;

    let log = CapturedLog::default();
    let _subscriber = log.install();
    let view = hanging_view();
    let pool_id = B256::repeat_byte(0x51);
    let started = tokio::time::Instant::now();
    assert!(
        bounded("admit getPool", view.status(pool_id))
            .await
            .is_none()
    );
    assert_eq!(
        started.elapsed(),
        DEFAULT_RPC_CALL_TIMEOUT,
        "the admit read uses the default bound"
    );
    let reason = view
        .negative
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&pool_id)
        .map(|(reason, _)| *reason);
    assert_eq!(
        reason,
        Some(NegativeReason::Fault),
        "the timed-out pool is negative-cached as a fault"
    );
    assert!(
        bounded("admit getPool", view.status(pool_id))
            .await
            .is_none(),
        "the negative cache refuses the next request"
    );
    assert_eq!(
        started.elapsed(),
        DEFAULT_RPC_CALL_TIMEOUT,
        "the next request does not wait on the chain again"
    );
    let warns = log.lines("admit getPool failed")?;
    assert_eq!(warns.len(), 1, "only the first request reads: {warns:?}");
    let line = warns.first().map_or("", String::as_str);
    assert!(line.contains("WARN"), "{line}");
    assert!(
        line.contains("admit getPool timed out after 10s"),
        "the WARN keeps the failure class: {line}"
    );
    assert!(
        line.contains("suppressed_for=5s"),
        "the WARN names the fault window: {line}"
    );
    Ok(())
}

/// Concurrent requests for one pool share one in-flight `getPool`: they all
/// end when the single hung read times out, and only that read reaches the
/// RPC.
#[tokio::test(start_paused = true)]
async fn admit_getpool_coalesces_concurrent_reads() {
    use crate::chain_events::test_support::bounded;
    use crate::pool_view::PoolView;
    use std::sync::atomic::Ordering;

    let (view, calls) = counting_hanging_view();
    let pool_id = B256::repeat_byte(0x55);
    let started = tokio::time::Instant::now();
    let answers = bounded(
        "admit getPool",
        futures_util::future::join_all((0..8).map(|_| view.status(pool_id))),
    )
    .await;
    assert!(
        answers.iter().all(Option::is_none),
        "every request is refused"
    );
    assert_eq!(
        started.elapsed(),
        crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT,
        "the waiters end with the one read, not after reads of their own"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "one getPool for eight requests"
    );
    assert!(
        view.inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty(),
        "the ended read leaves no in-flight entry"
    );
}

/// With the negative cache full of in-window entries, the read's fault is
/// not cached; its waiters still take the read's answer instead of each
/// starting a read of its own in turn.
#[tokio::test(start_paused = true)]
async fn admit_getpool_waiters_take_the_answer_when_the_cache_is_full() {
    use crate::chain_events::test_support::bounded;
    use crate::pool_view::PoolView;
    use std::sync::atomic::Ordering;

    let (view, calls) = counting_hanging_view();
    {
        let mut negative = view
            .negative
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for i in 0..RESOLVE_NEGATIVE_CACHE_MAX {
            let id = B256::from(U256::from(i).to_be_bytes::<32>());
            remember_negative(&mut negative, id, NegativeReason::Verdict);
        }
    }
    let pool_id = B256::repeat_byte(0xfe);
    let started = tokio::time::Instant::now();
    let answers = bounded(
        "admit getPool",
        futures_util::future::join_all((0..8).map(|_| view.status(pool_id))),
    )
    .await;
    assert!(
        answers.iter().all(Option::is_none),
        "every request is refused"
    );
    assert_eq!(
        started.elapsed(),
        crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT,
        "the waiters end with the one read"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "one getPool for eight requests"
    );
    assert!(
        !view
            .negative
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&pool_id),
        "the full cache did not record the fault"
    );
}

/// A reader that is cancelled mid-read ends the read for its waiters: one of
/// them takes over and issues its own `getPool`.
#[tokio::test(start_paused = true)]
async fn admit_getpool_waiter_takes_over_a_cancelled_read() {
    use crate::chain_events::test_support::bounded;
    use crate::pool_view::PoolView;
    use std::sync::atomic::Ordering;

    let (view, calls) = counting_hanging_view();
    let pool_id = B256::repeat_byte(0x56);
    let cancel_after = Duration::from_secs(1);
    let started = tokio::time::Instant::now();
    let (reader, waiter) = bounded("admit getPool", async {
        tokio::join!(
            tokio::time::timeout(cancel_after, view.status(pool_id)),
            view.status(pool_id)
        )
    })
    .await;
    assert!(reader.is_err(), "the first reader is cancelled");
    assert!(waiter.is_none(), "the waiter's own read times out");
    assert_eq!(
        started.elapsed(),
        cancel_after + crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT,
        "the waiter reads from the cancellation on"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2, "the waiter issued a read");
}

/// A pool negative-cached for a fault is read again once the short fault
/// window lapses, while a verdict of the same age still suppresses the read.
/// The cache stamps each entry with a `std::time::Instant`, which the paused
/// tokio clock does not advance, so each entry is backdated rather than
/// waited out.
#[tokio::test(start_paused = true)]
async fn admit_getpool_rereads_a_faulted_pool_after_the_fault_window() {
    use crate::chain_events::test_support::bounded;
    use crate::pool_view::PoolView;

    let view = hanging_view();
    let pool_id = B256::repeat_byte(0x54);
    let backdate = |reason| {
        view.negative
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(pool_id, (reason, between_windows()));
    };

    backdate(NegativeReason::Fault);
    let started = tokio::time::Instant::now();
    assert!(
        bounded("admit getPool", view.status(pool_id))
            .await
            .is_none()
    );
    assert_eq!(
        started.elapsed(),
        crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT,
        "a lapsed fault re-reads the pool (and waits out the hung read)"
    );

    backdate(NegativeReason::Verdict);
    let started = tokio::time::Instant::now();
    assert!(
        bounded("admit getPool", view.status(pool_id))
            .await
            .is_none()
    );
    assert_eq!(
        started.elapsed(),
        Duration::ZERO,
        "a verdict of the same age suppresses the read"
    );
}

/// A held `Registered` read answers at once against a hung RPC: the admit
/// path sends no read for it.
#[tokio::test(start_paused = true)]
async fn a_registered_read_answers_without_reaching_a_hung_rpc() -> Result<()> {
    use crate::chain_events::test_support::bounded;
    use crate::pool_view::PoolView;
    use std::sync::atomic::Ordering;

    let (view, calls) = counting_hanging_view();
    let pool_id = B256::repeat_byte(0x52);
    let signer = Address::from([7u8; 20]);
    let aged = Instant::now()
        .checked_sub(UNREGISTERED_AUTH_TTL * 2)
        .ok_or_else(|| anyhow::anyhow!("clock too close to its epoch"))?;
    view.auth_cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(
            (pool_id, signer),
            AuthRead {
                auth: registered(1_000_000, 200_000),
                folded_at_read: 0,
                at: aged,
            },
        );
    let started = tokio::time::Instant::now();
    assert_eq!(
        bounded(
            "admit getAuthorization",
            view.signer_authorization(pool_id, signer)
        )
        .await,
        Some(registered(1_000_000, 200_000)),
    );
    assert_eq!(started.elapsed(), Duration::ZERO, "no wait on the chain");
    assert_eq!(calls.load(Ordering::Relaxed), 0, "no getAuthorization sent");
    Ok(())
}

/// Concurrent confirms of one `(pool, signer)` share one in-flight
/// `getAuthorization`: they all end when the single hung read times out,
/// and only that read reaches the RPC.
#[tokio::test(start_paused = true)]
async fn admit_getauthorization_coalesces_concurrent_reads() {
    use crate::chain_events::test_support::bounded;
    use crate::pool_view::PoolView;
    use std::sync::atomic::Ordering;

    let (view, calls) = counting_hanging_view();
    let pool_id = B256::repeat_byte(0x56);
    let signer = Address::from([9u8; 20]);
    let started = tokio::time::Instant::now();
    let answers = bounded(
        "admit getAuthorization",
        futures_util::future::join_all((0..8).map(|_| view.signer_authorization(pool_id, signer))),
    )
    .await;
    assert!(
        answers.iter().all(Option::is_none),
        "every confirm is refused"
    );
    assert_eq!(
        started.elapsed(),
        crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT,
        "the waiters end with the one read, not after reads of their own"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "one getAuthorization for eight confirms"
    );
    assert_eq!(signer_auth_counts(&view.metrics), (0, 8, 0));
    assert!(
        view.auth_inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty(),
        "the ended read leaves no in-flight entry"
    );
}

/// A hung admit `getAuthorization` with no cached read times out and refuses
/// the signer.
#[tokio::test(start_paused = true)]
async fn admit_getauthorization_hang_with_no_cached_read_refuses() -> Result<()> {
    use crate::chain_events::test_support::bounded;
    use crate::pool_view::PoolView;

    let log = CapturedLog::default();
    let _subscriber = log.install();
    let view = hanging_view();
    let pool_id = B256::repeat_byte(0x53);
    let signer = Address::from([8u8; 20]);
    assert_eq!(
        bounded(
            "admit getAuthorization",
            view.signer_authorization(pool_id, signer)
        )
        .await,
        None
    );
    // A fault caches nothing: a cached `Unregistered` here would answer the
    // fast path for a full TTL.
    assert!(
        view.auth_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty(),
        "a timed-out read leaves the signer cache empty"
    );
    let warns = log.lines("refusing this signer")?;
    let line = warns.first().map_or("", String::as_str);
    assert!(line.contains("WARN"), "{line}");
    assert!(
        line.contains("admit getAuthorization timed out after 10s"),
        "the WARN keeps the failure class: {line}"
    );
    Ok(())
}

/// A held `Registered` read adds what the projection folds for the signer
/// after the read, from any provider, so a signer that drains its shared cap
/// at other nodes is not admitted on the old headroom.
#[tokio::test]
async fn a_registered_read_adds_the_spent_folded_since() {
    use crate::pool_view::PoolView;

    let pool_id = B256::repeat_byte(0x47);
    let signer = Address::from([8u8; 20]);
    let (view, _asserter) = mocked_getauth_view(&[authz(1_000_000, 200_000)]);
    let lane = |paid: u64| PaymentPool::LaneSettled {
        signer,
        newPaidCumulative: paid,
        bytesPaid: 0,
    };
    view.projection
        .record_opened(pool_id, Address::from([1u8; 20]), U256::from(5_000_000u64));
    // Folded before the read: the read's `spent` already holds it.
    view.projection
        .record_redeemed(pool_id, Address::from([9u8; 20]), &[lane(200_000)]);
    assert_eq!(
        view.signer_authorization(pool_id, signer).await,
        Some(registered(1_000_000, 200_000))
    );
    view.projection
        .record_redeemed(pool_id, Address::from([10u8; 20]), &[lane(650_000)]);
    assert_eq!(
        view.signer_authorization(pool_id, signer).await,
        Some(registered(1_000_000, 850_000)),
        "a drain folded since the read adds to spent"
    );
}

/// A fault after a cached `Unregistered` read has aged past the TTL refuses
/// the signer: a registration may have landed since, with its cap spent.
#[tokio::test]
async fn an_expired_unregistered_read_does_not_answer_a_fault() -> Result<()> {
    use crate::pool_view::PoolView;

    let pool_id = B256::repeat_byte(0x46);
    let signer = Address::from([7u8; 20]);
    let (view, _asserter) = mocked_getauth_view(&[authz(0, 0)]);
    assert_eq!(
        view.signer_authorization(pool_id, signer).await,
        Some(SignerAuthorization::Unregistered)
    );
    age_auth_read(&view, pool_id, signer)?;
    assert_eq!(
        view.signer_authorization(pool_id, signer).await,
        None,
        "an old Unregistered read is not trusted after a fault"
    );
    assert_eq!(signer_auth_counts(&view.metrics), (0, 1, 1));
    Ok(())
}

/// An `Unregistered` read lapses inside its window once the projection folds
/// a redemption by the signer: that redemption registered it.
#[tokio::test]
async fn a_folded_redemption_lapses_an_unregistered_read() {
    use crate::pool_view::PoolView;

    let pool_id = B256::repeat_byte(0x48);
    let signer = Address::from([11u8; 20]);
    let (view, asserter) = mocked_getauth_view(&[authz(0, 0), authz(1_000_000, 50_000)]);
    assert_eq!(
        view.signer_authorization(pool_id, signer).await,
        Some(SignerAuthorization::Unregistered)
    );
    view.projection
        .record_opened(pool_id, Address::from([1u8; 20]), U256::from(5_000_000u64));
    view.projection.record_redeemed(
        pool_id,
        Address::from([9u8; 20]),
        &[PaymentPool::LaneSettled {
            signer,
            newPaidCumulative: 50_000,
            bytesPaid: 0,
        }],
    );
    assert_eq!(
        view.signer_authorization(pool_id, signer).await,
        Some(registered(1_000_000, 50_000)),
        "the fold sends a re-read, which finds the registration"
    );
    assert_eq!(asserter.read_q().len(), 0, "both reads were sent");
    assert_eq!(
        view.signer_authorization(pool_id, signer).await,
        Some(registered(1_000_000, 50_000)),
    );
    assert_eq!(signer_auth_counts(&view.metrics), (1, 1, 1));
}

fn auth_read(auth: SignerAuthorization, folded_at_read: u64, age: Duration) -> AuthRead {
    AuthRead {
        auth,
        folded_at_read,
        at: Instant::now().checked_sub(age).unwrap_or_else(Instant::now),
    }
}

#[test]
fn cached_auth_classifies_each_read() {
    let fresh_unreg = auth_read(SignerAuthorization::Unregistered, 10, Duration::ZERO);
    assert_eq!(cached_auth(None, 0), CachedAuth::Absent);
    assert_eq!(
        cached_auth(Some(&fresh_unreg), 10),
        CachedAuth::Fresh(SignerAuthorization::Unregistered)
    );
    assert_eq!(
        cached_auth(Some(&fresh_unreg), 11),
        CachedAuth::Lapsed,
        "a fold since the read lapses an Unregistered read"
    );
    let old_unreg = auth_read(
        SignerAuthorization::Unregistered,
        10,
        UNREGISTERED_AUTH_TTL * 2,
    );
    assert_eq!(cached_auth(Some(&old_unreg), 10), CachedAuth::Lapsed);
    let old_reg = auth_read(registered(1_000, 100), 40, UNREGISTERED_AUTH_TTL * 2);
    assert_eq!(
        cached_auth(Some(&old_reg), 65),
        CachedAuth::Fresh(registered(1_000, 125)),
        "a registered read adds the fold since the read"
    );
    assert_eq!(
        cached_auth(Some(&old_reg), 0),
        CachedAuth::Fresh(registered(1_000, 100)),
        "a fold below the baseline adds nothing"
    );
}

#[test]
fn remember_auth_prunes_lapsed_then_evicts_the_oldest() {
    let key = |i: usize| {
        let mut bytes = [0u8; 20];
        bytes[..8].copy_from_slice(&(i as u64).to_be_bytes());
        (B256::ZERO, Address::from(bytes))
    };
    let mut cache = HashMap::new();
    let oldest = auth_read(registered(1, 0), 0, Duration::from_secs(10));
    cache.insert(key(0), oldest);
    for i in 1..AUTH_CACHE_MAX {
        cache.insert(key(i), auth_read(registered(1, 0), 0, Duration::ZERO));
    }
    let lapsed = auth_read(
        SignerAuthorization::Unregistered,
        0,
        UNREGISTERED_AUTH_TTL * 2,
    );
    cache.insert(key(1), lapsed);
    let fresh = auth_read(registered(1, 0), 0, Duration::ZERO);

    remember_auth(&mut cache, key(AUTH_CACHE_MAX), fresh);
    assert_eq!(cache.len(), AUTH_CACHE_MAX, "the lapsed read made room");
    assert!(!cache.contains_key(&key(1)));
    assert!(cache.contains_key(&key(0)), "nothing else was evicted");

    remember_auth(&mut cache, key(AUTH_CACHE_MAX + 1), fresh);
    assert_eq!(cache.len(), AUTH_CACHE_MAX);
    assert!(!cache.contains_key(&key(0)), "the oldest read was evicted");

    remember_auth(&mut cache, key(2), fresh);
    assert_eq!(cache.len(), AUTH_CACHE_MAX, "a held key replaces in place");
}

/// A second call for a `Registered` signer is served from cache, consuming no
/// further `getAuthorization` (proven by the single queued response and a
/// `Some` result on the second call).
#[tokio::test]
async fn signer_authorization_second_call_hits_cache() -> Result<()> {
    use crate::pool_view::PoolView;

    let (view, asserter) = mocked_getauth_view(&[authz(1_000, 200)]);
    let pool_id = B256::repeat_byte(0x55);
    let signer = Address::from([6u8; 20]);

    let first = view
        .signer_authorization(pool_id, signer)
        .await
        .ok_or_else(|| anyhow::anyhow!("the first call resolves on-chain"))?;
    assert_eq!(first, registered(1_000, 200));
    assert_eq!(asserter.read_q().len(), 0, "one getAuthorization consumed");

    let second = view
        .signer_authorization(pool_id, signer)
        .await
        .ok_or_else(|| anyhow::anyhow!("the second call is served from cache"))?;
    assert_eq!(
        second,
        registered(1_000, 200),
        "the cached read is returned"
    );
    assert_eq!(
        asserter.read_q().len(),
        0,
        "a registered read answers with no second getAuthorization"
    );
    Ok(())
}

#[test]
fn open_funded_is_redeemable() {
    assert!(pool_is_redeemable(
        Some(status(500, Lifecycle::Open)),
        1_000
    ));
}

#[test]
fn open_drained_is_held() {
    assert!(!pool_is_redeemable(Some(status(0, Lifecycle::Open)), 1_000));
}

#[test]
fn closing_drained_is_dropped() {
    assert!(!pool_is_redeemable(
        Some(status(0, Lifecycle::Closing { deadline: 2_000 })),
        1_000
    ));
}

/// A funded `Closing` pool stays redeemable while its deadline lies more than
/// the landing slack ahead.
#[test]
fn closing_funded_before_the_landing_slack_is_redeemable() {
    assert!(pool_is_redeemable(
        Some(status(500, Lifecycle::Closing { deadline: 2_000 })),
        2_000 - REDEEM_LANDING_SLACK_SECS - 1
    ));
}

/// A funded `Closing` pool whose deadline falls within the landing slack is
/// dropped: the batch could land past the deadline and revert `PoolClosed`.
#[test]
fn closing_funded_within_the_landing_slack_is_dropped() {
    assert!(!pool_is_redeemable(
        Some(status(500, Lifecycle::Closing { deadline: 2_000 })),
        2_000 - REDEEM_LANDING_SLACK_SECS
    ));
    assert!(!pool_is_redeemable(
        Some(status(500, Lifecycle::Closing { deadline: 2_000 })),
        1_999
    ));
}

/// A `Closing` deadline near `u64::MAX` saturates rather than wraps.
#[test]
fn closing_slack_saturates_at_the_top_of_the_clock() {
    assert!(!pool_is_redeemable(
        Some(status(500, Lifecycle::Closing { deadline: u64::MAX })),
        u64::MAX - 1
    ));
}

#[test]
fn closing_funded_at_or_after_deadline_is_dropped() {
    assert!(!pool_is_redeemable(
        Some(status(500, Lifecycle::Closing { deadline: 2_000 })),
        2_000
    ));
    assert!(!pool_is_redeemable(
        Some(status(500, Lifecycle::Closing { deadline: 2_000 })),
        2_500
    ));
}

#[test]
fn partition_keeps_redeemable_and_counts_skips() {
    // pool 1: open+funded (keep), pool 2: open+drained (skip), pool 3: unknown (keep, fail open)
    let s1 = signed_lane_state(1, 10, 20, None);
    let s2 = signed_lane_state(2, 11, 20, None);
    let s3 = signed_lane_state(3, 12, 20, None);
    let mut snap: HashMap<PoolId, Option<PoolStatus>> = HashMap::new();
    snap.insert(s1.pool_id, Some(status(500, Lifecycle::Open)));
    snap.insert(s2.pool_id, Some(status(0, Lifecycle::Open)));
    // s3's pool intentionally absent from snap -> None -> fail open
    let (kept, skipped) = partition_redeemable(vec![s1.clone(), s2, s3.clone()], &snap, 1_000);
    let kept_pools: Vec<_> = kept.iter().map(|st| st.pool_id).collect();
    assert_eq!(skipped, 1);
    assert!(kept_pools.contains(&s1.pool_id));
    assert!(kept_pools.contains(&s3.pool_id));
    assert_eq!(kept.len(), 2);
}

/// Build a [`PlannedLane`] for a given pool/signer, with an optional
/// capability registration, for `group_by_pool` tests.
fn planned(pool: u8, signer: u8, unredeemed: u64, register: bool) -> PlannedLane {
    let signer_addr = Address::from([signer; 20]);
    let reg = register.then(|| PaymentPool::CapabilityReg {
        signer: signer_addr,
        spendingCap: 1_000_000,
        expiry: 0,
        ownerSig: Bytes::from(vec![9u8; 65]),
    });
    PlannedLane {
        pool_id: PoolId::from([pool; 32]),
        key: LaneKey {
            pool_id: PoolId::from([pool; 32]),
            signer: signer_addr,
            provider: Address::from([0xEE; 20]),
        },
        owed: U256::from(unredeemed),
        unredeemed: U256::from(unredeemed),
        voucher: PaymentPool::LaneVoucher {
            signer: signer_addr,
            cumulative: unredeemed,
            bytesDelivered: 0,
            r: B256::ZERO,
            vs: B256::ZERO,
            // A sealed settlement voucher: the cooperative shape, which
            // walks nothing on-chain and compresses to almost no calldata.
            chainRoot: B256::ZERO,
            preimage: B256::ZERO,
            chainMeter: U256::ZERO,
        },
        register: reg,
    }
}

#[test]
fn group_by_pool_buckets_lanes_and_keeps_insertion_order() {
    let lanes = vec![
        planned(1, 10, 100, true),
        planned(2, 11, 200, false),
        planned(1, 12, 300, true),
    ];
    let batches = group_by_pool(&lanes);
    // Two pools, first-seen order: pool 1 then pool 2. Indexed via `.first()`
    // / `.get()` rather than `[]` per the workspace's anti-panic policy.
    assert_eq!(batches.len(), 2);
    let batch0 = batches.first();
    assert_eq!(batch0.map(|b| b.poolId), Some(PoolId::from([1u8; 32])));
    assert_eq!(batch0.map(|b| b.vouchers.len()), Some(2)); // both pool-1 lanes
    assert_eq!(batch0.map(|b| b.capabilities.len()), Some(2)); // both registered
    let batch1 = batches.get(1);
    assert_eq!(batch1.map(|b| b.poolId), Some(PoolId::from([2u8; 32])));
    assert_eq!(batch1.map(|b| b.vouchers.len()), Some(1));
    assert_eq!(batch1.map(|b| b.capabilities.len()), Some(0)); // register == false
}

/// Sum a chunk's unredeemed values, for `chunk_redemptions` tests. Delegates
/// to the production [`sum_unredeemed`] so the tests exercise the same
/// summation that feeds the `decdn_unredeemed_usdc` gauge.
fn total_unredeemed(chunk: &[PlannedLane]) -> U256 {
    sum_unredeemed(chunk)
}

#[test]
fn sum_unredeemed_totals_planned_lanes() {
    // Empty set is zero — the gauge reads 0 when nothing is owed.
    assert_eq!(sum_unredeemed(&[]), U256::ZERO);
    let plans = vec![
        planned(1, 10, 100, false),
        planned(1, 11, 250, false),
        planned(2, 12, 1_000_000, true),
    ];
    assert_eq!(sum_unredeemed(&plans), U256::from(1_000_350u64));
}

#[test]
fn reconcile_plans_drops_fully_settled_and_keeps_remainder() {
    // Three lanes, all owed 1000. On-chain: lane 0 already settled to 1000
    // (drop), lane 1 partially at 600 (keep, remainder 400), lane 2 never
    // redeemed at 0 (keep, remainder 1000).
    let mut plans = vec![
        planned(1, 0, 1_000, false),
        planned(1, 1, 1_000, false),
        planned(1, 2, 1_000, false),
    ];
    for p in &mut plans {
        p.owed = U256::from(1_000u64);
    }
    let onchain = [U256::from(1_000u64), U256::from(600u64), U256::from(0u64)];
    let (kept, skipped) = reconcile_plans(plans, &onchain);
    assert_eq!(skipped, 1, "the fully-settled lane is dropped");
    assert_eq!(kept.len(), 2);
    // Survivors carry the recomputed remainder, not the stale full owed.
    assert_eq!(kept.first().map(|p| p.unredeemed), Some(U256::from(400u64)));
    assert_eq!(
        kept.get(1).map(|p| p.unredeemed),
        Some(U256::from(1_000u64))
    );
}

#[test]
fn reconcile_plans_drops_when_onchain_exceeds_owed() {
    // A watermark strictly above owed (a fresher voucher already redeemed
    // elsewhere) still settles this claim to zero — drop it.
    let mut plans = vec![planned(1, 0, 1_000, false)];
    if let Some(p) = plans.first_mut() {
        p.owed = U256::from(1_000u64);
    }
    let (kept, skipped) = reconcile_plans(plans, &[U256::from(5_000u64)]);
    assert_eq!(skipped, 1);
    assert!(kept.is_empty());
}

#[test]
fn reconcile_plans_short_slice_keeps_untouched_tail() {
    // Fail-open parity guard: a slice shorter than the plans keeps the
    // unread tail unchanged rather than mis-pairing.
    let plans = vec![planned(1, 0, 1_000, false), planned(1, 1, 1_000, false)];
    let (kept, skipped) = reconcile_plans(plans, &[U256::from(1_000u64)]);
    assert_eq!(skipped, 1, "the read lane (settled) is dropped");
    assert_eq!(kept.len(), 1, "the unread lane is kept unchanged");
    assert_eq!(
        kept.first().map(|p| p.unredeemed),
        Some(U256::from(1_000u64))
    );
}

/// A mocked `PaymentPool` whose `eth_call` queue returns one ABI-encoded
/// `getWatermarks` result per entry — one entry per expected batch, each
/// holding that batch's lanes.
///
/// Encoded with `getWatermarksCall::abi_encode_returns`, not `SolValue`: the
/// return is a *dynamic* array, so unlike the static-tuple `Pool` /
/// `Authorization` fixtures elsewhere in this file, the standalone
/// value encoding is not the function-return encoding (the head carries an
/// offset word).
fn mocked_getwatermarks_pool(
    responses: &[Vec<PaymentPool::Lane>],
) -> (
    PaymentPool::PaymentPoolInstance<impl Provider + Clone + 'static>,
    alloy::providers::mock::Asserter,
) {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolCall;

    let asserter = Asserter::new();
    for lanes in responses {
        asserter.push_success(&Bytes::from(
            PaymentPool::getWatermarksCall::abi_encode_returns(lanes),
        ));
    }
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    (PaymentPool::new(Address::ZERO, provider), asserter)
}

fn lane(amount: u64) -> PaymentPool::Lane {
    PaymentPool::Lane {
        amount,
        bytesDelivered: 0,
    }
}

/// A mocked `PaymentPool` whose `eth_call` queue answers one
/// `getAuthorizations` per entry of `responses`.
fn mocked_getauthorizations_pool(
    responses: &[Vec<PaymentPool::Authorization>],
) -> PaymentPool::PaymentPoolInstance<impl Provider + Clone + 'static> {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolCall;

    let asserter = Asserter::new();
    for auths in responses {
        asserter.push_success(&Bytes::from(
            PaymentPool::getAuthorizationsCall::abi_encode_returns(auths),
        ));
    }
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    PaymentPool::new(Address::ZERO, provider)
}

/// A landed chunk counts every lane into `pool_redemptions` and persists
/// `registered_until` for the lane whose `CapabilityReg` rode in it (#2154).
/// The persisted value is the chain's registered expiry, not the attached
/// `CapabilityReg`'s: another provider may have registered a different
/// capability for the signer first (#2265). A failed receipt wait whose
/// receipt a by-hash fetch found takes this path too.
#[tokio::test]
async fn landed_chunk_counts_redemptions_and_persists_registration() -> Result<()> {
    use decdn_incentive::MemoryPoolStateStore;

    let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());
    let registering = signed_lane_state(7, 50, 0xEE, None);
    let registered = signed_lane_state(7, 51, 0xEE, None);
    store.record(&registering)?;
    store.record(&registered)?;
    let mut with_reg = planned(7, 50, 100, true);
    if let Some(reg) = with_reg.register.as_mut() {
        reg.expiry = 1_900_000_000;
    }
    let chain_expiry = 1_800_000_000;
    let contract = mocked_getauthorizations_pool(&[vec![PaymentPool::Authorization {
        cap: 40,
        expiry: chain_expiry,
        spent: 40,
    }]]);
    let lanes = vec![with_reg, planned(7, 51, 100, false)];
    let metrics = Metrics::new();

    record_landed_chunk(&contract, &store, &lanes, &metrics).await;

    let persisted = store
        .get(registering.key())?
        .ok_or_else(|| anyhow::anyhow!("the registering lane's row exists"))?;
    assert_eq!(
        persisted.registered_until, chain_expiry,
        "the chain's registered expiry, not the attached CapabilityReg's"
    );
    let untouched = store
        .get(registered.key())?
        .ok_or_else(|| anyhow::anyhow!("the second lane's row exists"))?;
    assert_eq!(untouched.registered_until, registered.registered_until);
    let text = metrics.encode()?;
    anyhow::ensure!(
        text.lines().any(|l| l == "decdn_pool_redemptions_total 2"),
        "{text}"
    );
    Ok(())
}

/// A failed post-redeem registration read persists nothing, so the next
/// sweep attaches the `CapabilityReg` again and re-reads.
#[tokio::test]
async fn landed_chunk_read_failure_persists_no_registration() -> Result<()> {
    use decdn_incentive::MemoryPoolStateStore;

    let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());
    let registering = signed_lane_state(7, 50, 0xEE, None);
    store.record(&registering)?;
    let contract = mocked_getauthorizations_pool(&[]);

    record_landed_chunk(
        &contract,
        &store,
        &[planned(7, 50, 100, true)],
        &Metrics::new(),
    )
    .await;

    let persisted = store
        .get(registering.key())?
        .ok_or_else(|| anyhow::anyhow!("the registering lane's row exists"))?;
    assert_eq!(persisted.registered_until, 0);
    Ok(())
}

/// Only a revert and a refused send count as redemption failures (#2154).
/// An unconfirmed chunk may have mined, so it counts only into its
/// `onchain_tx_*` bucket.
#[test]
fn only_reverts_and_refused_sends_are_redemption_failures() {
    use alloy::providers::PendingTransactionError;
    use alloy::transports::TransportErrorKind;

    let tx_hash = B256::repeat_byte(0x17);
    let last_lookup = || "no receipt yet".to_owned();

    assert!(is_redemption_failure(&TxOutcome::Reverted(blank_receipt())));
    assert!(is_redemption_failure(&TxOutcome::SendErr(
        TransportErrorKind::custom_str("rejected").into()
    )));
    assert!(!is_redemption_failure(&TxOutcome::Landed(blank_receipt())));
    assert!(!is_redemption_failure(&TxOutcome::ReceiptErr {
        error: PendingTransactionError::TransportError(TransportErrorKind::custom_str(
            "error code 26: Unknown block"
        )),
        tx_hash,
        last_lookup: last_lookup(),
    }));
    assert!(!is_redemption_failure(&TxOutcome::Timeout {
        tx_hash,
        last_lookup: last_lookup(),
    }));
}

/// A minimal receipt for outcome classification tests, which read only the
/// `TxOutcome` variant.
fn blank_receipt() -> alloy::rpc::types::TransactionReceipt {
    alloy::rpc::types::TransactionReceipt {
        inner: alloy::consensus::ReceiptEnvelope::Eip1559(alloy::consensus::ReceiptWithBloom {
            receipt: alloy::consensus::Receipt {
                status: alloy::consensus::Eip658Value::Eip658(true),
                cumulative_gas_used: 0,
                logs: Vec::new(),
            },
            logs_bloom: alloy::primitives::Bloom::ZERO,
        }),
        transaction_hash: B256::ZERO,
        transaction_index: None,
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

/// #2340: every `redeemMany` outcome but a landed one parks a hinted lane,
/// the unconfirmed ones included.
#[test]
fn every_outcome_but_landed_is_a_chain_fault() {
    use alloy::providers::PendingTransactionError;
    use alloy::transports::TransportErrorKind;

    let tx_hash = B256::repeat_byte(0x17);
    let last_lookup = || "no receipt yet".to_owned();

    assert!(!is_chain_fault(&TxOutcome::Landed(blank_receipt())));
    assert!(is_chain_fault(&TxOutcome::Reverted(blank_receipt())));
    assert!(is_chain_fault(&TxOutcome::SendErr(
        TransportErrorKind::custom_str("rejected").into()
    )));
    assert!(is_chain_fault(&TxOutcome::ReceiptErr {
        error: PendingTransactionError::TransportError(TransportErrorKind::custom_str(
            "error code 26: Unknown block"
        )),
        tx_hash,
        last_lookup: last_lookup(),
    }));
    assert!(is_chain_fault(&TxOutcome::Timeout {
        tx_hash,
        last_lookup: last_lookup(),
    }));
}

/// The whole point of the pre-redeem read (#2076): a lane the chain already
/// shows settled to its claim value leaves the redeem batch, and a survivor's
/// `unredeemed` is recomputed from the fresh read rather than the stale plan.
#[tokio::test]
async fn reconcile_drops_a_lane_the_chain_already_settled() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (contract, _asserter) = mocked_getwatermarks_pool(&[vec![lane(1_000), lane(400)]]);
    let plans = vec![planned(1, 0, 1_000, false), planned(1, 1, 1_000, false)];

    let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
    assert!(!read_failed, "every batch landed");

    assert_eq!(kept.len(), 1, "the settled lane leaves the batch");
    let survivor = kept
        .first()
        .ok_or_else(|| anyhow::anyhow!("one lane survives"))?;
    assert_eq!(survivor.key.signer, Address::from([1u8; 20]));
    assert_eq!(
        survivor.unredeemed,
        U256::from(600u64),
        "recomputed from the fresh on-chain paid (1000 owed − 400 paid)"
    );
    let text = metrics.encode()?;
    anyhow::ensure!(
        text.lines()
            .any(|l| l == "decdn_redemption_reconciled_skip_total 1"),
        "{text}"
    );
    Ok(())
}

/// Fail-open: a read the node could not make never holds up a redemption.
#[tokio::test]
async fn reconcile_fails_open_on_a_read_error() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    // An empty queue makes the mocked transport error on the first call.
    let (contract, _asserter) = mocked_getwatermarks_pool(&[]);
    let plans = vec![planned(1, 0, 1_000, false), planned(1, 1, 2_000, false)];

    let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
    assert!(read_failed, "a batch failed");

    assert_eq!(kept.len(), 2, "every lane survives an unreadable batch");
    assert_eq!(
        kept.iter().map(|p| p.unredeemed).collect::<Vec<_>>(),
        vec![U256::from(1_000u64), U256::from(2_000u64)]
    );
    let text = metrics.encode()?;
    anyhow::ensure!(
        text.lines()
            .any(|l| l == "decdn_redemption_reconciled_skip_total 0"),
        "{text}"
    );
    Ok(())
}

/// An under-returning batch reconciles the prefix and keeps the unread tail.
#[tokio::test]
async fn reconcile_pairs_the_prefix_on_a_short_return() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (contract, _asserter) = mocked_getwatermarks_pool(&[vec![lane(1_000)]]);
    let plans = vec![planned(1, 0, 1_000, false), planned(1, 1, 2_000, false)];

    let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
    assert!(read_failed, "a short return is a failed read");

    assert_eq!(kept.len(), 1, "the read lane is settled and drops");
    let survivor = kept
        .first()
        .ok_or_else(|| anyhow::anyhow!("the unread lane survives"))?;
    assert_eq!(survivor.key.signer, Address::from([1u8; 20]));
    assert_eq!(
        survivor.unredeemed,
        U256::from(2_000u64),
        "the unread lane is kept unchanged"
    );
    Ok(())
}

/// Spanning `WATERMARK_READ_BATCH_MAX` splits the read, and a later batch
/// that fails keeps the savings from the batches that landed: the settled
/// lane in batch 1 still leaves the redeem set even though batch 2 errored.
#[tokio::test]
async fn reconcile_keeps_the_first_batch_when_a_later_one_fails() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    // Batch 1 reads in full (its first lane settled); batch 2 has no queued
    // response, so the mocked transport errors on it.
    let mut first = vec![lane(0); WATERMARK_READ_BATCH_MAX];
    if let Some(head) = first.first_mut() {
        *head = lane(1_000);
    }
    let (contract, _asserter) = mocked_getwatermarks_pool(&[first]);
    let plans: Vec<PlannedLane> = (0..=WATERMARK_READ_BATCH_MAX)
        .map(|i| planned(1, u8::try_from(i % 251).unwrap_or(0), 1_000, false))
        .collect();

    let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
    assert!(read_failed, "a batch failed");

    assert_eq!(
        kept.len(),
        WATERMARK_READ_BATCH_MAX,
        "only the settled lane from the batch that landed is dropped; the \
         unread tail survives the failed batch"
    );
    let text = metrics.encode()?;
    anyhow::ensure!(
        text.lines()
            .any(|l| l == "decdn_redemption_reconciled_skip_total 1"),
        "{text}"
    );
    Ok(())
}

/// A short return in an early batch stops the read there: the prefix it did
/// deliver reconciles and every later lane is kept unchanged.
#[tokio::test]
async fn reconcile_stops_at_a_short_early_batch() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    // Batch 1 under-returns (2 lanes for 512), so batch 2 is never issued.
    let (contract, asserter) =
        mocked_getwatermarks_pool(&[vec![lane(1_000), lane(1_000)], vec![lane(1_000)]]);
    let plans: Vec<PlannedLane> = (0..=WATERMARK_READ_BATCH_MAX)
        .map(|i| planned(1, u8::try_from(i % 251).unwrap_or(0), 1_000, false))
        .collect();

    let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
    assert!(read_failed, "a short return is a failed read");

    assert_eq!(
        kept.len(),
        WATERMARK_READ_BATCH_MAX - 1,
        "the two lanes the short batch covered are settled and drop"
    );
    assert_eq!(
        asserter.read_q().len(),
        1,
        "the second batch is never issued after a short return"
    );
    Ok(())
}

/// Two batches that both land. This is what pins the chunking itself: the
/// mock returns whatever is queued regardless of how many triples were
/// asked for, so a test whose batches never both succeed passes just as
/// well against an implementation that does not chunk at all. Putting the
/// settled lane in the *second* batch, and requiring the queue to be
/// drained, fixes the call count, the batch size, the accumulation across
/// batches and the cross-batch pairing offset at once.
#[tokio::test]
async fn reconcile_reads_every_batch_and_pairs_across_the_boundary() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    // Batch 1: 512 untouched lanes. Batch 2: the single settled lane.
    let (contract, asserter) =
        mocked_getwatermarks_pool(&[vec![lane(0); WATERMARK_READ_BATCH_MAX], vec![lane(1_000)]]);
    let plans: Vec<PlannedLane> = (0..=WATERMARK_READ_BATCH_MAX)
        .map(|i| planned(1, u8::try_from(i % 251).unwrap_or(0), 1_000, false))
        .collect();

    let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
    assert!(!read_failed, "every batch landed");

    assert_eq!(
        asserter.read_q().len(),
        0,
        "both batches are issued; an unchunked read would leave one queued"
    );
    assert_eq!(
        kept.len(),
        WATERMARK_READ_BATCH_MAX,
        "the lane settled in the SECOND batch is the one dropped"
    );
    let text = metrics.encode()?;
    for expected in [
        "decdn_redemption_reconciled_skip_total 1",
        "decdn_redemption_reconcile_ok_total 2",
        "decdn_redemption_reconcile_failures_total 0",
    ] {
        anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
    }
    Ok(())
}

/// A batch that fails stops the read: no later batch is issued, so a lane
/// settled beyond the failure is never paired against the wrong plan.
#[tokio::test]
async fn reconcile_stops_at_a_failed_middle_batch() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    // Batch 1 lands. Batch 2 has no queued response and errors. Batch 3's
    // response stays queued, proving the read stopped rather than skipped.
    let (contract, asserter) =
        mocked_getwatermarks_pool(&[vec![lane(0); WATERMARK_READ_BATCH_MAX]]);
    let plans: Vec<PlannedLane> = (0..=2 * WATERMARK_READ_BATCH_MAX)
        .map(|i| planned(1, u8::try_from(i % 251).unwrap_or(0), 1_000, false))
        .collect();

    let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
    assert!(read_failed, "a batch failed");

    assert_eq!(
        asserter.read_q().len(),
        0,
        "batch 1 consumed the only queued response"
    );
    assert_eq!(
        kept.len(),
        2 * WATERMARK_READ_BATCH_MAX + 1,
        "batch 1 found nothing settled and batches 2-3 were never read, so \
         every lane survives"
    );
    let text = metrics.encode()?;
    for expected in [
        "decdn_redemption_reconcile_ok_total 1",
        "decdn_redemption_reconcile_failures_total 1",
    ] {
        anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
    }
    Ok(())
}

/// A return LONGER than the batch proves the decoder and the chain disagree
/// about the return shape, so its values pair with nothing reliably. The
/// batch fails rather than dropping a lane on data it cannot trust.
#[tokio::test]
async fn reconcile_fails_the_batch_on_an_over_return() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    // Three lanes for a two-plan batch, the first of them "settled".
    let (contract, _asserter) =
        mocked_getwatermarks_pool(&[vec![lane(1_000), lane(1_000), lane(1_000)]]);
    let plans = vec![planned(1, 0, 1_000, false), planned(1, 1, 1_000, false)];

    let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
    assert!(read_failed, "an over-return is a failed read");

    assert_eq!(
        kept.len(),
        2,
        "no lane is dropped on a return the decoder cannot trust"
    );
    let text = metrics.encode()?;
    for expected in [
        "decdn_redemption_reconciled_skip_total 0",
        "decdn_redemption_reconcile_failures_total 1",
    ] {
        anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
    }
    Ok(())
}

/// The idle steady state issues no `eth_call` at all.
#[tokio::test]
async fn reconcile_empty_plans_issues_no_call() {
    let metrics = Arc::new(Metrics::new());
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(1)]]);

    let (kept, read_failed) = reconcile_onchain_watermarks(&contract, vec![], &metrics).await;
    assert!(!read_failed, "no batch was issued");

    assert!(kept.is_empty());
    assert_eq!(
        asserter.read_q().len(),
        1,
        "the queued getWatermarks response is untouched by an empty plan set"
    );
}

/// A sub-floor hint issues no `getWatermarks` call (#2217): the floor runs
/// on the cached values before the read, and a set that fails it there
/// cannot pass it after the read.
#[tokio::test]
async fn a_sub_floor_plan_set_reads_nothing_from_the_chain() -> Result<()> {
    use decdn_incentive::MemoryPoolStateStore;

    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);

    let faulted = redeem_planned_lanes(
        &contract,
        &store,
        vec![planned(1, 0, 10, false)],
        U256::from(1_000_000u64),
        300,
        true,
        &metrics,
    )
    .await;

    assert!(!faulted, "a deferred set is not a chain fault");

    assert_eq!(
        asserter.read_q().len(),
        1,
        "the queued getWatermarks response is untouched by a sub-floor set"
    );
    let text = metrics.encode()?;
    for expected in [
        "decdn_redemption_reconcile_ok_total 0",
        "decdn_redemption_reconcile_failures_total 0",
    ] {
        anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
    }
    Ok(())
}

/// A set whose cached total clears the floor reads every lane, even a lane
/// whose cached chunk fails the floor: the read can drop a lane, shrink the
/// chunk count and re-pack the rest into a chunk that clears. Here the cap
/// of 2 deals `[whale, b]` and `[a]`; `[a]` alone is below the floor, but
/// `[a, b]` clears it once the read drops the whale. A read of only the
/// first chunk would see a short return and count a reconcile failure.
#[tokio::test]
async fn a_set_that_clears_the_floor_in_total_reads_every_lane() -> Result<()> {
    use decdn_incentive::MemoryPoolStateStore;

    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());
    // The chain shows every lane settled, so nothing reaches `redeemMany`.
    let (contract, asserter) =
        mocked_getwatermarks_pool(&[vec![lane(1_000_000), lane(300_000), lane(300_000)]]);

    let faulted = redeem_planned_lanes(
        &contract,
        &store,
        vec![
            planned(1, 0, 1_000_000, false),
            planned(1, 1, 300_000, false),
            planned(1, 2, 300_000, false),
        ],
        U256::from(500_000u64),
        2,
        true,
        &metrics,
    )
    .await;

    assert!(
        !faulted,
        "a set the chain shows settled is not a chain fault"
    );
    assert_eq!(asserter.read_q().len(), 0, "the one read was issued");
    let text = metrics.encode()?;
    for expected in [
        "decdn_redemption_reconcile_ok_total 1",
        "decdn_redemption_reconcile_failures_total 0",
        "decdn_redemption_reconciled_skip_total 3",
    ] {
        anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
    }
    Ok(())
}

/// A zero floor always passes the floor gate, so every lane is still
/// reconciled against the chain.
#[tokio::test]
async fn a_zero_floor_still_reads_every_lane() -> Result<()> {
    use decdn_incentive::MemoryPoolStateStore;

    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());
    // Both dust lanes read as settled, so nothing reaches `redeemMany`.
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(1), lane(1)]]);

    let faulted = redeem_planned_lanes(
        &contract,
        &store,
        vec![planned(1, 0, 1, false), planned(1, 1, 1, false)],
        U256::ZERO,
        300,
        false,
        &metrics,
    )
    .await;

    assert!(
        !faulted,
        "a set the chain shows settled is not a chain fault"
    );
    assert_eq!(asserter.read_q().len(), 0, "the dust lanes were read");
    let text = metrics.encode()?;
    for expected in [
        "decdn_redemption_reconcile_ok_total 1",
        "decdn_redemption_reconciled_skip_total 2",
    ] {
        anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
    }
    Ok(())
}

/// A settlement service over `contract` and `store`, built without
/// `bootstrap`: no redemption task runs, so `shutdown` goes straight to
/// `final_redeem_sweep`. The paid cache and pool projection start empty, so
/// planning treats the whole claim as unredeemed and fails open on the pool.
fn shutdown_service<P: Provider + Clone + 'static>(
    contract: PaymentPool::PaymentPoolInstance<P>,
    store: Arc<dyn PoolStateStore>,
    self_address: Address,
    redeem_threshold: U256,
    metrics: Arc<Metrics>,
) -> PoolSettlementService<P> {
    let (redeem_tx, _redeem_rx) = mpsc::channel(1);
    PoolSettlementService {
        contract,
        redeem_tx,
        store,
        paid: PaidWatermarks::default(),
        self_address,
        redeem_threshold,
        redeem_max_vouchers_per_tx: 300,
        metrics,
        pool_view: PoolProjection::new(),
        redeemer: std::sync::Mutex::new(None),
    }
}

/// Persist one lane that provider `[20; 20]` holds. The lane is registered
/// (`registered_until = u64::MAX`), so its plan carries no `CapabilityReg`
/// and needs no `owner_sig`. Returns the provider address and what the lane
/// is owed.
fn seed_one_owed_lane(store: &dyn PoolStateStore) -> Result<(Address, U256)> {
    let mut st = signed_lane_state(1, 10, 20, None);
    st.registered_until = u64::MAX;
    store.record(&st)?;
    Ok((Address::from([20u8; 20]), st.owed()))
}

/// A fresh in-memory store holding the lane from [`seed_one_owed_lane`].
fn store_with_one_owed_lane() -> Result<(Arc<dyn PoolStateStore>, Address, U256)> {
    let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
    let (me, owed) = seed_one_owed_lane(store.as_ref())?;
    Ok((store, me, owed))
}

/// An in-memory lane store whose `flush` always fails.
struct FailingFlushStore(decdn_incentive::MemoryPoolStateStore);

impl PoolStateStore for FailingFlushStore {
    fn load_all(&self) -> Result<Vec<LaneState>, StoreError> {
        self.0.load_all()
    }

    fn record(&self, state: &LaneState) -> Result<(), StoreError> {
        self.0.record(state)
    }

    fn forget(&self, key: LaneKey) -> Result<(), StoreError> {
        self.0.forget(key)
    }

    fn get(&self, key: LaneKey) -> Result<Option<LaneState>, StoreError> {
        self.0.get(key)
    }

    fn flush(&self) -> Result<(), StoreError> {
        Err(StoreError::Backend("flush refused".into()))
    }
}

/// `final_redeem_sweep` applies the configured floor: a lane whose
/// unredeemed value is below `redeem_threshold` is left alone at shutdown.
/// The floor gate returns before the pre-submit `getWatermarks` read, so the
/// queued response stays unconsumed and no `redeemMany` is built.
#[tokio::test]
async fn shutdown_sweep_leaves_a_sub_floor_lane_unread() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (store, me, owed) = store_with_one_owed_lane()?;
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
    let svc = shutdown_service(
        contract,
        store,
        me,
        owed + U256::from(1u64),
        Arc::clone(&metrics),
    );

    svc.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        asserter.read_q().len(),
        1,
        "the queued getWatermarks response is untouched by a sub-floor lane"
    );
    let text = metrics.encode()?;
    for expected in [
        format!("decdn_unredeemed_usdc {owed}"),
        "decdn_redemption_reconcile_ok_total 0".to_owned(),
        "decdn_redemption_failures_total 0".to_owned(),
    ] {
        anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
    }
    Ok(())
}

/// `final_redeem_sweep` passes a lane owed exactly `redeem_threshold` on to
/// the pre-submit reconcile. The chain reports the lane settled, so nothing
/// reaches `redeemMany`.
#[tokio::test]
async fn shutdown_sweep_reconciles_a_lane_at_the_floor() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (store, me, owed) = store_with_one_owed_lane()?;
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(u64::try_from(owed)?)]]);
    let svc = shutdown_service(contract, store, me, owed, Arc::clone(&metrics));

    svc.shutdown(Duration::from_secs(5)).await;

    assert_eq!(asserter.read_q().len(), 0, "the lane was read");
    let text = metrics.encode()?;
    for expected in [
        "decdn_redemption_reconcile_ok_total 1",
        "decdn_redemption_reconcile_failures_total 0",
        "decdn_redemption_reconciled_skip_total 1",
        "decdn_onchain_tx_send_failed_total 0",
        "decdn_redemption_failures_total 0",
    ] {
        anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
    }
    Ok(())
}

/// `final_redeem_sweep` submits a `redeemMany` for an unsettled lane at the
/// floor. The first RPC call the send makes fails, so the send counts one
/// refused send and one redemption failure.
#[tokio::test]
async fn shutdown_sweep_submits_redeem_many_for_an_unsettled_lane() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (store, me, owed) = store_with_one_owed_lane()?;
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
    asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
    let svc = shutdown_service(contract, store, me, owed, Arc::clone(&metrics));

    svc.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        asserter.read_q().len(),
        0,
        "the lane was read and the send tried"
    );
    let text = metrics.encode()?;
    for expected in [
        "decdn_redemption_reconcile_ok_total 1",
        "decdn_lane_flush_failures_total 0",
        "decdn_onchain_tx_send_failed_total 1",
        "decdn_redemption_failures_total 1",
    ] {
        anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
    }
    Ok(())
}

/// `final_redeem_sweep` does not let a failed lane-store flush stop the
/// submit: the shutdown sweep still sends `redeemMany` for an unsettled lane
/// at the floor.
#[tokio::test]
async fn shutdown_sweep_redeems_despite_a_failed_flush() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(FailingFlushStore(
        decdn_incentive::MemoryPoolStateStore::new(),
    ));
    let (me, owed) = seed_one_owed_lane(store.as_ref())?;
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
    asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
    let svc = shutdown_service(contract, store, me, owed, Arc::clone(&metrics));

    svc.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        asserter.read_q().len(),
        0,
        "the lane was read and the send tried"
    );
    let text = metrics.encode()?;
    for expected in [
        "decdn_lane_flush_failures_total 1",
        "decdn_onchain_tx_send_failed_total 1",
        "decdn_redemption_failures_total 1",
    ] {
        anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
    }
    Ok(())
}

/// A lane this node provides, owed value, with capability `expiry`.
fn expiring_lane(signer: u8, expiry: u64) -> LaneState {
    let mut st = signed_lane_state(1, signer, 20, None);
    st.expiry = expiry;
    st
}

#[test]
fn serve_cutoff_wake_is_one_second_past_the_cutoff() {
    let me = Address::from([20u8; 20]);
    let paid = PaidWatermarks::default();
    let st = expiring_lane(1, 10_000);
    assert_eq!(serve_cutoff_wake(&st, &paid, me, 420, 0, 0), Some(9_581));
    assert_eq!(
        serve_cutoff_wake(&st, &paid, me, 420, 9_580, 9_580),
        Some(9_581),
        "a wake still ahead of the last scan is kept"
    );
    assert_eq!(
        serve_cutoff_wake(&st, &paid, me, 420, 9_581, 9_581),
        None,
        "the sweep at a cutoff does not schedule that cutoff again"
    );
    assert_eq!(
        serve_cutoff_wake(&expiring_lane(1, 200), &paid, me, 100, 0, 0),
        Some(101),
        "an expiry inside the margin saturates instead of underflowing"
    );
}

/// A cutoff that passed with no scan after it is due at once: at start
/// (`swept_at == 0`), or for a hint handled after its lane's cutoff.
#[test]
fn a_cutoff_passed_unscanned_is_due_now() {
    let me = Address::from([20u8; 20]);
    let paid = PaidWatermarks::default();
    let st = expiring_lane(1, 10_000);
    assert_eq!(
        serve_cutoff_wake(&st, &paid, me, 420, 0, 9_700),
        Some(9_700),
        "the node was down at the cutoff"
    );
    assert_eq!(
        serve_cutoff_wake(&st, &paid, me, 420, 9_500, 9_700),
        Some(9_700),
        "the last scan ran before the cutoff"
    );
    assert_eq!(
        serve_cutoff_wake(&st, &paid, me, 420, 9_600, 9_700),
        None,
        "a scan after the cutoff already read the final claim"
    );
    assert_eq!(
        serve_cutoff_wake(&st, &paid, me, 420, 0, 10_000 - REDEEM_LANDING_SLACK_SECS),
        None,
        "a lane inside the landing slack cannot be redeemed, so it is not due"
    );
}

#[test]
fn serve_cutoff_wake_skips_lanes_that_need_no_redeem() {
    let me = Address::from([20u8; 20]);
    let paid = PaidWatermarks::default();
    assert_eq!(
        serve_cutoff_wake(&expiring_lane(1, 0), &paid, me, 420, 0, 0),
        None,
        "a lane with no tracked expiry never expires"
    );
    let other = Address::from([9u8; 20]);
    assert_eq!(
        serve_cutoff_wake(&expiring_lane(1, 10_000), &paid, other, 420, 0, 0),
        None,
        "another provider's lane"
    );
    let settled = expiring_lane(1, 10_000);
    paid.set(settled.key(), settled.owed());
    assert_eq!(
        serve_cutoff_wake(&settled, &paid, me, 420, 0, 0),
        None,
        "a fully paid lane"
    );
}

#[test]
fn next_serve_cutoff_picks_the_earliest_wake() {
    let me = Address::from([20u8; 20]);
    let paid = PaidWatermarks::default();
    let states = [
        expiring_lane(1, 30_000),
        expiring_lane(2, 0),
        expiring_lane(3, 10_000),
        expiring_lane(4, 20_000),
    ];
    assert_eq!(
        next_serve_cutoff(&states, &paid, me, 420, 0, 0),
        Some(9_581)
    );
    assert_eq!(
        next_serve_cutoff(&states, &paid, me, 420, 9_581, 9_581),
        Some(19_581),
        "a scanned cutoff yields to the next one"
    );
    assert_eq!(
        next_serve_cutoff(&states, &paid, me, 420, 0, 9_700),
        Some(9_700),
        "an unscanned passed cutoff comes first"
    );
    assert_eq!(next_serve_cutoff(&[], &paid, me, 420, 0, 0), None);
}

/// The voucher path's margin predicate refuses from the cutoff on, and the
/// wake falls one second after it: no proof can land after the cutoff sweep
/// reads the lane.
#[test]
fn the_cutoff_wake_follows_the_voucher_margin_gate() {
    use decdn_common::config::inside_capability_expiry_margin;
    let me = Address::from([20u8; 20]);
    let paid = PaidWatermarks::default();
    let (expiry, margin) = (10_000, 420);
    let wake = serve_cutoff_wake(&expiring_lane(1, expiry), &paid, me, margin, 0, 0);
    assert_eq!(wake, Some(9_581));
    assert!(!inside_capability_expiry_margin(expiry, margin, 9_579));
    assert!(
        inside_capability_expiry_margin(expiry, margin, 9_580),
        "the gate is closed one second before the wake"
    );
}

/// An in-memory lane store that counts `load_all` calls and fails every
/// call after the first `ok_loads`.
struct CountingLoadStore {
    inner: decdn_incentive::MemoryPoolStateStore,
    loads: std::sync::atomic::AtomicUsize,
    ok_loads: usize,
}

impl CountingLoadStore {
    fn new(ok_loads: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: decdn_incentive::MemoryPoolStateStore::new(),
            loads: std::sync::atomic::AtomicUsize::new(0),
            ok_loads,
        })
    }

    fn loads(&self) -> usize {
        self.loads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl PoolStateStore for CountingLoadStore {
    fn load_all(&self) -> Result<Vec<LaneState>, StoreError> {
        let n = self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n < self.ok_loads {
            self.inner.load_all()
        } else {
            Err(StoreError::Backend("load refused".into()))
        }
    }

    fn record(&self, state: &LaneState) -> Result<(), StoreError> {
        self.inner.record(state)
    }

    fn forget(&self, key: LaneKey) -> Result<(), StoreError> {
        self.inner.forget(key)
    }

    fn get(&self, key: LaneKey) -> Result<Option<LaneState>, StoreError> {
        self.inner.get(key)
    }

    fn flush(&self) -> Result<(), StoreError> {
        self.inner.flush()
    }
}

/// Spawn `redeemer_loop` with self-tick `interval`. With a one-hour tick,
/// only a hint or a serve cutoff makes it act within a test. Returns the
/// hint sender (drop it to end the loop), the loop's handle, and its margin.
fn spawn_redeemer<P: Provider + Clone + 'static>(
    contract: PaymentPool::PaymentPoolInstance<P>,
    store: Arc<dyn PoolStateStore>,
    floor: U256,
    interval: Duration,
    metrics: Arc<Metrics>,
) -> (mpsc::Sender<LaneKey>, JoinHandle<()>, u64) {
    let margin = capability_expiry_margin_secs(interval.as_secs());
    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(redeemer_loop(
        contract,
        store,
        PaidWatermarks::default(),
        Address::from([20u8; 20]),
        floor,
        300,
        interval,
        margin,
        rx,
        metrics,
        PoolProjection::new(),
    ));
    (tx, handle, margin)
}

/// The margin of a redeemer with a one-hour self-tick.
fn hour_tick_margin() -> u64 {
    capability_expiry_margin_secs(Duration::from_hours(1).as_secs())
}

/// Record a registered lane for each signer in `signers`, all expiring at
/// `expiry`. Returns the first lane's key and the lanes' total owed value.
fn record_lanes(
    store: &dyn PoolStateStore,
    signers: std::ops::RangeInclusive<u8>,
    expiry: u64,
) -> Result<(LaneKey, U256)> {
    let mut first = None;
    let mut total = U256::ZERO;
    for signer in signers {
        let mut st = expiring_lane(signer, expiry);
        st.registered_until = u64::MAX;
        total += st.owed();
        store.record(&st)?;
        first.get_or_insert(st.key());
    }
    Ok((first.context("at least one lane")?, total))
}

/// Wait up to 15 s for `done` to hold.
async fn await_until(what: &str, mut done: impl FnMut() -> Result<bool>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !done()? {
        anyhow::ensure!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

/// Wait up to 15 s for `line` to appear in the encoded metrics.
async fn await_metric_line(metrics: &Metrics, line: &str) -> Result<()> {
    await_until(line, || Ok(metrics.encode()?.lines().any(|l| l == line))).await
}

/// Assert every line in `lines` is in the encoded metrics.
fn ensure_metric_lines(metrics: &Metrics, lines: &[&str]) -> Result<()> {
    let text = metrics.encode()?;
    for line in lines {
        anyhow::ensure!(text.lines().any(|l| l == *line), "{line}\n{text}");
    }
    Ok(())
}

/// #2233: several sub-floor lanes hold capabilities with the same expiry.
/// Their serve cutoff is about 3 s away, long before the next hourly
/// self-tick, and together they clear the floor. The lanes appear after the
/// loop starts, so a hint registers the cutoff. The cutoff sweep sends one
/// `redeemMany` for all of them, no earlier than the wake, and runs once.
#[tokio::test]
async fn sub_floor_lanes_are_redeemed_together_at_their_serve_cutoff() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let store = CountingLoadStore::new(usize::MAX);
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0), lane(0), lane(0)]]);
    // The redeemMany send fails at its first RPC call, so the send is metered.
    asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());

    let lane_value = expiring_lane(1, 0).owed();
    let floor = lane_value * U256::from(3u64);
    let (tx, handle, margin) = spawn_redeemer(
        contract,
        Arc::clone(&store) as Arc<dyn PoolStateStore>,
        floor,
        Duration::from_hours(1),
        Arc::clone(&metrics),
    );
    // Let the loop run its startup scan on the empty store.
    await_until("the startup scan", || Ok(store.loads() == 1)).await?;

    let expiry = unix_now() + margin + 2;
    let wake = expiry - margin + 1;
    let (hinted, _) = record_lanes(store.as_ref(), 1..=3, expiry)?;
    assert!(lane_value < floor, "each lane alone is below the floor");
    tx.send(hinted).await?;

    await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 1").await?;
    assert!(
        unix_now() >= wake,
        "the cutoff sweep runs no earlier than the wake"
    );
    assert_eq!(asserter.read_q().len(), 0, "the chunk was read and sent");

    // The sweep does not re-fire its passed cutoff: any second sweep would
    // reach the empty mock queue and count a reconcile failure.
    tokio::time::sleep(Duration::from_secs(2)).await;
    ensure_metric_lines(
        &metrics,
        &[
            "decdn_redemption_reconcile_ok_total 1",
            "decdn_redemption_reconcile_failures_total 0",
            "decdn_onchain_tx_send_failed_total 1",
        ],
    )?;
    assert_eq!(store.loads(), 2, "the startup scan and one cutoff sweep");

    drop(tx);
    handle.await?;
    Ok(())
}

/// Lanes persisted before the loop starts keep their cutoff sweep. A set
/// that stays below the floor at the cutoff is swept once and left
/// unredeemed: the sweep reads nothing from the chain.
#[tokio::test]
async fn a_sub_floor_set_at_its_serve_cutoff_reads_nothing() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let store = CountingLoadStore::new(usize::MAX);
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0), lane(0)]]);
    let expiry = unix_now() + hour_tick_margin() + 2;
    let (_, total) = record_lanes(store.as_ref(), 1..=2, expiry)?;
    let (tx, handle, _) = spawn_redeemer(
        contract,
        Arc::clone(&store) as Arc<dyn PoolStateStore>,
        total + U256::from(1u64),
        Duration::from_hours(1),
        Arc::clone(&metrics),
    );

    // Every sweep publishes the planned total, so this line marks the
    // cutoff sweep.
    await_metric_line(&metrics, &format!("decdn_unredeemed_usdc {total}")).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(store.loads(), 2, "the startup scan and one cutoff sweep");
    drop(tx);
    handle.await?;
    assert_eq!(
        asserter.read_q().len(),
        1,
        "the queued getWatermarks response is untouched by a sub-floor set"
    );
    Ok(())
}

/// A lane whose cutoff passed while the node was down, with time left
/// before the landing slack, is swept at once on start rather than after
/// the first self-tick.
#[tokio::test]
async fn a_cutoff_passed_before_start_is_swept_at_once() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
    asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
    // The cutoff passed 10 s ago; the landing slack is an hour away.
    let expiry = unix_now() + hour_tick_margin() - 10;
    let (_, total) = record_lanes(store.as_ref(), 1..=1, expiry)?;
    let (tx, handle, _) = spawn_redeemer(
        contract,
        store,
        total,
        Duration::from_hours(1),
        Arc::clone(&metrics),
    );

    await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 1").await?;
    ensure_metric_lines(&metrics, &["decdn_redemption_reconcile_ok_total 1"])?;

    drop(tx);
    handle.await?;
    Ok(())
}

/// A hint handled after its lane's cutoff, with no sweep since, makes the
/// cutoff sweep due at once rather than leaving the lane to the next tick.
#[tokio::test]
async fn a_hint_after_an_unscanned_cutoff_sweeps_at_once() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let store = CountingLoadStore::new(usize::MAX);
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0), lane(0)]]);
    asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
    let lane_value = expiring_lane(1, 0).owed();
    let (tx, handle, margin) = spawn_redeemer(
        contract,
        Arc::clone(&store) as Arc<dyn PoolStateStore>,
        lane_value * U256::from(2u64),
        Duration::from_hours(1),
        Arc::clone(&metrics),
    );
    await_until("the startup scan", || Ok(store.loads() == 1)).await?;

    // The cutoff passed 10 s ago; the landing slack is an hour away.
    let expiry = unix_now() + margin - 10;
    let (hinted, _) = record_lanes(store.as_ref(), 1..=2, expiry)?;
    tx.send(hinted).await?;

    await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 1").await?;
    ensure_metric_lines(&metrics, &["decdn_redemption_reconcile_ok_total 1"])?;

    drop(tx);
    handle.await?;
    Ok(())
}

/// A failed load at the cutoff sweep drops the passed cutoff instead of
/// re-firing it: the loop parks until the next tick or hint.
#[tokio::test]
async fn a_failed_load_at_the_cutoff_does_not_spin() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    // The startup scan succeeds; every later load fails.
    let store = CountingLoadStore::new(1);
    let (contract, _asserter) = mocked_getwatermarks_pool(&[]);
    let expiry = unix_now() + hour_tick_margin() + 2;
    let (_, total) = record_lanes(store.as_ref(), 1..=1, expiry)?;
    let (tx, handle, _) = spawn_redeemer(
        contract,
        Arc::clone(&store) as Arc<dyn PoolStateStore>,
        total,
        Duration::from_hours(1),
        Arc::clone(&metrics),
    );

    await_until("the failed cutoff load", || Ok(store.loads() >= 2)).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(store.loads(), 2, "the passed cutoff is not re-fired");

    drop(tx);
    handle.await?;
    Ok(())
}

/// The self-tick sweeps an owed lane that no hint and no cutoff names.
#[tokio::test]
async fn the_self_tick_sweeps_a_lane_without_a_hint() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
    asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
    // No tracked expiry, so no cutoff wake.
    let (_, total) = record_lanes(store.as_ref(), 1..=1, 0)?;
    let (tx, handle, _) = spawn_redeemer(
        contract,
        store,
        total,
        Duration::from_secs(1),
        Arc::clone(&metrics),
    );

    await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 1").await?;
    ensure_metric_lines(&metrics, &["decdn_redemption_reconcile_ok_total 1"])?;

    drop(tx);
    handle.await?;
    Ok(())
}

/// #2340: a hint whose redemption hits a chain fault parks its lane. The
/// first hint reads the watermark and its `redeemMany` send fails. Every
/// later hint for the lane is dropped and counted, and reads nothing: a
/// further read would reach the empty mock queue and count a reconcile
/// failure.
#[tokio::test]
async fn a_chain_fault_parks_the_lane_until_the_next_sweep() -> Result<()> {
    const HINTS: u64 = 4;
    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
    asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
    // No tracked expiry, so no cutoff wake, and a one-hour tick: only the
    // hints act within the test.
    let (hinted, total) = record_lanes(store.as_ref(), 1..=1, 0)?;
    let (tx, handle, _) = spawn_redeemer(
        contract,
        store,
        total,
        Duration::from_hours(1),
        Arc::clone(&metrics),
    );

    for _ in 0..HINTS {
        tx.send(hinted).await?;
    }

    await_metric_line(
        &metrics,
        &format!("decdn_redeem_hints_parked_total {}", HINTS - 1),
    )
    .await?;
    ensure_metric_lines(
        &metrics,
        &[
            "decdn_redemption_reconcile_ok_total 1",
            "decdn_redemption_reconcile_failures_total 0",
            "decdn_onchain_tx_send_failed_total 1",
        ],
    )?;
    assert_eq!(asserter.read_q().len(), 0, "the one hint read and sent");

    drop(tx);
    handle.await?;
    Ok(())
}

/// A mocked `PaymentPool` that answers each entry of `responses` in order:
/// `Some(lanes)` is a `getWatermarks` read, `None` is a failed RPC call (a
/// `redeemMany` send).
fn mocked_redeem_pool(
    responses: &[Option<Vec<PaymentPool::Lane>>],
) -> (
    PaymentPool::PaymentPoolInstance<impl Provider + Clone + 'static>,
    alloy::providers::mock::Asserter,
) {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolCall;

    let asserter = Asserter::new();
    for response in responses {
        match response {
            Some(lanes) => asserter.push_success(&Bytes::from(
                PaymentPool::getWatermarksCall::abi_encode_returns(lanes),
            )),
            None => asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error()),
        }
    }
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    (PaymentPool::new(Address::ZERO, provider), asserter)
}

/// #2340: a sweep with a chain fault keeps the parked lane parked. A hint
/// parks the lane, the self-tick sweep retries it and its send fails, and a
/// hint after that sweep is dropped and reads nothing: a further read would
/// reach the empty mock queue and count a reconcile failure. So an RPC
/// outage costs the lane one failed attempt per interval. The paused clock
/// fires the tick only once the loop is idle.
#[tokio::test(start_paused = true)]
async fn a_failed_sweep_keeps_the_lane_parked() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
    // One read and one failed send each for the first hint and the sweep.
    let (contract, asserter) =
        mocked_redeem_pool(&[Some(vec![lane(0)]), None, Some(vec![lane(0)]), None]);
    let (hinted, total) = record_lanes(store.as_ref(), 1..=1, 0)?;
    let (tx, handle, _) = spawn_redeemer(
        contract,
        store,
        total,
        Duration::from_secs(3),
        Arc::clone(&metrics),
    );

    tx.send(hinted).await?;
    await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 1").await?;

    // The self-tick sweep retries the parked lane and faults.
    await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 2").await?;
    ensure_metric_lines(&metrics, &["decdn_redemption_reconcile_ok_total 2"])?;

    // The faulted sweep kept the parked set, so this hint is dropped.
    tx.send(hinted).await?;
    await_metric_line(&metrics, "decdn_redeem_hints_parked_total 1").await?;
    ensure_metric_lines(
        &metrics,
        &[
            "decdn_redemption_reconcile_ok_total 2",
            "decdn_redemption_reconcile_failures_total 0",
            "decdn_onchain_tx_send_failed_total 2",
        ],
    )?;
    assert_eq!(
        asserter.read_q().len(),
        0,
        "the hint and the sweep each read and sent once"
    );

    drop(tx);
    handle.await?;
    Ok(())
}

/// #2340: a sweep without a chain fault releases a parked lane. A hint
/// parks the lane and a second hint is dropped. The self-tick sweep reads
/// the lane as settled on-chain, so it submits nothing and faults nothing,
/// and a hint after that sweep reads the watermark again. The paused clock
/// fires the tick only once the loop is idle, so both hints land before the
/// sweep.
#[tokio::test(start_paused = true)]
async fn a_clean_sweep_releases_a_parked_lane() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
    let (hinted, total) = record_lanes(store.as_ref(), 1..=1, 0)?;
    let settled = u64::try_from(total)?;
    // A read and a failed send for the first hint, then a read that shows
    // the lane settled for the sweep and for the hint after it.
    let (contract, asserter) = mocked_redeem_pool(&[
        Some(vec![lane(0)]),
        None,
        Some(vec![lane(settled)]),
        Some(vec![lane(settled)]),
    ]);
    let (tx, handle, _) = spawn_redeemer(
        contract,
        store,
        total,
        Duration::from_secs(3),
        Arc::clone(&metrics),
    );

    tx.send(hinted).await?;
    tx.send(hinted).await?;
    await_metric_line(&metrics, "decdn_redeem_hints_parked_total 1").await?;
    ensure_metric_lines(&metrics, &["decdn_onchain_tx_send_failed_total 1"])?;

    // The self-tick sweep finds the lane settled and faults nothing.
    await_metric_line(&metrics, "decdn_redemption_reconcile_ok_total 2").await?;
    ensure_metric_lines(&metrics, &["decdn_redemption_reconciled_skip_total 1"])?;

    // The clean sweep cleared the parked set, so this hint reads again.
    tx.send(hinted).await?;
    await_metric_line(&metrics, "decdn_redemption_reconcile_ok_total 3").await?;
    ensure_metric_lines(
        &metrics,
        &[
            "decdn_redemption_reconciled_skip_total 2",
            "decdn_redemption_reconcile_failures_total 0",
            "decdn_onchain_tx_send_failed_total 1",
            "decdn_redeem_hints_parked_total 1",
        ],
    )?;
    assert_eq!(asserter.read_q().len(), 0, "every queued answer was read");

    drop(tx);
    handle.await?;
    Ok(())
}

/// #2340: a chain fault parks only its own lane. Lane A's hint faults and
/// parks A, so A's second hint is dropped. Lane B's hint still reads and
/// sends.
#[tokio::test]
async fn a_chain_fault_parks_only_its_own_lane() -> Result<()> {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolCall;

    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
    // One read and one failed send for lane A's first hint, then the same
    // for lane B's hint.
    let asserter = Asserter::new();
    for _ in 0..2 {
        asserter.push_success(&Bytes::from(
            PaymentPool::getWatermarksCall::abi_encode_returns(&vec![lane(0)]),
        ));
        asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
    }
    let contract = PaymentPool::new(
        Address::ZERO,
        ProviderBuilder::new().connect_mocked_client(asserter.clone()),
    );
    // No tracked expiry, so no cutoff wake, and a one-hour tick: only the
    // hints act within the test. The floor is one lane's value, so each
    // one-lane hint clears it.
    let (lane_a, _) = record_lanes(store.as_ref(), 1..=2, 0)?;
    let lane_b = expiring_lane(2, 0).key();
    let (tx, handle, _) = spawn_redeemer(
        contract,
        store,
        expiring_lane(1, 0).owed(),
        Duration::from_hours(1),
        Arc::clone(&metrics),
    );

    tx.send(lane_a).await?;
    tx.send(lane_a).await?;
    tx.send(lane_b).await?;

    await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 2").await?;
    ensure_metric_lines(
        &metrics,
        &[
            "decdn_redeem_hints_parked_total 1",
            "decdn_redemption_reconcile_ok_total 2",
            "decdn_redemption_reconcile_failures_total 0",
        ],
    )?;
    assert_eq!(asserter.read_q().len(), 0, "both lanes read and sent once");

    drop(tx);
    handle.await?;
    Ok(())
}

/// A failed strict flush skips the submit and adds no chain fault of its
/// own: the flush is local to this node.
#[tokio::test]
async fn a_failed_strict_flush_skips_the_submit_without_a_chain_fault() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(FailingFlushStore(
        decdn_incentive::MemoryPoolStateStore::new(),
    ));
    let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);

    let faulted = redeem_planned_lanes(
        &contract,
        &store,
        vec![planned(1, 0, 1_000, false)],
        U256::from(1u64),
        300,
        true,
        &metrics,
    )
    .await;

    assert!(!faulted, "a failed flush is not a chain fault");
    assert_eq!(asserter.read_q().len(), 0, "the watermark was read");
    ensure_metric_lines(
        &metrics,
        &[
            "decdn_redemption_reconcile_ok_total 1",
            "decdn_onchain_tx_send_failed_total 0",
        ],
    )
}

/// A failed watermark read stays a chain fault when a failed strict flush
/// then skips the submit.
#[tokio::test]
async fn a_failed_read_stays_a_chain_fault_past_a_failed_strict_flush() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let store: Arc<dyn PoolStateStore> = Arc::new(FailingFlushStore(
        decdn_incentive::MemoryPoolStateStore::new(),
    ));
    // An empty queue fails the watermark read.
    let (contract, _asserter) = mocked_getwatermarks_pool(&[]);

    let faulted = redeem_planned_lanes(
        &contract,
        &store,
        vec![planned(1, 0, 1_000, false)],
        U256::from(1u64),
        300,
        true,
        &metrics,
    )
    .await;

    assert!(faulted, "the failed read is a chain fault");
    ensure_metric_lines(
        &metrics,
        &[
            "decdn_redemption_reconcile_failures_total 1",
            "decdn_onchain_tx_send_failed_total 0",
        ],
    )
}

#[test]
fn chunk_redemptions_caps_vouchers_per_chunk() {
    // 5 lanes, cap 2 => 3 chunks (2 + 2 + 1). All above floor.
    let plans = (0..5).map(|i| planned(1, i, 1_000_000, false)).collect();
    let chunks = chunk_redemptions(plans, U256::from(1u64), 2);
    assert_eq!(chunks.len(), 3);
    assert!(chunks.iter().all(|c| c.len() <= 2));
    assert_eq!(chunks.iter().map(Vec::len).sum::<usize>(), 5);
}

#[test]
fn chunk_redemptions_drops_below_floor_chunk() {
    // One dust lane, floor 1 USDC => nothing submitted (defers).
    let plans = vec![planned(1, 0, 10, false)];
    let chunks = chunk_redemptions(plans, U256::from(1_000_000u64), 300);
    assert!(chunks.is_empty());
}

#[test]
fn chunk_redemptions_zero_floor_keeps_everything() {
    // A zero floor keeps even a pure-dust chunk.
    let plans = vec![planned(1, 0, 1, false), planned(1, 1, 1, false)];
    let chunks = chunk_redemptions(plans, U256::ZERO, 300);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks.first().map(Vec::len), Some(2));
}

#[test]
fn chunk_redemptions_spreads_value_so_dust_rides_along() {
    // 2 whales + 2 dust, cap 2 => 2 chunks. Value-spreading puts one whale in
    // each chunk, so each chunk clears a floor no single dust lane could.
    let plans = vec![
        planned(1, 0, 1_000_000, false), // whale
        planned(1, 1, 1_000_000, false), // whale
        planned(1, 2, 5, false),         // dust
        planned(1, 3, 5, false),         // dust
    ];
    let chunks = chunk_redemptions(plans, U256::from(500_000u64), 2);
    assert_eq!(chunks.len(), 2);
    // Every submitted chunk clears the floor (dust rode along with a whale).
    assert!(
        chunks
            .iter()
            .all(|c| total_unredeemed(c) >= U256::from(500_000u64))
    );
    // All four lanes survived (none stranded).
    assert_eq!(chunks.iter().map(Vec::len).sum::<usize>(), 4);
}

#[test]
fn chunk_redemptions_empty_input_is_empty() {
    assert!(chunk_redemptions(Vec::new(), U256::ZERO, 300).is_empty());
}

/// A 65-byte `r‖s‖v` signature with a low `s` and the recovery byte set to
/// `v` (a `[u8; 65]` so a const index stays provably in-bounds for the
/// anti-panic lints).
fn sig_with_v(v: u8) -> [u8; 65] {
    let mut s = [7u8; 65];
    if let Some(last) = s.last_mut() {
        *last = v;
    }
    s
}

#[test]
fn compaction_folds_raw_y_parity_into_the_top_bit_of_s() -> Result<()> {
    // `s` here is 0x0707…07, so its top bit is free: parity 0 leaves it
    // clear, parity 1 sets it, and the rest of `s` is untouched.
    let (r, vs) = compact_voucher_signature(&sig_with_v(0))?;
    assert_eq!(r, B256::repeat_byte(7), "r passes through unchanged");
    assert_eq!(
        vs,
        B256::repeat_byte(7),
        "parity 0 leaves the top bit clear"
    );

    let (_, vs_odd) = compact_voucher_signature(&sig_with_v(1))?;
    assert_eq!(
        vs_odd.0.first(),
        Some(&0x87),
        "parity 1 sets the top bit of s"
    );
    assert_eq!(vs_odd.0.get(1), Some(&7), "and disturbs nothing else");
    Ok(())
}

#[test]
fn compaction_accepts_the_eth_v_convention_identically() -> Result<()> {
    assert_eq!(
        compact_voucher_signature(&sig_with_v(27))?,
        compact_voucher_signature(&sig_with_v(0))?,
        "27 and 0 are the same parity"
    );
    assert_eq!(
        compact_voucher_signature(&sig_with_v(28))?,
        compact_voucher_signature(&sig_with_v(1))?,
        "28 and 1 are the same parity"
    );
    Ok(())
}

#[test]
fn compaction_rejects_a_signature_it_cannot_represent() {
    // Not 65 bytes: there is no `r‖s‖v` to split.
    assert!(compact_voucher_signature(&[1u8, 2, 3]).is_err());

    // An unusable recovery id.
    assert!(compact_voucher_signature(&sig_with_v(4)).is_err());

    // High `s` with the top bit obviously set.
    let mut high_s = sig_with_v(27);
    if let Some(top) = high_s.get_mut(32) {
        *top = 0xFF;
    }
    assert!(compact_voucher_signature(&high_s).is_err());
}

/// The band a "is the top bit free?" test would wave through: `n / 2` sits
/// below `2^255`, so roughly `2^128` values of `s` have a clear top bit and
/// are still high-`s`. Compaction would fold the recovery bit into one
/// happily, and the contract's `ECDSA.tryRecover` would then reject it —
/// reverting the whole `redeemMany` and taking every honest lane in the
/// batch with it.
#[test]
fn compaction_rejects_high_s_whose_top_bit_is_clear() {
    // n/2 + 1: the smallest high-`s` value, and its top bit is 0.
    let half_plus_one = alloy::primitives::U256::from_be_bytes([
        0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0x5d, 0x57, 0x6e, 0x73, 0x57, 0xa4, 0x50, 0x1d, 0xdf, 0xe9, 0x2f, 0x46, 0x68, 0x1b,
        0x20, 0xa1,
    ]);
    let s_bytes = half_plus_one.to_be_bytes::<32>();
    assert_eq!(
        s_bytes.first().map(|b| b & 0x80),
        Some(0),
        "top bit is clear"
    );

    let mut raw = [7u8; 65];
    if let Some(slot) = raw.get_mut(32..64) {
        slot.copy_from_slice(&s_bytes);
    }
    if let Some(last) = raw.last_mut() {
        *last = 27;
    }

    assert!(
        compact_voucher_signature(&raw).is_err(),
        "a clear top bit does not make `s` canonical"
    );
}

/// The debounce decorator forwards through to the inner store on the first
/// record, coalesces sub-threshold advances, and forces a buffered block out
/// on flush. Driven by an in-test [`KeyedCheckpointStore`] double, since the
/// incentive crate ships no memory checkpoint store.
#[test]
fn debounced_checkpoint_coalesces_then_flushes() {
    #[derive(Default)]
    struct MemCk(std::sync::Mutex<HashMap<CheckpointKey, u64>>);
    impl KeyedCheckpointStore for MemCk {
        fn load_checkpoint(&self, key: CheckpointKey) -> Result<Option<u64>, StoreError> {
            Ok(self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&key)
                .copied())
        }
        fn record_checkpoint(&self, key: CheckpointKey, block: u64) -> Result<(), StoreError> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key, block);
            Ok(())
        }
    }
    let inner: Arc<dyn KeyedCheckpointStore> = Arc::new(MemCk::default());
    let deb = DebouncedCheckpointStore::with_params(
        Arc::clone(&inner),
        10,
        Duration::from_hours(1),
        Box::new(Instant::now),
    );
    let key = CheckpointKey::PoolOpened;
    let load = |s: &Arc<dyn KeyedCheckpointStore>| s.load_checkpoint(key).ok().flatten();
    // First advancing record forwards durably (re-anchor the floor promptly).
    let _ = deb.record_checkpoint(key, 5);
    assert_eq!(load(&inner), Some(5));
    // A sub-`flush_blocks` advance buffers only; the durable floor holds.
    let _ = deb.record_checkpoint(key, 8);
    assert_eq!(load(&inner), Some(5));
    assert_eq!(deb.load_checkpoint(key).ok().flatten(), Some(8));
    // A flush forces the buffered block out durably (the shutdown path).
    let _ = deb.flush_checkpoint(key);
    assert_eq!(load(&inner), Some(8));
}

#[test]
fn is_oversize_send_err_matches_known_markers() {
    for m in [
        "err: gas required exceeds allowance (30000000)",
        "transaction exceeds block gas limit",
        "oversized data",
        "TRANSACTION TOO LARGE",
    ] {
        assert!(is_oversize_send_err(m), "should flag: {m}");
    }
}

#[test]
fn is_oversize_send_err_ignores_unrelated_errors() {
    for m in ["nonce too low", "connection refused", "execution reverted"] {
        assert!(!is_oversize_send_err(m), "should not flag: {m}");
    }
}

/// The settlement route watches exactly the five `PaymentPool` lifecycle
/// events the paid-watermark cache and pool projection need — no more, no
/// fewer. `PoolOpened` in particular must never be dropped: it is the
/// projection's only signal that a pool exists. `PoolCloseInitiated` folds a
/// pool's `Closing` deadline into the projection so the redeemer's solvency
/// gate drops a drained or past-deadline lane instead of submitting a
/// `redeemMany` that reverts `PoolClosed`; the node still runs no force-redeem
/// on close.
#[test]
fn route_topic0s_covers_every_pool_lifecycle_event() {
    assert_eq!(
        settlement_route_topic0s(),
        vec![
            PaymentPool::PoolOpened::SIGNATURE_HASH,
            PaymentPool::PoolRedeemed::SIGNATURE_HASH,
            PaymentPool::PoolToppedUp::SIGNATURE_HASH,
            PaymentPool::PoolCloseInitiated::SIGNATURE_HASH,
            PaymentPool::PoolReclaimed::SIGNATURE_HASH,
        ]
    );
}

/// The settlement route resumes from the durable `PoolOpened` checkpoint,
/// rewound by the reorg margin, and anchors a cold (first-ever) boot at
/// head rather than replaying all of history.
#[test]
fn cursor_start_resumes_from_pool_opened_checkpoint() {
    #[derive(Default)]
    struct NoopCk;
    impl KeyedCheckpointStore for NoopCk {
        fn load_checkpoint(&self, _key: CheckpointKey) -> Result<Option<u64>, StoreError> {
            Ok(None)
        }
        fn record_checkpoint(&self, _key: CheckpointKey, _block: u64) -> Result<(), StoreError> {
            Ok(())
        }
    }
    let store: Arc<dyn KeyedCheckpointStore> = Arc::new(NoopCk);
    let start = cursor_start(store);
    match start {
        CursorStart::FromCheckpoint {
            checkpoint,
            reorg_margin,
            cold_start,
        } => {
            assert_eq!(checkpoint.key, CheckpointKey::PoolOpened);
            assert_eq!(reorg_margin, REORG_MARGIN_BLOCKS);
            assert_eq!(cold_start, ColdStart::Head);
        }
        CursorStart::Seeded { .. } => unreachable!("expected FromCheckpoint, got Seeded"),
        CursorStart::HeadMinusWindow { .. } => {
            unreachable!("expected FromCheckpoint, got HeadMinusWindow")
        }
    }
}

/// The redeemer's expiry check: `0` never expires, and a capability counts
/// as expired from the landing slack before its expiry onward.
#[test]
fn capability_expired_holds_the_landing_slack() {
    let expiry = 10_000;
    assert!(!super::capability_expired(0, u64::MAX), "0 = not tracked");
    assert!(!super::capability_expired(
        expiry,
        expiry - REDEEM_LANDING_SLACK_SECS - 1
    ));
    assert!(super::capability_expired(
        expiry,
        expiry - REDEEM_LANDING_SLACK_SECS
    ));
    assert!(super::capability_expired(expiry, expiry + 1));
    assert!(
        super::capability_expired(u64::MAX, u64::MAX - 1),
        "saturates"
    );
}

/// `decdn_redemption_skipped_expired_total` as the scrape reads it.
fn skipped_expired(metrics: &Metrics) -> u64 {
    let text = metrics.encode().unwrap_or_default();
    text.lines()
        .find_map(|l| l.strip_prefix("decdn_redemption_skipped_expired_total "))
        .and_then(|v| v.parse().ok())
        .unwrap_or(u64::MAX)
}

/// The planner skips a lane with value owed whose capability expires within
/// the landing slack, and meters it on its own counter. The same lane plans
/// while its expiry lies further ahead.
#[test]
fn plan_lanes_skips_an_expired_capability() {
    let me = Address::from([20u8; 20]);
    // Held registration material, so the unregistered lane plans when live.
    let st = signed_lane_state(1, 10, 20, Some(sig_with_v(1)));
    let expiry = st.expiry;
    let paid = PaidWatermarks::default();
    let projection = PoolProjection::new();

    let metrics = Arc::new(Metrics::new());
    let live_now = expiry - REDEEM_LANDING_SLACK_SECS - 1;
    let plans = plan_lanes(&paid, me, vec![st.clone()], &metrics, &projection, live_now);
    assert_eq!(plans.len(), 1, "a live capability plans");
    assert_eq!(skipped_expired(&metrics), 0);

    let late_now = expiry - REDEEM_LANDING_SLACK_SECS;
    let plans = plan_lanes(&paid, me, vec![st], &metrics, &projection, late_now);
    assert!(
        plans.is_empty(),
        "an expiring capability pays 0 and is skipped"
    );
    assert_eq!(skipped_expired(&metrics), 1);
}

/// A fully redeemed lane stays in the store until its pool is reclaimed, so
/// every sweep sees it. Once its capability expires, the planner drops it
/// without metering: no value is stranded.
#[test]
fn plan_lanes_drops_an_expired_lane_with_nothing_owed_silently() {
    let me = Address::from([20u8; 20]);
    let st = signed_lane_state(1, 10, 20, Some(sig_with_v(1)));
    let late_now = st.expiry - REDEEM_LANDING_SLACK_SECS;
    let paid = PaidWatermarks::default();
    paid.set(st.key(), st.owed());
    let projection = PoolProjection::new();
    let metrics = Arc::new(Metrics::new());

    for _ in 0..3 {
        let plans = plan_lanes(&paid, me, vec![st.clone()], &metrics, &projection, late_now);
        assert!(
            plans.is_empty(),
            "a fully redeemed lane has nothing to plan"
        );
    }
    assert_eq!(
        skipped_expired(&metrics),
        0,
        "a lane with nothing owed strands no value"
    );
}

#[test]
fn is_registered_treats_zero_and_past_as_unregistered() {
    assert!(!super::is_registered(0, 1000), "0 = unknown");
    assert!(!super::is_registered(999, 1000), "expired");
    assert!(super::is_registered(1001, 1000), "live");
}

/// A lane with a signed voucher and non-zero owed amount, ready for
/// `plan_lane` tests. `owner_sig` is the lane's own registration material —
/// `Some` for a lane whose intake verified an owner grant, `None` for one
/// that never captured one.
fn signed_lane_state(pool: u8, signer: u8, provider: u8, owner_sig: Option<[u8; 65]>) -> LaneState {
    let mut st = LaneState::hydrate(
        PoolId::from([pool; 32]),
        Address::from([signer; 20]),
        Address::from([provider; 20]),
        U256::from(10_000_000u64),
        1_800_000_000,
        U256::from(1_000u64),
        U256::from(1_048_576u64),
        Some(sig_with_v(0)),
        decdn_incentive::LaneChain::NONE,
    );
    st.owner_sig = owner_sig;
    st
}

/// A grace close counts only for this provider's lane on the closing pool
/// that the chain has not fully paid.
#[test]
fn holds_unredeemed_matches_only_an_unpaid_lane_of_this_pool_and_provider() {
    let pool = PoolId::from([1; 32]);
    let me = Address::from([20; 20]);
    let owed = signed_lane_state(1, 10, 20, None);
    assert!(holds_unredeemed(std::slice::from_ref(&owed), pool, me));

    let mut paid = owed.clone();
    paid.paid_cumulative = paid.owed();
    assert!(!holds_unredeemed(&[paid], pool, me));

    let other_provider = signed_lane_state(1, 10, 21, None);
    assert!(!holds_unredeemed(&[other_provider], pool, me));

    let other_pool = signed_lane_state(2, 10, 20, None);
    assert!(!holds_unredeemed(&[other_pool], pool, me));
}

#[test]
fn plan_lane_registered_omits_capability_reg() -> Result<()> {
    let st = signed_lane_state(1, 10, 20, None);
    let paid = PaidWatermarks::default();
    let plan = plan_lane(
        &st,
        &paid,
        Address::from([20u8; 20]),
        &RegistrationStatus::Registered,
    )?
    .ok_or_else(|| anyhow::anyhow!("registered lane with owed balance should plan"))?;
    assert!(
        plan.register.is_none(),
        "a lane this node has registered attaches no CapabilityReg"
    );
    assert_eq!(plan.key, st.key());
    Ok(())
}

#[test]
fn plan_lane_unregistered_with_material_attaches_registration() -> Result<()> {
    let st = signed_lane_state(1, 11, 21, Some(sig_with_v(1)));
    let paid = PaidWatermarks::default();
    let plan = plan_lane(
        &st,
        &paid,
        Address::from([21u8; 20]),
        &RegistrationStatus::Unregistered,
    )?
    .ok_or_else(|| anyhow::anyhow!("unregistered lane with held material should plan"))?;
    let reg = plan.register.ok_or_else(|| {
        anyhow::anyhow!("an unregistered signer with owner_sig attaches a CapabilityReg")
    })?;
    assert_eq!(reg.signer, st.signer, "reg names the lane's signer");
    assert_eq!(reg.expiry, st.expiry, "reg carries the lane's expiry");
    assert_eq!(
        reg.ownerSig.as_ref(),
        sig_with_v(1).as_slice(),
        "reg carries the lane's own owner signature"
    );
    assert_eq!(plan.key, st.key());
    Ok(())
}

#[test]
fn plan_lane_unregistered_without_material_is_skipped() -> Result<()> {
    let st = signed_lane_state(1, 12, 22, None);
    let paid = PaidWatermarks::default();
    let plan = plan_lane(
        &st,
        &paid,
        Address::from([22u8; 20]),
        &RegistrationStatus::Unregistered,
    )?;
    assert!(
        plan.is_none(),
        "an unregistered lane with no owner_sig is a durability fault and is skipped"
    );
    Ok(())
}

/// #2052 acceptance: a lane redeemed to its owed value before a restart — the
/// durable row carries `paid_cumulative == owed` — must NOT be re-planned once
/// the in-memory cache is rebuilt from the store. Exercises the real bootstrap
/// rehydration path ([`rehydrate_paid_watermarks`]) over a store, then plans.
#[test]
fn redeemed_lane_not_replanned_after_restart() -> Result<()> {
    use decdn_incentive::MemoryPoolStateStore;

    let provider = Address::from([40u8; 20]);
    let mut st = signed_lane_state(1, 30, 40, None);
    // On-chain the lane was fully redeemed before the restart; the durable
    // lane row records that.
    st.paid_cumulative = st.owed();
    anyhow::ensure!(!st.paid_cumulative.is_zero(), "fixture must be redeemable");

    let store = MemoryPoolStateStore::new();
    store.record(&st)?;

    // Restart: the volatile cache is gone. Rebuild it from the store exactly
    // as `bootstrap` does — this is the fix under test.
    let paid = rehydrate_paid_watermarks(&store, provider);

    let plan = plan_lane(&st, &paid, provider, &RegistrationStatus::Registered)?;
    assert!(
        plan.is_none(),
        "a lane already redeemed to its owed value must not be re-planned after restart (#2052)"
    );
    Ok(())
}

/// #2052: a partially-redeemed lane still owes the remainder after a restart,
/// so rehydration must leave exactly that remainder to plan — never the full
/// face value (the amnesia bug) and never nothing.
#[test]
fn partially_redeemed_lane_plans_only_the_remainder_after_restart() -> Result<()> {
    use decdn_incentive::MemoryPoolStateStore;

    let provider = Address::from([41u8; 20]);
    let mut st = signed_lane_state(1, 31, 41, None);
    let remainder = U256::from(250u64);
    st.paid_cumulative = st.owed() - remainder;

    let store = MemoryPoolStateStore::new();
    store.record(&st)?;
    let paid = rehydrate_paid_watermarks(&store, provider);

    let plan = plan_lane(&st, &paid, provider, &RegistrationStatus::Registered)?
        .ok_or_else(|| anyhow::anyhow!("a partially-redeemed lane still owes and should plan"))?;
    assert_eq!(
        plan.unredeemed, remainder,
        "only the un-redeemed remainder is planned, not the full owed value"
    );
    Ok(())
}

/// The voucher-record path carries `paid_cumulative` from the live in-memory
/// lane, which never learns the redeemed watermark. `record` must not let that
/// zero clobber a persisted non-zero value, or a later frontier advance would
/// silently reopen the #2052 amnesia within a single run.
#[test]
fn record_does_not_regress_paid_cumulative() -> Result<()> {
    use decdn_incentive::MemoryPoolStateStore;

    let store = MemoryPoolStateStore::new();
    let mut st = signed_lane_state(1, 32, 42, None);
    st.paid_cumulative = U256::from(600u64);
    store.record(&st)?;

    // A later voucher record for the same lane carries paid_cumulative back at
    // zero (the shape the serve path produces).
    let mut advanced = signed_lane_state(1, 32, 42, None);
    advanced.paid_cumulative = U256::ZERO;
    store.record(&advanced)?;

    let got = store
        .get(st.key())?
        .ok_or_else(|| anyhow::anyhow!("lane must persist"))?;
    assert_eq!(
        got.paid_cumulative,
        U256::from(600u64),
        "record must not regress the persisted paid watermark to zero"
    );
    Ok(())
}
