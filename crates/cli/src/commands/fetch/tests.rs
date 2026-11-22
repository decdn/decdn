use super::*;
use alloy::primitives::{Address, B256};
use alloy::signers::local::PrivateKeySigner;
use decdn_client::NoSourceHasBlob;
use decdn_client::ProvenSize;
use decdn_client::bundle::parse_hash;
use decdn_incentive::capacity_bond_domain;
use std::assert_matches;

fn common() -> cli::ClientFetchArgs {
    cli::ClientFetchArgs {
        node_id: Some("n".into()),
        rediscover: false,
        addr: None,
        relay_url: None,
        provider_address: Some("0x0000000000000000000000000000000000000001".into()),
        governance: cli::GovernanceChainArgs::default(),
        payment: cli::PoolSelectArgs::default(),
        capacity_bond_address: None,
        region: None,
        proxy_warming: false,
        proxy_warming_rtt_threshold_ms: 150,
        proxy_warming_margin_ms: 30,
        max_sources: 4,
        give_up_after_secs: None,
        keystore: None,
        keystore_password_file: None,
        data_dir: Some(PathBuf::from("/tmp/d")),
        working_deposit_micro_usdc: None,
        max_blob_mb: 1024,
        max_rate_per_mb: 0,
        pay_ahead_cap: 640,
        pay_ahead_divisor: 2,
        loss_weight: 20,
        timeout_ms: 3_600_000,
        capability: None,
        capability_file: None,
    }
}

fn config(body: &str) -> FileConfig {
    toml::from_str(body).expect("parse test config")
}

/// The password file is CLI/env-only — absent unless the operator passes
/// the flag, and carried through verbatim when they do. An absolute path
/// keeps the assertion off the ambient `$HOME` that `expand_tilde` reads.
#[test]
fn keystore_password_file_flows_through_and_defaults_to_none() {
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\n\
         payment_chains = [{ rpc_url = \"http://config:8545\", payment_pools = [\"0x3333333333333333333333333333333333333333\"] }]\n\
         capacity_bond_address = \"0x4444444444444444444444444444444444444444\"\n",
    );
    assert!(
        resolve_chain(&common(), &file)
            .unwrap()
            .keystore_password_file
            .is_none()
    );

    let mut a = common();
    a.keystore_password_file = Some(PathBuf::from("/abs/pw.txt"));
    assert_eq!(
        resolve_chain(&a, &file).unwrap().keystore_password_file,
        Some(PathBuf::from("/abs/pw.txt"))
    );
}

/// `-o -` streams to stdout; every other path — including a file literally
/// named `-` addressed as `./-` — writes to disk. Only the exact single-dash
/// output selects the stdout stream (#1848 4b).
#[test]
fn dash_output_selects_the_stdout_stream() {
    assert!(wants_stdout(Path::new("-")));
    assert!(!wants_stdout(Path::new("./-")));
    assert!(!wants_stdout(Path::new("-.bin")));
    assert!(!wants_stdout(Path::new("out.bin")));
}

/// A stdout stream that failed because the fetch gave up ends with the
/// fetch's typed `GaveUp`, not the reader's message, so it exits 75; with
/// no fetch error, the read or write error stands.
#[test]
fn a_stdout_give_up_keeps_its_type() {
    let gave_up = decdn_client::GaveUp {
        idle: Duration::from_mins(10),
    };
    let read_err = anyhow::anyhow!("read verified stream: stream fetch failed: {gave_up}");
    let err = stream_error(read_err, Some(anyhow::Error::new(gave_up)));
    assert_eq!(err.downcast_ref::<decdn_client::GaveUp>(), Some(&gave_up));

    let write_err = stream_error(anyhow::anyhow!("write to stdout: broken pipe"), None);
    assert!(write_err.downcast_ref::<decdn_client::GaveUp>().is_none());
    assert!(write_err.to_string().contains("broken pipe"));
}

/// `copy_verified` drains every byte to the sink in order, reports `0`
/// before the first read (a stream resumes nothing), and reports the final
/// cumulative position as the blob's total — the stdout stream's bar signal
/// (#1848 4b).
#[tokio::test]
async fn copy_verified_streams_all_bytes_and_reports_final_total() -> anyhow::Result<()> {
    let data = vec![7u8; 5000];
    let total = u64::try_from(data.len())?;
    let mut reader: &[u8] = &data;
    let mut sink: Vec<u8> = Vec::new();

    let positions = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&positions);
    let cb = move |pos: u64, _total: u64| {
        observed.lock().unwrap().push(pos);
    };
    let cb_ref: &dyn Fn(u64, u64) = &cb;

    let drained = copy_verified(&mut reader, &mut sink, total, Some(cb_ref)).await?;

    assert_eq!(drained, total, "every byte is drained");
    assert_eq!(sink, data, "the sink receives the bytes in order");
    let positions = positions.lock().unwrap();
    assert_eq!(positions.first(), Some(&0), "the first report is no byte");
    assert_eq!(
        positions.last(),
        Some(&total),
        "the final progress report reaches the blob's total"
    );
    Ok(())
}

fn ctx() -> PoolContext {
    PoolContext {
        pool_id: B256::ZERO,
        provider: Address::ZERO,
        deposit: U256::ZERO,
        client_signer: Arc::new(PrivateKeySigner::random()),
        voucher_domain: capacity_bond_domain(1, Address::ZERO),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
        capability: None,
    }
}

/// The refusal these tests annotate, built the way the fetch path builds it: the typed
/// `UpstreamRefused` sentinel (#1144). Never hand-roll one with
/// `anyhow!("delivery refused: …")` — the annotation downcasts, so a look-alike string
/// would exercise nothing and pass against a hint that never fires in production.
fn refusal(error: StreamError) -> anyhow::Error {
    anyhow::Error::new(UpstreamRefused::mid_stream(error))
}

/// A delegated `SpendingCapExhausted` is reconnected to the owner-side remedy; the
/// delegate holds no wallet on the pool, so "top up / issue a capability for a new
/// signer key" is the fix.
#[test]
fn delegated_spending_cap_exhausted_gets_the_owner_remedy_hint() {
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
        bundle: None,
        proof_generation: None,
    });
    let annotated = super::annotate_delegated_exhaustion(err);
    assert!(
        annotated.to_string().contains("funding needed"),
        "expected the funding remedy, got: {annotated}"
    );
}

/// A drained signer registration names the remedy a write-once
/// registration leaves: a capability for a new signer key (#2338), whether
/// the fetch ended "funding needed" or on a mid-stream
/// `SignerCapExhausted`.
#[test]
fn delegated_drained_signer_gets_the_new_key_remedy() {
    for err in [
        anyhow::Error::new(NoAffordableSource {
            deposit: U256::from(5u32),
        }),
        anyhow::Error::new(UpstreamVoucherRejected {
            reason: decdn_protocol::client::VoucherRejectReason::SignerCapExhausted,
            bundle: None,
            proof_generation: None,
        }),
    ] {
        let annotated = format!("{:#}", super::annotate_delegated_exhaustion(err));
        assert!(
            annotated.contains("funding needed") && annotated.contains("new signer key"),
            "expected the new-key remedy, got: {annotated}"
        );
    }
}

/// The SDK's typed delegated stops render as the CLI's remedies: a capability
/// that no longer pays names a new signer key, and a pool that cannot pay
/// under a healthy capability names the owner's top-up.
#[test]
fn delegated_typed_funding_stops_render_their_remedies() {
    let pool = alloy::primitives::B256::repeat_byte(0x11);
    let exhausted = || {
        anyhow::Error::new(NoAffordableSource {
            deposit: U256::from(5u32),
        })
    };
    let new_capability = format!(
        "{:#}",
        super::annotate_delegated_exhaustion(exhausted().context(
            decdn_client::FundingNeeded::NewCapability {
                pool,
                cause: decdn_client::CapabilityCause::CapSpent,
            }
        ))
    );
    assert!(
        new_capability.contains("new signer key") && new_capability.contains("spent its cap"),
        "{new_capability}"
    );
    let publisher = format!(
        "{:#}",
        super::annotate_delegated_exhaustion(
            exhausted().context(decdn_client::FundingNeeded::PublisherPool { pool })
        )
    );
    assert!(
        publisher.contains(&format!("ask the owner of pool {pool} to top it up"))
            && publisher.contains("registered terms are spent"),
        "{publisher}"
    );
}

/// Any error the delegated-exhaustion annotator does not name passes
/// through untouched.
#[test]
fn delegated_non_cap_error_is_untouched() {
    let annotated = super::annotate_delegated_exhaustion(anyhow::anyhow!("stalled"));
    assert_eq!(annotated.to_string(), "stalled");
}

/// A delegated `CapabilityExpired` rejection also gets the owner-remedy
/// hint: the delegate cannot mint itself a fresh capability either.
#[test]
fn delegated_capability_expired_gets_the_owner_remedy_hint() {
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: decdn_protocol::client::VoucherRejectReason::CapabilityExpired,
        bundle: None,
        proof_generation: None,
    });
    let annotated = super::annotate_delegated_exhaustion(err);
    assert!(
        annotated.to_string().contains("funding needed"),
        "expected the funding remedy, got: {annotated}"
    );
}

/// A delegated `PoolExhausted` rejection also gets the owner-remedy
/// hint: it is a pool-wide deposit shortfall, not this signer's cap.
#[test]
fn delegated_pool_exhausted_gets_the_owner_remedy_hint() {
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: decdn_protocol::client::VoucherRejectReason::PoolExhausted,
        bundle: None,
        proof_generation: None,
    });
    let annotated = super::annotate_delegated_exhaustion(err);
    assert!(
        annotated.to_string().contains("funding needed"),
        "expected the funding remedy, got: {annotated}"
    );
}

/// Every source saying `NotFound` ends the fetch as `NoSourceHasBlob`, with no
/// signed refusal left in the chain to read a quoted rate from, so it passes
/// through untouched.
#[test]
fn no_source_has_blob_is_untouched() {
    let annotated = annotate_cache_miss(anyhow::Error::new(NoSourceHasBlob), &ctx());
    assert_eq!(annotated.to_string(), NoSourceHasBlob.to_string());
    assert!(annotated.downcast_ref::<NoSourceHasBlob>().is_some());
}

/// A delegated fetch no provider's voucher fits names the owner-side remedy.
#[test]
fn delegated_no_affordable_source_gets_the_owner_remedy_hint() {
    let err = anyhow::Error::new(NoAffordableSource {
        deposit: U256::from(5u32),
    });
    let annotated = annotate_delegated_exhaustion(err);
    assert!(
        annotated
            .to_string()
            .contains("Ask the pool owner to top up"),
        "{annotated}"
    );
}

/// A mid-stream `NotFound` carries no signed refusal, so there is no quoted
/// rate to compare the deposit against: it passes through untouched.
#[test]
fn unsigned_notfound_refusal_is_untouched() {
    let err = refusal(StreamError::NotFound);
    let want = err.to_string();
    let annotated = annotate_cache_miss(err, &ctx());
    assert_eq!(annotated.to_string(), want);
}

/// An open-stage `error` refusal the node signed at `rate_per_mb`, built the
/// way the fetch path holds one: an `UpstreamRefused` whose `evidence()` is
/// the signed response.
fn signed_open(rate_per_mb: u64, error: StreamError) -> anyhow::Error {
    use decdn_incentive::stream_sig::StreamSlashData;
    use decdn_protocol::client::{StreamResponse, StreamResponseBody};

    let body = StreamResponseBody {
        hash: [7; 32],
        ok: false,
        rate_per_mb,
        timestamp_us: 1_700_000_000_000_000,
    };
    let operator = PrivateKeySigner::random();
    let sig = StreamSlashData::from_response_body(&body)
        .sign(&operator, &capacity_bond_domain(1, Address::ZERO))
        .unwrap();
    let response = StreamResponse {
        body,
        slash_sig: sig.as_bytes().to_vec(),
    };
    anyhow::Error::new(UpstreamRefused::open_for_test(response, error))
}

/// [`signed_open`] for a `NotFound` refusal.
fn signed_notfound(rate_per_mb: u64) -> anyhow::Error {
    signed_open(rate_per_mb, StreamError::NotFound)
}

/// A pool context with `deposit − prior_amount` of headroom.
fn ctx_with_headroom(deposit: u64, prior_amount: u64) -> PoolContext {
    PoolContext {
        deposit: U256::from(deposit),
        prior_amount: U256::from(prior_amount),
        ..ctx()
    }
}

/// The branch the annotation exists for: a signed `NotFound` whose quoted
/// rate prices one chunk above what the pool has left. The node collapses
/// that refusal onto `NotFound`, so the context is the only place the
/// operator learns the deposit is the cause.
#[test]
fn signed_notfound_above_the_headroom_names_the_deposit() {
    let rate = 1_000;
    let estimate = min_payment(decdn_protocol::client::CHUNK_BYTES, rate);
    let headroom = estimate - U256::from(1u8);
    let deposit = 10_000u64;
    let prior = deposit - u64::try_from(headroom).unwrap();

    let err = signed_notfound(rate);
    let want_root = err.to_string();
    let annotated = annotate_cache_miss(err, &ctx_with_headroom(deposit, prior));
    let msg = annotated.to_string();
    assert!(
        msg.contains(&format!("remaining deposit ({headroom})")),
        "names the remaining deposit: {msg}"
    );
    assert!(msg.contains("--working-deposit-micro-usdc"), "{msg}");
    assert!(
        annotated.downcast_ref::<UpstreamRefused>().is_some(),
        "the typed refusal stays in the chain"
    );
    assert_eq!(annotated.root_cause().to_string(), want_root);
}

/// Enough headroom for the node's one-chunk reservation: the deposit is not
/// the cause, so the refusal passes through untouched.
#[test]
fn signed_notfound_within_the_headroom_is_untouched() {
    let rate = 1_000;
    let estimate = u64::try_from(min_payment(decdn_protocol::client::CHUNK_BYTES, rate)).unwrap();
    let err = signed_notfound(rate);
    let want = err.to_string();
    let annotated = annotate_cache_miss(err, &ctx_with_headroom(estimate, 0));
    assert_eq!(annotated.to_string(), want);
}

/// A zero quoted rate prices one chunk at 0, which even an empty pool covers,
/// so a free quote is never blamed on the deposit.
#[test]
fn signed_notfound_at_rate_zero_is_untouched() {
    assert_eq!(
        min_payment(decdn_protocol::client::CHUNK_BYTES, 0),
        U256::ZERO
    );
    let err = signed_notfound(0);
    let want = err.to_string();
    let annotated = annotate_cache_miss(err, &ctx_with_headroom(0, 0));
    assert_eq!(annotated.to_string(), want);
}

/// A signed open-stage refusal that is NOT `NotFound` is never annotated,
/// even when its quoted rate prices a chunk above an empty pool: the filter
/// on the refusal kind, not the headroom, is what passes it through.
#[test]
fn signed_non_notfound_refusal_is_untouched() {
    for error in [StreamError::Declined, StreamError::Unfunded] {
        let err = signed_open(1_000, error);
        let want = format!("{err:#}");
        let annotated = annotate_cache_miss(err, &ctx_with_headroom(0, 0));
        assert_eq!(format!("{annotated:#}"), want, "no context added");
    }
}

/// A refusal that is NOT `NotFound`, or a non-refusal failure, is never
/// annotated: a larger deposit cannot fix it.
#[test]
fn non_notfound_errors_are_untouched() {
    for err in [
        refusal(StreamError::Declined),
        refusal(StreamError::Unfunded),
        anyhow::anyhow!("connect failed: timed out"),
    ] {
        let want = err.to_string();
        let annotated = annotate_cache_miss(err, &ctx());
        assert_eq!(annotated.to_string(), want);
    }
}

#[test]
fn flags_override_config() {
    let mut c = common();
    c.governance.governance_rpc_url = Some("http://gov-flag:8545".into());
    c.governance.governance_chain_id = Some(99);
    let pp = "0x1111111111111111111111111111111111111111";
    c.payment.pool = Some(format!("eip155:421614:{pp}"));
    c.payment.pool_rpc_url = Some("http://flag:8545".into());
    c.capacity_bond_address = Some("0x2222222222222222222222222222222222222222".into());
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\npayment_chains = [{ rpc_url = \"http://config:8545\", payment_pools = [\"0x3333333333333333333333333333333333333333\"] }]\ncapacity_bond_address = \"0x4444444444444444444444444444444444444444\"\n",
    );
    let r = resolve_chain(&c, &file).unwrap();
    assert_eq!(
        r.config.payment_pools.len(),
        1,
        "--pool pays through one pool"
    );
    let pool = r.config.first_pool().unwrap();
    assert_eq!(pool.rpc_url, "http://flag:8545");
    assert_eq!(pool.deployment.chain_id, 421_614);
    assert_eq!(pool.deployment.payment_pool, Address::from_str(pp).unwrap());
    assert_eq!(r.config.governance.chain_id, 99);
    assert_eq!(r.config.governance.rpc_url, "http://gov-flag:8545");
}

/// The registry and the signature domains live on the governance chain, so a
/// fetch needs its RPC: a payment chain's endpoint never stands in for it.
#[test]
fn the_governance_rpc_is_required() {
    let file = config(
        "[blockchain]\npayment_chains = [{ rpc_url = \"http://pay:8545\", payment_pools = [\"0x3333333333333333333333333333333333333333\"] }]\ncapacity_bond_address = \"0x4444444444444444444444444444444444444444\"\n",
    );
    let err = resolve_chain(&common(), &file).expect_err("no governance rpc");
    assert!(err.to_string().contains("--governance-rpc-url"), "{err}");
    let mut c = common();
    c.governance.governance_rpc_url = Some("http://gov:8545".into());
    assert_eq!(
        resolve_chain(&c, &file).unwrap().config.governance.rpc_url,
        "http://gov:8545"
    );
}

/// The governance chain and each payment chain keep their own RPC endpoint
/// and chain id: registry reads and the `CapacityBond` domain use the
/// governance chain, vouchers each pool's payment chain.
#[test]
fn governance_and_payment_chains_stay_apart() {
    let file = config(
        "[blockchain]\nrpc_url = \"http://gov:8545\"\nchain_id = 11155111\n\
         capacity_bond_address = \"0x5555555555555555555555555555555555555555\"\n\
         [[blockchain.payment_chains]]\nrpc_url = \"http://arb:8545\"\nchain_id = 421614\n\
         payment_pools = [\"0x3333333333333333333333333333333333333333\", \
         \"0x6666666666666666666666666666666666666666\"]\n\
         [[blockchain.payment_chains]]\nrpc_url = \"http://base:8545\"\nchain_id = 84532\n\
         payment_pools = [\"0x7777777777777777777777777777777777777777\"]\n",
    );
    let r = resolve_chain(&common(), &file).unwrap();
    assert_eq!(r.config.governance.rpc_url, "http://gov:8545");
    assert_eq!(r.config.governance.chain_id, 11_155_111);
    let pools: Vec<_> = r
        .config
        .payment_pools
        .iter()
        .map(|p| {
            (
                p.rpc_url.as_str(),
                p.deployment.chain_id,
                p.deployment.payment_pool,
            )
        })
        .collect();
    assert_eq!(
        pools,
        vec![
            ("http://arb:8545", 421_614, Address::repeat_byte(0x33)),
            ("http://arb:8545", 421_614, Address::repeat_byte(0x66)),
            ("http://base:8545", 84_532, Address::repeat_byte(0x77)),
        ]
    );
}

#[test]
fn config_fills_unset_flags_and_defaults() {
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_chains = [{ rpc_url = \"http://config:8545\", payment_pools = [\"0x3333333333333333333333333333333333333333\"] }]\ncapacity_bond_address = \"0x4444444444444444444444444444444444444444\"\nbuyer_working_deposit_micro_usdc = 5000000\n",
    );
    let r = resolve_chain(&common(), &file).unwrap();
    assert_eq!(r.config.first_pool().unwrap().rpc_url, "http://config:8545");
    // chain_id absent everywhere → default.
    assert_eq!(r.config.governance.chain_id, DEFAULT_CHAIN_ID);
    assert_eq!(r.working_deposit, U256::from(5_000_000u64));
    // keystore defaults under the data dir.
    assert_eq!(
        r.keystore,
        eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
    );
}

/// `resolve_chain` must enforce the same deposit invariant the daemon
/// resolver does. It reads the raw `[blockchain]` table plus the
/// CLI flags rather than going through `resolve_blockchain_into`, so without
/// its own check `decdn get` would accept a config file that
/// `decdn config validate` rejects — a validator that does not validate what
/// actually runs — and `--working-deposit-micro-usdc 0` would reach the chain
/// and surface as an opaque `openPool` `ZeroAmount` revert.
#[test]
fn resolve_chain_rejects_deposits_the_daemon_resolver_would_reject() {
    let base = "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_chains = [{ rpc_url = \"http://config:8545\", payment_pools = [\"0x3333333333333333333333333333333333333333\"] }]\ncapacity_bond_address = \"0x4444444444444444444444444444444444444444\"\n";

    // A zero working deposit can never open a pool.
    let err = resolve_chain(
        &common(),
        &config(&format!("{base}buyer_working_deposit_micro_usdc = 0\n")),
    )
    .expect_err("a zero working deposit must be refused at resolve time");
    assert!(
        err.to_string().contains("buyer_working_deposit_micro_usdc"),
        "the error must name the offending field; got: {err}"
    );
}

/// A usable rate renders as a human `X/s`; a sub-1-byte/s rate (no data yet,
/// or a stall) and any non-finite value both render as `--`.
#[test]
fn fmt_rate_shows_human_units_and_placeholder_below_one() {
    assert!(fmt_rate(2.0 * 1024.0 * 1024.0).ends_with("/s"));
    assert!(fmt_rate(2.0 * 1024.0 * 1024.0).contains("MiB"));
    assert_eq!(fmt_rate(0.0), "--");
    assert_eq!(fmt_rate(0.4), "--");
    assert_eq!(fmt_rate(f64::NAN), "--");
    assert_eq!(fmt_rate(f64::INFINITY), "--");
}

/// ETA divides remaining bytes by the smoothed rate; below a usable rate it
/// reports `ETA --` rather than a divide-by-tiny blow-up, and a huge
/// projection is clamped so `Duration::from_secs_f64` cannot overflow.
#[test]
fn fmt_eta_projects_and_guards_low_rate() {
    assert_eq!(fmt_eta(10 << 20, 0.0), "ETA --");
    assert_eq!(fmt_eta(10 << 20, 0.9), "ETA --");
    assert!(fmt_eta(10 << 20, 10.0 * 1024.0 * 1024.0).starts_with("ETA "));
    // A near-zero rate with bytes left must not panic on the clamp path.
    let _ = fmt_eta(u64::MAX, 1.0);
}

/// The summary measures elapsed and bytes-moved from the FIRST observed
/// sample, not the final position — so a resumed fetch that began at a
/// non-zero `base_present` reports only what this run actually transferred.
#[test]
fn summary_reports_delta_from_first_sample_not_absolute_position() {
    let t0 = Instant::now();
    let state = SpeedState {
        // Resumed at 40 MiB already present, ran for 2s to 60 MiB.
        started: Some((t0, 40 << 20)),
        last: Some((t0 + Duration::from_secs(2), 60 << 20)),
        ewma_bps: None,
    };
    let meter = DeliveryMeter {
        state: Arc::new(Mutex::new(state)),
    };
    let (elapsed, moved) = meter
        .summary()
        .expect("a delivered sample yields a summary");
    assert_eq!(elapsed, Duration::from_secs(2));
    assert_eq!(
        moved,
        20 << 20,
        "only this run's 20 MiB, not the 60 MiB total"
    );
}

/// No delivery ever observed (a failure before the first byte) yields no
/// summary, so the caller falls back to the bare byte/output line.
#[test]
fn summary_is_none_before_any_delivery() {
    let meter = DeliveryMeter {
        state: Arc::new(Mutex::new(SpeedState::default())),
    };
    assert!(meter.summary().is_none());
}

fn replacement(closed: u8, opened: u8) -> PoolReplaced {
    PoolReplaced {
        closed: B256::repeat_byte(closed),
        opened: B256::repeat_byte(opened),
    }
}

/// A pass that a funding recovery step ended by opening a new pool runs the
/// remaining work again, once. A second replacement in the same command ends
/// it "funding needed"; a pass that ends any other way ends the command.
#[test]
fn a_replaced_pool_runs_the_work_again_at_most_once() {
    let mut replaced = None;
    let first = replacement(1, 2);
    assert!(
        next_pass(Err(anyhow::Error::new(first)), &mut replaced, false).is_none(),
        "the first replacement runs the work again"
    );
    assert_eq!(replaced, Some(first));

    let err = next_pass(
        Err(anyhow::Error::new(replacement(2, 3))),
        &mut replaced,
        false,
    )
    .expect("a second replacement ends the command")
    .expect_err("it ends funding needed");
    assert!(
        format!("{err:#}").contains("accepts no more funds either"),
        "{err:#}"
    );

    let mut replaced = Some(first);
    assert_matches!(
        next_pass(Ok(()), &mut replaced, false),
        Some(Ok(())),
        "the second pass completes"
    );
    let mut replaced = None;
    let err = next_pass(Err(anyhow::anyhow!("not found")), &mut replaced, false)
        .expect("any other end ends the command")
        .expect_err("with its own error");
    assert_eq!(format!("{err}"), "not found");
    assert_eq!(replaced, None);
}

/// #2519: a late lane build's escrow folded over the `PoolReplaced` that
/// ended the pass leaves the replacement only as text (`fold_late_escrow`,
/// whose shape the bundle engine's tests pin). The escrow error ends the
/// command with no second pass; the gate still lists the replacement for the
/// command to name.
#[test]
fn a_replacement_under_a_late_escrow_runs_nothing_again() {
    let new = replacement(1, 2);
    let mut replaced = None;
    let err = next_pass(
        Err(
            anyhow::anyhow!("a lane build may have escrowed USDC no record credits")
                .context(format!("the fetch failed ({new})")),
        ),
        &mut replaced,
        false,
    )
    .expect("an escrow error runs nothing again")
    .expect_err("it ends the command");
    assert!(
        !format!("{err:#}").contains("now the current pool"),
        "{err:#}"
    );
    assert_eq!(replaced, None);
}

/// A stream to stdout never runs again: the bytes it wrote cannot be taken
/// back. Its replacement ends the command and names the new pool.
#[test]
fn a_stdout_stream_names_the_new_pool_and_does_not_run_again() {
    let mut replaced = None;
    let err = next_pass(
        Err(anyhow::Error::new(replacement(1, 2))),
        &mut replaced,
        true,
    )
    .expect("a stdout stream ends the command")
    .expect_err("with the replacement");
    assert!(
        format!("{err:#}").contains(&B256::repeat_byte(2).to_string()),
        "{err:#}"
    );
    assert_eq!(replaced, None);
}

/// A self-owned pool no provider's voucher fits names the top-up command
/// with the pool id; any other error passes through.
#[test]
fn a_self_owned_pool_no_voucher_fits_names_the_top_up_command() {
    let pool = B256::repeat_byte(7);
    let err = anyhow::Error::new(NoAffordableSource {
        deposit: U256::from(5u32),
    });
    let hinted = format!("{:#}", with_top_up_hint(err, Some(pool)));
    assert!(
        hinted.contains(&format!(
            "decdn pool top-up --pool-id {pool} --amount-micro-usdc"
        )),
        "{hinted}"
    );
    let other = with_top_up_hint(anyhow::anyhow!("reset"), Some(pool));
    assert_eq!(other.to_string(), "reset");
}

/// A delegated command's slot refuses a loaded key the capability does not
/// authorize, naming both addresses and the remedy: every voucher it signed
/// would be refused as `WrongSigner`.
#[test]
fn a_delegated_slot_rejects_a_key_the_capability_does_not_name() {
    let delegate = PrivateKeySigner::random();
    let capability = decdn_incentive::Capability {
        signer: delegate.address(),
        spending_cap: 1_000,
        pool_id: B256::repeat_byte(0x11),
        expiry: u64::MAX,
    }
    .sign(&delegate, &capacity_bond_domain(1, Address::ZERO))
    .unwrap();
    let grant = CapabilityGrant::from_signed_capability(&capability);
    assert!(delegated_slot(&grant, Arc::new(delegate.clone())).is_ok());

    let other = PrivateKeySigner::random();
    let err = delegated_slot(&grant, Arc::new(other.clone())).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains(&delegate.address().to_string()), "{msg}");
    assert!(msg.contains(&other.address().to_string()), "{msg}");
    assert!(msg.contains("load the delegate keystore"), "{msg}");
}

const ENTRY_A: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
const ENTRY_B: &str = "1111111111111111111111111111111111111111111111111111111111111111";

fn manifest_json(entries: &[(&str, &str)]) -> Vec<u8> {
    let entries: Vec<String> = entries
        .iter()
        .map(|(path, hash)| format!(r#"{{"path":"{path}","hash":"{hash}"}}"#))
        .collect();
    format!(r#"{{"version":1,"entries":[{}]}}"#, entries.join(",")).into_bytes()
}

#[test]
fn entry_hash_picks_the_named_entry() {
    let bytes = manifest_json(&[
        ("a/first.bin", &format!("b3:{ENTRY_A}")),
        ("b/second.bin", ENTRY_B),
    ]);
    assert_eq!(
        entry_hash(&bytes, "b/second.bin").unwrap(),
        parse_hash(ENTRY_B).unwrap()
    );
    assert_eq!(
        entry_hash(&bytes, "a/first.bin").unwrap(),
        parse_hash(ENTRY_A).unwrap()
    );
}

#[test]
fn entry_hash_names_a_missing_entry() {
    let bytes = manifest_json(&[("a/first.bin", ENTRY_A)]);
    let err = format!("{:#}", entry_hash(&bytes, "first.bin").unwrap_err());
    assert!(err.contains("no entry \"first.bin\""), "{err}");
}

#[test]
fn entry_hash_rejects_a_blob_that_is_not_a_manifest() {
    for bytes in [
        &b"\x00\x01raw video bytes"[..],
        br#"{"version":2,"entries":[]}"#,
    ] {
        let err = format!("{:#}", entry_hash(bytes, "a").unwrap_err());
        assert!(err.contains("not a bundle manifest"), "{err}");
    }
}

/// One malformed entry hash makes the whole manifest malformed, so a URI
/// that names a valid sibling entry fails too. The error names the bad entry
/// and gives no raw-blob hint.
#[test]
fn entry_hash_names_the_entry_of_a_malformed_hash() {
    let bytes = manifest_json(&[("a.bin", "not-hex"), ("b.bin", ENTRY_B)]);
    for path in ["a.bin", "b.bin"] {
        let err = format!("{:#}", entry_hash(&bytes, path).unwrap_err());
        assert!(err.contains(MALFORMED_MANIFEST), "{err}");
        assert!(!err.contains("raw blob"), "{err}");
        assert!(
            err.contains("bundle entry \"a.bin\" has a malformed hash"),
            "{err}"
        );
    }
}

#[tokio::test]
async fn manifest_buf_holds_the_cap_and_fails_past_it() {
    use tokio::io::AsyncWriteExt as _;
    let mut buf = ManifestBuf::default();
    let chunk = vec![0u8; 1 << 20];
    for _ in 0..(MAX_MANIFEST_BYTES / chunk.len()) {
        buf.write_all(&chunk).await.unwrap();
    }
    assert_eq!(buf.0.lock().unwrap().len(), MAX_MANIFEST_BYTES);
    let err = buf.write_all(&[0]).await.unwrap_err();
    assert!(err.to_string().contains("larger than"), "{err}");
    assert_eq!(buf.0.lock().unwrap().len(), MAX_MANIFEST_BYTES);
}

#[test]
fn max_manifest_mb_is_the_byte_cap_in_mib() {
    assert_eq!(
        u64::try_from(MAX_MANIFEST_BYTES >> 20).unwrap(),
        MAX_MANIFEST_MB
    );
}

#[test]
fn a_manifest_fetch_caps_at_the_manifest_size() {
    assert_eq!(
        manifest_cap_mb(0),
        MAX_MANIFEST_MB,
        "no cap takes the manifest cap"
    );
    assert_eq!(manifest_cap_mb(10), 10, "a lower cap stays");
    assert_eq!(manifest_cap_mb(1 << 20), MAX_MANIFEST_MB);
}

/// The cap on bytes a fetch holds: `--max-blob-mb` (0 for none), and under
/// it the size the caller knows the blob to have. A declared size never turns
/// into "no cap": zero caps at one byte, so a blob a manifest calls empty
/// still ends at its first chunk group.
#[test]
fn a_declared_size_caps_the_fetch_and_never_lifts_the_cap() {
    const MIB: u64 = 1024 * 1024;
    assert_eq!(held_cap(0, None), 0, "no cap at all");
    assert_eq!(held_cap(2, None), 2 * MIB);
    assert_eq!(held_cap(0, Some(5)), 5);
    assert_eq!(
        held_cap(2, Some(5 * MIB)),
        2 * MIB,
        "the smaller bound wins"
    );
    assert_eq!(held_cap(2, Some(5)), 5);
    assert_eq!(held_cap(0, Some(0)), 1, "zero is a cap, not its absence");
    assert_eq!(held_cap(2, Some(0)), 1);
}

#[test]
fn size_hints_forget_the_oldest_past_the_bound() {
    let key = |n: usize| {
        let mut h = [0u8; 32];
        h[..8].copy_from_slice(&n.to_le_bytes());
        h
    };
    let size = ProvenSize::assume_for_tests;
    let mut sizes = SizeHints::default();
    for n in 0..MAX_SIZE_HINTS {
        sizes.insert(key(n), size(7));
    }
    // A repeat insert keeps the first size and the entry's place.
    sizes.insert(key(0), size(8));
    assert_eq!(sizes.get(&key(0)), Some(size(7)));
    assert_eq!(sizes.order.len(), MAX_SIZE_HINTS);
    sizes.insert(key(MAX_SIZE_HINTS), size(9));
    assert_eq!(sizes.get(&key(0)), None, "the oldest is forgotten");
    assert_eq!(sizes.get(&key(1)), Some(size(7)));
    assert_eq!(sizes.get(&key(MAX_SIZE_HINTS)), Some(size(9)));
    assert_eq!(
        (sizes.sizes.len(), sizes.order.len()),
        (MAX_SIZE_HINTS, MAX_SIZE_HINTS)
    );
}

/// A blob an earlier fetch verified starts the next fetch at its size, so a
/// pinned node is not opened for it before its lane opens.
#[test]
fn a_verified_size_is_the_next_fetchs_known_size() {
    let sizes = Mutex::new(SizeHints::default());
    assert_eq!(known_size(&[5; 32], &sizes), None);
    sizes
        .lock()
        .unwrap()
        .insert([5; 32], ProvenSize::assume_for_tests(4096));
    assert_eq!(
        known_size(&[5; 32], &sizes),
        Some(ProvenSize::assume_for_tests(4096))
    );
    assert_eq!(known_size(&[6; 32], &sizes), None);
}
