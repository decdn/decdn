use super::*;
use alloy::primitives::{Address, B256};
use alloy::signers::local::PrivateKeySigner;
use decdn_client::sign_client_binding;
use decdn_incentive::bind_node_id_domain;
use std::assert_matches;

fn common() -> cli::ClientFetchArgs {
    cli::ClientFetchArgs {
        node_id: Some("n".into()),
        rediscover: false,
        addr: None,
        relay_url: None,
        provider_address: Some("0x0000000000000000000000000000000000000001".into()),
        rpc_url: None,
        payment_pool_address: None,
        slash_judge_address: None,
        capacity_bond_address: None,
        region: None,
        proxy_warming: false,
        proxy_warming_rtt_threshold_ms: 150,
        proxy_warming_margin_ms: 30,
        max_sources: 4,
        give_up_after_secs: None,
        chain_id: None,
        keystore: None,
        keystore_password_file: None,
        data_dir: Some(PathBuf::from("/tmp/d")),
        working_deposit_micro_usdc: None,
        max_blob_mb: 1024,
        max_rate_per_mb: 0,
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
         payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n\
         slash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
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

/// `copy_verified` drains every byte to the sink in order and reports the
/// final cumulative position as the blob's total — the stdout stream's bar
/// signal (#1848 4b).
#[tokio::test]
async fn copy_verified_streams_all_bytes_and_reports_final_total() -> anyhow::Result<()> {
    let data = vec![7u8; 5000];
    let total = u64::try_from(data.len())?;
    let mut reader: &[u8] = &data;
    let mut sink: Vec<u8> = Vec::new();

    let last = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let observed = Arc::clone(&last);
    let cb = move |pos: u64, _total: u64| {
        observed.store(pos, std::sync::atomic::Ordering::SeqCst);
    };
    let cb_ref: &dyn Fn(u64, u64) = &cb;

    let drained = copy_verified(&mut reader, &mut sink, total, Some(cb_ref)).await?;

    assert_eq!(drained, total, "every byte is drained");
    assert_eq!(sink, data, "the sink receives the bytes in order");
    assert_eq!(
        last.load(std::sync::atomic::Ordering::SeqCst),
        total,
        "the final progress report reaches the blob's total"
    );
    Ok(())
}

fn ctx_with(binding: Option<decdn_protocol::client::ClientBinding>) -> PoolContext {
    ctx_with_deposit(binding, U256::ZERO)
}

fn ctx_with_deposit(
    binding: Option<decdn_protocol::client::ClientBinding>,
    deposit: U256,
) -> PoolContext {
    PoolContext {
        pool_id: B256::ZERO,
        provider: Address::ZERO,
        deposit,
        client_signer: Arc::new(PrivateKeySigner::random()),
        voucher_domain: bind_node_id_domain(1, Address::ZERO),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: binding,
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

/// An unbound (no `capacity_bond_address`) fetch refused with `NotFound` gets
/// the actionable hint attached, reconnecting the opaque refusal to its cause.
#[test]
fn unbound_notfound_refusal_gets_actionable_hint() {
    let annotated = annotate_unbound_cache_miss(refusal(StreamError::NotFound), &ctx_with(None));
    assert!(
        annotated.to_string().contains("capacity_bond_address"),
        "expected the binding hint, got: {annotated}"
    );
}

/// Every source saying `NotFound` ends the fetch as `NoSourceHasBlob`, with
/// no `UpstreamRefused` left in the chain: an unbound fetch still gets the
/// binding hint, and a bound one passes through untouched.
#[test]
fn unbound_no_source_has_blob_gets_the_binding_hint() {
    let absent = || anyhow::Error::new(NoSourceHasBlob);
    let annotated = annotate_unbound_cache_miss(absent(), &ctx_with(None));
    assert!(
        annotated.to_string().contains("capacity_bond_address"),
        "expected the binding hint, got: {annotated}"
    );
    assert!(annotated.downcast_ref::<NoSourceHasBlob>().is_some());

    let signer = PrivateKeySigner::random();
    let binding = sign_client_binding(&signer, B256::ZERO, &bind_node_id_domain(1, Address::ZERO))
        .expect("sign binding");
    let bound = annotate_unbound_cache_miss(absent(), &ctx_with(Some(binding)));
    assert_eq!(bound.to_string(), NoSourceHasBlob.to_string());
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

/// A bound fetch's error is passed through untouched — a `NotFound` there is a
/// genuine miss, not a missing-binding problem.
#[test]
fn bound_notfound_refusal_is_untouched() {
    let signer = PrivateKeySigner::random();
    let binding = sign_client_binding(&signer, B256::ZERO, &bind_node_id_domain(1, Address::ZERO))
        .expect("sign binding");
    let annotated =
        annotate_unbound_cache_miss(refusal(StreamError::NotFound), &ctx_with(Some(binding)));
    assert!(!annotated.to_string().contains("capacity_bond_address"));
}

/// A refusal that is NOT `NotFound` gets no binding hint even when unbound: a
/// client binding authorizes reactive pull-through, so it cannot fix a node
/// that declines the blob or one the requester cannot fund.
#[test]
fn unbound_non_notfound_refusal_is_untouched() {
    for error in [StreamError::Declined, StreamError::Unfunded] {
        let annotated = annotate_unbound_cache_miss(refusal(error.clone()), &ctx_with(None));
        assert!(
            !annotated.to_string().contains("capacity_bond_address"),
            "{error:?} must not get the binding hint"
        );
    }
}

/// A non-`NotFound` failure (e.g. a transport error) is never mislabeled as a
/// missing-binding problem, even when unbound.
#[test]
fn unbound_non_notfound_error_is_untouched() {
    let annotated = annotate_unbound_cache_miss(
        anyhow::anyhow!("connect failed: timed out"),
        &ctx_with(None),
    );
    assert!(!annotated.to_string().contains("capacity_bond_address"));
}

#[test]
fn flags_override_config() {
    let mut c = common();
    c.rpc_url = Some("http://flag:8545".into());
    c.chain_id = Some(99);
    let pp = "0x1111111111111111111111111111111111111111";
    c.payment_pool_address = Some(pp.into());
    c.slash_judge_address = Some("0x2222222222222222222222222222222222222222".into());
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\npayment_pool_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
    );
    let r = resolve_chain(&c, &file).unwrap();
    assert_eq!(r.config.rpc_url, "http://flag:8545");
    assert_eq!(r.config.chain_id, 99);
    assert_eq!(r.config.payment_pool, Address::from_str(pp).unwrap());
}

#[test]
fn config_fills_unset_flags_and_defaults() {
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_pool_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\nbuyer_working_deposit_micro_usdc = 5000000\n",
    );
    let r = resolve_chain(&common(), &file).unwrap();
    assert_eq!(r.config.rpc_url, "http://config:8545");
    // chain_id absent everywhere → default.
    assert_eq!(r.config.chain_id, DEFAULT_CHAIN_ID);
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
    let base = "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_pool_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n";

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
            "decdn pool top-up --pool {pool} --amount-micro-usdc"
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
    .sign(&delegate, &bind_node_id_domain(1, Address::ZERO))
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

#[test]
fn entry_hash_names_the_entry_of_a_malformed_hash() {
    let bytes = manifest_json(&[("a.bin", "not-hex")]);
    let err = format!("{:#}", entry_hash(&bytes, "a.bin").unwrap_err());
    assert!(
        err.contains("bundle entry \"a.bin\" has a malformed hash"),
        "{err}"
    );
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
