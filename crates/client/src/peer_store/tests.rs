use super::*;

fn sample_record() -> PeerRecord {
    // A deterministic PublicKey/Address for tests: generate from a seed.
    let secret = iroh::SecretKey::from_bytes(&[7u8; 32]);
    let node_id = secret.public();
    PeerRecord {
        node_id,
        eth_address: Address::repeat_byte(0xAB),
        region_hint: Region::parse("US"),
        multiaddrs: Bytes::new(),
        identity_seen_at_secs: 1_000,
        latency_ms: None,
        last_sampled_at_secs: None,
        rate_per_mb: None,
        sample_count: 0,
        last_failure_at_secs: None,
    }
}

#[test]
fn first_sample_sets_value_then_ewma_folds() {
    let cfg = StoreConfig::default();
    let mut r = sample_record();
    r.fold_latency(40.0, cfg.ewma_alpha);
    assert_eq!(r.latency_ms, Some(40.0));
    assert_eq!(r.sample_count, 1);
    r.fold_latency(140.0, cfg.ewma_alpha);
    // 0.7*40 + 0.3*140 = 70
    assert!((r.latency_ms.unwrap_or_default() - 70.0).abs() < 1e-9);
    assert_eq!(r.sample_count, 2);
}

#[test]
fn latency_freshness_respects_ttl() {
    let cfg = StoreConfig::default();
    let mut r = sample_record();
    r.last_sampled_at_secs = Some(1_000);
    assert!(r.latency_fresh(1_000 + cfg.latency_ttl_secs, &cfg));
    assert!(!r.latency_fresh(1_000 + cfg.latency_ttl_secs + 1, &cfg));
}

#[test]
fn failure_suppresses_then_expires() {
    let cfg = StoreConfig::default();
    let mut r = sample_record();
    r.latency_ms = Some(20.0);
    r.last_sampled_at_secs = Some(2_000);
    r.last_failure_at_secs = Some(2_000);
    assert!(r.failure_suppressed(2_000, &cfg));
    assert!(!r.selectable(2_000, &cfg));
    let after = 2_000 + cfg.failure_suppress_secs;
    assert!(!r.failure_suppressed(after, &cfg));
    assert!(r.selectable(after, &cfg));
}

#[test]
fn identity_prunable_past_horizon() {
    let cfg = StoreConfig::default();
    let r = sample_record();
    assert!(!r.identity_prunable(1_000 + cfg.identity_prune_secs, &cfg));
    assert!(r.identity_prunable(1_000 + cfg.identity_prune_secs + 1, &cfg));
}

#[test]
fn identity_freshness_respects_refresh_horizon() {
    let cfg = StoreConfig::default();
    let r = sample_record();
    assert!(r.identity_fresh(1_000 + cfg.identity_refresh_secs, &cfg));
    assert!(!r.identity_fresh(1_000 + cfg.identity_refresh_secs + 1, &cfg));
}

use tempfile::tempdir;

fn key(b: u8) -> PublicKey {
    iroh::SecretKey::from_bytes(&[b; 32]).public()
}

fn candidate(b: u8) -> NodeCandidate {
    NodeCandidate {
        node_id: key(b),
        eth_address: Address::repeat_byte(b),
        region_hint: Region::parse("US"),
        multiaddrs: Bytes::new(),
    }
}

#[test]
fn upsert_then_get_roundtrips_identity() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    store.upsert_identity(&candidate(1), 5_000)?;
    let got = store
        .get(&key(1))
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    assert_eq!(got.eth_address, Address::repeat_byte(1));
    assert_eq!(got.identity_seen_at_secs, 5_000);
    assert_eq!(got.latency_ms, None);
    Ok(())
}

fn candidate_with_addr(b: u8, multiaddr: &str) -> anyhow::Result<NodeCandidate> {
    let mut cand = candidate(b);
    cand.multiaddrs = Bytes::from(decdn_incentive::node_register::pack_multiaddrs(&[
        multiaddr.to_string(),
    ])?);
    Ok(cand)
}

#[test]
fn upsert_persists_multiaddrs_and_as_candidate_carries_them() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    let cand = candidate_with_addr(5, "/ip4/203.0.113.10/udp/4433/quic-v1")?;
    store.upsert_identity(&cand, 5_000)?;
    let got = store
        .get(&key(5))
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    assert_eq!(got.multiaddrs, cand.multiaddrs);
    // Projected back for a registry-outage dial, the cached address decodes.
    assert_eq!(
        got.as_candidate().dial_addrs(),
        vec!["203.0.113.10:4433".parse()?]
    );
    Ok(())
}

#[test]
fn record_without_multiaddrs_field_still_deserializes() -> anyhow::Result<()> {
    // A record written before `multiaddrs` existed must still load (with
    // empty addresses) rather than fail and drop its latency/identity stats.
    // Build a real record, strip the field an older writer never wrote, and
    // reload — no hand-guessing of the PublicKey/Address JSON encodings.
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    store.upsert_identity(&candidate(9), 5_000)?;
    let rec = store
        .get(&key(9))
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    let mut value = serde_json::to_value(&rec)?;
    value
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("record is not a JSON object"))?
        .remove("multiaddrs");
    let reloaded: PeerRecord = serde_json::from_value(value)?;
    assert!(reloaded.multiaddrs.is_empty());
    assert_eq!(reloaded.identity_seen_at_secs, 5_000); // unrelated fields survive
    Ok(())
}

#[test]
fn record_sample_preserves_cached_multiaddrs() -> anyhow::Result<()> {
    let cfg = StoreConfig::default();
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    let cand = candidate_with_addr(6, "/ip4/198.51.100.7/udp/5000/quic-v1")?;
    store.upsert_identity(&cand, 5_000)?;
    // A later stats-only update must not clear the cached addresses.
    store.record_sample(&key(6), 30.0, 7, 5_100, &cfg)?;
    let got = store
        .get(&key(6))
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    assert_eq!(got.multiaddrs, cand.multiaddrs);
    Ok(())
}

#[test]
fn sample_preserves_identity_and_folds_latency() -> anyhow::Result<()> {
    let cfg = StoreConfig::default();
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    store.upsert_identity(&candidate(2), 5_000)?;
    store.record_sample(&key(2), 30.0, 7, 5_100, &cfg)?;
    let got = store
        .get(&key(2))
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    assert_eq!(got.latency_ms, Some(30.0));
    assert_eq!(got.rate_per_mb, Some(7));
    assert_eq!(got.last_sampled_at_secs, Some(5_100));
    assert_eq!(got.eth_address, Address::repeat_byte(2)); // identity intact
    Ok(())
}

#[test]
fn failure_then_success_clears_stamp() -> anyhow::Result<()> {
    let cfg = StoreConfig::default();
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    store.upsert_identity(&candidate(3), 5_000)?;
    store.record_failure(&key(3), 6_000)?;
    assert!(
        store
            .get(&key(3))
            .and_then(|r| r.last_failure_at_secs)
            .is_some()
    );
    store.record_sample(&key(3), 25.0, 3, 6_100, &cfg)?;
    assert!(
        store
            .get(&key(3))
            .and_then(|r| r.last_failure_at_secs)
            .is_none()
    );
    Ok(())
}

/// A stream open (#2196) files its quote and clears the failure stamp, but
/// its time-to-first-byte is not a distance measure, so the probe RTT and
/// its freshness clock stay exactly as the last probe left them.
#[test]
fn record_open_files_rate_and_clears_failure_without_touching_latency() -> anyhow::Result<()> {
    let cfg = StoreConfig::default();
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    store.upsert_identity(&candidate(5), 5_000)?;
    store.record_sample(&key(5), 57.0, 3, 5_100, &cfg)?;
    store.record_failure(&key(5), 5_200)?;
    store.record_open(&key(5), 9)?;
    let got = store
        .get(&key(5))
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    assert_eq!(got.rate_per_mb, Some(9));
    assert_eq!(got.last_failure_at_secs, None);
    assert_eq!(got.latency_ms, Some(57.0));
    assert_eq!(got.sample_count, 1);
    assert_eq!(got.last_sampled_at_secs, Some(5_100));
    Ok(())
}

/// Concurrent writers never lose a probe sample: a stream open that races
/// the harvest must not write back a record read before the sample landed.
#[test]
fn concurrent_opens_never_drop_a_probe_sample() -> anyhow::Result<()> {
    const ROUNDS: u32 = 25;
    const WRITERS: u32 = 4;
    let cfg = StoreConfig::default();
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    store.upsert_identity(&candidate(7), 5_000)?;
    std::thread::scope(|scope| {
        for _ in 0..WRITERS {
            scope.spawn(|| {
                for _ in 0..ROUNDS {
                    let _ = store.record_sample(&key(7), 50.0, 1, 5_100, &cfg);
                }
            });
            scope.spawn(|| {
                for _ in 0..ROUNDS {
                    let _ = store.record_open(&key(7), 2);
                }
            });
        }
    });
    let got = store
        .get(&key(7))
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    assert_eq!(got.sample_count, WRITERS * ROUNDS);
    Ok(())
}

#[test]
fn record_open_on_unknown_peer_is_a_noop() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    store.record_open(&key(8), 9)?;
    assert!(store.get(&key(8)).is_none());
    Ok(())
}

#[test]
fn load_all_skips_corrupt_files() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    store.upsert_identity(&candidate(4), 5_000)?;
    std::fs::create_dir_all(dir.path().join("peers"))?;
    std::fs::write(dir.path().join("peers").join("garbage.json"), b"{not json")?;
    let all = store.load_all();
    assert_eq!(all.len(), 1);
    Ok(())
}

#[test]
fn prune_drops_very_stale_identity_keeps_fresh() -> anyhow::Result<()> {
    let cfg = StoreConfig::default();
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    store.upsert_identity(&candidate(1), 0)?; // ancient identity
    store.upsert_identity(&candidate(2), 1_000_000)?; // fresh identity
    let now = 1_000_000 + cfg.identity_prune_secs; // key(1) is prunable, key(2) is not
    store.prune_and_cap(now, &cfg)?;
    assert!(store.get(&key(1)).is_none());
    assert!(store.get(&key(2)).is_some());
    Ok(())
}

#[test]
fn successive_samples_fold_by_ewma() -> anyhow::Result<()> {
    let cfg = StoreConfig::default();
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    store.upsert_identity(&candidate(1), 1_000)?;
    store.record_sample(&key(1), 200.0, 5, 1_000, &cfg)?;
    store.record_sample(&key(1), 20.0, 5, 1_100, &cfg)?;
    let r = store
        .get(&key(1))
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    // EWMA: 0.7*200 + 0.3*20 = 146.
    assert!((r.latency_ms.unwrap_or_default() - 146.0).abs() < 1e-9);
    assert_eq!(r.sample_count, 2);
    Ok(())
}

#[test]
fn cap_evicts_least_recently_sampled() -> anyhow::Result<()> {
    let cfg = StoreConfig {
        lru_cap: 2,
        ..Default::default()
    };
    let dir = tempdir()?;
    let store = PeerStore::open(dir.path());
    for b in 1u8..=3 {
        store.upsert_identity(&candidate(b), 1_000_000)?;
        store.record_sample(&key(b), 10.0, 1, 1_000_000 + u64::from(b), &cfg)?;
    }
    store.prune_and_cap(1_000_100, &cfg)?;
    assert_eq!(store.load_all().len(), 2);
    assert!(store.get(&key(1)).is_none()); // oldest sample evicted
    Ok(())
}
