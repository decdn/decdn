# Per-region bandwidth accounting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give operators a per-region bytes-in / bytes-out view of traffic, keyed by the counterparty peer's self-attested `NodeAnnounce.region`, exposed as a periodic structured log and an admin RPC + CLI command (#750).

**Architecture:** A `RegionAccountant` in the `node` crate holds an in-memory `region → {bytes_in, bytes_out}` map. The paid-delivery handler records bytes served OUT at each accepted voucher; region is resolved from the gossip peer table behind a `RegionResolver` trait (production impl reads the table; tests inject a stub). A periodic runtime task logs cumulative totals; `AdminRpc::region_stats` + `decdn node region-stats` expose the same snapshot. `bytes_in` is fully modeled and exposed but has no production caller yet (node-to-node pull-through is not orchestrated) — `record_pulled` is the documented seam.

**Tech Stack:** Rust (edition 2024, MSRV 1.95), tokio, `async-trait` (already in the lockfile at 0.1.89), `iroh_metrics` (unchanged), jsonrpsee (admin RPC), serde.

**Spec:** `docs/superpowers/specs/2026-06-05-per-region-bandwidth-accounting-design.md`

**Conventions (read before starting):**

- Anti-panic policy is clippy-enforced workspace-wide: no `unwrap`/`expect`/`panic`/indexing. Use combinators, `.get()`, and `saturating_*`. Poisoned locks must be handled (skip + `tracing::warn!`), never `unwrap`ed.
- `rustfmt.toml` sets `max_width = 100`.
- Build + lint with `cargo build && cargo clippy`; test with `cargo nextest run -p <crate>`. Clippy is the usual CI failure.
- Commit messages are conventional-commits; end each with the trailer shown in the commit steps.

---

## File structure

| File | Responsibility | Change |
|------|----------------|--------|
| `crates/common/src/admin.rs` | shared admin wire types + `AdminRpc` trait | add `RegionBytes`, `RegionStatsResponse`, `region_stats` method |
| `crates/node/src/region_accounting.rs` | region resolution + in-memory accumulator | **new** |
| `crates/node/src/lib.rs` | crate module list | expose `region_accounting` |
| `crates/node/Cargo.toml` | node deps | add `async-trait` |
| `Cargo.toml` (workspace root) | workspace deps | add `async-trait` |
| `crates/node/src/handlers/client.rs` | paid-delivery handler | accountant field + attach + `record_served` call |
| `crates/common/src/config/types.rs` | raw TOML config | add `region_accounting_interval_sec` |
| `crates/common/src/config/resolved.rs` | resolved config | add `region_accounting_interval_sec` |
| `crates/common/src/config/mod.rs` | config resolver + default const | resolve the new field |
| `crates/node/src/runtime/mod.rs` | runtime wiring | build accountant, attach, spawn log task, drain stop |
| `crates/node/src/admin.rs` | admin RPC server impl | `with_region_accountant` builder + `region_stats` impl |
| `crates/common/src/cli/node.rs` | CLI arg/command definitions | `RegionStatsArgs` + `NodeCommand::RegionStats` |
| `crates/cli/src/commands/node.rs` | CLI command handlers | `region_stats` fn + `write_region_stats_table` |

Tasks are ordered so each builds only on earlier ones: DTOs → accountant → handler wiring → config+runtime → admin RPC → CLI.

---

### Task 1: Admin DTOs + `region_stats` trait method (`common`)

**Files:**

- Modify: `crates/common/src/admin.rs`

- [ ] **Step 1: Write the failing round-trip test**

Add to the `#[cfg(test)] mod tests` block in `crates/common/src/admin.rs` (next to `channels_response_round_trips`):

```rust
#[test]
fn region_stats_response_round_trips() {
    let resp = RegionStatsResponse {
        regions: vec![
            RegionBytes {
                region: "DE".to_string(),
                bytes_in: 1_048_576,
                bytes_out: 5_242_880,
            },
            RegionBytes {
                region: "UNKNOWN".to_string(),
                bytes_in: 0,
                bytes_out: 2_097_152,
            },
        ],
    };
    let json = serde_json::to_string(&resp).expect("serialize RegionStatsResponse");
    let back: RegionStatsResponse =
        serde_json::from_str(&json).expect("deserialize RegionStatsResponse");
    assert_eq!(back.regions.len(), 2);
    let first = back.regions.first().expect("first region");
    assert_eq!(first.region, "DE");
    assert_eq!(first.bytes_in, 1_048_576);
    assert_eq!(first.bytes_out, 5_242_880);
}

#[test]
fn region_stats_response_legacy_shape_defaults_zero() {
    // An older/partial server that omits the byte fields must still
    // deserialize, with the missing counters defaulting to zero.
    let json = r#"{"regions":[{"region":"FR"}]}"#;
    let back: RegionStatsResponse =
        serde_json::from_str(json).expect("deserialize partial RegionStatsResponse");
    let only = back.regions.first().expect("one region");
    assert_eq!(only.region, "FR");
    assert_eq!(only.bytes_in, 0);
    assert_eq!(only.bytes_out, 0);
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `cargo nextest run -p decdn-common region_stats_response 2>&1 | tail -20`
Expected: compile error — `RegionStatsResponse` / `RegionBytes` not found.

- [ ] **Step 3: Add the DTOs**

Add near the other response DTOs in `crates/common/src/admin.rs` (e.g. just after the `ChannelSnapshot` struct). Match the existing `#[derive(...)]` attributes the other DTOs use in this file (`Debug, Clone, Serialize, Deserialize`, plus `PartialEq` if the surrounding DTOs carry it):

```rust
/// One region's cumulative byte counters (issue #750). `region` is an
/// ISO 3166-1 alpha-2 code (the peer's self-attested `NodeAnnounce.region`,
/// ADR 030) or the literal `"UNKNOWN"` bucket for traffic whose counterparty
/// has no known region (a non-peer end-client, or a peer not in the table).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionBytes {
    /// ISO 3166-1 alpha-2 region code, or `"UNKNOWN"`.
    pub region: String,
    /// Cumulative bytes this node has *pulled from* counterparties in this
    /// region since process start. Reads `0` until node-to-node pull-through
    /// is orchestrated (no production caller of `record_pulled` yet, #750).
    /// `#[serde(default)]` keeps older servers that omit the field round-tripping.
    #[serde(default)]
    pub bytes_in: u64,
    /// Cumulative bytes this node has *served to* counterparties in this
    /// region since process start.
    #[serde(default)]
    pub bytes_out: u64,
}

/// Response for `admin_v1_regionStats` (issue #750): per-region cumulative
/// bytes-in / bytes-out, region-sorted. Empty when no accountant is wired.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionStatsResponse {
    /// One entry per region observed since process start, sorted by region code.
    pub regions: Vec<RegionBytes>,
}
```

- [ ] **Step 4: Add the trait method**

In the `#[rpc(server, client, namespace = "admin_v1")] pub trait AdminRpc` block, add after the `channels` method:

```rust
    /// Return cumulative per-region bandwidth (issue #750): bytes served to
    /// and pulled from each region, keyed by the counterparty peer's
    /// self-attested `NodeAnnounce.region` (ADR 030), with a `"UNKNOWN"`
    /// bucket for unattributable traffic. Totals are cumulative since process
    /// start. Backs `decdn node region-stats`. Returns an empty list (not an
    /// error) on a node with no accounting wired.
    #[method(name = "regionStats")]
    async fn region_stats(&self) -> RpcResult<RegionStatsResponse>;
```

This change makes every `impl AdminRpc` incomplete — the node impl is added in Task 5. To keep the workspace compiling between tasks, this task only needs `decdn-common` to compile (the trait + DTOs). The `decdn-node` impl gap is filled in Task 5; do not build `-p decdn-node` at the end of this task.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo nextest run -p decdn-common region_stats 2>&1 | tail -20`
Expected: both tests PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/common/src/admin.rs
git commit -m "feat(common): RegionStats admin DTOs + region_stats RPC method (#750)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `RegionAccountant` + `RegionResolver` module (`node`)

**Files:**

- Create: `crates/node/src/region_accounting.rs`
- Modify: `crates/node/src/lib.rs`, `crates/node/Cargo.toml`, `Cargo.toml` (workspace root)

- [ ] **Step 1: Add the `async-trait` dependency**

In the workspace root `Cargo.toml` under `[workspace.dependencies]`, add (keep the list alphabetically ordered if it is):

```toml
async-trait = "0.1"
```

In `crates/node/Cargo.toml` under `[dependencies]`, add:

```toml
async-trait = { workspace = true }
```

- [ ] **Step 2: Create the module with failing tests**

Create `crates/node/src/region_accounting.rs`:

```rust
//! Per-region bandwidth accounting (#750).
//!
//! Aggregates bytes served (and, via a documented seam, pulled) keyed by the
//! counterparty peer's self-attested `NodeAnnounce.region` (ADR 030). Region
//! is resolved through [`RegionResolver`] — the production impl reads the
//! gossip peer table; tests inject a stub. Totals are cumulative since process
//! start (Prometheus-counter semantics) and live only in memory.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use decdn_common::RegionBytes;
use decdn_gossip::PeerTable;
use tokio::sync::RwLock;

/// Region bucket for traffic whose counterparty has no known region — a
/// non-peer end-client, or a peer not currently in the gossip peer table.
pub const UNKNOWN_REGION: &str = "UNKNOWN";

/// Resolve an iroh node id (32 raw bytes) to its self-attested region.
///
/// Async because the production impl reads the `tokio::sync::RwLock`-guarded
/// peer table. Boxed via `async-trait` so the accountant can hold an
/// `Arc<dyn RegionResolver>` and tests can inject a stub.
#[async_trait]
pub trait RegionResolver: Send + Sync {
    /// The peer's region code, or `None` if the peer is unknown.
    async fn region_of(&self, node_id: &[u8; 32]) -> Option<String>;
}

/// Production resolver: looks the node id up in the shared gossip peer table.
pub struct PeerTableResolver(Arc<RwLock<PeerTable>>);

impl PeerTableResolver {
    #[must_use]
    pub fn new(peer_table: Arc<RwLock<PeerTable>>) -> Self {
        Self(peer_table)
    }
}

#[async_trait]
impl RegionResolver for PeerTableResolver {
    async fn region_of(&self, node_id: &[u8; 32]) -> Option<String> {
        let guard = self.0.read().await;
        guard.get(node_id).map(|e| e.announce.region.clone())
    }
}

/// Cumulative byte counters for one region.
#[derive(Debug, Default, Clone, Copy)]
struct RegionTotals {
    bytes_in: u64,
    bytes_out: u64,
}

/// In-memory per-region byte accumulator.
#[derive(Debug)]
pub struct RegionAccountant {
    resolver: Arc<dyn RegionResolver>,
    totals: Mutex<HashMap<String, RegionTotals>>,
}

impl std::fmt::Debug for dyn RegionResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<dyn RegionResolver>")
    }
}

impl RegionAccountant {
    #[must_use]
    pub fn new(resolver: Arc<dyn RegionResolver>) -> Self {
        Self {
            resolver,
            totals: Mutex::new(HashMap::new()),
        }
    }

    /// Record `bytes` served OUT to `peer`, bucketed by `peer`'s region (or
    /// [`UNKNOWN_REGION`]).
    pub async fn record_served(&self, peer: &[u8; 32], bytes: u64) {
        let region = self.region_for(peer).await;
        self.add(region, 0, bytes);
    }

    /// Record `bytes` pulled IN from `peer`, bucketed by `peer`'s region.
    ///
    /// **Forward-compatible seam (#750):** node-to-node paid pull-through is
    /// not orchestrated in production yet (the serving handler returns
    /// `NotFound` on a local miss; cache pull-through targets opaque S3/R2
    /// origins with no region). The future pull orchestrator calls this; until
    /// then `bytes_in` reads `0` in production.
    pub async fn record_pulled(&self, peer: &[u8; 32], bytes: u64) {
        let region = self.region_for(peer).await;
        self.add(region, bytes, 0);
    }

    /// Region-sorted snapshot of all buckets.
    #[must_use]
    pub fn snapshot(&self) -> Vec<RegionBytes> {
        let Ok(totals) = self.totals.lock() else {
            tracing::warn!("region accountant totals lock poisoned; reporting empty snapshot");
            return Vec::new();
        };
        let mut out: Vec<RegionBytes> = totals
            .iter()
            .map(|(region, t)| RegionBytes {
                region: region.clone(),
                bytes_in: t.bytes_in,
                bytes_out: t.bytes_out,
            })
            .collect();
        out.sort_by(|a, b| a.region.cmp(&b.region));
        out
    }

    async fn region_for(&self, peer: &[u8; 32]) -> String {
        self.resolver
            .region_of(peer)
            .await
            .unwrap_or_else(|| UNKNOWN_REGION.to_string())
    }

    fn add(&self, region: String, bytes_in: u64, bytes_out: u64) {
        let Ok(mut totals) = self.totals.lock() else {
            tracing::warn!(%region, "region accountant totals lock poisoned; dropping update");
            return;
        };
        let entry = totals.entry(region).or_default();
        entry.bytes_in = entry.bytes_in.saturating_add(bytes_in);
        entry.bytes_out = entry.bytes_out.saturating_add(bytes_out);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Stub resolver backed by a fixed node-id → region map.
    struct StubResolver(HashMap<[u8; 32], String>);

    #[async_trait]
    impl RegionResolver for StubResolver {
        async fn region_of(&self, node_id: &[u8; 32]) -> Option<String> {
            self.0.get(node_id).cloned()
        }
    }

    fn accountant_with(map: HashMap<[u8; 32], String>) -> RegionAccountant {
        RegionAccountant::new(Arc::new(StubResolver(map)))
    }

    #[tokio::test]
    async fn served_bytes_bucket_by_resolved_region() {
        let peer = [1u8; 32];
        let mut map = HashMap::new();
        map.insert(peer, "DE".to_string());
        let acc = accountant_with(map);

        acc.record_served(&peer, 1000).await;
        acc.record_served(&peer, 500).await;

        let snap = acc.snapshot();
        assert_eq!(snap.len(), 1);
        let de = snap.first().unwrap();
        assert_eq!(de.region, "DE");
        assert_eq!(de.bytes_out, 1500);
        assert_eq!(de.bytes_in, 0);
    }

    #[tokio::test]
    async fn unknown_peer_lands_in_unknown_bucket() {
        let acc = accountant_with(HashMap::new());
        acc.record_served(&[9u8; 32], 42).await;

        let snap = acc.snapshot();
        let only = snap.first().unwrap();
        assert_eq!(only.region, UNKNOWN_REGION);
        assert_eq!(only.bytes_out, 42);
    }

    #[tokio::test]
    async fn pulled_bytes_accumulate_into_bytes_in() {
        let peer = [2u8; 32];
        let mut map = HashMap::new();
        map.insert(peer, "FR".to_string());
        let acc = accountant_with(map);

        acc.record_pulled(&peer, 700).await;
        acc.record_served(&peer, 300).await;

        let only = acc.snapshot().into_iter().next().unwrap();
        assert_eq!(only.region, "FR");
        assert_eq!(only.bytes_in, 700);
        assert_eq!(only.bytes_out, 300);
    }

    #[tokio::test]
    async fn snapshot_is_region_sorted() {
        let (a, b, c) = ([1u8; 32], [2u8; 32], [3u8; 32]);
        let mut map = HashMap::new();
        map.insert(a, "US".to_string());
        map.insert(b, "DE".to_string());
        map.insert(c, "FR".to_string());
        let acc = accountant_with(map);
        acc.record_served(&a, 1).await;
        acc.record_served(&b, 1).await;
        acc.record_served(&c, 1).await;

        let regions: Vec<String> = acc.snapshot().into_iter().map(|r| r.region).collect();
        assert_eq!(regions, vec!["DE", "FR", "US"]);
    }

    #[tokio::test]
    async fn served_bytes_saturate_at_u64_max() {
        let peer = [7u8; 32];
        let mut map = HashMap::new();
        map.insert(peer, "DE".to_string());
        let acc = accountant_with(map);

        acc.record_served(&peer, u64::MAX).await;
        acc.record_served(&peer, 1).await; // would overflow → must saturate

        assert_eq!(acc.snapshot().first().unwrap().bytes_out, u64::MAX);
    }
}
```

> Note on `impl std::fmt::Debug for dyn RegionResolver`: `RegionAccountant` derives `Debug`, but `Arc<dyn RegionResolver>` has no `Debug` bound. The hand-rolled impl above satisfies the derive. If clippy/orphan rules object, instead drop `#[derive(Debug)]` on `RegionAccountant` and hand-write a terse `impl Debug` that prints only the struct name (mirroring `ChannelStatusHandles` in `admin.rs`). Either is fine; pick whichever compiles cleanly.

- [ ] **Step 3: Expose the module**

In `crates/node/src/lib.rs`, add the module declaration alongside the other `pub mod`/`mod` entries (e.g. near `pub mod client_requester;`). It must be `pub` so the runtime, handler, and admin modules — and integration tests — can reach `RegionAccountant`:

```rust
pub mod region_accounting;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo nextest run -p decdn-node region_accounting 2>&1 | tail -30`
Expected: all five tests PASS.

- [ ] **Step 5: Lint**

Run: `cargo clippy -p decdn-node 2>&1 | tail -20`
Expected: no warnings (clippy is the usual CI failure — fix any anti-panic/`Debug` issues here).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/node/Cargo.toml crates/node/src/region_accounting.rs crates/node/src/lib.rs
git commit -m "feat(node): RegionAccountant + peer-table region resolver (#750)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Wire bytes-out into the paid-delivery handler (`node`)

**Files:**

- Modify: `crates/node/src/handlers/client.rs`
- Test: `crates/node/tests/client_loopback.rs`

- [ ] **Step 1: Write the failing integration test**

Add to `crates/node/tests/client_loopback.rs`, modeled exactly on the existing
`accepted_voucher_advances_shared_activity_clock` test (same helpers:
`fresh_key`, `local_endpoint`, `spawn_server`, `cache_with_blob`,
`MemoryChannelStateStore`, `permissive_limiter`, `build_handler`,
`channel_context`, `channel_id`, `TOKEN`, `RATE_PER_MB`, `slash_domain`).
Add the imports `use std::collections::HashMap;`,
`use std::sync::Arc;` (likely already present), and
`use decdn_node::region_accounting::{RegionAccountant, RegionResolver, UNKNOWN_REGION};`
plus `use async_trait::async_trait;` at the top of the file if not present.

```rust
/// Wiring guard for per-region accounting (#750): a real signed-voucher accept
/// through the live `ClientHandler` must record the served bytes against the
/// resolving region. The `record_served` call site (`handlers/client.rs`) has
/// no other test caller — a dropped or mis-placed call would silently leave
/// every region at zero. We attach an accountant whose stub resolver maps the
/// *client's* node id to "DE", drive one successful delivery, and assert "DE"
/// now carries the delivered bytes as bytes_out.
#[tokio::test(flavor = "multi_thread")]
async fn accepted_voucher_records_served_bytes_by_region() -> anyhow::Result<()> {
    /// Stub resolver: client node id → fixed region.
    struct OneRegion {
        node_id: [u8; 32],
        region: String,
    }
    #[async_trait]
    impl RegionResolver for OneRegion {
        async fn region_of(&self, node_id: &[u8; 32]) -> Option<String> {
            (node_id == &self.node_id).then(|| self.region.clone())
        }
    }

    let payload = vec![0xABu8; 1_572_864]; // 1.5 MiB → crosses a voucher interval
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
        TOKEN,
        deposit,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn ChannelStateStore> = store.clone();
    let handler = build_handler(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
    )?;

    // The client endpoint's key is what the handler sees as `client_node_id`.
    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let accountant = Arc::new(RegionAccountant::new(Arc::new(OneRegion {
        node_id: *client_id.as_bytes(),
        region: "DE".to_string(),
    })));
    handler.attach_region_accountant(Arc::clone(&accountant));
    assert!(
        accountant.snapshot().is_empty(),
        "no delivery yet → no region buckets"
    );

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let ctx = channel_context(Arc::clone(&client_signer), deposit);

    let got = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await?;
    anyhow::ensure!(got.as_ref() == payload.as_slice(), "delivered bytes mismatch");

    let snap = accountant.snapshot();
    let de = snap
        .iter()
        .find(|r| r.region == "DE")
        .ok_or_else(|| anyhow::anyhow!("expected a DE bucket, got {snap:?}"))?;
    anyhow::ensure!(
        de.bytes_out == payload.len() as u64,
        "DE bytes_out = {}, expected {}",
        de.bytes_out,
        payload.len()
    );
    anyhow::ensure!(de.bytes_in == 0, "bytes_in must stay 0 (no pull path)");
    anyhow::ensure!(
        snap.iter().all(|r| r.region != UNKNOWN_REGION),
        "resolved client must not fall through to UNKNOWN"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `cargo nextest run -p decdn-node accepted_voucher_records_served_bytes_by_region 2>&1 | tail -20`
Expected: compile error — `attach_region_accountant` not found on `ClientHandler`.

- [ ] **Step 3: Add the handler field + attach method**

In `crates/node/src/handlers/client.rs`:

Add the import near the other `crate::` imports:

```rust
use crate::region_accounting::RegionAccountant;
```

Add a field to `struct ClientHandler` (next to `voucher_activity`):

```rust
    /// Per-region bandwidth accountant (issue #750), attached post-construction
    /// via [`ClientHandler::attach_region_accountant`]. `None` until attached
    /// (tests / no admin surface) — recording is best-effort, so an unattached
    /// handler simply skips it.
    region_accountant: OnceLock<Arc<RegionAccountant>>,
```

Initialize it in `ClientHandler::new` (in the struct literal, next to
`voucher_activity: OnceLock::new(),`):

```rust
            region_accountant: OnceLock::new(),
```

Add the attach method next to `attach_voucher_activity`:

```rust
    /// Attach the per-region bandwidth accountant (issue #750). Called once
    /// during runtime wiring; a second call is ignored (the `OnceLock` keeps
    /// the first). After this, each accepted voucher records the delivered
    /// bytes against the paying peer's region.
    pub fn attach_region_accountant(&self, accountant: Arc<RegionAccountant>) {
        let _ = self.region_accountant.set(accountant);
    }
```

- [ ] **Step 4: Record served bytes at voucher acceptance**

In `collect_voucher`, in the `Ok(applied)` arm, immediately after the existing
`self.record_receipt(hash, delta_bytes, client_node_id, wire.nonce).await;`
line, add:

```rust
                // Per-region bandwidth accounting (#750). Best-effort: an
                // unattached accountant (tests / no admin surface) skips.
                // `delta_bytes` is exactly the bytes paid for this interval.
                if let Some(acc) = self.region_accountant.get() {
                    acc.record_served(&client_node_id.0, delta_bytes).await;
                }
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo nextest run -p decdn-node accepted_voucher_records_served_bytes_by_region 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 6: Run the full handler/loopback test set + clippy to check no regressions**

Run: `cargo nextest run -p decdn-node client_loopback 2>&1 | tail -20 && cargo clippy -p decdn-node 2>&1 | tail -10`
Expected: all loopback tests PASS, no clippy warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/node/src/handlers/client.rs crates/node/tests/client_loopback.rs
git commit -m "feat(node): record served bytes per region on voucher accept (#750)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Config field + runtime wiring + periodic log task (`common` + `node`)

**Files:**

- Modify: `crates/common/src/config/types.rs`, `crates/common/src/config/resolved.rs`, `crates/common/src/config/mod.rs`, `crates/node/src/runtime/mod.rs`

- [ ] **Step 1: Add the raw config field**

In `crates/common/src/config/types.rs`, add to `struct ObservabilityConfig` (after `otlp_endpoint`):

```rust
    /// Interval in seconds between per-region bandwidth accounting log lines
    /// (issue #750). `0` disables the periodic log. Absent → default
    /// `DEFAULT_REGION_ACCOUNTING_INTERVAL_SEC` (3600).
    pub region_accounting_interval_sec: Option<u64>,
```

- [ ] **Step 2: Add the resolved config field**

In `crates/common/src/config/resolved.rs`, add to `struct ResolvedObservability`
(after `otlp_endpoint`):

```rust
    /// Interval in seconds for the per-region bandwidth accounting log
    /// (issue #750). `0` disables the periodic log.
    pub region_accounting_interval_sec: u64,
```

- [ ] **Step 3: Add the default const + resolve the field**

In `crates/common/src/config/mod.rs`, add a default constant next to
`DEFAULT_ADMIN_PORT`:

```rust
/// Default interval (seconds) for the per-region bandwidth accounting log
/// (issue #750). One hour — the log is one snapshot line per region per tick.
pub const DEFAULT_REGION_ACCOUNTING_INTERVAL_SEC: u64 = 3600;
```

In `resolve_observability_into`, add resolution (file-only; there is no CLI arg
for this knob) before the final `ResolvedObservability { ... }` literal:

```rust
    let region_accounting_interval_sec = file
        .and_then(|o| o.region_accounting_interval_sec)
        .unwrap_or(DEFAULT_REGION_ACCOUNTING_INTERVAL_SEC);
```

Add `region_accounting_interval_sec,` to that `ResolvedObservability { ... }`
literal.

- [ ] **Step 4: Update every other `ResolvedObservability { ... }` literal**

These will now fail to compile (missing field). Find them all:

Run: `rg -n 'ResolvedObservability \{' crates/common/src/`

Add `region_accounting_interval_sec: DEFAULT_REGION_ACCOUNTING_INTERVAL_SEC,`
(or `0` where a test wants the log disabled) to each — at minimum the test
helpers `obs` and `obs_with_admin` near the bottom of
`crates/common/src/config/mod.rs`. Use `DEFAULT_REGION_ACCOUNTING_INTERVAL_SEC`
for those two.

- [ ] **Step 5: Add a resolver default test**

Add to the config tests in `crates/common/src/config/mod.rs` (near
`resolve_observability_defaults_admin_port_to_9191`):

```rust
#[test]
fn resolve_observability_defaults_region_accounting_interval() -> anyhow::Result<()> {
    let obs = resolve_observability(&obs_cli(None, None), None)?;
    assert_eq!(
        obs.region_accounting_interval_sec,
        DEFAULT_REGION_ACCOUNTING_INTERVAL_SEC
    );
    Ok(())
}
```

(`obs_cli` is the existing CLI-args test helper used by the adjacent admin-port
test; reuse it verbatim.)

- [ ] **Step 6: Run the config tests + build common**

Run: `cargo nextest run -p decdn-common config 2>&1 | tail -20 && cargo build -p decdn-common 2>&1 | tail -5`
Expected: PASS, common builds.

- [ ] **Step 7: Write the failing log-task shutdown test**

In `crates/node/src/runtime/mod.rs`, in the `#[cfg(test)] mod tests` block,
add a test mirroring the existing `run_dispatch_gc` shutdown-promptness test
(search the file for `stop_tx.send(()).expect("receiver still alive")` to find
the pattern around line ~2200):

```rust
    #[tokio::test]
    async fn region_accounting_log_stops_promptly_on_signal() {
        use std::collections::HashMap;
        use std::sync::Arc;

        use crate::region_accounting::{RegionAccountant, RegionResolver};

        struct NoRegions;
        #[async_trait::async_trait]
        impl RegionResolver for NoRegions {
            async fn region_of(&self, _node_id: &[u8; 32]) -> Option<String> {
                None
            }
        }
        let _ = HashMap::<u8, u8>::new(); // silence unused import if not needed

        let accountant = Arc::new(RegionAccountant::new(Arc::new(NoRegions)));
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        // A long interval so the test can only finish via the stop signal,
        // not a tick — same assertion shape as the dispatch-GC test.
        let handle = tokio::spawn(run_region_accounting_log(
            accountant,
            stop_rx,
            Duration::from_secs(3600),
        ));
        stop_tx.send(()).expect("receiver still alive");
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("log task did not stop within 5s")
            .expect("log task panicked");
    }
```

(Remove the `HashMap` no-op line if it triggers an unused-import lint; it's only
there as a guard — prefer deleting it.)

- [ ] **Step 8: Run to verify it fails to compile**

Run: `cargo nextest run -p decdn-node region_accounting_log_stops_promptly 2>&1 | tail -20`
Expected: compile error — `run_region_accounting_log` not found.

- [ ] **Step 9: Implement the periodic log task**

In `crates/node/src/runtime/mod.rs`, add near `run_dispatch_gc`:

```rust
/// Periodic per-region bandwidth accounting log (#750). Mirrors
/// [`run_dispatch_gc`]'s shutdown shape: stops on its own oneshot at a clean
/// await boundary so a regression that ignores `stop_rx` is directly testable.
/// The first tick is burned so the first line lands one interval after startup
/// (there is nothing to report at t=0). Emits one structured line per region;
/// totals are cumulative since process start.
async fn run_region_accounting_log(
    accountant: Arc<crate::region_accounting::RegionAccountant>,
    mut stop_rx: oneshot::Receiver<()>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = &mut stop_rx => {
                tracing::debug!("region accounting log shutdown signal received");
                return;
            }
            _ = ticker.tick() => {
                for r in accountant.snapshot() {
                    tracing::info!(
                        event = "region_bandwidth",
                        region = %r.region,
                        bytes_in = r.bytes_in,
                        bytes_out = r.bytes_out,
                        "per-region bandwidth (cumulative since start)"
                    );
                }
            }
        }
    }
}
```

- [ ] **Step 10: Run the log-task test to verify it passes**

Run: `cargo nextest run -p decdn-node region_accounting_log_stops_promptly 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 11: Wire the accountant into the runtime**

In `crates/node/src/runtime/mod.rs`, in `run(...)`:

(a) Build the accountant right after the `peer_table` is constructed (search for
`let peer_table = Arc::new(RwLock::new(PeerTable::new(`):

```rust
    // Per-region bandwidth accountant (#750). Resolves regions from the shared
    // peer table; shared (via Arc) with the client handler (records served
    // bytes) and the admin surface (reads the snapshot).
    let region_accountant = Arc::new(crate::region_accounting::RegionAccountant::new(
        Arc::new(crate::region_accounting::PeerTableResolver::new(Arc::clone(
            &peer_table,
        ))),
    ));
```

(b) Attach it to the client handler. Find the existing
`client_handler.attach_voucher_activity(Arc::clone(&voucher_activity));` line
(~712) and add immediately after:

```rust
    client_handler.attach_region_accountant(Arc::clone(&region_accountant));
```

Note: `region_accountant` is declared after the `client_handler` block in the
current file order (peer_table is built ~line 942, the handler ~653). Move the
accountant construction to **before** the `client_handler.attach_*` calls — i.e.
build `peer_table` earlier, or build the accountant from `peer_table` and attach
in the same region where the other `attach_*` calls happen. Concretely: relocate
the `region_accountant` construction (step 11a) to just above the
`attach_voucher_activity` call, constructing the `PeerTableResolver` from the
already-built `peer_table`. If `peer_table` is constructed *after* the handler
attaches, instead move the `peer_table` construction up to before the handler
wiring — `PeerTable::new` has no dependency on the handler. Verify ordering
compiles; the only constraint is `peer_table` exists before the accountant, and
the accountant exists before `attach_region_accountant`.

(c) Spawn the log task with the other periodic tasks (near the
`run_dispatch_gc` spawn, ~815), guarded by the config interval:

```rust
    // Periodic per-region bandwidth log (#750). `interval == 0` disables it,
    // mirroring the RPC watchdog's opt-out. Same oneshot-stop shape as the GCs.
    let region_log_stop_tx = if cfg.observability.region_accounting_interval_sec > 0 {
        let (tx, rx) = oneshot::channel::<()>();
        tasks.spawn(run_region_accounting_log(
            Arc::clone(&region_accountant),
            rx,
            Duration::from_secs(cfg.observability.region_accounting_interval_sec),
        ));
        Some(tx)
    } else {
        tracing::info!(
            "region accounting log disabled (observability.region_accounting_interval_sec = 0)"
        );
        None
    };
```

(d) Fire the stop sender in the drain sequence. Find the block firing the GC
stop senders (search for `let _ = dispatch_gc_stop_tx.send(());`, ~1134) and add:

```rust
    if let Some(tx) = region_log_stop_tx {
        let _ = tx.send(());
    }
```

- [ ] **Step 12: Build, test, lint the whole node + common**

Run: `cargo build -p decdn-node -p decdn-common 2>&1 | tail -10 && cargo clippy -p decdn-node -p decdn-common 2>&1 | tail -10 && cargo nextest run -p decdn-node -p decdn-common 2>&1 | tail -20`
Expected: builds clean, no clippy warnings, all tests PASS.

- [ ] **Step 13: Commit**

```bash
git add crates/common/src/config crates/node/src/runtime/mod.rs
git commit -m "feat(node): periodic per-region bandwidth log + config knob (#750)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Admin RPC server impl (`node`)

**Files:**

- Modify: `crates/node/src/admin.rs`

- [ ] **Step 1: Write the failing tests**

In `crates/node/src/admin.rs` `#[cfg(test)] mod tests`, add (mirroring
`channels_without_handles_returns_empty`). The helper `state_with(...)` / the
`AdminRpcImpl::new(state)` pattern already exist in this module — reuse them.

```rust
    #[tokio::test]
    async fn region_stats_without_accountant_returns_empty() {
        let (state, _tmp) = state_with(vec![]).await;
        let rpc = AdminRpcImpl::new(state);
        let resp = rpc.region_stats().await.expect("region_stats ok");
        assert!(resp.regions.is_empty());
    }

    #[tokio::test]
    async fn region_stats_reports_attached_accountant_snapshot() {
        use crate::region_accounting::{RegionAccountant, RegionResolver};

        struct Fixed(String);
        #[async_trait]
        impl RegionResolver for Fixed {
            async fn region_of(&self, _node_id: &[u8; 32]) -> Option<String> {
                Some(self.0.clone())
            }
        }

        let accountant = Arc::new(RegionAccountant::new(Arc::new(Fixed("DE".to_string()))));
        accountant.record_served(&[1u8; 32], 4096).await;

        let (state, _tmp) = state_with(vec![]).await;
        let state = state.with_region_accountant(Arc::clone(&accountant));
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc.region_stats().await.expect("region_stats ok");
        let de = resp
            .regions
            .iter()
            .find(|r| r.region == "DE")
            .expect("DE bucket present");
        assert_eq!(de.bytes_out, 4096);
    }
```

(If `state_with` returns just `state` rather than `(state, _tmp)`, match the
local convention — check an adjacent test. The `#[async_trait]` in scope is the
`jsonrpsee::core::async_trait` already imported at the top of `admin.rs`; it
works for the stub impl too.)

- [ ] **Step 2: Run to verify it fails to compile**

Run: `cargo nextest run -p decdn-node region_stats_without_accountant 2>&1 | tail -20`
Expected: compile error — `with_region_accountant` / `region_stats` missing.

- [ ] **Step 3: Add the field + builder + import**

In `crates/node/src/admin.rs`:

Add the import:

```rust
use crate::region_accounting::RegionAccountant;
```

Add a field to `struct AdminState` (next to `channels: Option<ChannelStatusHandles>,`):

```rust
    /// Per-region bandwidth accountant (issue #750), attached via
    /// [`AdminState::with_region_accountant`]. `None` → `region_stats` returns
    /// an empty list (a node with no accounting wired has nothing to report).
    region_accountant: Option<Arc<RegionAccountant>>,
```

Initialize it as `None` in `AdminState::new`'s struct literal (next to
`channels: None,`):

```rust
            region_accountant: None,
```

Add the builder next to `with_channels` / `with_dht`:

```rust
    /// Attach the per-region bandwidth accountant so `admin_v1_regionStats`
    /// can report its snapshot (issue #750). The production runtime calls this
    /// once after `new`; without it, `region_stats` returns an empty list.
    #[must_use]
    pub fn with_region_accountant(mut self, accountant: Arc<RegionAccountant>) -> Self {
        self.region_accountant = Some(accountant);
        self
    }
```

`AdminState` derives `Debug`; `Arc<RegionAccountant>` is `Debug` (Task 2 ensured
`RegionAccountant: Debug`), so no manual `Debug` work is needed here.

- [ ] **Step 4: Implement the trait method**

In the `#[async_trait] impl AdminRpc for AdminRpcImpl` block, add (next to
`channels`). Import `RegionStatsResponse` from `decdn_common` if the file's
`use decdn_common::{...}` list doesn't already pull it in:

```rust
    async fn region_stats(&self) -> RpcResult<RegionStatsResponse> {
        // No accountant wired (unit tests / a node with no accounting surface):
        // report an empty list, not an error — zero regions is legitimate and
        // the CLI renders an empty-table sentinel for it.
        let Some(acc) = self.state.region_accountant.as_ref() else {
            return Ok(RegionStatsResponse {
                regions: Vec::new(),
            });
        };
        Ok(RegionStatsResponse {
            regions: acc.snapshot(),
        })
    }
```

- [ ] **Step 5: Wire it in the runtime**

In `crates/node/src/runtime/mod.rs`, find the `admin::AdminState::new(...)`
builder chain (it ends with `.with_dht(...)` and `.with_channels(...)`, ~1010–1050)
and add to the chain:

```rust
        .with_region_accountant(Arc::clone(&region_accountant))
```

(Place it alongside the other `.with_*` calls. `region_accountant` is already in
scope from Task 4.)

- [ ] **Step 6: Run tests + clippy + build**

Run: `cargo nextest run -p decdn-node region_stats 2>&1 | tail -20 && cargo build -p decdn-node 2>&1 | tail -5 && cargo clippy -p decdn-node 2>&1 | tail -10`
Expected: tests PASS, builds, no clippy warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/node/src/admin.rs crates/node/src/runtime/mod.rs
git commit -m "feat(node): admin_v1_regionStats backed by the region accountant (#750)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: CLI `decdn node region-stats` (`common` + `cli`)

**Files:**

- Modify: `crates/common/src/cli/node.rs`, `crates/cli/src/commands/node.rs`

- [ ] **Step 1: Add the args struct + command variant**

In `crates/common/src/cli/node.rs`:

Add the variant to `enum NodeCommand` (next to `Channels(ChannelsArgs)`):

```rust
    /// Show cumulative per-region bandwidth (bytes in/out) from the running node.
    RegionStats(RegionStatsArgs),
```

Add the args struct modeled on `ChannelsArgs` (copy its fields verbatim — they
are the standard admin-client knobs: `admin_url`, `config`, `timeout_ms`,
`json`). Locate `pub struct ChannelsArgs` (~line 175) and add after it:

```rust
/// Arguments for `decdn node region-stats`.
#[derive(Debug, clap::Args)]
pub struct RegionStatsArgs {
    /// Admin JSON-RPC URL (overrides config-derived default).
    #[arg(long)]
    pub admin_url: Option<String>,
    /// Config file to resolve the admin URL from when `--admin-url` is unset.
    #[arg(long)]
    pub config: Option<std::path::PathBuf>,
    /// Per-request timeout in milliseconds.
    #[arg(long, default_value_t = 5000)]
    pub timeout_ms: u64,
    /// Emit raw JSON instead of the table.
    #[arg(long)]
    pub json: bool,
}
```

> Match `ChannelsArgs` exactly — if its fields differ (e.g. attribute names,
> defaults), copy those instead of the above so the two commands stay
> consistent. Re-export `RegionStatsArgs` wherever `ChannelsArgs` is re-exported
> (`crates/common/src/cli/mod.rs` line ~22 lists `ChannelsArgs` — add
> `RegionStatsArgs` to the same `use`/`pub use`).

- [ ] **Step 2: Write the failing formatting test**

In `crates/cli/src/commands/node.rs` test module (where `write_channels_table`
tests live; search for `write_channels_table` / `fn write_` tests), add:

```rust
    #[test]
    fn region_stats_table_renders_rows_and_empty_sentinel() {
        use decdn_common::{RegionBytes, RegionStatsResponse};

        let mut buf = Vec::new();
        write_region_stats_table(
            &mut buf,
            &RegionStatsResponse {
                regions: vec![
                    RegionBytes {
                        region: "DE".to_string(),
                        bytes_in: 0,
                        bytes_out: 1_048_576,
                    },
                    RegionBytes {
                        region: "UNKNOWN".to_string(),
                        bytes_in: 2_097_152,
                        bytes_out: 0,
                    },
                ],
            },
        )
        .expect("write table");
        let out = String::from_utf8(buf).expect("utf8");
        assert!(out.contains("DE"), "table must list DE: {out}");
        assert!(out.contains("UNKNOWN"), "table must list UNKNOWN: {out}");

        let mut empty = Vec::new();
        write_region_stats_table(&mut empty, &RegionStatsResponse { regions: vec![] })
            .expect("write empty");
        let out = String::from_utf8(empty).expect("utf8");
        assert!(out.contains("(no region data)"), "empty sentinel: {out}");
    }
```

- [ ] **Step 3: Run to verify it fails to compile**

Run: `cargo nextest run -p decdn-cli region_stats_table 2>&1 | tail -20`
Expected: compile error — `write_region_stats_table` not found.

- [ ] **Step 4: Add the command handler + table writer**

In `crates/cli/src/commands/node.rs`:

Add `RegionStatsResponse` (and `RegionBytes` if needed) to the
`use decdn_common::{...}` import list at the top (the list at lines ~16-17
already imports `ChannelsResponse`, `ChannelSnapshot`, etc.).

Add the dispatch arm in the `match &args.cmd` block (next to the `Channels` arm,
~line 57):

```rust
        cli::NodeCommand::RegionStats(r) => region_stats(r, global_config).await,
```

Add the command handler modeled on `channels` (~line 592):

```rust
/// `decdn node region-stats`: call `admin_v1_regionStats` on the running node
/// and print cumulative per-region bandwidth (issue #750).
pub async fn region_stats(
    args: &cli::RegionStatsArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let parsed: RegionStatsResponse = client
        .region_stats()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty = serde_json::to_string_pretty(&parsed)
            .context("failed to encode region stats as JSON")?;
        println!("{pretty}");
    } else {
        let mut stdout = io::stdout().lock();
        write_region_stats_table(&mut stdout, &parsed)
            .context("failed to write region stats table")?;
    }

    Ok(())
}

/// Write the per-region bandwidth table to `w`. Pure function (takes
/// `&mut impl Write`) so the formatting is unit-testable without an HTTP hop,
/// mirroring [`write_channels_table`]. Byte counts are raw decimal so a
/// scraping script sees a stable shape.
fn write_region_stats_table(
    w: &mut impl io::Write,
    resp: &RegionStatsResponse,
) -> io::Result<()> {
    writeln!(w, "regions={}", resp.regions.len())?;
    if resp.regions.is_empty() {
        return writeln!(w, "(no region data)");
    }
    writeln!(w, "{:<8} {:>16} {:>16}", "REGION", "BYTES_IN", "BYTES_OUT")?;
    for r in &resp.regions {
        writeln!(w, "{:<8} {:>16} {:>16}", r.region, r.bytes_in, r.bytes_out)?;
    }
    Ok(())
}
```

> The method name on the generated client is `region_stats` (jsonrpsee derives
> the client method from the trait method name, not the `name = "regionStats"`
> wire name) — the same way `client.channels()` maps to the `channels` method.

- [ ] **Step 5: Run the formatting test**

Run: `cargo nextest run -p decdn-cli region_stats_table 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 6: Build + clippy the CLI**

Run: `cargo build -p decdn-cli 2>&1 | tail -10 && cargo clippy -p decdn-cli 2>&1 | tail -10`
Expected: builds (clap derive picks up the new subcommand), no clippy warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/common/src/cli/node.rs crates/common/src/cli/mod.rs crates/cli/src/commands/node.rs
git commit -m "feat(cli): decdn node region-stats command (#750)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Full workspace verification

**Files:**
 none (verification only)

- [ ] **Step 1: Build + lint + format the whole workspace**

Run: `cargo build && cargo clippy && cargo fmt -- --check`
Expected: clean build, no clippy warnings, formatting passes. Fix any
`max_width = 100` / anti-panic issues surfaced here.

- [ ] **Step 2: Run the full test suite**

Run: `cargo nextest run`
Expected: all tests PASS, including the new `region_accounting`,
`accepted_voucher_records_served_bytes_by_region`,
`region_accounting_log_stops_promptly_on_signal`, `region_stats_*`, and
`region_stats_table_*` tests.

- [ ] **Step 3: License/advisory audit**

Run: `cargo deny check`
Expected: PASS (the only new dep, `async-trait`, is already in the tree).

- [ ] **Step 4: Pre-commit hooks**

Run: `pre-commit run --all-files`
Expected: PASS (or auto-fixes; re-stage and re-run if a hook rewrites a file).

- [ ] **Step 5: Final commit if pre-commit auto-fixed anything**

```bash
git add -A
git commit -m "chore(node): pre-commit fixups for per-region accounting (#750)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

(Skip if the working tree is clean after step 4.)

---

## Self-review notes

- **Spec coverage:** attribution-by-counterparty-region (Task 2 `region_for` +
  `PeerTableResolver`, Task 3 wiring); `UNKNOWN` bucket (Task 2); periodic log
  (Task 4); admin RPC (Tasks 1 + 5); CLI (Task 6); bytes-in seam
  (`record_pulled`, Task 2, no production caller — explicitly tested in Task 2
  and documented); cumulative semantics (Task 2/4); config interval with
  `0`-disables (Task 4). All spec sections map to a task.
- **No labeled Prometheus counters** — confirmed infeasible on the
  `iroh_metrics` backend; not attempted.
- **Type consistency:** `RegionBytes` / `RegionStatsResponse` defined once in
  `common` (Task 1) and reused everywhere; `record_served` / `record_pulled` /
  `snapshot` / `attach_region_accountant` / `with_region_accountant` names are
  consistent across Tasks 2–6.
- **Inter-task compilation:** Task 1 leaves `impl AdminRpc for AdminRpcImpl`
  incomplete until Task 5 — that is called out in Task 1 Step 4 (build only
  `decdn-common` there). All other tasks build cleanly on their own.
