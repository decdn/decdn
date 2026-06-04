# Per-region bandwidth accounting log (#750)

**Status:** Approved design — ready for implementation planning
**Issue:** [#750](https://github.com/decdn/decdn/issues/750) — per-region bytes-in/out accounting for capacity planning and data-residency reporting
**Date:** 2026-06-05

## Problem

The node tracks per-peer reputation and payment vouchers but has no aggregated
view of bytes served broken down by geographic region. Operators want a
per-region bytes-in / bytes-out breakdown for regional capacity bonding and
data-residency reporting.

## Decisions (resolved during brainstorming)

1. **Region attribution = counterparty peer region.** Bytes are bucketed by the
   counterparty's self-attested `NodeAnnounce.region` (ADR 030), looked up in the
   gossip peer table. There is no client-side region in the wire protocol, so
   traffic to/from non-peer end-clients (and any peer not currently in the table)
   is bucketed under a single `UNKNOWN` region. This is the only breakdown the
   wire protocol supports.

2. **Surface = periodic structured log + admin RPC** (both). Labeled Prometheus
   counters (`decdn_bytes_served_total{region=...}`) are **not feasible**: the
   `iroh_metrics` backend used here does not support per-field/dynamic labels
   (see the comment at `crates/node/src/metrics.rs:167` and the split
   `dht_rate_limit_rejected_*` counters). Region is an open-ended key, so it
   cannot be a metric label without a different backend.

3. **Bytes-in is a forward-compatible seam.** Node-to-node paid pull-through
   (`stream_fetch`) is currently only exercised from tests — there is no
   production orchestration that pulls a cache miss from an upstream node (the
   serving handler returns `NotFound` on a local miss; cache pull-through targets
   opaque S3/R2 origins with no region). So `bytes_in` is fully modeled and
   exposed but reads `0` in production until pull-through is orchestrated. The
   single integration point (`RegionAccountant::record_pulled`) is documented at
   that future call site.

4. **Totals are cumulative since process start** (Prometheus-counter semantics).
   Operators diff across log lines for a window. Simpler than reset-on-read and
   does not lose counts on a missed scrape.

## Architecture

```
voucher accepted (delta_bytes, client_node_id)
        │
        ▼
ClientHandler.collect_voucher ──► RegionAccountant.record_served(node_id, bytes)
                                          │ resolve region via RegionResolver
                                          ▼
                                  totals: HashMap<region, {bytes_in, bytes_out}>
                                          │
                       ┌──────────────────┴───────────────────┐
                       ▼                                       ▼
        run_region_accounting_log (periodic)        AdminRpc.region_stats
        tracing::info!(event="region_bandwidth")    └► decdn node region-stats (CLI)
```

### Components

#### 1. `crates/node/src/region_accounting.rs` (new module)

- `trait RegionResolver: Send + Sync { async fn region_of(&self, node_id: &[u8; 32]) -> Option<String>; }`
  — abstraction so the accountant is unit-testable without a live peer table.
- `struct PeerTableResolver(Arc<RwLock<PeerTable>>)` — production impl. Takes a
  brief read lock, `get(node_id)`, clones `entry.announce.region`; returns `None`
  when the peer is absent. (`PeerTable` is `decdn_gossip`; `RwLock` is
  `tokio::sync::RwLock`, matching how `admin.rs` holds the table — so resolution
  is `async`.)
- `struct RegionAccountant { resolver: Arc<dyn RegionResolver>, totals: Mutex<HashMap<String, RegionTotals>> }`
  - `struct RegionTotals { bytes_in: u64, bytes_out: u64 }`
  - `const UNKNOWN: &str = "UNKNOWN"` — bucket for unresolved regions.
  - `async fn record_served(&self, peer: &[u8; 32], bytes: u64)` — resolve region
    (or `UNKNOWN`), `saturating_add` into `bytes_out`.
  - `async fn record_pulled(&self, peer: &[u8; 32], bytes: u64)` — same into
    `bytes_in`. **Forward seam: no production caller today.**
  - `fn snapshot(&self) -> Vec<RegionBytes>` — clone totals into a region-sorted
    `Vec` (deterministic output for the log and the RPC).
  - Anti-panic: a poisoned `totals` lock is skip-and-log (never `unwrap`);
    all arithmetic saturating.

#### 2. Bytes-out wiring — `crates/node/src/handlers/client.rs`

- Add field `region_accountant: OnceLock<Arc<RegionAccountant>>` plus
  `pub fn attach_region_accountant(&self, a: Arc<RegionAccountant>)`, mirroring the
  existing `voucher_activity` / `redeem_hint` post-construction attachment idiom.
  This avoids touching the already-`#[allow(too_many_arguments)]` `new()`.
- In `collect_voucher`, on the `apply_voucher` `Ok` arm — beside the existing
  `record_receipt` call, after the per-channel guard is dropped — best-effort:

  ```rust
  if let Some(acc) = self.region_accountant.get() {
      acc.record_served(&client_node_id.0, delta_bytes).await;
  }
  ```

  `delta_bytes` is exactly the bytes paid for this voucher interval. An unattached
  accountant (tests, no admin surface) is a no-op.

#### 3. Periodic log — `crates/node/src/runtime/mod.rs`

- `async fn run_region_accounting_log(accountant: Arc<RegionAccountant>, interval: Duration, shutdown: ...)`
  following the `run_dispatch_gc` template: `tokio::time::interval`,
  `MissedTickBehavior::Delay`, first tick burned, spawned into the `tasks`
  `JoinSet`, shutdown-aware `select!`.
- Each tick: `snapshot()` → emit one structured line per region:
  `tracing::info!(event = "region_bandwidth", region = %r.region, bytes_in = r.bytes_in, bytes_out = r.bytes_out)`.
- Keep the per-region emit in a small pure helper so formatting is unit-testable
  without driving the timer.
- New config `ObservabilityConfig::region_accounting_interval_sec`
  (`crates/common/src/config/types.rs`), default `3600`, `0` disables — mirroring
  `rpc_watchdog_interval_sec`. Wire the disabled branch like the RPC watchdog.

#### 4. Admin RPC + CLI

- `crates/common/src/admin.rs`:
  - `async fn region_stats(&self) -> RpcResult<RegionStatsResponse>;` on `AdminRpc`.
  - `struct RegionStatsResponse { regions: Vec<RegionBytes> }` and
    `struct RegionBytes { region: String, bytes_in: u64, bytes_out: u64 }` — serde,
    `#[serde(default)]` on fields for forward-compat, with a `region_stats_response_round_trips`
    test alongside the existing `*_round_trips` / `*_legacy_shape_defaults` tests.
- `crates/node/src/admin.rs`: `AdminState` gains
  `region_accountant: Option<Arc<RegionAccountant>>`; `region_stats()` returns
  `snapshot()` mapped to the DTO, or an empty `Vec` when unwired (not an error).
  All existing `AdminState` test constructors pass `None`.
- `crates/cli/src/commands/node.rs`: `decdn node region-stats` subcommand calling
  the RPC and printing a region / bytes-in / bytes-out table, following the
  existing `node peers` / `node channels` print pattern.

## Data flow

1. A paid voucher is accepted → `ClientHandler` calls `record_served(client_node_id, delta_bytes)`.
2. `RegionAccountant` resolves the region via `PeerTableResolver` (or `UNKNOWN`) and
   saturating-adds to that region's `bytes_out`.
3. The periodic task snapshots and logs all regions on its interval.
4. `AdminRpc::region_stats` snapshots on demand; the CLI prints it.

## Error handling

| Condition | Behavior |
|-----------|----------|
| Poisoned `totals` lock | Skip the update, `tracing::warn!`; never panic (anti-panic policy). |
| Counter overflow | `saturating_add` (no wrap, no panic). |
| Peer not in table / end-client | `UNKNOWN` bucket. |
| Accountant unattached | `record_*` are no-ops; `region_stats` → empty `Vec`. |

## Testing

- **Unit (`region_accounting.rs`):** stub `RegionResolver` → `record_served` buckets
  by region; absent region → `UNKNOWN`; saturating add at `u64::MAX`; `snapshot`
  is region-sorted. `PeerTableResolver` returns the announce region / `None`.
- **DTO (`common`):** `RegionStatsResponse` round-trips; legacy-shape (missing
  fields) defaults to zero / empty.
- **Integration (extend a `client_loopback`-style test):** serve a blob to a peer
  whose announce is in the table → `region_stats` shows `bytes_out` under that
  region; a peer absent from the table lands in `UNKNOWN`.
- **Log task:** unit-test the pure per-region formatting helper (no timer).

## Files touched

| File | Change |
|------|--------|
| `crates/node/src/region_accounting.rs` | new: resolver trait, `PeerTableResolver`, `RegionAccountant`, `RegionBytes` |
| `crates/node/src/lib.rs` | expose the module |
| `crates/node/src/handlers/client.rs` | `OnceLock` field + `attach_region_accountant` + `record_served` call |
| `crates/node/src/runtime/mod.rs` | build accountant, attach to handler, spawn log task, wire config |
| `crates/node/src/admin.rs` | `AdminState` field + `region_stats` impl + test constructors pass `None` |
| `crates/common/src/admin.rs` | `AdminRpc::region_stats` + DTOs + round-trip test |
| `crates/common/src/config/types.rs` | `region_accounting_interval_sec` |
| `crates/cli/src/commands/node.rs` | `region-stats` subcommand |

## Out of scope

- Production node-to-node pull-through orchestration (separate work; `record_pulled`
  is the seam it will call).
- Per-region metric labels (blocked on the metrics backend).
- Reset-on-read / windowed aggregation (cumulative chosen).
- Persisting totals across restarts (in-memory, like the other operational counters).
