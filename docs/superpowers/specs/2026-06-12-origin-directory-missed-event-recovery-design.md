# Chain origin-directory watcher: missed-event recovery + reorder-immunity

**Issue:** #855 — *node: chain origin-directory watcher has no missed-event recovery and ignores `updateIndex` — lost removals permanently resurrect revoked operators*

**Severity:** Medium (security). A revoked/blacklisted operator can linger in the
authorized-origin cache for the life of the process, so the node keeps treating
it as an authorized origin (prefetch authorized-origin gate, FIND_VALUE origin
discovery).

**Scope of file change:** `crates/node/src/dht/chain_origin_directory.rs` (plus a
metrics line if a clean seam exists). No contract, ADR, or config changes.

---

## Problem

`ChainOriginDirectory` keeps an in-memory projection of the on-chain origin
directory, populated at bootstrap by **authoritative `getOrigins` point-reads**
and then kept current by a background watcher that follows seven
`OriginAssignment` / `PublisherRegistry` events. The watcher applies each event's
**payload as a delta**. Two defects follow from that, both sharing one root
cause — *the live cache trusts event delivery completeness and ordering*:

1. **Outage recovery (Scenario 1).** On any RPC stream error the watcher backs
   off and reinstalls all filters **at head** (`run_watcher_once` re-`.watch()`s)
   — no backfill, no checkpoint. Events emitted during the backoff window are
   never replayed. A lost `AssignmentRevoked` / `BlacklistedAssignmentPruned` /
   `DefaultOpenOperatorRemoved` leaves the operator authorized in the cache for
   the life of the process.
   - Location: `chain_origin_directory.rs:406-432` (watcher backoff/re-arm),
     `:443-561` (`run_watcher_once`).

2. **Live cross-filter reorder (Scenario 2 — no outage needed).** The seven
   filters poll independently and are drained via `tokio::select!`, so
   `activateAssignment(ns)`@N followed by `revokeAssignment(ns, C)`@N+1 can be
   observed revoke-first, activate-second: the revoke removes C from the old set,
   then the activate re-inserts C from its full-set payload. The revoke is lost.
   The same race hits `DefaultOpenAllowlistUpdated` vs add/remove — the very race
   `updateIndex` exists to detect.
   - Location: `chain_origin_directory.rs:443-561`; handler payload-as-delta logic
     at `:600-691`.

`origin_assignment.rs:87` mirrors the contract's `DefaultOpenAllowlistUpdated`
`updateIndex` ("a monotonic version for observers") but **no consumer reads it**
(grep: zero matches).

The bootstrap path already does the right thing — it reads authoritative
`getOrigins` point sets precisely "to avoid replaying assignment-mutation
ordering" (`:336-337`). The live path abandons that and reintroduces ordering
sensitivity.

## Reference precedent

`crates/node/src/reputation_indexer.rs` already solves missed-event recovery for
the settlement indexer: it carries a `backfill_from: Option<u64>` cursor in a
`&mut` state threaded across `run_watcher_once` calls, runs a one-shot bring-up
backfill over `[backfill_from, head]` at the top of each cycle, and **clears the
cursor only after the window completes successfully** so a transient RPC failure
repeats the whole backfill. This design mirrors that discipline. (`#762` similarly
added checkpoint+backfill to the channel watcher; `#832` added re-arm backfill to
the reputation indexer.)

## Design

### Core principle

Extend the bootstrap's authoritative `getOrigins` strategy into the live path:
**an event stops being a delta to apply and becomes a trigger to re-read chain
truth.** Because `getOrigins(ns)` returns the current authoritative member set,
delivery order and single-event loss stop mattering — whoever reads last reads
the truth, and out-of-order readers converge on the same authoritative state.
This subsumes `updateIndex` versioning (we do not wire it) and the #851
namespace-0 dispatch (preserved below).

### 1. Collapse seven delta handlers into two authoritative resync routines

| Event(s) | New handling |
|---|---|
| `AssignmentActivated(ns)`, `AssignmentRevoked(ns≠0)`, `BlacklistedAssignmentPruned(ns≠0)` | `resync_namespace(ns)` |
| `DefaultOpenAllowlistUpdated`, `DefaultOpenOperatorAdded`, `DefaultOpenOperatorRemoved`, `AssignmentRevoked(ns==0)`, `BlacklistedAssignmentPruned(ns==0)` | `resync_default_open()` |
| `ContentClaimed(hash, ns)` | record `hash → ns`; if `ns` not yet known, `resync_namespace(ns)` (unchanged behavior, `:578-597`) |

- **`resync_namespace(ns)`**: `getOrigins(ns)` → resolve any newly-surfaced
  operators' `NodeId` (`resolve_and_store_operators`) → replace
  `origins_of_ns[ns]` wholesale → `publish_authorized_count`.
- **`resync_default_open()`**: `getOrigins(0)` → resolve new operators → replace
  `default_open` wholesale → `publish_authorized_count`. The `ns == 0` revoke/prune
  path dispatches here, preserving #851 (a governance revoke / permissionless
  prune of a default-open operator targets the default-open set, not a
  per-namespace set).

This removes `on_assignment_activated`, `on_default_open_replaced`,
`on_default_open_added`, `remove_default_open`, and the payload-delta body of
`remove_origin`, replacing them with the two routines above. The `tokio::select!`
dispatch shrinks to: namespace events → `resync_namespace`; default-open events →
`resync_default_open`; `ContentClaimed` → `on_content_claimed`.

### 2. Watcher re-arm resync (fixes Scenario 1)

- Introduce `struct WatcherState { last_block: u64, resync_from: Option<u64> }`,
  threaded by `&mut` across `run_watcher_once` calls (mirrors
  `reputation_indexer`'s `IndexerState`).
- `last_block` is initialized to the bootstrap head (`get_block_number` taken in
  `bootstrap`), and updated to `max(last_block, log.block_number)` on every
  observed event (the `_log` currently discarded at `:506` etc. carries
  `block_number: Option<u64>`, as used by `reputation_indexer.rs:331`).
- On a watcher **error** only, `watcher_loop` arms `resync_from = Some(last_block)`
  before sleeping/backoff. A *clean* re-subscribe does **not** arm — see the
  filters-first ordering below, which buffers across that gap. The first cycle
  after bootstrap has `resync_from == None` (bootstrap already snapshotted), so no
  redundant resync on the happy path.
- In `run_watcher_once`, **establish the seven `.watch()` filters first**, then —
  if `resync_from` is `Some(start)` — run the **resync pass**, then go live. (This
  is the one place the implementation deliberately improves on the original draft,
  which ran the resync before the filters: installing the filters first means any
  event emitted *during* the resync RPC buffers in the streams and is drained — as
  an idempotent re-read — once the select loop starts, rather than falling into a
  fresh gap. This mirrors `reputation_indexer`'s filters-then-backfill order.) The
  resync pass:
  1. Incremental `ContentClaimed` replay over
     `[max(start − REORG_OVERLAP, head − MAX_RESYNC_REPLAY_BLOCKS), head]`
     (windowed by `REPLAY_WINDOW_BLOCKS`) — catches new `hash → namespace` claims
     missed during the outage. The `MAX_RESYNC_REPLAY_BLOCKS` floor bounds the
     scan when `last_block` is stale; a clamp emits a `warn!` + resolve-failure
     metric so the truncated tail is visible. New namespaces are resync'd as they
     surface.
  2. `resync_namespace(ns)` for **every known/claimed namespace** (sorted, for
     deterministic error reporting), reflecting any revoke/prune lost in the gap.
  3. `resync_default_open()`.
  4. Fold `head` into `last_block` and **clear `resync_from = None` only after the
     whole pass succeeds** — a transient RPC failure leaves it `Some` so the next
     retry repeats the full pass (the `reputation_indexer` discipline). This
     arm/clear logic lives in `WatcherState::apply_resync`, unit-tested directly.

`REORG_OVERLAP` is a small fixed lookback (a handful of blocks) so a reorg around
the gap boundary cannot drop a `ContentClaimed`. Re-replaying a few already-seen
`ContentClaimed` logs is idempotent (`namespaces_of` is a set insert).

> Note: step 2's `getOrigins` re-read is what actually closes the security hole —
> it reflects current authoritative membership, so any revoke/prune/remove lost
> during the outage is gone after the pass. Step 1 is for completeness (new
> claims), not the security fix.

### 3. Error handling — the one subtlety

Live re-reads do RPC, which can fail mid-event. The rule preserves today's
guarantees and never weakens a removal:

- **Removal-class events** (`AssignmentRevoked`, `BlacklistedAssignmentPruned`,
  `DefaultOpenOperatorRemoved`): attempt the authoritative re-read; **on RPC
  failure, fall back to the precise delta removal** `set.remove(operator)` using
  the operator address from the event payload. A revoke is therefore *never less
  effective than today's code* — the revoked operator is dropped immediately even
  if `getOrigins` is unavailable. Bump `…_resolve_failure` + `warn!`.
- **Addition/replace-class events** (`AssignmentActivated`,
  `DefaultOpenOperatorAdded`, `DefaultOpenAllowlistUpdated`): attempt the re-read;
  **on RPC failure, defer** (bump `…_resolve_failure`, `warn!`; self-heals on the
  next event for that namespace or the next re-arm resync). Safe-closed — an
  un-added operator serves nothing and authorizes nothing.

Because removals always take effect (delta fallback) and deferred additions are
safe-closed, **no periodic safety-resync timer is needed** (YAGNI): the two named
scenarios are fully covered by per-event authoritative re-read + re-arm resync.

### 4. Test seam

The current tests (`cache_with`, `StubStakers`) exercise only the pure cache
resolution methods; they cannot drive the RPC handlers. Introduce a **minimal
trait seam** over the three contract reads the watcher needs, so handlers are
unit-testable with a stub (matching the RPC-seam intent noted in
`chain_staker_set`):

```rust
trait OriginChainReads {
    async fn get_origins(&self, ns: U256) -> Result<Vec<Address>>;
    async fn node_id_of(&self, op: Address) -> Result<Option<NodeId>>;
    async fn content_claimed(&self, from: u64, to: u64) -> Result<Vec<(Hash, U256)>>;
    async fn head_block(&self) -> Result<u64>;
}
```

The production impl wraps the existing `Contracts<P>` alloy instances; a test impl
returns scripted maps/errors. `resync_namespace`, `resync_default_open`,
`on_content_claimed`, and the re-arm resync pass are written against the trait.

## Testing plan

Unit tests (no chain), each asserting the resolved cache state via the existing
`resolve` / `has_any` helpers:

1. **Scenario 1 (outage recovery).** Seed cache: ns1 → {C}, binding C. Simulate a
   lost `Revoked(ns1, C)` (never delivered). Trigger the re-arm resync with a stub
   where `get_origins(ns1)` no longer returns C ⇒ C absent from cache afterward.
2. **Scenario 2 (live reorder).** Deliver `Revoked(ns1, C)` then
   `Activated(ns1, …)`; stub `get_origins(ns1)` (authoritative) excludes C ⇒ C
   stays gone regardless of arrival order. Symmetric test for the activate-first
   order.
3. **Removal RPC-failure fallback.** `get_origins(ns1)` errors during a
   `Revoked(ns1, C)` ⇒ C still removed via the precise delta fallback;
   resolve-failure metric incremented.
4. **Addition RPC-failure defer.** `get_origins` errors during `Activated` ⇒ set
   unchanged, no panic, metric incremented, self-heals when a later resync
   succeeds.
5. **Default-open ns0 revoke** routes to `resync_default_open` (preserves #851):
   `Revoked(0, C)` with stub `get_origins(0)` excluding C ⇒ C dropped from
   `default_open`.
6. **`ContentClaimed` replay in resync** surfaces a new namespace and resync's its
   origins.
7. **Cursor bookkeeping.** `last_block` advances to the max observed
   `log.block_number`; `resync_from` is set on error and cleared only after a
   fully-successful pass (assert it survives a mid-pass RPC error).

## Scope guardrails (YAGNI — explicitly out)

- No change to the bootstrap snapshot shape or the `StakerSet` liveness split.
- No `updateIndex` wiring (authoritative re-read subsumes it).
- No per-namespace debounce of live re-reads (testnet event volume is low; one
  `getOrigins` point read per event is acceptable).
- No periodic safety-resync timer.
- Reuse existing metrics (`…_watcher_restarts` / backoff, `…_resolve_failure`,
  `…_operator_count`); add a single `…_resync_total` counter only if it drops in
  cleanly.

## Acceptance criteria

- A revoke/prune/remove emitted during a watcher backoff window is reflected in
  the cache after the next successful re-arm (no longer lingers for the process
  lifetime).
- An out-of-order revoke/activate (or default-open update/add/remove) pair
  converges to the on-chain authoritative set regardless of observation order.
- A removal event always drops the operator even when `getOrigins` is unavailable.
- `cargo nextest run -p decdn-node` green, including the new cases; `cargo clippy`
  clean under the workspace anti-panic lints; `cargo fmt --check` clean.
