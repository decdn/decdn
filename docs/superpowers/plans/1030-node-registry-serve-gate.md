# G-NODE-01: node onboarding full sequence (bare → accepting delivery) — issue #1030

Worktree `.claude/worktrees/1030-node-registry-serve-gate`, branch
`feat/1030-node-registry-serve-gate`, based on `origin/main` @ `fcde9987`.
All line anchors below are verified against that commit.

## Context
> **Status: implemented.** This plan was executed on `feat/1030-node-registry-serve-gate`.
> Three deviations from the plan as written, all found during implementation:
>
> - **`AdminState::new` kept `const fn`.** The plan predicted it would have to
>   drop it. The new field is an `Option<Arc<dyn StakerSet>>`, and `None` is
>   const-constructible, so only the `with_staker_set` builder is non-const.
> - **`launch_configured` outgrew the 100-line clippy ceiling.** The
>   onboard-or-fund branch is a free `provision_on_chain` function instead of
>   being inline.
> - **One extra file needed updating**: `crates/cli/tests/admin_rpc.rs` hand-builds
>   the `admin_v1_health` JSON specifically so DTO drift fails a test. Adding
>   `registry_active` broke it, exactly as that test intends.
>
> Everything else landed as planned, including the predicted trap at
> `crates/node/tests/support/mod.rs` (`ConfigStakerSet::empty()` refuses all
> delivery, so the fixture had to pass a set containing its own node id).


Issue #1030 asks for an e2e journey covering the whole operator onboarding arc: a bare
data dir → `decdn setup` (approve → `bond` → `declareMbps` → `registerNode`) → daemon
bring-up → paid delivery, plus three negatives.

Two things block writing it as specified, and exploration settled both:

1. **No bare-node fixture.** `NodeFixture::launch_configured` unconditionally calls
   `chain.onboard_operator(...)` before spawning the daemon
   (`crates/e2e/src/node.rs:382`), so every journey starts *already* bonded and
   registered. Nothing today drives a **successful** `decdn setup` from nothing —
   `cli_setup_partial.rs` only covers the mid-sequence failure path.

2. **The load-bearing negative describes behavior that does not exist.** The issue
   requires "daemon refuses paid delivery before registration is confirmed on-chain".
   The `cdn/client/v1` serve path never reads `CapacityBond`: `serve_stream`
   (`crates/node/src/handlers/client/dispatch.rs:110`) has no registration gate, and
   `ServeRejectReason` (`crates/node/src/handlers/client/mod.rs:531`) has no matching
   variant. The only registration awareness is `crates/node/src/binding_check.rs`, a
   one-shot **advisory** startup sample whose own module doc says it "never blocks
   startup".

That gap is not a spec question — **ADR 019 § Phase 4 already mandates the gate.** Its
acceptance table names criterion #1 "Node is active in the on-chain registry —
`CapacityBond.isActiveNode(nodeId)` returns `true`" as a precondition for accepting
`StreamRequest` on `cdn/client/v1` (`adr/019-node-onboarding.md` § Phase 4). The daemon
never implemented it, so an unregistered — and therefore **unslashable** — node earns
USDC today.

So this work has two halves: **implement the ADR-019 serve gate**, then **write the
journey that proves the whole arc**, using the gate as the hinge between the "bare" and
"accepting delivery" states.

Intended outcome: a node absent from the on-chain active set refuses paid delivery and
says so in its metrics and admin health; running `decdn setup` against a live bare daemon
flips it to serving **with no restart**; and `crates/e2e/tests/g_node_01_onboarding.rs`
proves the positive arc plus the two contract-level negatives.

---

## Part A — Implement the ADR-019 registry serve gate (`crates/node`)

### A1. Reuse the existing staker set — build no new watcher

The daemon **already maintains exactly this predicate**.
`dht::capacity_bond_registry::bootstrap` (`crates/node/src/runtime/mod.rs:874`)
enumerates `CapacityBond.getRegisteredNodes()` (fully paginated,
`capacity_bond_registry.rs:439-474` — no page-size truncation) with its per-entry
on-chain `active[]`, which is `isActive(ethAddress)` — the exact `isActiveNode` semantics
ADR 019 names. It publishes an `Arc<dyn StakerSet>` (`runtime/mod.rs:883`) whose sole
hot-path method is `is_active(&NodeId) -> bool` (`crates/node/src/dht/staker_set.rs:30`).
`NodeId` is `decdn_protocol::NodeId`, the 32-byte iroh key
(`crates/node/src/dht/routing.rs:27`).

The set is kept live by the registry route on the shared multiplexed poller
(`runtime/mod.rs:891`), following `NodeRegistered`, `NodeDeregistered`, `NodeAutoEjected`,
`Reinstated`, `UnbondingRequested` (`capacity_bond_registry.rs:596-603`), and is
re-enumerated authoritatively every `REGISTRY_RESYNC_INTERVAL` = 15 min via
`on_tick_complete` (`capacity_bond_registry.rs:411`).

So the gate is one predicate:

```rust
staker_set.is_active(&self_node_id)
```

**Do not add a new watcher or route.** `MultiplexedPollerBuilder::build` rejects a
duplicate `(address, topic0)` (`multiplexed_poller.rs:300-320`) and
`runtime/mod.rs:1318-1330` `?`-propagates it, so a second `CapacityBond` route claiming
any of those five topics is a **hard boot failure**. It would also break
`crates/node/tests/anvil_bringup_shutdown_e2e.rs`, which deliberately boots an
unregistered operator.

Two further reasons this beats a bespoke `addressToNodeId` watcher — both are traps:

- **`addressToNodeId` is the wrong oracle.** `deregisterNode`, `NodeAutoEjected`,
  `EjectedByBlacklist` and `requestUnbond` never clear it
  (`contracts/src/CapacityBond.sol:893-912`). A gate reading it would keep serving a
  deregistered or ejected node — the exact hole this feature exists to close.
- **No "could not confirm" state.** The registry bootstrap is already fatal at startup
  (`runtime/mod.rs:874-882` `?`), so a running daemon necessarily has a confirmed set.
  Absence is a real negative, not an unknown — which sidesteps the brickability
  `binding_check.rs:19-27` argues against at length.

`binding_check` stays **unchanged**: the operator-facing *diagnostic* ("which key problem
do you have — `Mismatch` or `Unbound`"). The staker set is the *enforcement* boolean.
Keeping them separate avoids churning `admin_v1_health.binding`, whose documented
once-at-bring-up sampling `g_node_07_rotate_key.rs:379/428` asserts against.

### A2. Close the `EjectedByBlacklist` gap in the registry route

`registry_route_topic0s()` (`capacity_bond_registry.rs:596`) omits
`EjectedByBlacklist`, so a blacklist ejection only lands at the 15-min resync. Add the
topic0 and an `on_operator_change(event.operator, false)` arm (~`:341`), and extend the
pinning test `route_topic0s_covers_every_staker_membership_event` (`:631`) to six events.
No collision — nothing claims that topic0. This is independently correct and makes every
consumer of the staker set (DHT admission included) right, not just the new gate.

### A3. Refusal plumbing

- **Reason.** Add `ServeRejectReason::NotRegistered` (`handlers/client/mod.rs:531`),
  collapsing to `StreamError::NotFound` in the arm at `mod.rs:573-588` alongside
  `CacheMiss` / `UnknownChannel` / `OwnerMismatch`: the client re-routes and scores the
  node no-fault rather than degraded. Refresh the stale "six/seven reasons" counts in that
  doc comment while there.
- **Metric.** This codebase uses **one `Counter` per reason, no label dimension**
  (`metrics.rs:1097-1101` says so explicitly). Three edits: the field on the
  `MetricsGroup`-derived struct near `metrics.rs:1220`; the `recorders!` entry near
  `metrics.rs:2040`; and the emission arm in
  `crates/node/src/handlers/client/wire.rs:109-134`. Also extend
  `serve_stream_rejected_counters_start_at_zero_and_increment_per_reason`
  (`metrics.rs:3138`) — it is an explicit list, not derive-exhaustive, so omitting it
  silently drops the new series' zero-export guard. Scrape name:
  **`decdn_serve_stream_rejected_not_registered_total`**.
- **Gate placement.** Insert in `serve_stream` **after** the chain denylist check
  (`dispatch.rs:215`) and **before** the lane-key resolution (`dispatch.rs:226`).
  - It must sit *below* `let rate_per_mb = self.clamped_rate()` (`dispatch.rs:186`) —
    the signed body needs the rate, and `clamped_rate()` is side-effecting so it must not
    be called twice (#1518).
  - It must sit *below* the two denylist checks (`:210`, `:215`), or an unregistered node
    would answer a blacklisted hash with `NotFound` instead of `HashBlacklisted`,
    weakening the ADR 011 takedown signal. Compliance codes keep priority.
  - Everything above `:186` exits via `reset_stream` with no signed response, so this is
    the first position that yields the signed `StreamResponse { ok: false }` that
    `ClientFixture::refused_stream` (`crates/e2e/src/client.rs:730`) and
    `UpstreamRefused::evidence()` require.
  - **Document the irony in the module doc**: a node failing this gate is by definition
    unslashable (`SlashJudge._checkRegistered` resolves through the binding), so the
    `slash_sig` on its own refusal is unattributable. Producing it is still right — it
    costs nothing and keeps the wire shape uniform — but it is not usable evidence.

### A4. Threading `ClientHandlerDeps`

Add `staker_set: Arc<dyn StakerSet>` + the local `NodeId` to `ClientHandlerDeps`
(`mod.rs:719`, field near `:749`), as **required positional `new()` params** — follow the
`content_deny` precedent at `mod.rs:748-760`, which argues that a forgotten wiring must
not be indistinguishable from "allow everything".

Five construction sites, all must be updated:

| Site | Note |
|---|---|
| `crates/node/src/runtime/mod.rs:1169` | production; `staker_set` (`:883`) and `infra.secret_key.public()` are both already in scope |
| `crates/node/src/handlers/client/mod.rs:1851` | `handler_over_store` |
| `crates/node/src/handlers/client/mod.rs:1906` | `handler_for_tests_with_floor` |
| `crates/node/src/handlers/client/mod.rs:2308` | capability-intake tests |
| **`crates/node/tests/support/mod.rs:407`** | **the choke point** for `client_loopback.rs`, `node_origin_pull.rs`, `node_to_node_pull_through.rs`, `origin_range_pull.rs`, `anvil_settlement_e2e.rs` |

**The trap:** `ConfigStakerSet::empty()` refuses everything (`staker_set.rs:79-86`). Each
test construction must pass a set containing its own node id, or the entire
`crates/node/tests` suite fails at once. The `build_handler_with(..., |deps| ...)` seam
(`support/mod.rs:449`) is how a new unit test flips it to refusing — same shape as
`deps.rate_bounds = …` at `client_loopback.rs:6970`.

### A5. Admin surface

Add `registry_active: bool` to `HealthResponse` (`crates/common/src/admin.rs:73`), fed
from `staker_set.is_active(&self_node_id)` in `AdminState`. This is the operator's answer
to "why is my node not earning" and the journey's no-restart observable. Adding a field
is a wire change and therefore fine — CLAUDE.md § *Pre-launch*.

Note `AdminState::new` and `with_binding` are `const fn` (`crates/node/src/admin.rs:303`,
`:331`) and `AdminState` derives `Debug, Clone`. Holding an `Arc<dyn StakerSet>` forces
`new` to drop `const`. No caller is in a const context (all sites are
`crates/cli/tests/admin_rpc.rs` and `runtime/mod.rs`), so this is invisible.

### A6. Deliberate scope boundaries

`cdn/probe/v1` is **not** gated: a probe is unpaid, it is the rate-discovery channel, and
gating it would perturb the signed probe/stream evidence pairs
`g_gov_03_real_evidence.rs` depends on. The daemon's own **buyer** leg (node-to-node
cache-miss pulls) is likewise untouched — an unregistered node may still buy, it just may
not sell. Record both as explicit non-decisions in the module doc so neither is "fixed"
later by accident.

Residual, worth naming: a client can probe `has_blob: true`, pay to open a stream, and
get `NotFound` with no way to distinguish it from a real miss. That is acceptable (it is
the same shape as `EvictedSinceProbe`), but it makes the operator-side counter the only
place the cause is visible — one more reason to get the metric name right.

### A7. ADR

Edit `adr/019-node-onboarding.md` § Phase 4: criterion #1 is *enforced* on the serve
path, and eligibility comes from the same live registry projection the DHT staker set
uses, so a fresh registration reaches a running daemon without a restart. ASD-STE100
voice per CLAUDE.md. No new ADR number — the decision already lives here; only its
enforcement is new.

---

## Part B — Bare-node fixture (`crates/e2e/src`)

### B1. `NodeFixture::launch_bare`

`launch_configured` always onboards at `crates/e2e/src/node.rs:382`. Add a parameter (or
sibling entry point) that provisions everything **except** the on-chain onboarding: data
dir at `0o700`, eth keystore + signer, iroh key, seeded fs origin, ports, rendered
`node.toml`, cache pre-warm, daemon spawn, `wait_healthy`. The daemon must still come up
healthy — only `chain.onboard_operator` is skipped.

Still `fund_eth` the operator (gas for the `setup` transactions), but **not**
`transfer_token` — the journey funds TOKEN itself so `setup`'s pre-flight has a real
funding step to clear.

### B2. `CapacityBond` custom errors in `crates/e2e/src/bindings.rs`

The `CapacityBond` `sol!` block (`bindings.rs:57`) declares no errors, and
`assert::expect_revert` (`assert.rs:18`) matches on `E::SELECTOR`. Add:

```solidity
error BondBelowMinimum(uint256 bond, uint256 required);
error NodeIdAlreadyBound(address currentOwner);
error AddressAlreadyBound(bytes32 currentNodeId);
```

with the same "declared so a reverted call decodes to a *named* error" comment the file
carries at `bindings.rs:155-157`. Shared block, not a local `sol!` module — more than one
journey will want them.

### B3. `ChainFixture::register_node_raw`

The negatives need to submit `registerNode` as an arbitrary signer with an arbitrary
bond and capture the revert; `onboard_operator` (`chain.rs:461`) hard-codes the happy
path. Extract its signature half into
`register_node_raw(operator, node_secret, region, multiaddr) -> Result<()>` — EIP-712
binding signature (`register_node_signing_hash` + `bind_node_id_domain`) plus the ed25519
ownership proof (`node_register::ownership_message_digest`) — sending `registerNode` and
returning the `anyhow`-wrapped error so `expect_revert_anyhow` can downcast it
(`assert.rs:36`). Rebuild `onboard_operator` on top of it; no duplicated crypto.

---

## Part C — Tests

### C1. Unit — the gate itself

In `crates/node/src/handlers/client/` (or `client_loopback.rs`): the gate refuses when
the staker set omits the local id and admits when it contains it, asserting the counter
delta. Template: `build_handler_with(..., |deps| deps.staker_set = …)`,
`client_loopback.rs:6970` shape.

### C2. Free e2e coverage — extend `g_node_07_rotate_key.rs::run_unbound`

That journey already drives the daemon into `Mismatch` (`:381`) and then only *probes*
(`:459`). Add a `client.fetch(...)` there expecting `UpstreamRefused` with
`StreamError::NotFound`, plus
`node.scrape_metric("decdn_serve_stream_rejected_not_registered_total") == 1`. This is
the cheapest proof that a hand-swapped, unslashable key stops earning.

### C3. The journey — `crates/e2e/tests/g_node_01_onboarding.rs` (new)

Three `#[tokio::test(flavor = "multi_thread")]` targets. Standard header:
`#![cfg(feature = "anvil-e2e")]` + the five-lint allow block (`smoke.rs:32-41`),
`OVERALL_TIMEOUT = decdn_e2e::timeout::STANDARD` unless the sequential poll ladder
exceeds ~150 s (`crates/e2e/src/timeout.rs:41-55` is the rule — do not copy a
neighbour's constant), `Box::pin(run())` inside `tokio::time::timeout`,
`ensure_decdn_cli_built()?` first. CLI helpers (`run_setup_json`, `last_json_line`,
`json_str`, `json_bool`) copied verbatim from `g_node_06_unbond.rs:611-662` and
`cli_setup_partial.rs:179-196`.

**C3a — positive: `onboarding_from_bare_reaches_paid_delivery`**

1. `ChainFixture::launch()`, then `NodeFixture::launch_bare(&chain, "US", &payload)`.
   Payload ≥ 2 MiB: at the fixture's `rate_per_mb = 10` that is a 20 µUSDC claim, above
   `redeem_threshold_micro_usdc = 10`, which is what makes the seller redeem so on-chain
   served-bytes actually move.
2. **The pre-registration negative, folded in as step 1 of the arc.**
   `chain.is_registered(operator) == false`; `admin_v1_health.registry_active == false`;
   a paid fetch fails; and a 0→1 delta on
   `decdn_serve_stream_rejected_not_registered_total`. The metric delta is load-bearing —
   every refusal collapses to wire `NotFound`, so the counter is the only place the
   *cause* is observable.
3. Fund TOKEN to `max(min_bond, bond_required(TARGET_MBPS))` via `chain.transfer_token`,
   then run
   `decdn setup --mbps N --region US --yes --accept-terms --json --config <path>`.
4. Assert the `--json` summary (schema at `crates/cli/src/commands/setup.rs:1257-1329`):
   `partial == false`; `preflight.{chain_id_ok,clock_ok,funding_ok,native_ok}` all true;
   `bond.{approve_tx,bond_tx,declare_tx}` non-null; `register.submitted == true` and
   `register.node_id` == the daemon's live iroh key; `readiness.registry_active == true`,
   `readiness.declared_mbps == N`.
5. Cross-check on chain: `chain.is_active(operator)`, `chain.declared_mbps(operator)`,
   `chain.node_id_of(operator)` == the local key.
6. **No restart.** `poll` `admin_v1_health.registry_active` until true — the assertion
   that proves the registry route flips the gate live, and the "bare → accepting
   delivery" hinge.
7. ADR 019 Phase 3 state-sync: rate floor — `client.probe(...)` quote clamps to the
   on-chain floor (`g_gov_02_rate_bounds.rs` idiom); blacklist sync — the listener
   answering at all is the proof, since the router is gated on initial sync
   (`runtime/mod.rs` `gate_listener_on_blacklist_sync`); DHT/registry view —
   `admin_v1_status`.
8. Paid delivery now succeeds: `client.fetch(...)` exact bytes; a lane on the paid
   `pool_id` via `admin_v1_lanes`; `read_pool` deposit == `DEPOSIT_MICRO_USDC`; and
   `poll` `chain.served_bytes(operator)` into `[content_bytes, wire_bytes]` — the
   **range**, not equality, per the #1381 flake note at `smoke.rs:148-166`, with
   `wire_bytes` from `decdn_bao_range::align_range(0, 0, len)?.wire_len()`.

**C3b — negative: `register_below_min_bond_reverts`**

State plainly in the file doc that the issue's wording is imprecise. `bond()` has **no
floor at all** — `contracts/src/CapacityBond.sol:587-589` guards only `ZeroAmount`.
Under-bonding is caught downstream. So `bond(min_bond - 1)` **succeeds**, and the
subsequent `registerNode` reverts `BondBelowMinimum(bond, required)`
(`CapacityBond.sol:807-809`), asserted with
`expect_revert_anyhow::<BondBelowMinimum>`.

`BondBelowMinimum` has **zero** coverage anywhere in the repo today; only its sibling
`BondBelowCurve` is tested (`contracts/test/CapacityBond.t.sol:784`).

**C3c — negative: `node_id_reregistration_by_another_address_reverts`**

Onboard operator A normally. Then a fresh operator B: `fund_eth`,
`transfer_token(minBond)`, `approve`, `bond(minBond)` — **B must be bonded first**,
because `_checkRegistrationPreconditions` runs before `_checkBindingOneToOne`
(`CapacityBond.sol:780-781`), so an unbonded B hits `BondBelowMinimum` and the test would
pass for the wrong reason.

B then calls `registerNode` with **A's node id** and *valid* signatures — a real EIP-712
binding signature under B's eth key and a real ed25519 proof under A's node key (the test
holds it). That proves the guard which fires is the one-to-one binding rule, not a
signature failure. Assert `NodeIdAlreadyBound(currentOwner == A)`
(`CapacityBond.sol:824-827`), then that `chain.operator_of_node_id(a_node_id)` is still A.

Also zero existing coverage — `NodeIdAlreadyBound` appears only in the contract and in
CLI prose.

---

## Files touched

| File | Change |
|---|---|
| `crates/node/src/dht/capacity_bond_registry.rs` | `EjectedByBlacklist` topic0 + arm + pinning test |
| `crates/node/src/handlers/client/mod.rs` | `NotRegistered` reason, `wire_error` arm, deps fields, 3 test constructions |
| `crates/node/src/handlers/client/dispatch.rs` | the gate, between `:215` and `:226` |
| `crates/node/src/handlers/client/wire.rs` | metric emission arm |
| `crates/node/src/metrics.rs` | counter field, recorder, zero-export guard test |
| `crates/node/src/runtime/mod.rs` | thread `staker_set` + self `NodeId` into `ClientHandlerDeps::new` (`:1169`) and `AdminState` |
| `crates/node/src/admin.rs`, `crates/common/src/admin.rs` | `registry_active` on `HealthResponse`; drop `const fn` |
| `crates/node/tests/support/mod.rs` | `client_handler_deps` passes a serving staker set |
| `adr/019-node-onboarding.md` | Phase 4 — criterion #1 is enforced |
| `crates/e2e/src/node.rs` | `launch_bare` |
| `crates/e2e/src/chain.rs` | `register_node_raw`; `onboard_operator` rebuilt on it |
| `crates/e2e/src/bindings.rs` | three `CapacityBond` error decls |
| `crates/e2e/tests/g_node_07_rotate_key.rs` | refusal assertion in `run_unbound` |
| `crates/e2e/tests/g_node_01_onboarding.rs` | **new** — the journey |

No CI edit: the journeys job runs `cargo nextest run -p decdn-e2e --features anvil-e2e`
over the whole package, so a new `tests/*.rs` is picked up automatically.

---

## Verification

```bash
cargo build && cargo nextest run --no-fail-fast          # macOS: --no-fail-fast (issue #697)
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt -- --check
```

The `--all-features` clippy run is what lints the new gated e2e target (#1551) — a scoped
run will not.

The journey — **rebuild both binaries first**, or the fixtures exec stale ones and fail
with confusing TOML-parse / unrecognized-subcommand errors:

```bash
cargo build -p decdn-node -p decdn-cli
(cd contracts && forge build)
cargo nextest run -p decdn-e2e --features anvil-e2e -E 'binary(g_node_01_onboarding)'
```

Select the **binary**, not the file stem — nextest's positional filter matches test
*function* names, so a bare `g_node_01` runs zero tests.

Regression sweep for the new gate:

```bash
cargo nextest run -p decdn-node --no-fail-fast          # the tests/support choke point
cargo nextest run -p decdn-e2e --features anvil-e2e --test-threads 2
cargo nextest run -p decdn-node --features anvil-e2e \
  --test anvil_settlement_e2e --test anvil_bringup_shutdown_e2e
```

Analysis says these are safe — every e2e fixture node is onboarded *before* the daemon
spawns (`crates/e2e/src/node.rs:382`), so there is no bring-up race;
`g_node_06_unbond.rs` does no paid fetch at all; `g_node_07_rotate_key.rs` only probes
while in `Mismatch`; and `anvil_bringup_shutdown_e2e.rs` deliberately runs unregistered
but never serves. The sweep is the proof, not that reasoning.

Contracts are untouched, but the gate reads their ABI:

```bash
(cd contracts && forge fmt --check && FOUNDRY_PROFILE=ci forge build --sizes --deny warnings && forge test)
```

Commit with `--no-verify` after running the gate manually; pre-commit hooks here can
`reset: moving to FETCH_HEAD` and corrupt HEAD.
