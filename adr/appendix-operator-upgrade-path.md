# Appendix: Operator Protocol-Upgrade Runbook

> **Appendix, not a core protocol ADR.** Operator-facing companion to [ADR 013 — Schema Evolution](013-schema-evolution.md#adr-013-schema-evolution). This appendix sequences the safe-restart procedure every release shares and the operator actions for compatible Tier 1 and Tier 2 changes. A future Tier 3 change defines its own migration.

## Context

The protocol evolves under the [ADR 013](013-schema-evolution.md#adr-013-schema-evolution) three-tier scheme (Tier 1 minor, Tier 2 medium, Tier 3 major), which specifies the wire mechanics but not the operator question — *what do I do when a release ships?*

This runbook answers that question for Tier 1 and Tier 2, which keep the current ALPN and have stable standing procedures, plus the restart hygiene every release shares. Tier 3 changes the ALPN contract, so its rollout is designed around the concrete break and defined by the ADR that ships it. Nothing here redefines a protocol mechanism.

## Tier overview

Per [ADR 013 § Decision](013-schema-evolution.md#adr-013-schema-evolution), each schema change is exactly one tier:

| Tier | Trigger | Wire identifier | Operator action |
|------|---------|------|-----------------|
| **1 — Minor** | Append optional fields to an existing struct via two-phase deserialization | Same (e.g. `cdn/client/v1`) | None — see [§ Tier 1 — minor evolution](#tier-1--minor-evolution) |
| **2 — Medium** | Add new optional protocol message variants, no ALPN bump | Same | Config-only (opt-in flags), see [§ Tier 2 — medium evolution](#tier-2--medium-evolution) |
| **3 — Major** | Remove a field; change a type; reorder enum variants; add a mandatory field; change signed-field set; change framing | Bumped ALPN | Follow the focused migration ADR shipped with the breaking change — see [§ Tier 3 — major break](#tier-3--major-break-alpn-bump) |

## Restarting a node safely

Tier-independent: this applies to every restart — a Tier 1 pull-and-restart, a config change, host maintenance — not just to protocol upgrades.

A node that answered `has_blob: true` to a probe and then answers the follow-up `StreamRequest` with `StreamResponse { ok: false }` has advertised a blob it could not serve — an availability/reputation failure with the requester ([ADR 008](008-reputation.md#adr-008-reputation-system)), scored against this node's local standing. The probe-triggered eviction hold keeps a just-advertised blob resident so the follow-up pull succeeds ([ADR 005 § Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold)), but eviction holds are in-process state and do not survive a restart — so a restart that drops just-advertised blobs turns those pending pulls into `ok: false` refusals.

1. **Stop new inbound connections** at your load balancer or firewall. This is the step `decdn node drain` cannot do for you — drain is graceful *shutdown* (the `admin_v1_drain` call in [`appendix-local-admin-http.md`](appendix-local-admin-http.md#appendix-local-admin-http-surface), equivalent to SIGTERM), not a stop-accepting mode, so anything it takes down is already committed.
2. **Wait for both gauges to reach zero** on `/metrics` (canonical registry: [`appendix-observability.md`](appendix-observability.md#appendix-observability-and-metrics)):
   - `decdn_probe_hold_slots_used == 0` — every outstanding probe commitment has aged out, so the restart breaks none of them. This is the gauge that matters; it has no admin-RPC equivalent.
   - `decdn_streams_active{direction="inbound"} == 0` — no delivery in flight.
3. **Shut down** with `decdn node drain --wait`, which polls `admin_v1_health` for `in_flight_streams == 0` rather than returning the moment the trigger fires.
4. **Upgrade and restart.** Confirm with `decdn node health`, then re-enable inbound at the load balancer.

Stopping without step 1 is not itself slashable — an unanswered connection is not evidence — but it leaves the window above open for as long as a peer holds a fresh positive probe result for a blob you come back without.

## Tier 1 and Tier 2 operator checklists

### Tier 1 — minor evolution

A Tier 1 release adds optional fields to existing structs. By construction:

- Wire format stays compatible (postcard two-phase deserialization handles missing trailing extensions).
- ALPN string unchanged.
- Signed field set unchanged ([ADR 013 § Signed Field Freezing](013-schema-evolution.md#signed-field-freezing)) — slash evidence stays verifiable across versions.
- Payment pools and on-chain bindings unaffected.

**Operator action:** Pull and restart at your normal cadence, following [§ Restarting a node safely](#restarting-a-node-safely). No flag day, no client coordination, and no upgrade-specific drain beyond the standing restart procedure. Skipping a Tier 1 release entirely keeps you interoperable indefinitely — peers on the new version just won't see the optional fields you don't emit.

### Tier 2 — medium evolution

A Tier 2 release adds new optional message variants (e.g. a `Ping`/`Pong` keepalive on `cdn/client/v1`). The ALPN string is unchanged; legacy peers that don't understand the new variant close the stream with `UNSUPPORTED_MESSAGE` and the sender falls back ([ADR 013 § Tier 2](013-schema-evolution.md#tier-2--medium-new-message-types-no-alpn-bump)).

**Operator checklist:**

1. **Read the release notes** for new operator-config flags introduced with the variant. Typically opt-in (e.g. `enable_keepalive = true`); defaults are conservative.
2. **Pull and restart at your normal cadence,** per [§ Restarting a node safely](#restarting-a-node-safely). No Tier 2 release needs coordination beyond that standing procedure.
3. **Watch the logs, not a metric — the fallback is unmetered.** A legacy peer rejecting the new variant closes with `UNSUPPORTED_MESSAGE` and the sender falls back; that path emits a `tracing::debug!` (e.g. `crates/node/src/dht/client.rs`'s batch-store fallback) and increments **no counter**. Expected and benign for legacy peers, but a sustained rate on your own outbound streams suggests config drift, so grep for the fallback line for one rolling window after restart. No `decdn_streams_failed_total{reason="protocol_error"}` counter is exported ([`appendix-observability.md`](appendix-observability.md#appendix-observability-and-metrics) carries it as `planned`); a per-variant fallback counter is the missing piece.
4. **Configure new metrics** in your dashboard if the release exposes them (canonical registry: [`appendix-observability.md`](appendix-observability.md#appendix-observability-and-metrics)).

Payment pools and stake state are unaffected.

## Tier 3 — major break (ALPN bump)

Tier 3 is a classification boundary, not a standing rollout procedure. The ALPN string changes (`cdn/client/v1` → `cdn/client/v2`). The accompanying ADR MUST define the supported-version set, deployment order, client behavior, channel or signature migration, rollback conditions, observability, and retirement criteria that the actual break requires.

The current runtime supports the `…/v1` identifiers documented by the protocol ADRs. No transition guarantee exists until a concrete Tier 3 ADR defines and implements it.

## Cross-ADR Impact

- [ADR 013 — Schema Evolution: framing, enum discipline, evolution tiers, and signed-field freezing](013-schema-evolution.md#adr-013-schema-evolution)
- [ADR 005 — Probe-Triggered Eviction Hold (drain prerequisite)](005-protocol.md#probe-triggered-eviction-hold)
- [`appendix-local-admin-http.md` — `admin_v1_drain` invocation](appendix-local-admin-http.md#appendix-local-admin-http-surface)
- [`appendix-observability.md` — metrics referenced in the restart and Tier 2 checklists](appendix-observability.md#appendix-observability-and-metrics)
