# Appendix: Operator Protocol-Upgrade Runbook

> **Appendix, not a core protocol ADR.** Operator-facing companion to [ADR 013 — Schema Evolution](013-schema-evolution.md#adr-013-schema-evolution). This appendix sequences the operator actions for compatible Tier 1 and Tier 2 changes. A future Tier 3 change defines its own migration.

## Context

The protocol evolves under the [ADR 013](013-schema-evolution.md#adr-013-schema-evolution) three-tier scheme (Tier 1 minor, Tier 2 medium, Tier 3 major). Tier 1 and Tier 2 keep the current ALPN and have useful standing operator procedures. Tier 3 changes the ALPN or topic contract and therefore needs a migration designed around the concrete break.

This runbook fills that gap. It does not redefine any protocol mechanism.

## Tier overview

Per [ADR 013 § Decision](013-schema-evolution.md#adr-013-schema-evolution), each schema change is exactly one tier:

| Tier | Trigger | Wire identifier | Operator action |
|------|---------|------|-----------------|
| **1 — Minor** | Append optional fields to an existing struct via two-phase deserialization | Same (e.g. `cdn/client/v1`) | None — see [§ Tier 1 — minor evolution](#tier-1--minor-evolution) |
| **2 — Medium** | Add new optional protocol or gossip payload variants, no ALPN bump | Same | Config-only (opt-in flags), see [§ Tier 2 — medium evolution](#tier-2--medium-evolution) |
| **3 — Major** | Remove a field; change a type; reorder enum variants; add a mandatory field; change signed-field set; change framing | Bumped ALPN or topic | Follow the focused migration ADR shipped with the breaking change |

## Tier 1 and Tier 2 operator checklists

### Tier 1 — minor evolution

A Tier 1 release adds optional fields to existing structs. By construction:

- Wire format stays compatible (postcard two-phase deserialization handles missing trailing extensions).
- ALPN string unchanged.
- Signed field set unchanged ([ADR 013 § Signed Field Freezing](013-schema-evolution.md#signed-field-freezing)) — slash evidence stays verifiable across versions.
- Payment channels and on-chain bindings unaffected.

**Operator action:** Pull and restart at your normal cadence. No drain, flag day, or client coordination. Skipping a Tier 1 release entirely keeps you interoperable indefinitely — peers on the new version just won't see the optional fields you don't emit.

### Tier 2 — medium evolution

A Tier 2 release adds new optional message variants (e.g. a `Ping`/`Pong` keepalive on `cdn/client/v1`). The ALPN string is unchanged; legacy peers that don't understand the new variant close the stream with `UNSUPPORTED_MESSAGE` and the sender falls back ([ADR 013 § Tier 2](013-schema-evolution.md#tier-2--medium-new-message-types-no-alpn-bump)).

**Operator checklist:**

1. **Read the release notes** for new operator-config flags introduced with the variant. Typically opt-in (e.g. `enable_keepalive = true`); defaults are conservative.
2. **Pull and restart at your normal cadence.** Drain only if the release notes say so — most Tier 2 releases need none.
3. **Monitor `decdn_streams_failed_total{reason="protocol_error"}`** for one rolling window after restart. A spike means a peer is rejecting the new variant — expected and benign for legacy peers, but a sustained rate from your own outbound streams suggests config drift.
4. **Configure new metrics** in your dashboard if the release exposes them (canonical registry: [`appendix-observability.md`](appendix-observability.md#appendix-observability-and-metrics)).

Payment channels and stake state are unaffected.

## Tier 3 — major ALPN bump

Tier 3 is a classification boundary, not a standing rollout procedure. The ALPN string changes (`cdn/client/v1` → `cdn/client/v2`), or a gossip topic's semantic contract changes. The accompanying ADR MUST define the supported-version set, deployment order, client behavior, channel or signature migration, rollback conditions, observability, and retirement criteria that the actual break requires.

The current runtime supports the `…/v1` identifiers documented by the protocol ADRs. No transition guarantee exists until a concrete Tier 3 ADR defines and implements it.

## Cross-ADR Impact

- [ADR 013 — Schema Evolution: framing, enum discipline, evolution tiers, and signed-field freezing](013-schema-evolution.md#adr-013-schema-evolution)
- [`appendix-observability.md` — metrics referenced in the Tier 2 checklist](appendix-observability.md#appendix-observability-and-metrics)
