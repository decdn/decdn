# Appendix: Operator Protocol-Upgrade Runbook

> **Appendix, not a core protocol ADR.** Operator-facing companion to [ADR 013 — Schema Evolution](013-schema-evolution.md). Tier semantics, two-phase deserialization, ALPN negotiation, and the deprecation timeline are defined in ADR 013; this appendix only sequences the operator actions each tier implies.

## Context

The protocol evolves under the ADR 013 three-tier scheme (Tier 1 minor, Tier 2 medium, Tier 3 major). The mechanics are well-specified; the operator question — *what do I do when a release ships?* — is not (which releases need config edits, when payment channels are at risk, how to roll a fleet upgrade without dropping traffic).

This runbook fills that gap. It does not redefine any protocol mechanism.

## 1. Tier overview

Per [ADR 013 §Decision](013-schema-evolution.md), each schema change is exactly one tier:

| Tier | Trigger | ALPN | Operator action |
|------|---------|------|-----------------|
| **1 — Minor** | Append optional fields to an existing struct via two-phase deserialization | Same (e.g. `cdn/client/v1`) | None — see §2.1 |
| **2 — Medium** | Add new optional message types or gossip envelope versions, no ALPN bump | Same | Config-only (opt-in flags), see §2.2 |
| **3 — Major** | Remove a field; change a type; reorder enum variants; add a mandatory field; change signed-field set; change framing | Bumped (`cdn/client/v1` → `cdn/client/v2`) | Coordinated rolling upgrade, see §3 |

The [ADR 013 §Deprecation Timeline](013-schema-evolution.md#deprecation-timeline) gives the schedule (T+0 release, T+4 weeks adoption target, T+12 weeks old-version removal). Operators MUST plan fleet upgrades against that schedule.

## 2. Tier 1 and Tier 2 operator checklists

### 2.1 Tier 1 — minor evolution

A Tier 1 release adds optional fields to existing structs. By construction:

- Wire format stays compatible (postcard two-phase deserialization handles missing trailing extensions).
- ALPN string unchanged.
- Signed field set unchanged ([ADR 013 §Signed Field Freezing](013-schema-evolution.md)) — slash evidence stays verifiable across versions.
- Payment channels and on-chain bindings unaffected.

**Operator action:** Pull and restart at your normal cadence. No drain, flag day, deprecation window, or client coordination. Skipping a Tier 1 release entirely keeps you interoperable indefinitely — peers on the new version just won't see the optional fields you don't emit.

### 2.2 Tier 2 — medium evolution

A Tier 2 release adds new optional message variants (e.g. a `Ping`/`Pong` keepalive on `cdn/client/v1`) or a new gossip envelope version. The ALPN string is unchanged; legacy peers that don't understand the new variant close the stream with `UNSUPPORTED_MESSAGE` and the sender falls back ([ADR 013 §Tier 2](013-schema-evolution.md)).

**Operator checklist:**

1. **Read the release notes** for new operator-config flags introduced with the variant. Typically opt-in (e.g. `enable_keepalive = true`); defaults are conservative.
2. **Pull and restart at your normal cadence.** Drain only if the release notes say so — most Tier 2 releases need none.
3. **Monitor `decdn_streams_failed_total{reason="protocol_error"}`** for one rolling window after restart. A spike means a peer is rejecting the new variant — expected and benign for legacy peers, but a sustained rate from your own outbound streams suggests config drift.
4. **Configure new metrics** in your dashboard if the release exposes them (canonical registry: [`appendix-observability.md`](appendix-observability.md)).

Payment channels and stake state are unaffected.

## 3. Tier 3 — major ALPN bump

This is the only tier with cross-fleet coordination cost. The ALPN string changes (`cdn/client/v1` → `cdn/client/v2`); a node running only `v1` becomes invisible to clients that have rotated to `v2`-only after the deprecation window.

**Read [ADR 013 §ALPN Version Negotiation](013-schema-evolution.md#alpn-version-negotiation) before starting** — it explains why both versions can run on a single `iroh::Endpoint` simultaneously, the entire basis of the rolling upgrade below.

### 3.1 Pre-cutover (T+0 to T+4 weeks)

A Tier 3 release ships a node binary supporting both versions, giving you four weeks of dual-version time to migrate. On day zero, only read the change set.

| Check | Why |
|-------|-----|
| **Config-field deltas.** Diff your operator config against the release's example config for new mandatory, renamed, or removed fields. | Tier 3 is the only tier where required config can change. |
| **Payment-channel validity.** Tier 3 may change the channel-ID formula or voucher format, invalidating **open channels** for the affected token/protocol (release notes state this). Worked example: [ADR 010 §Migration from ADR 003](010-multi-token.md#migration-from-adr-003) — PoC `StablePaymentChannel` decommissioned and replaced atomically; existing channels force-closed via `forceCloseChannel`. | Close open old-protocol channels before the cutover window or risk losing payments. |
| **Voucher-signer compatibility.** If the EIP-712 voucher domain or typed-data hash changes (Tier 3 §Signed Field Freezing — changing the signed field set is major), upgrade the off-chain voucher signer in lockstep with the node binary. | An old signer's pre-bump signatures fail `SignatureChecker.isValidSignatureNow` against the new contract ([ADR 024 §1](024-account-abstraction.md)). |
| **Local dispute monitor compatibility.** The in-process dispute monitor ([ADR 003](003-payments.md) Option C) reads local-store voucher state and submits `disputeChannel` calls. After a Tier 3 voucher-format change it must be on the new binary before any new-format channels open, else it cannot decode them. It ships with the node binary, so the only operator action is sequencing node and contract upgrades correctly. | Primary stale-close defense ([Appendix: Fraud Detection](appendix-fraud-detection.md)); a stale binary leaves new-format closes unmonitored. |

### 3.2 Cutover — rolling upgrade (per node)

The release ships a binary registering **both** ALPN handlers (`v1` and `v2`) on one `iroh::Endpoint`, per [ADR 013 §Multi-version support](013-schema-evolution.md), making the rolling upgrade per-node and zero-downtime in aggregate.

For each node in the fleet, in any order:

1. **Drain** new inbound connections. The intended surface, `admin_v1_drain`, is listed in [`appendix-local-admin-http.md`](appendix-local-admin-http.md) but not yet implemented; until it ships, drain via your external load balancer (stop forwarding new connections) or block the node's QUIC port at the firewall. Either way, confirm:
   - `decdn_streams_active{direction="inbound"} == 0`
   - `decdn_probe_hold_slots_used == 0` (avoids the phantom-slash window per [ADR 005 §Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold))
2. **Stop** the node.
3. **Upgrade config** with any new mandatory fields from §3.1.
4. **Replace the binary** with the new release.
5. **Restart.** Confirm both ALPN handlers are registered (look for `cdn/client/v2` in startup logs alongside the existing `v1` line). `/health` reports `ready`.
6. **Smoke test:**
   - `decdn_quic_0rtt_attempts_total` advancing once 0-RTT clients warm
   - `decdn_streams_completed_total` advancing for both `v1` and `v2` clients
   - `decdn_streams_failed_total{reason="protocol_error"}` not rate-spiking
7. **Un-drain.**
8. **Move to the next node.** No inter-node ordering requirement — gossip and probing converge.

If the smoke test fails on the first node, **stop the rollout**, roll the binary back on that node, and investigate. Fleet-wide failures during the dual-version window are recoverable; failures during §3.3 (post-removal) are not.

### 3.3 Post-cutover (T+4 to T+12 weeks)

By T+4 weeks the whole fleet should run the dual-version binary. Clients begin preferring the new version per [ADR 013 §Deprecation Timeline](013-schema-evolution.md#deprecation-timeline); traffic mix shifts toward `v2`.

| Phase | Operator action |
|-------|-----------------|
| **T+4 to T+8 weeks** | Monitor the `decdn_streams_completed_total` `v1`/`v2` mix. When `v1` drops below ~5% of total, plan removal. |
| **T+12 weeks** | The release schedule **may** ship a `v1`-removal binary. Upgrade per §3.2 again. After this rollout, peers still running `v1`-only become unreachable. |

**You MUST keep `v1` support until T+12 weeks.** [ADR 013 §Multi-version support](013-schema-evolution.md#alpn-version-negotiation) makes this a protocol guarantee: "a node MUST support at least the current and previous major version simultaneously during a transition period." T+12 weeks is the earliest the deprecation timeline permits old-version removal. Dropping `v1` earlier breaks the guarantee — legacy clients on the old ALPN see `no_application_protocol` TLS alerts and fall through to the next probe candidate, indistinguishable from a node outage.

**At T+12 weeks** old-version support MAY be removed, depending on the operator-visible traffic mix. Permitted, not required.

## 4. Coordination touchpoints

A Tier 3 upgrade has two out-of-protocol coordination surfaces. Neither is automated; both are operator responsibilities.

### 4.1 Clients

For Tier 3, clients control the ALPN proposal order. You don't negotiate with them directly, but:

- **Public-facing operators** should coordinate with major client deployments (CDN consumers, not end-users) ahead of the T+0 release. The "major clients" list is your own concern; the protocol doesn't enumerate them.
- **Probe traffic during the dual-version window** arrives on both ALPNs; both must be answered correctly.

### 4.2 Governance

For Tier 3 upgrades touching governance-controlled parameters (rate bounds in [ADR 003 §Rate Bounds Refresh](003-payments.md), token allowlist in [ADR 010](010-multi-token.md), slashing schedule in [ADR 026 §8](026-gauge-boost-tokenomics.md)):

- The authorizing governance proposal is on its own timeline ([ADR 009](009-governance.md): 7-day vote + 48-hour timelock). The protocol release usually ships **before** the vote concludes, with the new behavior gated on an on-chain flag.
- Operators MUST verify the relevant on-chain governance state before activating the new behavior locally — consult the release notes for the specific contract call (e.g. `Governance.upgradeActivated(uint256 versionId)` returns true).

## 5. Failure modes and rollback

| Symptom during Tier 3 rollout | Likely cause | Rollback |
|-------------------------------|--------------|----------|
| Restarted node reports `not_ready` for `rate_bounds_loaded` | Tier 3 changed the rate-bounds RPC response format; node parsed an old-format response | Roll back to the last-green binary (previous stable or dual-version). The governance flag may take up to 48h to flip per [ADR 009](009-governance.md) timelock; don't block availability waiting for it. After it flips, redo the §3.2 cutover for that node. |
| `decdn_streams_failed_total{reason="protocol_error"}` spikes after restart | Old peers receiving the new ALPN version's framing | Expected during the dual-version window. Investigate only if sustained from peers known to have already upgraded. |
| `closeChannel` reverts with new contract | Voucher format changed; old vouchers invalid against the new `PaymentChannel` | Use `forceCloseChannel` per [ADR 010](010-multi-token.md). The dispute window applies. |
| `decdn_quic_0rtt_rejected_total` spikes after restart | Old client-cached session tickets are not 0-RTT-replayable on the new ALPN | Self-corrects within one session-ticket lifetime. No action needed. |
| Mass `closeChannel` calls clog the L2 sequencer | Large fleets force-closing all old-format channels at once | Batch closes across operators; use `forceCloseChannel` only for tokens removed from the allowlist; let unforced channels settle naturally over their `maxChannelDuration` (default 90 days per [ADR 010](010-multi-token.md)). |

## 6. What this runbook does not cover

- **In-place protocol downgrades.** The Tier 3 deprecation timeline is one-directional. Operator software may roll back to the dual-version binary in an emergency, but reverting a `v2`-only binary to `v1`-only after `v1` removal is unsupported.
- **Cross-chain protocol coordination.** Assumes a single L2 deployment ([`appendix-l2-deployment.md`](appendix-l2-deployment.md)). Multi-L2 deployments will need a separate per-deployment-coordinator pattern.
- **Client-side migration.** Governed by client deployment policy, not this runbook. Client smart-wallet upgrades follow [ADR 024](024-account-abstraction.md).

## Cross-ADR Impact

- [ADR 013 — Schema Evolution: tier semantics, ALPN negotiation, deprecation timeline](013-schema-evolution.md)
- [ADR 010 — Migration from ADR 003: contract-level worked example](010-multi-token.md#migration-from-adr-003)
- [ADR 005 — Probe-Triggered Eviction Hold (drain prerequisite)](005-protocol.md#probe-triggered-eviction-hold)
- [Appendix: Fraud Detection — local dispute monitor and permissionless challengers](appendix-fraud-detection.md)
- [ADR 024 — Smart-account verification across versions](024-account-abstraction.md)
- [`appendix-local-admin-http.md` — `admin_v1_drain` invocation](appendix-local-admin-http.md)
- [`appendix-observability.md` — metrics referenced in the smoke-test checklist](appendix-observability.md)
