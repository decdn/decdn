# Appendix: Operator Protocol-Upgrade Runbook

> **This is an appendix, not a core protocol ADR.** It is the operator-facing companion to [ADR 013 — Schema Evolution](013-schema-evolution.md). Tier semantics, two-phase deserialization, ALPN negotiation, and the deprecation timeline are defined in ADR 013 — this appendix only sequences the operator actions implied by each tier.

## Context

The protocol evolves under the three-tier scheme in ADR 013 (Tier 1 minor, Tier 2 medium, Tier 3 major). The protocol-level mechanics are well-specified, but the operator-facing question — *what do I do when a release ships?* — is not. New operators reading ADR 013 cold cannot tell which kinds of releases require config edits, when payment channels are at risk, when watchtower relationships need re-coordination, or how to roll an upgrade across a multi-node fleet without dropping traffic.

This runbook fills that gap. It does not redefine any protocol mechanism.

## §1. Tier overview

Per [ADR 013 §Decision](013-schema-evolution.md), each schema change falls into exactly one of three tiers:

| Tier | Trigger | ALPN | Operator action |
|------|---------|------|-----------------|
| **1 — Minor** | Append optional fields to an existing struct via two-phase deserialization | Same (e.g. `cdn/client/v1`) | None — see §2.1 |
| **2 — Medium** | Add new optional message types or gossip envelope versions, no ALPN bump | Same | Config-only (opt-in flags), see §2.2 |
| **3 — Major** | Remove a field; change a type; reorder enum variants; add a mandatory field; change signed-field set; change framing | Bumped (`cdn/client/v1` → `cdn/client/v2`) | Coordinated rolling upgrade, see §3 |

The deprecation timeline in [ADR 013 §Deprecation Timeline](013-schema-evolution.md#deprecation-timeline) gives the production schedule (T+0 release, T+4 weeks adoption target, T+12 weeks old-version removal). Operators MUST plan their fleet upgrades against that schedule.

## §2. Tier 1 and Tier 2 operator checklists

### §2.1 Tier 1 — minor evolution

A Tier 1 release adds optional fields to existing structs. By construction:

- Wire format stays compatible (postcard two-phase deserialization handles missing trailing extensions).
- ALPN string is unchanged.
- Signed field set is unchanged ([ADR 013 §Signed Field Freezing](013-schema-evolution.md)) — slash evidence remains verifiable across versions.
- Payment channels, watchtower escrows, and on-chain bindings are unaffected.

**Operator action:** Pull the new node release at your normal cadence and restart. No drain required. There is no flag day, no deprecation window, and no client coordination.

If you skip a Tier 1 release entirely you remain interoperable indefinitely — peers on the new version simply will not see the optional fields you do not emit.

### §2.2 Tier 2 — medium evolution

A Tier 2 release adds new optional message variants (e.g. a `Ping`/`Pong` keepalive on `cdn/client/v1`) or a new gossip envelope version. The ALPN string is unchanged; legacy peers that don't understand the new variant close the stream with `UNSUPPORTED_MESSAGE` and the sender falls back ([ADR 013 §Tier 2](013-schema-evolution.md)).

**Operator checklist:**

1. **Read the release notes** for any new operator-config flags introduced alongside the new variant. These are typically opt-in (e.g. `enable_keepalive = true`) — defaults are conservative.
2. **Pull and restart at your normal cadence.** Drain is recommended only if the release notes say so. Most Tier 2 releases require none.
3. **Monitor `decdn_streams_failed_total{reason="protocol_error"}`** for one rolling window after the restart. A spike indicates a peer is rejecting the new variant — that is expected and benign for legacy peers, but a sustained rate from your own outbound streams to the new variant suggests a config drift.
4. **Configure new metrics** in your dashboard if the release exposes them (the canonical registry lives in [`appendix-observability.md`](appendix-observability.md)).

Payment channels, watchtower escrows, and stake state are unaffected.

## §3. Tier 3 — major ALPN bump

This is the only tier with cross-fleet coordination cost. The ALPN string changes (`cdn/client/v1` → `cdn/client/v2`); a node running only `v1` becomes invisible to clients that have rotated to `v2`-only after the deprecation window.

**Read [ADR 013 §ALPN Version Negotiation](013-schema-evolution.md#alpn-version-negotiation) before starting** — it explains why both versions can run on a single `iroh::Endpoint` simultaneously, which is the entire basis of the rolling-upgrade procedure below.

### §3.1 Pre-cutover (T+0 to T+4 weeks)

When a Tier 3 release is announced, you have four weeks of dual-version-supported time to migrate. The release ships a node binary that supports both versions; you do not need to do anything on day zero except read the change set.

| Check | Why |
|-------|-----|
| **Config-field deltas.** Diff your operator config against the release's example config. New mandatory fields, renamed fields, or removed fields land in this release. | Tier 3 is the only tier where required config can change. |
| **Payment-channel validity.** Tier 3 changes can include the channel-ID formula or the voucher format. A change here invalidates **open channels** for the affected token/protocol. The release notes will state this explicitly. The contract-level worked example is [ADR 010 §Migration from ADR 003](010-multi-token.md#migration-from-adr-003) — the PoC `StablePaymentChannel` is decommissioned and replaced atomically; existing channels are force-closed via `forceCloseChannel`. | Operators must close out open channels in the old protocol before the cutover window or risk losing payments. |
| **Voucher-signer compatibility.** If the EIP-712 voucher domain or typed-data hash changes (Tier 3 §Signed Field Freezing — modifying the signed field set is a major change), the off-chain voucher signer must be upgraded in lockstep with the node binary. | An old voucher signer producing pre-bump signatures against a new contract will fail `SignatureChecker.isValidSignatureNow` ([ADR 024 §1](024-account-abstraction.md)). |
| **Watchtower re-coordination.** Watchtowers ([ADR 007](007-watchtower.md)) maintain `voucherStateHash` commitments per channel; a Tier 3 voucher-format change means existing watchtower escrows cover obsolete state. New escrows must be opened against the new format, and the watchtower itself must be on a compatible binary. | Watchtower disputes during the transition are otherwise unwinnable. |
| **Reputation / receipt continuity.** Receipts ([ADR 027](027-distinct-client-receipts.md)) are signed by the *requester* key; their verifier lives in the node. A Tier 3 receipt-format change is rare but possible — release notes flag it explicitly. | Operators must coordinate with their downstream receipt-using infrastructure (gauge claim) before the cutover. |

### §3.2 Cutover — rolling upgrade (per node)

The release ships a binary that registers **both** ALPN handlers (`v1` and `v2`) on the same `iroh::Endpoint`, per [ADR 013 §Multi-version support](013-schema-evolution.md). This makes the rolling upgrade per-node and zero-downtime in aggregate.

For each node in the fleet, in any order:

1. **Drain** new inbound connections. The `admin_v1_drain` admin RPC method is the intended invocation surface and is listed in [`appendix-local-admin-http.md`](appendix-local-admin-http.md) but is not yet implemented (tracked as [#244](https://github.com/decdn/decdn/issues/244)); until it ships, drain via your external load balancer (stop forwarding new connections to the node) or block the node's QUIC port at the firewall. Either way, confirm:
   - `decdn_streams_active{direction="inbound"} == 0`
   - `decdn_probe_hold_slots_used == 0` (avoids the phantom-slash window per [ADR 005 §Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold))
2. **Stop** the node.
3. **Upgrade config** with any new mandatory fields identified in §3.1.
4. **Replace the binary** with the new release.
5. **Restart.** Confirm both ALPN handlers are registered (look for `cdn/client/v2` in the node's startup logs alongside the existing `v1` line). `/health` reports `ready`.
6. **Smoke test:**
   - `decdn_quic_0rtt_attempts_total` advancing once 0-RTT clients warm
   - `decdn_streams_completed_total` advancing for both `v1` and `v2` clients
   - `decdn_streams_failed_total{reason="protocol_error"}` not rate-spiking
7. **Un-drain.**
8. **Move to the next node.** There is no inter-node ordering requirement — gossip and probing converge.

If the smoke test fails on the first node, **stop the rollout**, roll the binary back on that node, and investigate. Fleet-wide failures during the dual-version window are recoverable; failures during §3.3 (post-removal) are not.

### §3.3 Post-cutover (T+4 to T+12 weeks)

By T+4 weeks the entire fleet should be running the dual-version binary. Clients begin preferring the new version per [ADR 013 §Deprecation Timeline](013-schema-evolution.md#deprecation-timeline) — your traffic mix will shift toward `v2`.

| Phase | Operator action |
|-------|-----------------|
| **T+4 to T+8 weeks** | Monitor the `decdn_streams_completed_total` mix between `v1` and `v2`. When `v1` traffic drops below ~5% of total, you can plan removal. |
| **T+12 weeks** | The release schedule **may** ship a `v1`-removal binary. Operators upgrade per §3.2 again. After this rollout, peers still running `v1`-only become unreachable. |

**You MUST keep `v1` support until T+12 weeks.** [ADR 013 §Multi-version support](013-schema-evolution.md#alpn-version-negotiation) makes this a protocol-level guarantee: "a node MUST support at least the current and previous major version simultaneously during a transition period." T+12 weeks is the earliest point at which the deprecation timeline permits old-version removal. Operators who unilaterally drop `v1` earlier break the guarantee — legacy clients on the old ALPN see `no_application_protocol` TLS alerts and fall through to the next probe candidate, which is observably indistinguishable from a node outage.

**At T+12 weeks**, the deprecation timeline says old-version support MAY be removed. Whether to actually remove it depends on the operator-visible traffic mix; it is permitted, not required.

## §4. Coordination touchpoints

A Tier 3 upgrade has three out-of-protocol coordination surfaces. None are automated; all are operator responsibilities.

### §4.1 Watchtowers

If you contract with a watchtower ([ADR 007](007-watchtower.md)):

- **Read the watchtower's release notes** alongside the node release. A reputable watchtower publishes its supported protocol versions; do not upgrade your node ahead of your watchtower.
- **For voucher-format changes:** open new watchtower escrows against the new format **before** opening any new payment channels under the new format. Existing escrows for old-format channels remain valid until those channels settle.
- **Heartbeat continuity:** the watchtower's `voucherStateHash` ([ADR 007](007-watchtower.md)) cycles per heartbeat; a watchtower mid-upgrade may briefly publish a hash for a no-longer-canonical state. Tolerate up to one heartbeat window of inconsistency before alarming.

### §4.2 Clients

For Tier 3, clients control the ALPN proposal order. You do not negotiate with them directly, but:

- **Public-facing operators** should coordinate with major client deployments (CDN consumers, not end-users) ahead of the T+0 release. The list of "major clients" is your own operator concern; the protocol does not enumerate them.
- **Probe traffic during the dual-version window** will arrive on both ALPNs. Both must be answered correctly.

### §4.3 Governance

For Tier 3 upgrades that touch governance-controlled parameters (rate bounds in [ADR 003 §Rate Bounds Refresh](003-payments.md), token allowlist in [ADR 010](010-multi-token.md), slashing schedule in [ADR 026 §8](026-gauge-boost-tokenomics.md)):

- The governance proposal that authorises the new behaviour is on its own timeline ([ADR 009](009-governance.md): 7-day vote + 48-hour timelock). The protocol release usually ships **before** the governance vote concludes, with the new behaviour gated on an on-chain flag.
- Operators MUST verify the relevant on-chain governance state before activating the new behaviour locally — consult the release notes for the specific contract call (e.g. `Governance.upgradeActivated(uint256 versionId)` returns true).

## §5. Failure modes and rollback

| Symptom during Tier 3 rollout | Likely cause | Rollback |
|-------------------------------|--------------|----------|
| Restarted node reports `not_ready` for `rate_bounds_loaded` | Tier 3 changed the rate-bounds RPC response format and the node parsed an old-format response | Roll back to the previous stable binary or to the dual-version binary (whichever was last green) to restore service. The governance flag may take up to 48h to flip per [ADR 009](009-governance.md) timelock; do not block availability waiting for it. After the flag flips, redo the §3.2 cutover for that node. |
| `decdn_streams_failed_total{reason="protocol_error"}` spikes after restart | Old peers receiving the new ALPN version's framing | Expected during the dual-version window. Investigate only if rate is sustained from peers known to have already upgraded. |
| `closeChannel` reverts with new contract | Voucher format changed; old vouchers no longer valid against the new `PaymentChannel` | Use `forceCloseChannel` per [ADR 010](010-multi-token.md). The dispute window applies. |
| `decdn_quic_0rtt_rejected_total` spikes after restart | Old session tickets cached by clients are not 0-RTT-replayable on the new ALPN | Self-corrects within one session-ticket lifetime. No action needed. |
| Watchtower disputes a channel mid-upgrade | Watchtower running an older binary saw a new-format voucher and could not parse it | Resolve via the standard counter-evidence path ([ADR 007](007-watchtower.md)); upgrade the watchtower; do not re-open the channel until both sides are on the new format. |
| Mass `closeChannel` calls clog the L2 sequencer | Large fleets force-closing all old-format channels at once | Batch the closes across operators; use `forceCloseChannel` only for tokens that were removed from the allowlist; let unforced channels settle naturally over their `maxChannelDuration` (default 90 days per [ADR 010](010-multi-token.md)). |

## §6. What this runbook does not cover

- **In-place protocol downgrades.** Once a Tier 3 release ships, the deprecation timeline is one-directional. Operator software may roll back to the dual-version binary in an emergency, but the protocol does not support reverting from a `v2`-only binary back to `v1`-only after `v1` has been removed.
- **Cross-chain protocol coordination.** This runbook assumes a single L2 deployment ([`appendix-l2-deployment.md`](appendix-l2-deployment.md)). Future multi-L2 deployments will require a separate per-deployment-coordinator pattern.
- **Client-side migration.** Client behaviour during a Tier 3 upgrade is governed by client deployment policy, not by this runbook. Client smart-wallet upgrades follow [ADR 024](024-account-abstraction.md).

## Cross-references

- [ADR 013 — Schema Evolution: tier semantics, ALPN negotiation, deprecation timeline](013-schema-evolution.md)
- [ADR 010 — Migration from ADR 003: contract-level worked example](010-multi-token.md#migration-from-adr-003)
- [ADR 005 — Probe-Triggered Eviction Hold (drain prerequisite)](005-protocol.md#probe-triggered-eviction-hold)
- [ADR 007 — Watchtower coordination during transitions](007-watchtower.md)
- [ADR 024 — Smart-account verification across versions](024-account-abstraction.md)
- [ADR 027 — Receipt format and gauge-claim continuity](027-distinct-client-receipts.md)
- [`appendix-local-admin-http.md` — `admin_v1_drain` invocation](appendix-local-admin-http.md)
- [`appendix-observability.md` — metrics referenced in the smoke-test checklist](appendix-observability.md)
