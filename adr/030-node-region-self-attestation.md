# ADR 030: Node Region Self-Attestation

**Date:** 2026-05-14
**Status:** Draft
**Touches:** [ADR 001](001-network.md), [ADR 008](008-reputation.md), [ADR 009](009-governance.md), [ADR 011](011-content-takedown.md), [ADR 012](012-client.md), [ADR 017](017-privacy.md), [ADR 019](019-node-onboarding.md), [ADR 026](026-gauge-boost-tokenomics.md), [ADR 028](028-slashing-appeals.md)

## Context

Each node declares a region — an ISO 3166-1 alpha-2 country code — at registration (`regionHint` on `StakingRegistry`) and in every gossiped `NodeAnnounce` (the `region` field on the signed body; see [ADR 001 § NodeAnnounce](001-network.md)). The declared region is load-bearing in four places:

- **Regional blacklist enforcement.** A node only applies blacklist entries that are global or match its declared region ([ADR 011 § Regional Scope](011-content-takedown.md#regional-scope)), and is only slashable for serving a hash blacklisted in its declared scope ([ADR 011 § Slashing — Regional slash eligibility](011-content-takedown.md#slashing)).
- **Gossip topic routing.** Nodes publish to the regional topic `cdn/region/{cc}/v1` in addition to the global topic ([ADR 001 § Gossip Topics](001-network.md)).
- **Peer-selection geo-diversity.** Clients prefer geographically diverse providers when other ranking factors tie ([ADR 001 § Selection](001-network.md), implemented in `crates/node/src/selection.rs`).
- **Appeals standing.** Operator standing under [ADR 011 § Blacklist Entry Appeals — Standing](011-content-takedown.md#standing) path 2 is gated on `node.region` matching the disputed entry's region.

Three ADRs ([001 § Consequences](001-network.md#consequences), [011 § Regional Scope](011-content-takedown.md#regional-scope), [019 § Future Work](019-node-onboarding.md#future-work)) name "a decentralized oracle or third-party attestation service" as the production mitigation against region misreporting but provide no design — choice of oracle, IP→region resolution, mismatch handling, slashing implications, and operator UX are all unspecified. Issue #400 tracks the gap as `prod-blocking` / `missing-decision`. Without a decision, regional compliance under [ADR 011](011-content-takedown.md) is structurally fragile and the appeals-standing flip surface called out in [ADR 011 § Standing](011-content-takedown.md#standing) remains an open gaming vector.

This ADR closes the gap by **rejecting** the oracle/attestation-service path and committing to self-attested regions as the production posture, with one protocol-level hardening to remove the appeals-standing flip surface that does not self-correct under the existing latency/reputation feedback loop.

## Decision

### 1. Self-attestation is canonical

A node's region is whatever the operator declares — at `StakingRegistry.registerNode` time, in subsequent `StakingRegistry.updateRegion` calls (§ 3 below), and in every signed `NodeAnnounce` body. The protocol accepts the declaration at face value. There is no on-chain or in-gossip IP-geolocation cross-check, and no required signature from any third-party attestation service. The gossip-layer validation in `crates/gossip/src/validation.rs` remains syntactic only (ISO 3166-1 alpha-2 shape; two ASCII uppercase letters from a known code set per [ADR 001](001-network.md)) — no semantic geolocation gate is added.

### 2. Soft mitigation: latency-vs.-claim reputation penalty (canonical)

The reputation penalty described in [ADR 001 § Consequences](001-network.md#consequences) — clients apply a reputation penalty when observed latency contradicts the claimed region (default heuristic: RTT > 150ms to a node in the same claimed region) — is the canonical continuous mitigation. This ADR does not respecify the threshold, sample size, or decay curve; any tightening of those parameters is a follow-up in the [ADR 008](008-reputation.md) reputation domain and not a precondition for closing #400. The penalty is self-correcting (a misdeclaring node loses payouts proportional to how badly its declared region contradicts measured RTT) and requires no new protocol surface.

### 3. Appeals-standing hardening: `regionLastChanged` + 7-day window

The only gaming surface that the latency/reputation loop does not address is **appeals-standing flipping** ([ADR 011 § Standing](011-content-takedown.md#standing) path 2): an operator can flip `regionHint` immediately before filing a blacklist appeal to gain standing in any region. Today this is restrained by multisig discretion ("heightened scrutiny on intake") — a norm, not a protocol invariant.

This ADR adds a protocol invariant:

**StakingRegistry interface delta:**

- New field on the operator record: `regionLastChanged: uint64` (Unix timestamp in seconds). Set to `block.timestamp` on `registerNode` and on every successful `updateRegion`. Read-only otherwise.
- New operator-facing call: `updateRegion(string newRegion)`. Mirrors the operator-callable, no-bond shape of the existing `updateMultiaddrs` ([ADR 019 § NAT and Multiaddr Handling](019-node-onboarding.md#nat-and-multiaddr-handling); cooldown semantics in [ADR 001 § Multiaddr Update Policy](001-network.md#multiaddr-update-policy)) — caller must be the operator's registered Ethereum address. No analogous per-call cooldown is specified for `updateRegion`; the 7-day `REGION_STABILITY_WINDOW` (§ below) is the only timing gate. Emits `RegionUpdated(nodeId, oldRegion, newRegion, timestamp)`.
- Existing operator record field `regionHint` keeps its current type and semantics; this ADR adds the timestamp alongside it. No migration of pre-existing records — `regionLastChanged` for legacy records defaults to `firstRegisteredAt`, which is the most conservative reading (region has been stable since registration).

**Appeals-standing eligibility under [ADR 011 § Standing](011-content-takedown.md#standing) path 2** is amended to require:

```
block.timestamp - operator.regionLastChanged >= REGION_STABILITY_WINDOW
```

with `REGION_STABILITY_WINDOW = 7 days` (governable, hard bounds `[1d, 30d]` per the [ADR 009](009-governance.md) safety-bound pattern). Filings inside the 7-day window do not auto-revert — they remain admissible at multisig discretion only, preserving the existing soft-norm fallback for legitimate post-migration filers (datacenter relocation, ISP change) while making the protocol invariant strict by default. The contract enforces the strict path; the multisig discretion path remains an off-chain governance norm and is unchanged by this ADR.

**Threshold rationale.** Seven days is short enough that an honest operator who genuinely relocates is not locked out of regional appeals standing for more than a week, and long enough that the cost of flipping region purely to gain standing — losing seven days of latency-vs.-claim reputation in the *target* region while the timestamp ripens — is comparable to the bond required to file in the first place. The 30-day upper bound matches the [ADR 028 § 5](028-slashing-appeals.md#5-hard-caps-and-frequency-limits) `APPEAL_FILING_WINDOW` default; the 1-day lower bound prevents governance from defanging the invariant entirely. Interaction with [ADR 011 § Filing window](011-content-takedown.md#filing-window): `BLACKLIST_APPEAL_FILING_WINDOW` defaults to 14 days with hard bounds `[3d, 30d]`. The strict 7-day check sits inside the default filing window, but governance tightening the filing window below 7 days (permitted by its lower bound) creates a span where path-2 strict-rule standing is unavailable even though the filing window is still open; sub-window filings remain admissible at multisig discretion only, preserving the soft-norm fallback.

### 4. Misdeclaration is operator legal exposure, not a protocol offense

The protocol provides regional blacklist scoping as a *compliance affordance for honest operators*, not as forced enforcement against dishonest ones. An operator who misreports their region to evade a regional takedown remains the physical party subject to their actual jurisdiction's content law — courts reach the operator through their staked TOKEN, registered Ethereum address, and registered multiaddrs without needing protocol-level geolocation. This is the same posture every L1 takes toward miner/validator location: the protocol provides mechanism; jurisdictions provide enforcement.

The incentive analysis cuts in the protocol's favor on this:

- **Misdeclaration *expands* the operator's slashing surface.** By declaring region `X`, an operator opts into `X`'s regional blacklist under [ADR 011 § Slashing — Regional slash eligibility](011-content-takedown.md#slashing). A US-physical operator who declares EU to evade DMCA is now slashable under both EU regional blacklist entries *and* the global blacklist, while remaining legally exposed to DMCA via their physical operations. Net effect: more slashing surface, not less.
- **Latency penalty taxes the misdeclarer in their target region.** Misdeclared region degrades latency-vs.-claim reputation in the target region, depressing payouts there. The misdeclarer pays for their bad claim every minute they hold it.
- **Appeals-standing flip is foreclosed by § 3 above.**

These are the operationally relevant misdeclaration cases. No new slashing offense for "region misreporting" itself is introduced — there is no protocol-level ground truth against which to slash, and inventing one would require importing exactly the oracle dependency this ADR rejects.

### 5. Privacy disposition unchanged

[ADR 017](017-privacy.md) classifies self-reported region (P-03, P-12, P-13) as accepted T2 exposure: intentionally public for client selection, inherent to the discoverability of a CDN serving regional clients. This ADR does not change that disposition. No new privacy surface is introduced — `regionLastChanged` is a timestamp, not new content, and is already implicit in the on-chain transaction history of `registerNode` / `updateRegion`. Surfacing it as a structured field is a readability change, not a leakage change.

## Consequences

**Positive:**

- Closes #400 with a concrete decision; supersedes the three "future work / production mitigation: oracle" passages in [ADR 001](001-network.md), [ADR 011](011-content-takedown.md), and [ADR 019](019-node-onboarding.md).
- No new external trust root and no new external dependency.
- Appeals-standing flipping becomes a protocol invariant rather than a multisig norm.
- Operator UX unchanged for honest deployments — VPN, anycast, mobile, and multi-region operators are not penalized by an IP-geolocation gate that would systematically misclassify them.
- Compatible with the existing on-chain surface: `updateRegion` follows the operator-callable, no-bond shape of the existing `updateMultiaddrs` mutator ([ADR 019 § NAT and Multiaddr Handling](019-node-onboarding.md#nat-and-multiaddr-handling)), and `regionLastChanged` adopts the timestamp-field storage shape already used by `firstRegisteredAt` on the operator record ([ADR 019 § Re-Onboarding after Deregistration or Auto-Ejection](019-node-onboarding.md#re-onboarding-after-deregistration-or-auto-ejection)) — with opposite mutability semantics (updated on every successful `updateRegion`, vs. `firstRegisteredAt`'s write-once-on-first-registration).

**Negative:**

- The protocol cannot prevent a transiently misdeclaring node from serving regionally-blacklisted content before the latency penalty and/or legal channels catch up. This is the residual risk this ADR consciously accepts; ADR 011 § Slashing already routes the post-hoc consequence (regional slash eligibility under the *declared* region).
- The multisig-discretion fallback for sub-7d appeal filings remains an off-chain norm. Operators with legitimate post-relocation filings still depend on multisig judgement during that window.
- The 7-day stability window slightly raises the cost of legitimate operator relocation when it overlaps with a regional appeal — an operator who relocates and *then* wants to file in the new region must wait out the window or accept multisig-discretion intake.

**Risks:**

- **Coordinated region-spoofing attack on a region's reputation.** A fleet of misdeclaring nodes could pollute regional gossip topics and depress the region's measured reputation. Bounded by (a) the latency penalty (the attacker's reputation collapses as fast as their RTTs are sampled), (b) the cost of staking each node ([ADR 026 § 7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) minimum stake), and (c) the gossip-bandwidth cost. Not free; self-correcting.
- **Governance-set `REGION_STABILITY_WINDOW` drift.** Governance lowering the window toward the 1-day floor weakens the appeals-standing protection. The hard bounds `[1d, 30d]` per [ADR 009](009-governance.md) safety-bound pattern make this an above-the-line governance question rather than a silent regression.

## Alternatives Considered

- **IP-geolocation oracle (Chainlink Functions, governance-approved oracle set, or similar).** Rejected. (a) Introduces a centralized trust root — the oracle operator(s) become a choke point that can blackhole or misclassify a node's region; (b) systematically misclassifies legitimate deployments behind VPN, anycast, IXP relays, or mobile/cellular allocations; (c) creates a net-new external dependency for the codebase (no oracle infrastructure exists in `crates/incentive/` or the contracts directory today); (d) does not produce a strictly better signal than the latency/reputation loop already in place — IP-geolocation databases are themselves imperfect heuristics over BGP allocations.
- **Required attestation at announce time (every `NodeAnnounce` carries a fresh oracle-signed attestation; gossip drops announces without one).** Rejected for the same reasons plus a harder operational floor — a brief oracle outage now causes every node's announces to age out, partitioning the gossip mesh until the oracle recovers.
- **Peer-witnessed latency challenge with on-chain dispute (mirror [ADR 028 § 3](028-slashing-appeals.md#3-eligibility-and-evidence-standard) evidence-bundle pattern for region claims).** Rejected as overengineered. The same latency signal is already used at lower cost by the [ADR 001 § Consequences](001-network.md#consequences) reputation penalty; promoting it to an on-chain adjudication path adds bond economics, multisig load, and a new ratification window without changing the operational outcome (a misdeclaring node already loses payouts under the soft path).
- **Scoping-only ADR (document the requirements/interface, defer mechanism).** Rejected — this is the posture issue #400 already objects to, and re-issuing it under a new ADR number does not close the gap.

## Forward references

- Any tightening of the [ADR 001 § Consequences](001-network.md#consequences) latency-vs.-claim reputation penalty (threshold, sample size, aggregation window, decay) is in scope for the [ADR 008](008-reputation.md) reputation domain and not blocked by this ADR.
- The contract-level implementation of `regionLastChanged` and `updateRegion` lands when the `StakingRegistry` contract is implemented; the interface spec in § 3 above is the canonical reference for that work.
