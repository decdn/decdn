# ADR 030: Node Region Self-Attestation

**Date:** 2026-05-14
**Status:** Draft

## Context

Each node declares a region — an ISO 3166-1 alpha-2 country code — at registration as `regionHint` on `CapacityBond` (see [ADR 001 § Node Region Metadata](001-network.md#node-region-metadata)). The `CapacityBond` registry refresh resolves the active set into a `NodeId → region` map. The declared region is load-bearing in two places:

- **Regional blacklist enforcement.** A node applies only blacklist entries that are global or match its declared region ([ADR 011 § Regional Scope](011-content-takedown.md#regional-scope)).
- **Peer-selection penalty.** A pulling node applies the RTT-vs-claim reputation penalty against a node whose measured latency contradicts its declared region ([ADR 001 § Node Selection Algorithm](001-network.md#node-selection-algorithm), implemented in `crates/node/src/node_origin/mod.rs`).

Three ADRs ([001 § Consequences](001-network.md#consequences), [011 § Regional Scope](011-content-takedown.md#regional-scope), [019 § Deferred & Open](019-node-onboarding.md#deferred--open)) name "a decentralized oracle or third-party attestation service" as the production mitigation against region misreporting, but specify no design — oracle choice, IP→region resolution, mismatch handling, and operator UX are all open. This gap is production-blocking: without a decision, regional compliance under [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting) is structurally fragile, and the reactive blacklist-scope flip ([ADR 011 § Regional Scope](011-content-takedown.md#regional-scope)) remains open.

This ADR closes the gap by **rejecting** the oracle/attestation-service path and committing to self-attested regions as the production posture, with one protocol-level hardening — a region-change stability window — that removes the region-flip surface that does not self-correct under the existing latency/reputation loop.

## Decision

### Self-attestation is canonical

A node's region is whatever the operator declares — at `CapacityBond.registerNode` and in `CapacityBond.updateRegion` calls ([§ Region-stability window](#region-stability-window)). The protocol accepts it at face value: no on-chain IP-geolocation cross-check, no required third-party attestation signature. The contract stores `regionHint` as a `string` and validates only its length ([§ Region-stability window](#region-stability-window)); it does not verify that the code names the node's physical location.

### Soft mitigation: latency-vs.-claim reputation penalty (canonical)

The penalty in [ADR 001 § Consequences](001-network.md#consequences) — a pulling node applies a reputation penalty when observed latency contradicts the claimed region (default heuristic: RTT > 150ms to a node in the same claimed region) — is the canonical continuous mitigation. It is self-correcting: a misdeclaring node loses payouts proportional to how badly its declared region contradicts measured RTT, with no new protocol surface. This ADR does not respecify the threshold, sample size, or decay curve; that tuning is a follow-up in the [ADR 008](008-reputation.md#adr-008-reputation-system) reputation domain, not a precondition here.

### Region-stability window

One gaming surface is not addressed by the latency/reputation loop, because it pays off the instant a region is changed — faster than RTT sampling can tax the new claim:

- **Reactive blacklist-scope flipping** ([ADR 011 § Regional Scope](011-content-takedown.md#regional-scope)): a node serving a hash flips `regionHint` out of the entry's region the moment a regional blacklist entry lands, shedding the obligation to drop the content.

A single protocol invariant — a region-change stability window — closes it.

**CapacityBond interface delta:**

- `regionLastChanged: uint64` (Unix seconds, matching `firstBondedAt` and `lastMultiaddrUpdate` in [ADR 003 § Data Structure](003-payments.md#data-structure)). Set to `block.timestamp` by `registerNode` and restamped on every successful `updateRegion`, so it is never zero for an active node and always names the instant the operator's current region took effect. Both the ripening window and the `updateRegion` cooldown run from it directly.
- `regionPrev: string` — the region in effect immediately before the most recent region change. `updateRegion` and `registerNode` set it to the prior `regionHint` when the region changes. A re-registration into the same region keeps it. Read only while a change is unripened (`block.timestamp - effective < REGION_STABILITY_WINDOW`); ignored once the window elapses. Shares the `string` shape of `regionHint` (below).
- `updateRegion(string newRegion)`: mirrors the operator-callable, no-bond shape of `updateMultiaddrs` ([ADR 019 § NAT and Multiaddr Handling](019-node-onboarding.md#nat-and-multiaddr-handling); cooldown semantics in [ADR 003 § Multiaddr Update Policy](003-payments.md#multiaddr-update-policy)) — caller must be the operator's registered Ethereum address. **Reverts if `block.timestamp - regionLastChanged < REGION_STABILITY_WINDOW`** (cooldown equals the stability window). Because `regionLastChanged` is stamped at registration, the cooldown applies uniformly from the moment the current region took effect — an operator must let a region stabilize for a full window before changing it, the first change included. This keeps at most one unripened change in flight at a time, so the single `regionPrev` slot is unambiguous: a reactive flip after an entry lands keeps the previous region's entries applying (via `regionPrev`) for a full `REGION_STABILITY_WINDOW`, and a second flip cannot begin until the first has ripened and those entries have aged out of scope. On success it sets `regionPrev = <prior regionHint>`, `regionHint = newRegion`, `regionLastChanged = block.timestamp`, and emits `RegionUpdated(bytes32 indexed nodeId, string oldRegion, string newRegion)` — `indexed nodeId` matches every other `CapacityBond` event ([ADR 003 § Interface](003-payments.md#interface-additions-to-capacitybond)); the block timestamp is implicit in the log header, so a separate parameter would be redundant.
- `registerNode` applies the same cooldown to a region change. A re-registration into a region other than the current `regionHint` reverts `RegionCooldownActive` while `block.timestamp - regionLastChanged < REGION_STABILITY_WINDOW`. Without this rule, two `deregisterNode` / `registerNode` cycles in one block move the region twice and push the region held before the flip out of both slots. A first registration has no prior region (`regionLastChanged == 0`), so the cooldown does not apply to it.
- `regionHint` keeps its `string` type and semantics, untouched. Blacklist entries are keyed by a `bytes32` packed region key ([ADR 011 § Region representation](011-content-takedown.md#contract-contentblacklist)); the conversion happens at the scope-check boundary, where the operator's declared string is packed left-aligned into `bytes32` and compared against the entry key. Keeping `CapacityBond` on `string` costs one packing step per check and keeps the operator-facing field ([ADR 003 § Data Structure](003-payments.md#data-structure)) human-readable in registry reads and events.

The amendment keys off `regionLastChanged` directly as the `effective` timestamp — it is always set (at registration, restamped on each change), so no fallback is needed:

```
effective = operator.regionLastChanged
```

**Blacklist scope** ([ADR 011 § Regional Scope](011-content-takedown.md#regional-scope)). A node is in scope for an entry iff the entry is global, OR `entry.region == regionHint`, OR (within the ripening window AND `entry.region == regionPrev`). The window is evaluated at **`block.timestamp`** (`block.timestamp - effective < REGION_STABILITY_WINDOW`), because the predicate answers a present-tense question: what must the node apply and refuse to serve now. `ContentBlacklist.isHashBlacklistedForOperator` evaluates this predicate on-chain, and a node evaluates the same predicate against its local deny-set ([ADR 011 § Enumerating the deny-set](011-content-takedown.md#enumerating-the-deny-set)).

A region change adds the new region's entries immediately but does not drop the previous region's entries until the change ripens. Combined with the `updateRegion` cooldown, a reactive flip after an entry lands buys nothing for a full `REGION_STABILITY_WINDOW`: the node must keep refusing the previous region's entries for that time.

`REGION_STABILITY_WINDOW = 7 days` (governable, hard bounds `[3d, 30d]` per the [ADR 009](009-governance.md#adr-009-governance-model) safety-bound pattern). The *blacklist-scope* amendment is strict with no discretion path: the previous region's entries simply keep applying until the change ripens.

**Threshold rationale.** Seven days is short enough not to penalise an honest relocator unduly, and long enough that flipping region to shed a regional entry's obligations costs seven days of unservable old-region content plus seven days of latency-vs.-claim reputation in the *target* region. The 3-day lower bound prevents governance from defanging the invariant.

### Misdeclaration is operator legal exposure, not a protocol offense

The protocol provides regional blacklist scoping as a *compliance affordance for honest operators*, not forced enforcement against dishonest ones. An operator who misreports region to evade a regional takedown remains the physical party subject to their jurisdiction's content law — courts reach the operator through staked TOKEN, registered Ethereum address, and registered multiaddrs without protocol-level geolocation. This is the posture every L1 takes toward miner/validator location: protocol provides mechanism, jurisdictions provide enforcement.

The residual exposure decomposes into two cases:

- **Reactive misdeclaration (flip after an entry lands).** Foreclosed by [§ Region-stability window](#region-stability-window): the previous region's entries persist for a full `REGION_STABILITY_WINDOW`, and `updateRegion` is cooldown-gated.
- **Pre-positioned misdeclaration (false region from registration, before any entry exists).** *Not* prevented, and the window does not claim to. A node that has always declared `Y` while physically in `X` is, under [ADR 011 § Regional Scope](011-content-takedown.md#regional-scope), simply out of scope for `X`-only entries. Its deny-set never holds region-scoped `X` takedowns.

Backstops for the pre-positioned case are deliberately off-protocol: (a) declared-jurisdiction legal exposure; and (b) the latency-vs.-claim reputation penalty ([§ Soft mitigation: latency-vs.-claim reputation penalty (canonical)](#soft-mitigation-latency-vs-claim-reputation-penalty-canonical)) taxing the false `Y` claim continuously. Region misreporting is not a protocol offense: there is no protocol-level ground truth to measure it against, and inventing one would require the oracle dependency this ADR rejects.

### Privacy disposition unchanged

[ADR 017](017-privacy.md#adr-017-privacy-analysis) classifies self-reported region (P-03, P-12, P-13) as accepted T2 exposure: intentionally public for client selection, inherent to the discoverability of a CDN serving regional clients. This ADR does not change that. `regionLastChanged` is a timestamp, not new content, and is already implicit in the on-chain transaction history of `registerNode` / `updateRegion`; surfacing it as a structured field is a readability change, not a leakage change.

## Consequences

### Positive

- Closes the region-attestation gap with a concrete decision, resolving the "production mitigation: oracle" passages in [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh), [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting), and [ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow).
- No new external trust root and no new external dependency.
- Reactive blacklist-scope flipping becomes a protocol invariant rather than an unmitigated gap.
- Operator UX unchanged for honest deployments — VPN, anycast, mobile, and multi-region operators are not penalized by an IP-geolocation gate that would systematically misclassify them.
- Compatible with the existing on-chain surface: `updateRegion` follows the `updateMultiaddrs` shape ([ADR 019 § NAT and Multiaddr Handling](019-node-onboarding.md#nat-and-multiaddr-handling)), and `regionLastChanged` adopts the `uint64` timestamp shape of `lastMultiaddrUpdate` ([ADR 003 § Data Structure](003-payments.md#data-structure)). `registerNode` writes both. Every successful `updateRegion` restamps `regionLastChanged`, as `updateMultiaddrs` restamps `lastMultiaddrUpdate`.

### Negative

- The protocol cannot prevent a **pre-positioned** misdeclaring node (false region since registration, before any entry exists) from staying out of scope for the real region's entries. Reactive flipping is foreclosed by [§ Region-stability window](#region-stability-window), but a persistently false declaration is accepted at face value — the residual risk this ADR consciously accepts. Backstops are off-protocol (declared-jurisdiction legal exposure plus the continuous latency-vs.-claim reputation penalty): [ADR 011 § Regional Scope](011-content-takedown.md#regional-scope) routes blacklist scope under the *declared* region, which by construction never reaches a region the operator never declared.
- The 7-day window slightly raises the cost of legitimate relocation: an operator who moves keeps the previous region's blacklist obligations until the change ripens. It also means a freshly-registered operator waits one window before their first `updateRegion` — correcting a mis-entered region is not instant — the accepted cost of a uniform cooldown with no first-change carve-out.

### Risks

- **Coordinated region-spoofing attack on a region's reputation.** A fleet of misdeclaring nodes could depress the region's measured reputation. Bounded by (a) the latency penalty (attacker reputation collapses as fast as RTTs are sampled), (b) the capacity-bond cost per node ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)), and (c) the on-chain registration transaction cost per node. Not free; self-correcting.
- **Governance-set `REGION_STABILITY_WINDOW` drift.** Lowering the window toward the 3-day floor weakens the blacklist-scope protection. The hard bounds `[3d, 30d]` per [ADR 009](009-governance.md#adr-009-governance-model) make this an above-the-line governance question rather than a silent regression; the 3-day floor keeps a flip uneconomic at the worst legal setting (see Threshold rationale).

## Cross-ADR Impact

- Tightening the [ADR 001 § Consequences](001-network.md#consequences) latency-vs-claim reputation penalty (threshold, sample size, window, decay) is in scope for the [ADR 008](008-reputation.md#adr-008-reputation-system) reputation domain, not blocked by this ADR.
- Contract-level `regionLastChanged` / `regionPrev` / `updateRegion` (incl. the stability-window cooldown) land with the `CapacityBond` implementation; the [§ Region-stability window](#region-stability-window) interface spec is the canonical reference for that work.
