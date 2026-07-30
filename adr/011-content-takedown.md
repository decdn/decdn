# ADR 011: Content Takedown and Hash Blacklisting

**Date:** 2026-03-30
**Status:** Accepted

## Context

deCDN is delivery infrastructure optimised for regional performance, not a censorship-resistant storage network. Node operators are businesses with real legal obligations: DMCA safe harbour (US), DSA hosting provider duties (EU), and national laws around illegal content (CSAM, terrorist material) require that operators have a working takedown mechanism. Without one, every operator runs uninsured legal exposure.

Origin assignment is the symmetric problem: which operators are authorized to act as origins for which content. Without a positive authority, origin assignment is purely off-protocol — content owners self-coordinate, the network has no Sybil resistance on origin claims, and there is no protocol-enforced redundancy for important content. Both halves of origin governance — negative (blacklisting) and positive (assignment) — share infrastructure (cross-contract integration, governance authority, runtime enforcement) and are specified together here.

No existing ADR addressed either question. This ADR establishes:

1. A governance-controlled on-chain hash blacklist
2. Regional governance bodies for jurisdiction-scoped takedowns
3. Node behavior when a hash or origin is blacklisted
4. An emergency fast-path for time-critical removals
5. The slashing regime for non-compliance
6. The known limitations of hash-based blacklisting and the mitigations available
7. A governance-controlled positive authority for origin assignment (`OriginAssignment`) — publisher-propose / DAO-ratify per registered namespace; namespace 0 (`namespaceId == 0`) has no authorized origins — built on the publisher/namespace identity primitive defined in [ADR 002](002-content-addressing.md#publisher-identity-and-namespaces)

## Decision

Content governance over origins has two symmetric authorities, both DAO-controlled:

- **Negative authority — `ContentBlacklist`.** Removes hashes and operators via two governance paths: a global path (network-wide removal) and a regional path (jurisdiction-scoped removal via a designated regional governance body). Nodes must evict blacklisted content and stop announcing it within a defined compliance window; serving a blacklisted hash after the compliance window is a slashable offense. Origin nodes that repeatedly source blacklisted content can themselves be blacklisted by NodeId or operator address, independent of any specific hash — the primary mitigation for hash evasion via trivial re-encoding.
- **Positive authority — `OriginAssignment`.** Authorizes specific operators to act as origins for specific namespaces (defined in [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)). Publishers propose operator sets; governance ratifies each proposal via the standard timelock path. Namespace 0 (`namespaceId == 0`) has no authorized origins — content under it is served best-effort from cache/DHT per [ADR 002 § Retrieval by namespace](002-content-addressing.md#retrieval-by-namespace). `ContentBlacklist` and `OriginAssignment` integrate via runtime checks with lazy storage cleanup — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist).

Each node also maintains a local denylist for operator-initiated removal without waiting for governance.

## Contract: ContentBlacklist

```solidity
interface IContentBlacklist {
    // Global governance path (standard voting + timelock) — GOVERNANCE_ROLE
    function addHashGlobal(bytes32 blake3Hash, string calldata reason) external;
    function removeHashGlobal(bytes32 blake3Hash) external;

    // Regional governance path — REGIONAL_BODY_ROLE, callable only by a
    // registered regional body. Both revert on the global sentinel, so a
    // regional body can never reach a global entry.
    function addHashRegional(bytes32 region, bytes32 blake3Hash, string calldata reason) external;
    function removeHashRegional(bytes32 region, bytes32 blake3Hash) external;

    // Operator blacklisting — global governance only
    function addOperator(address operator) external;
    function removeOperator(address operator) external;

    // Emergency multisig path (3-of-5, no timelock) — hash and origin.
    // Permanent (no sunset): unlawful-content removal discharges an ongoing
    // legal duty — see ADR 009, Emergency Multisig (capability-split sunset).
    // Only the protocol-wide pause sunsets at 12 months, not this path.
    // Emergency adds are GLOBAL by construction, hence no region parameter.
    // ONE-WAY: both revert if the target is already blacklisted by governance
    // (a hash entry with emergency == false, or an origin with no expiry
    // record). The emergency path may only ADD enforcement, never weaken it —
    // otherwise re-adding a governance entry would arm auto-expiry on it and
    // hand the multisig a delayed removeHashGlobal, which is GOVERNANCE_ROLE
    // only and reachable by no other multisig route. Re-adding over an existing
    // EMERGENCY entry stays permitted (category escalation, or re-arming one
    // that lapsed) — that sustains a takedown rather than undoing one.
    // Category determines emergency entry expiry:
    //   GENERAL — 14-day auto-expiry (default)
    //   CSAM, TERRORIST — 90-day auto-expiry (severe content must not be re-exposed due to governance latency)
    // enum Category { GENERAL, CSAM, TERRORIST }
    function emergencyAdd(bytes32 blake3Hash, uint8 category, string calldata reason) external;
    function emergencyAddOrigin(address operatorAddress, uint8 category, string calldata reason) external;

    // Emergency entries expire after their category-specific deadline unless ratified by governance.
    // Expiry is derived from the entry's addedAt timestamp: addedAt + expiryForCategory(category).
    // isHashBlacklisted* views return false after this deadline unless a governance addHashGlobal
    // has been called for the same hash — re-adding under governance clears the
    // `emergency` flag, which is what makes the entry permanent.
    //
    // Permissionless materialization of that expiry. Reverts unless the entry is
    // an emergency entry past its deadline. Deleting the entry is what advances
    // getBlacklistVersion() and emits HashRemoved — without it the enforced set
    // would shrink with the counter frozen, and a delta-polling node would never
    // learn (see § Blacklist version). The origin variant is the only signal at
    // all on its side, since OriginBlacklistUpdated carries no version.
    function expireEmergencyEntry(bytes32 region, bytes32 blake3Hash) external;
    function expireEmergencyOrigin(address operatorAddress) external;

    // Compliance-window governance (see § Compliance Window). Both bounded to
    // [1 hour, 7 days]; the value is stamped onto an entry at add time, so a
    // change never moves the boundary for entries already added.
    function setComplianceWindow(uint64 newWindow) external;
    function setEmergencyComplianceWindow(uint64 newWindow) external;

    // Regional body registry — global governance only, except suspendRegionalBody
    // (emergency multisig). A body is bound to exactly one region and may only
    // write entries for that region: REGIONAL_BODY_ROLE alone is not authority.
    // `emergencyMultisig` names the EMERGENCY_MULTISIG_ROLE holder to check
    // signer-disjointness against; it is verified to hold the role, so it cannot
    // be pointed at a decoy (see § Signer non-overlap).
    function registerRegionalBody(bytes32 region, address body, address emergencyMultisig) external;
    function deregisterRegionalBody(bytes32 region) external;
    function suspendRegionalBody(bytes32 region) external;          // emergency multisig only
    function ratifyRegionalBodySuspension(bytes32 region) external; // within 14 days
    function unsuspendRegionalBody(bytes32 region) external;

    // Views
    // The three hash views answer progressively wider scopes: global only,
    // global ∪ one named region, and the full per-operator predicate of
    // § Regional Scope (global ∪ current region ∪ previous region while a region
    // change is unripened), which reads the operator's region fields from
    // CapacityBond. The per-operator view is the serve-time counterpart of the
    // slash-eligibility gate in ADR 014 § Blacklist violation — same three legs,
    // evaluated at block.timestamp rather than at a served response.
    function isHashBlacklisted(bytes32 blake3Hash) external view returns (bool);
    function isHashBlacklistedInRegion(bytes32 blake3Hash, bytes32 region) external view returns (bool);
    function isHashBlacklistedForOperator(bytes32 blake3Hash, address operator) external view returns (bool);
    function isOriginBlacklisted(address operatorAddress) external view returns (bool);
    function getHashEntry(bytes32 region, bytes32 blake3Hash) external view returns (BlacklistEntry memory);
    function getBlacklistVersion() external view returns (uint256);

    // Events. `version` is the getBlacklistVersion() value AFTER the change, so a
    // delta consumer can order events and confirm no gap. Non-indexed: EVM topic
    // filters are set-membership, not range, so indexing it buys no range query
    // (see § Polling). Every hash-set change emits exactly one of the two below,
    // so the counter never advances with no matching log.
    event HashBlacklisted(bytes32 indexed region, bytes32 indexed blake3Hash, uint256 version, string reason);
    event HashRemoved(bytes32 indexed region, bytes32 indexed blake3Hash, uint256 version);
    // Origin blacklisting is a single toggle, deliberately OUTSIDE the version
    // mechanism: it carries no version and is enforced via OriginAssignment
    // cross-reference (§ Permissionless property), not the hash version poll.
    event OriginBlacklistUpdated(address indexed operatorAddress, bool blacklisted);
}

struct BlacklistEntry {
    bytes32 blake3Hash;
    uint256 addedAt;          // block timestamp when added
    uint256 effectiveAt;      // addedAt + the compliance window in force AT ADD TIME
                              // (emergencyComplianceWindow for emergency adds).
                              // Stamped, never derived: a later governance change
                              // to the window must not move the slash boundary
                              // under deliveries already served.
    bytes32 region;           // packed region key; bytes32("GLOBAL") for global
    string  reason;           // free-form, e.g. "DMCA-2026-001", "CSAM", "DSA-DE-001"
    bool    emergency;        // true if added via emergency multisig path
    uint8   category;         // Category; determines the emergency auto-expiry term.
                              // Meaningless when emergency == false.
}
```

> **Region representation.** `region` is **`bytes32` at every layer** — the external signatures, the events, the mapping keys, and the `BlacklistEntry` struct above. There is no `string` boundary form and no canonicalization step: callers pass the packed key directly, so the value written by a regional body is byte-identical to the one a scope check reads back.
>
> A region key is the region string packed left-aligned and zero-padded into `bytes32`, matching Solidity's own `bytes32("literal")` packing. Two sentinels are reserved: `bytes32("GLOBAL")` marks a global entry, and `bytes32(0)` is *unset* — never a valid region. `addHashRegional`, `removeHashRegional`, and `registerRegionalBody` all reject both sentinels, so a regional body can neither reach a global entry nor write against an unset key.
>
> `bytes32` rather than a narrower `bytes2` — the key must be comparable against the value scope matching reads, which is the operator's **on-chain `regionHint`**, and `CapacityBond` caps that field at 16 bytes, not 2 (`MAX_REGION_HINT_BYTES`; [ADR 014 § Blacklist violation](014-on-chain-verification.md#blacklist-violation) is where the slash path performs the comparison). Gossip separately constrains the *gossiped* region to an ISO 3166-1 alpha-2 code, but that is a different field on a different layer — the on-chain `regionHint` the scope check reads is not gossip-validated, so a two-byte entry key could not represent every region the registry admits. A `bytes32` key is also topic-native, which is what lets `HashBlacklisted` / `HashRemoved` index `region` directly. The `BlacklistEntry` layout is the struct above.

### Blacklist version

`getBlacklistVersion()` returns a monotonically increasing counter incremented on every change to the enforced hash set, across all paths: every hash add, every hash removal, and every `expireEmergencyEntry`.

Emergency auto-expiry is the one set change that is not caused by a transaction: the entry simply stops being enforceable when its deadline passes, with no write and no log. The liveness views honour that deadline immediately — an expired entry is unenforceable whether or not anyone cleans it up — and a node applies exactly those views (`isHashBlacklistedForOperator`, `isOriginBlacklisted` / `isOperatorBlacklisted`) when it filters its enumerated snapshot, so a lapsed emergency entry is dropped even while it still sits in the raw `blacklistedHashes` / `blacklistedAddresses` membership. `expireEmergencyEntry` (and `expireEmergencyOrigin` on the origin side) then removes it from that membership: permissionless, callable by anyone once the deadline passes, it deletes the entry through the ordinary removal path so the counter advances and a `HashRemoved` lands in the log like any other removal. The counter is no longer a delta-fetch cursor — nodes rebuild the full deny-set by enumeration ([§ Enumerating the deny-set](#enumerating-the-deny-set)) — but it remains a cheap monotonic liveness signal for sync-lag monitoring.

### Reason field

Free-form string, stored on-chain for auditability. Operators can reference legal notice identifiers (e.g., DMCA case numbers, DSA notice IDs) or use short category labels.

### `region` field

`bytes32("GLOBAL")` means the entry applies to every node. Any other key scopes the entry to nodes that declare that region — an ISO 3166-1 alpha-2 code in the ordinary case, though the key is packed from the operator's on-chain `regionHint`, which `CapacityBond` caps at 16 bytes. A node is in scope if the entry is global, if its declared region matches the entry's region, or — while a region change has not yet ripened — if its previous region matches (see [§ Regional Scope](#regional-scope)).

## Regional Governance Bodies

A regional body is an address (multisig or governance contract) registered by global governance for a specific jurisdiction. It can issue region-scoped blacklist entries for its jurisdiction without a global vote, but cannot issue global entries or blacklist origins — those remain global governance only.

**Registration binds a body to exactly one region, and that binding is the authority.** Holding `REGIONAL_BODY_ROLE` is necessary but not sufficient: `addHashRegional` / `removeHashRegional` additionally require the caller to be the body registered for the region named in the call. Without that, any one registered body could write entries for every other jurisdiction, which would make the whole regional split decorative. The one-region-per-body rule is enforced in both directions (a region has at most one body; a body serves at most one region) so a suspension in one jurisdiction cannot be routed around by the same body acting in another.

**At launch:** No regional bodies are registered. The `DEFAULT_ADMIN_ROLE` holder (deployer pre-handover, `TimelockController` post-handover) acts as sole governance. The contract surface supports regional bodies from day one so they can be added by governance vote without a contract redeploy.

**Production-scale operation:** Regional bodies are expected for at minimum EU (DSA compliance) and US (DMCA). Each body is a 3-of-5 multisig constituted with signers who have legal presence in the relevant jurisdiction.

**Signer non-overlap with the emergency multisig.** The emergency multisig can suspend a regional body (see [§ Regional Governance Bodies](#regional-governance-bodies)), so a signer on both a regional body and the emergency multisig would grade their own homework. `registerRegionalBody(region, body, emergencyMultisig)` requires that the candidate body's signer set is disjoint from the current emergency multisig signer set; registration reverts on overlap.

Disjointness is verified on-chain where both sides expose a Safe-shaped `getOwners()` view: the two owner sets are compared pairwise, and a candidate owner that holds `EMERGENCY_MULTISIG_ROLE` directly is an overlap regardless of whether the multisig side turned out to be enumerable. The `emergencyMultisig` argument is checked to actually hold the role, so the probe cannot be aimed at a decoy address to manufacture a clean result. Where on-chain enumeration is infeasible for either implementation, registration still succeeds and the `RegionalBodyRegistered` event carries `signersVerified = false`; governance MUST then verify disjointness off-chain before passing the registration proposal and document it in the proposal. The event is the on-chain record of which of the two regimes applied to a given registration. Subsequent rotations on either side that introduce overlap are a governance obligation to detect and resolve — either rotate the overlapping signer out of the body or deregister the body before it issues another entry.

**Suspension:** The emergency multisig can suspend a regional body immediately via `suspendRegionalBody(region)`. Suspended bodies cannot issue new entries but existing entries remain active — suspension bounds the body's future authority, it is not a mass retraction of the jurisdiction's takedowns. Suspension must be ratified (`ratifyRegionalBodySuspension`) or reversed (`unsuspendRegionalBody`) by governance vote within 14 days, the same ratification window as emergency blacklist entries.

Governance silence past that window **lapses the suspension** and the body resumes writing, exactly as an unratified emergency entry expires. Both are unilateral multisig acts taken without a vote, and neither is a standing act of governance; sustaining one indefinitely on silence alone would let the multisig disable a jurisdiction permanently with no vote ever taken. Where a body genuinely must stay out, the durable instrument is `deregisterRegionalBody`, which is a governance act.

Regional bodies operate independently within their scope. A hash blacklisted by the EU body is a compliance obligation only for nodes that declare an EU region. A hash blacklisted globally is a compliance obligation for all nodes regardless of region.

## Removing a Wrongful Entry

Regional bodies acting in good faith can still issue entries that are later contested — a wrongly served takedown notice, a body that drifts outside its declared jurisdiction, or a notice that misidentifies content. Two tools answer that, at two different scopes.

For a **single disputed entry**, the removal functions are the recourse, and the two scopes are not interchangeable. `removeHashGlobal` (global entries) is restricted to `GOVERNANCE_ROLE`, which DecdnGovernor proposals reach via the standard timelock; it routes to `_removeHashRegional(GLOBAL_REGION, …)` and so cannot touch a regional entry. `removeHashRegional` is restricted to `REGIONAL_BODY_ROLE`, reverts on the `GLOBAL_REGION` sentinel, and additionally requires the caller to be that region's currently-registered, unsuspended body — so it is a *regional body's* unilateral power, and governance reaches a regional entry only by replacing the body (`deregisterRegionalBody` then `registerRegionalBody`). Both bump `getBlacklistVersion()` so nodes pick the removal up on their next poll.

For a **systemically misbehaving body**, `suspendRegionalBody` bars that body from writing, subject to governance ratification within 14 days. Two properties matter operationally and pull in opposite directions: it retracts nothing — every entry already issued stays live, enforceable and slashable — and because `_requireActiveBodyFor` gates removals as well as additions, it also *blocks* the body from taking its own entries down. Suspension is therefore the right tool against a body issuing bad entries and the wrong one against a body refusing to remove them; for the latter, replace the body (see [§ Regional Governance Bodies](#regional-governance-bodies)).

Operators slashed under an entry later removed by either route seek restitution through [ADR 028](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation), which lists blacklist offenses as appealable. That is a separate contract with its own bond and adjudication, and it is the only appeal surface the protocol carries: there is no second, content-policy appeal state machine layered on top of the blacklist itself. A per-entry interim-relief primitive — multisig-suspend plus governor-ratify on one `(hash, region)` — is worth revisiting at the governance vote that registers the first regional body, since until a body exists the adjudicator and the issuer of a contested entry would be the same key.

## Compliance Window

One rule governs every path: an entry becomes slashable at `effectiveAt = addedAt + window`, where `window` is the parameter for the path that added it. Serving before `effectiveAt` is never an offense.

| Path | `window` | Parameter |
|------|----------|-----------|
| Standard governance vote (global) | 24 hours | `complianceWindow` |
| Regional governance body | 24 hours | `complianceWindow` |
| Emergency multisig add | 2 hours | `emergencyComplianceWindow` |

The emergency path is deliberately one-way with respect to governance: `emergencyAdd` / `emergencyAddOrigin` revert on a target governance has already blacklisted permanently. The multisig can always make enforcement stricter and never looser — `removeHashGlobal` is `GOVERNANCE_ROLE` only, and without this rule a re-add through the emergency path would arm auto-expiry on a standing governance decision and accomplish the same removal on a 14-day delay with no vote.

The 24-hour window accounts for nodes that are offline or have a long poll interval. The 2-hour emergency window is tight enough to matter for active illegal content while giving online nodes time to act. The emergency multisig blacklist path is permanent (no sunset): it discharges an ongoing legal duty to remove unlawful content. Under the capability-split sunset, only the protocol-wide pause expires at 12 months, not this path — see [ADR 009 § Emergency Multisig](009-governance.md#emergency-multisig).

Both windows are governable parameters sharing one set of hardcoded bounds: minimum 1 hour, maximum 7 days. The floor is what keeps the emergency path honest — governance cannot compress the window below a single default poll cycle and slash nodes for content they had no opportunity to learn about.

`effectiveAt` is stamped onto the entry at add time from the window then in force. Changing a window therefore affects only subsequent adds; it can neither retroactively expose already-served deliveries to a slash nor retroactively immunize them. `SlashJudge` anchors its slash-eligibility comparison to `effectiveAt`, not `addedAt`.

### One-hour removal orders

Some statutory regimes bind the operator that receives a removal order to a sub-day deadline — the EU Terrorist Content Online Regulation's one-hour clock is the tightest. Such an order is discharged at the operator level: the receiving operator adds the hash to its [local denylist](#local-denylist), which takes effect on the next reload (no restart, no gossip, no governance round-trip) and is scoped to that operator's own node. This is the fastest removal path the protocol offers, it is entirely within the recipient's control, and it binds exactly what the order binds — the recipient's own serving.

The network does not build a sub-hour global propagation lane, and none is required. A removal order reaches one operator, not every node; the rest of the network is covered by the protocol-level paths — an emergency multisig `emergencyAdd` (effective immediately, `effectiveAt = addedAt`, two-hour compliance window) for network-wide removal, or a standard or regional governance add for the slower cases. The one-hour compliance-window floor above and the emergency multisig's mandate to discharge a one-hour-clock removal order (see [ADR 009 § Emergency Multisig](009-governance.md#emergency-multisig)) already size the on-chain mechanisms to this clock. A dedicated sub-hour global broadcast would add propagation surface and centralization pressure without changing what any single order requires.

## Hash Evasion and Origin Blacklisting

Hash-based blacklisting covers only exact copies of a blob. A one-byte change produces a completely different BLAKE3 hash and evades the blacklist — a known limitation shared by every hash-based content moderation system.

**The protocol's primary response is origin blacklisting.** If an origin-backed node repeatedly sources blacklisted content — whether the same blob or trivially re-encoded variants — governance can blacklist the operator's Ethereum address. `ContentBlacklist.addOperator()` calls `CapacityBond.ejectNode(operatorAddress)` via a cross-contract call; the `CapacityBond` grants the `ContentBlacklist` contract address the `BLACKLIST_ROLE`, permitting this call. A blacklisted origin:

- **Ejected from `CapacityBond`** — emits `EjectedByBlacklist`, sets the permanent `blacklistEjected` latch, and (when the operator had a registered node) sets `active = false` and emits `NodeAutoEjected` ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)). The node-deactivation effects follow the same code path as bond-shortfall auto-ejection ([ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn))
- **Effectively removed from every namespace's authorized origin set** at runtime; storage cleanup is lazy and permissionless via `pruneBlacklistedAssignment` — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist)
- **Remaining bond enters forced unbonding** — the standard unbonding window applies (14 days default per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve), governable [7, 60] days). Bond remains slashable during unbonding
- **Address banned while blacklisted** — cannot register new nodes under the same Ethereum address unless governance removes the blacklist entry via `removeOperator(operatorAddress)`. Re-entry otherwise requires a new identity funded with a fresh capacity bond (`bond = k × Mbps^α`; ≈50,000 TOKEN for a 1 Gbps entry tier at default `k=12.6`, `α=1.2` per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve))
- **The operator's registered NodeId is excluded from peer tables** — gossip validation rejects messages from that blacklisted node, and any existing peer-table entry is removed when the `NodeAutoEjected` event is received (see [appendix-peer-table-eviction.md](appendix-peer-table-eviction.md#appendix-peer-table-eviction-policy))

> **Two ejection causes, one master gate.** `CapacityBond.ejected` is set by both recoverable slash auto-ejection (bond fell below `minBond/2`; [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)) and permanent governance blacklisting, but governance blacklisting additionally sets a separate `blacklistEjected` latch. `bond()` reinstatement clears `ejected` *only* when `blacklistEjected` is unset — so a blacklisted operator **cannot self-reinstate by re-bonding** above `minBond`. Lifting the blacklist is a governance action: `removeOperator` calls `CapacityBond.unEjectNode`, which clears the latch only; the operator then re-enters through the normal re-bond path (a fresh `bond()` reaching `minBond`, then `registerNode`). A purely slash-ejected operator (no blacklist) remains recoverable by re-bonding as before.

> **Ejection vs. slashing.** Origin blacklisting triggers ejection (forced unbonding of remaining bond), *not* the escalating slash schedule: the bond is not burned, it is returned after the unbonding period assuming no separate slashable offense occurs during unbonding. By contrast, *serving* a blacklisted hash after the compliance window is a slashable offense under the escalating schedule in [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn), where the bond is partially burned and the challenger rewarded. A node operator can face both: slashing for serving blacklisted content, followed by origin blacklisting and ejection if the behavior persists.

This raises re-upload-evasion cost from trivial (change a byte) to significant — the operator must fund and register a new identity with a fresh capacity bond — so repeat evasion becomes progressively more expensive.

**Perceptual hashing is out of scope for the protocol.** Perceptual hash algorithms (PhotoDNA/PDQF for images, TMK for video) detect near-duplicate content but are content-type specific — there is no single perceptual hash for arbitrary binary blobs, and deCDN is content-agnostic. Perceptual hash checking for known illegal content categories is an operator obligation handled off-chain via industry hash databases — NCMEC's hash-sharing program for CSAM, and StopNCII for non-consensual intimate imagery — not a protocol primitive. This operator obligation is not left wholly discretionary: it is an acknowledged duty accepted at registration under [ADR 019 § Operator Safety Obligations](019-node-onboarding.md#operator-safety-obligations), which records operator assent on-chain without adding any content primitive here.

### Fast re-reporting path

When a re-encoded variant of a known-bad blob is identified, governance can add the new hash via the emergency multisig path (2-hour compliance window). Fast re-reporting combined with origin blacklisting makes sustained evasion operationally difficult even though no single mechanism closes the gap completely.

## Origin Assignment Authority

The mechanisms above describe the DAO's *negative* authority over origins: blacklisting bad actors. This section specifies the symmetric *positive* authority: which operators are authorized to act as origin backers for which content.

### Why positive authority is part of governance

Without positive authority, origin assignment is purely off-protocol — content owners independently configure backends and the network has no on-chain notion of "this operator is responsible for serving namespace X". This is workable for content owners who run their own infrastructure but provides no protocol-level guarantees: no Sybil resistance on origin claims (any bonded operator can claim to be an origin), no enforced redundancy (a single origin can be a single point of failure), no accountability path for takedown-compliance failures (governance can blacklist after the fact but cannot pre-authorize). Positive authority gives the DAO a tool to grant *and* withhold the origin role, mirroring the existing tool to remove it.

The publisher and namespace primitives are defined in [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces). Recap:

- A **publisher** is an Ethereum address that owns at least one namespace in `PublisherRegistry` (acquired implicitly on the first successful `createNamespace()` call; no separate registration step).
- A **namespace** is a publisher-owned `uint256` identifier for a content set and the unit of origin addressing; a request names the namespace its content is published under.
- **Namespace 0** (`namespaceId == 0`) has no publisher and no authorized origins; content served under it is best-effort from cache/DHT ([ADR 002 § Namespace 0](002-content-addressing.md#namespace-0)). Origin assignment applies only to registered namespaces, via the publisher-propose / DAO-ratify flow.

### Contract: OriginAssignment

```solidity
interface IOriginAssignment {
    // Publisher proposes a candidate origin set for one of their namespaces.
    // Reverts if msg.sender is not the namespace owner, if any operator is not
    // active in CapacityBond at proposal time, if operators.length == 0
    // (use revokeAssignment for explicit removal), or if operators.length
    // exceeds maxOriginsPerNamespace.
    function proposeAssignment(uint256 namespaceId, address[] calldata operators) external;

    // Governance ratifies a pending proposal after the assignment timelock.
    // Reverts if no pending proposal exists, if the timelock has not elapsed,
    // or if any pending operator is no longer active in CapacityBond or is
    // currently blacklisted in ContentBlacklist. On revert for either
    // validation reason, the pending proposal is auto-cleared so the
    // publisher can immediately submit a fresh `proposeAssignment` without a
    // separate cancellation step.
    function activateAssignment(uint256 namespaceId) external;

    // Publisher cancels their own pending proposal before activation.
    // Reverts if msg.sender is not the namespace owner, or if no pending
    // proposal exists. Equivalent to letting `proposeAssignment` overwrite,
    // but explicit for the case where the publisher wants to leave the
    // namespace in its current activated state without queuing a new set.
    function cancelAssignmentProposal(uint256 namespaceId) external;

    // Revocation paths:
    //  - Publisher may revoke a single operator from their own namespace at any time.
    //  - Governance may revoke an operator from any namespace.
    // Reverts if `operator` is not a member of the active set for `namespaceId`
    // — typo protection; revoking a non-member is always a programming error.
    // Revocation may drop the active set to zero; the namespace simply enters
    // the unassigned state until a new proposal is activated.
    function revokeAssignment(uint256 namespaceId, address operator) external;

    // Permissionless storage cleanup for blacklisted operators.
    // Reverts unless the operator is currently blacklisted in ContentBlacklist
    // (read via ContentBlacklist.isOriginBlacklisted). Callable by anyone — the
    // contract performs the lookup itself rather than trusting the caller. This
    // pattern avoids the unbounded gas cost of removing a blacklisted operator
    // from every namespace in one transaction; runtime authorization checks
    // (probe, peer table) consult ContentBlacklist directly so cleanup latency
    // does not affect security.
    function pruneBlacklistedAssignment(uint256 namespaceId, address operator) external;

    // Wires the read-direction integration with ContentBlacklist for
    // pruneBlacklistedAssignment. Called once during post-deploy initialization
    // (see ADR 016) and not expected to change thereafter; GOVERNANCE_ROLE only.
    function setContentBlacklist(address contentBlacklist) external;

    // Governable parameters with safety bounds (see ADR 009)
    function setMaxOriginsPerNamespace(uint256 cap) external;
    function setAssignmentTimelock(uint256 secondsDelay) external;

    // Views. For namespaceId == 0 these return empty / false — namespace 0
    // has no authorized origins.
    function isAuthorizedOrigin(uint256 namespaceId, address operator) external view returns (bool);
    function getOrigins(uint256 namespaceId) external view returns (address[] memory);
    function getPendingAssignment(uint256 namespaceId)
        external view returns (address[] memory operators, uint256 readyAt);

    // Events
    event AssignmentProposed(uint256 indexed namespaceId, address indexed proposer, address[] operators, uint256 readyAt);
    event AssignmentProposalCancelled(uint256 indexed namespaceId, address indexed proposer, bool autoCleared);
    event AssignmentActivated(uint256 indexed namespaceId, address[] operators);
    event AssignmentRevoked(uint256 indexed namespaceId, address indexed operator, address indexed by);
    event BlacklistedAssignmentPruned(uint256 indexed namespaceId, address indexed operator, address indexed pruner);
}
```

### Edge cases

- **Empty operator array (`operators.length == 0`)** — `proposeAssignment` reverts. `revokeAssignment` is the explicit removal path; a zero-length proposal would silently masquerade as a removal and obscure intent.
- **Proposal expiry** — none. Pending proposals sit indefinitely until `activateAssignment` (governance) or `cancelAssignmentProposal` (publisher). If governance is unresponsive, the publisher cancels and re-proposes; no expiry timer.
- **Re-proposal while a proposal is already pending** — `proposeAssignment` overwrites the existing pending proposal and resets `readyAt` to `block.timestamp + assignmentTimelock`. The old proposal is discarded; only the latest is observable. Emits `AssignmentProposalCancelled` (with `autoCleared = true`) for the discarded proposal followed by `AssignmentProposed` for the new one.
- **Activation-revert auto-clear** — when `activateAssignment` reverts because pending operators became inactive or blacklisted during the timelock window, the pending proposal is cleared and `AssignmentProposalCancelled(autoCleared=true)` fires; the publisher submits a fresh proposal without an explicit cancellation call.
- **`ContentBlacklist` unbound during the deployment window** — until `setContentBlacklist` is called post-deploy (see [ADR 016 § Post-Deployment Initialization](016-contract-interactions.md#post-deployment-initialization)), `activateAssignment` skips the blacklist check and validates only against `CapacityBond.isActive`. Once set the check is mandatory thereafter; `setContentBlacklist(address(0))` reverts to prevent regressing into the deployment-window state. `pruneBlacklistedAssignment` reverts until the binding is set.
- **`revokeAssignment` of a non-member operator** — reverts. Typo protection; the explicit error surfaces accidental address mismatches that would otherwise pass silently.

### Lifecycle

1. **Publisher proposal.** The publisher calls `proposeAssignment(namespaceId, operators)`. The contract validates that the proposer owns the namespace, that every candidate is currently active in `CapacityBond`, that `operators.length >= 1`, and that the operator count does not exceed `maxOriginsPerNamespace`. The proposal enters a pending state with `readyAt = block.timestamp + assignmentTimelock` (governance-bounded between 24 hours and 14 days; see [ADR 009](009-governance.md#adr-009-governance-model)).
2. **Governance ratification.** Governance reviews the proposal off-chain during the timelock window. After it elapses, a governance proposal calls `activateAssignment(namespaceId)`. Before replacing the active set, activation re-checks every pending operator against `CapacityBond.isActive` and `ContentBlacklist.isOriginBlacklisted` so a proposal cannot go live with operators that became inactive or were blacklisted during the delay window; if any operator now fails validation, activation reverts and the publisher must submit a fresh proposal. Successful activation replaces the namespace's authorized operator set atomically.
3. **Operator notification.** Operators in the activated set are now authorized to act as origins for the namespace. They configure their origin store locally and begin serving the namespace's content. The wire protocol does not distinguish origins from cache nodes at probe time — origin status is a publisher-level commitment surfaced via `getOrigins(namespaceId)` for off-chain consumers.
4. **Revocation.** A publisher may unilaterally remove an operator from their own namespace's set (e.g., the operator is performing poorly). Governance may revoke any operator from any namespace via the standard proposal path (e.g., the operator is misbehaving but has not yet crossed the blacklist threshold). Blacklisting (`ContentBlacklist.addOperator`) takes effect via runtime checks rather than a cross-call — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist).

The two-step propose-then-ratify flow is deliberate: it gives publishers agency over which operators they trust (publishers know their content best) while keeping the DAO as the authority that confirms the assignment is consistent with protocol-wide policy (e.g., not concentrating too many namespaces on a small operator set, not assigning to operators with poor reputation). Either party can refuse to advance the flow — publishers by not proposing, governance by not ratifying — and the namespace simply continues with its existing assignment (or remains unassigned).

### Namespace 0

`namespaceId == 0` has no publisher and no authorized origins. `OriginAssignment` holds no set for it: `getOrigins(0)` is empty and `isAuthorizedOrigin(0, op)` is always false. Namespace-0 content is served best-effort from cache or DHT-discovered holders ([ADR 002 § Namespace 0](002-content-addressing.md#namespace-0)); the origin role does not apply to it.

### Unassigned namespaces

A registered namespace with no activated assignment is **unassigned**. No operator is authorized as origin for its content, but the protocol still permits cache-only serving from any bonded operator that happens to hold the blob — see [ADR 005 § cdn/probe/v1](005-protocol.md#cdnprobev1--latency-probe). A publisher who never proposes an assignment prevents any new origin from picking up the namespace's content from canonical storage; cached copies eventually expire. This is by design — it lets a publisher withdraw a content set from the network by leaving its namespace unassigned or revoking its origins.

### Duplicate-address rejection

The contract rejects proposals whose `operators` array contains duplicate addresses. Without this, a publisher could submit `[A, A, A]` and concentrate origin responsibility on one operator while appearing to commit to multiple. Activation also re-validates that every pending operator is still active and not blacklisted before the set goes live. Operator-set sizing — including how many operators a publisher commits per namespace — is a publisher/governance policy decision, not a contract invariant; the protocol does not enforce a redundancy floor beyond the requirement that `proposeAssignment` contain at least one operator (use `revokeAssignment` for explicit removal).

### Cross-contract integration

- `OriginAssignment` reads `PublisherRegistry.ownerOf(namespaceId)` to validate proposer ownership.
- `OriginAssignment` reads `CapacityBond.isActive(operator)` to validate origin candidates at proposal time. The check is opportunistic, not enforced at probe time — an operator who unbonds mid-assignment is filtered by clients via the standard bond-active check, not by `OriginAssignment` (avoiding expensive cross-contract checks on every assignment lookup).
- `OriginAssignment.pruneBlacklistedAssignment` reads `ContentBlacklist.isOriginBlacklisted(operator)` to decide whether to remove an entry. Permissionless callers can clean up storage one (`namespaceId`, operator) pair at a time.
- `ContentBlacklist.addOperator(operator)` does not call into `OriginAssignment` — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist) below for the rationale and the runtime-check pattern.

### Interaction with ContentBlacklist

`ContentBlacklist.addOperator(operator)` does **not** call `OriginAssignment` to evict the operator from every namespace. The naïve approach — iterate every namespace the operator is assigned to and remove them in one transaction — is unbounded: an operator in N namespaces costs O(N) storage writes, and a prolific operator could exceed the block gas limit, blocking the blacklist transaction entirely.

Off-chain consumers of `OriginAssignment.getOrigins(namespaceId)` (clients selecting peers for first-fetch, off-chain monitors checking publisher availability commitments) cross-reference each returned operator against `ContentBlacklist.isOriginBlacklisted` and treat blacklisted entries as unauthorized regardless of stale `OriginAssignment` state. Storage cleanup happens lazily and permissionlessly via `OriginAssignment.pruneBlacklistedAssignment(namespaceId, operator)`: each call removes one entry; anyone may call it (the contract checks `ContentBlacklist.isOriginBlacklisted` itself, so the caller cannot grief by claiming a non-blacklisted operator is blacklisted). Reputation services and other public-good infrastructure will likely run pruning jobs.

Net: blacklisting an operator is O(1) on-chain (one ejection call) and storage cleanup is O(1) per call with no transaction-size limit — no design path requires iterating an operator's full namespace set.

### Permissionless property

This authority extends the DAO's role from negative-only (blacklisting) to positive-and-negative (assignment + blacklisting) for registered namespaces. Cache-only serving remains permissionless — any bonded operator may fetch cached blobs from authorized origins and re-serve them regardless of `OriginAssignment` membership; only the *origin* role becomes DAO-gated, via publisher-proposed sets. Namespace 0 has no authorized origins at all. See [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh) for the updated permissionless-role model.

## Node Behavior

### Enumerating the deny-set

Each node maintains a local deny-set mirroring `ContentBlacklist` — the blacklisted address union (origins and operators) plus the in-scope blacklisted hashes — and builds it by **enumerating current chain state**, never by replaying event history from a block floor.

- **On boot**, the node reads the complete current deny-set directly from the contract at a single pinned block. It pages the address union from `blacklistedAddresses` and keeps only the live members — those satisfying `isOriginBlacklisted(a) || isOperatorBlacklisted(a)`, the same disjunction `OriginAssignment` enforces, so a voted-out operator whose entry never touched the origin mapping is still caught. It reads the in-scope hashes via `getScopeRegions(operator)` and then `blacklistedHashes(region, …)` per returned region, filtering each for liveness through `isHashBlacklistedForOperator`. Both sets remove by swap-and-pop, so every page and the count it is checked against are read at the same pinned block; a `seen != count` mismatch aborts the snapshot rather than seating a partial set. Enumeration returns raw membership, so the per-address / per-hash liveness reads are what honour emergency auto-expiry — omitting a lapsed entry is the fail-open direction, which merely under-enforces a stale ban.
- **While running**, the node follows the contract's events from the boot snapshot head: `HashBlacklisted` / `HashRemoved` for hash entries, and both `OriginBlacklistUpdated` and `OperatorBlacklisted` / `OperatorBlacklistCleared` for the address union. Both address event families feed the one deny-set, because the operator leg is separate on-chain — `addOperator` sets `isOperatorBlacklisted` and never emits `OriginBlacklistUpdated`, so a node that watched only the origin event would keep serving a blacklisted operator.
- **Periodically**, the node re-enumerates the deny-set at a fresh pinned block as a backstop and re-scopes its hash set. This repairs any event lost to a reorg or an RPC-backoff gap, and catches [ADR 030 § Region-stability window](030-node-region-self-attestation.md#region-stability-window) region/ripening transitions, which change a hash's scope while emitting no event.

`getBlacklistVersion()` remains a cheap liveness signal for the `blacklist_sync_lag_seconds` metric, but the node no longer keys a block-range log query on a last-seen version checkpoint: enumeration reads the full current state on every boot, so there is no delta cursor to persist and no offline version gap to recover — a node returning after any downtime rebuilds the complete deny-set from one snapshot. Origin blacklisting is no longer a separate poll from hash entries; the address union folds both into the same enumeration and tail (see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist) and [§ Permissionless property](#permissionless-property)). Nodes SHOULD expose the `blacklist_sync_lag_seconds` metric for operational monitoring — see [Appendix: Observability](appendix-observability.md#appendix-observability-and-metrics).

The node MUST NOT accept connections until its initial enumeration completes.

**Pre-cache check:** Before caching any newly-fetched blob (whether from origin pull-through or peer pull), the node MUST check `isHashBlacklistedForOperator(hash, ownOperatorAddress)` and reject the blob if blacklisted. This enables proactive blacklisting of known-bad hashes before any node caches them, and resolves the node's own regional scope rather than global entries alone.

On startup, nodes always fetch the full current blacklist (global + their region) before accepting connections.

### On Blacklist Event

When a node receives a new blacklisted hash, it must, **in order**:

1. **Stop publishing** — withdraw any DHT STORE records for the hash and stop re-publishing immediately ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale))
2. **Stop serving** — reject any new `StreamRequest` for the hash immediately, returning `HashBlacklisted`
3. **Evict from cache** — delete the blob from local storage within the compliance window

The publish-first ordering is critical: continuing to serve a blacklisted hash after the compliance window is a blacklist-violation slashing offense ([ADR 014 § SlashJudge Contract](014-on-chain-verification.md#slashjudge-contract)), and a signed response for that hash is dispositive evidence. Disk eviction can be async; DHT-record and probe-response suppression must be synchronous.

When a node receives a blacklisted origin address, it additionally stops accepting any `StreamRequest` that presents a channel funded by that operator address, and removes all of that origin's NodeIds from its local peer table.

In-flight streams for a blacklisted hash — or on a channel funded by a newly blacklisted origin — are terminated at the next MB boundary, after the voucher for the bytes already delivered is collected. Termination is a stream reset with no `StreamEnd` sentinel, not an error frame: [ADR 005 § Stream errors](005-protocol.md#adr-005-wire-protocol) makes `VoucherRejected` the only `StreamError` that travels mid-stream, and the QUIC code is `NO_ERROR` so the client does not score the node as faulty for discharging a takedown ([ADR 013 § Application Error Codes](013-schema-evolution.md#application-error-codes)). A client that re-requests the hash gets the signed `HashBlacklisted` refusal from the open-time gate, and can request a refund of the unused channel balance.

### Regional Scope

A node applies only blacklist entries that are global or match its declared region (`node.region` in config). Entries for other regions are ignored. Nodes are not required to enforce takedowns outside their declared jurisdiction — regional compliance is the operator's legal obligation for their own node. A region change does not take effect for blacklist-scope or slash-eligibility purposes until it has been stable for `REGION_STABILITY_WINDOW`: during that window the node remains in scope for its previous region's entries in addition to the new region's, and `updateRegion` itself reverts before the window elapses (see [ADR 030 § Region-stability window](030-node-region-self-attestation.md#region-stability-window)). This forecloses a reactive flip out of a region the moment an entry lands.

Node region is self-reported and unverified at the protocol level. **Production posture:** self-attested regions are accepted at face value per [ADR 030](030-node-region-self-attestation.md#adr-030-node-region-self-attestation); the IP-geolocation oracle / third-party attestation path was considered and rejected. Reactive misdeclaration (flipping region after an entry lands) is foreclosed by the stability window above. A pre-positioned misdeclaration (a region declared false from registration) is *not* prevented by the protocol and, for a region-scoped entry, leaves the operator out of scope under [§ Slashing](#slashing) — the backstops are the operator's legal exposure under their actual jurisdiction's content law plus the continuous reputation penalty for latency-vs.-claim contradictions ([ADR 001 § Consequences](001-network.md#consequences)), the canonical soft mitigation. See [ADR 030 § Misdeclaration is operator legal exposure, not a protocol offense](030-node-region-self-attestation.md#misdeclaration-is-operator-legal-exposure-not-a-protocol-offense) for the full decomposition.

### Local Denylist

Each node supports a local denylist in config:

```toml
[content]
denied_hashes = [
    "abcdef1234...",              # bare 64-char lowercase hex, as in cache.pinned_hashes
]
denied_origins = [
    "0xOperatorAddress...",
]
```

Hashes take the same bare 64-character lowercase-hex form as `cache.pinned_hashes`, so an operator has one hash spelling across the whole config file. An invalid entry fails startup rather than being skipped: a typo in a takedown must not silently leave content served.

Local denylist entries take effect on the next reload and behave identically to governance blacklist entries. They are not gossiped to peers and require no governance action. This covers operators receiving direct legal notices affecting only their node, or operators proactively removing content they find objectionable.

The lists are hot-reloadable (`decdn node reload` / SIGHUP), which is load-bearing rather than convenient: [§ One-hour removal orders](#one-hour-removal-orders) makes this the only mechanism sized to a sub-day statutory deadline, and a restart-only denylist would put a daemon bounce — dropping every in-flight paid stream — on the critical path of discharging a legal order.

`denied_origins` is unioned with the on-chain origin blacklist at the delivery gate, so the wire refusal cannot distinguish a local entry from a governance one.

### StreamRequest Response

```rust
enum StreamError {
    // ... existing errors ...
    HashBlacklisted,      // hash is on the governance blacklist or local denylist
    OriginBlacklisted,    // the channel's operator address is blacklisted
}
```

The response does not distinguish between governance and local denylist sources — a client able to tell them apart could map an operator's private legal exposure by probing. Both answer `HashBlacklisted`, which requires the governance half to be gated on its own deny-set rather than falling through to the eviction arm: a hash refused as `EvictedSinceProbe` while no on-chain entry explains it is a hash the operator denied privately, so leaving governance on the eviction code would have made the *local* code the fingerprint. `EvictedSinceProbe` therefore now means an eviction with no blacklist entry behind it (corruption recovery, a manual `decdn node evict`). The governance/local distinction survives only in the operator's own metrics (`decdn_serve_stream_rejected_hash_denied_total` for the local list, `…_chain_hash_denied_total` for a governance entry), which no client can read.

Clients should retry on a different node for `HashBlacklisted`: a local entry binds only that node. `OriginBlacklisted` is not worth retrying anywhere — it is a statement about the requester's own funding address, so every node refuses identically until governance lifts the entry.

## Slashing

Serving a blacklisted hash after the compliance window is a slashable offense, subject to the escalating schedule in [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn). Repeated offenses trigger cumulative bond loss; nodes whose bond drops below 50% of the minimum are auto-ejected. Individual slash percentages are capped at 50% per offense ([ADR 009 § Safety bounds](009-governance.md#governable-parameters-with-safety-bounds)). The standard 100 TOKEN challenge bond from [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn) applies.

### Slash evidence

The challenger submits:

- The `blake3Hash`
- A `ProbeResponse` with `has_blob: true` for the blacklisted hash, timestamped after the compliance window. On-chain verification uses the `slash_sig` scheme from [ADR 014 § Slash Signatures — secp256k1 EIP-712](014-on-chain-verification.md#slash-signatures--secp256k1-eip-712): the EIP-712 secp256k1 `slash_sig` is verified via `ecrecover` and the recovered address mapped to the node's identity via `CapacityBond.nodeIdOf`. This is the primary evidence path. Alternatively, a `StreamResponse` with `ok: true` for the blacklisted hash (binding `hash` and `channel_id` in the signed data) is also sufficient. Client-signed vouchers alone are NOT sufficient — vouchers do not contain the hash and the `channel_id → hash` binding is not on-chain verifiable.
- The `BlacklistEntry.effectiveAt` timestamp showing the compliance window had passed

The `ContentBlacklist` contract verifies that `effectiveAt` is in the past relative to the delivery timestamp and that the hash is still on the blacklist. If the hash was subsequently removed, the slash is invalid.

Regional slash eligibility: a node is only slashable for serving a hash it was in-scope to remove — a node that declared region `US` is not slashable for serving a hash blacklisted only by the EU regional body. "In-scope" here is the same three-leg predicate as [§ Regional Scope](#regional-scope), including the post-`updateRegion` stability window — but for the punitive slash path the ripening window is evaluated at the served `responseTs`, not at challenge-submission time (see [ADR 030 § Region-stability window](030-node-region-self-attestation.md#region-stability-window) item 2, the canonical specification). A node that had changed region within `REGION_STABILITY_WINDOW` of the serve remains slashable under its previous region's entries, so a region flip cannot shed slash exposure for an entry that was live in the prior region before the serve — nor can stalling the challenge until the change ripens evade it.

### Grace period for offline nodes

A slash requires an active challenger submitting evidence of a post-window delivery. Nodes that reconnect, sync the blacklist, and evict before serving any content are safe.

## Consequences

### Positive

- Global and regional blacklisting coexist in one contract — no separate deployment for jurisdictions
- Regional bodies can act without a global governance vote, matching the speed of real-world legal processes (DSA requires expeditious removal)
- Origin blacklisting makes hash evasion progressively expensive — each re-upload requires a fresh capacity bond and a new identity
- Emergency path addresses CSAM and actively-exploited material without a 5-day vote cycle
- Reason field and on-chain audit trail support legal defensibility for operators
- Local denylist preserves operator autonomy for direct legal notices
- Origin assignment authority gives publishers a protocol-level way to commit specific operators to serving their content; the operator-set size is a publisher/governance decision, not a contract-enforced floor
- Symmetric blacklist/assignment infrastructure: a blacklisted operator is treated as unauthorized at every runtime check across every namespace they were authorized to serve, with lazy storage cleanup (see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist))
- Namespace 0 (`namespaceId == 0`) has no authorized origins and its content is served best-effort from cache/DHT; only registered namespaces carry a DAO-authorized origin set, so on-chain state is bounded by the number of namespaces rather than by content volume

### Negative

- Hash-based blacklisting covers exact copies only; trivial re-encoding evades it. This is a fundamental limitation with no protocol-level solution for content-agnostic blobs
- Node region is self-reported and unverified; regional compliance relies on operator legal incentive, not cryptographic enforcement
- Governance becomes a content moderation body, requiring off-chain processes (abuse intake, legal review) the protocol does not define
- Multiple regional bodies add governance coordination overhead; regional bodies can disagree on scope
- Blacklisted content remains content-addressable and verifiable off-network; eviction stops CDN serving but does not prevent redistribution by other means
- Origin assignment authority places positive node-role authorization in governance scope alongside the existing negative authority (blacklisting). Capture risk and operator-concentration risk are governance concerns, not just off-protocol coordination concerns
- Cache-only role is permissionless per [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh); the origin role is governance-gated for registered namespaces via per-namespace `OriginAssignment`, and namespace 0 has no origin role at all — reliable retrieval requires the requester to know the namespace
- A wrongly issued regional entry has no fast interim-relief path: the issuing body can remove it in one transaction, but if that body will not act, the only route is a DecdnGovernor proposal through the standard timelock (~10 days) during which the content stays unservable. A minimal interim-relief primitive is worth revisiting at the governance vote that registers the first regional body

## Governance Process (Off-Chain)

The minimum viable process at launch:

1. A `takedown@` contact address is published alongside the node registry
2. Notices are triaged by the `DEFAULT_ADMIN_ROLE` holder (deployer pre-handover, then global governance multisig once `TimelockController` is wired)
3. Clearly illegal content (CSAM, actively-exploited material) → emergency multisig path
4. DMCA / DSA notices → appropriate governance body (global or regional) with the notice ID in the `reason` field
5. Repeat-offender origin nodes → global governance vote for origin blacklisting
6. Disputed entries → see [§ Removing a Wrongful Entry](#removing-a-wrongful-entry): `removeHashRegional` for a regional entry (the issuing body's own power) or a DecdnGovernor `removeHashGlobal` proposal for a global one
