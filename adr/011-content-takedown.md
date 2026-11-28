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
5. The consequence of non-compliance
6. The known limitations of hash-based blacklisting and the mitigations available
7. A governance-controlled positive authority for origin assignment (`OriginAssignment`) — a swappable vetting policy decides which wallets may seat origins, and a vetted publisher then seats origins for its own namespaces instantly; namespace 0 (`namespaceId == 0`) has no authorized origins — built on the publisher/namespace identity primitive defined in [ADR 002](002-content-addressing.md#publisher-identity-and-namespaces)

## Decision

Content governance over origins has two symmetric authorities, both DAO-controlled:

- **Negative authority — `ContentBlacklist`.** Removes hashes and operators via two governance paths: a global path (network-wide removal) and a regional path (jurisdiction-scoped removal via a designated regional governance body). Nodes must evict blacklisted content and stop announcing it within a defined compliance window. An operator that keeps serving blacklisted content can be blacklisted and ejected ([§ Hash Evasion and Origin Blacklisting](#hash-evasion-and-origin-blacklisting)). Origin nodes that repeatedly source blacklisted content can themselves be blacklisted by NodeId or operator address, independent of any specific hash — the primary mitigation for hash evasion via trivial re-encoding.
- **Positive authority — `OriginAssignment`.** Authorizes specific operators to act as origins for specific namespaces (defined in [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)). A swappable vetting policy decides which publisher wallets may seat origins; the vetted publisher then picks its own origins from the bonded operator set with no further governance action. Namespace 0 (`namespaceId == 0`) has no authorized origins — content under it is served best-effort from cache/DHT per [ADR 002 § Retrieval by namespace](002-content-addressing.md#retrieval-by-namespace). `ContentBlacklist` and `OriginAssignment` integrate via runtime checks with lazy storage cleanup — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist).

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
    // an emergency entry past its deadline. Deleting the entry removes it from
    // the enumerable membership, appends an expire record to the mutation
    // ledger and emits HashRemoved. Without it the lapsed entry stays in the
    // raw blacklistedHashes set a node enumerates at boot (see § Node
    // Behavior). The origin variant does the same for blacklistedAddresses.
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
    // CapacityBond. The per-operator view evaluates the three legs at
    // block.timestamp.
    function isHashBlacklisted(bytes32 blake3Hash) external view returns (bool);
    function isHashBlacklistedInRegion(bytes32 blake3Hash, bytes32 region) external view returns (bool);
    function isHashBlacklistedForOperator(bytes32 blake3Hash, address operator) external view returns (bool);
    function isOriginBlacklisted(address operatorAddress) external view returns (bool);
    function getHashEntry(bytes32 region, bytes32 blake3Hash) external view returns (BlacklistEntry memory);

    // Mutation ledger (see § Following the mutation ledger). Append-only: one
    // record per state change that an authorised caller makes.
    // getMutations clamps like the paged enumeration views.
    function mutationCount() external view returns (uint256);
    function getMutations(uint256 from, uint256 limit) external view returns (Mutation[] memory);

    // Events. They serve indexers and audit. A node does not read them; it
    // follows the mutation ledger (see § Node Behavior).
    event HashBlacklisted(bytes32 indexed region, bytes32 indexed blake3Hash, string reason);
    event HashRemoved(bytes32 indexed region, bytes32 indexed blake3Hash);
    // Origin blacklisting is a single toggle, enforced via OriginAssignment
    // cross-reference (§ Permissionless property).
    event OriginBlacklistUpdated(address indexed operatorAddress, bool blacklisted);
}

struct BlacklistEntry {
    bytes32 blake3Hash;
    uint256 addedAt;          // block timestamp when added
    uint256 effectiveAt;      // addedAt + the compliance window in force AT ADD TIME
                              // (emergencyComplianceWindow for emergency adds).
                              // Stamped, never derived: a later governance change
                              // to the window must not move the compliance
                              // deadline under deliveries already served.
    bytes32 region;           // packed region key; bytes32("GLOBAL") for global
    string  reason;           // free-form, e.g. "DMCA-2026-001", "CSAM", "DSA-DE-001"
    bool    emergency;        // true if added via emergency multisig path
    uint8   category;         // Category; determines the emergency auto-expiry term.
                              // Meaningless when emergency == false.
}

// Each kind overwrites the whole state of its key. Order is ABI-frozen.
enum MutationKind {
    HASH_ADD, HASH_EMERGENCY_ADD, HASH_RATIFY, HASH_REMOVE, HASH_EXPIRE,
    ORIGIN_SET, ORIGIN_CLEAR, ORIGIN_EMERGENCY_ADD, ORIGIN_EXPIRE,
    OPERATOR_ADD, OPERATOR_REMOVE,
    BODY_REGISTER, BODY_DEREGISTER, BODY_SUSPEND, BODY_SUSPENSION_RATIFY, BODY_UNSUSPEND,
    COMPLIANCE_WINDOW_SET, EMERGENCY_COMPLIANCE_WINDOW_SET
}

struct Mutation {             // kind, at and expiresAt share one storage slot
    MutationKind kind;
    uint64  at;               // block timestamp of the write
    uint64  expiresAt;        // 0 = nothing lapses; else the state holds while now <= expiresAt
    bytes32 region;           // region of a regional hash or a body record; else 0
    bytes32 key;              // the hash, the address left-padded to 32 bytes, or a window in seconds
}
```

> **Region representation.** `region` is **`bytes32` at every layer** — the external signatures, the events, the mapping keys, and the `BlacklistEntry` struct above. There is no `string` boundary form and no canonicalization step: callers pass the packed key directly, so the value written by a regional body is byte-identical to the one a scope check reads back.
>
> A region key is the region string packed left-aligned and zero-padded into `bytes32`, matching Solidity's own `bytes32("literal")` packing. Two sentinels are reserved: `bytes32("GLOBAL")` marks a global entry, and `bytes32(0)` is *unset* — never a valid region. `addHashRegional`, `removeHashRegional`, and `registerRegionalBody` all reject both sentinels, so a regional body can neither reach a global entry nor write against an unset key.
>
> `bytes32` rather than a narrower `bytes2` — the key must be comparable against the value scope matching reads, which is the operator's **on-chain `regionHint`**, and `CapacityBond` caps that field at 16 bytes, not 2 (`MAX_REGION_HINT_BYTES`; `isHashBlacklistedForOperator` performs the comparison). The on-chain `regionHint` is the only region the scope check reads, and it can hold values wider than a two-byte ISO 3166-1 alpha-2 code, so a two-byte entry key could not represent every region the registry admits. A `bytes32` key is also topic-native, which is what lets `HashBlacklisted` / `HashRemoved` index `region` directly. The `BlacklistEntry` layout is the struct above.

### Emergency auto-expiry

Emergency auto-expiry is the one set change that is not caused by a transaction. The entry stops being enforceable when its deadline passes, with no write, no log and no ledger record. The liveness views honour that deadline immediately: an expired entry is unenforceable whether or not anyone cleans it up. The mutation ledger carries the deadline as `expiresAt` on the add record, so a node expires the entry locally at the same instant ([§ Following the mutation ledger](#following-the-mutation-ledger)). `expireEmergencyEntry` (and `expireEmergencyOrigin` on the origin side) then removes the entry from the raw `blacklistedHashes` / `blacklistedAddresses` membership. Anyone can call it once the deadline passes. It deletes the entry through the ordinary removal path, appends an expire record to the ledger and emits `HashRemoved` like any other removal.

### Reason field

Free-form string, emitted on-chain in the `HashBlacklisted` event for auditability. The contract does not store it. Operators can reference legal notice identifiers (e.g., DMCA case numbers, DSA notice IDs) or use short category labels.

### `region` field

`bytes32("GLOBAL")` means the entry applies to every node. Any other key scopes the entry to nodes that declare that region — an ISO 3166-1 alpha-2 code in the ordinary case, though the key is packed from the operator's on-chain `regionHint`, which `CapacityBond` caps at 16 bytes. A node is in scope if the entry is global, if its declared region matches the entry's region, or — while a region change has not yet ripened — if its previous region matches (see [§ Regional Scope](#regional-scope)).

## Regional Governance Bodies

A regional body is an address (multisig or governance contract) registered by global governance for a specific jurisdiction. It can issue region-scoped blacklist entries for its jurisdiction without a global vote, but cannot issue global entries or blacklist origins — those remain global governance only.

**Registration binds a body to exactly one region, and that binding is the authority.** Holding `REGIONAL_BODY_ROLE` is necessary but not sufficient: `addHashRegional` / `removeHashRegional` additionally require the caller to be the body registered for the region named in the call. Without that, any one registered body could write entries for every other jurisdiction, which would make the whole regional split decorative. The one-region-per-body rule is enforced in both directions (a region has at most one body; a body serves at most one region) so a suspension in one jurisdiction cannot be routed around by the same body acting in another.

**At launch:** No regional bodies are registered. The `GOVERNANCE_ROLE` holder (deployer pre-handover, `TimelockController` post-handover) acts as sole governance. The contract surface supports regional bodies from day one so they can be added by governance vote without a contract redeploy.

**Production-scale operation:** Regional bodies are expected for at minimum EU (DSA compliance) and US (DMCA). Each body is a 3-of-5 multisig constituted with signers who have legal presence in the relevant jurisdiction.

**Signer non-overlap with the emergency multisig.** The emergency multisig can suspend a regional body (see [§ Regional Governance Bodies](#regional-governance-bodies)), so a signer on both a regional body and the emergency multisig would grade their own homework. `registerRegionalBody(region, body, emergencyMultisig)` requires that the candidate body's signer set is disjoint from the current emergency multisig signer set; registration reverts on overlap.

Disjointness is verified on-chain where both sides expose a Safe-shaped `getOwners()` view: the two owner sets are compared pairwise, and a candidate owner that holds `EMERGENCY_MULTISIG_ROLE` directly is an overlap regardless of whether the multisig side turned out to be enumerable. The `emergencyMultisig` argument is checked to actually hold the role, so the probe cannot be aimed at a decoy address to manufacture a clean result. Where on-chain enumeration is infeasible for either implementation, registration still succeeds and the `RegionalBodyRegistered` event carries `signersVerified = false`; governance MUST then verify disjointness off-chain before passing the registration proposal and document it in the proposal. The event is the on-chain record of which of the two regimes applied to a given registration. Subsequent rotations on either side that introduce overlap are a governance obligation to detect and resolve — either rotate the overlapping signer out of the body or deregister the body before it issues another entry.

**Suspension:** The emergency multisig can suspend a regional body immediately via `suspendRegionalBody(region)`. Suspended bodies cannot issue new entries but existing entries remain active — suspension bounds the body's future authority, it is not a mass retraction of the jurisdiction's takedowns. Suspension must be ratified (`ratifyRegionalBodySuspension`) or reversed (`unsuspendRegionalBody`) by governance vote within 14 days, the same ratification window as emergency blacklist entries.

Governance silence past that window **lapses the suspension** and the body resumes writing, exactly as an unratified emergency entry expires. Both are unilateral multisig acts taken without a vote, and neither is a standing act of governance; sustaining one indefinitely on silence alone would let the multisig disable a jurisdiction permanently with no vote ever taken. Where a body genuinely must stay out, the durable instrument is `deregisterRegionalBody`, which is a governance act.

Regional bodies operate independently within their scope. A hash blacklisted by the EU body is a compliance obligation only for nodes that declare an EU region. A hash blacklisted globally is a compliance obligation for all nodes regardless of region.

## Removing a Wrongful Entry

Regional bodies acting in good faith can still issue entries that are later contested — a wrongly served takedown notice, a body that drifts outside its declared jurisdiction, or a notice that misidentifies content. Two tools answer that, at two different scopes.

For a **single disputed entry**, the removal functions are the recourse, and the two scopes are not interchangeable. `removeHashGlobal` (global entries) is restricted to `GOVERNANCE_ROLE`, which DecdnGovernor proposals reach via the standard timelock; it routes to `_removeHashRegional(GLOBAL_REGION, …)` and so cannot touch a regional entry. `removeHashRegional` is restricted to `REGIONAL_BODY_ROLE`, reverts on the `GLOBAL_REGION` sentinel, and additionally requires the caller to be that region's currently-registered, unsuspended body — so it is a *regional body's* unilateral power, and governance reaches a regional entry only by replacing the body (`deregisterRegionalBody` then `registerRegionalBody`). Both append a remove record to the mutation ledger, emit `HashRemoved` and drop the entry from the enumerable membership, so nodes pick the removal up on their next ledger poll.

For a **systemically misbehaving body**, `suspendRegionalBody` bars that body from writing, subject to governance ratification within 14 days. Two properties matter operationally and pull in opposite directions: it retracts nothing — every entry already issued stays live and enforceable — and because `_requireActiveBodyFor` gates removals as well as additions, it also *blocks* the body from taking its own entries down. Suspension is therefore the right tool against a body issuing bad entries and the wrong one against a body refusing to remove them; for the latter, replace the body (see [§ Regional Governance Bodies](#regional-governance-bodies)).

The removal functions are the only dispute surface for an entry: no separate content-policy dispute process sits on top of the blacklist. A per-entry interim-relief primitive — multisig-suspend plus governor-ratify on one `(hash, region)` — is worth revisiting at the governance vote that registers the first regional body, since until a body exists the adjudicator and the issuer of a contested entry would be the same key.

## Compliance Window

One rule governs every path: a node must comply with an entry by `effectiveAt = addedAt + window`, where `window` is the parameter for the path that added it. Serving before `effectiveAt` is never non-compliance.

| Path | `window` | Parameter |
|------|----------|-----------|
| Standard governance vote (global) | 24 hours | `complianceWindow` |
| Regional governance body | 24 hours | `complianceWindow` |
| Emergency multisig add | 2 hours | `emergencyComplianceWindow` |

The emergency path is deliberately one-way with respect to governance: `emergencyAdd` / `emergencyAddOrigin` revert on a target governance has already blacklisted permanently. The multisig can always make enforcement stricter and never looser — `removeHashGlobal` is `GOVERNANCE_ROLE` only, and without this rule a re-add through the emergency path would arm auto-expiry on a standing governance decision and accomplish the same removal on a 14-day delay with no vote.

The 24-hour window accounts for nodes that are offline or have a long poll interval. The 2-hour emergency window is tight enough to matter for active illegal content while giving online nodes time to act. The emergency multisig blacklist path is permanent (no sunset): it discharges an ongoing legal duty to remove unlawful content. Under the capability-split sunset, only the protocol-wide pause expires at 12 months, not this path — see [ADR 009 § Emergency Multisig](009-governance.md#emergency-multisig).

Both windows are governable parameters sharing one set of hardcoded bounds: minimum 1 hour, maximum 7 days. The floor is what keeps the emergency path honest — governance cannot compress the window below a single default poll cycle and hold nodes to a deadline for content they had no opportunity to learn about.

`effectiveAt` is stamped onto the entry at add time from the window then in force. Changing a window therefore affects only subsequent adds; it can neither retroactively make already-served deliveries non-compliant nor retroactively make them compliant. A re-add of a live entry is idempotent for the stamps: it keeps `addedAt` and `effectiveAt`, updates the reason, and either ratifies an emergency entry (clears `emergency`) or escalates its category. Only an entry that is absent or has lapsed is stamped afresh.

### One-hour removal orders

Some statutory regimes bind the operator that receives a removal order to a sub-day deadline — the EU Terrorist Content Online Regulation's one-hour clock is the tightest. Such an order is discharged at the operator level: the receiving operator adds the hash to its [local denylist](#local-denylist), which takes effect on the next reload (no restart, no propagation step, no governance round-trip) and is scoped to that operator's own node. This is the fastest removal path the protocol offers, it is entirely within the recipient's control, and it binds exactly what the order binds — the recipient's own serving.

The network does not build a sub-hour global propagation lane, and none is required. A removal order reaches one operator, not every node; the rest of the network is covered by the protocol-level paths — an emergency multisig `emergencyAdd` (effective immediately, `effectiveAt = addedAt`, two-hour compliance window) for network-wide removal, or a standard or regional governance add for the slower cases. The one-hour compliance-window floor above and the emergency multisig's mandate to discharge a one-hour-clock removal order (see [ADR 009 § Emergency Multisig](009-governance.md#emergency-multisig)) already size the on-chain mechanisms to this clock. A dedicated sub-hour global broadcast would add propagation surface and centralization pressure without changing what any single order requires.

## Hash Evasion and Origin Blacklisting

Hash-based blacklisting covers only exact copies of a blob. A one-byte change produces a completely different BLAKE3 hash and evades the blacklist — a known limitation shared by every hash-based content moderation system.

**The protocol's primary response is origin blacklisting.** If an origin-backed node repeatedly sources blacklisted content — whether the same blob or trivially re-encoded variants — governance can blacklist the operator's Ethereum address. `ContentBlacklist.addOperator()` calls `CapacityBond.ejectNode(operatorAddress)` via a cross-contract call; the `CapacityBond` grants the `ContentBlacklist` contract address the `BLACKLIST_ROLE`, permitting this call. A blacklisted origin:

- **Ejected from `CapacityBond`** — sets the `blacklistEjected` latch. Only `unEjectNode` clears it. On an operator that is not already ejected, it also sets `ejected`, emits `EjectedByBlacklist`, and (when the operator had a registered node) sets `active = false` and emits `NodeAutoEjected` ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)).
- **Effectively removed from every namespace's authorized origin set** at runtime; consumers of `getOrigins` drop a blacklisted operator, and storage cleanup is lazy and permissionless via `pruneInactiveOrigin` once the operator-level blacklist ejects it from `CapacityBond` — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist)
- **Bond stays in place** — ejection does not move the bond. The operator withdraws it with `unbond`, which returns the TOKEN at once ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve))
- **Address banned while blacklisted** — cannot register new nodes under the same Ethereum address unless governance removes the blacklist entry via `removeOperator(operatorAddress)`. Re-entry otherwise requires a new identity funded with a fresh capacity bond (`bond = k × Mbps^α`; ≈50,000 TOKEN for a 1 Gbps entry tier at default `k=12.6`, `α=1.2` per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve))
- **The operator's registered NodeId leaves the registry active set** — the `NodeAutoEjected` event removes it, so peers stop discovering and dialing it, and the `cdn/dht/v1` STORE path rejects its records ([ADR 022 § STORE Flow](022-content-discovery.md#store-flow-cache-event--dht-publish))

> **One ejection cause, two flags.** `ejectNode` is the only path that sets `CapacityBond.ejected`, and it also sets a separate `blacklistEjected` latch. `bond()` reinstatement clears `ejected` *only* when `blacklistEjected` is unset — so a blacklisted operator **cannot self-reinstate by re-bonding** above `minBond`. Lifting the blacklist is a governance action: `removeOperator` calls `CapacityBond.unEjectNode`, which clears the latch only; the operator then re-enters through the normal re-bond path (a `bond()` that holds `minBond`, then `registerNode`).

> **Ejection is the protocol consequence.** A node that serves a blacklisted hash after the compliance window faces operator blacklisting and ejection when the behavior persists. Ejection removes the node from the active set. It does not burn or seize the bond. The operator's legal exposure under its own jurisdiction is the other consequence, and it sits outside the protocol.

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
- **Namespace 0** (`namespaceId == 0`) has no publisher and no authorized origins. On a request it means only that the requester does not name the namespace ([ADR 002 § Namespace 0](002-content-addressing.md#namespace-0)). Origin assignment applies only to registered namespaces.

### Two planes: vetting and seating

Origin authorization answers two different questions, and each one gets its own plane:

1. **May this wallet seat origins at all?** This is decided by a swappable **vetting policy**. `OriginAssignment` holds one governance-settable `IVettingPolicy` address and asks it `isVetted(publisher)`; it depends on nothing more. How a wallet becomes vetted — a manual approval, a timelocked governance grant, an on-chain attestation, or no gate — lives entirely inside the installed policy.
2. **Does this publisher want operator B serving its namespace?** This is the publisher's own question. The publisher asks it constantly. Each answer is a single-operator delta. Each delta takes effect in the transaction that carries it.

The **cold plane** is publisher vetting, delegated to the policy. `addOrigin` seats an origin only when `vettingPolicy.isVetted(msg.sender)` is true. A concrete policy is always installed — never the zero address — so the check is unconditional and fails closed; "deny everyone" is a policy whose `isVetted` returns false, not a null policy. Governance changes the vetting *procedure* by installing a different policy with `setVettingPolicy`, never by changing `OriginAssignment`.

The **hot plane** is origin seating. A vetted publisher calls `addOrigin(namespaceId, operator)` for a namespace it owns. The contract validates that operator alone — active in `CapacityBond`, not blacklisted, not already seated, within `maxOriginsPerNamespace` — and seats it immediately. `removeOrigin` unseats one operator the same way. Validation covers only the operator being added, so a transient failure on a live origin never blocks the seating of a redundant one.

This split preserves one guarantee. The network — through whatever policy governance installs — decides who may seat origins. The publisher only chooses among bonded operators, and only for its own namespaces.

### The vetting seam

The whole dependency between `OriginAssignment` and any policy is one view function:

```solidity
interface IVettingPolicy {
    // True iff `publisher` may currently seat origins.
    function isVetted(address publisher) external view returns (bool);
}
```

That one function is the whole dependency. A policy is free to add its own surface — a publisher-initiated on-chain application, a timelocked notice window, an attestation read — as its own layer; none of it is part of the seam, and `OriginAssignment` neither knows nor cares.

### Vetting policies

Governance installs one policy at genesis and swaps it at any time with `setVettingPolicy`. Each is a small contract behind the seam above:

- **ManualVettingPolicy** — the genesis default. A `VETTER_ROLE` holder approves publishers. `VETTER_ROLE`'s admin is `GOVERNANCE_ROLE`, so governance decides *who* vets — an operator wallet, the signing key of a publisher sign-up service, a multisig, a Safe, or the Governor — and moves that authority by reassigning the role, with no contract swap. The role confers only vetting, and governance revokes or re-points it at any time, so it is not a standing back door. A vetter approves a publisher in one of two ways:
  - **Signed approval.** The vetter signs an EIP-712 `Vetting(publisher, nonce)` message off-chain and gives it to the publisher. The publisher submits it with `claimVetting(publisher, signature)` and pays the gas. The contract accepts the signature only if its signer holds `VETTER_ROLE` when the claim lands and `nonce` equals the publisher's current `nonces` value. Only an EOA vetter can sign. A signature has no expiry. Revoking the signer's role voids its unclaimed signatures.
  - **Direct call.** The vetter calls `setVetted(publisher, bool)` and pays the gas. This is also the only way to un-vet. An un-vet increments the publisher's nonce, so a signature issued before the un-vet cannot vet the publisher again.

  How a vetter decides to approve — an email sign-up, an identity check, a governance proposal — stays off-chain. The contract records only that a vetter approved the wallet. A change of procedure needs no contract change. A policy swap voids all unclaimed signatures, because each policy is its own EIP-712 domain.
- **OpenVettingPolicy** — vets every publisher. For local and test deployments only; never installed on a production network.
- Future policies fit the same seam without touching `OriginAssignment`, for example a policy with a publisher-initiated on-chain application and enforced notice window. Each is a governance `setVettingPolicy` call.

### Contract: OriginAssignment

```solidity
interface IOriginAssignment {
    // ---- Vetting: delegated to a swappable policy ----

    // The installed vetting authority. addOrigin seats an origin only when
    // vettingPolicy.isVetted(msg.sender) is true.
    function vettingPolicy() external view returns (IVettingPolicy);

    // Install a different vetting policy, changing the vetting procedure without
    // changing this contract. Rejects address(0) — a policy is always present, so
    // addOrigin needs no zero branch and fails closed. GOVERNANCE_ROLE only.
    function setVettingPolicy(IVettingPolicy newPolicy) external;

    // ---- Hot plane: origin seating (publisher self-serve, instant) ----

    // Vetted namespace owner seats one more authorized origin. Effective
    // immediately. Reverts if msg.sender does not own the namespace, is not
    // vetted (per the installed policy), or if the operator is not active in
    // CapacityBond, is already seated, or would exceed maxOriginsPerNamespace.
    // Validation covers ONLY the operator being added: operators already in the
    // set are never re-checked, so a transient failure on a live origin cannot
    // block seating a new one. Blacklist status is not checked here — the seat
    // set is a routing hint that consumers filter themselves (see § Interaction
    // with ContentBlacklist), and an operator-level blacklist ejects from
    // CapacityBond, so the isActive check already rejects it.
    function addOrigin(uint256 namespaceId, address operator) external;

    // Removal paths:
    //  - Publisher may unseat a single operator from its own namespace at any time.
    //  - Governance may unseat an operator from any namespace.
    // Reverts if `operator` is not a member of the active set for `namespaceId`
    // — typo protection; unseating a non-member is always a programming error.
    // Removal may drop the active set to zero; the namespace simply enters the
    // unassigned state until the publisher seats another origin.
    function removeOrigin(uint256 namespaceId, address operator) external;

    // Permissionless storage cleanup for operators that have left the active
    // set. Reverts unless CapacityBond.isActive(operator) is false — so a caller
    // cannot grief by naming a still-active operator. This reaches every exit:
    // unbond, deregister, and operator-level blacklist (which
    // ejects from CapacityBond). Callable by anyone — the contract performs the
    // isActive lookup itself rather than trusting the caller. Storage hygiene,
    // not security: consumers already skip an inactive or blacklisted operator
    // at routing time (see § Interaction with ContentBlacklist), so cleanup
    // latency does not affect delivery; this only keeps the on-chain set from
    // accumulating dead entries against maxOriginsPerNamespace.
    function pruneInactiveOrigin(uint256 namespaceId, address operator) external;

    // Governable parameter with safety bounds (see ADR 009). Emits its own
    // update event: MaxOriginsPerNamespaceUpdated. Vetting-related parameters, if
    // any, live on the installed policy, not here.
    function setMaxOriginsPerNamespace(uint256 cap) external;

    // Views. For namespaceId == 0 these return empty / false — namespace 0
    // has no authorized origins.
    function isAuthorizedOrigin(uint256 namespaceId, address operator) external view returns (bool);
    function getOrigins(uint256 namespaceId) external view returns (address[] memory);
    // Passthrough to vettingPolicy().isVetted(publisher), so a caller reading
    // vetting status need not know the policy address.
    function isVettedPublisher(address publisher) external view returns (bool);

    // Namespace key-set enumeration, so an off-chain directory can discover
    // which namespaces have origins without replaying events. Order is not
    // stable across mutations, so page at one pinned block height.
    function assignedNamespaceCount() external view returns (uint256);
    function assignedNamespaces(uint256 offset, uint256 limit) external view returns (uint256[] memory);

    // Events.
    event VettingPolicyUpdated(address indexed oldPolicy, address indexed newPolicy);
    event OriginAdded(uint256 indexed namespaceId, address indexed operator, address indexed by);
    event OriginRemoved(uint256 indexed namespaceId, address indexed operator, address indexed by);
    event InactiveOriginPruned(uint256 indexed namespaceId, address indexed operator, address indexed pruner);
}
```

`OriginAdded` is the only "origin seated" event. An off-chain consumer treats each event as a signal to re-read `getOrigins(namespaceId)`, not as a delta to apply — see [ADR 022 § FIND_VALUE Flow](022-content-discovery.md#adr-022--content-discovery-at-scale). The vetting lifecycle emits from the installed policy, not from `OriginAssignment`; a consumer that tracks vetting grants follows `VettingPolicyUpdated` to know which policy is current.

### Edge cases

- **Vetting lifecycle** — internal to the installed policy. Request expiry, cancellation, timelock bounds, and the entry conditions for a request are all defined by the policy, not by `OriginAssignment`. The manual policy has no request at all; the timelocked policy holds a pending request until it is granted or cancelled, with no expiry timer.
- **Swapping the policy** — takes effect for future seating only. A wallet vetted under the old policy may no longer be vetted under the new one, so its next `addOrigin` reverts `PublisherNotVetted`; origins it already seated are untouched (the same bounded-work reasoning as un-vetting below).
- **Un-vetting a publisher with live origins** — the seated origins stay. The policy marking a wallet un-vetted stops new `addOrigin` calls immediately. Governance then removes the live origins one at a time with `removeOrigin`. Eviction from every namespace in one call is unbounded in the publisher's namespace count. `ContentBlacklist.addOperator` avoids a cross-call for the same reason — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist).
- **Namespace transferred to an unvetted owner** — the origins already seated for it keep serving. The new owner can call `removeOrigin` but not `addOrigin` until the installed policy vets them. Transfers do not interrupt delivery.
- **`addOrigin` for an operator that is already seated** — reverts `DuplicateOperator`, whatever the current set size. The duplicate check precedes the cap check, so a re-add never reports `TooManyOrigins`.
- **`maxOriginsPerNamespace` lowered below a live set** — nothing is evicted. The cap binds `addOrigin` only, so an existing set may exceed it until the publisher removes operators.
- **A reverting or misconfigured policy** — `addOrigin` reverts, failing closed. Governance owns the policy address, so a loud failure is preferable to silently denying every publisher.
- **A seated origin is blacklisted** — nothing on `OriginAssignment` reacts; the seat set is a routing hint, and consumers of `getOrigins` already drop a blacklisted operator (see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist)). `pruneInactiveOrigin` clears the stale entry once the operator leaves the active set, which an operator-level blacklist forces by ejecting it from `CapacityBond`.
- **`removeOrigin` of a non-member operator** — reverts. Typo protection; the explicit error surfaces accidental address mismatches that would otherwise pass silently.

### Lifecycle

1. **Publisher vetting.** The publisher becomes vetted through the installed policy. Under the genesis `ManualVettingPolicy` a `VETTER_ROLE` holder approves the wallet off-chain. The holder either gives the publisher a signed `Vetting` approval, which the publisher submits with `claimVetting`, or calls `setVetted(publisher, true)` itself. Once the approval lands, the publisher can seat origins immediately. Vetting is per wallet and covers every namespace the publisher owns, now and later.
2. **Origin seating.** The vetted publisher calls `addOrigin(namespaceId, operator)` for each operator it wants. The contract validates ownership of the namespace, the publisher's vetted status (via the policy), and that operator's activity, blacklist status, uniqueness, and the per-namespace cap. The operator is authorized when the transaction confirms.
3. **Operator notification.** Seated operators are now authorized to act as origins for the namespace. They configure their origin store locally and begin serving the namespace's content. The wire protocol does not distinguish origins from cache nodes in a discovery answer — origin status is a publisher-level commitment surfaced via `getOrigins(namespaceId)` for off-chain consumers.
4. **Removal.** A publisher may unilaterally unseat an operator from its own namespace (e.g., the operator is performing poorly). Governance may unseat any operator from any namespace, and may un-vet the publisher — through the installed policy — to stop further seating (e.g., the publisher is misbehaving but has not yet crossed the blacklist threshold). Blacklisting (`ContentBlacklist.addOperator`) takes effect via runtime checks rather than a cross-call — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist).

The split between the two decisions is deliberate. Publishers know their content best, so they choose which operators serve it. The network decides whether a publisher may seat origins at all, through a policy governance can swap as compliance needs change. Only that second question needs protocol-wide policy.

The backstops are reactive, not preventive. Governance removes a bad origin, un-vets a bad publisher, or blacklists a bad operator. Each of the three takes effect on its own, without waiting for the others. Content integrity does not depend on any of them. Clients verify bytes against the BLAKE3 address, whoever served them ([ADR 002](002-content-addressing.md#adr-002-content-addressing)).

### Namespace 0

`namespaceId == 0` has no publisher and no authorized origins. `OriginAssignment` holds no set for it: `getOrigins(0)` is empty and `isAuthorizedOrigin(0, op)` is always false. `addOrigin(0, op)` reverts for every caller, because `ownerOf(0)` is the zero address. Namespace 0 holds no seats, so it attests nothing. A hash becomes discoverable and servable only through an operator that holds an origin seat in a registered namespace, whose live origin attestation every DHT record and every served answer carries ([ADR 002 § Namespace 0](002-content-addressing.md#namespace-0), [ADR 022 § Origin Attestation](022-content-discovery.md#origin-attestation)).

### Unassigned namespaces

A registered namespace with no seated origin is **unassigned**. No operator is authorized as origin for its content, and no operator signs an origin attestation in it. A cached copy stays servable only while its holder holds a live attestation for the hash in some namespace ([ADR 005 § Serving rule](005-protocol.md#serving-rule)). When a publisher unseats its origins, their attestations in the namespace become invalid at each node's next seat-set read, and each cache then stops serving the hash, unless it holds a live attestation for the hash in another namespace. This is by design: it lets a publisher withdraw a content set from the network by leaving its namespace unassigned or unseating its origins. A publisher that keeps its origins seated and has them stop attesting a hash withdraws that hash within the TTL that they sign ([ADR 022 § Origin Attestation](022-content-discovery.md#origin-attestation)).

### Duplicate-address rejection

The contract rejects an `addOrigin` for an operator already seated in the namespace. The authorized set is an `EnumerableSet`, so a repeat add cannot inflate it. The revert turns a silent no-op into an explicit caller error. It also stops a publisher from reading a successful transaction as added redundancy. Operator-set sizing is a publisher and governance policy decision, not a contract invariant. The protocol enforces no redundancy floor.

### Cross-contract integration

- `OriginAssignment` reads `PublisherRegistry.ownerOf(namespaceId)` to validate that the caller owns the namespace it seats or unseats an origin for.
- `OriginAssignment` makes no `PublisherRegistry.namespaceCount` read — vetting is the installed policy's concern; `OriginAssignment` only asks it `isVetted(publisher)`. A policy that gates its own application on namespace ownership would read `namespaceCount` itself.
- `OriginAssignment` reads `CapacityBond.isActive(operator)` to validate an origin candidate at seating time. The check is opportunistic, not enforced at discovery time — an operator who unbonds while seated is filtered by clients via the standard bond-active check, not by `OriginAssignment` (avoiding expensive cross-contract checks on every assignment lookup). `pruneInactiveOrigin` reads the same `isActive` to permissionlessly clean up an entry once the operator leaves the active set, one (`namespaceId`, operator) pair at a time.
- `OriginAssignment` holds no `ContentBlacklist` binding. The seat set is a routing hint, and blacklist filtering is the consumer's job (see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist)) — pushing it there keeps the seat set free of a governance-settable pointer and a deployment-window bootstrap.
- `ContentBlacklist.addOperator(operator)` does not call into `OriginAssignment` — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist) below for the rationale and the runtime-check pattern.

### Interaction with ContentBlacklist

`ContentBlacklist.addOperator(operator)` does **not** call `OriginAssignment` to evict the operator from every namespace. The naïve approach — iterate every namespace the operator is assigned to and remove them in one transaction — is unbounded: an operator in N namespaces costs O(N) storage writes, and a prolific operator could exceed the block gas limit, blocking the blacklist transaction entirely. Nor does `OriginAssignment` hold a `ContentBlacklist` binding: the seat set is a routing hint, so enforcement lives with the party that acts on it.

Consumers of `OriginAssignment.getOrigins(namespaceId)` protect themselves. A node's origin-routing filter and off-chain monitors cross-reference each returned operator against **both** `ContentBlacklist.isOriginBlacklisted` and `isOperatorBlacklisted` (the same union its delivery gate consults) and drop an operator blacklisted through either, regardless of stale `OriginAssignment` state — so a blacklisted operator left in the seat set is never routed to. This is the trustless shape: the seat check would be opportunistic anyway (it never re-runs after seating), and every consumer must re-filter, so filtering belongs at the consumer and the on-chain guard adds nothing but a governance surface. An operator-level blacklist additionally ejects the operator from `CapacityBond`, so `isActive` returns false and the standard bond-active filter drops it too.

Storage cleanup happens lazily and permissionlessly via `OriginAssignment.pruneInactiveOrigin(namespaceId, operator)`: each call removes one entry whose operator has left the active set (`CapacityBond.isActive(operator) == false`); anyone may call it (the contract evaluates `isActive` itself, so the caller cannot grief by naming a still-active operator). An operator-level blacklist reaches this path via its ejection; a purely origin-level blacklist (which does not eject) leaves an active operator in the set, but the consumer filter already skips it, so the residual entry is harmless storage. Reputation services and other public-good infrastructure will likely run pruning jobs.

Net: blacklisting an operator is O(1) on-chain (one ejection call), consumers enforce it at routing time, and storage cleanup is O(1) per call with no transaction-size limit — no design path requires iterating an operator's full namespace set.

### Permissionless property

This authority extends the DAO's role from negative-only (blacklisting) to positive-and-negative (assignment + blacklisting) for registered namespaces. Cache-only serving remains permissionless — any bonded operator may fetch cached blobs from authorized origins and re-serve them regardless of `OriginAssignment` membership; only the *origin* role becomes DAO-gated, via the publisher-vetting plane. Serving and discovery follow the origin role: a cached copy is servable and discoverable only while its holder holds a live attestation of a seated origin ([ADR 022 § Origin Attestation](022-content-discovery.md#origin-attestation)). Namespace 0 has no authorized origins at all. See [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh) for the updated permissionless-role model.

## Node Behavior

### Enumerating the deny-set

Each node keeps a local deny index that mirrors `ContentBlacklist`. The index holds the blacklisted address union (origins and operators), the blacklisted hashes of each region the node has read, the regional-body state and the two compliance windows. Each index entry stores its `expiresAt` (0 for an entry with no auto-expiry). The node evaluates its scope at lookup time. The node reads all chain state by `eth_call`. It does not read event logs.

On boot, the node builds the index from the enumeration views. It pins the governance chain's `safe` block and does all of these reads at that block:

- It reads `mutationCount` and keeps the value as its ledger cursor.
- It pages the address union from `blacklistedAddresses` and keeps only the live members. A live member satisfies `isOriginBlacklisted(a) || isOperatorBlacklisted(a)`. This is the same disjunction that `OriginAssignment` consumers apply, so the index catches an operator that `addOperator` blacklisted without an origin entry.
- It reads its scope with `getScopeRegions(operator)`. It pages `blacklistedHashes(region, …)` for each returned region. For each paged hash, one `getHashEntry(region, hash)` read gives both the liveness and the expiry of the entry. An entry is live when its `addedAt` is not 0, and it is not an emergency entry past its category term. The node keeps a hash under a region only when the entry in that region is live.
- An emergency entry gets `expiresAt` equal to `addedAt` plus its category term. A non-emergency entry gets `expiresAt` 0. For each paged address that `isOriginBlacklisted` reports live, the node reads `getEmergencyOrigin` and sets the expiry of that origin by the same rule.
- It reads `getRegionalBody` for each region in scope, and `complianceWindow` and `emergencyComplianceWindow`.

Both sets remove by swap-and-pop, so each page and the count it is checked against come from the pinned block. A `seen != count` mismatch aborts the snapshot. The node does not seat a partial set. Enumeration returns raw membership, so the liveness reads drop a lapsed emergency entry that nobody has expired yet.

The node persists no index and no cursor. Each boot builds a new snapshot, so a node that returns after downtime has no gap to recover. Nodes SHOULD expose the `decdn_blacklist_watcher_last_tick_timestamp_seconds` gauge for operational monitoring. See [Appendix: Observability](appendix-observability.md#appendix-observability-and-metrics).

The node MUST NOT accept connections until its initial enumeration completes.

### Following the mutation ledger

`ContentBlacklist` keeps an append-only mutation ledger. The rule is: everything that changes the blacklist is recorded, and only an authorised caller can write. Each state change appends one `Mutation` record. This covers every hash, origin, operator, emergency and expiry write, every regional-body change (registration, deregistration, suspension, suspension ratification, unsuspension) and every compliance-window change. A hash add or re-add always appends a record, because it carries a new notice in its reason. Any other call that leaves the state as it was appends nothing. The two permissionless expiry calls also append a record, because they change the raw membership. Each record carries all that a node needs to apply it, with no other read.

While running, the node polls `mutationCount` by `eth_call` at an interval of at most 5 minutes. This interval is well inside the 1-hour compliance-window floor ([§ Compliance Window](#compliance-window)) and inside the 30-minute default grace of [§ Serving while chain-stale](#serving-while-chain-stale). Each poll pins the governance chain's `safe` block and does all of its reads at that block. A record at or below the `safe` block does not change, so a reorg cannot replace a record that the node applied. On Ethereum the `safe` block trails the head by a few minutes, which stays well inside the compliance windows.

1. The node reads `mutationCount`. If the count is below the cursor, the chain reorganised past the `safe` block. The node then builds a new snapshot as on boot.
2. The node reads `getScopeRegions(operator)`. [ADR 030 § Region-stability window](030-node-region-self-attestation.md#region-stability-window) region and ripening transitions change the scope with no write to `ContentBlacklist`. The ledger holds every record since the node's snapshot, but the snapshot holds only the regions in scope at that time. So when a region enters the scope for the first time since boot, the node pages `blacklistedHashes` for it and filters it as on boot. It replaces what the index holds for that region with the result.
3. If the count is above the cursor, the node reads `getMutations(cursor, …)` in pages up to the count. It applies the records in ledger order and sets the cursor to the count.

The node applies every record in order. It does not filter records by region or by body. Each record overwrites the whole state of its key:

- `HASH_ADD`, `HASH_EMERGENCY_ADD` and `HASH_RATIFY` set the hash present in the record's region with the record's `expiresAt`. Region 0 is the global scope. `HASH_REMOVE` and `HASH_EXPIRE` delete the hash from that region.
- `ORIGIN_SET` and `ORIGIN_EMERGENCY_ADD` set the origin leg present with the record's `expiresAt`. `ORIGIN_CLEAR` and `ORIGIN_EXPIRE` delete it. `OPERATOR_ADD` and `OPERATOR_REMOVE` set and clear the operator leg.
- The body kinds set the body state of the record's region: the body address in `key`, and for `BODY_SUSPEND` the moment an unratified suspension lapses in `expiresAt`.
- `COMPLIANCE_WINDOW_SET` and `EMERGENCY_COMPLIANCE_WINDOW_SET` carry the new window in seconds in `key`.

The index stores no body per hash, and needs none. A body change alters who can write entries for a region. It does not alter which entries are live: entries stay live and enforceable through suspension and deregistration ([§ Regional Governance Bodies](#regional-governance-bodies)). A compliance-window change moves only the `effectiveAt` of later adds. So the body and window records change no deny answer, and the node can apply them without a change to its hash index.

A hash present in a region outside the node's scope does not affect a deny answer, because the node evaluates scope at lookup. Because each record is a full overwrite, a record that a snapshot already reflects leaves the index correct. An add and a remove of the same key between two polls leave the entry count unchanged, but they move the ledger by two records, so the node applies both.

The node expires an entry locally when its clock passes `expiresAt`. No record marks that moment, and the node needs none. The later `HASH_EXPIRE` or `ORIGIN_EXPIRE` record only deletes a key that the node already treats as not live.

A successful poll stamps the chain-freshness clock, whether or not the count moved. Liveness for an address keeps the union of both legs: the node refuses an address while its origin leg or its operator leg is live. `addOperator` writes only the operator leg, so a node that tracked only the origin leg would keep serving a blacklisted operator.

The per-item views `isHashBlacklistedForOperator`, `isOriginBlacklisted`, `isOperatorBlacklisted` and `getHashEntry` remain spot checks. The pre-cache check below uses one. An operator or a monitor can use them to audit the local index. The events remain for indexers and audit.

**Pre-cache check:** Before caching any newly-fetched blob (whether from origin pull-through or peer pull), the node MUST check `isHashBlacklistedForOperator(hash, ownOperatorAddress)` and reject the blob if blacklisted. This enables proactive blacklisting of known-bad hashes before any node caches them, and resolves the node's own regional scope rather than global entries alone.

On startup, nodes always fetch the full current blacklist (global + their region) before accepting connections.

### On Blacklist Event

When a node receives a new blacklisted hash, it must, **in order**:

1. **Stop publishing** — withdraw any DHT STORE records for the hash and stop re-publishing immediately ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale))
2. **Stop serving** — reject any new `StreamRequest` for the hash immediately, returning `Declined` ([§ StreamRequest Response](#streamrequest-response))
3. **Evict from cache** — delete the blob from local storage within the compliance window

The publish-first ordering is critical: continuing to serve a blacklisted hash after the compliance window is non-compliance ([§ Hash Evasion and Origin Blacklisting](#hash-evasion-and-origin-blacklisting)), and the node's signed response for that hash attributes the serve to it ([ADR 005 § Signer binding](005-protocol.md#signer-binding)). Disk eviction can be async; DHT-record and serve suppression must be synchronous.

When a node receives a blacklisted origin address, it additionally stops accepting any `StreamRequest` whose pool is owned by that operator address, and removes all of that origin's NodeIds from its local registry-derived node view.

In-flight streams for a blacklisted hash — or on a pool funded by a newly blacklisted origin — are terminated at the next chunk boundary, after the preimage for the chunk already delivered is revealed. Termination is a stream reset with no `StreamEnd` sentinel, not an error frame: [ADR 005 § `VoucherRejected` semantics](005-protocol.md#voucherrejected-semantics) makes `VoucherRejected` the only `StreamError` that travels mid-stream, and the QUIC code is `NO_ERROR` so the client does not score the node as faulty for discharging a takedown ([ADR 013 § Application Error Codes](013-schema-evolution.md#application-error-codes)). A client that re-requests the hash gets the `Declined` refusal from the open-time gate and tries other nodes.

### Serving while chain-stale

Every serve-path guard that enforces on-chain state reads a local projection. The deny-set, the pool-solvency check, and the per-signer capability cap all advance only while the node's chain reads reach the RPC. During an RPC outage each guard passes on stale state. A takedown, a drained pool, or a spent capability that lands on-chain during the outage is invisible to the node. The node keeps signing stream responses it can no longer vouch for. Serving a hash past its compliance window is non-compliance, and the node signs a `StreamResponse{ok:true}` that attributes the serve to it.

The node bounds this exposure with a grace window. It records the wall-clock time of the last successful blacklist ledger poll. The blacklist is governance-chain state, so the clock follows the governance-chain reads. When the node has been unable to reach the chain for longer than `chain_staleness_grace_sec`, the serve path refuses new requests, discovery opens included. The serve refusal is `NotFound`, which is reputation-benign, so a client re-routes to a peer whose chain reads are live. The node can answer differently once its chain reads recover, so the refusal does not remove it from the client's candidates. This is the runtime counterpart of the boot readiness gate: the boot gate keeps every listener shut until the first enumeration completes, and the staleness gate stops the same signed surfaces when the chain reads go stale after boot.

The signal is the successful poll *tick*, not the last ledger record. A quiet chain appends no records for long periods. Keying on records would take a node dark on any quiet period. A successful tick stamps the clock, including a tick where `mutationCount` did not move. The boot enumeration stamps it too, so a node that has just vetted its deny-set is never treated as stale.

The grace window is one operator-local value, always evaluated. There is no separate on/off flag. An operator who accepts the stale-serving risk sets the window large. The default is 30 minutes, which sits inside the one-hour compliance-window floor, so a node stops serving before an outage-hidden takedown can pass its compliance deadline, while a short RPC blip never takes the node dark. A value of `0` is rejected at config resolution: a zero grace refuses every serve. This gate is distinct from the RPC connectivity watchdog, which only reports reachability to a metric and gates nothing. The window is a node risk preference, not a governance parameter, so it carries no on-chain surface.

A node with no chain configured (development and test) runs no blacklist sync, so the runtime wires no freshness clock and the gate is inactive. This is the same fail-open shape the pool-solvency guards use when no chain is present.

### Regional Scope

A node applies only blacklist entries that are global or match its declared region (`node.region` in config). Entries for other regions are ignored. Nodes are not required to enforce takedowns outside their declared jurisdiction — regional compliance is the operator's legal obligation for their own node. A region change does not take effect for blacklist scope until it has been stable for `REGION_STABILITY_WINDOW`: during that window the node remains in scope for its previous region's entries in addition to the new region's, and `updateRegion` itself reverts before the window elapses (see [ADR 030 § Region-stability window](030-node-region-self-attestation.md#region-stability-window)). This forecloses a reactive flip out of a region the moment an entry lands.

Node region is self-reported and unverified at the protocol level. **Production posture:** self-attested regions are accepted at face value per [ADR 030](030-node-region-self-attestation.md#adr-030-node-region-self-attestation); the IP-geolocation oracle / third-party attestation path was considered and rejected. Reactive misdeclaration (flipping region after an entry lands) is foreclosed by the stability window above. A pre-positioned misdeclaration (a region declared false from registration) is *not* prevented by the protocol and, for a region-scoped entry, leaves the operator out of scope under [§ Regional Scope](#regional-scope) — the backstops are the operator's legal exposure under their actual jurisdiction's content law plus the continuous reputation penalty for latency-vs.-claim contradictions ([ADR 001 § Consequences](001-network.md#consequences)), the canonical soft mitigation. See [ADR 030 § Misdeclaration is operator legal exposure, not a protocol offense](030-node-region-self-attestation.md#misdeclaration-is-operator-legal-exposure-not-a-protocol-offense) for the full decomposition.

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

Local denylist entries take effect on the next reload and behave identically to governance blacklist entries. They are local to the node and require no governance action. This covers operators receiving direct legal notices affecting only their node, or operators proactively removing content they find objectionable.

The lists are hot-reloadable (`decdn node reload` / SIGHUP), which is load-bearing rather than convenient: [§ One-hour removal orders](#one-hour-removal-orders) makes this the only mechanism sized to a sub-day statutory deadline, and a restart-only denylist would put a daemon bounce — dropping every in-flight paid stream — on the critical path of discharging a legal order.

`denied_origins` is unioned with the on-chain origin blacklist at the delivery gate, so the wire refusal cannot distinguish a local entry from a governance one.

### StreamRequest Response

A node refuses a blacklisted hash, and a request whose pool owner is a blacklisted origin, with `Declined` ([ADR 005 § Open-time refusal classes](005-protocol.md#open-time-refusal-classes)). The same code covers the governance blacklist, the local denylist, the on-chain origin blacklist, the local origin denylist, and a blob the operator withdrew with no blacklist entry behind it (a manual `decdn node evict`, or a stored-corruption quarantine, [ADR 040](040-cache-policy.md#pinning-durable-operator-evict-and-the-open-stream-pin-stay-engine-enforced)). A client able to tell these sources apart could map an operator's private legal exposure by sending requests. The distinction survives only in the operator's own metrics (`decdn_serve_stream_rejected_hash_denied_total` for the local list, `…_chain_hash_denied_total` for a governance entry), which no client can read.

A client removes the refusing node for this hash for the rest of the fetch, and tries other nodes. A local entry binds only that node. A client does not end the fetch on the refusal, even for an origin entry: a node's local origin denylist binds only that node, and the wire does not tell it apart from the on-chain list. When every candidate refuses `Declined`, the fetch ends early with "no node will serve this" ([ADR 039 § Failure handling](039-multi-source-parallel-fetch.md#failure-handling-reassign-only-tail)).

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
- Namespace 0 (`namespaceId == 0`) has no authorized origins; only registered namespaces carry a DAO-authorized origin set, so on-chain state is bounded by the number of namespaces rather than by content volume

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
2. Notices are triaged by the `GOVERNANCE_ROLE` holder (deployer pre-handover, then global governance multisig once `TimelockController` is wired)
3. Clearly illegal content (CSAM, actively-exploited material) → emergency multisig path
4. DMCA / DSA notices → appropriate governance body (global or regional) with the notice ID in the `reason` field
5. Repeat-offender origin nodes → global governance vote for origin blacklisting
6. Disputed entries → see [§ Removing a Wrongful Entry](#removing-a-wrongful-entry): `removeHashRegional` for a regional entry (the issuing body's own power) or a DecdnGovernor `removeHashGlobal` proposal for a global one
