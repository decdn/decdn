# ADR 011: Content Takedown and Hash Blacklisting

**Date:** 2026-03-30
**Status:** Draft

## Context

deCDN is delivery infrastructure optimised for regional performance, not a censorship-resistant storage network. Node operators are businesses with real legal obligations: DMCA safe harbour (US), DSA hosting provider duties (EU), and national laws around illegal content (CSAM, terrorist material) require that operators have a working takedown mechanism. Without one, every operator runs uninsured legal exposure.

Origin assignment is the symmetric problem: which operators are authorized to act as origins for which content. Without a positive authority, origin assignment is purely off-protocol — content owners self-coordinate, the network has no Sybil resistance on origin claims, and there is no protocol-enforced redundancy for important content. Both halves of origin governance — negative (blacklisting) and positive (assignment) — naturally share infrastructure (cross-contract integration, governance authority, runtime enforcement) and are specified together in this ADR.

No existing ADR addressed either question. This ADR establishes:

1. A governance-controlled on-chain hash blacklist
2. Regional governance bodies for jurisdiction-scoped takedowns
3. Node behavior when a hash or origin is blacklisted
4. An emergency fast-path for time-critical removals
5. The slashing regime for non-compliance
6. The known limitations of hash-based blacklisting and the mitigations available
7. A governance-controlled positive authority for origin assignment per registered namespace (`OriginAssignment`), built on the publisher/namespace identity primitive defined in [ADR 002](002-content-addressing.md#publisher-identity-and-namespaces)

## Decision

Content governance over origins has two symmetric authorities, both DAO-controlled:

- **Negative authority — `ContentBlacklist`.** Removes hashes and operators. Two governance paths exist: a global path (network-wide removal) and a regional path (jurisdiction-scoped removal via a designated regional governance body). Nodes are required to evict blacklisted content and stop announcing it within a defined compliance window. Serving a blacklisted hash after the compliance window is a slashable offense. Origin nodes that repeatedly source blacklisted content can themselves be blacklisted by NodeId or operator address, independent of any specific hash — this is the primary mitigation for hash evasion via trivial re-encoding.
- **Positive authority — `OriginAssignment`.** Authorizes specific operators to act as origins for specific namespaces (defined in [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)). Publishers propose operator sets for their namespaces; governance ratifies via the standard timelock path. Default-open content (`namespaceId == 0`) is exempt — any staked operator may serve as origin without DAO authorization. The `ContentBlacklist` and `OriginAssignment` contracts integrate so that blacklisting an operator atomically removes them from every namespace's authorized set.

Each node also maintains a local denylist for operator-initiated removal without waiting for governance.

## Contract: ContentBlacklist

```solidity
interface IContentBlacklist {
    // Global governance path (standard voting + timelock)
    function addHash(bytes32 blake3Hash, string calldata reason) external;
    function removeHash(bytes32 blake3Hash) external;

    // Regional governance path — callable only by a registered regional body
    function addHashRegional(bytes32 blake3Hash, string calldata region, string calldata reason) external;
    function removeHashRegional(bytes32 blake3Hash, string calldata region) external;

    // Origin blacklisting — global governance only
    function addOrigin(address operatorAddress, string calldata reason) external;
    function removeOrigin(address operatorAddress) external;

    // Emergency multisig path (3-of-5, no timelock) — hash and origin
    // Subject to 12-month sunset: blacklistDeadline = deployTimestamp + 365 days
    // (see ADR 009, Emergency Multisig)
    // Category determines emergency entry expiry:
    //   GENERAL — 14-day auto-expiry (default)
    //   CSAM, TERRORIST — 90-day auto-expiry (severe content must not be re-exposed due to governance latency)
    // enum Category { GENERAL, CSAM, TERRORIST }
    function emergencyAdd(bytes32 blake3Hash, uint8 category, string calldata reason) external;
    function emergencyAddOrigin(address operatorAddress, uint8 category, string calldata reason) external;

    // Emergency entries expire after their category-specific deadline unless ratified by governance.
    // Expiry is derived from the entry's addedAt timestamp: addedAt + expiryForCategory(category).
    // isBlacklisted returns false after this deadline unless a governance addHash
    // has been called for the same hash.

    // Regional body registry — global governance only
    function registerRegionalBody(string calldata region, address body) external;
    function deregisterRegionalBody(string calldata region) external;
    function suspendRegionalBody(string calldata region) external;
    function unsuspendRegionalBody(string calldata region) external;

    // Views
    function isBlacklisted(bytes32 blake3Hash) external view returns (bool);
    function isBlacklistedInRegion(bytes32 blake3Hash, string calldata region) external view returns (bool);
    function isOriginBlacklisted(address operatorAddress) external view returns (bool);
    function getEntry(bytes32 blake3Hash) external view returns (BlacklistEntry memory);
    function getBlacklistVersion() external view returns (uint256);

    // Events
    event HashBlacklisted(bytes32 indexed blake3Hash, uint256 indexed version, uint256 effectiveAt, string region, string reason, bool emergency);
    event HashRemoved(bytes32 indexed blake3Hash, uint256 indexed version, string region);
    event OriginBlacklisted(address indexed operatorAddress, uint256 indexed version, string reason);
    event OriginRemoved(address indexed operatorAddress, uint256 indexed version);
}

struct BlacklistEntry {
    bytes32 blake3Hash;
    uint256 addedAt;          // block timestamp when added
    uint256 effectiveAt;      // addedAt + compliance window (0 for emergency adds)
    string  region;           // ISO 3166-1 alpha-2, or "" for global
    string  reason;           // free-form, e.g. "DMCA-2026-001", "CSAM", "DSA-DE-001"
    bool    emergency;        // true if added via emergency multisig path
}
```

> **Gas optimization (production):** The `region` field uses `string` for PoC readability. Production implementations SHOULD use `bytes2` for ISO 3166-1 alpha-2 codes (always exactly 2 ASCII characters), with `bytes2(0)` as the global sentinel. This reduces storage costs.

### Blacklist version

`getBlacklistVersion()` returns a monotonically increasing counter incremented on every add/remove operation across all paths. Nodes cache the last-seen version and only re-fetch deltas when the version advances, minimising RPC load.

### Reason field

Free-form string, stored on-chain for auditability. Operators can reference legal notice identifiers (e.g., DMCA case numbers, DSA notice IDs) or use short category labels.

### `region` field

Empty string means the entry applies globally. An ISO 3166-1 alpha-2 code scopes the entry to nodes that declare that region. A node is in scope if its declared region matches the entry's region or the entry is global.

## Regional Governance Bodies

A regional body is an address (multisig or governance contract) registered by global governance for a specific jurisdiction. It can issue region-scoped blacklist entries for its jurisdiction without a global vote. It cannot issue global entries or blacklist origins — those remain global governance only.

**PoC:** No regional bodies are registered. The admin key acts as sole governance. The contract is designed to support regional bodies from day one so they can be added by governance vote without a contract redeploy.

**Production:** Regional bodies are expected for at minimum EU (DSA compliance) and US (DMCA). Each body is a 3-of-5 multisig constituted with signers who have legal presence in the relevant jurisdiction.

**Suspension:** The emergency multisig can suspend a regional body immediately via `suspendRegionalBody(region)`. Suspended bodies cannot issue new entries but existing entries remain active. Suspension must be ratified or reversed by a governance vote within 14 days (same ratification window as emergency blacklist entries).

Regional bodies operate independently within their scope. A hash blacklisted by the EU body is a compliance obligation only for nodes that declare an EU region. A hash blacklisted globally is a compliance obligation for all nodes regardless of region.

## Compliance Window

| Path | Compliance window |
|------|-------------------|
| Standard governance vote (global) | 24 hours after `effectiveAt` |
| Regional governance body | 24 hours after `effectiveAt` |
| Emergency multisig add | `effectiveAt = addedAt` — slash applies after 2 hours |

The 24-hour window accounts for nodes that are offline or have a long poll interval. The 2-hour emergency window is tight enough to matter for active illegal content while giving online nodes time to act. The emergency multisig path is subject to a 12-month sunset (`blacklistDeadline = deployTimestamp + 365 days`) — see [ADR 009](009-governance.md#emergency-multisig).

The compliance window is a governable parameter (hardcoded bounds: minimum 1 hour, maximum 7 days).

## Hash Evasion and Origin Blacklisting

Hash-based blacklisting covers only exact copies of a blob. A one-byte change produces a completely different BLAKE3 hash and evades the blacklist. This is a known limitation shared by every hash-based content moderation system.

**The protocol's primary response is origin blacklisting.** If an origin-backed node repeatedly sources blacklisted content — whether the same blob or trivially re-encoded variants — governance can blacklist the operator's Ethereum address. `ContentBlacklist.addOrigin()` calls `StakingRegistry.ejectNode(operatorAddress)` via a cross-contract call; the `StakingRegistry` grants the `ContentBlacklist` contract address the `BLACKLIST_ROLE`, permitting this call. A blacklisted origin:

- **Ejected from `StakingRegistry`** — sets `active = false`, emits `NodeAutoEjected` ([ADR 001](001-network.md)). This follows the same code path as stake-based auto-ejection ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn))
- **Effectively removed from every namespace's authorized origin set** — runtime checks (probes, peer-table validation, slashing evaluation) treat a blacklisted operator as unauthorized regardless of `OriginAssignment` membership; see [§ Origin Assignment Authority — interaction with ContentBlacklist](#interaction-with-contentblacklist). Storage cleanup is a separate, permissionless step (`OriginAssignment.pruneBlacklistedAssignments`) that can be called per-namespace by anyone, avoiding the unbounded gas cost of iterating every assignment at blacklist time
- **Remaining stake enters forced unbonding** — the standard unbonding period applies (7 days PoC / governable in production, minimum 3 days). Stake remains slashable during unbonding ([ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake))
- **Address banned while blacklisted** — cannot register new nodes under the same Ethereum address unless governance removes the blacklist entry via `removeOrigin(operatorAddress)`. Re-entry otherwise requires a new identity funded with fresh stake (minimum 50,000 TOKEN — [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake))
- **The operator's registered NodeId is excluded from peer tables** — gossip validation rejects messages from that blacklisted node

> **Ejection vs. slashing.** Origin blacklisting triggers ejection (forced unbonding of remaining stake), *not* the escalating slash schedule. The operator's stake is not burned — it is returned after the unbonding period, assuming no separate slashable offense occurs during unbonding. By contrast, *serving* a blacklisted hash after the compliance window is a slashable offense under the escalating schedule in [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn), where stake is partially burned and the challenger is rewarded. A node operator can face both: slashing for serving blacklisted content, followed by origin blacklisting and ejection if the behavior persists.

This raises the cost of re-upload evasion from trivial (change a byte) to significant: the operator must fund and register a new identity with fresh stake. Repeat evasion becomes progressively more expensive.

**Perceptual hashing is out of scope for the protocol.** Perceptual hash algorithms (PhotoDNA/PDQF for images, TMK for video) detect near-duplicate content but are content-type specific — there is no single perceptual hash for arbitrary binary blobs. deCDN is content-agnostic and cannot know whether a blob is an image, video, or other data. Perceptual hash checking for known illegal content categories (CSAM) is an operator obligation handled off-chain via industry databases (NCMEC, StopNCII), not a protocol primitive.

### Fast re-reporting path

When a re-encoded variant of a known-bad blob is identified, governance can add the new hash via the emergency multisig path (2-hour compliance window). The combination of fast re-reporting and origin blacklisting makes sustained evasion operationally difficult even if no single mechanism closes the gap completely.

## Origin Assignment Authority

The mechanisms above describe the DAO's *negative* authority over origins: blacklisting bad actors. This section specifies the symmetric *positive* authority: which operators are authorized to act as origin backers for which content.

### Why positive authority is part of governance

Without positive authority, origin assignment is purely off-protocol — content owners independently configure backends and the network has no on-chain notion of "this operator is responsible for serving namespace X". This is workable for content owners who can run their own infrastructure but provides no protocol-level guarantees: no Sybil resistance on origin claims (anyone with stake can claim to be an origin), no enforced redundancy (a single origin operator can be a single point of failure), no accountability path for takedown compliance failures (governance can blacklist after the fact but cannot pre-authorize). Adding positive authority gives the DAO a tool to grant *and* withhold the origin role, mirroring the existing tool to remove it.

The publisher and namespace primitives are defined in [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces). Recap:

- A **publisher** is an Ethereum address registered in `PublisherRegistry`.
- A **namespace** is a publisher-owned `uint256` identifier under which blob hashes are claimed.
- The **default-open namespace** (`namespaceId == 0`) governs all unclaimed content; any staked operator may serve as origin for default-open content. Origin assignment authority applies only to non-zero namespaces.

### Contract: OriginAssignment

```solidity
interface IOriginAssignment {
    // Publisher proposes a candidate origin set for one of their namespaces.
    // Reverts if msg.sender is not the namespace owner, if any operator is not
    // active in StakingRegistry at proposal time, or if the operator count is
    // outside [minRedundancy, maxOriginsPerNamespace].
    function proposeAssignment(uint256 namespaceId, address[] calldata operators) external;

    // Governance ratifies a pending proposal after the assignment timelock.
    // Reverts if no pending proposal exists or if the timelock has not elapsed.
    function activateAssignment(uint256 namespaceId) external;

    // Revocation paths:
    //  - Publisher may revoke a single operator from their own namespace at any time.
    //  - Governance may revoke an operator from any namespace.
    // Revocation that drops the active set below minRedundancy is allowed; the
    // namespace simply enters an under-redundant state until a new proposal is
    // activated. The min-redundancy invariant binds activations, not revocations.
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

    // Governable parameters with safety bounds (see ADR 009)
    function setMinRedundancy(uint256 floor) external;
    function setAssignmentTimelock(uint256 secondsDelay) external;

    // Views
    function isAuthorizedOrigin(uint256 namespaceId, address operator) external view returns (bool);
    // Historical view used by SlashJudge for phantom-origin evidence — true if
    // the operator was in the namespace's authorized set at `timestamp`. See
    // "Historical state and slashing evidence" below for the storage model and
    // the multi-cycle (re-authorization) semantics.
    function isAuthorizedOriginAt(uint256 namespaceId, address operator, uint64 timestamp)
        external view returns (bool);
    function getOrigins(uint256 namespaceId) external view returns (address[] memory);
    function getPendingAssignment(uint256 namespaceId)
        external view returns (address[] memory operators, uint256 readyAt);

    // Events
    event AssignmentProposed(uint256 indexed namespaceId, address indexed proposer, address[] operators, uint256 readyAt);
    event AssignmentActivated(uint256 indexed namespaceId, address[] operators);
    event AssignmentRevoked(uint256 indexed namespaceId, address indexed operator, address indexed by);
    event BlacklistedAssignmentPruned(uint256 indexed namespaceId, address indexed operator, address indexed pruner);
}
```

### Lifecycle

1. **Publisher proposal.** The publisher calls `proposeAssignment(namespaceId, operators)`. The contract validates that the proposer owns the namespace, that every candidate is currently active in `StakingRegistry`, and that the operator count satisfies the `minRedundancy` and `maxOriginsPerNamespace` bounds. The proposal enters a pending state with a `readyAt` timestamp computed as `block.timestamp + assignmentTimelock` (governance-bounded between 24 hours and 14 days; see [ADR 009](009-governance.md)).
2. **Governance ratification.** During the timelock window, governance reviews the proposal off-chain. After the timelock elapses, a governance proposal calls `activateAssignment(namespaceId)`. Activation replaces the namespace's authorized operator set with the pending operators atomically.
3. **Operator notification.** Operators in the activated set are now authorized to act as origins for the namespace. They configure their origin store locally and respond `is_origin: true` to probes for the namespace's content (see [ADR 005 § cdn/probe/v1](005-protocol.md#cdnprobev1--latency-probe)).
4. **Revocation.** A publisher may unilaterally remove an operator from their own namespace's set (e.g., the operator is performing poorly). Governance may revoke any operator from any namespace via the standard proposal path (e.g., the operator is misbehaving but has not yet crossed the blacklist threshold). `ContentBlacklist.addOrigin` triggers `removeAllAssignments` automatically; see [§ Hash Evasion and Origin Blacklisting](#hash-evasion-and-origin-blacklisting).

The two-step propose-then-ratify flow is deliberate: it gives publishers agency over which operators they trust (publishers know their content best) while keeping the DAO as the authority that confirms the assignment is consistent with protocol-wide policy (e.g., not concentrating too many namespaces on a small operator set, not assigning to operators with poor reputation). Either party can refuse to advance the flow — publishers by not proposing, governance by not ratifying — and the namespace simply continues with its existing assignment (or remains unassigned).

### Default-open and unassigned namespaces

A namespace that has no activated assignment is **unassigned**. No operator is authorized as origin for unassigned content, but the protocol still permits cache-only serving from any staked operator that happens to hold the blob — see [ADR 005 § cdn/probe/v1](005-protocol.md#cdnprobev1--latency-probe). Publishers who claim content but never propose an assignment effectively prevent any new origin from picking up the content from canonical storage; cached copies eventually expire. This is by design — it lets a publisher delete their content set from the network by claiming the hashes and refusing to assign origins.

The default-open namespace (`namespaceId == 0`) is exempt from this contract entirely. Default-open content has no `OriginAssignment` entry; any staked operator may serve as origin without DAO authorization. This is the long-tail / permissionless fallback for content whose publisher has not registered.

### Minimum-redundancy invariant

The contract enforces `operators.length >= minRedundancy` at proposal time and at activation time, and additionally rejects proposals whose `operators` array contains duplicate addresses (without this, a publisher could submit `[A, A, A]` to satisfy `minRedundancy = 3` while still concentrating origin responsibility on a single operator). `minRedundancy` is a governance-bounded parameter (see [ADR 009](009-governance.md); range `[1, 10]`, default `3`) and is constrained by the cross-parameter invariant `1 ≤ minRedundancy ≤ maxOriginsPerNamespace`. The invariant ensures that no registered namespace can be activated with a single point of failure. The invariant is *not* enforced on revocation — a publisher or governance may revoke operators down to zero, but new activations must satisfy the floor. Under-redundant namespaces are observable via the `getOrigins` view; clients and watchtowers may surface this as a health indicator for the namespace's owner.

### Historical state and slashing evidence

Phantom-origin slashing requires `SlashJudge` to verify that an operator was *not* in a namespace's authorized set at the timestamp of a signed `ProbeResponse` (see [ADR 005 § cdn/probe/v1](005-protocol.md#cdnprobev1--latency-probe)). Mutating storage in place would make this impossible to evaluate retroactively — an honest operator that was authorized at probe time could be revoked an hour later and then falsely slashed using the now-stale probe response.

`OriginAssignment` therefore stores a per-(namespace, operator) checkpoint history: an append-only array of `{activatedAt, revokedAt}` entries. `revokedAt = type(uint64).max` marks an entry as currently active. `isAuthorizedOriginAt(namespaceId, operator, t)` returns `true` iff some checkpoint satisfies `activatedAt <= t < revokedAt`. The query is O(log N) with binary search over the checkpoint array; in practice `N` per pair is tiny (most operators are activated once, revoked once, never re-activated).

The slash-evidence-age bound from [ADR 009](009-governance.md) (default 7 days, range 1–30 days) limits how old a probe response may be when submitted as evidence. Checkpoint arrays older than the maximum evidence age may be pruned by a permissionless garbage-collection call; the contract retains only the entries needed to evaluate the current evidence window plus a margin for in-flight challenges. This caps storage growth at `O(maxEvidenceAge × authorization_churn)` per pair rather than unbounded history.

Re-authorization of a previously revoked operator appends a new checkpoint; older checkpoints continue to authorize old probe responses correctly. The publisher / governance can revoke and re-activate freely without invalidating in-flight evidence.

### Cross-contract integration

- `OriginAssignment` reads `PublisherRegistry.ownerOf(namespaceId)` to validate proposer ownership.
- `OriginAssignment` reads `StakingRegistry.isActive(operator)` to validate origin candidates at proposal time. The check is opportunistic, not enforced at probe time — an operator who unbonds mid-assignment is filtered by clients via the standard staking check, not by `OriginAssignment` (avoiding expensive cross-contract checks on every assignment lookup).
- `OriginAssignment.pruneBlacklistedAssignment` reads `ContentBlacklist.isOriginBlacklisted(operator)` to decide whether to remove an entry. Permissionless callers can clean up storage one (`namespaceId`, operator) pair at a time.
- `ContentBlacklist.addOrigin(operator)` does not call into `OriginAssignment` — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist) below for the rationale and the runtime-check pattern.

### Interaction with ContentBlacklist

`ContentBlacklist.addOrigin(operator)` does **not** call `OriginAssignment` to evict the operator from every namespace. The naïve approach — iterate over every namespace the operator is assigned to and remove them in one transaction — is unbounded: an operator in N namespaces costs O(N) storage writes, and a prolific operator could exceed the block gas limit, blocking the blacklist transaction entirely.

Instead, security is enforced at runtime by checking both contracts:

- **Probe-time** ([ADR 005 § cdn/probe/v1](005-protocol.md#cdnprobev1--latency-probe)): a node MUST consult `ContentBlacklist.isOriginBlacklisted(self)` before signing `is_origin: true`, in addition to `OriginAssignment.isAuthorizedOrigin`. A blacklisted operator that signs `is_origin: true` is slashable as a phantom-origin offense regardless of the stale `OriginAssignment` entry.
- **Requester-side** ([ADR 005](005-protocol.md)): probing requesters apply the same combined check before accepting an `is_origin: true` claim from a peer.
- **Slashing evidence** ([ADR 011 § Slashing](#slashing)): `SlashJudge` evaluates phantom-origin evidence by checking both contracts at the response timestamp; either a missing `OriginAssignment` entry OR a present `ContentBlacklist` blacklist entry constitutes unauthorized origin behaviour.

Storage cleanup happens lazily and permissionlessly via `OriginAssignment.pruneBlacklistedAssignment(namespaceId, operator)`. Each call removes one entry; anyone may call it (the contract checks `ContentBlacklist.isOriginBlacklisted` itself, so the caller cannot grief by claiming a non-blacklisted operator is blacklisted). Watchtowers and reputation services will likely run pruning jobs as a public good. Pruning is a cleanup optimisation, not a security primitive — the security guarantee is the runtime check, not the storage state.

This pattern means: blacklisting an operator is an O(1) on-chain action (one ejection call), runtime checks are O(1) per probe (two views), and storage cleanup is O(1) per call with no transaction-size limit. No design path requires iterating over an operator's full namespace set.

### Permissionless property

This authority extends the DAO's role from negative-only (blacklisting) to positive-and-negative (assignment + blacklisting). Permissionless cache-only serving is unaffected: any staked operator may continue to fetch cached blobs from authorized origins and re-serve them to clients regardless of `OriginAssignment` membership. Only the *origin* role — being the canonical source-of-truth for content under a registered namespace — becomes DAO-gated. Default-open content remains permissionlessly origin-servable. Together these constraints preserve the network's open-participation property for the cache role and for unregistered content while giving registered publishers the on-chain durability and accountability guarantees the registry exists to provide. See [ADR 001](001-network.md) for the updated permissionless-role model.

## Node Behavior

### Polling

Nodes poll `getBlacklistVersion()` on a configurable interval (`blacklist_poll_interval`, default 10 minutes). When the version has advanced, the node fetches new entries since its last-seen version, filtered to its declared region plus global entries. Delta fetching relies on contract event logs: `HashBlacklisted` and `OriginBlacklisted` events include an indexed `version` field, enabling efficient `eth_getLogs` queries filtered by version range. Nodes SHOULD expose `blacklist_sync_lag_seconds` and `blacklist_version_behind` metrics for operational monitoring — see [architecture.md § Observability](architecture.md#observability).

#### Version sync recovery

If a node has been offline or missed multiple version bumps, delta fetching may be insufficient (events may have been pruned from the RPC provider's log retention window). The recovery strategy is:

1. If the gap between `last_seen_version` and `current_version` is ≤ 100 versions: fetch deltas normally via contract events.
2. If the gap exceeds 100 versions (or the delta fetch fails): perform a full re-sync by calling `getBlacklistVersion()` and iterating all events from the contract's deployment block. This is expensive but correct.
3. As a fallback, if the full event log is unavailable (RPC provider pruned old events): the node fetches the current blacklist state by calling `isBlacklisted` for all hashes in its local cache. This is O(cache_size) RPC calls but ensures no stale content is served.

The node MUST NOT accept connections until its blacklist is synced to the current version.

**Pre-cache check:** Before caching any newly-fetched blob (whether from origin pull-through or peer pull), the node MUST check `isBlacklisted(hash)` and reject the blob if blacklisted. This enables proactive blacklisting of known-bad hashes before any node caches them.

On startup, nodes always fetch the full current blacklist (global + their region) before accepting connections.

### On Blacklist Event

When a node receives a new blacklisted hash, it must, **in order**:

1. **Stop announcing** — omit the hash from `popular_hashes` in all future `NodeAnnounce` gossip messages immediately
2. **Stop serving** — reject any new `StreamRequest` for the hash immediately, returning `HashBlacklisted`
3. **Evict from cache** — delete the blob from local storage within the compliance window

The announce-first ordering is critical: announcing content that is then not delivered triggers the phantom-blob detection path ([ADR 005 — `cdn/probe/v1`](005-protocol.md#cdnprobev1--latency-probe)). Eviction from disk can be async; announcement suppression must be synchronous.

When a node receives a blacklisted origin address, it additionally stops accepting any `StreamRequest` that presents a channel funded by that operator address, and removes all of that origin's NodeIds from its local peer table.

In-flight streams for a blacklisted hash are terminated at the next MB boundary. The client receives a `HashBlacklisted` error and can request a refund of the unused channel balance.

### Regional Scope

A node applies only blacklist entries that are global or match its declared region (`node.region` in config). Entries for other regions are ignored. Nodes are not required to enforce takedowns outside their declared jurisdiction — regional compliance is the operator's legal obligation for their own node.

Node region is self-reported and unverified at the protocol level. **PoC acceptance:** the PoC accepts self-reported regions as sufficient. An operator who misreports their region to evade a regional takedown bears the legal risk of that choice — the protocol provides the mechanism; legal compliance is the operator's responsibility. **Production mitigation:** IP-geolocation cross-checking via a decentralized oracle or third-party attestation service (consistent with the approach in [ADR 001](001-network.md)). Regional takedowns would then be enforced against both declared region and verified geolocation, with a mismatch triggering a compliance review. This is deferred to production because IP-geolocation infrastructure adds complexity and a new external dependency.

### Local Denylist

Each node supports a local denylist in config:

```toml
[content]
denied_hashes = [
    "blake3:abcdef1234...",
]
denied_origins = [
    "0xOperatorAddress...",
]
```

Local denylist entries take effect immediately and behave identically to governance blacklist entries. They are not gossiped to peers and require no governance action. This covers operators receiving direct legal notices affecting only their node, or operators proactively removing content they find objectionable.

### StreamRequest Response

```rust
enum StreamError {
    // ... existing errors ...
    HashBlacklisted,      // hash is on the governance blacklist or local denylist
    OriginBlacklisted,    // the channel's operator address is blacklisted
    UnauthorizedOrigin,   // requester asked the node to act as origin (e.g., over a payment
                          // channel that flags origin-only delivery) but the node is not in
                          // the namespace's OriginAssignment set; cache-only delivery from
                          // this node remains available via a normal StreamRequest
}
```

The response does not distinguish between governance and local denylist sources. Clients should retry on a different node. `UnauthorizedOrigin` is distinct: the node is reachable and may have the blob, but cannot act as the canonical origin. Requesters that strictly require an origin source (rather than a cache copy) should retry against the namespace's authorized operator set (`OriginAssignment.getOrigins(namespaceId)`); requesters that accept cache delivery should retry the same node with the origin-only flag cleared.

## Slashing

Serving a blacklisted hash after the compliance window is a slashable offense, subject to the escalating schedule in [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn). Repeated offenses trigger cumulative stake loss; nodes whose stake drops below 50% of the minimum are auto-ejected. Individual slash percentages are capped at 50% per offense ([ADR 009 § Safety bounds](009-governance.md#governable-parameters-with-safety-bounds)). The standard 100 TOKEN challenge bond from [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) applies.

### Slash evidence

The challenger submits:

- The `blake3Hash`
- A node-signed `ProbeResponse` with `has_blob: true` for the blacklisted hash, timestamped after the compliance window. The probe signature (defined in [ADR 005](005-protocol.md)) cryptographically binds the node's identity to the hash claim. On-chain verification uses the dual-key scheme from [ADR 014](014-on-chain-verification.md): the `slash_sig` (EIP-712 secp256k1 signature) is verified via `ecrecover`, and the recovered address is mapped to the node's identity in `StakingRegistry`. This is the primary evidence path. Alternatively, a node-signed `StreamResponse` with `ok: true` for the blacklisted hash (binding `hash` and `channel_id` in the signed data) is also sufficient. Client-signed vouchers alone are NOT sufficient evidence — vouchers do not contain the hash and the `channel_id → hash` binding is not on-chain verifiable.
- The `BlacklistEntry.effectiveAt` timestamp showing the compliance window had passed

The `ContentBlacklist` contract verifies that `effectiveAt` is in the past relative to the delivery timestamp and that the hash is still on the blacklist. If the hash was subsequently removed, the slash is invalid.

Regional slash eligibility: a node is only slashable for serving a hash it was in-scope to remove. A node that declared region `US` is not slashable for serving a hash that was only blacklisted by the EU regional body.

### Grace period for offline nodes

A slash requires an active challenger submitting evidence of a post-window delivery. Nodes that reconnect, sync the blacklist, and evict before serving any content are safe.

## Consequences

**Positive:**

- Global and regional blacklisting coexist in one contract — no separate deployment for jurisdictions
- Regional bodies can act without a global governance vote, matching the speed of real-world legal processes (DSA requires expeditious removal)
- Origin blacklisting makes hash evasion progressively expensive — each re-upload requires fresh stake and a new identity
- Emergency path addresses CSAM and actively-exploited material without a 5-day vote cycle
- Reason field and on-chain audit trail support legal defensibility for operators
- Local denylist preserves operator autonomy for direct legal notices
- Origin assignment authority gives publishers a protocol-level way to commit specific operators to serving their content with an enforced minimum-redundancy invariant — no withholding-by-single-origin failure mode for registered namespaces
- Symmetric blacklist/assignment infrastructure means a blacklisted operator is atomically evicted from every namespace they were authorized to serve — no manual reassignment, no stale entries
- Default-open namespace preserves the long-tail / permissionless story for content whose owner has not registered

**Negative:**

- Hash-based blacklisting covers exact copies only; trivial re-encoding evades it. This is a fundamental limitation with no protocol-level solution for content-agnostic blobs
- Node region is self-reported and unverified; regional compliance relies on operator legal incentive, not cryptographic enforcement
- Governance becomes a content moderation body, requiring off-chain processes (abuse intake, legal review) the protocol does not define
- Multiple regional bodies add governance coordination overhead; regional bodies can disagree on scope
- Blacklisted content remains content-addressable and verifiable off-network; eviction stops CDN serving but does not prevent redistribution by other means
- Origin assignment authority extends governance into a new category — positive node-role authorization — that did not previously exist. Capture risk and operator-concentration risk are now governance concerns, not just off-protocol coordination concerns
- The strict-gating model amends the unconditional permissionless-origin claim from [ADR 001](001-network.md). Cache-only role is preserved as permissionless, but the origin role for registered namespaces is governance-gated. The default-open namespace partially mitigates this by preserving permissionless origin for unregistered content

## Governance Process (Off-Chain)

The minimum viable process for PoC:

1. A `takedown@` contact address is published alongside the node registry
2. Notices are triaged by the admin key holder (PoC) or global governance multisig (production)
3. Clearly illegal content (CSAM, actively-exploited material) → emergency multisig path
4. DMCA / DSA notices → appropriate governance body (global or regional) with the notice ID in the `reason` field
5. Repeat-offender origin nodes → global governance vote for origin blacklisting
6. Disputed removals → governance vote with a comment period before the timelock executes

## ADRs Affected

- **ADR 001** (Network Topology) — `NodeAnnounce` must suppress blacklisted hashes from `popular_hashes`; blacklisted origin NodeIds are excluded from peer tables; `StreamError::HashBlacklisted`, `StreamError::OriginBlacklisted`, and `StreamError::UnauthorizedOrigin` are new error variants; the unconditional permissionless-origin claim in the consequences section is amended to reflect DAO-gated origin role
- **ADR 002** (Content Addressing) — content-addressed blobs can be removed from the network layer even though the hash remains valid; this is explicitly accepted. The Publisher Identity and Namespaces section in ADR 002 defines the primitives this ADR's `OriginAssignment` mechanism builds on
- **ADR 003** (Payments) — multi-origin redundancy is now DAO-supervised via the `OriginAssignment` minimum-redundancy invariant rather than off-protocol content-owner coordination
- **ADR 005** (Protocol) — probe responses include an `is_origin` field; nodes must verify `OriginAssignment` membership before responding `is_origin: true` for content in a registered namespace
- **[ADR 026](026-gauge-boost-tokenomics.md)** (Tokenomics) — serving blacklisted content added to the slashable offense list; origin blacklisting triggers same stake ejection path as repeated slashing
- **ADR 009** (Governance) — `ContentBlacklist` and `OriginAssignment` contracts added to governance-controlled contracts; emergency multisig scope documented in ADR 009 as the single source of truth, covering both contract pausing and content/origin blacklisting; regional body registry introduced as a new governance primitive; new governable parameters with safety bounds for assignment timelock, minimum redundancy, and per-publisher namespace caps
- **ADR 016** (Contract Interactions) — `PublisherRegistry` and `OriginAssignment` added to the contract inventory, deployment order, call graph, role matrix, and reentrancy analysis
- **ADR 022** (Content Discovery) — DHT STORE remains permissionless; the `OriginAssignment` view layer is consulted at probe / request time rather than at DHT publish time, and the existing "publisher" terminology in ADR 022 is qualified to distinguish DHT STORE publishers from on-chain content publishers
