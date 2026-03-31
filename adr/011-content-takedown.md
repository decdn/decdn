# ADR 011: Content Takedown and Hash Blacklisting

**Date:** 2026-03-30
**Status:** Draft

## Context

deCDN is delivery infrastructure optimised for regional performance, not a censorship-resistant storage network. Node operators are businesses with real legal obligations: DMCA safe harbour (US), DSA hosting provider duties (EU), and national laws around illegal content (CSAM, terrorist material) require that operators have a working takedown mechanism. Without one, every operator runs uninsured legal exposure.

No existing ADR addresses content removal. This ADR establishes:

1. A governance-controlled on-chain hash blacklist
2. Regional governance bodies for jurisdiction-scoped takedowns
3. Node behaviour when a hash or origin is blacklisted
4. An emergency fast-path for time-critical removals
5. The slashing regime for non-compliance
6. The known limitations of hash-based blacklisting and the mitigations available

## Decision

Content takedown is governed at the network level via an on-chain `ContentBlacklist` contract. Two governance paths exist: a global path (network-wide removal) and a regional path (jurisdiction-scoped removal via a designated regional governance body). Nodes are required to evict blacklisted content and stop announcing it within a defined compliance window. Serving a blacklisted hash after the compliance window is a slashable offense.

Origin nodes that repeatedly source blacklisted content can themselves be blacklisted by NodeId or operator address, independent of any specific hash — this is the primary mitigation for hash evasion via trivial re-encoding.

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
    function emergencyAdd(bytes32 blake3Hash, string calldata reason) external;
    function emergencyAddOrigin(address operatorAddress, string calldata reason) external;

    // Regional body registry — global governance only
    function registerRegionalBody(string calldata region, address body) external;
    function deregisterRegionalBody(string calldata region) external;

    // Views
    function isBlacklisted(bytes32 blake3Hash) external view returns (bool);
    function isBlacklistedInRegion(bytes32 blake3Hash, string calldata region) external view returns (bool);
    function isOriginBlacklisted(address operatorAddress) external view returns (bool);
    function getEntry(bytes32 blake3Hash) external view returns (BlacklistEntry memory);
    function getBlacklistVersion() external view returns (uint256);

    // Events
    event HashBlacklisted(bytes32 indexed blake3Hash, uint256 effectiveAt, string region, string reason, bool emergency);
    event HashRemoved(bytes32 indexed blake3Hash, string region);
    event OriginBlacklisted(address indexed operatorAddress, string reason);
    event OriginRemoved(address indexed operatorAddress);
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

**Blacklist version.** `getBlacklistVersion()` returns a monotonically increasing counter incremented on every add/remove operation across all paths. Nodes cache the last-seen version and only re-fetch deltas when the version advances, minimising RPC load.

**Reason field.** Free-form string, stored on-chain for auditability. Operators can reference legal notice identifiers (e.g., DMCA case numbers, DSA notice IDs) or use short category labels.

**`region` field.** Empty string means the entry applies globally. An ISO 3166-1 alpha-2 code scopes the entry to nodes that declare that region. A node is in scope if its declared region matches the entry's region or the entry is global.

## Regional Governance Bodies

A regional body is an address (multisig or governance contract) registered by global governance for a specific jurisdiction. It can issue region-scoped blacklist entries for its jurisdiction without a global vote. It cannot issue global entries or blacklist origins — those remain global governance only.

**PoC:** No regional bodies are registered. The admin key acts as sole governance. The contract is designed to support regional bodies from day one so they can be added by governance vote without a contract redeploy.

**Production:** Regional bodies are expected for at minimum EU (DSA compliance) and US (DMCA). Each body is a 3-of-5 multisig constituted with signers who have legal presence in the relevant jurisdiction.

Regional bodies operate independently within their scope. A hash blacklisted by the EU body is a compliance obligation only for nodes that declare an EU region. A hash blacklisted globally is a compliance obligation for all nodes regardless of region.

## Compliance Window

| Path | Compliance window |
|------|-------------------|
| Standard governance vote (global) | 24 hours after `effectiveAt` |
| Regional governance body | 24 hours after `effectiveAt` |
| Emergency multisig add | `effectiveAt = addedAt` — slash applies after 2 hours |

The 24-hour window accounts for nodes that are offline or have a long poll interval. The 2-hour emergency window is tight enough to matter for active illegal content while giving online nodes time to act.

The compliance window is a governable parameter (hardcoded bounds: minimum 1 hour, maximum 7 days).

## Hash Evasion and Origin Blacklisting

Hash-based blacklisting covers only exact copies of a blob. A one-byte change produces a completely different BLAKE3 hash and evades the blacklist. This is a known limitation shared by every hash-based content moderation system.

**The protocol's primary response is origin blacklisting.** If an origin-backed node repeatedly sources blacklisted content — whether the same blob or trivially re-encoded variants — governance can blacklist the operator's Ethereum address. A blacklisted origin:

- Is removed from the `StakingRegistry` (same effect as stake ejection)
- Cannot register new nodes under the same address
- Has all its NodeIds excluded from `CacheAnnounce` routing tables

This raises the cost of re-upload evasion from trivial (change a byte) to significant: the operator must fund and register a new identity with fresh stake. Repeat evasion becomes progressively more expensive.

**Perceptual hashing is out of scope for the protocol.** Perceptual hash algorithms (PhotoDNA/PDQF for images, TMK for video) detect near-duplicate content but are content-type specific — there is no single perceptual hash for arbitrary binary blobs. deCDN is content-agnostic and cannot know whether a blob is an image, video, or other data. Perceptual hash checking for known illegal content categories (CSAM) is an operator obligation handled off-chain via industry databases (NCMEC, StopNCII), not a protocol primitive.

**Fast re-reporting path.** When a re-encoded variant of a known-bad blob is identified, governance can add the new hash via the emergency multisig path (2-hour compliance window). The combination of fast re-reporting and origin blacklisting makes sustained evasion operationally difficult even if no single mechanism closes the gap completely.

## Node Behaviour

### Polling

Nodes poll `getBlacklistVersion()` on a configurable interval (`blacklist_poll_interval`, default 10 minutes). When the version has advanced, the node fetches new entries since its last-seen version, filtered to its declared region plus global entries.

On startup, nodes always fetch the full current blacklist (global + their region) before accepting connections.

### On Blacklist Event

When a node receives a new blacklisted hash, it must, **in order**:

1. **Stop announcing** — omit the hash from all future `CacheAnnounce` gossip messages immediately
2. **Stop serving** — reject any new `StreamRequest` for the hash immediately, returning `HashBlacklisted`
3. **Evict from cache** — delete the blob from local storage within the compliance window

The announce-first ordering is critical: announcing content that is then not delivered triggers the phantom-blob detection path (ADR 003). Eviction from disk can be async; announcement suppression must be synchronous.

When a node receives a blacklisted origin address, it additionally stops accepting any `StreamRequest` that presents a channel funded by that operator address, and removes all of that origin's NodeIds from its local routing table.

In-flight streams for a blacklisted hash are terminated at the next MB boundary. The client receives a `HashBlacklisted` error and can request a refund of the unused channel balance.

### Regional Scope

A node applies only blacklist entries that are global or match its declared region (`node.region` in config). Entries for other regions are ignored. Nodes are not required to enforce takedowns outside their declared jurisdiction — regional compliance is the operator's legal obligation for their own node.

Node region is self-reported and unverified at the protocol level. An operator who misreports their region to evade a regional takedown bears the legal risk of that choice — the protocol provides the mechanism; legal compliance is the operator's responsibility.

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
    HashBlacklisted,     // hash is on the governance blacklist or local denylist
    OriginBlacklisted,   // the channel's operator address is blacklisted
}
```

The response does not distinguish between governance and local denylist sources. Clients should retry on a different node.

## Slashing

Serving a blacklisted hash after the compliance window is a slashable offense. It uses the existing escalating schedule from [ADR 004](004-tokenomics.md):

| Offense | Slash |
|---------|-------|
| First | 10% of stake |
| Second within 30 days | 15% of stake |
| Third within 30 days | 100% of stake (full ejection) |

**Slash evidence.** The challenger submits:
- The `blake3Hash`
- A signed `StreamResponse` (or delivery receipt) from the offending node, timestamped after the compliance window
- The `BlacklistEntry.effectiveAt` timestamp showing the compliance window had passed

The `ContentBlacklist` contract verifies that `effectiveAt` is in the past relative to the delivery timestamp and that the hash is still on the blacklist. If the hash was subsequently removed, the slash is invalid.

Regional slash eligibility: a node is only slashable for serving a hash it was in-scope to remove. A node that declared region `US` is not slashable for serving a hash that was only blacklisted by the EU regional body.

**Grace period for offline nodes.** A slash requires an active challenger submitting evidence of a post-window delivery. Nodes that reconnect, sync the blacklist, and evict before serving any content are safe.

## Consequences

**Positive:**

- Global and regional blacklisting coexist in one contract — no separate deployment for jurisdictions
- Regional bodies can act without a global governance vote, matching the speed of real-world legal processes (DSA requires expeditious removal)
- Origin blacklisting makes hash evasion progressively expensive — each re-upload requires fresh stake and a new identity
- Emergency path addresses CSAM and actively-exploited material without a 5-day vote cycle
- Reason field and on-chain audit trail support legal defensibility for operators
- Local denylist preserves operator autonomy for direct legal notices

**Negative:**

- Hash-based blacklisting covers exact copies only; trivial re-encoding evades it. This is a fundamental limitation with no protocol-level solution for content-agnostic blobs
- Node region is self-reported and unverified; regional compliance relies on operator legal incentive, not cryptographic enforcement
- Governance becomes a content moderation body, requiring off-chain processes (abuse intake, legal review) the protocol does not define
- Multiple regional bodies add governance coordination overhead; regional bodies can disagree on scope
- Blacklisted content remains content-addressable and verifiable off-network; eviction stops CDN serving but does not prevent redistribution by other means

## Governance Process (Off-Chain)

The minimum viable process for PoC:

1. A `takedown@` contact address is published alongside the node registry
2. Notices are triaged by the admin key holder (PoC) or global governance multisig (production)
3. Clearly illegal content (CSAM, actively-exploited material) → emergency multisig path
4. DMCA / DSA notices → appropriate governance body (global or regional) with the notice ID in the `reason` field
5. Repeat-offender origin nodes → global governance vote for origin blacklisting
6. Disputed removals → governance vote with a comment period before the timelock executes

## ADRs Affected

- **ADR 001** (Network Topology) — `CacheAnnounce` must suppress blacklisted hashes and exclude blacklisted origin NodeIds; `StreamError::HashBlacklisted` and `StreamError::OriginBlacklisted` are new error variants
- **ADR 002** (Content Addressing) — content-addressed blobs can be removed from the network layer even though the hash remains valid; this is explicitly accepted
- **ADR 004** (Tokenomics) — serving blacklisted content added to the slashable offense list; origin blacklisting triggers same stake ejection path as repeated slashing
- **ADR 009** (Governance) — `ContentBlacklist` contract added to governance-controlled contracts; emergency multisig scope documented in ADR 009 as the single source of truth, covering both contract pausing and content/origin blacklisting; regional body registry introduced as a new governance primitive
