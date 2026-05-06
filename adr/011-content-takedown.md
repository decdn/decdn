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
7. A governance-controlled positive authority for origin assignment across all namespaces (`OriginAssignment`) — per-namespace publisher-propose / DAO-ratify for registered namespaces, single global DAO-maintained allow-list for the default-open namespace — built on the publisher/namespace identity primitive defined in [ADR 002](002-content-addressing.md#publisher-identity-and-namespaces)

## Decision

Content governance over origins has two symmetric authorities, both DAO-controlled:

- **Negative authority — `ContentBlacklist`.** Removes hashes and operators. Two governance paths exist: a global path (network-wide removal) and a regional path (jurisdiction-scoped removal via a designated regional governance body). Nodes are required to evict blacklisted content and stop announcing it within a defined compliance window. Serving a blacklisted hash after the compliance window is a slashable offense. Origin nodes that repeatedly source blacklisted content can themselves be blacklisted by NodeId or operator address, independent of any specific hash — this is the primary mitigation for hash evasion via trivial re-encoding.
- **Positive authority — `OriginAssignment`.** Authorizes specific operators to act as origins for specific namespaces (defined in [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces)). For registered namespaces, publishers propose operator sets. Governance ratifies each proposal via the standard timelock path. For default-open content (`namespaceId == 0`), the DAO maintains a single global allow-list (see [§ Default-open allow-list](#default-open-allow-list)). `ContentBlacklist` and `OriginAssignment` integrate via runtime checks with lazy storage cleanup — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist).

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

    // Per-entry appeal flow (regional entries only — see § Blacklist Entry Appeals).
    // openBlacklistAppeal reverts on global entries (region == "") and on entries
    // already past their filing window. Bond is pulled via TOKEN.transferFrom.
    function openBlacklistAppeal(
        bytes32 blake3Hash,
        string  calldata region,
        bytes32 evidenceBundleHash
    ) external returns (uint256 appealId);
    function fastTrackAppeal(uint256 appealId) external;        // emergency multisig only
    function rejectAppeal(uint256 appealId) external;           // emergency multisig only
    function ratifyAppealRemoval(uint256 appealId) external;    // ve-Governor only
    function reverseAppeal(uint256 appealId) external;          // ve-Governor only

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
    event BlacklistAppealOpened(uint256 indexed appealId, bytes32 indexed blake3Hash, string region, address indexed filer, bytes32 evidenceBundleHash);
    event BlacklistAppealFastTracked(uint256 indexed appealId);
    event BlacklistAppealRejected(uint256 indexed appealId);
    event BlacklistAppealRatified(uint256 indexed appealId);
    event BlacklistAppealReversed(uint256 indexed appealId);
    event BlacklistAppealLapsed(uint256 indexed appealId);
}

struct BlacklistEntry {
    bytes32 blake3Hash;
    uint256 addedAt;          // block timestamp when added
    uint256 effectiveAt;      // addedAt + compliance window (0 for emergency adds)
    string  region;           // ISO 3166-1 alpha-2, or "" for global
    string  reason;           // free-form, e.g. "DMCA-2026-001", "CSAM", "DSA-DE-001"
    bool    emergency;        // true if added via emergency multisig path
    bool    suspended;        // true while a regional appeal is in interim-relief
                              // or pending ratification; isBlacklisted views return
                              // false during this window. See § Blacklist Entry Appeals.
    uint256 suspendedAt;      // block timestamp at which suspended last flipped to true;
                              // used to compute evidence-age windows for slash challenges
                              // that replay across resumption. 0 if never suspended.
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

## Blacklist Entry Appeals

Regional bodies acting in good faith can still issue entries that are later contested — a wrongly served takedown notice, a regional body that drifts outside its declared jurisdiction, or a notice that misidentifies content. The body-level `suspendRegionalBody` mechanism is the right tool when a body is systemically misbehaving but is the wrong tool for a single disputed entry: it freezes every entry the body has issued, including legitimate ones. This section specifies a per-entry path that mirrors the fast-track + ratification structure used for regional-body suspension and for [ADR 028 § Slashing appeals](028-slashing-appeals.md), narrowed to the content-policy domain.

The two appeal paths are deliberately decoupled. ADR 028 covers operator-side restitution for slashes incurred during legitimate operational failure. The path defined here covers content-policy challenges to the underlying blacklist entry itself. Operators slashed under an entry that is later removed by this path may seek individual restitution via [ADR 028 § Slashing appeals](028-slashing-appeals.md) — that ADR already lists blacklist offenses as appealable.

### Scope

Appellable entries:

- **Regional entries** (primary case). Issued via `addHashRegional`.
- **Emergency entries** during their live window (secondary). The category-specific auto-expiry from [§ Compliance Window](#compliance-window) is the structural deterrent against indefinite emergency use; the appeal path here addresses disputes during the live window.

Global standard-vote entries (`addHash`) are explicitly **not** appellable through this fast-track. They have already passed the full ve-Governor process and re-litigation belongs in a standard governance amendment, not this lighter-weight path. The slow-path override remains available — see [§ Global Override](#global-override) below.

Grounds for appeal:

- **(a) Out-of-scope.** The regional body issued an entry outside its declared jurisdiction (e.g., the EU body blacklisting content with no documented EU nexus).
- **(b) Procedural defect.** The body deviated from its own constituted process (e.g., a 3-of-5 multisig issued an entry with only two valid signatures).
- **(c) Substantive defect / wrongful takedown.** The underlying notice is invalid — a valid DMCA counter-notice was already filed, the publisher has jurisdictional immunity, the content does not match the notice.

**PoC degradation.** During the bootstrap window described in [§ Regional Governance Bodies](#regional-governance-bodies) — no regional bodies registered, admin key acts as sole governance — the appeal path is structurally non-functional: `addHash` and emergency entries are issued by the same admin key that would sit on the multisig hearing the appeal, and `openBlacklistAppeal` reverts on global entries (`region == ""`) which are the only kind issued in PoC. The slow-path global override (see [§ Global Override](#global-override)) is the operative recourse during PoC and remains the operative recourse for global entries in production. The fast-track path defined here becomes load-bearing only after the first regional body is registered by governance vote, at which point regional `addHashRegional` entries are issuable and an independent multisig + ve-Governor pairing exists to hear appeals. Implementations of `openBlacklistAppeal` MAY revert with `AppealPathNotYetActive` until the first regional body is registered, surfacing the degradation explicitly to clients rather than failing on the `region == ""` check downstream.

### Standing

Any of the following may file an appeal:

1. **The affected publisher** per [ADR 002 § Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces) — the namespace owner whose content the disputed hash falls under, including for default-open content where the publisher of record has been independently established off-chain.
2. **Any operator currently in compliance scope.** An operator whose declared `node.region` matches the entry's region — i.e., one whose stake is exposed to slashing under the entry. This catches operator-side disputes (compliance burden, jurisdictional mismatch with the operator's own legal posture).
3. **Any TOKEN holder** with balance ≥ `APPEAL_FILER_TOKEN_THRESHOLD` (default 10,000 TOKEN; governable bounds `[1,000, 100,000]`). This opens a proxy path for end users and content advocates without requiring on-chain content ownership; the bond and frequency cap below are the deterrents against frivolous filings.

Standing is verified at the filing block. An operator who unbonds after filing, or a TOKEN holder who falls below the threshold mid-flow, does not lose standing for an already-open appeal — but cannot file new ones until standing is restored.

**Synthetic-standing clawback.** A TOKEN holder who crosses `APPEAL_FILER_TOKEN_THRESHOLD` only via a flash loan or short-term borrow, files an appeal, and unwinds the position within `STANDING_LOOKBACK_BLOCKS` (default 7,200 blocks ≈ 24 hours on a 12-second L2 block time, fixed) is treated as failing the standing check retroactively. The contract pulls the standing balance check at filing block T and re-checks it at block T + `STANDING_LOOKBACK_BLOCKS`; if the second check shows the filer's balance has dropped below the threshold without a proportional refund of the original loan source, the appeal is closed and the bond is forfeited (100% burned). This guards the filer-standing path without preventing legitimate filers from rebalancing portfolios after filing — the lookback is short and the threshold check is on net balance, not the absence of any outflow. Operator and publisher standing checks are not subject to this clawback (operator stake and namespace ownership are not flash-loanable).

### Filing window

| Parameter | Default | Hard bounds | Rationale |
| --- | ---: | --- | --- |
| `BLACKLIST_APPEAL_FILING_WINDOW` | 14 days | `[3d, 30d]` | Shorter than [ADR 028 §5](028-slashing-appeals.md#5-hard-caps-and-frequency-limits)'s 30-day window because content delisting is reversible (the entry can be re-issued) and stakeholder action should be prompt while the disputed content is still relevant. |

For emergency entries, the filing window is `min(remaining auto-expiry, BLACKLIST_APPEAL_FILING_WINDOW)`. Filings past the auto-expiry are moot — the entry has already lapsed by operation of [§ Compliance Window](#compliance-window) — and `openBlacklistAppeal` reverts.

**Auto-expiry during the appeal lifecycle.** Once an appeal is open, the multisig review and ratification windows (each up to 14 days) can in principle outlive the underlying emergency entry's category-specific auto-expiry (14 days for `GENERAL`, 90 days for `CSAM` / `TERRORIST`). When auto-expiry fires while an appeal is open: any active interim-relief is cleared, the bond is refunded to the filer (the entry has lapsed of its own accord — the filer is no worse off than if the appeal had succeeded), and the concurrent-appeal slot is released. Subsequent calls to `ratifyAppealRemoval` or `reverseAppeal` against the terminated appeal revert with `BlacklistAppealAlreadyClosed` (same error as the global-override case in [§ Global Override](#global-override)). The contract emits `BlacklistAppealLapsed`. Implementations check entry liveness inside the ratify / reverse paths rather than scheduling a separate auto-cancel transaction; the lapse is observed lazily on the next governance action against the appeal.

### Authority and flow

Appeals are heard by the existing **emergency multisig** under the same fast-track authority pattern that [§ Regional Governance Bodies](#regional-governance-bodies) uses for body suspension and that [ADR 028 §6](028-slashing-appeals.md#6-contract-surface) uses for `fastTrackAppeal` / `rejectAppeal`. This is **not a new multisig power** — it is a sub-mode of [ADR 009 § Emergency Multisig](009-governance.md#emergency-multisig)'s existing authority, reusing the 3-of-5 threshold, signing semantics, and post-incident reporting obligations.

```mermaid
sequenceDiagram
    participant F as Filer
    participant CB as ContentBlacklist
    participant EM as Emergency Multisig
    participant Gov as ve-Governor
    Note over F,Gov: T+0 — disputed entry added by regional body
    F->>CB: TOKEN.approve(CB, BLACKLIST_APPEAL_BOND)
    F->>CB: openBlacklistAppeal(hash, region, evidenceBundleHash)
    CB-->>F: appealId; bond escrowed; entry.suspended = false (still enforced)
    Note over CB: BLACKLIST_MULTISIG_REVIEW_WINDOW = 14d
    alt multisig acts within window
        EM->>CB: fastTrackAppeal(appealId) or rejectAppeal(appealId)
        alt fast-tracked
            CB->>CB: entry.suspended = true (isBlacklisted views return false)
            Note over CB: BLACKLIST_RATIFICATION_WINDOW = 14d
            alt ve-Governor ratifies
                Gov->>CB: ratifyAppealRemoval(appealId)
                CB->>CB: _removeHashRegional(hash, region); bond refunded
            else ve-Governor reverses
                Gov->>CB: reverseAppeal(appealId)
                CB->>CB: entry.suspended = false; original effectiveAt preserved
                CB->>CB: 100% of bond burned
            else governance silent past BLACKLIST_RATIFICATION_WINDOW
                CB->>CB: entry.suspended = false; original effectiveAt preserved; bond refunded
            end
        else rejected at intake
            CB->>CB: 100% of bond burned
        end
    else multisig silent past BLACKLIST_MULTISIG_REVIEW_WINDOW
        CB->>CB: appeal lapses; bond refunded
    end
```

The original `effectiveAt` is preserved across suspension. Resetting `effectiveAt` to `block.timestamp + complianceWindow` on resumption was rejected because it would shield operators who never evicted in the first place — a dishonest operator already past the original compliance window when suspension began would receive a fresh window on resumption, retroactively immunizing pre-suspension delivery. Instead, evidence-age semantics handle the honest-operator case: the `MAX_EVIDENCE_AGE_US` clock from [ADR 014 §2](014-on-chain-verification.md#2-blake3-content-corruption--optimistic-challenge-response) is computed relative to `entry.suspendedAt` rather than the current block while a challenge replays post-resumption, so suspension does not retroactively immunize pre-suspension evidence and does not require challengers to re-witness. Honest operators who continued to serve during suspension (relying on `isBlacklisted == false`) are protected directly: deliveries with timestamps inside the suspension window are not admissible as slash evidence, because the views correctly returned `false` at delivery time. Operators detect resumption via the standard `getBlacklistVersion()` polling cycle (default 10 minutes — see [§ Polling](#polling)).

### Bond and frequency caps

| Parameter | Default | Hard bounds | Rationale |
| --- | ---: | --- | --- |
| `BLACKLIST_APPEAL_BOND` | 1,000 TOKEN | `[100, 10,000]` | Matches [ADR 028 §4](028-slashing-appeals.md#4-appeal-bond) for operational parity. The two ADRs use distinct contract storage; defaults align but the parameters may diverge under governance. |
| `BLACKLIST_MULTISIG_REVIEW_WINDOW` | 14 days | `[3d, 30d]` | Same shape and bounds as [ADR 028 §5](028-slashing-appeals.md#5-hard-caps-and-frequency-limits)'s `MULTISIG_REVIEW_WINDOW`. |
| `BLACKLIST_RATIFICATION_WINDOW` | 14 days | (fixed) | Mirrors the 14-day ratification window used elsewhere in this ADR for body suspension. |
| `APPEAL_FILER_FREQUENCY` | 1 successful appeal per 90 days | (fixed) | Per filing address, regardless of whether the filer is a publisher, operator, or TOKEN holder. Resets on the date of the most recent ratified-success. Failed appeals consume bond but do not count against the cap. |
| `BODY_CONCURRENT_APPEAL_CAP` | 3 | (fixed) | Caps **active interim-relief states** (`entry.suspended == true`) per regional body, **not** filed-but-unresolved appeals. Filings against a body remain unbounded — the cap only constrains how many of the body's entries can simultaneously be in the suspension state. The multisig is the gate: once three of a body's entries are in interim-relief, `fastTrackAppeal` reverts on a fourth until one resolves (ratify / reverse / lapse), so the multisig must triage. Filed-but-not-yet-fast-tracked appeals are unaffected by the cap. This avoids a denial-of-service vector where an adversary files three frivolous appeals to lock out legitimate filings — frivolous filings can be rejected at intake (`rejectAppeal`, 100% bond burn) without ever entering interim-relief. |

Bond outcomes:

- **Ratified-success:** bond refunded in full to the filer.
- **Reversal:** 100% of bond burned. Routing reversal proceeds to a regional-body operating-budget pool would create a perverse incentive — bodies would have direct economic motive to issue marginal-but-defensible entries that attract reversible appeals. Burn-only mirrors the [ADR 014 §3 Bond Handling](014-on-chain-verification.md#bond-handling) "no prevailing-party payout when there is no neutral counter-party" model.
- **Lapse / governance silence:** bond refunded — the filer is not at fault for governance inaction.
- **Rejection at intake:** 100% of bond burned; no counter-bundle filer to credit (mirrors [ADR 028 §4](028-slashing-appeals.md#4-appeal-bond)).

The bond is the primary economic deterrent against pro-forma filings; the per-filer 90-day cap and the perjury denylist below are the secondary deterrents.

### Evidence

The appeal bundle (referenced on-chain by `evidenceBundleHash`) must include a sworn EIP-712 declaration plus **at least one** of the following corroborating evidence types:

| Evidence type | Examples |
| --- | --- |
| (a) Counter-notice or legal opinion | Signed DMCA counter-notice; signed jurisdictional opinion; EIP-712-signed statement from the namespace publisher attesting non-infringement and identifying the original notice |
| (b) Jurisdictional documentation | The regional body's own constituting document or registration declaring its jurisdictional scope, paired with a documented nexus mismatch for ground (a) — out-of-scope |
| (c) Regional body process record | The body's published transparency log entry (or its absence) demonstrating procedural defect for ground (b) — wrong threshold met, off-quorum action, missing required publication |

Operational-failure evidence types from [ADR 028 §3](028-slashing-appeals.md#3-eligibility-and-evidence-standard) (gossip telemetry, peer-attested downtime) are **not** admissible here. The two domains use disjoint evidence sets; an operator whose grievance is "I was offline during the compliance window" must use the ADR 028 path. Filing such evidence on this path is grounds for rejection at intake, not perjury.

The sworn declaration is an EIP-712-signed statement (secp256k1, signed by the filer's registered Ethereum address) attesting that the evidence is genuine and that the filer has standing under [§ Standing](#standing). **Perjury — proven by post-hoc evidence — triggers full bond forfeit and adds the filer's address to a 365-day per-address filing denylist** maintained by `ContentBlacklist`. The denylist applies only to this appeal path; it does not affect the filer's TOKEN holdings, staking position, or any other protocol role.

### Interaction with active slashes

While `entry.suspended == true`:

- `isBlacklisted(hash)` and `isBlacklistedInRegion(hash, region)` return `false`. This short-circuits `SlashJudge` per [ADR 014](014-on-chain-verification.md): a blacklist-offense challenge submitted during interim-relief reverts with `BlacklistEntrySuspended`, distinct from `HashNotBlacklisted`, so challengers can distinguish a never-blacklisted hash from a temporarily suspended one.
- New slash challenges for the disputed hash cannot be opened.
- Pre-suspension evidence is preserved across resumption. `BlacklistEntry.suspendedAt` records the block timestamp at which fast-track flipped `suspended = true`. On reversal or lapse, `SlashJudge` admits challenges whose `evidence.timestamp_us` falls in the half-open window `[entry.addedAt + complianceWindow, entry.suspendedAt)` for `MAX_EVIDENCE_AGE_US` after resumption — the evidence-age clock is computed relative to `entry.suspendedAt`, not the current block, so a multi-week appeal lifecycle does not retroactively immunize pre-suspension delivery whose evidence would otherwise age past [ADR 014 §2](014-on-chain-verification.md#2-blake3-content-corruption--optimistic-challenge-response)'s 5-day default. Evidence with `timestamp_us ≥ entry.suspendedAt` and `< resumption block` is **not** admissible — the views returned `false` at that delivery time, and operators relying on the suspended view must be protected.
- Already-resolved slashes against operators for the disputed hash are **not** auto-reversed. Operators in that position seek individual restitution via [ADR 028 § Slashing appeals](028-slashing-appeals.md) using the `slashId` of their original slash. ADR 028 already lists blacklist offenses as appealable; the heightened-scrutiny guidance from [ADR 028 §1](028-slashing-appeals.md#1-scope) for blacklist appeals is somewhat relaxed when the underlying entry has been removed via the path here, since the operational-failure rationale is no longer the only viable defense.

This decoupling is the explicit boundary between the two ADRs. Removing a wrongful entry going forward (this ADR) does not mechanically refund slashes already taken (ADR 028); the two grievances are filed and bonded separately.

### Contract surface

The new entry points are listed in [§ Contract: ContentBlacklist](#contract-contentblacklist) above. Implementation notes:

- `openBlacklistAppeal` reverts if `region == ""` (global entries are out of scope), if the entry is past its `BLACKLIST_APPEAL_FILING_WINDOW`, if the body's concurrent-appeal cap is full, if the filer fails standing checks, or if the filer is on the perjury denylist. Bond is pulled via `TOKEN.transferFrom`; the appeal record is stored and `BlacklistAppealOpened` is emitted.
- `fastTrackAppeal` / `rejectAppeal` are restricted to the emergency multisig (the same address with the same threshold as the existing `suspendRegionalBody` flow). Sub-mode of [ADR 009 § Emergency Multisig](009-governance.md#emergency-multisig) capability — does not require an additional multisig power.
- `ratifyAppealRemoval` / `reverseAppeal` are restricted to GOVERNANCE_ROLE (ve-Governor). Ratification calls the contract's internal `_removeHashRegional` and emits both `BlacklistAppealRatified` and the standard `HashRemoved` event. Reversal clears `suspended`, resets `effectiveAt`, and applies the bond split.
- The full ABI (per-appeal storage layout, exact event topics, gas-optimized struct packing) is deferred to a future contract-implementation ADR — same approach as [ADR 028 §6](028-slashing-appeals.md#6-contract-surface).

### Global Override

The slow-path global override is independent of the appeal flow above. ve-Governor proposals may call `removeHash` (global entries) and `removeHashRegional` (regional entries) directly via the standard timelock, regardless of any open appeal. Both functions are restricted to `GOVERNANCE_ROLE`; this is now explicitly documented as their access control. The slow path is always available for cases that do not fit the fast-track — global standard-vote entries, frequency-capped filers, expired filing windows, or coordinated multi-region disputes that warrant a single ve-Governor decision rather than per-region multisig action.

If `removeHash` or `removeHashRegional` fires while an appeal is open against the same `(blake3Hash, region)` pair, that appeal terminates immediately: any active interim-relief is cleared (`entry.suspended` is moot once the entry is gone), the bond is refunded to the filer (the override achieved the filer's intended outcome), and the body's concurrent-appeal slot under [§ Bond and frequency caps](#bond-and-frequency-caps) is released. Subsequent calls to `ratifyAppealRemoval` or `reverseAppeal` against the terminated appeal revert with `BlacklistAppealAlreadyClosed`. The contract emits `BlacklistAppealLapsed` so off-chain consumers can reconcile.

## Compliance Window

| Path | Compliance window |
|------|-------------------|
| Standard governance vote (global) | 24 hours after `effectiveAt` |
| Regional governance body | 24 hours after `effectiveAt` |
| Emergency multisig add | `effectiveAt = addedAt` — slash applies after 2 hours |

The 24-hour window accounts for nodes that are offline or have a long poll interval. The 2-hour emergency window is tight enough to matter for active illegal content while giving online nodes time to act. The emergency multisig path is subject to a 12-month sunset (`blacklistDeadline = deployTimestamp + 365 days`) — see [ADR 009](009-governance.md#emergency-multisig).

The compliance window is a governable parameter (hardcoded bounds: minimum 1 hour, maximum 7 days).

Entries with `suspended == true` (see [§ Blacklist Entry Appeals](#blacklist-entry-appeals)) do not accrue compliance obligation while suspended: `isBlacklisted` and `isBlacklistedInRegion` return `false`, and slashes for the suspended hash cannot be opened. On reversal or lapse, the original `effectiveAt` is preserved — operators detect resumption via the next `getBlacklistVersion()` poll cycle (default 10 minutes; see [§ Polling](#polling)) and must evict before serving any new request. Honest operators who served during the suspension window are protected at the evidence layer rather than via a compliance-window extension — see [§ Interaction with active slashes](#interaction-with-active-slashes).

## Hash Evasion and Origin Blacklisting

Hash-based blacklisting covers only exact copies of a blob. A one-byte change produces a completely different BLAKE3 hash and evades the blacklist. This is a known limitation shared by every hash-based content moderation system.

**The protocol's primary response is origin blacklisting.** If an origin-backed node repeatedly sources blacklisted content — whether the same blob or trivially re-encoded variants — governance can blacklist the operator's Ethereum address. `ContentBlacklist.addOrigin()` calls `StakingRegistry.ejectNode(operatorAddress)` via a cross-contract call; the `StakingRegistry` grants the `ContentBlacklist` contract address the `BLACKLIST_ROLE`, permitting this call. A blacklisted origin:

- **Ejected from `StakingRegistry`** — sets `active = false`, emits `NodeAutoEjected` ([ADR 001](001-network.md)). This follows the same code path as stake-based auto-ejection ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn))
- **Effectively removed from every namespace's authorized origin set** at runtime; storage cleanup is lazy and permissionless via `pruneBlacklistedAssignment` — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist)
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
- The **default-open namespace** (`namespaceId == 0`) governs all unclaimed content. Only operators in the DAO-maintained default-open allow-list may serve as origin for default-open content (see [§ Default-open allow-list](#default-open-allow-list)). Origin assignment authority applies to all namespaces; registered namespaces follow the publisher-propose / DAO-ratify flow, while the default-open namespace is governed by a single global allow-list set by the DAO directly.

### Contract: OriginAssignment

```solidity
interface IOriginAssignment {
    // Publisher proposes a candidate origin set for one of their namespaces.
    // Reverts if msg.sender is not the namespace owner, if any operator is not
    // active in StakingRegistry at proposal time, or if the operator count is
    // outside [minRedundancy, maxOriginsPerNamespace].
    function proposeAssignment(uint256 namespaceId, address[] calldata operators) external;

    // Governance ratifies a pending proposal after the assignment timelock.
    // Reverts if no pending proposal exists, if the timelock has not elapsed,
    // or if any pending operator is no longer active in StakingRegistry or is
    // currently blacklisted in ContentBlacklist.
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

    // Default-open allow-list (namespaceId == 0). GOVERNANCE_ROLE only; see
    // "Default-open allow-list" below for the lifecycle, bootstrap rule, and
    // validation invariants.
    function setDefaultOpenAllowlist(address[] calldata operators) external;
    function addDefaultOpenOperator(address operator) external;
    function removeDefaultOpenOperator(address operator) external;

    // Wires the read-direction integration with ContentBlacklist for
    // pruneBlacklistedAssignment. Called once during post-deploy initialization
    // (see ADR 016) and not expected to change thereafter; GOVERNANCE_ROLE only.
    function setContentBlacklist(address contentBlacklist) external;

    // Governable parameters with safety bounds (see ADR 009)
    function setMinRedundancy(uint256 floor) external;             // non-zero namespaces
    function setMaxOriginsPerNamespace(uint256 cap) external;      // non-zero namespaces
    function setAssignmentTimelock(uint256 secondsDelay) external; // non-zero namespaces
    function setDefaultOpenMinRedundancy(uint256 floor) external;
    function setDefaultOpenMaxOrigins(uint256 cap) external;

    // Views. For namespaceId == 0 these read the default-open allow-list.
    function isAuthorizedOrigin(uint256 namespaceId, address operator) external view returns (bool);
    function getOrigins(uint256 namespaceId) external view returns (address[] memory);
    function getPendingAssignment(uint256 namespaceId)
        external view returns (address[] memory operators, uint256 readyAt);

    // Bootstrap state for the default-open allow-list.
    function defaultOpenAllowlistActive() external view returns (bool);
    function defaultOpenActivatedAt() external view returns (uint64);

    // Events
    event AssignmentProposed(uint256 indexed namespaceId, address indexed proposer, address[] operators, uint256 readyAt);
    event AssignmentActivated(uint256 indexed namespaceId, address[] operators);
    event AssignmentRevoked(uint256 indexed namespaceId, address indexed operator, address indexed by);
    event BlacklistedAssignmentPruned(uint256 indexed namespaceId, address indexed operator, address indexed pruner);
    event DefaultOpenAllowlistUpdated(address[] operators, uint256 indexed updateIndex);
    event DefaultOpenOperatorAdded(address indexed operator);
    event DefaultOpenOperatorRemoved(address indexed operator);
    event DefaultOpenAllowlistActivated();
}
```

### Lifecycle

1. **Publisher proposal.** The publisher calls `proposeAssignment(namespaceId, operators)`. The contract validates that the proposer owns the namespace, that every candidate is currently active in `StakingRegistry`, and that the operator count satisfies the `minRedundancy` and `maxOriginsPerNamespace` bounds. The proposal enters a pending state with a `readyAt` timestamp computed as `block.timestamp + assignmentTimelock` (governance-bounded between 24 hours and 14 days; see [ADR 009](009-governance.md)).
2. **Governance ratification.** During the timelock window, governance reviews the proposal off-chain. After the timelock elapses, a governance proposal calls `activateAssignment(namespaceId)`. Before replacing the active set, activation re-checks every pending operator against `StakingRegistry.isActive` and `ContentBlacklist.isOriginBlacklisted` so a proposal cannot go live with operators that became inactive or were blacklisted during the delay window. If any operator now fails validation, activation reverts and the publisher must submit a fresh proposal. Successful activation replaces the namespace's authorized operator set atomically.
3. **Operator notification.** Operators in the activated set are now authorized to act as origins for the namespace. They configure their origin store locally and begin serving the namespace's content. The wire protocol does not distinguish origins from cache nodes at probe time — origin status is a publisher-level commitment surfaced via `getOrigins(namespaceId)` for off-chain consumers.
4. **Revocation.** A publisher may unilaterally remove an operator from their own namespace's set (e.g., the operator is performing poorly). Governance may revoke any operator from any namespace via the standard proposal path (e.g., the operator is misbehaving but has not yet crossed the blacklist threshold). Blacklisting (`ContentBlacklist.addOrigin`) takes effect via runtime checks rather than a cross-call — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist).

The two-step propose-then-ratify flow is deliberate: it gives publishers agency over which operators they trust (publishers know their content best) while keeping the DAO as the authority that confirms the assignment is consistent with protocol-wide policy (e.g., not concentrating too many namespaces on a small operator set, not assigning to operators with poor reputation). Either party can refuse to advance the flow — publishers by not proposing, governance by not ratifying — and the namespace simply continues with its existing assignment (or remains unassigned).

### Default-open allow-list

The default-open namespace has no publisher, so the per-namespace propose / ratify flow does not apply. Instead, the DAO directly maintains a single global allow-list of operators authorized to serve as origin for *any* default-open hash. The allow-list is held in `OriginAssignment` under the same per-namespace `EnumerableSet` storage used for registered namespaces, keyed by `namespaceId == 0` — `isAuthorizedOrigin(0, op)` is the same view used everywhere else, with no special case downstream.

**Lifecycle.** Allow-list updates are GOVERNANCE_ROLE-only single-step proposals under the Governor's standard timelock — no separate `defaultOpenAssignmentTimelock` parameter. `setDefaultOpenAllowlist(operators)` replaces the active set atomically; `addDefaultOpenOperator` / `removeDefaultOpenOperator` are convenience deltas with the same authority and delay. Each transition appends a checkpoint per affected operator. The contract enforces `operators.length ∈ [defaultOpenMinRedundancy, defaultOpenMaxOrigins]`, that every operator is `StakingRegistry.isActive` at activation time, and rejects duplicate addresses.

**Bootstrap.** `isAuthorizedOrigin(0, op)` is permissive (returns `true` for any active staker) until the first non-empty activation, then strict (returns set membership). The activation atomically flips `defaultOpenAllowlistActive` to `true`, sets `defaultOpenActivatedAt`, and emits `DefaultOpenAllowlistActivated` — a single observable transition so reputation and node tooling can pivot cleanly. Rationale: PoC must be able to serve content on day one without governance, so a hard cutover is unworkable; an implicit genesis-seeded set would be opaque and hard to reason about post-hoc.

**Parameters and bounds.** `defaultOpenMinRedundancy` (default 10) and `defaultOpenMaxOrigins` (default 100) are bounded by [ADR 009](009-governance.md), with the cross-parameter invariants `5 ≤ defaultOpenMinRedundancy ≤ defaultOpenMaxOrigins ≤ 500` and `defaultOpenMinRedundancy ≥ minRedundancy` enforced at the contract layer. Both are higher than the per-registered-namespace bounds because one approved operator may serve any default-open hash — the surface area is the entire long tail.

### Unassigned namespaces

A registered namespace with no activated assignment is **unassigned**. No operator is authorized as origin for unassigned content, but the protocol still permits cache-only serving from any staked operator that happens to hold the blob — see [ADR 005 § cdn/probe/v1](005-protocol.md#cdnprobev1--latency-probe). Publishers who claim content but never propose an assignment effectively prevent any new origin from picking up the content from canonical storage; cached copies eventually expire. This is by design — it lets a publisher delete their content set from the network by claiming the hashes and refusing to assign origins.

### Minimum-redundancy invariant

The contract enforces `operators.length >= minRedundancy` at proposal time and at activation time, and additionally rejects proposals whose `operators` array contains duplicate addresses (without this, a publisher could submit `[A, A, A]` to satisfy `minRedundancy = 3` while still concentrating origin responsibility on a single operator). Activation also re-validates that every pending operator is still active and not blacklisted before the set can go live. `minRedundancy` is a governance-bounded parameter (see [ADR 009](009-governance.md); range `[1, 10]`, default `3`) and is constrained by the cross-parameter invariant `1 ≤ minRedundancy ≤ maxOriginsPerNamespace`. The invariant ensures that no registered namespace can be activated with a single point of failure. The invariant is *not* enforced on revocation — a publisher or governance may revoke operators down to zero, but new activations must satisfy the floor. Under-redundant namespaces are observable via the `getOrigins` view; clients and off-chain monitors may surface this as a health indicator for the namespace's owner.

### Cross-contract integration

- `OriginAssignment` reads `PublisherRegistry.ownerOf(namespaceId)` to validate proposer ownership.
- `OriginAssignment` reads `StakingRegistry.isActive(operator)` to validate origin candidates at proposal time. The check is opportunistic, not enforced at probe time — an operator who unbonds mid-assignment is filtered by clients via the standard staking check, not by `OriginAssignment` (avoiding expensive cross-contract checks on every assignment lookup).
- `OriginAssignment.pruneBlacklistedAssignment` reads `ContentBlacklist.isOriginBlacklisted(operator)` to decide whether to remove an entry. Permissionless callers can clean up storage one (`namespaceId`, operator) pair at a time.
- `ContentBlacklist.addOrigin(operator)` does not call into `OriginAssignment` — see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist) below for the rationale and the runtime-check pattern.

### Interaction with ContentBlacklist

`ContentBlacklist.addOrigin(operator)` does **not** call `OriginAssignment` to evict the operator from every namespace. The naïve approach — iterate over every namespace the operator is assigned to and remove them in one transaction — is unbounded: an operator in N namespaces costs O(N) storage writes, and a prolific operator could exceed the block gas limit, blocking the blacklist transaction entirely.

Off-chain consumers of `OriginAssignment.getOrigins(namespaceId)` (clients selecting peers for first-fetch, off-chain monitors checking publisher availability commitments) cross-reference each returned operator against `ContentBlacklist.isOriginBlacklisted` and treat blacklisted entries as unauthorized regardless of stale `OriginAssignment` state. Storage cleanup happens lazily and permissionlessly via `OriginAssignment.pruneBlacklistedAssignment(namespaceId, operator)`: each call removes one entry; anyone may call it (the contract checks `ContentBlacklist.isOriginBlacklisted` itself, so the caller cannot grief by claiming a non-blacklisted operator is blacklisted). Reputation services and other public-good infrastructure will likely run pruning jobs.

This pattern means: blacklisting an operator is an O(1) on-chain action (one ejection call), and storage cleanup is O(1) per call with no transaction-size limit. No design path requires iterating over an operator's full namespace set.

### Permissionless property

This authority extends the DAO's role from negative-only (blacklisting) to positive-and-negative (assignment + blacklisting) for all namespaces, including default-open. Cache-only serving remains permissionless — any staked operator may fetch cached blobs from authorized origins and re-serve them regardless of `OriginAssignment` membership. Only the *origin* role becomes DAO-gated, with publisher-proposed sets for registered namespaces and the global allow-list for default-open content. See [ADR 001](001-network.md) for the updated permissionless-role model.

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

1. **Stop publishing** — withdraw any DHT STORE records for the hash and stop re-publishing immediately ([ADR 022](022-content-discovery.md))
2. **Stop serving** — reject any new `StreamRequest` for the hash immediately, returning `HashBlacklisted`
3. **Evict from cache** — delete the blob from local storage within the compliance window

The publish-first ordering is critical: continuing to publish DHT records and probe-respond `has_blob: true` after the compliance window triggers phantom-blob slashing ([ADR 005 — `cdn/probe/v1`](005-protocol.md#cdnprobev1--latency-probe)). Eviction from disk can be async; DHT-record suppression and probe-response suppression must be synchronous.

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
- A `ProbeResponse` with `has_blob: true` for the blacklisted hash, timestamped after the compliance window. On-chain verification uses the `slash_sig` scheme from [ADR 014 §1](014-on-chain-verification.md#1-slash-signatures--secp256k1-eip-712): the EIP-712 secp256k1 `slash_sig` is verified via `ecrecover`, and the recovered address is mapped to the node's identity via `StakingRegistry.nodeIdOf`. This is the primary evidence path. Alternatively, a `StreamResponse` with `ok: true` for the blacklisted hash (binding `hash` and `channel_id` in the signed data) is also sufficient. Client-signed vouchers alone are NOT sufficient evidence — vouchers do not contain the hash and the `channel_id → hash` binding is not on-chain verifiable.
- The `BlacklistEntry.effectiveAt` timestamp showing the compliance window had passed

The `ContentBlacklist` contract verifies that `effectiveAt` is in the past relative to the delivery timestamp and that the hash is still on the blacklist. If the hash was subsequently removed, the slash is invalid.

Regional slash eligibility: a node is only slashable for serving a hash it was in-scope to remove. A node that declared region `US` is not slashable for serving a hash that was only blacklisted by the EU regional body.

### Grace period for offline nodes

A slash requires an active challenger submitting evidence of a post-window delivery. Nodes that reconnect, sync the blacklist, and evict before serving any content are safe.

### Interaction with appeals

Slash challenges cannot be opened against operators while the disputed entry is in interim-relief (see [§ Blacklist Entry Appeals](#blacklist-entry-appeals)) — `SlashJudge` reads `isBlacklisted == false` from `ContentBlacklist` during the suspension window and rejects the challenge on that basis. Operators slashed under an entry that is *later* removed via the appeals path are not auto-restituted; they may file individually through [ADR 028 § Slashing appeals](028-slashing-appeals.md), which already lists blacklist offenses as appealable. The two paths are filed and bonded separately by design — see [§ Blacklist Entry Appeals — Interaction with active slashes](#interaction-with-active-slashes).

## Consequences

**Positive:**

- Global and regional blacklisting coexist in one contract — no separate deployment for jurisdictions
- Regional bodies can act without a global governance vote, matching the speed of real-world legal processes (DSA requires expeditious removal)
- Origin blacklisting makes hash evasion progressively expensive — each re-upload requires fresh stake and a new identity
- Emergency path addresses CSAM and actively-exploited material without a 5-day vote cycle
- Reason field and on-chain audit trail support legal defensibility for operators
- Local denylist preserves operator autonomy for direct legal notices
- Origin assignment authority gives publishers a protocol-level way to commit specific operators to serving their content with an enforced minimum-redundancy invariant — no withholding-by-single-origin failure mode for registered namespaces
- Symmetric blacklist/assignment infrastructure: a blacklisted operator is treated as unauthorized at every runtime check across every namespace they were authorized to serve, with lazy storage cleanup (see [§ Interaction with ContentBlacklist](#interaction-with-contentblacklist))
- Default-open content is governed by a single DAO-maintained allow-list with its own redundancy floor, closing the prior loophole where any staked operator could claim origin for unregistered content while keeping a uniform `OriginAssignment` storage and view model across all namespaces
- Per-entry appeals (see [§ Blacklist Entry Appeals](#blacklist-entry-appeals)) close the regional-blacklist due-process gap with a bounded fast-track. Body-level `suspendRegionalBody` no longer has to serve as a sledgehammer for single-entry disputes — a wrongly served takedown can be challenged without freezing every other entry the body issued
- Appeal standing extends to publishers, affected operators, and TOKEN holders above a threshold — content advocates and end-user proxies can file without on-chain content ownership, while the bond and frequency caps deter pro-forma filings
- The two appeal paths (this ADR's content-policy appeal and [ADR 028](028-slashing-appeals.md)'s operational-failure appeal) are deliberately decoupled with disjoint evidence sets, so a single grievance maps cleanly to a single path

**Negative:**

- Hash-based blacklisting covers exact copies only; trivial re-encoding evades it. This is a fundamental limitation with no protocol-level solution for content-agnostic blobs
- Node region is self-reported and unverified; regional compliance relies on operator legal incentive, not cryptographic enforcement
- Governance becomes a content moderation body, requiring off-chain processes (abuse intake, legal review) the protocol does not define
- Multiple regional bodies add governance coordination overhead; regional bodies can disagree on scope
- Blacklisted content remains content-addressable and verifiable off-network; eviction stops CDN serving but does not prevent redistribution by other means
- Origin assignment authority extends governance into a new category — positive node-role authorization — that did not previously exist. Capture risk and operator-concentration risk are now governance concerns, not just off-protocol coordination concerns
- The strict-gating model amends the unconditional permissionless-origin claim from [ADR 001](001-network.md). Cache-only role is preserved as permissionless, but the origin role is governance-gated for all content — per-namespace `OriginAssignment` for registered namespaces and the default-open allow-list for unregistered content. PoC ships in the default-open bootstrap window (see [§ Default-open allow-list](#default-open-allow-list)); production governance closes it
- The appeal flow adds five entry points (`openBlacklistAppeal` / `fastTrackAppeal` / `rejectAppeal` / `ratifyAppealRemoval` / `reverseAppeal`), per-appeal escrow accounting, and a per-address perjury denylist to `ContentBlacklist`, increasing the contract's surface area and audit cost — same trade-off acknowledged in [ADR 028 §6](028-slashing-appeals.md#6-contract-surface)
- Filers must front `BLACKLIST_APPEAL_BOND` (1,000 TOKEN default) at filing time. For cold-start participants and small-balance TOKEN holders this is a real frictional cost. The bond is governance-bounded `[100, 10,000]` so it can be reduced during the PoC window if observed filing volumes warrant
- The per-body concurrent-appeal cap and per-filer 90-day frequency cap trade off coverage for griefing resistance: a coordinated good-faith dispute against many entries issued by a single body can be queued behind the cap. Mitigations are observable on-chain (cap-reached events should be surfaced in operator tooling) and the slow-path ve-Governor override remains available for cases that overflow the fast-track

## Governance Process (Off-Chain)

The minimum viable process for PoC:

1. A `takedown@` contact address is published alongside the node registry
2. Notices are triaged by the admin key holder (PoC) or global governance multisig (production)
3. Clearly illegal content (CSAM, actively-exploited material) → emergency multisig path
4. DMCA / DSA notices → appropriate governance body (global or regional) with the notice ID in the `reason` field
5. Repeat-offender origin nodes → global governance vote for origin blacklisting
6. Disputed regional entries → see [§ Blacklist Entry Appeals](#blacklist-entry-appeals) for the per-entry path. The slow-path global override (ve-Governor `removeHash` / `removeHashRegional` proposal) remains available for entries that fall outside the fast-track scope

## ADRs Affected

- **[ADR 001](001-network.md)** (Network Topology) — blacklisted origin NodeIds are excluded from peer tables; `StreamError::HashBlacklisted`, `StreamError::OriginBlacklisted`, and `StreamError::UnauthorizedOrigin` are new error variants; the unconditional permissionless-origin claim in the consequences section is amended to reflect DAO-gated origin role
- **[ADR 002](002-content-addressing.md)** (Content Addressing) — content-addressed blobs can be removed from the network layer even though the hash remains valid; this is explicitly accepted. The [Publisher Identity and Namespaces](002-content-addressing.md#publisher-identity-and-namespaces) section in ADR 002 defines the primitives this ADR's `OriginAssignment` mechanism builds on
- **[ADR 003](003-payments.md)** (Payments) — multi-origin redundancy is now DAO-supervised via the `OriginAssignment` minimum-redundancy invariant rather than off-protocol content-owner coordination
- **[ADR 005](005-protocol.md#cdnprobev1--latency-probe)** (Protocol) — origin status is *not* signaled at probe time; the wire response describes only "I have the bytes." Origin authorization is queried off-chain via `OriginAssignment.getOrigins(namespaceId)` for routing/discovery; the protocol does not slash for impersonating origin status because no claim is made on the wire
- **[ADR 026](026-gauge-boost-tokenomics.md)** (Tokenomics) — serving blacklisted content added to the slashable offense list; origin blacklisting triggers same stake ejection path as repeated slashing
- **[ADR 009](009-governance.md)** (Governance) — `ContentBlacklist` and `OriginAssignment` contracts added to governance-controlled contracts; emergency multisig scope documented in ADR 009 as the single source of truth, covering both contract pausing and content/origin blacklisting; regional body registry introduced as a new governance primitive; new governable parameters with safety bounds for assignment timelock, minimum redundancy, per-publisher namespace caps, and default-open allow-list redundancy floor / cap
- **[ADR 016](016-contract-interactions.md)** (Contract Interactions) — `PublisherRegistry` and `OriginAssignment` added to the contract inventory, deployment order, call graph, role matrix, and reentrancy analysis
- **[ADR 022](022-content-discovery.md)** (Content Discovery) — DHT STORE remains permissionless; the `OriginAssignment` view layer is consulted at probe / request time rather than at DHT publish time, and the existing "publisher" terminology in ADR 022 is qualified to distinguish DHT STORE publishers from on-chain content publishers
- **[ADR 028](028-slashing-appeals.md)** (Slashing Appeals) — paired appeal mechanism for the operator side. ADR 028 covers operator-outage restitution for slashes already taken (USDC payout from `SafetyReserve`); the [§ Blacklist Entry Appeals](#blacklist-entry-appeals) path defined here covers content-policy challenges to the underlying blacklist entry (entry removal via `_removeHashRegional`). The two paths use disjoint evidence sets and are filed and bonded separately. ADR 028's existing footer reference to issue #131 is anchored by the new appeals section
