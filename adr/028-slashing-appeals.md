# ADR 028: Slashing Appeals and Dispute Escalation

**Date:** 2026-05-27
**Status:** Draft

## Context

The protocol slashes operator bonds at 5% / 15% / 50% escalation tiers ([ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)). Under **escrow-on-slash** the slashed TOKEN is held in `CapacityBond` escrow rather than distributed immediately. This ADR specifies the operator-facing appeal that decides whether that escrow is refunded to the operator or distributed (50% challenger / 50% burn) at finality. The two `SlashJudge` offenses — rate manipulation, blacklist violation — still execute the bond reduction immediately on successful on-chain verification ([ADR 014 § Bond Handling](014-on-chain-verification.md#bond-handling)), but the funds are now recoverable until the appeal window resolves. Operators hit while offline have a bounded recourse path.

Without a documented escalation path, every legitimate-outage slash (network outage, NTP drift, regional ISP failure, hosting-provider incident) becomes either a permanent operator loss (damages onboarding/trust) or an ad-hoc emergency-multisig discretion event (unbounded multisig precedent). A bounded mechanism is needed before mainnet. This ADR adds a governance-level appeal layered on the escrow-on-slash machinery — no new governance bodies, no changes to the slash *bond-reduction* primitive. Disputing the *underlying blacklist entry* is a different matter, handled by the ordinary removal path in [ADR 011 § Removing a Wrongful Entry](011-content-takedown.md#removing-a-wrongful-entry).

## Decision

The slashed **operator** (and only the operator — `msg.sender` must equal the operator recorded in the slash) may file a **slashing appeal** within 30 days of a `SlashJudge` resolution. The appeal lives in the standalone **`SlashAppeal`** contract; opening one posts a TOKEN appeal bond and locks the slash's escrow on `CapacityBond` (`markAppealOpen`). The appeal slot is one-shot per `slashId`, so filing is operator-restricted: a permissionless filer would let anyone — notably the recorded challenger, who earns 50% of an upheld slash — burn the operator's only chance at recourse with a junk appeal (the bond, perjury re-slash, and frequency cap do not deter a throwaway/Sybil griefer). See [§ Eligibility and evidence standard](#eligibility-and-evidence-standard). Appeals are heard by the existing emergency multisig under a fast-track authority mirroring [ADR 011 § Regional Governance Bodies](011-content-takedown.md#regional-governance-bodies)' suspension pattern: interim relief granted by the multisig (3-of-5), then granted or upheld by the operator-weighted Governor within 14 days (post-transition; bootstrap-multisig phase rules per [ADR 009 § Bootstrap-multisig phase](009-governance.md#bootstrap-multisig-phase)).

A **successful** appeal (`grantAppeal`) calls `CapacityBond.settleAppealGranted`, which **refunds the full escrowed TOKEN to the operator** — their own slashed capital, in TOKEN, with no USDC conversion, no TWAP oracle, and no restitution cap — and releases this slash from the `slashedAtEpoch` zero-out, recomputing the watermark over the operator's remaining still-standing slashes — cleared to zero only when none remain ([ADR 036 § Slashing zero-out](036-served-bytes-voting-weight.md#slashing-zero-out)). A **failed** appeal (`upholdAppeal`, `rejectAppeal`, or review-window lapse) distributes the slash escrow 50% challenger / 50% burn, identical to the no-appeal `finalizeUnappealedSlash` path, and burns the separate appeal bond in full. The operator's lifetime offense counter is **not** modified in either case (see [§ Reputation handling](#reputation-handling)) — an appeal restitutes capital, not standing.

### Scope

Both `SlashJudge` offense types are appealable: rate manipulation, blacklist. Each executes at reveal time (the `submit*Challenge` call, after a prior `commitChallenge`) with no in-protocol counter-evidence opportunity ([ADR 014 § Bond Handling](014-on-chain-verification.md#bond-handling)); the legitimate-outage rationale applies to both.

The [§ Eligibility and evidence standard](#eligibility-and-evidence-standard) bar is the gate against frivolous appeals, not the offense type. **Scope limitation: [ADR 028](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation) covers appeals against the *slash event itself* on operational-failure grounds — the operator could not comply because of an outage, NTP drift, or similar.** Disputing whether the blacklisted hash belongs on the list at all is out of scope; that is the removal path in [ADR 011 § Removing a Wrongful Entry](011-content-takedown.md#removing-a-wrongful-entry), which carries no appeal state machine of its own. The evidence standard admits no content-policy arguments; only the operational-failure evidence types are admissible. The multisig is expected to apply heightened scrutiny to blacklist-offense appeals (deliberate moderation noncompliance, not operational failure); this guidance is not coded into the contract.

### Appeal flow

```mermaid
sequenceDiagram
    participant Op as Operator / Appellant
    participant SA as SlashAppeal
    participant CB as CapacityBond
    participant EM as Emergency Multisig
    participant Gov as DecdnGovernor
    Note over Op,Gov: T+0 — SlashJudge resolution reduces bond; slashed TOKEN held in CapacityBond escrow
    Op->>SA: T+0..30d — TOKEN.approve(SA, APPEAL_BOND)
    Op->>SA: openSlashAppeal(slashId, evidenceBundleHash) — bond transferred
    SA->>CB: markAppealOpen(slashId) — escrow locked (Escrowed→AppealOpen)
    Note over SA: APPEAL_REVIEW_WINDOW = 14d
    alt multisig acts within window
        alt fast-track approved
            EM->>SA: fastTrackAppeal(slashId)
            Note over SA: APPEAL_RATIFICATION_WINDOW = 14d
            alt DecdnGovernor grants (operator vindicated)
                Gov->>SA: grantAppeal(slashId)
                SA->>CB: settleAppealGranted — escrow refunded to operator, slashedAtEpoch recomputed
                SA-->>Op: APPEAL_BOND refunded
            else DecdnGovernor upholds (slash stands)
                Gov->>SA: upholdAppeal(slashId)
                SA->>CB: settleAppealUpheld — escrow 50% challenger / 50% burn
                SA->>SA: 100% of APPEAL_BOND burned
            else governance silent past APPEAL_RATIFICATION_WINDOW
                Op->>SA: cleanupExpiredAppeal(slashId) — operator-favorable grant, APPEAL_BOND refunded
            end
        else multisig rejects at intake
            EM->>SA: rejectAppeal(slashId)
            SA->>CB: settleAppealUpheld — escrow 50% challenger / 50% burn
            SA->>SA: 100% of bond burned
        end
    else multisig silent past APPEAL_REVIEW_WINDOW
        Op->>SA: cleanupExpiredAppeal(slashId) — upheld, 100% of APPEAL_BOND burned, escrow 50/50
    end
```

The operator action — `openSlashAppeal(slashId, evidenceBundleHash)` — is the entry point on `SlashAppeal`. It reads the slash record from `CapacityBond`, requires `msg.sender` to be that recorded operator (operator-only filing — see § Decision), enforces the 30-day filing window and the 365-day frequency cap, and calls `markAppealOpen` to lock the escrow. The fast-track / ratification structure is the same pattern [ADR 011 § Regional Governance Bodies](011-content-takedown.md#regional-governance-bodies) uses for regional-body suspension.

**Escrow-on-slash eliminates clawback exposure structurally.** The slashed TOKEN sits in `CapacityBond` escrow from the moment of the slash — nothing is paid to the challenger or burned until finality. So a successful appeal simply refunds the operator's own escrowed TOKEN, and a failed appeal distributes it. There is no separate restitution pool, no USDC↔TOKEN conversion, and no clawback of already-distributed funds. The only trade-off is that the challenger's 50% reward waits until finality (≤30 days with no appeal, longer if an appeal runs) rather than being paid at slash time — a Negative consequence below.

### Eligibility and evidence standard

To file an appeal the operator must include in the on-chain evidence bundle (referenced by `evidenceBundleHash`) **at least two of** the following corroborating evidence types, plus a sworn declaration:

| Evidence type | Examples |
| --- | --- |
| (a) Cryptographically signed third-party attestation | ISP outage notice, cloud-provider RCA / status-page incident ID with vendor signature, NTP server log, IXP outage bulletin |
| (b) Verifiable network telemetry | Gossip-mesh disconnection witnessed by ≥3 reputable peers per [ADR 008](008-reputation.md#adr-008-reputation-system); probe-fan-out timeouts logged by independent probers during the slash window |
| (c) Co-signed attestation from another registered node operator | EIP-712-signed statement from a reputable peer operator per [ADR 008](008-reputation.md#adr-008-reputation-system) confirming observed downtime in the same datacenter / region |

The sworn declaration is an EIP-712-signed statement (secp256k1, signed by the operator's registered Ethereum address — distinct from the Ed25519 wire identity used in `cdn/probe/v1` and `cdn/client/v1`), attesting under penalty of bond forfeiture that the outage was genuine and no slashable offense was committed during the window. **Perjury — proven by post-hoc evidence — is grounds for re-slashing at the next escalation tier (5%→15%→50%; if already at the 50% tier, perjury triggers immediate auto-ejection per [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn) and full forfeit of the remaining bond) and full forfeit of the appeal bond, in addition to the original slash.** The re-slash executes via the standard `SlashJudge.submit*Challenge()` flow with the sworn declaration entered as evidence.

Evidence type (b) is the on-chain-verifiable path; (a) and (c) are off-chain-rooted but hash-referenced on-chain. The bundle is an attested off-chain artifact referenced by `evidenceBundleHash` on the `SlashAppeal` record.

**Conflict tiebreaker.** If counter-evidence surfaces during the multisig review window contradicting the operator's bundle (e.g. ≥3 reputable peers attest the operator was *up* during the slash window, contradicting (b) gossip-disconnection witnesses), the emergency multisig adjudicates (it can `rejectAppeal` rather than `fastTrackAppeal`). Category (a) signed third-party records are weighted above category (b) gossip telemetry on conflict; category (c) co-signed peer attestations are advisory only when (a) and (b) disagree.

### Appeal bond

The appellant posts `APPEAL_BOND` in TOKEN at the time of filing. Default 1,000 TOKEN; governable with hard bounds `[100, 10,000]` per [ADR 009](009-governance.md#adr-009-governance-model) safety-bound pattern.

- **Granted appeal (Governor `grantAppeal`):** bond refunded to the appellant in full.
- **Failed appeal (multisig `rejectAppeal`, Governor `upholdAppeal`, or review-window lapse):** 100% of the bond is burned. The disposition is identical for every slash-standing terminal path because there is no neutral counterparty to receive it.
- **Ratification-window lapse (Governor silent past `APPEAL_RATIFICATION_WINDOW`):** `cleanupExpiredAppeal` grants operator-favorably and refunds the bond — the operator already cleared the multisig fast-track, so governance inaction is not the appellant's fault.

The bond is the primary economic deterrent against pro-forma appeals filed hoping for multisig sympathy. The 365-day frequency cap ([§ Hard caps and frequency limits](#hard-caps-and-frequency-limits)) and the perjury re-slash ([§ Eligibility and evidence standard](#eligibility-and-evidence-standard)) are secondary deterrents.

### Hard caps and frequency limits

| Parameter | Default | Hard bounds | Rationale |
| --- | ---: | --- | --- |
| `APPEAL_FILING_WINDOW` | 30 days | (fixed; constant on `CapacityBond`) | Allows operators to discover the slash, gather logs, and file. Enforced by `CapacityBond.markAppealOpen`; also gates the permissionless `finalizeUnappealedSlash`. |
| `APPEAL_BOND` | 1,000 TOKEN | `[100, 10,000]` | High enough to deter abuse, low enough that an operator with a genuine outage will pay it. Governable on `SlashAppeal`. |
| `APPEAL_REVIEW_WINDOW` | 14 days | (fixed) | Time the emergency multisig has to fast-track or reject. After this, `cleanupExpiredAppeal` upholds the slash. |
| `APPEAL_RATIFICATION_WINDOW` | 14 days | (fixed, mirrors [ADR 011](011-content-takedown.md#regional-governance-bodies)) | DecdnGovernor must grant or uphold within this window. After this, `cleanupExpiredAppeal` grants operator-favorably. |
| `APPEAL_FREQUENCY_WINDOW` | 1 accepted appeal per 365 days | (fixed) | Prevents a serially-failing operator from rolling outage appeals indefinitely. Resets on the date the *previous* granted appeal landed. |

No `MAX_APPEAL_RESTITUTION` cap exists: under escrow-on-slash a granted appeal returns the operator's *own* escrowed TOKEN, so there is nothing to cap and no TWAP/USDC conversion. There is also no restitution-pool solvency budget — the funds are already escrowed against the specific `slashId`.

**Window timing invariant.** `APPEAL_FILING_WINDOW`, `APPEAL_REVIEW_WINDOW`, and `APPEAL_RATIFICATION_WINDOW` are sequential, each on its own clock starting from a distinct event (slash block, `openSlashAppeal` block, `fastTrackAppeal` block respectively). The filing window is enforced on `CapacityBond` (`markAppealOpen` reverts once `block.timestamp > slashedAt + APPEAL_FILING_WINDOW`); the review and ratification windows are enforced on `SlashAppeal` via `cleanupExpiredAppeal`. The filing window gates *when* an appeal may be opened, not how long it has to conclude.

**Cluster-slash residual exposure.** Both slashable offenses require the operator's own signed messages ([ADR 014 § Bond Handling](014-on-chain-verification.md#bond-handling)), so an external adversary cannot drive a cluster of slashes — the failure mode is operator misconfiguration (buggy release, NTP drift, blacklist sync gap). On a cluster, the operator consumes the `APPEAL_FREQUENCY_WINDOW` slot on the most clear-cut case and absorbs the residual on the others; [§ Reputation handling](#reputation-handling) keeps the reputation cost and lifetime offense counter regardless. Reputation decay ([ADR 008](008-reputation.md#adr-008-reputation-system)) bounds the residual. Governance can revisit the frequency window if observed volume warrants.

### Contract surface

This ADR specifies the contract surface at the semantic level — function signatures, modifiers, authorization gates. The appeal state machine lives in the standalone **`SlashAppeal`** contract; the escrow it operates on lives in `CapacityBond` and is moved only through three `SLASH_APPEAL_ROLE`-gated hooks (`markAppealOpen` / `settleAppealUpheld` / `settleAppealGranted`, see [ADR 016 § Access Control Matrix](016-contract-interactions.md#access-control-matrix)). Appeals are keyed by `slashId` (one appeal per slash); the `CapacityBond` escrow status (`Escrowed`→`AppealOpen`) is the one-shot guard.

```solidity
// On SlashAppeal. `slashId` is allocated by CapacityBond.slash (ADR 014 §
// SlashJudge → CapacityBond) — globally monotonic, single counter across
// both offense types. `evidenceBundleHash` references the off-chain bundle.
function openSlashAppeal(uint256 slashId, bytes32 evidenceBundleHash) external; // posts APPEAL_BOND, calls markAppealOpen
function fastTrackAppeal(uint256 slashId) external onlyEmergencyMultisig;
function rejectAppeal(uint256 slashId) external onlyEmergencyMultisig;   // uphold, burn full appeal bond
function grantAppeal(uint256 slashId) external onlyGovernor;             // operator vindicated → settleAppealGranted
function upholdAppeal(uint256 slashId) external onlyGovernor;            // slash stands → settleAppealUpheld + burn full appeal bond
function cleanupExpiredAppeal(uint256 slashId) external;                 // permissionless lapse handler

// On CapacityBond (SLASH_APPEAL_ROLE — held by SlashAppeal).
function markAppealOpen(uint256 slashId) external;        // Escrowed → AppealOpen; enforces the filing window
function settleAppealUpheld(uint256 slashId) external;    // escrow 50% challenger / 50% burn
function settleAppealGranted(uint256 slashId) external;   // escrow refunded to operator + slashedAtEpoch recomputed
// Permissionless no-appeal finality, also on CapacityBond:
function finalizeUnappealedSlash(uint256 slashId) external; // after the filing window: escrow 50/50
```

`openSlashAppeal` reads the slash record from `CapacityBond`, requires the caller to be the recorded operator (operator-only filing), enforces the 30-day filing window and 365-day frequency cap, pulls `APPEAL_BOND` (`TOKEN.transferFrom`), records the appeal, and calls `markAppealOpen` to lock the escrow. No USDC and no `payout()` machinery are involved — a granted appeal refunds the operator's escrowed TOKEN directly via `settleAppealGranted`.

**Multisig capability scope.** `fastTrackAppeal` and `rejectAppeal` are gated by `SlashAppeal.EMERGENCY_MULTISIG_ROLE`, the same 3-of-5 emergency multisig as [ADR 009 § Emergency Multisig](009-governance.md#emergency-multisig). The threshold and signing semantics are unchanged from [ADR 009](009-governance.md#emergency-multisig).

**Dependency on slash identifiers.** The `slashId` argument refers to the slash record minted by `CapacityBond.slash` (driven by `SlashJudge` per [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)) — a globally monotonic non-zero counter across both offense types. Without a minted slash record, no appeal can be filed.

A standalone `SlashAppeal` contract (rather than inlining the appeal logic into `CapacityBond`) keeps `CapacityBond` comfortably under the 24 KB EIP-170 limit and isolates the dispute-policy surface from the escrow custodian; the cross-contract coupling is the narrow `SLASH_APPEAL_ROLE` hook set above.

### Reputation handling

A successful appeal **does not** reverse the operator's reputation event ([ADR 008](008-reputation.md#adr-008-reputation-system)); the operator bears the residual reputation cost. This is deliberate:

- Reputation is a locally-observed signal, not contract state — each peer scores the operator independently from its own interactions ([ADR 008](008-reputation.md#adr-008-reputation-system)), so there is no central record to reverse, and post-hoc reversal would weaken its meaning as an ongoing observability metric.
- Bearing the reputation cost pressures against frivolous outage appeals: a genuine-outage operator accepts the hit because the alternative (the slash) is worse; a marginal-claim operator has less incentive to file since the reputation stays.
- Reputation decay and reset paths in [ADR 008](008-reputation.md#adr-008-reputation-system) provide a slower restoration on continued good behavior.

The lifetime offense counter ([ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn)) is similarly **not** decremented. The escalation tier (5%→15%→50%) advances on the next offense as if the slash had stood. This preserves the deterrent: an appeal restitutes capital, not standing.

### Frivolous-appeal abuse model

Modeled abuse paths and counters:

| Abuse path | Counter |
| --- | --- |
| Operator A is slashed legitimately, files appeal hoping multisig sympathy. | Full `APPEAL_BOND` burn on every failed path; perjury re-slash ([§ Eligibility and evidence standard](#eligibility-and-evidence-standard)) if sworn declaration is contradicted by the multisig's review; reputation cost stands ([§ Reputation handling](#reputation-handling)). |
| Operator commits a real offense, fabricates an outage to escape consequences. | Each [§ Eligibility and evidence standard](#eligibility-and-evidence-standard) evidence type is cryptographically bound to a third party whose own signing identity is at risk on perjury: (a) requires a verifiable third-party signature (ISP, cloud provider, NTP source); (b) requires peer-gossip telemetry that other parties have signed; (c) requires an EIP-712 signature from a peer operator whose reputation and bond are themselves on the line. The ≥2-of-3 requirement therefore forces collusion across at least two independent signing parties, and fabrication of any source is provable post-hoc by the original signer disclaiming the signature — triggering the perjury re-slash. |
| Operator games the 365-day frequency cap by spreading offenses across calendar years. | Cap is rolling, not calendar-bound — measured from the previous successful ratification date. |
| Sybil operator network co-signs each other's (c) attestations. | Each qualifying co-signer must be a separately staked, capacity-bonded operator ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)), so a sybil ring of witnesses means funding multiple real bonds — the cost scales with the number of fake identities, not the ease of spinning them up. |
| Multisig grants interim relief, but ratification fails — operator could withdraw restitution before the reversal lands. | Structurally impossible: the slashed TOKEN never leaves `CapacityBond` escrow until finality. `grantAppeal` refunds it to the operator; `upholdAppeal` distributes it 50/50. There is no intermediate operator-held capital to claw back. |
| Sybil ring files appeals to drain a shared reserve. | No shared reserve or appeal-bond incentive pool exists. Each appeal can only ever release the escrow of its own `slashId` back to that slash's operator — there is no pool to drain and no cross-appeal fund flow. The `APPEAL_BOND` + ≥2-evidence-source bar still gate frivolous filings. |

## Consequences

### Positive

- Closes the issue-403 gap with a bounded, documented mechanism — no ad-hoc multisig discretion for legitimate-outage cases.
- Reuses existing primitives: `CapacityBond` escrow, emergency multisig, DecdnGovernor, [ADR 011](011-content-takedown.md#regional-governance-bodies) ratification pattern. No new governance body; one small new contract (`SlashAppeal`). The appeal bond is deliberately *not* modeled on [ADR 014 § Bond Handling](014-on-chain-verification.md#bond-handling): a challenger bond there is returned whenever verification passes and is never forfeited after the fact, whereas an appeal bond is burned in full on every slash-standing terminal path.
- Every failed appeal bond has one deterministic disposition: 100% burn. No counter-evidence identity, claim path, disbursement path, or incentive-pool fund sink is required.
- Operator relief is bounded and predictable: the multisig fast-track decision lands within `APPEAL_REVIEW_WINDOW` (14 days) and the operator's escrowed TOKEN is refunded on `grantAppeal` within at most another 14 days (`APPEAL_RATIFICATION_WINDOW`), vs. the ~9-day minimum + indefinite proposal-drafting latency of a Governor-only path.
- Operator trust improves — onboarding pitches can point to a documented appeal path rather than "trust the multisig."
- Reputation and offense-count are preserved, so the repeat-behavior deterrent is intact.

### Negative

- Adds the `SlashAppeal` contract (six entry points: `openSlashAppeal` / `fastTrackAppeal` / `rejectAppeal` / `grantAppeal` / `upholdAppeal` / `cleanupExpiredAppeal`) plus three `SLASH_APPEAL_ROLE` escrow hooks on `CapacityBond`, increasing surface area and audit cost.
- Operators must front `APPEAL_BOND` (1,000 TOKEN default) to file — a real frictional cost at PoC TOKEN prices for genuinely-affected smaller operators. Cold-start considerations may motivate a lower default during the PoC window.
- **Challenger reward is delayed to finality.** Because the slashed TOKEN is escrowed until the appeal window resolves, the challenger's 50% is not paid at slash time — it lands on `finalizeUnappealedSlash` (≤30 days if no appeal) or on appeal resolution (up to ~58 days). This is the deliberate trade for clawback-free restitution; the bond reduction itself is immediate, so the operator's penalty is felt at slash time regardless.
- Evidence standard (≥2 corroborating sources) is documentation-heavy for solo operators without enterprise-grade observability.

### Risks

- **Multisig precedent drift.** Even with ratification oversight, repeated fast-tracking of borderline appeals could harden a soft norm of "the multisig will always grant interim relief." Mitigation: outcomes are public and observable per-appeal.
- **Sworn-declaration enforcement gap.** The perjury re-slash relies on post-hoc evidence surfacing. If post-hoc evidence is hard to obtain (private RPC logs, ISP records aged off), perjury becomes practically un-prosecutable. The bond forfeit and frequency cap remain as backup deterrents.
- **Stuck escrow on unresolved appeal.** If an appeal is opened but neither the multisig nor governance acts, the escrow stays locked. Mitigation: the permissionless `cleanupExpiredAppeal` always reaches a terminal state once the review/ratification windows lapse, so no escrow is locked indefinitely.
- **Reputation hit persists across appeal.** [§ Reputation handling](#reputation-handling) deliberately preserves reputation, so a slashed operator's [ADR 008](008-reputation.md#adr-008-reputation-system) reputation hit is not reversed on a successful appeal. The hit lowers selection probability ([ADR 001 node selection](001-network.md#node-selection-algorithm)), reducing real `bytes_delivered` and therefore the operator's USDC fee revenue under the FeeRouter split per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split). For high-volume operators this can be a far larger economic loss than the restituted slash. Restoration follows the slower [ADR 008](008-reputation.md#adr-008-reputation-system) decay paths. The operator absorbs this residual cost; the [§ Reputation handling](#reputation-handling) rationale (reversing peer-aggregated reputation post-hoc weakens its meaning as an ongoing signal) is the explicit trade-off.

## Cross-ADR Impact

- **[ADR 026 — Tokenomics](026-tokenomics.md#slashing-and-burn):** escrow-on-slash and the 50%-challenger / 50%-burn finality split are specified in § Slashing and burn; this ADR specifies the appeal that decides refund-vs-distribute.
- **[ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model):** the `SlashAppeal` contract, the `SLASH_APPEAL_ROLE` grant on `CapacityBond`, and the deployment / wiring order are reflected in the Contract Inventory, Access Control Matrix, and Post-Deployment Initialization.
- **[ADR 032 — SafetyReserve appeal-surface contract surface](_history/032-safety-reserve-appeals-contract.md):** RETIRED. The appeal state machine it pinned for `SafetyReserve` is re-homed to `SlashAppeal`; the canonical surface is [§ Contract surface](#contract-surface) above.
- **[ADR 033 — Safety and Insurance Reserve](_history/033-safety-insurance-reserve.md):** RETIRED. The reserve no longer exists; restitution is escrow-refund-in-TOKEN.
- Removing a wrongful blacklist *entry* is a separate concern with no appeal machinery of its own — see [ADR 011 § Removing a Wrongful Entry](011-content-takedown.md#removing-a-wrongful-entry). `SlashAppeal` is the protocol's only appeal surface.
