# ADR 028: Slashing Appeals and Dispute Escalation

**Date:** 2026-05-06
**Status:** Draft
**Touches:** [ADR 008](008-reputation.md), [ADR 009](009-governance.md), [ADR 011](011-content-takedown.md), [ADR 014](014-on-chain-verification.md), [ADR 018](018-liquidity-strategy.md), [ADR 026](026-gauge-boost-tokenomics.md)

## Context

The protocol slashes operator stake at 5% / 15% / 50% escalation tiers ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn)). One narrow due-process window exists today, which does not cover a node operator who suffers a slash because of a legitimate operational failure (network outage, NTP drift, regional ISP failure, hosting-provider incident):

- **48-hour appeal window** in `SafetyReserve` ([ADR 009 § SafetyReserve Payout Authorization](009-governance.md#safetyreserve-payout-authorization), [ADR 026 §5](026-gauge-boost-tokenomics.md#5-safety-and-insurance-reserve-3-bucket)). Protects payouts of the reserve, not slashes themselves; an operator cannot directly invoke this gate.

All three `SlashJudge` offenses — phantom delivery, rate manipulation, and blacklist violation — execute immediately on successful on-chain verification with **no counter-evidence window** ([ADR 014 §Bond Handling](014-on-chain-verification.md#bond-handling)). Operators hit by these offenses while offline have zero in-protocol recourse.

Without a documented escalation path, every legitimate-outage slash becomes either a permanent operator loss (damages onboarding and operator trust) or an ad-hoc emergency-multisig discretion event (sets unbounded multisig precedent). A bounded, documented mechanism is needed before mainnet. This ADR adds a governance-level appeal layered on top of the existing slash machinery — no new governance bodies, no changes to slash execution, no on-chain stake reversal.

The narrower regional-blacklist appeal mechanism (issue #131) is out of scope and tracked separately under ADR 011.

## Decision

A node operator may file a **slashing appeal** within 30 days of a `SlashJudge` resolution. Appeals are heard by the existing emergency multisig under a fast-track authority that mirrors [ADR 011 § Regional Governance Bodies](011-content-takedown.md#regional-governance-bodies)' suspension pattern: interim relief is granted by the multisig (3-of-5) and must be ratified or reversed by the ve-Governor within 14 days. Successful appeals are remedied by `SafetyReserve` restitution, an eligible payout category already enumerated in [ADR 026 §5](026-gauge-boost-tokenomics.md#5-safety-and-insurance-reserve-3-bucket) ("Incorrect slashing / appeal reversals"). The on-chain slash and the operator's lifetime offense counter ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn)) are not modified.

### 1. Scope

All three `SlashJudge` offense types are appealable: phantom, rate manipulation, blacklist. Each executes immediately at submit time with no in-protocol counter-evidence opportunity ([ADR 014 §Bond Handling](014-on-chain-verification.md#bond-handling)); the legitimate-outage rationale applies to all three.

The eligibility bar (§3) is the gate against frivolous appeals, not the offense type. **Scope limitation: ADR 028 covers appeals against the *slash event itself* on operational-failure grounds — the operator was unable to comply because of an outage, NTP drift, or similar.** Appeals against the *underlying [`ContentBlacklist`](011-content-takedown.md#contract-contentblacklist) entry* — i.e., disputing whether the blacklisted hash should be on the list at all — are out of scope here and tracked separately under [ADR 011](011-content-takedown.md) (issue #131). §3's evidence standard explicitly does not admit content-policy arguments (e.g., "the blacklist entry was wrongly issued" or "the takedown notice was overbroad"); only the operational-failure evidence types defined in §3 are admissible. The multisig is expected to apply heightened scrutiny when reviewing blacklist-offense appeals, given that blacklist offenses involve deliberate moderation noncompliance rather than purely operational failure modes. This guidance is not coded into the contract.

### 2. Appeal flow

```mermaid
sequenceDiagram
    participant Op as Operator
    participant SR as SafetyReserve
    participant EM as Emergency Multisig
    participant Gov as ve-Governor
    Note over Op,Gov: T+0 — SlashJudge resolution executes the slash
    Op->>SR: T+0..30d — TOKEN.approve(SR, APPEAL_BOND)
    Op->>SR: openSlashAppeal(slashId, evidenceBundleHash) — bond transferred
    SR-->>Op: appealId, appeal record stored
    Note over SR: MULTISIG_REVIEW_WINDOW = 14d
    alt multisig acts within window
        EM->>SR: fastTrackAppeal(appealId) or rejectAppeal(appealId)
        alt fast-track approved
            SR->>SR: provisional restitution moves to per-appeal escrow — not disbursed
            Note over SR: gate (3) — 48h SafetyReserve counter-bundle window (ADR 026 §5)
            alt counter-bundle filed and accepted within 48h
                SR->>SR: appeal fails — escrow returns to SR; 50% bond burned, 50% to counter-bundle filer
            else 48h elapses with no successful counter-bundle
                Note over SR: RATIFICATION_WINDOW = 14d begins (sequential, not parallel)
                alt ve-Governor ratifies
                    SR-->>Op: escrow released to operator — APPEAL_BOND refunded
                else ve-Governor reverses
                    SR->>SR: escrow returns to SafetyReserve — 50% bond burned, 50% to challenger-incentive pool
                else governance silent past RATIFICATION_WINDOW
                    SR->>SR: escrow returns to SafetyReserve — APPEAL_BOND refunded (operator not at fault)
                end
            end
        else multisig rejects at intake
            SR->>SR: 100% of bond burned (no counter-bundle filer to credit at intake)
        end
    else multisig silent past MULTISIG_REVIEW_WINDOW
        SR->>SR: appeal expires unless ve-Governor acts directly — APPEAL_BOND refunded
    end
```

The operator action — `openSlashAppeal(slashId, evidenceBundleHash)` — is a new entry point on `SafetyReserve`. The post-authorization gates (48-hour appeal window, post-incident reporting) are the ones already enumerated in [ADR 009 § SafetyReserve Payout Authorization](009-governance.md#safetyreserve-payout-authorization). The fast-track / ratification structure is the same pattern [ADR 011 § Regional Governance Bodies](011-content-takedown.md#regional-governance-bodies) uses for regional-body suspension.

**Escrow-until-ratification is the canonical disbursement path.** On `fastTrackAppeal`, the equivalent USDC payout is moved from the SafetyReserve general balance into a per-appeal escrow account inside `SafetyReserve` and only released to the operator on ratification. This eliminates clawback exposure entirely: a reversal simply returns the escrowed funds to the general balance. The trade-off — operator working-capital exposure during the ratification window — is documented as a Negative consequence below.

### 3. Eligibility and evidence standard

To file an appeal the operator must include in the on-chain evidence bundle (referenced by `evidenceBundleHash`) **at least two of** the following corroborating evidence types, plus a sworn declaration:

| Evidence type | Examples |
| --- | --- |
| (a) Cryptographically signed third-party attestation | ISP outage notice, cloud-provider RCA / status-page incident ID with vendor signature, NTP server log, IXP outage bulletin |
| (b) Verifiable network telemetry | Gossip-mesh disconnection witnessed by ≥3 reputable peers per [ADR 008](008-reputation.md); probe-fan-out timeouts logged by independent probers during the slash window |
| (c) Co-signed attestation from another registered node operator | EIP-712-signed statement from a reputable peer operator per [ADR 008](008-reputation.md) confirming observed downtime in the same datacenter / region |

The sworn declaration is an EIP-712-signed statement (secp256k1, signed by the operator's registered Ethereum address — distinct from the Ed25519 wire identity used in `cdn/probe/v1` and `cdn/client/v1`), attesting under penalty of stake that the outage was genuine and that no slashable offense was committed during the window. **Perjury — proven by post-hoc evidence — is grounds for re-slashing at the next escalation tier (5%→15%→50%; if the operator is already at the 50% tier, the perjury triggers immediate auto-ejection per [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) and full forfeit of the remaining stake) and full forfeit of the appeal bond, in addition to the original slash.** The re-slash is executed via the standard `SlashJudge.submit*Challenge()` flow with the operator's sworn declaration entered as evidence.

Evidence type (b) is the on-chain-verifiable path; (a) and (c) are off-chain-rooted but referenced by hash on-chain. The bundle format mirrors `SafetyReserve`'s existing attested-incident-bundle format ([ADR 026 §5 Spending controls](026-gauge-boost-tokenomics.md#spending-controls)).

**Conflict tiebreaker.** If counter-evidence surfaces during the 48-hour SafetyReserve appeal window that contradicts the operator's bundle (e.g., ≥3 reputable peers attest the operator was *up* during the slash window, contradicting (b) gossip-disconnection witnesses), the emergency multisig adjudicates. Off-chain attestations of category (a) — signed third-party records — are weighted above gossip telemetry of category (b) on conflict; co-signed peer attestations of category (c) are advisory only when (a) and (b) disagree. The multisig's adjudication decision is recorded in the post-incident registry alongside the conflicting evidence.

### 4. Appeal bond

The operator posts `APPEAL_BOND` in TOKEN at the time of filing. Default 1,000 TOKEN; governable with hard bounds `[100, 10,000]` per [ADR 009](009-governance.md) safety-bound pattern. Bond economics mirror [ADR 014 §Bond Handling](014-on-chain-verification.md#bond-handling):

- **Successful appeal (ratified by ve-Governor):** bond refunded to operator in full.
- **Multisig rejects at intake (no fast-track granted):** 100% of bond burned. No counter-bundle filer exists to credit at intake; this case has no direct analog in [ADR 014 §Bond Handling](014-on-chain-verification.md#bond-handling).
- **Failed appeal — successful counter-bundle in 48h `SafetyReserve` window:** mirrors [ADR 014 §Bond Handling](014-on-chain-verification.md#bond-handling) bond split — 50% of bond burned, 50% routed *directly to the counter-bundle filer* as the prevailing party.
- **Failed appeal — ve-Governor reverses (no counter-bundle filer):** 50% of bond burned, 50% credited to a `SafetyReserve` challenger-incentive pool used to compensate parties who file successful counter-bundles in *future* 48h windows. (This case has no specific prevailing party to route to directly.)
- **Governance silent past `MULTISIG_REVIEW_WINDOW` or `RATIFICATION_WINDOW`:** bond refunded — the operator is not at fault for governance inaction, and the appeal lapses without economic penalty.

The bond is the primary economic deterrent against pro-forma appeals filed in hopes of multisig sympathy. The 365-day frequency cap (§5) and the perjury re-slash (§3) are the secondary deterrents.

### 5. Hard caps and frequency limits

| Parameter | Default | Hard bounds | Rationale |
| --- | ---: | --- | --- |
| `APPEAL_FILING_WINDOW` | 30 days | `[7d, 90d]` | Allows operators to discover the slash, gather logs, and file. 30d matches the issue-403 suggestion. |
| `APPEAL_BOND` | 1,000 TOKEN | `[100, 10,000]` | High enough to deter abuse, low enough that an operator with a genuine outage will pay it. |
| `MULTISIG_REVIEW_WINDOW` | 14 days | `[3d, 30d]` | Time the emergency multisig has to grant interim relief. After this, the appeal expires unless governance acts directly. |
| `RATIFICATION_WINDOW` | 14 days | (fixed, mirrors [ADR 011](011-content-takedown.md#regional-governance-bodies)) | ve-Governor must ratify or reverse within this window. Same window as regional-body suspension. |
| `MAX_APPEAL_RESTITUTION` | 1× minimum stake denominated in USDC at the slash block's TWAP | (fixed) | An appeal cannot net the operator more than the slashable stake floor ([ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake)). Larger slashes are restituted up to this cap; the operator absorbs the residual. The TOKEN→USDC conversion uses the same Balancer V3 80/20 pool TWAP that [ADR 018](018-liquidity-strategy.md) uses for buyback-and-burn — specifically the same window length and oracle path as [ADR 018 § Buyback execution via Balancer V3](018-liquidity-strategy.md#buyback-execution-via-balancer-v3) — read at the slash block, not the appeal block, so the cap does not move with TOKEN price during the 30-day filing window. Reusing one oracle parameter set keeps appeal and buyback numerics auditable from a single configuration. |
| `OPERATOR_APPEAL_FREQUENCY` | 1 accepted appeal per 365 days | (fixed) | Prevents a serially-failing operator from rolling outage appeals indefinitely. Resets on the date the *previous* successful appeal was ratified. |

**Window timing invariant.** `APPEAL_FILING_WINDOW`, `MULTISIG_REVIEW_WINDOW`, and `RATIFICATION_WINDOW` are sequential and each runs on its own clock starting from a distinct event (slash block, `openSlashAppeal` block, `fastTrackAppeal` block respectively). They are therefore independently governable with no ordering relationship between their bounds — `MULTISIG_REVIEW_WINDOW` may exceed `APPEAL_FILING_WINDOW`, and an appeal filed at the last block of `APPEAL_FILING_WINDOW` continues to be processed for the full review and ratification windows after the filing window closes (the filing window gates *when* an appeal may be opened, not how long it has to conclude). `SafetyReserve` enforces this at the contract layer: `openSlashAppeal` reverts if `block.timestamp - slashBlock.timestamp > APPEAL_FILING_WINDOW`, but no setter or post-filing path consults `APPEAL_FILING_WINDOW` again. Updates to any of the three parameters are independent and do not require a paired update to another (cf. [ADR 009 § Governable Parameters with Safety Bounds](009-governance.md#governable-parameters-with-safety-bounds), which uses paired cross-parameter invariants only where parameters have a genuine ordering relationship).

`MAX_APPEAL_RESTITUTION` payouts count against `SafetyReserve`'s existing per-incident and per-rolling-window USDC ceilings ([ADR 009 § Emergency Multisig](009-governance.md#emergency-multisig) gate (4)) — appeals share the reserve's overall solvency budget with all other payout categories.

**SafetyReserve insolvency at ratification.** If `SafetyReserve` cannot fund the full restitution at ratification time (rolling-window cap reached, or insufficient general balance after concurrent incidents), the appeal succeeds in principle but the unfunded portion is recorded as a *pending claim* with a monotonic `claimId` at the current accrual epoch. Gates 1–3 of the four [ADR 026 §5 Spending controls](026-gauge-boost-tokenomics.md#spending-controls) (attested bundle, authorization, 48-hour appeal window) were checked through the appeal flow ending at `ratifyAppeal`; gate 4 (post-incident reporting) writes atomically on each disbursement. Once the claim is queued no further multisig action is required. The bond is refunded regardless. Disbursement is permissionless head-of-queue when reserve solvency permits, per [ADR 026 §5 Cross-category payout ordering](026-gauge-boost-tokenomics.md#cross-category-payout-ordering) — the queue is epoch-FIFO across all payout categories with `claimId` as a within-epoch monotonic tiebreaker, and no payout category has priority over another.

**Cluster-slash residual exposure.** All three slashable offenses require the operator's own signed messages ([ADR 014 §Bond Handling](014-on-chain-verification.md#bond-handling)), so an external adversary cannot drive a cluster of slashes — the failure mode is operator misconfiguration (buggy release, NTP drift, blacklist sync gap). When such a cluster occurs, the operator consumes the `OPERATOR_APPEAL_FREQUENCY` slot on the most clear-cut case and absorbs the residual on the others; an additional-appeals mechanism would only restitute USDC, since §7 keeps the reputation cost and lifetime offense counter regardless. Reputation decay ([ADR 008](008-reputation.md)) and the pending-claim path above bound the residual. Governance can revisit `OPERATOR_APPEAL_FREQUENCY` if observed volume warrants.

### 6. Contract surface

This ADR specifies the contract surface at the semantic level — function signatures, modifiers, and authorization gates. Storage layout, per-appeal escrow accounting, event-parameter shapes, and the permissionless `cleanupExpiredAppeal` lapse handler are pinned in [ADR 030 — SafetyReserve appeal-surface contract surface](030-safety-reserve-appeals-contract.md). Implementation lands in [#452](https://github.com/decdn/decdn/issues/452). The `SafetyReserve` contract is extended with:

```solidity
// `slashId` is allocated and emitted by SlashJudge.Slashed
// (ADR 014 §2 — globally monotonic, non-zero, single counter across all three offense types).
// `evidenceBundleHash` MUST equal the `evidenceHash` field of the referenced `Slashed` event.
function openSlashAppeal(uint256 slashId, bytes32 evidenceBundleHash) external returns (uint256 appealId);
function fastTrackAppeal(uint256 appealId) external onlyEmergencyMultisig;
function rejectAppeal(uint256 appealId) external onlyEmergencyMultisig;
function ratifyAppeal(uint256 appealId) external onlyGovernor;
function reverseAppeal(uint256 appealId) external onlyGovernor;
// Permissionless lapse handler — see [ADR 030 §4](030-safety-reserve-appeals-contract.md#4-state-machine).
function cleanupExpiredAppeal(uint256 appealId) external;
```

`openSlashAppeal` requires the bond transfer (`TOKEN.transferFrom` of `APPEAL_BOND`) and a non-zero `evidenceBundleHash`; it stores the appeal record and emits `SlashAppealOpened`. The actual restitution disbursement on ratification is routed through the existing `payout(bundleHash, recipient, amount)` entry point so [ADR 026 §5 Interface stability](026-gauge-boost-tokenomics.md#interface-stability)'s contract-stable signature for incident payouts is preserved — appeals are an additional *authorization* path into the same payout machinery, not an additional payout machinery. Subsequent gates are the same four payout gates from [ADR 026 §5 Spending controls](026-gauge-boost-tokenomics.md#spending-controls): attested bundle, authorization (multisig fast-track), 48-hour appeal window, post-incident reporting — applied **sequentially**, in that order. The §2 mermaid reflects this sequencing: the 48h SafetyReserve counter-bundle window completes before the 14-day ratification window opens, not in parallel.

**Multisig capability scope.** `fastTrackAppeal` and `rejectAppeal` are sub-modes of [ADR 009 § Emergency Multisig](009-governance.md#emergency-multisig)'s existing capability (4) "SafetyReserve fast-track authorization" — they consume appeal-specific arguments and emit appeal-specific events but do **not** create a new multisig power. The 3-of-5 threshold, signing semantics, and post-incident reporting obligations are unchanged from [ADR 009](009-governance.md#emergency-multisig), which enumerates these appeal-specific entry points inline under capability (4).

**Dependency on `SlashJudge` slash identifiers.** The `slashId` argument refers to the `Slashed(uint256 indexed slashId, address indexed operator, OffenseType offenseType, uint256 amount, bytes32 evidenceHash)` event canonicalised in [ADR 014 §2 `Slashed` event and `slashId` allocation](014-on-chain-verification.md#slashed-event-and-slashid-allocation). That ADR pins the event for all three offense types (phantom, rate, blacklist) — each of which resolves synchronously at submit time — and pins `slashId` as a globally monotonic non-zero counter. Operators reference this `slashId` directly in `openSlashAppeal`, with `evidenceBundleHash` matching the event's `evidenceHash` field. Without ADR 014's `Slashed` emission, no appeal can be filed.

Extending the existing `SafetyReserve` contract — rather than introducing a new `SlashAppealRegistry` — preserves the deployment budget, reuses the payout machinery, and keeps the public payout registry as the single source of truth for who received protocol restitution and why. The trade-off is acknowledged in [Forward references](#forward-references-follow-up-adrs): if `SafetyReserve` is ever split (e.g., separate reserves per incident category), the appeal-authorization functions must migrate alongside the slash-restitution payout category.

### 7. Reputation handling

A successful appeal **does not** reverse the operator's reputation event ([ADR 008](008-reputation.md)). The operator bears the residual reputation cost of the offense. This is deliberate:

- Reputation is a peer-aggregated signal, not a contract state — reversing it post-hoc is technically expensive and would weaken its meaning as an ongoing observability metric.
- Bearing the reputation cost creates pressure against frivolous outage appeals: the operator who genuinely had an outage accepts the reputation hit because the alternative (the slash itself) is worse; the operator with a marginal claim has less incentive to file because the reputation stays.
- Reputation decay and reset paths in [ADR 008](008-reputation.md) provide a slower restoration mechanism on continued good behavior.

The lifetime offense counter ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn)) is similarly **not** decremented. The escalation tier (5%→15%→50%) advances on the next offense as if the slash had stood. This preserves the deterrent: an appeal restitutes capital, not standing.

### 8. Frivolous-appeal abuse model

Modeled abuse paths and their counters:

| Abuse path | Counter |
| --- | --- |
| Operator A is slashed legitimately, files appeal hoping multisig sympathy. | `APPEAL_BOND` forfeit on reversal; perjury re-slash (§3) if sworn declaration is contradicted by the multisig's review; reputation cost stands (§7). |
| Operator commits a real offense, fabricates an outage to escape consequences. | Each §3 evidence type is cryptographically bound to a third party whose own signing identity is at risk on perjury: (a) requires a verifiable third-party signature (ISP, cloud provider, NTP source); (b) requires peer-gossip telemetry that other parties have signed; (c) requires an EIP-712 signature from a peer operator whose reputation and stake are themselves on the line. The ≥2-of-3 requirement therefore forces collusion across at least two independent signing parties, and fabrication of any source is provable post-hoc by the original signer disclaiming the signature — triggering the perjury re-slash. |
| Operator games the 365-day frequency cap by spreading offenses across calendar years. | Cap is rolling, not calendar-bound — measured from the previous successful ratification date. |
| Sybil operator network co-signs each other's (c) attestations. | Co-signers must be reputable peer operators per [ADR 008](008-reputation.md); reputation is itself peer-aggregated, raising the cost of building a sybil ring high enough to qualify as a witness. |
| Multisig grants interim relief, but ratification fails — operator could withdraw restitution before the reversal lands. | Closed by design: §2's escrow-until-ratification rule moves the restitution into a per-appeal escrow on `fastTrackAppeal` and only releases on `ratifyAppeal`. A `reverseAppeal` simply returns the escrowed funds to the SafetyReserve general balance — there is no operator-held capital to claw back. |
| Sybil ring of operators each file appeals to drain the SafetyReserve below incident-response thresholds. | `MAX_APPEAL_RESTITUTION` cap + `SafetyReserve` per-rolling-window USDC ceiling ([ADR 009](009-governance.md#emergency-multisig)) bound the worst case; sybil sets large enough to exceed those caps must each pass the §3 ≥2-evidence-source bar including reputable peer attestations, raising the cost of building the ring above the reserve damage it could cause. SafetyReserve insolvency triggers the deferred-claim path (§5), so a drain attack cannot deny the reserve to other incident categories indefinitely. |

## Consequences

### Positive

- Closes the issue-403 gap with a bounded, documented mechanism — no ad-hoc multisig discretion needed for legitimate-outage cases.
- Reuses existing primitives: `SafetyReserve` contract, emergency multisig, ve-Governor, [ADR 014 §Bond Handling](014-on-chain-verification.md#bond-handling) bond economics, [ADR 011](011-content-takedown.md#regional-governance-bodies) ratification pattern. No new governance body, no new contract.
- Operator relief is bounded and predictable: the multisig fast-track decision lands within `MULTISIG_REVIEW_WINDOW` (default 14 days) and disbursement follows ratification within at most another ~16 days (48h SafetyReserve counter-bundle window + 14d ratification, applied sequentially), vs. the ~9-day minimum + indefinite proposal-drafting latency of a Governor-only path.
- Operator trust improves measurably — onboarding pitches can point to a documented appeal path rather than "trust the multisig."
- Reputation and offense-count are preserved, so the deterrent against repeat behavior is intact.

### Negative

- Adds six new entry points (`openSlashAppeal`, `fastTrackAppeal`, `rejectAppeal`, `ratifyAppeal`, `reverseAppeal`, plus the permissionless `cleanupExpiredAppeal` introduced in [ADR 030 §6](030-safety-reserve-appeals-contract.md#6-multisig-capability-scope)) plus per-appeal escrow accounting to `SafetyReserve`, increasing the contract's surface area and audit cost.
- Operators must front `APPEAL_BOND` (1,000 TOKEN default) to file, which is a real frictional cost at PoC TOKEN prices for genuinely-affected smaller operators. Cold-start considerations may motivate a lower default during the PoC window.
- Escrow-until-ratification (§2) means the operator does not see disbursed restitution until ve-Governor ratification — up to ~16 days after the multisig fast-track (48h SafetyReserve counter-bundle window + 14d ratification, applied sequentially). For larger slashes this is real working-capital exposure during the holding period; the trade-off is buying out clawback exposure entirely.
- Evidence standard (≥2 corroborating sources) is documentation-heavy for solo operators without enterprise-grade observability.
- **TOKEN→USDC market risk.** `MAX_APPEAL_RESTITUTION` is denominated in USDC at the slash-block TWAP (§5). The full appeal lifecycle — 30-day filing window + multisig review + 48-hour SafetyReserve appeal + 14-day ratification — can run up to ~60 days, during which TOKEN may appreciate against USDC. The operator receives a fixed-USDC restitution that may buy back fewer TOKEN than were slashed, leaving them short of pre-slash protocol standing even after a successful appeal. The slash-block TWAP is the deliberate choice (settling at appeal-block TWAP would expose `SafetyReserve` to TOKEN price moves and incentivize timing the appeal); operators bear the residual price risk.

### Risks

- **Multisig precedent drift.** Even with ratification oversight, repeated multisig fast-tracking of borderline appeals could harden a soft norm of "the multisig will always grant interim relief." Mitigation: ratification reversals should be public and the post-incident registry should track multisig fast-track decisions vs. ratification outcomes as an observable metric.
- **Reserve solvency.** A correlated outage event (regional cloud provider failure) could trigger many simultaneous appeals against the same `SafetyReserve` budget. The per-rolling-window cap from [ADR 009](009-governance.md#emergency-multisig) bounds the worst case at the cost of pro-rata rationing across affected operators.
- **Sworn-declaration enforcement gap.** The perjury re-slash relies on post-hoc evidence surfacing. If post-hoc evidence is hard to obtain (private RPC logs, ISP records aged off), perjury becomes practically un-prosecutable. The bond forfeit and frequency cap remain as backup deterrents.
- **Cross-subsidy / depletion ratio.** A 50% slash on a min-stake operator nets `SafetyReserve` ~30% × stake from inflow ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn) distribution: 50% challenger / 30% reserve / 20% burn), while a successful appeal can pay out up to 1× minimum stake — a worst-case ~3.3× depletion ratio for the corresponding incident, rising further if the slash tier was 5% or 15% rather than 50%. The reserve is therefore funding a cross-subsidy from non-slash inflows (FeeRouter 3% safety bucket per [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553)) into slash restitution. This cross-subsidy is accepted explicitly for stakes at or near the minimum, where restitution can fully cover the slash; for operators staking well above the minimum, a 50% slash exceeds `MAX_APPEAL_RESTITUTION` and the operator absorbs the residual (§5). The protocol prioritizes reserve solvency over full-restitution-for-whales; restitution caps at the slashable stake floor, not at the operator's actual slash amount. Reserve sizing in [ADR 026 §5](026-gauge-boost-tokenomics.md#5-safety-and-insurance-reserve-3-bucket) — and the underlying economic-model spec — must accommodate this depletion ratio when projecting reserve solvency. The deferred-claim path (§5) bounds the worst case at the cost of operator working-capital exposure during depletion windows.
- **Reputation hit persists across appeal.** §7 deliberately preserves reputation, which means a slashed operator's [ADR 008](008-reputation.md) reputation hit is not reversed on a successful appeal. The reputation hit lowers the operator's selection probability ([ADR 001 node selection](001-network.md#node-selection-algorithm)), which reduces real `bytes_delivered` and therefore their share of the [ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula) gauge-boost formula. For high-volume operators this can be a far larger economic loss than the restituted slash. Restoration follows the slower [ADR 008](008-reputation.md) decay paths. The operator absorbs this residual cost; the §7 rationale (reversing peer-aggregated reputation post-hoc weakens its meaning as an ongoing signal) is the explicit trade-off.

## Alternatives Considered

- **ve-Governor-only path (no multisig fast-track).** Rejected: ~9-day minimum governance latency (7d voting + 48h timelock per [ADR 009](009-governance.md#production-ve-weighted-governance)) is too slow for an operator who needs working-capital relief during an active business. The multisig fast-track + ratification structure is borrowed exactly from [ADR 011 § Regional Governance Bodies](011-content-takedown.md#regional-governance-bodies) for the same reason.
- **Dedicated arbitration committee.** Rejected: introduces a new on-chain governance body, a new election mechanism, and a new attack surface, none of which is justified by the appeal volume the protocol expects (single-digit appeals per quarter at PoC scale, low-tens at production scale).
- **On-chain slash reversal.** Rejected: clawback on already-distributed challenger rewards (50% of slashed amount per [ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn)) is intractable — the challenger may have already moved the funds. `SafetyReserve` restitution is equivalent in capital terms and avoids the clawback complexity entirely. Reputation/offense-count preservation is a feature, not a bug (§7).
- **Hybrid stake reversal + reputation reset.** Rejected for the same clawback reason, plus the reputation-preservation rationale in §7.
- **Narrowing scope to a subset of offenses.** Rejected: phantom, rate, and blacklist all execute immediately with no in-protocol due process; restricting appeals to a subset would leave a corresponding portion of operator-trust gap unaddressed.

## Forward references (follow-up ADRs)

- Storage layout, per-appeal escrow accounting from §2, and the six Solidity event signatures (`SlashAppealOpened` / `SlashAppealFastTracked` / `SlashAppealRejected` / `SlashAppealRatified` / `SlashAppealReversed` / `SlashAppealLapsed`) are pinned in [ADR 030 — SafetyReserve appeal-surface contract surface](030-safety-reserve-appeals-contract.md). ADR 030 also introduces a sixth external entry point `cleanupExpiredAppeal(appealId)` (permissionless) for the two lapse cases below; see [ADR 030 §6](030-safety-reserve-appeals-contract.md#6-multisig-capability-scope).
- **`SafetyReserve` future split.** If the reserve is ever decomposed into separate per-category contracts (e.g. distinct reserves for slash-restitution vs. SLA-breach vs. payment-channel downtime), the following state and entry points must migrate alongside the slash-restitution payout category:
  1. The six §6 entry points (`openSlashAppeal` / `fastTrackAppeal` / `rejectAppeal` / `ratifyAppeal` / `reverseAppeal` / `cleanupExpiredAppeal` per [ADR 030 §6](030-safety-reserve-appeals-contract.md#6-multisig-capability-scope)).
  2. Per-appeal escrow accounting from §2.
  3. The pending-claim register from §5 (including the cross-category epoch-FIFO queue pinned in [ADR 026 §5 Cross-category payout ordering](026-gauge-boost-tokenomics.md#cross-category-payout-ordering)).
  4. The challenger-incentive pool state implied by §4.

  The migration must preserve §2's escrow semantics. This is a known coupling cost of the §6 reuse decision and is intentional for PoC; revisiting at the time of any reserve split is sufficient.

- The narrower regional-blacklist appeal mechanism (issue #131) is tracked separately under [ADR 011](011-content-takedown.md). The two appeal paths are deliberately decoupled — content-policy disputes and operator-outage disputes have different evidence standards and different stakeholder pools.
