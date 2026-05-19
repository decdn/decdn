# ADR 033: Safety and Insurance Reserve

**Date:** 2026-04-25
**Status:** Draft

## Context

The 3% safety/insurance bucket of the `FeeRouter` six-bucket split ([ADR 026 §2](026-tokenomics.md#2-feerouter-split-40407553)) is held in a governance-gated `SafetyReserve` contract. This ADR specifies that reserve: eligible payout categories, spending controls, cross-category payout ordering, interface stability, and the `ISafetyReserve` contract surface. The economic model that sizes the bucket is [ADR 026](026-tokenomics.md); the appeal-surface that feeds slash-restitution claims into it is [ADR 032](032-safety-reserve-appeals-contract.md).

## Decision

The 3% safety bucket is held in `SafetyReserve`, a governance-gated incident reserve. Eligible payout categories:

- Incorrect slashing / appeal reversals.
- Relay, sequencer, or payment-channel downtime.
- Bad-data incidents where user recourse is more valuable than pure burn.
- Future incident-response contracts that integrate via the stable `payout(bundleHash, recipient, amount)` interface.

### Spending controls

Disbursements require all of:

1. An attested incident bundle (cryptographic evidence of the failure, identity of the harmed party, proposed payout amount).
2. A governance proposal, or fast-track multisig approval (within hard caps per [ADR 009](009-governance.md)).
3. A 48-hour appeal window during which the bundle is challengeable on-chain.
4. **Post-incident reporting.** On payout settlement, `SafetyReserve` writes an immutable record to its public on-chain registry (see [ADR 009 § SafetyReserve Payout Authorization](009-governance.md#safetyreserve-payout-authorization) for the record fields and reporting obligations).

No path exists for unattested payouts; the `payout(bundleHash, recipient, amount)` entry point checks all four gates.

### Cross-category payout ordering

When `SafetyReserve` solvency is insufficient to immediately fund every authorized disbursement — most plausibly during a correlated-outage window combining slash-restitution appeals (per [ADR 028 §5](028-slashing-appeals.md#5-hard-caps-and-frequency-limits)) with concurrent SLA-breach payouts — the unfunded portion of each authorization is recorded as a *pending claim* and disbursed once solvency permits. The queue is keyed on `(accrualEpoch asc, claimId asc)`:

- **`accrualEpoch`** is the FeeRouter 1-week epoch ([ADR 026 §2 Epoch mechanics](026-tokenomics.md#epoch-mechanics)) in which the original `payout()` authorization first hit insolvency. SafetyReserve does not maintain a separate epoch clock; using the FeeRouter epoch keeps `accrualEpoch` derivable from any block timestamp without an additional canonical clock.
- **`claimId`** is a monotonic `uint256` counter assigned by `SafetyReserve` at authorization time, incremented atomically as each pending claim is recorded. It is the within-epoch tiebreaker — not a payout-category priority signal, just a deterministic disambiguator for the rare case of multiple claims accruing in the same epoch.

The queue ordering is therefore **epoch-FIFO across all payout categories with a per-claim monotonic tiebreaker within an epoch**. Three properties follow:

- **No payout category has cross-category priority.** All claimants — slash-appellants, SLA-breach, future integrations — enter one queue keyed by `(accrualEpoch, claimId)`; `claimId` is protocol-monotonic, not category-coded.
- **No multisig-as-orderer hazard.** Authorization order sets `claimId` only in the rare same-epoch tie, and deterministically; once authorized, queue position is fixed.
- **Forward-compatible with new payout categories.** Contracts integrating via the stable `payout(bundleHash, recipient, amount)` interface inherit these semantics without amending this ADR.

**Disbursement of queued claims is permissionless.** Gates 1–3 of the four [Spending controls](#spending-controls) (attested bundle, authorization, 48-hour appeal window) were checked at `payout()` authorization; gate 4 (post-incident reporting) writes atomically per disbursement. Thereafter any caller may invoke a `disbursePending()` head-of-queue path when solvency permits — no second-stage authorization exists, so the multisig cannot selectively re-authorize favored queued claims. This is what makes the ordering guarantee meaningful, and mirrors the permissionless-detection pattern in [Appendix: Fraud Detection](appendix-fraud-detection.md). The `disbursePending()` signature and the pending-claim storage shape are pinned in the [`ISafetyReserve` interface](#contract-safetyreserve) below; this section pins the ordering and permissionless-disbursement semantics.

### Interface stability

The `payout(bundleHash, recipient, amount)` signature is contract-stable: future incident-response tooling, insurance products, and SLA-style contracts integrate via this entry point without contract changes. Evidence formats live off-chain and are referenced by hash on-chain; the contract enforces the four payout gates uniformly regardless of caller identity (subject to `AccessControl` role grants per [ADR 016 §5](016-contract-interactions.md#5-access-control-matrix)). `payout()` is the AccessControl-gated authorization path; `disbursePending()` is permissionless by design (see [Cross-category payout ordering](#cross-category-payout-ordering)) and inherits its evidence-and-gates guarantees from the original `payout()` authorization that placed the claim on the queue.

### Contract: SafetyReserve

```solidity
interface ISafetyReserve {
    // ─── Payouts ──────────────────────────────────────────────────────
    // Single entry point for incident disbursements. USDC-only by design —
    // TOKEN from the 30% slashing redirect is swapped to USDC via
    // `swapAccumulatedTokens` before becoming available here. `bundle`
    // references an off-chain attested incident bundle. Enforces the four
    // payout gates uniformly:
    //   1. Attested bundle (cryptographic evidence)
    //   2. Authorization (Governor or emergency-multisig within hard caps)
    //   3. 48h appeal window since the bundle was first surfaced
    //   4. Post-incident registry write (atomic with disbursement)
    // Reverts if any gate fails. Returns the assigned incident id.
    function payout(
        bytes32 bundle,
        address recipient,
        uint256 usdcAmount
    ) external returns (uint256 id);

    // ─── Incident registry ────────────────────────────────────────────
    enum IncidentReason {
        OutageRestitution,
        SlashAppealRatification,
        ProtocolHack,
        MisattributionFix,
        Other
    }

    // Field order is illustrative; storage-slot packing is an
    // implementation/audit concern (gas micro-tuning is out of scope
    // here — see ADR 032). Logical order: what / who / how-much / when.
    struct Incident {
        bytes32 bundle;          // attested evidence hash
        address recipient;       // payout target
        uint256 usdcAmount;      // USDC base units (6 decimals)
        uint64 paidAt;           // block timestamp
        address paidBy;          // Governor or emergency-multisig that authorized
        IncidentReason reason;   // categorical tag for indexers and audit
    }

    function incidents(uint256 id) external view returns (Incident memory);
    function incidentCount() external view returns (uint256);

    // ─── Pending-claim queue (insolvency overflow of payout) ──────────
    // When payout() cannot be fully funded, the unfunded portion is
    // recorded as a PendingClaim. The queue is epoch-FIFO across all
    // payout categories, ordered (accrualEpoch asc, claimId asc) — see
    // § Cross-category payout ordering. Gates 1–4 were already enforced
    // by the originating payout(); disbursePending() adds no second-
    // stage authorization and is permissionless — any caller may drain
    // the head when reserve solvency permits.
    struct PendingClaim {
        uint256 claimId;        // protocol-monotonic, set at authorization
        uint256 usdcAmount;     // unfunded USDC base units (6 decimals)
        bytes32 bundle;         // attested evidence hash (from payout())
        // packed into one slot: address(20) + uint64(8) + enum(1) = 29 B
        address recipient;      // payout target
        uint64  accrualEpoch;   // FeeRouter epoch payout() first hit insolvency
        IncidentReason reason;  // categorical tag, not a priority signal
    }

    // Disburses the frontmost claim (lowest (accrualEpoch, claimId))
    // and performs the gate-4 post-incident registry write atomically,
    // mirroring payout(). Reverts if the queue is empty or the head is
    // still unfunded. Returns the incident id assigned to the claim.
    function disbursePending() external returns (uint256 id);

    function pendingClaimHead() external view returns (PendingClaim memory);
    function pendingClaimCount() external view returns (uint256);

    // ─── Slashing-redirect inflow (callback from StakingRegistry) ─────
    // Records the 30% slashed-TOKEN redirect against an indexable
    // operator+amount tuple. `SLASH_INFLOW_REPORTER_ROLE`-gated; granted
    // to `StakingRegistry` post-deploy per [ADR 016 § Post-Deployment
    // Initialization](016-contract-interactions.md#post-deployment-initialization).
    // TOKEN is transferred separately via `safeTransfer`; this is the
    // indexable accounting event.
    function recordSlashInflow(address operator, uint256 amount) external;

    // ─── TOKEN → USDC swap (keeper) ───────────────────────────────────
    // Swaps `amountIn` accumulated TOKEN to USDC against the
    // [ADR 018](018-liquidity-strategy.md) Balancer V3 80/20 pool — same
    // Vault-scoped self-approval, TWAP, `minOut`, and per-epoch
    // liquidity-cap defenses as `BuybackBurner`. Per-call batch shape lets
    // keepers MEV-sequence sub-swaps. `KEEPER_ROLE`-gated; `amountIn`
    // bounded by `min/maxBatchAmount`.
    function swapAccumulatedTokens(uint256 amountIn, uint256 minOut) external;

    // ─── Slash-appeal extensions (per ADR 028) ────────────────────────
    // Signature stubs only; full appeal state machine, window timing,
    // storage layout, per-appeal escrow accounting, and event-parameter
    // semantics live in
    // [ADR 032](032-safety-reserve-appeals-contract.md) and
    // [ADR 028 §6](028-slashing-appeals.md#6-contract-surface);
    // parameter values remain ADR 028 §5's responsibility.
    function openSlashAppeal(uint256 slashId, bytes32 evidenceBundleHash)
        external returns (uint256 appealId);
    function fastTrackAppeal(uint256 appealId) external;
    function rejectAppeal(uint256 appealId) external;
    function ratifyAppeal(uint256 appealId) external;
    function reverseAppeal(uint256 appealId) external;
    function cleanupExpiredAppeal(uint256 appealId) external;

    // ─── Governance setters ───────────────────────────────────────────
    function setGovernor(address newGovernor) external;
    function setEmergencyMultisig(address newMultisig) external;
    function setAppealWindow(uint64 seconds_) external;
    function setMinBatchAmount(uint256 amount) external;
    function setMaxBatchAmount(uint256 amount) external;
    function setPool(address newPool) external;
    function setSlippageToleranceBps(uint256 bps) external;

    // ─── Pause control ────────────────────────────────────────────────
    // `pause()` blocks `payout` and `swapAccumulatedTokens`;
    // `recordSlashInflow` keeps working so slashing accounting is never
    // lost during a pause window.
    function pause() external;
    function unpause() external;

    // ─── Events ───────────────────────────────────────────────────────
    event Paid(
        uint256 indexed id,
        address indexed recipient,
        uint256 usdcAmount,
        bytes32 bundle,
        address paidBy,
        IncidentReason reason
    );
    event SlashInflowRecorded(address indexed operator, uint256 amount);
    event SwapExecuted(uint256 amountIn, uint256 amountOut);
    event PendingClaimQueued(uint256 indexed claimId, uint64 indexed accrualEpoch, address indexed recipient, uint256 usdcAmount, IncidentReason reason);
    event PendingClaimDisbursed(uint256 indexed claimId, uint256 indexed incidentId, address indexed recipient, uint256 usdcAmount);
    // Parameter lists pinned in
    // [ADR 032 §3](032-safety-reserve-appeals-contract.md#3-solidity-event-signatures-all-six-pinned).
    event SlashAppealOpened(uint256 indexed appealId, uint256 indexed slashId, address indexed appellant, bytes32 evidenceBundleHash, uint256 bond);
    event SlashAppealFastTracked(uint256 indexed appealId, uint256 escrowAmount);
    event SlashAppealRejected(uint256 indexed appealId, uint256 bondSlashed);
    event SlashAppealRatified(uint256 indexed appealId, uint256 indexed incidentId, address recipient, uint256 restitutionAmount);
    event SlashAppealReversed(uint256 indexed appealId, uint256 escrowReturned, address bondSplitRecipient, uint256 bondSplitAmount);
    event SlashAppealLapsed(uint256 indexed appealId, uint256 escrowReturned, uint256 bondRefunded);
    event GovernorUpdated(address indexed oldAddr, address indexed newAddr);
    event EmergencyMultisigUpdated(address indexed oldAddr, address indexed newAddr);
    event AppealWindowUpdated(uint64 oldValue, uint64 newValue);
    event PoolUpdated(address indexed oldPool, address indexed newPool);
    event SlippageToleranceUpdated(uint256 oldBps, uint256 newBps);
    event MinBatchAmountUpdated(uint256 oldValue, uint256 newValue);
    event MaxBatchAmountUpdated(uint256 oldValue, uint256 newValue);
}
```

**Notes:**

- **USDC-only payouts.** `usdcAmount` is named explicitly so the constraint is visible in the storage layout and on every `Paid` event.
- **Governor and emergency-multisig addresses are governance-mutable.** The `setGovernor` / `setEmergencyMultisig` setters allow the eventual handover from the deployer EOA to `TimelockController` (per [ADR 016 § Post-Deployment Initialization](016-contract-interactions.md#post-deployment-initialization)) and any future re-pointing without contract redeployment. The 48h timelock constraint applies via `GOVERNANCE_ROLE`.
- **Appeal extensions are signature stubs.** This interface pins the function names and parameter types; the full state machine (`Open` → `FastTracked` / `Rejected` → `Ratified` / `Reversed` / `Lapsed`), window timing (filing, multisig review, ratification), and bond/restitution caps live in [ADR 028 §6](028-slashing-appeals.md#6-contract-surface).
