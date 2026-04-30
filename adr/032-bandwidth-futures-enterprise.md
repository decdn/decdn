# ADR 032: Bandwidth Futures and Enterprise SLA Tier

**Date:** 2026-04-25
**Status:** Deferred (target: post-launch follow-up)
**Prerequisites:** [ADR 026](026-gauge-boost-tokenomics.md) §5 SafetyReserve, [ADR 027](027-distinct-client-receipts.md) distinct-client delivery receipts, [ADR 030](030-preseed-usdc-deployment.md) §2e Enterprise SLA guarantee fund
**Touches:** [ADR 026](026-gauge-boost-tokenomics.md), [ADR 030](030-preseed-usdc-deployment.md), [ADR 003](003-payments.md), [ADR 018](018-liquidity-strategy.md)

---

## Context

deCDN (per [ADR 003](003-payments.md) and [ADR 026](026-gauge-boost-tokenomics.md)) serves
**best-effort delivery** at launch — clients open USDC payment channels, settle per-MB
at $0.01/GB, and the protocol guarantees content integrity (BLAKE3-addressed bytes) but
not availability, latency, or throughput. SLA is implicit: "the network is up, or it
isn't." This is sufficient for the freemium / Pro tier of clients (developers, indie
streaming, content sites, hobbyist deployments) and is the operational regime the
[ADR 026](026-gauge-boost-tokenomics.md) tokenomics are sized against.

It is **not** sufficient for two adjacent client segments the economic model and
market-dynamics analysis explicitly target:

1. **Streaming startups and content-aggregator clients** that need cost certainty for
   capacity planning. Pay-as-you-go pricing with monthly volume swings forces these
   clients to over-provision elsewhere or accept budgeting risk. The market-dynamics
   spec §2 calls out **Bandwidth Futures** — TOKEN-denominated pre-purchase contracts
   — as the structural answer: cost certainty for the client, revenue certainty for
   the operator, and a meaningful TOKEN utility sink on the demand side.
2. **Enterprise clients** (large-scale streaming, SaaS, e-commerce, regulated workloads)
   that require **explicit SLAs with penalty clauses**. These clients will not migrate
   from incumbent CDNs (Cloudflare, Akamai, Fastly) to a decentralized network on a
   handshake; they require a contractually-binding availability / latency / throughput
   guarantee with a credible recourse path when the SLA breaches. This is the highest
   per-byte revenue tier the network can target and the moat against pure
   pay-as-you-go competition (market-dynamics §2; preseed-capital-strategy §3).

[ADR 026](026-gauge-boost-tokenomics.md) §5 introduces `SafetyReserve` (3% of routed USDC) as a
governance-gated incident reserve with eligible payout categories that include
"Enterprise SLA compensation (per [ADR 030](030-preseed-usdc-deployment.md) /
[ADR 032](032-bandwidth-futures-enterprise.md))." [ADR 030](030-preseed-usdc-deployment.md)
§2e allocates 10% of the pre-seed pool ($100K floor / $300K target) as a
**paired backing layer** for `SafetyReserve` — covering Enterprise contracts whose
worst-case payout exceeds the early-period organic `SafetyReserve` balance. Both ADRs
forward-reference this ADR as the contract-format and product-tier authority.

This ADR is the **product-tier and contract-shape decision record** — it defines what a
Bandwidth Futures contract looks like, what an Enterprise SLA tier offers, how the two
products compose, and which launch-time prerequisites must be in place before adoption.
It is **not** a futures-DEX design (secondary-market mechanics are out of scope) and
does **not** pin specific SLA values (uptime %, latency ms, throughput targets are
per-contract and live with the Enterprise sales motion, not in this ADR).

**Deferred — post-launch follow-up.** Launch establishes the infrastructure (SafetyReserve,
pre-seed Enterprise fund, distinct-client receipts, the freemium → Pro ladder) on which
this ADR builds; the follow-up adoption pass is when futures and SLA-tier products turn
on. The deferral is a **capability ordering** decision, not a "maybe later" — the
prerequisites listed below are concrete and gate adoption.

---

## Decision

Both products ship together in the adoption pass:

1. **Bandwidth Futures** — TOKEN-denominated bandwidth pre-purchase contracts with
   structured settlement against actual delivery, providing cost certainty for clients
   and revenue certainty for participating operators.
2. **Enterprise SLA tier** — explicit SLA contracts with penalty clauses, backed in
   priority order by `SafetyReserve` (organic + slashing-replenished) and the pre-seed
   Enterprise SLA guarantee fund per [ADR 030](030-preseed-usdc-deployment.md) §2e.

The freemium / Pro / Enterprise tier ladder is the canonical client-segmentation model
once these products land.

### 1. Bandwidth Futures

A futures contract is a TOKEN-denominated, period-bounded commitment to deliver (or
have delivered) a specified bandwidth volume in a specified region (or globally), with
structured settlement at expiry against verified delivery.

#### 1.1 Contract format

| Field | Type | Notes |
| --- | --- | --- |
| `contractId` | `bytes32` | Unique on-chain identifier |
| `buyer` | `address` | Client purchasing the future |
| `seller` | `address` | DAO treasury wallet (at adoption) or designated operator pool |
| `bandwidthGB` | `uint256` | Total GB committed for the period |
| `region` | `bytes32` | Regional gauge identifier per [ADR 030](030-preseed-usdc-deployment.md) §2d, or `bytes32(0)` for "any" |
| `periodStart` | `uint64` | Block-timestamp start of delivery window |
| `periodDuration` | `uint64` | Seconds; multiples of the 1-week epoch per [ADR 026](026-gauge-boost-tokenomics.md) §2 |
| `strikeToken` | `uint256` | TOKEN paid by buyer at contract creation |
| `referenceUsdRate` | `uint256` | USD/GB reference at strike time (for under-delivery refund math) |
| `settlementMode` | `enum` | `PhysicalDelivery` (consume bandwidth) or `CashSettle` (compare delivered vs. committed) |
| `breachCompensationCap` | `uint256` | Max USDC payable on under-delivery; references SafetyReserve / pre-seed pairing per §2 below |

The format is intentionally compact — implementation details (event schemas, exact
storage layout, ERC-721 vs. ERC-1155 contract identity) live in adoption-time
implementation ADRs, not here.

#### 1.2 TOKEN denomination

Strike priced in TOKEN. Buyer pays TOKEN upfront at contract creation; seller (DAO or
designated reserve) commits to deliver bandwidth at a price expressed in
`strikeToken / bandwidthGB`. Two consequences:

- **Demand-side TOKEN sink.** Buyers must acquire TOKEN to purchase futures, creating
  organic demand independent of [ADR 031](031-bme-client-prepay.md)'s deferred BME path. Complements the
  operator-side (gauge boost) and delegator-side (delegator pool) TOKEN demand levers
  in [ADR 026](026-gauge-boost-tokenomics.md).
- **TOKEN-USD basis risk.** A TOKEN price drop between strike and expiry is shared:
  buyer keeps the discount-vs-USD; seller (DAO) absorbs the fiat-equivalent shortfall
  unless TOKEN appreciates above strike. Fixed-USD-rate futures with TOKEN-collateral
  were considered and rejected — they reintroduce the reflexive
  bootstrap risk that the [ADR 026](026-gauge-boost-tokenomics.md) §10 USDC pre-seed
  structure is designed to eliminate.

#### 1.3 Settlement

At `periodStart + periodDuration`:

| Outcome | Settlement |
| --- | --- |
| Fully delivered | Strike TOKEN released to seller; contract closes |
| Under-delivered (no SLA pairing) | Pro-rata TOKEN refund to buyer = `strikeToken × (bandwidthGB − min(delivered, bandwidthGB)) / bandwidthGB`; remainder to seller. The `min(...)` clamp prevents arithmetic underflow when over-delivery is reported (the over-delivered case is handled separately below; the clamp is defense-in-depth). |
| Under-delivered with SLA pairing breached | Pro-rata TOKEN refund **plus** USDC compensation drawn from `SafetyReserve` per §2 (capped at `breachCompensationCap`) |
| Over-delivered | Excess delivery is unbilled; future compensates only up to `bandwidthGB` |

Delivery counts come from the **distinct-client receipts oracle per
[ADR 027](027-distinct-client-receipts.md)** — the same attested-receipt mechanism that
gates gauge-pool eligibility. Without [ADR 027](027-distinct-client-receipts.md) live,
delivery accounting for a contract is not a credible oracle and the futures product
cannot ship; this is the single hardest prerequisite.

#### 1.4 Liquidity

- **Initial market-making.** The DAO is the seller of record at adoption, drawing
  TOKEN from the protocol-treasury allocation per [ADR 026](026-gauge-boost-tokenomics.md) §1
  and using pre-seed Enterprise SLA fund capacity per
  [ADR 030](030-preseed-usdc-deployment.md) §2e for the SLA-paired tranche of
  contracts.
- **Secondary market.** A Balancer V3 pool (per [ADR 018](018-liquidity-strategy.md))
  or a specialized perp-DEX integration may be added for futures-on-futures trading.
  **Out of scope for this ADR** — the secondary-market venue choice is its own
  design problem and is deferred to an adoption-time implementation ADR. Adoption is
  fine with primary-only issuance.
- **Operator participation.** Individual operators may also write futures against
  their own capacity. Operator-written futures default to `PhysicalDelivery` mode
  (the operator owns the delivery commitment) and are **not** SLA-paired by default —
  SLA pairing is reserved for DAO-issued contracts to keep the recourse path
  centralised.

#### 1.5 Oracle and MEV

Delivery oracle is [ADR 027](027-distinct-client-receipts.md) receipts — same wash-trading defense applies (operator self-issuing futures against sybil clients fails the same identity-diversity gate as the gauge pool). Strike and settlement MEV defenses inherit [ADR 018](018-liquidity-strategy.md): TWAP-priced strike, private-RPC routing for TOKEN moves, per-contract 30-day size caps.

### 2. Enterprise SLA Tier

An Enterprise contract is a per-client commercial agreement with explicit availability,
latency, and throughput targets, penalty clauses for breach, and a structured recourse
path backed by on-chain reserves.

#### 2.1 SLA contract format

The **shape** of an Enterprise contract is fixed by this ADR; the **values** in any
given contract are per-deal and live with the Enterprise sales motion.

| Field | Description |
| --- | --- |
| Availability target | E.g., "99.9% rolling 30-day"; per-contract |
| Latency target | E.g., "p95 ≤ 100 ms in named regions"; per-contract |
| Throughput target | E.g., "sustained 10 Gbps aggregate across N regions"; per-contract |
| Measurement window | Per-target rolling-window definition |
| Measurement source | Distinct-client receipts ([ADR 027](027-distinct-client-receipts.md)) + watchtower observations ([ADR 007](007-watchtower.md)) |
| Penalty schedule | Per-target, per-tier breach severity; expressed in USDC compensation |
| Maximum payout | Capped at signing; references `breachCompensationCap` in §1.1 if futures-paired |
| Term | Default 12 months, governance-overridable |
| Renewal | Auto-renew with 60-day cancellation window |
| Recourse priority | Order matches §2.2 below |

The contract template itself is governance-ratified at adoption and updated by
governance proposal thereafter.

#### 2.2 Backing — two layers in priority order

The recourse stack uses **two funding sources**, ordered to prevent double-funding,
matching the resolution logic enforced by `SafetyReserve.payout` per
[ADR 030](030-preseed-usdc-deployment.md) §2e and the
[ADR 026](026-gauge-boost-tokenomics.md) §5 spending controls:

| Priority | Source | Purpose | Origin |
| --- | --- | --- | --- |
| 1 | `SafetyReserve` (single commingled balance) | First line of defense | Organic: 3% of routed USDC per [ADR 026](026-gauge-boost-tokenomics.md) §2. Slashing-replenished: 30% of slashed stake per [ADR 026](026-gauge-boost-tokenomics.md) §8 (same wallet, same gates) |
| 2 | Pre-seed Enterprise SLA paired allocation | Second line of defense | [ADR 030](030-preseed-usdc-deployment.md) §2e (10% of pool, $100K–$300K) |

**A single SLA breach draws on exactly one of these two sources.** The
`SafetyReserve.payout` contract enforces priority — pre-seed paired allocation only
releases when the authorized payout amount exceeds the total `SafetyReserve` balance
available at decision time. This is identical to the
[ADR 030](030-preseed-usdc-deployment.md) §2e / §5 two-source rule and reuses the
same on-chain resolution code.

The `SafetyReserve` payout gates ([ADR 026](026-gauge-boost-tokenomics.md) §5: evidence bundle,
multisig-or-governance authorization, 48-hour appeal window, post-incident registry)
apply unchanged to both sources. The pre-seed paired allocation is **not** a
fast-track — it shares the full SafetyReserve gating regardless of which source funds
the payout.

#### 2.3 Coordination — avoid double-funding

| Mechanism | Where enforced |
| --- | --- |
| Exactly one of two funding sources per incident | `SafetyReserve.payout` (priority order in §2.2) |
| One pending payout per `(contract, breach-window)` | Contract-level invariant |
| Cumulative paired-allocation cap | [ADR 030](030-preseed-usdc-deployment.md) §2e success metrics (no cross-program raids) |
| Quarterly reporting | [ADR 030](030-preseed-usdc-deployment.md) §4 public registry |

#### 2.4 Scaling and pre-seed retirement

Per [ADR 030](030-preseed-usdc-deployment.md) §2e termination trigger: the pre-seed
pairing retires when **organic `SafetyReserve` inflow sustains a balance ≥ maximum
aggregate Enterprise SLA exposure for 6 consecutive months**. At that point:

- The pre-seed paired allocation returns to treasury (or, by governance vote, is
  redirected to other pre-seed programs whose triggers have not fired).
- All Enterprise SLA contracts continue with `SafetyReserve` as the sole backing
  source.
- New contracts use `SafetyReserve` directly; the §2.2 priority-3 source is retired
  from the contract template.

This is the canonical signal that the network has matured past the bootstrap regime
for Enterprise-tier credibility.

### 3. Freemium → Enterprise ladder

Three non-exclusive client segments differentiated contractually, not by the wire protocol. Freemium / Developer tier is a customer-acquisition funnel sized against the community / ecosystem allocation ([ADR 026](026-gauge-boost-tokenomics.md) §1) with a governance-set monthly GB allowance — not a free-forever offering. Pro tier is the [ADR 026](026-gauge-boost-tokenomics.md) §2 default ($0.01/GB, best-effort). Enterprise tier is application-gated through a designated multisig-supervised sales channel and uses the §2.2 backing stack.

---

## Consequences

### Positive

- **Unlocks the highest-margin client segment.** Enterprise contracts pay a per-byte
  premium over the Pro rate; the SafetyReserve + pre-seed backing is what makes that
  premium defensible. Without explicit SLAs, the Enterprise segment is unreachable
  regardless of network capacity.
- **Cost-certainty product on demand side.** Bandwidth futures give streaming
  startups a budgeting tool that pure pay-as-you-go cannot match. Cost certainty is
  the headline value-add the market-dynamics spec §2 identifies.
- **TOKEN demand sink, demand-side.** Future-buyers must acquire TOKEN to purchase
  contracts. Combined with the operator-side gauge-boost demand and delegator-side
  TWAP buys per [ADR 026](026-gauge-boost-tokenomics.md), this completes the
  three-pronged TOKEN demand curve (operator + delegator + client) without requiring
  the BME path in [ADR 031](031-bme-client-prepay.md).
- **Reuses launch infrastructure.** SafetyReserve, distinct-client receipts, and the
  pre-seed pairing are all launch deliverables. Adoption wires them into a product
  surface rather than introducing new on-chain primitives. Audit footprint is
  meaningfully smaller than a from-scratch design.
- **Self-deprecating pre-seed pairing.** [ADR 030](030-preseed-usdc-deployment.md)
  §2e termination trigger ties pre-seed retirement to organic SafetyReserve depth —
  the second-line backing fades out exactly when it is no longer needed, without a
  separate governance debate per contract.
- **Freemium funnel is principled.** The community / ecosystem allocation funds an
  acquisition channel rather than a retention sink; Enterprise pricing carries the
  margin.

### Negative

- **Oracle dependency is hard.** Both products depend on
  [ADR 027](027-distinct-client-receipts.md) being live and reliable in production.
  An oracle compromise (receipt-fraud at scale) is a settlement failure for futures
  *and* a SafetyReserve-draining attack vector for Enterprise SLAs simultaneously —
  the same single-point-of-failure that gauge-pool security depends on. ADR 027 must
  be production-hardened before this ADR ships.
- **Secondary-market depth is required for futures liquidity at scale.** Primary-only
  issuance is fine at adoption but caps how large the Bandwidth Futures product can
  grow. Secondary market design is its own follow-up ADR; until that lands,
  forward-rolling-by-buyer is the only liquidity path.
- **Operational overhead — Enterprise sales channel.** Running an Enterprise sales
  motion is non-trivial. Contract negotiation, onboarding, ongoing relationship
  management, and dispute handling are recurring DAO-funded work. The
  [ADR 030](030-preseed-usdc-deployment.md) §4 quarterly-report cadence partly covers
  this, but a dedicated Enterprise team is the realistic operating model.
- **Regulatory exposure for futures.** Bandwidth futures may be classified as derivatives in some jurisdictions; per-jurisdiction issuance posture and geo-fencing infrastructure are adoption-time prerequisites. Legal review is on the critical path but not a launch-day blocker globally.
- **TOKEN-USD basis risk for the DAO.** §1.2 TOKEN-strike futures put the DAO on the short-TOKEN side of the basis. Per-contract size caps and TWAP-priced strikes mitigate but don't eliminate.
- **Counterparty/adjudicator conflict.** The DAO is both Enterprise contract counterparty and `SafetyReserve` payout adjudicator. Mitigated by the [ADR 026](026-gauge-boost-tokenomics.md) §5 48h appeal window + public incident registry; large contracts may additionally require an independent appeals path (e.g. Kleros-style arbitration) above a size threshold.

### Risks

- **SafetyReserve under-capitalization at launch.** If organic `SafetyReserve` inflow
  has not yet built meaningful depth, the pre-seed paired allocation absorbs almost
  every payout — depleting the §2e $100K–$300K allocation rapidly. Mitigation is
  per-contract `breachCompensationCap` sizing + maximum-aggregate-exposure tracking
  per [ADR 030](030-preseed-usdc-deployment.md) §2e success metrics. **Do not
  underwrite Enterprise contracts whose aggregate worst-case exceeds the combined
  SafetyReserve + pre-seed depth.**
- **Futures wash-trading against the DAO seller.** A buyer who is also the operator
  (or a colluding operator) could purchase a future, "deliver" against a sybil client,
  and pocket the strike TOKEN. Mitigated by [ADR 027](027-distinct-client-receipts.md)
  distinct-client receipts (same defense as the gauge pool) plus operator-eligibility
  gating: operators participating in DAO-issued futures cannot also be the underlying
  delivery counterparty for the same contract.
- **Receipt-oracle compromise correlates failure modes.** Both products consume the
  same delivery oracle. A single oracle exploit harms futures settlement and SLA
  measurement simultaneously. [ADR 027](027-distinct-client-receipts.md) must be
  resilient to this concentration; an adoption readiness gate on ADR 027 is "what
  fraction of total contract notional depends on the receipt oracle being correct."
- **Enterprise SLA breach correlates with network-wide stress.** A regional outage
  triggers many Enterprise contracts simultaneously. SafetyReserve depth must be sized
  against **simultaneous breach scenarios**, not single-contract worst case.
  Sizing analysis lives in the [ADR 030](030-preseed-usdc-deployment.md) §2e success
  metrics + economic-model spec §7; adoption readiness gate.
- **Convex-style capture on regional bandwidth gauges.** If futures markets concentrate
  in specific regions (e.g., Brazil, Southeast Asia per
  [ADR 030](030-preseed-usdc-deployment.md) §2d priorities), gauge votes routing
  delivery capacity to those regions become economically dominant. The governance-
  weight-concentration risk in [ADR 026](026-gauge-boost-tokenomics.md) §Risks compounds in the
  futures regime; [ADR 028](028-sve-token-wrapper.md) native sveTOKEN wrapper is the structural mitigation
  and is a strongly-recommended prerequisite for futures-at-scale.
- **Regulatory action mid-contract.** If a jurisdiction reclassifies bandwidth futures
  as restricted derivatives after contracts are written, the DAO faces enforcement
  exposure on outstanding contracts. Mitigation: contract template includes a
  force-majeure clause that allows pro-rata cash settlement in TOKEN at the prevailing
  TWAP if an active jurisdictional restriction prevents physical delivery.

---

## Prerequisites for adoption

This ADR is deferred. Before adoption begins, all of the following must hold:

| # | Prerequisite | Source | Status gate |
| --- | --- | --- | --- |
| 1 | `PaymentChannel` per-contract billing path exists | [ADR 003](003-payments.md) | Settlement path can carry per-contract metadata (contract identifier in voucher payload) without breaking changes. Launch design must accommodate this shape; implementation can land later |
| 2 | Distinct-client delivery receipts live in production | [ADR 027](027-distinct-client-receipts.md) | Receipt format finalized, watchtower / reputation integration deployed, observed receipt-fraud rate below readiness threshold |
| 3 | `SafetyReserve` accumulates sufficient organic balance | [ADR 026](026-gauge-boost-tokenomics.md) §5 | Balance covers at least one expected single-incident worst case from the §2.1 contract templates without paired-allocation draws |
| 4 | Pre-seed Enterprise SLA fund funded | [ADR 030](030-preseed-usdc-deployment.md) §2e | At least the $100K floor allocation is custodied and operational |
| 5 | Native sveTOKEN wrapper live | [ADR 028](028-sve-token-wrapper.md) (deferred follow-up to ADR 026) | Convex-capture risk mitigated before futures liquidity scales |
| 6 | Regulatory review of futures product | External legal | Per-jurisdiction issuance posture defined; geo-fencing infrastructure in place where required |
| 7 | Enterprise sales channel established | Operational | Designated multisig-supervised team or DAO-elected role; standard contract template ratified by governance |
| 8 | SLA breach measurement infrastructure | [ADR 007](007-watchtower.md) + [ADR 020](020-observability.md) | Per-contract availability / latency / throughput metrics exported and challengeable via watchtower |

Prerequisites 1–4 are **hard gates**: any one absent blocks adoption. Prerequisites 5–8
are **strong recommendations**: adoption without one is possible but materially weakens
the product. Prerequisite 6 is jurisdiction-dependent and may delay adoption in specific
markets without delaying it globally.

---

## Forward references

This ADR is itself a forward reference from [ADR 026](026-gauge-boost-tokenomics.md) and
[ADR 030](030-preseed-usdc-deployment.md). Adoption will produce its own follow-up ADR
set covering at minimum:

- **Bandwidth Futures contract implementation** — exact storage layout, ERC-721 vs.
  ERC-1155 choice, event schemas, on-chain registry.
- **Secondary-market venue selection** — Balancer V3 weighted pool extension vs.
  specialized perp DEX vs. RFQ-style OTC; scoped against observed primary-market
  demand at adoption + 6 months.
- **Enterprise contract template ratification** — the §2.1 shape with concrete
  default values for availability / latency / throughput targets, penalty schedule,
  and dispute-resolution path.
- **Force-majeure and jurisdiction policy** — the §Risks regulatory clause expanded
  into a governance-ratified policy with enumerated jurisdictions and per-jurisdiction
  contract restrictions.

These follow-ups are out of scope for this ADR; the decision recorded here is the
product shape and the launch-time prerequisite set.

---

## ADRs to update on acceptance

Deferred — no launch-time ADRs change. At adoption, deltas land in [003](003-payments.md) (per-contract voucher metadata), [018](018-liquidity-strategy.md) (per-epoch liquidity caps cover futures-driven swap pressure if secondary market shares the pool), [026](026-gauge-boost-tokenomics.md) (§5 SafetyReserve cross-ref resolves), [030](030-preseed-usdc-deployment.md) (§2e contract-template forward-ref resolves).
