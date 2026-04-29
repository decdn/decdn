# ADR 031: Burn-and-Mint Client TOKEN Prepay

**Date:** 2026-04-25
**Status:** Deferred (target: post-launch follow-up after stability + pricing-oracle hardening)
**Touches:** [ADR 003](003-payments.md), [ADR 010](010-multi-token.md), [ADR 026](026-gauge-boost-tokenomics.md)
**Source design spec:** [`docs/superpowers/specs/2026-04-19-tokenomics-v2-survival-additions.md`](../docs/superpowers/specs/2026-04-19-tokenomics-v2-survival-additions.md) §2

---

## Context

[ADR 026](026-gauge-boost-tokenomics.md) creates three TOKEN demand sources, all of them on the operator side of the protocol:

1. Operator stake (50K TOKEN minimum, slashable; [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake)).
2. Gauge-pool ve-locking, where operators ve-lock TOKEN to capture a larger share of the 40% gauge boost pool ([ADR 026 §3](026-gauge-boost-tokenomics.md#3-gauge-boost-formula)).
3. The 7% delegator pool, which performs continuous TWAP USDC→TOKEN buys and routes the acquired TOKEN to ve-lockers ([ADR 026 §6](026-gauge-boost-tokenomics.md#6-delegator-pool--usdc--token-conversion)).

Clients in the [ADR 026](026-gauge-boost-tokenomics.md) design pay only USDC, via the [ADR 003](003-payments.md) payment-channel rails. They never touch TOKEN. The full TOKEN-demand surface is therefore mediated by operator recruitment and operator capital allocation. If operator-side ve-lock adoption falters — for any reason: a competing protocol, regulatory friction in major operator regions, a TOKEN-price shock that makes Case B economics break down ([ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake)) — the demand-side flywheel collapses and there is no usage-driven demand floor underneath it.

This is a known structural fragility. The source design spec (§2 of `2026-04-19-tokenomics-v2-survival-additions.md`) flags it explicitly: *"if operator recruitment falters, demand collapses with no floor."*

**Helium's Burn-and-Mint Equilibrium (BME) model offers the canonical demand-side template.** In BME, end users prepay for network capacity in the native token; the prepaid token is *burned* on consumption (not held by an operator); operators are compensated separately, in a stable unit, from a protocol-administered pool. The result is a usage-driven token sink that grows linearly with network consumption — independent of operator-side dynamics. The pattern has been in production on Helium since 2021 and on Helium Mobile and the IoT subnetworks since 2023.

The BME mechanic does not replace USDC payment channels; it sits *alongside* them as an opt-in client path. ADR 003's USDC channels remain the primary, default, and best-understood payment rail.

**Why this ADR is Deferred.** Two design dependencies are not yet ready for launch:

1. **Pricing oracle.** Converting client-prepaid TOKEN into operator-receivable USDC at consumption time requires a robust TOKEN→USDC oracle. [ADR 018](018-liquidity-strategy.md)'s Balancer V3 80/20 TWAP is a starting point, but oracle-manipulation defenses (longer windows, multi-source price feeds, circuit breakers) need post-launch operating data before they can be hardened to the level required for prepay flows that clients trust enough to fund.
2. **Launch contract scope.** Mainnet ships the [ADR 003](003-payments.md) USDC channel rail plus the [ADR 026](026-gauge-boost-tokenomics.md) `FeeRouter` / `VotingEscrow` / `SafetyReserve` surface. Adding `BmePrepay` to the launch audit scope is not justified given the ambiguous oracle dependency and the fact that the USDC rails must succeed before the BME path delivers any benefit (low-usage networks see negligible BME burn — see Consequences).

This ADR documents the design now so that the USDC payment-channel design ([ADR 003](003-payments.md)) does not preclude later integration. The follow-up adoption pass revisits and hardens the design against then-current oracle and contract-tooling state.

---

## Decision

The protocol will add an optional client-side TOKEN-prepay path implementing the Burn-and-Mint Equilibrium pattern, in a post-launch follow-up. **This ADR is Deferred — no launch-time implementation is committed.** The decision recorded here is forward-looking: it pins the design intent, the launch-time prerequisites that must be preserved, and the design surface that the follow-up will fill in.

The BME path coexists with USDC payment channels; it is opt-in for clients and opt-in for operators (operators may refuse BME-routed traffic).

### 1. Optional client TOKEN prepay path

Clients may optionally prepay bandwidth in TOKEN at a discount to the equivalent USDC rate. The discount is governable, sized at adoption time to motivate use without subsidizing the path past the point where network burn flow exceeds discount cost. The source-spec range is 5–8% discount; the final value is calibrated at adoption based on then-observed TOKEN price stability, oracle-manipulation cost, and competing demand-side levers.

| Parameter | Sizing intent | Notes |
| --- | --- | --- |
| Discount vs equivalent USDC rate | Source-spec range 5–8%; final value deferred | Governable within adoption-time bounds |
| Discount lower bound | Non-zero (otherwise no client incentive) | Set at adoption |
| Discount upper bound | Capped to avoid net-negative flow vs operator USDC base share | Set at adoption |
| Adjustment cadence | Bounded by [ADR 009](009-governance.md) timelock (48h) | Same controls as `FeeRouter` shares |

**Opt-in for clients.** A client choosing the BME path acquires TOKEN on the open market (or on the [ADR 018](018-liquidity-strategy.md) POL pool), deposits it into `BmePrepay` (or the chosen contract surface — see §5), and consumes against the deposit at the discounted rate. Clients who prefer the existing USDC channel rail simply ignore this path; nothing in [ADR 003](003-payments.md) changes for them.

**Opt-in for operators.** Operators may refuse BME-routed traffic if their cost-of-conversion exceeds the discount. This requires a per-operator advertised flag in probe / stream responses (forward-referenced — design fold-in deferred to the follow-up). Operators that accept BME traffic are entitled to the same per-byte revenue (in USDC-equivalent) as they would have received via the USDC channel rail.

### 2. Burn on consumption

Prepaid TOKEN held in `BmePrepay` is **burned** as bandwidth is consumed. No router skim, no treasury cut, no operator routing of the TOKEN itself. This is the pure-burn invariant that defines BME and that distinguishes it from a routed-payment design.

| Property | Value | Why |
| --- | --- | --- |
| Skim on burn | 0% | Pure BME — burn is the demand-side flywheel; partial routing dilutes it |
| Burn timing | At consumption (per-settlement or per-epoch; deferred) | Granularity is an adoption-time parameter; finer granularity = more burn events; coarser = lower gas cost |
| Burn unit | TOKEN (ERC-20) | The deposited token; no intermediate conversion before burn |
| Burn destination | `address(0)` or token-contract `_burn()` | Pinned to whatever the TOKEN contract supports |

**Why pure burn.** The 5% buyback-and-burn bucket in [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) is *also* deflationary, but it is an operator-side flow funded from USDC revenue. The BME burn is a *demand-side* flow funded from client capital. Mixing the two — e.g., routing some BME-burned TOKEN to the treasury or the safety reserve — converts BME into a hybrid pay-and-skim mechanic, which is no longer the mechanism the rest of this ADR's economic argument relies on.

### 3. Coexistence with USDC payment channels

The USDC channel rail and the BME prepay path must coexist at the operator-payment boundary without either being aware of the other's wire-level details.

| Path | Client unit | Operator receives | Settlement contract | Status |
| --- | --- | --- | --- | --- |
| USDC channel rail (default) | USDC | USDC, via `FeeRouter` | `PaymentChannel` ([ADR 003](003-payments.md), [ADR 010](010-multi-token.md)) | Launch |
| BME prepay (opt-in) | TOKEN (burned) | USDC equivalent | `BmePrepay` (or `PaymentChannel` extension; see §5) | Deferred follow-up |

The follow-up pass will pin one of two operator-payment options: **(A)** protocol disburses USDC equivalent from a managed reserve (stable operator cashflow, concentrates oracle risk in the reserve); **(B)** operator receives TOKEN and swaps independently (no reserve, but fragmented per-operator MEV exposure). The choice depends on then-observed TOKEN/USDC liquidity depth and L2 keeper-cost economics.

### 4. Pricing oracle dependency

The TOKEN→USDC pricing for prepay conversion is the load-bearing assumption of this design. Without a robust oracle, the discount is gameable in either direction (clients prepay near a TOKEN-price low and consume at a high; operators receive less USDC equivalent than the prepay was worth at deposit time, etc.).

| Oracle requirement | Adoption-time sizing intent | Notes |
| --- | --- | --- |
| Source pool | [ADR 018](018-liquidity-strategy.md) Balancer V3 80/20 TWAP, or a federated alternative | Same pool already serves the buyback path |
| TWAP window | Long enough to make manipulation cost > expected attacker gain | Deferred to adoption; depends on observed pool depth |
| Multi-source check | Required at adoption | Single-source TWAP is insufficient against a determined attacker with capital |
| Circuit breakers | Required: pause prepay ingress and consumption-time pricing on detected anomaly | Inheriting [ADR 018](018-liquidity-strategy.md)'s per-epoch liquidity caps as a baseline |
| Oracle-failure fallback | Pause BME path; clients fall back to USDC channel rail | Must not block legitimate USDC-rail traffic |

**Oracle-manipulation risk.** Documented and unavoidable as a category. The adoption pass must have a quantitative answer for *"what does it cost to push the TWAP enough to drain `BmePrepay` for a profit?"* before BME goes live. The buyback path ([ADR 018](018-liquidity-strategy.md)) tolerates some per-execution oracle drift because the burn is a one-way action by the protocol; the BME path *cannot* tolerate the same drift because the protocol is paying out USDC against a client-chosen TOKEN deposit. The risk profiles differ.

This ADR does not pin a specific oracle implementation. The hardening work is the post-launch operating-data prerequisite that justifies the Deferred status.

### 5. Smart contract sketch

A new contract `BmePrepay` (or, alternatively, an extension of `PaymentChannel` from [ADR 010](010-multi-token.md)) holds prepaid TOKEN balances per client and processes consumption events.

| Surface | Sketch | Notes |
| --- | --- | --- |
| `deposit(uint256 amount)` | Client deposits TOKEN; balance recorded per address | Discount applies on consumption, not deposit |
| `consume(client, operator, bytesDelivered, vouchers)` | Burns TOKEN, computes USDC-equivalent, disburses to operator (per §3 option) | Authorization model deferred |
| `withdraw(uint256 amount)` | Client withdraws unused TOKEN deposit | No penalty; this is not a lock |
| TOKEN burn | Either `_burn()` on the TOKEN contract or transfer to `address(0)` | Implementation detail |
| Pricing source | Oracle hook (see §4) | Pluggable to allow oracle hardening without redeploy |
| Oracle failure | Revert on `consume`; allow `withdraw` | Liveness preserved for clients |

**Design intent, not specification.** The adoption pass chooses between a separate `BmePrepay` contract (cleaner audit boundary) or a `PaymentChannel` extension (reuses channel lifecycle but doesn't fit the per-consumption oracle hook naturally).

### 6. Demand-side flywheel

The TOKEN burned on consumption is permanent supply reduction. Unlike the [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) 5% buyback-and-burn flow — which scales with network *revenue in USDC*, of which only a small fraction (5%) reaches the burn — the BME burn scales **linearly with network usage**, dollar-for-dollar at the chosen discount. At mature scale this becomes the dominant deflationary mechanism, *if* sufficient client traffic chooses the BME path.

**The "if" is real.** A low-usage network sees negligible BME burn. The mechanism only matters at scale; this is a key honest constraint on the design and is reflected in the Deferred status (deferring lets the launch deployment build the usage base first).

The launch deployment ships on operator-side and delegator-side TOKEN demand only ([ADR 026 §3, §6, §7](026-gauge-boost-tokenomics.md)); BME prepay adds client-side demand on adoption and produces the demand-floor the launch model lacks.

### 7. Defer rationale

This ADR exists to reserve design space and to document launch-time prerequisites. It is *not* a commitment to ship at launch. The deferral is justified by:

- **Pricing oracle is not yet hardened.** [ADR 018](018-liquidity-strategy.md) TWAP is sufficient for a one-way protocol burn flow; it is not sufficient for a two-way prepay/consume flow against client capital. Fixing this requires post-launch operating data.
- **Contract complexity adds non-trivial launch audit scope.** Mainnet already adds `FeeRouter`, `VotingEscrow`, `SafetyReserve`, and the `BuybackBurner` extension or `DelegatorBuyer` ([ADR 026 §10](026-gauge-boost-tokenomics.md)). Adding `BmePrepay` doubles the new-contract surface; deferring is the right frame.
- **Launch focuses on USDC rails.** Client onboarding, payment-channel UX, and the [ADR 026](026-gauge-boost-tokenomics.md) operator economics need launch attention. Adding a parallel TOKEN-payment path before stability fragments the launch story.
- **Low-usage networks see negligible benefit.** The BME flywheel is usage-driven. Adoption is timed to occur after the launch deployment has built non-trivial baseline usage; otherwise the mechanism contributes little while costing meaningful audit and development effort.

---

## Consequences

### Positive

- **Demand-side TOKEN sink that scales with usage.** BME burn grows linearly with network consumption, independent of operator recruitment. This is the demand-floor the launch [ADR 026](026-gauge-boost-tokenomics.md) design lacks.
- **Coexistence preserves USDC rail.** Clients who don't want to touch TOKEN never have to. The default UX is unchanged.
- **Proven mechanism.** Helium's BME has operated at meaningful scale since 2021; the failure modes and tuning levers are documented in production.
- **Deflationary at mature scale.** At sufficient client adoption, BME burn meaningfully exceeds the [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) 5% buyback-and-burn rate per unit of revenue, since 100% of the BME unit is burned vs 5% of the USDC unit.
- **Aligns long-term clients.** Clients who prepay are signaling commitment; the discount captures a fraction of that signal as a TOKEN-price-stability lever.
- **Independent of operator dynamics.** A weakening operator base does not eliminate BME demand; client traffic continues regardless of who serves it.

### Negative

- **Oracle dependency is non-trivial.** The pricing oracle must be hardened to a level above what [ADR 018](018-liquidity-strategy.md) buybacks require. This is the load-bearing engineering work and is the primary reason for the Deferred status.
- **Contract complexity adds audit scope.** Even a minimal `BmePrepay` design adds a new contract or a substantial `PaymentChannel` extension, with consumption-time oracle calls in the hot path.
- **Mechanism only matters at scale.** Low-usage networks see negligible BME burn; deferring mitigates the launch-time mismatch but doesn't eliminate the structural constraint.
- **Fragmented client UX.** Two payment paths to choose between is more complex than one. Documentation, client-software UI, and operator probe-response advertisement all carry incremental complexity.
- **Operator opt-out fragments the network.** Operators that refuse BME traffic create a sub-network distinction. Probe and routing logic must accommodate the BME-vs-USDC dimension on top of the existing per-token allowlist ([ADR 010](010-multi-token.md)).
- **Burn-rate volatility.** BME burn is usage-driven, so it is also usage-volatile. A surge in BME adoption produces a burn surge; a slump produces a burn slump. The buyback path ([ADR 018](018-liquidity-strategy.md)) is steadier per unit of revenue.

### Risks

- **Oracle manipulation.** A determined attacker pushes the TOKEN/USDC TWAP and then triggers a profitable consume/withdraw cycle. Multi-source oracle, longer TWAP windows, per-epoch liquidity caps, and circuit breakers are required at adoption. Failure to harden the oracle invalidates the entire mechanism — this is the single largest risk and is the explicit reason for deferral.
- **Reserve drain (Option A).** If the protocol mints USDC equivalent from a managed reserve, a coordinated BME-prepay surge could drain it. The adoption pass must size the reserve against worst-case adoption rates and have a circuit-breaker (pause new BME deposits, accept consume against existing deposits) ready.
- **Operator MEV exposure (Option B).** If operators receive TOKEN and swap themselves, each operator faces independent MEV exposure on their TOKEN→USDC swap. Aggregate this across many small operators and total MEV leakage may exceed Option A's reserve-management cost.
- **Adoption risk.** Even with a discount, clients may prefer the simpler USDC rail. If BME adoption stays low, the demand-floor argument doesn't materialize. The adoption pass should set adoption-rate KPIs and be prepared to deprecate BME if the data doesn't support it within a defined window.

---

## Launch prerequisites (for future BME adoption)

This ADR must not block launch; it must, however, ensure launch-time design choices do not preclude later BME integration.

- **[ADR 003](003-payments.md) payment-channel design must accommodate the future BME path without breaking changes.** The `FeeRouter` interface ([ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553)) and `PaymentChannel.settleChannel` flow must remain stable when a parallel `BmePrepay` consumption path is added. The `bytesDelivered` accounting is the natural shared input — both rails will produce per-operator byte counters that feed the gauge pool. `FeeRouter.routeSettlement(operator, bytesDelivered, amount)` should be callable by both `PaymentChannel` and a future `BmePrepay` consumption surface.
- **[ADR 018](018-liquidity-strategy.md) TWAP oracle must reach maturity sufficient for prepay pricing.** Launch hardens the buyback-side TWAP; the BME adoption pass extends it to multi-source and adds circuit breakers. The launch implementation should expose oracle-quality metrics (price-deviation alerts, manipulation-cost estimates, depth measurements) that the adoption pass can use to validate readiness.
- **[ADR 010](010-multi-token.md) considerations.** BME does not introduce a *new* token; it changes the unit clients pay in. The [ADR 010](010-multi-token.md) governance-managed token allowlist is unchanged — TOKEN is a first-class allowed payment unit on the BME rail (and only on the BME rail), which means the per-token rate-bounds and channel-id-with-token machinery from [ADR 010](010-multi-token.md) carry over conceptually but apply only inside `BmePrepay`. Launch should not assume "non-USDC tokens are exotic" in any way that would block adding TOKEN as a privileged BME unit later.
- **[ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) `FeeRouter` interface should remain stable.** The router's role in BME is open (does it intermediate the consume-side USDC disbursement, or is `BmePrepay` independent?) and is part of the adoption-time design choice. Either way, launch should not bake assumptions into `FeeRouter` that would force a breaking change to add BME.
- **[ADR 020](020-observability.md) observability surface.** Launch should add metrics for "USDC channel rail revenue per byte" and "USDC channel rail bytes per operator per epoch" with a label dimension that can later be split by payment-rail (USDC vs BME). Avoiding a label-shape change later costs less than retrofitting it under load.
- **[ADR 023](023-poc-production-seams.md) PoC/production seams.** On adoption, `BmePrepay` should plug into the existing wiring layer the same way [ADR 026](026-gauge-boost-tokenomics.md)'s contracts do — selector pattern in the `node` crate, leaf-crate cleanliness preserved. Launch should not introduce node-crate patterns that make adding a second payment rail expensive.

---

## Forward references

- This ADR is itself forward-referenced from [ADR 026 §"Forward references"](026-gauge-boost-tokenomics.md), where it appears as the demand-side counterpart to operator-side and delegator-side TOKEN demand.
- The adoption pass should also revisit [ADR 032 — Bandwidth Futures / Enterprise SLA tier](032-bandwidth-futures-enterprise.md) (deferred). BME prepay and bandwidth-futures both involve client-side TOKEN commitment with a discount; the two designs interact and may share contract surface.

---

## Open questions (resolved at adoption)

- Final discount value (within governable bounds; calibrated against observed TOKEN-price stability and oracle-manipulation cost).
- Choice between `BmePrepay`-as-new-contract and `PaymentChannel`-extension.
- Choice between Option A (protocol-managed USDC reserve) and Option B (operator self-swap), or a hybrid.
- Final pricing-oracle implementation (multi-source structure, TWAP windows, circuit-breaker thresholds).
- Operator advertisement format for "accepts BME traffic" in probe / stream responses ([ADR 005](005-protocol.md) integration).
- Burn timing granularity (per-settlement vs per-epoch) and gas-cost tradeoff on the chosen L2 ([ADR 021](021-l2-chain-selection.md)).
- Adoption-rate KPIs and deprecation criteria.
