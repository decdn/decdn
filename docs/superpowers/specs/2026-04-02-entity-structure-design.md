# deCDN Entity Structure Design

## Overview

Two legally independent entities, economically linked through service agreements and token grants:

1. **deCDN Verein** (Swiss Association, Zug) — protocol stewardship, TOKEN issuance, treasury, governance transition
2. **deCDN Labs Inc.** (Delaware C-Corp) — product development, equity fundraising, commercial operations

No ownership link between entities. Founding team sits on both sides in different legal capacities. All inter-entity transactions at arm's length.

```
+-----------------------------+       +-----------------------------+
|  deCDN Verein (Swiss Assn)  |       |  deCDN Labs Inc. (DE C-Corp)|
|                             |       |                             |
|  - Protocol stewardship     |       |  - Product development      |
|  - TOKEN issuance & treasury|<----->|  - SDKs, tooling, apps      |
|  - Governance transition    |service |  - Equity fundraising       |
|  - Grants program           |  agmt |  - Commercial partnerships  |
|  - Ecosystem fund           |       |  - Team employment          |
+-----------------------------+       +-----------------------------+
```

---

## Entity 1: deCDN Verein (Swiss Association)

### Legal Basis

Verein (Association) under Swiss Civil Code Art. 60-79, registered in Zug.

Verein chosen over Stiftung (Foundation) because:
- Member-governed — natural fit for DAO transition (members = TOKEN holders)
- Stiftung is board-governed with FESA regulatory oversight — harder to decentralize
- Cheaper and faster to set up

### Formation Requirements

- Minimum 2 founding members (from core team)
- Articles of Association (Statuten) defining purpose, membership, governance
- Founding assembly minutes
- Registration in Zug Commercial Register
- No minimum capital requirement

### Governance (Phased)

| Phase | Who Governs | How |
|---|---|---|
| Pre-token (now to launch) | Founding board (Vorstand), 3 members | Majority vote, monthly meetings |
| Token launch to 12 months | Board + TOKEN advisory vote | Board retains veto, token holders vote on grants/parameters |
| Mature (12+ months) | TOKEN holders via on-chain Governor | Board becomes executor of DAO decisions, no veto |

### Board Composition (Pre-Token)

- 2-3 founders as board members
- 1 Swiss-resident director (service provider, ~$5-8K/yr) — required for Zug registration
- Optional: 1 independent advisor for credibility

### Treasury Management

- **Bank account:** Sygnum or SEBA Bank (Swiss crypto-native), or Hypothekarbank Zug (traditional, crypto-friendly)
- **On-chain:** Gnosis Safe multisig (3-of-5), aligned with ADR 009 emergency multisig design
- **Fiat runway:** 12-18 months operating costs in CHF/USD in bank account
- **TOKEN treasury:** Held in multisig, governed by vesting schedules from ADR 004

### Compliance

- **SRO membership:** VQF or PolyReg for AML/KYC compliance (required for token issuance)
- **SRO annual fees:** ~CHF 2,000-5,000 depending on volume
- **AML officer:** Board member or outsourced compliance service
- **FINMA:** No license needed for utility token — file a "no-action" inquiry to confirm classification (~CHF 5-10K legal cost)
- **MiCA:** If selling to EU residents, file a crypto-asset whitepaper. Swiss entities can passport via bilateral agreements or register with an EU member state.

### Token Issuance Path

1. FINMA no-action letter confirming utility token classification
2. KYC/AML via SRO-compliant process (identity verification for buyers)
3. Public sale via launchpad or directly, with US persons excluded or Reg D only
4. Listing: DEX immediately, CEX after sufficient liquidity

TOKEN classifies as utility under FINMA guidelines because it has functional utility at launch: staking, governance voting, and fee discounts.

### Operational Roles

| Role | Who | Responsibility |
|---|---|---|
| Board president | Founder 1 | Verein governance, signing authority |
| Board member | Founder 2 | Technical oversight, treasury co-signer |
| Swiss director | Service provider | Local compliance, registered address |
| AML officer | Outsourced or board member | KYC process, suspicious activity reporting |
| Legal counsel | Swiss crypto law firm (MME, Lenz & Staehelin, Wenger Vieli) | Formation, FINMA inquiry, ongoing |

### Costs

- **Setup:** ~CHF 20-30K (legal + formation + FINMA inquiry)
- **Annual:** ~CHF 15-25K (director, SRO fees, registered office, accounting)

---

## Entity 2: deCDN Labs Inc. (Delaware C-Corp)

### Legal Basis

C-Corporation incorporated in Delaware, qualified to do business in team's state(s).

C-Corp chosen over LLC because:
- VCs overwhelmingly require C-Corp (preferred stock, clean cap table, standard SAFE/Series A docs)
- QSBS tax exemption — up to $10M capital gains exclusion per founder after 5 years
- Clean path from 2 founders to 100+ employees without restructuring

### Formation Requirements

- Certificate of Incorporation filed with Delaware Division of Corporations
- Bylaws, board consent, stock issuance
- Registered agent in Delaware (~$100-300/yr)
- Foreign qualification in state(s) where team physically works
- EIN (tax ID) from IRS

### Equity Structure

| Share Class | Allocation | Notes |
|---|---|---|
| Common stock (founders) | ~60-70% at founding | 4-year vesting, 1-year cliff, double-trigger acceleration |
| Common stock (employee pool) | ~15-20% | ESOP for future hires |
| Preferred stock | Reserved for investors | Issued at priced rounds |

Labs also receives ~15% TOKEN allocation from the Verein (per ADR 004 team allocation), separate from equity. Investors get equity upside in the company AND indirect TOKEN exposure through Labs' token treasury.

### Fundraising Path

| Stage | Instrument | Typical Range | Notes |
|---|---|---|---|
| Pre-seed | SAFE (post-money) | $500K-2M | YC-standard SAFE, fast close |
| Seed | SAFE or priced round | $2-5M | Depending on traction |
| Series A | Priced preferred | $8-20M | Requires product/revenue metrics |

### Governance

- Board: 1-3 founders initially, add investor board seats at Series A
- Protective provisions: Standard — investors get veto on sale, new equity issuance, debt above threshold
- Founder control: Maintain board majority through Series A if possible

### Banking & Treasury

- **Primary bank:** Mercury or Brex (crypto-friendly, startup-focused)
- **Payroll:** Gusto, Deel, or Remote (if distributed team)
- **Corporate card:** Brex or Ramp
- **TOKEN holdings:** Separate multisig wallet, not commingled with operating funds

### Compliance

- **Delaware franchise tax:** ~$400-2,000/yr (depends on shares authorized)
- **State taxes:** Income tax in state(s) where team works
- **Federal taxes:** C-Corp rate 21%, but startups rarely pay in early years (losses carry forward)
- **409A valuation:** Required before granting stock options, ~$2-5K via Carta or Pulley
- **83(b) elections:** Founders MUST file within 30 days of stock grant (critical tax optimization)

### Operational Roles

| Role | Who | Responsibility |
|---|---|---|
| CEO | Founder 1 | Company direction, fundraising, Verein liaison |
| CTO | Founder 2 | Protocol dev, technical architecture |
| CFO / Finance | Outsourced (Pilot, Kruze) | Bookkeeping, tax filings, 409A coordination |
| Legal counsel | US startup law firm (Cooley, Fenwick, Goodwin, or solo practitioners for early stage) | Formation, SAFEs, employment, IP assignment |

### IP Management

- All contributors sign CIIA (Confidential Information and Invention Assignment)
- Open-source protocol code: owned by Verein, licensed under permissive license (MIT/Apache 2.0)
- Proprietary tooling/apps: owned by Labs
- Clear IP boundary = clean story for both entities

### Costs

- **Setup:** ~$3-5K (legal + filing + registered agent)
- **Annual:** ~$5-10K (franchise tax, registered agent, bookkeeping, state compliance)

---

## Inter-Entity Relationships & Agreements

### 1. Protocol Development Services Agreement

- Labs performs protocol development (Rust crates, smart contracts, testing)
- Verein compensates Labs via quarterly grants in USDC + TOKEN
- Deliverables defined per quarter — milestone-based funding, not open-ended
- Either party can terminate with 90-day notice
- Establishes arm's length relationship, protects Verein's non-profit status, gives Labs predictable revenue

### 2. TOKEN Grant Agreement

- Verein grants Labs 15% of TOKEN supply (150M tokens per ADR 004)
- 4-year vesting, 1-year cliff, monthly thereafter
- Lockup: 6-12 months post-TGE before any sales
- Labs can distribute to employees via sub-grants (subject to Labs board approval)
- Clawback: unvested tokens return to Verein if service agreement terminates

### 3. Trademark License Agreement

- Verein owns the "deCDN" name, logo, brand assets
- Labs gets a royalty-free, non-exclusive license to use the brand
- Other ecosystem builders can also get brand licenses
- Verein can revoke if Labs acts against protocol interests (extreme case)

### 4. Data & Privacy Boundaries

- Verein handles KYC data for token sale — stored with SRO-compliant provider, not shared with Labs
- Labs handles user data for its products — standard privacy policy, GDPR if serving EU users
- No user data flows between entities

### Financial Flows

```
                    TOKEN grant (15%, vesting)
            +--------------------------------------+
            |                                      v
   +----------------+                +--------------------+
   |  deCDN Verein  |                |  deCDN Labs Inc.   |
   |                |----------------|                    |
   |  TOKEN treasury|  USDC grants   |  Equity + TOKEN    |
   |  Protocol fees |  (quarterly)   |  Product revenue   |
   |  Token sale    |                |  VC investment     |
   +----------------+                +--------------------+
            |                                  |
            v                                  v
   Ecosystem grants                   Employee salaries
   Bug bounties                       Infrastructure costs
   Audits                             Product development
   Community programs                 Business development
```

### Conflict of Interest Management

- Founders sit on Verein board AND lead Labs — normal but must be managed
- Verein board votes on Labs grants: founders recuse themselves, Swiss director + independent advisor vote
- All inter-entity transactions documented and at market rates
- Annual disclosure of cross-entity relationships in Verein's financial statements

### Divergence Scenarios

- **Labs pivots away from deCDN:** Service agreement terminates, unvested TOKEN returns, brand license revoked. Labs keeps equity, products, and vested TOKEN.
- **Verein replaces Labs:** New service agreement with new contributor. Labs continues independently with vested TOKEN.
- Clean separation means neither entity can hold the other hostage.

---

## KYC Requirements

| Activity | KYC Required? | Who Handles It? |
|----------|--------------|-----------------|
| Public token sale | Yes | Verein via SRO membership or licensed launchpad |
| Private/seed round (TOKEN) | Yes (accredited investor checks) | Verein + legal counsel |
| Equity round (C-Corp) | Standard investor verification | Labs + VC's own compliance |
| Using the deCDN protocol | No | N/A — permissionless |
| Running a node | No (just stake TOKEN) | N/A — permissionless |

KYC is a token-sale problem, not a protocol problem. The protocol architecture is permissionless by design. The Verein handles KYC for sale events via SRO-compliant processes.

---

## Timeline & Sequencing

### Phase 1: Delaware C-Corp (Weeks 1-2)
- Incorporate Labs — faster, cheaper, needed immediately
- Open bank account (Mercury)
- File 83(b) elections for founder stock (30-day hard deadline)
- Sign CIIAs for all contributors
- Can start fundraising (SAFEs) immediately

### Phase 2: Swiss Verein Formation (Weeks 1-8, parallel with Phase 1)
- Engage Swiss crypto law firm
- Draft Articles of Association
- Engage Swiss-resident director service
- Hold founding assembly
- Register in Zug Commercial Register
- Open Swiss bank account

### Phase 3: Inter-Entity Agreements (Weeks 6-10)
- Draft and sign service agreement, TOKEN grant agreement, trademark license
- Both entities' counsel review
- Set up multisig wallets for both entities

### Phase 4: Compliance Setup (Weeks 8-14)
- Verein joins SRO (VQF application takes 4-8 weeks)
- FINMA no-action inquiry submitted (response in 4-12 weeks)
- Labs 409A valuation if issuing options
- Do not issue TOKEN before SRO membership and FINMA confirmation

### Phase 5: Token Launch (Months 4-6+)
- KYC infrastructure ready
- Token smart contracts audited
- Public sale or initial distribution
- DEX liquidity provision
- Governance contracts deployed (OpenZeppelin Governor per ADR 009)

```
Week:  1    2    4    6    8    10   12   14   16+
       |----|----|----|----|----|----|----|----|---->
Labs   xxxxxx done: incorporated, banking, fundraising ready
Verein xxxxxxxxxxxxxxxxxx done: formed, registered
Agreements        xxxxxxxxxxxx done: signed
Compliance              xxxxxxxxxxxxxxxx done: SRO + FINMA
Token                                    xxxxxxxx> launch
```

### Cost Summary

**Year 1:**

| Item | Cost |
|---|---|
| Labs formation + legal | $3-5K |
| Verein formation + legal | CHF 20-30K |
| FINMA no-action inquiry | CHF 5-10K |
| Swiss director (annual) | CHF 5-8K |
| SRO membership (annual) | CHF 2-5K |
| Verein registered office | CHF 3-5K |
| Labs registered agent + franchise tax | $1-2K |
| Inter-entity agreements (legal) | $5-10K |
| **Total Year 1** | **~$45-70K** |

**Annual (Year 2+):**

| Item | Cost |
|---|---|
| Verein (director + SRO + office + accounting) | CHF 15-25K |
| Labs (agent + tax + bookkeeping) | $5-10K |
| Legal retainer (both entities) | $10-20K |
| **Total annual** | **~$30-55K** |

---

## Alternatives Considered

### Cayman Foundation + Delaware C-Corp
- Lower ongoing cost, most popular pattern in crypto
- Weaker token classification clarity than Switzerland, harder banking, no MiCA passporting
- Rejected in favor of Swiss regulatory clarity for utility token issuance

### Panama Private Foundation + Singapore Pte. Ltd.
- Cheapest total cost, excellent banking in Singapore
- Zero regulatory clarity in Panama, less institutional credibility for Western VCs
- Rejected due to regulatory risk and VC perception concerns
