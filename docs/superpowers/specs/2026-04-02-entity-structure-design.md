# deCDN Entity Structure Design

## Overview

Three entities: two Swiss (Verein + GmbH subsidiary) and one US (Delaware C-Corp), economically linked through service agreements, token grants, and a parent-subsidiary relationship:

1. **deCDN Verein** (Swiss Association, Zug) — protocol stewardship, treasury, governance transition
2. **deCDN Token GmbH** (Swiss LLC, Zug) — token issuance SPV, wholly owned by Verein
3. **deCDN Labs Inc.** (Delaware C-Corp) — product development, equity fundraising, commercial operations

The Verein owns the GmbH. The Verein and Labs have no ownership link — they are legally separate, economically linked through service agreements. Founding team sits on both sides in different legal capacities. All inter-entity transactions at arm's length.

DAO governance follows **Pattern A (Legal Fiction Separation)**: Verein's legal members remain a small identified group, while TOKEN holders govern permissionlessly via on-chain Governor. The board is contractually obligated to execute DAO decisions.

```
+-----------------------------+
|  deCDN Verein (Swiss Assn)  |
|                             |
|  - Protocol stewardship     |
|  - Treasury (post-TGE)      |
|  - Governance transition    |
|  - Grants program           |
|  - Ecosystem fund           |
+-----------------------------+
       |              |
       | owns         | service agmt
       v              v
+-----------------+   +-----------------------------+
| deCDN Token     |   |  deCDN Labs Inc. (DE C-Corp)|
| GmbH (SPV)     |   |                             |
|                 |   |  - Product development      |
| - Token sale    |   |  - SDKs, tooling, apps      |
| - KYC for sale  |   |  - Equity fundraising       |
| - Sale proceeds |   |  - Commercial partnerships  |
| - Dormant after |   |  - Team employment          |
|   TGE           |   |                             |
+-----------------+   +-----------------------------+
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

### Governance (Phased) — Pattern A: Legal Fiction Separation

The Verein's legal members remain a small, identified group (founders + Swiss director + key contributors). TOKEN holders are NOT formal Verein members under Swiss law. Instead, they govern via on-chain Governor, and the Verein board is contractually obligated to execute DAO decisions.

This avoids the Swiss Verein member-register problem: Swiss law expects Vereins to know their members, but TOKEN holders are pseudonymous on-chain addresses. Pattern A resolves this by keeping legal membership small and identified, while binding the board to follow on-chain governance.

**Verein articles must include:** "The board shall implement any governance proposal that passes the on-chain quorum threshold, unless doing so would violate Swiss law or the Verein's articles."

| Phase | Who Governs | How |
|---|---|---|
| Pre-token (now to launch) | Founding board (Vorstand), 3 members | Majority vote, monthly meetings |
| Token launch to 12 months | Board + TOKEN advisory vote | Board retains veto, token holders vote on grants/parameters |
| Mature (12+ months) | TOKEN holders via on-chain Governor | Board executes DAO decisions as legal obligation, no veto |

**Legal members (identified, KYC'd):** Founders, Swiss director, key contributors — small group with Swiss legal standing.

**TOKEN holders (pseudonymous):** Vote on-chain, no KYC required, no Swiss legal standing as Verein members — governance power is economic and contractual, not statutory.

**Risks and mitigations:**
- Board ignores a DAO vote → economic pressure (reputation damage, token price impact), legal members can replace board at general assembly
- Regulator challenges Pattern A → upgrade to Pattern B (tiered membership with formal "associate member" class for TOKEN holders) without restructuring. Articles should be drafted to support this upgrade path from day one.

### Board Composition (Pre-Token)

- 2-3 founders as board members
- Optional: 1 independent advisor for credibility

**Swiss residency requirement:** Swiss Civil Code Art. 60-79 imposes no residency requirement on Vorstand members. Whether one is needed depends on registration status:

| Registration Status | Residency Requirement | Cost |
|---|---|---|
| Unregistered Verein | None — entire board can be non-resident | $0 |
| Registered + domicile service | Swiss-domiciled representative via law firm or trust company (post-2023 corporate reform) | ~CHF 1-3K/yr |
| Registered + resident director | Swiss-resident board member or service provider | ~CHF 5-8K/yr |

Registration is required only if the Verein operates a commercial enterprise (Art. 61 para. 2 ZGB). A Verein that holds treasury and issues grants is likely not commercial, but many crypto Vereins in Zug voluntarily register for credibility and banking access.

**Recommendation:** Voluntarily register and use a domicile service (~CHF 1-3K/yr) rather than a resident director (~CHF 5-8K/yr). Swiss counsel should confirm whether banking (Sygnum, SEBA) and SRO membership require a resident board member in practice, even if not legally mandated.

### Treasury Management

- **Bank account:** Sygnum or SEBA Bank (Swiss crypto-native), or Hypothekarbank Zug (traditional, crypto-friendly)
- **On-chain treasury:** Gnosis Safe multisig (3-of-5) for holding funds and executing treasury operations. This is a separate treasury multisig, not the ADR 009 emergency multisig; the ADR 009 emergency multisig is scope-limited (pause + content blacklist only) and may reuse the same signer set but with restricted powers
- **Fiat runway:** 12-18 months operating costs in CHF/USD in bank account
- **TOKEN treasury:** Held in multisig, governed by vesting schedules from ADR 004

### Compliance

- **SRO membership:** VQF or PolyReg for AML/KYC compliance (required for token issuance)
- **SRO annual fees:** ~CHF 2,000-5,000 depending on volume
- **AML officer:** Board member or outsourced compliance service
- **FINMA:** No license needed for utility token — file a "no-action" inquiry to confirm classification (~CHF 5-10K legal + CHF 2-5K FINMA admin fees)
- **MiCA:** If selling to EU residents, file a crypto-asset whitepaper. Swiss entities must register with an EU member state authority as there is no direct passporting from Switzerland.

### Token Issuance Path

Token issuance is handled by the **deCDN Token GmbH** (SPV), not the Verein directly. This isolates token sale risk — if a regulator reclassifies the token, liability sits in the GmbH, not the Verein. See Entity 3 section for details.

The Verein holds the TOKEN treasury post-TGE after the GmbH transfers sale proceeds.

### Operational Roles

| Role | Who | Responsibility |
|---|---|---|
| Board president | Founder 1 | Verein governance, signing authority |
| Board member | Founder 2 | Technical oversight, treasury co-signer |
| Domicile representative | Law firm or trust company | Registered address, Swiss-domiciled signatory (if registered) |
| AML officer | Outsourced or board member | KYC process, suspicious activity reporting |
| Legal counsel (Swiss) | Swiss crypto law firm (MME, Lenz & Staehelin, Wenger Vieli) | Formation, FINMA inquiry, ongoing |
| Legal counsel (US securities) | US token securities specialist (Cooley/Fenwick/Latham token teams, Anderson Kill, DLx Law, Lewis Rice) | SAFT drafting, Form D + blue-sky filings, Reg S/Reg D compliance, transfer-restriction review. Engaged by GmbH at formation, not by Labs. |

### Costs

- **Setup:** ~CHF 15-25K (legal + formation)
- **Annual:** ~CHF 10-20K (domicile service, SRO fees, registered office, accounting)

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

Under this design, Labs is allocated up to ~15% TOKEN from the Verein out of the ADR 004 "Team & contributors" bucket (this is a design choice of this spec, not specified in ADR 004 itself), separate from equity. Investors get equity upside in the company AND indirect TOKEN exposure through Labs' token treasury.

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

## Entity 3: deCDN Token GmbH (Token Issuance SPV)

### Legal Basis

Swiss GmbH (Gesellschaft mit beschränkter Haftung), registered in Zug. Wholly owned subsidiary of the deCDN Verein.

Purpose-built as a single-purpose vehicle for token issuance. Isolates token sale risk from the Verein — if a regulator reclassifies TOKEN as a security, liability sits in the GmbH, not the Verein. The Verein's treasury, grants program, and governance role are shielded.

### Formation Requirements

- CHF 20,000 minimum share capital (paid in at formation)
- Verein is sole shareholder (Gesellschafter)
- Registration in Zug Commercial Register
- Articles of association defining single purpose: token issuance and distribution

### Directors

- 1-2 directors, overlapping with Verein board members
- Swiss-domiciled representative required for registration (can share the Verein's domicile service)

### Compliance

- **SRO membership:** Joins VQF or PolyReg (can potentially share membership with Verein, or separate — consult SRO)
- **FINMA no-action inquiry:** Filed by the GmbH specifically, confirming TOKEN utility classification
- **KYC/AML:** All token sale KYC handled by the GmbH, buyer data stored with SRO-compliant provider
- **MiCA:** If selling to EU residents, crypto-asset whitepaper filed by GmbH

### Token Issuance Path

1. GmbH formed 5-6 months before planned TGE (allows time for FINMA response; avoids paying for a dormant entity during early development)
2. SRO membership application
3. FINMA no-action inquiry filed by GmbH
4. KYC infrastructure set up (identity verification for buyers)
5. Public sale via launchpad or directly, under a **Reg S + Reg D 506(c) dual-track structure** (see "US Market Access" below)
6. Listing: DEX immediately, non-US CEX after sufficient liquidity (US CEX listings deferred — see "US Market Access")
7. Sale proceeds transferred to Verein treasury
8. GmbH goes dormant or is dissolved

TOKEN classifies as utility under FINMA guidelines because it has functional utility at launch: staking, governance voting, and fee discounts. **Note:** FINMA utility classification has no bearing on US securities law analysis, which applies the *Howey* test independently. The GmbH must plan for US securities compliance as a separate workstream — see next section.

### US Market Access (Reg S + Reg D 506(c))

Since *SEC v. Telegram* (2020) and *SEC v. Kik* (2020), the SEC has consistently treated primary token sales to US persons as securities offerings under *Howey*, regardless of utility framing. A public ICO to US retail is not viable without S-1 registration (prohibitive) or Reg A+ Tier 2 qualification (6-12 month SEC process, audited financials, ongoing reporting — deferred unless retail access becomes strategic).

The GmbH runs a **dual-track primary sale** to reach US capital legally while preserving a permissionless global offering:

| Track | Buyers | Instrument | Cap | Resale restriction |
|---|---|---|---|---|
| **Reg S** | Non-US persons only | TOKEN (direct sale at TGE) | Unlimited | Distribution compliance period (typically 40 days – 1 year depending on category) |
| **Reg D 506(c)** | US accredited investors, verified | SAFT pre-TGE → TOKEN at TGE | Unlimited | 1-year Rule 144 lockup from delivery |

**Why SAFTs for the US tranche:** The Simple Agreement for Future Tokens is the standard instrument for pre-TGE US accredited sales. It is a security at signing (purchase contract), delivers TOKEN at TGE, and keeps the securities analysis confined to the pre-TGE investor group. Post-lockup secondary trading of TOKEN itself is a separate analysis — by then the project argues TOKEN has matured into a functional utility network (the *Hinman* "sufficient decentralization" argument, weakened but not dead).

**Requirements for the GmbH:**

1. **Accreditation verification** — third-party vendor (VerifyInvestor, Parallel Markets, or CPA/attorney letter). Self-certification is not sufficient under 506(c).
2. **Form D filing** — filed with the SEC within 15 days of first US sale. State blue-sky notice filings in each state where a US purchaser resides (typically $100-500 per state).
3. **Geoblocking for the Reg S tranche** — IP-based blocking of US visitors, KYC rejection of US documents, purchase-page attestation. Sham geoblocks have been pierced by courts (*SEC v. LBRY*) — the block must be genuine.
4. **Transfer restrictions during lockup** — token contract or distribution wrapper must enforce the 1-year US lockup. Options: (a) legended SAFTs that deliver to a KYC'd allowlist contract for the lockup period, (b) vesting contract with US-address flags, (c) off-chain contractual covenant with economic penalties. Option (a) is cleanest.
5. **No general solicitation into the Reg S tranche from the US** — marketing channels must be segregated or carry clear geographic disclaimers.

**What Labs must NOT do:** Labs (Delaware C-Corp) must not conduct, market, or take payment for any token sale. Labs may receive its ADR 004 TOKEN allocation as compensation for services under the existing grant agreement, but any involvement in the primary sale flow collapses the GmbH SPV shield, destroys the Reg S position (the offering becomes US-originated), and gives the SEC maximum jurisdiction. All sale marketing, KYC, payment processing, and token distribution must flow through the GmbH.

**Airdrops:** Airdrops to US wallets are not a loophole. The SEC's 2024 enforcement posture (Wells notices to multiple projects) treats marketing-driven airdrops as unregistered offerings when recipients take actions of value (signup, staking, referral). Any retroactive or promotional airdrop program must exclude US addresses by the same mechanisms as the Reg S tranche, or be analyzed separately by US counsel.

**Secondary market:** Post-lockup, the GmbH's responsibility ends. US CEX listings (Coinbase, Kraken) require those venues' own securities review and are unlikely pre-maturity — plan for non-US CEXes (KuCoin, OKX, Bybit, Gate) and DEX-only US access for the first 12-24 months post-TGE. The GmbH should not list on or actively facilitate US CEX access during the lockup.

**US securities counsel:** A US token securities specialist must be engaged alongside Swiss counsel. Swiss counsel cannot opine on US securities law, and US startup counsel (Labs' firm) is generally not the right fit — token-specific securities work lives with specialists. Candidates: Cooley / Fenwick / Latham token teams, Anderson Kill, DLx Law, Lewis Rice, Morrison Cohen. Budget **$40-80K** for SAFT templates, Form D + blue-sky filings, transfer-restriction review, and issuance opinion.

### Lifecycle

| Phase | Status |
|---|---|
| Pre-TGE development | GmbH does not exist yet |
| 5-6 months before TGE | GmbH formed, SRO joined, FINMA inquiry filed (allow 8-16 weeks for response) |
| TGE | GmbH conducts token sale, handles KYC |
| Post-TGE (distribution complete) | Sale proceeds transferred to Verein |
| Post-distribution | GmbH goes dormant (~CHF 1-2K/yr) or dissolved (zero ongoing cost) |

### Costs

- **Formation:** ~CHF 5-10K (legal + filing) + CHF 20,000 (share capital)
- **Annual (active):** ~CHF 3-5K (SRO fees, registered office, accounting)
- **Annual (dormant):** ~CHF 1-2K (registered office, minimal accounting)
- **Dissolution:** ~CHF 2-5K (one-time)

---

## Inter-Entity Relationships & Agreements

### 0. Verein ↔ GmbH (Parent-Subsidiary)

- Verein is sole shareholder of GmbH
- GmbH purpose is limited to token issuance and distribution
- GmbH directors appointed by Verein board
- Sale proceeds flow from GmbH to Verein after TGE (via capital contribution, dividend, or intercompany loan — structure per Swiss tax counsel)
- GmbH goes dormant or is dissolved after distribution is complete

### 1. Protocol Development Services Agreement

- Labs performs protocol development (Rust crates, smart contracts, testing)
- Verein compensates Labs via quarterly grants in USDC + TOKEN
- Deliverables defined per quarter — milestone-based funding, not open-ended
- Either party can terminate with 90-day notice
- Establishes arm's length relationship via documented transfer pricing (e.g., Cost Plus model) to satisfy both IRS and Swiss cantonal tax requirements, protects Verein's non-profit status, gives Labs predictable revenue

### 2. TOKEN Grant Agreement

- Per ADR 004, 15% of TOKEN supply (150M tokens) is allocated to "Team & contributors"; this spec proposes granting that allocation to Labs (to be formalized in a companion ADR)
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

- GmbH SPV handles KYC data for token sale — stored with SRO-compliant provider, not shared with Verein or Labs
- Labs handles user data for its products — standard privacy policy, GDPR if serving EU users
- No user data flows between entities

### Financial Flows

```
                       TOKEN grant (15%, vesting)
               +--------------------------------------+
               |                                      v
   +----------------+                    +--------------------+
   |  deCDN Verein  |--------------------|  deCDN Labs Inc.   |
   |                |   USDC grants      |                    |
   |  TOKEN treasury|   (quarterly)      |  Equity + TOKEN    |
   |  Protocol fees |                    |  Product revenue   |
   +----------------+                    |  VC investment     |
         |    ^                          +--------------------+
         |    | sale proceeds                      |
   owns  |    | (post-TGE)                         v
         v    |                          Employee salaries
   +-----------------+                   Infrastructure costs
   | deCDN Token     |                   Product development
   | GmbH (SPV)     |                   Business development
   |                 |
   | Token sale      |
   | KYC for buyers  |
   +-----------------+
               |
               v
   +----------------+                    +--------------------+
   | Verein spends: |                    | Labs spends:       |
   | Ecosystem grants|                   | Employee salaries  |
   | Bug bounties   |                    | Infrastructure     |
   | Audits         |                    | Product dev        |
   | Community      |                    | Business dev       |
   +----------------+                    +--------------------+
```

### Conflict of Interest Management

- Founders sit on Verein board AND lead Labs — normal but must be managed
- Verein board votes on Labs grants: founders recuse themselves. Board must have enough independent members to form quorum during recusals — either expand to 4-5 members (adding 2-3 independent advisors) or define a reduced quorum for conflict-of-interest votes in the articles
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
| Public token sale (Reg S, non-US) | Yes | GmbH SPV via SRO membership or licensed launchpad; US persons blocked at IP, KYC, and attestation |
| Private/seed round (SAFT, US accredited) | Yes (Rule 506(c) third-party accreditation verification) | GmbH SPV + US securities counsel; Form D filed within 15 days of first sale |
| Equity round (C-Corp) | Standard investor verification | Labs + VC's own compliance |
| Verein legal membership | Yes (small group, trivial) | Verein board |
| DAO governance voting | No | N/A — permissionless, Pattern A |
| Using the deCDN protocol | No | N/A — permissionless |
| Running a node | No (just stake TOKEN) | N/A — permissionless |
| Secondary market trading | No | Exchange handles (CEX) or nobody (DEX) |

KYC is a token-sale problem, not a protocol or governance problem. The GmbH SPV handles KYC for the primary sale event. The Verein never touches buyer personal data. On-chain governance participation (voting, proposing) requires no KYC under Pattern A — TOKEN holders are not formal Verein members.

Secondary market trading is not the Verein's or GmbH's responsibility. CEXes handle their own KYC; DEX trading is permissionless.

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
- Draft Articles of Association (include Pattern A governance clauses + Pattern B upgrade path)
- Engage Swiss-resident director service
- Hold founding assembly
- Register in Zug Commercial Register
- Open Swiss bank account

### Phase 3: Inter-Entity Agreements (Weeks 6-10)
- Draft and sign service agreement, TOKEN grant agreement, trademark license
- Both entities' counsel review (Swiss counsel for Verein, US counsel for Labs)
- Set up multisig wallets for both entities

### Phase 4: Verein Compliance (Weeks 8-14)
- Verein joins SRO (VQF application takes 4-8 weeks)
- Labs 409A valuation if issuing options

### Phase 5: GmbH Formation (5-6 months before TGE)
- Form deCDN Token GmbH as Verein subsidiary
- GmbH joins SRO (or shares Verein's membership)
- FINMA no-action inquiry submitted by GmbH (response in 8-16 weeks; utility/governance hybrids may take longer)
- KYC infrastructure set up
- **GmbH is deferred until needed** — avoids paying for a dormant entity during early development

### Phase 6: Token Launch (Months 4-6+)
- Token smart contracts audited
- GmbH conducts public sale with KYC
- DEX liquidity provision
- Sale proceeds transferred to Verein treasury
- Governance contracts deployed (OpenZeppelin Governor per ADR 009)
- GmbH goes dormant or begins dissolution

```
Week:  1    2    4    6    8    10   12   14     TGE-6mo  TGE-3mo  TGE
       |----|----|----|----|----|----|----|----|---...---|--------|---->
Labs   xxxxxx done: incorporated, banking, fundraising ready
Verein xxxxxxxxxxxxxxxxxx done: formed, registered
Agreements        xxxxxxxxxxxx done: signed
Verein SRO              xxxxxxxxxxxxxxxx done
GmbH                                          xxxxxxxxxxxxxxxxxx done
FINMA                                              xxxxxxxxxxxxxxxxxx response
Token                                                                 xxx> launch
```

### Cost Summary

**Year 1 (pre-TGE, GmbH not yet formed):**

| Item | Cost |
|---|---|
| Labs formation + legal | $3-5K |
| Verein formation + legal | CHF 20-30K |
| Verein domicile service (annual) | CHF 1-3K |
| Verein SRO membership (annual) | CHF 2-5K |
| Verein registered office | CHF 3-5K |
| Labs registered agent + franchise tax | $1-2K |
| Inter-entity agreements (legal, cross-border Swiss + US counsel) | $10-20K |
| **Total Year 1 (pre-TGE)** | **~$41-70K** |

**TGE year (when GmbH is formed):**

| Item | Cost |
|---|---|
| GmbH formation + legal | CHF 5-10K |
| GmbH share capital | CHF 20,000 (recoverable on dissolution) |
| GmbH SRO membership | CHF 2-5K |
| FINMA no-action inquiry (legal + FINMA admin fees) | CHF 7-15K |
| GmbH KYC infrastructure | CHF 3-5K |
| US securities counsel (SAFT, Form D, blue-sky, opinion) | $40-80K |
| Accreditation verification vendor (per-investor fees) | $2-10K (volume-dependent) |
| **Additional TGE-year cost** | **~CHF 37-55K + ~$42-90K** |

**Annual (Year 2+, post-TGE):**

| Item | Cost |
|---|---|
| Verein (domicile + SRO + office + accounting) | CHF 10-20K |
| GmbH (dormant: registered office + minimal accounting) | CHF 1-2K |
| Labs (agent + tax + bookkeeping) | $5-10K |
| Legal retainer (all entities) | $10-20K |
| **Total annual** | **~$26-52K** |

Note: GmbH share capital (CHF 20K) is recoverable if the GmbH is dissolved after TGE. Dissolution costs ~CHF 2-5K.

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

### BVI SPV for Token Issuance (instead of Swiss GmbH)
- Cheaper (~$1-2K vs CHF 25-30K), no corporate tax, fast formation
- No regulatory clarity on tokens (gray zone, legal opinion only), no MiCA passporting for EU
- Undermines the Swiss credibility story — routing token sale through BVI looks like regulatory arbitrage
- FINMA may view the Verein as economic issuer anyway, thinning the SPV shield
- Rejected: cost savings (~CHF 20K) not worth losing regulatory clarity and institutional credibility

### Marshall Islands / UAE ADGM DAO LLC for Governance (instead of Swiss Verein)
- Both jurisdictions legally recognize TOKEN holders as governing members — no legal fiction needed
- Marshall Islands: untested in courts, no banking, low institutional credibility
- UAE/ADGM: newer framework (2023), growing credibility, good banking, purpose-built for DAOs
- Would require three jurisdictions (DAO LLC + Swiss GmbH for token + Delaware C-Corp) — more complexity
- Rejected in favor of Pattern A legal fiction with Swiss Verein: simpler two-jurisdiction structure (Switzerland + Delaware), industry-standard approach, upgrade path to Pattern B if needed

### Reg A+ Tier 2 for US Retail Token Sale
- Only exemption that reaches US non-accredited retail (up to $75M/yr)
- Precedents: INX (2020), Exodus (2021) — both took 12+ months for SEC qualification
- Requires audited financials, ongoing reporting (semi-annual + material event), SEC qualification process
- Cost: $500K-1M+ in legal, audit, and filing fees before first dollar raised
- Rejected for initial TGE: disproportionate cost and timeline for a protocol launch. Revisit if US retail access becomes strategic post-maturity.

### Full S-1 Registration of TOKEN
- Only path to unrestricted US public offering
- No crypto project has completed this; treated as prohibitive by every practitioner
- Rejected: not a realistic option in 2026.

### Skip US Entirely (Reg S Only)
- Simplest compliance posture — geoblock US, no Reg D, no Form D, no US counsel
- Forgoes US accredited capital (a meaningful fraction of crypto-native LPs and funds are US-domiciled)
- US funds can still participate via non-US feeder vehicles, but this pushes compliance burden onto investors and shrinks the addressable pool
- Considered but rejected: the marginal cost of adding a Reg D 506(c) SAFT tranche (~$40-80K) is small relative to the US accredited capital it unlocks.

### Pattern C: Full KYC on Governance Participants
- Every voting address linked to verified identity, governance is permissioned
- Fully compliant, zero legal risk
- Kills permissionless governance — most TOKEN holders won't KYC just to vote, participation drops to near zero
- Rejected: defeats the purpose of decentralization
