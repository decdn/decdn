# Bond-Vocabulary Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the operator-collateral surface from "stake" to "bond" across `CapacityBond.sol`, `StakeMath.sol`, their tests, deploy scripts, the `incentive` crate's `sol!` mirror, and residual ADR 026 wording — with zero change to mechanics, bounds, or storage layout.

**Architecture:** This is a pure identifier/vocabulary rename, not a behavior change. The existing test suite is the safety net: every assertion is renamed in lockstep with the surface, so the suite must stay green throughout. Verification is build-success + green tests + the `--sizes` gate unchanged + a residual `stake` grep returning only intentional matches. No new tests are written (no new behavior); instead each rename task ends by re-running the existing suite.

**Tech Stack:** Solidity (Foundry/forge), Rust (cargo/nextest, `alloy::sol!`), Markdown ADRs.

**Branch:** `rename/stake-to-bond` (already created; design spec already committed at `docs/superpowers/specs/2026-06-01-bond-vocabulary-rename-design.md`).

**Canonical rename map (apply consistently everywhere):**

| Old identifier | New identifier |
|---|---|
| `getStakeMultiple` | `getBondMultiple` |
| `requestUnstake` | `requestUnbond` |
| `_reduceStakeAtTier` | `_reduceBondAtTier` |
| `_enforceMinStakeBounds` | `_enforceMinBondBounds` |
| `activeStake` | `activeBond` |
| `setMinStake` | `setMinBond` |
| `MinStakeUpdated` | `MinBondUpdated` |
| `InsufficientStake` | `InsufficientBond` |
| `StakeBelowMinimum` | `BondBelowMinimum` |
| `remainingStake` | `remainingBond` |
| `newActiveStake` | `newActiveBond` |
| `oldMinStake` | `oldMinBond` |
| `newMinStake` | `newMinBond` |
| `minStake_` | `minBond_` |
| `stakeSlash` | `bondSlash` |
| `stakePortion` | `bondPortion` |
| `stakeOf` | `bondOf` |
| `minStake` | `minBond` |
| `Staked` | `Bonded` |
| `Unstaked` | `Unbonded` |
| `unstake` | `unbond` |
| `stake` (fn, params, locals, prose) | `bond` |
| `StakeMath` | `BondMath` |
| `StakeMath.sol` (file) | `BondMath.sol` |
| `MIN_STAKE` (env var) | `MIN_BOND` |
| `DEFAULT_MIN_STAKE` | `DEFAULT_MIN_BOND` |

**Ordering rule (critical):** When doing find/replace, **rename longest/most-specific identifiers first** (e.g. `getStakeMultiple`, `activeStake`, `requestUnstake`, `setMinStake`, `MinStakeUpdated`) **before** the bare `stake`/`unstake` tokens. Otherwise a global `stake → bond` pass corrupts `activeStake → active**bond**` mid-stream or mangles `unstake`. Use the editor's exact-string replace per identifier (`replace_all` on the precise token), not a blind `sed s/stake/bond/g`.

**Left untouched (already bond vocabulary):** `unbondingOf`, `unbondingPeriod`, `UnbondingRequested`, `UnbondingPeriodUpdated`, `firstBondedAt`, `_firstBondedAt`.

**Out of scope:** `SlashAppeal`/`SlashJudge`/`ContentBlacklist` appeal & challenge bonds (distinct, correctly named); `StakerSet`/`AllStaked` in `crates/node/tests/dht_loopback.rs` (node-membership test mock, not the `CapacityBond` ABI).

---

## Task 1: Establish green baseline + inventory

**Files:** none modified — this records the "before" state so we can prove the rename is behavior-neutral.

- [ ] **Step 1: Confirm contracts build and test green before any change**

Run:

```bash
cd contracts && forge build --sizes 2>&1 | tail -20 && forge test 2>&1 | tail -20
```

Expected: build succeeds; all tests PASS. Record the reported `CapacityBond` runtime/init size from the `--sizes` table (we compare against it at the end).

- [ ] **Step 2: Confirm Rust builds green before any change**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && cargo build -p decdn-incentive && cargo nextest run -p decdn-incentive 2>&1 | tail -15
```

Expected: build + tests PASS.

- [ ] **Step 3: Snapshot the full stake-occurrence inventory**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git grep -nE "[Ss]tak(e|ing|ed)|StakeMath|MIN_STAKE" -- contracts/src/CapacityBond.sol contracts/src/StakeMath.sol contracts/test/CapacityBond.t.sol contracts/script/ crates/incentive/ adr/026-tokenomics.md adr/028-slashing-appeals.md adr/036-served-bytes-voting-weight.md | wc -l
```

Expected: prints a number (the occurrence count). This is the worklist size; the final task drives it to "only intentional matches".

No commit (read-only baseline).

---

## Task 2: Rename `CapacityBond.sol` surface

**Files:**

- Modify: `contracts/src/CapacityBond.sol`

- [ ] **Step 1: Apply the rename map to all identifiers, longest-first**

In `contracts/src/CapacityBond.sol`, replace each identifier per the canonical map, processing the longest tokens first (see Ordering rule). Concretely, in this order, do an exact `replace_all` for each:

1. `getStakeMultiple` → `getBondMultiple`
2. `_reduceStakeAtTier` → `_reduceBondAtTier`
3. `_enforceMinStakeBounds` → `_enforceMinBondBounds`
4. `requestUnstake` → `requestUnbond`
5. `newActiveStake` → `newActiveBond`
6. `remainingStake` → `remainingBond`
7. `activeStake` → `activeBond`
8. `MinStakeUpdated` → `MinBondUpdated`
9. `setMinStake` → `setMinBond`
10. `InsufficientStake` → `InsufficientBond`
11. `StakeBelowMinimum` → `BondBelowMinimum`
12. `oldMinStake` → `oldMinBond`
13. `newMinStake` → `newMinBond`
14. `minStake_` → `minBond_`
15. `minStake` → `minBond`  *(now safe — all longer `*MinStake*`/`minStake_` variants already renamed)*
16. `stakeSlash` → `bondSlash`
17. `stakePortion` → `bondPortion`
18. `stakeOf` → `bondOf`
19. `StakeMath` → `BondMath` (both the `import { StakeMath } from "./StakeMath.sol"` symbol and the path `"./StakeMath.sol"` → `"./BondMath.sol"`, and the call site `StakeMath.reduceAtTier` → `BondMath.reduceAtTier`)
20. `Unstaked` → `Unbonded`
21. `Staked` → `Bonded`
22. `unstake` → `unbond`  *(the `unstake()` function name; `requestUnstake` already handled in step 4)*
23. Bare `stake` → `bond`: the `function stake(` declaration, the `stake` param/local names, and prose/NatSpec mentions of "stake"/"staking". **Inspect each remaining `stake` match individually** — do NOT touch any `unbond*`/`firstBonded*` tokens, and leave the words inside the out-of-scope list alone. Comments like "Storage — staking" → "Storage — bonding", "// Staking" → "// Bonding", "set on first successful `stake`" → "set on first successful `bond`".

Note the header comment at line ~31: `Renamed from StakingRegistry per ADR 026 v2.2 vocabulary. The stake / unstake / node-registry primitives are unchanged` → update to `The bond / unbond / node-registry primitives are unchanged` (keep the `StakingRegistry` historical reference — it names a prior contract, not current surface).

- [ ] **Step 2: Verify no unintended stake tokens remain in the contract**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git grep -nE "[Ss]tak(e|ing|ed)" -- contracts/src/CapacityBond.sol
```

Expected: only the intentional `StakingRegistry` historical mention (header comment). Anything else is a miss — fix it.

- [ ] **Step 3: Commit (contract only — won't build until BondMath exists; that's fine for an isolated commit, OR defer commit to after Task 3)**

Defer the commit to the end of Task 3 so the tree compiles. (No commit here.)

---

## Task 3: Rename `StakeMath.sol` → `BondMath.sol`

**Files:**

- Rename: `contracts/src/StakeMath.sol` → `contracts/src/BondMath.sol`
- (Import already repointed in Task 2 Step 1.19)

- [ ] **Step 1: Rename the file and the library symbol**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git mv contracts/src/StakeMath.sol contracts/src/BondMath.sol
```

Then in `contracts/src/BondMath.sol`, replace the library declaration `library StakeMath` → `library BondMath` and any `StakeMath` mentions in its NatSpec/comments → `BondMath`. Leave `reduceAtTier(uint256 active, uint256 unbonding, uint256 tierBps)` signature and param names unchanged.

- [ ] **Step 2: Verify the library file is clean**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git grep -nE "StakeMath|[Ss]tak(e|ing|ed)" -- contracts/src/BondMath.sol
```

Expected: no matches (the body uses `active`/`unbonding`/`tierBps`, already neutral).

- [ ] **Step 3: Build contracts (src only) to confirm they compile**

Run:

```bash
cd contracts && forge build 2>&1 | tail -20
```

Expected: compiles. (Tests not updated yet — `forge build` compiles `test/` too, so if `CapacityBond.t.sol` references old names this will error. If it errors only on test files, that's expected — proceed to Task 4 and revisit build there. If it errors on `src/`, fix before continuing.)

- [ ] **Step 4: Commit src-side rename**

```bash
cd "$(git rev-parse --show-toplevel)" && git add contracts/src/CapacityBond.sol contracts/src/BondMath.sol && git commit -m "refactor(contracts): rename stake→bond on CapacityBond surface; StakeMath→BondMath

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Update Foundry tests

**Files:**

- Modify: `contracts/test/CapacityBond.t.sol`

- [ ] **Step 1: Apply the rename map to the test file**

In `contracts/test/CapacityBond.t.sol`, apply the same canonical map (longest-first ordering) to every reference: `stake(` → `bond(`, `unstake(` → `unbond(`, `requestUnstake(` → `requestUnbond(`, `stakeOf` → `bondOf`, `getStakeMultiple` → `getBondMultiple`, `setMinStake` → `setMinBond`, `activeStake` → `activeBond`, `minStake` → `minBond`, `Staked` → `Bonded`, `Unstaked` → `Unbonded`, `MinStakeUpdated` → `MinBondUpdated`, `InsufficientStake` → `InsufficientBond`, `StakeBelowMinimum` → `BondBelowMinimum`, and any `StakeMath` import → `BondMath`. Update test function names and comments too (e.g. `test_stake_*` → `test_bond_*`, `test_unstake_*` → `test_unbond_*`) for readability.

- [ ] **Step 2: Verify the test file is clean**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git grep -nE "[Ss]tak(e|ing|ed)|StakeMath" -- contracts/test/CapacityBond.t.sol
```

Expected: no matches (or only deliberate ones — none expected).

- [ ] **Step 3: Build + run the CapacityBond test suite**

Run:

```bash
cd contracts && forge build 2>&1 | tail -10 && forge test --match-contract CapacityBondTest 2>&1 | tail -25
```

Expected: compiles; all `CapacityBondTest` tests PASS with the renamed surface.

- [ ] **Step 4: Commit**

```bash
cd "$(git rev-parse --show-toplevel)" && git add contracts/test/CapacityBond.t.sol && git commit -m "test(contracts): update CapacityBond tests to bond vocabulary

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Update deploy scripts

**Files:**

- Modify: `contracts/script/BaseProtocolDeploy.s.sol`
- Modify: `contracts/script/DeployProtocol.s.sol`

- [ ] **Step 1: Update `BaseProtocolDeploy.s.sol`**

Replace the config-struct field `uint256 minStake;` → `uint256 minBond;` and the constructor argument `minStake_: cfg.minStake,` → `minBond_: cfg.minBond,` (the ctor param name `minBond_` matches Task 2). Update any related comments.

- [ ] **Step 2: Update `DeployProtocol.s.sol`**

Replace: `MIN_STAKE` env-var string → `MIN_BOND`; `DEFAULT_MIN_STAKE` → `DEFAULT_MIN_BOND`; `cfg.minStake` → `cfg.minBond`; the serialize key `vm.serializeUint(params, "minStake", cfg.minStake)` → `vm.serializeUint(params, "minBond", cfg.minBond)`; and the NatSpec line `MIN_STAKE (default 50_000e18)` → `MIN_BOND (default 50_000e18)`.

- [ ] **Step 3: Verify scripts are clean**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git grep -nE "[Ss]tak(e|ing|ed)|MIN_STAKE|StakeMath" -- contracts/script/
```

Expected: no matches.

- [ ] **Step 4: Build scripts**

Run:

```bash
cd contracts && forge build 2>&1 | tail -10
```

Expected: compiles (scripts included).

- [ ] **Step 5: Commit**

```bash
cd "$(git rev-parse --show-toplevel)" && git add contracts/script/ && git commit -m "chore(contracts): update deploy scripts to bond vocabulary (MIN_BOND env)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Full contract CI-parity verification

**Files:** none modified (verification gate).

- [ ] **Step 1: Format check**

Run: `cd contracts && forge fmt --check`
Expected: no diff. If it reports formatting, run `forge fmt`, re-stage, and amend the most recent commit.

- [ ] **Step 2: CI-profile build with size gate and warnings-as-errors**

Run:

```bash
cd contracts && FOUNDRY_PROFILE=ci forge build --sizes --deny warnings 2>&1 | tail -25
```

Expected: build succeeds, no warnings. Compare `CapacityBond` size to the Task 1 Step 1 figure — it must be **unchanged** (identifier renames are zero-bytecode). If it differs, investigate (a rename should never change size).

- [ ] **Step 3: Full test suite**

Run: `cd contracts && forge test 2>&1 | tail -25`
Expected: all tests PASS (other suites reference `CapacityBond` via deploy helpers — they exercise the renamed surface and must stay green).

- [ ] **Step 4: Static analysis**

Run:

```bash
cd contracts && aderyn -o /tmp/aderyn.md --no-snippets --skip-update-check 2>&1 | tail -10
cd contracts && slither . --config-file slither.config.json 2>&1 | tail -20
```

Expected: aderyn no new high findings; slither no new medium+ findings. Rename is analysis-neutral.

No commit (gate only). If Step 1 forced a `forge fmt`, that amend is the only change.

---

## Task 7: Update Rust `incentive` `sol!` mirror

**Files:**

- Modify: `crates/incentive/src/capacity_bond.rs`

- [ ] **Step 1: Apply the rename map inside the `alloy::sol!` macro body**

`capacity_bond.rs` mirrors the Solidity surface literally inside `alloy::sol! { ... }`. Apply the canonical map to the macro body: function names (`stake`/`unstake`/`requestUnstake`/`stakeOf`/`getStakeMultiple`/`setMinStake`), event names (`Staked`→`Bonded`, `Unstaked`→`Unbonded`, `MinStakeUpdated`→`MinBondUpdated`), error names (`InsufficientStake`→`InsufficientBond`, `StakeBelowMinimum`→`BondBelowMinimum`), state-getter names (`activeStake`→`activeBond`, `minStake`→`minBond`), and the doc-comments that mention `stake`/`minStake`/`activeStake` (e.g. the module/struct docs at the top of the file). Match the renamed Solidity ABI exactly — `sol!` selector generation depends on the names matching the deployed contract.

- [ ] **Step 2: Update any in-crate callers of the renamed binding accessors**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git grep -nE "Staked|Unstaked|requestUnstake|stakeOf|getStakeMultiple|setMinStake|activeStake|minStake" -- crates/incentive/
```

Expected after edits: only the renamed forms remain. For any caller still referencing old binding types/methods, update it to the new name.

- [ ] **Step 3: Build, lint, and test the incentive crate**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && cargo build -p decdn-incentive && cargo clippy -p decdn-incentive --all-targets 2>&1 | tail -15 && cargo nextest run -p decdn-incentive 2>&1 | tail -15
```

Expected: builds, clippy clean (anti-panic lints unaffected by a rename), tests PASS.

- [ ] **Step 4: Confirm the out-of-scope DHT mock still compiles untouched**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && cargo build -p decdn-node 2>&1 | tail -10
```

Expected: builds. `crates/node/tests/dht_loopback.rs` `StakerSet`/`AllStaked` is intentionally not renamed; confirm it still compiles (it does not depend on the CapacityBond binding names).

- [ ] **Step 5: Commit**

```bash
cd "$(git rev-parse --show-toplevel)" && git add crates/incentive/ && git commit -m "refactor(incentive): mirror bond vocabulary in CapacityBond sol! bindings

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: ADR wording sweep (source-of-truth sync)

**Files:**

- Modify: `adr/026-tokenomics.md`
- Verify only: `adr/028-slashing-appeals.md`, `adr/036-served-bytes-voting-weight.md`

- [ ] **Step 1: Update collateral-pool "stake" wording in ADR 026**

In `adr/026-tokenomics.md`, update the collateral-pool references to bond vocabulary, preserving meaning:

- Line ~122: "no separate flat minimum **stake**" → "no separate flat minimum **bond**".
- Line ~227: "Slashed credit joins the **stake-slash** total in escrow-on-slash" → "**bond-slash**"; "Credit already *claimed* into **`activeStake`**" → "**`activeBond`**"; "slashed by the **stake-reduction** path" → "**bond-reduction** path".
- Line ~259: "reduces the operator's **`activeStake`**/unbonding/unclaimed-credit" → "**`activeBond`**/unbonding/unclaimed-credit".
- Line ~333: "No separate flat minimum **stake**" → "No separate flat minimum **bond**".

Leave "appeal bond" / "challenge bond" usage in ADR 028 alone (distinct concept).

- [ ] **Step 2: Confirm ADR 028 and 036 have no collateral-pool "stake" wording to change**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git grep -nE "[Ss]tak(e|ing|ed)" -- adr/028-slashing-appeals.md adr/036-served-bytes-voting-weight.md
```

Expected: no matches referring to the operator collateral pool. If any appear, update them to bond vocabulary (skip "appeal/challenge bond" contexts). If none, no edit needed.

- [ ] **Step 3: Verify ADR 026 collateral references are now bond-vocab**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git grep -nE "[Ss]tak(e|ing|ed)" -- adr/026-tokenomics.md
```

Expected: no remaining collateral-pool "stake" references (any survivor must be a deliberate historical mention; none expected).

- [ ] **Step 4: Run ADR hygiene pre-commit hooks**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && pre-commit run --files adr/026-tokenomics.md 2>&1 | tail -20
```

Expected: "adr reference hygiene" and "adr book list in sync" hooks PASS (links/anchors intact — we changed prose, not headings).

- [ ] **Step 5: Commit**

```bash
cd "$(git rev-parse --show-toplevel)" && git add adr/026-tokenomics.md adr/028-slashing-appeals.md adr/036-served-bytes-voting-weight.md && git commit -m "docs(adr): align ADR 026 collateral wording with bond vocabulary

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Final residual sweep + whole-tree verification

**Files:** none modified unless a residual is found.

- [ ] **Step 1: Residual stake-on-collateral-path grep across all touched areas**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git grep -nE "[Ss]tak(e|ing|ed)|StakeMath|MIN_STAKE" -- contracts/src/CapacityBond.sol contracts/src/BondMath.sol contracts/test/CapacityBond.t.sol contracts/script/ crates/incentive/ adr/026-tokenomics.md
```

Expected: the ONLY acceptable survivors are the historical `StakingRegistry` mention in `CapacityBond.sol`'s header comment. Anything else → fix and re-commit to the relevant task's file.

- [ ] **Step 2: Whole-repo grep for stragglers referencing the old surface**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git grep -nE "\.stake\(|\.unstake\(|requestUnstake|getStakeMultiple|stakeOf|setMinStake|MinStakeUpdated|InsufficientStake|StakeBelowMinimum|StakeMath" -- ':!docs/superpowers/' ':!adr/_history/'
```

Expected: no matches (the `StakerSet`/`AllStaked` DHT mock won't match these specific patterns). Any match outside the out-of-scope mock is a stale reference — update it.

- [ ] **Step 3: Final full contract + Rust verification**

Run:

```bash
cd "$(git rev-parse --show-toplevel)/contracts" && forge fmt --check && FOUNDRY_PROFILE=ci forge build --sizes --deny warnings 2>&1 | tail -5 && forge test 2>&1 | tail -10
cd "$(git rev-parse --show-toplevel)" && cargo build && cargo clippy --all-targets 2>&1 | tail -10 && cargo nextest run 2>&1 | tail -15
```

Expected: all green; `CapacityBond` size matches the Task 1 baseline.

- [ ] **Step 4: Confirm clean tree, ready for PR**

Run:

```bash
cd "$(git rev-parse --show-toplevel)" && git status && git log --oneline main..HEAD
```

Expected: clean working tree; commit log shows the spec + the rename commits. Proceed to finishing-a-development-branch.
