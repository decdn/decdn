# Issue #1073 Publish CLI End-to-End Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish and verify an Anvil-backed test that drives the real `decdn publish` namespace, claim, and assignment commands.

**Architecture:** A feature-gated integration test launches the existing chain and node fixtures, invokes the built CLI as subprocesses, and reads the resulting contract state through Alloy e2e bindings. Assignment remains proposal-only, so pending state changes while active origins stay empty.

**Tech Stack:** Rust 2024, Tokio, Alloy generated bindings, Anvil/Forge fixture, `std::process::Command`, Cargo Nextest.

## Global Constraints

- Preserve Claude's inherited edits and harden them; the user explicitly waived recreating them through strict test-first development.
- Keep all changes on `test/1073-publish-e2e` and rebase that branch onto current local `main` before verification.
- Do not change production CLI or Solidity behavior.
- Keep the test behind the existing `anvil-e2e` feature.
- Treat `publish assign` as proposal-only: assert pending operators/deadline and empty active origins.
- If Anvil or Forge is unavailable, complete compile-only verification and report the execution limitation explicitly.

---

### Task 1: Rebase the inherited e2e patch safely

**Files:**
- Modify: `crates/e2e/src/bindings.rs`
- Create: `crates/e2e/tests/cli_publish.rs`

**Interfaces:**
- Consumes: Claude's uncommitted binding and test plus current local `main`.
- Produces: the same e2e patch on the latest fixture and CLI code.

- [ ] **Step 1: Inspect and stash the inherited files**

Run:

```bash
git status --short --branch
git diff --check
git stash push -u -m codex-1073-inherited -- crates/e2e/src/bindings.rs crates/e2e/tests/cli_publish.rs
```

Expected: the worktree is clean and the design commit remains at `HEAD`.

- [ ] **Step 2: Rebase and restore**

Run:

```bash
git rebase main
git stash pop
git diff --check
```

Expected: both inherited paths are restored on current `main` with no whitespace errors.

### Task 2: Validate the binding and compile the inherited test

**Files:**
- Modify: `crates/e2e/src/bindings.rs`
- Create: `crates/e2e/tests/cli_publish.rs`

**Interfaces:**
- Produces: `OriginAssignment::getPendingAssignment(uint256)` returning `operators` and `readyAt`.
- Consumes: the existing deployed `OriginAssignment` contract ABI and e2e fixtures.

- [ ] **Step 1: Pin the contract-compatible binding**

Keep this function in the `OriginAssignment` `sol!` block:

```solidity
function getPendingAssignment(uint256 namespaceId)
    external
    view
    returns (address[] memory operators, uint256 readyAt);
```

Verify it matches `contracts/src/OriginAssignment.sol`, whose public view widens stored `uint64 readyAt` to the declared `uint256` return.

- [ ] **Step 2: Compile the feature-gated target**

Run:

```bash
cargo check -p decdn-e2e --features anvil-e2e --test cli_publish
```

Expected: the new binding, subprocess helper, JSON parser, and on-chain assertions compile with no errors.

### Task 3: Harden the CLI and state assertions

**Files:**
- Modify: `crates/e2e/tests/cli_publish.rs`

**Interfaces:**
- Consumes: `ChainFixture`, `NodeFixture`, the rendered node config, and the built `decdn` binary.
- Produces: one bounded test proving all three writes and their expected active/pending split.

- [ ] **Step 1: Keep the namespace creation assertion**

The command and state check must remain equivalent to:

```rust
let create = run_publish(config, &["namespace", "create", "--json"])?;
let namespace_id = parse_namespace_id(&create.stdout)?;
assert_eq!(
    registry.ownerOf(U256::from(namespace_id)).call().await?,
    operator,
);
```

`parse_namespace_id` must require a non-empty JSON line, `submitted == true`, and a numeric `namespace_id`.

- [ ] **Step 2: Keep the claim assertion**

Submit a deterministic 32-byte hash and require exact namespace membership:

```rust
let claim_hash = B256::repeat_byte(0x42);
run_publish(
    config,
    &[
        "claim",
        &format!("{claim_hash:#x}"),
        "--namespace",
        &namespace_id.to_string(),
    ],
)?;
assert_eq!(
    registry.namespaceOf(claim_hash).call().await?,
    vec![U256::from(namespace_id)],
);
```

- [ ] **Step 3: Keep the proposal-only assignment assertions**

After `publish assign`, require:

```rust
let pending = assignment
    .getPendingAssignment(U256::from(namespace_id))
    .call()
    .await?;
assert_eq!(pending.operators, vec![operator]);
assert!(pending.readyAt != U256::ZERO);
assert!(
    assignment
        .getOrigins(U256::from(namespace_id))
        .call()
        .await?
        .is_empty()
);
```

Do not activate the assignment inside the test.

- [ ] **Step 4: Audit bounded and actionable failures**

Confirm the test has a 780-second overall Tokio timeout, subprocess failures include exit status/stdout/stderr, binary lookup honors `DECDN_CLI_BIN`, and `DECDN_KEYSTORE_PASSWORD` is set from the fixture constant.

### Task 4: Run available verification and commit

**Files:**
- Verify: `crates/e2e/src/bindings.rs`
- Verify: `crates/e2e/tests/cli_publish.rs`

- [ ] **Step 1: Run formatting and compile/lint checks**

Run:

```bash
cargo fmt -- --check
cargo check -p decdn-e2e --features anvil-e2e --test cli_publish
cargo clippy -p decdn-e2e --features anvil-e2e --test cli_publish -- -D warnings
```

Expected: all commands exit 0 with no warnings.

- [ ] **Step 2: Detect local chain tooling**

Run:

```bash
command -v anvil
command -v forge
```

Expected: if both commands print paths, continue to Step 3; otherwise record compile-only verification and skip only Step 3.

- [ ] **Step 3: Build the binaries and run the focused e2e test when tooling exists**

Run:

```bash
cargo build -p decdn-node -p decdn-cli
cargo nextest run -p decdn-e2e --features anvil-e2e cli_publish
```

Expected: `publish_namespace_claim_assign_land_on_chain` passes and fixture cleanup completes.

- [ ] **Step 4: Commit the e2e change**

Run:

```bash
git add crates/e2e/src/bindings.rs crates/e2e/tests/cli_publish.rs
git commit -m "test(e2e): drive publish CLI against Anvil (#1073)"
```

Expected: branch status is clean and contains separate design and implementation commits.
