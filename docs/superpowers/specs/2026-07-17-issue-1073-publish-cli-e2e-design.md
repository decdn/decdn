# issue #1073 publish CLI end-to-end test design

## Goal

Exercise the `decdn publish namespace create`, `publish claim`, and `publish
assign` binary paths against the real Anvil fixture and verify their resulting
`PublisherRegistry` and `OriginAssignment` state.

## Scope

Add one `anvil-e2e`-gated integration test under `crates/e2e/tests` and only the
missing read binding needed to inspect pending assignments. Production CLI and
contract behavior are unchanged. Assignment activation is not added because
the CLI command is deliberately proposal-only.

## Test flow

The test launches `ChainFixture`, then launches one onboarded `NodeFixture`.
That fixture supplies the rendered node configuration, keystore, signer, and
an active bonded operator that is valid as an assignment target.

The test locates the built `decdn` binary, supplies the fixture config and
`DECDN_KEYSTORE_PASSWORD`, and runs these commands synchronously:

1. `publish namespace create --json`; parse the numeric namespace ID and assert
   `PublisherRegistry.ownerOf` equals the fixture operator.
2. `publish claim <hash> --namespace <id>`; assert `namespaceOf(hash)` returns
   exactly that namespace.
3. `publish assign <id> <operator>`; assert `getPendingAssignment` contains
   exactly the operator and a non-zero `readyAt` value.

Because `publish assign` calls `proposeAssignment`, not
`activateAssignment`, `getOrigins(id)` must remain empty. This is the expected
active-state assertion: the write is visible in pending state and has not
bypassed the governance delay.

Each subprocess must exit successfully. Failures include the status, stdout,
and stderr. Namespace JSON parsing takes the last non-empty stdout line and
requires `submitted: true`. An overall Tokio timeout bounds the test while RAII
fixture cleanup terminates Anvil and the daemon on every exit path.

## Binding change

Extend the e2e `OriginAssignment` Alloy binding with the existing contract view
function `getPendingAssignment(uint256)`, returning the operator array and
`readyAt`. No generated files or production bindings change.

## Verification

The e2e crate must compile with `--features anvil-e2e`. If `anvil` and `forge`
are available, run the focused `cli_publish` test after building `decdn-node`
and `decdn`. Otherwise the local evidence is compile-only and CI remains the
execution environment. Formatting and Clippy must also pass for the e2e crate.

## Acceptance criteria

- The real CLI binary signs and submits all three publish operations.
- Namespace ownership and `namespaceOf` reflect the first two writes.
- Pending assignment contains the proposed operator and non-zero deadline.
- Active origins remain empty before activation.
- Subprocess and overall timeouts fail with actionable diagnostics.
- The feature-gated e2e target compiles, and runs locally when Anvil/Forge are
  installed.
