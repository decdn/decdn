# ed25519-vectors

Differential test-vector generator for [`Ed25519Verifier.sol`](../../src/Ed25519Verifier.sol).
Emits the Solidity consumed by [`Ed25519Verifier.t.sol`](../Ed25519Verifier.t.sol)
and [`CapacityBondRegionE2E.t.sol`](../CapacityBondRegionE2E.t.sol), writing it
into those files in place (see [Regenerate](#regenerate)).

The on-chain verifier must be **at least as strict as `ed25519-dalek::verify_strict`** —
the check deCDN nodes run off-chain (issue #669). A verifier more permissive than
dalek would let an attacker bind a NodeId with a signature the network itself
rejects. These vectors are how the test suite pins that parity against the
reference implementation rather than against our own re-derivation.

## Regenerate

```bash
cargo run -- --write   # from this directory: splices both .t.sol files in place
(cd ../.. && forge fmt test/Ed25519Verifier.t.sol test/CapacityBondRegionE2E.t.sol)
```

`--write` replaces everything between the `AUTO-GENERATED` / `END AUTO-GENERATED`
markers in each consuming test — you no longer copy-paste by hand:

- `Ed25519Verifier.t.sol` — the `smallOrder` array, `OFF_CURVE_PK`, and
  `_validVectors()` (sections 1–3).
- `CapacityBondRegionE2E.t.sol` — the `REG_*` constants (section 4).

Always run `forge fmt` afterwards: the generator emits unformatted Solidity and
`forge fmt` owns the final layout (numeric separators, line wrapping). Plain
`cargo run` (no `--write`) still prints both blocks to stdout if you want to
eyeball them.

**CI enforces this.** The `ed25519-vectors` job in
[`.github/workflows/ci.yml`](../../../.github/workflows/ci.yml) reruns
`--write` + `forge fmt` and fails on any diff, and the Rust test
[`ed25519_pin_parity`](../../../crates/incentive/tests/ed25519_pin_parity.rs)
fails if these pins drift from the `ed25519-dalek` / `curve25519-dalek` the node
(`decdn-incentive`) resolves. So a stale vector or an unmatched pin bump is
caught at PR time, not at runtime (issue #1008).

Offline-capable: the dependency versions are exact-pinned and committed in
`Cargo.lock`, so `cargo run` resolves against the registry cache already
populated by a normal workspace build — no network fetch required.

## Reference versions

| Crate | Pin | Role |
|---|---|---|
| `ed25519-dalek` | `=2.2.0` | `verify_strict` — the parity bar |
| `curve25519-dalek` | `=4.1.3` | `EIGHT_TORSION`, point decompression |
| `hex` | `=0.4.3` | encoding only |
| `sha3` | `=0.10.8` | keccak256 for the `registerNode` digest (section 4) |

Bumping a pin is a deliberate act: update the `=` pins here **and** in the
node's `ed25519-dalek` (root `Cargo.toml`) together — the pin-parity guard fails
if they diverge — then re-run `cargo run -- --write`, `forge fmt`, and confirm
the `Ed25519Verifier` and `CapacityBondRegionE2E` suites still pass. Also update
the `Reference:` line and the version literals in `src/main.rs`.
`Cargo.lock` is committed (this is a binary, not a library)
so the transitive closure is reproducible too. This crate carries an empty
`[workspace]` table so it stays isolated from the parent Rust workspace (which
excludes `contracts/` anyway).

## What it emits

1. **8 small-order encodings** — derived from dalek's `EIGHT_TORSION`, not
   hand-written. The generator asserts each `is_small_order()` and that all 8
   are distinct, so the set is provably the exact one `verify_strict` rejects as
   `A` or `R`.
2. **6 valid `(pk, msg, R, s)` vectors** — from deterministic seeds, each
   confirmed with `verify_strict` and asserted torsion-free. Messages span
   edge byte patterns (all-zero, all-one, high-bit-set, incrementing) to
   exercise the decompression sign/parity paths.
3. **1 off-curve key** — canonical `y` (`< p`) whose `x²` is a non-residue, so
   dalek's `decompress()` returns `None`. Isolates the on-curve guard from the
   non-canonical-`y` guard.
4. **1 `registerNode` ownership vector** — a real ed25519 signature over the
   `CapacityBond.registerNode` digest
   `keccak256(nodeId ‖ operator ‖ chainId ‖ registrationNonce)`, with the public
   key as the NodeId. The operator is Foundry's default account #0 (so its key is
   forge-signable for the EIP-712 binding signature) and `chainId` is pinned to
   the Foundry default `31337`; the consuming test asserts both so any drift
   fails loudly. Lets `CapacityBondRegionE2E.t.sol` register a node through the
   production verifier instead of a mock.
