# ed25519-vectors

Differential test-vector generator for [`Ed25519Verifier.sol`](../../src/Ed25519Verifier.sol).
Emits paste-ready Solidity consumed by [`Ed25519Verifier.t.sol`](../Ed25519Verifier.t.sol).

The on-chain verifier must be **at least as strict as `ed25519-dalek::verify_strict`** —
the check deCDN nodes run off-chain (issue #669). A verifier more permissive than
dalek would let an attacker bind a NodeId with a signature the network itself
rejects. These vectors are how the test suite pins that parity against the
reference implementation rather than against our own re-derivation.

## Regenerate

```bash
cargo run            # from this directory
```

Copy the output verbatim into `Ed25519Verifier.t.sol`, replacing everything
between the `AUTO-GENERATED` / `END AUTO-GENERATED` markers (the `smallOrder`
array, `OFF_CURVE_PK`, and `_validVectors()`).

Offline-capable: the dependency versions are exact-pinned and committed in
`Cargo.lock`, so `cargo run` resolves against the registry cache already
populated by a normal workspace build — no network fetch required.

## Reference versions

| Crate | Pin | Role |
|---|---|---|
| `ed25519-dalek` | `=2.2.0` | `verify_strict` — the parity bar |
| `curve25519-dalek` | `=4.1.3` | `EIGHT_TORSION`, point decompression |
| `hex` | `=0.4.3` | encoding only |

Bumping a pin is a deliberate act: re-run `cargo run`, re-paste, and confirm the
12 tests still pass. `Cargo.lock` is committed (this is a binary, not a library)
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
