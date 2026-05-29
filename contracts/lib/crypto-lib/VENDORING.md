# Vendored: Smoo.th Crypto Lib (SCL) — ed25519 / EIP-6565 closure

- **Upstream:** <https://github.com/get-smooth/crypto-lib>
- **Pinned commit:** `d714e9824e2e2e44be3c8fd498e0de651ddc9425` (`main`, 2024-12-14)
- **License:** MIT (see `LICENSE`)
- **Audits (upstream `doc/Audits/`):** Veridise (`VAR_SmoothCryptoLib_*`) and
  CryptoExperts (`CRX_smooth_report_*`). EIP-6565 ed25519 is the
  Ethereum-Foundation-funded reference implementation.

## Why vendored (not a submodule)

The repo-wide CI gates (`forge fmt --check`, `forge build --deny warnings`,
slither, aderyn, coverage) only need to skip this code, which they do for
everything under `lib/`. Vendoring the **narrowest compile closure** of
`src/lib/libSCL_EIP6565.sol` (the ed25519 verifier) keeps the surface auditable
and pinned. `src/Ed25519Verifier.sol` is the only consumer.

## Files vendored (exact compile closure of `libSCL_EIP6565.sol`)

```
src/lib/libSCL_EIP6565.sol          ed25519 Verify / Verify_LE (RIP-6565)
src/modular/SCL_modular.sol         ModInv (modexp)
src/modular/SCL_sqrtMod_5mod8.sol   SqrtMod for p = 5 mod 8 (ed25519)
src/fields/SCL_wei25519.sol         ed25519 field/curve constants (Weierstrass-25519)
src/elliptic/SCL_ecOncurve.sol      ec_isOnCurve
src/elliptic/SCL_mulmuladdX_fullgenW.sol  ecGenMulmuladdB4W (Shamir + 4-bit window)
src/hash/SCL_sha512.sol             byte-swap helpers
src/include/SCL_errcodes.sol        error selectors
src/include/SCL_field.h.sol         generic field header  [PATCHED — see below]
src/include/SCL_mask.h.sol          precompile address constants
external/sha512/Sha2Ext.sol         in-EVM SHA-512
external/sha512/LibBytes.sol         byte utilities for SHA-512
```

Deliberately **excluded** (not in the ed25519 path): `libSCL_eddsaUtils.sol`
(off-chain / test-only; header says *"NEVER USE THIS ONCHAIN"*),
`libSCL_eccUtils.sol`, `SCL_mulmuladdX_fullgen_b4.sol`, and all secp256r1 /
secp256k1 / RIP-7212 / RIP-7696 / MPC sources.

## Local modifications

Exactly **one**, and it is build configuration, not cryptographic logic:

- `src/include/SCL_field.h.sol` — the active field import was repointed from
  `@solidity/fields/SCL_secp256r1.sol` to `../fields/SCL_wei25519.sol`
  (importing only `{ p, gx, gy, n, pMINUS_2, nMINUS_2 }`). Upstream defaults
  this generic header to secp256r1 and ships ed25519 as an alternate
  configuration (its own commented-out line). This header's only consumer in
  our closure is `SCL_modular.sol`; repointing it (a) drops the entire secp256r1
  source tree from the vendored footprint and (b) makes the otherwise-unused
  `nModInv` use the ed25519 group order rather than P-256's. EIP-6565 passes
  every modulus explicitly, so the ed25519 verification result is identical
  either way.

To re-vendor, re-fetch the files above at the pinned commit and re-apply that
single import change.
