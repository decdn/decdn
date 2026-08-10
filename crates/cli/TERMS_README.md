# Maintaining `TERMS.md`

`TERMS.md` is the **canonical operator terms** — the single source of the terms
text the node software embeds and displays at registration. Its exact bytes are
the preimage of the on-chain `termsHash`: the registration clickwrap records
`keccak256(<bytes of TERMS.md>)` (see ADR 019 § Operator Safety Obligations).

**This file (`TERMS_README.md`) is not embedded or hashed** — it is maintainer
guidance only. Keep all developer notes here, never inside `TERMS.md`, so that
editing guidance can never perturb the terms hash.

Both files live in `crates/cli/` rather than at the repo root because
`crates/cli/src/commands/terms.rs` embeds `TERMS.md` with `include_str!`, which
cannot reach outside its own package — from the repo root the path would be
unreachable in the published `.crate` and `cargo install decdn-cli` would fail
to build.

## Rules

- **Any byte change to `TERMS.md` changes the hash.** A new version becomes
  canonical only when governance sets `currentTermsHash` to the new hash
  (`DecdnGovernor` + timelock). Until then, a node built against the new text
  would submit a hash that `registerNode` rejects.
- **Do not paraphrase the terms in code or docs** — reference them by version.
- Bump the `Version:` line in `TERMS.md` for any substantive change so the
  accepted version is self-identifying.
- The embedding/clickwrap wiring (display + `keccak256` + acceptance) lives in
  `crates/cli/src/commands/terms.rs`. A `#[test]` there locks the embedded hash
  to a literal, so any edit to `TERMS.md` fails the test suite until that literal
  is updated too — deliberately, since a terms change is a gated event that must
  be paired with a governance `setCurrentTermsHash` bump.
