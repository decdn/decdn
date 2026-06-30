# Maintaining `TERMS.md`

`TERMS.md` is the **canonical operator terms** — the single source of the terms
text the node software embeds and displays at registration. Its exact bytes are
the preimage of the on-chain `termsHash`: the registration clickwrap records
`keccak256(<bytes of TERMS.md>)` (see ADR 019 § Operator Safety Obligations).

**This file (`TERMS_README.md`) is not embedded or hashed** — it is maintainer
guidance only. Keep all developer notes here, never inside `TERMS.md`, so that
editing guidance can never perturb the terms hash.

## Rules

- **Any byte change to `TERMS.md` changes the hash.** A new version becomes
  canonical only when governance sets `currentTermsHash` to the new hash
  (`DecdnGovernor` + timelock). Until then, a node built against the new text
  would submit a hash that `registerNode` rejects.
- **Do not paraphrase the terms in code or docs** — reference them by version.
- Bump the `Version:` line in `TERMS.md` for any substantive change so the
  accepted version is self-identifying.
- The embedding/clickwrap wiring (display + `keccak256` + signature) lands with
  the node-software change; until then `TERMS.md` is the design-of-record text,
  not yet wired into a running binary.
