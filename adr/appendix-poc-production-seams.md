# Appendix: PoC/Production Seam Architecture (Rust implementation)

> **This is an appendix, not a core protocol ADR.** The durable rule — *PoC and production differ only in operational scope and component implementations, never in the on-chain contract surface, and mode selection happens at composition boundaries rather than threaded through domain logic* — is a protocol design principle. The concrete Rust mechanism below applies to this codebase; other implementations may express the same principle differently (build tags, runtime config, DI containers). The implementation crates are early — treat the specifics as guidance, not a frozen API.

## Context

Several ADRs describe PoC-vs-production differences, and they fall into three kinds: **network-scale operational choices** (single RPC endpoint, clients holding ETH for gas, simplified peer bootstrap), **component implementations** (file-based vs platform-keychain key storage, local-only vs gossip-weighted reputation), and **governance process** (admin key vs capacity-weighted Governor with the bootstrap-multisig transition phase per [ADR 009](009-governance.md#adr-009-governance-model)).

Crucially, none of these is a *contract-surface* difference. Per [ADR 016 § Contract Inventory](016-contract-interactions.md#contract-inventory), the full on-chain surface ships in a single audit pass with governance-tunable economics from day one; "PoC" is a network-scale milestone (tens of nodes on a testnet), not a reduced contract surface — there are no contract-surface scope reductions, only the non-contract differences this appendix addresses.

The question this appendix answers: **how does the codebase express the non-contract differences without scattering `if mode == PoC` checks through every crate?**

## Decision

**Mode selection lives only at the composition boundary — the `node` binary's wiring layer. Domain crates are leaf: they contain no mode-conditional logic.**

- **Trait seams at crate boundaries.** Each mode-varying capability — key storage, reputation scoring, the payment-channel client, the governance/parameter reader, and the on-chain contract clients (`FeeRouter`, `CapacityBond`, `BuybackBurner`, `SlashAppeal`) — is a trait owned by its crate. PoC and production concrete implementations sit behind that trait; the wiring layer constructs the right one.
- **Leaf crates stay pure.** The domain crates carry no mode branching. The two-binary split (`node` daemon, `decdn` CLI) shares config schema and identity loading via `common`; mode selection is a `node`-wiring concern. The current crate/binary layout is defined by the workspace `CLAUDE.md` and [appendix-binaries.md](appendix-binaries.md#appendix-decdn-binaries--decdn-node--decdn-split) — that is the source of truth; this appendix deliberately does not restate a crate list that would rot.
- **One source of truth for numeric differences.** Mode-dependent constants (challenge bond, announce interval, dispute window, bootstrap-peer minimum) live in a single constants type with a per-mode constructor — never as magic numbers in domain logic.
- **Compile-time, not runtime.** No `NetworkMode` enum is threaded through call sites; selection is resolved once, at wiring, so a PoC build cannot accidentally run production logic or vice versa. The exact compile-time mechanism (feature, build profile, or cfg) is a wiring-layer detail kept out of every other crate.
- **Contracts are not a seam.** The on-chain surface is production-shaped on every network, including the Sepolia testnet PoC. The only contract-side seam is *deployed contract vs in-process test double* for unit/integration harnesses with no chain — never "PoC stub contract → production contract." Simplified launch economics are the same contracts with different governance-set parameters (`FeeRouter.setShares(...)` etc., per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics)), not a reduced surface.
- **Solidity selection is outside Rust.** Which contract addresses a network points at is a Foundry deploy-script / chain-id-keyed-config concern — the standard Foundry pattern, not a Rust feature.

## Consequences

- Zero mode-conditional branches in domain-crate logic; the PoC/production difference is auditable in one place.
- Both modes are exercised in CI so neither path rots.
- Operational scope (RPC trust, key storage, governance process) can graduate independently without touching domain crates or the contract surface.
- Trade-off: each seam carries two concrete implementations until the PoC operational scope is retired, and adding a seam means registering it in the wiring layer.
- Neutral: Solidity contract selection sits outside Rust's build system by design; it is managed via Foundry deploy scripts, already the standard pattern.
