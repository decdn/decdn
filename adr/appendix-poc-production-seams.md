# Appendix: PoC/Production Seam Architecture (Rust implementation)

> **This is an appendix, not a core protocol ADR.** The durable rule is a protocol design principle: *PoC and production differ only in operational scope and component implementations, never in the on-chain contract surface. Mode selection happens at composition boundaries, not threaded through domain logic.* The Rust mechanism below applies to this codebase. Other implementations may express the same principle with build tags, runtime config, or DI containers. The implementation crates are early. Treat the specifics as guidance, not a frozen API.

## Context

Several ADRs describe PoC-vs-production differences. They fall into three kinds:

- **Network-scale operational choices:** single RPC endpoint, clients holding ETH for gas, simplified peer bootstrap.
- **Component implementations:** file-based vs platform-keychain key storage.
- **Governance process:** admin key vs capacity-weighted Governor, with the bootstrap-multisig transition phase per [ADR 009](009-governance.md#adr-009-governance-model).

None of these is a *contract-surface* difference. Per [ADR 016 § Contract Inventory](016-contract-interactions.md#contract-inventory), the full on-chain surface ships in a single audit pass with governance-tunable economics from day one. "PoC" is a network-scale milestone (tens of nodes on a testnet), not a reduced contract surface. There are no contract-surface scope reductions, only the non-contract differences this appendix addresses.

This appendix answers one question: **how does the codebase express the non-contract differences without scattering `if mode == PoC` checks through every crate?**

## Decision

**Mode selection lives only at the composition boundary: the `node` binary's wiring layer. Domain crates are leaf. They contain no mode-conditional logic.**

- **Trait seams at crate boundaries.** Each mode-varying capability is a trait owned by its crate: key storage, the payment-pool client, the governance/parameter reader, and the on-chain contract clients (`FeeRouter`, `CapacityBond`, `BuybackBurner`, `SlashAppeal`). PoC and production concrete implementations sit behind that trait. The wiring layer constructs the right one.
- **Leaf crates stay pure.** Domain crates carry no mode branching. The two-binary split (`node` daemon, `decdn` CLI) shares config schema and identity loading via `common`. Mode selection is a `node`-wiring concern. The workspace `CLAUDE.md` and [appendix-binaries.md](appendix-binaries.md#appendix-decdn-binaries--decdn-node--decdn-split) define the current crate/binary layout and are the source of truth. This appendix does not restate a crate list that would rot.
- **One source of truth for numeric differences.** Mode-dependent constants live in a single constants type with a per-mode constructor, never as magic numbers in domain logic: challenge bond, announce interval, dispute window, bootstrap-peer minimum.
- **Compile-time, not runtime.** No `NetworkMode` enum is threaded through call sites. Selection resolves once, at wiring, so a PoC build cannot run production logic or vice versa. The compile-time mechanism (feature, build profile, or cfg) is a wiring-layer detail kept out of every other crate.
- **Contracts are not a seam.** The on-chain surface is production-shaped on every network, including the Sepolia testnet PoC. The only contract-side seam is *deployed contract vs in-process test double* for unit/integration harnesses with no chain, never "PoC stub contract → production contract." Simplified launch economics are the same contracts with different governance-set parameters (`FeeRouter.setShares(...)` etc., per [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics)), not a reduced surface.
- **Solidity selection is outside Rust.** Which contract addresses a network points at is a Foundry deploy-script / chain-id-keyed-config concern, the standard Foundry pattern, not a Rust feature.

### Discovery-provider seam

The `node` runtime's wiring (`build_endpoint`) selects which NodeId→address discovery the iroh endpoint uses. The leaf crates and the config schema do **not**. `common` config carries plain Strings (`network.discovery.{pkarr_url, dns_origin, peers}`). Resolution validates them with the same parsers the node uses: parseable URL, non-empty origin, valid iroh NodeId (the canonical 64-char lowercase-hex form), parseable `SocketAddr`. The node parses them into iroh providers (`PkarrPublisher` / `DnsAddressLookup` / `MemoryLookup`) and composes them via `Endpoint::builder().address_lookup(..)`.

Absent config keeps the n0-hosted default (`presets::N0`). Present config builds on `presets::Minimal` and adds only the configured legs. Relay selection (`network.relay_urls`) is an independent, orthogonal seam built the same way. Dropping the n0 *discovery* leg does not disable relays: when no custom relay map is set, the node restores the n0 relay default that `N0` would have applied. This is the same pattern as the relay-map and origin-backend seams: plain data in the config crate, provider selection at the `node` composition boundary. The static peer map (`MemoryLookup`) supports fully n0-independent, offline networks.

## Consequences

- Domain-crate logic has zero mode-conditional branches. The PoC/production difference is auditable in one place.
- CI exercises both modes, so neither path rots.
- Operational scope (RPC trust, key storage, governance process) can graduate independently, without touching domain crates or the contract surface.
- Trade-off: each seam carries two concrete implementations until the PoC operational scope is retired. Adding a seam means registering it in the wiring layer.
- Neutral: Solidity contract selection sits outside Rust's build system by design. Foundry deploy scripts manage it, already the standard pattern.
