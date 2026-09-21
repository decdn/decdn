# deCDN

[![CI](https://github.com/decdn/decdn/actions/workflows/ci.yml/badge.svg)](https://github.com/decdn/decdn/actions/workflows/ci.yml)
[![Security](https://github.com/decdn/decdn/actions/workflows/security.yml/badge.svg)](https://github.com/decdn/decdn/actions/workflows/security.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![MSRV](https://img.shields.io/badge/MSRV-1.95.0-orange.svg)](rust-toolchain.toml)

Decentralized CDN where nodes cache and serve content-addressed blobs over [iroh](https://iroh.computer/) QUIC, and clients pay per-MB via off-chain USDC vouchers backed by a shared on-chain payment pool. Rust implementation. The initial network deployment targets tens of nodes on Arbitrum Sepolia, while the on-chain surface ships at full production shape with governance-tunable economics from day one ([ADR 016](adr/016-contract-interactions.md)).

## Status

Pre-launch. The workspace version is `0.0.0`, there is no release tag, and nothing is
published to crates.io or a container registry yet. Eleven crates and two binaries are
implemented; no crate is a stub.

Until the first public deployment, wire, ABI, config, and storage formats change without
compatibility shims — there are no live peers to keep in step.

## How It Works

- **Nodes** bond TOKEN proportional to declared bandwidth capacity, cache content, and serve BLAKE3-addressed blobs over QUIC
- **Clients** take candidate nodes from their peer store or the on-chain registry, pick the RTT-nearest — from their RTT map, or by probing ([ADR 037](adr/037-regional-proxy-warming.md)) — stream content, and pay via off-chain USDC vouchers
- On a **cache miss**, nodes pull from peers (paid), cache locally, and stream to the client simultaneously
- All byte transfers are paid — client-to-node and node-to-node

```
  ┌────────────┐ ┌────────────┐ ┌────────────┐
  │    Node    │◄┤    Node    │◄┤    Node    │
  │  (cached)  ├►│  (origin)  ├►│  (cached)  │
  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
        │              │              │
     pays per-MB    pays per-MB    pays per-MB
       (USDC)         (USDC)         (USDC)
        │              │              │
  ┌─────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐
  │   Client   │ │   Client   │ │   Client   │
  └────────────┘ └────────────┘ └────────────┘
```

### Wire Protocols (Core CDN)

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency + availability probing |
| `cdn/client/v1` | All paid delivery (client→node and node→node) |
| `cdn/dht/v1` | Content discovery via Kademlia DHT (see [ADR 022](adr/022-content-discovery.md)) |

Peer discovery is on-chain, not broadcast: the node set is the `CapacityBond` active
staker set, read directly from the contract ([ADR 001](adr/001-network.md)).

### Binaries

| Binary | Role |
|--------|------|
| `decdn-node` | Daemon — runs the cache node service. Single subcommand: `decdn-node run [--config <path>]`. |
| `decdn` | User CLI — every command a human types. See the table below. |

The container image ships `decdn-node` only. Publishers grab the
`decdn-${VERSION}-${TARGET}.tar.gz` release archive; operators grab
both. See [`adr/appendix-binaries.md`](adr/appendix-binaries.md) for
the dockerd-style split rationale.

### CLI Surface

| Command group | Purpose |
|---------------|---------|
| `key-gen`, `whoami` | Generate, then inspect, the Ed25519 node key and Ethereum keystore |
| `config {init,validate}` | Render a config file and check one before the daemon reads it |
| `setup` | Guided operator onboarding over `key-gen` / `node bond` / `node register` ([ADR 019](adr/019-node-onboarding.md)) |
| `node {…}` | Operator admin against a running daemon over the loopback surface, plus the on-chain lifecycle (bond, register, deregister, rotate-key) and read-only diagnostics (doctor, lookup, slashes) |
| `probe`, `fetch` | Probe a node over `cdn/probe/v1`; paid single-blob fetch over `cdn/client/v1` |
| `pool {…}` | Client payment-pool lifecycle: list/status, open, top-up, close, reclaim, assign (delegated spend) |
| `publish {…}` | Publisher control plane: create namespaces, seat and unseat authorized origins |
| `origin {import}` | Seed a cache origin store from local content, offline and config-free |
| `bundle {pull}` | Fetch directory bundles by manifest |
| `appeal {slash}` | File a slash appeal and post the appeal bond ([ADR 028](adr/028-slashing-appeals.md)) |

Run `decdn --help` for the full subcommand tree.

### Install

From the first tagged release — nothing is published yet, see [Status](#status) above:

```bash
cargo install decdn-cli          # the `decdn` CLI
cargo install decdn-node         # the daemon
docker pull decdn/decdn-node     # or ghcr.io/decdn/decdn-node
```

Signed release archives are attached to each [GitHub
release](https://github.com/decdn/decdn/releases); they are the channel a
maintainer's signature covers, so prefer them if you verify signatures — see
[SECURITY.md](SECURITY.md). Operator terms accepted at registration live in
[`crates/cli/TERMS.md`](crates/cli/TERMS.md), embedded verbatim in the CLI.

## Quickstart

Run a node. `setup` performs the pre-flight checks, bonds TOKEN against the on-chain
`bondRequired(mbps)` curve, and registers the node, then prints a go/no-go summary.

```bash
decdn config init
decdn setup --mbps 100 --region US --multiaddr /ip4/203.0.113.10/udp/4433/quic-v1
decdn-node run --config ~/.decdn/node.toml
decdn node health          # then `node status`, `node lanes`, `node pools`, `node top`
```

Fetch content. A pool is a single deposit that fans out to every provider paid from it,
so there is no per-provider open.

```bash
decdn pool open --deposit-micro-usdc 5000000
decdn fetch --hash <blake3-hash> --output ./blob.bin
```

Publishers additionally create a namespace and seat the origins authorized to serve it:

```bash
decdn publish namespace create
decdn publish assign <namespace-id> <0xOPERATOR>
```

Chain endpoints and contract addresses come from the config file, or from explicit
`--rpc-url` and `--*-address` flags on each command.

## Architecture

### Crate Structure

```
crates/
  node/         — daemon binary `decdn-node`: runtime bring-up, handlers, admin RPC server, dispatch limiter
  cli/          — user CLI binary `decdn`: every command a human types (see the CLI Surface table above)
  common/       — shared types: config schema + resolver, identity loading, AdminRpc trait + DTOs, and the clap definitions for both binaries
  protocol/     — shared types, wire format, ALPN message definitions (leaf crate, minimal deps)
  config-types/ — config-vocabulary value types shared by cache + common (leaf crate)
  bao-range/    — iroh-blobs-free bao verified-range helpers: chunk-group alignment, range encode/verify, origin-store layout (leaf crate, ADR 038)
  cache/        — cache engine wrapping iroh-blobs + origin pull-through
  client-pull/  — reusable `cdn/client/v1` paid-pull requester and buyer-side channel open, shared by node and cli
  incentive/    — shared payment pools, staking, vouchers (alloy for Ethereum)
  reputation/   — local per-peer reputation scoring (ADR 008)
  e2e/          — test-only cross-layer Rust↔contract fixtures (anvil + the production deploy script)
contracts/      — Solidity contracts + Foundry (repo root, excluded from workspace)
```

The daemon is itself a paying client on its upstream cache-miss leg, which is why `node`
depends on `client-pull` like any other consumer. Three leaf crates — `protocol`,
`config-types`, and `bao-range` — keep the publisher CLI free of any blob store or AWS
SDK; `.github/scripts/check_crate_edges.py` enforces that boundary in CI.

### Key Design Decisions

Architecture decision records live in [`adr/`](adr/). [`adr/README.md`](adr/README.md) is
the entry point with reading order; [`adr/architecture.md`](adr/architecture.md) is the
living overview. Highlights:

- **Content addressing:** BLAKE3 hashes; clients verify on receipt ([ADR 002](adr/002-content-addressing.md#adr-002-content-addressing))
- **Encryption-agnostic protocol:** publishers may encrypt content before upload; the CDN content-addresses and delivers the resulting bytes, while key distribution stays outside the protocol ([ADR 002](adr/002-content-addressing.md#adr-002-content-addressing))
- **Dual currency:** USDC for payments, TOKEN for staking and governance; per-bucket economics are governance-tunable from day one ([ADR 026](adr/026-tokenomics.md), [ADR 016](adr/016-contract-interactions.md))
- **No exposed origins:** origin backends (S3/R2/B2) are opaque per-node config
- **Settlement:** provider-only redemption — no party submits a value on another's behalf, so an owner's `closePool` cannot understate a node's earnings; the node's periodic redeem sweep runs inside the close grace window to collect earned vouchers before the owner can reclaim ([ADR 003](adr/003-payments.md#adr-003-payment-model))
- **Discovery:** `cdn/dht/v1` Kademlia DHT over the on-chain active set, with the on-chain origin directory as the deterministic last-resort fallback ([ADR 022](adr/022-content-discovery.md))
- **Verified ranges:** the BLAKE3 content hash already is a Merkle root, and the outboard supplies the interior nodes, so any byte range verifies on its own — this is what makes resume and parallel fetch safe ([ADR 038](adr/038-bao-verified-range-streaming.md))
- **Multi-source fetch:** a client saturates its downlink from the cheapest adequate set of holders, reassigning ranges as sources finish or stall ([ADR 039](adr/039-multi-source-parallel-fetch.md))
- **Cache policy:** admission and eviction are node-local and pluggable, never a wire concern ([ADR 040](adr/040-cache-policy.md))
- **Regional locality:** latency-driven proxy warming closes the gap between keyspace-routed discovery and geographic demand ([ADR 037](adr/037-regional-proxy-warming.md))
- **Refuse to serve:** a node declines a cache miss whose upstream price leaves no margin after the fee split, rather than serving at a loss ([ADR 041](adr/041-refuse-to-serve.md))
- **Reputation:** local per-peer EWMA over observed outcomes, with no cross-node aggregation ([ADR 008](adr/008-reputation.md))
- **Slashing appeals:** slashed TOKEN sits in escrow and the operator has a bounded 30-day appeal backed by an appeal bond ([ADR 028](adr/028-slashing-appeals.md))
- **Governance:** single day-one governance contract surface (`DecdnGovernor` + `TimelockController`); served-bytes-weighted operator voting with hardcoded safety bounds — only the process evolves by phase (admin key → bootstrap multisig → DAO), not the contract set ([ADR 009](adr/009-governance.md), [ADR 036](adr/036-served-bytes-voting-weight.md))
- **Content takedown:** governance-controlled hash blacklisting with regional compliance bodies ([ADR 011](adr/011-content-takedown.md))
- **Client architecture:** lightweight QUIC endpoints; registry bootstrap with fallback; per-connection ephemeral identity binding ([ADR 012](adr/012-client.md))
- **Schema evolution:** varint-length framing, protocol enums, three-tier evolution model ([ADR 013](adr/013-schema-evolution.md))
- **On-chain verification:** single secp256k1 EIP-712 slash signature (`slash_sig`) per message with optimistic challenge-response; the Ed25519 NodeId is connection identity only, authenticated separately by the QUIC handshake ([ADR 014](adr/014-on-chain-verification.md))
- **Liquidity:** protocol-owned liquidity via a Uniswap V3 50/50 TOKEN/USDC pool ([ADR 018](adr/018-liquidity-strategy.md))
- **Production L2:** Arbitrum One for all on-chain contracts ([appendix](adr/appendix-l2-deployment.md))

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for development environment setup, build commands, coding standards, and contribution guidelines.

## Operations

- [Operator runbook](docs/runbook.md)
- [`monitoring/`](monitoring/) — Grafana dashboard and Prometheus alert rules
- [Observability appendix](adr/appendix-observability.md) — the metric surface behind those dashboards
- [Key rotation appendix](adr/appendix-operator-key-rotation.md) and [upgrade-path appendix](adr/appendix-operator-upgrade-path.md)
- [Release process](RELEASING.md) — cutting, signing and publishing a release
- [Security policy](SECURITY.md) — reporting a vulnerability, verifying a release

## License

Dual-licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
