# deCDN

[![CI](https://github.com/decdn/decdn/actions/workflows/ci.yml/badge.svg)](https://github.com/decdn/decdn/actions/workflows/ci.yml)
[![Security](https://github.com/decdn/decdn/actions/workflows/security.yml/badge.svg)](https://github.com/decdn/decdn/actions/workflows/security.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![MSRV](https://img.shields.io/badge/MSRV-1.95.0-orange.svg)](rust-toolchain.toml)

A decentralized CDN. Bonded operators cache and serve BLAKE3-addressed blobs over
[iroh](https://iroh.computer/) QUIC, and clients pay for every megabyte in USDC through
off-chain vouchers backed by a shared on-chain payment pool. The receiver verifies every
byte against its content hash, so any node can serve any blob without being trusted.

The implementation is Rust. The test network runs on Arbitrum Sepolia; production targets
Arbitrum One. The contracts ship at full production shape, with governance-tunable
economics from day one ([ADR 016](adr/016-contract-interactions.md)).

## Status

Pre-launch. The workspace version is `0.0.0`, there is no release tag, and nothing is
published to crates.io or a container registry yet. To run deCDN today,
[build it from source](#build-from-source).

Wire, ABI, config, and storage formats change without compatibility shims until the first
public deployment. Many ADRs are still marked Draft, so the specification can change too.

## How it works

```text
┌─────────────────── on-chain (Arbitrum) ────────────────────┐
│  CapacityBond   bonded TOKEN · registry of active nodes    │
│  PaymentPool    USDC deposits · voucher redemption         │
└─────▲──────────────────▲──────────────────────────▲────────┘
      │ deposit          │ bond · deposit · redeem  │ bond · redeem
┌─────┴──────┐    ┌──────┴───────┐    ┌─────────────┴─┐    ┌───────────────┐
│   Client   │◄──►│  Cache node  │◄──►│  Origin node  │◄───┤ S3/R2/B2/HTTP │
└────────────┘    └──────────────┘    └───────────────┘    └───────────────┘
    ◄─► bytes one way, USDC vouchers the other (cdn/client/v1)
```

- **Nodes** bond TOKEN against their declared bandwidth, register on-chain, then cache
  and serve content. The node set is the `CapacityBond` active set, read directly from the
  contract ([ADR 001](adr/001-network.md)).
- **Clients** pick nodes from their peer store or the on-chain registry by measured RTT,
  stream content, and pay each node with signed vouchers drawn against one pool deposit
  ([ADR 012](adr/012-client.md), [ADR 003](adr/003-payments.md)).
- **On a cache miss**, a node pulls from a peer and streams to the client at the same
  time. The node pays for that upstream pull from its own pool, like any other client:
  every byte transfer is paid.
- **Origin backends** (S3, R2, B2, HTTP, filesystem) are private per-node config. No
  origin URL is ever exposed ([ADR 002](adr/002-content-addressing.md)).

## Try it

### Build from source

You need the Rust toolchain that [`rust-toolchain.toml`](rust-toolchain.toml) pins
(`rustup` installs it automatically), a C compiler, and `make`.

```bash
git clone --recurse-submodules https://github.com/decdn/decdn.git
cd decdn
cargo build --release -p decdn-cli -p decdn-node
# Binaries: target/release/decdn and target/release/decdn-node
```

[CONTRIBUTING.md](CONTRIBUTING.md#development-environment) lists the full development
prerequisites, including Foundry for the contracts.

### Run a node on Arbitrum Sepolia

What you need:

- **Arbitrum Sepolia ETH** for gas.
- **TOKEN** for the bond. The bond is `max(minBond, bondRequired(mbps))`, where
  `bondRequired` is a super-linear curve over declared bandwidth
  ([ADR 026 § Capacity-bond curve](adr/026-tokenomics.md#capacity-bond-curve),
  [§ Minimum bond](adr/026-tokenomics.md#minimum-bond-is-non-retroactive)). The testnet
  deployed with `minBond` at 50,000 TOKEN ([manifest](crates/cli/deployments/421614.json)),
  which covers every tier up to about 1 Gbps. `decdn node bond --dry-run` prints the live
  target. There is no public TOKEN source before TGE, so ask the maintainers in
  [GitHub issues](https://github.com/decdn/decdn/issues).
- **USDC** for the node's own buyer pool, which pays for cache-miss pulls from peers
  (10 USDC working deposit by default). Without it, every cache miss falls through to
  origin ([runbook](docs/runbook.md#node-to-node-pulls-never-succeed-buyer-wallet-or-pool)).
  Circle's [testnet faucet](https://developers.circle.com/stablecoins/docs/usdc-on-testnet)
  dispenses it.
- **A keystore password source** for the headless daemon: `DECDN_KEYSTORE_PASSWORD` or
  `--keystore-password-file` ([runbook](docs/runbook.md#keystore-will-not-unlock)).

```bash
decdn config init            # writes ~/.decdn/node.toml with the testnet RPC and contract addresses
decdn setup --mbps 100 --region US --multiaddr /ip4/203.0.113.10/udp/4433/quic-v1
decdn-node run
decdn node health            # then: node status, node lanes, node top
```

`setup` generates the node key and keystore when neither exists, shows the operator terms,
bonds, registers the node, and prints a go/no-go summary
([ADR 019](adr/019-node-onboarding.md)). The node listens on UDP 4433 over IPv4 and IPv6, so a
dual-stack host also passes `--multiaddr /ip6/<addr>/udp/4433/quic-v1`. `--region` is an ISO 3166-1 alpha-2 country code
([ADR 030](adr/030-node-region-self-attestation.md)).

To serve your own content, import it into a local origin store with
`decdn origin import --input <path> --to <dir>`, then write the node config with
`decdn config init --origin file://<absolute-dir>` (`--force` replaces an existing one).
For an S3 origin, import to a local directory and `aws s3 sync` it to the bucket: the two
layouts are identical.

### Fetch content

A client uses its own keystore under `~/.decdn/client`, separate from any node identity
on the same host. Fund the address that `key-gen` prints with Arbitrum Sepolia ETH and
USDC.

```bash
decdn config init --client --output ~/.decdn/client.toml
decdn key-gen --output-dir ~/.decdn/client
decdn --config ~/.decdn/client.toml fetch --hash <blake3-hash> -o ./blob.bin
```

`fetch` probes bonded nodes and picks one that holds the blob. When none does, it picks a
bonded node that pulls the blob through from a peer or its origin, so a cache miss still
succeeds. It pays the chosen node from a pool you own.

`fetch` reuses your existing pool or opens one with a 10 USDC working deposit, and it tops
the pool up on-chain without prompting when the balance runs low. Fund the wallet with
only what you are willing to spend. One pool pays every provider, so there is no
per-provider setup. Use `decdn pool {…}` to inspect, close, or reclaim a pool.

### Publish content

A publisher creates a namespace and seats the bonded operators that serve as its origins.
`publish` moves no bytes: content reaches clients from the seated operators' origin
backends, and a client routes to them with `fetch --namespace <id>`.

The installed vetting policy decides who may seat origins
([ADR 011 § Vetting policies](adr/011-content-takedown.md#vetting-policies)). The testnet
runs the manual policy, so a `VETTER_ROLE` holder must vet the publisher's address before
`assign` succeeds. That step happens out-of-band, not through the CLI.

```bash
decdn publish namespace create                     # prints the namespace id
decdn publish assign <namespace-id> <0xOPERATOR>...
```

### Local devnet

To run without testnet funds, [`contracts/dev-deploy.sh`](contracts/dev-deploy.sh) starts
Anvil and deploys the full protocol, a mock USDC, and a TOKEN faucet. See
[CONTRIBUTING.md § Local deployment](CONTRIBUTING.md#local-deployment-anvil).

The `decdn-e2e` tests are runnable end-to-end journeys: onboarding, paid fetch, and
publishing. They need `cargo-nextest` and Foundry, and they drive the debug binaries, so
build those first:

```bash
cargo build -p decdn-cli -p decdn-node
cargo nextest run -p decdn-e2e --features anvil-e2e
```

## Install

These commands work from the first tagged release (see [Status](#status)):

```bash
cargo install --locked decdn-cli decdn-node
docker pull ghcr.io/decdn/decdn-node     # mirrored to docker.io/decdn/decdn-node
```

Each [GitHub release](https://github.com/decdn/decdn/releases) also carries
`decdn-${VERSION}-${TARGET}` and `decdn-node-${VERSION}-${TARGET}` archives (`.tar.gz`,
or `.zip` on Windows). A maintainer signs the `SHA256SUMS` manifest that covers the
archives and the digest that names the container image. crates.io artifacts carry no
maintainer signature. [SECURITY.md](SECURITY.md) explains how to verify each channel.

## Commands

deCDN ships two binaries, split like `dockerd` and `docker`
([appendix](adr/appendix-binaries.md)):

- **`decdn-node`**: the daemon. It has one subcommand, `decdn-node run [--config <path>]`.
- **`decdn`**: every command a human types.

| `decdn` command | Purpose |
|-----------------|---------|
| `key-gen`, `whoami` | Generate, then inspect, the Ed25519 node key and Ethereum keystore |
| `config {init,validate}` | Write a config file; check one before the daemon reads it |
| `setup` | Guided operator onboarding over `key-gen`, `node bond`, and `node register` |
| `node {…}` | Admin of a running daemon (`health`, `status`, `lanes`, `pools`, `top`, `evict`, `reload`, `drain`, `slashes`); the on-chain lifecycle (`bond`, `unbond`, `register`, `deregister`, `rotate-key`, `update-multiaddrs`, `update-region`); diagnostics (`doctor`, `lookup`) |
| `probe`, `fetch` | Probe a node over `cdn/probe/v1`; paid single-blob fetch over `cdn/client/v1` |
| `bundle {pull}` | Fetch a directory bundle by manifest ([appendix](adr/appendix-bundles.md)) |
| `pool {…}` | Client payment-pool lifecycle: `list`, `open`, `top-up`, `close`, `reclaim`, `assign` |
| `publish {…}` | Publisher control plane: create namespaces; seat and unseat authorized origins |
| `origin {import}` | Write local content into a local filesystem origin store, offline and config-free |
| `appeal {slash}` | File a slash appeal and post the appeal bond ([ADR 028](adr/028-slashing-appeals.md)) |

Run `decdn <command> --help` for flags. Every chain endpoint and contract address comes
from the config file, and each command also accepts it as a flag (`--rpc-url`,
`--*-address`). Operator terms accepted at registration live in
[`crates/cli/TERMS.md`](crates/cli/TERMS.md), embedded verbatim in the CLI.

## Architecture

### Wire protocols

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency and availability probing |
| `cdn/client/v1` | All paid delivery, client→node and node→node |
| `cdn/dht/v1` | Content discovery between nodes via a Kademlia DHT ([ADR 022](adr/022-content-discovery.md)) |

### Crates

```text
crates/
  node/         — daemon binary `decdn-node`: runtime bring-up, handlers, admin RPC server, dispatch limiter
  cli/          — user CLI binary `decdn`
  common/       — config schema + resolver, identity loading, AdminRpc trait + DTOs, clap definitions for both binaries
  protocol/     — wire format and ALPN message definitions (leaf crate)
  config-types/ — config-vocabulary value types shared by cache + common (leaf crate)
  bao-range/    — bao verified-range helpers and the origin-store layout (leaf crate, ADR 038)
  cache/        — cache engine wrapping iroh-blobs + origin pull-through
  client/       — `cdn/client/v1` paid-pull requester and buyer-side pool open, shared by node and cli
  incentive/    — payment pools, staking, vouchers (alloy for Ethereum)
  reputation/   — local per-peer reputation scoring (ADR 008)
  e2e/          — test-only Rust↔contract fixtures (anvil + the production deploy script)
contracts/      — Solidity contracts + Foundry (excluded from the Cargo workspace)
```

The daemon is itself a paying client on its upstream cache-miss leg, so `node` depends on
`client` like any other consumer. The three leaf crates keep the publisher CLI free of any
blob store or AWS SDK. `.github/scripts/check_crate_edges.py` enforces that boundary in CI.

The contracts cover operator bonding, USDC settlement and the fee split, buyback-and-burn,
served-bytes governance, slashing and appeals, content takedown, and publisher namespaces.
[`contracts/README.md`](contracts/README.md) lists each contract, and
[ADR 016 § Contract Inventory](adr/016-contract-interactions.md#contract-inventory) maps
how they interact.

## Design at a glance

| Area | Design | ADRs |
|------|--------|------|
| Content | Blobs are BLAKE3-addressed, and the receiver verifies every byte. The hash is a Merkle root, so any byte range verifies on its own, which makes resume and multi-source fetch safe. Publishers may encrypt content before upload; key distribution stays outside the protocol. | [002](adr/002-content-addressing.md), [038](adr/038-bao-verified-range-streaming.md) |
| Discovery | Clients never query the DHT: they pick bonded nodes from their peer store or the `CapacityBond` registry by measured RTT. Nodes find holders for a cache miss through the `cdn/dht/v1` Kademlia DHT; for a namespaced request, the on-chain origin set is the directory of last resort. Latency-driven proxy warming creates regional copies where demand is. | [012](adr/012-client.md), [022](adr/022-content-discovery.md), [037](adr/037-regional-proxy-warming.md) |
| Delivery | A large fetch spreads disjoint byte ranges across several holders, at most one per operator, and reassigns ranges as sources finish or stall. Cache admission and eviction are node-local. A node refuses a cache miss whose upstream price leaves no margin after the fee split. | [039](adr/039-multi-source-parallel-fetch.md), [040](adr/040-cache-policy.md), [041](adr/041-refuse-to-serve.md) |
| Payments | A client funds one shared `PaymentPool` and pays each provider with cumulative off-chain vouchers. Only the provider redeems its own vouchers, so a pool owner's close cannot understate what a node earned. A close starts a grace window in which nodes keep redeeming, and each node's periodic sweep redeems well inside it. | [003](adr/003-payments.md) |
| Tokenomics | USDC pays for bytes. Operators bond TOKEN on a super-linear curve over declared bandwidth, with a flat `minBond` floor. Fee-split buckets are governance-tunable. Protocol-owned liquidity is a Uniswap V3 50/50 TOKEN/USDC pool that governance seeds after launch. | [026](adr/026-tokenomics.md), [016](adr/016-contract-interactions.md), [018](adr/018-liquidity-strategy.md) |
| Enforcement | Every `ProbeResponse` and `StreamResponse` carries a secp256k1 EIP-712 `slash_sig`, so a node's quotes and deliveries are on-chain evidence; the Ed25519 NodeId is connection identity only. A challenge is a commit–reveal that resolves on-chain at reveal time. Slashed TOKEN sits in escrow for a 30-day appeal window. Governance blacklists content hashes. Reputation is a local per-peer EWMA. | [014](adr/014-on-chain-verification.md), [028](adr/028-slashing-appeals.md), [011](adr/011-content-takedown.md), [008](adr/008-reputation.md) |
| Governance | `DecdnGovernor` with a `TimelockController`. No TOKEN balance carries a vote; a bond only makes an operator eligible. Vote weight is trailing-window served bytes, capped per epoch by declared capacity and overall by a per-operator share, ramped by tenure since first bond, and zeroed by a standing slash. The process moves from admin key to bootstrap multisig to DAO; the contract set stays the same. | [009](adr/009-governance.md), [036](adr/036-served-bytes-voting-weight.md) |
| Evolution | Varint-length framing, protocol enums, and a three-tier schema-evolution model. Production targets Arbitrum One. | [013](adr/013-schema-evolution.md), [L2 appendix](adr/appendix-l2-deployment.md) |

For the full design, start at [`adr/README.md`](adr/README.md). It gives the reading order,
and [`adr/architecture.md`](adr/architecture.md) is the living overview.

## Documentation

| Topic | Where |
|-------|-------|
| Protocol specification (ADRs) and glossary | [`adr/`](adr/), [`adr/glossary.md`](adr/glossary.md) |
| Operating a node | [Operator runbook](docs/runbook.md), [key rotation](adr/appendix-operator-key-rotation.md), [upgrade path](adr/appendix-operator-upgrade-path.md) |
| Monitoring | [`monitoring/`](monitoring/) dashboards and alert rules; [metric surface](adr/appendix-observability.md) |
| Contracts | [`contracts/README.md`](contracts/README.md) |
| Contributing | [CONTRIBUTING.md](CONTRIBUTING.md): environment, build, test, code style, ADR conventions |
| Releases | [RELEASING.md](RELEASING.md): cutting, signing, and publishing a release |
| Security | [SECURITY.md](SECURITY.md): reporting a vulnerability, verifying a release |

## License

Dual-licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
