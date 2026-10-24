<!-- markdownlint-disable-file MD033 MD041 -->
<p align="center">
  <a href="https://decdn.org">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset=".github/assets/wordmark-dark.svg">
      <img alt="deCDN" src=".github/assets/wordmark-light.svg" width="150">
    </picture>
  </a>
</p>

<h3 align="center">A decentralized CDN. Anyone who bonds can serve bytes; clients pay per megabyte.</h3>

<p align="center">
  Bonded operators cache and serve BLAKE3-addressed content over
  <a href="https://iroh.computer/">iroh</a> QUIC. Clients pay for every megabyte in USDC
  and verify every byte against its content hash, so any node can serve any blob without
  being trusted.
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/422fa510-2844-4f27-b3ff-649a36711792" alt="decdn get pulling content from the deCDN testnet">
</p>

<p align="center">
  <a href="https://github.com/decdn/decdn/actions/workflows/ci.yml?query=branch%3Amain"><img src="https://img.shields.io/github/actions/workflow/status/decdn/decdn/ci.yml?branch=main&label=CI&logo=github" alt="CI"></a>
  <a href="https://github.com/decdn/decdn/actions/workflows/security.yml?query=branch%3Amain"><img src="https://img.shields.io/github/actions/workflow/status/decdn/decdn/security.yml?branch=main&label=security&logo=github" alt="Security"></a>
  <a href="https://github.com/decdn/decdn/actions/workflows/codeql.yml?query=branch%3Amain"><img src="https://img.shields.io/github/actions/workflow/status/decdn/decdn/codeql.yml?branch=main&label=CodeQL&logo=github" alt="CodeQL"></a>
  <a href="https://github.com/decdn/decdn/releases/latest"><img src="https://img.shields.io/github/v/release/decdn/decdn?sort=semver&logo=github" alt="Release"></a>
  <a href="https://crates.io/crates/decdn-cli"><img src="https://img.shields.io/crates/v/decdn-cli?logo=rust&label=crates.io" alt="crates.io"></a>
  <a href="https://docs.rs/decdn-client"><img src="https://img.shields.io/docsrs/decdn-client?logo=docs.rs&label=docs.rs" alt="docs.rs"></a>
  <a href="https://hub.docker.com/r/decdn/decdn-node"><img src="https://img.shields.io/docker/v/decdn/decdn-node?sort=semver&logo=docker&label=docker" alt="Docker"></a>
  <a href="Cargo.toml"><img src="https://img.shields.io/crates/msrv/decdn-client?logo=rust&label=MSRV" alt="MSRV"></a>
  <a href="#license"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue" alt="License: MIT OR Apache-2.0"></a>
  <a href="https://discord.gg/vVbNswKVGX"><img src="https://img.shields.io/badge/chat-Discord-5865F2?logo=discord&logoColor=white" alt="Discord"></a>
</p>

<h3 align="center">
  <a href="https://decdn.org">Website</a> ·
  <a href="https://docs.decdn.org">Docs</a> ·
  <a href="https://stats.decdn.org">Network stats</a> ·
  <a href="https://discord.gg/vVbNswKVGX">Discord</a> ·
  <a href="https://x.com/decdn_">X</a>
</h3>

## ⚡ Try it in one line

Download Qwen2.5 0.5B Instruct, a 491 MB GGUF that runs in llama.cpp or ollama, from the
testnet. It takes about a minute on a 100 Mbps line. On macOS or Linux:

```bash
curl -fsSL https://up.decdn.org/decdn.sh | sh -s -- get 'decdn:d0a3eac609db6f49de12213574a302e42afd45f15b2cd89aa46b8a3e91f74d13/?ns=1'
```

On Windows, in PowerShell:

```powershell
& ([scriptblock]::Create((irm https://up.decdn.org/decdn.ps1))) get 'decdn:d0a3eac609db6f49de12213574a302e42afd45f15b2cd89aa46b8a3e91f74d13/?ns=1'
```

The file's SHA-256 is `74a4da8c9fdbcd15bd1f6d01d621410d31c6fc00986f5eb687824e7b93d7a9db`, the
one [Hugging Face](https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF) publishes. For a
bigger pull, Mistral 7B Instruct v0.3 is about 29 GB; either line takes its URI in place of
the one above:

```bash
curl -fsSL https://up.decdn.org/decdn.sh | sh -s -- get 'decdn:9194000d7b356650e6924a7746ec4afb0b705838b65913c0e46cba6b15af69e5/?ns=1'
```

The line installs `decdn` and asks you to pass one check in the browser (it opens the
link, or prints it). Then it downloads into the current directory and verifies every byte
on the way in. It refuses to overwrite a file it did not write itself; add `--overwrite`
after the URI to replace one. Run it again after an interruption and it resumes. A sponsor
pays for the transfer, so you need no wallet, keys, or account.
Later, `decdn self-update` moves the installed binary to the latest signed release.

The [catalogue on decdn.org](https://decdn.org/#try) lists more seeded content. To pay for
your own users' downloads the same way, run [sponsord](docs/sponsord/README.md), the
sponsorship daemon and onramp that ship with every release.

## ✨ Features

- **Verified bytes, untrusted nodes.** Content is BLAKE3-addressed and the hash is a Merkle
  root, so any byte range verifies on its own, which makes resume and multi-source fetch
  safe ([ADR 002](adr/002-content-addressing.md), [ADR 038](adr/038-bao-verified-range-streaming.md)).
- **Pay per megabyte.** A client funds one shared `PaymentPool` in USDC and pays each node
  with cumulative off-chain vouchers ([ADR 003](adr/003-payments.md)).
- **Anyone can serve.** Operators bond TOKEN against their declared bandwidth, register
  on-chain, then cache and serve content ([ADR 001](adr/001-network.md),
  [ADR 026](adr/026-tokenomics.md)).
- **Multi-source fetch.** A large fetch spreads disjoint byte ranges across several
  holders and reassigns ranges as sources finish or stall
  ([ADR 039](adr/039-multi-source-parallel-fetch.md)).
- **Private origins.** S3, R2, B2, HTTP, and filesystem backends are per-node config. No
  origin URL is ever exposed ([ADR 002](adr/002-content-addressing.md)).
- **Enforced on-chain.** Every quote and stream response carries a signature that serves
  as on-chain slash evidence, a slashed operator has 30 days to appeal, and governance
  blacklists content hashes
  ([ADR 014](adr/014-on-chain-verification.md), [ADR 028](adr/028-slashing-appeals.md),
  [ADR 011](adr/011-content-takedown.md)).
- **Governed by service.** No TOKEN balance carries a vote. Vote weight is trailing-window
  served bytes ([ADR 036](adr/036-served-bytes-voting-weight.md)).

## 🧭 Status

Public testnet on Arbitrum Sepolia; production targets Arbitrum One. The contracts ship at
full production shape, with governance-tunable economics from day one
([ADR 016](adr/016-contract-interactions.md)). The implementation is Rust. Signed release
archives and container images ship with each
[release](https://github.com/decdn/decdn/releases/latest), and crates publish to crates.io.

Wire, ABI, config, and storage formats change without compatibility shims until the
mainnet deployment. Many ADRs are still marked Draft, so the specification can change too.

## 📦 Install

```bash
cargo install --locked decdn-cli decdn-node
docker pull ghcr.io/decdn/decdn-node     # daemon + CLI; mirrored to docker.io/decdn/decdn-node
docker pull ghcr.io/decdn/decdn          # CLI only; mirrored to docker.io/decdn/decdn
docker pull ghcr.io/decdn/sponsord       # sponsorship daemon, for publishers who pay for their users' downloads
docker pull ghcr.io/decdn/sponsord-onramp # its public onramp; both mirrored to docker.io/decdn/…
```

Each [GitHub release](https://github.com/decdn/decdn/releases) also carries
`decdn-${VERSION}-${TARGET}` and `decdn-node-${VERSION}-${TARGET}` archives (`.tar.gz`,
or `.zip` on Windows), and `sponsord-…` and `sponsord-onramp-…` archives for Linux.
[SECURITY.md](SECURITY.md) explains how to verify each channel.

A `decdn` installed from an archive or by the one-liner updates itself:
`decdn self-update` fetches the latest release, verifies its `SHA256SUMS.minisig`
against a maintainer key the binary embeds, and replaces the binary in place. A binary
`cargo install` placed, or one in a directory you cannot write (the container
image, a package), is refused with the command to run instead. Once a day, after
a command succeeds with the terminal as its output, `decdn` prints one line when a
newer release exists; `[self_update] check = false` in `node.toml` or
`DECDN_SELF_UPDATE_CHECK=0` turns that off. `decdn-node` never updates itself.

<details>
<summary><b>Running the container images</b></summary>

The `decdn-node` image runs the daemon and also carries the CLI, so
`docker exec <container> decdn node status` reaches the daemon's local admin
port. The `decdn` image runs the CLI. Mount your key directory and a working
directory:

```bash
docker run --rm -it -v ~/.decdn:/home/decdn/.decdn -v "$PWD":/work -w /work \
  ghcr.io/decdn/decdn get decdn:<hash> -o blob.bin
```

Both images run as uid 1000. If your user has another uid, add
`--user "$(id -u):$(id -g)" -e HOME=/home/decdn`. Without a TTY, mount a file
that holds the keystore password and point `DECDN_KEYSTORE_PASSWORD_FILE` at it.

A maintainer signs the `SHA256SUMS` manifest that covers the release archives, and each
container image's digest file and SBOM. crates.io artifacts carry no maintainer signature.

</details>

<details>
<summary><b>Build from source</b></summary>

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

</details>

## 🚀 Quick start

### Fetch with your own wallet

A client uses its own keystore under `~/.decdn/client`, separate from any node identity
on the same host. Fund the address that `key-gen` prints with Arbitrum Sepolia ETH and
USDC.

```bash
decdn config init --client --output ~/.decdn/client.toml
decdn key-gen --output-dir ~/.decdn/client
decdn --config ~/.decdn/client.toml get decdn:<blake3-hash> -o ./blob.bin
```

`get` pays each node it uses from a pool you own. It reuses your existing pool or opens one with a 10 USDC working deposit, and it tops the
pool up on-chain without prompting when the balance runs low. Fund the wallet with only
what you are willing to spend.

<details>
<summary><b><code>decdn:</code> URIs, node selection, and overwrite rules</b></summary>

A `decdn:` URI names a blob, a whole bundle, or one file of a bundle
([appendix](adr/appendix-bundles.md#decdn-uris)):

| URI | `get` pulls | `-o` is | Without `-o` |
|-----|-------------|---------|--------------|
| `decdn:<hash>` | the blob | a file, or `-` for stdout | `./<hash>` |
| `decdn:<hash>/` | every file of the bundle whose manifest has this hash | a directory | `.` |
| `decdn:<hash>/a/b.txt` | that one file of the bundle | a directory, or `-` for stdout | `./b.txt` |
| any of these with `?ns=<id>` | the same, routed to a namespace's origins | the same | the same |

Without `-o`, a bundle or entry pull refuses to replace a file unless an earlier pull wrote it
there and it has not changed since. That earlier pull must be of the same entry, or have
written the same bytes. A new version of a bundle that changes a file is refused, so pass `-o .`
to update a bundle in place: the pull replaces the files that changed and skips the rest.
`--overwrite` fetches every file again.

`get` probes bonded nodes and picks one that holds the blob. When none does, it tries
bonded non-holders nearest first. Each fills the miss from its own origin, or from a peer
if it enables pull-through. A node with neither answers not-found, and `get` moves to
the next. One pool pays every provider, so there is no per-provider setup. Use
`decdn pool {…}` to inspect, close, or reclaim a pool.

</details>

### Run a node

You need Arbitrum Sepolia ETH for gas, TOKEN for the bond, and a keystore password source
for the headless daemon. The [operator guide](https://docs.decdn.org/run-a-node/overview)
covers [testnet funds](https://docs.decdn.org/run-a-node/testnet-funds), the
[bond](https://docs.decdn.org/run-a-node/bond-and-earnings)
([ADR 026 § Capacity-bond curve](adr/026-tokenomics.md#capacity-bond-curve)), and the
[full quickstart](https://docs.decdn.org/run-a-node/quickstart).

```bash
decdn config init            # writes ~/.decdn/node.toml with the testnet RPC and contract addresses
decdn setup --mbps 100 --region US --multiaddr /ip4/203.0.113.10/udp/4433/quic-v1
decdn-node run
decdn node health            # then: node status, node lanes, node top
```

`setup` generates the node key and keystore when neither exists, shows the operator terms,
bonds, registers the node, and prints a go/no-go summary
([ADR 019](adr/019-node-onboarding.md)).

<details>
<summary><b>Bond size, pull-through, IPv6, and serving your own content</b></summary>

- **Bond.** The bond is `max(minBond, bondRequired(mbps))`, where `bondRequired` is a curve
  over declared bandwidth, super-linear at the default exponent
  ([ADR 026 § Capacity-bond curve](adr/026-tokenomics.md#capacity-bond-curve),
  [§ Minimum bond](adr/026-tokenomics.md#minimum-bond-is-non-retroactive)). The testnet
  deployed with `minBond` at 50,000 TOKEN ([manifest](crates/cli/deployments/421614.json)),
  which at the default curve (k = 12.6, α = 1.2) covers declarations up to about
  997 Mbps; 1,000 Mbps needs about 50,162 TOKEN. `decdn node bond --mbps <n> --dry-run`
  prints the live target (it unlocks the keystore first). The maintainers send testnet TOKEN: send your EVM operator
  address (`decdn whoami` prints it) through any channel on the
  [contact page](https://decdn.org/#contact).
- **USDC.** The node's own buyer pool pays for cache-miss pulls from peers (10 USDC working
  deposit by default). The node spends it only with
  `cache.node_to_node_pull_through_enabled = true`, which is off by default. With it off,
  a cache miss the node cannot serve from its own backend returns not-found
  ([runbook](docs/runbook.md#node-to-node-pulls-never-succeed-buyer-wallet-or-pool)).
  Circle's [testnet faucet](https://faucet.circle.com) dispenses it.
- **Keystore password.** Set `DECDN_KEYSTORE_PASSWORD` or pass `--keystore-password-file`
  ([runbook](docs/runbook.md#keystore-will-not-unlock)).
- **Network.** The node listens on UDP 4433 over IPv4 and IPv6, so a dual-stack host also
  passes `--multiaddr /ip6/<addr>/udp/4433/quic-v1`. `--region` is an ISO 3166-1 alpha-2
  country code ([ADR 030](adr/030-node-region-self-attestation.md)).
- **Your own content.** Import it into a local origin store with
  `decdn origin import --input <path> --to <dir>`, then write the node config with
  `decdn config init --origin file://<absolute-dir>` (`--force` replaces an existing one).
  For an S3 origin, import to a local directory and `aws s3 sync` it to the bucket: the
  two layouts are identical.

</details>

### Publish content

A publisher creates a namespace and seats the bonded operators that serve as its origins.
`publish` moves no bytes. A client passes the namespace with `get decdn:<hash>?ns=<id>`,
and a node that misses the blob pulls it from one of the seated operators' origin backends
([ADR 005 § Namespace routing](adr/005-protocol.md#namespace-routing)).

```bash
decdn publish namespace create                     # prints the namespace id
decdn publish assign <namespace-id> <0xOPERATOR>...
```

The installed vetting policy decides who may seat origins
([ADR 011 § Vetting policies](adr/011-content-takedown.md#vetting-policies)). The testnet
runs the manual policy, so a `VETTER_ROLE` holder must vet the publisher's address before
`assign` succeeds. That step happens out-of-band, not through the CLI.

<details>
<summary><b>Local devnet</b></summary>

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

</details>

## 🏗️ How it works

```text
┌─────────────────── on-chain (Arbitrum) ────────────────────┐
│  CapacityBond   bonded TOKEN · registry of active nodes    │
│  PaymentPool    USDC deposits · voucher redemption         │
└─────▲──────────────────▲──────────────────────────▲────────┘
      │ deposit ·        │ bond · deposit · redeem  │ bond · redeem
      │ close · reclaim  │                          │
┌─────┴──────┐    ┌──────┴───────┐    ┌─────────────┴─┐    ┌──────────────────┐
│   Client   │◄──►│  Cache node  │◄──►│  Origin node  │◄───┤ S3/R2/B2/HTTP/fs │
└────────────┘    └──────────────┘    └───────────────┘    └──────────────────┘
    ◄─► bytes one way, USDC vouchers the other (cdn/client/v1)
```

- **Nodes** bond TOKEN against their declared bandwidth, register on-chain, then cache
  and serve content. The node set is the `CapacityBond` active set, read directly from the
  contract ([ADR 001](adr/001-network.md)).
- **Clients** pick nodes from their peer store or the on-chain registry by measured RTT,
  stream content, and pay each node with signed vouchers drawn against one pool deposit
  ([ADR 012](adr/012-client.md), [ADR 003](adr/003-payments.md)).
- **On a cache miss**, a node fills from its own origin backend if it has one. With node-to-node
  pull-through enabled (off by default), it can also pull from a peer and stream to the
  client at the same time. The node pays for that upstream pull from its own pool, like
  any other client: every byte transfer is paid.
- **Origin backends** (S3, R2, B2, HTTP, filesystem) are private per-node config. No
  origin URL is ever exposed ([ADR 002](adr/002-content-addressing.md)).

## 🧰 Commands

The node stack ships as two binaries, split like `dockerd` and `docker`
([appendix](adr/appendix-binaries.md)): **`decdn-node`** is the daemon, with one
subcommand, `run`; **`decdn`** is every command a human types. The sponsorship
daemon and its onramp are two more ([`docs/sponsord/`](docs/sponsord/README.md)). Run
`decdn <command> --help` for flags.

<details>
<summary><b>All <code>decdn</code> commands</b></summary>

| `decdn` command | Purpose |
|-----------------|---------|
| `key-gen`, `whoami` | Generate, then inspect, the Ed25519 node key and Ethereum keystore |
| `config {init,validate}` | Write a config file; check one before the daemon reads it |
| `setup` | Guided operator onboarding over `key-gen`, `node bond`, and `node register` |
| `node {…}` | Admin of a running daemon (`health`, `status`, `lanes`, `pools`, `top`, `evict`, `reload`, `drain`, `slashes`); the on-chain lifecycle (`bond`, `unbond`, `register`, `deregister`, `rotate-key`, `update-multiaddrs`, `update-region`); diagnostics (`doctor`, `lookup`) |
| `probe` | Probe a node over `cdn/probe/v1` |
| `get` | Paid pull over `cdn/client/v1` of what a `decdn:` URI names: a blob, a directory bundle, or one file of a bundle ([appendix](adr/appendix-bundles.md)) |
| `pool {…}` | Client payment-pool lifecycle: `list`, `open`, `top-up`, `close`, `reclaim`, `assign` |
| `publish {…}` | Publisher control plane: `namespace create`; seat and unseat authorized origins with `assign` and `revoke` |
| `origin {import}` | Write local content into a local filesystem origin store, offline and config-free |
| `appeal {slash}` | File a slash appeal and post the appeal bond ([ADR 028](adr/028-slashing-appeals.md)) |
| `self-update` | Replace this binary with the latest release whose `SHA256SUMS.minisig` the embedded key verifies; `--check` only reports |

The global `--config <path>` selects the config file, and every `decdn-node run` flag also
reads a `DECDN_*` environment variable (`decdn-node run --help`). Every chain endpoint and
contract address comes from the config file, and each command also accepts it as a flag
(`--rpc-url`, `--*-address`). Operator terms accepted at registration live in
[`crates/cli/TERMS.md`](crates/cli/TERMS.md), embedded verbatim in the CLI.

</details>

## 📐 Architecture

For the full design, start at [`adr/README.md`](adr/README.md). It gives the reading order,
and [`adr/architecture.md`](adr/architecture.md) is the living overview.

<details>
<summary><b>Wire protocols</b></summary>

| ALPN | Purpose |
|------|---------|
| `cdn/probe/v1` | Latency and availability probing |
| `cdn/client/v1` | All paid delivery, client→node and node→node |
| `cdn/dht/v1` | Content discovery between nodes via a Kademlia DHT ([ADR 022](adr/022-content-discovery.md)) |

</details>

<details>
<summary><b>Crates and contracts</b></summary>

```text
crates/
  node/         — daemon binary `decdn-node`: runtime bring-up, handlers, admin RPC server, dispatch limiter
  cli/          — user CLI binary `decdn`
  common/       — config schema + resolver, identity loading, AdminRpc trait + DTOs, the `decdn` clap tree and the daemon's `run` flags
  protocol/     — wire format and ALPN message definitions (leaf crate)
  config-types/ — config-vocabulary value types shared by cache + common (leaf crate)
  bao-range/    — bao verified-range helpers and the origin-store layout (leaf crate, ADR 038)
  cache/        — cache engine wrapping iroh-blobs + origin pull-through
  client/       — `cdn/client/v1` paid-pull requester and buyer-side pool open, shared by node and cli
  incentive/    — payment pools, staking, vouchers (alloy for Ethereum)
  reputation/   — local per-peer reputation scoring (ADR 008)
  e2e/          — test-only Rust↔contract fixtures (anvil + the production deploy script)
  sponsord/     — sponsorship daemon binary `sponsord`: signs download capabilities against a PaymentPool, keeps it topped up
  sponsord-onramp/ — public onramp binary `sponsord-onramp`: the gate, the `decdn.sh` / `decdn.ps1` installers, the hand-off
  sponsord-core/ — the daemon's logic as a library, built on `client` and `incentive`
  sponsord-api/ — sponsord wire types, error codes and typed HTTP clients (leaf crate)
  sponsord-e2e/ — test-only: the daemon, the onramp and an installer run together
contracts/      — Solidity contracts + Foundry (excluded from the Cargo workspace)
```

The daemon is itself a paying client on its upstream cache-miss leg, so `node` depends on
`client` like any other consumer; so does `sponsord-core`, which opens the pool a sponsor
pays from. The leaf crates keep the publisher CLI, the client SDK and the sponsord servers
free of any blob store or AWS SDK. `.github/scripts/check_crate_edges.py` enforces that
boundary in CI.

The contracts cover operator bonding, USDC settlement and the fee split, buyback-and-burn,
served-bytes governance, slashing and appeals, content takedown, and publisher namespaces.
[`contracts/README.md`](contracts/README.md) lists each contract, and
[ADR 016 § Contract Inventory](adr/016-contract-interactions.md#contract-inventory) maps
how they interact.

</details>

<details>
<summary><b>Design at a glance</b></summary>

| Area | Design | ADRs |
|------|--------|------|
| Content | Blobs are BLAKE3-addressed, and the receiver verifies every byte. The hash is a Merkle root, so any byte range verifies on its own, which makes resume and multi-source fetch safe. Publishers may encrypt content before upload; key distribution stays outside the protocol. | [002](adr/002-content-addressing.md), [038](adr/038-bao-verified-range-streaming.md) |
| Discovery | Clients never query the DHT: they pick bonded nodes from their peer store or the `CapacityBond` registry by measured RTT. Nodes find holders for a cache miss through the `cdn/dht/v1` Kademlia DHT; for a namespaced request, the on-chain origin set is the directory of last resort. Latency-driven proxy warming creates regional copies where demand is. | [012](adr/012-client.md), [022](adr/022-content-discovery.md), [037](adr/037-regional-proxy-warming.md) |
| Delivery | A large fetch spreads disjoint byte ranges across several holders, at most one per operator, and reassigns ranges as sources finish or stall. Cache admission and eviction are node-local. A node refuses a cache miss whose upstream price leaves no margin after the fee split. | [039](adr/039-multi-source-parallel-fetch.md), [040](adr/040-cache-policy.md), [041](adr/041-refuse-to-serve.md) |
| Payments | A client funds one shared `PaymentPool` and pays each provider with cumulative off-chain vouchers. Only the provider redeems its own vouchers, so a pool owner's close cannot understate what a node earned. A close starts a grace window in which nodes keep redeeming, and each node's periodic sweep redeems well inside it. | [003](adr/003-payments.md) |
| Tokenomics | USDC pays for bytes. Operators bond TOKEN on a governance-tunable curve over declared bandwidth (super-linear at the default exponent), with a flat `minBond` floor. Fee-split buckets are governance-tunable. Protocol-owned liquidity is a Uniswap V3 50/50 TOKEN/USDC pool that governance seeds after launch. | [026](adr/026-tokenomics.md), [016](adr/016-contract-interactions.md), [018](adr/018-liquidity-strategy.md) |
| Enforcement | Every `ProbeResponse` and `StreamResponse` carries a secp256k1 EIP-712 `slash_sig`, so a node's quotes and deliveries are on-chain evidence; the Ed25519 NodeId authenticates connections and the registration proof, never slash evidence. A challenge is a commit–reveal that resolves on-chain at reveal time. Slashed TOKEN sits in escrow for a 30-day appeal window. Governance blacklists content hashes. Reputation is a local per-peer EWMA. | [014](adr/014-on-chain-verification.md), [028](adr/028-slashing-appeals.md), [011](adr/011-content-takedown.md), [008](adr/008-reputation.md) |
| Governance | `DecdnGovernor` with a `TimelockController`. No TOKEN balance carries a vote. Vote weight is trailing-window served bytes, capped per epoch by declared capacity and overall by a per-operator share, ramped by tenure since first bond, and zeroed by a standing slash inside the trailing window. The process moves from admin key to bootstrap multisig to DAO; the contract set stays the same. | [009](adr/009-governance.md), [036](adr/036-served-bytes-voting-weight.md) |
| Evolution | Varint-length framing, protocol enums, and a three-tier schema-evolution model. Production targets Arbitrum One. | [013](adr/013-schema-evolution.md), [L2 appendix](adr/appendix-l2-deployment.md) |

</details>

## 📚 Documentation

| Topic | Where |
|-------|-------|
| Project overview | [decdn.org](https://decdn.org) |
| User and operator docs | [docs.decdn.org](https://docs.decdn.org) |
| Live testnet statistics | [stats.decdn.org](https://stats.decdn.org) |
| Protocol specification (ADRs) and glossary | [`adr/`](adr/), [`adr/glossary.md`](adr/glossary.md) |
| Operating a node | [Operator runbook](docs/runbook.md), [key rotation](adr/appendix-operator-key-rotation.md), [upgrade path](adr/appendix-operator-upgrade-path.md) |
| Monitoring | [Dashboards and alert rules](https://github.com/decdn/devops/tree/main/charts/decdn-node/files/monitoring) (in `decdn/devops`); [metric surface](adr/appendix-observability.md) |
| Contracts | [`contracts/README.md`](contracts/README.md) |
| Sponsored downloads | [`docs/sponsord/`](docs/sponsord/README.md): the daemon and onramp behind `decdn.sh` and `decdn.ps1` |
| Releases | [RELEASING.md](RELEASING.md): cutting, signing, and publishing a release |

## 🤝 Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) covers the environment, build, tests, code style, ADR
conventions, the issue forms and the label scheme. Report security vulnerabilities privately as
[SECURITY.md](SECURITY.md) describes.

## License

Dual-licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
