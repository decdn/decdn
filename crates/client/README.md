# decdn-client

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC with off-chain vouchers backed by a shared on-chain payment pool.

> **Status: early implementation.** deCDN runs a public testnet on Arbitrum Sepolia and is pre-mainnet. Wire formats, APIs and on-chain interfaces change without compatibility shims until mainnet.

The deCDN client SDK: fetch BLAKE3-addressed blobs from deCDN nodes, pay for them per megabyte
from a USDC payment pool, and verify every byte as it arrives. Two entry points cover most uses:

- `Downloader` saves a blob, or a set of blobs, to files at full throughput across every holder,
  and resumes a partial download.
- `Streamer` reads one blob in order, and pays only for what the reader reaches plus one
  read-ahead window.
- `bundle::BundlePuller` pulls a whole bundle manifest into a directory. It finds the holders,
  opens or reuses the pool (or pays under a delegated capability), skips and links what is
  already on disk, resumes partial files, and records what every lane paid.

The crate documentation has the setup sequence, the guarantees, and the mistakes to avoid. The
`download` and `stream` examples run the whole sequence end to end against a live deployment.
They read the deployment and the buyer from environment variables: `DECDN_RPC_URL`,
`DECDN_CHAIN_ID`, `DECDN_PAYMENT_POOL`, `DECDN_SLASH_JUDGE`, `DECDN_CAPACITY_BOND`,
`DECDN_KEYSTORE`, `DECDN_KEYSTORE_PASSWORD`, `DECDN_DATA_DIR`, and `DECDN_DEPOSIT` (the pool
deposit in USDC base units, used only when the buyer has no pool yet). The hash argument is the
blob's BLAKE3 hash as 64 hex characters:

```bash
cargo run -p decdn-client --example download -- <blake3-hash-hex> <output-path>
```

The `bundle` example pulls a bundle as a delegate. It reads the same deployment variables, the
delegate's keystore, and its capability token in `DECDN_CAPABILITY`. When the pull reports that
the capability runs low, it loads the next key and capability from `DECDN_KEYSTORE_NEXT` and
`DECDN_CAPABILITY_NEXT` and swaps them in without stopping the pull:

```bash
cargo run -p decdn-client --example bundle -- <bundle-hash-hex> <output-dir>
```

The `decdn` CLI's `get` is built on this crate: it parses a `decdn:` URI with `uri::DecdnUri`,
and its bundle route is a thin caller of `BundlePuller`. So is a node's own cache-miss pull: a
node that misses in its cache is a paying client of the node upstream of it.
The crate links no blob store and no AWS SDK, and CI checks that its dependencies stay that way.
Its public API is snapshot-tested in CI.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
