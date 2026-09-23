# decdn-client

Part of [deCDN](https://github.com/decdn/decdn) — a decentralized CDN where nodes cache and serve BLAKE3-addressed blobs over [iroh](https://iroh.computer) QUIC, and clients pay per megabyte in USDC over off-chain payment channels.

> **Status: early implementation.** deCDN is pre-launch — no network is deployed. Wire formats, APIs and on-chain interfaces change without compatibility shims.

The deCDN client SDK: fetch BLAKE3-addressed blobs from deCDN nodes, pay for them per megabyte
from a USDC payment pool, and verify every byte as it arrives. Two entry points cover most uses:

- `Downloader` saves a blob, or a set of blobs, to files at full throughput across every holder,
  and resumes a partial download.
- `Streamer` reads one blob in order, and pays only for what the reader reaches plus one
  read-ahead window.

The crate documentation has the setup sequence, the guarantees, and the mistakes to avoid. The
`download` and `stream` examples run the whole sequence end to end:

```bash
cargo run -p decdn-client --example download -- <blake3-hash> <output-path>
```

The `decdn` CLI's `fetch` and `bundle pull` are built on this crate, and so is a node's own
cache-miss pull: a node that misses in its cache is a paying client of the node upstream of it.
The crate links no blob store and no AWS SDK, and CI checks that its dependencies stay that way.
Its public API is snapshot-tested in CI.

## License

Licensed under either of [Apache-2.0](https://github.com/decdn/decdn/blob/main/LICENSE-APACHE)
or [MIT](https://github.com/decdn/decdn/blob/main/LICENSE-MIT) at your option.
