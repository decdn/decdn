<!-- description: deCDN CLI: fetch, pull and publish BLAKE3-addressed content on deCDN, paid in USDC. -->
# decdn

The [deCDN](https://github.com/decdn/decdn) command-line client, for publishers and
clients. deCDN nodes cache and serve BLAKE3-addressed blobs over
[iroh](https://iroh.computer/) QUIC. The CLI pays them for every megabyte in USDC and
verifies every byte against its content hash. It covers `decdn get`,
`decdn publish …` and the rest of the `decdn` commands.

The network is a public testnet on Arbitrum Sepolia. Wire, config and storage formats
change without compatibility shims until the mainnet deployment.

## Quick start

```bash
docker pull decdn/decdn
docker run --rm decdn/decdn --help
```

Mount your key directory and a working directory:

```bash
docker run --rm -it -v ~/.decdn:/home/decdn/.decdn -v "$PWD":/work -w /work \
  decdn/decdn get decdn:<hash> -o blob.bin
```

With no arguments the container prints `decdn --help`. Any arguments replace that. The
image keeps its keystore and config in the `/home/decdn/.decdn` volume.

The image runs as uid 1000. If your host user has another uid, add
`--user "$(id -u):$(id -g)" -e HOME=/home/decdn`. Without a TTY, mount a file that holds
the keystore password and point `DECDN_KEYSTORE_PASSWORD_FILE` at it.

## Public gateway

The same image runs a public HTTP gateway behind a proxy that terminates TLS for
`decdn.link` and `*.decdn.link` (register the host on the Public Suffix List, so the
per-bundle sites' cookies stay apart). The gateway holds no keystore and no pool: it
pays with the capabilities a pool owner registers to it, so the container keeps no
secret and you can replace it with nothing to tear down:

```bash
docker run -d --name decdn-gateway -p 127.0.0.1:8080:8080 \
  -v ~/.decdn:/home/decdn/.decdn \
  decdn/decdn gateway --public-url https://decdn.link --listen 0.0.0.0:8080 \
  --metrics-listen 127.0.0.1:9100
```

Fund it from the pool owner's machine: read the gateway's pending key, issue it a
capability, and register that:

```bash
KEY=$(curl -s https://decdn.link/_delegate | jq -r .address)
CAP=$(decdn pool assign --pool <pool-id> --signer "$KEY" --cap-micro-usdc 5000000 \
  --expiry-secs 86400 | grep '^dcap1:')
curl -X PUT https://decdn.link/_capability -H 'content-type: application/json' \
  -d "{\"capability\": \"$CAP\"}"
```

`GET /_capability` and `GET /_status` show each capability's cap, expiry and spend.
Register the next capability before the current one runs low or nears its expiry: the
gateway pays with the first one that can pay. The keys live in memory, so a restarted
gateway needs new capabilities. The routes take no authentication; put it on the proxy
if you want it.

`--metrics-listen` serves Prometheus text at `GET /metrics` on that loopback address, for
your scraper, and the gateway's state at `GET /_status` for `decdn dashboard --gateway`;
it answers only a loopback `Host`. The series and the state are listed in
[`adr/appendix-gateway.md`](https://github.com/decdn/decdn/blob/main/adr/appendix-gateway.md#metrics).

`gateway/cache` under the volume keeps what the gateway pulled, up to `--cache-mb`
(10 GiB by default), and serves a repeat request from it with no pull. A request no
capability can pay for gets `503` with the canonical `decdn:` URI to fetch with a paying
client. A bundle entry with a size in its manifest takes one `Range` (`curl -r`, a resumed
download, a BEP 19 webseed `url-list`): the response is `206` over exactly that window,
paid for by the chunk-group-aligned span around it.

Every answer is readable from any origin (`Access-Control-Allow-Origin: *`), so a web page
or the [TypeScript SDK](https://github.com/decdn/sdk-typescript) in a browser fetches
straight from the gateway. `OPTIONS` is a preflight that admits `Range` on content and a
JSON `PUT` to `/_pointer` and `/_capability` on the bare host. Let the proxy pass `OPTIONS` and the gateway's CORS
headers through unchanged, and add none of its own: a second `Access-Control-Allow-Origin`
makes a browser refuse the answer. See
[`adr/appendix-gateway.md`](https://github.com/decdn/decdn/blob/main/adr/appendix-gateway.md#cross-origin-reads).

A namespace is a stable host, `https://<ns>.decdn.link/`, once its owner points it at a
bundle: `decdn publish point <ns> decdn:<hash>/ --gateway https://decdn.link` signs a
pointer with the namespace owner's keystore and `PUT`s it; the gateway checks the
signer against `PublisherRegistry.ownerOf` and keeps the pointer in `gateway/pointers`
under the volume. The registry comes from `--publisher-registry-address`, the config
file, or the built-in testnet. See
[`adr/appendix-gateway.md`](https://github.com/decdn/decdn/blob/main/adr/appendix-gateway.md#namespace-pointer).

## Tags

| Tag | Points at |
|-----|-----------|
| `latest` | the newest stable release |
| `<version>`, e.g. `0.2.1` | that release |
| `<major>.<minor>`, e.g. `0.2` | the newest stable release in that minor line |

A prerelease (for example `0.3.0-rc1`) gets only its exact version tag.

This image is built for linux/amd64 and linux/arm64. It is also on GitHub Container
Registry as [`ghcr.io/decdn/decdn`](https://github.com/decdn/decdn/pkgs/container/decdn),
with the same digest.

## Verify the image

A maintainer signs each release's image digest with GPG, and every tag is created only
after that signature. To pin the exact bytes, import the maintainer keys from
[`KEYS`](https://github.com/decdn/decdn/blob/main/KEYS) (`gpg --import KEYS`). Download
`decdn-image-digest.txt` and its `.asc` from the
[release](https://github.com/decdn/decdn/releases/latest), then run:

```bash
gpg --verify decdn-image-digest.txt.asc decdn-image-digest.txt
docker pull "decdn/decdn@$(cut -d@ -f2 decdn-image-digest.txt)"
```

[SECURITY.md](https://github.com/decdn/decdn/blob/main/SECURITY.md#verify-the-container-images)
has the key fingerprints to check against and the full steps.

## Related images

- [`decdn/decdn-node`](https://hub.docker.com/r/decdn/decdn-node): the node daemon, with
  this CLI beside it.

## Links

- [Source and README](https://github.com/decdn/decdn)
- [Documentation](https://docs.decdn.org)
- [Releases](https://github.com/decdn/decdn/releases)
- License: MIT OR Apache-2.0
