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

The same image runs a public HTTP gateway that pays for every request from its own
pool, behind a proxy that terminates TLS for `decdn.link` and `*.decdn.link` (register
the host on the Public Suffix List, so the per-bundle sites' cookies stay apart):

```bash
docker run -d --name decdn-gateway -p 127.0.0.1:8080:8080 \
  -v ~/.decdn:/home/decdn/.decdn \
  -v /etc/decdn/keystore-password:/run/secrets/keystore-password:ro \
  -e DECDN_KEYSTORE_PASSWORD_FILE=/run/secrets/keystore-password \
  decdn/decdn gateway --public-url https://decdn.link --listen 0.0.0.0:8080 \
  --allowance-bytes-per-day 20GiB
```

The keystore under the volume funds the pool (`--working-deposit-micro-usdc` sizes it),
and `gateway/store` under the same volume keeps the pool and its lanes across restarts.
`--allowance-bytes-per-day` bounds the bytes the gateway delivers per UTC day. A request
past the allowance, or one whose size does not fit what is left, gets `402` with the
canonical `decdn:` URI to fetch with a paying client; a pool that cannot pay gets `503`.
A bundle entry with a size in its manifest takes one `Range` (`curl -r`, a resumed
download, a BEP 19 webseed `url-list`): the response is `206` over exactly that window,
paid for by the chunk-group-aligned span around it.

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
