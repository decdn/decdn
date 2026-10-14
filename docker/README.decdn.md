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
