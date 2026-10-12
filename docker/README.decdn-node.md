<!-- description: deCDN node daemon, with the decdn CLI for admin. Caches and serves BLAKE3-addressed blobs. -->
# decdn-node

The [deCDN](https://github.com/decdn/decdn) node daemon. A deCDN node caches and serves
BLAKE3-addressed blobs over [iroh](https://iroh.computer/) QUIC, and clients pay it for
every megabyte in USDC. The image also carries the `decdn` CLI, so you can reach the
daemon's local admin port with `docker exec`.

The network is a public testnet on Arbitrum Sepolia. Wire, config and storage formats
change without compatibility shims until the mainnet deployment.

## Quick start

```bash
docker pull decdn/decdn-node
docker run --rm decdn/decdn-node --version
```

With no arguments the container runs `decdn-node run`. It listens for QUIC on
`4433/udp`. It serves Prometheus metrics on port `9090`, bound to `127.0.0.1` inside the
container by default; set `DECDN_METRICS_BIND=0.0.0.0` to scrape it from outside. It keeps
its keystore, config and, by default, its cache in the `/home/decdn/.decdn` volume.

Check a running node from the CLI inside the same container:

```bash
docker exec <container> decdn node status
```

The image runs as uid 1000. If your host user has another uid, add
`--user "$(id -u):$(id -g)" -e HOME=/home/decdn`. Without a TTY, mount a file that holds
the keystore password and point `DECDN_KEYSTORE_PASSWORD_FILE` at it.

## Run a node

- [Operator guide](https://docs.decdn.org/run-a-node/overview): set up, bond and run a node.
- [decdn/devops](https://github.com/decdn/devops): Docker Compose, Helm and Ansible
  deployments of this image.

## Tags

| Tag | Points at |
|-----|-----------|
| `latest` | the newest stable release |
| `<version>`, e.g. `0.2.1` | that release |
| `<major>.<minor>`, e.g. `0.2` | the newest stable release in that minor line |

A prerelease (for example `0.3.0-rc1`) gets only its exact version tag.

This image is built for linux/amd64 and linux/arm64. It is also on GitHub Container
Registry as
[`ghcr.io/decdn/decdn-node`](https://github.com/decdn/decdn/pkgs/container/decdn-node),
with the same digest.

## Verify the image

A maintainer signs each release's image digest with GPG, and every tag is created only
after that signature. To pin the exact bytes, import the maintainer keys from
[`KEYS`](https://github.com/decdn/decdn/blob/main/KEYS) (`gpg --import KEYS`). Download
`decdn-node-image-digest.txt` and its `.asc` from the
[release](https://github.com/decdn/decdn/releases/latest), then run:

```bash
gpg --verify decdn-node-image-digest.txt.asc decdn-node-image-digest.txt
docker pull "decdn/decdn-node@$(cut -d@ -f2 decdn-node-image-digest.txt)"
```

[SECURITY.md](https://github.com/decdn/decdn/blob/main/SECURITY.md#verify-the-container-images)
has the key fingerprints to check against and the full steps.

## Related images

- [`decdn/decdn`](https://hub.docker.com/r/decdn/decdn): the `decdn` CLI only, for
  publishers and clients.

## Links

- [Source and README](https://github.com/decdn/decdn)
- [Documentation](https://docs.decdn.org)
- [Releases](https://github.com/decdn/decdn/releases)
- License: MIT OR Apache-2.0
