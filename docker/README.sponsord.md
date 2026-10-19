<!-- description: deCDN sponsorship daemon: signs capped download capabilities against a USDC PaymentPool. -->
# sponsord

The [deCDN](https://github.com/decdn/decdn) sponsorship daemon. A publisher runs it to
pay for their users' downloads: it signs capped, expiring capabilities against the
publisher's `PaymentPool` and keeps the pool topped up from a treasury wallet. Users
then download through the [onramp](https://hub.docker.com/r/decdn/sponsord-onramp)
without a wallet of their own.

The network is a public testnet on Arbitrum Sepolia. Wire, config and storage formats
change without compatibility shims until the mainnet deployment.

## Quick start

The daemon is stateless and configured by `SPONSORD_*` environment variables, with its
secrets in mounted files. The
[operator guide](https://github.com/decdn/decdn/blob/main/docs/sponsord/operator.md)
walks through them, and
[`deploy/sponsord/compose.yaml`](https://github.com/decdn/decdn/blob/main/deploy/sponsord/compose.yaml)
runs the daemon and the onramp together:

```bash
docker pull decdn/sponsord
docker run --rm decdn/sponsord --help
```

The image listens on `8090` (`/v1/capabilities`, `/v1/info`, `/healthz`, `/metrics`)
and runs as uid 1000. Keep it inside your network: only the onramp is public.

## Tags

| Tag | Points at |
|-----|-----------|
| `latest` | the newest stable release |
| `<version>`, e.g. `0.2.1` | that release |
| `<major>.<minor>`, e.g. `0.2` | the newest stable release in that minor line |

A prerelease (for example `0.3.0-rc1`) gets only its exact version tag. The version is
the deCDN release the daemon shipped with.

This image is built for linux/amd64 and linux/arm64. It is also on GitHub Container
Registry as [`ghcr.io/decdn/sponsord`](https://github.com/orgs/decdn/packages/container/package/sponsord),
with the same digest.

## Verify the image

A maintainer signs each release's image digest with GPG, and every tag is created only
after that signature. To pin the exact bytes, import the maintainer keys from
[`KEYS`](https://github.com/decdn/decdn/blob/main/KEYS) (`gpg --import KEYS`). Download
`sponsord-image-digest.txt` and its `.asc` from the
[release](https://github.com/decdn/decdn/releases/latest), then run:

```bash
gpg --verify sponsord-image-digest.txt.asc sponsord-image-digest.txt
docker pull "decdn/sponsord@$(cut -d@ -f2 sponsord-image-digest.txt)"
```

[SECURITY.md](https://github.com/decdn/decdn/blob/main/SECURITY.md#verify-the-container-images)
has the key fingerprints to check against and the full steps.

## Related images

- [`decdn/sponsord-onramp`](https://hub.docker.com/r/decdn/sponsord-onramp): the public
  onramp in front of this daemon.
- [`decdn/decdn-node`](https://hub.docker.com/r/decdn/decdn-node): the node daemon.
- [`decdn/decdn`](https://hub.docker.com/r/decdn/decdn): the CLI.

## Links

- [Source and README](https://github.com/decdn/decdn/blob/main/docs/sponsord/README.md)
- [Documentation](https://docs.decdn.org)
- [Releases](https://github.com/decdn/decdn/releases)
- License: MIT OR Apache-2.0
