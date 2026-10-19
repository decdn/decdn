# The images are assembled from the release binaries, not compiled from source.
#
# Four of this file's stages are the published images:
#   * `decdn-node` (the default, last stage): the daemon, plus the `decdn` CLI so
#     an operator can run `docker exec <container> decdn node status` against the
#     daemon's local admin RPC.
#   * `decdn`: the CLI only, for publishers and clients (`decdn get`,
#     `decdn publish …`).
#   * `sponsord`: the sponsorship daemon, which signs capabilities against a
#     publisher's PaymentPool and keeps it topped up.
#   * `sponsord-onramp`: the public onramp in front of it, which serves the
#     `decdn.sh` / `decdn.ps1` installers behind a gate.
#
# `.github/workflows/release.yml` builds every target once in `build-binaries`,
# then the `docker` job unpacks the Linux release archives into dist/<arch>/ and
# builds each stage. That means every binary in any image is byte-identical
# to the one in its `<binary>-<version>-<target>.tar.gz`, which the signed
# SHA256SUMS covers — compiling here instead would produce second, unrelated
# binaries that the release signature says nothing about.
#
# Build them yourself the same way:
#   mkdir -p dist/amd64
#   cargo build --release -p decdn-node -p decdn-cli -p sponsord -p sponsord-onramp
#   cp target/release/{decdn-node,decdn,sponsord,sponsord-onramp} dist/amd64/
#   docker build -t decdn-node .
#   docker build --target decdn -t decdn .
#   docker build --target sponsord -t sponsord .
#   docker build --target sponsord-onramp -t sponsord-onramp .
#
# To build the sponsord images from source instead (a fork, an unreleased
# commit), use deploy/sponsord/Dockerfile.
# Pinned by digest, not just by tag. `trixie-slim` is a moving target, so two
# builds of the same release tag could ship different base layers — which sits
# badly with a release whose whole story is that a maintainer signs exact bytes.
# The digest is also what makes the `docker` Dependabot ecosystem useful here:
# it can bump a digest, but it cannot derive a version from `trixie-slim`, so
# without this pin it would open no PRs at all. For the same reason it never
# moves the base to the next Debian release. That move is a manual edit of the
# `FROM` line below, plus the codename in the `upgrade` comment under it and in
# `.github/dependabot.yml`. This is the multi-arch index digest, so linux/amd64
# and linux/arm64 both resolve from it.
FROM debian:trixie-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f AS base

# `upgrade` applies the security fixes the pinned base predates. Debian
# publishes them to trixie-security well before it rebuilds `trixie-slim`,
# so the digest alone ships known-fixed CVEs that the weekly Trivy image scan
# fails on. The release build runs this layer uncached, so every release takes
# the security updates current at build time.
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 decdn && \
    useradd --uid 1000 --gid decdn --create-home decdn

# The CLI image. It is also the base of the node image below, so both images
# share this layer and the node image has `decdn` on PATH.
FROM base AS decdn

# TARGETARCH is set by buildx per platform: amd64 or arm64. The dist/ layout is
# keyed on it rather than on the Rust target triple so this COPY needs no
# per-platform branching.
ARG TARGETARCH
COPY --chmod=0755 dist/${TARGETARCH}/decdn /usr/local/bin/decdn

USER decdn
WORKDIR /home/decdn

# Keystore and config. Both binaries resolve them under `$HOME/.decdn`, and the
# node image inherits this volume.
VOLUME ["/home/decdn/.decdn"]

# The CLI only dials out, so it exposes no port. `CMD ["--help"]` makes a bare
# `docker run <image>` print usage and exit 0; any arguments replace it
# (`docker run <image> get decdn:<hash> -o <path>`).
ENTRYPOINT ["decdn"]
CMD ["--help"]

# The sponsord daemon. Stateless: its configuration is `SPONSORD_*`
# environment variables (docs/sponsord/operator.md), its secrets mounted files
# named by SPONSORD_API_TOKEN_FILE, SPONSORD_TREASURY_PASSWORD_FILE and
# SPONSORD_TREASURY_KEYSTORE. No volume: it keeps nothing on disk.
FROM base AS sponsord

ARG TARGETARCH
COPY --chmod=0755 dist/${TARGETARCH}/sponsord /usr/local/bin/sponsord

USER decdn
WORKDIR /home/decdn

# The binary defaults to loopback; inside a container that is unreachable.
ENV SPONSORD_BIND=0.0.0.0:8090
# HTTP API (/v1/capabilities, /v1/info, /healthz, /metrics). Keep this comment
# on its own line: a trailing comment after EXPOSE is a parse error.
EXPOSE 8090
ENTRYPOINT ["sponsord"]

# The public onramp. Stateless too: it holds capabilities in memory only for
# the browser-to-installer hand-off, so it needs no volume either.
FROM base AS sponsord-onramp

ARG TARGETARCH
COPY --chmod=0755 dist/${TARGETARCH}/sponsord-onramp /usr/local/bin/sponsord-onramp

USER decdn
WORKDIR /home/decdn

ENV ONRAMP_BIND=0.0.0.0:8080
# /healthz, /decdn.sh, /decdn.ps1, /fund, /v1/fund, /v1/capability.
EXPOSE 8080
ENTRYPOINT ["sponsord-onramp"]

# The node image: the CLI image plus the daemon, with the daemon as entrypoint.
FROM decdn AS decdn-node

ARG TARGETARCH
COPY --chmod=0755 dist/${TARGETARCH}/decdn-node /usr/local/bin/decdn-node

# QUIC transport (UDP), Prometheus metrics. Keep this comment on its own line —
# Dockerfile `#` only starts a comment at the beginning of a line, so a trailing
# one is parsed as arguments and fails with `invalid containerPort: #`.
EXPOSE 4433/udp 9090

# `CMD ["run"]` makes `docker run <image>` start the daemon by default;
# operators can still override (`docker run <image> --version`,
# `docker run <image> run --config /etc/decdn/node.toml`, etc.). Without
# CMD, `decdn-node` would print clap usage and exit when invoked with no
# arguments.
ENTRYPOINT ["decdn-node"]
CMD ["run"]
