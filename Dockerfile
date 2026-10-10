# The images are assembled from the release binaries, not compiled from source.
#
# Two of this file's stages are the published images:
#   * `decdn-node` (the default, last stage): the daemon, plus the `decdn` CLI so
#     an operator can run `docker exec <container> decdn node status` against the
#     daemon's local admin RPC.
#   * `decdn`: the CLI only, for publishers and clients (`decdn fetch`,
#     `decdn bundle pull`, `decdn publish …`).
#
# `.github/workflows/release.yml` builds every target once in `build-binaries`,
# then the `docker` job unpacks the Linux release archives into dist/<arch>/ and
# builds each stage. That means every binary in either image is byte-identical
# to the one in its `<binary>-<version>-<target>.tar.gz`, which the signed
# SHA256SUMS covers — compiling here instead would produce second, unrelated
# binaries that the release signature says nothing about.
#
# Build them yourself the same way:
#   mkdir -p dist/amd64
#   cargo build --release -p decdn-node -p decdn-cli
#   cp target/release/decdn-node target/release/decdn dist/amd64/
#   docker build -t decdn-node .
#   docker build --target decdn -t decdn .
# Pinned by digest, not just by tag. `bookworm-slim` is a moving target, so two
# builds of the same release tag could ship different base layers — which sits
# badly with a release whose whole story is that a maintainer signs exact bytes.
# The digest is also what makes the `docker` Dependabot ecosystem useful here:
# it can bump a digest, but it cannot derive a version from `bookworm-slim`, so
# without this pin it would open no PRs at all. This is the multi-arch index
# digest, so linux/amd64 and linux/arm64 both resolve from it.
FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS base

# `upgrade` applies the security fixes the pinned base predates. Debian
# publishes them to bookworm-security well before it rebuilds `bookworm-slim`,
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
# (`docker run <image> fetch --hash <hash> -o <path>`).
ENTRYPOINT ["decdn"]
CMD ["--help"]

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
