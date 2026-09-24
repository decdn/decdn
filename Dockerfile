# The image is assembled from the release binaries, not compiled from source.
#
# `.github/workflows/release.yml` builds every target once in `build-binaries`,
# then the `docker` job unpacks the two Linux `decdn-node` archives into
# dist/<arch>/ and builds this file. That means the binary in the image is
# byte-identical to the one in decdn-node-<version>-<target>.tar.gz, which the
# signed SHA256SUMS covers — compiling here instead would produce a second,
# unrelated binary that the release signature says nothing about.
#
# Build it yourself the same way:
#   mkdir -p dist/amd64
#   cargo build --release -p decdn-node
#   cp target/release/decdn-node dist/amd64/
#   docker build -t decdn-node .
# Pinned by digest, not just by tag. `bookworm-slim` is a moving target, so two
# builds of the same release tag could ship different base layers — which sits
# badly with a release whose whole story is that a maintainer signs exact bytes.
# The digest is also what makes the `docker` Dependabot ecosystem useful here:
# it can bump a digest, but it cannot derive a version from `bookworm-slim`, so
# without this pin it would open no PRs at all. This is the multi-arch index
# digest, so linux/amd64 and linux/arm64 both resolve from it.
FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 decdn && \
    useradd --uid 1000 --gid decdn --create-home decdn

# TARGETARCH is set by buildx per platform: amd64 or arm64. The dist/ layout is
# keyed on it rather than on the Rust target triple so this COPY needs no
# per-platform branching.
ARG TARGETARCH
COPY --chmod=0755 dist/${TARGETARCH}/decdn-node /usr/local/bin/decdn-node

USER decdn
WORKDIR /home/decdn

VOLUME ["/home/decdn/.decdn"]

# QUIC transport, Prometheus metrics. Keep this comment on its own line —
# Dockerfile `#` only starts a comment at the beginning of a line, so a trailing
# one is parsed as arguments and fails with `invalid containerPort: #`.
EXPOSE 4433 9090

# Container ships the daemon only. Operators wanting the user CLI
# (`decdn fetch`, `decdn node …`, `decdn key-gen`) install it from the
# `decdn-${VERSION}-${TARGET}.tar.gz` release archive.
#
# `CMD ["run"]` makes `docker run <image>` start the daemon by default;
# operators can still override (`docker run <image> --version`,
# `docker run <image> run --config /etc/decdn/node.toml`, etc.). Without
# CMD, the daemon-only `decdn-node` binary would print clap usage and
# exit when invoked with no arguments.
ENTRYPOINT ["decdn-node"]
CMD ["run"]
