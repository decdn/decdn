# The images are assembled from the release binaries, not compiled from source.
#
# A server's release (`sponsord-v*` or `sponsord-onramp-v*`) builds its Linux
# archives in `.github/workflows/release.yml`'s `build` job; the `docker` job
# then unpacks the two into dist/<arch>/ and builds the matching target of this
# file. The binary in an image is therefore byte-identical to the one in
# <binary>-<version>-<target>.tar.gz, which the signed SHA256SUMS covers;
# compiling here would produce a second, unrelated binary that the release
# signature says nothing about.
#
# Two final targets, one per server: `sponsord` and `sponsord-onramp`. Build one
# yourself the same way:
#   mkdir -p dist/amd64
#   cargo build --release -p sponsord
#   cp target/release/sponsord dist/amd64/
#   docker build --target sponsord -t sponsord .
#
# To build an image from source instead (a fork, an unreleased commit), use
# deploy/Dockerfile.

# Pinned by digest, not just by tag: `bookworm-slim` moves, and a release is a
# maintainer signing exact bytes. The digest is also what lets Dependabot's
# `docker` ecosystem open bump PRs. It is the multi-arch index digest, so
# linux/amd64 and linux/arm64 both resolve from it. glibc 2.36, above the
# 2.35 the release binaries are built against on ubuntu-22.04.
FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 sponsord && \
    useradd --uid 1000 --gid sponsord --create-home sponsord

# The signing daemon. Stateless; its configuration is SPONSORD_* environment
# variables (docs/operator.md), its secrets mounted files named by
# SPONSORD_API_TOKEN_FILE, SPONSORD_TREASURY_PASSWORD_FILE and
# SPONSORD_TREASURY_KEYSTORE.
FROM base AS sponsord
# TARGETARCH is set by buildx per platform: amd64 or arm64. The dist/ layout is
# keyed on it rather than on the Rust target triple, so the COPY needs no
# per-platform branching.
ARG TARGETARCH
COPY --chmod=0755 dist/${TARGETARCH}/sponsord /usr/local/bin/sponsord
USER sponsord
WORKDIR /home/sponsord
# The binary defaults to loopback; inside a container that is unreachable.
ENV SPONSORD_BIND=0.0.0.0:8090
# HTTP API (/v1/capabilities, /v1/info, /healthz, /metrics). Keep this comment
# on its own line: a trailing comment after EXPOSE is a parse error.
EXPOSE 8090
ENTRYPOINT ["sponsord"]

# The public onramp. Stateless too: it holds capabilities in memory only for
# the browser-to-CLI hand-off, so it needs no volume.
FROM base AS sponsord-onramp
ARG TARGETARCH
COPY --chmod=0755 dist/${TARGETARCH}/sponsord-onramp /usr/local/bin/sponsord-onramp
USER sponsord
WORKDIR /home/sponsord
ENV ONRAMP_BIND=0.0.0.0:8080
# /healthz, /decdn.sh, /decdn.ps1, /fund, /v1/fund, /v1/capability, /v1/profile.
EXPOSE 8080
ENTRYPOINT ["sponsord-onramp"]
