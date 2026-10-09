# The images are assembled from the release binaries, not compiled from source.
#
# A release (`v*`) builds each binary's Linux archives in `.github/workflows/release.yml`'s `build` job; the `docker` job
# then unpacks the two into dist/<arch>/ and builds the matching target of this
# file. The binary in an image is therefore byte-identical to the one in
# <binary>-<version>-<target>.tar.gz, which the signed SHA256SUMS covers;
# compiling here would produce a second, unrelated binary that the release
# signature says nothing about. (The decdn-sponsored image's second binary,
# `decdn`, is decdn's own: see that target.)
#
# Three final targets, one per binary: `sponsord`, `sponsord-onramp` and
# `decdn-sponsored`. Build one yourself the same way:
#   mkdir -p dist/amd64
#   cargo build --release -p sponsord
#   cp target/release/sponsord dist/amd64/
#   docker build --target sponsord -t sponsord .
#
# `decdn-sponsored` also needs the `decdn` it spawns, from decdn's own release
# at the tag Cargo.toml pins; .github/scripts/fetch-decdn.sh puts it in
# dist/<arch>/:
#   cargo build --release -p decdn-sponsored
#   cp target/release/decdn-sponsored dist/amd64/
#   .github/scripts/fetch-decdn.sh dist
#   docker build --target decdn-sponsored -t decdn-sponsored .
#
# To build a server image from source instead (a fork, an unreleased commit),
# use deploy/Dockerfile.

# Pinned by digest, not just by tag: `bookworm-slim` moves, and a release is a
# maintainer signing exact bytes. The digest is also what lets Dependabot's
# `docker` ecosystem open bump PRs. It is the multi-arch index digest, so
# linux/amd64 and linux/arm64 both resolve from it. glibc 2.36, above the
# 2.35 the release binaries are built against on ubuntu-22.04, and above the
# floor decdn's release sets for `decdn`.
FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS base

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

# The end-user CLI, for a download inside a container. Someone still opens the
# printed link to pass the onramp's gate: each download has its own key and
# its own check. Configured by
# environment instead of the installer's ~/.decdn/sponsor.toml:
# DECDN_SPONSOR_ONRAMP_URL (required), DECDN_SPONSOR_DECDN_BIN and
# DECDN_SPONSOR_DATA_DIR (crates/decdn-sponsored/README.md).
FROM base AS decdn-sponsored
ARG TARGETARCH
COPY --chmod=0755 dist/${TARGETARCH}/decdn-sponsored /usr/local/bin/decdn-sponsored
# The `decdn` binary the CLI hands each pull to. It is not one of this
# release's archives: release.yml's docker job fetches it from decdn's release
# at the tag Cargo.toml pins, verified against decdn's signed SHA256SUMS
# (.github/scripts/fetch-decdn.sh).
COPY --chmod=0755 dist/${TARGETARCH}/decdn /usr/local/bin/decdn
USER sponsord
WORKDIR /home/sponsord
# Created as the runtime user so a named volume mounted here starts owned by
# it; Docker would otherwise create the mount point as root.
RUN mkdir -p /home/sponsord/.decdn
# No volume: what the CLI keeps under ~/.decdn (an interrupted download's key
# and capability, and decdn's peer and channel stores) only lets a later run
# resume or start faster, so mounting it is the user's choice. The CLI only
# dials out, so it exposes no port. A bare `docker run <image>` prints usage
# and exits 0.
ENTRYPOINT ["decdn-sponsored"]
CMD ["--help"]
