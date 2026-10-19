# A sponsord server image built from source, for a fork or an unreleased
# commit. The server images each release publishes (ghcr.io/decdn/sponsord,
# -onramp) come from the root Dockerfile instead, which packages the signed
# release binaries. Same runtime user (uid 1000) and layout, so either works
# with deploy/sponsord/compose.yaml.
#
# From the repository root, `--build-arg BIN=sponsord` or `BIN=sponsord-onramp`:
#
#   docker build -f deploy/sponsord/Dockerfile --build-arg BIN=sponsord -t sponsord .
#
# The toolchain tag is the workspace's pinned toolchain (rust-toolchain.toml);
# check_toolchain_pin.py holds it to the same version.
FROM rust:1.99.0-trixie AS build
ARG BIN
WORKDIR /src
COPY . .
RUN test -n "$BIN" \
 && cargo build --release --locked -p "$BIN" \
 && cp "target/release/$BIN" /usr/local/bin/server

FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 1000 decdn \
 && useradd --uid 1000 --gid decdn --create-home decdn
COPY --from=build /usr/local/bin/server /usr/local/bin/server
USER decdn
WORKDIR /home/decdn
ENTRYPOINT ["/usr/local/bin/server"]
