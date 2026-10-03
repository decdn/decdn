# A server image built from source, for a fork or an unreleased commit. The
# images each release publishes (ghcr.io/decdn/sponsord, -onramp) come from the
# root Dockerfile instead, which packages the signed release binaries. Same
# runtime user (uid 1000) and layout, so either works with deploy/compose.yaml.
#
# From the repository root, `--build-arg BIN=sponsord` or `BIN=sponsord-onramp`:
#
#   docker build -f deploy/Dockerfile --build-arg BIN=sponsord -t sponsord .

FROM rust:1.95-bookworm AS build
ARG BIN
WORKDIR /src
COPY . .
RUN test -n "$BIN" \
 && cargo build --release --locked -p "$BIN" \
 && cp "target/release/$BIN" /usr/local/bin/server

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 1000 sponsord \
 && useradd --uid 1000 --gid sponsord --create-home sponsord
COPY --from=build /usr/local/bin/server /usr/local/bin/server
USER sponsord
WORKDIR /home/sponsord
ENTRYPOINT ["/usr/local/bin/server"]
