# One image per server: build with `--build-arg BIN=sponsord` or
# `--build-arg BIN=sponsord-onramp`, from the repository root:
#
#   docker build -f deploy/Dockerfile --build-arg BIN=sponsord -t sponsord .
#
# deploy/compose.yaml builds both.

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
 && useradd --system --uid 10001 --no-create-home sponsord
COPY --from=build /usr/local/bin/server /usr/local/bin/server
USER sponsord
ENTRYPOINT ["/usr/local/bin/server"]
