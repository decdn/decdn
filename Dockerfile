# ---- Stage 1: Build ----
FROM rust:1.85-bookworm AS builder

WORKDIR /build

# Copy manifests first for dependency layer caching
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo .cargo
COPY crates/node/Cargo.toml crates/node/Cargo.toml
COPY crates/protocol/Cargo.toml crates/protocol/Cargo.toml
COPY crates/cache/Cargo.toml crates/cache/Cargo.toml
COPY crates/incentive/Cargo.toml crates/incentive/Cargo.toml
COPY crates/reputation/Cargo.toml crates/reputation/Cargo.toml
COPY crates/common/Cargo.toml crates/common/Cargo.toml
COPY crates/cli/Cargo.toml crates/cli/Cargo.toml

# Create stub sources so cargo can resolve the workspace and fetch deps
RUN mkdir -p crates/node/src crates/protocol/src crates/cache/src \
             crates/incentive/src crates/reputation/src \
             crates/common/src crates/cli/src && \
    echo "fn main() {}" > crates/node/src/main.rs && \
    echo "fn main() {}" > crates/cli/src/main.rs && \
    touch crates/protocol/src/lib.rs crates/cache/src/lib.rs \
          crates/incentive/src/lib.rs crates/reputation/src/lib.rs \
          crates/common/src/lib.rs crates/cli/src/lib.rs

# Build dependencies only (cached until Cargo.toml/Cargo.lock change)
RUN cargo build --release --package decdn-node || true

# Copy actual source and rebuild
COPY crates crates
RUN touch crates/node/src/main.rs && \
    cargo build --release --package decdn-node

# ---- Stage 2: Runtime ----
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 decdn && \
    useradd --uid 1000 --gid decdn --create-home decdn

COPY --from=builder /build/target/release/decdn-node /usr/local/bin/decdn-node

USER decdn
WORKDIR /home/decdn

VOLUME ["/home/decdn/.decdn"]

EXPOSE 4433 9090  # QUIC transport, Prometheus metrics

# Container ships the daemon only. Operators wanting the user CLI
# (`decdn pull`, `decdn node …`, `decdn key-gen`) install it from the
# `decdn-${VERSION}-${TARGET}.tar.gz` release archive.
#
# `CMD ["run"]` makes `docker run <image>` start the daemon by default;
# operators can still override (`docker run <image> --version`,
# `docker run <image> run --config /etc/decdn/node.toml`, etc.). Without
# CMD, the daemon-only `decdn-node` binary would print clap usage and
# exit when invoked with no arguments.
ENTRYPOINT ["decdn-node"]
CMD ["run"]
