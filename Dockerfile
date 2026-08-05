# ── Builder ──────────────────────────────────────────────────────────────────
FROM rust:1-bookworm AS builder

# tmq → zmq → libzmq: needed to build and link.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libzmq3-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Pre-build dependencies against a stub main so `cargo build` is cached across
# source-only changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Build the real binary.
COPY src ./src
RUN touch src/main.rs \
    && cargo build --release \
    && strip target/release/btcpool-rs

# ── Runtime ──────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# Runtime shared libs the binary links: libzmq5 (tmq/ZMQ) and libsqlite3
# (rusqlite stats DB — without it the dynamic linker fails before main()).
RUN apt-get update && apt-get install -y --no-install-recommends \
        libzmq5 libsqlite3-0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Run as a dedicated unprivileged user — the binary needs no root: the stratum
# and dashboard ports are >1024, and bitcoind's RPC cookie is read via a
# supplementary group supplied at run time (see docker-compose.yml / README).
# uid/gid 10001 is fixed so a host-side `chown` of the data volume stays valid
# across image rebuilds.
RUN groupadd --gid 10001 btcpool \
    && useradd --uid 10001 --gid 10001 --create-home --home-dir /home/btcpool \
        --shell /usr/sbin/nologin btcpool

COPY --from=builder /build/target/release/btcpool-rs /usr/local/bin/btcpool-rs

# /app is the working dir, so relative paths in config.toml (stats_db_path,
# found_block_dir) resolve under it; /app/data is the persistence mount point.
# Both are owned by the runtime user so the SQLite DB and found-block archives
# are writable.
RUN mkdir -p /app/data && chown -R btcpool:btcpool /app
WORKDIR /app

# The default `~/.bitcoin/.cookie` cookie_path now expands under the non-root
# home — mount your host cookie at /home/btcpool/.bitcoin/.cookie.
ENV HOME=/home/btcpool

USER btcpool

# Stratum (SV1 + SV2) and the dashboard/metrics HTTP port.
EXPOSE 3333 9090

# Mount your config at /app/config.toml (see docker-compose.yml).
ENTRYPOINT ["btcpool-rs"]
CMD ["/app/config.toml"]
