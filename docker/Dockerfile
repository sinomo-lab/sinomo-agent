# Sinomo dev-cluster node image.
#
# A single image that runs one cluster node: the shadow supervisor (PID 1) which
# starts and supervises the main daemon, and which can perform self-rollouts by
# building from a managed source repo. All three compose nodes share this image;
# each gets its own data volume. Containers talk over the compose bridge network
# using LAN-discovered endpoints (no external iroh relay needed).
#
# The Rust toolchain + source are kept in the image because `rollout apply` /
# `rollout cluster` build the new binary from the managed source repo in-container.
FROM rust:1.90-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates iproute2 procps \
    && rm -rf /var/lib/apt/lists/*

# Build the daemon binary. The source is copied (target/ excluded via .dockerignore)
# and committed so the in-container managed source repo + build.rs commit stamp work.
WORKDIR /sinomo-src
COPY . /sinomo-src
RUN git config --global user.email "dev@sinomo.local" \
    && git config --global user.name "sinomo-dev" \
    && git config --global init.defaultBranch main \
    && if [ ! -d .git ]; then git init -q && git add -A && git commit -qm "dev-cluster baseline"; fi

RUN cargo build -p sinomo \
    && install -Dm755 target/debug/sinomo /usr/local/bin/sinomo

COPY docker/entrypoint.sh /usr/local/bin/sinomo-node-entrypoint
RUN chmod +x /usr/local/bin/sinomo-node-entrypoint

ENV SINOMO_DATA_DIR=/data
ENTRYPOINT ["/usr/local/bin/sinomo-node-entrypoint"]
