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
FROM rust:1.95-bookworm

ARG SINOMO_BENCH_DEPS=0

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
        iproute2 \
        libasound2-dev \
        procps \
    && rm -rf /var/lib/apt/lists/*

RUN if [ "$SINOMO_BENCH_DEPS" = "1" ]; then \
        apt-get update && \
        apt-get install -y --no-install-recommends \
            docker.io \
            python3 \
            python-is-python3 \
            python3-pip && \
        rm -rf /var/lib/apt/lists/* && \
        python -m pip install --break-system-packages --no-cache-dir swebench; \
    fi

# Build the daemon binary and the opt-in readiness module. The source is copied
# (target/ excluded via .dockerignore) and committed so the in-container managed
# source repo + build.rs commit stamp work.
WORKDIR /sinomo-src
COPY . /sinomo-src
# A normal clone ships a real .git directory, which we preserve so build.rs stamps the
# real commit. A git *worktree* instead ships a .git *file* pointing at an external
# gitdir absent from the image; git then hard-fails repo discovery on that dangling
# pointer for EVERY command -- even `config --global` -- so it must be dropped up front,
# before any git runs. We only remove a .git that is not a real directory (preserving a
# clone's), then the fallback inits a fresh baseline when no real repo remains.
RUN { [ -d .git ] || rm -rf .git; } \
    && git config --global user.email "dev@sinomo.local" \
    && git config --global user.name "sinomo-dev" \
    && git config --global init.defaultBranch main \
    && if [ ! -d .git ]; then git init -q && git add -A && git commit -qm "dev-cluster baseline"; fi

RUN cargo build -p sinomo -p sinomo-benchmarks -p sinomo-observe --features sinomo-observe/cpu-profiler \
    && install -Dm755 target/debug/sinomo /usr/local/bin/sinomo \
    && install -Dm755 target/debug/libsinomo_benchmarks.so \
        /opt/sinomo/test-modules/libsinomo_benchmarks.so

COPY docker/entrypoint.sh /usr/local/bin/sinomo-node-entrypoint
COPY docker/single-node-entrypoint.sh /usr/local/bin/sinomo-single-node-entrypoint
RUN chmod +x /usr/local/bin/sinomo-node-entrypoint /usr/local/bin/sinomo-single-node-entrypoint

ENV SINOMO_DATA_DIR=/data
ENTRYPOINT ["/usr/local/bin/sinomo-node-entrypoint"]
