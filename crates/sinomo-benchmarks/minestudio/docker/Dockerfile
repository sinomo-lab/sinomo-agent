FROM mambaorg/micromamba:1.5.10-jammy

USER root

ENV DEBIAN_FRONTEND=noninteractive \
    DISPLAY=:99 \
    MINESTUDIO_HEADLESS=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        ffmpeg \
        libegl1-mesa \
        libgl1-mesa-dev \
        libglu1-mesa-dev \
        mesa-utils \
        procps \
        tini \
        xauth \
        x11-utils \
        xterm \
        xvfb \
    && rm -rf /var/lib/apt/lists/*

RUN micromamba install -y -n base -c conda-forge python=3.10 openjdk=8 pip \
    && micromamba clean -a -y

ENV JAVA_HOME=/opt/conda \
    PATH=/opt/conda/bin:/opt/sinomo/minestudio:$PATH

RUN python -m pip install --upgrade 'pip<26' 'setuptools<81' wheel \
    && python -m pip install minestudio fastapi 'uvicorn[standard]' pillow numpy psutil

WORKDIR /opt/sinomo/minestudio

COPY crates/sinomo-benchmarks/minestudio/minestudio_action_bridge.py /opt/sinomo/minestudio/minestudio_action_bridge.py
COPY crates/sinomo-benchmarks/minestudio/docker/entrypoint.sh /opt/sinomo/minestudio/entrypoint.sh
COPY crates/sinomo-benchmarks/minestudio/docker/minestudio_http_server.py /opt/sinomo/minestudio/minestudio_http_server.py
COPY crates/sinomo-benchmarks/minestudio/docker/minestudio_client.py /opt/sinomo/minestudio/minestudio_client.py

RUN chmod +x /opt/sinomo/minestudio/entrypoint.sh /opt/sinomo/minestudio/minestudio_client.py

EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/sinomo/minestudio/entrypoint.sh"]
CMD ["server"]
