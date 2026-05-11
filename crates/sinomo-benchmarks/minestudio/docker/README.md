# MineStudio Docker Runtime

This folder contains a Dockerized MineStudio runtime for real Minecraft
simulator episodes. It is for benchmark and debugging use only; production
Sinomo crates do not depend on it.

The container uses:

- Python 3.10 and OpenJDK 8 from conda-forge;
- MineStudio from PyPI;
- Xvfb and Mesa for headless CPU rendering;
- a small FastAPI control wrapper around `minestudio.simulator.MinecraftSim`.

## Start

```sh
docker compose -f crates/sinomo-benchmarks/minestudio/docker/compose.yml up --build
```

The wrapper listens on `http://127.0.0.1:18080` by default. Override the host
port with `SINOMO_MINESTUDIO_PORT`.

Compose keeps MineStudio's downloaded simulator engine in the
`minestudio-engine` volume and Hugging Face artifacts in `huggingface-cache`.
The first reset can still be slow while those caches are populated.

## Interact

```sh
python3 crates/sinomo-benchmarks/minestudio/docker/minestudio_client.py health
python3 crates/sinomo-benchmarks/minestudio/docker/minestudio_client.py reset --output benchmark-results/minestudio-reset.png
python3 crates/sinomo-benchmarks/minestudio/docker/minestudio_client.py step \
  --ticks 5 \
  --macro-json '{"verb":"look","target":"sky","duration_ticks":5}' \
  --output benchmark-results/minestudio-step.png
```

The vision loop can use the same wrapper instead of importing MineStudio on the
host:

```sh
SINOMO_MINESTUDIO_ENV_URL=http://127.0.0.1:18080 \
SINOMO_PYTORCH_ENDPOINT=http://10.14.5.15:32153 \
python3 crates/sinomo-benchmarks/minestudio/minestudio_vision_action_loop.py
```

For the live-style duplex workflow, keep the same wrapper running and use:

```sh
SINOMO_MINESTUDIO_ENV_URL=http://127.0.0.1:18080 \
SINOMO_PYTORCH_ENDPOINT=http://10.14.5.15:32153 \
python3 crates/sinomo-benchmarks/minestudio/minestudio_live_duplex_loop.py
```

That harness sends chronological frame sequences with `stream=true`, consumes
streamed MiniCPM-o output chunks when the endpoint supports SSE, and applies a
recovered action back to the paused MineStudio environment.
It also enables MineStudio's `RecordCallback` by default so the run directory
contains high-frame-rate episode MP4s under `minestudio-recording/`. Those
videos are recorded by MineStudio during `reset` and every `step`, so they show
what happened between the lower-rate observation frames sent to the model. Use
`--no-record-video` to keep only the PNG observation sequence and JSON reports.

The HTTP API exposes:

- `GET /health`
- `POST /reset`
- `GET /observe`
- `POST /step`
- `POST /close`

`/reset`, `/observe`, and `/step` return the current rendered frame as a PNG
inside `frame.data` using base64 encoding. The frame comes from MineStudio's
real `info["pov"]` when available, falling back to `obs["image"]`.

## Smoke

Run a one-shot reset and step inside the container:

```sh
docker compose -f crates/sinomo-benchmarks/minestudio/docker/compose.yml run --rm minestudio smoke
```

The first simulator run can be slow because MineStudio may download or compile
Minecraft assets.
