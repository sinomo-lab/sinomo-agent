#!/usr/bin/env bash
set -euo pipefail

STATE_FILE="${SINOMO_MEDIA_TEST_STATE:-/tmp/sinomo-media-test.env}"
SINK_NAME="${SINOMO_TEST_PULSE_SINK:-sinomo_test_sink}"
SOURCE_NAME="${SINOMO_TEST_PULSE_SOURCE:-sinomo_test_mic}"
SINK_DESC="${SINOMO_TEST_SPEAKER_DEVICE:-SinomoTestSpeaker}"
SOURCE_DESC="${SINOMO_TEST_MIC_DEVICE:-SinomoTestMic}"
VIDEO_NR="${SINOMO_TEST_VIDEO_NR:-42}"
CAMERA_LABEL="${SINOMO_TEST_CAMERA_LABEL:-SinomoTestCamera}"
CAMERA_DEVICE="/dev/video${VIDEO_NR}"

usage() {
  cat <<USAGE
Usage: $0 start|stop|status

Creates virtual media devices for:
  cargo test -p sinomo --test native_media_e2e -- --ignored

The start command prints the exports needed by the test. It uses PulseAudio or
PipeWire's pactl interface for virtual mic/speaker and v4l2loopback for camera.
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

write_state() {
  cat >"${STATE_FILE}" <<STATE
PULSE_SINK_MODULE=${PULSE_SINK_MODULE:-}
PULSE_SOURCE_MODULE=${PULSE_SOURCE_MODULE:-}
AUDIO_FEED_PID=${AUDIO_FEED_PID:-}
VIDEO_FEED_PID=${VIDEO_FEED_PID:-}
SINOMO_TEST_NATIVE_MEDIA=1
SINOMO_TEST_MIC_DEVICE=${SOURCE_DESC}
SINOMO_TEST_SPEAKER_DEVICE=${SINK_DESC}
SINOMO_TEST_CAMERA_DEVICE=${CAMERA_DEVICE}
STATE
}

start() {
  require_cmd pactl
  require_cmd ffmpeg

  if [[ -f "${STATE_FILE}" ]]; then
    echo "state file exists; stopping previous setup first: ${STATE_FILE}" >&2
    stop || true
  fi

  PULSE_SINK_MODULE="$(pactl load-module module-null-sink "sink_name=${SINK_NAME}" "sink_properties=device.description=${SINK_DESC}")"
  PULSE_SOURCE_MODULE="$(pactl load-module module-remap-source "master=${SINK_NAME}.monitor" "source_name=${SOURCE_NAME}" "source_properties=device.description=${SOURCE_DESC}")"

  ffmpeg -hide_banner -loglevel error \
    -f lavfi -i "sine=frequency=440:sample_rate=48000" \
    -f pulse "${SINK_NAME}" >/tmp/sinomo-media-audio-feed.log 2>&1 &
  AUDIO_FEED_PID="$!"

  if [[ ! -e "${CAMERA_DEVICE}" ]]; then
    if ! command -v modprobe >/dev/null 2>&1; then
      echo "missing modprobe and ${CAMERA_DEVICE} does not exist" >&2
      exit 1
    fi
    sudo modprobe v4l2loopback "devices=1" "video_nr=${VIDEO_NR}" "card_label=${CAMERA_LABEL}" "exclusive_caps=1"
  fi

  ffmpeg -hide_banner -loglevel error -re \
    -f lavfi -i "testsrc=size=640x480:rate=2" \
    -vcodec mjpeg -f v4l2 "${CAMERA_DEVICE}" >/tmp/sinomo-media-video-feed.log 2>&1 &
  VIDEO_FEED_PID="$!"

  write_state
  cat <<EXPORTS
Virtual media devices started.

Run:
  export SINOMO_TEST_NATIVE_MEDIA=1
  export SINOMO_TEST_MIC_DEVICE=${SOURCE_DESC}
  export SINOMO_TEST_SPEAKER_DEVICE=${SINK_DESC}
  export SINOMO_TEST_CAMERA_DEVICE=${CAMERA_DEVICE}
  cargo test -p sinomo --test native_media_e2e -- --ignored --nocapture

Stop with:
  $0 stop
EXPORTS
}

stop() {
  if [[ -f "${STATE_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${STATE_FILE}"
  fi
  if [[ -n "${AUDIO_FEED_PID:-}" ]]; then
    kill "${AUDIO_FEED_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${VIDEO_FEED_PID:-}" ]]; then
    kill "${VIDEO_FEED_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${PULSE_SOURCE_MODULE:-}" ]]; then
    pactl unload-module "${PULSE_SOURCE_MODULE}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${PULSE_SINK_MODULE:-}" ]]; then
    pactl unload-module "${PULSE_SINK_MODULE}" >/dev/null 2>&1 || true
  fi
  rm -f "${STATE_FILE}"
  echo "Virtual media devices stopped."
}

status() {
  if [[ -f "${STATE_FILE}" ]]; then
    cat "${STATE_FILE}"
  else
    echo "No state file at ${STATE_FILE}"
  fi
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  -h|--help|help|"") usage ;;
  *)
    usage >&2
    exit 2
    ;;
esac
