# Native Media E2E Harness

This directory contains the host-local harness for testing the native audio and
video pipeline end to end without using a real model provider.

## Fast Deterministic Tests

These run in the normal Rust test suite and do not require devices:

```bash
cargo test -p sinomo-audio
cargo test -p sinomo-video
```

They cover raw audio decoding, stereo-to-mono capture conversion, resampling,
speaker output encoding, and video frame metadata wrapping.

## Virtual Device E2E

The ignored integration test drives the real `sinomo voice call` command against
`fake_realtime_server.py`. It requires virtual mic, camera, and speaker devices.

On Linux with PulseAudio or PipeWire plus `v4l2loopback`:

```bash
tests/media/setup_virtual_media.sh start
export SINOMO_TEST_NATIVE_MEDIA=1
export SINOMO_TEST_MIC_DEVICE=SinomoTestMic
export SINOMO_TEST_SPEAKER_DEVICE=SinomoTestSpeaker
export SINOMO_TEST_CAMERA_DEVICE=/dev/video42
cargo test -p sinomo --test native_media_e2e -- --ignored --nocapture
tests/media/setup_virtual_media.sh stop
```

The test verifies:

- native microphone chunks reach the realtime WebSocket endpoint,
- native camera frames reach the endpoint,
- synthetic model output is written to the CLI output sidecar,
- the call exits successfully with native speaker playback enabled on the
  selected device (the test does not inspect what the speaker sink received).

If your CPAL device names differ, run `sinomo voice devices` and export the
matching selectors before running the ignored test.
