# Android video codec comparison

This opt-in fixture compares Sinomo's image encoder/decoder with Android
hardware H.264/H.265 codecs. It generates its own UI image and requests no
camera, microphone, accessibility, or screen-sharing permissions. It does not
invoke a model or modify Sinomo's runtime configuration.

Use Android API 29 or newer. `inventory` reports the device's hardware/software
classification. Hardware runs explicitly select hardware encoders and decoders;
missing support and failed lanes remain errors or unsupported results.

## Build and run

Build the Rust image-test executable for `aarch64-linux-android` with the
configured Android NDK compiler/linker:

```sh
cargo test -p sinomo-lm --lib --profile android-debug \
  --target aarch64-linux-android --no-run
```

Use the test executable printed by that successful command as
`IMAGE_TEST_BINARY`. An NDK `llvm-strip --strip-debug -o ...` copy reduces the
fixture APK size without changing its optimization settings. The Android debug
profile retains assertions and optimizes image encoding, JPEG decoding, and the
App Use pixel path. Do not substitute a release binary without labeling the
comparison separately.

```sh
export CODEC_TEST_KEY_PASSWORD=android
python3 scripts/verification/android-video-codecs/build.py \
  --sdk "$ANDROID_HOME" --output target/codec-probe \
  --keystore target/android-host/android-debug.keystore \
  --password-env CODEC_TEST_KEY_PASSWORD \
  --image-binary "$IMAGE_TEST_BINARY"
adb -s "$ANDROID_SERIAL" install -r -t target/codec-probe/codec-probe.apk
python3 scripts/verification/android-video-codecs/run.py \
  --serial "$ANDROID_SERIAL" --output target/codec-results
python3 scripts/verification/android-video-codecs/analyze.py \
  --run target/codec-results
```

The builder also produces a DEX JAR, but use the Activity for graphics tests:
Android graphics/font initialization is part of the normal application process.
The Rust executable is packaged as a read-only native asset and launched as a
child of the foreground fixture, making its scheduling context comparable to
the Java codec worker. Its diagnostic output records its cgroup.

`run.py` requires a fresh output directory. It retains per-mode raw samples,
completion/failure records, and a manifest, and stops the fixture after each
bounded run. It does not install or uninstall packages. An optional
`--baseline-image-binary` build argument supports an `images-baseline` control.

After collecting results, export `files/comparison` using `adb exec-out run-as
net.sinomo.codecs.probe tar ...` if elementary streams, PNG references or JPEGs
are needed for independent decoding. Remove the disposable fixture with
`adb uninstall net.sinomo.codecs.probe`.

## Measurement contract

- Codec-ready cases share one 304×640 reference image. JPEG uses qualities
  75/85/95. Video uses requested bitrates of 0.5/1/2 Mb/s; requested and observed
  formats are retained because device rate control may differ.
- Each ordinary lane submits 35 frames at 10 fps and excludes the first five
  from warm summaries. Both independent keyframes and predictive streams are
  tested. Sparse cases reuse codec instances with one-second input gaps.
- Video uses a persistent EGL texture and encoder input Surface, no B frames,
  and one frame in flight. Every output timestamp is checked, every access unit
  is decoded, and independent-frame requests must produce sync frames. Standard
  latency hints are requests, not evidence that a driver honored them.
- A separate full-source case includes GPU scaling from 1080×2280. The image
  path includes its existing CPU conversion/resize. Its aspect-preserving
  output is 303×640; video uses even-width 304×640. Keep this geometry/filter
  difference separate from the common-input codec comparison.
- Warm encode-to-decode timing includes submission and buffer handoff. Cold
  first-frame timing additionally includes encoder, decoder and EGL setup.
  Validation, result-file writes and fixture generation are outside warm codec
  timings. No observation overhead is subtracted from measured intervals.
- Luma PSNR is measured against the reference. Inspect decoded color/range
  metadata and independently decoded images too: JPEG and hardware video can
  have different chroma subsampling, so luma PSNR alone is not equal-quality
  evidence. Payload byte counts exclude the network envelope; startup codec
  configuration bytes are reported separately.
- These are local codec experiments. They exclude OS capture, a physical peer
  hop, display presentation, and model ingestion. They do not replace Sinomo
  admission, streaming, revocation or end-to-end latency acceptance.

## Applying the result

The useful OBS reference is its GPU texture handoff and frame-to-packet timing
in `libobs/obs-video-gpu-encode.c`, plus the relationship between B frames,
lookahead, buffer count and output delay in `plugins/obs-nvenc/nvenc.c`.
Interactive snapshots need their own buffering policy; OBS's broadcast defaults
are not a single-frame latency contract.

Production adoption must keep codec ownership in the media layer and preserve
node-local capture consent. Hardware acceleration benefits most from capture
surfaces/textures that avoid CPU readback. Define an explicit surface lifetime
between capture and encoding rather than adding codec/network policy to an OS
provider.

H.264/H.265 also require negotiated codec configuration, keyframe handling and
ordered decoder state. Consume reference frames before publishing a latest
**decoded** frame; dropping arbitrary compressed P frames is not equivalent to
dropping independent JPEGs. Model-facing JPEG/PNG formats remain a separate
contract from transport coding.

API references: [MediaCodec input/output surfaces](https://developer.android.com/reference/android/media/MediaCodec),
[encoder latency and decoder low-latency settings](https://developer.android.com/reference/android/media/MediaFormat).

## Integrated cross-node comparison

App Use `/app-use/capture` and camera `/media/video/capture` requests accept
`transport: "image" | "h264" | "auto"`. Camera image/H.264 streams retain
versions 1/2. Screen and App Use streams use version 3, including geometry change
notifications and content bounds. H.264 uses a stream-local hardware encoder;
explicit H.264 requests fail if that backend is unavailable. Auto
selects the installed hardware adapter except for coordinate-grid images.
Check the capture response's selected version, not just the default image
version in the session descriptor. HEVC remains a standalone experiment.

The H.264 record contains Annex-B SPS/PPS with its initial IDR access unit and
monotonic packet sequence numbers. The shared video reader validates dimensions
and decodes every reference frame before a consumer may replace its latest
frame. `VideoStreamReader::read_decoded` returns pixels directly for preview;
model consumers convert only their selected frames to JPEG/PNG. The Android
adapter uploads existing native pixels to an encoder surface for GPU scaling;
the pixel path includes CPU readback and native-module transfer. Android screen
sharing can instead supply direct encoder surfaces through the generic media
screen source. Accessibility screenshots are a separate, rate-limited source.

For a comparison, build the app and its native provider with `android-debug`,
activate the provider through the normal module interface, and join a second
real node through the supported cluster workflow. Use identical source content,
requested cadence, maximum dimension and consumer build settings for both lanes.
Start screen sharing and grant the exact share locally before opening an App Use
session; neither joining nor a codec request grants device access.

Build the model-free consumer with `cargo build -p sinomo-cli --example media_e2e`.
Supply a JSON request file containing `mode: "video-latency"`, the capture route,
category `media`, a typed capture request, and `samples: 105`. The capture request
must include the session ID and a complete image encoding configuration (for
example `frame_format: "jpeg"`, `encoding_quality: 85`, `max_dimension: 640`,
`max_frame_rate_hz: 10`). Run:

```bash
SINOMO_DATA_DIR=/path/to/calling-node \
  target/debug/examples/media_e2e CALLER_DAEMON_ADDRESS TARGET_NODE request.json
```

Use separate finite sessions for the two lanes and close each session explicitly.
Preserve the first five samples as warmup and report startup independently.
The consumer records payload bytes, source image-acquisition time, local receive/decode
completion time, frame gaps, and decoded geometry. Measure and report clock offset
and uncertainty before interpreting timestamps across nodes; arrival gaps alone
are not end-to-end latency. These timings end at decoded pixels, before display
presentation or model ingestion. Test revocation and stream cancellation as
separate gates, and retain admission failures rather than substituting local
codec results. Remove only test-owned memberships, forwards, fixtures and grants.

`analyze_cross_node.py --run DIR` validates `cross-image.jsonl` and
`cross-h264.jsonl`, including decoded dimensions, source sequence advancement,
and contiguous H.264 packet sequences. It uses the shared verification reporter
for percentiles. Clock files `clock-measure-before.json` and
`clock-measure-after.json` contain repeated bracket observations with
`host_start_ns`, `phone_ns`, `host_end_ns`, `host_minus_phone_ns` (host midpoint
minus phone), and `uncertainty_ns` (half the host interval). Obtain the phone
sample with `adb shell date +%s%N` between host `time.time_ns()` observations.
The report assumes the offset stays within the before/after brackets; a clock
step invalidates that assumption. Keep these raw observations alongside results.

The consumer's `network-ping` mode measures nonce-verified RPC round trips for
0, 16 KiB and 64 KiB prepared responses, with five warmups and 30 measured calls
per size. This separates basic transport/dispatch cost from capture and codecs;
its percentiles must not be subtracted from frame-age percentiles.

## Capture-stage attribution

Enable an Observe capture with `sinomo.perf=trace` around a bounded streaming
workload. `media.frame.encode` spans measure encoding; `media.frame.source`
events carry the logical `stream_id`, source sequence, acquisition timestamp,
native handoff time, encoded-ready time and pull duration. The consumer's `wire_received_unix_us`
marks complete record arrival before decoder scheduling. Join frames by source
stream ID, source sequence and acquisition timestamp, and reject missing or
ambiguous matches. Native Android diagnostics do not carry the logical stream ID;
ambiguous native-stage matches are reported as unavailable instead of overwritten.

For Android's internal handoff, save the existing value of
`adb shell getprop log.tag.SinomoMediaStage`, then enable
`adb shell setprop log.tag.SinomoMediaStage V`. The tag emits metadata-only JSON
for Java row copying, Java mailbox age, local socket writes, native normalization,
publication, native frame preparation/serialization and host callbacks. Collect
with `adb logcat -d -v raw -s SinomoMediaStage:V '*:S'` and restore the previous
property afterward. Logging is off by default. A native `call_next_us` measurement
may include waiting before acquisition; socket writes and reads overlap. Do not
add these stage percentiles as if they were disjoint durations.

`network-ping` also records client send/receive wall clocks and the remote
response timestamp. Use zero-payload samples before and after streaming to bound
clock offset with a much shorter round trip than shell-based clock sampling.
This still assumes no clock step between brackets.

`analyze_capture_stages.py --frames FRAMES --traces SOURCE_TRACE --native-log LOG
--clock-before PINGS --clock-after PINGS --output SUMMARY` joins those records and
uses the standard verification percentile reducer. Its post-encode delivery
interval includes queues, forwarding and receiver parsing, not just the link.
Preserve raw process metadata: if an embedded app omits its trace node ID, record
that limitation and independently verify its node/PID using daemon status. Do
not rewrite missing identity fields or report the strict collector's rejection
as a successful capture.


## Android receiver verification

The Android application installs a MediaCodec decoder adapter. On Android 10 or
newer it prefers a decoder positively identified as hardware accelerated, with
flexible YUV output at the negotiated dimensions. Unsupported hardware or a
failure before the first decoded frame falls back to OpenH264. After a successful
frame, decoder failures terminate the stream; switching an uninitialized decoder
onto dependent frames would lose reference state. Older Android versions and
hosts without the application adapter retain OpenH264. The selected backend is
available through `VideoStreamReader::decoder_backend` and decode trace records.

Decoded output remains RGB pixels for existing consumers. Image plane cropping,
row/pixel strides and BT.601/BT.709 full/limited range conversion are honored.
Hardware decoding does not eliminate that pixel conversion or provide a direct
video display surface.

The JNI probe compiles the production application adapter and calls the
production stream reader. It decodes identical predictive H.264 frames through
software and hardware, checks RGB output against a generated color fixture, and
records decode/conversion and total reader timing. It requires an API 29+ device
with a compatible hardware encoder and decoder. No screen-sharing or camera
permission is requested; its temporary device directory is removed on completion.
With `ANDROID_HOME`, `ANDROID_NDK_HOME`, and the Android Cargo linker/compiler
variables configured as for the app's native builds, run from the repository root:

```sh
cargo build -p sinomo-video --features native-devices --example android_decoder_probe \
  --profile android-debug --target aarch64-linux-android --target-dir target/android-host
python3 scripts/verification/android-video-codecs/run_decoder_probe.py \
  --library target/android-host/aarch64-linux-android/android-debug/examples/libandroid_decoder_probe.so \
  --output target/android-decoder-probe/results
```

Five warmup frames precede 100 measured frames. This is a local receiver test,
excluding capture, network delivery, presentation and model image encoding.
Compare the two backends within the same run; do not treat its simple color
fixture as an end-to-end streaming latency result. The standalone command-line
media probe does not install the application's Java adapter and therefore cannot
verify hardware receiver selection by itself.
