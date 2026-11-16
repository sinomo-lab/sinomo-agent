# Android App Use device fixture

Use a development device explicitly selected for testing. Preserve its installed
APK, local permission document, accessibility services and rotation settings.
Install the current Sinomo APK without clearing app data, and use the local
Sinomo App Use screen to enable persistent Accessibility observation/control.
Test it before starting screen sharing. For video tests, approve a separate screen
share and grant its exact capture/stop resource. Keep other accessibility services
intact.

Build the two disposable APKs with the installed Android SDK:

```sh
python3 modules/desktop/app-use/android/fixture/build.py --sdk "$ANDROID_HOME" \
  --output target/app-use-android-fixtures \
  --keystore target/android-host/android-debug.keystore \
  --password-env CARGO_APK_DEV_KEYSTORE_PASSWORD
```

The fixture package is `net.sinomo.appuse.fixture`; its `.FixtureActivity`
provides an accessible editor, click counter, animated capture surface and
gesture counters. `run-as net.sinomo.appuse.fixture cat files/state.json` exposes
the observed effects. Launching with `--ez secure true` sets `FLAG_SECURE` and
records it in that state; verify the flag before asserting protected capture.
Launching with `--ez password true` creates a password editor with a synthetic
value for testing semantic redaction and action denial.

The separate `net.sinomo.appuse.inspect/.Inspect` instrumentation emits bounded
UI node JSON through `adb shell am instrument -w`. It explicitly requests
`FLAG_DONT_SUPPRESS_ACCESSIBILITY_SERVICES`. **Do not use stock `uiautomator
dump` while testing Accessibility:** its default automation connection suppresses
the service under test. Projection must remain independent of Accessibility.

Build `cargo build -p sinomo-app-use-module --example android_native_probe
--target aarch64-linux-android` with the same NDK compiler/linker configuration
as the provider. Copy the probe and actual provider `.so` into a private cache
directory using `run-as net.sinomo.app`; execute the probe under that app UID:

```text
android_native_probe PROVIDER_SO PERMISSION_STORE FRAME_DIRECTORY
```

The probe reads one JSON object per stdin line. `command` contains the ordinary
`DesktopProviderCommand`, such as `{"command":{"action":"capabilities"}}`.
It loads the independent dylib, uses the real SOBJ ABI and permission store, and
validates native BGRA frames. `save_frame:true` saves `frame.bgra` and metadata
for visual comparison. The probe also builds for Linux compositor tests. Local
probe controls inspect permissions, change broader test consent, expose/revoke
an exact grant, and unload/reload the provider; none are
part of the shipped app or its remote tool surface.

Exercise denial before grants, capture-only versus input grants, native clicks,
empty hinted text fields, Unicode insertion/backspace, gestures, six-second
input idle, 30-second idle expiry, release/close cleanup, partial-input errors,
protected surfaces, capture/input revocation, OS stop, rotation, accessibility
disablement, and unload/reload. Verify semantic observe/act and screenshots with
no projection. After approving video, wait more than 30 seconds without clients,
connect and disconnect multiple readers, switch apps, disable Accessibility, and
rotate the device: projection must stay active. Check geometry notifications and
continued H.264 decoding, then verify explicit agent stop and local user stop.
Old coordinate leases must fail after rotation; a fresh semantic observation
must work. Revoking Accessibility must not stop independently approved video. Read fixture state after each effect; OS gesture
acceptance alone does not prove that a click reached the intended app. Wait for
the fixture to be foreground before sending input.

Restore the prior node consent and device settings, stop sharing, remove only
the temporary fixture/inspector packages and probe files, and retain the
source/artifact hashes and test evidence outside tracked documentation.
