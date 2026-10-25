# Real Wayland compositor verification in Docker

This lane runs KWin's virtual Wayland backend, KDE's real ScreenCast/RemoteDesktop
portal, PipeWire/WirePlumber, and a GTK Wayland fixture inside one disposable
container. Mesa llvmpipe renders the desktop in software. It tests the actual
provider dylib through the native SOBJ ABI and observes application state after
input, rather than accepting a successful D-Bus call as proof of a click.

The host needs Docker, Python 3, Git, and an existing `/dev/udmabuf` device for this
KWin software-buffer path. The runtime passes that device and its group, drops
Linux capabilities, disables network access, and does not mount host display,
input, D-Bus or desktop-runtime sockets. KWin's optional scheduling file
capability is removed in the image so it can run under those restrictions.
No GUI packages or services are installed on the host.

From the repository root, build artifacts and run entirely in Docker:

```sh
python3 modules/desktop/app-use/tests/run_compositor.py --build-image --build-artifacts
```

To use already built Linux artifacts with a compatible C runtime:

```sh
python3 modules/desktop/app-use/tests/run_compositor.py --build-image
python3 modules/desktop/app-use/tests/run_compositor.py --scale 2
```

`--provider` and `--probe` select explicit artifacts. The default probe filename
retains its Android fixture name, but the same source supports Linux.
`--build-artifacts` uses a separate builder image, a read-only source mount,
private Cargo/target directories and normal Cargo worker scheduling under the
shared build lock. The runtime image does not need a Rust toolchain. Use
`--keep` only when intentionally retaining the isolated desktop for diagnosis.

The runner records image/container configuration, package versions, source patch
and untracked provider source, artifact hashes, native request/response traces,
app counters, frame metadata and a captured PNG under a unique directory in
`target/app-use-wayland-compositor/`. `result.json` records every completed check
and any failure. Default cleanup removes only the container created by that run.

Coverage includes local chooser denial and approval without remembered authorization,
separate node-local grants, real pixels and fresh frames, click/text/relative
motion/wheel effects, held-key release, cached-frame revocation and stale-target
rejection after unload. Portal setup controls are activated through AT-SPI inside
the disposable desktop; application input is sent only through the provider.

At scaled outputs, use the portal-advertised target bounds when mapping video
coordinates. They need not equal the GTK application's logical dimensions. The
fixture records both so a scaling error cannot be hidden by a successful portal
response.

This is functional compositor/portal evidence, not GPU performance, physical
monitor, suspend/resume, display-manager login or all-desktop compatibility
evidence. Those need the corresponding environment. If `/dev/udmabuf` is not
available, a small KVM guest with virtual graphics and the same compositor stack
is the fallback; this runner does not load host kernel modules or install a host
desktop to make the test pass.
