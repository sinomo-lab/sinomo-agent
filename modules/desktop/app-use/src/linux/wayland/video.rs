//! Consume only the portal's restricted PipeWire FD. Never connect to the
//! ambient PipeWire socket or reconnect a removed stream by recycled node ID.
use super::*;
use pipewire as pw;
use pw::{properties::properties, spa};
use sinomo_device::{NativeVideoPlane, NativeVideoSampleFormat};
use std::cell::RefCell;
use std::rc::Rc;

struct VideoState {
    shared: Arc<Shared>,
    permissions: LocalNodePermissionStore,
    target: DesktopTarget,
    format: spa::param::video::VideoInfoRaw,
    size: Option<(u32, u32)>,
    latest: Rc<RefCell<Option<NativeVideoSample>>>,
    failure: Rc<RefCell<Option<DesktopError>>>,
    started: Instant,
    last: Option<Instant>,
    cadence: Duration,
    first_pts: Option<i64>,
    last_timestamp: u64,
    sequence: u64,
    active: bool,
}
impl VideoState {
    fn fail(&self, error: DesktopError) {
        *self.failure.borrow_mut() = Some(error);
        self.shared.stop();
    }
    fn process(&mut self, stream: &pw::stream::Stream) -> DesktopResult<()> {
        let Some(mut buffer) = stream.dequeue_buffer() else {
            return Ok(());
        };
        if self.last.is_some_and(|last| last.elapsed() < self.cadence) {
            return Ok(());
        }
        self.shared.target()?;
        authorize(&self.permissions, &self.target, "capture")?;
        let pts = if let Some(header) = buffer.find_meta::<spa::buffer::meta::MetaHeader>() {
            if header.flags().intersects(
                spa::buffer::meta::MetaHeaderFlags::CORRUPTED
                    | spa::buffer::meta::MetaHeaderFlags::GAP,
            ) {
                return Ok(());
            }
            (header.pts() >= 0).then_some(header.pts())
        } else {
            None
        };
        let (width, height) = self
            .size
            .ok_or_else(|| platform("PipeWire sent pixels before negotiating a format"))?;
        if buffer.datas_mut().len() != 1 {
            return Err(platform("PipeWire packed RGB frame must have one plane"));
        }
        let data = &mut buffer.datas_mut()[0];
        if data.as_raw().chunk.is_null() {
            return Err(platform("PipeWire frame has no chunk"));
        }
        let (offset, size, stride) = (
            data.chunk().offset() as usize,
            data.chunk().size() as usize,
            data.chunk().stride(),
        );
        if size == 0 {
            return Ok(());
        }
        let bytes = data
            .data()
            .ok_or_else(|| unsupported("portal did not provide a CPU-mappable PipeWire frame"))?;
        let pixels = copy_pixels(
            bytes,
            offset,
            size,
            stride,
            width,
            height,
            self.format.format(),
        )?;
        self.shared.target()?;
        authorize(&self.permissions, &self.target, "capture")?;
        self.sequence += 1;
        let timestamp = pts
            .map(|pts| {
                pts.saturating_sub(*self.first_pts.get_or_insert(pts))
                    .max(0) as u64
            })
            .unwrap_or_else(|| self.started.elapsed().as_nanos() as u64);
        // Header metadata is optional. Switching between native PTS and the
        // local fallback must never move the shared video timeline backwards.
        let timestamp = timestamp.max(self.last_timestamp);
        self.last_timestamp = timestamp;
        let len = pixels.len();
        *self.latest.borrow_mut() = Some(NativeVideoSample {
            sequence: self.sequence,
            format: NativeVideoSampleFormat::Bgra8,
            width,
            height,
            timestamp_ns: timestamp,
            duration_ns: self.cadence.as_nanos() as u64,
            captured_at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            data: pixels.into(),
            planes: Arc::from([NativeVideoPlane {
                offset: 0,
                len,
                width,
                height,
                stride: width * 4,
            }]),
        });
        self.last = Some(Instant::now());
        Ok(())
    }
}
pub(super) fn capture(
    lease: &Lease,
    fd: OwnedFd,
    config: DesktopCaptureConfig,
    permissions: LocalNodePermissionStore,
    mailbox: &CaptureMailbox,
    stop: mpsc::Receiver<()>,
    ready: mpsc::SyncSender<DesktopResult<()>>,
) -> DesktopResult<()> {
    let mut ready = Some(ready);
    let result = run(lease, fd, config, permissions, mailbox, stop, &mut ready);
    if let Some(ready) = ready {
        let _ = ready.send(result.clone());
    }
    result
}
fn run(
    lease: &Lease,
    fd: OwnedFd,
    config: DesktopCaptureConfig,
    permissions: LocalNodePermissionStore,
    mailbox: &CaptureMailbox,
    stop: mpsc::Receiver<()>,
    ready: &mut Option<mpsc::SyncSender<DesktopResult<()>>>,
) -> DesktopResult<()> {
    pw::init();
    let mainloop = pw::main_loop::MainLoopRc::new(None).map_err(platform)?;
    let context = pw::context::ContextRc::new(&mainloop, None).map_err(platform)?;
    let core = context.connect_fd_rc(fd, None).map_err(platform)?;
    let failure = Rc::new(RefCell::new(None));
    let core_failure = failure.clone();
    let shared = lease.shared.clone();
    let _core_listener = core
        .add_listener_local()
        .error(move |_, _, _, message| {
            *core_failure.borrow_mut() =
                Some(platform(format!("PipeWire connection failed: {message}")));
            shared.stop();
        })
        .register();
    let mut props = properties! { *pw::keys::MEDIA_TYPE => "Video", *pw::keys::MEDIA_CATEGORY => "Capture", *pw::keys::MEDIA_ROLE => "Screen", *pw::keys::NODE_NAME => "sinomo-app-use-capture" };
    if let Some(serial) = lease.target.serial {
        props.insert("target.object", serial.to_string());
    }
    let stream = pw::stream::StreamBox::new(&core, "Sinomo App Use", props).map_err(platform)?;
    let latest = Rc::new(RefCell::new(None));
    let state = VideoState {
        shared: lease.shared.clone(),
        permissions: permissions.clone(),
        target: config.target.clone(),
        format: Default::default(),
        size: None,
        latest: latest.clone(),
        failure: failure.clone(),
        started: Instant::now(),
        last: None,
        cadence: Duration::from_secs_f64(1.0 / config.frames_per_second as f64),
        first_pts: None,
        last_timestamp: 0,
        sequence: 0,
        active: false,
    };
    let _listener = stream
        .add_local_listener_with_user_data(state)
        .state_changed(|_, data, _, state| match state {
            pw::stream::StreamState::Error(error) => data.fail(platform(error)),
            pw::stream::StreamState::Unconnected if data.active => {
                data.fail(DesktopError::Closed("PipeWire stream was removed".into()))
            }
            pw::stream::StreamState::Paused | pw::stream::StreamState::Streaming => {
                data.active = true
            }
            _ => {}
        })
        .param_changed(|_, data, id, pod| {
            if id != spa::param::ParamType::Format.as_raw() {
                return;
            }
            let Some(pod) = pod else {
                return;
            };
            if let Err(error) = data.format.parse(pod) {
                data.fail(platform(error));
                return;
            }
            let size = (data.format.size().width, data.format.size().height);
            if !valid_dimensions(size.0, size.1) {
                data.fail(platform("PipeWire frame dimensions exceed limits"));
                return;
            }
            if data.size.is_some_and(|old| old != size) {
                data.fail(DesktopError::TargetUnavailable(
                    "Wayland stream resized; approve a new portal target before more input".into(),
                ));
                return;
            }
            data.size = Some(size);
        })
        .process(|stream, data| {
            if let Err(error) = data.process(stream) {
                data.fail(error);
            }
        })
        .register()
        .map_err(platform)?;
    let object = spa::pod::object!(
        spa::utils::SpaTypes::ObjectParamFormat,
        spa::param::ParamType::EnumFormat,
        spa::pod::property!(
            spa::param::format::FormatProperties::MediaType,
            Id,
            spa::param::format::MediaType::Video
        ),
        spa::pod::property!(
            spa::param::format::FormatProperties::MediaSubtype,
            Id,
            spa::param::format::MediaSubtype::Raw
        ),
        spa::pod::property!(
            spa::param::format::FormatProperties::VideoFormat,
            Choice,
            Enum,
            Id,
            spa::param::video::VideoFormat::BGRx,
            spa::param::video::VideoFormat::BGRx,
            spa::param::video::VideoFormat::BGRA,
            spa::param::video::VideoFormat::RGBx,
            spa::param::video::VideoFormat::RGBA,
            spa::param::video::VideoFormat::RGB,
            spa::param::video::VideoFormat::BGR
        )
    );
    let pod = spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &spa::pod::Value::Object(object),
    )
    .map_err(platform)?
    .0
    .into_inner();
    let mut params =
        [spa::pod::Pod::from_bytes(&pod).ok_or_else(|| platform("invalid video format pod"))?];
    stream
        .connect(
            spa::utils::Direction::Input,
            if lease.target.serial.is_some() {
                None
            } else {
                Some(lease.target.node)
            },
            pw::stream::StreamFlags::AUTOCONNECT
                | pw::stream::StreamFlags::MAP_BUFFERS
                | pw::stream::StreamFlags::DONT_RECONNECT,
            &mut params,
        )
        .map_err(platform)?;
    let started = Instant::now();
    let lease_end = started + Duration::from_millis(config.lease_ms.min(300_000));
    loop {
        if let Some(error) = failure.borrow_mut().take() {
            return Err(error);
        }
        lease.shared.target()?;
        authorize(&permissions, &config.target, "capture")?;
        match stop.try_recv() {
            Ok(()) | Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if Instant::now() >= lease_end {
            return Ok(());
        }
        if ready.is_some() && started.elapsed() > Duration::from_secs(7) {
            return Err(platform(
                "PipeWire did not produce a frame within the capture startup deadline",
            ));
        }
        if mainloop
            .loop_()
            .iterate(pw::loop_::Timeout::Finite(Duration::from_millis(50)))
            < 0
        {
            return Err(platform("PipeWire loop failed"));
        }
        if let Some(sample) = latest.borrow_mut().take() {
            lease.shared.target()?;
            authorize(&permissions, &config.target, "capture")?;
            mailbox.publish(sample);
            if let Some(ready) = ready.take() {
                let _ = ready.send(Ok(()));
            }
        }
    }
}

pub(super) fn valid_dimensions(width: u32, height: u32) -> bool {
    width != 0
        && height != 0
        && u64::from(width) * u64::from(height) <= (MAX_FRAME_BYTES / 4) as u64
}

pub(super) fn copy_pixels(
    data: &[u8],
    offset: usize,
    size: usize,
    stride: i32,
    width: u32,
    height: u32,
    format: spa::param::video::VideoFormat,
) -> DesktopResult<Vec<u8>> {
    use spa::param::video::VideoFormat as F;
    let channels = if [F::RGB, F::BGR].contains(&format) {
        3
    } else if [F::BGRx, F::BGRA, F::RGBx, F::RGBA].contains(&format) {
        4
    } else {
        return Err(unsupported(
            "PipeWire capture requires packed RGB/BGR memory buffers",
        ));
    };
    let row = (width as usize)
        .checked_mul(channels)
        .ok_or_else(|| platform("frame width overflow"))?;
    let output_row = (width as usize)
        .checked_mul(4)
        .ok_or_else(|| platform("frame width overflow"))?;
    let len = output_row
        .checked_mul(height as usize)
        .filter(|n| *n <= MAX_FRAME_BYTES)
        .ok_or_else(|| platform("frame exceeds byte limit"))?;
    let stride = usize::try_from(stride)
        .map_err(|_| unsupported("negative PipeWire video strides are unsupported"))?;
    if row == 0 || height == 0 || stride < row {
        return Err(platform("invalid PipeWire stride/dimensions"));
    }
    let used = stride
        .checked_mul(height as usize - 1)
        .and_then(|v| v.checked_add(row))
        .ok_or_else(|| platform("frame stride overflow"))?;
    if used > size || offset.checked_add(size).is_none_or(|end| end > data.len()) {
        return Err(platform("PipeWire chunk exceeds mapped buffer"));
    }
    let mut pixels = vec![0; len];
    for y in 0..height as usize {
        let source = &data[offset + y * stride..offset + y * stride + row];
        let output = &mut pixels[y * output_row..(y + 1) * output_row];
        for (pixel, source) in output
            .as_chunks_mut::<4>()
            .0
            .iter_mut()
            .zip(source.chunks_exact(channels))
        {
            pixel[..3].copy_from_slice(&source[..3]);
            if [F::RGB, F::RGBx, F::RGBA].contains(&format) {
                pixel.swap(0, 2);
            }
            pixel[3] = 255;
        }
    }
    Ok(pixels)
}
