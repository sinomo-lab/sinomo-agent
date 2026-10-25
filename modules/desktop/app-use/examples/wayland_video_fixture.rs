//! Disposable native PipeWire video producer for the private Wayland fixture.
#[cfg(not(target_os = "linux"))]
fn main() {}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use pipewire as pw;
    use pw::{properties::properties, spa};
    use std::io::Read;
    use std::time::Duration;
    pw::init();
    let mainloop = pw::main_loop::MainLoopRc::new(None)?;
    let context = pw::context::ContextRc::new(&mainloop, None)?;
    let core = context.connect_rc(None)?;
    let stream = pw::stream::StreamBox::new(
        &core,
        "Sinomo Wayland fixture",
        properties! {
            *pw::keys::MEDIA_TYPE => "Video", *pw::keys::MEDIA_CATEGORY => "Capture",
            *pw::keys::MEDIA_CLASS => "Video/Source", *pw::keys::NODE_NAME => "sinomo-wayland-test-source",
            "node.always-process" => "true", "node.want-driver" => "true",
        },
    )?;
    let _listener = stream
        .add_local_listener_with_user_data(0u64)
        .param_changed(|stream, _, id, pod| {
            if id != spa::param::ParamType::Format.as_raw() || pod.is_none() {
                return;
            }
            let integer = |key, value| spa::pod::Property {
                key,
                flags: spa::pod::PropertyFlags::empty(),
                value: spa::pod::Value::Int(value),
            };
            let types = (1 << spa::buffer::DataType::MemPtr.as_raw())
                | (1 << spa::buffer::DataType::MemFd.as_raw());
            let object = spa::pod::object!(
                spa::utils::SpaTypes::ObjectParamBuffers,
                spa::param::ParamType::Buffers,
                integer(spa::sys::SPA_PARAM_BUFFERS_buffers, 4),
                integer(spa::sys::SPA_PARAM_BUFFERS_blocks, 1),
                integer(spa::sys::SPA_PARAM_BUFFERS_size, 64 * 48 * 4),
                integer(spa::sys::SPA_PARAM_BUFFERS_stride, 64 * 4),
                spa::pod::Property {
                    key: spa::sys::SPA_PARAM_BUFFERS_dataType,
                    flags: spa::pod::PropertyFlags::empty(),
                    value: spa::pod::Value::Choice(spa::pod::ChoiceValue::Int(spa::utils::Choice(
                        spa::utils::ChoiceFlags::empty(),
                        spa::utils::ChoiceEnum::Flags {
                            default: types,
                            flags: vec![types]
                        }
                    )))
                }
            );
            let values = spa::pod::serialize::PodSerializer::serialize(
                std::io::Cursor::new(Vec::new()),
                &spa::pod::Value::Object(object),
            )
            .unwrap()
            .0
            .into_inner();
            if let Err(error) =
                stream.update_params(&mut [spa::pod::Pod::from_bytes(&values).unwrap()])
            {
                eprintln!("fixture buffers: {error}");
            }
        })
        .process(|stream, count| {
            let Some(mut buffer) = stream.dequeue_buffer() else {
                return;
            };
            let Some(data) = buffer.datas_mut().first_mut() else {
                return;
            };
            let Some(bytes) = data.data() else {
                return;
            };
            let len = 64 * 48 * 4;
            if bytes.len() < len {
                return;
            }
            *count += 1;
            for (index, pixel) in bytes[..len].as_chunks_mut::<4>().0.iter_mut().enumerate() {
                *pixel = [(*count % 255) as u8, (index % 255) as u8, 170, 255];
            }
            let chunk = data.chunk_mut();
            *chunk.offset_mut() = 0;
            *chunk.stride_mut() = 64 * 4;
            *chunk.size_mut() = len as u32;
        })
        .register()?;
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
            Id,
            spa::param::video::VideoFormat::BGRx
        ),
        spa::pod::property!(
            spa::param::format::FormatProperties::VideoSize,
            Rectangle,
            spa::utils::Rectangle {
                width: 64,
                height: 48
            }
        ),
        spa::pod::property!(
            spa::param::format::FormatProperties::VideoFramerate,
            Fraction,
            spa::utils::Fraction { num: 25, denom: 1 }
        )
    );
    let values = spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &spa::pod::Value::Object(object),
    )?
    .0
    .into_inner();
    stream.connect(
        spa::utils::Direction::Output,
        None,
        pw::stream::StreamFlags::MAP_BUFFERS,
        &mut [spa::pod::Pod::from_bytes(&values).unwrap()],
    )?;
    let (tx, stop) = std::sync::mpsc::channel();
    let stdin = std::thread::spawn(move || {
        let _ = std::io::stdin().read(&mut [0]);
        let _ = tx.send(());
    });
    while stop.try_recv().is_err() {
        mainloop
            .loop_()
            .iterate(pw::loop_::Timeout::Finite(Duration::from_millis(50)));
    }
    stdin.join().unwrap();
    Ok(())
}
