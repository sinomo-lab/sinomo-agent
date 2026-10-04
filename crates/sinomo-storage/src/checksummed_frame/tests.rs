use super::*;
use std::fs;
use std::io::Cursor;

fn root(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(name)
}

fn complete(reader: &mut (impl Read + Seek), path: &Path) -> ValidatedFrame {
    match read_frame(reader, path, FrameLimits::default()).expect("read frame") {
        FrameRead::Complete(frame) => frame,
        other => panic!("expected complete frame, got {other:?}"),
    }
}

fn failed_sync(path: &Path) -> DurableFileError {
    DurableFileError::Io {
        path: path.display().to_string(),
        kind: io::ErrorKind::Other,
        detail: "injected sync failure".into(),
    }
}

#[test]
fn batch_append_has_identical_frames_and_one_final_sync() {
    let root = root("checksummed-frame-batch");
    let path = root.join("batch.log");
    let serial_path = root.join("serial.log");
    let payload = vec![19; COPY_BUFFER_BYTES + 7];
    let records: &[(&[u8], &[u8])] = &[(b"first", b"one"), (b"second", &payload), (b"last", b"")];
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).unwrap();
    let mut serial = FrameAppender::open(&serial_path, FrameLimits::default()).unwrap();
    for (header, payload) in records {
        serial.append_bytes(header, payload).unwrap();
    }
    let expected_bytes = fs::read(&serial_path).unwrap();
    let syncs = std::cell::Cell::new(0);
    let frames = writer
        .append_batch_bytes_with_sync(records, |file, path| {
            syncs.set(syncs.get() + 1);
            assert_eq!(file.metadata().unwrap().len(), expected_bytes.len() as u64);
            sync_open_file(file, path, DurableFileSync::All)
        })
        .unwrap();
    assert_eq!(syncs.get(), 1);
    assert_eq!(frames.len(), records.len());
    assert_eq!(fs::read(&path).unwrap(), expected_bytes);
    let mut reader = File::open(&path).unwrap();
    for (index, frame) in frames.iter().enumerate() {
        assert_eq!(complete(&mut reader, &path), *frame.frame());
        assert_eq!(frame.prefix().frames(), index as u64 + 1);
        assert_eq!(frame.prefix().end(), frame.frame().frame_end());
        assert!(frame.prefix().belongs_to(&writer));
    }
    assert_eq!(
        writer.durable_prefix().digest(),
        serial.durable_prefix().digest()
    );
    drop(writer);
    let reopened = FrameAppender::open(&path, FrameLimits::default()).unwrap();
    assert_eq!(reopened.durable_prefix().frames(), 3);
    assert_eq!(
        reopened.durable_prefix().digest(),
        serial.durable_prefix().digest()
    );
}

#[test]
fn failed_batch_sync_preserves_old_frontier_and_blocks_even_empty_retry() {
    let root = root("checksummed-frame-batch-sync-error");
    let path = root.join("batch.log");
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).unwrap();
    let before = writer.append_bytes(b"old", b"durable").unwrap();
    let records: &[(&[u8], &[u8])] = &[(b"new-one", b"one"), (b"new-two", b"two")];
    assert!(matches!(
        writer.append_batch_bytes_with_sync(records, |_, path| Err(failed_sync(path))),
        Err(FrameError::Storage(_))
    ));
    assert_eq!(writer.durable_prefix().end(), before.prefix().end());
    assert_eq!(writer.durable_prefix().frames(), 1);
    assert!(!before.prefix().belongs_to(&writer));
    for records in [records, &[]] {
        assert!(matches!(
            writer.append_batch_bytes(records),
            Err(FrameError::RecoveryRequired)
        ));
    }
    assert!(
        writer
            .recover_with_sync(TailRecovery::RequireClean, |_, path| Err(failed_sync(path)))
            .is_err()
    );
    assert!(!writer.durable_prefix().belongs_to(&writer));
    let recovered = writer.recover(TailRecovery::RequireClean).unwrap();
    assert_eq!(recovered.frames(), 3);
    assert!(recovered.belongs_to(&writer));
    assert!(!before.prefix().belongs_to(&writer));
    writer
        .append_batch_bytes(&[(b"later", b"durable")])
        .unwrap();
}

#[test]
fn batch_recovery_keeps_complete_members_without_claiming_group_atomicity() {
    let root = root("checksummed-frame-batch-cut");
    let path = root.join("batch.log");
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).unwrap();
    let old = writer.append_bytes(b"old", b"durable").unwrap();
    assert!(
        writer
            .append_batch_bytes_with_sync(
                &[(b"first", b"one"), (b"second", b"two")],
                |_, path| Err(failed_sync(path))
            )
            .is_err()
    );
    let mut reader = File::open(&path).unwrap();
    complete(&mut reader, &path);
    let retained = complete(&mut reader, &path);
    // Only the uncertified batch tail is cut. No acknowledged byte is lost.
    writer.file.set_len(retained.frame_end() + 2).unwrap();
    let prefix = writer
        .recover(TailRecovery::TruncateIncomplete {
            minimum_durable_end: old.prefix().end(),
        })
        .unwrap();
    assert_eq!(prefix.frames(), 2);
    assert_eq!(prefix.end(), retained.frame_end());
    let replacement = writer.append_batch_bytes(&[(b"second", b"two")]).unwrap();
    assert_eq!(replacement[0].frame().frame_start(), retained.frame_end());
}

#[test]
fn batch_limits_are_preflighted_before_any_write_or_sync() {
    let root = root("checksummed-frame-batch-limits");
    let path = root.join("batch.log");
    let mut writer = FrameAppender::open(&path, FrameLimits::new(4, 8).unwrap()).unwrap();
    assert!(matches!(
        writer.append_batch_bytes(&[(b"ok", b"one"), (b"too-large", b"two")]),
        Err(FrameError::LengthLimit)
    ));
    let too_many = vec![(&b""[..], &b""[..]); MAX_BATCH_FRAMES + 1];
    assert!(matches!(
        writer.append_batch_bytes(&too_many),
        Err(FrameError::BatchLimit)
    ));
    let empty = writer
        .append_batch_bytes_with_sync(&[], |_, _| panic!("empty group must not sync"))
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    assert!(writer.durable_prefix().belongs_to(&writer));
    drop(writer);
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).unwrap();
    let payload = vec![0; 1024 * 1024];
    let bytes_over_limit = vec![(&b""[..], payload.as_slice()); 66];
    assert!(matches!(
        writer.append_batch_bytes(&bytes_over_limit),
        Err(FrameError::BatchLimit)
    ));
    assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    assert!(writer.durable_prefix().belongs_to(&writer));
}

#[test]
fn batch_real_write_failure_requires_recovery() {
    let root = root("checksummed-frame-batch-write-error");
    let path = root.join("batch.log");
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).unwrap();
    writer.file = File::open(&path).unwrap();
    assert!(matches!(
        writer.append_batch_bytes(&[(b"one", b"payload")]),
        Err(FrameError::Storage(_))
    ));
    assert_eq!(writer.durable_prefix().end(), 0);
    assert!(!writer.durable_prefix().belongs_to(&writer));
    assert!(matches!(
        writer.append_batch_bytes(&[]),
        Err(FrameError::RecoveryRequired)
    ));
}

#[test]
fn streamed_roundtrip_returns_exact_header_payload_offsets_and_prefix() {
    let root = root("checksummed-frame-roundtrip");
    let path = root.join("frames.log");
    let payload: Vec<u8> = (0..100_003).map(|index| (index % 251) as u8).collect();
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).expect("open");
    let first = writer
        .append_bytes(b"opaque domain header", &payload)
        .expect("append");
    let second = writer
        .append_bytes(b"empty payload", b"")
        .expect("empty frame");
    assert_eq!(first.frame().payload_bytes(), payload.len() as u64);
    assert_eq!(first.prefix().frames(), 1);
    assert_eq!(second.prefix().frames(), 2);
    assert_eq!(first.frame().frame_end(), second.frame().frame_start());
    assert!(first.prefix().belongs_to(&writer));

    let mut file = File::open(&path).expect("reader");
    let frame = complete(&mut file, &path);
    assert_eq!(frame, *first.frame());
    assert_eq!(frame.header(), b"opaque domain header");
    assert_eq!(frame.payload_checksum(), blake3::hash(&payload).as_bytes());
    let next = complete(&mut file, &path);
    assert_eq!(next, *second.frame());
    assert_eq!(
        read_frame(&mut file, &path, FrameLimits::default()).expect("EOF"),
        FrameRead::CleanEof {
            offset: second.prefix().end()
        }
    );
    file.seek(SeekFrom::Start(frame.payload_offset()))
        .expect("seek payload");
    let mut bytes = vec![0; frame.payload_bytes() as usize];
    file.read_exact(&mut bytes).expect("read payload");
    assert_eq!(bytes, payload);

    let digest = *second.prefix().digest();
    drop(writer);
    let reopened = FrameAppender::open(&path, FrameLimits::default()).expect("reopen and sync");
    assert_eq!(reopened.durable_prefix().digest(), &digest);
    assert_eq!(reopened.durable_prefix().frames(), 2);
    assert!(!first.prefix().belongs_to(&reopened));
}

#[test]
fn every_partial_byte_boundary_is_incomplete_not_a_complete_frame() {
    let root = root("checksummed-frame-byte-boundaries");
    let path = root.join("frames.log");
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).expect("open");
    writer
        .append_bytes(b"header", b"payload-with-a-tail")
        .expect("append");
    let bytes = fs::read(&path).expect("bytes");
    for length in 0..bytes.len() {
        let mut reader = Cursor::new(&bytes[..length]);
        let result = read_frame(&mut reader, &path, FrameLimits::default()).expect("bounded read");
        if length == 0 {
            assert_eq!(result, FrameRead::CleanEof { offset: 0 });
        } else {
            assert!(
                matches!(result, FrameRead::Incomplete(_)),
                "cut={length}, result={result:?}"
            );
        }
    }
    let mut reader = Cursor::new(bytes);
    complete(&mut reader, &path);
}

#[test]
fn complete_final_corruption_is_never_treated_as_a_torn_tail() {
    let root = root("checksummed-frame-corruption");
    let path = root.join("frames.log");
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).expect("open");
    let frame = writer.append_bytes(b"header", b"payload").expect("append");
    drop(writer);
    let original = fs::read(&path).expect("bytes");
    for index in 0..original.len() {
        let mut bytes = original.clone();
        bytes[index] ^= 0x80;
        let result =
            read_frame(&mut Cursor::new(bytes), &path, FrameLimits::default()).expect("read");
        assert!(
            matches!(result, FrameRead::Corrupt(_)),
            "mutation={index}, result={result:?}"
        );
    }
    let mut corrupt = original.clone();
    corrupt[frame.frame().payload_offset() as usize] ^= 1;
    fs::write(&path, &corrupt).expect("damage complete last frame");
    let result = FrameAppender::open_with_recovery(
        &path,
        FrameLimits::default(),
        TailRecovery::TruncateIncomplete {
            minimum_durable_end: 0,
        },
    );
    assert!(matches!(result, Err(FrameError::Corrupt(_))));
    assert_eq!(fs::read(&path).expect("unchanged"), corrupt);
}

struct BoundedReader<R> {
    inner: R,
    maximum_request: usize,
}

impl<R: Read> Read for BoundedReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        assert!(
            bytes.len() <= COPY_BUFFER_BYTES,
            "payload-sized allocation/read"
        );
        self.maximum_request = self.maximum_request.max(bytes.len());
        self.inner.read(bytes)
    }
}

impl<R: Seek> Seek for BoundedReader<R> {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.inner.seek(from)
    }
}

#[test]
fn payload_reads_and_writes_are_streaming_and_lengths_checked_before_body() {
    let root = root("checksummed-frame-streaming");
    let path = root.join("frames.log");
    let bytes = vec![7; COPY_BUFFER_BYTES * 3 + 11];
    let mut source = BoundedReader {
        inner: Cursor::new(&bytes),
        maximum_request: 0,
    };
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).expect("open");
    writer
        .append(b"small header", &mut source, bytes.len() as u64)
        .expect("append stream");
    assert_eq!(source.maximum_request, COPY_BUFFER_BYTES);
    let mut reader = BoundedReader {
        inner: File::open(&path).expect("file"),
        maximum_request: 0,
    };
    complete(&mut reader, &path);
    assert_eq!(reader.maximum_request, COPY_BUFFER_BYTES);

    // A valid prefix claiming an oversized payload must fail without reading or
    // allocating that payload. Corrupt lengths do not masquerade as torn EOF.
    let oversized = prefix(0, MAX_PAYLOAD_BYTES + 1);
    let mut reader = Cursor::new(oversized);
    assert!(matches!(
        read_frame(&mut reader, &path, FrameLimits::default()).expect("oversized"),
        FrameRead::Corrupt(CorruptFrame {
            reason: FrameCorruption::LengthLimit,
            ..
        })
    ));
    assert_eq!(reader.position(), PREFIX_BYTES as u64);
    let mut damaged = prefix(0, 1);
    damaged[16] ^= 0x40;
    assert!(matches!(
        read_frame(&mut Cursor::new(damaged), &path, FrameLimits::default())
            .expect("bad length checksum"),
        FrameRead::Corrupt(CorruptFrame {
            reason: FrameCorruption::PrefixChecksum,
            ..
        })
    ));
}

#[test]
fn failed_sync_visible_frame_cannot_certify_itself_or_allow_another_append() {
    let root = root("checksummed-frame-sync-failure");
    let path = root.join("frames.log");
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).expect("open");
    let result = writer.append_with_sync(b"binding", &mut Cursor::new(b"bytes"), 5, |_, path| {
        Err(failed_sync(path))
    });
    assert!(matches!(result, Err(FrameError::Storage(_))));
    assert_eq!(writer.durable_prefix().end(), 0);
    assert!(!writer.durable_prefix().belongs_to(&writer));
    let visible = complete(&mut File::open(&path).expect("reader"), &path);
    assert_eq!(visible.header(), b"binding");
    assert!(matches!(
        writer.append_bytes(b"later", b"payload"),
        Err(FrameError::RecoveryRequired)
    ));
    let failed_retry =
        writer.recover_with_sync(TailRecovery::RequireClean, |_, path| Err(failed_sync(path)));
    assert!(matches!(failed_retry, Err(FrameError::Storage(_))));
    assert_eq!(writer.durable_prefix().end(), 0);
    assert!(matches!(
        writer.append_bytes(b"later", b"payload"),
        Err(FrameError::RecoveryRequired)
    ));
    let recovered = writer
        .recover(TailRecovery::RequireClean)
        .expect("validate and actually resync");
    assert_eq!(recovered.end(), visible.frame_end());
    assert_eq!(recovered.frames(), 1);
    assert!(recovered.belongs_to(&writer));
    writer
        .append_bytes(b"later", b"payload")
        .expect("new append after recovery");
}

#[test]
fn incomplete_suffix_recovery_preserves_acknowledged_prefix_and_honors_frontier() {
    let root = root("checksummed-frame-tail-recovery");
    let path = root.join("frames.log");
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).expect("open");
    let first = writer.append_bytes(b"first", b"durable").expect("first");
    assert!(matches!(
        writer.append(b"second", &mut Cursor::new(b"short"), 10),
        Err(FrameError::PayloadEnded { .. })
    ));
    let before = fs::read(&path).expect("incomplete bytes");
    assert!(matches!(
        writer.recover(TailRecovery::RequireClean),
        Err(FrameError::Incomplete(_))
    ));
    assert_eq!(fs::read(&path).expect("no implicit truncation"), before);
    assert!(matches!(
        writer.recover(TailRecovery::TruncateIncomplete {
            minimum_durable_end: first.frame().frame_end() + 1
        }),
        Err(FrameError::DurablePrefixLost { .. })
    ));
    assert_eq!(fs::read(&path).expect("frontier refusal"), before);
    let recovered = writer
        .recover(TailRecovery::TruncateIncomplete {
            minimum_durable_end: first.frame().frame_end(),
        })
        .expect("explicit safe suffix recovery");
    assert_eq!(recovered.end(), first.frame().frame_end());
    assert_eq!(recovered.frames(), 1);
    assert!(!first.prefix().belongs_to(&writer));
    assert_eq!(
        complete(&mut File::open(&path).expect("reader"), &path),
        *first.frame()
    );
    let next = writer.append_bytes(b"second", b"complete").expect("resume");
    assert_eq!(next.frame().frame_start(), first.frame().frame_end());
}

#[test]
fn actual_write_error_poisoning_and_input_limits_do_not_mint_tokens() {
    let root = root("checksummed-frame-write-error");
    let path = root.join("frames.log");
    let limits = FrameLimits::new(4, 8).expect("limits");
    let mut writer = FrameAppender::open(&path, limits).expect("open");
    assert!(matches!(
        writer.append_bytes(b"large-header", b""),
        Err(FrameError::LengthLimit)
    ));
    assert!(matches!(
        writer.append_bytes(b"ok", b"123456789"),
        Err(FrameError::LengthLimit)
    ));
    assert_eq!(fs::metadata(&path).expect("metadata").len(), 0);
    assert!(writer.durable_prefix().belongs_to(&writer));
    // Private test access substitutes a read-only handle to exercise a real
    // operating-system write failure without depending on disk-full behavior.
    writer.file = File::open(&path).expect("read only");
    assert!(matches!(
        writer.append_bytes(b"ok", b"bytes"),
        Err(FrameError::Storage(_))
    ));
    assert!(matches!(
        writer.append_bytes(b"ok", b"bytes"),
        Err(FrameError::RecoveryRequired)
    ));
    assert_eq!(writer.durable_prefix().end(), 0);
}

#[test]
fn each_successful_append_runs_a_barrier_even_for_identical_frame_bytes() {
    let root = root("checksummed-frame-required-barrier");
    let path = root.join("frames.log");
    let mut writer = FrameAppender::open(&path, FrameLimits::default()).expect("open");
    let mut barriers = 0;
    for _ in 0..2 {
        let token = writer
            .append_with_sync(b"same", &mut Cursor::new(b"payload"), 7, |file, path| {
                barriers += 1;
                sync_open_file(file, path, DurableFileSync::All)
            })
            .expect("append and sync");
        assert_eq!(token.prefix().frames(), barriers);
    }
    assert_eq!(barriers, 2);
}

#[test]
fn distinct_rotation_files_keep_old_reader_offsets_and_reject_cross_file_tokens() {
    let root = root("checksummed-frame-rotation");
    let old_path = root.join("segment-0.log");
    let new_path = root.join("segment-1.log");
    let mut old = FrameAppender::open(&old_path, FrameLimits::default()).expect("old segment");
    let old_token = old
        .append_bytes(b"generation zero", b"retained bytes")
        .expect("old data");
    let mut retained = File::open(&old_path).expect("held reader");
    drop(old);
    let mut next = FrameAppender::open(&new_path, FrameLimits::default())
        .expect("new segment and parent durability");
    let new_token = next
        .append_bytes(b"generation one", b"next bytes")
        .expect("new data");
    assert_eq!(new_token.frame().frame_start(), 0);
    assert!(!old_token.prefix().belongs_to(&next));
    assert!(new_token.prefix().belongs_to(&next));
    retained
        .seek(SeekFrom::Start(old_token.frame().payload_offset()))
        .expect("old offset");
    let mut old_bytes = [0; 14];
    retained
        .read_exact(&mut old_bytes)
        .expect("retained payload");
    assert_eq!(&old_bytes, b"retained bytes");
    // Pointer publication and retired-file reclamation remain the domain
    // owner's job; this primitive never removes either segment implicitly.
    assert!(old_path.is_file());
    assert!(new_path.is_file());
}

struct ReadFailure(Cursor<Vec<u8>>);
impl Read for ReadFailure {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected read error",
        ))
    }
}
impl Seek for ReadFailure {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.0.seek(position)
    }
}

#[test]
fn io_errors_remain_errors_and_are_not_reclassified_as_incomplete() {
    let mut reader = ReadFailure(Cursor::new(Vec::new()));
    let result = read_frame(
        &mut reader,
        Path::new("diagnostic.log"),
        FrameLimits::default(),
    );
    assert!(matches!(
        result,
        Err(FrameError::Storage(DurableFileError::Io {
            kind: io::ErrorKind::PermissionDenied,
            ..
        }))
    ));
}
