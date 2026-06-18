use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedClusterKvWatchEventLogBatch {
    pub records: Vec<Vec<u8>>,
    pub bytes: u64,
}

pub fn open_cluster_kv_watch_event_log_for_append(paths: &ClusterKvPaths) -> Result<File, KvError> {
    durable_file::open_file_for_append(&paths.watch_events_log)
        .map(|append| append.file)
        .map_err(kv_durable_file_error)
}

pub fn lock_cluster_kv_watch_event_log_for_append(
    paths: &ClusterKvPaths,
    file: &File,
) -> Result<(), KvError> {
    file.lock_exclusive().map_err(|source| KvError::Io {
        path: paths.watch_events_log.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn encode_cluster_kv_watch_event_log_batch(
    events: &[KvEvent],
) -> Result<EncodedClusterKvWatchEventLogBatch, KvError> {
    let mut records = Vec::with_capacity(events.len());
    let mut bytes = 0_u64;
    for event in events {
        let record = sinomo_object_serde::to_bytes(event, KV_OBJECT_VERSION_ID, LengthMode::U32)
            .map_err(|source| KvError::Serialization {
                detail: source.to_string(),
            })?;
        let len = u32::try_from(record.len()).map_err(|_| KvError::Serialization {
            detail: "cluster kv watch-event log record exceeds u32 length".to_string(),
        })?;
        bytes = bytes.saturating_add(4).saturating_add(u64::from(len));
        records.push(record);
    }
    Ok(EncodedClusterKvWatchEventLogBatch { records, bytes })
}

pub fn append_cluster_kv_watch_event_log_batch_to_file(
    paths: &ClusterKvPaths,
    file: &mut File,
    batch: &EncodedClusterKvWatchEventLogBatch,
) -> Result<u64, KvError> {
    if batch.records.is_empty() {
        return Ok(0);
    }
    durable_file::append_length_prefixed_records(
        file,
        &paths.watch_events_log,
        batch.records.iter().map(Vec::as_slice),
        durable_file::DurableRecordAppendOptions::new(durable_file::DurableLengthEndian::Big)
            .sync(durable_file::DurableFileSync::Data),
    )
    .map_err(kv_durable_file_error)
}

pub fn truncate_cluster_kv_watch_event_log_file(
    paths: &ClusterKvPaths,
    file: &mut File,
) -> Result<(), KvError> {
    durable_file::truncate_open_file_and_sync(
        file,
        &paths.watch_events_log,
        0,
        durable_file::DurableFileSync::Data,
    )
    .map_err(kv_durable_file_error)
}

pub fn decode_cluster_kv_watch_event_log_record(record: &[u8]) -> Result<KvEvent, KvError> {
    sinomo_object_serde::from_bytes(record).map_err(|source| KvError::Serialization {
        detail: source.to_string(),
    })
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventsRead {
    pub events: Vec<KvEvent>,
    pub log_records: usize,
    pub append_safe: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventLogRead {
    pub events: Vec<KvEvent>,
    pub records: usize,
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventAppendCacheView {
    pub events: Vec<KvEvent>,
    pub log_records: usize,
    pub log_bytes: u64,
    pub append_safe: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventAppendReport {
    pub appended: Vec<KvEvent>,
    pub retained: Vec<KvEvent>,
    pub log_records: usize,
    pub append_safe: bool,
}

pub fn read_cluster_kv_watch_events(
    paths: &ClusterKvPaths,
) -> Result<ClusterKvWatchEventsRead, KvError> {
    read_cluster_kv_watch_events_with_observer(paths, |_| {})
}

pub fn read_cluster_kv_watch_events_with_observer<F>(
    paths: &ClusterKvPaths,
    on_log_read: F,
) -> Result<ClusterKvWatchEventsRead, KvError>
where
    F: FnMut(u64),
{
    let checkpoint = read_cluster_kv_watch_event_checkpoint(paths)?;
    let log = read_cluster_kv_watch_event_log_with_observer(paths, on_log_read)?;
    let events =
        compact_cluster_kv_watch_events(checkpoint.into_iter().chain(log.events).collect());
    Ok(ClusterKvWatchEventsRead {
        events,
        log_records: log.records,
        append_safe: !log.truncated,
    })
}

pub fn read_cluster_kv_watch_event_checkpoint(
    paths: &ClusterKvPaths,
) -> Result<Vec<KvEvent>, KvError> {
    read_optional_object(&paths.watch_events).map(|events| events.unwrap_or_default())
}

pub fn write_cluster_kv_watch_event_checkpoint(
    paths: &ClusterKvPaths,
    events: &[KvEvent],
) -> Result<(), KvError> {
    let parent = paths
        .watch_events
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    fs::create_dir_all(parent).map_err(|source| KvError::Io {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    write_object_file(&paths.watch_events, &events)
}

pub fn append_cluster_kv_watch_events_to_storage(
    paths: &ClusterKvPaths,
    incoming: &[KvEvent],
    cache: Option<&ClusterKvWatchEventAppendCacheView>,
) -> Result<ClusterKvWatchEventAppendReport, KvError> {
    append_cluster_kv_watch_events_to_storage_with_observers(paths, incoming, cache, |_| {}, |_| {})
}

pub fn append_cluster_kv_watch_events_to_storage_with_observers<R, W>(
    paths: &ClusterKvPaths,
    incoming: &[KvEvent],
    cache: Option<&ClusterKvWatchEventAppendCacheView>,
    mut on_log_read: R,
    mut on_log_write: W,
) -> Result<ClusterKvWatchEventAppendReport, KvError>
where
    R: FnMut(u64),
    W: FnMut(u64),
{
    if incoming.is_empty() {
        return Ok(ClusterKvWatchEventAppendReport::default());
    }

    let mut log_file = open_cluster_kv_watch_event_log_for_append(paths)?;
    lock_cluster_kv_watch_event_log_for_append(paths, &log_file)?;

    let (existing, log_records, log_bytes) =
        if let Some(cache) = cache.filter(|cache| cache.append_safe) {
            (cache.events.clone(), cache.log_records, cache.log_bytes)
        } else {
            let checkpoint = read_cluster_kv_watch_event_checkpoint(paths)?;
            let log = read_cluster_kv_watch_event_log_from_file_with_observer(
                paths,
                &mut log_file,
                &mut on_log_read,
            )?;
            let existing =
                compact_cluster_kv_watch_events(checkpoint.into_iter().chain(log.events).collect());
            if log.truncated {
                write_cluster_kv_watch_event_checkpoint(paths, &existing)?;
                truncate_cluster_kv_watch_event_log_file(paths, &mut log_file)?;
                (existing, 0, 0)
            } else {
                let log_bytes = log_file
                    .metadata()
                    .map(|metadata| metadata.len())
                    .unwrap_or(0);
                (existing, log.records, log_bytes)
            }
        };

    let retained = compact_cluster_kv_watch_events_incremental(existing.clone(), incoming);
    let appended = retained
        .iter()
        .filter(|event| !cluster_kv_events_contain(&existing, event))
        .cloned()
        .collect::<Vec<_>>();
    if appended.is_empty() {
        return Ok(ClusterKvWatchEventAppendReport {
            appended,
            retained,
            log_records,
            append_safe: true,
        });
    }

    let encoded_appended = encode_cluster_kv_watch_event_log_batch(&appended)?;
    let mut current_log_records = log_records;
    let should_checkpoint_without_append = cluster_kv_watch_event_append_should_checkpoint(
        incoming.len(),
        appended.len(),
        current_log_records,
        log_bytes,
        encoded_appended.bytes,
    );
    if should_checkpoint_without_append {
        write_cluster_kv_watch_event_checkpoint(paths, &retained)?;
        truncate_cluster_kv_watch_event_log_file(paths, &mut log_file)?;
        current_log_records = 0;
    } else {
        let written = append_cluster_kv_watch_event_log_batch_to_file(
            paths,
            &mut log_file,
            &encoded_appended,
        )?;
        if written > 0 {
            on_log_write(written);
        }
        current_log_records = current_log_records.saturating_add(appended.len());
        if log_file
            .metadata()
            .map(|metadata| metadata.len() >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES)
            .unwrap_or(false)
        {
            write_cluster_kv_watch_event_checkpoint(paths, &retained)?;
            truncate_cluster_kv_watch_event_log_file(paths, &mut log_file)?;
            current_log_records = 0;
        }
    }

    Ok(ClusterKvWatchEventAppendReport {
        appended,
        retained,
        log_records: current_log_records,
        append_safe: true,
    })
}

pub fn read_cluster_kv_watch_event_log(
    paths: &ClusterKvPaths,
) -> Result<ClusterKvWatchEventLogRead, KvError> {
    read_cluster_kv_watch_event_log_with_observer(paths, |_| {})
}

pub fn read_cluster_kv_watch_event_log_with_observer<F>(
    paths: &ClusterKvPaths,
    on_log_read: F,
) -> Result<ClusterKvWatchEventLogRead, KvError>
where
    F: FnMut(u64),
{
    let mut file = match File::open(&paths.watch_events_log) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ClusterKvWatchEventLogRead::default());
        }
        Err(source) => {
            return Err(KvError::Io {
                path: paths.watch_events_log.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    file.lock_shared().map_err(|source| KvError::Io {
        path: paths.watch_events_log.display().to_string(),
        detail: source.to_string(),
    })?;
    read_cluster_kv_watch_event_log_from_file_with_observer(paths, &mut file, on_log_read)
}

pub fn read_cluster_kv_watch_event_log_from_file_with_observer<F>(
    paths: &ClusterKvPaths,
    file: &mut File,
    mut on_log_read: F,
) -> Result<ClusterKvWatchEventLogRead, KvError>
where
    F: FnMut(u64),
{
    file.seek(SeekFrom::Start(0))
        .map_err(|source| KvError::Io {
            path: paths.watch_events_log.display().to_string(),
            detail: source.to_string(),
        })?;
    let mut events = Vec::new();
    let mut record_number = 0_usize;
    let mut read_bytes = 0_u64;
    let mut needs_repair = false;
    loop {
        let next_record_number = record_number.saturating_add(1);
        let record_start = file.stream_position().map_err(|source| KvError::Io {
            path: paths.watch_events_log.display().to_string(),
            detail: source.to_string(),
        })?;
        let file_len = file
            .metadata()
            .map_err(|source| KvError::Io {
                path: paths.watch_events_log.display().to_string(),
                detail: source.to_string(),
            })?
            .len();
        let record = match durable_file::read_length_prefixed_record_with_observer(
            file,
            &paths.watch_events_log,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Big)
                .max_record_bytes(MAX_CLUSTER_KV_WATCH_EVENT_RECORD_BYTES),
            |bytes| {
                read_bytes = read_bytes.saturating_add(bytes);
                on_log_read(bytes);
            },
        ) {
            Ok(Some(record)) => record,
            Ok(None) => {
                if record_start < file_len {
                    eprintln!(
                        "warning: ignoring partial cluster kv watch-event log record {} in {}",
                        next_record_number,
                        paths.watch_events_log.display()
                    );
                    return Ok(ClusterKvWatchEventLogRead {
                        events,
                        records: record_number,
                        truncated: true,
                    });
                }
                break;
            }
            Err(error) if error.detail().contains("length-prefixed record is") => {
                eprintln!(
                    "warning: ignoring corrupt cluster kv watch-event log record {} in {}",
                    next_record_number,
                    paths.watch_events_log.display()
                );
                return Ok(ClusterKvWatchEventLogRead {
                    events,
                    records: record_number,
                    truncated: true,
                });
            }
            Err(error) => return Err(kv_durable_file_error(error)),
        };
        record_number = next_record_number;
        if record.bytes.is_empty() {
            eprintln!(
                "warning: ignoring corrupt cluster kv watch-event log record {} in {}",
                record_number,
                paths.watch_events_log.display()
            );
            return Ok(ClusterKvWatchEventLogRead {
                events,
                records: record_number.saturating_sub(1),
                truncated: true,
            });
        }
        match decode_cluster_kv_watch_event_log_record(&record.bytes) {
            Ok(event) => events.push(event),
            Err(source) => {
                eprintln!(
                    "warning: ignoring unreadable cluster kv watch-event log record {} in {}: {}",
                    record_number,
                    paths.watch_events_log.display(),
                    source
                );
                return Ok(ClusterKvWatchEventLogRead {
                    events,
                    records: record_number.saturating_sub(1),
                    truncated: true,
                });
            }
        }
        if record_number >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS
            || read_bytes >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES
        {
            needs_repair = true;
        }
        if events.len() > MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS {
            events = compact_cluster_kv_watch_events(events);
        }
    }
    if needs_repair {
        eprintln!(
            "warning: repairing overgrown cluster kv watch-event log {} after reading {} records / {} bytes",
            paths.watch_events_log.display(),
            record_number,
            read_bytes
        );
    }
    Ok(ClusterKvWatchEventLogRead {
        events,
        records: record_number,
        truncated: needs_repair,
    })
}

pub fn cluster_kv_watch_event_payload_bytes(event: &KvEvent) -> usize {
    let value_bytes = match &event.kind {
        KvEventKind::Put { value } => value.len(),
        KvEventKind::Delete => 0,
    };
    event.key.len().saturating_add(value_bytes)
}
