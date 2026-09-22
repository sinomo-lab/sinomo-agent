//! Bounded process log collection and a read-only tail of operational journals.
use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use fs2::FileExt;
use time::OffsetDateTime;

use crate::{ObserveLogSource, ObserveLogsRequest, ObserveLogsResponse, OperationalLogRecord};

const LOG_BUFFER_RECORDS: usize = 4_096;
const LOG_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const LOG_RECORD_BYTES: usize = 16 * 1024;
const LOG_DISK_SCAN_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct LogBuffer {
    records: VecDeque<(OperationalLogRecord, usize)>,
    bytes: usize,
    evicted: u64,
}

impl LogBuffer {
    pub(crate) fn push(&mut self, mut record: OperationalLogRecord) {
        bound_record(&mut record);
        let bytes = record_size(&record);
        while self.records.len() >= LOG_BUFFER_RECORDS || self.bytes + bytes > LOG_BUFFER_BYTES {
            let Some((_, old_bytes)) = self.records.pop_front() else {
                break;
            };
            self.bytes -= old_bytes;
            self.evicted = self.evicted.saturating_add(1);
        }
        self.bytes += bytes;
        self.records.push_back((record, bytes));
    }

    pub(crate) fn snapshot(
        &self,
        request: &ObserveLogsRequest,
    ) -> (Vec<OperationalLogRecord>, u64, bool) {
        let mut records = self
            .records
            .iter()
            .rev()
            .filter(|(record, _)| request.matches(record))
            .take(request.bounded_limit() + 1)
            .map(|(record, _)| record.clone())
            .collect::<Vec<_>>();
        let truncated = records.len() > request.bounded_limit();
        records.truncate(request.bounded_limit());
        records.reverse();
        (records, self.evicted, truncated)
    }
}

fn record_size(record: &OperationalLogRecord) -> usize {
    serde_json::to_vec(record)
        .expect("log records serialize")
        .len()
}

pub(crate) fn truncate_text(value: &mut String, bytes: usize) -> bool {
    if value.len() <= bytes {
        return false;
    }
    let mut end = bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    true
}

fn bound_record(record: &mut OperationalLogRecord) {
    record.truncated |= truncate_text(&mut record.message, 4_096);
    record.truncated |= truncate_text(&mut record.component, 256);
    if let Some(id) = record.operation_id.as_mut() {
        record.truncated |= truncate_text(id, 256);
    }
    if record_size(record) > LOG_RECORD_BYTES {
        // Keep a readable prefix and explicitly identify omitted structured data.
        let mut fields = record.fields.to_string();
        truncate_text(&mut fields, 1_024);
        record.fields = serde_json::json!({"truncated_fields": fields});
        truncate_text(&mut record.message, 1_024);
        record.truncated = true;
    }
}

/// Collect enabled runtime logging and retained operational diagnostics. This
/// synchronous disk reader belongs on a blocking worker, never an async worker.
/// Collection is a tail snapshot, not an audit archive or a cross-node ordering.
pub fn read_observe_logs(data_dir: &Path, request: &ObserveLogsRequest) -> ObserveLogsResponse {
    let (role, node_id) = crate::runtime::observe_runtime_process_identity();
    let mut response = ObserveLogsResponse {
        observed_at: Some(OffsetDateTime::now_utc()),
        process_started_at: crate::observe_process_started_at(),
        process_role: Some(role),
        node_id,
        ..Default::default()
    };
    if let Some((records, evicted, truncated)) = crate::runtime::runtime_logs_snapshot(request) {
        response.runtime_available = true;
        response.runtime_evicted = evicted;
        response.truncated = truncated;
        response.records = records;
    }
    if request.source != Some(ObserveLogSource::Runtime) {
        read_operational_tail(data_dir, request, &mut response);
    }
    response.records.sort_by_key(|record| record.timestamp);
    let limit = request.bounded_limit();
    if response.records.len() > limit {
        response.truncated = true;
        response.records.drain(..response.records.len() - limit);
    }
    // Account for JSON escaping as well as record count, leaving room for
    // provenance, source errors and the response envelope.
    let mut bytes = 4_096;
    let mut keep = 0;
    for record in response.records.iter().rev() {
        bytes += record_size(record) + 1;
        if bytes > crate::OBSERVE_LOGS_MAX_RESPONSE_BYTES {
            break;
        }
        keep += 1;
    }
    if keep < response.records.len() {
        response.truncated = true;
        response.records.drain(..response.records.len() - keep);
    }
    response
}

fn read_operational_tail(
    data_dir: &Path,
    request: &ObserveLogsRequest,
    response: &mut ObserveLogsResponse,
) {
    let path = crate::operational_logs_path(data_dir);
    let result = (|| -> Result<(), String> {
        let lock = crate::open_jsonl_lock(&path).map_err(|e| e.to_string())?;
        // A log viewer must not queue behind a writer or a stalled filesystem.
        // Report this source as temporarily unavailable while returning runtime logs.
        FileExt::try_lock_shared(&lock)
            .map_err(|e| format!("operational log journal is busy: {e}"))?;
        let mut remaining = LOG_DISK_SCAN_BYTES;
        let mut matched = 0;
        for path in
            crate::jsonl_read_paths_chronological(&path, crate::OPERATIONAL_JSONL_RETAINED_FILES)
                .into_iter()
                .rev()
        {
            let mut file = match std::fs::File::open(&path) {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(format!("operational log {}: {e}", path.display())),
            };
            let size = file.metadata().map_err(|e| e.to_string())?.len();
            let read = size.min(remaining);
            if read < size {
                response.truncated = true;
            }
            if read == 0 {
                continue;
            }
            let start = size - read;
            // Read the preceding byte so an exact newline boundary does not
            // cause the first complete record in the scan window to be skipped.
            file.seek(SeekFrom::Start(start.saturating_sub(1)))
                .map_err(|e| e.to_string())?;
            let mut bytes = Vec::with_capacity(read as usize + 1);
            file.take(read + u64::from(start > 0))
                .read_to_end(&mut bytes)
                .map_err(|e| e.to_string())?;
            remaining -= read;
            let skip = if start == 0 {
                0
            } else {
                bytes
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(bytes.len(), |index| index + 1)
            };
            let mut invalid = 0;
            for line in bytes[skip..].split(|byte| *byte == b'\n').rev() {
                let line = line.trim_ascii();
                let line = line.strip_prefix(&[0]).map_or(line, |_| {
                    let first = line
                        .iter()
                        .position(|byte| *byte != 0)
                        .unwrap_or(line.len());
                    line[first..].trim_ascii_start()
                });
                if line.is_empty() {
                    continue;
                }
                let mut record = match serde_json::from_slice::<OperationalLogRecord>(line) {
                    Ok(record) => record,
                    Err(_) => {
                        invalid += 1;
                        continue;
                    }
                };
                record.source = ObserveLogSource::Operational;
                if !request.matches(&record) {
                    continue;
                }
                matched += 1;
                if matched > request.bounded_limit() {
                    response.truncated = true;
                    break;
                }
                bound_record(&mut record);
                response.records.push(record);
            }
            if invalid > 0 {
                response.errors.push(format!(
                    "operational log {}: {invalid} malformed records omitted",
                    path.file_name().unwrap_or_default().to_string_lossy()
                ));
            }
            if matched > request.bounded_limit() {
                break;
            }
        }
        Ok(())
    })();
    if let Err(mut error) = result {
        truncate_text(&mut error, 512);
        response.errors.push(error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OperationalSeverity;

    fn record(message: String) -> OperationalLogRecord {
        OperationalLogRecord {
            timestamp: OffsetDateTime::now_utc(),
            severity: OperationalSeverity::Warn,
            component: "test".into(),
            message,
            operation_id: None,
            fields: serde_json::Value::Null,
            source: ObserveLogSource::Runtime,
            truncated: false,
        }
    }

    #[test]
    fn collection_bounds_bytes_and_records_and_preserves_utf8() {
        let mut buffer = LogBuffer::default();
        for _ in 0..5_000 {
            buffer.push(record("界\u{0001}".repeat(10_000)));
        }
        assert!(buffer.records.len() <= LOG_BUFFER_RECORDS);
        assert!(buffer.bytes <= LOG_BUFFER_BYTES);
        assert!(buffer.evicted > 0);
        let (records, _, truncated) = buffer.snapshot(&ObserveLogsRequest {
            limit: 2,
            ..Default::default()
        });
        assert_eq!(records.len(), 2);
        assert!(truncated);
        assert!(
            records
                .iter()
                .all(|r| r.truncated && record_size(r) <= LOG_RECORD_BYTES)
        );
    }

    #[test]
    fn disk_tail_filters_before_limiting_and_reports_corruption() {
        let dir = sinomo_testdir::TestDir::new("observe-log-tail");
        for message in ["match old", "skip", "match new", "skip again"] {
            crate::append_operational_log(
                dir.path(),
                OperationalSeverity::Info,
                "dfs",
                message,
                None,
                serde_json::Value::Null,
            )
            .unwrap();
        }
        use std::io::Write;
        writeln!(
            std::fs::OpenOptions::new()
                .append(true)
                .open(crate::operational_logs_path(dir.path()))
                .unwrap(),
            "broken record"
        )
        .unwrap();
        let response = read_observe_logs(
            dir.path(),
            &ObserveLogsRequest {
                contains: Some("match".into()),
                source: Some(ObserveLogSource::Operational),
                limit: 1,
                ..Default::default()
            },
        );
        assert_eq!(response.records.len(), 1);
        assert_eq!(response.records[0].message, "match new");
        assert!(response.truncated);
        assert_eq!(response.errors.len(), 1);
        assert!(
            serde_json::to_vec(&response).unwrap().len() <= crate::OBSERVE_LOGS_MAX_RESPONSE_BYTES
        );
    }
    #[test]
    fn disk_scan_is_bounded_and_preserves_complete_tail_records() {
        use std::io::Write;
        let dir = sinomo_testdir::TestDir::new("observe-log-scan-bound");
        let path = crate::operational_logs_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = std::fs::File::create(&path).unwrap();
        file.seek(SeekFrom::Start(LOG_DISK_SCAN_BYTES + 1_024))
            .unwrap();
        writeln!(file).unwrap();
        for message in ["one", "two"] {
            writeln!(
                file,
                "{}",
                serde_json::to_string(&record(message.into())).unwrap()
            )
            .unwrap();
        }
        drop(file);
        let response = read_observe_logs(
            dir.path(),
            &ObserveLogsRequest {
                source: Some(ObserveLogSource::Operational),
                ..Default::default()
            },
        );
        assert_eq!(
            response
                .records
                .iter()
                .map(|r| r.message.as_str())
                .collect::<Vec<_>>(),
            ["one", "two"]
        );
        assert!(response.truncated);
        assert!(response.errors.is_empty());
    }

    #[test]
    fn busy_journal_does_not_discard_collected_runtime_logs() {
        let dir = sinomo_testdir::TestDir::new("observe-log-busy");
        let lock = crate::open_jsonl_lock(&crate::operational_logs_path(dir.path())).unwrap();
        FileExt::lock_exclusive(&lock).unwrap();
        let mut response = ObserveLogsResponse {
            records: vec![record("runtime survives".into())],
            ..Default::default()
        };
        read_operational_tail(dir.path(), &ObserveLogsRequest::default(), &mut response);
        assert_eq!(response.records.len(), 1);
        assert!(response.errors[0].contains("busy"));
        FileExt::unlock(&lock).unwrap();
    }
}
