use serde_json::json;
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
use sinomo_dfs::{
    DfsFileSystem, DfsFileSystemId, DfsMetadataShardId, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsReplicationPolicy, DfsVersion, LocalContentStore,
    LocalJsonMetadataBackend, NetworkedDfsService, NetworkedDfsServiceEvent,
};
use sinomo_network::InProcessNetwork;
use sinomo_node::NodeId;
use std::env;
use std::fs;
use std::io::{Read, Result as IoResult};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug)]
struct Args {
    scenario: Scenario,
    root: PathBuf,
    explicit_root: bool,
    files: usize,
    dirs: usize,
    file_bytes: usize,
    blob_bytes: u64,
    blob_range_bytes: usize,
    network_blob_bytes: u64,
    list_page_size: usize,
    keep: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    All,
    Namespace,
    MetadataReplication,
    Blob,
    NetworkBlob,
}

impl Scenario {
    fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "all" => Ok(Self::All),
            "namespace" => Ok(Self::Namespace),
            "metadata-replication" => Ok(Self::MetadataReplication),
            "blob" => Ok(Self::Blob),
            "network-blob" => Ok(Self::NetworkBlob),
            _ => Err(format!(
                "unknown scenario `{value}`; use all, namespace, metadata-replication, blob, or network-blob"
            )
            .into()),
        }
    }
}

fn main() -> BenchResult<()> {
    let args = Args::parse()?;
    match args.scenario {
        Scenario::All => {
            run_namespace(&args)?;
            run_metadata_replication(&args)?;
            run_blob(&args)?;
            run_network_blob(&args)?;
        }
        Scenario::Namespace => run_namespace(&args)?,
        Scenario::MetadataReplication => run_metadata_replication(&args)?,
        Scenario::Blob => run_blob(&args)?,
        Scenario::NetworkBlob => run_network_blob(&args)?,
    }
    if !args.keep && !args.explicit_root {
        remove_dir_all_if_exists(&args.root)?;
    }
    Ok(())
}

impl Args {
    fn parse() -> BenchResult<Self> {
        let mut args = env::args().skip(1);
        let mut scenario = Scenario::All;
        let mut root = None::<PathBuf>;
        let mut files = env_usize("SINOMO_DFS_BENCH_FILES", 10_000)?;
        let mut dirs = env_usize("SINOMO_DFS_BENCH_DIRS", 128)?;
        let mut file_bytes = env_usize("SINOMO_DFS_BENCH_FILE_BYTES", 32)?;
        let mut blob_bytes = env_u64("SINOMO_DFS_BENCH_BLOB_BYTES", 64 * 1024 * 1024)?;
        let mut blob_range_bytes = env_usize("SINOMO_DFS_BENCH_BLOB_RANGE_BYTES", 1024 * 1024)?;
        let mut network_blob_bytes =
            env_u64("SINOMO_DFS_BENCH_NETWORK_BLOB_BYTES", 8 * 1024 * 1024)?;
        let mut list_page_size = env_usize("SINOMO_DFS_BENCH_LIST_PAGE_SIZE", 1024)?;
        let mut keep = false;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--scenario" => scenario = Scenario::parse(&required_value(&arg, args.next())?)?,
                "--root" => root = Some(PathBuf::from(required_value(&arg, args.next())?)),
                "--files" => files = parse_usize(&arg, args.next())?,
                "--dirs" => dirs = parse_usize(&arg, args.next())?,
                "--file-bytes" => file_bytes = parse_usize(&arg, args.next())?,
                "--blob-bytes" => blob_bytes = parse_u64(&arg, args.next())?,
                "--blob-range-bytes" => blob_range_bytes = parse_usize(&arg, args.next())?,
                "--network-blob-bytes" => network_blob_bytes = parse_u64(&arg, args.next())?,
                "--list-page-size" => list_page_size = parse_usize(&arg, args.next())?,
                "--keep" => keep = true,
                "--bench" => {}
                "--help" | "-h" => {
                    print_help();
                    process::exit(0);
                }
                _ => return Err(format!("unknown argument `{arg}`").into()),
            }
        }

        if dirs == 0 {
            return Err("--dirs must be greater than zero".into());
        }
        if list_page_size == 0 {
            return Err("--list-page-size must be greater than zero".into());
        }

        let (root, explicit_root) = match root {
            Some(root) => (root, true),
            None => (default_root(), false),
        };

        Ok(Self {
            scenario,
            root,
            explicit_root,
            files,
            dirs,
            file_bytes,
            blob_bytes,
            blob_range_bytes,
            network_blob_bytes,
            list_page_size,
            keep,
        })
    }

    fn scenario_root(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
}

fn run_namespace(args: &Args) -> BenchResult<()> {
    let root = args.scenario_root("namespace");
    remove_dir_all_if_exists(&root)?;
    fs::create_dir_all(&root)?;

    let node = node("node_dfs_bench_namespace");
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node.clone()),
        LocalContentStore::new(root.join("content"))?,
    )
    .with_metadata_backend(LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        &metadata_root,
    )?)?;

    let setup_start = Instant::now();
    dfs.bootstrap_distributed_sifs()?;
    dfs.posix_mkdir_all("/services/dfs-scale")?;
    for dir_index in 0..args.dirs {
        dfs.posix_mkdir_all(&bench_dir(dir_index))?;
    }
    emit(
        "namespace_setup",
        setup_start.elapsed(),
        json!({ "dirs": args.dirs, "root": root }),
    );

    let payload_size = args.file_bytes as u64;
    let create_start = Instant::now();
    for index in 0..args.files {
        dfs.add_object_metadata(file_metadata(
            &node,
            index,
            args.dirs,
            Some(valid_fake_hash(index)),
            Some(payload_size),
        )?)?;
    }
    let create_elapsed = create_start.elapsed();
    emit(
        "namespace_create_metadata",
        create_elapsed,
        json!({
            "files": args.files,
            "dirs": args.dirs,
            "files_per_second": rate(args.files as u64, create_elapsed),
        }),
    );

    let list_start = Instant::now();
    let mut listed = 0usize;
    for dir_index in 0..args.dirs {
        listed = listed.saturating_add(dfs.posix_list_dir(&bench_dir(dir_index))?.len());
    }
    let list_elapsed = list_start.elapsed();
    emit(
        "namespace_list_all_dirs",
        list_elapsed,
        json!({
            "files": args.files,
            "dirs": args.dirs,
            "listed": listed,
            "entries_per_second": rate(listed as u64, list_elapsed),
        }),
    );

    let paged_list_start = Instant::now();
    let mut paged_listed = 0usize;
    let mut pages = 0usize;
    for dir_index in 0..args.dirs {
        let dir = bench_dir(dir_index);
        let mut cursor = None;
        loop {
            let page = dfs.posix_list_dir_page(&dir, cursor.as_ref(), args.list_page_size)?;
            paged_listed = paged_listed.saturating_add(page.entries.len());
            pages = pages.saturating_add(1);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
    }
    let paged_list_elapsed = paged_list_start.elapsed();
    emit(
        "namespace_list_all_dirs_paged",
        paged_list_elapsed,
        json!({
            "files": args.files,
            "dirs": args.dirs,
            "listed": paged_listed,
            "pages": pages,
            "page_size": args.list_page_size,
            "entries_per_second": rate(paged_listed as u64, paged_list_elapsed),
        }),
    );

    drop(dfs);
    let reopen_start = Instant::now();
    let reopened = NetworkedDfsService::new(
        network.add_node(node.clone()),
        LocalContentStore::new(root.join("content"))?,
    )
    .with_metadata_backend(LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        &metadata_root,
    )?)?;
    let reopen_elapsed = reopen_start.elapsed();
    let reopened_objects = reopened
        .metadata_backend()
        .expect("metadata backend")
        .objects_for_file_system(&sifs_id())?
        .len();
    emit(
        "namespace_reopen_hydrate",
        reopen_elapsed,
        json!({
            "files": args.files,
            "dirs": args.dirs,
            "objects_loaded": reopened_objects,
            "objects_per_second": rate(reopened_objects as u64, reopen_elapsed),
        }),
    );

    Ok(())
}

fn run_metadata_replication(args: &Args) -> BenchResult<()> {
    let root = args.scenario_root("metadata-replication");
    remove_dir_all_if_exists(&root)?;
    fs::create_dir_all(&root)?;

    let owner = node("node_dfs_bench_owner");
    let replica = node("node_dfs_bench_replica");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content"))?,
    )
    .with_metadata_backend(LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("owner-metadata"),
    )?)?;
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content"))?,
    )
    .with_metadata_backend(LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("replica-metadata"),
    )?)?;

    let populate_start = Instant::now();
    owner_dfs.add_file_system(DfsFileSystem::sifs(owner.clone()))?;
    for index in 0..args.files {
        owner_dfs.add_object_metadata(file_metadata(
            &owner,
            index,
            args.dirs,
            Some(valid_fake_hash(index)),
            Some(args.file_bytes as u64),
        )?)?;
    }
    let populate_elapsed = populate_start.elapsed();
    emit(
        "metadata_replication_populate_primary",
        populate_elapsed,
        json!({
            "files": args.files,
            "primary_files_per_second": rate(args.files as u64, populate_elapsed),
        }),
    );

    let catchup_start = Instant::now();
    let mut after_sequence = 0u64;
    let mut batches = 0usize;
    let mut entries = 0usize;
    while let Some(batch) = owner_dfs.send_metadata_journal_since(&replica, after_sequence)? {
        after_sequence = batch.next_after_sequence;
        entries = entries.saturating_add(batch.entries.len());
        batches = batches.saturating_add(1);
        replica_dfs.process_incoming()?;
        owner_dfs.process_incoming()?;
    }
    let catchup_elapsed = catchup_start.elapsed();
    let replica_objects = replica_dfs
        .metadata_backend()
        .expect("metadata backend")
        .objects_for_file_system(&sifs_id())?
        .len();
    emit(
        "metadata_replication_catchup",
        catchup_elapsed,
        json!({
            "files": args.files,
            "batches": batches,
            "entries": entries,
            "last_sequence": after_sequence,
            "entries_per_second": rate(entries as u64, catchup_elapsed),
            "replica_objects": replica_objects,
        }),
    );

    Ok(())
}

fn run_blob(args: &Args) -> BenchResult<()> {
    let root = args.scenario_root("blob");
    remove_dir_all_if_exists(&root)?;
    fs::create_dir_all(&root)?;
    let store = LocalContentStore::new(&root)?;

    let write_start = Instant::now();
    let manifest = store.put_object_version_from_reader(
        "dfsbenchblob",
        "versionblob00000001",
        RepeatingReader::new(args.blob_bytes),
    )?;
    let write_elapsed = write_start.elapsed();
    emit(
        "blob_stream_write",
        write_elapsed,
        json!({
            "bytes": args.blob_bytes,
            "mib_per_second": mib_per_second(args.blob_bytes, write_elapsed),
            "content_hash": manifest.content_hash,
        }),
    );

    let read_start = Instant::now();
    let mut reader = store.open_blob_stream(&manifest.content_hash)?;
    let mut buffer = [0u8; 1024 * 1024];
    let mut read_bytes = 0u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        read_bytes = read_bytes.saturating_add(read as u64);
    }
    let read_elapsed = read_start.elapsed();
    emit(
        "blob_stream_read",
        read_elapsed,
        json!({
            "bytes": read_bytes,
            "mib_per_second": mib_per_second(read_bytes, read_elapsed),
        }),
    );

    let range_offset = args.blob_bytes.saturating_sub(args.blob_range_bytes as u64) / 2;
    let range_start = Instant::now();
    let range =
        store.read_blob_range(&manifest.content_hash, range_offset, args.blob_range_bytes)?;
    let range_elapsed = range_start.elapsed();
    emit(
        "blob_range_read",
        range_elapsed,
        json!({
            "offset": range_offset,
            "bytes": range.len(),
            "requested_bytes": args.blob_range_bytes,
            "mib_per_second": mib_per_second(range.len() as u64, range_elapsed),
        }),
    );

    Ok(())
}

fn run_network_blob(args: &Args) -> BenchResult<()> {
    let root = args.scenario_root("network-blob");
    remove_dir_all_if_exists(&root)?;
    fs::create_dir_all(&root)?;

    let owner = node("node_dfs_bench_net_owner");
    let replica = node("node_dfs_bench_net_replica");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content"))?,
    )
    .with_metadata_backend(LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("owner-metadata"),
    )?)?;
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content"))?,
    )
    .with_metadata_backend(LocalJsonMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("replica-metadata"),
    )?)?;

    let file_system_id = sifs_id();
    let object_id = DfsObjectId::from_legacy_named("dfsbenchnetblob").expect("object id");
    let base_version = DfsVersion::from_legacy_named("versionnetbase").expect("version");
    let target_version = DfsVersion::from_legacy_named("versionnettarget").expect("version");
    let object = DfsObjectMetadata {
        object_id: object_id.clone(),
        file_system_id: file_system_id.clone(),
        path: "/services/dfs-scale/network-blob.bin".to_string(),
        kind: DfsObjectKind::File,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: None,
        content_size_bytes: None,
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: base_version,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    };
    owner_dfs.add_object_metadata(object.clone())?;
    replica_dfs.add_object_metadata(object)?;

    let replica_object_id = object_id.clone();
    let replica_worker = std::thread::spawn(move || -> BenchResult<ReplicaWorkerReport> {
        let deadline = Instant::now() + std::time::Duration::from_secs(60);
        let mut report = ReplicaWorkerReport::default();
        loop {
            let events = replica_dfs.process_incoming()?;
            report.process_loops = report.process_loops.saturating_add(1);
            report.event_count = report.event_count.saturating_add(events.len());
            report.staged |= events.iter().any(|event| {
                matches!(
                    event,
                    NetworkedDfsServiceEvent::BlobStaged { object_id: staged, .. }
                        if staged == &replica_object_id
                )
            });
            report.committed |= events.iter().any(|event| {
                matches!(
                    event,
                    NetworkedDfsServiceEvent::VersionCommitted { object_id: committed, .. }
                        if committed == &replica_object_id
                )
            });
            if report.committed {
                break;
            }
            if Instant::now() >= deadline {
                return Err("replica did not observe network blob commit before timeout".into());
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Ok(report)
    });

    let write_start = Instant::now();
    let write = owner_dfs.write_distributed_eventual_from_reader(
        DfsWriteRequest {
            file_system_id,
            object_id: object_id.clone(),
            target_version: target_version.clone(),
            requesting_node_id: owner.clone(),
        },
        RepeatingReader::new(args.network_blob_bytes),
        vec![owner.clone(), replica.clone()],
        DfsReplicationPolicy::new(2, 2)?,
    )?;

    let deadline = Instant::now() + std::time::Duration::from_secs(60);
    let mut owner_event_count = 0usize;
    let owner_committed = loop {
        owner_dfs.retry_pending_repairs()?;
        owner_event_count = owner_event_count.saturating_add(owner_dfs.process_incoming()?.len());
        let owner_committed = owner_dfs
            .metadata()
            .object(&object_id)
            .is_some_and(|object| object.version == target_version);
        if owner_committed {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    };
    let write_elapsed = write_start.elapsed();
    let replica_report = replica_worker
        .join()
        .map_err(|_| "replica network blob worker panicked")??;

    emit(
        "network_blob_replicated_write",
        write_elapsed,
        json!({
            "bytes": args.network_blob_bytes,
            "mib_per_second": mib_per_second(args.network_blob_bytes, write_elapsed),
            "initial_outcome": format!("{write:?}"),
            "owner_events": owner_event_count,
            "replica_events": replica_report.event_count,
            "replica_process_loops": replica_report.process_loops,
            "replica_staged": replica_report.staged,
            "owner_committed": owner_committed,
            "replica_committed": replica_report.committed,
        }),
    );

    Ok(())
}

#[derive(Clone, Debug, Default)]
struct ReplicaWorkerReport {
    event_count: usize,
    process_loops: usize,
    staged: bool,
    committed: bool,
}

fn file_metadata(
    owner: &NodeId,
    index: usize,
    dirs: usize,
    content_hash: Option<String>,
    content_size_bytes: Option<u64>,
) -> BenchResult<DfsObjectMetadata> {
    Ok(DfsObjectMetadata {
        object_id: DfsObjectId::from_legacy_named(format!("dfsbenchobj{index:016x}"))?,
        file_system_id: sifs_id(),
        path: format!("{}/file-{index:08}.bin", bench_dir(index % dirs)),
        kind: DfsObjectKind::File,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash,
        content_size_bytes,
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: index as u64,
        created_at_unix_nanos: index as u64,
        changed_at_unix_nanos: index as u64,
        version: DfsVersion::from_legacy_named(format!("dfsbenchver{index:016x}"))?,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    })
}

fn bench_dir(index: usize) -> String {
    format!("/services/dfs-scale/dir-{index:04}")
}

fn sifs_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named("sifs").expect("sifs id")
}

fn node(value: &str) -> NodeId {
    NodeId::from_legacy_named(value).expect("node id")
}

fn valid_fake_hash(index: usize) -> String {
    format!("blake3:{index:064x}")
}

fn emit(name: &str, elapsed: std::time::Duration, fields: serde_json::Value) {
    let mut value = json!({
        "scenario": name,
        "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
    });
    if let (Some(target), Some(fields)) = (value.as_object_mut(), fields.as_object()) {
        for (key, field) in fields {
            target.insert(key.clone(), field.clone());
        }
    }
    println!("{value}");
}

fn rate(count: u64, elapsed: std::time::Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds == 0.0 {
        return count as f64;
    }
    count as f64 / seconds
}

fn mib_per_second(bytes: u64, elapsed: std::time::Duration) -> f64 {
    rate(bytes, elapsed) / (1024.0 * 1024.0)
}

fn default_root() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    env::temp_dir().join(format!("sinomo-dfs-scale-bench-{}-{nanos}", process::id()))
}

fn remove_dir_all_if_exists(path: &Path) -> BenchResult<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn env_usize(name: &str, default: usize) -> BenchResult<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|error| format!("{name}={value:?} is not a usize: {error}").into()),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("could not read {name}: {error}").into()),
    }
}

fn env_u64(name: &str, default: u64) -> BenchResult<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|error| format!("{name}={value:?} is not a u64: {error}").into()),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("could not read {name}: {error}").into()),
    }
}

fn parse_usize(flag: &str, value: Option<String>) -> BenchResult<usize> {
    let value = required_value(flag, value)?;
    value
        .parse::<usize>()
        .map_err(|error| format!("{flag} value {value:?} is not a usize: {error}").into())
}

fn parse_u64(flag: &str, value: Option<String>) -> BenchResult<u64> {
    let value = required_value(flag, value)?;
    value
        .parse::<u64>()
        .map_err(|error| format!("{flag} value {value:?} is not a u64: {error}").into())
}

fn required_value(flag: &str, value: Option<String>) -> BenchResult<String> {
    value.ok_or_else(|| format!("{flag} requires a value").into())
}

fn print_help() {
    println!(
        "Usage: cargo bench -p sinomo-dfs --bench scale -- [OPTIONS]\n\
         \n\
         Options:\n\
           --scenario all|namespace|metadata-replication|blob|network-blob\n\
           --root PATH\n\
           --files COUNT\n\
           --dirs COUNT\n\
           --file-bytes BYTES\n\
           --blob-bytes BYTES\n\
           --blob-range-bytes BYTES\n\
           --network-blob-bytes BYTES\n\
           --list-page-size COUNT\n\
           --keep\n\
         \n\
         Environment defaults:\n\
           SINOMO_DFS_BENCH_FILES, SINOMO_DFS_BENCH_DIRS,\n\
           SINOMO_DFS_BENCH_FILE_BYTES, SINOMO_DFS_BENCH_BLOB_BYTES,\n\
           SINOMO_DFS_BENCH_BLOB_RANGE_BYTES,\n\
           SINOMO_DFS_BENCH_NETWORK_BLOB_BYTES,\n\
           SINOMO_DFS_BENCH_LIST_PAGE_SIZE"
    );
}

struct RepeatingReader {
    remaining: u64,
    state: u64,
}

impl RepeatingReader {
    fn new(bytes: u64) -> Self {
        Self {
            remaining: bytes,
            state: 0x9e37_79b9_7f4a_7c15,
        }
    }
}

impl Read for RepeatingReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if self.remaining == 0 || buf.is_empty() {
            return Ok(0);
        }
        let to_fill = buf
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        for byte in &mut buf[..to_fill] {
            self.state ^= self.state << 7;
            self.state ^= self.state >> 9;
            self.state = self.state.wrapping_mul(0x2545_f491_4f6c_dd1d);
            *byte = self.state as u8;
        }
        self.remaining -= to_fill as u64;
        Ok(to_fill)
    }
}
