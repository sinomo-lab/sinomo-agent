use serde_json::json;
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
use sinomo_dfs::{
    DfsFileSystem, DfsFileSystemId, DfsFrameTransport, DfsMetadataShardId, DfsNamespaceJobStatus,
    DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsOperationalMetrics, DfsReadResult,
    DfsReplicationPolicy, DfsVersion, DfsWriteOptions, LocalContentStore, LocalSobjMetadataBackend,
    NetworkedDfsRangeReadOutcome, NetworkedDfsService, NetworkedDfsServiceEvent,
};
use sinomo_network::InProcessNetwork;
use sinomo_node::NodeId;
use std::env;
use std::fs;
use std::io::{Read, Result as IoResult};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug)]
struct Args {
    scenario: Scenario,
    preset: Preset,
    root: PathBuf,
    explicit_root: bool,
    files: usize,
    dirs: usize,
    file_bytes: usize,
    blob_bytes: u64,
    blob_range_bytes: usize,
    network_blob_bytes: u64,
    write_at_ops: usize,
    write_at_bytes: usize,
    list_page_size: usize,
    metadata_batch_size: usize,
    namespace_job_steps: usize,
    network_timeout_secs: u64,
    keep: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    All,
    Namespace,
    PosixCreate,
    PosixRead,
    PosixWriteAt,
    MetadataReplication,
    DirectoryJobs,
    Blob,
    NetworkBlob,
    NetworkWriteAt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Preset {
    Smoke,
    Scale,
    Production,
}

#[derive(Clone, Copy, Debug)]
struct BenchDefaults {
    files: usize,
    dirs: usize,
    file_bytes: usize,
    blob_bytes: u64,
    blob_range_bytes: usize,
    network_blob_bytes: u64,
    write_at_ops: usize,
    write_at_bytes: usize,
    list_page_size: usize,
    metadata_batch_size: usize,
    namespace_job_steps: usize,
    network_timeout_secs: u64,
}

impl Scenario {
    fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "all" => Ok(Self::All),
            "namespace" => Ok(Self::Namespace),
            "posix-create" => Ok(Self::PosixCreate),
            "posix-read" => Ok(Self::PosixRead),
            "posix-write-at" => Ok(Self::PosixWriteAt),
            "metadata-replication" => Ok(Self::MetadataReplication),
            "directory-jobs" => Ok(Self::DirectoryJobs),
            "blob" => Ok(Self::Blob),
            "network-blob" => Ok(Self::NetworkBlob),
            "network-write-at" => Ok(Self::NetworkWriteAt),
            _ => Err(format!(
                "unknown scenario `{value}`; use all, namespace, posix-create, posix-read, posix-write-at, metadata-replication, directory-jobs, blob, network-blob, or network-write-at"
            )
            .into()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Namespace => "namespace",
            Self::PosixCreate => "posix-create",
            Self::PosixRead => "posix-read",
            Self::PosixWriteAt => "posix-write-at",
            Self::MetadataReplication => "metadata-replication",
            Self::DirectoryJobs => "directory-jobs",
            Self::Blob => "blob",
            Self::NetworkBlob => "network-blob",
            Self::NetworkWriteAt => "network-write-at",
        }
    }
}

impl Preset {
    fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "scale" => Ok(Self::Scale),
            "production" => Ok(Self::Production),
            _ => Err(format!("unknown preset `{value}`; use smoke, scale, or production").into()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Scale => "scale",
            Self::Production => "production",
        }
    }

    fn defaults(self) -> BenchDefaults {
        match self {
            Self::Smoke => BenchDefaults {
                files: 10_000,
                dirs: 128,
                file_bytes: 32,
                blob_bytes: 64 * 1024 * 1024,
                blob_range_bytes: 1024 * 1024,
                network_blob_bytes: 8 * 1024 * 1024,
                write_at_ops: 64,
                write_at_bytes: 4 * 1024,
                list_page_size: 1024,
                metadata_batch_size: 1024,
                namespace_job_steps: 1024,
                network_timeout_secs: 60,
            },
            Self::Scale => BenchDefaults {
                files: 100_000,
                dirs: 1024,
                file_bytes: 32,
                blob_bytes: 1024 * 1024 * 1024,
                blob_range_bytes: 8 * 1024 * 1024,
                network_blob_bytes: 512 * 1024 * 1024,
                write_at_ops: 1024,
                write_at_bytes: 16 * 1024,
                list_page_size: 4096,
                metadata_batch_size: 4096,
                namespace_job_steps: 4096,
                network_timeout_secs: 300,
            },
            Self::Production => BenchDefaults {
                files: 1_000_000,
                dirs: 4096,
                file_bytes: 32,
                blob_bytes: 4 * 1024 * 1024 * 1024,
                blob_range_bytes: 16 * 1024 * 1024,
                network_blob_bytes: 4 * 1024 * 1024 * 1024,
                write_at_ops: 8192,
                write_at_bytes: 64 * 1024,
                list_page_size: 4096,
                metadata_batch_size: 4096,
                namespace_job_steps: 4096,
                network_timeout_secs: 1800,
            },
        }
    }
}

fn main() -> BenchResult<()> {
    let args = Args::parse()?;
    args.emit_config();
    match args.scenario {
        Scenario::All => {
            run_namespace(&args)?;
            run_posix_create(&args)?;
            run_posix_read(&args)?;
            run_posix_write_at(&args)?;
            run_metadata_replication(&args)?;
            run_directory_jobs(&args)?;
            run_blob(&args)?;
            run_network_blob(&args)?;
            run_network_write_at(&args)?;
        }
        Scenario::Namespace => run_namespace(&args)?,
        Scenario::PosixCreate => run_posix_create(&args)?,
        Scenario::PosixRead => run_posix_read(&args)?,
        Scenario::PosixWriteAt => run_posix_write_at(&args)?,
        Scenario::MetadataReplication => run_metadata_replication(&args)?,
        Scenario::DirectoryJobs => run_directory_jobs(&args)?,
        Scenario::Blob => run_blob(&args)?,
        Scenario::NetworkBlob => run_network_blob(&args)?,
        Scenario::NetworkWriteAt => run_network_write_at(&args)?,
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
        let mut preset = match env::var("SINOMO_DFS_BENCH_PRESET") {
            Ok(value) => Preset::parse(&value)?,
            Err(env::VarError::NotPresent) => Preset::Smoke,
            Err(error) => {
                return Err(format!("could not read SINOMO_DFS_BENCH_PRESET: {error}").into());
            }
        };
        let defaults = preset.defaults();
        let mut root = None::<PathBuf>;
        let mut files = env_usize("SINOMO_DFS_BENCH_FILES", defaults.files)?;
        let mut dirs = env_usize("SINOMO_DFS_BENCH_DIRS", defaults.dirs)?;
        let mut file_bytes = env_usize("SINOMO_DFS_BENCH_FILE_BYTES", defaults.file_bytes)?;
        let mut blob_bytes = env_bytes("SINOMO_DFS_BENCH_BLOB_BYTES", defaults.blob_bytes)?;
        let mut blob_range_bytes = byte_count_to_usize(
            "SINOMO_DFS_BENCH_BLOB_RANGE_BYTES",
            env_bytes(
                "SINOMO_DFS_BENCH_BLOB_RANGE_BYTES",
                defaults.blob_range_bytes as u64,
            )?,
        )?;
        let mut network_blob_bytes = env_bytes(
            "SINOMO_DFS_BENCH_NETWORK_BLOB_BYTES",
            defaults.network_blob_bytes,
        )?;
        let mut write_at_ops = env_usize("SINOMO_DFS_BENCH_WRITE_AT_OPS", defaults.write_at_ops)?;
        let mut write_at_bytes = byte_count_to_usize(
            "SINOMO_DFS_BENCH_WRITE_AT_BYTES",
            env_bytes(
                "SINOMO_DFS_BENCH_WRITE_AT_BYTES",
                defaults.write_at_bytes as u64,
            )?,
        )?;
        let mut list_page_size =
            env_usize("SINOMO_DFS_BENCH_LIST_PAGE_SIZE", defaults.list_page_size)?;
        let mut metadata_batch_size = env_usize(
            "SINOMO_DFS_BENCH_METADATA_BATCH_SIZE",
            defaults.metadata_batch_size,
        )?;
        let mut namespace_job_steps = env_usize(
            "SINOMO_DFS_BENCH_NAMESPACE_JOB_STEPS",
            defaults.namespace_job_steps,
        )?;
        let mut network_timeout_secs = env_u64(
            "SINOMO_DFS_BENCH_NETWORK_TIMEOUT_SECS",
            defaults.network_timeout_secs,
        )?;
        let mut keep = false;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--scenario" => scenario = Scenario::parse(&required_value(&arg, args.next())?)?,
                "--preset" => {
                    preset = Preset::parse(&required_value(&arg, args.next())?)?;
                    let defaults = preset.defaults();
                    files = defaults.files;
                    dirs = defaults.dirs;
                    file_bytes = defaults.file_bytes;
                    blob_bytes = defaults.blob_bytes;
                    blob_range_bytes = defaults.blob_range_bytes;
                    network_blob_bytes = defaults.network_blob_bytes;
                    write_at_ops = defaults.write_at_ops;
                    write_at_bytes = defaults.write_at_bytes;
                    list_page_size = defaults.list_page_size;
                    metadata_batch_size = defaults.metadata_batch_size;
                    namespace_job_steps = defaults.namespace_job_steps;
                    network_timeout_secs = defaults.network_timeout_secs;
                }
                "--root" => root = Some(PathBuf::from(required_value(&arg, args.next())?)),
                "--files" => files = parse_usize(&arg, args.next())?,
                "--dirs" => dirs = parse_usize(&arg, args.next())?,
                "--file-bytes" => file_bytes = parse_usize(&arg, args.next())?,
                "--blob-bytes" => blob_bytes = parse_byte_count(&arg, args.next())?,
                "--blob-range-bytes" => {
                    blob_range_bytes =
                        byte_count_to_usize(&arg, parse_byte_count(&arg, args.next())?)?
                }
                "--network-blob-bytes" => network_blob_bytes = parse_byte_count(&arg, args.next())?,
                "--write-at-ops" => write_at_ops = parse_usize(&arg, args.next())?,
                "--write-at-bytes" => {
                    write_at_bytes =
                        byte_count_to_usize(&arg, parse_byte_count(&arg, args.next())?)?
                }
                "--list-page-size" => list_page_size = parse_usize(&arg, args.next())?,
                "--metadata-batch-size" => metadata_batch_size = parse_usize(&arg, args.next())?,
                "--namespace-job-steps" => namespace_job_steps = parse_usize(&arg, args.next())?,
                "--network-timeout-secs" => network_timeout_secs = parse_u64(&arg, args.next())?,
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
        if metadata_batch_size == 0 {
            return Err("--metadata-batch-size must be greater than zero".into());
        }
        if namespace_job_steps == 0 {
            return Err("--namespace-job-steps must be greater than zero".into());
        }
        if network_timeout_secs == 0 {
            return Err("--network-timeout-secs must be greater than zero".into());
        }
        if write_at_ops == 0 {
            return Err("--write-at-ops must be greater than zero".into());
        }
        if write_at_bytes == 0 {
            return Err("--write-at-bytes must be greater than zero".into());
        }

        let (root, explicit_root) = match root {
            Some(root) => (root, true),
            None => (default_root(), false),
        };

        Ok(Self {
            scenario,
            preset,
            root,
            explicit_root,
            files,
            dirs,
            file_bytes,
            blob_bytes,
            blob_range_bytes,
            network_blob_bytes,
            write_at_ops,
            write_at_bytes,
            list_page_size,
            metadata_batch_size,
            namespace_job_steps,
            network_timeout_secs,
            keep,
        })
    }

    fn scenario_root(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn emit_config(&self) {
        emit(
            "bench_config",
            std::time::Duration::ZERO,
            json!({
                "preset": self.preset.as_str(),
                "selected_scenario": self.scenario.as_str(),
                "root": self.root,
                "explicit_root": self.explicit_root,
                "files": self.files,
                "dirs": self.dirs,
                "file_bytes": self.file_bytes,
                "blob_bytes": self.blob_bytes,
                "blob_range_bytes": self.blob_range_bytes,
                "network_blob_bytes": self.network_blob_bytes,
                "write_at_ops": self.write_at_ops,
                "write_at_bytes": self.write_at_bytes,
                "list_page_size": self.list_page_size,
                "metadata_batch_size": self.metadata_batch_size,
                "namespace_job_steps": self.namespace_job_steps,
                "network_timeout_secs": self.network_timeout_secs,
                "keep": self.keep,
            }),
        );
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
    .with_metadata_backend(LocalSobjMetadataBackend::new(
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
    let mut batch = Vec::with_capacity(args.metadata_batch_size);
    let mut batches = 0usize;
    for index in 0..args.files {
        batch.push(file_metadata(
            &node,
            index,
            args.dirs,
            Some(valid_fake_hash(index)),
            Some(payload_size),
        )?);
        if batch.len() >= args.metadata_batch_size {
            dfs.add_object_metadata_batch(batch.drain(..))?;
            batches = batches.saturating_add(1);
        }
    }
    if !batch.is_empty() {
        dfs.add_object_metadata_batch(batch.drain(..))?;
        batches = batches.saturating_add(1);
    }
    let create_elapsed = create_start.elapsed();
    emit(
        "namespace_create_metadata",
        create_elapsed,
        json!({
            "files": args.files,
            "dirs": args.dirs,
            "metadata_batch_size": args.metadata_batch_size,
            "metadata_batches": batches,
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
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        &metadata_root,
    )?)?;
    let reopen_elapsed = reopen_start.elapsed();
    let reopened_objects = reopened
        .metadata_backend()
        .expect("metadata backend")
        .count_objects_for_file_system(&sifs_id())?;
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

fn run_posix_create(args: &Args) -> BenchResult<()> {
    let root = args.scenario_root("posix-create");
    remove_dir_all_if_exists(&root)?;
    fs::create_dir_all(&root)?;

    let node = node("node_dfs_bench_posix_create");
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node.clone()),
        LocalContentStore::new(root.join("content"))?,
    )
    .with_metadata_backend(LocalSobjMetadataBackend::new(
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
        "posix_create_setup",
        setup_start.elapsed(),
        json!({ "dirs": args.dirs, "root": root }),
    );

    let options = DfsWriteOptions::replicated_eventual(vec![node.clone()]);
    let create_start = Instant::now();
    let mut written_bytes = 0u64;
    for index in 0..args.files {
        let path = format!("{}/file-{index:08}.bin", bench_dir(index % args.dirs));
        dfs.posix_write_file_from_reader(
            &path,
            RepeatingReader::new(args.file_bytes as u64),
            options.clone(),
        )?;
        written_bytes = written_bytes.saturating_add(args.file_bytes as u64);
    }
    let create_elapsed = create_start.elapsed();
    emit(
        "posix_stream_create_files",
        create_elapsed,
        json!({
            "files": args.files,
            "dirs": args.dirs,
            "file_bytes": args.file_bytes,
            "written_bytes": written_bytes,
            "files_per_second": rate(args.files as u64, create_elapsed),
            "mib_per_second": mib_per_second(written_bytes, create_elapsed),
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
        "posix_create_list_all_dirs_paged",
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
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        &metadata_root,
    )?)?;
    let reopen_elapsed = reopen_start.elapsed();
    let reopened_objects = reopened
        .metadata_backend()
        .expect("metadata backend")
        .count_objects_for_file_system(&sifs_id())?;
    emit(
        "posix_create_reopen_hydrate",
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

fn run_posix_read(args: &Args) -> BenchResult<()> {
    let root = args.scenario_root("posix-read");
    remove_dir_all_if_exists(&root)?;
    fs::create_dir_all(&root)?;

    let node = node("node_dfs_bench_posix_read");
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node.clone()),
        LocalContentStore::new(root.join("content"))?,
    )
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        &metadata_root,
    )?)?;

    let setup_start = Instant::now();
    dfs.bootstrap_distributed_sifs()?;
    dfs.posix_mkdir_all("/services/dfs-scale")?;
    emit(
        "posix_read_setup",
        setup_start.elapsed(),
        json!({ "root": root }),
    );

    let options = DfsWriteOptions::replicated_eventual(vec![node.clone()]);
    let path = "/services/dfs-scale/stream-read.bin";
    let write_start = Instant::now();
    let stat =
        dfs.posix_write_file_from_reader(path, RepeatingReader::new(args.blob_bytes), options)?;
    let write_elapsed = write_start.elapsed();
    emit(
        "posix_read_stream_write",
        write_elapsed,
        json!({
            "bytes": args.blob_bytes,
            "object_id": stat.object_id,
            "version": stat.version,
            "content_hash": stat.content_hash,
            "mib_per_second": mib_per_second(args.blob_bytes, write_elapsed),
        }),
    );

    let read_start = Instant::now();
    let mut stream = dfs.posix_open_file_stream(path)?;
    let mut buffer = [0u8; 1024 * 1024];
    let mut read_bytes = 0u64;
    loop {
        let read = stream.reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        read_bytes = read_bytes.saturating_add(read as u64);
    }
    let read_elapsed = read_start.elapsed();
    if read_bytes != args.blob_bytes {
        return Err(format!(
            "POSIX stream read returned {read_bytes} bytes, expected {}",
            args.blob_bytes
        )
        .into());
    }
    emit(
        "posix_stream_read",
        read_elapsed,
        json!({
            "bytes": read_bytes,
            "reported_size_bytes": stream.size_bytes,
            "content_hash": stream.content_hash,
            "mib_per_second": mib_per_second(read_bytes, read_elapsed),
        }),
    );

    let range_offset = args.blob_bytes.saturating_sub(args.blob_range_bytes as u64) / 2;
    let range_start = Instant::now();
    let mut range_stream =
        dfs.posix_open_file_range_stream(path, range_offset, args.blob_range_bytes)?;
    let mut range_stream_bytes = 0u64;
    loop {
        let read = range_stream.reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        range_stream_bytes = range_stream_bytes.saturating_add(read as u64);
    }
    let range_elapsed = range_start.elapsed();
    if range_stream_bytes != range_stream.range_size_bytes {
        return Err(format!(
            "POSIX range stream read returned {range_stream_bytes} bytes, expected {}",
            range_stream.range_size_bytes
        )
        .into());
    }
    emit(
        "posix_range_stream_read",
        range_elapsed,
        json!({
            "offset": range_stream.offset,
            "bytes": range_stream_bytes,
            "requested_bytes": args.blob_range_bytes,
            "range_size_bytes": range_stream.range_size_bytes,
            "total_size_bytes": range_stream.total_size_bytes,
            "content_hash": range_stream.content_hash,
            "mib_per_second": mib_per_second(range_stream_bytes, range_elapsed),
        }),
    );

    Ok(())
}

fn run_posix_write_at(args: &Args) -> BenchResult<()> {
    let root = args.scenario_root("posix-write-at");
    remove_dir_all_if_exists(&root)?;
    fs::create_dir_all(&root)?;

    let node = node("node_dfs_bench_posix_write_at");
    let network = InProcessNetwork::new();
    let metadata_root = root.join("metadata");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node.clone()),
        LocalContentStore::new(root.join("content"))?,
    )
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        &metadata_root,
    )?)?;

    let setup_start = Instant::now();
    dfs.bootstrap_distributed_sifs()?;
    dfs.posix_mkdir_all("/services/dfs-scale")?;
    emit(
        "posix_write_at_setup",
        setup_start.elapsed(),
        json!({ "root": root }),
    );

    let options = DfsWriteOptions::replicated_eventual(vec![node.clone()]);
    let path = "/services/dfs-scale/random-write.bin";
    let base_start = Instant::now();
    dfs.posix_write_file_from_reader(path, RepeatingReader::new(args.blob_bytes), options.clone())?;
    let base_elapsed = base_start.elapsed();
    emit(
        "posix_write_at_base_stream_write",
        base_elapsed,
        json!({
            "bytes": args.blob_bytes,
            "mib_per_second": mib_per_second(args.blob_bytes, base_elapsed),
        }),
    );

    let patch_start = Instant::now();
    let mut patched_bytes = 0u64;
    let mut last_offset = 0u64;
    for index in 0..args.write_at_ops {
        let offset = write_at_offset(index, args.blob_bytes, args.write_at_bytes);
        last_offset = offset;
        dfs.posix_write_file_at_from_reader(
            path,
            offset,
            RepeatingReader::with_seed(args.write_at_bytes as u64, index as u64),
            options.clone(),
        )?;
        patched_bytes = patched_bytes.saturating_add(args.write_at_bytes as u64);
    }
    let patch_elapsed = patch_start.elapsed();
    let stat = dfs.posix_stat(path)?;
    let manifest = dfs
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())?;
    let logical_blob_materialized = dfs.content().has_blob(&manifest.content_hash)?;
    let content_stats = dfs.content().stats()?;
    emit(
        "posix_write_at_stream_patches",
        patch_elapsed,
        json!({
            "base_bytes": args.blob_bytes,
            "ops": args.write_at_ops,
            "patch_bytes": args.write_at_bytes,
            "patched_bytes": patched_bytes,
            "ops_per_second": rate(args.write_at_ops as u64, patch_elapsed),
            "mib_per_second": mib_per_second(patched_bytes, patch_elapsed),
            "result_size_bytes": manifest.size_bytes,
            "extent_count": manifest.extents.len(),
            "logical_blob_materialized": logical_blob_materialized,
            "content_stats": content_stats,
        }),
    );

    let verify_len = args.write_at_bytes.min(args.blob_range_bytes);
    let verify_start = Instant::now();
    let verify = dfs.posix_read_file_range(path, last_offset, verify_len)?;
    let verify_elapsed = verify_start.elapsed();
    emit(
        "posix_write_at_verify_range_read",
        verify_elapsed,
        json!({
            "offset": last_offset,
            "bytes": verify.len(),
            "requested_bytes": verify_len,
            "mib_per_second": mib_per_second(verify.len() as u64, verify_elapsed),
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
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("owner-metadata"),
    )?)?;
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content"))?,
    )
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("replica-metadata"),
    )?)?;

    let populate_start = Instant::now();
    owner_dfs.add_file_system(DfsFileSystem::sifs(owner.clone()))?;
    let mut batch = Vec::with_capacity(args.metadata_batch_size);
    let mut batches = 0usize;
    for index in 0..args.files {
        batch.push(file_metadata(
            &owner,
            index,
            args.dirs,
            Some(valid_fake_hash(index)),
            Some(args.file_bytes as u64),
        )?);
        if batch.len() >= args.metadata_batch_size {
            owner_dfs.add_object_metadata_batch(batch.drain(..))?;
            batches = batches.saturating_add(1);
        }
    }
    if !batch.is_empty() {
        owner_dfs.add_object_metadata_batch(batch.drain(..))?;
        batches = batches.saturating_add(1);
    }
    let populate_elapsed = populate_start.elapsed();
    emit(
        "metadata_replication_populate_primary",
        populate_elapsed,
        json!({
            "files": args.files,
            "metadata_batch_size": args.metadata_batch_size,
            "metadata_batches": batches,
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
        .count_objects_for_file_system(&sifs_id())?;
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

fn run_directory_jobs(args: &Args) -> BenchResult<()> {
    let root = args.scenario_root("directory-jobs");
    remove_dir_all_if_exists(&root)?;
    fs::create_dir_all(&root)?;

    let node = node("node_dfs_bench_directory_jobs");
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node.clone()),
        LocalContentStore::new(root.join("content"))?,
    )
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("metadata"),
    )?)?;

    let clear_root = "/services/dfs-scale/clear-job";
    let rename_old_root = "/services/dfs-scale/rename-job-old";
    let rename_new_root = "/services/dfs-scale/rename-job-new";

    let setup_start = Instant::now();
    dfs.bootstrap_distributed_sifs()?;
    create_tree_dirs(&mut dfs, clear_root, args.dirs)?;
    create_tree_dirs(&mut dfs, rename_old_root, args.dirs)?;
    emit(
        "directory_jobs_setup",
        setup_start.elapsed(),
        json!({
            "dirs_per_tree": args.dirs,
            "root": root,
        }),
    );

    let populate_start = Instant::now();
    let clear_metadata_batches = populate_tree_metadata(
        &mut dfs,
        &node,
        "clear",
        clear_root,
        args.files,
        args.dirs,
        args.file_bytes,
        args.metadata_batch_size,
        0,
    )?;
    let rename_metadata_batches = populate_tree_metadata(
        &mut dfs,
        &node,
        "rename",
        rename_old_root,
        args.files,
        args.dirs,
        args.file_bytes,
        args.metadata_batch_size,
        args.files,
    )?;
    let populate_elapsed = populate_start.elapsed();
    emit(
        "directory_jobs_populate_metadata",
        populate_elapsed,
        json!({
            "files_per_tree": args.files,
            "dirs_per_tree": args.dirs,
            "metadata_batch_size": args.metadata_batch_size,
            "clear_metadata_batches": clear_metadata_batches,
            "rename_metadata_batches": rename_metadata_batches,
            "files_per_second": rate(args.files.saturating_mul(2) as u64, populate_elapsed),
        }),
    );

    let clear_start = Instant::now();
    let mut clear_job =
        dfs.posix_clear_dir_recursive_bounded(clear_root, args.namespace_job_steps)?;
    let mut clear_batches = 1usize;
    while clear_job.status != DfsNamespaceJobStatus::Completed {
        clear_job =
            dfs.posix_resume_namespace_job_bounded(&clear_job.job_id, args.namespace_job_steps)?;
        clear_batches = clear_batches.saturating_add(1);
    }
    let clear_elapsed = clear_start.elapsed();
    emit(
        "directory_jobs_recursive_clear",
        clear_elapsed,
        json!({
            "files": args.files,
            "dirs": args.dirs,
            "max_steps_per_batch": args.namespace_job_steps,
            "job_batches": clear_batches,
            "processed_objects": clear_job.processed_object_count,
            "objects_per_second": rate(clear_job.processed_object_count, clear_elapsed),
        }),
    );

    let clear_verify_start = Instant::now();
    let (clear_remaining_entries, clear_verify_pages) =
        count_directory_entries_paged(&dfs, clear_root, args.list_page_size)?;
    let clear_verify_elapsed = clear_verify_start.elapsed();
    emit(
        "directory_jobs_recursive_clear_verify_paged",
        clear_verify_elapsed,
        json!({
            "remaining_entries": clear_remaining_entries,
            "pages": clear_verify_pages,
            "page_size": args.list_page_size,
        }),
    );
    if clear_remaining_entries != 0 {
        return Err(format!(
            "recursive clear left {clear_remaining_entries} visible entries under {clear_root}"
        )
        .into());
    }

    let rename_start = Instant::now();
    let mut rename_job = dfs.posix_rename_directory_bounded(
        rename_old_root,
        rename_new_root,
        args.namespace_job_steps,
    )?;
    let mut rename_batches = 1usize;
    while rename_job.status != DfsNamespaceJobStatus::Completed {
        rename_job =
            dfs.posix_resume_namespace_job_bounded(&rename_job.job_id, args.namespace_job_steps)?;
        rename_batches = rename_batches.saturating_add(1);
    }
    let rename_elapsed = rename_start.elapsed();
    emit(
        "directory_jobs_rename_subtree",
        rename_elapsed,
        json!({
            "files": args.files,
            "dirs": args.dirs,
            "max_steps_per_batch": args.namespace_job_steps,
            "job_batches": rename_batches,
            "processed_objects": rename_job.processed_object_count,
            "objects_per_second": rate(rename_job.processed_object_count, rename_elapsed),
        }),
    );

    let rename_verify_start = Instant::now();
    let rename_listing =
        count_tree_entries_paged(&dfs, rename_new_root, args.dirs, args.list_page_size)?;
    let rename_verify_elapsed = rename_verify_start.elapsed();
    emit(
        "directory_jobs_rename_verify_paged",
        rename_verify_elapsed,
        json!({
            "expected_dirs": args.dirs,
            "expected_files": args.files,
            "root_entries": rename_listing.root_entries,
            "file_entries": rename_listing.file_entries,
            "pages": rename_listing.pages,
            "page_size": args.list_page_size,
            "entries_per_second": rate(
                rename_listing
                    .root_entries
                    .saturating_add(rename_listing.file_entries) as u64,
                rename_verify_elapsed
            ),
        }),
    );
    if rename_listing.root_entries != args.dirs || rename_listing.file_entries != args.files {
        return Err(format!(
            "renamed tree verification mismatch: root entries {} expected {}, file entries {} expected {}",
            rename_listing.root_entries, args.dirs, rename_listing.file_entries, args.files
        )
        .into());
    }

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

    let range_stream_start = Instant::now();
    let mut range_reader = store.open_blob_range_stream(
        &manifest.content_hash,
        range_offset,
        args.blob_range_bytes,
    )?;
    let mut range_stream_bytes = 0u64;
    loop {
        let read = range_reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        range_stream_bytes = range_stream_bytes.saturating_add(read as u64);
    }
    let range_stream_elapsed = range_stream_start.elapsed();
    emit(
        "blob_range_stream_read",
        range_stream_elapsed,
        json!({
            "offset": range_offset,
            "bytes": range_stream_bytes,
            "requested_bytes": args.blob_range_bytes,
            "mib_per_second": mib_per_second(range_stream_bytes, range_stream_elapsed),
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
    let reader = node("node_dfs_bench_net_reader");
    let range_reader = node("node_dfs_bench_net_range_reader");
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content"))?,
    )
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("owner-metadata"),
    )?)?;
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content"))?,
    )
    .with_metadata_backend(LocalSobjMetadataBackend::new(
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

    let network_timeout = Duration::from_secs(args.network_timeout_secs);
    let replica_object_id = object_id.clone();
    let replica_worker = std::thread::spawn(move || -> BenchResult<ReplicaWorkerReport> {
        let deadline = Instant::now() + network_timeout;
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
            std::thread::sleep(Duration::from_millis(1));
        }
        report.metrics = Some(replica_dfs.operational_metrics()?);
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
    let initial_owner_metrics = owner_dfs.operational_metrics()?;

    let deadline = Instant::now() + Duration::from_secs(args.network_timeout_secs);
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
        std::thread::sleep(Duration::from_millis(1));
    };
    let write_elapsed = write_start.elapsed();
    let replica_report = replica_worker
        .join()
        .map_err(|_| "replica network blob worker panicked")??;
    let final_owner_metrics = owner_dfs.operational_metrics()?;

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
            "initial_owner_metrics": initial_owner_metrics,
            "final_owner_metrics": final_owner_metrics,
            "replica_metrics": replica_report.metrics,
        }),
    );

    if !owner_committed {
        return Err("owner did not observe network blob commit before timeout".into());
    }

    let mut reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        LocalContentStore::new(root.join("reader-content"))?,
    )
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("reader-metadata"),
    )?)?;
    reader_dfs.add_file_system(DfsFileSystem::sifs(owner.clone()))?;
    let mut reader_object = owner_dfs
        .metadata()
        .object(&object_id)
        .ok_or("owner metadata missing committed network blob object")?
        .clone();
    reader_object.current_locations = vec![owner.clone()];
    let expected_content_hash = reader_object.content_hash.clone();
    reader_dfs.add_object_metadata(reader_object)?;

    let read_start = Instant::now();
    let requested_sources = reader_dfs.request_remote_object_read(&object_id)?;
    if requested_sources.is_empty() {
        return Err("remote read benchmark could not route initial read request".into());
    }
    let deadline = Instant::now() + Duration::from_secs(args.network_timeout_secs);
    let mut owner_read_events = 0usize;
    let mut reader_read_events = 0usize;
    let mut read_process_loops = 0usize;
    loop {
        let owner_events = owner_dfs.process_incoming()?;
        let reader_events = reader_dfs.process_incoming()?;
        owner_read_events = owner_read_events.saturating_add(owner_events.len());
        reader_read_events = reader_read_events.saturating_add(reader_events.len());
        read_process_loops = read_process_loops.saturating_add(1);
        let mut completed = false;
        for event in &reader_events {
            if let NetworkedDfsServiceEvent::RemoteReadCompleted {
                object_id: completed_object_id,
                result,
                ..
            } = event
                && completed_object_id == &object_id
            {
                match result {
                    DfsReadResult::Found(data) if data.content_hash == expected_content_hash => {
                        completed = true;
                    }
                    DfsReadResult::Found(data) => {
                        return Err(format!(
                            "remote read completed with unexpected content hash {:?}, expected {:?}",
                            data.content_hash, expected_content_hash
                        )
                        .into());
                    }
                    other => {
                        return Err(format!(
                            "remote read benchmark completed without object bytes: {other:?}"
                        )
                        .into());
                    }
                }
            }
        }
        if completed {
            break;
        }
        if Instant::now() >= deadline {
            return Err("remote read benchmark did not complete before timeout".into());
        }
        if owner_events.is_empty() && reader_events.is_empty() {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    let read_elapsed = read_start.elapsed();
    let verify_bytes = byte_count_to_usize(
        "network read verification bytes",
        args.network_blob_bytes.min(args.blob_range_bytes as u64),
    )?;
    let cached_range =
        match reader_dfs.read_local_object_range_with_state(&object_id, 0, verify_bytes, false)? {
            NetworkedDfsRangeReadOutcome::Found { data, .. } => data.bytes.len(),
            other => {
                return Err(format!(
                    "remote read benchmark did not cache a readable object range: {other:?}"
                )
                .into());
            }
        };
    let reader_metrics = reader_dfs.operational_metrics()?;
    emit(
        "network_blob_remote_read",
        read_elapsed,
        json!({
            "bytes": args.network_blob_bytes,
            "mib_per_second": mib_per_second(args.network_blob_bytes, read_elapsed),
            "initial_sources": requested_sources,
            "owner_events": owner_read_events,
            "reader_events": reader_read_events,
            "reader_process_loops": read_process_loops,
            "verification_range_bytes": cached_range,
            "reader_metrics": reader_metrics,
        }),
    );

    let range_bytes = byte_count_to_usize(
        "network range read bytes",
        args.network_blob_bytes.min(args.blob_range_bytes as u64),
    )?;
    let range_offset = args.network_blob_bytes.saturating_sub(range_bytes as u64) / 2;
    let mut range_reader_dfs = NetworkedDfsService::new(
        network.add_node(range_reader.clone()),
        LocalContentStore::new(root.join("range-reader-content"))?,
    )
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("root"),
        root.join("range-reader-metadata"),
    )?)?;
    range_reader_dfs.add_file_system(DfsFileSystem::sifs(owner.clone()))?;
    let mut range_reader_object = owner_dfs
        .metadata()
        .object(&object_id)
        .ok_or("owner metadata missing committed network blob object for range read")?
        .clone();
    range_reader_object.current_locations = vec![owner.clone()];
    range_reader_dfs.add_object_metadata(range_reader_object)?;

    let range_start = Instant::now();
    let requested_range_sources =
        range_reader_dfs.request_remote_object_range_read(&object_id, range_offset, range_bytes)?;
    if requested_range_sources.is_empty() {
        return Err("remote range read benchmark could not route initial read request".into());
    }
    let deadline = Instant::now() + Duration::from_secs(args.network_timeout_secs);
    let mut owner_range_events = 0usize;
    let mut reader_range_events = 0usize;
    let mut range_process_loops = 0usize;
    let mut range_chunks = 0usize;
    let mut received_range_bytes = 0u64;
    loop {
        let owner_events = owner_dfs.process_incoming()?;
        let reader_events = range_reader_dfs.process_incoming()?;
        owner_range_events = owner_range_events.saturating_add(owner_events.len());
        reader_range_events = reader_range_events.saturating_add(reader_events.len());
        range_process_loops = range_process_loops.saturating_add(1);
        let mut completed = false;
        for event in &reader_events {
            match event {
                NetworkedDfsServiceEvent::RemoteRangeReadChunkReceived {
                    object_id: chunk_object_id,
                    content_hash,
                    range_offset: chunk_range_offset,
                    range_size_bytes,
                    object_total_size_bytes,
                    bytes,
                    complete,
                    ..
                } if chunk_object_id == &object_id => {
                    if content_hash != &expected_content_hash {
                        return Err(format!(
                            "remote range read chunk used unexpected content hash {:?}, expected {:?}",
                            content_hash, expected_content_hash
                        )
                        .into());
                    }
                    if *chunk_range_offset != range_offset {
                        return Err(format!(
                            "remote range read chunk offset mismatch: expected {range_offset}, actual {chunk_range_offset}"
                        )
                        .into());
                    }
                    if *range_size_bytes != range_bytes as u64 {
                        return Err(format!(
                            "remote range read chunk size mismatch: expected {range_bytes}, actual {range_size_bytes}"
                        )
                        .into());
                    }
                    if *object_total_size_bytes != args.network_blob_bytes {
                        return Err(format!(
                            "remote range read object size mismatch: expected {}, actual {object_total_size_bytes}",
                            args.network_blob_bytes
                        )
                        .into());
                    }
                    received_range_bytes = received_range_bytes.saturating_add(bytes.len() as u64);
                    range_chunks = range_chunks.saturating_add(1);
                    completed |= *complete;
                }
                NetworkedDfsServiceEvent::RemoteReadCompleted {
                    object_id: completed_object_id,
                    result: DfsReadResult::RangeFound(data),
                    ..
                } if completed_object_id == &object_id => {
                    if data.content_hash != expected_content_hash {
                        return Err(format!(
                            "remote inline range read used unexpected content hash {:?}, expected {:?}",
                            data.content_hash, expected_content_hash
                        )
                        .into());
                    }
                    if data.offset != range_offset || data.bytes.len() != range_bytes {
                        return Err(format!(
                            "remote inline range read mismatch: expected offset {range_offset} bytes {range_bytes}, actual offset {} bytes {}",
                            data.offset,
                            data.bytes.len()
                        )
                        .into());
                    }
                    received_range_bytes =
                        received_range_bytes.saturating_add(data.bytes.len() as u64);
                    completed = true;
                }
                NetworkedDfsServiceEvent::RemoteReadCompleted {
                    object_id: completed_object_id,
                    result,
                    ..
                } if completed_object_id == &object_id => {
                    return Err(format!(
                        "remote range read benchmark completed without range bytes: {result:?}"
                    )
                    .into());
                }
                _ => {}
            }
        }
        if completed {
            break;
        }
        if Instant::now() >= deadline {
            return Err("remote range read benchmark did not complete before timeout".into());
        }
        if owner_events.is_empty() && reader_events.is_empty() {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    let range_elapsed = range_start.elapsed();
    let range_reader_metrics = range_reader_dfs.operational_metrics()?;
    let range_cached_full_object = range_reader_dfs.read_local_object(&object_id)?.is_some();
    emit(
        "network_blob_remote_range_read",
        range_elapsed,
        json!({
            "object_bytes": args.network_blob_bytes,
            "range_offset": range_offset,
            "requested_bytes": range_bytes,
            "received_bytes": received_range_bytes,
            "chunks": range_chunks,
            "mib_per_second": mib_per_second(received_range_bytes, range_elapsed),
            "initial_sources": requested_range_sources,
            "owner_events": owner_range_events,
            "reader_events": reader_range_events,
            "reader_process_loops": range_process_loops,
            "cached_full_object": range_cached_full_object,
            "reader_metrics": range_reader_metrics,
        }),
    );

    Ok(())
}

fn run_network_write_at(args: &Args) -> BenchResult<()> {
    let root = args.scenario_root("network-write-at");
    remove_dir_all_if_exists(&root)?;
    fs::create_dir_all(&root)?;

    let owner = node("node_dfs_bench_net_write_at_owner");
    let replica = node("node_dfs_bench_net_write_at_replica");
    let object_id = DfsObjectId::from_legacy_named("dfsbenchnetwriteat").expect("object id");
    let base_version = DfsVersion::from_legacy_named("versionnetwriteatbase").expect("version");
    let path = "/services/dfs-scale/network-write-at.bin";
    let network = InProcessNetwork::new();
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content"))?,
    )
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("owner"),
        root.join("owner-metadata"),
    )?)?;
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(root.join("replica-content"))?,
    )
    .with_metadata_backend(LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("replica"),
        root.join("replica-metadata"),
    )?)?;

    owner_dfs.bootstrap_distributed_sifs()?;
    replica_dfs.bootstrap_distributed_sifs()?;
    let base_start = Instant::now();
    let owner_base = owner_dfs.content().put_object_version_from_reader(
        object_id.as_str(),
        base_version.as_str(),
        RepeatingReader::new(args.blob_bytes),
    )?;
    let replica_base = replica_dfs.content().put_object_version_from_reader(
        object_id.as_str(),
        base_version.as_str(),
        RepeatingReader::new(args.blob_bytes),
    )?;
    if owner_base.content_hash != replica_base.content_hash {
        return Err("network write_at base content diverged between owner and replica".into());
    }
    let mut object = file_metadata(
        &owner,
        0,
        1,
        Some(owner_base.content_hash),
        Some(args.blob_bytes),
    )?;
    object.object_id = object_id.clone();
    object.path = path.to_string();
    object.version = base_version;
    object.current_locations = vec![owner.clone(), replica.clone()];
    owner_dfs.add_object_metadata(object.clone())?;
    replica_dfs.add_object_metadata(object)?;
    let base_elapsed = base_start.elapsed();
    emit(
        "network_write_at_base_seed",
        base_elapsed,
        json!({
            "bytes": args.blob_bytes,
            "mib_per_second": mib_per_second(args.blob_bytes.saturating_mul(2), base_elapsed),
        }),
    );

    let policy = DfsReplicationPolicy::new(2, 2)?;
    let patch_start = Instant::now();
    let mut owner_events = 0usize;
    let mut replica_events = 0usize;
    let mut process_loops = 0usize;
    let mut patched_bytes = 0u64;
    let mut last_offset = 0u64;
    let mut last_patch = Vec::new();
    for index in 0..args.write_at_ops {
        let offset = write_at_offset(index, args.blob_bytes, args.write_at_bytes);
        let previous_version = bench_object_metadata(&owner_dfs, &object_id)?
            .version
            .clone();
        owner_dfs.posix_write_file_at_from_reader(
            path,
            offset,
            RepeatingReader::with_seed(args.write_at_bytes as u64, index as u64),
            DfsWriteOptions {
                policy: policy.clone(),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
            },
        )?;
        let deadline = Instant::now() + Duration::from_secs(args.network_timeout_secs);
        loop {
            let replica_batch = replica_dfs.process_incoming()?;
            let owner_batch = owner_dfs.process_incoming()?;
            let replica_commit_batch = replica_dfs.process_incoming()?;
            owner_events = owner_events.saturating_add(owner_batch.len());
            replica_events = replica_events
                .saturating_add(replica_batch.len())
                .saturating_add(replica_commit_batch.len());
            process_loops = process_loops.saturating_add(1);
            let owner_object = bench_object_metadata(&owner_dfs, &object_id)?;
            let replica_object = bench_object_metadata(&replica_dfs, &object_id)?;
            if owner_object.version != previous_version
                && replica_object.version == owner_object.version
                && replica_object.content_hash == owner_object.content_hash
            {
                break;
            }
            if Instant::now() >= deadline {
                return Err(
                    format!("network write_at op {index} did not commit before timeout").into(),
                );
            }
            if owner_batch.is_empty() && replica_batch.is_empty() && replica_commit_batch.is_empty()
            {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        patched_bytes = patched_bytes.saturating_add(args.write_at_bytes as u64);
        last_offset = offset;
        last_patch = patch_bytes(index, args.write_at_bytes);
    }
    let patch_elapsed = patch_start.elapsed();
    let owner_object = bench_object_metadata(&owner_dfs, &object_id)?;
    let replica_object = bench_object_metadata(&replica_dfs, &object_id)?;
    let owner_manifest = owner_dfs
        .content()
        .read_manifest(object_id.as_str(), owner_object.version.as_str())?;
    let replica_manifest = replica_dfs
        .content()
        .read_manifest(object_id.as_str(), replica_object.version.as_str())?;
    let owner_logical_blob_materialized =
        owner_dfs.content().has_blob(&owner_manifest.content_hash)?;
    let replica_logical_blob_materialized = replica_dfs
        .content()
        .has_blob(&replica_manifest.content_hash)?;
    let owner_metrics = owner_dfs.operational_metrics()?;
    let replica_metrics = replica_dfs.operational_metrics()?;
    emit(
        "network_write_at_patches",
        patch_elapsed,
        json!({
            "base_bytes": args.blob_bytes,
            "ops": args.write_at_ops,
            "patch_bytes": args.write_at_bytes,
            "patched_bytes": patched_bytes,
            "ops_per_second": rate(args.write_at_ops as u64, patch_elapsed),
            "mib_per_second": mib_per_second(patched_bytes, patch_elapsed),
            "owner_events": owner_events,
            "replica_events": replica_events,
            "process_loops": process_loops,
            "owner_extent_count": owner_manifest.extents.len(),
            "replica_extent_count": replica_manifest.extents.len(),
            "owner_logical_blob_materialized": owner_logical_blob_materialized,
            "replica_logical_blob_materialized": replica_logical_blob_materialized,
            "owner_metrics": owner_metrics,
            "replica_metrics": replica_metrics,
        }),
    );

    let verify_start = Instant::now();
    let owner_verify = owner_dfs.posix_read_file_range(path, last_offset, last_patch.len())?;
    let replica_verify = match replica_dfs.read_local_object_range_with_state(
        &object_id,
        last_offset,
        last_patch.len(),
        false,
    )? {
        NetworkedDfsRangeReadOutcome::Found { data, .. } => data.bytes,
        other => {
            return Err(format!(
                "network write_at benchmark could not read replica verification range: {other:?}"
            )
            .into());
        }
    };
    if owner_verify != last_patch || replica_verify != last_patch {
        return Err("network write_at verification range did not match final patch".into());
    }
    let verify_elapsed = verify_start.elapsed();
    emit(
        "network_write_at_verify_range_read",
        verify_elapsed,
        json!({
            "offset": last_offset,
            "bytes": last_patch.len(),
            "mib_per_second": mib_per_second(last_patch.len() as u64, verify_elapsed),
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
    metrics: Option<DfsOperationalMetrics>,
}

#[derive(Clone, Debug, Default)]
struct TreeListingReport {
    root_entries: usize,
    file_entries: usize,
    pages: usize,
}

fn create_tree_dirs<T: DfsFrameTransport>(
    dfs: &mut NetworkedDfsService<T>,
    root_path: &str,
    dirs: usize,
) -> BenchResult<()> {
    dfs.posix_mkdir_all(root_path)?;
    for dir_index in 0..dirs {
        dfs.posix_mkdir_all(&tree_dir_path(root_path, dir_index))?;
    }
    Ok(())
}

fn populate_tree_metadata<T: DfsFrameTransport>(
    dfs: &mut NetworkedDfsService<T>,
    owner: &NodeId,
    label: &str,
    root_path: &str,
    files: usize,
    dirs: usize,
    file_bytes: usize,
    batch_size: usize,
    hash_offset: usize,
) -> BenchResult<usize> {
    let mut batch = Vec::with_capacity(batch_size);
    let mut batches = 0usize;
    for index in 0..files {
        batch.push(tree_file_metadata(
            owner,
            label,
            root_path,
            index,
            dirs,
            file_bytes,
            hash_offset.saturating_add(index),
        )?);
        if batch.len() >= batch_size {
            dfs.add_object_metadata_batch(batch.drain(..))?;
            batches = batches.saturating_add(1);
        }
    }
    if !batch.is_empty() {
        dfs.add_object_metadata_batch(batch.drain(..))?;
        batches = batches.saturating_add(1);
    }
    Ok(batches)
}

fn tree_file_metadata(
    owner: &NodeId,
    label: &str,
    root_path: &str,
    index: usize,
    dirs: usize,
    file_bytes: usize,
    hash_index: usize,
) -> BenchResult<DfsObjectMetadata> {
    Ok(DfsObjectMetadata {
        object_id: DfsObjectId::from_legacy_named(format!("dfsbench{label}obj{index:016x}"))?,
        file_system_id: sifs_id(),
        path: format!(
            "{}/file-{index:08}.bin",
            tree_dir_path(root_path, index % dirs)
        ),
        kind: DfsObjectKind::File,
        owner_node_id: owner.clone(),
        current_locations: vec![owner.clone()],
        content_hash: Some(valid_fake_hash(hash_index)),
        content_size_bytes: Some(file_bytes as u64),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: index as u64,
        created_at_unix_nanos: index as u64,
        changed_at_unix_nanos: index as u64,
        version: DfsVersion::from_legacy_named(format!("dfsbench{label}ver{index:016x}"))?,
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    })
}

fn count_tree_entries_paged<T: DfsFrameTransport>(
    dfs: &NetworkedDfsService<T>,
    root_path: &str,
    dirs: usize,
    page_size: usize,
) -> BenchResult<TreeListingReport> {
    let (root_entries, root_pages) = count_directory_entries_paged(dfs, root_path, page_size)?;
    let mut report = TreeListingReport {
        root_entries,
        pages: root_pages,
        ..TreeListingReport::default()
    };
    for dir_index in 0..dirs {
        let (entries, pages) =
            count_directory_entries_paged(dfs, &tree_dir_path(root_path, dir_index), page_size)?;
        report.file_entries = report.file_entries.saturating_add(entries);
        report.pages = report.pages.saturating_add(pages);
    }
    Ok(report)
}

fn count_directory_entries_paged<T: DfsFrameTransport>(
    dfs: &NetworkedDfsService<T>,
    path: &str,
    page_size: usize,
) -> BenchResult<(usize, usize)> {
    let mut entries = 0usize;
    let mut pages = 0usize;
    let mut cursor = None;
    loop {
        let page = dfs.posix_list_dir_page(path, cursor.as_ref(), page_size)?;
        entries = entries.saturating_add(page.entries.len());
        pages = pages.saturating_add(1);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok((entries, pages))
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

fn tree_dir_path(root_path: &str, index: usize) -> String {
    format!("{}/dir-{index:04}", root_path.trim_end_matches('/'))
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

fn write_at_offset(index: usize, object_size_bytes: u64, patch_bytes: usize) -> u64 {
    let patch_bytes = patch_bytes as u64;
    let max_start = object_size_bytes.saturating_sub(patch_bytes);
    if max_start == 0 {
        return 0;
    }
    let mut value = (index as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    value % (max_start + 1)
}

fn patch_bytes(index: usize, len: usize) -> Vec<u8> {
    let mut reader = RepeatingReader::with_seed(len as u64, index as u64);
    let mut bytes = vec![0_u8; len];
    reader
        .read_exact(&mut bytes)
        .expect("in-memory patch reader should fill requested bytes");
    bytes
}

fn bench_object_metadata<T: DfsFrameTransport>(
    dfs: &NetworkedDfsService<T>,
    object_id: &DfsObjectId,
) -> BenchResult<DfsObjectMetadata> {
    match dfs.metadata().object(object_id) {
        Some(object) => Ok(object.clone()),
        None => Err(format!("benchmark object metadata missing for {object_id}").into()),
    }
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

fn env_bytes(name: &str, default: u64) -> BenchResult<u64> {
    match env::var(name) {
        Ok(value) => parse_byte_count_value(name, &value),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("could not read {name}: {error}").into()),
    }
}

fn parse_u64(flag: &str, value: Option<String>) -> BenchResult<u64> {
    let value = required_value(flag, value)?;
    value
        .parse::<u64>()
        .map_err(|error| format!("{flag} value {value:?} is not a u64: {error}").into())
}

fn parse_usize(flag: &str, value: Option<String>) -> BenchResult<usize> {
    let value = required_value(flag, value)?;
    value
        .parse::<usize>()
        .map_err(|error| format!("{flag} value {value:?} is not a usize: {error}").into())
}

fn parse_byte_count(flag: &str, value: Option<String>) -> BenchResult<u64> {
    let value = required_value(flag, value)?;
    parse_byte_count_value(flag, &value)
}

fn byte_count_to_usize(label: &str, value: u64) -> BenchResult<usize> {
    value
        .try_into()
        .map_err(|_| format!("{label} value is too large for this platform").into())
}

fn parse_byte_count_value(label: &str, value: &str) -> BenchResult<u64> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(format!("{label} value must not be empty").into());
    }
    let split_at = normalized
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(normalized.len());
    let (digits, suffix) = normalized.split_at(split_at);
    if digits.is_empty() {
        return Err(format!("{label} value {value:?} must start with digits").into());
    }
    let base = digits
        .parse::<u64>()
        .map_err(|error| format!("{label} value {value:?} is not a byte count: {error}"))?;
    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" => 1_000,
        "m" | "mb" => 1_000_000,
        "g" | "gb" => 1_000_000_000,
        "t" | "tb" => 1_000_000_000_000,
        "kib" => 1024,
        "mib" => 1024 * 1024,
        "gib" => 1024 * 1024 * 1024,
        "tib" => 1024_u64.pow(4),
        _ => {
            return Err(format!(
                "{label} value {value:?} has unsupported suffix; use B, KB, MB, GB, TB, KiB, MiB, GiB, or TiB"
            )
            .into())
        }
    };
    base.checked_mul(multiplier)
        .ok_or_else(|| format!("{label} value {value:?} overflows u64").into())
}

fn required_value(flag: &str, value: Option<String>) -> BenchResult<String> {
    value.ok_or_else(|| format!("{flag} requires a value").into())
}

fn print_help() {
    println!(
        "Usage: cargo bench -p sinomo-dfs --bench scale -- [OPTIONS]\n\
         \n\
         Options:\n\
           --scenario all|namespace|posix-create|posix-read|posix-write-at|metadata-replication|directory-jobs|blob|network-blob|network-write-at\n\
           --preset smoke|scale|production\n\
           --root PATH\n\
           --files COUNT\n\
           --dirs COUNT\n\
           --file-bytes BYTES\n\
           --blob-bytes BYTES\n\
           --blob-range-bytes BYTES\n\
           --network-blob-bytes BYTES\n\
           --write-at-ops COUNT\n\
           --write-at-bytes BYTES\n\
           --list-page-size COUNT\n\
           --metadata-batch-size COUNT\n\
           --namespace-job-steps COUNT\n\
           --network-timeout-secs SECONDS\n\
           --keep\n\
         \n\
         Environment defaults:\n\
           SINOMO_DFS_BENCH_PRESET,\n\
           SINOMO_DFS_BENCH_FILES, SINOMO_DFS_BENCH_DIRS,\n\
           SINOMO_DFS_BENCH_FILE_BYTES, SINOMO_DFS_BENCH_BLOB_BYTES,\n\
           SINOMO_DFS_BENCH_BLOB_RANGE_BYTES,\n\
           SINOMO_DFS_BENCH_NETWORK_BLOB_BYTES,\n\
           SINOMO_DFS_BENCH_WRITE_AT_OPS, SINOMO_DFS_BENCH_WRITE_AT_BYTES,\n\
           SINOMO_DFS_BENCH_LIST_PAGE_SIZE,\n\
           SINOMO_DFS_BENCH_METADATA_BATCH_SIZE,\n\
           SINOMO_DFS_BENCH_NAMESPACE_JOB_STEPS,\n\
           SINOMO_DFS_BENCH_NETWORK_TIMEOUT_SECS\n\
         \n\
         Presets:\n\
           smoke: 10k files, 64 MiB local blob, 8 MiB network blob, 60s network timeout\n\
           scale: 100k files, 1 GiB local blob, 512 MiB network blob, 1k write_at ops, 300s network timeout\n\
           production: 1M files, 4 GiB local blob, 4 GiB network blob, 8k write_at ops, 1800s network timeout\n\
         \n\
         Byte flags accept B, KB, MB, GB, TB, KiB, MiB, GiB, and TiB suffixes."
    );
}

struct RepeatingReader {
    remaining: u64,
    state: u64,
}

impl RepeatingReader {
    fn new(bytes: u64) -> Self {
        Self::with_seed(bytes, 0)
    }

    fn with_seed(bytes: u64, seed: u64) -> Self {
        Self {
            remaining: bytes,
            state: 0x9e37_79b9_7f4a_7c15 ^ seed.wrapping_mul(0xbf58_476d_1ce4_e5b9),
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
