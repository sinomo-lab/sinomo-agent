//! Reuse the exact owned-child/raw-file harness for a genuinely healthy full3 fixture.
//! A manifest or retained history is DATA; fresh native calls supply new provenance.
use super::*;
use crate::metadata_backend::native_prefix_history::{
    NATIVE_PREFIX_HISTORY_FILE, NativePrefixHistoryDisposition, NativePrefixHistoryRecord,
    with_after_publication_cut_for_test,
};
use crate::node_transport::OwnedNativeMetadataPrefixRound;
use crate::test_runtime::RetainedNativeHealthyFixture;

const CHILD: &str = "runtime_host::tests::incomplete_genesis::owned_process::healthy::retained_healthy_full3_process_child";
const ENV: &str = "SINOMO_DFS_RETAINED_HEALTHY_FULL3_INPUT";
const SEED: &str = "/native-healthy-retained-seed";
const OP_CAP: Duration = sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Cut {
    Returned,
    AfterPublication,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthyInput {
    owned_root: PathBuf,
    invocation: String,
    cut: Cut,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthyReady {
    pid: u32,
    invocation: String,
    cut: Cut,
    manifest: RetainedNativeHealthyFixture,
    history_path: PathBuf,
    history: NativePrefixHistoryRecord,
    history_bytes: Vec<u8>,
    // Parent spawn precedes helper entry: parentStart + elapsed + OP_CAP is
    // conservatively EARLIER than the child's original intake deadline.
    intake_elapsed_nanos: u64,
    intake_returned: bool,
}
fn ready_owned(owned: &mut OwnedChild, deadline: Instant) -> TestResult<HealthyReady> {
    loop {
        if Instant::now() >= deadline {
            return Err("healthy Ready exceeded original60s".into());
        }
        let child = owned
            .child
            .as_mut()
            .ok_or("owned child absent before Ready")?;
        if let Some(status) = child.try_wait()? {
            owned.child = None;
            return Err(format!("healthy child ended before Ready: {status}").into());
        }
        let path = owned.root.join("ready.sobj");
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                let ready: HealthyReady = read_data(&path)?;
                if ready.pid != child.id() || Instant::now() >= deadline {
                    return Err("healthy Ready identity/deadline differs".into());
                }
                return Ok(ready);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        std::thread::sleep(
            Duration::from_millis(10).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}
fn capture_actual_round(
    native: &DfsTestRuntime,
    sync_first: bool,
) -> TestResult<OwnedNativeMetadataPrefixRound> {
    let shard = native.root_shard()?;
    assert_eq!(shard.replica_node_ids.len(), 2);
    let mut service = native.metadata_primary_service(SEED)?;
    let object = service
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&shard.file_system_id, SEED)?
        .ok_or("genuine retained namespace seed missing")?;
    let mut target = service.capture_metadata_quorum_sync_target(&object)?;
    if sync_first {
        let proof = service.sync_metadata_shard_to_quorum(&target, OP_CAP)?;
        let all = std::iter::once(shard.primary_node_id.clone())
            .chain(shard.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            proof
                .acknowledged_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            all
        );
        target = service.capture_metadata_quorum_sync_target(&object)?;
    }
    drop(service); // never hold a metadata lease across actual native RPC
    let nodes: Vec<NodeId> = std::iter::once(shard.primary_node_id.clone())
        .chain(shard.replica_node_ids.iter().cloned())
        .collect();
    let mut transport = crate::runtime_host::daemon_dfs_node_transport_on_runtime(
        shard.primary_node_id.clone(),
        nodes,
        native.data_dir(&shard.primary_node_id),
        &native.runtime_handle(),
    );
    let deadline = Instant::now() + OP_CAP;
    transport.set_operation_deadline(Some(deadline));
    let round = transport.capture_metadata_prefix_full_assignment_round(&target)?;
    assert!(
        round.is_complete(),
        "actual full3 native round incomplete: {:?}",
        round.finalization_error()
    );
    assert_eq!(round.deadline(), deadline);
    assert_eq!(round.peer_outcomes().len(), 2);
    assert_eq!(
        round
            .peer_outcomes()
            .iter()
            .map(|p| p.peer().clone())
            .collect::<BTreeSet<_>>(),
        shard.replica_node_ids.iter().cloned().collect()
    );
    for peer in round.peer_outcomes() {
        let cap = peer
            .native_return()
            .ok_or("generic DATA cannot replace an actual native return")?;
        cap.validate_binding(
            peer.peer(),
            peer.command(),
            &native.data_dir(&shard.primary_node_id).root(),
        )?;
        assert_eq!(Some(cap.response()), peer.response());
    }
    Ok(round)
}
fn file_bytes_and_identity(path: &Path) -> TestResult<(Vec<u8>, FileId)> {
    let before = fs::symlink_metadata(path)?;
    if !before.is_file() || before.file_type().is_symlink() {
        return Err("historical path is not a regular file".into());
    }
    let file = File::open(path)?;
    let id = FileId::of(&before);
    assert_eq!(FileId::of(&file.metadata()?), id);
    let bytes = read_pinned(&file, id.size, DATA_CAP)?;
    assert_eq!(FileId::of(&fs::symlink_metadata(path)?), id);
    Ok((bytes, id))
}
fn run_healthy(cut: Cut) {
    let fixture = sinomo_testdir::TestDir::new("retained-healthy-full3-owned-sigkill");
    let root = fixture.path().to_path_buf();
    let child_tmp = root.join("child-tmp");
    fs::create_dir(&child_tmp).unwrap();
    let input = HealthyInput {
        owned_root: root.clone(),
        invocation: format!("{}:{cut:?}", root.file_name().unwrap().to_string_lossy()),
        cut,
    };
    let input_path = root.join("input.sobj");
    write_data(&input_path, &input).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", CHILD, "--ignored", "--nocapture"])
        .process_group(0)
        .env(ENV, &input_path)
        .env("TMPDIR", &child_tmp)
        .env("SINOMO_TEST_KEEP_TMP", "1")
        .stdin(Stdio::piped())
        .stdout(File::create(root.join("child.stdout.log")).unwrap())
        .stderr(File::create(root.join("child.stderr.log")).unwrap());
    let started = Instant::now();
    let ready_deadline = started + READY_CAP;
    let mut owned = OwnedChild::spawn(fixture, &mut command).unwrap();
    let mut report = serde_json::json!({"cut":cut,"scope":"all3 actual native nodes die in one exact owned process; no surviving-node availability claim",
        "limits":"raw preserved before any native reopen; historical DATA is not provenance/authority; no power-loss or quietness acceptance"});
    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> TestResult<()> {
        let ready = ready_owned(&mut owned, ready_deadline)?;
        assert_eq!(ready.invocation, input.invocation);
        assert_eq!(ready.cut, cut);
        assert_eq!(ready.intake_returned, cut == Cut::Returned);
        let expected = ready.manifest;
        assert_eq!(expected.nodes.len(), 3);
        assert_eq!(expected.shard.replica_node_ids.len(), 2);
        let all = expected
            .nodes
            .iter()
            .map(|n| n.node_id.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            all,
            std::iter::once(expected.shard.primary_node_id.clone())
                .chain(expected.shard.replica_node_ids.iter().cloned())
                .collect()
        );
        assert_eq!(expected.shards.len(), 3);
        let mut pins = pin_ancestors(&child_tmp, &expected.fixture_root)?;
        let fixture_id = DirectoryId::read(&expected.fixture_root)?;
        assert_eq!(
            (fixture_id.device, fixture_id.inode),
            expected.fixture_identity
        );
        for node in &expected.nodes {
            pins.extend(pin_ancestors(&expected.fixture_root, &node.data_root)?);
            let id = DirectoryId::read(&node.data_root)?;
            assert_eq!((id.device, id.inode), node.data_identity);
        }
        for shard in &expected.shards {
            pins.extend(pin_ancestors(&expected.fixture_root, &shard.shard_root)?);
            let id = DirectoryId::read(&shard.shard_root)?;
            assert_eq!((id.device, id.inode), shard.shard_identity);
        }
        let data = expected
            .nodes
            .iter()
            .find(|n| n.node_id == expected.shard.primary_node_id)
            .unwrap();
        let shard_id = DfsMetadataShardId::new(&expected.shard.shard_id);
        let expected_history =
            crate::local_dfs_metadata_shard_path(&SinomoDataDir::new(&data.data_root), &shard_id)
                .join(NATIVE_PREFIX_HISTORY_FILE);
        assert_eq!(ready.history_path, expected_history);
        assert_eq!(
            crate::metadata_io::encode_sobj(&ready.history)?,
            ready.history_bytes
        );
        assert_eq!(ready.history.source().prefix(), &expected.prefix);
        let kill_before = if cut == Cut::AfterPublication {
            started
                .checked_add(Duration::from_nanos(ready.intake_elapsed_nanos))
                .unwrap()
                .checked_add(OP_CAP)
                .unwrap()
                .min(ready_deadline)
        } else {
            ready_deadline
        };
        assert!(
            Instant::now() < kill_before,
            "no extension of child intake or ready deadline"
        );
        report["owned_process_identity"] = serde_json::to_value(&owned.identity)?;
        report["manifest"] = serde_json::to_value(&expected)?;
        report["historical_record"] = serde_json::to_value(&ready.history)?;
        report["intake_returned_before_kill"] = serde_json::json!(ready.intake_returned);
        let status = owned.kill_and_reap(kill_before)?;
        assert_eq!(status.signal(), Some(9));
        report["actual_signal"] = serde_json::json!(status.signal());
        report["ready_kill_elapsed_seconds"] = serde_json::json!(started.elapsed().as_secs_f64());
        for (path, file, id) in &pins {
            assert_eq!(DirectoryId::read(path)?, *id);
            assert_eq!(DirectoryId::of(&file.metadata()?), *id);
        }
        // NO parent native validator/initializer/inspection before this capture.
        let mut entries = 0;
        let mut byte_count = 0;
        let raw_pins = PinnedTree::open(
            expected.fixture_root.clone(),
            fixture_id.uid,
            &mut entries,
            &mut byte_count,
        )?;
        let raw = raw_pins.capture()?;
        let relative = ready.history_path.strip_prefix(&expected.fixture_root)?;
        assert_eq!(
            raw.files
                .iter()
                .find(|f| f.relative == relative)
                .ok_or("historical file absent after kill")?
                .bytes,
            ready.history_bytes
        );
        let encoded = crate::metadata_io::encode_sobj(&raw)?;
        assert!(encoded.len() <= RAW_CAP + DATA_CAP);
        crate::metadata_io::write_file_atomic(
            &root.join("raw-postkill-before-native-open.sobj"),
            &encoded,
        )?;
        report["raw_postkill"] = serde_json::json!({"files":raw.files.len(),"entries":entries,"bytes":byte_count,
            "blake3":blake3::hash(&encoded).to_hex().to_string(),"before_any_native_validation_or_open":true});
        report["refusals"] = serde_json::json!([]);
        let mut missing = expected.clone();
        missing.nodes[0].data_root = expected.fixture_root.join("missing-native-root");
        let mut foreign = expected.clone();
        foreign.nodes[0].data_identity = expected.nodes[1].data_identity;
        let mut incomplete = expected.clone();
        incomplete.shard.replica_node_ids.pop();
        let mut wrong_prefix = expected.clone();
        let old = wrong_prefix.prefix.digest.pop().unwrap();
        wrong_prefix
            .prefix
            .digest
            .push(if old == '0' { '1' } else { '0' });
        let mut wrong_epoch = expected.clone();
        wrong_epoch.shard.epoch = wrong_epoch.shard.epoch.checked_add(1).unwrap();
        let mut wrong_fence = expected.clone();
        let fence = wrong_fence
            .shards
            .iter_mut()
            .find_map(|shard| shard.freeze.as_mut())
            .ok_or("actual initialized fixture has no retained completed fence")?;
        fence.activated_epoch = Some(expected.shard.epoch.checked_add(1).unwrap());
        for (label, bad) in [
            ("missing-root", missing),
            ("foreign-physical-node", foreign),
            ("incomplete-full-set", incomplete),
            ("conflicting-prefix", wrong_prefix),
            ("foreign-epoch", wrong_epoch),
            ("conflicting-retained-fence", wrong_fence),
        ] {
            let result = DfsTestRuntime::validate_retained_healthy_fixture(&bad);
            report["refusals"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({"case":label,"result":result}));
            assert!(result.is_err(), "unsupported preflight accepted {label}");
            assert_eq!(
                raw_pins.capture()?,
                raw,
                "preflight must not repair or change any retained byte"
            );
        }
        let valid = DfsTestRuntime::validate_retained_healthy_fixture(&expected);
        report["exact_preflight"] = serde_json::to_value(&valid)?;
        valid?;
        assert_eq!(raw_pins.capture()?, raw);
        let history_before = file_bytes_and_identity(&ready.history_path)?;
        let reopened = DfsTestRuntime::reopen_retained_healthy_fixture(expected.clone());
        if let Err(error) = &reopened {
            report["constructor_error"] = serde_json::json!(error);
        }
        let (native, startup) = reopened?;
        report["native_startup"] = serde_json::to_value(&startup)?;
        let verified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> TestResult<()> {
                startup?;
                assert_eq!(native.root_shard()?, expected.shard);
                for node in &expected.nodes {
                    let context = native.component_context(&node.node_id)?;
                    let actual = crate::runtime_host::require_daemon_dfs_data_owner_lock(
                        &crate::runtime_host::DfsRuntimePaths::new(native.data_dir(&node.node_id)),
                    )?;
                    assert_eq!(actual.storage_owner_file_identity()?, node.owner_identity);
                    drop(actual);
                    drop(context);
                }
                // Exact native prefix observations are newly collected; none is
                // reconstructed from manifest, raw bytes or the historical record.
                let round = capture_actual_round(&native, false)?;
                assert_ne!(round.round_id(), ready.history.round_id());
                assert_eq!(
                    round.common_source_observation().unwrap().prefix(),
                    &expected.prefix
                );
                let old_attempts = ready
                    .history
                    .peers()
                    .iter()
                    .map(|p| p.attempt_id())
                    .collect::<BTreeSet<_>>();
                assert!(
                    round
                        .peer_outcomes()
                        .iter()
                        .all(|p| !old_attempts.contains(p.native_return().unwrap().attempt_id()))
                );
                report["fresh_native_round"] = serde_json::json!({"round_id":round.round_id(),"complete":round.is_complete(),
                "target":round.target(),"prefix":round.common_source_observation().unwrap().prefix(),
                "peers":round.peer_outcomes().iter().map(|p|serde_json::json!({"peer":p.peer(),"attempt":p.native_return().unwrap().attempt_id(),"command":p.command(),"response":p.response()})).collect::<Vec<_>>()});
                let mut service = native.metadata_primary_service(SEED)?;
                let cold = service
                    .read_native_prefix_history(&shard_id)?
                    .ok_or("cold history vanished")?;
                assert_eq!(cold, ready.history);
                let duplicate =
                    service.retain_native_prefix_history(&round, Instant::now() + OP_CAP)?;
                assert_eq!(
                    duplicate.disposition(),
                    NativePrefixHistoryDisposition::ExistingHistoricalData
                );
                assert_eq!(duplicate.record(), &ready.history);
                assert_eq!(
                    file_bytes_and_identity(&ready.history_path)?,
                    history_before
                );
                report["historical_after_reopen"] = serde_json::json!({"same_first_record":true,"file_bytes_identity_unchanged":true,
                "disposition":format!("{:?}",duplicate.disposition()),"prior_caller_result":if cut==Cut::AfterPublication {"unknown: process killed before return; no new ACK"} else {"historical DATA success only; no serving authority"}});
                assert_eq!(native.observation_dropped_count(), 0);
                Ok(())
            },
        ));
        let closed = native.close();
        report["normal_reconstructed_close"] = serde_json::to_value(&closed)?;
        if let Err(panic) = verified {
            std::panic::resume_unwind(panic);
        }
        verified.unwrap()?;
        closed?;
        Ok(())
    }));
    report["control_outcome"] = match &checked {
        Ok(Ok(())) => serde_json::json!("completed"),
        Ok(Err(e)) => serde_json::json!(e.to_string()),
        Err(_) => serde_json::json!("panic; original assertion retained"),
    };
    let saved = save_report(
        &root.join("retained-healthy-full3-process-report.json"),
        &report,
    );
    if !matches!(&checked, Ok(Ok(()))) || saved.is_err() {
        owned.keep();
    }
    if let Err(panic) = checked {
        std::panic::resume_unwind(panic);
    }
    checked.unwrap().unwrap();
    saved.unwrap();
}
#[test]
fn retained_healthy_full3_survives_owned_sigkill_with_fresh_native_proof() {
    run_healthy(Cut::Returned);
}
#[test]
fn retained_healthy_full3_post_sync_sigkill_preserves_unknown_outcome_without_reack() {
    run_healthy(Cut::AfterPublication);
}

#[test]
#[ignore = "owned healthy full3 process helper; invoked only by exact parent"]
fn retained_healthy_full3_process_child() -> TestResult<()> {
    let child_started = Instant::now();
    let input_path =
        PathBuf::from(std::env::var_os(ENV).ok_or("explicit healthy parent input required")?);
    let input: HealthyInput = read_data(&input_path)?;
    if input_path != input.owned_root.join("input.sobj")
        || std::env::var_os("TMPDIR").map(PathBuf::from) != Some(input.owned_root.join("child-tmp"))
        || input.invocation
            != format!(
                "{}:{:?}",
                input
                    .owned_root
                    .file_name()
                    .ok_or("owned root name missing")?
                    .to_string_lossy(),
                input.cut
            )
    {
        return Err("healthy input paths/invocation differ".into());
    }
    DirectoryId::read(&input.owned_root)?;
    let (native, _) = DfsTestRuntime::start_cluster("retained-healthy-full3", 3)?;
    let shard = native.root_shard()?;
    let mut main = native.client(&shard.primary_node_id)?;
    let deadline = Instant::now() + OP_CAP;
    let old = main.set_transport_operation_deadline(Some(deadline));
    let seeded = main.posix_mkdir(SEED);
    main.set_transport_operation_deadline(old);
    seeded?;
    assert!(Instant::now() < deadline);
    drop(main);
    let round = capture_actual_round(&native, true)?;
    let manifest = native.capture_retained_healthy_fixture(&round)?;
    let history_path = crate::local_dfs_metadata_shard_path(
        native.data_dir(&shard.primary_node_id),
        &DfsMetadataShardId::new(&shard.shard_id),
    )
    .join(NATIVE_PREFIX_HISTORY_FILE);
    assert!(!history_path.exists());
    let mut service = native.metadata_primary_service(SEED)?;
    let intake_started = Instant::now();
    let intake_deadline = intake_started + OP_CAP;
    let elapsed = u64::try_from(intake_started.duration_since(child_started).as_nanos())?;
    if input.cut == Cut::AfterPublication {
        let ready_path = input.owned_root.join("ready.sobj");
        let expected_path = history_path.clone();
        let result = with_after_publication_cut_for_test(
            move |path, deadline| {
                assert_eq!(path, expected_path);
                assert_eq!(deadline, intake_deadline);
                assert!(Instant::now() < deadline);
                let bytes = fs::read(path).expect("actual synced historical bytes");
                let history = sinomo_core::object::serde_ext::from_bytes(&bytes)
                    .expect("actual synced historical DATA");
                let ready = HealthyReady {
                    pid: std::process::id(),
                    invocation: input.invocation,
                    cut: input.cut,
                    manifest,
                    history_path: expected_path,
                    history,
                    history_bytes: bytes,
                    intake_elapsed_nanos: elapsed,
                    intake_returned: false,
                };
                write_data(&ready_path, &ready).expect("owned Ready preservation");
                loop {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    std::thread::sleep(remaining);
                }
            },
            || service.retain_native_prefix_history(&round, intake_deadline),
        );
        drop(service);
        let closed = native.close();
        return Err(format!("parent did not kill inside original post-publication deadline: result={result:?},close={closed:?}").into());
    }
    let result = service.retain_native_prefix_history(&round, intake_deadline)?;
    assert_eq!(
        result.disposition(),
        NativePrefixHistoryDisposition::WrittenAndSynced
    );
    let ready = HealthyReady {
        pid: std::process::id(),
        invocation: input.invocation,
        cut: input.cut,
        manifest,
        history_bytes: fs::read(&history_path)?,
        history_path,
        history: result.record().clone(),
        intake_elapsed_nanos: elapsed,
        intake_returned: true,
    };
    drop(service);
    write_data(&input.owned_root.join("ready.sobj"), &ready)?;
    let mut byte = [0u8];
    let waited = io::stdin().read_exact(&mut byte);
    std::hint::black_box((&native, &round));
    let closed = native.close();
    Err(
        format!("parent did not kill owned healthy fixture: stdin={waited:?},close={closed:?}")
            .into(),
    )
}
