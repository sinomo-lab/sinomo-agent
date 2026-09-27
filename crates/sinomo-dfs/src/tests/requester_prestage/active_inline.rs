//! Actual public AwaitCommit dispatch and canonical completion on active storage.

use super::*;
use crate::runtime_host::{DfsDataOwnerLock, DfsRuntimePaths};
use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext as sobj};
use sinomo_core::storage::checksummed_frame::{FrameLimits, FrameRead, read_frame};
use sinomo_core::storage::durable_file::{self, DurableAtomicFileOptions};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompletionBoundary {
    Unchanged,
    CatalogAdvanced,
    UnknownLegacyCopy,
}

#[derive(Deserialize)]
struct ObservedRecord {
    operation: ObservedOperation,
}

#[derive(Deserialize)]
enum ObservedOperation {
    Bind {
        manifest: ContentManifest,
        reservation: Option<ContentStoreReservationRecord>,
    },
    Link {
        manifest: ContentManifest,
        reservation: Option<ContentStoreReservationRecord>,
    },
    Reserve(ContentStoreReservationRecord),
}

#[derive(Debug)]
struct ObservedBinding {
    manifest: Option<ContentManifest>,
    reservation: ContentStoreReservationRecord,
    payload_bytes: u64,
    full_bind: bool,
}

/// Inspect the actual checksummed files, independently of the resident map.
/// No process-global counter or production observation hook is needed.
fn inline_journal(root: &Path) -> (BTreeMap<PathBuf, Vec<u8>>, Vec<ObservedBinding>) {
    let generations = root.join("inline-bindings/generations");
    assert!(root.join("inline-bindings/current.sobj").is_file());
    let mut images = BTreeMap::new();
    for generation in std::fs::read_dir(generations).unwrap() {
        for entry in std::fs::read_dir(generation.unwrap().path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("slog") {
                images.insert(path.clone(), std::fs::read(path).unwrap());
            }
        }
    }
    let mut bindings = Vec::new();
    for (path, bytes) in &images {
        let mut reader = std::io::Cursor::new(bytes);
        loop {
            match read_frame(
                &mut reader,
                path,
                FrameLimits::new(64 * 1024, 16 * 1024).unwrap(),
            )
            .unwrap()
            {
                FrameRead::Complete(frame) => {
                    let record: ObservedRecord = sobj::from_bytes(frame.header()).unwrap();
                    let (manifest, reservation, full_bind) = match record.operation {
                        ObservedOperation::Bind {
                            manifest,
                            reservation,
                        } => (Some(manifest), reservation, true),
                        ObservedOperation::Link {
                            manifest,
                            reservation,
                        } => (Some(manifest), reservation, false),
                        ObservedOperation::Reserve(reservation) => (None, Some(reservation), false),
                    };
                    bindings.push(ObservedBinding {
                        manifest,
                        reservation: reservation.expect("nonempty write retains its exact root"),
                        payload_bytes: frame.payload_bytes(),
                        full_bind,
                    });
                }
                FrameRead::CleanEof { .. } => break,
                other => panic!("public write left an incomplete inline frame: {other:?}"),
            }
        }
    }
    (images, bindings)
}

/// Install one valid old-format physical reservation alongside the inline root.
/// The requester is waiting for its primary; take the same owner -> reservation
/// flocks so this fixture cannot race staging or completion.
fn add_unknown_legacy_copy(root: &Path, identity: &ContentStoreReservationIdentity) {
    #[derive(Serialize)]
    struct LegacyReservation<'a> {
        format_version: u32,
        identity: &'a ContentStoreReservationIdentity,
        reserved_at_unix_nanos: u64,
    }
    let lock = |name: &str| {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join(name))
            .unwrap();
        fs2::FileExt::lock_exclusive(&file).unwrap();
        file
    };
    let _owner = lock("content-owner.lock");
    let _reservation = lock("content-reservations/write.lock");
    let mut digest = blake3::Hasher::new();
    digest.update(b"sinomo-content-identity-v1");
    for field in [
        identity.object_id.as_bytes(),
        identity.version.as_bytes(),
        identity.content_hash.as_bytes(),
    ] {
        digest.update(&(field.len() as u64).to_le_bytes());
        digest.update(field);
    }
    let digest = digest.finalize().to_hex().to_string();
    let path = root
        .join("content-reservations")
        .join(&digest[..2])
        .join(format!("{digest}.sobj"));
    let bytes = sobj::to_bytes(
        &LegacyReservation {
            format_version: 1,
            identity,
            reserved_at_unix_nanos: u64::MAX - 1,
        },
        1,
        LengthMode::U32,
    )
    .unwrap();
    durable_file::write_file_create_new_atomic(
        &path,
        &bytes,
        DurableAtomicFileOptions::new("reservation", "test-legacy-copy").sync_all(),
    )
    .unwrap();
    durable_file::sync_directory(&root.join("content-reservations")).unwrap();
}

fn public_inline_create(boundary: CompletionBoundary) {
    let root = temp_root(&format!("requester-inline-public-{boundary:?}"));
    let data_dir = SinomoDataDir::new(root.join("requester-runtime"));
    let mut owner = DfsDataOwnerLock::acquire(&DfsRuntimePaths::new(&data_dir)).unwrap();
    owner.activate_content_bindings_before_routes().unwrap();
    let content_root = data_dir.dfs_dir().join("content");
    let mut fixture = prestage_fixture_with_requester_content(root.path(), content_root.clone());
    let payload = vec![0x8b; 4096];
    let mut policy = DfsReplicationPolicy::new(1, 1).unwrap();
    policy.required_node_ids.push(fixture.requester_id.clone());
    policy.forbidden_node_ids.push(fixture.primary_id.clone());
    let candidates = vec![fixture.requester_id.clone(), fixture.primary_id.clone()];
    let mut staged_image = None;
    let mut captured = None;
    let mut committed_response = false;
    let stat = std::thread::scope(|scope| {
        let requester = &mut fixture.requester;
        let bytes = payload.as_slice();
        let writer = scope.spawn(move || {
            requester.posix_write_file(
                "/public-inline.bin",
                bytes,
                DfsWriteOptions {
                    policy,
                    replica_candidates: candidates,
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while !writer.is_finished() {
            let messages = fixture.primary.transport.drain_incoming_messages().unwrap();
            for message in &messages {
                if let DfsNetworkMessage::DistributedWriteRequest(command) = &message.message {
                    assert!(captured.is_none(), "healthy public write dispatches once");
                    assert!(command.publication.is_current());
                    assert!(command.requester_blob_pre_staged && command.bytes.is_empty());
                    assert_eq!(
                        command.content_hash.as_deref(),
                        Some(content_hash(bytes).as_str())
                    );
                    assert_eq!(command.content_size_bytes, Some(4096));
                    assert_durable_binding(
                        &content_root,
                        &command.request.object_id,
                        &command.request.target_version,
                        bytes,
                    );
                    assert!(
                        !content_root
                            .join("objects")
                            .join(command.request.object_id.as_str())
                            .join(command.request.target_version.as_str())
                            .join("manifest.sobj")
                            .exists(),
                        "this regression must exercise inline, not legacy manifests"
                    );
                    let (image, bindings) = inline_journal(&content_root);
                    let selected = bindings
                        .iter()
                        .filter(|binding| {
                            binding.reservation.identity.object_id
                                == command.request.object_id.as_str()
                                && binding.reservation.identity.version
                                    == command.request.target_version.as_str()
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(selected.len(), 1);
                    let binding = selected[0];
                    assert!(binding.full_bind);
                    assert_eq!(binding.manifest.as_ref().unwrap().size_bytes, 4096);
                    assert_eq!(
                        binding.manifest.as_ref().unwrap().content_hash,
                        content_hash(bytes)
                    );
                    assert_eq!(binding.payload_bytes, 4096);
                    assert_eq!(
                        binding.reservation.identity.content_hash,
                        content_hash(bytes)
                    );
                    assert!(binding.reservation.causal_stamp.is_some());
                    match boundary {
                        CompletionBoundary::Unchanged => {}
                        CompletionBoundary::CatalogAdvanced => {
                            LocalContentStore::new(&content_root)
                                .unwrap()
                                .begin_content_root_catalog_scan()
                                .unwrap();
                        }
                        CompletionBoundary::UnknownLegacyCopy => {
                            add_unknown_legacy_copy(&content_root, &binding.reservation.identity);
                        }
                    }
                    staged_image = Some(image);
                    captured = Some(command.clone());
                }
            }
            let events = fixture
                .primary
                .process_routed_messages_with_options(messages, false)
                .unwrap();
            committed_response |= events.iter().any(|event| {
                matches!(
                    event,
                    NetworkedDfsServiceEvent::RemoteWriteServed {
                        result: DfsDistributedWriteResult::Outcome {
                            outcome: NetworkedDfsWriteOutcome::Committed { .. }
                        },
                        ..
                    }
                )
            });
            assert!(
                Instant::now() < deadline,
                "public AwaitCommit did not finish"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        writer
            .join()
            .unwrap()
            .expect("public AwaitCommit must return committed stat")
    });
    let command = captured.expect("manifest-only public dispatch observed");
    assert!(committed_response);
    assert_eq!(stat.object_id, command.request.object_id);
    assert_eq!(stat.version, command.request.target_version);
    assert_eq!(
        stat.content_hash.as_deref(),
        command.content_hash.as_deref()
    );
    let (after_image, bindings) = inline_journal(&content_root);
    let selected = bindings
        .iter()
        .filter(|binding| {
            binding.reservation.identity.object_id == stat.object_id.as_str()
                && binding.reservation.identity.version == stat.version.as_str()
        })
        .collect::<Vec<_>>();
    if boundary == CompletionBoundary::Unchanged {
        assert_eq!(
            after_image,
            staged_image.unwrap(),
            "validated Current completion must not append another binding/reservation"
        );
        assert_eq!(selected.len(), 1);
    } else {
        assert_ne!(after_image, staged_image.unwrap());
        assert_eq!(
            selected.len(),
            2,
            "unproven epoch/provenance must retain generic renewal"
        );
        assert!(
            !selected[1].full_bind,
            "renewal reuses the exact inline payload"
        );
        assert_eq!(selected[1].payload_bytes, 0);
        assert!(
            selected[1].manifest.is_none(),
            "generic completion renews the reservation"
        );
        if boundary == CompletionBoundary::CatalogAdvanced {
            assert_ne!(
                selected[0].reservation.causal_stamp,
                selected[1].reservation.causal_stamp
            );
        }
    }
    assert_durable_binding(&content_root, &stat.object_id, &stat.version, &payload);
    assert!(
        !fixture
            .primary
            .content
            .has_blob(&content_hash(&payload))
            .unwrap()
    );
    assert!(
        fixture
            .primary_sent
            .lock()
            .unwrap()
            .iter()
            .all(|message| !matches!(
                message,
                DfsNetworkMessage::StageBlob(_) | DfsNetworkMessage::FetchBlob(_)
            )),
        "canonical requester binding must need no primary content roundtrip"
    );
    drop(fixture);
    drop(owner);
}

#[test]
fn public_await_commit_inline_binding_skips_completion_renewal() {
    public_inline_create(CompletionBoundary::Unchanged);
}

#[test]
fn public_await_commit_inline_binding_renews_after_catalog_cut() {
    public_inline_create(CompletionBoundary::CatalogAdvanced);
}

#[test]
fn public_await_commit_inline_binding_renews_with_unknown_legacy_copy() {
    public_inline_create(CompletionBoundary::UnknownLegacyCopy);
}
