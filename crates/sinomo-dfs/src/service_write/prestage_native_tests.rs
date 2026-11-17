use super::*;

#[test]
fn native_w1_prestaging_requires_a_policy_selected_requester() {
    for mode in ["tentative", "required", "forbidden"] {
        let name = format!("native-w1-prestage-{mode}");
        let path = format!("/services/w1-prestage-{mode}.bin");
        let (runtime, initial, shard) =
            crate::test_support::current_native_metadata_service(&name, 3, &path).unwrap();
        drop(initial);
        let primary = shard.primary_node_id.clone();
        let requester = shard.replica_node_ids[0].clone();
        let mut managed =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
        if mode == "required" {
            managed.required_node_ids.push(requester.clone());
        } else if mode == "forbidden" {
            managed.forbidden_node_ids.push(requester.clone());
        }
        runtime
            .apply_placement_policy(&primary, &path, DfsPlacementScope::Subtree, managed)
            .unwrap();
        let mut caller = runtime.client(&requester).unwrap();
        let (_, _, policy) = caller
            .managed_replication_policy_for_path(&path, DfsObjectPolicy::ReplicatedEventual, 3)
            .unwrap();
        assert_eq!(
            (policy.min_commit_replicas, policy.desired_replicas),
            (1, 2)
        );
        assert_eq!(
            policy.required_node_ids.contains(&requester),
            mode == "required"
        );
        assert_eq!(
            policy.forbidden_node_ids.contains(&requester),
            mode == "forbidden"
        );
        let capture = runtime.capture_served_work(&primary, 32).unwrap();
        for payload in [b"native create".as_slice(), b"native overwrite".as_slice()] {
            let stat = caller
                .posix_write_file(
                    &path,
                    payload,
                    DfsWriteOptions {
                        policy: policy.clone(),
                        replica_candidates: vec![
                            requester.clone(),
                            primary.clone(),
                            shard.replica_node_ids[1].clone(),
                        ],
                        wait_for_commit: true,
                        foreground_replica_repair: false,
                    },
                )
                .unwrap();
            assert_eq!(caller.posix_read_file_distributed(&path).unwrap(), payload);
            let records = capture.records();
            let command = records
                .iter()
                .rev()
                .find_map(|work| {
                    if work.command.routed_message.source_node_id != requester {
                        return None;
                    }
                    let DfsNetworkMessage::DistributedWriteRequest(command) =
                        &work.command.routed_message.message
                    else {
                        return None;
                    };
                    command
                        .object_metadata
                        .as_ref()
                        .filter(|object| object.path == path)
                        .map(|_| command)
                })
                .expect("actual authenticated primary write");
            assert_eq!(
                command.requester_blob_pre_staged,
                mode == "required",
                "tentative requester placement must not select manifest-only delivery"
            );
            let owner = runtime.metadata_primary_service(&path).unwrap();
            let version = owner
                .metadata_backend()
                .unwrap()
                .get_version(&stat.object_id, &stat.version)
                .unwrap()
                .unwrap();
            assert!(version.visible);
            let durability = version.content_durability.unwrap();
            assert_eq!(
                (
                    durability.policy.min_commit_replicas,
                    durability.policy.desired_replicas
                ),
                (1, 2)
            );
            if mode == "required" {
                assert!(durability.desired_replica_node_ids.contains(&requester));
            } else if mode == "forbidden" {
                assert!(!durability.desired_replica_node_ids.contains(&requester));
            }
        }
        drop(capture);
        drop(caller);
        runtime.close().unwrap();
    }
}
