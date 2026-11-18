//! A member that restarts between staging its shard's converted image and
//! that image's activation.
use super::*;

/// Services has no storage record, so its current-format copies are old
/// storage that migration converts per shard. The primary installs the image
/// on both members, and member 2 stops just before the activation commits
/// (node 0, the cluster's KV voter, keeps running). The primary activates;
/// once member 2 is back, its unit for the staged generation activates the
/// image it staged before the restart, and it serves the shard.
#[test]
fn native_a_member_restarted_after_staging_activates_its_staged_image() {
    let mut cluster = start(
        "shard-conversion-member-restart",
        3,
        &[
            ("root", 0, &[1, 2]),
            ("services", 1, &[0, 2]),
            ("users", 2, &[0, 1]),
        ],
    );
    write_namespace(&cluster);
    hold(&mut cluster, &["services"]);
    let fs = cluster.fs.clone();
    let owners = owners(&cluster);
    crate::authority::native_fixtures::remove_storage_record(&owners[0], &fs, "services");
    assert!(
        header(&owners[1], "services").is_some_and(|header| matches!(
            header.state,
            sinomo_dfs::DfsMetadataGenerationState::Active { .. }
        )),
        "the record-less copy is current format"
    );
    let staged = RefCell::new(None::<DfsMetadataShardConversion>);
    let unit = metadata_unit(&owners[1], "services");
    let member = cluster.nodes[2].clone();
    let native = &mut cluster.native;
    let env = BeforeActivation {
        inner: OwnerConversionEnv::new(
            &owners[1],
            &unit,
            NativeContributionSource::new(Arc::clone(&owners[1].authority)),
        ),
        before_activation: RefCell::new(Some(Box::new(|| {
            let attempt = storage(&owners[1], &fs, "services")
                .unwrap()
                .converting_attempt()
                .unwrap()
                .clone();
            assert!(attempt.sealed().is_some());
            let stage = stage_root(&owners[2], &fs, "services", &attempt.attempt_id);
            assert!(
                stage.join("staged.json").is_file(),
                "member 2 staged the attempt"
            );
            assert!(stage.join("image/metadata-generation.sobj").is_file());
            native.stop_node(&member).unwrap();
            *staged.borrow_mut() = Some(attempt);
        }))),
        activations: RefCell::new(Vec::new()),
    };
    assert_eq!(settle(&env, &fs, "services"), ConversionProgress::Ready);
    let outcomes = env
        .activations
        .borrow()
        .iter()
        .map(|(_, outcome)| *outcome)
        .collect::<Vec<_>>();
    drop(env);
    assert_eq!(outcomes, vec![Submitted::Committed]);
    let attempt = staged
        .into_inner()
        .expect("member 2 stopped before the activation");
    drop(owners);

    let failure = cluster
        .native
        .reopen_node_with_startup_action(cluster.nodes[2].clone(), Box::new(|_| Err(HELD.into())))
        .unwrap_err();
    assert!(failure.contains(HELD), "{failure}");
    let owners = super::owners(&cluster);
    let config = shard_config(&owners[1], &fs, "services");
    let record = await_activated(&cluster, &owners, "services", &configured(&config));
    assert_eq!(
        record.converted_activation().unwrap().0.attempt_id,
        attempt.attempt_id
    );
    await_unstaged(&cluster, &owners, "services", &attempt.attempt_id);
    release(&cluster, &owners, "services");
    for reader in 0..cluster.nodes.len() {
        await_serving(&cluster, reader, &config, "/services/file");
    }
    write_beside_closed(&cluster, 0, "/services/written-after-member-restart");
    drop(owners);
    cluster.native.close().unwrap();
}
