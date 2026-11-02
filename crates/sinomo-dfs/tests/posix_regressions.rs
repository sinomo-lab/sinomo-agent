use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsNodeTransport, DfsWriteOptions, NetworkedDfsService, test_support::DfsTestRuntime,
};
use std::path::Path;

fn service(
    _root: &Path,
    owner: &NodeId,
) -> (DfsTestRuntime, NetworkedDfsService<DfsNodeTransport>) {
    let mut runtime = DfsTestRuntime::new("native-posix-regression", owner.clone()).unwrap();
    runtime.start_nodes().unwrap();
    let service = runtime.client(owner).unwrap();
    (runtime, service)
}

#[test]
fn hardlink_at_old_rename_path_preserves_renamed_file() {
    let root = sinomo_testdir::TestDir::new("dfs-review-hardlink");
    let owner = NodeId::from_legacy_named("review-owner").unwrap();
    let (_runtime, mut dfs) = service(&root, &owner);
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    dfs.posix_write_file("/services/a", b"original", options.clone())
        .unwrap();
    dfs.posix_write_file("/services/c", b"different", options)
        .unwrap();
    dfs.posix_rename("/services/a", "/services/b").unwrap();
    assert_eq!(dfs.posix_read_file("/services/b").unwrap(), b"original");
    dfs.posix_hardlink("/services/c", "/services/a").unwrap();
    assert_eq!(
        dfs.posix_read_file("/services/b")
            .expect("renamed file must survive"),
        b"original"
    );
}

#[test]
fn local_full_read_leaves_content_verification_to_scrub() {
    let root = sinomo_testdir::TestDir::new("dfs-review-corruption");
    let owner = NodeId::from_legacy_named("review-owner").unwrap();
    let (_runtime, mut dfs) = service(&root, &owner);
    let original = vec![b'a'; 1024 * 1024];
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    dfs.posix_write_file("/services/content", &original, options.clone())
        .unwrap();
    let stat = dfs.posix_stat("/services/content").unwrap();
    let hash = stat
        .content_hash
        .as_ref()
        .unwrap()
        .strip_prefix("blake3:")
        .unwrap();
    let blob_path = _runtime
        .data_dir(&owner)
        .dfs_dir()
        .join("content")
        .join("blobs")
        .join(&hash[..2])
        .join(hash);
    assert!(blob_path.is_file(), "blob missing: {blob_path:?}");
    dfs.posix_write_file_at("/services/content", 0, b"b", options)
        .unwrap();
    assert_eq!(dfs.posix_read_file("/services/content").unwrap()[1], b'a');
    std::fs::write(&blob_path, vec![b'z'; original.len()]).unwrap();
    let result = dfs.posix_read_file("/services/content");
    let bytes = result.expect("ordinary reads do not rehash stored content");
    assert_eq!(bytes[1], b'z');
    assert!(
        dfs.content()
            .scrub()
            .unwrap()
            .issues
            .iter()
            .any(|issue| matches!(
                issue,
                sinomo_dfs::ContentStoreScrubIssue::CorruptBlob { .. }
            ))
    );
}

#[test]
fn unlink_resolves_symlink_before_parent_component() {
    let root = sinomo_testdir::TestDir::new("dfs-review-dotdot");
    let owner = NodeId::from_legacy_named("review-owner").unwrap();
    let (_runtime, mut dfs) = service(&root, &owner);
    dfs.posix_mkdir_all("/services/real/sub").unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    dfs.posix_write_file("/services/victim", b"must survive", options.clone())
        .unwrap();
    dfs.posix_write_file("/services/real/victim", b"unlink me", options.clone())
        .unwrap();
    dfs.posix_symlink("/services/real/sub", "/services/link", options)
        .unwrap();
    dfs.posix_unlink("/services/link/../victim").unwrap();
    assert_eq!(
        dfs.posix_read_file("/services/victim")
            .expect("unrelated file must survive"),
        b"must survive"
    );
    assert!(dfs.posix_read_file("/services/real/victim").is_err());
}

#[test]
fn rename_can_revisit_a_previous_destination() {
    let root = sinomo_testdir::TestDir::new("dfs-review-rename-cycle");
    let owner = NodeId::from_legacy_named("review-owner").unwrap();
    let (_runtime, mut dfs) = service(&root, &owner);
    dfs.posix_write_file(
        "/services/a",
        b"original",
        DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .unwrap();
    dfs.posix_rename("/services/a", "/services/b").unwrap();
    dfs.posix_rename("/services/b", "/services/a").unwrap();
    dfs.posix_rename("/services/a", "/services/b")
        .expect("a new rename is not a replay of the first rename");
    assert_eq!(dfs.posix_read_file("/services/b").unwrap(), b"original");
}

#[test]
fn reads_and_mutations_walk_dot_components_after_symlinks() {
    let root = sinomo_testdir::TestDir::new("dfs-posix-dot-walk");
    let owner = NodeId::generate();
    let (_runtime, mut dfs) = service(&root, &owner);
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    dfs.posix_mkdir_all("/services/real/sub").unwrap();
    dfs.posix_symlink("real/sub", "/services/link", options.clone())
        .unwrap();
    dfs.posix_symlink("link/../data", "/services/indirect", options.clone())
        .unwrap();
    dfs.posix_write_file("/services/link/../data", b"correct", options.clone())
        .unwrap();
    for path in [
        "/services/link/../data",
        "/services/indirect",
        "/services/real/sub/../data",
    ] {
        assert_eq!(dfs.posix_read_file(path).unwrap(), b"correct");
        assert_eq!(dfs.posix_read_file_distributed(path).unwrap(), b"correct");
    }
    assert!(dfs.posix_stat("/services/data").is_err());
    dfs.posix_mkdir_all("/services/link/../nested/child")
        .unwrap();
    assert!(dfs.posix_stat("/services/real/nested/child").is_ok());
    dfs.posix_rename("/services/link/../data", "/services/link/../moved")
        .unwrap();
    dfs.posix_hardlink("/services/link/../moved", "/services/link/../alias")
        .unwrap();
    assert_eq!(
        dfs.posix_read_file("/services/real/alias").unwrap(),
        b"correct"
    );
    dfs.posix_unlink("/services/link/../alias").unwrap();
    dfs.posix_rmdir("/services/link/../nested/child").unwrap();
    assert!(dfs.posix_stat("/services/real/nested/child").is_err());
    for path in ["/services/real/moved/../bad", "/services/missing/../bad"] {
        assert!(
            dfs.posix_write_file(path, b"wrong", options.clone())
                .is_err()
        );
    }
    assert!(dfs.posix_stat("/services/bad").is_err());
}

#[test]
fn directory_rename_can_revisit_a_previous_destination_after_reopen() {
    let root = sinomo_testdir::TestDir::new("dfs-directory-rename-cycle");
    let owner = NodeId::generate();
    let (mut runtime, mut dfs) = service(&root, &owner);
    dfs.posix_mkdir_all("/services/a/child").unwrap();
    dfs.posix_rename("/services/a", "/services/b").unwrap();
    dfs.posix_rename("/services/b", "/services/a").unwrap();
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    dfs.posix_rename("/services/a", "/services/b").unwrap();
    assert!(dfs.posix_stat("/services/b/child").is_ok());
    assert!(dfs.posix_stat("/services/a").is_err());
}

#[test]
fn main_sifs_no_links_open_preserves_identity_counts_and_link_rejection() {
    use sinomo_dfs::{
        DfsPosixError, open_mounted_sifs_service_with_peer_transports_and_default_control_plane,
    };
    let nodes = DfsTestRuntime::ordered_node_ids("native-no-links-authority-stat", 3);
    let mut runtime =
        DfsTestRuntime::new("native-no-links-authority-stat", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let root = "/users/no-links-authority-stat";
    let file = format!("{root}/data.bin");
    let hardlink = format!("{root}/alias.bin");
    let symlink = format!("{root}/symbolic");
    let mut writer = runtime.client(&nodes[0]).unwrap();
    writer.posix_mkdir_all(root).unwrap();
    writer.posix_mkdir(&format!("{root}/first")).unwrap();
    writer.posix_mkdir(&format!("{root}/second")).unwrap();
    let options = DfsWriteOptions::replicated_eventual(nodes.clone());
    writer
        .posix_write_file(&file, b"actual no-link bytes", options.clone())
        .unwrap();
    writer.posix_hardlink(&file, &hardlink).unwrap();
    writer.posix_symlink(root, &symlink, options).unwrap();
    let expected_dir = writer.posix_stat_distributed(root).unwrap();
    let expected_file = writer.posix_stat_distributed(&file).unwrap();
    assert_eq!(expected_dir.link_count, 4);
    assert_eq!(expected_file.link_count, 2);
    drop(writer);

    let (sync, asynchronous) = runtime.peer_transports(&nodes[1]).unwrap();
    let mut opened = open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
        runtime.data_dir(&nodes[1]).clone(),
        sync,
        asynchronous,
    )
    .unwrap();
    let actual_dir = opened
        .service
        .posix_open_beneath_no_links_distributed("/users", root)
        .unwrap();
    assert_eq!(actual_dir, expected_dir);
    let actual_file = opened
        .service
        .posix_open_beneath_no_links_distributed("/users", &file)
        .unwrap();
    assert_eq!(actual_file, expected_file);
    for path in [&hardlink, &symlink] {
        assert!(matches!(
            opened
                .service
                .posix_open_beneath_no_links_distributed("/users", path),
            Err(DfsPosixError::InvalidPath { .. })
        ));
    }
    assert!(matches!(
        opened
            .service
            .posix_open_beneath_no_links_distributed(root, "/services"),
        Err(DfsPosixError::InvalidPath { .. })
    ));
    drop(opened);
    runtime.close().unwrap();
}
