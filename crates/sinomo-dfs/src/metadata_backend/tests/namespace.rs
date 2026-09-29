//! Namespace regression tests.

use super::*;

#[test]
fn subtree_pages_seek_only_the_requested_scope() {
    use crate::metadata_pages::{OBJECT_PAGE_PATH_VISITS, objects_for_subtree_page_from_tables};
    let mut tables = DfsMetadataTables::default();
    let paths = (0..1024)
        .map(|i| format!("/a/{i:04}"))
        .chain((0..1024).map(|i| format!("/work-backup/{i:04}")))
        .chain((0..1024).map(|i| format!("/z/{i:04}")))
        .chain([
            "/work".into(),
            "/work/child".into(),
            "/work/deep/item".into(),
        ]);
    for path in paths {
        let mut record = object(version("subtree-range-version"));
        record.object_id = DfsObjectId::from_legacy_named(path.replace('/', "_")).unwrap();
        record.path = path;
        for operation in crate::metadata_object_index::object_index_table_ops_with_previous(
            &tables, &record, None,
        ) {
            tables.apply(operation);
        }
    }
    for prefix in ["/work", "work", "/work/"] {
        OBJECT_PAGE_PATH_VISITS.with(|count| count.set(0));
        let mut cursor = None;
        let mut selected = Vec::new();
        loop {
            let page = objects_for_subtree_page_from_tables(
                &tables,
                &file_system_id(),
                prefix,
                cursor.as_ref(),
                1,
            );
            selected.extend(page.objects.into_iter().map(|object| object.path));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(selected, ["/work", "/work/child", "/work/deep/item"]);
        assert_eq!(
            OBJECT_PAGE_PATH_VISITS.with(|count| count.get()),
            5,
            "subtree pages must not visit preceding, similarly named or following subtrees"
        );
    }
}

#[test]
fn subtree_pages_preserve_nested_shard_continuations() {
    let root = temp_root("subtree-pages-nested-shards");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".into(),
                },
            ),
            shard_config(
                "private",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services/team/private".into(),
                },
            ),
        ],
    )
    .unwrap();
    backend.put_file_system(&file_system()).unwrap();
    let expected = BTreeSet::from([
        "/services/team",
        "/services/team/a",
        "/services/team/private/a",
    ]);
    for path in
        expected
            .iter()
            .copied()
            .chain(["/other", "/services/team-backup/a", "/services/zzz"])
    {
        let mut record = object(version("subtree-shards-version"));
        record.object_id = DfsObjectId::from_legacy_named(path.replace('/', "_")).unwrap();
        record.path = path.into();
        backend.put_object(&record).unwrap();
    }
    let mut cursor = None;
    let mut paths = BTreeSet::new();
    loop {
        let page = backend
            .objects_for_subtree_page(&file_system_id(), "/services/team", cursor.as_ref(), 1)
            .unwrap();
        for object in page.objects {
            assert!(paths.insert(object.path));
        }
        cursor = page.next_cursor;
        if let Some(cursor) = &cursor {
            assert!(cursor.shard_id.is_some());
        } else {
            break;
        }
    }
    assert_eq!(paths, expected.into_iter().map(str::to_string).collect());
    cleanup(&root);
}

#[test]
fn object_pages_resume_at_normalized_index_keys_with_bounded_work() {
    use crate::metadata_pages::{
        OBJECT_PAGE_PATH_VISITS, objects_for_file_system_page_from_tables,
    };
    const OBJECTS: usize = 1024;
    for leading_slash in [false, true] {
        let mut tables = DfsMetadataTables::default();
        for index in 0..OBJECTS {
            let mut record = object(version("page-range-version"));
            record.object_id =
                DfsObjectId::from_legacy_named(&format!("page-range-{index}")).unwrap();
            record.path = format!("{}scan/{index:04}", if leading_slash { "/" } else { "" });
            for operation in crate::metadata_object_index::object_index_table_ops_with_previous(
                &tables, &record, None,
            ) {
                tables.apply(operation);
            }
        }
        OBJECT_PAGE_PATH_VISITS.with(|count| count.set(0));
        let mut cursor = None;
        let mut ids = BTreeSet::new();
        loop {
            let page = objects_for_file_system_page_from_tables(
                &tables,
                &file_system_id(),
                cursor.as_ref(),
                1,
            );
            assert_eq!(page.objects.len(), 1);
            assert!(ids.insert(page.objects[0].object_id.clone()));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(ids.len(), OBJECTS);
        assert_eq!(
            OBJECT_PAGE_PATH_VISITS.with(|count| count.get()),
            2 * OBJECTS - 1,
            "each continuation reads only its row and one lookahead, not the earlier prefix"
        );
        // Both legacy object-spelled cursors and normalized cursor spellings
        // must seek the same index position, including a deleted boundary row.
        let first = objects_for_file_system_page_from_tables(&tables, &file_system_id(), None, 1);
        let mut after = first.next_cursor.unwrap();
        tables.objects.remove(&after.object_id);
        after.path = normalize_path(&after.path);
        OBJECT_PAGE_PATH_VISITS.with(|count| count.set(0));
        let resumed =
            objects_for_file_system_page_from_tables(&tables, &file_system_id(), Some(&after), 1);
        assert_eq!(normalize_path(&resumed.objects[0].path), "scan/0001");
        assert_eq!(OBJECT_PAGE_PATH_VISITS.with(|count| count.get()), 2);
    }
}

#[test]
fn sharded_backend_pages_objects_and_directory_children_with_shard_cursors() {
    let root = temp_root("sharded_backend_pages_with_shard_cursors");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");

    let mut root_file = object(version("version_1"));
    root_file.object_id =
        DfsObjectId::from_legacy_named("obj_sharded_page_root_file").expect("root file");
    root_file.path = "/root.txt".to_string();

    let mut users_dir = object(version("version_1"));
    users_dir.object_id =
        DfsObjectId::from_legacy_named("obj_sharded_page_users").expect("users dir");
    users_dir.path = "/users".to_string();
    users_dir.kind = DfsObjectKind::Directory;
    users_dir.content_hash = None;
    users_dir.content_size_bytes = None;

    let mut services_dir = object(version("version_1"));
    services_dir.object_id =
        DfsObjectId::from_legacy_named("obj_sharded_page_services").expect("services dir");
    services_dir.path = "/services".to_string();
    services_dir.kind = DfsObjectKind::Directory;
    services_dir.content_hash = None;
    services_dir.content_size_bytes = None;

    let mut service_file = object(version("version_1"));
    service_file.object_id =
        DfsObjectId::from_legacy_named("obj_sharded_page_service_file").expect("service file");
    service_file.path = "/services/app.txt".to_string();

    for object in [&root_file, &users_dir, &services_dir, &service_file] {
        backend.put_object(object).expect("object");
    }

    let mut object_cursor = None;
    let mut paged_object_ids = BTreeSet::new();
    let mut saw_object_shard_cursor = false;
    loop {
        let page = backend
            .objects_for_file_system_page(&file_system.file_system_id, object_cursor.as_ref(), 2)
            .expect("object page");
        paged_object_ids.extend(page.objects.into_iter().map(|object| object.object_id));
        if page
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.shard_id.is_some())
        {
            saw_object_shard_cursor = true;
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        object_cursor = Some(next_cursor);
    }
    let full_object_ids = backend
        .objects_for_file_system(&file_system.file_system_id)
        .expect("full objects")
        .into_iter()
        .map(|object| object.object_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(paged_object_ids, full_object_ids);
    assert!(
        saw_object_shard_cursor,
        "sharded object pages should carry shard cursors for bounded continuation"
    );

    let mut dir_cursor = None;
    let mut root_child_names = Vec::new();
    let mut saw_directory_shard_cursor = false;
    loop {
        let page = backend
            .child_objects_for_directory_page(
                &file_system.file_system_id,
                "/",
                dir_cursor.as_ref(),
                1,
            )
            .expect("directory page");
        root_child_names.extend(page.objects.into_iter().map(|object| {
            object
                .path
                .rsplit('/')
                .next()
                .expect("child name")
                .to_string()
        }));
        if page
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.shard_id.is_some())
        {
            saw_directory_shard_cursor = true;
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        dir_cursor = Some(next_cursor);
    }
    root_child_names.sort();
    assert_eq!(root_child_names, vec!["root.txt", "services", "users"]);
    assert!(
        saw_directory_shard_cursor,
        "sharded directory pages should carry shard cursors for bounded continuation"
    );

    cleanup(&root);
}

#[test]
fn local_backend_lists_directory_children_from_dentry_index() {
    let root = temp_root("local_backend_lists_directory_children_from_dentry_index");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let mut root_dir = object(version("version_1"));
    root_dir.object_id = DfsObjectId::from_legacy_named("obj_root_dir").expect("object");
    root_dir.path = "/".to_string();
    root_dir.kind = DfsObjectKind::Directory;
    root_dir.content_hash = None;
    root_dir.content_size_bytes = None;
    let mut docs_dir = object(version("version_1"));
    docs_dir.object_id = DfsObjectId::from_legacy_named("obj_docs_dir").expect("object");
    docs_dir.path = "/docs".to_string();
    docs_dir.kind = DfsObjectKind::Directory;
    docs_dir.content_hash = None;
    docs_dir.content_size_bytes = None;
    let mut readme = object(version("version_1"));
    readme.path = "/docs/readme.md".to_string();
    let mut changelog = object(version("version_1"));
    changelog.object_id = DfsObjectId::from_legacy_named("obj_changelog").expect("object");
    changelog.path = "/docs/changelog.md".to_string();
    let mut nested = object(version("version_1"));
    nested.object_id = DfsObjectId::from_legacy_named("obj_setup").expect("object");
    nested.path = "/docs/guides/setup.md".to_string();
    let mut outside = object(version("version_1"));
    outside.object_id = DfsObjectId::from_legacy_named("obj_lib").expect("object");
    outside.path = "/src/lib.rs".to_string();
    for object in [&root_dir, &docs_dir, &readme, &changelog, &nested, &outside] {
        backend.put_object(object).expect("object");
    }
    let mut changelog_tombstone = changelog.clone();
    changelog_tombstone.kind = DfsObjectKind::Tombstone;
    changelog_tombstone.version = version("version_2");
    changelog_tombstone.content_hash = None;
    backend.put_object(&changelog_tombstone).expect("tombstone");

    let children = backend
        .child_objects_for_directory(&file_system_id(), "/docs")
        .expect("children");
    assert_eq!(
        children
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/docs/readme.md"]
    );

    let root_children = backend
        .child_objects_for_directory(&file_system_id(), "/")
        .expect("root children");
    assert_eq!(
        root_children
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/docs"]
    );

    let root_page = backend
        .child_objects_for_directory_page(&file_system_id(), "/", None, 10)
        .expect("root page");
    assert_eq!(
        root_page
            .objects
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/docs"]
    );

    cleanup(&root);
}

#[test]
fn local_backend_routes_new_large_directory_entries_to_hash_fragments() {
    let root = temp_root("local_backend_routes_new_large_directory_entries_to_hash_fragments");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let mut existing = object(version("version_1"));
    existing.path = "/docs/existing.md".to_string();
    backend.put_object(&existing).expect("existing object");
    let existing_record = table_path_record(&backend, &existing.file_system_id, &existing.path);
    assert_eq!(existing_record.dirfrag_id, DfsDirFragmentId::new("root"));

    let mut root_dirfrag = table_dirfrag(
        &backend,
        &existing_record.parent_inode_id,
        &DfsDirFragmentId::new("root"),
    );
    root_dirfrag.entry_count = root_dirfrag.split_after_count;
    // Test-only threshold setup is not a metadata mutation and therefore
    // must not invent a journal sequence without a matching prefix proof.
    backend.tables.apply(DfsMetadataTableOp::PutDirFragment {
        dirfrag: root_dirfrag,
    });

    let mut bucketed = object(version("version_1"));
    bucketed.object_id = DfsObjectId::from_legacy_named("obj_bucketed").expect("object");
    bucketed.path = "/docs/bucketed.md".to_string();
    let candidate_cursor = backend
        .indexed_directory_page_cursor_for_object(&bucketed)
        .expect("candidate dentry cursor");
    assert_ne!(
        candidate_cursor
            .dentry
            .as_ref()
            .expect("indexed candidate cursor")
            .dirfrag_id,
        DfsDirFragmentId::new("root"),
        "an overlay candidate must use the same bucket fragment as its future durable dentry"
    );
    backend.put_object(&bucketed).expect("bucketed object");
    assert_eq!(
        backend
            .indexed_directory_page_cursor_for_object(&bucketed)
            .expect("durable dentry cursor"),
        candidate_cursor,
        "settling an overlay must not change its pagination position"
    );

    let bucketed_record = table_path_record(&backend, &bucketed.file_system_id, &bucketed.path);
    assert_ne!(
        bucketed_record.dirfrag_id,
        DfsDirFragmentId::new("root"),
        "large directories should route new entries away from the root dentry fragment"
    );
    assert!(
        table_dentry_exists(&backend, &bucketed_record),
        "bucketed dentry record should be materialized in its selected fragment"
    );

    let children = backend
        .child_objects_for_directory(&file_system_id(), "/docs")
        .expect("children");
    assert_eq!(
        children
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/docs/bucketed.md", "/docs/existing.md"],
        "directory listing must merge root and bucket fragments"
    );

    cleanup(&root);
}

#[test]
fn local_backend_pages_filesystem_objects_and_directory_children() {
    let root = temp_root("local_backend_pages_objects_and_children");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let mut dir = object(version("version_1"));
    dir.object_id = DfsObjectId::from_legacy_named("obj_page_dir").expect("dir object");
    dir.path = "/paged".to_string();
    dir.kind = DfsObjectKind::Directory;
    dir.content_hash = None;
    dir.content_size_bytes = None;
    backend.put_object(&dir).expect("dir");

    for index in 0..5 {
        let mut file = object(version("version_1"));
        file.object_id =
            DfsObjectId::from_legacy_named(format!("obj_page_file_{index}")).expect("object");
        file.path = format!("/paged/file-{index}.txt");
        file.kind = DfsObjectKind::File;
        backend.put_object(&file).expect("file");
    }

    let first_object_page = backend
        .objects_for_file_system_page(&file_system_id(), None, 3)
        .expect("first object page");
    assert_eq!(first_object_page.objects.len(), 3);
    assert!(first_object_page.next_cursor.is_some());
    let second_object_page = backend
        .objects_for_file_system_page(&file_system_id(), first_object_page.next_cursor.as_ref(), 3)
        .expect("second object page");
    assert_eq!(second_object_page.objects.len(), 3);
    assert!(second_object_page.next_cursor.is_none());
    assert_eq!(
        backend
            .count_objects_for_file_system(&file_system_id())
            .expect("count objects"),
        6
    );

    let mut cursor = None;
    let mut child_names = Vec::new();
    let mut pages = 0usize;
    loop {
        let page = backend
            .child_objects_for_directory_page(&file_system_id(), "/paged", cursor.as_ref(), 2)
            .expect("child page");
        assert!(page.objects.len() <= 2);
        child_names.extend(
            page.objects
                .iter()
                .map(|object| object.path.rsplit('/').next().unwrap().to_string()),
        );
        pages = pages.saturating_add(1);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    child_names.sort();
    assert_eq!(
        child_names,
        vec![
            "file-0.txt",
            "file-1.txt",
            "file-2.txt",
            "file-3.txt",
            "file-4.txt"
        ]
    );
    assert_eq!(pages, 3);

    cleanup(&root);
}

#[test]
fn local_backend_removes_stale_path_and_dentry_indexes() {
    let root = temp_root("local_backend_removes_stale_path_and_dentry_indexes");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let mut readme = object(version("version_1"));
    readme.path = "/docs/readme.md".to_string();
    backend.put_object(&readme).expect("object");
    let old_path_record = path_record_for_object(&readme);
    assert!(table_path_exists(
        &backend,
        &file_system_id(),
        "/docs/readme.md"
    ));
    assert!(table_dentry_exists(&backend, &old_path_record));
    assert_eq!(
        table_dirfrag(
            &backend,
            &old_path_record.parent_inode_id,
            &DfsDirFragmentId::new("root")
        )
        .entry_count,
        1
    );

    readme.path = "/docs/renamed.md".to_string();
    readme.version = version("version_2");
    backend.put_object(&readme).expect("renamed object");
    assert!(
        !table_path_exists(&backend, &file_system_id(), "/docs/readme.md"),
        "renaming should remove the obsolete path index"
    );
    assert!(
        !table_dentry_exists(&backend, &old_path_record),
        "renaming should remove the obsolete directory entry"
    );
    assert!(table_path_exists(
        &backend,
        &file_system_id(),
        "/docs/renamed.md"
    ));
    let renamed_path_record = path_record_for_object(&readme);
    assert!(table_dentry_exists(&backend, &renamed_path_record));
    assert_eq!(
        backend
            .child_objects_for_directory(&file_system_id(), "/docs")
            .expect("children")
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/docs/renamed.md"]
    );
    assert_eq!(
        table_dirfrag(
            &backend,
            &old_path_record.parent_inode_id,
            &DfsDirFragmentId::new("root")
        )
        .entry_count,
        1
    );

    readme.kind = DfsObjectKind::Tombstone;
    readme.content_hash = None;
    readme.version = version("version_3");
    backend.put_object(&readme).expect("tombstone object");
    assert!(
        !table_dentry_exists(&backend, &renamed_path_record),
        "tombstoning should remove the visible directory entry"
    );
    assert!(
        table_path_exists(&backend, &file_system_id(), "/docs/renamed.md"),
        "tombstone path record should remain addressable"
    );
    assert!(
        backend
            .child_objects_for_directory(&file_system_id(), "/docs")
            .expect("children after tombstone")
            .is_empty()
    );
    assert_eq!(
        table_dirfrag(
            &backend,
            &old_path_record.parent_inode_id,
            &DfsDirFragmentId::new("root")
        )
        .entry_count,
        0
    );

    cleanup(&root);
}

#[test]
fn local_backend_removes_stale_path_index_when_object_moves_filesystems_without_renaming() {
    let root = temp_root("local_backend_removes_stale_path_index_when_object_moves_filesystems");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    let file_system = file_system();
    let other_file_system = DfsFileSystem {
        file_history: Default::default(),
        file_system_id: DfsFileSystemId::from_legacy_named("otherfs").expect("filesystem"),
        display_name: "other".to_string(),
        owner_node_id: node("node_owner"),
        subtree_policies: Vec::new(),
    };
    backend.put_file_system(&file_system).expect("filesystem");
    backend
        .put_file_system(&other_file_system)
        .expect("other filesystem");

    let mut object = object(version("version_1"));
    object.path = "/services/shared-name.txt".to_string();
    backend.put_object(&object).expect("object");
    assert!(table_path_exists(
        &backend,
        &file_system.file_system_id,
        &object.path
    ));

    object.file_system_id = other_file_system.file_system_id.clone();
    backend
        .put_object(&object)
        .expect("move filesystem without rename");

    assert!(
        !table_path_exists(&backend, &file_system.file_system_id, &object.path),
        "moving an object between filesystems must remove the old filesystem path index even when the path string is unchanged"
    );
    assert!(
        backend
            .get_object_by_path(&file_system.file_system_id, "/services/shared-name.txt")
            .expect("old filesystem lookup")
            .is_none()
    );
    assert_eq!(
        backend
            .get_object_by_path(
                &other_file_system.file_system_id,
                "/services/shared-name.txt"
            )
            .expect("new filesystem lookup")
            .expect("object")
            .object_id,
        object.object_id
    );

    cleanup(&root);
}
