use sinomo_api::{DFS_NAMESPACE_CHANGED_EVENT_KIND, DaemonEventSubscribeRequest};

#[test]
fn root_subscription_uses_legacy_all_paths_wire_filter() {
    let request = DaemonEventSubscribeRequest::for_path_prefix("/");
    let wire = serde_json::to_value(&request).unwrap();
    assert_eq!(
        wire["event_kind_prefixes"],
        serde_json::json!([DFS_NAMESPACE_CHANGED_EVENT_KIND])
    );
    assert!(
        wire.get("path_prefixes").is_none(),
        "global metadata-committed invalidations have no paths and must reach root subscribers"
    );
    let decoded: DaemonEventSubscribeRequest = serde_json::from_value(wire).unwrap();
    assert!(decoded.path_prefixes.is_empty());
}

#[test]
fn subtree_subscription_retains_exact_path_filter() {
    let request = DaemonEventSubscribeRequest::for_path_prefix("/services");
    assert_eq!(
        serde_json::to_value(request).unwrap()["path_prefixes"],
        serde_json::json!(["/services"])
    );
}

#[test]
fn root_dfs_subscription_includes_global_placement_and_namespace_events() {
    let request = DaemonEventSubscribeRequest::for_dfs_path_prefix("/");
    assert_eq!(request.event_kind_prefixes, ["dfs."]);
    assert!(request.path_prefixes.is_empty());
}

#[test]
fn configuration_subscription_receives_only_configuration_publications() {
    let request = DaemonEventSubscribeRequest::for_dfs_configuration();
    assert_eq!(
        request.event_kind_prefixes,
        [sinomo_api::DFS_CONFIGURATION_PUBLISHED_EVENT_KIND]
    );
    assert!(
        request.path_prefixes.is_empty(),
        "configuration announcements carry no namespace paths"
    );
}

#[test]
fn configuration_publication_order_is_per_generation_and_revision() {
    use sinomo_api::DfsConfigurationPublication;
    use sinomo_core::kv::KvRevision;
    let held = DfsConfigurationPublication {
        generation: 4,
        projection_generation: 7,
        storage_generation: 2,
        revision: Some(KvRevision(30)),
    };
    assert!(!held.advances_past(&held));
    for advanced in [
        DfsConfigurationPublication {
            generation: 5,
            ..held
        },
        DfsConfigurationPublication {
            projection_generation: 8,
            ..held
        },
        DfsConfigurationPublication {
            storage_generation: 3,
            ..held
        },
        DfsConfigurationPublication {
            revision: Some(KvRevision(31)),
            ..held
        },
    ] {
        assert!(advanced.advances_past(&held), "{advanced:?}");
    }
    let withdrawn = DfsConfigurationPublication {
        revision: None,
        ..held
    };
    assert!(!withdrawn.advances_past(&held));
    assert!(held.advances_past(&withdrawn));
    let wire = serde_json::to_value(held).unwrap();
    assert_eq!(
        serde_json::from_value::<DfsConfigurationPublication>(wire).unwrap(),
        held
    );
}
