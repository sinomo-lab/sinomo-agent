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
