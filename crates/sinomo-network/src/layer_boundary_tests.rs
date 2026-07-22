//! Compile-adjacent source checks for the internal network dependency graph.
//!
//! Rust modules in one crate can otherwise reach any sibling with `crate::`.
//! These tests make the architecture's forbidden reverse imports explicit.

fn assert_no_dependencies(module: &str, source: &str, forbidden: &[&str]) {
    for dependency in forbidden {
        assert!(
            !source.contains(dependency),
            "{module} must not depend on `{dependency}`"
        );
    }
}

#[test]
fn direct_link_does_not_import_higher_network_layers() {
    assert_no_dependencies(
        "direct_link",
        include_str!("direct_link.rs"),
        &[
            "crate::connectivity_snapshot",
            "crate::routing",
            "crate::object_rpc_stream",
            "crate::rpc",
            "crate::session",
        ],
    );
}

#[test]
fn connectivity_snapshot_does_not_import_routing_or_rpc() {
    assert_no_dependencies(
        "connectivity_snapshot",
        include_str!("connectivity_snapshot.rs"),
        &[
            "crate::routing",
            "crate::object_rpc_stream",
            "crate::rpc",
            "sinomo_service",
        ],
    );
}

#[test]
fn routing_does_not_import_sobj_rpc_or_service_catalogs() {
    for (module, source) in [
        ("routing", include_str!("routing.rs")),
        ("routed_stream_pool", include_str!("routed_stream_pool.rs")),
    ] {
        assert_no_dependencies(
            module,
            source,
            &[
                "crate::object_rpc_stream",
                "crate::rpc",
                "crate::object",
                "sinomo_object",
                "sinomo_service",
            ],
        );
        for forbidden_type in ["ObjectRpc", "SinomoObject", "ServiceCatalog"] {
            assert!(
                !source.contains(forbidden_type),
                "{module} must not mention application type `{forbidden_type}`"
            );
        }
    }
}

#[test]
fn object_rpc_stream_does_not_import_routing_or_connectivity() {
    let source = include_str!("object_rpc_stream.rs");
    assert_no_dependencies(
        "object_rpc_stream",
        source,
        &[
            "crate::routing",
            "crate::route",
            "crate::connectivity",
            "crate::directory",
            "crate::direct_link",
            "sinomo_service",
        ],
    );
    for forbidden_type in ["ClusterEndpointRecord", "CandidatePath", "RoutedConnector"] {
        assert!(
            !source.contains(forbidden_type),
            "object RPC must not mention routing type `{forbidden_type}`"
        );
    }
}
