use super::*;
use sinomo_runtime::node::LocalNodePermissions;
use sinomo_runtime::service::ServiceRegistry;

#[test]
fn the_media_service_owns_every_audio_route_it_advertises() {
    let registry = register_services(ServiceRegistry::builder()).build();

    for route in MEDIA_AUDIO_OBJECT_RPC_ROUTES {
        assert!(registry.owns(route), "{route} is advertised but unowned");
    }
    assert!(registry.owns(MEDIA_AUDIO_DEVICES_ROUTE));
    assert!(registry.owns(MEDIA_AUDIO_CAPTURE_ROUTE));
    assert!(registry.owns(MEDIA_AUDIO_PLAYBACK_ROUTE));
    // The category is what discovery publishes instances under, so it must stay stable.
    assert_eq!(
        registry.category_for_route(MEDIA_AUDIO_CAPTURE_ROUTE),
        Some(MEDIA_RPC_CATEGORY)
    );
}

#[test]
fn capture_streams_a_response_body_and_playback_streams_a_request_body() {
    let routes = MediaAudioService::new().route_categories();
    let capture = routes
        .iter()
        .find(|route| route.route == MEDIA_AUDIO_CAPTURE_ROUTE)
        .expect("capture route");
    let playback = routes
        .iter()
        .find(|route| route.route == MEDIA_AUDIO_PLAYBACK_ROUTE)
        .expect("playback route");

    // Capture sends media back, so its response body is the stream.
    assert_eq!(capture.response_body_mode, ServiceBodyMode::Required);
    // Playback sends media out, so its request body is the stream.
    assert_eq!(playback.request_body_mode, ServiceBodyMode::Required);
    assert_eq!(playback.response_body_mode, ServiceBodyMode::None);

    for route in [capture, playback] {
        assert_eq!(route.category, MEDIA_RPC_CATEGORY);
        assert_eq!(
            route.auth_scope.as_deref(),
            Some(MEDIA_AUDIO_ROUTE_AUTH_SCOPE)
        );
        assert!(!route.required_capabilities.is_empty());
    }
}

#[test]
fn the_devices_route_reads_without_streaming_either_body() {
    let routes = MediaAudioService::new().route_categories();
    let devices = routes
        .iter()
        .find(|route| route.route == MEDIA_AUDIO_DEVICES_ROUTE)
        .expect("devices route");

    // Listing is a plain request/response, unlike the two media streams.
    assert_eq!(devices.response_body_mode, ServiceBodyMode::None);
    assert_eq!(devices.category, MEDIA_RPC_CATEGORY);
    // Listing is cheap, so it is not held to the per-device stream limit.
    assert_eq!(devices.concurrency_limit, Some(8));
    assert!(
        devices
            .required_capabilities
            .iter()
            .any(|capability| capability.contains("devices"))
    );
}

#[test]
fn only_operations_that_suit_a_device_kind_are_offered() {
    assert_eq!(
        audio_device_operations("microphone"),
        [MEDIA_CAPTURE_OPERATION]
    );
    assert_eq!(
        audio_device_operations("speaker"),
        [MEDIA_PLAYBACK_OPERATION]
    );
    // A camera is not an audio device, so this service offers nothing for it.
    assert!(audio_device_operations("camera").is_empty());
}

#[test]
fn a_node_without_device_support_serves_no_devices() {
    let dir = sinomo_testdir::TestDir::new("media-audio-devices-listing");
    let mut permissions = LocalNodePermissions::new();
    permissions
        .expose_resource_operation(
            audio_resource_id("microphone", Some("usb-mic")),
            MEDIA_CAPTURE_OPERATION,
        )
        .expect("expose a microphone");
    sinomo_runtime::node::LocalNodePermissionStore::new(dir.path())
        .save(&permissions)
        .expect("save exposures");

    // Exposure alone does not conjure hardware: without device support this build enumerates
    // nothing, so a grant for an absent device is never advertised.
    if !cfg!(feature = "native-devices") {
        assert!(node_audio_devices().is_empty());
    }
}

#[test]
fn a_device_resource_id_names_the_selector_so_one_grant_is_not_every_device() {
    assert_eq!(audio_resource_id("microphone", None), "microphone.default");
    assert_eq!(
        audio_resource_id("microphone", Some("usb-mic")),
        "microphone.usb-mic"
    );
    assert_eq!(audio_resource_id("speaker", Some("  ")), "speaker.default");
    // Selectors that resource identifiers cannot carry are reduced, not rejected silently.
    assert_eq!(
        audio_resource_id("microphone", Some("front/left mic")),
        "microphone.front-left-mic"
    );
    assert_ne!(
        audio_resource_id("microphone", Some("usb-mic")),
        audio_resource_id("microphone", None)
    );
}

#[test]
fn a_device_is_served_only_after_the_owning_node_exposes_that_operation() {
    let dir = sinomo_testdir::TestDir::new("media-audio-exposure");
    let store = sinomo_runtime::node::LocalNodePermissionStore::new(dir.path());
    let resource_id = audio_resource_id("microphone", Some("usb-mic"));

    // Nothing is exposed by default, and an unreadable or absent document fails closed.
    assert!(!store.resource_operation_exposed(&resource_id, MEDIA_CAPTURE_OPERATION));

    let mut permissions = LocalNodePermissions::new();
    permissions
        .expose_resource_operation(resource_id.clone(), MEDIA_CAPTURE_OPERATION)
        .expect("expose the microphone for capture");
    store.save(&permissions).expect("save exposures");

    assert!(store.resource_operation_exposed(&resource_id, MEDIA_CAPTURE_OPERATION));
    // Exposing capture does not expose playback, and it does not expose another device.
    assert!(!store.resource_operation_exposed(&resource_id, MEDIA_PLAYBACK_OPERATION));
    assert!(!store.resource_operation_exposed(
        &audio_resource_id("microphone", None),
        MEDIA_CAPTURE_OPERATION
    ));
}
