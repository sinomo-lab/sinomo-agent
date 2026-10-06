use sinomo_api::dfs::{DfsRepairRetryRequest, DfsRepairRetryResponse};

#[test]
fn repair_retry_legacy_wire_values_have_no_namespace_compaction_scope() {
    let request_id = sinomo_core::Id::generate();
    let request: DfsRepairRetryRequest = serde_json::from_value(serde_json::json!({
        "request_id": request_id,
    }))
    .unwrap();
    assert!(request.compact_namespace_jobs.is_empty());
    let response: DfsRepairRetryResponse = serde_json::from_value(serde_json::json!({
        "request_id": request_id,
        "requested_generation": 1,
    }))
    .unwrap();
    assert!(response.compact_namespace_jobs.is_empty());
    assert!(
        serde_json::to_value(request)
            .unwrap()
            .get("compact_namespace_jobs")
            .is_none()
    );
}

#[test]
fn repair_retry_wire_acknowledgement_preserves_the_exact_requested_scope() {
    let request = DfsRepairRetryRequest {
        request_id: sinomo_core::Id::generate(),
        compact_namespace_jobs: vec!["file-restore:old-a".into(), "file-restore:old-b".into()],
    };
    let decoded: DfsRepairRetryRequest =
        serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
    assert_eq!(decoded, request);
    let response = DfsRepairRetryResponse {
        request_id: request.request_id,
        requested_generation: 4,
        compact_namespace_jobs: request.compact_namespace_jobs,
    };
    let decoded: DfsRepairRetryResponse =
        serde_json::from_slice(&serde_json::to_vec(&response).unwrap()).unwrap();
    assert_eq!(decoded, response);
}
