use super::*;
use sinomo_api::{DaemonApiClient, DaemonObjectRpcDestinationProcessAffinity};
use sinomo_core::network::{ClusterPaths, ObjectRpcRequest};
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture, ServiceRoute,
};
use std::time::Duration;
pub const LOCAL_ROUTE: &str = "/observe/usage/local";
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct NodeQuery {
    #[serde(flatten)]
    query: UsageQuery,
    #[serde(default)]
    settings: Option<UsageSettings>,
    #[serde(default)]
    as_of_ms: Option<u64>,
}
#[derive(Clone)]
pub struct UsageService;
impl Service for UsageService {
    fn name(&self) -> &'static str {
        "observe-usage"
    }
    fn routes(&self) -> &'static [&'static str] {
        &[USAGE_QUERY_ROUTE, LOCAL_ROUTE]
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .map(|route| {
                ServiceRoute::from_route_metadata(
                    *route,
                    USAGE_RPC_CATEGORY,
                    if *route == LOCAL_ROUTE {
                        "local-or-cluster-member"
                    } else {
                        "local-command"
                    },
                    ["observe.usage.read"],
                )
            })
            .collect()
    }
    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            let envelope: NodeQuery = ctx.decode_request_body(&request).await?;
            let query = envelope.query;
            let as_of_ms = if request.route == LOCAL_ROUTE {
                envelope.as_of_ms.ok_or_else(|| {
                    ServiceError::Service("usage peer query requires a collection timestamp".into())
                })?
            } else {
                sinomo_api::observe::recording::timestamp_ms()
            };
            query.validate().map_err(ServiceError::Service)?;
            if request.route == USAGE_QUERY_ROUTE && !ctx.signer.is_local() {
                return Err(ServiceError::Service(
                    "cluster usage aggregation requires a local caller".into(),
                ));
            }
            let data_dir = ctx.data_dir.as_ref().clone();
            let state =
                sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(&data_dir));
            let node = state
                .as_ref()
                .map(|s| s.local_node_id.to_string())
                .unwrap_or_else(|_| "local".into());
            let settings = if request.route == LOCAL_ROUTE {
                envelope.settings.ok_or_else(|| {
                    ServiceError::Service(
                        "usage peer query requires the collector's price snapshot".into(),
                    )
                })?
            } else if let Some(cluster) = ctx.cluster.as_deref() {
                let store =
                    super::config_store().map_err(|e| ServiceError::Service(e.to_string()))?;
                let value = store
                    .get_async(
                        &data_dir,
                        Some(cluster),
                        sinomo_api::app::usage::pricing::SETTINGS_KEY,
                    )
                    .await
                    .map_err(|e| ServiceError::Service(e.to_string()))?;
                serde_json::from_value::<UsageSettings>(value.value)
                    .map_err(|e| ServiceError::Service(e.to_string()))?
            } else {
                UsageSettings::default()
            };
            sinomo_api::app::usage::pricing::config_definition()
                .map_err(|e| ServiceError::Service(e.to_string()))?
                .validate_value(
                    &serde_json::to_value(&settings)
                        .map_err(|e| ServiceError::Service(e.to_string()))?,
                )
                .map_err(|e| ServiceError::Service(e.to_string()))?;
            let response = if request.route == LOCAL_ROUTE {
                local(data_dir, query, node, settings, as_of_ms)
                    .await
                    .map_err(ServiceError::Service)?
            } else {
                let mut targets = match state {
                    Ok(state) => state
                        .members
                        .into_iter()
                        .filter(|m| m.state.is_authorized())
                        .map(|m| m.node_id.to_string())
                        .collect::<Vec<_>>(),
                    Err(_) if ctx.cluster.is_none() => vec![node.clone()],
                    Err(e) => {
                        return Err(ServiceError::Service(format!(
                            "usage target discovery: {e}"
                        )));
                    }
                };
                if !targets.contains(&node) {
                    targets.push(node.clone());
                }
                if !query.nodes.is_empty() {
                    targets.retain(|n| query.nodes.contains(n));
                }
                targets.sort();
                targets.dedup();
                let mut response = UsageQueryResponse::default();
                for requested in &query.nodes {
                    if !targets.contains(requested) {
                        response.failed.push(UsageNodeFailure {
                            node: requested.clone(),
                            error: "node is not an authorized collection target".into(),
                        });
                    }
                }
                if targets.len() > 32 {
                    response.omitted.extend(targets.drain(32..));
                }
                let node_query = query.clone();
                let samples =
                    sinomo_api::observe::collect_bounded(targets, move |target: String| {
                        let local_node = node.clone();
                        let data = data_dir.clone();
                        let query = node_query.clone();
                        let settings = settings.clone();
                        async move {
                            if target == local_node {
                                local(data, query, target, settings, as_of_ms).await
                            } else {
                                remote(&data, &target, &query, &settings, as_of_ms).await
                            }
                        }
                    })
                    .await;
                for (node, result) in samples {
                    match result {
                        Ok(part) => merge(&mut response, part),
                        Err(error) => response.failed.push(UsageNodeFailure { node, error }),
                    }
                }
                response
                    .records
                    .sort_by(|a, b| b.timestamp_ms.cmp(&a.timestamp_ms).then(a.id.cmp(&b.id)));
                let has_more =
                    response.next_cursor.is_some() || response.records.len() > query.limit;
                response.records.truncate(query.limit);
                response
                    .record_costs
                    .retain(|id, _| response.records.iter().any(|record| record.id == *id));
                response.next_cursor = has_more
                    .then(|| response.records.last().map(UsageCursor::from_record))
                    .flatten();
                if response.groups.len() > query.max_groups {
                    response.groups.truncate(query.max_groups);
                    response.truncated = true;
                    response.warnings.push("Cluster grouping limit reached; totals still include all responding nodes.".into());
                }
                response
            };
            ctx.respond_value(request.route, response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}
async fn local(
    data: SinomoDataDir,
    query: UsageQuery,
    node: String,
    settings: UsageSettings,
    as_of_ms: u64,
) -> Result<UsageQueryResponse, String> {
    tokio::task::spawn_blocking(move || {
        UsageLedger::new(&data).query_with_settings_at(&query, &node, &settings, as_of_ms)
    })
    .await
    .map_err(|e| e.to_string())?
}
async fn remote(
    data_dir: &SinomoDataDir,
    node: &str,
    query: &UsageQuery,
    settings: &UsageSettings,
    as_of_ms: u64,
) -> Result<UsageQueryResponse, String> {
    let node = sinomo_core::node::NodeId::new(node).map_err(|e| e.to_string())?;
    let request = ObjectRpcRequest::encode_body(
        LOCAL_ROUTE,
        USAGE_RPC_CATEGORY,
        &NodeQuery {
            query: query.clone(),
            settings: Some(settings.clone()),
            as_of_ms: Some(as_of_ms),
        },
    )
    .map_err(|e| e.to_string())?;
    let timeout = Duration::from_secs(5);
    let paths = sinomo_api::DaemonPaths::new(data_dir);
    let client = tokio::task::spawn_blocking(move || DaemonApiClient::connect_for_paths(&paths))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    client
        .with_timeout(timeout)
        .object_rpc_to_target_with_deadline_and_affinity_async(
            &node,
            &request,
            timeout,
            Some(timeout),
            true,
            DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
        )
        .await
        .map_err(|e| e.to_string())?
        .decode_body()
        .map_err(|e| e.to_string())
}
fn merge(response: &mut UsageQueryResponse, part: UsageQueryResponse) {
    response.record_costs.extend(part.record_costs);
    response.truncated |= part.truncated;
    response.warnings.extend(part.warnings);
    if part.next_cursor.is_some() {
        response.next_cursor = part.next_cursor;
    }
    response.ledger_sizes.extend(part.ledger_sizes);
    response.history_truncated |= part.history_truncated;
    for record in part.quota_history {
        retain_recent(&mut response.quota_history, record, 200);
    }
    for record in part.skill_history {
        retain_recent(&mut response.skill_history, record, 200);
    }
    for group in part.groups {
        if let Some(existing) = response
            .groups
            .iter_mut()
            .find(|g| g.node == group.node && g.dimensions == group.dimensions)
        {
            merge_totals(&mut existing.totals, &group.totals);
            pricing::merge_cost(&mut existing.cost, &group.cost);
        } else {
            response.groups.push(group);
        }
    }
    for budget in part.budgets {
        if let Some(old) = response.budgets.iter_mut().find(|b| {
            b.name == budget.name
                && b.currency == budget.currency
                && b.period_start_ms == budget.period_start_ms
        }) {
            old.spent += budget.spent;
            old.projected += budget.projected;
            old.unpriced_requests += budget.unpriced_requests;
        } else {
            response.budgets.push(budget);
        }
    }
    sinomo_api::app::usage::pricing::merge_cost(&mut response.cost, &part.cost);
    merge_totals(&mut response.totals, &part.totals);
    response.records.extend(part.records);
    response.answered.extend(part.answered);
    response.failed.extend(part.failed);
    response.omitted.extend(part.omitted);
    response.ledger_bytes = response.ledger_bytes.saturating_add(part.ledger_bytes);
    for bucket in part.series {
        if let Some(old) = response
            .series
            .iter_mut()
            .find(|b| b.start_ms == bucket.start_ms)
        {
            merge_totals(&mut old.totals, &bucket.totals);
            pricing::merge_cost(&mut old.cost, &bucket.cost);
            for (provider, cost) in bucket.providers {
                pricing::merge_cost(old.providers.entry(provider).or_default(), &cost);
            }
        } else {
            response.series.push(bucket);
        }
    }
    response.series.sort_by_key(|b| b.start_ms);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn routes_use_host_authorization_vocabulary_and_finalize() {
        let service = UsageService;
        let routes = service.route_categories();
        assert_eq!(routes[0].auth_scope.as_deref(), Some("local-command"));
        assert_eq!(
            routes[1].auth_scope.as_deref(),
            Some("local-or-cluster-member")
        );
        sinomo_core::service::ServiceRegistry::builder()
            .register(service)
            .build();
    }
    #[test]
    fn merge_keeps_partial_node_coverage_and_adds_totals() {
        let mut response = UsageQueryResponse::default();
        let mut answered = UsageQueryResponse {
            answered: vec!["online".into()],
            ..Default::default()
        };
        answered.totals.model_requests = 3;
        merge(&mut response, answered);
        merge(
            &mut response,
            UsageQueryResponse {
                failed: vec![UsageNodeFailure {
                    node: "offline".into(),
                    error: "timed out".into(),
                }],
                ..Default::default()
            },
        );
        assert_eq!(response.totals.model_requests, 3);
        assert!(response.partial());
        assert_eq!(response.answered, vec!["online"]);
        assert_eq!(response.failed[0].node, "offline");
    }
}

#[cfg(test)]
mod integration {
    use super::*;
    use sinomo_core::{
        network::*,
        service::{ResponseSink, ServiceRegistry, ServiceSigner},
    };
    use std::sync::{Arc, Mutex};
    struct Sink(Arc<Mutex<Option<ObjectRpcOutgoingResponse>>>);
    impl ResponseSink for Sink {
        fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
            *self.0.lock().unwrap() = Some(response);
            Ok(())
        }
    }
    async fn serve(data: SinomoDataDir, listener: AsyncDaemonControlListener) {
        let stream = listener.accept().await.unwrap();
        let mut stream = stream.into_inner();
        let context = read_local_object_rpc_carrier_prelude(&mut stream)
            .await
            .unwrap();
        let mut acceptor =
            accept_local_object_rpc_stream_carrier(Box::new(stream), &context).unwrap();
        let stream = acceptor.accept().await.unwrap().unwrap();
        let peer = RpcPeerContext::unauthenticated_local(
            stream.session_id.clone(),
            stream.channel_binding,
        );
        serve_service_rpc_peer_stream(
            stream,
            &peer,
            ObjectFrameIoOptions::default(),
            move |incoming, _| async move {
                let response = Arc::new(Mutex::new(None));
                let context = ServiceContext::new(
                    Arc::new(data),
                    None,
                    ServiceSigner::Local,
                    incoming.body,
                    Box::new(Sink(response.clone())),
                );
                ServiceRegistry::builder()
                    .register(crate::ObserveService::new())
                    .build()
                    .dispatch(context, incoming.request)
                    .await
                    .map_err(|e| NetworkError::Io {
                        operation: "usage fixture dispatch".into(),
                        detail: e.to_string(),
                    })?;
                let outgoing = response.lock().unwrap().take().unwrap();
                Ok(outgoing)
            },
        )
        .await
        .unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_node_queries_preserve_offline_coverage_and_a_single_price_snapshot() {
        let cluster = sinomo_core::Id::generate();
        let mut roots = Vec::new();
        let mut servers = Vec::new();
        let mut addresses = BTreeMap::new();
        let mut node_ids = Vec::new();
        for index in 0..3 {
            let root = sinomo_testdir::TestDir::new("observe-usage-node");
            let data = SinomoDataDir::new(&root);
            data.ensure_root_layout().unwrap();
            let identity =
                sinomo_core::node::load_or_create_main_node_identity(&data.root()).unwrap();
            let node = sinomo_core::Id::generate();
            let state = sinomo_core::ClusterBootstrapState::initialized_locally(
                cluster.clone(),
                "usage-fixture",
                node.clone(),
                format!("node-{index}"),
                identity.public_key,
                time::OffsetDateTime::now_utc(),
            );
            sinomo_core::cluster::write_cluster_state(&ClusterPaths::new(&data), &state).unwrap();
            let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
                .await
                .unwrap();
            let address = listener.local_addr_string().unwrap();
            if index < 2 {
                let record = UsageRecord {
                    id: sinomo_core::Id::generate().to_string(),
                    timestamp_ms: 60_000,
                    attribution: UsageAttribution {
                        node: node.to_string(),
                        ..Default::default()
                    },
                    event: UsageEvent::ModelRequest {
                        provider: "fixture".into(),
                        account: "work".into(),
                        model: "test".into(),
                        operation: "completion".into(),
                        status: UsageStatus::Succeeded,
                        error_class: None,
                        retry_count: Some(0),
                        duration_ms: Some(20),
                        first_output_ms: None,
                        reported: true,
                        units: LanguageModelUsage {
                            input_tokens: Some(1_000_000),
                            ..Default::default()
                        },
                    },
                };
                UsageLedger::new(&data).append(&record).unwrap();
                servers.push(tokio::spawn(serve(data, listener)));
            } else {
                drop(listener);
            }
            addresses.insert(node.to_string(), address);
            node_ids.push(node.to_string());
            roots.push(root);
        }
        let client_root = sinomo_testdir::TestDir::new("observe-usage-collector");
        let client_data = SinomoDataDir::new(&client_root);
        client_data.ensure_root_layout().unwrap();
        let settings = UsageSettings {
            prices: vec![UsagePrice {
                provider: "fixture".into(),
                model_pattern: "test".into(),
                operation: "completion".into(),
                effective_from_ms: 0,
                currency: "USD".into(),
                units: BTreeMap::from([("input_tokens".into(), 2.0)]),
                source: "collector snapshot".into(),
            }],
            ..Default::default()
        };
        let request = ObjectRpcRequest::encode_body(
            LOCAL_ROUTE,
            USAGE_RPC_CATEGORY,
            &NodeQuery {
                query: UsageQuery::default(),
                settings: Some(settings),
                as_of_ms: Some(120_000),
            },
        )
        .unwrap();
        let offline = node_ids[2].clone();
        let samples = sinomo_api::observe::collect_bounded(node_ids, move |node| {
            let address = addresses[&node].clone();
            let data = client_data.clone();
            let request = request.clone();
            async move {
                let runtime = tokio::runtime::Handle::current();
                let result = sinomo_api::transport::call_daemon_address_rpc_async(
                    data,
                    &address,
                    ObjectRpcCall::value(request),
                    ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                        Duration::from_secs(2),
                    )),
                    &runtime,
                )
                .await
                .map_err(|e| e.to_string())?;
                result
                    .decode_body::<UsageQueryResponse>()
                    .await
                    .map_err(|e| e.to_string())
            }
        })
        .await;
        let mut response = UsageQueryResponse::default();
        for (node, result) in samples {
            match result {
                Ok(part) => merge(&mut response, part),
                Err(error) => response.failed.push(UsageNodeFailure { node, error }),
            }
        }
        assert_eq!(response.answered.len(), 2, "{response:?}");
        assert_eq!(response.failed.len(), 1);
        assert_eq!(response.failed[0].node, offline);
        assert!(response.partial());
        assert_eq!(response.totals.model_requests, 2);
        assert_eq!(response.cost.amounts["USD"], 4.0);
        assert_eq!(response.cost.priced_requests, 2);
        for server in servers {
            server.await.unwrap();
        }
    }
}
