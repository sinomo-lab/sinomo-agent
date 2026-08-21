//! Hosted cluster KV object-RPC service.
//!
//! The cluster crate owns the wire protocol, local read/watch behavior, and
//! authorization fences. A process host supplies only authority and commit
//! effects so alternate runtimes can register the same service without
//! duplicating protocol dispatch.

use crate::ClusterKvPathsExt;
use sinomo_kv::{
    CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE, CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE,
    CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE, CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
    CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE, CLUSTER_KV_LOCAL_ENTRY_ROUTE,
    CLUSTER_KV_LOCAL_READ_WATCH_ROUTES, CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE,
    CLUSTER_KV_REQUEST_OUTCOME_ROUTE, CLUSTER_KV_RPC_ROUTE_POLICIES, CLUSTER_KV_RPC_ROUTES,
    CLUSTER_KV_TXN_ROUTE, CLUSTER_KV_WATCH_EVENTS_ROUTE, ClusterKvAuthoritativeEntriesRequest,
    ClusterKvAuthoritativeEntriesResponse, ClusterKvAuthoritativeRangePrefixRequest,
    ClusterKvAuthoritativeSnapshotRequest, ClusterKvAuthoritativeWatchStartRequest,
    ClusterKvEntryRequest, ClusterKvLocalReadWatchHost, ClusterKvLocalReadWatchRequest,
    ClusterKvRequestOutcomeRead, ClusterKvRequestOutcomeRequest, ClusterKvTxnRpcResponse,
    ClusterKvWatchCancelRequest, ClusterKvWatchCancelResponse, ClusterKvWatchDrainRequest,
    ClusterKvWatchDrainResponse, ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse,
    ClusterKvWatchPrefixRequest, ClusterKvWatchRegistration, KvAuthoritativeRead, KvEntry, KvError,
    KvLocalRead, KvSnapshot, KvTxnRequest, MAX_KV_TXN_COMPARES, MAX_KV_TXN_OPS_PER_BRANCH,
    create_cluster_kv_watch_for_node, drain_cluster_kv_watch_for_node,
    handle_cluster_kv_watch_events, pack_cluster_kv_authoritative_entries_response,
    read_cluster_kv_local_entry, read_cluster_kv_local_prefix,
    validate_cluster_kv_authoritative_watch_start_response,
    validate_cluster_kv_request_outcome_read,
};
use sinomo_network::{ClusterPaths, NetworkError, ObjectRpcRequest};
use sinomo_service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute, ServiceSigner,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub type ClusterKvServiceFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ServiceError>> + Send + 'a>>;

/// Authority and commit effects required by the cluster-owned KV service.
///
/// Implementations are process adapters: they provide transport and runtime
/// access, while this service retains request decoding, authorization, and
/// response validation.
pub trait ClusterKvServiceHost: Send + Sync + 'static {
    fn authoritative_snapshot<'a>(
        &'a self,
        allow_forwarding: bool,
    ) -> ClusterKvServiceFuture<'a, KvAuthoritativeRead<KvSnapshot>>;

    fn authoritative_entry<'a>(
        &'a self,
        key: String,
        allow_forwarding: bool,
    ) -> ClusterKvServiceFuture<'a, KvAuthoritativeRead<Option<KvEntry>>>;

    fn authoritative_entries<'a>(
        &'a self,
        keys: Vec<String>,
        allow_forwarding: bool,
    ) -> ClusterKvServiceFuture<'a, KvAuthoritativeRead<Vec<Option<KvEntry>>>>;

    fn authoritative_range_prefix<'a>(
        &'a self,
        prefix: String,
        allow_forwarding: bool,
    ) -> ClusterKvServiceFuture<'a, KvAuthoritativeRead<Vec<KvEntry>>>;

    fn request_outcome<'a>(
        &'a self,
        request: ClusterKvRequestOutcomeRequest,
        allow_forwarding: bool,
    ) -> ClusterKvServiceFuture<'a, ClusterKvRequestOutcomeRead>;

    fn commit_transaction<'a>(
        &'a self,
        request: KvTxnRequest,
        allow_forwarding: bool,
    ) -> ClusterKvServiceFuture<'a, ClusterKvTxnRpcResponse>;
}

pub struct HostedClusterKvService {
    host: Arc<dyn ClusterKvServiceHost>,
}

impl HostedClusterKvService {
    pub fn new(host: Arc<dyn ClusterKvServiceHost>) -> Self {
        Self { host }
    }
}

fn local_node_id(paths: &ClusterPaths) -> Result<sinomo_node::NodeId, ServiceError> {
    crate::read_cluster_state_without_kv(paths)
        .map(|state| state.local_node_id)
        .map_err(ServiceError::from)
}

struct LocalReadWatchHost<'a> {
    paths: &'a ClusterPaths,
}

impl ClusterKvLocalReadWatchHost for LocalReadWatchHost<'_> {
    type Error = ServiceError;

    fn kv_error(error: KvError) -> Self::Error {
        ServiceError::from(error)
    }

    fn range_prefix(&mut self, prefix: &str) -> Result<KvLocalRead<Vec<KvEntry>>, Self::Error> {
        let node_id = local_node_id(self.paths)?;
        read_cluster_kv_local_prefix(&self.paths.kv_paths(), &node_id, prefix)
            .map_err(ServiceError::from)
    }

    fn create_watch(
        &mut self,
        request: ClusterKvWatchPrefixRequest,
    ) -> Result<ClusterKvWatchRegistration, Self::Error> {
        let node_id = local_node_id(self.paths)?;
        create_cluster_kv_watch_for_node(&self.paths.kv_paths(), node_id, request)
            .map_err(ServiceError::from)
    }

    fn drain_watch(
        &mut self,
        request: ClusterKvWatchDrainRequest,
    ) -> Result<ClusterKvWatchDrainResponse, Self::Error> {
        let node_id = local_node_id(self.paths)?;
        drain_cluster_kv_watch_for_node(&self.paths.kv_paths(), node_id, request)
            .map_err(ServiceError::from)
    }

    fn cancel_watch(
        &mut self,
        request: ClusterKvWatchCancelRequest,
    ) -> Result<ClusterKvWatchCancelResponse, Self::Error> {
        sinomo_kv::cancel_cluster_kv_watch(&self.paths.kv_paths(), request)
            .map_err(ServiceError::from)
    }
}

enum ClusterKvServiceResponse {
    LocalEntry(KvLocalRead<Option<KvEntry>>),
    LocalRange(KvLocalRead<Vec<KvEntry>>),
    WatchRegistration(ClusterKvWatchRegistration),
    WatchDrain(ClusterKvWatchDrainResponse),
    WatchCancel(ClusterKvWatchCancelResponse),
    WatchEvents(ClusterKvWatchEventsResponse),
    LocalSnapshot(KvLocalRead<KvSnapshot>),
    AuthoritativeEntry(KvAuthoritativeRead<Option<KvEntry>>),
    AuthoritativeEntries(ClusterKvAuthoritativeEntriesResponse),
    AuthoritativeSnapshot(KvAuthoritativeRead<KvSnapshot>),
    AuthoritativeRange(KvAuthoritativeRead<Vec<KvEntry>>),
    AuthoritativeWatch(KvAuthoritativeRead<ClusterKvWatchRegistration>),
    RequestOutcome(ClusterKvRequestOutcomeRead),
    Transaction(ClusterKvTxnRpcResponse),
}

impl ClusterKvServiceResponse {
    fn respond(self, ctx: &mut ServiceContext) -> Result<(), ServiceError> {
        match self {
            Self::LocalEntry(response) => ctx.respond_value(CLUSTER_KV_LOCAL_ENTRY_ROUTE, response),
            Self::LocalRange(response) => {
                ctx.respond_value(sinomo_kv::CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE, response)
            }
            Self::WatchRegistration(response) => {
                ctx.respond_value(sinomo_kv::CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE, response)
            }
            Self::WatchDrain(response) => {
                ctx.respond_value(sinomo_kv::CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE, response)
            }
            Self::WatchCancel(response) => {
                ctx.respond_value(sinomo_kv::CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE, response)
            }
            Self::WatchEvents(response) => {
                ctx.respond_value(CLUSTER_KV_WATCH_EVENTS_ROUTE, response)
            }
            Self::LocalSnapshot(response) => {
                ctx.respond_value(CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE, response)
            }
            Self::AuthoritativeEntry(response) => {
                ctx.respond_value(CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE, response)
            }
            Self::AuthoritativeEntries(response) => {
                ctx.respond_value(CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE, response)
            }
            Self::AuthoritativeSnapshot(response) => {
                ctx.respond_value(CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE, response)
            }
            Self::AuthoritativeRange(response) => {
                ctx.respond_value(CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE, response)
            }
            Self::AuthoritativeWatch(response) => {
                ctx.respond_value(CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE, response)
            }
            Self::RequestOutcome(response) => {
                ctx.respond_value(CLUSTER_KV_REQUEST_OUTCOME_ROUTE, response)
            }
            Self::Transaction(response) => ctx.respond_value(CLUSTER_KV_TXN_ROUTE, response),
        }
    }
}

fn local_read_watch_request(
    paths: &ClusterPaths,
    request: ClusterKvLocalReadWatchRequest,
) -> Result<ClusterKvServiceResponse, ServiceError> {
    let mut host = LocalReadWatchHost { paths };
    match request {
        ClusterKvLocalReadWatchRequest::RangePrefix(prefix) => host
            .range_prefix(&prefix)
            .map(ClusterKvServiceResponse::LocalRange),
        ClusterKvLocalReadWatchRequest::CreateWatch(request) => host
            .create_watch(request)
            .map(ClusterKvServiceResponse::WatchRegistration),
        ClusterKvLocalReadWatchRequest::DrainWatch(request) => host
            .drain_watch(request)
            .map(ClusterKvServiceResponse::WatchDrain),
        ClusterKvLocalReadWatchRequest::CancelWatch(request) => host
            .cancel_watch(request)
            .map(ClusterKvServiceResponse::WatchCancel),
    }
}

fn deny_remote_transaction_authority(reason: &str) -> ServiceError {
    tracing::warn!(
        target: "sinomo.cluster.kv_auth",
        reason,
        "remote cluster KV transaction denied by authoritative membership policy"
    );
    ServiceError::Network(NetworkError::AuthDenied)
}

pub fn remote_cluster_kv_authorization_keys(
    signer: &ServiceSigner,
) -> Result<(String, String), ServiceError> {
    let ServiceSigner::Remote { node_id, .. } = signer else {
        return Err(deny_remote_transaction_authority(
            "authorization fence requires a remote signer",
        ));
    };
    let node_id = node_id
        .parse::<sinomo_node::NodeId>()
        .map_err(|_| deny_remote_transaction_authority("signer node id is invalid"))?;
    Ok((
        sinomo_node::cluster_member_key(&node_id),
        sinomo_node::cluster_capability_policy_key(&node_id),
    ))
}

/// Binds a remote transaction to the exact membership and capability-policy
/// entries observed by the receiving voter.
pub fn fence_remote_cluster_kv_txn_request_with_entries(
    member_entry: Option<&KvEntry>,
    policy_entry: Option<&KvEntry>,
    signer: &ServiceSigner,
    mut request: KvTxnRequest,
) -> Result<KvTxnRequest, ServiceError> {
    let ServiceSigner::Remote {
        node_id,
        public_key,
    } = signer
    else {
        return Err(deny_remote_transaction_authority(
            "authorization fence requires a remote signer",
        ));
    };
    if !request.txn.failure.is_empty() {
        return Err(ServiceError::Service(
            "remote cluster KV transactions cannot contain failure-branch mutations because authorization guards must fail closed"
                .to_string(),
        ));
    }

    let node_id = node_id
        .parse::<sinomo_node::NodeId>()
        .map_err(|_| deny_remote_transaction_authority("signer node id is invalid"))?;
    let member_key = sinomo_node::cluster_member_key(&node_id);
    let member_entry = member_entry
        .filter(|entry| entry.key == member_key)
        .ok_or_else(|| deny_remote_transaction_authority("signer member is absent"))?;
    let member: crate::ClusterMemberRecord = member_entry.decode()?;
    if member.node_id != node_id
        || !member.state.is_authorized()
        || member.node_public_key != *public_key
    {
        return Err(deny_remote_transaction_authority(
            "signer member identity is stale or revoked",
        ));
    }

    let policy_key = sinomo_node::cluster_capability_policy_key(&node_id);
    let policy_entry = policy_entry
        .filter(|entry| entry.key == policy_key)
        .ok_or_else(|| deny_remote_transaction_authority("signer capability policy is absent"))?;
    let policy: sinomo_node::ClusterCapabilityPolicyRecord = policy_entry.decode()?;
    let required_capability = sinomo_kv::CLUSTER_KV_WRITE_CAPABILITY;
    let advertises_capability = policy
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == required_capability);
    let allows_capability = policy
        .allowed_capabilities
        .iter()
        .any(|capability| capability.as_str() == required_capability);
    if policy.cluster_id != member.cluster_id
        || policy.node_id != node_id
        || !advertises_capability
        || !allows_capability
    {
        return Err(deny_remote_transaction_authority(
            "signer capability policy does not grant cluster.kv.write",
        ));
    }

    let compare_count = request
        .txn
        .compares
        .len()
        .checked_add(4)
        .unwrap_or(usize::MAX);
    if compare_count > MAX_KV_TXN_COMPARES {
        return Err(KvError::TxnLimit {
            compares: compare_count,
            success_ops: request.txn.success.len(),
            failure_ops: request.txn.failure.len(),
            max_compares: MAX_KV_TXN_COMPARES,
            max_ops_per_branch: MAX_KV_TXN_OPS_PER_BRANCH,
        }
        .into());
    }
    request
        .txn
        .compares
        .try_reserve(4)
        .map_err(|_| KvError::ResourceExhausted {
            operation: "remote cluster KV authorization fence compares".to_string(),
            requested_bytes: compare_count
                .saturating_mul(std::mem::size_of::<sinomo_kv::KvCompare>().max(1)),
        })?;
    request
        .txn
        .compares
        .push(sinomo_kv::KvCompare::ModifyRevisionEquals {
            key: member_key.clone(),
            revision: member_entry.modify_revision,
        });
    request
        .txn
        .compares
        .push(sinomo_kv::KvCompare::ValueEquals {
            key: member_key,
            value: member_entry.value.clone(),
        });
    request
        .txn
        .compares
        .push(sinomo_kv::KvCompare::ModifyRevisionEquals {
            key: policy_key.clone(),
            revision: policy_entry.modify_revision,
        });
    request
        .txn
        .compares
        .push(sinomo_kv::KvCompare::ValueEquals {
            key: policy_key,
            value: policy_entry.value.clone(),
        });
    Ok(request)
}

async fn run_blocking<T>(
    route: impl Into<String>,
    label: &'static str,
    task: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
) -> Result<T, ServiceError>
where
    T: Send + 'static,
{
    let route = route.into();
    tokio::task::spawn_blocking(task).await.map_err(|source| {
        ServiceError::Service(format!("{label} route {route} task failed: {source}"))
    })?
}

impl Service for HostedClusterKvService {
    fn name(&self) -> &'static str {
        "cluster-kv"
    }

    fn routes(&self) -> &'static [&'static str] {
        CLUSTER_KV_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        CLUSTER_KV_RPC_ROUTE_POLICIES
            .iter()
            .map(|policy| {
                ServiceRoute::from_route_metadata(
                    policy.route,
                    policy.category,
                    policy.auth.as_scope(),
                    policy.required_capabilities.iter().copied(),
                )
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let host = Arc::clone(&self.host);
        Box::pin(async move {
            let cluster_paths = ClusterPaths::new(ctx.data_dir.as_ref());
            let route = request.route.clone();
            if route == CLUSTER_KV_LOCAL_ENTRY_ROUTE {
                let request: ClusterKvEntryRequest = ctx.decode_request_body(&request).await?;
                let task_paths = cluster_paths.clone();
                let response = run_blocking(route.clone(), "cluster kv local entry", move || {
                    let node_id = local_node_id(&task_paths)?;
                    read_cluster_kv_local_entry(&task_paths.kv_paths(), &node_id, &request.key)
                        .map(ClusterKvServiceResponse::LocalEntry)
                        .map_err(ServiceError::from)
                })
                .await?;
                response.respond(&mut ctx)?;
                return Ok(ServiceDisposition::Continue);
            }
            if CLUSTER_KV_LOCAL_READ_WATCH_ROUTES.contains(&route.as_str()) {
                let request = match route.as_str() {
                    sinomo_kv::CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE => {
                        ClusterKvLocalReadWatchRequest::RangePrefix(
                            ctx.decode_request_body(&request).await?,
                        )
                    }
                    sinomo_kv::CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE => {
                        ClusterKvLocalReadWatchRequest::CreateWatch(
                            ctx.decode_request_body(&request).await?,
                        )
                    }
                    sinomo_kv::CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE => {
                        ClusterKvLocalReadWatchRequest::DrainWatch(
                            ctx.decode_request_body(&request).await?,
                        )
                    }
                    sinomo_kv::CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE => {
                        ClusterKvLocalReadWatchRequest::CancelWatch(
                            ctx.decode_request_body(&request).await?,
                        )
                    }
                    other => return Err(ServiceError::UnknownRoute(other.to_string())),
                };
                let task_paths = cluster_paths.clone();
                let response =
                    run_blocking(route.clone(), "cluster kv local read/watch", move || {
                        local_read_watch_request(&task_paths, request)
                    })
                    .await?;
                response.respond(&mut ctx)?;
                return Ok(ServiceDisposition::Continue);
            }
            if route == CLUSTER_KV_WATCH_EVENTS_ROUTE {
                let request: ClusterKvWatchEventsRequest =
                    ctx.decode_request_body(&request).await?;
                let task_paths = cluster_paths.clone();
                let response =
                    run_blocking(route.clone(), "cluster kv watch-event fanout", move || {
                        let state = crate::read_cluster_state(&task_paths)?;
                        handle_cluster_kv_watch_events(&task_paths.kv_paths(), &state, request)
                            .map(ClusterKvServiceResponse::WatchEvents)
                            .map_err(ServiceError::from)
                    })
                    .await?;
                response.respond(&mut ctx)?;
                return Ok(ServiceDisposition::Continue);
            }

            let allow_forwarding = ctx.signer.is_local();
            let response = match route.as_str() {
                CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    let task_paths = cluster_paths.clone();
                    run_blocking(route.clone(), "cluster kv local snapshot", move || {
                        let state = crate::read_cluster_state_without_kv(&task_paths)?;
                        crate::read_cluster_kv_local_snapshot(&task_paths, &state)
                            .map(ClusterKvServiceResponse::LocalSnapshot)
                            .map_err(ServiceError::from)
                    })
                    .await?
                }
                CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE => {
                    let _: ClusterKvAuthoritativeSnapshotRequest =
                        ctx.decode_request_body(&request).await?;
                    ClusterKvServiceResponse::AuthoritativeSnapshot(
                        host.authoritative_snapshot(allow_forwarding).await?,
                    )
                }
                CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE => {
                    let request: ClusterKvEntryRequest = ctx.decode_request_body(&request).await?;
                    ClusterKvServiceResponse::AuthoritativeEntry(
                        host.authoritative_entry(request.key, allow_forwarding)
                            .await?,
                    )
                }
                CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE => {
                    let request: ClusterKvAuthoritativeEntriesRequest =
                        ctx.decode_request_body(&request).await?;
                    let keys = request.keys;
                    let entries = host
                        .authoritative_entries(keys.clone(), allow_forwarding)
                        .await?;
                    ClusterKvServiceResponse::AuthoritativeEntries(
                        pack_cluster_kv_authoritative_entries_response(&keys, &entries)?,
                    )
                }
                CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE => {
                    let request: ClusterKvAuthoritativeRangePrefixRequest =
                        ctx.decode_request_body(&request).await?;
                    ClusterKvServiceResponse::AuthoritativeRange(
                        host.authoritative_range_prefix(request.prefix, allow_forwarding)
                            .await?,
                    )
                }
                CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE => {
                    let request: ClusterKvAuthoritativeWatchStartRequest =
                        ctx.decode_request_body(&request).await?;
                    // Watch registrations are node-local. The receiving node must
                    // establish the barrier itself before installing the watch.
                    let snapshot = host.authoritative_snapshot(false).await?;
                    let authority = snapshot.authority;
                    let watch_request = ClusterKvWatchPrefixRequest {
                        prefix: request.prefix,
                        start_revision: authority.revision.next(),
                    };
                    let task_paths = cluster_paths.clone();
                    let registration = run_blocking(
                        route.clone(),
                        "cluster kv authoritative watch registration",
                        move || {
                            let node_id = local_node_id(&task_paths)?;
                            create_cluster_kv_watch_for_node(
                                &task_paths.kv_paths(),
                                node_id,
                                watch_request,
                            )
                            .map_err(ServiceError::from)
                        },
                    )
                    .await?;
                    let response = KvAuthoritativeRead {
                        value: registration,
                        authority,
                    };
                    validate_cluster_kv_authoritative_watch_start_response(&response)?;
                    ClusterKvServiceResponse::AuthoritativeWatch(response)
                }
                CLUSTER_KV_REQUEST_OUTCOME_ROUTE => {
                    let request: ClusterKvRequestOutcomeRequest =
                        ctx.decode_request_body(&request).await?;
                    let outcome = host.request_outcome(request, allow_forwarding).await?;
                    validate_cluster_kv_request_outcome_read(&outcome)?;
                    ClusterKvServiceResponse::RequestOutcome(outcome)
                }
                CLUSTER_KV_TXN_ROUTE => {
                    let request: KvTxnRequest = ctx.decode_request_body(&request).await?;
                    let submitted_command_digest = request
                        .clone()
                        .into_command_request()
                        .digest()
                        .map_err(ServiceError::from)?;
                    let request = match &ctx.signer {
                        ServiceSigner::Local | ServiceSigner::LocalNode { .. } => request,
                        ServiceSigner::Remote { .. } => {
                            let (member_key, policy_key) =
                                remote_cluster_kv_authorization_keys(&ctx.signer)?;
                            let task_paths = cluster_paths.clone();
                            let (member_entry, policy_entry) = run_blocking(
                                route.clone(),
                                "cluster kv remote authorization entries",
                                move || {
                                    let state = crate::read_cluster_state_without_kv(&task_paths)?;
                                    let kv_paths = task_paths.kv_paths();
                                    let member_entry = read_cluster_kv_local_entry(
                                        &kv_paths,
                                        &state.local_node_id,
                                        &member_key,
                                    )?
                                    .value;
                                    let policy_entry = read_cluster_kv_local_entry(
                                        &kv_paths,
                                        &state.local_node_id,
                                        &policy_key,
                                    )?
                                    .value;
                                    Ok((member_entry, policy_entry))
                                },
                            )
                            .await?;
                            fence_remote_cluster_kv_txn_request_with_entries(
                                member_entry.as_ref(),
                                policy_entry.as_ref(),
                                &ctx.signer,
                                request,
                            )?
                        }
                    };
                    let mut result = host.commit_transaction(request, allow_forwarding).await?;
                    result.submitted_command_digest = Some(submitted_command_digest);
                    ClusterKvServiceResponse::Transaction(result)
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            response.respond(&mut ctx)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_cluster_kv_service(
    builder: ServiceRegistryBuilder,
    host: Arc<dyn ClusterKvServiceHost>,
) -> ServiceRegistryBuilder {
    builder.register(HostedClusterKvService::new(host))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestHost;

    impl ClusterKvServiceHost for TestHost {
        fn authoritative_snapshot<'a>(
            &'a self,
            _allow_forwarding: bool,
        ) -> ClusterKvServiceFuture<'a, KvAuthoritativeRead<KvSnapshot>> {
            Box::pin(async { Err(ServiceError::Service("unused".to_string())) })
        }

        fn authoritative_entry<'a>(
            &'a self,
            _key: String,
            _allow_forwarding: bool,
        ) -> ClusterKvServiceFuture<'a, KvAuthoritativeRead<Option<KvEntry>>> {
            Box::pin(async { Err(ServiceError::Service("unused".to_string())) })
        }

        fn authoritative_entries<'a>(
            &'a self,
            _keys: Vec<String>,
            _allow_forwarding: bool,
        ) -> ClusterKvServiceFuture<'a, KvAuthoritativeRead<Vec<Option<KvEntry>>>> {
            Box::pin(async { Err(ServiceError::Service("unused".to_string())) })
        }

        fn authoritative_range_prefix<'a>(
            &'a self,
            _prefix: String,
            _allow_forwarding: bool,
        ) -> ClusterKvServiceFuture<'a, KvAuthoritativeRead<Vec<KvEntry>>> {
            Box::pin(async { Err(ServiceError::Service("unused".to_string())) })
        }

        fn request_outcome<'a>(
            &'a self,
            _request: ClusterKvRequestOutcomeRequest,
            _allow_forwarding: bool,
        ) -> ClusterKvServiceFuture<'a, ClusterKvRequestOutcomeRead> {
            Box::pin(async { Err(ServiceError::Service("unused".to_string())) })
        }

        fn commit_transaction<'a>(
            &'a self,
            _request: KvTxnRequest,
            _allow_forwarding: bool,
        ) -> ClusterKvServiceFuture<'a, ClusterKvTxnRpcResponse> {
            Box::pin(async { Err(ServiceError::Service("unused".to_string())) })
        }
    }

    #[test]
    fn service_routes_follow_kv_policies() {
        let service = HostedClusterKvService::new(Arc::new(TestHost));
        assert_eq!(service.routes(), CLUSTER_KV_RPC_ROUTES);
        assert_eq!(
            service.route_categories(),
            CLUSTER_KV_RPC_ROUTE_POLICIES
                .iter()
                .map(|policy| ServiceRoute::from_route_metadata(
                    policy.route,
                    policy.category,
                    policy.auth.as_scope(),
                    policy.required_capabilities.iter().copied(),
                ))
                .collect::<Vec<_>>()
        );
    }
}
