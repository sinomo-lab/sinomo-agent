use crate::NetworkError;
use std::future::Future;
use std::pin::Pin;

pub type ClusterMembershipRpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;
