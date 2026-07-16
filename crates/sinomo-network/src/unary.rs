//! Explicit compatibility between unary object values and canonical sessions.
//!
//! The session transport itself only carries bounded control heads plus
//! aggregate-unbounded byte streams. Callers that still expose `Vec<u8>` unary
//! request/response values opt into this adapter and supply their own materialize
//! limit. The adapter always uses the stream lane for a non-empty value; it
//! never branches on payload size.

use crate::{
    NetworkError, ObjectRouteDescriptor, ObjectRpcBodySource, ObjectRpcRequest,
    ObjectRpcRouteCatalog, ObjectRpcRouteReadiness, ObjectRpcSessionCall, ObjectRpcSessionRequest,
    ObjectRpcSessionResponse, ObjectRpcSessionResult,
};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower_layer::Layer;
use tower_service::Service;

pub(crate) const OBJECT_RPC_BUFFERED_REQUEST_BODY_METADATA_KEY: &str =
    "sinomo.rpc-session.buffered-request-body";
pub(crate) const OBJECT_RPC_BUFFERED_REQUEST_BODY_METADATA_VALUE: &str = "v1";
pub(crate) const OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY: &str =
    "sinomo.rpc-session.buffered-response-body";
pub(crate) const OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_VALUE: &str = "v1";

/// Explicit adapter for legacy unary values carried by a canonical session.
///
/// Encoding is size-independent. Decoding is intentionally bounded by the
/// owning API because materializing a stream into `Vec<u8>` is an application
/// resource decision, not a network transfer limit.
#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectRpcUnarySessionAdapter;

impl ObjectRpcUnarySessionAdapter {
    /// Moves one non-empty unary request value into the canonical stream lane.
    pub fn encode_request(
        mut request: ObjectRpcRequest,
    ) -> Result<ObjectRpcSessionCall, NetworkError> {
        if request.auth.is_some() {
            return Err(NetworkError::Serialization {
                detail: "cannot adapt an already signed object RPC request".to_string(),
            });
        }
        if request.body.is_empty() {
            return Ok(ObjectRpcSessionCall::buffered(request));
        }
        if request
            .metadata
            .contains_key(OBJECT_RPC_BUFFERED_REQUEST_BODY_METADATA_KEY)
        {
            return Err(NetworkError::Serialization {
                detail: "object RPC request already contains the reserved unary-session marker"
                    .to_string(),
            });
        }
        let payload = std::mem::take(&mut request.body);
        request.metadata.insert(
            OBJECT_RPC_BUFFERED_REQUEST_BODY_METADATA_KEY.to_string(),
            OBJECT_RPC_BUFFERED_REQUEST_BODY_METADATA_VALUE.to_string(),
        );
        Ok(ObjectRpcSessionCall::new(
            request,
            ObjectRpcBodySource::bytes(payload),
        ))
    }

    /// Returns whether this authenticated request carries a unary value in its
    /// stream lane, rejecting malformed uses of the reserved marker.
    pub fn request_is_encoded(request: &ObjectRpcSessionRequest) -> Result<bool, NetworkError> {
        let Some(version) = request
            .request
            .metadata
            .get(OBJECT_RPC_BUFFERED_REQUEST_BODY_METADATA_KEY)
        else {
            return Ok(false);
        };
        if version != OBJECT_RPC_BUFFERED_REQUEST_BODY_METADATA_VALUE
            || !request.request.body.is_empty()
            || request.body.is_empty()
        {
            return Err(NetworkError::Serialization {
                detail: "invalid unary object RPC request stream marker".to_string(),
            });
        }
        Ok(true)
    }

    /// Materializes an authenticated unary request for a legacy handler.
    pub async fn decode_request(
        request: &mut ObjectRpcSessionRequest,
        max_bytes: usize,
    ) -> Result<bool, NetworkError> {
        if !Self::request_is_encoded(request)? {
            return Ok(false);
        }
        let body = std::mem::take(&mut request.body);
        let decoded = body.collect_to_vec(max_bytes).await?;
        request.request.body = decoded;
        request
            .request
            .metadata
            .remove(OBJECT_RPC_BUFFERED_REQUEST_BODY_METADATA_KEY);
        // Authentication has already been converted into an out-of-band host
        // context. The materialized application value no longer matches the
        // signed wire head and must never be re-verified or relayed as-is.
        request.request.auth = None;
        Ok(true)
    }

    /// Moves a buffered unary response value into the canonical stream lane.
    /// A response with a semantic attachment is already canonical and remains
    /// untouched.
    pub fn encode_response(
        mut response: ObjectRpcSessionResponse,
    ) -> Result<ObjectRpcSessionResponse, NetworkError> {
        if response
            .response
            .metadata
            .contains_key(OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY)
        {
            return Err(NetworkError::Serialization {
                detail: "local object RPC response contains the reserved unary-session marker"
                    .to_string(),
            });
        }
        if !response.body.is_empty() || response.response.body.is_empty() {
            return Ok(response);
        }
        let payload = std::mem::take(&mut response.response.body);
        response.response.metadata.insert(
            OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY.to_string(),
            OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_VALUE.to_string(),
        );
        response.body = ObjectRpcBodySource::bytes(payload);
        Ok(response)
    }

    /// Returns whether this session result carries a unary response value in
    /// its stream lane, rejecting malformed uses of the reserved marker.
    ///
    /// Unmarked body streams are semantic session attachments. Callers that
    /// only need to unwrap an optional compatibility envelope can use this
    /// predicate without consuming or rejecting those canonical streams.
    pub fn response_is_encoded(result: &ObjectRpcSessionResult) -> Result<bool, NetworkError> {
        let Some(version) = result
            .response
            .metadata
            .get(OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY)
        else {
            return Ok(false);
        };
        if version != OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_VALUE
            || !result.response.body.is_empty()
            || result.body.is_empty()
        {
            return Err(NetworkError::Serialization {
                detail: "invalid unary object RPC response stream marker".to_string(),
            });
        }
        Ok(true)
    }

    /// Materializes a marked unary response for a legacy caller. An unmarked
    /// canonical semantic response stream is rejected so a unary caller cannot
    /// silently discard it; session callers must retain the session result.
    pub async fn decode_response(
        mut result: ObjectRpcSessionResult,
        max_bytes: usize,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        if !Self::response_is_encoded(&result)? {
            if !result.body.is_empty() {
                return Err(NetworkError::Serialization {
                    detail: "unary object RPC response contained an unmarked semantic body stream"
                        .to_string(),
                });
            }
            return Ok(result);
        }
        let body = std::mem::take(&mut result.body);
        result.response.body = body.collect_to_vec(max_bytes).await?;
        result
            .response
            .metadata
            .remove(OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY);
        result
            .response
            .metadata
            .remove(crate::session::OBJECT_RPC_RESPONSE_HEAD_PROOF_METADATA_KEY);
        Ok(result)
    }
}

/// Places legacy unary materialization inside a route's policy permit and
/// timeout while leaving the canonical router and session transport streaming.
#[derive(Clone, Copy, Debug)]
pub struct ObjectRpcUnarySessionAdapterLayer {
    max_request_bytes: usize,
}

impl ObjectRpcUnarySessionAdapterLayer {
    pub const fn new(max_request_bytes: usize) -> Self {
        Self { max_request_bytes }
    }
}

#[derive(Clone, Debug)]
pub struct ObjectRpcUnarySessionAdapterService<S> {
    inner: S,
    max_request_bytes: usize,
}

impl<S> ObjectRpcUnarySessionAdapterService<S> {
    fn take_poll_ready_inner_for_call(&mut self) -> S
    where
        S: Clone,
    {
        let replacement = self.inner.clone();
        std::mem::replace(&mut self.inner, replacement)
    }
}

impl<S> Layer<S> for ObjectRpcUnarySessionAdapterLayer {
    type Service = ObjectRpcUnarySessionAdapterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ObjectRpcUnarySessionAdapterService {
            inner,
            max_request_bytes: self.max_request_bytes,
        }
    }
}

impl<S> ObjectRpcRouteCatalog for ObjectRpcUnarySessionAdapterService<S>
where
    S: ObjectRpcRouteCatalog,
{
    fn object_rpc_route_descriptor(&self, route: &str) -> Option<ObjectRouteDescriptor> {
        self.inner.object_rpc_route_descriptor(route)
    }

    fn object_rpc_route_readiness(&self, route: &str) -> Option<ObjectRpcRouteReadiness> {
        self.inner.object_rpc_route_readiness(route)
    }
}

impl<S> Service<ObjectRpcSessionRequest> for ObjectRpcUnarySessionAdapterService<S>
where
    S: Service<ObjectRpcSessionRequest, Response = ObjectRpcSessionResponse, Error = NetworkError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = ObjectRpcSessionResponse;
    type Error = NetworkError;
    type Future = Pin<
        Box<dyn Future<Output = Result<ObjectRpcSessionResponse, NetworkError>> + Send + 'static>,
    >;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: ObjectRpcSessionRequest) -> Self::Future {
        let max_request_bytes = self.max_request_bytes;
        let mut inner = self.take_poll_ready_inner_for_call();
        Box::pin(async move {
            ObjectRpcUnarySessionAdapter::decode_request(&mut request, max_request_bytes).await?;
            let response = inner.call(request).await?;
            ObjectRpcUnarySessionAdapter::encode_response(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;

    use super::*;
    use crate::{ObjectRpcIncomingBody, ObjectRpcResponse};

    fn request(payload: Vec<u8>) -> ObjectRpcRequest {
        ObjectRpcRequest::from_encoded_body("/test/unary-session", "test", payload)
    }

    #[tokio::test]
    async fn every_nonempty_unary_request_uses_the_stream_lane() {
        for payload in [vec![0x11], vec![0x22; 64 * 1024], vec![0x33; 1024 * 1024]] {
            let expected = payload.clone();
            let call = ObjectRpcUnarySessionAdapter::encode_request(request(payload))
                .expect("encode unary request");

            assert!(call.request.body.is_empty());
            assert_eq!(
                call.request
                    .metadata
                    .get(OBJECT_RPC_BUFFERED_REQUEST_BODY_METADATA_KEY)
                    .map(String::as_str),
                Some(OBJECT_RPC_BUFFERED_REQUEST_BODY_METADATA_VALUE)
            );
            assert_eq!(call.body.total_bytes(), Some(expected.len() as u64));
            assert!(!call.body.is_empty());

            let ObjectRpcSessionCall { request, body } = call;
            let streamed = body
                .collect_to_vec(expected.len())
                .await
                .expect("collect encoded request stream");
            assert_eq!(streamed, expected);

            let mut incoming =
                ObjectRpcSessionRequest::new(request, ObjectRpcIncomingBody::bytes(streamed));
            assert!(
                ObjectRpcUnarySessionAdapter::decode_request(&mut incoming, expected.len())
                    .await
                    .expect("decode unary request")
            );
            assert_eq!(incoming.request.body, expected);
            assert!(incoming.body.is_empty());
            assert!(
                !incoming
                    .request
                    .metadata
                    .contains_key(OBJECT_RPC_BUFFERED_REQUEST_BODY_METADATA_KEY)
            );
        }
    }

    #[tokio::test]
    async fn request_decode_bound_applies_only_when_materializing_the_adapter() {
        let payload = vec![0x44; 4097];
        let call = ObjectRpcUnarySessionAdapter::encode_request(request(payload.clone()))
            .expect("encoding is not constrained by a decode bound");
        assert_eq!(call.body.total_bytes(), Some(payload.len() as u64));

        let mut incoming = ObjectRpcSessionRequest::new(
            call.request,
            ObjectRpcIncomingBody::bytes(payload.clone()),
        );
        let error = ObjectRpcUnarySessionAdapter::decode_request(&mut incoming, payload.len() - 1)
            .await
            .expect_err("materialization must enforce its caller-provided bound");

        assert!(matches!(
            error,
            NetworkError::FrameTooLarge {
                actual_bytes,
                max_bytes
            } if actual_bytes == payload.len() && max_bytes == payload.len() - 1
        ));
    }

    #[tokio::test]
    async fn buffered_unary_response_round_trips_through_the_stream_lane() {
        let expected = vec![0x55; 96 * 1024 + 7];
        let encoded =
            ObjectRpcUnarySessionAdapter::encode_response(ObjectRpcSessionResponse::buffered(
                ObjectRpcResponse::from_encoded_body("/test/unary-session", expected.clone()),
            ))
            .expect("encode unary response");

        assert!(encoded.response.body.is_empty());
        assert_eq!(
            encoded
                .response
                .metadata
                .get(OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY)
                .map(String::as_str),
            Some(OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_VALUE)
        );
        assert_eq!(encoded.body.total_bytes(), Some(expected.len() as u64));

        let streamed = encoded
            .body
            .collect_to_vec(expected.len())
            .await
            .expect("collect encoded response stream");
        let decoded = ObjectRpcUnarySessionAdapter::decode_response(
            ObjectRpcSessionResult::new(encoded.response, ObjectRpcIncomingBody::bytes(streamed)),
            expected.len(),
        )
        .await
        .expect("decode unary response");

        assert_eq!(decoded.response.body, expected);
        assert!(decoded.body.is_empty());
        assert!(
            !decoded
                .response
                .metadata
                .contains_key(OBJECT_RPC_BUFFERED_RESPONSE_BODY_METADATA_KEY)
        );
    }

    #[tokio::test]
    async fn unary_response_decode_rejects_an_unmarked_semantic_body() {
        let result = ObjectRpcSessionResult::new(
            ObjectRpcResponse::encode_body("/test/unary-session", &"response-head")
                .expect("response head"),
            ObjectRpcIncomingBody::bytes(b"semantic-body".to_vec()),
        );

        let error = ObjectRpcUnarySessionAdapter::decode_response(result, 1024)
            .await
            .expect_err("a unary caller must not silently discard a semantic body");

        assert!(matches!(
            error,
            NetworkError::Serialization { detail }
                if detail.contains("unmarked semantic body stream")
        ));
    }

    #[test]
    fn canonical_session_call_accepts_a_ten_tib_logical_body_without_allocating_it() {
        const TEN_TIB: u64 = 10 * 1024 * 1024 * 1024 * 1024;

        let logical_body =
            ObjectRpcBodySource::from_reader(std::io::repeat(0).take(TEN_TIB), Some(TEN_TIB));
        let prepared = ObjectRpcSessionCall::new(request(Vec::new()), logical_body)
            .prepare_for_signing()
            .expect("prepare canonical large logical session body");

        assert_eq!(prepared.body.total_bytes(), Some(TEN_TIB));
        assert!(!prepared.body.is_empty());
    }
}
