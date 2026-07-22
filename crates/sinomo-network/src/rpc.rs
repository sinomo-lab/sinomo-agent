use crate::error::NetworkError;
use crate::object::SinomoObjectCodec;
use serde::{Deserialize, Serialize};
use sinomo_base::Id;
use std::collections::BTreeMap;
use time::OffsetDateTime;

fn serialize_network_bytes<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    crate::fallible_serde::serialize_bytes(bytes, serializer)
}

fn deserialize_network_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::fallible_serde::deserialize_bytes_with_operation(deserializer, "object rpc bytes")
}

fn serialize_optional_network_bytes<S>(
    bytes: &Option<Vec<u8>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    crate::fallible_serde::serialize_optional_bytes(bytes, serializer)
}

fn deserialize_optional_network_bytes<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::fallible_serde::deserialize_optional_bytes_with_operation(
        deserializer,
        "object rpc auth bytes",
    )
}

pub const OBJECT_RPC_ERROR_METADATA_KEY: &str = "sinomo-error";
pub const OBJECT_RPC_ERROR_DETAIL_METADATA_KEY: &str = "sinomo-error-detail";
pub const OBJECT_RPC_OPERATION_ID_METADATA_KEY: &str = "sinomo-operation-id";
pub const OBJECT_RPC_CANCELS_OPERATION_ID_METADATA_KEY: &str = "sinomo-cancels-operation-id";
pub const OBJECT_RPC_RESPONSE_RETRY_SAFE_METADATA_KEY: &str = "sinomo-response-retry-safe";
pub const OBJECT_RPC_RESPONDER_NODE_ID_METADATA_KEY: &str = "sinomo-responder-node-id";
pub const OBJECT_RPC_TRACEPARENT_METADATA_KEY: &str = "traceparent";
pub const OBJECT_RPC_TRACESTATE_METADATA_KEY: &str = "tracestate";
pub const OBJECT_RPC_OBSERVE_SAMPLED_METADATA_KEY: &str = "sinomo-observe-sampled";

pub const CLUSTER_HEALTH_RPC_CATEGORY: &str = "cluster.health";
pub const CLUSTER_HEALTH_ROUTE: &str = "/cluster/health";
pub const CLUSTER_HEALTH_INCLUDE_PEER_VIEWS_METADATA_KEY: &str = "sinomo-health-peer-views";
pub const CLUSTER_HEALTH_SCOPE_METADATA_KEY: &str = "sinomo-health-scope";
pub const CLUSTER_HEALTH_SCOPE_LOCAL: &str = "local";
pub const CLUSTER_NETWORK_RPC_CATEGORY: &str = "cluster.network";
pub const CLUSTER_NETWORK_CONNECTIVITY_ROUTE: &str = "/cluster/network/connectivity";
pub const CLUSTER_NETWORK_PING_ROUTE: &str = "/cluster/network/ping";
pub const CLUSTER_MEMBERSHIP_RPC_CATEGORY: &str = "cluster.membership";
pub const CLUSTER_MEMBER_PUBLISH_ROUTE: &str = "/cluster/membership/member-publish";
pub const CLUSTER_INVITE_PUBLISH_ROUTE: &str = "/cluster/membership/invite-publish";
pub const CLUSTER_JOIN_CHALLENGE_ROUTE: &str = "/cluster/join/challenge";
pub const CLUSTER_JOIN_REGISTER_ROUTE: &str = "/cluster/join/register";
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcRequest {
    pub route: String,
    pub category: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_network_bytes",
        deserialize_with = "deserialize_optional_network_bytes"
    )]
    pub auth: Option<Vec<u8>>,
    #[serde(
        serialize_with = "serialize_network_bytes",
        deserialize_with = "deserialize_network_bytes"
    )]
    pub body: Vec<u8>,
}

impl ObjectRpcRequest {
    pub fn from_encoded_body(
        route: impl Into<String>,
        category: impl Into<String>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            route: route.into(),
            category: category.into(),
            metadata: BTreeMap::new(),
            auth: None,
            body,
        }
    }

    pub fn encode_body<T: Serialize + ?Sized>(
        route: impl Into<String>,
        category: impl Into<String>,
        body: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            route: route.into(),
            category: category.into(),
            metadata: BTreeMap::new(),
            auth: None,
            body: SinomoObjectCodec::network_frame().encode(body)?,
        })
    }

    pub fn decode_body<T: serde::de::DeserializeOwned>(&self) -> Result<T, NetworkError> {
        SinomoObjectCodec::network_frame().decode(&self.body)
    }

    pub fn decode_body_borrowed<'de, T>(&'de self) -> Result<T, NetworkError>
    where
        T: serde::Deserialize<'de>,
    {
        SinomoObjectCodec::network_frame().decode_borrowed(&self.body)
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_auth(mut self, auth: Vec<u8>) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn with_operation_id(self, operation_id: impl Into<String>) -> Self {
        self.with_metadata(OBJECT_RPC_OPERATION_ID_METADATA_KEY, operation_id)
    }

    pub fn with_cancels_operation_id(self, operation_id: impl Into<String>) -> Self {
        self.with_metadata(OBJECT_RPC_CANCELS_OPERATION_ID_METADATA_KEY, operation_id)
    }

    pub fn with_response_retry_safe(self) -> Self {
        self.with_metadata(OBJECT_RPC_RESPONSE_RETRY_SAFE_METADATA_KEY, "true")
    }

    pub fn with_traceparent(self, traceparent: impl Into<String>) -> Self {
        self.with_metadata(OBJECT_RPC_TRACEPARENT_METADATA_KEY, traceparent)
    }

    pub fn with_tracestate(self, tracestate: impl Into<String>) -> Self {
        self.with_metadata(OBJECT_RPC_TRACESTATE_METADATA_KEY, tracestate)
    }

    pub fn with_observe_sampled(self, sampled: bool) -> Self {
        self.with_metadata(
            OBJECT_RPC_OBSERVE_SAMPLED_METADATA_KEY,
            if sampled { "true" } else { "false" },
        )
    }

    pub fn with_trace_context_if_missing(mut self) -> Self {
        self.ensure_trace_context();
        self
    }

    pub fn ensure_trace_context(&mut self) {
        self.metadata
            .entry(OBJECT_RPC_TRACEPARENT_METADATA_KEY.to_string())
            .or_insert_with(new_object_rpc_traceparent);
    }

    pub fn operation_id(&self) -> Option<&str> {
        self.metadata
            .get(OBJECT_RPC_OPERATION_ID_METADATA_KEY)
            .map(String::as_str)
    }

    pub fn cancels_operation_id(&self) -> Option<&str> {
        self.metadata
            .get(OBJECT_RPC_CANCELS_OPERATION_ID_METADATA_KEY)
            .map(String::as_str)
    }

    pub fn is_response_retry_safe(&self) -> bool {
        self.metadata
            .get(OBJECT_RPC_RESPONSE_RETRY_SAFE_METADATA_KEY)
            .is_some_and(|value| value == "true")
    }

    pub fn traceparent(&self) -> Option<&str> {
        self.metadata
            .get(OBJECT_RPC_TRACEPARENT_METADATA_KEY)
            .map(String::as_str)
    }

    pub fn tracestate(&self) -> Option<&str> {
        self.metadata
            .get(OBJECT_RPC_TRACESTATE_METADATA_KEY)
            .map(String::as_str)
    }

    pub fn observe_sampled(&self) -> bool {
        self.metadata
            .get(OBJECT_RPC_OBSERVE_SAMPLED_METADATA_KEY)
            .is_some_and(|value| value == "true")
    }
}

pub fn new_object_rpc_traceparent() -> String {
    let seed = format!(
        "{}:{}:{}",
        Id::generate(),
        OffsetDateTime::now_utc().unix_timestamp_nanos(),
        std::process::id()
    );
    let trace_id = object_rpc_trace_hex("trace", &seed, 32);
    let span_id = object_rpc_trace_hex("span", &seed, 16);
    format!("00-{trace_id}-{span_id}-01")
}

fn object_rpc_trace_hex(label: &str, seed: &str, len: usize) -> String {
    let hash = blake3::hash(format!("{label}:{seed}").as_bytes());
    let hex = hash.to_hex().to_string();
    hex[..len].to_string()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcCancelRequest {
    pub operation_id: String,
    pub reason: String,
}

impl ObjectRpcCancelRequest {
    pub fn new(operation_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            operation_id: operation_id.into(),
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcCancelResponse {
    pub operation_id: String,
    pub cancelled: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcResponse {
    pub route: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(
        serialize_with = "serialize_network_bytes",
        deserialize_with = "deserialize_network_bytes"
    )]
    pub body: Vec<u8>,
}

impl ObjectRpcResponse {
    pub fn from_encoded_body(route: impl Into<String>, body: Vec<u8>) -> Self {
        Self {
            route: route.into(),
            metadata: BTreeMap::new(),
            body,
        }
    }

    pub fn encode_body<T: Serialize + ?Sized>(
        route: impl Into<String>,
        body: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            route: route.into(),
            metadata: BTreeMap::new(),
            body: SinomoObjectCodec::network_frame().encode(body)?,
        })
    }

    pub fn mark_responder_node_id(&mut self, node_id: impl Into<String>) {
        self.metadata
            .entry(OBJECT_RPC_RESPONDER_NODE_ID_METADATA_KEY.to_string())
            .or_insert_with(|| node_id.into());
    }

    pub fn responder_node_id(&self) -> Option<&str> {
        self.metadata
            .get(OBJECT_RPC_RESPONDER_NODE_ID_METADATA_KEY)
            .map(String::as_str)
    }

    pub fn decode_body<T: serde::de::DeserializeOwned>(&self) -> Result<T, NetworkError> {
        self.error_detail()?;
        SinomoObjectCodec::network_frame().decode(&self.body)
    }

    pub fn decode_body_borrowed<'de, T>(&'de self) -> Result<T, NetworkError>
    where
        T: serde::Deserialize<'de>,
    {
        self.error_detail()?;
        SinomoObjectCodec::network_frame().decode_borrowed(&self.body)
    }

    pub fn encode_error(
        route: impl Into<String>,
        detail: impl Into<String>,
    ) -> Result<Self, NetworkError> {
        let detail = detail.into();
        let mut metadata = BTreeMap::new();
        metadata.insert(
            OBJECT_RPC_ERROR_METADATA_KEY.to_string(),
            "true".to_string(),
        );
        metadata.insert(
            OBJECT_RPC_ERROR_DETAIL_METADATA_KEY.to_string(),
            detail.clone(),
        );
        Ok(Self {
            route: route.into(),
            metadata,
            body: SinomoObjectCodec::network_frame().encode(&detail)?,
        })
    }

    pub fn error_detail(&self) -> Result<(), NetworkError> {
        if self
            .metadata
            .get(OBJECT_RPC_ERROR_METADATA_KEY)
            .is_some_and(|value| value == "true")
        {
            let detail = self
                .metadata
                .get(OBJECT_RPC_ERROR_DETAIL_METADATA_KEY)
                .cloned()
                .unwrap_or_else(|| "daemon returned an RPC error".to_string());
            if let Some(detail) = detail.strip_prefix("cluster is read-only: ") {
                return Err(NetworkError::ClusterReadOnly {
                    detail: detail.to_string(),
                });
            }
            if matches!(
                detail.as_str(),
                "device authorization was denied" | "network authorization was denied"
            ) {
                return Err(NetworkError::AuthDenied);
            }
            return Err(NetworkError::ObjectRpc {
                route: self.route.clone(),
                detail,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct HugeByteSizeHintDeserializer;

    impl<'de> serde::Deserializer<'de> for HugeByteSizeHintDeserializer {
        type Error = sinomo_object::Error;

        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_seq(HugeByteSizeHintSeqAccess)
        }

        fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_seq(HugeByteSizeHintSeqAccess)
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
            option unit unit_struct newtype_struct seq tuple tuple_struct map struct
            enum identifier ignored_any
        }
    }

    struct HugeByteSizeHintSeqAccess;

    impl<'de> serde::de::SeqAccess<'de> for HugeByteSizeHintSeqAccess {
        type Error = sinomo_object::Error;

        fn next_element_seed<T>(&mut self, _seed: T) -> Result<Option<T::Value>, Self::Error>
        where
            T: serde::de::DeserializeSeed<'de>,
        {
            Ok(None)
        }

        fn size_hint(&self) -> Option<usize> {
            Some(usize::MAX)
        }
    }

    #[derive(Serialize)]
    struct OwnedPayload<'a> {
        name: &'a str,
        #[serde(with = "serde_bytes")]
        bytes: &'a [u8],
    }

    #[derive(Deserialize)]
    struct BorrowedPayload<'a> {
        name: &'a str,
        #[serde(with = "serde_bytes")]
        bytes: &'a [u8],
    }

    #[test]
    fn object_rpc_bytes_deserializer_rejects_huge_size_hint_before_collecting() {
        let error = deserialize_network_bytes(HugeByteSizeHintDeserializer)
            .expect_err("huge byte size hint should fail fallibly");
        assert_eq!(
            NetworkError::object(error),
            NetworkError::ResourceExhausted {
                operation: "object rpc bytes".to_string(),
                requested_bytes: usize::MAX,
            }
        );
    }

    #[test]
    fn rpc_body_borrowed_decode_uses_encoded_body_buffer() {
        let bytes = [1_u8, 2, 3, 4, 5];
        let request = ObjectRpcRequest::encode_body(
            "/test/borrowed",
            "test",
            &OwnedPayload {
                name: "payload",
                bytes: &bytes,
            },
        )
        .expect("request");
        let decoded: BorrowedPayload<'_> = request.decode_body_borrowed().expect("decode request");
        assert_eq!(decoded.name, "payload");
        assert_eq!(decoded.bytes, bytes);
        assert!(slice_is_borrowed_from(
            decoded.name.as_bytes(),
            &request.body
        ));
        assert!(slice_is_borrowed_from(decoded.bytes, &request.body));

        let response = ObjectRpcResponse::encode_body(
            "/test/borrowed",
            &OwnedPayload {
                name: "response",
                bytes: &bytes,
            },
        )
        .expect("response");
        let decoded: BorrowedPayload<'_> =
            response.decode_body_borrowed().expect("decode response");
        assert_eq!(decoded.name, "response");
        assert_eq!(decoded.bytes, bytes);
        assert!(slice_is_borrowed_from(
            decoded.name.as_bytes(),
            &response.body
        ));
        assert!(slice_is_borrowed_from(decoded.bytes, &response.body));
    }

    fn slice_is_borrowed_from(slice: &[u8], source: &[u8]) -> bool {
        let source_start = source.as_ptr() as usize;
        let source_end = source_start + source.len();
        let slice_start = slice.as_ptr() as usize;
        let slice_end = slice_start + slice.len();
        slice_start >= source_start && slice_end <= source_end
    }
}
