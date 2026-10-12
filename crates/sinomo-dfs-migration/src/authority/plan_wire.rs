//! One current typed native-plan projection for ordinary migration RPCs.
//! The original flattened domain plan remains DATA; this wire uses explicit
//! typed fields and requires nullable activation presence and exact row alias.
use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativePlanWire {
    mapping_uses_source_pending: bool,
    expected_assignments: Vec<DfsMetadataShardConfig>,
    desired_assignments: Vec<DfsMetadataShardConfig>,
    target_exact_prefix: sinomo_dfs::DfsMetadataPrefixProof,
    current_image_fingerprint: String,
    combined_layout_source_fences: Vec<serde_json::Value>,
    source_cut: MetadataSourceCut,
    captured_entries: Vec<MetadataCapturedKvEntry>,
    captured_authority: KvAuthorityProof,
    stage_request: KvTxnRequest,
    expected_handoff: sinomo_dfs::DfsMetadataShardMembershipHandoff,
    #[serde(deserialize_with = "required_activation_request")]
    activation_request: Option<KvTxnRequest>,
}
fn required_activation_request<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<KvTxnRequest>, D::Error> {
    Option::deserialize(decoder)
}
impl NativePlanWire {
    pub(super) fn restore(self) -> Result<MetadataImportPlan> {
        if !self.mapping_uses_source_pending {
            return Err(error(
                "retained verification wire changes its exact mapping alias",
            ));
        }
        let pending_operation_mapping = self.source_cut.pending_operations.clone();
        Ok(MetadataImportPlan {
            facts: MetadataImportPlanFacts {
                expected_assignments: self.expected_assignments,
                desired_assignments: self.desired_assignments,
                target_exact_prefix: self.target_exact_prefix,
                current_image_fingerprint: self.current_image_fingerprint,
                pending_operation_mapping,
                combined_layout_source_fences: self.combined_layout_source_fences,
                source_cut: self.source_cut,
            },
            captured_entries: self.captured_entries,
            captured_authority: self.captured_authority,
            stage_request: self.stage_request,
            expected_handoff: self.expected_handoff,
            activation_request: self.activation_request,
        })
    }
}
pub(super) struct NativePlanRef<'a>(pub(super) &'a MetadataImportPlan);
impl Serialize for NativePlanRef<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::{Error as _, SerializeMap};
        let plan = self.0;
        if plan.pending_operation_mapping != plan.source_cut.pending_operations {
            return Err(S::Error::custom(
                "current native plan wire has unequal pending mapping DATA",
            ));
        }
        let mut fields = serializer.serialize_map(Some(12))?;
        fields.serialize_entry("mapping_uses_source_pending", &true)?;
        fields.serialize_entry("expected_assignments", &plan.expected_assignments)?;
        fields.serialize_entry("desired_assignments", &plan.desired_assignments)?;
        fields.serialize_entry("target_exact_prefix", &plan.target_exact_prefix)?;
        fields.serialize_entry("current_image_fingerprint", &plan.current_image_fingerprint)?;
        fields.serialize_entry(
            "combined_layout_source_fences",
            &plan.combined_layout_source_fences,
        )?;
        fields.serialize_entry("source_cut", &plan.source_cut)?;
        fields.serialize_entry("captured_entries", &plan.captured_entries)?;
        fields.serialize_entry("captured_authority", &plan.captured_authority)?;
        fields.serialize_entry("stage_request", &plan.stage_request)?;
        fields.serialize_entry("expected_handoff", &plan.expected_handoff)?;
        fields.serialize_entry("activation_request", &plan.activation_request)?;
        fields.end()
    }
}
pub(super) fn serialize<S: serde::Serializer>(
    plan: &MetadataImportPlan,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    NativePlanRef(plan).serialize(serializer)
}

pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<MetadataImportPlan, D::Error> {
    NativePlanWire::deserialize(decoder)?
        .restore()
        .map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    // This shared codec fixture reads an actual saved normal producer. It never
    // supplies a receiver ACK, author guard, stage effect or current context.
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SavedChallengeData {
        request_id: String,
        plan: MetadataImportPlan,
    }

    async fn decode_chunks<T>(
        bytes: &[u8],
        chunk_size: usize,
    ) -> std::result::Result<T, sinomo_core::network::NetworkError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        // Reuse the shared bounded body facility and the normal decoder. The
        // producer is owned and joined here, including expected early rejection.
        let (sender, incoming) = sinomo_core::network::ObjectRpcIncomingBody::test_channel(
            Some(u64::try_from(bytes.len()).unwrap()),
            2,
        );
        let chunks: Vec<_> = bytes.chunks(chunk_size).map(<[u8]>::to_vec).collect();
        let producer = tokio::spawn(async move {
            for chunk in chunks {
                if sender.send(chunk).await.is_err() {
                    // Negative controls can reject before consuming the body.
                    break;
                }
            }
        });
        let decoded = incoming.decode_sobj().await;
        producer.await.unwrap();
        decoded
    }

    #[test]
    #[ignore = "requires exact actual saved normal SourceFloorChallenge DATA"]
    fn actual_stage_plan_wire_keeps_full_data_across_slice_and_chunks() {
        // Borrow the existing shared embedded fixture executor without starting
        // any node. Its local DATA is not authority or a positive protocol fact.
        let name = "native-plan-wire-codec";
        let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 1);
        let fixture =
            sinomo_dfs::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        fixture.runtime_handle().block_on(async {
        let path = std::env::var("SINOMO_MIGRATION_SOURCE_FLOOR_CHALLENGE")
            .expect("operator must supply genuine saved normal caller DATA");
        let actual: SavedChallengeData =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        NativeImportAuthority::validate_plan(&actual.plan).unwrap();
        let expected = serde_json::to_value(&actual.plan).unwrap();
        let expected_digest = digest(&actual.plan).unwrap();
        let challenge = super::super::staged_service::SourceFloorChallenge {
            request_id: actual.request_id.clone(),
            plan: actual.plan.clone(),
        };
        let codec = sinomo_core::network::SinomoObjectCodec::network_frame();
        let encoded = codec.encode(&challenge).unwrap();
        let decoded: super::super::staged_service::SourceFloorChallenge =
            codec.decode(&encoded).unwrap();
        assert_eq!(decoded.request_id, actual.request_id);
        assert_eq!(serde_json::to_value(&decoded.plan).unwrap(), expected);
        assert_eq!(digest(&decoded.plan).unwrap(), expected_digest);
        for chunk_size in [1, 7, 4093] {
            let decoded: super::super::staged_service::SourceFloorChallenge =
                decode_chunks(&encoded, chunk_size).await.unwrap();
            assert_eq!(decoded.request_id, actual.request_id);
            assert_eq!(serde_json::to_value(&decoded.plan).unwrap(), expected);
            assert_eq!(digest(&decoded.plan).unwrap(), expected_digest);
        }
        let wire = serde_json::to_value(&challenge).unwrap();
        for field in [
            "mapping_uses_source_pending",
            "expected_assignments",
            "desired_assignments",
            "target_exact_prefix",
            "current_image_fingerprint",
            "combined_layout_source_fences",
            "source_cut",
            "captured_entries",
            "captured_authority",
            "stage_request",
            "expected_handoff",
            "activation_request",
        ] {
            let mut missing =
                sinomo_core::object::serde_ext::to_document(&challenge, 1, codec.length_mode)
                    .unwrap();
            assert_eq!(
                missing.to_bytes().unwrap(),
                encoded,
                "negative controls must start with the exact typed native encoding"
            );
            let sinomo_core::object::Payload::Dynamic(sinomo_core::object::Value::Object(root)) =
                &mut missing.payload
            else {
                panic!("actual challenge document is an object");
            };
            let sinomo_core::object::Value::Object(plan) = &mut root
                .fields
                .iter_mut()
                .find(|field| field.key == "plan")
                .unwrap()
                .value
            else {
                panic!("actual typed native plan is an object");
            };
            let before = plan.fields.len();
            plan.fields.retain(|entry| entry.key != field);
            assert_eq!(plan.fields.len() + 1, before);
            // Preserve every other native Option/byte/type marker while
            // deleting just this real field from the current SOBJ document.
            let bytes = missing.to_bytes().unwrap();
            let error = codec
                .decode::<super::super::staged_service::SourceFloorChallenge>(&bytes)
                .unwrap_err()
                .to_string();
            assert!(error.contains(field), "missing {field}: {error}");
            let error =
                decode_chunks::<super::super::staged_service::SourceFloorChallenge>(&bytes, 7)
                    .await
                    .unwrap_err()
                    .to_string();
            assert!(error.contains(field), "missing streamed {field}: {error}");
        }
        let mut wrong_alias = wire.clone();
        wrong_alias["plan"]["mapping_uses_source_pending"] = serde_json::json!(false);
        assert!(
            serde_json::from_value::<super::super::staged_service::SourceFloorChallenge>(
                wrong_alias
            )
            .is_err()
        );
        let mut unexpected = wire;
        unexpected["plan"]["pending_operation_mapping"] =
            serde_json::to_value(&actual.plan.pending_operation_mapping).unwrap();
        assert!(
            serde_json::from_value::<super::super::staged_service::SourceFloorChallenge>(
                unexpected
            )
            .is_err()
        );
        let old_flattened = codec.encode(&actual).unwrap();
        assert!(
            codec
                .decode::<super::super::staged_service::SourceFloorChallenge>(&old_flattened)
                .is_err(),
            "current native wire must not keep a flattened legacy decoder branch"
        );
        assert!(
            decode_chunks::<super::super::staged_service::SourceFloorChallenge>(&old_flattened, 7,)
                .await
                .is_err()
        );
        // Stage and activation requests are complete DATA, never discarded.
        assert_eq!(
            serde_json::to_value(&decoded.plan.stage_request).unwrap(),
            serde_json::to_value(&actual.plan.stage_request).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&decoded.plan.activation_request).unwrap(),
            serde_json::to_value(&actual.plan.activation_request).unwrap()
        );
        });
        // Every shared body producer was joined before ordinary fixture close.
        fixture.close().unwrap();
    }
}
