//! Original SOBJ type evidence for schema-owned source payload leaves. Callers
//! name declared paths; this module never discovers or walks opaque user data.
use super::{MetadataBackendError, Result};
use serde::{Deserialize, Serialize};
use sinomo_core::object::{DocumentView, ElementView, PayloadView};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceRawDocument {
    #[serde(with = "serde_bytes")]
    pub(crate) bytes: Vec<u8>,
    pub(crate) row_pointer: String,
}
impl SourceRawDocument {
    pub(crate) fn row(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            row_pointer: String::new(),
        }
    }
    pub(crate) fn context(&self) -> Result<SourceDocumentContext<'_>> {
        SourceDocumentContext::new(&self.bytes, &self.row_pointer)
    }
    pub(crate) fn decoded_value(&self) -> Result<serde_json::Value> {
        decode_original_document(&self.bytes)
    }
}
/// One source JSON decoder for actual SOBJ Bytes, lists, scalars and objects.
/// Original type evidence remains in the document; no payload is normalized.
pub(crate) fn decode_original_document(bytes: &[u8]) -> Result<serde_json::Value> {
    DocumentView::parse(bytes).map_err(error)?;
    let json = super::wire_json::journal_json(bytes)?;
    serde_json::from_slice(&json).map_err(error)
}
pub(crate) enum SourcePayloadWire {
    /// Actual SOBJ Bytes, rather than a numeric JSON list with the same values.
    CanonicalBytes(Vec<u8>),
    /// Original scalar/list/array/object JSON. Its logical value stays untouched.
    Json,
}
pub(crate) struct SourceDocumentContext<'a> {
    document: DocumentView<'a>,
    row_pointer: &'a str,
}
fn error(value: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(format!("original source payload type: {value}"))
}
fn present<'a, 'b>(mut element: &'b ElementView<'a>) -> Result<&'b ElementView<'a>> {
    while let ElementView::Optional(optional) = element {
        let Some(value) = optional.value.as_deref() else {
            return Ok(element);
        };
        element = value;
    }
    Ok(element)
}
fn segment(value: &str) -> Result<String> {
    let mut result = String::new();
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character != '~' {
            result.push(character);
            continue;
        }
        result.push(match characters.next() {
            Some('0') => '~',
            Some('1') => '/',
            _ => return Err(error("invalid declared JSON pointer escape")),
        });
    }
    Ok(result)
}
impl<'a> SourceDocumentContext<'a> {
    /// Parse once per bounded record, then resolve every declared leaf from the
    /// same original type view without repeated IO or whole-document parsing.
    pub(crate) fn new(bytes: &'a [u8], row_pointer: &'a str) -> Result<Self> {
        if bytes.len() > 64 * 1024 * 1024 + 32 {
            return Err(error("raw row document exceeds the source record bound"));
        }
        if !row_pointer.is_empty() && !row_pointer.starts_with('/') {
            return Err(error(
                "row pointer is not relative to the original document",
            ));
        }
        let document = DocumentView::parse(bytes).map_err(error)?;
        if !matches!(document.payload, PayloadView::Dynamic(_)) {
            return Err(error("source row lacks a dynamic original type view"));
        }
        Ok(Self {
            document,
            row_pointer,
        })
    }
    pub(crate) fn raw_payload_wire(
        &self,
        relative_json_pointer: &str,
    ) -> Result<SourcePayloadWire> {
        if !relative_json_pointer.is_empty() && !relative_json_pointer.starts_with('/') {
            return Err(error("payload pointer is not relative to its declared row"));
        }
        let PayloadView::Dynamic(root) = &self.document.payload else {
            unreachable!()
        };
        let pointer = format!("{}{}", self.row_pointer, relative_json_pointer);
        let mut value = root;
        for key in pointer.split('/').skip(1) {
            value = present(value)?;
            let key = segment(key)?;
            value = match value {
                ElementView::Object(object) => object
                    .get(&key)
                    .ok_or_else(|| error("declared payload path is absent in original bytes"))?,
                ElementView::List(list) => {
                    let index: usize = key.parse().map_err(error)?;
                    if index.to_string() != key {
                        return Err(error("noncanonical source list index"));
                    }
                    list.values
                        .get(index)
                        .ok_or_else(|| error("declared source list index exceeds actual row"))?
                }
                _ => {
                    return Err(error(
                        "declared payload path crosses a scalar or opaque leaf",
                    ));
                }
            };
        }
        Ok(match present(value)? {
            ElementView::Bytes(bytes) => {
                if bytes.len() > 64 * 1024 {
                    return Err(error("original module payload exceeds 64 KiB"));
                }
                SourcePayloadWire::CanonicalBytes(bytes.to_vec())
            }
            _ => SourcePayloadWire::Json,
        })
    }
}
pub(crate) fn raw_payload_wire(
    raw: &SourceRawDocument,
    relative_json_pointer: &str,
) -> Result<SourcePayloadWire> {
    raw.context()?.raw_payload_wire(relative_json_pointer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::object::{LengthMode, serde_ext};
    use std::collections::BTreeMap;

    #[derive(Serialize)]
    struct Metadata<'a> {
        schema_version: u32,
        #[serde(with = "serde_bytes")]
        payload: &'a [u8],
    }
    #[derive(Serialize)]
    struct Row<'a> {
        module_metadata: BTreeMap<&'a str, Metadata<'a>>,
    }
    #[test]
    fn actual_bytes_and_identical_numeric_json_list_keep_distinct_wire_kinds() {
        let fixture = sinomo_testdir::TestDir::new("source-opaque-payload-wire-kinds");
        let encoded = serde_ext::to_bytes(
            &Row {
                module_metadata: BTreeMap::from([(
                    "owner/namespace",
                    Metadata {
                        schema_version: 1,
                        payload: b"null",
                    },
                )]),
            },
            1,
            LengthMode::U32,
        )
        .unwrap();
        let logical = serde_json::json!({"module_metadata":{"owner/namespace":{
            "schema_version":1,"payload":[110,117,108,108]}}});
        let numeric = serde_ext::to_bytes(&logical, 1, LengthMode::U32).unwrap();
        let original = fixture.join("bytes.sobj");
        let other = fixture.join("json.sobj");
        std::fs::write(&original, &encoded).unwrap();
        std::fs::write(&other, &numeric).unwrap();
        let byte_value: serde_json::Value =
            decode_original_document(&std::fs::read(&original).unwrap()).unwrap();
        let json_value: serde_json::Value =
            decode_original_document(&std::fs::read(&other).unwrap()).unwrap();
        assert_eq!(byte_value, json_value);
        let pointer = "/module_metadata/owner~1namespace/payload";
        assert!(
            matches!(raw_payload_wire(&SourceRawDocument::row(encoded),pointer).unwrap(),
            SourcePayloadWire::CanonicalBytes(bytes) if bytes==b"null")
        );
        assert!(matches!(
            raw_payload_wire(&SourceRawDocument::row(numeric), pointer).unwrap(),
            SourcePayloadWire::Json
        ));
    }
    #[test]
    fn source_envelope_row_pointer_and_declared_leaf_do_not_examine_opaque_children() {
        let value = serde_json::json!({"Table":{"op":{"type":"put-version","version":{
            "module_metadata":{"module":{"schema_version":1,"payload":{
                "metadata":{"payload":[123,125]},"namespace":"opaque user value"}}}}}}});
        let bytes = serde_ext::to_bytes(&value, 1, LengthMode::U32).unwrap();
        let raw = SourceRawDocument {
            bytes: bytes.clone(),
            row_pointer: "/Table/op".into(),
        };
        let context = raw.context().unwrap();
        assert!(matches!(
            context
                .raw_payload_wire("/version/module_metadata/module/payload")
                .unwrap(),
            SourcePayloadWire::Json
        ));
        assert_eq!(raw.bytes, bytes);
        assert!(
            context
                .raw_payload_wire("/version/module_metadata/absent/payload")
                .is_err()
        );
        assert!(
            context
                .raw_payload_wire("/version/module_metadata/module~2/payload")
                .is_err()
        );
    }
}
