# sinomo-object

`sinomo-object` implements the Sinomo Object binary format. It is intended for Sinomo networking and non-user-facing data storage where typed values, ordered objects, and efficient access to large payloads are useful.

The crate provides:

- owned construction types such as `Document`, `Value`, `Object`, `List`, and `Array`;
- binary encoding and borrowed parsing for `SOBJ` documents;
- ordered object fields with duplicate-key rejection;
- typed lists, fixed-size arrays, optional values, and schema metadata;
- a dependency-free plain-text representation in `text`;
- optional serde integration behind the `serde` feature.

The format is specified in [`../../docs/sinomo-object-format.md`](../../docs/sinomo-object-format.md).

## Binary Documents

```rust
use sinomo_object::{
    Document, Field, LengthMode, Object, Value,
};

let document = Document::dynamic(
    1,
    LengthMode::U32,
    Value::Object(Object::new(vec![
        Field::new("id", Value::Uint64(42)),
        Field::new("name", Value::String("node-a".to_owned())),
    ])),
);

let bytes = document.to_bytes()?;
```

## Borrowed Views

`DocumentView` parses from an existing byte slice. String, byte, array, and schema payload data are exposed as borrowed views where possible.

```rust
use sinomo_object::{DocumentView, ElementView, PayloadView};

let view = DocumentView::parse(&bytes)?;

if let PayloadView::Dynamic(ElementView::Object(object)) = &view.payload {
    if let Some(ElementView::String(name)) = object.get("name") {
        assert_eq!(*name, "node-a");
    }
}
```

Arrays keep large raw payloads borrowed from the original document buffer:

```rust
use sinomo_object::{Array, ElementType, TypeId};

let tensor = Array::new(
    ElementType::Builtin(TypeId::Float),
    4,
    vec![2, 2],
    vec![0; 16],
)?;
```

## Plain Text

The `text` module can format and parse Sinomo values and documents. The syntax keeps numeric types explicit.

```rust
use sinomo_object::text;

let value = text::parse_value(
    r#"{"id": uint64(42), "samples": list<int32>[int32(1), int32(2)]}"#,
)?;

let rendered = text::value_to_text(&value);
let reparsed = text::parse_value(&rendered)?;

assert_eq!(reparsed, value);
```

Text documents include metadata, schema, and payload:

```text
document(version=1,length=uint32,align=false,schema=[],payload={"ok": true})
```

## Serde Feature

Serde support is optional:

```toml
sinomo-object = { path = "../sinomo-object", features = ["serde"] }
```

With the feature enabled, `serde_ext` can serialize and deserialize ordinary Rust structures through dynamic Sinomo documents.

```rust
use serde::{Deserialize, Serialize};
use sinomo_object::{LengthMode, serde_ext};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Packet {
    id: u64,
    label: String,
    samples: Vec<i32>,
    note: Option<String>,
}

let packet = Packet {
    id: 42,
    label: "sensor-a".to_owned(),
    samples: vec![1, 2, 3],
    note: Some("warm".to_owned()),
};

let bytes = serde_ext::to_bytes(&packet, 1, LengthMode::U32)?;
let decoded: Packet = serde_ext::from_bytes(&bytes)?;

assert_eq!(decoded, packet);
```

Current serde support targets dynamic Sinomo documents. Deserialization from raw schema payloads is not implemented yet.

## Validation

Readers validate document magic, length mode, document length, UTF-8 strings, object key uniqueness, typed list contents, array sizes, and optional presence bytes. Unsupported extension type ids are rejected unless support is added by a higher-level implementation.

