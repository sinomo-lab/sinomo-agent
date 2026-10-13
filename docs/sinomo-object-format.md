# Sinomo Object Format

## Overview

Sinomo Object is a binary object format for efficient Sinomo communication and serialization. It represents ordered dict-like objects, homogeneous containers, rectangular arrays, and schema-defined document payloads.

The format is designed around:

- little-endian numeric encoding,
- compact type identifiers,
- ordered object fields,
- a top-level document metadata prelude,
- optional schemas that remove repeated field names and type tags from payloads,
- padding that can be ignored by parsers,
- alignment hints that allow serializers to choose padding without changing the logical document.

## Encoding Basics

All fixed-width numbers are encoded in little endian.

```text
byte    := 1 unsigned byte
bool    := byte(0x00 | 0x01)
uint8   := 1 byte
int8    := 1 byte, two's complement
uint16  := 2 bytes
int16   := 2 bytes, two's complement
uint32  := 4 bytes
int32   := 4 bytes, two's complement
uint64  := 8 bytes
int64   := 8 bytes, two's complement
float   := 4 bytes, IEEE-754 binary32
double  := 8 bytes, IEEE-754 binary64
```

Every length field in a document uses the document's selected length mode:

```text
length_mode 2 := uint16
length_mode 4 := uint32
length_mode 8 := uint64
length        := uint16 | uint32 | uint64, selected by metadata.length_mode
```

`length_mode` values other than `2`, `4`, and `8` are invalid.

## Type IDs

Element type ids are one-byte values.

```text
0x00 padding
0x01 boolean
0x02 uint8
0x03 int8
0x04 uint16
0x05 int16
0x06 uint32
0x07 int32
0x08 uint64
0x09 int64
0x0A float
0x0B double
0x20 document
0x21 object
0x22 list
0x23 array
0x24 string
0x25 bytes
0x26 optional
0x80-0xFF extension
```

Readers must reject unknown built-in type ids below `0x80`. Extension values should be length-delimited so readers can skip unsupported extensions.

Padding bytes are encoded as `0x00` and are ignored only where this specification allows `padding*`.

## Document

A document is the top-level unit.

```text
document := magic metadata schema payload padding*
magic    := byte[4] ; "SOBJ"
payload  := element | schema_payload
```

Metadata is a fixed prelude so readers can determine the length width before parsing later sections.

When the schema section has `type_count == 0`, the payload is a dynamic `element`. When the schema section has one or more schema types, the payload is a `schema_payload`.

```text
metadata :=
  version_id      uint16
  length_mode     uint8
  alignment_hint  uint8
  document_length length
```

`version_id` identifies the Sinomo Object format version or negotiated application version.

`length_mode` selects the integer width used by every `length` field in the document, including `document_length`.

`alignment_hint` has these values:

```text
0 := not aligned
1 := aligned
```

When `alignment_hint` is `1`, all basic type values are guaranteed to begin at an offset aligned to their encoded size. Serializers should use schema layout and padding to achieve this. The document remains a standard Sinomo Object document because parsers ignore padding in allowed positions.

`document_length` is the total encoded document length in bytes, including the magic bytes, metadata, schema, payload, and padding.

## Basic Values

Basic value encodings are the raw little-endian bytes of their type.

```text
boolean := bool
uint8   := uint8
int8    := int8
uint16  := uint16
int16   := int16
uint32  := uint32
int32   := int32
uint64  := uint64
int64   := int64
float   := float
double  := double
```

## Strings and Bytes

```text
string := length utf8_bytes
bytes  := length byte*
```

String length is the number of UTF-8 payload bytes. Strings are not null-terminated. String payloads must be valid UTF-8.

Bytes length is the number of raw payload bytes.

## Dynamic Elements

A dynamic element carries its type id inline.

```text
element := padding* type_id value padding*
```

The value format is determined by `type_id`.

## Object

An object is an ordered map from string keys to dynamic elements.

```text
object := length pair*
pair   := padding* key element
key    := string
```

`length` is the number of key/value pairs.

Object order is semantically significant and must be preserved by readers and writers. Keys must be unique within one object. Readers should reject duplicate keys unless a higher-level compatibility mode explicitly permits them.

## List

A list is a homogeneous container.

```text
list := element_type length list_value*
```

The element type appears once. Individual values do not repeat the type id. `element_type` uses the schema element type encoding defined below.

For built-in element types, `list_value` is the raw value for that type. For schema-defined object types, `list_value` is the schema payload for that type.

## Array

An array is a rectangular multi-dimensional homogeneous container.

```text
array :=
  element_type
  element_size
  rank
  dimensions
  raw_data

element_size := length
rank         := uint8
dimensions   := length[rank]
raw_data     := byte[element_size * product(dimensions)]
```

`element_size` is the encoded byte size of one array element. It must be nonzero.

Arrays may contain fixed-size basic values or fixed-size schema-defined structures. Variable-size values such as `string`, `bytes`, dynamic `object`, dynamic `list`, and schema types with size `0` are not valid array element types.

Elements are stored in row-major order.

For built-in fixed-size types, `element_size` must equal the canonical encoded size of the type. For schema-defined structures, `element_size` must equal the declared schema payload size for that schema type, and each array element is the fixed-size schema payload bytes for one object.

## Optional

Optional fields are first-class values.

```text
optional<T> := present value?
present     := bool
```

When `present` is `0x00`, no value bytes follow. When `present` is `0x01`, the value is encoded according to `T`.

In dynamic objects, an optional value is encoded with type id `0x26`, followed by the optional element type and the optional payload:

```text
dynamic_optional := optional_type present value?
optional_type    := element_type
```

In schema-defined payloads, the schema already provides `T`, so only `present` and the value bytes are encoded.

## Schema

The schema section defines compact object layouts that can be used by the document payload, lists, arrays, and nested fields.

```text
schema :=
  type_count schema_type*

type_count := length

schema_type :=
  schema_type_id
  schema_type_name
  schema_payload_size
  field_count
  schema_field*

schema_type_id       := uint16
schema_type_name     := string
schema_payload_size  := length
field_count          := length

schema_field :=
  field_name
  schema_element_type

field_name := string
```

`schema_payload_size` is the calculated encoded byte size of one schema payload instance. A value of `0` means the schema type is dynamically sized.

A schema type can declare a nonzero `schema_payload_size` only when every field has a fixed encoded size. Because absent optional values omit their value bytes, schema types containing optional fields are dynamically sized and must declare size `0`.

Schema ids are document-local `uint16` identifiers.

## Schema Element Types

Schema element types describe field layouts and container element layouts.

```text
schema_element_type :=
    builtin_type
  | schema_ref
  | list_type
  | array_type
  | object_type
  | optional_type
  | extension_type

builtin_type   := byte(0x01..0x0B | 0x24 | 0x25)
schema_ref     := byte(0x40) schema_type_id
list_type      := byte(0x41) schema_element_type
array_type     := byte(0x42) schema_element_type
object_type    := byte(0x43)
optional_type  := byte(0x44) schema_element_type
extension_type := byte(0x80..0xFF) length byte*
```

`object_type` means a normal dynamic object.

`array_type` must resolve to a fixed-size built-in type or a schema type with nonzero `schema_payload_size`.

## Schema Payload

When a payload uses a schema-defined object type, field names and field type ids are omitted. Fields are encoded in schema declaration order.

```text
schema_payload :=
  root_schema_type_id
  root_count
  schema_object*

root_schema_type_id := uint16
root_count          := length

schema_object := field_value*
```

Each `field_value` is encoded according to its `schema_element_type`.

Example schema:

```text
"object_type_1" id_1 size 0 {
  "key_1": int32,
  "key_2": string,
  "key_3": list<"object_type_2" id_2 size 0 {
    "key_1": int32,
    "key_2": array<double>
  }>,
  "key_4": object,
  "key_5": optional<uint64>
}
```

The schema payload layout is:

```text
uint16 id_1
length root_count

repeated root_count:
  int32 key_1
  string key_2
  length key_3_count
    repeated key_3_count:
      int32 key_1
      array<double> key_2
  object key_4
  bool key_5_present
  uint64 key_5_value, only when present
```

For a fixed-size schema type usable as an array element:

```text
"vector3" id_10 size 12 {
  "x": float,
  "y": float,
  "z": float
}
```

An `array<"vector3" id_10>` stores:

```text
schema_ref(id_10)
element_size 12
rank
dimensions
byte[12 * product(dimensions)]
```

## Alignment and Padding

Padding is represented by one or more `0x00` bytes in positions where `padding*` is allowed.

Parsers must ignore padding in those positions. Parsers must not treat `0x00` as padding inside raw payloads such as strings, bytes, or array raw data.

Padding is not allowed before length-prefixed sections such as the schema section, object keys, strings, bytes, or array dimensions because a valid little-endian length may begin with zero bytes.

When `alignment_hint` is `1`, serializers should insert padding so each basic type begins at an offset divisible by its encoded size:

```text
uint16, int16              := 2-byte aligned
uint32, int32, float       := 4-byte aligned
uint64, int64, double      := 8-byte aligned
uint8, int8, boolean       := 1-byte aligned
```

Containers may contain padding before nested values as needed. Schema-defined payloads should account for padding when declaring nonzero `schema_payload_size`.

## Zero-Copy Operation

Sinomo Object documents should be usable directly from a contiguous immutable byte region, such as a memory-mapped file, receive buffer, shared memory segment, or network send buffer. Parsers should prefer borrowed views into the original region for strings, bytes, arrays, lists, and schema payloads instead of copying payload bytes into owned allocations.

Large byte sequences, tensors, and file-like payloads should be encoded as `bytes` or `array` values whose raw payload is already in final wire format. For numeric tensors, prefer `array` with a fixed-size element type, declared rank, and dimensions. For arbitrary files, prefer `bytes`. When the source data is already contiguous and little-endian, a serializer can write the document header and container descriptors, then reference or append the existing payload region without transforming the data.

Implementations should support a builder mode that reserves space for `document_length` and other length fields, streams or appends raw payload regions, then patches the reserved length fields at finalization. This allows one-pass serialization into a preallocated buffer or memory-mapped output file. With `length_mode 8`, the same strategy supports very large payloads without changing the layout.

For network transmission, implementations should expose scatter/gather output where possible. A sender can emit the small document header, metadata, schema, and container descriptors as one or more buffers, followed by existing tensor or file buffers as additional slices. If the transport requires a single contiguous region, implementations can still build the final document in a memory-mapped or preallocated buffer and copy only the small descriptors around the large payload.

Canonical SOBJ documents retain their up-front document and container lengths.
An unknown-length network producer is therefore represented as a typed SOBJ
value plus an object-RPC semantic attachment, not as an indefinite-length SOBJ
value. Object RPC streams that attachment incrementally; when its length is not
known in advance, the body's finish record supplies the observed final length.
Object RPC adds no content digest or per-call authentication; applications that
need content identity carry and verify their own digest, as described in the
[network architecture](network-architecture.md#payload-invariants).
Known-length contiguous values may still use the scatter/gather document path
above when doing so avoids a payload copy.

For in-memory reads, implementations should maintain offsets instead of decoded owned values:

- an object view stores ordered field offsets and borrowed key slices,
- a list view stores the element type, count, and the offset of the first element,
- an array view stores element type, element size, rank, dimensions, and a borrowed raw data slice,
- a schema payload view stores the schema id, count, and base offset.

Indexes for fast lookup are implementation artifacts and should be built lazily. For ordered objects, a reader can scan fields sequentially without allocation, then optionally build a key-to-offset index only when random lookup is needed.

Schemas should be preferred for high-volume records because fixed field order and known field types let readers calculate offsets without materializing intermediate objects. Fixed-size schema types are especially useful inside arrays because each element can be addressed as `base + index * element_size`.

Alignment-aware writers should use `alignment_hint 1` for documents intended for memory-mapped numeric access. Aligned arrays and fixed-size schema payloads allow readers on compatible platforms to reinterpret borrowed bytes as typed slices after validating magic, length mode, endianness, element type, element size, dimensions, and alignment.

Writers should avoid compression or encryption inside the core document when zero-copy access is required. Those transforms are better applied at a transport or storage layer that can advertise that the resulting bytes must be decoded before direct Sinomo Object access.

## Validation Rules

Readers must validate:

- magic is `SOBJ`,
- `length_mode` is `2`, `4`, or `8`,
- `alignment_hint` is `0` or `1`,
- `document_length` matches the available document bytes,
- strings contain valid UTF-8,
- list values match the declared element type,
- arrays have nonzero `element_size`,
- array byte length equals `element_size * product(dimensions)`,
- fixed-size array element types have the expected declared size,
- schema ids are unique within the document,
- schema references resolve to declared schema ids,
- nonzero `schema_payload_size` values are consistent with the schema layout.

Readers should reject malformed optional values where the presence byte is not `0x00` or `0x01`.
