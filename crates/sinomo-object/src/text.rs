//! Plain-text Sinomo Object representation.
//!
//! The text format is intended for diagnostics, fixtures, and hand-authored
//! non-user-facing data. It keeps Sinomo's typed nature explicit:
//!
//! ```text
//! {"id": uint64(42), "name": "node-a", "samples": list<int32>[int32(1), int32(2)]}
//! ```

use std::fmt::Write;

use crate::{
    AlignmentHint, Array, Document, ElementType, Error, Field, LengthMode, List, Metadata, Object,
    OptionalValue, Payload, Schema, SchemaField, SchemaType, TypeId, Value,
};

pub fn value_to_text(value: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, value);
    out
}

pub fn document_to_text(document: &Document) -> String {
    let mut out = String::new();
    out.push_str("document(");
    let _ = write!(out, "version={},", document.metadata.version_id);
    out.push_str("length=");
    out.push_str(length_mode_name(document.metadata.length_mode));
    out.push(',');
    out.push_str("align=");
    out.push_str(match document.metadata.alignment_hint {
        AlignmentHint::NotAligned => "false",
        AlignmentHint::Aligned => "true",
    });
    out.push(',');
    out.push_str("schema=");
    write_schema(&mut out, &document.schema);
    out.push(',');
    out.push_str("payload=");
    match &document.payload {
        Payload::Dynamic(value) => write_value(&mut out, value),
        Payload::Schema {
            root_schema_type_id,
            root_count,
            data,
        } => {
            let _ = write!(
                out,
                "schema_payload(id={},count={},data=",
                root_schema_type_id, root_count
            );
            write_hex(&mut out, data);
            out.push(')');
        }
    }
    out.push(')');
    out
}

pub fn parse_value(input: &str) -> crate::Result<Value> {
    let mut parser = Parser::new(input);
    let value = parser.parse_value()?;
    parser.finish()?;
    Ok(value)
}

pub fn parse_document(input: &str) -> crate::Result<Document> {
    let mut parser = Parser::new(input);
    let document = parser.parse_document()?;
    parser.finish()?;
    Ok(document)
}

fn write_value(out: &mut String, value: &Value) {
    match value {
        Value::Boolean(value) => out.push_str(if *value { "true" } else { "false" }),
        Value::Uint8(value) => write_typed(out, "uint8", *value),
        Value::Int8(value) => write_typed(out, "int8", *value),
        Value::Uint16(value) => write_typed(out, "uint16", *value),
        Value::Int16(value) => write_typed(out, "int16", *value),
        Value::Uint32(value) => write_typed(out, "uint32", *value),
        Value::Int32(value) => write_typed(out, "int32", *value),
        Value::Uint64(value) => write_typed(out, "uint64", *value),
        Value::Int64(value) => write_typed(out, "int64", *value),
        Value::Float(value) => write_typed(out, "float", format_float(*value as f64)),
        Value::Double(value) => write_typed(out, "double", format_float(*value)),
        Value::Object(object) => write_object(out, object),
        Value::List(list) => write_list(out, list),
        Value::Array(array) => write_array(out, array),
        Value::String(value) => write_string(out, value),
        Value::Bytes(value) => {
            out.push_str("bytes(");
            write_hex(out, value);
            out.push(')');
        }
        Value::Optional(optional) => write_optional(out, optional),
    }
}

fn write_typed<T: std::fmt::Display>(out: &mut String, name: &str, value: T) {
    out.push_str(name);
    out.push('(');
    let _ = write!(out, "{value}");
    out.push(')');
}

fn format_float(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-inf".to_owned()
    } else {
        value.to_string()
    }
}

fn write_object(out: &mut String, object: &Object) {
    out.push('{');
    for (index, field) in object.fields.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        write_string(out, &field.key);
        out.push_str(": ");
        write_value(out, &field.value);
    }
    out.push('}');
}

fn write_list(out: &mut String, list: &List) {
    out.push_str("list<");
    write_element_type(out, &list.element_type);
    out.push_str(">[");
    for (index, value) in list.values.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        write_value(out, value);
    }
    out.push(']');
}

fn write_array(out: &mut String, array: &Array) {
    out.push_str("array<");
    write_element_type(out, &array.element_type);
    out.push_str(">(size=");
    let _ = write!(out, "{}", array.element_size);
    out.push_str(",dims=[");
    for (index, dimension) in array.dimensions.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        let _ = write!(out, "{dimension}");
    }
    out.push_str("],data=");
    write_hex(out, &array.data);
    out.push(')');
}

fn write_optional(out: &mut String, optional: &OptionalValue) {
    out.push_str("optional<");
    write_element_type(out, &optional.element_type);
    out.push_str(">(");
    match &optional.value {
        Some(value) => write_value(out, value),
        None => out.push_str("none"),
    }
    out.push(')');
}

fn write_schema(out: &mut String, schema: &Schema) {
    out.push('[');
    for (index, schema_type) in schema.types.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        let _ = write!(out, "type(id={},name=", schema_type.id);
        write_string(out, &schema_type.name);
        let _ = write!(out, ",size={},fields=[", schema_type.payload_size);
        for (field_index, field) in schema_type.fields.iter().enumerate() {
            if field_index > 0 {
                out.push(',');
            }
            write_string(out, &field.name);
            out.push(':');
            write_element_type(out, &field.element_type);
        }
        out.push_str("])");
    }
    out.push(']');
}

fn write_element_type(out: &mut String, element_type: &ElementType) {
    match element_type {
        ElementType::Builtin(type_id) => out.push_str(type_name(*type_id)),
        ElementType::SchemaRef(id) => {
            let _ = write!(out, "schema({id})");
        }
        ElementType::List(inner) => {
            out.push_str("list<");
            write_element_type(out, inner);
            out.push('>');
        }
        ElementType::Array(inner) => {
            out.push_str("array<");
            write_element_type(out, inner);
            out.push('>');
        }
        ElementType::Object => out.push_str("object"),
        ElementType::Optional(inner) => {
            out.push_str("optional<");
            write_element_type(out, inner);
            out.push('>');
        }
        ElementType::Extension { id, data } => {
            let _ = write!(out, "extension(0x{id:02X},");
            write_hex(out, data);
            out.push(')');
        }
    }
}

fn write_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other if other.is_control() => {
                let _ = write!(out, "\\u{:04X}", other as u32);
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

fn write_hex(out: &mut String, bytes: &[u8]) {
    out.push_str("0x");
    for byte in bytes {
        let _ = write!(out, "{byte:02X}");
    }
}

fn type_name(type_id: TypeId) -> &'static str {
    match type_id {
        TypeId::Boolean => "boolean",
        TypeId::Uint8 => "uint8",
        TypeId::Int8 => "int8",
        TypeId::Uint16 => "uint16",
        TypeId::Int16 => "int16",
        TypeId::Uint32 => "uint32",
        TypeId::Int32 => "int32",
        TypeId::Uint64 => "uint64",
        TypeId::Int64 => "int64",
        TypeId::Float => "float",
        TypeId::Double => "double",
        TypeId::String => "string",
        TypeId::Bytes => "bytes",
        TypeId::Object => "object",
        TypeId::List => "list",
        TypeId::Array => "array",
        TypeId::Optional => "optional",
        TypeId::Document => "document",
        TypeId::Padding => "padding",
    }
}

fn length_mode_name(mode: LengthMode) -> &'static str {
    match mode {
        LengthMode::U16 => "uint16",
        LengthMode::U32 => "uint32",
        LengthMode::U64 => "uint64",
    }
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    const fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn finish(&mut self) -> crate::Result<()> {
        self.skip_ws();
        if self.is_eof() {
            Ok(())
        } else {
            Err(self.error("unexpected trailing input"))
        }
    }

    fn parse_document(&mut self) -> crate::Result<Document> {
        self.expect_keyword("document")?;
        self.expect_char('(')?;
        self.expect_keyword("version")?;
        self.expect_char('=')?;
        let version_id = self.parse_u16_value("version id")?;
        self.expect_char(',')?;
        self.expect_keyword("length")?;
        self.expect_char('=')?;
        let length_mode = self.parse_length_mode()?;
        self.expect_char(',')?;
        self.expect_keyword("align")?;
        self.expect_char('=')?;
        let alignment_hint = if self.consume_keyword("true") {
            AlignmentHint::Aligned
        } else if self.consume_keyword("false") {
            AlignmentHint::NotAligned
        } else {
            return Err(self.error("expected true or false"));
        };
        self.expect_char(',')?;
        self.expect_keyword("schema")?;
        self.expect_char('=')?;
        let schema = self.parse_schema()?;
        self.expect_char(',')?;
        self.expect_keyword("payload")?;
        self.expect_char('=')?;
        let payload = if self.peek_keyword("schema_payload") {
            self.parse_schema_payload()?
        } else {
            Payload::Dynamic(self.parse_value()?)
        };
        self.expect_char(')')?;
        Ok(Document {
            metadata: Metadata {
                version_id,
                length_mode,
                alignment_hint,
                document_length: 0,
            },
            schema,
            payload,
        })
    }

    fn parse_value(&mut self) -> crate::Result<Value> {
        self.skip_ws();
        if self.consume_keyword("true") {
            return Ok(Value::Boolean(true));
        }
        if self.consume_keyword("false") {
            return Ok(Value::Boolean(false));
        }
        if self.peek_char() == Some('"') {
            return Ok(Value::String(self.parse_string()?));
        }
        if self.peek_char() == Some('{') {
            return self.parse_object().map(Value::Object);
        }
        let ident = self.parse_ident()?;
        match ident.as_str() {
            "boolean" => self.parse_call_bool().map(Value::Boolean),
            "uint8" | "u8" => self.parse_call_u64().and_then(|v| {
                u8::try_from(v)
                    .map(Value::Uint8)
                    .map_err(|_| self.error("uint8 out of range"))
            }),
            "int8" | "i8" => self.parse_call_i64().and_then(|v| {
                i8::try_from(v)
                    .map(Value::Int8)
                    .map_err(|_| self.error("int8 out of range"))
            }),
            "uint16" | "u16" => self.parse_call_u64().and_then(|v| {
                u16::try_from(v)
                    .map(Value::Uint16)
                    .map_err(|_| self.error("uint16 out of range"))
            }),
            "int16" | "i16" => self.parse_call_i64().and_then(|v| {
                i16::try_from(v)
                    .map(Value::Int16)
                    .map_err(|_| self.error("int16 out of range"))
            }),
            "uint32" | "u32" => self.parse_call_u64().and_then(|v| {
                u32::try_from(v)
                    .map(Value::Uint32)
                    .map_err(|_| self.error("uint32 out of range"))
            }),
            "int32" | "i32" => self.parse_call_i64().and_then(|v| {
                i32::try_from(v)
                    .map(Value::Int32)
                    .map_err(|_| self.error("int32 out of range"))
            }),
            "uint64" | "u64" => self.parse_call_u64().map(Value::Uint64),
            "int64" | "i64" => self.parse_call_i64().map(Value::Int64),
            "float" | "f32" => self.parse_call_f64().map(|v| Value::Float(v as f32)),
            "double" | "f64" => self.parse_call_f64().map(Value::Double),
            "bytes" => self.parse_bytes_call().map(Value::Bytes),
            "list" => self.parse_list_after_name().map(Value::List),
            "array" => self.parse_array_after_name().map(Value::Array),
            "optional" => self.parse_optional_after_name().map(Value::Optional),
            _ => Err(self.error("expected value")),
        }
    }

    fn parse_object(&mut self) -> crate::Result<Object> {
        self.expect_char('{')?;
        let mut fields = Vec::new();
        if self.consume_char('}') {
            return Ok(Object::new(fields));
        }
        loop {
            let key = self.parse_string()?;
            self.expect_char(':')?;
            let value = self.parse_value()?;
            fields.push(Field::new(key, value));
            if self.consume_char('}') {
                break;
            }
            self.expect_char(',')?;
        }
        Ok(Object::new(fields))
    }

    fn parse_list_after_name(&mut self) -> crate::Result<List> {
        self.expect_char('<')?;
        let element_type = self.parse_element_type()?;
        self.expect_char('>')?;
        self.expect_char('[')?;
        let mut values = Vec::new();
        if self.consume_char(']') {
            return Ok(List::new(element_type, values));
        }
        loop {
            values.push(self.parse_value()?);
            if self.consume_char(']') {
                break;
            }
            self.expect_char(',')?;
        }
        for value in &values {
            if !value_matches_element_type(value, &element_type) {
                return Err(self.error("list value does not match declared element type"));
            }
        }
        Ok(List::new(element_type, values))
    }

    fn parse_array_after_name(&mut self) -> crate::Result<Array> {
        self.expect_char('<')?;
        let element_type = self.parse_element_type()?;
        self.expect_char('>')?;
        self.expect_char('(')?;
        self.expect_keyword("size")?;
        self.expect_char('=')?;
        let element_size = self.parse_u64()?;
        self.expect_char(',')?;
        self.expect_keyword("dims")?;
        self.expect_char('=')?;
        let dimensions = self.parse_dimensions()?;
        self.expect_char(',')?;
        self.expect_keyword("data")?;
        self.expect_char('=')?;
        let data = self.parse_hex_bytes()?;
        self.expect_char(')')?;
        Array::new(element_type, element_size, dimensions, data)
    }

    fn parse_optional_after_name(&mut self) -> crate::Result<OptionalValue> {
        self.expect_char('<')?;
        let element_type = self.parse_element_type()?;
        self.expect_char('>')?;
        self.expect_char('(')?;
        let value = if self.consume_keyword("none") {
            None
        } else {
            Some(Box::new(self.parse_value()?))
        };
        self.expect_char(')')?;
        if let Some(value) = value.as_deref() {
            if !value_matches_element_type(value, &element_type) {
                return Err(self.error("optional value does not match declared element type"));
            }
        }
        Ok(OptionalValue {
            element_type,
            value,
        })
    }

    fn parse_schema(&mut self) -> crate::Result<Schema> {
        self.expect_char('[')?;
        let mut types = Vec::new();
        if self.consume_char(']') {
            return Ok(Schema { types });
        }
        loop {
            self.expect_keyword("type")?;
            self.expect_char('(')?;
            self.expect_keyword("id")?;
            self.expect_char('=')?;
            let id = self.parse_u16_value("schema id")?;
            self.expect_char(',')?;
            self.expect_keyword("name")?;
            self.expect_char('=')?;
            let name = self.parse_string()?;
            self.expect_char(',')?;
            self.expect_keyword("size")?;
            self.expect_char('=')?;
            let payload_size = self.parse_u64()?;
            self.expect_char(',')?;
            self.expect_keyword("fields")?;
            self.expect_char('=')?;
            let fields = self.parse_schema_fields()?;
            self.expect_char(')')?;
            types.push(SchemaType {
                id,
                name,
                payload_size,
                fields,
            });
            if self.consume_char(']') {
                break;
            }
            self.expect_char(',')?;
        }
        Ok(Schema { types })
    }

    fn parse_schema_fields(&mut self) -> crate::Result<Vec<SchemaField>> {
        self.expect_char('[')?;
        let mut fields = Vec::new();
        if self.consume_char(']') {
            return Ok(fields);
        }
        loop {
            let name = self.parse_string()?;
            self.expect_char(':')?;
            let element_type = self.parse_element_type()?;
            fields.push(SchemaField { name, element_type });
            if self.consume_char(']') {
                break;
            }
            self.expect_char(',')?;
        }
        Ok(fields)
    }

    fn parse_schema_payload(&mut self) -> crate::Result<Payload> {
        self.expect_keyword("schema_payload")?;
        self.expect_char('(')?;
        self.expect_keyword("id")?;
        self.expect_char('=')?;
        let root_schema_type_id = self.parse_u16_value("schema payload id")?;
        self.expect_char(',')?;
        self.expect_keyword("count")?;
        self.expect_char('=')?;
        let root_count = self.parse_u64()?;
        self.expect_char(',')?;
        self.expect_keyword("data")?;
        self.expect_char('=')?;
        let data = self.parse_hex_bytes()?;
        self.expect_char(')')?;
        Ok(Payload::Schema {
            root_schema_type_id,
            root_count,
            data,
        })
    }

    fn parse_element_type(&mut self) -> crate::Result<ElementType> {
        let ident = self.parse_ident()?;
        match ident.as_str() {
            "boolean" | "bool" => Ok(ElementType::Builtin(TypeId::Boolean)),
            "uint8" | "u8" => Ok(ElementType::Builtin(TypeId::Uint8)),
            "int8" | "i8" => Ok(ElementType::Builtin(TypeId::Int8)),
            "uint16" | "u16" => Ok(ElementType::Builtin(TypeId::Uint16)),
            "int16" | "i16" => Ok(ElementType::Builtin(TypeId::Int16)),
            "uint32" | "u32" => Ok(ElementType::Builtin(TypeId::Uint32)),
            "int32" | "i32" => Ok(ElementType::Builtin(TypeId::Int32)),
            "uint64" | "u64" => Ok(ElementType::Builtin(TypeId::Uint64)),
            "int64" | "i64" => Ok(ElementType::Builtin(TypeId::Int64)),
            "float" | "f32" => Ok(ElementType::Builtin(TypeId::Float)),
            "double" | "f64" => Ok(ElementType::Builtin(TypeId::Double)),
            "string" => Ok(ElementType::Builtin(TypeId::String)),
            "bytes" => Ok(ElementType::Builtin(TypeId::Bytes)),
            "object" => Ok(ElementType::Object),
            "schema" => {
                self.expect_char('(')?;
                let id = self.parse_u16_value("schema reference id")?;
                self.expect_char(')')?;
                Ok(ElementType::SchemaRef(id))
            }
            "list" => {
                self.expect_char('<')?;
                let inner = self.parse_element_type()?;
                self.expect_char('>')?;
                Ok(ElementType::List(Box::new(inner)))
            }
            "array" => {
                self.expect_char('<')?;
                let inner = self.parse_element_type()?;
                self.expect_char('>')?;
                Ok(ElementType::Array(Box::new(inner)))
            }
            "optional" => {
                self.expect_char('<')?;
                let inner = self.parse_element_type()?;
                self.expect_char('>')?;
                Ok(ElementType::Optional(Box::new(inner)))
            }
            "extension" => {
                self.expect_char('(')?;
                let id = self.parse_hex_u8()?;
                self.expect_char(',')?;
                let data = self.parse_hex_bytes()?;
                self.expect_char(')')?;
                Ok(ElementType::Extension { id, data })
            }
            _ => Err(self.error("expected element type")),
        }
    }

    fn parse_call_bool(&mut self) -> crate::Result<bool> {
        self.expect_char('(')?;
        let value = if self.consume_keyword("true") {
            true
        } else if self.consume_keyword("false") {
            false
        } else {
            return Err(self.error("expected true or false"));
        };
        self.expect_char(')')?;
        Ok(value)
    }

    fn parse_call_u64(&mut self) -> crate::Result<u64> {
        self.expect_char('(')?;
        let value = self.parse_u64()?;
        self.expect_char(')')?;
        Ok(value)
    }

    fn parse_call_i64(&mut self) -> crate::Result<i64> {
        self.expect_char('(')?;
        let value = self.parse_i64()?;
        self.expect_char(')')?;
        Ok(value)
    }

    fn parse_call_f64(&mut self) -> crate::Result<f64> {
        self.expect_char('(')?;
        let value = self.parse_number_token()?;
        self.expect_char(')')?;
        match value.as_str() {
            "NaN" | "nan" => Ok(f64::NAN),
            "inf" | "+inf" => Ok(f64::INFINITY),
            "-inf" => Ok(f64::NEG_INFINITY),
            other => other
                .parse::<f64>()
                .map_err(|_| self.error("invalid float")),
        }
    }

    fn parse_bytes_call(&mut self) -> crate::Result<Vec<u8>> {
        self.expect_char('(')?;
        let bytes = self.parse_hex_bytes()?;
        self.expect_char(')')?;
        Ok(bytes)
    }

    fn parse_dimensions(&mut self) -> crate::Result<Vec<u64>> {
        self.expect_char('[')?;
        let mut dimensions = Vec::new();
        if self.consume_char(']') {
            return Ok(dimensions);
        }
        loop {
            dimensions.push(self.parse_u64()?);
            if self.consume_char(']') {
                break;
            }
            self.expect_char(',')?;
        }
        Ok(dimensions)
    }

    fn parse_string(&mut self) -> crate::Result<String> {
        self.expect_char('"')?;
        let mut out = String::new();
        loop {
            let Some(ch) = self.next_char() else {
                return Err(self.error("unterminated string"));
            };
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let escaped = self
                        .next_char()
                        .ok_or_else(|| self.error("unterminated escape"))?;
                    match escaped {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let code = self.parse_fixed_hex_u32(4)?;
                            let ch = char::from_u32(code)
                                .ok_or_else(|| self.error("invalid unicode escape"))?;
                            out.push(ch);
                        }
                        other => return Err(self.error_at(format!("invalid escape `{other}`"))),
                    }
                }
                other => out.push(other),
            }
        }
    }

    fn parse_length_mode(&mut self) -> crate::Result<LengthMode> {
        let ident = self.parse_ident()?;
        match ident.as_str() {
            "uint16" | "u16" | "2" => Ok(LengthMode::U16),
            "uint32" | "u32" | "4" => Ok(LengthMode::U32),
            "uint64" | "u64" | "8" => Ok(LengthMode::U64),
            _ => Err(self.error("expected length mode")),
        }
    }

    fn parse_u64(&mut self) -> crate::Result<u64> {
        self.parse_number_token()?
            .parse::<u64>()
            .map_err(|_| self.error("invalid unsigned integer"))
    }

    fn parse_u16_value(&mut self, name: &str) -> crate::Result<u16> {
        let value = self.parse_u64()?;
        u16::try_from(value).map_err(|_| self.error_at(format!("{name} out of range")))
    }

    fn parse_i64(&mut self) -> crate::Result<i64> {
        self.parse_number_token()?
            .parse::<i64>()
            .map_err(|_| self.error("invalid signed integer"))
    }

    fn parse_number_token(&mut self) -> crate::Result<String> {
        self.skip_ws();
        let start = self.pos;
        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.' | '_') {
                self.next_char();
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(self.error("expected number"));
        }
        Ok(self.input[start..self.pos].replace('_', ""))
    }

    fn parse_hex_u8(&mut self) -> crate::Result<u8> {
        let bytes = self.parse_hex_bytes()?;
        if bytes.len() != 1 {
            return Err(self.error("expected one-byte hex value"));
        }
        Ok(bytes[0])
    }

    fn parse_hex_bytes(&mut self) -> crate::Result<Vec<u8>> {
        self.skip_ws();
        if !self.input[self.pos..].starts_with("0x") {
            return Err(self.error("expected hex bytes starting with 0x"));
        }
        self.pos += 2;
        let start = self.pos;
        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_hexdigit() || ch == '_' {
                self.next_char();
            } else {
                break;
            }
        }
        let raw = self.input[start..self.pos].replace('_', "");
        if raw.len() % 2 != 0 {
            return Err(self.error("hex byte string must have even length"));
        }
        let mut bytes = Vec::with_capacity(raw.len() / 2);
        for index in (0..raw.len()).step_by(2) {
            let byte = u8::from_str_radix(&raw[index..index + 2], 16)
                .map_err(|_| self.error("invalid hex byte"))?;
            bytes.push(byte);
        }
        Ok(bytes)
    }

    fn parse_fixed_hex_u32(&mut self, digits: usize) -> crate::Result<u32> {
        let start = self.pos;
        for _ in 0..digits {
            let Some(ch) = self.next_char() else {
                return Err(self.error("short unicode escape"));
            };
            if !ch.is_ascii_hexdigit() {
                return Err(self.error("invalid unicode escape"));
            }
        }
        u32::from_str_radix(&self.input[start..self.pos], 16)
            .map_err(|_| self.error("invalid unicode escape"))
    }

    fn parse_ident(&mut self) -> crate::Result<String> {
        self.skip_ws();
        let start = self.pos;
        let Some(first) = self.peek_char() else {
            return Err(self.error("expected identifier"));
        };
        if !(first.is_ascii_alphabetic() || first == '_') {
            return Err(self.error("expected identifier"));
        }
        self.next_char();
        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                self.next_char();
            } else {
                break;
            }
        }
        Ok(self.input[start..self.pos].to_owned())
    }

    fn expect_keyword(&mut self, keyword: &str) -> crate::Result<()> {
        if self.consume_keyword(keyword) {
            Ok(())
        } else {
            Err(self.error_at(format!("expected `{keyword}`")))
        }
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        self.skip_ws();
        let rest = &self.input[self.pos..];
        if !rest.starts_with(keyword) {
            return false;
        }
        let end = self.pos + keyword.len();
        if self.input[end..]
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            return false;
        }
        self.pos = end;
        true
    }

    fn peek_keyword(&mut self, keyword: &str) -> bool {
        self.skip_ws();
        let rest = &self.input[self.pos..];
        rest.starts_with(keyword)
            && !self.input[self.pos + keyword.len()..]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    }

    fn expect_char(&mut self, expected: char) -> crate::Result<()> {
        if self.consume_char(expected) {
            Ok(())
        } else {
            Err(self.error_at(format!("expected `{expected}`")))
        }
    }

    fn consume_char(&mut self, expected: char) -> bool {
        self.skip_ws();
        if self.peek_char() == Some(expected) {
            self.next_char();
            true
        } else {
            false
        }
    }

    fn skip_ws(&mut self) {
        while self.peek_char().is_some_and(char::is_whitespace) {
            self.next_char();
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn next_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn error(&self, message: &str) -> Error {
        self.error_at(message.to_owned())
    }

    fn error_at(&self, message: String) -> Error {
        Error::Text(format!("{message} at byte {}", self.pos))
    }
}

fn value_matches_element_type(value: &Value, element_type: &ElementType) -> bool {
    match (value, element_type) {
        (Value::Boolean(_), ElementType::Builtin(TypeId::Boolean))
        | (Value::Uint8(_), ElementType::Builtin(TypeId::Uint8))
        | (Value::Int8(_), ElementType::Builtin(TypeId::Int8))
        | (Value::Uint16(_), ElementType::Builtin(TypeId::Uint16))
        | (Value::Int16(_), ElementType::Builtin(TypeId::Int16))
        | (Value::Uint32(_), ElementType::Builtin(TypeId::Uint32))
        | (Value::Int32(_), ElementType::Builtin(TypeId::Int32))
        | (Value::Uint64(_), ElementType::Builtin(TypeId::Uint64))
        | (Value::Int64(_), ElementType::Builtin(TypeId::Int64))
        | (Value::Float(_), ElementType::Builtin(TypeId::Float))
        | (Value::Double(_), ElementType::Builtin(TypeId::Double))
        | (Value::String(_), ElementType::Builtin(TypeId::String))
        | (Value::Bytes(_), ElementType::Builtin(TypeId::Bytes))
        | (Value::Object(_), ElementType::Object) => true,
        (Value::List(list), ElementType::List(inner)) => list.element_type == **inner,
        (Value::Array(array), ElementType::Array(inner)) => array.element_type == **inner,
        (Value::Optional(optional), ElementType::Optional(inner)) => {
            optional.element_type == **inner
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Payload, PayloadView};

    #[test]
    fn value_text_round_trips_ordered_object() {
        let input = r#"{"name": "node-a", "blob": bytes(0x00FF10), "nums": list<int32>[int32(-1), int32(2)], "maybe": optional<uint16>(none)}"#;

        let value = parse_value(input).unwrap();
        let Value::Object(object) = &value else {
            panic!("expected object");
        };
        let keys = object
            .fields
            .iter()
            .map(|field| field.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, ["name", "blob", "nums", "maybe"]);

        let text = value_to_text(&value);
        let reparsed = parse_value(&text).unwrap();
        assert_eq!(reparsed, value);
    }

    #[test]
    fn array_text_round_trips() {
        let value =
            parse_value("array<uint16>(size=2,dims=[2,2],data=0x0100020003000400)").unwrap();

        let Value::Array(array) = &value else {
            panic!("expected array");
        };
        assert_eq!(array.element_type, ElementType::Builtin(TypeId::Uint16));
        assert_eq!(array.element_size, 2);
        assert_eq!(array.dimensions, [2, 2]);

        let text = value_to_text(&value);
        assert_eq!(parse_value(&text).unwrap(), value);
    }

    #[test]
    fn document_text_round_trips_schema_payload() {
        let document = Document {
            metadata: Metadata::new(3, LengthMode::U64).aligned(),
            schema: Schema {
                types: vec![SchemaType {
                    id: 9,
                    name: "pair".to_owned(),
                    payload_size: 8,
                    fields: vec![
                        SchemaField {
                            name: "x".to_owned(),
                            element_type: ElementType::Builtin(TypeId::Int32),
                        },
                        SchemaField {
                            name: "y".to_owned(),
                            element_type: ElementType::Builtin(TypeId::Int32),
                        },
                    ],
                }],
            },
            payload: Payload::Schema {
                root_schema_type_id: 9,
                root_count: 1,
                data: [1i32, -2i32]
                    .into_iter()
                    .flat_map(i32::to_le_bytes)
                    .collect(),
            },
        };

        let text = document_to_text(&document);
        let parsed = parse_document(&text).unwrap();
        assert_eq!(parsed.metadata.version_id, 3);
        assert_eq!(parsed.metadata.length_mode, LengthMode::U64);
        assert_eq!(parsed.metadata.alignment_hint, AlignmentHint::Aligned);
        assert_eq!(parsed.schema, document.schema);
        assert_eq!(parsed.payload, document.payload);

        let bytes = parsed.to_bytes().unwrap();
        let view = crate::DocumentView::parse(&bytes).unwrap();
        let PayloadView::Schema {
            root_schema_type_id,
            root_count,
            data,
        } = view.payload
        else {
            panic!("expected schema payload");
        };
        assert_eq!(root_schema_type_id, 9);
        assert_eq!(root_count, 1);
        assert_eq!(data.len(), 8);
    }

    #[test]
    fn document_text_round_trips_dynamic_payload() {
        let input = r#"document(version=1,length=uint32,align=false,schema=[],payload={"ok": true, "count": uint64(5)})"#;
        let document = parse_document(input).unwrap();
        let text = document_to_text(&document);
        let reparsed = parse_document(&text).unwrap();
        assert_eq!(reparsed.metadata.version_id, 1);
        assert_eq!(reparsed.schema.types.len(), 0);
        assert_eq!(reparsed.payload, document.payload);
    }

    #[test]
    fn typed_text_rejects_mismatched_list_values() {
        let err = parse_value("list<uint8>[uint8(1), uint16(2)]").unwrap_err();
        assert!(matches!(err, Error::Text(message) if message.contains("list value")));
    }
}
