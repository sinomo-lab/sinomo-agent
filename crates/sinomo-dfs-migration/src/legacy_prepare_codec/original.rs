//! Source-only expansion of original packed rows. Attachments stay JSON data;
//! no current request, pin, version, or module-payload DTO is decoded here.
use super::{PackedTimeV2, unpack_time};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

type Result<T> = std::result::Result<T, String>;
pub(crate) struct OriginalPackedPrepare {
    pub(crate) value: Value,
    /// Expanded canonical payload pointer -> ORIGINAL packed-body pointer.
    pub(crate) payload_sources: BTreeMap<String, String>,
}
fn obj(value: &Value) -> Result<&Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| "original packed object expected".into())
}
fn get(value: &Value, key: &str) -> Result<Value> {
    value
        .get(key)
        .cloned()
        .ok_or_else(|| format!("original packed field {key} missing"))
}
fn array<'a>(value: &'a Value, key: &str) -> Result<&'a [Value]> {
    match value.get(key) {
        None => Ok(&[]),
        Some(Value::Array(rows)) => Ok(rows),
        _ => Err(format!("original packed list {key} invalid")),
    }
}
fn list(value: &Value, key: &str) -> Result<Vec<Value>> {
    match value.get(key) {
        None => Ok(Vec::new()),
        Some(Value::Array(rows)) => Ok(rows.clone()),
        _ => Err(format!("original packed list {key} invalid")),
    }
}
fn uint(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| "original packed unsigned integer expected".into())
}
fn field_number(value: &Value, key: &str, default: bool) -> Result<Value> {
    match value.get(key) {
        None if default => Ok(json!(0)),
        Some(v) if v.as_u64().is_some() => Ok(v.clone()),
        _ => Err(format!("invalid original packed number {key}")),
    }
}
fn allowed(value: &Value, fields: &[&str]) -> Result<()> {
    if obj(value)?
        .keys()
        .any(|key| !fields.contains(&key.as_str()))
    {
        return Err("unknown original packed field".into());
    }
    Ok(())
}
fn source_scope(value: &Value, fs: &Value) -> Result<()> {
    if value
        .get("file_system_id")
        .is_some_and(|actual| actual != fs)
    {
        return Err("original packed attachment crosses filesystem".into());
    }
    Ok(())
}
fn time(value: &Value) -> Result<Value> {
    let packed: PackedTimeV2 = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
    let instant = unpack_time(packed, "original source time", 0).map_err(|e| e.to_string())?;
    Ok(Value::String(
        instant
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|e| e.to_string())?,
    ))
}
fn optional(value: &Value, key: &str) -> Value {
    value.get(key).cloned().unwrap_or(Value::Null)
}
fn string(value: &Value, key: &str) -> Result<Value> {
    let v = get(value, key)?;
    if v.as_str().is_none() {
        return Err(format!("original packed string {key} missing"));
    }
    Ok(v)
}
fn escaped(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}
fn payloads(
    map: &Value,
    source: &str,
    target: &str,
    out: &mut BTreeMap<String, String>,
) -> Result<()> {
    for (namespace, value) in obj(map)? {
        if value.get("payload").is_none() {
            return Err("original module metadata omitted payload".into());
        }
        let namespace = escaped(namespace);
        out.insert(
            format!("{target}/{namespace}/payload"),
            format!("{source}/{namespace}/payload"),
        );
    }
    Ok(())
}
fn common(outer: &Value, body: &Value, digest_key: &str) -> Result<Value> {
    let fs = string(outer, "file_system_id")?;
    let operation = string(outer, "operation_id")?;
    if operation
        .as_str()
        .is_some_and(|s| s.trim().is_empty() || s.contains('\0'))
    {
        return Err("original operation identity invalid".into());
    }
    let status = string(outer, "status")?;
    if !["prepared", "committed", "aborted", "superseded"].contains(&status.as_str().unwrap()) {
        return Err("original prepare status invalid".into());
    }
    Ok(
        json!({"operation_id":operation,"file_system_id":fs,"shard_id":get(body,"s")?,
        "owner_epoch":field_number(body,"e",false)?,"operation_digest":string(body,digest_key)?,
        "expected_object_versions":[],"expected_path_occupants":[],"content_receipts":[],
        "transaction":{},"completed_request":optional(body,"q"),"completed_request_completion":optional(body,"c"),"status":status}),
    )
}
fn expand_v1(outer: &Value, body: &Value) -> Result<OriginalPackedPrepare> {
    allowed(body, &["f", "s", "e", "d", "o", "p", "r", "t", "q", "c"])?;
    if uint(&get(body, "f")?)? != 1 {
        return Err("original packed-v1 version unsupported".into());
    }
    let mut value = common(outer, body, "d")?;
    let fs = value["file_system_id"].clone();
    let tx = get(body, "t")?;
    allowed(&tx, &["i", "o", "v", "file_version_pins", "l", "t"])?;
    let mut objects = Vec::new();
    for row in list(&tx, "o")? {
        allowed(
            &row,
            &[
                "o", "p", "k", "t", "n", "l", "h", "z", "a", "m", "u", "g", "aa", "mm", "cc", "xx",
                "v", "y", "c", "e",
            ],
        )?;
        objects.push(json!({"object_id":string(&row,"o")?,"file_system_id":fs,"path":string(&row,"p")?,
            "kind":get(&row,"k")?,"tombstone_provenance":optional(&row,"t"),"owner_node_id":string(&row,"n")?,
            "current_locations":list(&row,"l")?,"content_hash":optional(&row,"h"),"content_size_bytes":optional(&row,"z"),
            "link_target_object_id":optional(&row,"a"),"mode":field_number(&row,"m",true)?,"uid":field_number(&row,"u",true)?,
            "gid":field_number(&row,"g",true)?,"accessed_at_unix_nanos":field_number(&row,"aa",true)?,
            "modified_at_unix_nanos":field_number(&row,"mm",true)?,"created_at_unix_nanos":field_number(&row,"cc",true)?,
            "changed_at_unix_nanos":field_number(&row,"xx",true)?,"version":string(&row,"v")?,"policy":get(&row,"y")?,
            "created_by_event_id":optional(&row,"c"),"last_modified_by_event_id":optional(&row,"e")}));
    }
    let mut versions = Vec::new();
    for row in list(&tx, "v")? {
        allowed(
            &row,
            &["g", "o", "v", "h", "z", "l", "e", "t", "b", "r", "d"],
        )?;
        versions.push(json!({"file_system_id":fs,"object_id":string(&row,"o")?,"version":string(&row,"v")?,
            "content_hash":optional(&row,"h"),"size_bytes":optional(&row,"z"),"locations":list(&row,"l")?,
            "created_by_event_id":optional(&row,"e"),"created_at":get(&row,"t")?,"visible":get(&row,"b")?,
            "history_order":field_number(&row,"g",true)?,"retained":optional(&row,"r"),"content_durability":optional(&row,"d"),
            "retention":{},"module_metadata":{}}));
    }
    value["transaction"] = json!({"transaction_id":string(&tx,"i")?,"file_system_id":fs,"objects":objects,"versions":versions,
        "advisory_lock_states":list(&tx,"l")?,"file_version_pins":optional(&tx,"file_version_pins"),"created_at":get(&tx,"t")?,
        "content_reference_identity":null,"contribution_digest":null,"extension_expectations":[]});
    value["expected_object_versions"] = Value::Array(
        list(body, "o")?
            .into_iter()
            .map(|row| {
                Ok(json!({"object_id":string(&row,"o")?,"expected_version":optional(&row,"v")}))
            })
            .collect::<Result<_>>()?,
    );
    value["expected_path_occupants"] = Value::Array(
        list(body, "p")?
            .into_iter()
            .map(|row| {
                Ok(json!({"path":string(&row,"p")?,"expected_object_id":optional(&row,"o")}))
            })
            .collect::<Result<_>>()?,
    );
    value["content_receipts"]=Value::Array(list(body,"r")?.into_iter().map(|row|Ok(json!({
        "receipt":{"file_system_id":fs,"object_id":string(&row,"o")?,"committed_version":string(&row,"v")?,
        "owner_node_id":string(&row,"n")?,"event_id":string(&row,"e")?},"replica_node_id":string(&row,"r")?,
        "content_hash":optional(&row,"h"),"received_at":get(&row,"t")?}))).collect::<Result<_>>()?);
    for row in value["content_receipts"].as_array().unwrap() {
        source_scope(&row["receipt"], &fs)?;
    }
    Ok(OriginalPackedPrepare {
        value,
        payload_sources: BTreeMap::new(),
    })
}
struct Indexed<'a> {
    dictionaries: &'a Value,
}
impl Indexed<'_> {
    fn at(&self, name: &str, index: &Value) -> Result<Value> {
        let index = usize::try_from(uint(index)?).map_err(|e| e.to_string())?;
        let entries = array(self.dictionaries, name)?;
        entries
            .get(index)
            .cloned()
            .ok_or_else(|| format!("original dictionary {name} reference out of bounds"))
    }
    fn locations(&self, index: &Value) -> Result<Value> {
        let sets = get(self.dictionaries, "l")?;
        let offsets = array(&sets, "o")?;
        let nodes = array(&sets, "n")?;
        let index = usize::try_from(uint(index)?).map_err(|e| e.to_string())?;
        let start = offsets.get(index).ok_or("original location-set absent")?;
        let end = offsets
            .get(index + 1)
            .ok_or("original location-set end absent")?;
        let range = nodes
            .get(
                usize::try_from(uint(start)?).map_err(|e| e.to_string())?
                    ..usize::try_from(uint(end)?).map_err(|e| e.to_string())?,
            )
            .ok_or("original location-set range invalid")?;
        range
            .iter()
            .map(|node| self.at("n", node))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array)
    }
}
fn dense(table: &Value, key: &str, rows: usize) -> Result<Vec<Value>> {
    let values = list(table, key)?;
    if values.len() != rows {
        return Err(format!("original dense column {key} length differs"));
    }
    Ok(values)
}
fn bitmap(table: &Value, key: &str, rows: usize) -> Result<Vec<Value>> {
    let column = get(table, key)?;
    allowed(&column, &["v", "m"])?;
    let values = dense(&column, "v", rows)?;
    let missing = list(&column, "m")?;
    if !missing.is_empty() && missing.len() != rows.div_ceil(64) {
        return Err("original missing bitmap length differs".into());
    }
    if rows % 64 != 0
        && missing
            .last()
            .is_some_and(|last| uint(last).is_ok_and(|word| word >> (rows % 64) != 0))
    {
        return Err("original missing bitmap has excess bits".into());
    }
    values
        .into_iter()
        .enumerate()
        .map(|(row, value)| {
            let absent = missing
                .get(row / 64)
                .map(uint)
                .transpose()?
                .is_some_and(|word| word & (1u64 << (row % 64)) != 0);
            Ok(if absent { Value::Null } else { value })
        })
        .collect()
}
fn sparse(table: &Value, key: &str, rows: usize) -> Result<BTreeMap<usize, Value>> {
    let mut out = BTreeMap::new();
    let mut previous = None;
    for row in list(table, key)? {
        allowed(&row, &["i", "v"])?;
        let index = usize::try_from(uint(&get(&row, "i")?)?).map_err(|e| e.to_string())?;
        if index >= rows || previous.is_some_and(|last| index <= last) {
            return Err(format!("original sparse column {key} not strictly ordered"));
        }
        out.insert(index, get(&row, "v")?);
        previous = Some(index);
    }
    Ok(out)
}
fn tags(value: &Value, variants: &[&str]) -> Result<Value> {
    variants
        .get(usize::try_from(uint(value)?).map_err(|e| e.to_string())?)
        .map(|v| json!(v))
        .ok_or_else(|| "original enum tag unsupported".into())
}
fn expand_v2(outer: &Value, body: &Value) -> Result<OriginalPackedPrepare> {
    allowed(
        body,
        &[
            "u",
            "w",
            "j",
            "f",
            "s",
            "e",
            "g",
            "d",
            "x",
            "p",
            "r",
            "i",
            "o",
            "v",
            "file_version_pins",
            "l",
            "t",
            "q",
            "c",
        ],
    )?;
    let format = uint(&get(body, "f")?)?;
    if ![2, 3, 4, 5, 6].contains(&format) {
        return Err("original packed-v2 version unsupported".into());
    }
    let mut value = common(outer, body, "g")?;
    let fs = value["file_system_id"].clone();
    let dictionaries = get(body, "d")?;
    allowed(&dictionaries, &["o", "p", "v", "h", "n", "e", "l"])?;
    for field in ["o", "p", "v", "h", "n", "e"] {
        let rows = list(&dictionaries, field)?;
        let mut unique = BTreeSet::new();
        if rows
            .iter()
            .any(|row| row.as_str().is_none() || !unique.insert(row.as_str().unwrap().to_owned()))
        {
            return Err("original dictionary repeats or has non-string values".into());
        }
    }
    let indexed = Indexed {
        dictionaries: &dictionaries,
    };
    let sets = get(&dictionaries, "l")?;
    allowed(&sets, &["o", "n"])?;
    let offsets = list(&sets, "o")?;
    let nodes = list(&sets, "n")?;
    if offsets.first().and_then(Value::as_u64) != Some(0)
        || offsets.last().and_then(Value::as_u64) != Some(nodes.len() as u64)
        || offsets.windows(2).any(|pair| {
            pair[0].as_u64().is_none()
                || pair[1].as_u64().is_none()
                || pair[0].as_u64() > pair[1].as_u64()
        })
    {
        return Err("original location offsets malformed".into());
    }
    for node in &nodes {
        indexed.at("n", node)?;
    }
    let mut payload_sources = BTreeMap::new();
    let objects = get(body, "o")?;
    let count = list(&objects, "o")?.len();
    allowed(
        &objects,
        &[
            "o", "p", "k", "n", "l", "h", "z", "a", "m", "u", "g", "aa", "mm", "cc", "xx", "v",
            "y", "c", "e", "t",
        ],
    )?;
    let mut columns = BTreeMap::new();
    for key in [
        "o", "p", "k", "n", "l", "m", "u", "g", "aa", "mm", "cc", "xx", "v", "y",
    ] {
        columns.insert(key, dense(&objects, key, count)?);
    }
    for key in ["h", "z", "a", "c", "e"] {
        columns.insert(key, bitmap(&objects, key, count)?);
    }
    let mut tombstones = BTreeMap::new();
    let mut previous = None;
    for item in list(&objects, "t")? {
        allowed(&item, &["r", "p"])?;
        let row = usize::try_from(uint(&get(&item, "r")?)?).map_err(|e| e.to_string())?;
        if row >= count || previous.is_some_and(|last| row <= last) {
            return Err("original tombstone sparse order invalid".into());
        }
        tombstones.insert(row, get(&item, "p")?);
        previous = Some(row);
    }
    let mut expanded_objects = Vec::new();
    for row in 0..count {
        let mut object = json!({"file_system_id":fs,"object_id":indexed.at("o",&columns["o"][row])?,"path":indexed.at("p",&columns["p"][row])?,
            "kind":tags(&columns["k"][row],&["file","directory","symlink","hardlink","artifact","mount","tombstone"])?});
        for (target, key, dictionary) in [("owner_node_id", "n", "n"), ("version", "v", "v")] {
            object[target] = indexed.at(dictionary, &columns[key][row])?;
        }
        object["policy"] = tags(
            &columns["y"][row],
            &[
                "local-owned",
                "replicated-confirmed",
                "replicated-eventual",
                "append-only",
                "ephemeral-local",
            ],
        )?;
        object["current_locations"] = indexed.locations(&columns["l"][row])?;
        for (target, key, dictionary) in [
            ("content_hash", "h", "h"),
            ("link_target_object_id", "a", "o"),
            ("created_by_event_id", "c", "e"),
            ("last_modified_by_event_id", "e", "e"),
        ] {
            object[target] = if columns[key][row].is_null() {
                Value::Null
            } else {
                indexed.at(dictionary, &columns[key][row])?
            };
        }
        for (target, key) in [
            ("content_size_bytes", "z"),
            ("mode", "m"),
            ("uid", "u"),
            ("gid", "g"),
            ("accessed_at_unix_nanos", "aa"),
            ("modified_at_unix_nanos", "mm"),
            ("created_at_unix_nanos", "cc"),
            ("changed_at_unix_nanos", "xx"),
        ] {
            object[target] = columns[key][row].clone();
        }
        object["tombstone_provenance"] = tombstones.remove(&row).unwrap_or(Value::Null);
        expanded_objects.push(object);
    }
    let versions = get(body, "v")?;
    let count = list(&versions, "o")?.len();
    allowed(
        &versions,
        &[
            "x", "m", "g", "o", "v", "h", "z", "l", "e", "t", "b", "r", "d",
        ],
    )?;
    let mut columns = BTreeMap::new();
    for key in ["o", "v", "l", "t", "b"] {
        columns.insert(key, dense(&versions, key, count)?);
    }
    for key in ["h", "z", "e"] {
        columns.insert(key, bitmap(&versions, key, count)?);
    }
    let retained = sparse(&versions, "r", count)?;
    let durabilities = sparse(&versions, "d", count)?;
    let retention = sparse(&versions, "x", count)?;
    let metadata = sparse(&versions, "m", count)?;
    let mut orders = BTreeMap::new();
    let mut previous = None;
    for item in list(&versions, "g")? {
        let pair = item
            .as_array()
            .filter(|pair| pair.len() == 2)
            .ok_or("original history order pair invalid")?;
        let index = usize::try_from(uint(&pair[0])?).map_err(|e| e.to_string())?;
        if index >= count || previous.is_some_and(|last| index <= last) {
            return Err("original history order sparse rows invalid".into());
        }
        orders.insert(index, pair[1].clone());
        previous = Some(index);
    }
    // Format 6 authors namespaced version metadata. Its canonical source
    // shape has no legacy history fields, even when both namespaces are empty.
    // Retain a source with legacy sparse data rather than discarding that data.
    if format == 6 && (!orders.is_empty() || !retained.is_empty()) {
        return Err("original packed-v6 contains legacy revision metadata".into());
    }
    let metadata_rows = list(&versions, "m")?;
    let mut expanded_versions = Vec::new();
    for row in 0..count {
        let module_metadata = metadata.get(&row).cloned().unwrap_or(json!({}));
        if let Some(sparse_index) = metadata_rows
            .iter()
            .position(|item| item.get("i").and_then(Value::as_u64) == Some(row as u64))
        {
            payloads(
                &module_metadata,
                &format!("/v/m/{sparse_index}/v"),
                &format!("/transaction/versions/{row}/module_metadata"),
                &mut payload_sources,
            )?;
        }
        expanded_versions.push(json!({"file_system_id":fs,"object_id":indexed.at("o",&columns["o"][row])?,"version":indexed.at("v",&columns["v"][row])?,
            "content_hash":if columns["h"][row].is_null(){Value::Null}else{indexed.at("h",&columns["h"][row])?},"size_bytes":columns["z"][row],
            "locations":indexed.locations(&columns["l"][row])?,"created_by_event_id":if columns["e"][row].is_null(){Value::Null}else{indexed.at("e",&columns["e"][row])?},
            "created_at":time(&columns["t"][row])?,"visible":columns["b"][row],
            "content_durability":durabilities.get(&row).cloned().unwrap_or(Value::Null),
            "retention":retention.get(&row).cloned().unwrap_or(json!({})),"module_metadata":module_metadata}));
        if format != 6 {
            let version = expanded_versions
                .last_mut()
                .expect("expanded source version");
            version["history_order"] = orders.get(&row).cloned().unwrap_or(json!(0));
            version["retained"] = retained.get(&row).cloned().unwrap_or(Value::Null);
        }
    }
    value["transaction"] = json!({"transaction_id":string(body,"i")?,"file_system_id":fs,"objects":expanded_objects,"versions":expanded_versions,
        "advisory_lock_states":list(body,"l")?,"file_version_pins":optional(body,"file_version_pins"),"created_at":time(&get(body,"t")?)?,
        "content_reference_identity":optional(body,"u"),"contribution_digest":optional(body,"j"),"extension_expectations":list(body,"w")?});
    for (field, table_name, index_name, optional_name, key_dictionary, optional_dictionary) in [
        ("expected_object_versions", "x", "o", "v", "o", "v"),
        ("expected_path_occupants", "p", "p", "o", "p", "o"),
    ] {
        let mut table = get(body, table_name)?;
        allowed(&table, &[index_name, optional_name])?;
        // These two original expectation bitmap fields were explicitly
        // defaulted by their source schemas, including the empty table case.
        if table.get(optional_name).is_none() {
            table[optional_name] = json!({});
        }
        let keys = list(&table, index_name)?;
        let optional_values = bitmap(&table, optional_name, keys.len())?;
        value[field] = Value::Array(
            keys.iter()
                .zip(optional_values)
                .map(|(key, optional_value)| {
                    let primary = indexed.at(key_dictionary, key)?;
                    let secondary = if optional_value.is_null() {
                        Value::Null
                    } else {
                        indexed.at(optional_dictionary, &optional_value)?
                    };
                    Ok(if field == "expected_object_versions" {
                        json!({"object_id":primary,"expected_version":secondary})
                    } else {
                        json!({"path":primary,"expected_object_id":secondary})
                    })
                })
                .collect::<Result<_>>()?,
        );
    }
    let receipts = get(body, "r")?;
    let count = list(&receipts, "o")?.len();
    allowed(&receipts, &["o", "v", "n", "e", "r", "h", "t"])?;
    let mut columns = BTreeMap::new();
    for key in ["o", "v", "n", "e", "r", "t"] {
        columns.insert(key, dense(&receipts, key, count)?);
    }
    columns.insert("h", bitmap(&receipts, "h", count)?);
    let mut expanded_receipts = Vec::new();
    for row in 0..count {
        expanded_receipts.push(json!({"receipt":{"file_system_id":fs,
        "object_id":indexed.at("o",&columns["o"][row])?,"committed_version":indexed.at("v",&columns["v"][row])?,"owner_node_id":indexed.at("n",&columns["n"][row])?,"event_id":indexed.at("e",&columns["e"][row])?},
        "replica_node_id":indexed.at("n",&columns["r"][row])?,"content_hash":if columns["h"][row].is_null(){Value::Null}else{indexed.at("h",&columns["h"][row])?},"received_at":time(&columns["t"][row])?}));
    }
    value["content_receipts"] = Value::Array(expanded_receipts);
    Ok(OriginalPackedPrepare {
        value,
        payload_sources,
    })
}
pub(crate) fn unpack_original(outer: &Value) -> Result<OriginalPackedPrepare> {
    let v1 = outer.get("packed_prepare_v1").filter(|v| !v.is_null());
    let v2 = outer.get("packed_prepare_v2").filter(|v| !v.is_null());
    let mut result = match (v1, v2) {
        (Some(body), None) => expand_v1(outer, body)?,
        (None, Some(body)) => expand_v2(outer, body)?,
        _ => return Err("original packed mutation must have exactly one body".into()),
    };
    for object in result.value["transaction"]["objects"]
        .as_array_mut()
        .unwrap()
    {
        let locations = object["current_locations"]
            .as_array_mut()
            .ok_or("original object locations invalid")?;
        let mut seen = BTreeSet::new();
        locations.retain(|node| seen.insert(node.to_string()));
        if object["kind"] != json!("tombstone") {
            object["tombstone_provenance"] = Value::Null;
        }
    }
    for version in result.value["transaction"]["versions"]
        .as_array_mut()
        .unwrap()
    {
        let locations = version["locations"]
            .as_array_mut()
            .ok_or("original version locations invalid")?;
        let mut seen = BTreeSet::new();
        locations.retain(|node| seen.insert(node.to_string()));
    }
    for lock in result.value["transaction"]["advisory_lock_states"]
        .as_array_mut()
        .unwrap()
    {
        lock["authority_rebind_required"] = json!(true);
    }
    for path in result.value["expected_path_occupants"]
        .as_array_mut()
        .unwrap()
    {
        let source = path["path"].as_str().ok_or("original path expected")?;
        path["path"] = json!(source.trim_matches('/'));
    }
    result.value["expected_object_versions"]
        .as_array_mut()
        .unwrap()
        .sort_by(|left, right| {
            (
                left["object_id"].as_str(),
                left["expected_version"].as_str(),
            )
                .cmp(&(
                    right["object_id"].as_str(),
                    right["expected_version"].as_str(),
                ))
        });
    result.value["expected_path_occupants"]
        .as_array_mut()
        .unwrap()
        .sort_by(|left, right| {
            (left["path"].as_str(), left["expected_object_id"].as_str())
                .cmp(&(right["path"].as_str(), right["expected_object_id"].as_str()))
        });
    result.value["content_receipts"]
        .as_array_mut()
        .unwrap()
        .sort_by_key(|row| {
            (
                row["receipt"]["object_id"].to_string(),
                row["receipt"]["committed_version"].to_string(),
                row["replica_node_id"].to_string(),
            )
        });
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn captured_prepare() -> Value {
        let source = crate::metadata::source_document::decode_original_document(include_bytes!(
            "testdata/original-f6-services-sequence6.sobj"
        ))
        .unwrap();
        assert_eq!(source["sequence"], 6);
        assert_eq!(source["shard_id"], "services");
        assert_eq!(source["mutation"]["packed_prepare_v2"]["f"], 6);
        source["mutation"].clone()
    }

    #[test]
    fn actual_f3_history_prepare_keeps_its_original_location_priority() {
        let source = crate::metadata::source_document::decode_original_document(include_bytes!(
            "testdata/original-f3-users-sequence746.sobj"
        ))
        .unwrap();
        assert_eq!(source["sequence"], 746);
        let source = &source["mutation"];
        assert_eq!(source["packed_prepare_v2"]["f"], 3);
        let decoded = crate::legacy_wire::decode_original_packed_prepare(source).unwrap();
        assert_eq!(
            decoded.value["operation_digest"],
            source["packed_prepare_v2"]["g"]
        );
        assert_eq!(
            decoded.value["transaction"]["versions"][0]["locations"],
            json!([
                "01kr4w089vqgdxpb84t6bq1qxn",
                "01kr3d54d45zkm0p0xgps8k7mt",
                "01kxfky4pw2rd2cgj85yrs5yk2"
            ])
        );
        // Released normalization removes repeats while retaining the first node's
        // position. This altered input tests that decoder rule, not source custody.
        let mut repeated = source.clone();
        repeated["packed_prepare_v2"]["d"]["l"] = json!({"o":[0,5],"n":[0,1,0,2,1]});
        let expanded = unpack_original(&repeated).unwrap();
        assert_eq!(
            expanded.value["transaction"]["versions"][0]["locations"],
            decoded.value["transaction"]["versions"][0]["locations"]
        );
        let mut changed_order = source.clone();
        changed_order["packed_prepare_v2"]["d"]["l"]["n"] = json!([1, 0, 2]);
        assert!(crate::legacy_wire::decode_original_packed_prepare(&changed_order).is_err());
    }

    #[test]
    fn actual_f6_source_versions_verify_original_digest_without_legacy_fields() {
        let source = captured_prepare();
        let decoded = crate::legacy_wire::decode_original_packed_prepare(&source).unwrap();
        assert_eq!(
            decoded.value["operation_digest"],
            source["packed_prepare_v2"]["g"]
        );
        assert_eq!(decoded.value["operation_id"], source["operation_id"]);
        let versions = decoded.value["transaction"]["versions"].as_array().unwrap();
        assert_eq!(versions.len(), 3);
        for version in versions {
            assert!(version.get("history_order").is_none());
            assert!(version.get("retained").is_none());
            assert_eq!(version["retention"], json!({}));
            assert_eq!(version["module_metadata"], json!({}));
        }
        let current = crate::legacy_wire::normalize_namespace_prepare(decoded.value).unwrap();
        assert_eq!(
            current["operation_digest"],
            source["packed_prepare_v2"]["g"]
        );
        assert_eq!(
            current["transaction"]["versions"].as_array().unwrap().len(),
            3
        );

        let mut changed_path = source.clone();
        changed_path["packed_prepare_v2"]["d"]["p"][0] = json!("/services/changed");
        assert!(crate::legacy_wire::decode_original_packed_prepare(&changed_path).is_err());
        let mut unknown_column = source.clone();
        unknown_column["packed_prepare_v2"]["v"]["unsupported_source_field"] = json!(true);
        assert!(crate::legacy_wire::decode_original_packed_prepare(&unknown_column).is_err());
        let mut changed_version = source;
        changed_version["packed_prepare_v2"]["d"]["v"][0] = json!("00000000000000000000000001");
        assert!(crate::legacy_wire::decode_original_packed_prepare(&changed_version).is_err());
    }

    #[test]
    fn source_generation_keeps_legacy_sparse_data_and_rejects_it_in_f6() {
        let source = captured_prepare();
        for format in 2..=5 {
            let mut legacy = source.clone();
            legacy["packed_prepare_v2"]["f"] = json!(format);
            legacy["packed_prepare_v2"]["v"]["g"] = json!([[0, 17]]);
            let expanded = unpack_original(&legacy).unwrap();
            assert_eq!(
                expanded.value["transaction"]["versions"][0]["history_order"],
                17
            );
            assert_eq!(
                expanded.value["transaction"]["versions"][0]["retained"],
                Value::Null
            );
        }
        // No source digest claim is made for changed generation DATA above.
        // Format 6 cannot silently discard either historical sparse column.
        let mut legacy_order = source.clone();
        legacy_order["packed_prepare_v2"]["v"]["g"] = json!([[0, 17]]);
        assert!(unpack_original(&legacy_order).is_err());
        let mut legacy_capture = source;
        legacy_capture["packed_prepare_v2"]["v"]["r"] =
            json!([{"i":0,"v":{"actual_source_data":true}}]);
        assert!(unpack_original(&legacy_capture).is_err());
    }
}
