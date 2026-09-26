//! Shared OpenAI/Codex tool identity translation. Runtime names stay qualified;
//! the Responses wire represents the namespace and local name separately.
use serde_json::{Value, json};

pub(crate) fn qualify_name(item: &Value) -> Option<String> {
    let name = item
        .get("name")
        .or_else(|| item.get("tool_name"))?
        .as_str()?;
    Some(match item.get("namespace").and_then(Value::as_str) {
        None | Some("" | "functions") => name.to_string(),
        Some(namespace) => format!("{namespace}.{name}"),
    })
}

pub(crate) fn split_name(item: &mut Value) {
    if item.get("namespace").is_some() {
        return;
    }
    let Some((namespace, name)) = item["name"].as_str().and_then(|name| name.split_once('.'))
    else {
        return;
    };
    let (namespace, name) = (namespace.to_string(), name.to_string());
    // Custom results correlate by call_id and have no namespace wire field.
    if item["type"] != "custom_tool_call_output" {
        item["namespace"] = json!(namespace);
    }
    item["name"] = json!(name);
}

pub(crate) fn group_tools(
    tools: impl IntoIterator<Item = Value>,
    default_namespace: bool,
) -> Vec<Value> {
    let mut grouped: Vec<Value> = Vec::new();
    for mut tool in tools {
        if tool["type"] == "namespace" {
            if let Some(existing) = grouped
                .iter_mut()
                .find(|entry| entry["type"] == "namespace" && entry["name"] == tool["name"])
            {
                if let Some(children) = tool["tools"].as_array() {
                    existing["tools"]
                        .as_array_mut()
                        .unwrap()
                        .extend(children.clone());
                }
            } else {
                grouped.push(tool);
            }
            continue;
        }
        split_name(&mut tool);
        let namespace = tool
            .as_object_mut()
            .and_then(|tool| tool.remove("namespace"))
            .or_else(|| default_namespace.then(|| json!("functions")));
        let Some(namespace) = namespace else {
            grouped.push(tool);
            continue;
        };
        if let Some(existing) = grouped
            .iter_mut()
            .find(|entry| entry["type"] == "namespace" && entry["name"] == namespace)
        {
            existing["tools"].as_array_mut().unwrap().push(tool);
        } else {
            grouped.push(
                json!({"type": "namespace", "name": namespace, "description": "", "tools": [tool]}),
            );
        }
    }
    grouped
}
