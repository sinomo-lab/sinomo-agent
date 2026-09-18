use crate::LanguageModelError;
use reqwest::blocking::Client;
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Clone, Copy)]
pub(crate) enum CatalogProtocol {
    OpenAi,
    Anthropic,
    Gemini,
}

pub(crate) fn endpoint(base: &str, path: &str) -> String {
    let base = base.trim().trim_end_matches('/');
    let path = if base.ends_with("/v1") {
        path.strip_prefix("v1/").unwrap_or(path)
    } else {
        path
    };
    format!("{base}/{path}")
}

/// Follow provider cursors on the same endpoint, never a server-supplied URL.
pub(crate) fn list_models(
    http: &Client,
    url: &str,
    headers: HeaderMap,
    protocol: CatalogProtocol,
    provider: &str,
) -> Result<Vec<String>, LanguageModelError> {
    collect_models(protocol, provider, |cursor| {
        let error = |detail: String| LanguageModelError::ProviderInvocationFailed {
            provider: provider.into(),
            model: crate::PROVIDER_DEFAULT_MODEL.into(),
            detail,
        };
        let mut request = http.get(url).headers(headers.clone());
        if let Some(cursor) = cursor {
            request = request.query(&[(
                if matches!(protocol, CatalogProtocol::Gemini) {
                    "pageToken"
                } else {
                    "after_id"
                },
                cursor,
            )]);
        }
        let response = request.send().map_err(|e| error(e.to_string()))?;
        if !response.status().is_success() {
            return Err(error(format!(
                "model discovery failed: HTTP {}: {}",
                response.status(),
                response.text().unwrap_or_default()
            )));
        }
        response.json().map_err(|e| error(e.to_string()))
    })
}

pub(crate) fn collect_models(
    protocol: CatalogProtocol,
    provider: &str,
    mut fetch: impl FnMut(Option<&str>) -> Result<Value, LanguageModelError>,
) -> Result<Vec<String>, LanguageModelError> {
    let error = |detail: String| LanguageModelError::ProviderInvocationFailed {
        provider: provider.into(),
        model: crate::PROVIDER_DEFAULT_MODEL.into(),
        detail,
    };
    let mut ids = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let mut cursor: Option<String> = None;
    for _ in 0..256 {
        let value = fetch(cursor.as_deref())?;
        let entries = value
            .get(if matches!(protocol, CatalogProtocol::Gemini) {
                "models"
            } else {
                "data"
            })
            .and_then(Value::as_array)
            .ok_or_else(|| error("invalid model catalog: missing model array".into()))?;
        for model in entries {
            if matches!(protocol, CatalogProtocol::Gemini)
                && !model
                    .get("supportedGenerationMethods")
                    .and_then(Value::as_array)
                    .is_some_and(|methods| methods.iter().any(|method| method == "generateContent"))
            {
                continue;
            }
            if let Some(id) = model
                .get(if matches!(protocol, CatalogProtocol::Gemini) {
                    "name"
                } else {
                    "id"
                })
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|id| !id.is_empty())
            {
                ids.insert(
                    if matches!(protocol, CatalogProtocol::Gemini) {
                        id.strip_prefix("models/").unwrap_or(id)
                    } else {
                        id
                    }
                    .to_string(),
                );
            }
        }
        cursor = match protocol {
            CatalogProtocol::Gemini => value
                .get("nextPageToken")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            _ if value.get("has_more").and_then(Value::as_bool) == Some(true) => Some(
                value
                    .get("last_id")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| error("model catalog has_more without last_id".into()))?
                    .to_string(),
            ),
            _ => None,
        };
        let Some(next) = &cursor else {
            return Ok(ids.into_iter().collect());
        };
        if !seen.insert(next.clone()) {
            return Err(error("model catalog repeated a pagination cursor".into()));
        }
    }
    Err(error("model catalog exceeded pagination limit".into()))
}
