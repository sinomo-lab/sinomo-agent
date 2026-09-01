//! Daemon-originated component events.

use std::path::{Path, PathBuf};

use sinomo_core::{
    Confidence, EventEnvelope, EventKind, Id, LatencyClass, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source,
};

use crate::{DaemonError, DaemonPaths, read_daemon_status};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationalSeverity {
    Info,
    Warn,
    Error,
}

impl OperationalSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn append_operational_event_best_effort(
    paths: &DaemonPaths,
    severity: OperationalSeverity,
    component: &str,
    kind: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) {
    let message = message.into();
    let _ = publish_operational_component_event(
        paths,
        severity,
        component,
        kind,
        &message,
        operation_id,
        fields,
    );
}

pub fn append_operational_log_best_effort(
    paths: &DaemonPaths,
    severity: OperationalSeverity,
    component: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) {
    let message = message.into();
    let _ = publish_operational_component_event(
        paths,
        severity,
        component,
        "daemon.log.recorded",
        &message,
        operation_id,
        fields,
    );
}

#[allow(clippy::too_many_arguments)]
fn publish_operational_component_event(
    paths: &DaemonPaths,
    severity: OperationalSeverity,
    component: &str,
    kind: &str,
    message: &str,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) -> bool {
    let data_dir = match daemon_data_dir_path(paths) {
        Ok(data_dir) => data_dir,
        Err(_) => return false,
    };
    let mut payload = serde_json::Map::new();
    payload.insert("severity".to_string(), serde_json::json!(severity.as_str()));
    payload.insert("component".to_string(), serde_json::json!(component));
    payload.insert("message".to_string(), serde_json::json!(message));
    if let Some(operation_id) = operation_id {
        payload.insert("operation_id".to_string(), serde_json::json!(operation_id));
    }
    if !fields.is_null() {
        payload.insert("fields".to_string(), fields);
    }
    let salience = match severity {
        OperationalSeverity::Info => 0.4,
        OperationalSeverity::Warn => 0.75,
        OperationalSeverity::Error => 0.95,
    };
    let event = EventEnvelope::new(
        Id::generate(),
        time::OffsetDateTime::now_utc(),
        operational_origin_node_id(paths, &data_dir),
        Source::new("daemon.operational").expect("static event source"),
        match EventKind::new(kind) {
            Ok(kind) => kind,
            Err(error) => {
                tracing::warn!(%error, kind, "invalid daemon operational event kind");
                return false;
            }
        },
        Salience::new(salience).expect("operational salience"),
        Confidence::new(1.0).expect("operational confidence"),
        RoutingMetadata::new(
            RoutingScope::Local,
            LatencyClass::Background,
            PrivacyClass::Private,
        ),
    )
    .with_payload(serde_json::Value::Object(payload));
    let component_event = sinomo_module::ComponentEvent {
        publisher: sinomo_module::ComponentEventPublisher {
            component_id: component.to_string(),
            component_version: env!("CARGO_PKG_VERSION").to_string(),
            artifact_id: None,
            artifact_generation: 0,
        },
        schema_id: "sinomo.operational-event".to_string(),
        schema_version: "1.0.0".to_string(),
        envelope: event,
    };
    match crate::service_adapter::publish_component_event_for_paths(
        paths,
        component_event,
        sinomo_module::ComponentEventDurability::Durable,
    ) {
        Ok(published) => published,
        Err(error) => {
            tracing::warn!(%error, "daemon component event publish failed");
            false
        }
    }
}

fn daemon_data_dir_path(paths: &DaemonPaths) -> Result<PathBuf, DaemonError> {
    paths
        .root
        .parent()
        .map(PathBuf::from)
        .ok_or_else(|| DaemonError::Io {
            path: paths.root.display().to_string(),
            detail: "daemon root has no data directory parent".to_string(),
        })
}

fn operational_origin_node_id(paths: &DaemonPaths, data_dir: &Path) -> Id {
    if let Ok(status) = read_daemon_status(paths)
        && let Some(cluster) = status.cluster
    {
        return cluster.node_id;
    }
    Id::derived(
        "sinomo.daemon.operational.local-node",
        data_dir.display().to_string().as_bytes(),
    )
}
