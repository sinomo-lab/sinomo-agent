//! Original namespace predicates carried by a conditional current publication.
//! These helpers own no authority or persistence; the native prepared writer
//! checks and reserves their comparisons with the existing publication body.

#[cfg(test)]
#[path = "write_precondition/opened_scope_native_tests.rs"]
mod opened_scope_native_tests;

use crate::{
    DfsCurrentWritePrecondition as C, DfsDirectoryWritePrecondition,
    DfsNamespaceObjectPrecondition as P, DfsNamespaceObjectVersionExpectation,
    DfsNamespacePathOccupantExpectation, DfsObjectKind, DfsObjectMetadata,
    DfsWritePreconditionFailure as F, DfsWriteTargetPrecondition as Target,
};

fn identity(object: &DfsObjectMetadata) -> P {
    P {
        object_id: object.object_id.clone(),
        version: object.version.clone(),
    }
}

pub(crate) fn directories(condition: &C) -> Vec<(&str, &P)> {
    match condition {
        C::Scoped { directories, .. } | C::OpenedScope { directories, .. } => directories
            .iter()
            .map(|directory| (directory.path.as_str(), &directory.expected))
            .collect(),
        C::ParentOnly { path, parent } => vec![(parent_path(path), parent)],
        C::CompareAndSwap { .. } => Vec::new(),
    }
}

fn parent_path(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some(("", _)) => "/",
        Some((parent, _)) => parent,
        None => "/",
    }
}

pub(crate) fn mismatch<E>(
    condition: &C,
    mut lookup: impl FnMut(&str) -> Result<Option<DfsObjectMetadata>, E>,
) -> Result<Option<F>, E> {
    for (path, expected) in directories(condition) {
        let actual = lookup(path)?;
        if actual.as_ref().is_none_or(|object| {
            object.kind != DfsObjectKind::Directory || identity(object) != *expected
        }) {
            return Ok(Some(F::Directory {
                path: path.to_string(),
                expected: expected.clone(),
                actual: actual.as_ref().map(identity),
                actual_kind: actual.map(|object| object.kind),
            }));
        }
    }
    let expected = match condition {
        C::Scoped {
            target: Target::Absent {},
            ..
        }
        | C::OpenedScope {
            target: Target::Absent {},
            ..
        } => None,
        C::Scoped {
            target: Target::Match { expected },
            ..
        }
        | C::OpenedScope {
            target: Target::Match { expected },
            ..
        }
        | C::CompareAndSwap { expected, .. } => Some(expected),
        C::ParentOnly { .. } => return Ok(None),
    };
    let actual = lookup(condition.path())?;
    let matches = match expected {
        Some(expected) => actual.as_ref().is_some_and(|object| {
            matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Artifact)
                && identity(object) == *expected
        }),
        None => actual
            .as_ref()
            .is_none_or(|object| object.kind == DfsObjectKind::Tombstone),
    };
    Ok((!matches).then(|| F::Target {
        path: condition.path().to_string(),
        expected: expected.cloned(),
        actual: actual.as_ref().map(identity),
        actual_kind: actual.map(|object| object.kind),
    }))
}

fn append(
    path: &str,
    expected: &P,
    objects: &mut Vec<DfsNamespaceObjectVersionExpectation>,
    paths: &mut Vec<DfsNamespacePathOccupantExpectation>,
) -> Result<(), String> {
    if let Some(row) = objects
        .iter()
        .find(|row| row.object_id == expected.object_id)
    {
        if row.expected_version.as_ref() != Some(&expected.version) {
            return Err(format!(
                "conditional write has competing observations for {}",
                expected.object_id
            ));
        }
    } else {
        objects.push(DfsNamespaceObjectVersionExpectation {
            object_id: expected.object_id.clone(),
            expected_version: Some(expected.version.clone()),
        });
    }
    if let Some(row) = paths.iter().find(|row| {
        crate::namespace::metadata_path(&row.path) == crate::namespace::metadata_path(path)
    }) {
        if row.expected_object_id.as_ref() != Some(&expected.object_id) {
            return Err(format!(
                "conditional write has competing path observations for {path}"
            ));
        }
    } else {
        paths.push(DfsNamespacePathOccupantExpectation {
            path: crate::namespace::metadata_path(path),
            expected_object_id: Some(expected.object_id.clone()),
        });
    }
    Ok(())
}

pub(crate) fn bind_guard(
    condition: &C,
    objects: &mut Vec<DfsNamespaceObjectVersionExpectation>,
    paths: &mut Vec<DfsNamespacePathOccupantExpectation>,
) -> Result<(), String> {
    condition.validate()?;
    let mut original_objects = Vec::new();
    let mut original_paths = Vec::new();
    for (path, expected) in directories(condition) {
        append(path, expected, &mut original_objects, &mut original_paths)?;
    }
    if let C::Scoped {
        target: Target::Match { expected },
        ..
    }
    | C::OpenedScope {
        target: Target::Match { expected },
        ..
    }
    | C::CompareAndSwap { expected, .. } = condition
    {
        append(
            condition.path(),
            expected,
            &mut original_objects,
            &mut original_paths,
        )?;
    }
    // The transaction footprint was captured at the receiver. Its current
    // comparisons must not rebase the caller's original guarded observations.
    for original in original_objects {
        if let Some(row) = objects
            .iter_mut()
            .find(|row| row.object_id == original.object_id)
        {
            *row = original;
        } else {
            objects.push(original);
        }
    }
    for original in original_paths {
        if let Some(row) = paths
            .iter_mut()
            .find(|row| crate::namespace::metadata_path(&row.path) == original.path)
        {
            *row = original;
        } else {
            paths.push(original);
        }
    }
    // Absent means no live destination. Its actual tombstone predecessor is
    // captured by exclusive-create admission, not replaced with a guessed null.
    Ok(())
}

pub(crate) fn guard_is_bound(
    condition: &C,
    objects: &[DfsNamespaceObjectVersionExpectation],
    paths: &[DfsNamespacePathOccupantExpectation],
) -> bool {
    let mut bound_objects = objects.to_vec();
    let mut bound_paths = paths.to_vec();
    bind_guard(condition, &mut bound_objects, &mut bound_paths).is_ok()
        && bound_objects == objects
        && bound_paths == paths
}

pub(crate) fn scoped_from_reads(
    root: &str,
    path: &str,
    reads: &crate::service_state::NamespaceReadSet,
    file_system_id: &crate::DfsFileSystemId,
    target: Target,
) -> Result<C, String> {
    let mut paths = vec!["/".to_string()];
    let mut prefix = String::new();
    for component in parent_path(path)
        .split('/')
        .filter(|component| !component.is_empty())
    {
        prefix.push('/');
        prefix.push_str(component);
        paths.push(prefix.clone());
    }
    let mut directories = Vec::with_capacity(paths.len());
    for path in paths {
        let object_id = reads
            .paths
            .get(&(
                file_system_id.clone(),
                crate::namespace::metadata_path(&path),
            ))
            .and_then(Option::as_ref)
            .ok_or_else(|| format!("no original directory observation for {path}"))?;
        let version = reads
            .objects
            .get(&(file_system_id.clone(), object_id.clone()))
            .and_then(Option::as_ref)
            .ok_or_else(|| format!("no original directory version for {path}"))?;
        directories.push(DfsDirectoryWritePrecondition {
            path,
            expected: P {
                object_id: object_id.clone(),
                version: version.clone(),
            },
        });
    }
    let condition = C::Scoped {
        root: root.to_string(),
        path: path.to_string(),
        directories,
        target,
    };
    condition.validate()?;
    Ok(condition)
}

/// Retain the actual root capability already observed by the absolute open,
/// without making its outside prefix another effect-time pathname resolution.
pub(crate) fn opened_scope_from_reads(
    root: &str,
    path: &str,
    reads: &crate::service_state::NamespaceReadSet,
    file_system_id: &crate::DfsFileSystemId,
    target: Target,
) -> Result<C, String> {
    let C::Scoped {
        root,
        path,
        mut directories,
        target,
    } = scoped_from_reads(root, path, reads, file_system_id, target)?
    else {
        unreachable!("the full original walk constructor returns Scoped")
    };
    let root_index = directories
        .iter()
        .position(|directory| directory.path == root)
        .ok_or("opened scope lost its original root directory")?;
    directories.drain(..root_index);
    let condition = C::OpenedScope {
        root,
        path,
        directories,
        target,
    };
    condition.validate()?;
    Ok(condition)
}

impl<T: crate::DfsMessageTransport> crate::NetworkedDfsService<T> {
    pub(crate) fn check_current_write_precondition(
        &mut self,
        publication: &crate::DfsWritePublication,
        file_system_id: &crate::DfsFileSystemId,
        object_id: &crate::DfsObjectId,
        path: &str,
        base_version: Option<&crate::DfsVersion>,
    ) -> Result<(), crate::DfsError> {
        publication
            .validate()
            .map_err(|message| crate::DfsError::Metadata { message })?;
        let Some(condition) = publication.current_precondition() else {
            return Ok(());
        };
        if condition.path() != path {
            return Err(crate::DfsError::Metadata {
                message: "conditional write target differs from its object path".into(),
            });
        }
        let target = match condition {
            C::Scoped {
                target: Target::Match { expected },
                ..
            }
            | C::OpenedScope {
                target: Target::Match { expected },
                ..
            }
            | C::CompareAndSwap { expected, .. } => Some(expected),
            C::Scoped {
                target: Target::Absent {},
                ..
            }
            | C::OpenedScope {
                target: Target::Absent {},
                ..
            }
            | C::ParentOnly { .. } => None,
        };
        if target.is_some_and(|expected| {
            &expected.object_id != object_id || Some(&expected.version) != base_version
        }) || (target.is_none() && base_version.is_some())
        {
            return Err(crate::DfsError::Metadata {
                message: "conditional write does not bind its original create or CAS identity"
                    .into(),
            });
        }
        let mut paths = directories(condition)
            .into_iter()
            .map(|(path, _)| path)
            .collect::<Vec<_>>();
        paths.push(path);
        let shards = self.namespace_shards_for_exact_paths(file_system_id, paths)?;
        let shard_id = if self.control_plane.is_some() {
            let [shard] = shards.as_slice() else {
                return Err(crate::DfsError::UnsupportedWritePrecondition {
                    path: path.to_string(),
                    reason: "conditional namespace walk spans physical metadata shards".into(),
                });
            };
            if shard.primary_node_id != self.node_id {
                return Err(crate::DfsError::StaleOwnerEpoch {
                    file_system_id: file_system_id.clone(),
                    local_node_id: self.node_id.clone(),
                    primary_node_id: shard.primary_node_id.clone(),
                    epoch: shard.epoch,
                });
            }
            crate::DfsMetadataShardId::new(&shard.shard_id)
        } else {
            self.local_prepared_operation_shard()?
        };
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "conditional write requires its actual native metadata owner".into(),
            })?;
        if let Some(failure) = mismatch(condition, |path| {
            backend.get_object_by_path_on_shard(&shard_id, file_system_id, path)
        })? {
            return Err(crate::DfsError::WritePreconditionFailed { failure });
        }
        Ok(())
    }
}

/// Negative publication reserves the observation that proved no effect, rather
/// than the original now-false predicate. The immutable publication keeps that
/// original predicate for exact replay and terminal identity checks.
pub(crate) fn bind_failure(
    failure: &F,
    objects: &mut Vec<DfsNamespaceObjectVersionExpectation>,
    paths: &mut Vec<DfsNamespacePathOccupantExpectation>,
) -> Result<(), String> {
    failure.validate()?;
    let (path, actual) = match failure {
        F::Directory { path, actual, .. } | F::Target { path, actual, .. } => (path, actual),
    };
    if let Some(actual) = actual {
        append(path, actual, objects, paths)?;
    } else {
        let path = crate::namespace::metadata_path(path);
        if let Some(row) = paths
            .iter()
            .find(|row| crate::namespace::metadata_path(&row.path) == path)
        {
            if row.expected_object_id.is_some() {
                return Err("failed write changed its negative path observation".into());
            }
        } else {
            paths.push(DfsNamespacePathOccupantExpectation {
                path,
                expected_object_id: None,
            });
        }
    }
    Ok(())
}

pub(crate) fn failure_is_bound(
    failure: &F,
    objects: &[DfsNamespaceObjectVersionExpectation],
    paths: &[DfsNamespacePathOccupantExpectation],
) -> bool {
    let mut observed_objects = objects.to_vec();
    let mut observed_paths = paths.to_vec();
    bind_failure(failure, &mut observed_objects, &mut observed_paths).is_ok()
        && observed_objects.len() == objects.len()
        && observed_paths.len() == paths.len()
}

impl<T: crate::DfsMessageTransport> crate::NetworkedDfsService<T> {
    pub(crate) fn fail_current_write_precondition(
        &mut self,
        intent_id: &str,
        failure: F,
    ) -> Result<(), crate::DfsError> {
        use crate::{DfsError, DfsWriteIntentState as S, DfsWriteIntentStatus as Status};
        let original =
            self.coordinator
                .intent(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Metadata {
                    message: "conditional failure has no original live intent".into(),
                })?;
        let condition =
            original
                .publication
                .current_precondition()
                .ok_or_else(|| DfsError::Metadata {
                    message: "conditional failure has no original publication predicate".into(),
                })?;
        if let S::PreconditionFailed { failure: recorded } = &original.state {
            return if recorded == &failure {
                Ok(())
            } else {
                Err(DfsError::Metadata {
                    message: "conditional failure changes the terminal cause".into(),
                })
            };
        }
        if original.state != S::AwaitingReceipts {
            return Err(DfsError::Metadata {
                message: "conditional failure cannot replace a closed intent".into(),
            });
        }
        let operation_id = format!("precondition-failed:{intent_id}");
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "conditional failure requires its actual durable intent owner".into(),
            })?;
        if backend
            .prepared_operation_record(&format!("write:{intent_id}"))?
            .is_some()
        {
            return Err(DfsError::WriteReconciliationRequired {
                operation_id: original.operation_id.clone().unwrap_or_else(|| intent_id.to_string()),
                object_id: original.object_id.clone(),
                reason: "the exact original content preparation owns reconciliation before any negative outcome".into(),
            });
        }
        if let Some(recorded) = backend.prepared_operation_record(&operation_id)? {
            recorded.validate()?;
            let terminal = recorded
                .metadata_effects
                .iter()
                .find_map(|effect| match effect {
                    crate::DfsPreparedMetadataEffect::PutIntent { intent } => Some(intent),
                    _ => None,
                })
                .ok_or_else(|| DfsError::Metadata {
                    message: "conditional failure preparation has no exact intent".into(),
                })?;
            let mut expected = crate::intent::distributed_intent_from_record(terminal);
            expected.state = S::AwaitingReceipts;
            if expected != original
                || terminal.status
                    != (Status::PreconditionFailed {
                        failure: failure.clone(),
                    })
            {
                return Err(DfsError::Metadata {
                    message: "conditional failure retry changed its original body".into(),
                });
            }
            if recorded.status == crate::DfsPreparedOperationStatus::Prepared {
                if !self.recover_one_prepared_operation(recorded.clone())? {
                    return Err(DfsError::WriteReconciliationRequired {
                        operation_id: original
                            .operation_id
                            .clone()
                            .unwrap_or_else(|| intent_id.to_string()),
                        object_id: original.object_id.clone(),
                        reason:
                            "the current owner has not completed the exact negative preparation"
                                .into(),
                    });
                }
            } else {
                self.commit_stored_prepared_operation(&recorded.digest)?;
            }
        } else {
            let mut terminal = backend
                .get_intent(&original.object_id, intent_id)?
                .ok_or_else(|| DfsError::Metadata {
                    message: "conditional failure has no actual staged intent".into(),
                })?;
            if crate::intent::distributed_intent_from_record(&terminal) != original {
                return Err(DfsError::Metadata {
                    message: "conditional failure differs from its actual staged body".into(),
                });
            }
            let object = terminal
                .object_metadata
                .clone()
                .ok_or_else(|| DfsError::Metadata {
                    message: "conditional failure lost its exact content descriptor".into(),
                })?;
            let scope = self.metadata_effect_scope_for_object(&object, operation_id)?;
            let _execution = self.acquire_prepared_execution(&scope)?;
            let backend = self
                .metadata_backend
                .as_ref()
                .expect("durable intent owner");
            let observed = mismatch(condition, |path| {
                backend.get_object_by_path_on_shard(&scope.shard_id, &original.file_system_id, path)
            })?;
            if observed.as_ref() != Some(&failure) {
                return Err(DfsError::Metadata {
                    message: "conditional failure no longer matches the actual native observation"
                        .into(),
                });
            }
            let current = backend.get_object_on_shard(&scope.shard_id, &original.object_id)?;
            let occupant = backend.get_object_by_path_on_shard(
                &scope.shard_id,
                &original.file_system_id,
                &object.path,
            )?;
            let mut objects = vec![DfsNamespaceObjectVersionExpectation {
                object_id: original.object_id.clone(),
                expected_version: current.map(|row| row.version),
            }];
            let mut paths = vec![DfsNamespacePathOccupantExpectation {
                path: crate::namespace::metadata_path(&object.path),
                expected_object_id: occupant.map(|row| row.object_id),
            }];
            bind_failure(&failure, &mut objects, &mut paths)
                .map_err(|message| DfsError::Metadata { message })?;
            terminal.status = Status::PreconditionFailed {
                failure: failure.clone(),
            };
            terminal.updated_at = Some(time::OffsetDateTime::now_utc());
            self.publish_metadata_effects_on_shard(
                scope,
                objects,
                paths,
                vec![crate::DfsPreparedMetadataEffect::PutIntent { intent: terminal }],
            )?;
        }
        let durable = self
            .metadata_backend
            .as_ref()
            .expect("durable intent owner")
            .get_intent(&original.object_id, intent_id)?
            .ok_or_else(|| DfsError::Metadata {
                message: "conditional failure lost its durable terminal outcome".into(),
            })?;
        let mut expected = original.clone();
        expected.state = S::PreconditionFailed {
            failure: failure.clone(),
        };
        if crate::intent::distributed_intent_from_record(&durable) != expected {
            return Err(DfsError::Metadata {
                message: "conditional failure did not retain its exact terminal outcome".into(),
            });
        }
        self.coordinator
            .fail_intent_precondition(intent_id, failure)?;
        self.remove_pending_write_state(intent_id);
        self.clear_active_replica_stage(&original.object_id, &original.target_version);
        Ok(())
    }
}

pub(crate) fn failure_matches_condition(condition: &C, failure: &F) -> bool {
    if condition.validate().is_err() || failure.validate().is_err() {
        return false;
    }
    match failure {
        F::Directory {
            path,
            expected,
            actual,
            actual_kind,
        } => {
            directories(condition)
                .iter()
                .any(|(guard_path, guard)| guard_path == &path.as_str() && *guard == expected)
                && (actual.as_ref() != Some(expected)
                    || actual_kind.as_ref() != Some(&DfsObjectKind::Directory))
        }
        F::Target {
            path,
            expected,
            actual,
            actual_kind,
        } => {
            if path != condition.path() {
                return false;
            }
            let guard = match condition {
                C::Scoped {
                    target: Target::Absent {},
                    ..
                }
                | C::OpenedScope {
                    target: Target::Absent {},
                    ..
                } => None,
                C::Scoped {
                    target: Target::Match { expected },
                    ..
                }
                | C::OpenedScope {
                    target: Target::Match { expected },
                    ..
                }
                | C::CompareAndSwap { expected, .. } => Some(expected),
                C::ParentOnly { .. } => return false,
            };
            if expected.as_ref() != guard {
                return false;
            }
            match guard {
                Some(guard) => {
                    actual.as_ref() != Some(guard)
                        || !matches!(
                            actual_kind,
                            Some(DfsObjectKind::File | DfsObjectKind::Artifact)
                        )
                }
                None => actual_kind
                    .as_ref()
                    .is_some_and(|kind| *kind != DfsObjectKind::Tombstone),
            }
        }
    }
}

impl<T: crate::DfsMessageTransport> crate::NetworkedDfsService<T> {
    pub(crate) fn recover_current_write_precondition_failure(
        &mut self,
        intent_id: &str,
    ) -> Result<Option<F>, crate::DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).filter(|intent| {
            intent.state == crate::DfsWriteIntentState::AwaitingReceipts
                && intent.publication.current_precondition().is_some()
        }) else {
            return Ok(None);
        };
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        let Some(record) =
            backend.prepared_operation_record(&format!("precondition-failed:{intent_id}"))?
        else {
            return Ok(None);
        };
        record.validate()?;
        let failure = record
            .metadata_effects
            .iter()
            .find_map(|effect| match effect {
                crate::DfsPreparedMetadataEffect::PutIntent { intent: terminal }
                    if terminal.intent_id == intent.intent_id =>
                {
                    match &terminal.status {
                        crate::DfsWriteIntentStatus::PreconditionFailed { failure } => {
                            Some(failure.clone())
                        }
                        _ => None,
                    }
                }
                _ => None,
            })
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "negative preparation lost its exact terminal cause".into(),
            })?;
        self.fail_current_write_precondition(intent_id, failure.clone())?;
        Ok(Some(failure))
    }
}

#[cfg(test)]
mod native_tests;
