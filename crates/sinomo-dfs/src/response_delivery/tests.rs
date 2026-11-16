//! AFTER-only custody tests for the runtime-owned response delivery boundary.
//! Include as a child of response_delivery; these exercise delivery, not write authority.

use super::*;
use crate::{
    DfsDistributedWriteResponse, DfsDistributedWriteResult, DfsNetworkMessage, DfsObjectId,
    DfsRoutedMessage, DfsVersion, NetworkedDfsWriteOutcome,
};
use sinomo_core::node::NodeId;
use std::sync::Arc;
use std::time::Duration;

fn object_id() -> DfsObjectId {
    DfsObjectId::from_legacy_named("delivery-object").unwrap()
}

fn message(request_id: &str, committed: bool) -> DfsRoutedMessage {
    DfsRoutedMessage {
        source_node_id: NodeId::from_legacy_named("delivery-primary").unwrap(),
        target_node_id: NodeId::from_legacy_named("delivery-requester").unwrap(),
        frame_id: 1,
        message_id: 1,
        message: DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
            request_id: request_id.into(),
            object_id: object_id(),
            result: DfsDistributedWriteResult::Outcome {
                outcome: if committed {
                    NetworkedDfsWriteOutcome::Committed {
                        intent_id: "delivery-intent".into(),
                        version: DfsVersion::from_legacy_named("delivery-version").unwrap(),
                        content_hash: crate::content_hash(b"delivery payload"),
                        repair_jobs: Vec::new(),
                    }
                } else {
                    NetworkedDfsWriteOutcome::Pending {
                        intent_id: "delivery-intent".into(),
                        durable_receipts: 0,
                        required_receipts: 1,
                    }
                },
            },
            metadata_journal_batches: Vec::new(),
        }),
    }
}

#[test]
fn response_delivery_isolates_owners_and_rejects_alias_collision_atomically() {
    let router = Arc::new(DfsRoutedResponseDelivery::new());
    let first = DfsRoutedResponseInboxOwner::new();
    let second = DfsRoutedResponseInboxOwner::new();
    let first_ids = vec!["first".into(), "shared-alias".into()];
    let _first = router
        .register(&first, "first", &object_id(), &first_ids)
        .unwrap()
        .unwrap();
    let _second = router
        .register(&second, "second", &object_id(), &["second".into()])
        .unwrap()
        .unwrap();
    assert!(
        router
            .register(
                &second,
                "colliding",
                &object_id(),
                &["unclaimed-before-collision".into(), "shared-alias".into()],
            )
            .is_err()
    );
    assert!(
        !router
            .try_deliver(&message("unclaimed-before-collision", true))
            .unwrap()
    );
    assert!(router.try_deliver(&message("shared-alias", true)).unwrap());
    assert_eq!(first.drain().unwrap(), vec![message("shared-alias", true)]);
    assert_eq!(second.len().unwrap(), 0);
    assert!(router.try_deliver(&message("second", true)).unwrap());
    assert_eq!(second.drain().unwrap(), vec![message("second", true)]);
    assert_eq!(first.len().unwrap(), 0);
}

#[test]
fn response_delivery_stale_lease_drop_cannot_remove_replacement_owner() {
    let router = Arc::new(DfsRoutedResponseDelivery::new());
    let first = DfsRoutedResponseInboxOwner::new();
    let old_lease = router
        .register(
            &first,
            "request",
            &object_id(),
            &["request".into(), "alias".into()],
        )
        .unwrap()
        .unwrap();
    drop(first);
    let replacement = DfsRoutedResponseInboxOwner::new();
    let _replacement_lease = router
        .register(
            &replacement,
            "request",
            &object_id(),
            &["request".into(), "alias".into()],
        )
        .unwrap()
        .unwrap();
    drop(old_lease);
    assert!(router.try_deliver(&message("alias", true)).unwrap());
    assert_eq!(replacement.drain().unwrap(), vec![message("alias", true)]);
}

#[test]
fn response_delivery_overflow_rejects_custody_and_keeps_registration_retryable() {
    let router = Arc::new(DfsRoutedResponseDelivery::new());
    let inbox = DfsRoutedResponseInboxOwner::new();
    let _lease = router
        .register(&inbox, "request", &object_id(), &["request".into()])
        .unwrap()
        .unwrap();
    let response = message("request", true);
    // Saturate the module's real message bound without exceeding its byte bound.
    // Overflow must be an error, never a successful acknowledgement or silent drop.
    for _ in 0..MAX_INBOX_MESSAGES {
        assert!(router.try_deliver(&response).unwrap());
    }
    assert!(router.try_deliver(&response).is_err());
    let accepted = inbox.drain().unwrap();
    assert_eq!(accepted.len(), MAX_INBOX_MESSAGES);
    assert!(accepted.iter().all(|entry| entry == &response));
    assert!(router.try_deliver(&response).unwrap());
    assert_eq!(inbox.drain().unwrap(), vec![response]);
}

#[test]
fn response_delivery_rejects_one_oversized_journal_without_consuming_custody() {
    let router = Arc::new(DfsRoutedResponseDelivery::new());
    let inbox = DfsRoutedResponseInboxOwner::new();
    let _lease = router
        .register(&inbox, "request", &object_id(), &["request".into()])
        .unwrap()
        .unwrap();
    let mut oversized = message("request", true);
    let DfsNetworkMessage::DistributedWriteResponse(response) = &mut oversized.message else {
        unreachable!();
    };
    let payload_bytes = MAX_INBOX_BYTES + 1;
    let shard = crate::DfsMetadataShardId::new("root");
    let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
        crate::DfsPreparedOperationScope {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: shard.clone(),
            owner_epoch: 1,
            operation_id: "delivery-oversized-body".into(),
        },
        Vec::new(),
        Vec::new(),
        vec![crate::DfsPreparedMetadataEffect::PutFileSystem {
            file_system: crate::DfsFileSystem {
                catalog_generation: 0,
                metadata_storage: None,
                file_system_id: crate::sifs_file_system_id(),
                owner_node_id: NodeId::from_legacy_named("delivery-primary").unwrap(),
                display_name: "x".repeat(payload_bytes),
                subtree_policies: Vec::new(),
                retention_policies: Default::default(),
                module_metadata: Default::default(),
            },
        }],
        time::OffsetDateTime::UNIX_EPOCH,
    )
    .unwrap();
    let digest = crate::DfsPreparedOperationDigest::at_sequence(&payload, 1).unwrap();
    response.metadata_journal_batches.push(
        crate::test_support::structural_journal_frame_batch(crate::DfsMetadataJournalBatch {
            shard_id: shard.clone(),
            next_after_sequence: 1,
            entries: vec![crate::DfsMetadataJournalEntry {
                shard_id: shard,
                sequence: 1,
                created_at: time::OffsetDateTime::UNIX_EPOCH,
                mutation: crate::DfsMetadataMutation::PrepareOperationV7 {
                    file_system_id: crate::sifs_file_system_id(),
                    operation_id: payload.operation_id.clone(),
                    record: Box::new(crate::DfsPreparedOperationRecord {
                        digest,
                        payload: Arc::new(payload),
                        status: crate::DfsPreparedOperationStatus::Prepared,
                        committed_entry: None,
                    }),
                },
            }],
        })
        .unwrap(),
    );
    // The journal string alone exceeds the real byte limit. Supplying that
    // conservative wire-size lower bound exercises production custody directly,
    // without allowing the test-only encoder's own limits to cause the failure.
    assert!(
        router
            .try_deliver_with_size(&oversized, payload_bytes)
            .is_err()
    );
    assert_eq!(inbox.len().unwrap(), 0);
    assert!(inbox.drain().unwrap().is_empty());
    drop(oversized);
    let accepted = message("request", true);
    assert!(router.try_deliver(&accepted).unwrap());
    assert_eq!(inbox.drain().unwrap(), vec![accepted]);
}

#[test]
fn response_delivery_wakes_before_wait_and_after_async_wait_registration() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let router = Arc::new(DfsRoutedResponseDelivery::new());
    let inbox = DfsRoutedResponseInboxOwner::new();
    let _lease = router
        .register(&inbox, "request", &object_id(), &["request".into()])
        .unwrap()
        .unwrap();
    assert!(router.try_deliver(&message("request", true)).unwrap());
    assert!(inbox.wait(Duration::ZERO).unwrap());
    assert_eq!(inbox.drain().unwrap().len(), 1);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut wait = Box::pin(inbox.wait_async(Duration::from_secs(1)));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(wait.as_mut().poll(&mut context), Poll::Pending));
        assert!(router.try_deliver(&message("request", true)).unwrap());
        assert!(wait.await.unwrap());
    });
    assert_eq!(inbox.drain().unwrap(), vec![message("request", true)]);
}

#[test]
fn response_delivery_accepts_registered_terminal_alias_before_pending_reply() {
    let router = Arc::new(DfsRoutedResponseDelivery::new());
    let inbox = DfsRoutedResponseInboxOwner::new();
    let terminal_alias = crate::intent::stable_write_intent_id(
        &NodeId::from_legacy_named("delivery-requester").unwrap(),
        &DfsObjectId::from_legacy_named("delivery-object").unwrap(),
        "wire-request",
    );
    let _lease = router
        .register(
            &inbox,
            "wire-request",
            &object_id(),
            &["wire-request".into(), terminal_alias.clone()],
        )
        .unwrap()
        .unwrap();
    let terminal = message(&terminal_alias, true);
    let pending = message("wire-request", false);
    assert!(router.try_deliver(&terminal).unwrap());
    assert!(router.try_deliver(&pending).unwrap());
    assert_eq!(inbox.drain().unwrap(), vec![terminal, pending]);
}

#[test]
fn response_delivery_lease_drop_releases_every_alias_without_redirecting_other_owner() {
    let router = Arc::new(DfsRoutedResponseDelivery::new());
    let first = DfsRoutedResponseInboxOwner::new();
    let second = DfsRoutedResponseInboxOwner::new();
    let lease = router
        .register(
            &first,
            "first",
            &object_id(),
            &["first".into(), "first-alias".into()],
        )
        .unwrap()
        .unwrap();
    let _other = router
        .register(&second, "second", &object_id(), &["second".into()])
        .unwrap()
        .unwrap();
    drop(lease);
    assert!(!router.try_deliver(&message("first", true)).unwrap());
    assert!(!router.try_deliver(&message("first-alias", true)).unwrap());
    assert_eq!(first.len().unwrap(), 0);
    assert!(router.try_deliver(&message("second", true)).unwrap());
    assert_eq!(second.drain().unwrap(), vec![message("second", true)]);
}

#[test]
fn response_delivery_object_selection_does_not_grant_source_authority() {
    let router = Arc::new(DfsRoutedResponseDelivery::new());
    let inbox = DfsRoutedResponseInboxOwner::new();
    let _lease = router
        .register(&inbox, "request", &object_id(), &["request".into()])
        .unwrap()
        .unwrap();
    let mut untrusted = message("request", true);
    untrusted.source_node_id = NodeId::from_legacy_named("unexpected-primary").unwrap();
    assert!(router.try_deliver(&untrusted).unwrap());
    assert_eq!(inbox.drain().unwrap(), vec![untrusted]);
    // Effect rejection belongs to the unchanged owning service handler; the
    // production-ingress regression verifies that separate authority boundary.
    let mut wrong_object = message("request", true);
    let DfsNetworkMessage::DistributedWriteResponse(response) = &mut wrong_object.message else {
        unreachable!();
    };
    response.object_id = DfsObjectId::from_legacy_named("unexpected-object").unwrap();
    assert!(!router.try_deliver(&wrong_object).unwrap());
    assert_eq!(inbox.len().unwrap(), 0);
}

#[test]
fn response_delivery_same_token_on_distinct_objects_keeps_independent_owners() {
    let router = Arc::new(DfsRoutedResponseDelivery::new());
    let first = DfsRoutedResponseInboxOwner::new();
    let second = DfsRoutedResponseInboxOwner::new();
    let first_object = object_id();
    let second_object = DfsObjectId::from_legacy_named("another-delivery-object").unwrap();
    let first_lease = router
        .register(
            &first,
            "shared-token",
            &first_object,
            &["shared-alias".into()],
        )
        .unwrap()
        .unwrap();
    let _second_lease = router
        .register(
            &second,
            "shared-token",
            &second_object,
            &["shared-alias".into()],
        )
        .unwrap()
        .unwrap();
    let first_frame = message("shared-alias", true);
    let mut second_frame = message("shared-alias", true);
    let DfsNetworkMessage::DistributedWriteResponse(response) = &mut second_frame.message else {
        unreachable!();
    };
    response.object_id = second_object;
    assert!(router.try_deliver(&second_frame).unwrap());
    assert!(router.try_deliver(&first_frame).unwrap());
    assert_eq!(first.drain().unwrap(), vec![first_frame.clone()]);
    assert_eq!(second.drain().unwrap(), vec![second_frame.clone()]);
    drop(first_lease);
    assert!(!router.try_deliver(&first_frame).unwrap());
    assert!(router.try_deliver(&second_frame).unwrap());
    assert_eq!(second.drain().unwrap(), vec![second_frame]);
}

#[test]
fn response_delivery_shutdown_revokes_custody_and_wakes_registered_waiter() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let router = Arc::new(DfsRoutedResponseDelivery::new());
    let inbox = DfsRoutedResponseInboxOwner::new();
    let _lease = router
        .register(&inbox, "request", &object_id(), &["request".into()])
        .unwrap()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut wait = Box::pin(inbox.wait_async(Duration::from_secs(30)));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(wait.as_mut().poll(&mut context), Poll::Pending));
        router.shutdown();
        assert!(
            matches!(wait.as_mut().poll(&mut context), Poll::Ready(_)),
            "shutdown must wake the waiter without waiting for its timeout"
        );
    });
    assert!(!matches!(
        router.try_deliver(&message("request", true)),
        Ok(true)
    ));
    let later = DfsRoutedResponseInboxOwner::new();
    assert!(!matches!(
        router.register(&later, "later", &object_id(), &["later".into()]),
        Ok(Some(_))
    ));
    assert_eq!(inbox.len().unwrap(), 0);
}
