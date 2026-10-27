//! Source-owned live observations supplement a confirmed semantic advertisement.
//! They never replace configuration entries or authorize a write.

use crate::DfsNodeAdvertisement;
use sinomo_core::network::{ClusterObjectRpcOptions, NetworkError, ObjectRpcCall};
use sinomo_core::node::NodeId;
use time::OffsetDateTime;

pub(crate) fn matches_confirmed_content_advertisement(
    target: &NodeId,
    confirmed: &DfsNodeAdvertisement,
    live: &DfsNodeAdvertisement,
    now: OffsetDateTime,
) -> bool {
    &confirmed.node_id == target
        && &live.node_id == target
        && !confirmed.accepts_metadata
        && !live.accepts_metadata
        && crate::placement_environment::advertisement_semantics_equal(confirmed, live)
        && live.advertisement_generation >= confirmed.advertisement_generation
        && live.updated_at >= confirmed.updated_at
        && live.expires_at.is_some_and(|expiry| expiry > now)
}

pub(crate) async fn fetch_live_content_advertisement(
    transport: &dyn sinomo_module::ComponentObjectRpcTransport,
    target: &NodeId,
    confirmed: &DfsNodeAdvertisement,
    options: ClusterObjectRpcOptions,
) -> Result<DfsNodeAdvertisement, NetworkError> {
    // This transport authenticates the actual target. Only its content-owner
    // component registers the route; a node ping is never substituted.
    let response = transport
        .call_async(
            target,
            ObjectRpcCall::encode_value(
                crate::DFS_CONTENT_ADVERTISEMENT_LIVE_ROUTE,
                crate::DFS_RPC_CATEGORY,
                (),
            )?,
            options,
            false,
        )
        .await?;
    response.response.error_detail()?;
    let live: DfsNodeAdvertisement = response.decode_body().await?;
    if !matches_confirmed_content_advertisement(target, confirmed, &live, OffsetDateTime::now_utc())
    {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "live content-owner observation does not match the confirmed advertisement"
                .into(),
        });
    }
    Ok(live)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_owner_requires_identity_semantics_generation_and_real_expiry() {
        let node = NodeId::from_legacy_named("live_owner_contract").unwrap();
        let now = OffsetDateTime::now_utc();
        let mut confirmed = DfsNodeAdvertisement::content(node.clone());
        confirmed.updated_at = now - time::Duration::minutes(20);
        confirmed.expires_at = Some(now - time::Duration::minutes(5));
        confirmed.advertisement_generation = 7;
        let original = confirmed.clone();
        let mut live = confirmed.clone();
        live.updated_at = now;
        live.expires_at = Some(now + time::Duration::minutes(15));
        live.advertisement_generation = 8;
        assert!(matches_confirmed_content_advertisement(
            &node, &confirmed, &live, now
        ));
        assert_eq!(
            confirmed, original,
            "the expired canonical row stays unchanged"
        );
        for changed in 0..7 {
            let mut rejected = live.clone();
            match changed {
                0 => rejected.node_id = NodeId::from_legacy_named("wrong_live_owner").unwrap(),
                1 => rejected.accepts_content = false,
                2 => rejected.advertisement_generation = 6,
                3 => rejected.updated_at = confirmed.updated_at - time::Duration::seconds(1),
                4 => rejected.expires_at = Some(now),
                5 => rejected.expires_at = None,
                _ => rejected.accepts_metadata = true,
            }
            assert!(
                !matches_confirmed_content_advertisement(&node, &confirmed, &rejected, now),
                "case {changed}"
            );
        }
        assert!(!matches_confirmed_content_advertisement(
            &node,
            &confirmed,
            &live,
            live.expires_at.unwrap()
        ));
    }
}
