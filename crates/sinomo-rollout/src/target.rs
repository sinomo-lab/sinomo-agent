use crate::RolloutError;
use sinomo_network::is_loopback_connect_address;

pub fn rollout_target_platform_matches_local(
    target_os: Option<&str>,
    target_arch: Option<&str>,
) -> Option<bool> {
    match (target_os, target_arch) {
        (Some(target_os), Some(target_arch)) => {
            Some(target_os == std::env::consts::OS && target_arch == std::env::consts::ARCH)
        }
        _ => None,
    }
}

pub fn cluster_rollout_should_use_remote_build(
    address: &str,
    remote_build_requested: bool,
    binary_provided: bool,
    platform_matches_local: Option<bool>,
) -> bool {
    remote_build_requested
        || (!is_loopback_connect_address(address)
            && !binary_provided
            && platform_matches_local != Some(true))
}

pub fn ensure_rollout_target_platform_matches_local(
    address: &str,
    target_os: Option<&str>,
    target_arch: Option<&str>,
) -> Result<(), RolloutError> {
    let local = is_loopback_connect_address(address);
    match (target_os, target_arch) {
        (Some(target_os), Some(target_arch))
            if target_os == std::env::consts::OS && target_arch == std::env::consts::ARCH =>
        {
            Ok(())
        }
        (Some(target_os), Some(target_arch)) => Err(RolloutError::InvalidTarget {
            detail: format!(
                "target platform {target_os}/{target_arch} does not match local artifact platform {}/{}",
                std::env::consts::OS,
                std::env::consts::ARCH
            ),
        }),
        _ if local => Ok(()),
        _ => Err(RolloutError::InvalidTarget {
            detail:
                "target daemon does not report platform metadata; update it locally before binary rollout"
                    .to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_rollout_remote_build_policy_uses_remote_targets_and_platform() {
        assert!(cluster_rollout_should_use_remote_build(
            "10.0.0.2:21353",
            false,
            false,
            Some(false)
        ));
        assert!(cluster_rollout_should_use_remote_build(
            "10.0.0.2:21353",
            false,
            false,
            None
        ));
        assert!(!cluster_rollout_should_use_remote_build(
            "10.0.0.2:21353",
            false,
            false,
            Some(true)
        ));
        assert!(!cluster_rollout_should_use_remote_build(
            "10.0.0.2:21353",
            false,
            true,
            Some(false)
        ));
        assert!(!cluster_rollout_should_use_remote_build(
            "127.0.0.1:21353",
            false,
            false,
            Some(false)
        ));
        assert!(cluster_rollout_should_use_remote_build(
            "127.0.0.1:21353",
            true,
            true,
            Some(true)
        ));
    }

    #[test]
    fn rollout_target_platform_allows_local_missing_metadata() {
        ensure_rollout_target_platform_matches_local("127.0.0.1:21353", None, None)
            .expect("loopback target may be old during local rollout");
    }
}
