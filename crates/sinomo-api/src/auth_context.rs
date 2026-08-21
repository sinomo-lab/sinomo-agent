use sinomo_core::kv::ClusterKvRouteAuth;
use sinomo_core::service::ServiceRoute;

/// Verified daemon-control request context passed from signature middleware to
/// route authorization and service dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedDaemonCommand {
    pub route: String,
    pub signer: DaemonSignerAuthority,
    pub signer_node_id: String,
    /// Existing cluster membership state stores node public keys as URL-safe
    /// base64 strings.
    pub signer_public_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DaemonSignerAuthority {
    Local,
    Voter,
    Member,
    JoinCandidate,
    StatusProbe,
}

impl DaemonSignerAuthority {
    pub fn can_run_admin_command(&self) -> bool {
        matches!(self, Self::Local | Self::Voter)
    }

    pub fn can_run_local_command(&self) -> bool {
        matches!(self, Self::Local)
    }

    /// Applies the stable service-catalog authorization vocabulary.
    pub fn authorizes_route_scope(&self, scope: &str) -> bool {
        match scope {
            "authenticated" => true,
            "local-command" => self.can_run_local_command(),
            "local-or-cluster-member" | "cluster-member" => {
                matches!(self, Self::Local | Self::Voter | Self::Member)
            }
            "local-or-cluster-voter" => matches!(self, Self::Local | Self::Voter),
            "admin" => self.can_run_admin_command(),
            _ => false,
        }
    }

    pub fn authorizes_cluster_kv_route(&self, auth: ClusterKvRouteAuth) -> bool {
        match auth {
            ClusterKvRouteAuth::Authenticated => true,
            ClusterKvRouteAuth::LocalOnly => matches!(self, Self::Local),
            ClusterKvRouteAuth::LocalOrClusterMember => {
                matches!(self, Self::Local | Self::Voter | Self::Member)
            }
            ClusterKvRouteAuth::LocalOrRaftVoter => matches!(self, Self::Local | Self::Voter),
        }
    }

    pub const fn metadata_value(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Voter => "voter",
            Self::Member => "member",
            Self::JoinCandidate => "join-candidate",
            Self::StatusProbe => "status-probe",
        }
    }
}

impl AuthenticatedDaemonCommand {
    /// Authorizes a live service descriptor and delegates node-local consent to
    /// the runtime host. Dynamic services therefore use the same scope policy
    /// without linking daemon filesystem or permission-store implementation.
    pub fn authorizes_service_route_with<Permissions>(
        &self,
        descriptor: &ServiceRoute,
        permissions_granted: Permissions,
    ) -> bool
    where
        Permissions: FnOnce(&ServiceRoute) -> bool,
    {
        descriptor
            .auth_scope
            .as_deref()
            .is_none_or(|scope| self.signer.authorizes_route_scope(scope))
            && permissions_granted(descriptor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_scope_vocabulary_is_fail_closed() {
        assert!(DaemonSignerAuthority::Member.authorizes_route_scope("authenticated"));
        assert!(DaemonSignerAuthority::Member.authorizes_route_scope("cluster-member"));
        assert!(!DaemonSignerAuthority::Member.authorizes_route_scope("admin"));
        assert!(DaemonSignerAuthority::Voter.authorizes_route_scope("admin"));
        assert!(!DaemonSignerAuthority::Local.authorizes_route_scope("unknown-scope"));
    }

    #[test]
    fn authority_metadata_values_are_stable() {
        assert_eq!(DaemonSignerAuthority::Local.metadata_value(), "local");
        assert_eq!(DaemonSignerAuthority::Voter.metadata_value(), "voter");
        assert_eq!(DaemonSignerAuthority::Member.metadata_value(), "member");
        assert_eq!(
            DaemonSignerAuthority::JoinCandidate.metadata_value(),
            "join-candidate"
        );
        assert_eq!(
            DaemonSignerAuthority::StatusProbe.metadata_value(),
            "status-probe"
        );
    }
}
