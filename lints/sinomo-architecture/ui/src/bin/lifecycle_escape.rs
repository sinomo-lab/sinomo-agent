use sinomo_dfs_lint_ui::DfsControlPlaneKvClient;
/// sinomo:kv-lifecycle: Validate participants before activating a shard assignment.
fn admit(client: &dyn DfsControlPlaneKvClient) { client.get("/cluster/member"); }
/// sinomo:dfs-operation
pub fn write(client: &dyn DfsControlPlaneKvClient) { admit(client); }
fn main() {}
