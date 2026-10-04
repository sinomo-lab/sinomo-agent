use sinomo_dfs_lint_ui::DfsControlPlaneKvClient;
/// sinomo:kv-consistency: Fence publication against concurrent ownership changes.
fn fence(client: &dyn DfsControlPlaneKvClient) { client.get("/dfs/assignment"); }
/// sinomo:kv-configuration: Read filesystem configuration through its existing retained projection.
fn configuration(client: &dyn DfsControlPlaneKvClient) { client.get("/dfs/filesystems/fs/config"); }
/// sinomo:dfs-operation
pub fn write(client: &dyn DfsControlPlaneKvClient) { fence(client); configuration(client); }
/// sinomo:kv-lifecycle: Validate participants before activating a shard assignment.
pub fn admit(client: &dyn DfsControlPlaneKvClient) { client.get("/cluster/member"); }
fn main() {}
