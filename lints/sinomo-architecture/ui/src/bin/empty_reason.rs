use sinomo_dfs_lint_ui::DfsControlPlaneKvClient;
/// sinomo:kv-consistency:
fn fence(client: &dyn DfsControlPlaneKvClient) { client.get("/dfs/assignment"); }
/// sinomo:dfs-operation
pub fn write(client: &dyn DfsControlPlaneKvClient) { fence(client); }
fn main() {}
