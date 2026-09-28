use sinomo_dfs_lint_ui::DfsControlPlaneKvClient;
fn helper(client: &dyn DfsControlPlaneKvClient) { client.get("/dfs/arbitrary"); }
/// sinomo:dfs-operation
pub fn write(client: &dyn DfsControlPlaneKvClient) { helper(client); }
fn main() {}
