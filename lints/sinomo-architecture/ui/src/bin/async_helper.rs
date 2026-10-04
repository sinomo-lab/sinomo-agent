use sinomo_dfs_lint_ui::DfsControlPlaneKvClient;
async fn helper(client: &dyn DfsControlPlaneKvClient) { client.get("/dfs/arbitrary"); }
/// sinomo:dfs-operation
pub async fn write(client: &dyn DfsControlPlaneKvClient) { helper(client).await; }
fn main() {}
