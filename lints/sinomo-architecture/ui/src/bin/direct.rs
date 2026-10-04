use sinomo_dfs_lint_ui::DfsControlPlaneKvClient as Client;
/// sinomo:dfs-operation
pub fn write(client: &dyn Client) { client.get("/dfs/arbitrary"); }
fn main() {}
