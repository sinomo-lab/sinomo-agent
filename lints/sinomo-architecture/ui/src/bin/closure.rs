use sinomo_dfs_lint_ui::DfsControlPlaneKvClient;
/// sinomo:dfs-operation
pub fn write(client: &dyn DfsControlPlaneKvClient) {
    let callback = || client.get("/dfs/arbitrary");
    callback();
}
fn main() {}
