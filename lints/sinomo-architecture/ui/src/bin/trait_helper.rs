use sinomo_dfs_lint_ui::DfsControlPlaneKvClient;
pub trait Load { fn load(&self); }
pub struct Adapter<'a>(&'a dyn DfsControlPlaneKvClient);
impl Load for Adapter<'_> { fn load(&self) { self.0.get("/dfs/arbitrary"); } }
/// sinomo:dfs-operation
pub fn write(loader: &dyn Load) { loader.load(); }
fn main() {}
