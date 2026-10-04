struct Configuration { epoch: u64 }
#[derive(Clone)]
struct DaemonDfsControlPlaneKvClient;
/// sinomo:dfs-operation
fn write(config: &Configuration) -> u64 {
    let _local_value = String::from("cached").clone();
    let _client_handle = DaemonDfsControlPlaneKvClient.clone();
    config.epoch
}
fn main() { assert_eq!(write(&Configuration { epoch: 1 }), 1); }
