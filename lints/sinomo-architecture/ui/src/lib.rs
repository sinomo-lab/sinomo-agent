pub trait DfsControlPlaneKvClient {
    fn get(&self, key: &str);
}
