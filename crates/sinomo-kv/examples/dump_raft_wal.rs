use sinomo_kv::{DurableRaftKvNode, KvCommand, KvOp, KvWalPaths};
use sinomo_node::NodeId;

fn op_key(op: &KvOp) -> String {
    match op {
        KvOp::Put { key, value, .. } => format!("Put {key} ({}B)", value.len()),
        KvOp::Delete { key } => format!("Delete {key}"),
        other => format!("{other:?}").chars().take(60).collect(),
    }
}

fn main() {
    let root = std::env::args()
        .nth(1)
        .expect("usage: dump_raft_wal <node-dir>");
    let node_id = std::path::Path::new(&root)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let node_id: NodeId = node_id.parse().expect("node id");
    let paths = KvWalPaths::new(&root);
    let node = DurableRaftKvNode::open(node_id, paths).expect("open node");
    println!(
        "applied={} snapshot={} commit={} last_log={} log_len={}",
        node.applied_index(),
        node.snapshot_index(),
        node.hard_state().commit_index,
        node.last_log_index(),
        node.log().len()
    );
    for entry in node
        .log()
        .iter()
        .rev()
        .take(30)
        .collect::<Vec<_>>()
        .iter()
        .rev()
    {
        let summary = match &entry.command {
            KvCommand::Op(op) => op_key(op),
            KvCommand::Txn(txn) => format!(
                "Txn success=[{}]",
                txn.success
                    .iter()
                    .map(op_key)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };
        let summary: String = summary.chars().take(110).collect();
        println!("  {} {}", entry.index, summary);
    }
}
