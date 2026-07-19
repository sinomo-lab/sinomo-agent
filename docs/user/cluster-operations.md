# Cluster Operations

This guide is for operators creating and maintaining a trusted Sinomo cluster.
For identity, consensus, and failure semantics, see the
[clustering architecture](../clustering-architecture.md).

Command syntax can change with the installed binary. Use
`sinomo cluster help` as the final reference.

## Before You Start

Each machine needs:

- a Sinomo installation;
- a running main daemon;
- a distinct, persistent `SINOMO_DATA_DIR`;
- network reachability to an existing cluster member when joining.

Check the local processes before changing cluster state:

```sh
sinomo daemon status
sinomo daemon shadow status
```

The shadow daemon is a recovery endpoint. It is not a separate cluster member.

## Create a Cluster

A normal standalone installation is a one-node cluster:

```sh
sinomo cluster init --name home
sinomo cluster status
```

Cluster initialization assigns the local installation its cluster and node
identity. Do not initialize a second cluster on a machine that is meant to join
an existing one.

## Add a Node

On an existing member, create a short-lived invite. Provide an introducer address
that the new node can reach:

```sh
sinomo cluster invite \
  --introducer 192.168.1.10:21353 \
  --output join.txt
```

Transfer `join.txt` through a trusted channel. On the new machine:

```sh
sinomo cluster join join.txt --name laptop
```

Then verify the result from either member:

```sh
sinomo cluster status
sinomo cluster health
sinomo cluster peers
```

Invites are credentials. Keep their lifetime short and do not commit them to a
repository or place them in a shared workspace.

## Inspect Health

Use the least invasive command that answers the question:

| Command | Use |
|---|---|
| `sinomo cluster status` | Cluster/local identity, local membership state, term, KV revision/snapshot, member count, and state path. |
| `sinomo cluster health` | Per-node reachability, version, platform, and rollout state. |
| `sinomo cluster peers` | Known members with node ID, display name, descriptive role, and membership state. |
| `sinomo cluster doctor` | Endpoint, reachability, stale-status, and storage diagnostics. |

For a rollout problem, combine these checks with
`sinomo rollout diagnostics`; see [rollout operations](rollout-operations.md).

## Roles and Membership

The descriptive node role and control-plane authority are separate:

```sh
NODE_ID=your-node-id
sinomo cluster role "$NODE_ID" workstation
sinomo cluster state-authority "$NODE_ID" observer
sinomo cluster remove "$NODE_ID"
```

Use `role` to describe placement intent such as workstation, mobile, server, or
sensor. Use `state-authority` only when deliberately changing replicated state
membership. Keep an odd number of reliable authority nodes when possible;
intermittent laptops and mobile devices should normally not be required for
quorum.

The `state-authority` command uses operator-facing tokens that map to the
underlying membership roles:

| Command token | Membership role | Meaning |
|---|---|---|
| `authority` | voter | Participates in quorum and replicates state. |
| `replica` | learner | Replicates/catches up but does not vote. |
| `observer` | non-voting | Remains an authorized member without a vote. |

Join and reconfiguration output can temporarily show learner/catch-up state.
These roles are independent from `cluster role` values such as workstation or
mobile.

`cluster remove` rejects members that still hold state authority. Confirm the
remaining authority set is healthy before changing or removing a member.

## Move or Leave a Cluster

Preview a move without changing local state:

```sh
INVITE_PATH=/trusted/path/to/join.txt
sinomo cluster migrate plan "$INVITE_PATH"
```

Joining another cluster from a settled one-node cluster uses the migration path.
The old cluster state is quarantined locally as recovery evidence; target
cluster state becomes authoritative. Shared DFS data is not silently merged.

To leave the current cluster and return the installation to a fresh one-node
cluster:

```sh
sinomo cluster leave
```

Treat migration and leave as state transitions, not cleanup commands. Verify
the current cluster, node identity, and data directory before running them.

## Advertise Explicit Endpoints

Automatic discovery is sufficient on many LANs. For NAT, port forwarding, or a
machine with several interfaces, define externally reachable endpoints in:

```text
$SINOMO_DATA_DIR/configs/cluster/endpoints.json
```

Example:

```json
{
  "main": [
    {
      "address": "tcp://198.51.100.76:21353",
      "scope": "internet-direct"
    }
  ],
  "shadow": [
    {
      "address": "tcp://198.51.100.76:21354",
      "scope": "internet-direct"
    }
  ]
}
```

Only advertise addresses that route to this installation. A malformed endpoint
file is reported as a configuration error rather than ignored.

## Recovery

Start with observation rather than deleting local state:

```sh
sinomo daemon status
sinomo daemon shadow status
sinomo cluster health
sinomo cluster doctor
```

`cluster repair-members` and `cluster cleanup-legacy-shadow-members` are focused
repair commands. Read their current help, confirm the source peer and data
directory, and preserve a backup before using them. Never create a replacement
cluster identity merely to work around lost quorum; recovery must preserve the
existing authority history or make an explicit leave/migration decision.
