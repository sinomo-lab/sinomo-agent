/// T1-T6 TARGET ONLY. Exact bytes/phase IDs are injective aliases. Native
/// fsync/codec/provenance/dedup/authority/cross-owner certifiers are assumed.
module dfs_repair_terminal_contract {
  pure val Origins = Set(1, 2)
  pure val Nodes = Set(1, 2, 3) // A producer, B successor, C receiver
  pure val Roles = Set(1, 2) // receipt/location, visible-object/version families
  pure val Bodies = Map(1 -> 101, 2 -> 102)
  pure val Results = Map(1 -> 201, 2 -> 202)
  pure val RoleKeys = Origins.map(e => Roles.map(r => (e, r))).flatten()
  pure val TransferRoute = 1
  pure val SettlementRoute = 2
}
