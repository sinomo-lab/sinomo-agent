/// TARGET ONLY: certificate issuance, consensus, receiver idempotency, actual
/// bytes/fsync and distributed fencing are refinement assumptions, not proved.
module dfs_availability_effect_custody_contract {
  pure val Stores = Set(1, 2, 3)
  pure val Effects = Set(1, 2)
  pure val Attempts = Set(1, 2)
  pure val Pages = Set(1, 2)
  pure val Keys = Effects.map(e => Stores.map(n => (e, n))).flatten()
  pure val Absent = 0
  pure val DigestOnly = 1
  pure val FullExact = 2
  pure val Stage = 1
  pure val Repair = 2
  pure val Commit = 3
  pure val NoPayload = 0
  pure val Inline = 1
  pure val Attached = 2
  pure val Durable = 1
  pure val Committed = 2
  /// Target settlement, not the broader existing sender receipt handler.
  pure def settles(kind: int, outcome: int): bool =
    if (kind == Stage) Set(Durable, Committed).contains(outcome) else outcome == Committed
  /// FullExact is an injective alias for complete original bytes, not a hash.
  /// Inline bytes are included in FullExact head; attachment bytes are separate.
  pure def pageOf(effect: int): int = effect
  pure def needsAttachment(mode: int): bool = mode == Attached
}

/// C4/C7 TARGET companion. Concrete identities/material are injective aliases;
/// native authentication, exact-byte codecs, fsync/ancestor certification,
/// authority/frozen-prefix proof and immutable source sequencing are assumed.
module dfs_custody_ack_contract {
  type Transfer = { handoff: int, inventory: int, source: int, sourceQ: int, installer: int, installerQ: int }
  type Fact = { transfer: Transfer, source: int, sourceQ: int, seq: int,
    origin: int, originDigest: int, material: int, digest: int,
    prevCut: bool, prevSeq: int, prevDigest: int }
  type FactAck = { transfer: Transfer, seq: int, digest: int, installSeq: int }
  pure val transfer: Transfer = { handoff: 7, inventory: 80, source: 1, sourceQ: 10, installer: 2, installerQ: 20 }
  pure val pages = Set(1, 2)
  pure val origins = Set(1, 2)
  pure val cutSeq = 5
  pure val originDigests = Map(1 -> 101, 2 -> 102)
  pure val first: Fact = { transfer: transfer, source: 1, sourceQ: 10, seq: 11,
    origin: 1, originDigest: 101, material: 501, digest: 1501,
    prevCut: true, prevSeq: cutSeq, prevDigest: transfer.inventory }
  pure val second: Fact = first.with("seq", 15).with("origin", 2).with("originDigest", 102)
    .with("material", 502).with("digest", 1502).with("prevCut", false).with("prevSeq", 11).with("prevDigest", 1501)
  pure val future: Fact = first.with("seq", 23).with("material", 503).with("digest", 1503)
    .with("prevCut", false).with("prevSeq", 15).with("prevDigest", 1502)
}
