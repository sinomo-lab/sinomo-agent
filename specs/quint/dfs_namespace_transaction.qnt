/// DFS namespace transaction atomicity for first visible publication.
///
/// This module models the shard-local path where a new DFS object and its
/// first visible version are carried by one `DfsNamespaceTransaction`. It sits
/// above the DFS authority specs: the question here is table materialization
/// and replay, not primary election, routing, or receipt policy.
module dfs_namespace_transaction {
  import common.* from "./common"

  type DfsObject = ObjA | ObjB
  type DfsVersion = Version1 | Version2
  type NamespaceTxnId = TxnA | TxnB

  type Publication = {
    object: DfsObject,
    version: DfsVersion,
  }

  pure val Objects: Set[DfsObject] = Set(ObjA, ObjB)
  pure val Versions: Set[DfsVersion] = Set(Version1, Version2)
  pure val TxnIds: Set[NamespaceTxnId] = Set(TxnA, TxnB)

  /// Durable transaction record, standing in for the journal mutation plus the
  /// `PutNamespaceTransaction` table record used for recovery.
  var recordedTxn: NamespaceTxnId -> Option[Publication]

  /// Materialized object table row. In this model we only track the object
  /// version field because first publication is the path under verification.
  var objectRow: DfsObject -> Option[DfsVersion]

  /// Materialized visible-version table rows.
  var versionRows: DfsObject -> Set[DfsVersion]

  /// Transactions whose recovery planner has been observed. This is an
  /// accounting aid for idempotence checks; it is not an implementation table.
  var recovered: Set[NamespaceTxnId]

  pure val emptyRecordedTxns: NamespaceTxnId -> Option[Publication] =
    TxnIds.mapBy(_ => None)

  pure val emptyObjectRows: DfsObject -> Option[DfsVersion] =
    Objects.mapBy(_ => None)

  pure val emptyVersionRows: DfsObject -> Set[DfsVersion] =
    Objects.mapBy(_ => Set())

  pure def unwrapPublication(value: Option[Publication]): Publication =
    match value {
      | Some(inner) => inner
      | None => {
          object: ObjA,
          version: Version1,
        }
    }

  def transactionForObjectExists(object: DfsObject): bool =
    TxnIds.filter(txn =>
      match recordedTxn.get(txn) {
        | Some(publication) => publication.object == object
        | None => false
      }
    ).size() > 0

  def transactionForObjectVersionExists(object: DfsObject, version: DfsVersion): bool =
    TxnIds.filter(txn =>
      recordedTxn.get(txn) == Some({
        object: object,
        version: version,
      })
    ).size() > 0

  def canRecordPublication(
    txn: NamespaceTxnId,
    object: DfsObject,
    version: DfsVersion,
  ): bool = all {
    recordedTxn.get(txn) == None,
    not(transactionForObjectExists(object)),
    objectRow.get(object) == None,
    versionRows.get(object).size() == 0,
  }

  action init = all {
    recordedTxn' = emptyRecordedTxns,
    objectRow' = emptyObjectRows,
    versionRows' = emptyVersionRows,
    recovered' = Set(),
  }

  /// Normal backend apply for `apply_namespace_transaction`: record the
  /// transaction and materialize object plus version rows as one table batch.
  action applyPublicationTransaction(
    txn: NamespaceTxnId,
    object: DfsObject,
    version: DfsVersion,
  ): bool = all {
    canRecordPublication(txn, object, version),
    recordedTxn' = recordedTxn.set(txn, Some({
      object: object,
      version: version,
    })),
    objectRow' = objectRow.set(object, Some(version)),
    versionRows' = versionRows.set(object, Set(version)),
    recovered' = recovered,
  }

  /// Crash/reopen abstraction: the durable transaction exists, but object and
  /// version rows have not yet been materialized into the table snapshot.
  action recordPublicationForRecovery(
    txn: NamespaceTxnId,
    object: DfsObject,
    version: DfsVersion,
  ): bool = all {
    canRecordPublication(txn, object, version),
    recordedTxn' = recordedTxn.set(txn, Some({
      object: object,
      version: version,
    })),
    objectRow' = objectRow,
    versionRows' = versionRows,
    recovered' = recovered,
  }

  /// Recovery planner for a recorded transaction. It is idempotent because the
  /// materialized tables are maps/sets and the planner skips already-present
  /// rows in the Rust implementation.
  action recoverRecordedPublication(txn: NamespaceTxnId): bool = {
    val publication = unwrapPublication(recordedTxn.get(txn))
    all {
      isSome(recordedTxn.get(txn)),
      any {
        objectRow.get(publication.object) != Some(publication.version),
        not(versionRows.get(publication.object).contains(publication.version)),
        not(recovered.contains(txn)),
      },
      objectRow' = objectRow.set(publication.object, Some(publication.version)),
      versionRows' = versionRows.set(
        publication.object,
        versionRows.get(publication.object).union(Set(publication.version))
      ),
      recordedTxn' = recordedTxn,
      recovered' = recovered.union(Set(txn)),
    }
  }

  /// Duplicate replay of an already-complete transaction should not create new
  /// semantic rows or disturb the publication.
  action replayCompletePublication(txn: NamespaceTxnId): bool = {
    val publication = unwrapPublication(recordedTxn.get(txn))
    all {
      isSome(recordedTxn.get(txn)),
      objectRow.get(publication.object) == Some(publication.version),
      versionRows.get(publication.object).contains(publication.version),
      recordedTxn' = recordedTxn,
      objectRow' = objectRow,
      versionRows' = versionRows,
      recovered' = recovered.union(Set(txn)),
    }
  }

  action step = any {
    {
      nondet txn = oneOf(TxnIds)
      nondet object = oneOf(Objects)
      nondet version = oneOf(Versions)
      any {
        applyPublicationTransaction(txn, object, version),
        recordPublicationForRecovery(txn, object, version),
      }
    },
    {
      nondet txn = oneOf(TxnIds)
      any {
        recoverRecordedPublication(txn),
        replayCompletePublication(txn),
      }
    },
  }

  def materializedVersionsHaveObjects: bool =
    Objects.forall(object =>
      versionRows.get(object).forall(version =>
        objectRow.get(object) == Some(version)
      )
    )

  def materializedObjectsHaveVisibleVersions: bool =
    Objects.forall(object =>
      match objectRow.get(object) {
        | Some(version) => versionRows.get(object).contains(version)
        | None => versionRows.get(object).size() == 0
      }
    )

  def materializedRowsComeFromRecordedTransactions: bool =
    Objects.forall(object =>
      match objectRow.get(object) {
        | Some(version) => transactionForObjectVersionExists(object, version)
        | None => true
      }
    )

  def noPartialMaterializedTransaction: bool =
    TxnIds.forall(txn =>
      match recordedTxn.get(txn) {
        | Some(publication) => {
            val hasObject =
              objectRow.get(publication.object) == Some(publication.version)
            val hasVersion =
              versionRows.get(publication.object).contains(publication.version)
            hasObject == hasVersion
          }
        | None => true
      }
    )

  def transactionsAreUniquePerObject: bool =
    Objects.forall(object =>
      TxnIds.filter(txn =>
        match recordedTxn.get(txn) {
          | Some(publication) => publication.object == object
          | None => false
        }
      ).size() <= 1
    )

  def recoveredTransactionsWereRecorded: bool =
    recovered.forall(txn => isSome(recordedTxn.get(txn)))

  def safety: bool = all {
    materializedVersionsHaveObjects,
    materializedObjectsHaveVisibleVersions,
    materializedRowsComeFromRecordedTransactions,
    noPartialMaterializedTransaction,
    transactionsAreUniquePerObject,
    recoveredTransactionsWereRecorded,
  }
}
