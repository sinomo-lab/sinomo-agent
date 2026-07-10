/// Invariant checks for `dfs_namespace_transaction`.
module dfs_namespace_transaction_safety {
  import common.* from "../../lib/common"
  import dfs_namespace_transaction.* from "../../models/dfs/namespace_transaction"

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
