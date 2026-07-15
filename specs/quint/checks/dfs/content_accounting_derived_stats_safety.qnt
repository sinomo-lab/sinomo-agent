/// Safety and bounded reachability for DFS derived content accounting.
module dfs_content_accounting_derived_stats_safety {
  import dfs_content_accounting_derived_stats.* from "../../models/dfs/content_accounting_derived_stats"

  def contentCountersStayBoundedAndNonnegative: bool = all {
    contentAccounting.quotaLimitBytes >= 0,
    contentAccounting.quotaAuthoritativeBytes >= 0,
    contentAccounting.quotaReservationBytes >= 0,
    contentAccounting.synchronousQuotaWrites >= 0,
    contentAccounting.durableBlobBytes >= 0,
    contentAccounting.visibleMetadataBytes >= 0,
    contentAccounting.liveDerivedBytes >= 0,
    contentAccounting.persistedDerivedBytes >= 0,
    contentAccounting.acceptedWrites >= 0,
    contentAccounting.acceptedWrites <= 1,
    contentAccounting.rejectedWrites >= 0,
    contentAccounting.rejectedWrites <= 1,
    contentAccounting.crashes >= 0,
    contentAccounting.crashes <= 1,
    contentAccounting.rebuilds >= 0,
    contentAccounting.rebuilds <= 1,
  }

  /// A namespace entry can account for no more bytes than already exist in
  /// the durable blob set. Publication additionally records the prerequisite
  /// for the currently modeled request.
  def metadataVisibilityRequiresDurableBlob: bool = all {
    contentAccounting.visibleMetadataBytes <= contentAccounting.durableBlobBytes,
    contentAccounting.metadataPublishedForRequest implies all {
      contentAccounting.blobDurabilityEstablishedForRequest,
      contentAccounting.visibleMetadataBytes > 0,
      contentAccounting.durableBlobBytes > 0,
    },
    contentAccounting.lastTransition == DurableMetadataPublished implies all {
      contentAccounting.phase == MetadataVisible,
      contentAccounting.blobDurabilityEstablishedForRequest,
      contentAccounting.metadataPublishedForRequest,
      contentAccounting.visibleMetadataBytes == contentAccounting.durableBlobBytes,
    },
  }

  def acceptedPreparedWriteIsNotPrematurelyVisible: bool =
    contentAccounting.phase == WritePrepared implies all {
      contentAccounting.lastAdmission == AdmissionAccepted,
      not(contentAccounting.blobDurabilityEstablishedForRequest),
      not(contentAccounting.metadataPublishedForRequest),
      contentAccounting.durableBlobBytes == 0,
      contentAccounting.visibleMetadataBytes == 0,
    }

  /// The authoritative quota ledger includes both durable bytes and the one
  /// admitted reservation. Therefore admission cannot overcommit the quota
  /// while blob persistence is still pending.
  def quotaLedgerIsAuthoritative: bool =
    if (contentAccounting.quotaMode == QuotaEnabled) {
      all {
        contentAccounting.quotaLimitBytes == QuotaLimitBytes,
        contentAccounting.quotaAuthoritativeBytes
          == contentAccounting.durableBlobBytes + contentAccounting.quotaReservationBytes,
        contentAccounting.quotaAuthoritativeBytes <= contentAccounting.quotaLimitBytes,
        contentAccounting.synchronousQuotaWrites == contentAccounting.acceptedWrites,
      }
    } else {
      all {
        contentAccounting.quotaLimitBytes == 0,
        contentAccounting.quotaAuthoritativeBytes == 0,
        contentAccounting.quotaReservationBytes == 0,
        contentAccounting.synchronousQuotaWrites == 0,
      }
    }

  def everyQuotaAcceptedWriteRespectsLimit: bool =
    all {
      contentAccounting.quotaMode == QuotaEnabled,
      contentAccounting.lastAdmission == AdmissionAccepted,
    } implies all {
      contentAccounting.admissionQuotaTotal <= contentAccounting.quotaLimitBytes,
      contentAccounting.quotaAuthoritativeBytes <= contentAccounting.quotaLimitBytes,
    }

  /// Admission is the synchronization boundary for quota accounting. The
  /// durable blob transition later consumes the reservation without changing
  /// the authoritative total.
  def enabledQuotaAccountingIsSynchronous: bool =
    all {
      contentAccounting.quotaMode == QuotaEnabled,
      contentAccounting.lastTransition == WriteAdmissionAccepted,
    } implies all {
      contentAccounting.synchronousQuotaWrites
        == contentAccounting.syncWritesBeforeAdmission + 1,
      contentAccounting.quotaAuthoritativeBytes
        == contentAccounting.quotaBeforeAdmission + contentAccounting.requestBytes,
      contentAccounting.quotaReservationBytes == contentAccounting.requestBytes,
    }

  def rejectedQuotaRequestCannotMutateContentOrAccounting: bool =
    contentAccounting.lastTransition == WriteAdmissionRejected implies all {
      contentAccounting.quotaMode == QuotaEnabled,
      contentAccounting.lastAdmission == AdmissionRejected,
      contentAccounting.admissionQuotaTotal > contentAccounting.quotaLimitBytes,
      contentAccounting.quotaAuthoritativeBytes == contentAccounting.quotaBeforeAdmission,
      contentAccounting.synchronousQuotaWrites == contentAccounting.syncWritesBeforeAdmission,
      contentAccounting.quotaReservationBytes == 0,
      contentAccounting.durableBlobBytes == 0,
      contentAccounting.visibleMetadataBytes == 0,
      contentAccounting.acceptedWrites == 0,
      contentAccounting.rejectedWrites == 1,
    }

  /// Status and persisted observability snapshots do not participate in the
  /// admission predicate and cannot write the authoritative quota ledger.
  def statusObservationCannotChangeAdmissionOrAuthority: bool =
    contentAccounting.lastTransition == StatusObservedAndSnapshotPersisted implies all {
      contentAccounting.admissionProbeBeforeObservation
        == contentAccounting.admissionProbeAfterObservation,
      contentAccounting.quotaAuthoritativeBytes
        == contentAccounting.quotaBeforeObservation,
      contentAccounting.synchronousQuotaWrites
        == contentAccounting.syncWritesBeforeObservation,
    }

  def noQuotaWritesNeverSynchronouslyPersistAccounting: bool =
    contentAccounting.quotaMode == QuotaAbsent implies all {
      contentAccounting.quotaAuthoritativeBytes == 0,
      contentAccounting.quotaReservationBytes == 0,
      contentAccounting.synchronousQuotaWrites == 0,
    }

  /// Both live and persisted observability counters are rebuildable
  /// under-approximations. They may lag, but never invent durable bytes.
  def derivedStatsNeverExceedDurableBlobs: bool = all {
    contentAccounting.liveDerivedBytes <= contentAccounting.durableBlobBytes,
    contentAccounting.persistedDerivedBytes <= contentAccounting.durableBlobBytes,
    contentAccounting.statusReportedBytes <= contentAccounting.durableBlobBytes,
  }

  def crashLosesOnlyDerivedState: bool =
    contentAccounting.lastTransition == DerivedStatsLostOnCrash implies all {
      contentAccounting.phase == RuntimeCrashed,
      contentAccounting.durableBlobBytes == contentAccounting.durableBytesBeforeCrash,
      contentAccounting.visibleMetadataBytes == contentAccounting.visibleBytesBeforeCrash,
      contentAccounting.quotaAuthoritativeBytes == contentAccounting.quotaBytesBeforeCrash,
      contentAccounting.liveDerivedBytesBeforeCrash > 0,
      contentAccounting.liveDerivedBytes == 0,
      contentAccounting.persistedDerivedBytes == 0,
      not(contentAccounting.persistedSnapshotPresent),
    }

  def rebuildRestoresExactStatsWithoutNamespaceMutation: bool =
    contentAccounting.lastTransition == DerivedStatsRebuilt implies all {
      contentAccounting.phase == AccountingReady,
      contentAccounting.rebuilds == 1,
      derivedStatsAreExact(contentAccounting),
      contentAccounting.durableBlobBytes == contentAccounting.durableBytesBeforeCrash,
      contentAccounting.visibleMetadataBytes == contentAccounting.visibleBytesBeforeCrash,
      contentAccounting.quotaAuthoritativeBytes == contentAccounting.quotaBytesBeforeCrash,
    }

  def witnessStatusSnapshotDoesNotChangeAdmission: bool = all {
    contentAccounting.lastTransition == StatusObservedAndSnapshotPersisted,
    contentAccounting.quotaMode == QuotaAbsent,
    contentAccounting.persistedSnapshotPresent,
    contentAccounting.admissionProbeBeforeObservation,
    contentAccounting.admissionProbeAfterObservation,
    statusObservationCannotChangeAdmissionOrAuthority,
  }

  def witnessAcceptedWriteNotVisibleBeforeBlobDurability: bool = all {
    contentAccounting.lastTransition == WriteAdmissionAccepted,
    contentAccounting.phase == WritePrepared,
    contentAccounting.lastAdmission == AdmissionAccepted,
    not(contentAccounting.blobDurabilityEstablishedForRequest),
    contentAccounting.durableBlobBytes == 0,
    contentAccounting.visibleMetadataBytes == 0,
  }

  def witnessBlobDurableBeforeMetadataVisibility: bool = all {
    contentAccounting.lastTransition == PreparedBlobPersisted,
    contentAccounting.phase == BlobDurable,
    contentAccounting.blobDurabilityEstablishedForRequest,
    not(contentAccounting.metadataPublishedForRequest),
    contentAccounting.durableBlobBytes == contentAccounting.requestBytes,
    contentAccounting.visibleMetadataBytes == 0,
  }

  def witnessNoQuotaWriteAfterStatusSkipsSynchronousAccounting: bool = all {
    contentAccounting.lastTransition == DurableMetadataPublished,
    contentAccounting.quotaMode == QuotaAbsent,
    contentAccounting.statusReads == 1,
    contentAccounting.persistedSnapshotPresent,
    contentAccounting.acceptedWrites == 1,
    contentAccounting.synchronousQuotaWrites == 0,
    contentAccounting.durableBlobBytes == 1,
    contentAccounting.visibleMetadataBytes == 1,
    contentAccounting.persistedDerivedBytes == 0,
  }

  def witnessDerivedSnapshotMayLagDurableContent: bool = all {
    contentAccounting.quotaMode == QuotaAbsent,
    contentAccounting.persistedSnapshotPresent,
    contentAccounting.lastTransition == PreparedBlobPersisted,
    contentAccounting.persistedDerivedBytes < contentAccounting.durableBlobBytes,
    contentAccounting.synchronousQuotaWrites == 0,
  }

  def witnessQuotaOversizedWriteRejected: bool = all {
    contentAccounting.lastTransition == WriteAdmissionRejected,
    contentAccounting.quotaMode == QuotaEnabled,
    contentAccounting.requestBytes == 2,
    contentAccounting.quotaLimitBytes == 1,
    rejectedQuotaRequestCannotMutateContentOrAccounting,
  }

  def witnessQuotaAcceptedWriteAccountsSynchronously: bool = all {
    contentAccounting.lastTransition == WriteAdmissionAccepted,
    contentAccounting.quotaMode == QuotaEnabled,
    contentAccounting.requestBytes == 1,
    contentAccounting.quotaAuthoritativeBytes == 1,
    contentAccounting.quotaReservationBytes == 1,
    contentAccounting.synchronousQuotaWrites == 1,
    enabledQuotaAccountingIsSynchronous,
  }

  def witnessQuotaWritePublishesWithAuthoritativeAccounting: bool = all {
    contentAccounting.lastTransition == DurableMetadataPublished,
    contentAccounting.quotaMode == QuotaEnabled,
    contentAccounting.durableBlobBytes == 1,
    contentAccounting.visibleMetadataBytes == 1,
    contentAccounting.quotaAuthoritativeBytes == 1,
    contentAccounting.quotaReservationBytes == 0,
    contentAccounting.synchronousQuotaWrites == 1,
  }

  def witnessCrashLosesDerivedButNotDurableState: bool = all {
    contentAccounting.lastTransition == DerivedStatsLostOnCrash,
    contentAccounting.durableBlobBytes == 1,
    contentAccounting.visibleMetadataBytes == 0,
    contentAccounting.liveDerivedBytesBeforeCrash == 1,
    contentAccounting.liveDerivedBytes == 0,
    crashLosesOnlyDerivedState,
  }

  def witnessRebuildRestoresExactStatsWithoutNamespaceMutation: bool = all {
    contentAccounting.lastTransition == DerivedStatsRebuilt,
    contentAccounting.durableBlobBytes == 1,
    contentAccounting.visibleMetadataBytes == 0,
    contentAccounting.liveDerivedBytes == 1,
    contentAccounting.persistedDerivedBytes == 1,
    rebuildRestoresExactStatsWithoutNamespaceMutation,
  }

  def safety: bool = all {
    contentCountersStayBoundedAndNonnegative,
    metadataVisibilityRequiresDurableBlob,
    acceptedPreparedWriteIsNotPrematurelyVisible,
    quotaLedgerIsAuthoritative,
    everyQuotaAcceptedWriteRespectsLimit,
    enabledQuotaAccountingIsSynchronous,
    rejectedQuotaRequestCannotMutateContentOrAccounting,
    statusObservationCannotChangeAdmissionOrAuthority,
    noQuotaWritesNeverSynchronouslyPersistAccounting,
    derivedStatsNeverExceedDurableBlobs,
    crashLosesOnlyDerivedState,
    rebuildRestoresExactStatsWithoutNamespaceMutation,
  }
}
