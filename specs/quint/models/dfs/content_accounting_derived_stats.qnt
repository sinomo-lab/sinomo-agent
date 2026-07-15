/// DFS content accounting with authoritative quota and derived status stats.
///
/// Evidence class: target hardening for removing observability-only stats
/// persistence from the no-quota content-write hot path. A status read may
/// materialize a persisted derived snapshot, but that snapshot is never an
/// admission input and does not turn later no-quota mutations into synchronous
/// accounting writes. When quota is enabled, a bounded reservation is written
/// to the authoritative quota ledger in the same transition that admits the
/// request. Blob durability remains a separate prerequisite for namespace
/// metadata publication.
///
/// This model admits one unit-sized or two-unit-sized request against a
/// one-unit quota. It includes a crash after blob durability and an exact
/// rebuild of derived stats from durable blobs. Blob garbage collection,
/// deduplication, failed durable writes, concurrent reservations, and the
/// byte-level atomicity of either ledger are outside this bounded slice.
module dfs_content_accounting_derived_stats {
  type QuotaMode = QuotaAbsent | QuotaEnabled
  type AccountingPhase =
    | AccountingReady
    | WritePrepared
    | BlobDurable
    | MetadataVisible
    | WriteRejected
    | RuntimeCrashed
  type AdmissionOutcome =
    | AdmissionNotAttempted
    | AdmissionAccepted
    | AdmissionRejected
  type AccountingTransition =
    | AccountingInitialized
    | QuotaEnabledForStore
    | StatusObservedAndSnapshotPersisted
    | WriteAdmissionAccepted
    | WriteAdmissionRejected
    | PreparedBlobPersisted
    | DurableMetadataPublished
    | DerivedStatsLostOnCrash
    | DerivedStatsRebuilt
    | AccountingStuttered

  type ContentAccountingState = {
    quotaMode: QuotaMode,
    phase: AccountingPhase,
    quotaLimitBytes: int,
    quotaAuthoritativeBytes: int,
    quotaReservationBytes: int,
    synchronousQuotaWrites: int,
    durableBlobBytes: int,
    visibleMetadataBytes: int,
    liveDerivedBytes: int,
    persistedDerivedBytes: int,
    persistedSnapshotPresent: bool,
    statusReportedBytes: int,
    statusReads: int,
    requestBytes: int,
    lastAdmission: AdmissionOutcome,
    acceptedWrites: int,
    rejectedWrites: int,
    admissionQuotaTotal: int,
    quotaBeforeAdmission: int,
    syncWritesBeforeAdmission: int,
    blobDurabilityEstablishedForRequest: bool,
    metadataPublishedForRequest: bool,
    admissionProbeBeforeObservation: bool,
    admissionProbeAfterObservation: bool,
    quotaBeforeObservation: int,
    syncWritesBeforeObservation: int,
    durableBytesBeforeCrash: int,
    visibleBytesBeforeCrash: int,
    quotaBytesBeforeCrash: int,
    liveDerivedBytesBeforeCrash: int,
    crashes: int,
    rebuilds: int,
    lastTransition: AccountingTransition,
  }

  pure val QuotaLimitBytes: int = 1
  pure val RequestSizes: Set[int] = Set(1, 2)

  /// Derived snapshots and status values are deliberately absent from this
  /// predicate. Only the enabled authoritative quota ledger constrains an
  /// otherwise valid write.
  pure def writeWouldBeAdmitted(
    state: ContentAccountingState,
    requestSize: int,
  ): bool = all {
    RequestSizes.contains(requestSize),
    any {
      state.quotaMode == QuotaAbsent,
      all {
        state.quotaMode == QuotaEnabled,
        state.quotaAuthoritativeBytes + requestSize <= state.quotaLimitBytes,
      },
    },
  }

  pure def derivedStatsAreExact(state: ContentAccountingState): bool = all {
    state.liveDerivedBytes == state.durableBlobBytes,
    state.persistedSnapshotPresent,
    state.persistedDerivedBytes == state.durableBlobBytes,
  }

  pure val initialState: ContentAccountingState = {
    quotaMode: QuotaAbsent,
    phase: AccountingReady,
    quotaLimitBytes: 0,
    quotaAuthoritativeBytes: 0,
    quotaReservationBytes: 0,
    synchronousQuotaWrites: 0,
    durableBlobBytes: 0,
    visibleMetadataBytes: 0,
    liveDerivedBytes: 0,
    persistedDerivedBytes: 0,
    persistedSnapshotPresent: false,
    statusReportedBytes: 0,
    statusReads: 0,
    requestBytes: 0,
    lastAdmission: AdmissionNotAttempted,
    acceptedWrites: 0,
    rejectedWrites: 0,
    admissionQuotaTotal: 0,
    quotaBeforeAdmission: 0,
    syncWritesBeforeAdmission: 0,
    blobDurabilityEstablishedForRequest: false,
    metadataPublishedForRequest: false,
    admissionProbeBeforeObservation: false,
    admissionProbeAfterObservation: false,
    quotaBeforeObservation: 0,
    syncWritesBeforeObservation: 0,
    durableBytesBeforeCrash: 0,
    visibleBytesBeforeCrash: 0,
    quotaBytesBeforeCrash: 0,
    liveDerivedBytesBeforeCrash: 0,
    crashes: 0,
    rebuilds: 0,
    lastTransition: AccountingInitialized,
  }

  var contentAccounting: ContentAccountingState

  action init = contentAccounting' = initialState

  /// Quota is configured only on the empty bounded store. This isolates the
  /// synchronous quota path from any question of enabling quota over existing
  /// content, which requires a separate reconciliation protocol.
  action enableQuota = all {
    contentAccounting.phase == AccountingReady,
    contentAccounting.quotaMode == QuotaAbsent,
    contentAccounting.acceptedWrites == 0,
    contentAccounting.durableBlobBytes == 0,
    contentAccounting.visibleMetadataBytes == 0,
    contentAccounting' = contentAccounting
      .with("quotaMode", QuotaEnabled)
      .with("quotaLimitBytes", QuotaLimitBytes)
      .with("quotaAuthoritativeBytes", 0)
      .with("quotaReservationBytes", 0)
      .with("lastTransition", QuotaEnabledForStore),
  }

  /// A status observation may write a derived snapshot. It records the
  /// admission decision on each side of that observation so the check module
  /// can prove that neither the read nor snapshot existence changed it.
  action observeStatusAndPersistDerivedSnapshot = all {
    contentAccounting.phase != RuntimeCrashed,
    contentAccounting.statusReads == 0,
    contentAccounting' = contentAccounting
      .with("persistedDerivedBytes", contentAccounting.liveDerivedBytes)
      .with("persistedSnapshotPresent", true)
      .with("statusReportedBytes", contentAccounting.liveDerivedBytes)
      .with("statusReads", contentAccounting.statusReads + 1)
      .with(
        "admissionProbeBeforeObservation",
        writeWouldBeAdmitted(contentAccounting, 1)
      )
      .with(
        "admissionProbeAfterObservation",
        writeWouldBeAdmitted(contentAccounting, 1)
      )
      .with("quotaBeforeObservation", contentAccounting.quotaAuthoritativeBytes)
      .with("syncWritesBeforeObservation", contentAccounting.synchronousQuotaWrites)
      .with("lastTransition", StatusObservedAndSnapshotPersisted),
  }

  /// With quota enabled, acceptance synchronously reserves the exact request
  /// bytes in the authoritative ledger. Without quota, the same admission has
  /// no accounting persistence side effect, even if a derived snapshot exists.
  action beginWrite(requestSize: int): bool = all {
    contentAccounting.phase == AccountingReady,
    contentAccounting.acceptedWrites == 0,
    RequestSizes.contains(requestSize),
    if (writeWouldBeAdmitted(contentAccounting, requestSize)) {
      contentAccounting' = contentAccounting
        .with("phase", WritePrepared)
        .with("requestBytes", requestSize)
        .with("lastAdmission", AdmissionAccepted)
        .with("acceptedWrites", contentAccounting.acceptedWrites + 1)
        .with(
          "admissionQuotaTotal",
          contentAccounting.quotaAuthoritativeBytes + requestSize
        )
        .with("quotaBeforeAdmission", contentAccounting.quotaAuthoritativeBytes)
        .with("syncWritesBeforeAdmission", contentAccounting.synchronousQuotaWrites)
        .with(
          "quotaAuthoritativeBytes",
          if (contentAccounting.quotaMode == QuotaEnabled) {
            contentAccounting.quotaAuthoritativeBytes + requestSize
          } else {
            contentAccounting.quotaAuthoritativeBytes
          }
        )
        .with(
          "quotaReservationBytes",
          if (contentAccounting.quotaMode == QuotaEnabled) requestSize else 0
        )
        .with(
          "synchronousQuotaWrites",
          if (contentAccounting.quotaMode == QuotaEnabled) {
            contentAccounting.synchronousQuotaWrites + 1
          } else {
            contentAccounting.synchronousQuotaWrites
          }
        )
        .with("blobDurabilityEstablishedForRequest", false)
        .with("metadataPublishedForRequest", false)
        .with("lastTransition", WriteAdmissionAccepted)
    } else {
      contentAccounting' = contentAccounting
        .with("phase", WriteRejected)
        .with("requestBytes", requestSize)
        .with("lastAdmission", AdmissionRejected)
        .with("rejectedWrites", contentAccounting.rejectedWrites + 1)
        .with(
          "admissionQuotaTotal",
          contentAccounting.quotaAuthoritativeBytes + requestSize
        )
        .with("quotaBeforeAdmission", contentAccounting.quotaAuthoritativeBytes)
        .with("syncWritesBeforeAdmission", contentAccounting.synchronousQuotaWrites)
        .with("lastTransition", WriteAdmissionRejected)
    },
  }

  /// The in-memory derived counter may advance cheaply with durable content.
  /// Its persisted snapshot is intentionally untouched on the no-quota hot
  /// path and may therefore lag.
  action persistPreparedBlob = all {
    contentAccounting.phase == WritePrepared,
    contentAccounting.lastAdmission == AdmissionAccepted,
    contentAccounting.requestBytes > 0,
    contentAccounting' = contentAccounting
      .with("phase", BlobDurable)
      .with(
        "durableBlobBytes",
        contentAccounting.durableBlobBytes + contentAccounting.requestBytes
      )
      .with(
        "liveDerivedBytes",
        contentAccounting.liveDerivedBytes + contentAccounting.requestBytes
      )
      .with("quotaReservationBytes", 0)
      .with("blobDurabilityEstablishedForRequest", true)
      .with("lastTransition", PreparedBlobPersisted),
  }

  action publishMetadata = all {
    contentAccounting.phase == BlobDurable,
    contentAccounting.blobDurabilityEstablishedForRequest,
    not(contentAccounting.metadataPublishedForRequest),
    contentAccounting' = contentAccounting
      .with("phase", MetadataVisible)
      .with(
        "visibleMetadataBytes",
        contentAccounting.visibleMetadataBytes + contentAccounting.requestBytes
      )
      .with("metadataPublishedForRequest", true)
      .with("lastTransition", DurableMetadataPublished),
  }

  /// The durable blob, visible namespace, and authoritative quota ledger all
  /// survive. Only rebuildable observability state is discarded.
  action crashLoseDerivedStats = all {
    any {
      contentAccounting.phase == BlobDurable,
      contentAccounting.phase == MetadataVisible,
    },
    contentAccounting.crashes == 0,
    contentAccounting.liveDerivedBytes > 0,
    contentAccounting' = contentAccounting
      .with("phase", RuntimeCrashed)
      .with("liveDerivedBytes", 0)
      .with("persistedDerivedBytes", 0)
      .with("persistedSnapshotPresent", false)
      .with("durableBytesBeforeCrash", contentAccounting.durableBlobBytes)
      .with("visibleBytesBeforeCrash", contentAccounting.visibleMetadataBytes)
      .with("quotaBytesBeforeCrash", contentAccounting.quotaAuthoritativeBytes)
      .with("liveDerivedBytesBeforeCrash", contentAccounting.liveDerivedBytes)
      .with("crashes", contentAccounting.crashes + 1)
      .with("lastTransition", DerivedStatsLostOnCrash),
  }

  /// Rebuild scans durable blobs and changes no namespace or quota authority.
  action rebuildDerivedStatsFromDurableBlobs = all {
    contentAccounting.phase == RuntimeCrashed,
    contentAccounting.crashes == 1,
    contentAccounting.rebuilds == 0,
    contentAccounting' = contentAccounting
      .with("phase", AccountingReady)
      .with("liveDerivedBytes", contentAccounting.durableBlobBytes)
      .with("persistedDerivedBytes", contentAccounting.durableBlobBytes)
      .with("persistedSnapshotPresent", true)
      .with("statusReportedBytes", contentAccounting.durableBlobBytes)
      .with("rebuilds", contentAccounting.rebuilds + 1)
      .with("lastTransition", DerivedStatsRebuilt),
  }

  action stutter =
    contentAccounting' = contentAccounting.with("lastTransition", AccountingStuttered)

  action step = any {
    enableQuota,
    observeStatusAndPersistDerivedSnapshot,
    {
      nondet requestSize = oneOf(RequestSizes)
      beginWrite(requestSize)
    },
    persistPreparedBlob,
    publishMetadata,
    crashLoseDerivedStats,
    rebuildDerivedStatsFromDurableBlobs,
    stutter,
  }

  /// Coverage schedules call only real protocol actions. Symbolic verification
  /// continues to explore the unrestricted `step` relation.
  action noQuotaWriteAfterStatusCoverageStep =
    if (contentAccounting.phase == AccountingReady
      and contentAccounting.quotaMode == QuotaAbsent
      and contentAccounting.statusReads == 0
      and contentAccounting.acceptedWrites == 0) {
      observeStatusAndPersistDerivedSnapshot
    } else if (contentAccounting.phase == AccountingReady
      and contentAccounting.quotaMode == QuotaAbsent
      and contentAccounting.statusReads == 1
      and contentAccounting.acceptedWrites == 0) {
      beginWrite(1)
    } else if (contentAccounting.phase == WritePrepared
      and contentAccounting.quotaMode == QuotaAbsent) {
      persistPreparedBlob
    } else if (contentAccounting.phase == BlobDurable
      and contentAccounting.quotaMode == QuotaAbsent
      and contentAccounting.statusReads == 1) {
      publishMetadata
    } else {
      stutter
    }

  action quotaRejectCoverageStep =
    if (contentAccounting.phase == AccountingReady
      and contentAccounting.quotaMode == QuotaAbsent
      and contentAccounting.acceptedWrites == 0) {
      enableQuota
    } else if (contentAccounting.phase == AccountingReady
      and contentAccounting.quotaMode == QuotaEnabled
      and contentAccounting.acceptedWrites == 0) {
      beginWrite(2)
    } else {
      stutter
    }

  action quotaAcceptedCoverageStep =
    if (contentAccounting.phase == AccountingReady
      and contentAccounting.quotaMode == QuotaAbsent
      and contentAccounting.acceptedWrites == 0) {
      enableQuota
    } else if (contentAccounting.phase == AccountingReady
      and contentAccounting.quotaMode == QuotaEnabled
      and contentAccounting.acceptedWrites == 0) {
      beginWrite(1)
    } else if (contentAccounting.phase == WritePrepared
      and contentAccounting.quotaMode == QuotaEnabled) {
      persistPreparedBlob
    } else if (contentAccounting.phase == BlobDurable
      and contentAccounting.quotaMode == QuotaEnabled) {
      publishMetadata
    } else {
      stutter
    }

  action crashRebuildCoverageStep =
    if (contentAccounting.phase == AccountingReady
      and contentAccounting.quotaMode == QuotaAbsent
      and contentAccounting.statusReads == 0
      and contentAccounting.acceptedWrites == 0) {
      beginWrite(1)
    } else if (contentAccounting.phase == WritePrepared
      and contentAccounting.quotaMode == QuotaAbsent) {
      persistPreparedBlob
    } else if (contentAccounting.phase == BlobDurable
      and contentAccounting.quotaMode == QuotaAbsent
      and contentAccounting.statusReads == 0) {
      crashLoseDerivedStats
    } else if (contentAccounting.phase == RuntimeCrashed) {
      rebuildDerivedStatsFromDurableBlobs
    } else {
      stutter
    }

  action coverageStep = any {
    step,
    noQuotaWriteAfterStatusCoverageStep,
    quotaRejectCoverageStep,
    quotaAcceptedCoverageStep,
    crashRebuildCoverageStep,
  }
}
