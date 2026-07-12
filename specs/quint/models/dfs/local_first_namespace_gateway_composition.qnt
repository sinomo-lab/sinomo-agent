/// Composition of the bounded local-first namespace branch with the typed
/// reconciliation publication gateway.
///
/// Namespace planning may wait while the gateway collects and persists its
/// evidence, but the branch cannot reach a terminal result independently.
/// The single terminal action below commits the namespace result and the
/// matching gateway journal envelope in the same transition.
module dfs_local_first_namespace_gateway_composition {
  import common.* from "../../lib/common"
  import dfs_local_first_namespace from "local_first_namespace"
  import dfs_local_first_reconcile_gateway from "local_first_reconcile_gateway"
  import dfs_local_first_reconcile_permit from "../../contracts/dfs/local_first_reconcile_permit"

  /// The gateway's object revision is a generic bounded data-mutation
  /// revision in this composition. Namespace journal-head-only transactions
  /// deliberately do not advance it.
  var namespaceGatewayMutationRevision: int

  /// Keep one bounded journal slot available for the branch's terminal entry.
  /// Without this guard, environment-only strong commits could exhaust the
  /// gateway's finite exploration bound and strand an otherwise valid branch.
  def namespaceGatewayReconcileSlotReserved: bool =
    dfs_local_first_reconcile_gateway::gatewayCanonical.journalHead
      < dfs_local_first_reconcile_gateway::MaxGatewayJournalHead - 1

  pure def namespaceGatewayOutcome(
    namespaceOutcome: dfs_local_first_namespace::ReconcileOutcome
  ): dfs_local_first_reconcile_permit::ReconcileOutcome =
    match namespaceOutcome {
      | Applicable =>
          dfs_local_first_reconcile_permit::PublishApply
      | Converged =>
          dfs_local_first_reconcile_permit::PublishConverged
      | Conflict =>
          dfs_local_first_reconcile_permit::PublishConflict
      | Superseded =>
          dfs_local_first_reconcile_permit::PublishSuperseded
      | Unclassified =>
          dfs_local_first_reconcile_permit::PublishSuperseded
    }

  /// Only the exact one-operation manifests reachable from the bounded
  /// namespace branch are admitted to the publication gateway.
  pure def namespaceGatewayManifestAdmitted(
    operation: dfs_local_first_namespace::NamespaceOperation,
    source: dfs_local_first_namespace::EntryKey,
    destination: dfs_local_first_namespace::EntryKey
  ): bool =
    match operation {
      | CreateFile => all {
          source == destination,
          any {
            destination == dfs_local_first_namespace::RootSpareEntry,
            destination == dfs_local_first_namespace::DirectoryChildEntry,
          },
        }
      | MakeDirectory => all {
          source == dfs_local_first_namespace::RootSpareEntry,
          destination == dfs_local_first_namespace::RootSpareEntry,
        }
      | UnlinkFile => all {
          source == dfs_local_first_namespace::RootFileEntry,
          destination == dfs_local_first_namespace::RootFileEntry,
        }
      | RemoveDirectory => all {
          source == dfs_local_first_namespace::RootDirectoryEntry,
          destination == dfs_local_first_namespace::RootDirectoryEntry,
        }
      | RenameEntry => all {
          any {
            source == dfs_local_first_namespace::RootFileEntry,
            source == dfs_local_first_namespace::RootDirectoryEntry,
          },
          destination == dfs_local_first_namespace::RootSpareEntry,
        }
      | NoNamespaceOperation => false
    }

  pure def namespaceGatewayPayloadDigest(
    operation: dfs_local_first_namespace::NamespaceOperation,
    source: dfs_local_first_namespace::EntryKey,
    destination: dfs_local_first_namespace::EntryKey
  ): dfs_local_first_reconcile_permit::ReconcilePayloadDigest =
    match operation {
      | CreateFile =>
          if (destination
            == dfs_local_first_namespace::DirectoryChildEntry) {
            dfs_local_first_reconcile_permit::NamespaceCreateChildPayload
          } else {
            dfs_local_first_reconcile_permit::NamespaceCreateRootPayload
          }
      | MakeDirectory =>
          dfs_local_first_reconcile_permit::NamespaceMkdirPayload
      | UnlinkFile =>
          dfs_local_first_reconcile_permit::NamespaceUnlinkPayload
      | RemoveDirectory =>
          dfs_local_first_reconcile_permit::NamespaceRmdirPayload
      | RenameEntry =>
          if (source == dfs_local_first_namespace::RootDirectoryEntry) {
            dfs_local_first_reconcile_permit::NamespaceRenameDirectoryPayload
          } else {
            dfs_local_first_reconcile_permit::NamespaceRenameFilePayload
          }
      | NoNamespaceOperation =>
          dfs_local_first_reconcile_permit::NamespaceCreateRootPayload
    }

  def currentNamespaceGatewayPayloadDigest:
    dfs_local_first_reconcile_permit::ReconcilePayloadDigest =
    namespaceGatewayPayloadDigest(
      dfs_local_first_namespace::branchOperation,
      dfs_local_first_namespace::sourceEntry,
      dfs_local_first_namespace::destinationEntry
    )

  action init = all {
    dfs_local_first_namespace::init,
    dfs_local_first_reconcile_gateway::init,
    namespaceGatewayMutationRevision' = 0,
  }

  action beginComposedNamespaceBranch: bool = all {
    dfs_local_first_namespace::beginBranch,
    dfs_local_first_reconcile_gateway::stutterGateway,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action stageComposedNamespaceCreate(
    entry: dfs_local_first_namespace::EntryKey
  ): bool = all {
    dfs_local_first_namespace::stageCreate(entry),
    dfs_local_first_reconcile_gateway::stutterGateway,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action stageComposedNamespaceMkdir: bool = all {
    dfs_local_first_namespace::stageMkdir(
      dfs_local_first_namespace::RootSpareEntry
    ),
    dfs_local_first_reconcile_gateway::stutterGateway,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action stageComposedNamespaceUnlink: bool = all {
    dfs_local_first_namespace::stageUnlink(
      dfs_local_first_namespace::RootFileEntry
    ),
    dfs_local_first_reconcile_gateway::stutterGateway,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action stageComposedNamespaceRmdir: bool = all {
    dfs_local_first_namespace::stageRmdir(
      dfs_local_first_namespace::RootDirectoryEntry
    ),
    dfs_local_first_reconcile_gateway::stutterGateway,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action stageComposedNamespaceRename(
    source: dfs_local_first_namespace::EntryKey
  ): bool = all {
    dfs_local_first_namespace::stageRename(
      source,
      dfs_local_first_namespace::RootSpareEntry
    ),
    dfs_local_first_reconcile_gateway::stutterGateway,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action sealComposedNamespaceBranch: bool = all {
    dfs_local_first_namespace::sealBranch,
    dfs_local_first_reconcile_gateway::stutterGateway,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action commitComposedNamespaceStrongUnrelated: bool = all {
    namespaceGatewayReconcileSlotReserved,
    dfs_local_first_namespace::commitStrongUnrelated,
    dfs_local_first_reconcile_gateway::advanceStrongCanonicalHeadOnly,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action commitComposedNamespaceStrongEquivalent: bool = all {
    namespaceGatewayReconcileSlotReserved,
    dfs_local_first_namespace::commitStrongEquivalent,
    dfs_local_first_reconcile_gateway::advanceStrongCanonicalHead,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision + 1,
  }

  action commitComposedNamespaceStrongConflict: bool = all {
    namespaceGatewayReconcileSlotReserved,
    dfs_local_first_namespace::commitStrongConflict,
    dfs_local_first_reconcile_gateway::advanceStrongCanonicalHead,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision + 1,
  }

  action commitComposedNamespaceStrongSameObjectAba: bool = all {
    namespaceGatewayReconcileSlotReserved,
    dfs_local_first_namespace::commitStrongSameObjectAba,
    dfs_local_first_reconcile_gateway::advanceStrongCanonicalHead,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision + 1,
  }

  action commitComposedNamespaceStrongDirectoryChild: bool = all {
    namespaceGatewayReconcileSlotReserved,
    dfs_local_first_namespace::commitStrongDirectoryChild,
    dfs_local_first_reconcile_gateway::advanceStrongCanonicalHead,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision + 1,
  }

  action planComposedNamespaceGateway: bool = {
    val operation = dfs_local_first_namespace::branchOperation
    val source = dfs_local_first_namespace::sourceEntry
    val destination = dfs_local_first_namespace::destinationEntry
    val outcome = dfs_local_first_namespace::classifyReconciliation
    all {
      namespaceGatewayManifestAdmitted(operation, source, destination),
      outcome != dfs_local_first_namespace::Unclassified,
      dfs_local_first_namespace::prepareReconcilePlan,
      dfs_local_first_reconcile_gateway::planReconcilePublication(
        namespaceGatewayPayloadDigest(operation, source, destination),
        namespaceGatewayOutcome(outcome)
      ),
      namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
    }
  }

  /// A precommit crash loses only the gateway's volatile plan. The immutable
  /// namespace plan can recreate it while its predecessor head remains fresh.
  action resumeComposedNamespaceGatewayPlan: bool = all {
    dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::BranchPlanned,
    dfs_local_first_reconcile_gateway::gatewayPhase
      == dfs_local_first_reconcile_gateway::GatewayIdle,
    dfs_local_first_namespace::canonicalHead
      == dfs_local_first_namespace::planHead,
    dfs_local_first_namespace::planOutcome
      != dfs_local_first_namespace::Unclassified,
    namespaceGatewayManifestAdmitted(
      dfs_local_first_namespace::branchOperation,
      dfs_local_first_namespace::sourceEntry,
      dfs_local_first_namespace::destinationEntry
    ),
    dfs_local_first_namespace::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::planReconcilePublication(
      currentNamespaceGatewayPayloadDigest,
      namespaceGatewayOutcome(dfs_local_first_namespace::planOutcome)
    ),
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action collectComposedNamespaceGatewayEvidence: bool = all {
    dfs_local_first_namespace::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::collectAllGatewayEvidence,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action buildComposedNamespaceGatewayPermit: bool = all {
    dfs_local_first_namespace::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::buildReconcilePermit,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action appendComposedNamespaceGatewayEntry: bool = all {
    dfs_local_first_namespace::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::appendReconcileEntry,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  /// This is the only composed namespace terminal transition. An Applicable
  /// result changes both the namespace map and the gateway object revision;
  /// all other results commit only their common result/journal envelope.
  action commitComposedNamespaceTerminal(deliverReply: bool): bool = {
    val outcome = dfs_local_first_namespace::planOutcome
    all {
      dfs_local_first_namespace::commitReconcilePlan,
      dfs_local_first_reconcile_gateway::commitReconcileEntry(deliverReply),
      namespaceGatewayMutationRevision' =
        if (outcome == dfs_local_first_namespace::Applicable) {
          namespaceGatewayMutationRevision + 1
        } else {
          namespaceGatewayMutationRevision
        },
    }
  }

  action rejectComposedNamespaceStalePlan: bool = all {
    dfs_local_first_namespace::rejectStalePlan,
    dfs_local_first_reconcile_gateway::rejectStaleGatewayPublication,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  /// After a precommit crash, a strong transaction may invalidate the retained
  /// namespace plan while no volatile gateway plan exists to reject.
  action rejectComposedNamespaceStalePlanWithoutGateway: bool = all {
    dfs_local_first_reconcile_gateway::gatewayPhase
      == dfs_local_first_reconcile_gateway::GatewayIdle,
    dfs_local_first_namespace::rejectStalePlan,
    dfs_local_first_reconcile_gateway::stutterGateway,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action crashComposedNamespaceGatewayBeforeCommit: bool = all {
    dfs_local_first_namespace::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::crashGatewayBeforeCommit,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action restartComposedNamespaceGatewayBeforeCommit: bool = all {
    dfs_local_first_namespace::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::restartGatewayProcess,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action crashComposedNamespaceGatewayAfterCommit: bool = all {
    dfs_local_first_namespace::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::crashGatewayAfterCommit,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action restartComposedNamespaceGatewayAfterCommit: bool = all {
    dfs_local_first_namespace::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::restartGatewayProcess,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action materializeComposedNamespaceGateway: bool = all {
    dfs_local_first_namespace::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::materializeCommittedReconcileEntry,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action retryExactComposedNamespaceResult: bool = all {
    dfs_local_first_namespace::retryTerminalResult,
    dfs_local_first_reconcile_gateway::retryCommittedResult(
      currentNamespaceGatewayPayloadDigest
    ),
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action stutterComposedNamespaceTerminal: bool = all {
    dfs_local_first_namespace::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::stutterGateway,
    namespaceGatewayMutationRevision' = namespaceGatewayMutationRevision,
  }

  action step = any {
    beginComposedNamespaceBranch,
    {
      nondet entry = oneOf(dfs_local_first_namespace::CreatableEntries)
      stageComposedNamespaceCreate(entry)
    },
    stageComposedNamespaceMkdir,
    stageComposedNamespaceUnlink,
    stageComposedNamespaceRmdir,
    {
      nondet source = oneOf(dfs_local_first_namespace::RenameSources)
      stageComposedNamespaceRename(source)
    },
    sealComposedNamespaceBranch,
    commitComposedNamespaceStrongUnrelated,
    commitComposedNamespaceStrongEquivalent,
    commitComposedNamespaceStrongConflict,
    commitComposedNamespaceStrongSameObjectAba,
    commitComposedNamespaceStrongDirectoryChild,
    planComposedNamespaceGateway,
    resumeComposedNamespaceGatewayPlan,
    collectComposedNamespaceGatewayEvidence,
    buildComposedNamespaceGatewayPermit,
    appendComposedNamespaceGatewayEntry,
    {
      nondet deliverReply = oneOf(Set(true, false))
      commitComposedNamespaceTerminal(deliverReply)
    },
    rejectComposedNamespaceStalePlan,
    rejectComposedNamespaceStalePlanWithoutGateway,
    crashComposedNamespaceGatewayBeforeCommit,
    restartComposedNamespaceGatewayBeforeCommit,
    crashComposedNamespaceGatewayAfterCommit,
    restartComposedNamespaceGatewayAfterCommit,
    materializeComposedNamespaceGateway,
    retryExactComposedNamespaceResult,
    stutterComposedNamespaceTerminal,
  }
}
