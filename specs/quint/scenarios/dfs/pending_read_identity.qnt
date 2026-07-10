/// Deterministic regression for DFS read-response identity.
///
/// A wrong source first answers `Req1`; that response is discarded while the
/// original request remains pending, after which the correct response settles
/// it. The expected source then answers `Req2` with the wrong object; that
/// response is terminally rejected and cannot settle the request. Both
/// identity errors remain available as audit records.
module dfs_pending_read_identity_scenario {
  import common.* from "../../lib/common"
  import dfs_pending_request_lifecycle from "../../models/dfs/pending_request_lifecycle"

  type Phase =
    | Start
    | Read1Pending
    | WrongSourceRejected
    | Read1Settled
    | Read2Pending
    | WrongObjectRejected

  var phase: Phase

  action init = all {
    dfs_pending_request_lifecycle::init,
    phase' = Start,
  }

  action step =
    if (phase == Start) {
      all {
        dfs_pending_request_lifecycle::beginRead(
          dfs_pending_request_lifecycle::Req1,
          N1,
          dfs_pending_request_lifecycle::ObjA
        ),
        phase' = Read1Pending,
      }
    } else if (phase == Read1Pending) {
      all {
        dfs_pending_request_lifecycle::receiveReadResponse(
          dfs_pending_request_lifecycle::Req1,
          N2,
          dfs_pending_request_lifecycle::ObjA
        ),
        phase' = WrongSourceRejected,
      }
    } else if (phase == WrongSourceRejected) {
      all {
        dfs_pending_request_lifecycle::receiveReadResponse(
          dfs_pending_request_lifecycle::Req1,
          N1,
          dfs_pending_request_lifecycle::ObjA
        ),
        phase' = Read1Settled,
      }
    } else if (phase == Read1Settled) {
      all {
        dfs_pending_request_lifecycle::beginRead(
          dfs_pending_request_lifecycle::Req2,
          N1,
          dfs_pending_request_lifecycle::ObjA
        ),
        phase' = Read2Pending,
      }
    } else if (phase == Read2Pending) {
      all {
        dfs_pending_request_lifecycle::receiveReadResponse(
          dfs_pending_request_lifecycle::Req2,
          N1,
          dfs_pending_request_lifecycle::ObjB
        ),
        phase' = WrongObjectRejected,
      }
    } else {
      all {
        phase == WrongObjectRejected,
        dfs_pending_request_lifecycle::stutter,
        phase' = phase,
      }
    }

  def safety: bool =
    match phase {
      | Start => all {
          dfs_pending_request_lifecycle::readIdentityErrors == Set(),
          dfs_pending_request_lifecycle::settled == Set(),
        }
      | Read1Pending => all {
          dfs_pending_request_lifecycle::pendingReads.get(
            dfs_pending_request_lifecycle::Req1
          ) == Some({
            source: N1,
            object: dfs_pending_request_lifecycle::ObjA,
          }),
          not(dfs_pending_request_lifecycle::settled.contains(
            dfs_pending_request_lifecycle::Req1
          )),
        }
      | WrongSourceRejected => all {
          dfs_pending_request_lifecycle::pendingReads.get(
            dfs_pending_request_lifecycle::Req1
          ) == Some({
            source: N1,
            object: dfs_pending_request_lifecycle::ObjA,
          }),
          dfs_pending_request_lifecycle::discarded.contains(
            dfs_pending_request_lifecycle::Req1
          ),
          not(dfs_pending_request_lifecycle::settled.contains(
            dfs_pending_request_lifecycle::Req1
          )),
          dfs_pending_request_lifecycle::lastObservation
            == dfs_pending_request_lifecycle::ReadWrongSourceDiscarded(
              dfs_pending_request_lifecycle::Req1
            ),
          dfs_pending_request_lifecycle::readIdentityErrors.contains({
            requestId: dfs_pending_request_lifecycle::Req1,
            expectedSource: N1,
            actualSource: N2,
            expectedObject: dfs_pending_request_lifecycle::ObjA,
            actualObject: dfs_pending_request_lifecycle::ObjA,
          }),
        }
      | Read1Settled => all {
          dfs_pending_request_lifecycle::pendingReads.get(
            dfs_pending_request_lifecycle::Req1
          ) == None,
          dfs_pending_request_lifecycle::settled.contains(
            dfs_pending_request_lifecycle::Req1
          ),
          dfs_pending_request_lifecycle::lastObservation
            == dfs_pending_request_lifecycle::ReadSettled(
              dfs_pending_request_lifecycle::Req1
            ),
        }
      | Read2Pending => all {
          dfs_pending_request_lifecycle::pendingReads.get(
            dfs_pending_request_lifecycle::Req2
          ) == Some({
            source: N1,
            object: dfs_pending_request_lifecycle::ObjA,
          }),
          not(dfs_pending_request_lifecycle::settled.contains(
            dfs_pending_request_lifecycle::Req2
          )),
        }
      | WrongObjectRejected => all {
          dfs_pending_request_lifecycle::pendingReads.get(
            dfs_pending_request_lifecycle::Req2
          ) == None,
          dfs_pending_request_lifecycle::discarded.contains(
            dfs_pending_request_lifecycle::Req2
          ),
          not(dfs_pending_request_lifecycle::settled.contains(
            dfs_pending_request_lifecycle::Req2
          )),
          dfs_pending_request_lifecycle::lastObservation
            == dfs_pending_request_lifecycle::ReadWrongObjectRejected(
              dfs_pending_request_lifecycle::Req2
            ),
          dfs_pending_request_lifecycle::readIdentityErrors.contains({
            requestId: dfs_pending_request_lifecycle::Req1,
            expectedSource: N1,
            actualSource: N2,
            expectedObject: dfs_pending_request_lifecycle::ObjA,
            actualObject: dfs_pending_request_lifecycle::ObjA,
          }),
          dfs_pending_request_lifecycle::readIdentityErrors.contains({
            requestId: dfs_pending_request_lifecycle::Req2,
            expectedSource: N1,
            actualSource: N1,
            expectedObject: dfs_pending_request_lifecycle::ObjA,
            actualObject: dfs_pending_request_lifecycle::ObjB,
          }),
        }
    }
}
