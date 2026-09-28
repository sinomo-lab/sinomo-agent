#![feature(rustc_private)]

extern crate rustc_ast;
extern crate rustc_errors;
extern crate rustc_hir;
extern crate rustc_middle;
extern crate rustc_span;

use rustc_hir::{Expr, ExprKind, intravisit::FnKind};
use rustc_lint::{LateContext, LateLintPass, LintContext};
use rustc_middle::ty;
use rustc_span::{
    Span,
    def_id::{DefId, LocalDefId},
};
use std::collections::{HashMap, VecDeque};

dylint_linting::impl_late_lint! {
    /// Reject implicit KV access from DFS operations. A narrowly documented
    /// `sinomo:kv-consistency: REASON` function marks an intentional globally
    /// consistent operation. Configuration lifecycle work uses
    /// `sinomo:kv-lifecycle: REASON` and cannot be called by an ordinary operation.
    pub DFS_IMPLICIT_KV,
    Deny,
    "implicit KV operation on the DFS data path",
    DfsImplicitKv::default()
}

#[derive(Default)]
struct DfsImplicitKv {
    functions: HashMap<DefId, Function>,
    edges: HashMap<DefId, Vec<(DefId, Span)>>,
    routes: HashMap<DefId, Span>,
}

struct Function {
    span: Span,
    operation: bool,
    consistency: bool,
    lifecycle: bool,
}

#[allow(deprecated)] // Doc-string markers are intentionally unparsed attributes.
fn docs(cx: &LateContext<'_>, id: DefId) -> Vec<String> {
    cx.tcx
        .get_all_attrs(id)
        .iter()
        .filter_map(|attr| attr.doc_str())
        .map(|value| value.to_string())
        .collect()
}

fn marker(lines: &[String], prefix: &str) -> bool {
    lines.iter().any(|line| {
        line.trim()
            .strip_prefix(prefix)
            .is_some_and(|reason| reason.trim().len() >= 24)
    })
}

fn raw_kv(path: &str) -> bool {
    let method = path.rsplit("::").next().unwrap_or(path);
    let client = path.split("::").any(|part| {
        matches!(
            part,
            "DfsControlPlaneKvClient"
                | "AsyncDfsControlPlaneKvClient"
                | "DfsControlPlaneKvClient>"
                | "AsyncDfsControlPlaneKvClient>"
                | "KvClient"
        )
    });
    client
        || (path.contains("ClusterAccess") && method.starts_with("kv_"))
        || (path.contains("::InProcessKvStore::")
            && matches!(method, "get" | "put" | "txn" | "range_prefix"))
        || method.starts_with("read_cluster_kv_")
        || method == "read_cluster_members_from_kv"
        || method.starts_with("commit_cluster_kv_")
        || method.starts_with("submit_cluster_kv_")
        || method == "metadata_namespace_prepare_codec_admission_compares"
        || method == "metadata_namespace_prepare_codec_admission_compares_async"
}

impl<'tcx> LateLintPass<'tcx> for DfsImplicitKv {
    fn check_fn(
        &mut self,
        cx: &LateContext<'tcx>,
        _: FnKind<'tcx>,
        _: &'tcx rustc_hir::FnDecl<'tcx>,
        _: &'tcx rustc_hir::Body<'tcx>,
        span: Span,
        id: LocalDefId,
    ) {
        let id = id.to_def_id();
        let path = cx.tcx.def_path_str(id);
        let file = cx
            .sess()
            .source_map()
            .span_to_filename(span)
            .prefer_local_unconditionally()
            .to_string();
        let mut lines = docs(cx, id);
        let mut enclosing = id;
        while matches!(
            cx.tcx.def_kind(enclosing),
            rustc_hir::def::DefKind::Closure | rustc_hir::def::DefKind::SyntheticCoroutineBody
        ) {
            enclosing = cx.tcx.parent(enclosing);
            lines.extend(docs(cx, enclosing));
        }
        let test = path
            .split("::")
            .any(|part| part == "tests" || part.ends_with("_tests"));
        let client_impl = cx
            .tcx
            .opt_associated_item(id)
            .and_then(|item| item.trait_item_def_id())
            .is_some_and(|trait_item| {
                cx.tcx
                    .def_path_str(trait_item)
                    .contains("DfsControlPlaneKvClient")
            });
        let operation = !test
            && !client_impl
            && (lines
                .iter()
                .any(|line| line.trim() == "sinomo:dfs-operation")
                || file.contains("/sinomo-dfs/src/service_")
                || file.contains("/sinomo-dfs/src/posix")
                || file.contains("/sinomo-dfs/src/browser_service"));
        self.functions.insert(
            id,
            Function {
                span,
                operation,
                consistency: marker(&lines, "sinomo:kv-consistency:")
                    || marker(&lines, "sinomo:kv-configuration:"),
                lifecycle: marker(&lines, "sinomo:kv-lifecycle:"),
            },
        );
        // Resolve local trait wrappers conservatively, including dyn and generic
        // dispatch. A wrapper must not hide an implicit KV call from the lint.
        if let Some(item) = cx.tcx.opt_associated_item(id)
            && let Some(trait_item) = item.trait_item_def_id()
            && trait_item.is_local()
        {
            self.edges.entry(trait_item).or_default().push((id, span));
        }
    }

    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        let source = expr.hir_id.owner.def_id.to_def_id();
        let target = match expr.kind {
            ExprKind::MethodCall(..) => cx.typeck_results().type_dependent_def_id(expr.hir_id),
            ExprKind::Call(callee, _) => match cx.typeck_results().expr_ty(callee).kind() {
                ty::FnDef(id, _) => Some(*id),
                _ => None,
            },
            ExprKind::Closure(closure) => Some(closure.def_id.to_def_id()),
            ExprKind::Path(ref path) => match cx.qpath_res(path, expr.hir_id) {
                rustc_hir::def::Res::Def(kind, id)
                    if matches!(
                        kind,
                        rustc_hir::def::DefKind::Fn
                            | rustc_hir::def::DefKind::AssocFn
                            | rustc_hir::def::DefKind::Const { .. }
                            | rustc_hir::def::DefKind::AssocConst { .. }
                    ) =>
                {
                    Some(id)
                }
                _ => None,
            },
            ExprKind::Lit(lit) => {
                if let rustc_ast::LitKind::Str(value, _) = lit.node
                    && (value.as_str().starts_with("/cluster/kv/")
                        || value.as_str().starts_with("/daemon/cluster/kv/"))
                {
                    self.routes.insert(source, expr.span);
                }
                None
            }
            _ => None,
        };
        if let Some(target) = target {
            self.edges
                .entry(source)
                .or_default()
                .push((target, expr.span));
        }
    }

    fn check_crate_post(&mut self, cx: &LateContext<'tcx>) {
        // One reverse traversal computes all unsafe roots. Rewalking the full
        // service graph for each public entrypoint makes linting quadratic.
        let approved = |id: &DefId| self.functions.get(id).is_some_and(|f| f.consistency);
        let mut reverse: HashMap<DefId, Vec<(DefId, Span)>> = HashMap::new();
        let mut seeds = Vec::new();
        for (caller, edges) in &self.edges {
            for (callee, span) in edges {
                reverse.entry(*callee).or_default().push((*caller, *span));
                if !approved(callee) && raw_kv(&cx.tcx.def_path_str(*callee)) {
                    seeds.push(*callee);
                }
            }
        }
        seeds.extend(self.routes.keys().filter(|id| !approved(id)).copied());
        seeds.extend(
            self.functions
                .iter()
                .filter(|(_, f)| f.lifecycle)
                .map(|(id, _)| *id),
        );
        seeds.sort_by_key(|id| cx.tcx.def_path_str(*id));
        seeds.dedup();
        for callers in reverse.values_mut() {
            callers.sort_by_key(|(_, span)| span.lo());
        }
        let mut reaches: HashMap<DefId, Option<(DefId, Span)>> = HashMap::new();
        let mut pending = VecDeque::new();
        for seed in seeds {
            reaches.insert(seed, None);
            pending.push_back(seed);
        }
        while let Some(callee) = pending.pop_front() {
            for (caller, span) in reverse.get(&callee).into_iter().flatten() {
                if approved(caller) || reaches.contains_key(caller) {
                    continue;
                }
                reaches.insert(*caller, Some((callee, *span)));
                pending.push_back(*caller);
            }
        }
        let mut roots = self.functions.iter().collect::<Vec<_>>();
        roots.sort_by_key(|(_, function)| function.span.lo());
        for (root, function) in roots {
            if !function.operation || function.consistency || function.lifecycle {
                continue;
            }
            let Some(next) = reaches.get(root) else {
                continue;
            };
            let span = next
                .map(|(_, span)| span)
                .or_else(|| self.routes.get(root).copied())
                .unwrap_or(function.span);
            let mut chain = vec![cx.tcx.def_path_str(*root)];
            let mut cursor = *root;
            while let Some(Some((next, _))) = reaches.get(&cursor) {
                chain.push(cx.tcx.def_path_str(*next));
                cursor = *next;
            }
            cx.opt_span_lint(DFS_IMPLICIT_KV, Some(span), rustc_errors::DiagDecorator(|diag: &mut rustc_errors::Diag<'_, ()>| {
                diag.primary_message("DFS operation reaches KV outside an explicit consistency boundary");
                diag.note(format!("call path: {}", chain.join(" -> ")));
                diag.help("use existing configuration/network state; genuine global operations need a narrow documented sinomo:kv-consistency boundary");
            }));
        }
    }
}
