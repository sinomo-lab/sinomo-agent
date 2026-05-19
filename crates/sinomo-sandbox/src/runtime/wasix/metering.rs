use crate::SandboxWasiError;
use std::{
    fmt,
    sync::{Arc, Mutex},
};
use wasmer::{
    ExportIndex, GlobalInit, GlobalType, LocalFunctionIndex, Mutability, Type,
    sys::{
        CompilerConfig, Cranelift, EngineBuilder, Features, FunctionMiddleware, MiddlewareError,
        MiddlewareReaderState, ModuleMiddleware,
    },
    wasmparser::{BlockType as WasmParserBlockType, Operator},
};
use wasmer_types::{FunctionIndex, GlobalIndex, ImportIndex, ModuleInfo};

const WASIX_METERING_POINTS_PER_TIMEOUT_MS: u64 = 100_000;
const WASIX_SIGNAL_CHECK_INTERVAL_POINTS: u64 = 10_000;

pub(super) fn wasix_engine(timeout_ms: u64, instruction_metering: bool) -> wasmer::Engine {
    let mut compiler = Cranelift::default();
    if instruction_metering {
        compiler.push_middleware(Arc::new(SinomoWasixMetering::new(
            wasix_metering_initial_limit(timeout_ms),
            wasix_operator_cost,
        )));
    }
    let features = Features::all();
    EngineBuilder::new(compiler)
        .set_features(Some(features))
        .into()
}

pub(super) fn wasix_module_supports_instruction_metering(
    wasm: &[u8],
) -> Result<bool, SandboxWasiError> {
    if !wasm.starts_with(b"\0asm") {
        return Ok(true);
    }
    for payload in wasmparser::Parser::new(0).parse_all(wasm) {
        match payload.map_err(|error| SandboxWasiError::Runtime {
            detail: format!("parse WASIX module metering compatibility: {error}"),
        })? {
            wasmparser::Payload::ImportSection(imports) => {
                for import in imports.into_imports() {
                    let import = import.map_err(|error| SandboxWasiError::Runtime {
                        detail: format!("parse WASIX import for metering compatibility: {error}"),
                    })?;
                    if wasix_import_is_threading(import.module, import.name) {
                        return Ok(false);
                    }
                    if let wasmparser::TypeRef::Memory(memory) = import.ty
                        && memory.shared
                    {
                        return Ok(false);
                    }
                }
            }
            wasmparser::Payload::MemorySection(memories) => {
                for memory in memories {
                    if memory
                        .map_err(|error| SandboxWasiError::Runtime {
                            detail: format!(
                                "parse WASIX memory section for metering compatibility: {error}"
                            ),
                        })?
                        .shared
                    {
                        return Ok(false);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(true)
}

fn wasix_import_is_threading(module: &str, name: &str) -> bool {
    matches!(
        module,
        "wasix_32v1" | "wasix_64v1" | "wasi" | "wasi_unstable" | "wasi_snapshot_preview1"
    ) && matches!(
        name,
        "thread_spawn"
            | "thread_spawn_v2"
            | "thread-spawn"
            | "thread_join"
            | "thread_exit"
            | "thread_id"
            | "thread_sleep"
            | "sleep"
            | "usleep"
            | "nanosleep"
            | "clock_nanosleep"
            | "thread_signal"
            | "thread_parallelism"
            | "futex_wait"
            | "futex_wake"
    )
}

fn wasix_metering_initial_limit(timeout_ms: u64) -> u64 {
    timeout_ms
        .saturating_mul(WASIX_METERING_POINTS_PER_TIMEOUT_MS)
        .max(WASIX_METERING_POINTS_PER_TIMEOUT_MS)
}

fn wasix_operator_cost(operator: &Operator<'_>) -> u64 {
    if wasix_metering_is_accounting(operator) {
        1
    } else {
        0
    }
}

pub(super) fn is_wasix_metering_exhaustion(error: &str) -> bool {
    error.contains("wasmer_metering_points_exhausted")
}

#[derive(Clone, Debug)]
struct WasixMeteringGlobalIndexes {
    remaining_points: GlobalIndex,
    points_exhausted: GlobalIndex,
    signal_checkpoint: Option<FunctionIndex>,
}

struct SinomoWasixMetering<F: Fn(&Operator<'_>) -> u64 + Send + Sync> {
    initial_limit: u64,
    cost_function: Arc<F>,
    current_module_globals: Mutex<Option<WasixMeteringGlobalIndexes>>,
}

impl<F: Fn(&Operator<'_>) -> u64 + Send + Sync> SinomoWasixMetering<F> {
    fn new(initial_limit: u64, cost_function: F) -> Self {
        Self {
            initial_limit,
            cost_function: Arc::new(cost_function),
            current_module_globals: Mutex::new(None),
        }
    }
}

impl<F: Fn(&Operator<'_>) -> u64 + Send + Sync> fmt::Debug for SinomoWasixMetering<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SinomoWasixMetering")
            .field("initial_limit", &self.initial_limit)
            .field("cost_function", &"<function>")
            .field("current_module_globals", &self.current_module_globals)
            .finish()
    }
}

impl<F: Fn(&Operator<'_>) -> u64 + Send + Sync + 'static> ModuleMiddleware
    for SinomoWasixMetering<F>
{
    fn generate_function_middleware(
        &self,
        _local_function_index: LocalFunctionIndex,
    ) -> Box<dyn FunctionMiddleware> {
        Box::new(SinomoWasixFunctionMetering {
            cost_function: Arc::clone(&self.cost_function),
            global_indexes: self
                .current_module_globals
                .lock()
                .expect("WASIX metering global indexes")
                .clone()
                .expect("WASIX metering globals are initialized before functions"),
            accumulated_cost: 0,
            accumulated_signal_check_cost: 0,
        })
    }

    fn transform_module_info(&self, module_info: &mut ModuleInfo) -> Result<(), MiddlewareError> {
        let remaining_points = module_info
            .globals
            .push(GlobalType::new(Type::I64, Mutability::Var));
        module_info
            .global_initializers
            .push(GlobalInit::I64Const(self.initial_limit as i64));
        module_info.exports.insert(
            "wasmer_metering_remaining_points".to_string(),
            ExportIndex::Global(remaining_points),
        );

        let points_exhausted = module_info
            .globals
            .push(GlobalType::new(Type::I32, Mutability::Var));
        module_info
            .global_initializers
            .push(GlobalInit::I32Const(0));
        module_info.exports.insert(
            "wasmer_metering_points_exhausted".to_string(),
            ExportIndex::Global(points_exhausted),
        );

        *self
            .current_module_globals
            .lock()
            .expect("WASIX metering global indexes") = Some(WasixMeteringGlobalIndexes {
            remaining_points,
            points_exhausted,
            signal_checkpoint: wasix_signal_checkpoint_import(module_info),
        });

        Ok(())
    }
}

struct SinomoWasixFunctionMetering<F: Fn(&Operator<'_>) -> u64 + Send + Sync> {
    cost_function: Arc<F>,
    global_indexes: WasixMeteringGlobalIndexes,
    accumulated_cost: u64,
    accumulated_signal_check_cost: u64,
}

impl<F: Fn(&Operator<'_>) -> u64 + Send + Sync> fmt::Debug for SinomoWasixFunctionMetering<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SinomoWasixFunctionMetering")
            .field("cost_function", &"<function>")
            .field("global_indexes", &self.global_indexes)
            .field(
                "accumulated_signal_check_cost",
                &self.accumulated_signal_check_cost,
            )
            .finish()
    }
}

impl<F: Fn(&Operator<'_>) -> u64 + Send + Sync> FunctionMiddleware
    for SinomoWasixFunctionMetering<F>
{
    fn feed<'a>(
        &mut self,
        operator: Operator<'a>,
        state: &mut MiddlewareReaderState<'a>,
    ) -> Result<(), MiddlewareError> {
        self.accumulated_cost = self
            .accumulated_cost
            .saturating_add((self.cost_function)(&operator));
        self.accumulated_signal_check_cost = self
            .accumulated_signal_check_cost
            .saturating_add((self.cost_function)(&operator));

        if wasix_metering_is_accounting(&operator) && self.accumulated_cost > 0 {
            if let Some(signal_checkpoint) = self.global_indexes.signal_checkpoint
                && self.accumulated_signal_check_cost >= WASIX_SIGNAL_CHECK_INTERVAL_POINTS
            {
                state.extend(&[
                    Operator::Call {
                        function_index: signal_checkpoint.as_u32(),
                    },
                    Operator::Drop,
                ]);
                self.accumulated_signal_check_cost = 0;
            }
            state.extend(&[
                Operator::GlobalGet {
                    global_index: self.global_indexes.remaining_points.as_u32(),
                },
                Operator::I64Const {
                    value: self.accumulated_cost as i64,
                },
                Operator::I64LtU,
                Operator::If {
                    blockty: WasmParserBlockType::Empty,
                },
                Operator::I32Const { value: 1 },
                Operator::GlobalSet {
                    global_index: self.global_indexes.points_exhausted.as_u32(),
                },
                Operator::Unreachable,
                Operator::End,
                Operator::GlobalGet {
                    global_index: self.global_indexes.remaining_points.as_u32(),
                },
                Operator::I64Const {
                    value: self.accumulated_cost as i64,
                },
                Operator::I64Sub,
                Operator::GlobalSet {
                    global_index: self.global_indexes.remaining_points.as_u32(),
                },
            ]);
            self.accumulated_cost = 0;
        }

        state.push_operator(operator);
        Ok(())
    }
}

fn wasix_signal_checkpoint_import(module_info: &ModuleInfo) -> Option<FunctionIndex> {
    module_info.imports.iter().find_map(|(key, import_index)| {
        if key.field != "sched_yield" {
            return None;
        }
        let ImportIndex::Function(function_index) = import_index else {
            return None;
        };
        let signature_index = *module_info.functions.get(*function_index)?;
        let signature = module_info.signatures.get(signature_index)?;
        if signature.params().is_empty()
            && signature.results().len() == 1
            && matches!(signature.results()[0], Type::I32)
        {
            Some(*function_index)
        } else {
            None
        }
    })
}

fn wasix_metering_is_accounting(operator: &Operator<'_>) -> bool {
    matches!(
        operator,
        Operator::Loop { .. }
            | Operator::End
            | Operator::If { .. }
            | Operator::Else
            | Operator::Br { .. }
            | Operator::BrTable { .. }
            | Operator::BrIf { .. }
            | Operator::Call { .. }
            | Operator::CallIndirect { .. }
            | Operator::Return
            | Operator::Throw { .. }
            | Operator::ThrowRef
            | Operator::Rethrow { .. }
            | Operator::Delegate { .. }
            | Operator::Catch { .. }
            | Operator::ReturnCall { .. }
            | Operator::ReturnCallIndirect { .. }
            | Operator::BrOnCast { .. }
            | Operator::BrOnCastFail { .. }
            | Operator::CallRef { .. }
            | Operator::ReturnCallRef { .. }
            | Operator::BrOnNull { .. }
            | Operator::BrOnNonNull { .. }
    )
}
