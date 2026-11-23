//! Shared interception of an actual native source phase, never synthetic proof.
use crate::DfsMessageCommand;
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum DfsSourcePausePoint {
    TargetDispatch,
    SourceAcquired,
    BeforeSourceRegistration,
    BeforeBind,
}
#[derive(Clone, Debug)]
pub struct DfsSourcePreparationCapture {
    pub(crate) plan: crate::source_adoption::SourcePreparationPlan,
    pub(crate) token: crate::source_adoption::SourcePreparationToken,
    pub(crate) job_offer: Option<crate::source_adoption::DfsSourceAdoptionJobOffer>,
}
#[derive(Clone, Debug)]
enum Capture {
    Target(DfsMessageCommand),
    Preparation(DfsSourcePreparationCapture),
}
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

#[derive(Debug)]
struct PauseInner {
    captured: Mutex<Option<Capture>>,
    ready: std::sync::Condvar,
    gate: tokio::sync::Semaphore,
}
static PAUSES: OnceLock<Mutex<BTreeMap<(PathBuf, DfsSourcePausePoint), Weak<PauseInner>>>> =
    OnceLock::new();
#[derive(Debug)]
pub struct DfsSourcePhasePause {
    root: PathBuf,
    point: DfsSourcePausePoint,
    inner: Arc<PauseInner>,
}
impl DfsSourcePhasePause {
    pub(crate) fn install(root: PathBuf) -> Result<Self, String> {
        Self::install_at(root, DfsSourcePausePoint::TargetDispatch)
    }
    pub(crate) fn install_at(root: PathBuf, point: DfsSourcePausePoint) -> Result<Self, String> {
        let inner = Arc::new(PauseInner {
            captured: Mutex::new(None),
            ready: std::sync::Condvar::new(),
            gate: tokio::sync::Semaphore::new(0),
        });
        let mut registry = PAUSES
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| "source pause registry poisoned")?;
        registry.retain(|_, pause| pause.strong_count() > 0);
        if registry
            .get(&(root.clone(), point))
            .and_then(Weak::upgrade)
            .is_some()
        {
            return Err("source phase pause already installed".into());
        }
        registry.insert((root.clone(), point), Arc::downgrade(&inner));
        Ok(Self { root, point, inner })
    }
    pub fn wait_captured(&self, budget: Duration) -> Result<DfsMessageCommand, String> {
        match self.wait(budget)? {
            Capture::Target(command) => Ok(command),
            _ => Err("captured source preparation rather than target command".into()),
        }
    }
    pub fn wait_preparation(
        &self,
        budget: Duration,
    ) -> Result<DfsSourcePreparationCapture, String> {
        match self.wait(budget)? {
            Capture::Preparation(capture) => Ok(capture),
            _ => Err("captured target command rather than source preparation".into()),
        }
    }
    fn wait(&self, budget: Duration) -> Result<Capture, String> {
        let end = Instant::now() + budget;
        let mut captured = self
            .inner
            .captured
            .lock()
            .map_err(|_| "source capture poisoned")?;
        while captured.is_none() {
            let remaining = end
                .checked_duration_since(Instant::now())
                .ok_or("actual source phase was not captured")?;
            captured = self
                .inner
                .ready
                .wait_timeout(captured, remaining)
                .map_err(|_| "source capture poisoned")?
                .0;
        }
        Ok(captured.as_ref().unwrap().clone())
    }
    pub fn release(&self) {
        self.inner.gate.add_permits(1);
    }
    /// Release the paused phase when the returned guard drops. A test that
    /// joins the paused call's thread in a scope takes the guard inside that
    /// scope: a step that fails while the call is paused then ends the call
    /// and is reported, where the scope would otherwise wait for a thread
    /// nothing releases.
    pub fn release_on_drop(&self) -> DfsSourcePhaseRelease<'_> {
        DfsSourcePhaseRelease(self)
    }
}
/// Releases its paused phase when dropped.
#[derive(Debug)]
pub struct DfsSourcePhaseRelease<'a>(&'a DfsSourcePhasePause);
impl Drop for DfsSourcePhaseRelease<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}
impl Drop for DfsSourcePhasePause {
    fn drop(&mut self) {
        self.inner.gate.close();
        if let Ok(mut registry) = PAUSES.get_or_init(Default::default).lock() {
            if registry
                .get(&(self.root.clone(), self.point))
                .and_then(Weak::upgrade)
                .is_some_and(|inner| Arc::ptr_eq(&inner, &self.inner))
            {
                registry.remove(&(self.root.clone(), self.point));
            }
        }
    }
}
pub(crate) fn capture_and_pause(
    root: &Path,
    command: &DfsMessageCommand,
    runtime: &tokio::runtime::Handle,
) {
    capture_at(
        root,
        DfsSourcePausePoint::TargetDispatch,
        Capture::Target(command.clone()),
        runtime,
    );
}
pub(crate) fn capture_preparation_and_pause(
    root: &Path,
    point: DfsSourcePausePoint,
    plan: &crate::source_adoption::SourcePreparationPlan,
    token: &crate::source_adoption::SourcePreparationToken,
    job_offer: Option<&crate::source_adoption::DfsSourceAdoptionJobOffer>,
    runtime: &tokio::runtime::Handle,
) {
    capture_at(
        root,
        point,
        Capture::Preparation(DfsSourcePreparationCapture {
            plan: plan.clone(),
            token: token.clone(),
            job_offer: job_offer.cloned(),
        }),
        runtime,
    );
}
fn capture_at(
    root: &Path,
    point: DfsSourcePausePoint,
    value: Capture,
    runtime: &tokio::runtime::Handle,
) {
    let pause = PAUSES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(&(root.to_owned(), point))
        .and_then(|pause| pause.upgrade());
    let Some(pause) = pause else {
        return;
    };
    {
        let mut captured = pause.captured.lock().unwrap();
        *captured = Some(value);
        pause.ready.notify_all();
    }
    let _ = tokio::task::block_in_place(|| runtime.block_on(pause.gate.acquire()))
        .map(|permit| permit.forget());
}
