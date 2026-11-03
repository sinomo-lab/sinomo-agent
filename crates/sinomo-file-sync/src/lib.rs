//! Durable filesystem writeback staging and execution shared by host adapters.
//!
//! Callers supply their storage owner and exact scope. They own admission,
//! source capture and destination authority; this crate owns local durability,
//! immutable blobs, ambiguity reconciliation and completion ordering.
//! Persisted fields and hash domains are stable storage contracts.
//!
//! ```no_run
//! use sinomo_file_sync::DurableWritebackQueue;
//! use sinomo_core::local::SinomoDataDir;
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let data = SinomoDataDir::from_env_or_home()?;
//! let queue = DurableWritebackQueue::open(
//!     "module.files.external-edit", &data, "editor-session-id", "/services",
//! )?;
//! // Stage immutable bytes, enqueue their exact preconditions, then use the
//! // executor with an authority-owning mutation adapter and completion effect.
//! # Ok(())
//! # }
//! ```

pub mod local_change_queue;
pub mod sync_executor;
pub use local_change_queue::WritebackLocalChangeQueue as DurableWritebackQueue;
pub use local_change_queue::*;
pub use sync_executor::*;
