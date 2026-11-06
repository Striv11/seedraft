//! seedraft: a deterministic simulation testing harness (and, in later days, Raft on top of it).
//!
//! Everything nondeterministic (time, message delay, loss, reordering) is derived from one
//! `u64` seed, so any run can be replayed exactly.

pub mod disk;
pub mod kv;
pub mod lin;
pub mod raft;
pub mod rng;
pub mod runner;
pub mod shrink;
pub mod sim;
pub mod timeline;

pub use disk::{Disk, DiskConfig};
pub use rng::Rng;
pub use sim::{Event, NetConfig, NodeId, Sim, TraceEntry};
