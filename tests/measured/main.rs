//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): Events held to the millisecond rule (`CONTRIBUTING.md`, *Rust is
//! here for one reason*). A plain `cargo test` leaves it out; the landing
//! runs it after every other test of the module, alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - `latency.rs`: an Event reaches a waiting subscriber and a listener
//!   within about a millisecond of being published;
//! - `cluster_latency.rs`: crossing to another node adds well under a
//!   millisecond, over the test cluster the cluster tests stand up
//!   (`tests/cluster/test_cluster.rs`, shared, not copied);
//! - `hub.rs`: a publish costs microseconds;
//! - `act.rs`: a resume wakes a waiting drain at once.

#[path = "../cluster/test_cluster.rs"]
mod test_cluster;

mod act;
mod cluster_latency;
mod hub;
mod latency;
