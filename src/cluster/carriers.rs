//! The asking side of the links, inside the hub: which links carry this
//! hub's subscriptions to the other nodes, and how far each has carried
//! them.
//!
//! Every change of the subscriptions raises their generation. Every
//! member followed has a link here from the moment it is followed; once
//! made, it carries the generation it last asked for when the node it asks
//! answers *Wanted* (`link.rs`). A subscribe waits until every made link
//! carries its generation — one round trip — so that from the moment it
//! returns every member that is heard sends what it matches. A link being
//! made asks for every subscription as it is made, and a member that
//! cannot be reached is not waited for: it is unheard, and said to be
//! (`unheard.rs`). Following a new member waits for its link to be made,
//! so a subscribe after it waits only the round trip.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

use crate::filter::Filter;
use crate::hub::Hub;
use crate::signal::Signal;

/// The most a subscribe waits for a connected link to carry it: a node
/// answering slower than this is slower than any healthy round trip, and
/// is taken for broken by its link soon after.
pub(crate) const SETTLE: Duration = Duration::from_millis(250);

/// Where one link stands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Carrying {
    /// Being made: connecting, or the handshake.
    Making,
    /// Made, and carrying the subscriptions of this generation.
    Carries(u64),
    /// Its member cannot be reached: nothing waits for it.
    Unreachable,
}

/// The links and their generations.
#[derive(Default)]
pub(crate) struct Carriers {
    state: Mutex<State>,
    ready: Condvar,
}

#[derive(Default)]
struct State {
    generation: u64,
    next_link: u64,
    links: BTreeMap<u64, Carrying>,
}

impl Carriers {
    /// The subscriptions changed: their new generation.
    pub(crate) fn raise(&self) -> u64 {
        let mut state = self.lock();
        state.generation += 1;
        state.generation
    }

    /// A member followed: its link's number, being made.
    pub(crate) fn join(&self) -> u64 {
        let mut state = self.lock();
        state.next_link += 1;
        let link = state.next_link;
        state.links.insert(link, Carrying::Making);
        link
    }

    /// `link` is made, and carries nothing yet.
    pub(crate) fn made(&self, link: u64) {
        self.set(link, Carrying::Carries(0));
    }

    /// `link` carries `generation` now.
    pub(crate) fn carried(&self, link: u64, generation: u64) {
        self.set(link, Carrying::Carries(generation));
    }

    /// `link`'s member cannot be reached: nothing waits for it.
    pub(crate) fn unreachable(&self, link: u64) {
        self.set(link, Carrying::Unreachable);
    }

    /// `link`'s member is no longer followed.
    pub(crate) fn leave(&self, link: u64) {
        self.lock().links.remove(&link);
        self.ready.notify_all();
    }

    /// Wait, up to [`SETTLE`], until every link that is made carries
    /// `generation`; whether they all do. A link being made asks for every
    /// generation as it is made, and one whose member cannot be reached is
    /// not waited for.
    pub(crate) fn settle(&self, generation: u64) -> bool {
        let behind = |state: &mut State| {
            state
                .links
                .values()
                .any(|carrying| matches!(carrying, Carrying::Carries(at) if *at < generation))
        };
        let state = self.lock();
        let (mut state, _) = self
            .ready
            .wait_timeout_while(state, SETTLE, behind)
            .unwrap_or_else(PoisonError::into_inner);
        !behind(&mut state)
    }

    /// Wait, up to `most`, until none of `links` is being made: each made,
    /// or its member unreachable.
    pub(crate) fn made_all(&self, links: &[u64], most: Duration) {
        let state = self.lock();
        let _ = self
            .ready
            .wait_timeout_while(state, most, |state| {
                links
                    .iter()
                    .any(|link| state.links.get(link) == Some(&Carrying::Making))
            })
            .unwrap_or_else(PoisonError::into_inner);
    }

    fn set(&self, link: u64, carrying: Carrying) {
        if let Some(held) = self.lock().links.get_mut(&link) {
            *held = carrying;
        }
        self.ready.notify_all();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Hub {
    /// Every subscription's number and filter, and the generation they
    /// are at least as new as: what a link asks the other nodes for.
    pub(crate) fn wants(&self) -> (u64, Vec<(u64, Filter)>) {
        let generation = self.inner.carriers.lock().generation;
        let wants = self
            .slots()
            .iter()
            .map(|slot| (slot.id, slot.filter.clone()))
            .collect();
        (generation, wants)
    }

    /// Raise `signal` whenever the subscriptions change, for as long as
    /// it is held elsewhere.
    pub(crate) fn watch(&self, signal: &Arc<Signal>) {
        self.inner
            .watchers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::downgrade(signal));
    }
}
