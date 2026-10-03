//! The answering side of a link, inside the hub: what another node's
//! subscriptions want of this node's own Events, and the bounded queue they
//! wait in to cross.
//!
//! A tap is fed by [`crate::hub::Hub::publish`] only — the Events raised on
//! this node — never by what this node heard from another, so an Event
//! crosses one hop and never comes back. It matches by the filters the
//! asking node pushed down, so what no subscriber there wants never leaves
//! here. A full queue refuses the Event and counts it for each subscription
//! it matched, and the count crosses instead (`link.rs`, *Missed*).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use crate::Event;
use crate::filter::Filter;
use crate::hub::Hub;
use crate::queue::Queue;

/// What one asking node wants, by its subscriptions' numbers there.
pub(crate) type Wants = Arc<Vec<(u64, Filter)>>;

/// How many Events a link's queue holds while the link writes.
pub(crate) const CAPACITY: usize = 4096;

/// One link's filters and queue.
pub(crate) struct Tap {
    wants: RwLock<Wants>,
    pub(crate) queue: Queue,
    missed: Mutex<BTreeMap<u64, u64>>,
}

impl Tap {
    /// Replace what the asking node wants.
    pub(crate) fn want(&self, wants: Vec<(u64, Filter)>) {
        *self.wants.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(wants);
    }

    /// Queue `event` where any filter wants it; a full queue counts it for
    /// each subscription it matched.
    pub(crate) fn offer(&self, event: &Arc<Event>) {
        let wants = Arc::clone(&self.wants.read().unwrap_or_else(PoisonError::into_inner));
        if !wants.iter().any(|(_, filter)| filter.matches(event)) {
            return;
        }
        if self.queue.offer(event) || self.queue.is_closed() {
            return;
        }
        let mut missed = self.missed.lock().unwrap_or_else(PoisonError::into_inner);
        for (id, filter) in wants.iter() {
            if filter.matches(event) {
                *missed.entry(*id).or_default() += 1;
            }
        }
    }

    /// What was refused since the last time asked, by subscription.
    pub(crate) fn take_missed(&self) -> Vec<(u64, u64)> {
        std::mem::take(&mut *self.missed.lock().unwrap_or_else(PoisonError::into_inner))
            .into_iter()
            .collect()
    }
}

impl Hub {
    /// A new link's tap, fed from now on, wanting nothing yet.
    pub(crate) fn tap(&self) -> Arc<Tap> {
        let tap = Arc::new(Tap {
            wants: RwLock::new(Arc::new(Vec::new())),
            queue: Queue::new(CAPACITY),
            missed: Mutex::new(BTreeMap::new()),
        });
        self.change_taps(|taps| taps.push(Arc::clone(&tap)));
        tap
    }

    /// A link's tap closed and gone.
    pub(crate) fn untap(&self, tap: &Arc<Tap>) {
        tap.queue.close();
        self.change_taps(|taps| taps.retain(|held| !Arc::ptr_eq(held, tap)));
    }

    fn change_taps(&self, edit: impl FnOnce(&mut Vec<Arc<Tap>>)) {
        let mut held = self
            .inner
            .taps
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let mut next = held.as_ref().clone();
        edit(&mut next);
        *held = Arc::new(next);
    }
}
