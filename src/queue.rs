//! A bounded queue of Events, the one every subscription and every link to
//! another node is fed through: the publisher offers and never waits, the
//! drain waits for the first Event and wakes on its arrival.
//!
//! A full queue refuses the Event and counts it, paused or not. Paused, it
//! keeps queuing up to its capacity and hands nothing over until it is let
//! go of. Closed, it takes nothing more and wakes whoever waits.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::Event;
use crate::hub::Delivery;

/// One bounded queue.
pub(crate) struct Queue {
    capacity: usize,
    state: Mutex<State>,
    ready: Condvar,
}

/// What the queue holds and has done.
#[derive(Default)]
pub(crate) struct State {
    pub(crate) events: VecDeque<Arc<Event>>,
    /// Refused since the drain before, handed over at the next.
    pub(crate) refused: u64,
    pub(crate) closed: bool,
    /// Held by an operator: queuing goes on, handing over does not.
    pub(crate) paused: bool,
    /// Handed over since the queue was made.
    pub(crate) delivered: u64,
    /// Refused since the queue was made.
    pub(crate) missed: u64,
    /// Told something changed that a drain hands over with no Event: the
    /// members of the cluster not heard.
    pub(crate) told: bool,
}

impl Queue {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(State::default()),
            ready: Condvar::new(),
        }
    }

    pub(crate) const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Queue `event` unless the queue is closed or full; a full queue
    /// counts the refusal.
    pub(crate) fn offer(&self, event: &Arc<Event>) -> bool {
        let mut state = self.state();
        if state.closed {
            return false;
        }
        if state.events.len() >= self.capacity {
            state.refused += 1;
            state.missed += 1;
            return false;
        }
        state.events.push_back(Arc::clone(event));
        drop(state);
        self.ready.notify_one();
        true
    }

    /// Count `count` Events refused elsewhere for this queue — on the way
    /// to it from another node — as a full queue counts its own.
    pub(crate) fn refused_elsewhere(&self, count: u64) {
        let mut state = self.state();
        state.refused += count;
        state.missed += count;
        drop(state);
        self.ready.notify_one();
    }

    /// Wake the drain to hand over what changed, Event or not.
    pub(crate) fn tell(&self) {
        self.state().told = true;
        self.ready.notify_all();
    }

    /// Up to `max` Events, waiting up to `timeout` for the first. Paused,
    /// nothing is handed over: the wait lasts until it is resumed, closed
    /// or out of time, and what queued stays queued.
    pub(crate) fn take(&self, timeout: Duration, max: usize) -> Delivery {
        let state = self.state();
        let (mut state, _) = self
            .ready
            .wait_timeout_while(state, timeout, |state| {
                !state.closed
                    && (state.paused
                        || (state.events.is_empty() && state.refused == 0 && !state.told))
            })
            .unwrap_or_else(PoisonError::into_inner);
        if state.paused {
            return Delivery::default();
        }
        let count = state.events.len().min(max.max(1));
        state.delivered += count as u64;

        Delivery {
            events: state.events.drain(..count).collect(),
            refused: std::mem::take(&mut state.refused),
            unheard: Vec::new(),
            unheard_changed: std::mem::take(&mut state.told),
        }
    }

    /// Hold delivery, or let go of it; whether that changed anything.
    pub(crate) fn hold(&self, paused: bool) -> bool {
        let mut state = self.state();
        let changed = state.paused != paused;
        state.paused = paused;
        drop(state);
        self.ready.notify_all();
        changed
    }

    /// Stop taking Events and wake anyone waiting.
    pub(crate) fn close(&self) {
        self.state().closed = true;
        self.ready.notify_all();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.state().closed
    }

    /// What the queue holds and has done, read under its lock.
    pub(crate) fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
