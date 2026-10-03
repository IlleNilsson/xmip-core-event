//! A count raised by one thread and waited on by another: how a link to
//! another node learns, at once, that the subscriptions it carries changed
//! or that it should end, without looking again and again.

use std::sync::{Condvar, Mutex, PoisonError};
use std::time::Duration;

/// A count that only rises, and the threads waiting for it to.
#[derive(Default)]
pub(crate) struct Signal {
    raised: Mutex<u64>,
    ready: Condvar,
}

impl Signal {
    /// Raise it, waking every thread waiting.
    pub(crate) fn raise(&self) {
        *self.raised.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        self.ready.notify_all();
    }

    /// The count now.
    pub(crate) fn now(&self) -> u64 {
        *self.raised.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Wait up to `timeout` for the count to pass `seen`; the count then.
    pub(crate) fn wait(&self, seen: u64, timeout: Duration) -> u64 {
        let raised = self.raised.lock().unwrap_or_else(PoisonError::into_inner);
        let (raised, _) = self
            .ready
            .wait_timeout_while(raised, timeout, |raised| *raised == seen)
            .unwrap_or_else(PoisonError::into_inner);
        *raised
    }
}
