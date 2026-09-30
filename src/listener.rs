//! A subscription drained by a thread of its own, calling back for each
//! Event: the callback variant of ADR-0065 clause 2.
//!
//! The callback runs on the listener's thread, never the publisher's, so a
//! slow callback fills its own queue and nothing else. Dropping the
//! [`Listener`] closes the queue, which wakes the thread; it finishes the
//! callback it is in, unsubscribes, and is joined — unless the drop happens
//! inside the callback itself, where the thread cannot wait for itself and
//! is left to end on its own.

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::Event;
use crate::hub::{EventSubscription, Slot};

/// How long the thread sleeps between looks when nothing arrives; closing
/// wakes it at once, so this bounds nothing an operator waits for.
const IDLE: Duration = Duration::from_secs(60);

/// How many Events one drain hands the callback before it looks again.
const BATCH: usize = 256;

/// A subscription and the thread calling back for it.
pub struct Listener {
    slot: Arc<Slot>,
    thread: Option<JoinHandle<()>>,
}

impl EventSubscription {
    /// Call `each` for every Event, on a thread of its own, until the
    /// returned [`Listener`] is dropped.
    ///
    /// # Errors
    /// The operating system would not start the thread.
    pub fn listen(
        self,
        mut each: impl FnMut(&Event) + Send + 'static,
    ) -> std::io::Result<Listener> {
        let slot = self.slot();
        let thread = thread::Builder::new()
            .name("xmip-event-listener".to_string())
            .spawn(move || {
                let subscription = self;
                let slot = subscription.slot();
                while !slot.is_closed() {
                    for event in subscription.next(IDLE, BATCH).events {
                        each(&event);
                    }
                }
            })?;

        Ok(Listener {
            slot,
            thread: Some(thread),
        })
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.slot.close();
        if let Some(thread) = self.thread.take()
            && thread.thread().id() != thread::current().id()
        {
            // A callback that panicked has already said so on its thread.
            let _ = thread.join();
        }
    }
}
