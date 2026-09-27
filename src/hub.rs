//! The in-process hub: an Event published once reaches every subscription
//! whose filter matches, through a bounded queue each, and the publisher
//! never waits for a subscriber (ADR-0065 clause 2).
//!
//! **Publishing never blocks on a subscriber.** The subscriptions are a
//! snapshot the publisher takes by cloning one pointer; each match is one
//! short lock to push a shared pointer onto a queue, never held across a
//! subscriber's work. A full queue refuses the Event for that subscriber
//! and counts it, and the count is handed over — and audited — at the next
//! drain. A subscriber that stops draining costs the publisher nothing but
//! the count.
//!
//! Authorization is decided once, when the subscription is made, by
//! [`Subscriber::authorized`]; a subscription can only ever match what it
//! was allowed. Every subscription, every refusal, every delivery and every
//! Event a full queue refused is audited through the subscriber's own
//! `ProgramAudit`, off both the publisher's and the subscriber's path.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError, RwLock, Weak};
use std::time::Duration;

use authorize::Authorizer;
use xcore::Severity;

use crate::audit_trail::{Deliveries, described, record};
use crate::filter::Filter;
use crate::subscriber::{SameProcess, Subscriber};
use crate::{Event, EventError};

/// How many Events a queue holds when the subscriber names no bound.
pub const DEFAULT_CAPACITY: usize = 1024;

/// Every subscription, as the publisher reads it: one pointer to clone.
type Slots = Arc<Vec<Arc<Slot>>>;

/// Where Events are published and subscriptions made.
pub struct Hub {
    inner: Arc<Inner>,
}

struct Inner {
    policies: Vec<Arc<dyn Authorizer>>,
    slots: RwLock<Slots>,
    next_slot: AtomicU64,
}

/// One subscription's filter and queue, shared by the hub and the handle.
pub(crate) struct Slot {
    id: u64,
    filter: Filter,
    capacity: usize,
    queue: Mutex<Queue>,
    ready: Condvar,
}

#[derive(Default)]
struct Queue {
    events: VecDeque<Arc<Event>>,
    refused: u64,
    closed: bool,
}

/// What one drain handed over: the Events, oldest first, and how many a
/// full queue refused since the drain before.
#[derive(Clone, Debug, Default)]
pub struct Delivery {
    pub events: Vec<Arc<Event>>,
    pub refused: u64,
}

impl Hub {
    /// A hub whose subscribers are authorized by `policies`.
    #[must_use]
    pub fn new(policies: Vec<Arc<dyn Authorizer>>) -> Self {
        Self {
            inner: Arc::new(Inner {
                policies,
                slots: RwLock::new(Arc::new(Vec::new())),
                next_slot: AtomicU64::new(1),
            }),
        }
    }

    /// This process's hub: the one the runtime publishes to and the C
    /// boundary subscribes through, admitting subscribers in this process.
    pub fn process() -> &'static Self {
        static PROCESS: OnceLock<Hub> = OnceLock::new();
        PROCESS.get_or_init(|| Self::new(vec![Arc::new(SameProcess)]))
    }

    /// Subscribe `subscriber` to what `filter` asks for, through a queue of
    /// `capacity` Events (0 is [`DEFAULT_CAPACITY`]).
    ///
    /// # Errors
    /// The authorization gate refused it; the refusal is audited and its
    /// sentence is the error.
    pub fn subscribe(
        &self,
        subscriber: Subscriber,
        filter: Filter,
        capacity: usize,
    ) -> Result<Subscription, EventError> {
        let policies: Vec<&dyn Authorizer> =
            self.inner.policies.iter().map(|policy| &**policy).collect();
        let decision = subscriber.authorized(&filter, &policies);
        let about = described(&subscriber, &filter);

        if !decision.allowed() {
            let said = decision.to_string();
            record(
                &subscriber,
                "event.subscribe",
                Severity::Warning,
                &said,
                about,
            );
            return Err(EventError::new(said));
        }

        let slot = Arc::new(Slot {
            id: self.inner.next_slot.fetch_add(1, Ordering::Relaxed),
            filter,
            capacity: if capacity == 0 {
                DEFAULT_CAPACITY
            } else {
                capacity
            },
            queue: Mutex::new(Queue::default()),
            ready: Condvar::new(),
        });
        self.inner.change(|slots| slots.push(Arc::clone(&slot)));
        record(
            &subscriber,
            "event.subscribe",
            Severity::Information,
            "allowed",
            about,
        );

        Ok(Subscription {
            slot,
            hub: Arc::downgrade(&self.inner),
            subscriber,
            deliveries: Arc::default(),
        })
    }

    /// Hand `event` to every subscription whose filter matches; how many
    /// queues took it. Never waits for a subscriber.
    pub fn publish(&self, event: Event) -> usize {
        let slots = Arc::clone(
            &self
                .inner
                .slots
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        );
        let event = Arc::new(event);

        slots
            .iter()
            .filter(|slot| slot.filter.matches(&event))
            .filter(|slot| slot.offer(&event))
            .count()
    }

    /// How many subscriptions are open.
    #[must_use]
    pub fn subscriptions(&self) -> usize {
        self.inner
            .slots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl Inner {
    /// Replace the snapshot with a changed copy; the publisher keeps
    /// reading whichever snapshot it took.
    fn change(&self, edit: impl FnOnce(&mut Vec<Arc<Slot>>)) {
        let mut held = self.slots.write().unwrap_or_else(PoisonError::into_inner);
        let mut next: Vec<Arc<Slot>> = held.as_ref().clone();
        edit(&mut next);
        *held = Arc::new(next);
    }
}

impl Slot {
    /// Queue `event` unless the queue is closed or full; a full queue
    /// counts the refusal.
    fn offer(&self, event: &Arc<Event>) -> bool {
        let mut queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
        if queue.closed {
            return false;
        }
        if queue.events.len() >= self.capacity {
            queue.refused += 1;
            return false;
        }
        queue.events.push_back(Arc::clone(event));
        drop(queue);
        self.ready.notify_one();
        true
    }

    /// Up to `max` Events, waiting up to `timeout` for the first.
    fn take(&self, timeout: Duration, max: usize) -> Delivery {
        let queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
        let (mut queue, _) = self
            .ready
            .wait_timeout_while(queue, timeout, |queue| {
                queue.events.is_empty() && queue.refused == 0 && !queue.closed
            })
            .unwrap_or_else(PoisonError::into_inner);
        let count = queue.events.len().min(max.max(1));

        Delivery {
            events: queue.events.drain(..count).collect(),
            refused: std::mem::take(&mut queue.refused),
        }
    }

    /// Stop taking Events and wake anyone waiting.
    pub(crate) fn close(&self) {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed = true;
        self.ready.notify_all();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed
    }
}

/// An open subscription. Dropping it unsubscribes, and says so to audit.
pub struct Subscription {
    slot: Arc<Slot>,
    hub: Weak<Inner>,
    subscriber: Subscriber,
    deliveries: Arc<Deliveries>,
}

impl Subscription {
    /// Up to `max` Events (at least one), waiting up to `timeout` for the
    /// first and waking the moment one arrives. What a drain hands over is
    /// audited — the Events delivered, by identity, and how many a full
    /// queue refused — on the keeping thread, never on this one.
    #[must_use]
    pub fn next(&self, timeout: Duration, max: usize) -> Delivery {
        let delivery = self.slot.take(timeout, max);
        self.deliveries.note(&self.subscriber, &delivery);
        delivery
    }

    /// Who subscribed.
    #[must_use]
    pub const fn subscriber(&self) -> &Subscriber {
        &self.subscriber
    }

    pub(crate) fn slot(&self) -> Arc<Slot> {
        Arc::clone(&self.slot)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.slot.close();
        if let Some(inner) = self.hub.upgrade() {
            let id = self.slot.id;
            inner.change(|slots| slots.retain(|slot| slot.id != id));
        }
        let about = described(&self.subscriber, &self.slot.filter);
        record(
            &self.subscriber,
            "event.unsubscribe",
            Severity::Information,
            "closed",
            about,
        );
        // Handed to the keeper, never waited for here: the dropping thread
        // is the program's, and a disk is not its business. The program's
        // next direct record — its stop — is kept after this one.
    }
}
