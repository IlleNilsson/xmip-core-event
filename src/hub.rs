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
//!
//! **An operator holds, lets go of and ends a subscription** (ADR-0065,
//! amendment 2026-09-29; `act.rs`). Paused, a subscription stays and keeps
//! queuing up to its capacity, and nothing is handed over until it is
//! resumed; what a full queue refused meanwhile is counted as missed, as it
//! always is. Removed, it is closed and gone from the hub, and its holder's
//! next drain finds it closed. What the hub holds is listed as
//! `observe::EventSubscription` ([`Hub::standing`]), the record a node
//! publishes.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock, Weak};
use std::time::Duration;

use authorize::Authorizer;
use observe::{EventSubscription as Published, PauseState, Unheard};
use xcore::Severity;

use crate::audit_trail::{Deliveries, described, record};
use crate::cluster::carriers::Carriers;
use crate::cluster::tap::Tap;
use crate::filter::Filter;
use crate::gate::Gate;
use crate::queue::Queue;
use crate::signal::Signal;
use crate::subscriber::Subscriber;
use crate::{Event, EventError};

/// How many Events a queue holds when the subscriber names no bound.
pub const DEFAULT_CAPACITY: usize = 1024;

/// Every subscription, as the publisher reads it: one pointer to clone.
type Slots = Arc<Vec<Arc<Slot>>>;

/// Every link another node listens through, read the same way.
pub(crate) type Taps = Arc<Vec<Arc<Tap>>>;

/// Where Events are published and subscriptions made. A clone is the same
/// hub.
#[derive(Clone)]
pub struct Hub {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) gate: Gate,
    slots: RwLock<Slots>,
    next_slot: AtomicU64,
    /// The links the other nodes of the cluster hear this node's own
    /// Events through (`cluster.rs`).
    pub(crate) taps: RwLock<Taps>,
    /// Raised whenever the subscriptions change.
    pub(crate) watchers: Mutex<Vec<Weak<Signal>>>,
    /// The members of the cluster this hub does not hear now, by node.
    pub(crate) unheard: Mutex<BTreeMap<String, Unheard>>,
    /// The links carrying the subscriptions to the other nodes.
    pub(crate) carriers: Carriers,
}

/// One subscription's filter and queue, shared by the hub and the handle.
pub(crate) struct Slot {
    pub(crate) id: u64,
    pub(crate) subscriber: Subscriber,
    pub(crate) filter: Filter,
    since_unix_nanos: i64,
    pub(crate) queue: Queue,
}

/// What one drain handed over: the Events, oldest first; how many a full
/// queue refused since the drain before; and the members of the cluster
/// not heard now, whose Events cannot be among them — so nothing missing
/// is silent (ADR-0065, amendment 2026-10-02).
#[derive(Clone, Debug, Default)]
pub struct Delivery {
    pub events: Vec<Arc<Event>>,
    pub refused: u64,
    /// Every member not heard now: by this node, the member, since when
    /// and why.
    pub unheard: Vec<Unheard>,
    /// Whether the members not heard changed since the drain before: a
    /// drain wakes for that alone, with no Event.
    pub unheard_changed: bool,
}

impl Hub {
    /// A hub whose subscribers are authorized by `policies`.
    #[must_use]
    pub fn new(policies: Vec<Arc<dyn Authorizer>>) -> Self {
        Self {
            inner: Arc::new(Inner {
                gate: Gate::new(policies),
                slots: RwLock::new(Arc::new(Vec::new())),
                next_slot: AtomicU64::new(1),
                taps: RwLock::new(Arc::new(Vec::new())),
                watchers: Mutex::new(Vec::new()),
                unheard: Mutex::new(BTreeMap::new()),
                carriers: Carriers::default(),
            }),
        }
    }

    /// This process's hub: the one the runtime publishes to and the C
    /// boundary subscribes through. It admits nobody until it is handed the
    /// policies that allow (`authorize_by`): the node's as it starts, or the
    /// hosting program's.
    pub fn process() -> &'static Self {
        static PROCESS: OnceLock<Hub> = OnceLock::new();
        PROCESS.get_or_init(|| Self::new(Vec::new()))
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
    ) -> Result<EventSubscription, EventError> {
        let gate = self.inner.gate.policies();
        let policies: Vec<&dyn Authorizer> = gate.iter().map(|policy| &**policy).collect();
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
            subscriber: subscriber.clone(),
            filter,
            since_unix_nanos: observe::now_unix_nanos(),
            queue: Queue::new(if capacity == 0 {
                DEFAULT_CAPACITY
            } else {
                capacity
            }),
        });
        let generation = self.inner.change(|slots| slots.push(Arc::clone(&slot)));
        // Every node that is heard carries it before it is handed over.
        self.inner.carriers.settle(generation);
        record(
            &subscriber,
            "event.subscribe",
            Severity::Information,
            "allowed",
            about,
        );

        Ok(EventSubscription {
            slot,
            hub: Arc::downgrade(&self.inner),
            subscriber,
            deliveries: Arc::default(),
        })
    }

    /// Hand `event`, raised on this node, to every subscription whose
    /// filter matches, and to every other node of the cluster listening
    /// for it; how many of this hub's subscriptions took it. Never waits
    /// for a subscriber or a node.
    pub fn publish(&self, event: Event) -> usize {
        let event = Arc::new(event);
        let taken = self.deliver(&event);
        let taps = Arc::clone(
            &self
                .inner
                .taps
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for tap in taps.iter() {
            tap.offer(&event);
        }
        taken
    }

    /// Hand `event` to every subscription of this hub whose filter matches,
    /// and to nothing else; how many took it.
    pub(crate) fn deliver(&self, event: &Arc<Event>) -> usize {
        self.slots()
            .iter()
            .filter(|slot| slot.filter.matches(event))
            .filter(|slot| slot.queue.offer(event))
            .count()
    }

    /// How many subscriptions are open.
    #[must_use]
    pub fn subscriptions(&self) -> usize {
        self.slots().len()
    }

    /// Every open subscription as the node at `node` publishes it, oldest
    /// first.
    #[must_use]
    pub fn standing(&self, node: &str) -> Vec<Published> {
        self.slots()
            .iter()
            .map(|slot| slot.standing(node))
            .collect()
    }

    /// The open subscription numbered `id`, if there is one.
    pub(crate) fn slot(&self, id: u64) -> Option<Arc<Slot>> {
        self.slots().iter().find(|slot| slot.id == id).cloned()
    }

    /// Take the subscription numbered `id` out of the hub.
    pub(crate) fn forget(&self, id: u64) {
        self.inner
            .change(|slots| slots.retain(|slot| slot.id != id));
    }

    pub(crate) fn slots(&self) -> Slots {
        Arc::clone(
            &self
                .inner
                .slots
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }
}

impl Inner {
    /// Replace the snapshot with a changed copy; the publisher keeps
    /// reading whichever snapshot it took. Whoever watches the
    /// subscriptions is told. The subscriptions' new generation.
    fn change(&self, edit: impl FnOnce(&mut Vec<Arc<Slot>>)) -> u64 {
        {
            let mut held = self.slots.write().unwrap_or_else(PoisonError::into_inner);
            let mut next: Vec<Arc<Slot>> = held.as_ref().clone();
            edit(&mut next);
            *held = Arc::new(next);
        }
        let generation = self.carriers.raise();
        let mut watchers = self.watchers.lock().unwrap_or_else(PoisonError::into_inner);
        watchers.retain(|watcher| {
            watcher.upgrade().is_some_and(|signal| {
                signal.raise();
                true
            })
        });
        generation
    }
}

impl Slot {
    /// This subscription as the node at `node` publishes it.
    fn standing(&self, node: &str) -> Published {
        let queue = self.queue.state();
        Published {
            node: node.to_string(),
            id: self.id,
            subscriber: self.subscriber.name.clone(),
            party: self.subscriber.party.to_string(),
            action: self.filter.said(),
            scope: self.filter.reach().to_string(),
            state: if queue.paused {
                PauseState::Paused
            } else {
                PauseState::Active
            },
            queued: queue.events.len() as u64,
            capacity: self.queue.capacity() as u64,
            delivered: queue.delivered,
            missed: queue.missed,
            since_unix_nanos: self.since_unix_nanos,
        }
    }
}

/// An open subscription. Dropping it unsubscribes, and says so to audit.
/// An operator's remove closes it from outside: its next drain hands over
/// nothing and a listener's thread ends.
pub struct EventSubscription {
    slot: Arc<Slot>,
    pub(crate) hub: Weak<Inner>,
    subscriber: Subscriber,
    deliveries: Arc<Deliveries>,
}

impl EventSubscription {
    /// Up to `max` Events (at least one), waiting up to `timeout` for the
    /// first and waking the moment one arrives. What a drain hands over is
    /// audited — the Events delivered, by identity, and how many a full
    /// queue refused — on the keeping thread, never on this one.
    #[must_use]
    pub fn next(&self, timeout: Duration, max: usize) -> Delivery {
        let mut delivery = self.slot.queue.take(timeout, max);
        if let Some(inner) = self.hub.upgrade() {
            delivery.unheard = Hub { inner }.unheard();
        }
        self.deliveries.note(&self.subscriber, &delivery);
        delivery
    }

    /// Who subscribed.
    #[must_use]
    pub const fn subscriber(&self) -> &Subscriber {
        &self.subscriber
    }

    /// Its number in the hub: what an operator names it by.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.slot.id
    }

    /// Whether it was closed: removed by an operator.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.slot.queue.is_closed()
    }

    pub(crate) fn slot(&self) -> Arc<Slot> {
        Arc::clone(&self.slot)
    }
}

impl Drop for EventSubscription {
    fn drop(&mut self) {
        self.slot.queue.close();
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
