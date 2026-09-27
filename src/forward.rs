//! Over the wire, at least once: a subscription's Events carried to a
//! remote Party as [`crate::wire::WireEvent`]s, through one of Xmip's own transports,
//! each attempt judged by the resilience guards (ADR-0065 clause 3).
//!
//! An Event leaves the forwarder's keeping only when the wire acknowledged
//! it. When the guards give up on one, it and every Event after it stay,
//! in order, for the next pump: a remote subscriber may see an Event twice
//! and never misses one while this process runs. The transport is a
//! [`Wire`]: the HTTP, Kafka and AMQP transports each implement it, carry
//! the [`Carried`] the binding wrote where their protocol puts it, and
//! present the identity the Send side configured for the subscriber's
//! Party (ADR-0019 clause 3). Every carried Event and every give-up is
//! audited in the subscriber's audit.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use resilience::{Failure, Guard, Guarded, execute};
use xcore::{PartyId, Severity};

use crate::Event;
use crate::audit_queue::record;
use crate::binding::{Binding, Carried, Mode};
use crate::hub::Subscription;
use crate::wire::WireEvent;

/// A transport that carries a `WireEvent` to a Party.
pub trait Wire: Send {
    /// Carry `carried` to `party`, presenting the identity configured for
    /// it; `Ok` only once the far end acknowledged it.
    ///
    /// # Errors
    /// The attempt failed: retryable where trying again could change that.
    fn carry(&self, party: PartyId, carried: &Carried) -> Result<(), Failure>;
}

/// What one pump did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Pumped {
    /// Events the wire acknowledged.
    pub carried: usize,
    /// Events still kept for the next pump.
    pub pending: usize,
    /// Why the guards gave up, when they did.
    pub gave_up: Option<String>,
}

/// A subscription forwarded over a wire.
pub struct Forwarder<W: Wire> {
    subscription: Subscription,
    binding: Binding,
    mode: Mode,
    wire: W,
    pending: VecDeque<Arc<Event>>,
}

impl<W: Wire> Forwarder<W> {
    #[must_use]
    pub const fn new(subscription: Subscription, binding: Binding, mode: Mode, wire: W) -> Self {
        Self {
            subscription,
            binding,
            mode,
            wire,
            pending: VecDeque::new(),
        }
    }

    /// Take what arrived within `timeout`, then carry every kept Event,
    /// oldest first, each under `guards`, until one is given up on.
    pub fn pump(&mut self, timeout: Duration, guards: &[&dyn Guard]) -> Pumped {
        self.pending
            .extend(self.subscription.next(timeout, usize::MAX).events);
        let party = self.subscription.subscriber().party;
        let mut carried = Vec::new();
        let mut gave_up = None;

        while let Some(event) = self.pending.front() {
            match self.attempt(party, event, guards) {
                Ok(()) => {
                    carried.push(event.id.to_string());
                    self.pending.pop_front();
                }
                Err(reason) => {
                    gave_up = Some(reason);
                    break;
                }
            }
        }

        self.audit(&carried, gave_up.as_deref());
        Pumped {
            carried: carried.len(),
            pending: self.pending.len(),
            gave_up,
        }
    }

    /// The wire, as the forwarder holds it.
    #[must_use]
    pub const fn wire(&self) -> &W {
        &self.wire
    }

    /// One Event, written by the binding and carried under the guards.
    fn attempt(&self, party: PartyId, event: &Event, guards: &[&dyn Guard]) -> Result<(), String> {
        let carried = self
            .binding
            .write(&WireEvent::from_event(event), self.mode)
            .map_err(|error| error.to_string())?;

        match execute(guards, || self.wire.carry(party, &carried)) {
            Ok(Guarded::Done(())) => Ok(()),
            Ok(Guarded::Refused(reason)) => Err(reason),
            Ok(Guarded::Fallback) => Err("a guard asked for the fallback".to_string()),
            Err(failure) => Err(failure.reason),
        }
    }

    /// What this pump carried and why it stopped, in the subscriber's
    /// audit.
    fn audit(&self, carried: &[String], gave_up: Option<&str>) {
        let subscriber = self.subscription.subscriber();
        let party = subscriber.party.to_string();
        if !carried.is_empty() {
            let properties = BTreeMap::from([
                ("party".to_string(), party.clone()),
                ("count".to_string(), carried.len().to_string()),
                ("events".to_string(), carried.join(" ")),
            ]);
            let said = format!("{} Events carried", carried.len());
            record(
                subscriber,
                "event.forward",
                Severity::Information,
                &said,
                properties,
            );
        }
        if let Some(reason) = gave_up {
            let properties = BTreeMap::from([
                ("party".to_string(), party),
                ("pending".to_string(), self.pending.len().to_string()),
            ]);
            let said = format!("given up for now, kept for the next attempt: {reason}");
            record(
                subscriber,
                "event.forward",
                Severity::Warning,
                &said,
                properties,
            );
        }
    }
}
