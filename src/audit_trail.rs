//! What eventing audits, off the delivery path: every subscription,
//! delivery and refusal is recorded (ADR-0062), and none of them makes an
//! Event wait for a disk.
//!
//! An Event must reach a subscriber within about a millisecond of being
//! published (the owner, 2026-09-26: near, very near real time), so every
//! record here is handed to the audit capability's keeper
//! ([`audit::keeper`]) and the subscriber goes on. A subscription's
//! deliveries are noted, not recorded one by one ([`Deliveries`]): at most
//! one record of them waits at a time, and it takes every delivery and
//! refusal noted before it is kept, so a flood of Events costs a few
//! records rather than one each.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use audit::keeper::later;
use audit::program_audit::ProgramAudit;
use xcore::{EventId, ExecutionPhase, PartyId, Severity};

use crate::filter::Filter;
use crate::hub::Delivery;
use crate::subscriber::Subscriber;

/// A subscription's deliveries and refusals not yet recorded.
#[derive(Default)]
pub(crate) struct Deliveries {
    pending: Mutex<Pending>,
}

#[derive(Default)]
struct Pending {
    delivered: Vec<EventId>,
    refused: u64,
    queued: bool,
}

impl Deliveries {
    /// Note what one drain handed over, and hand a record of it to the
    /// keeper unless one is already waiting there.
    pub(crate) fn note(self: &Arc<Self>, subscriber: &Subscriber, delivery: &Delivery) {
        if delivery.events.is_empty() && delivery.refused == 0 {
            return;
        }
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        pending
            .delivered
            .extend(delivery.events.iter().map(|event| event.id));
        pending.refused += delivery.refused;
        if std::mem::replace(&mut pending.queued, true) {
            return;
        }
        drop(pending);

        let deliveries = Arc::clone(self);
        let audit = subscriber.audit.clone();
        let party = subscriber.party;
        later(move || deliveries.keep(&audit, party));
    }

    /// Record everything noted so far, on the keeper's thread.
    fn keep(&self, audit: &ProgramAudit, party: PartyId) {
        let (delivered, refused) = {
            let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
            pending.queued = false;
            (
                std::mem::take(&mut pending.delivered),
                std::mem::take(&mut pending.refused),
            )
        };
        if !delivered.is_empty() {
            let ids: Vec<String> = delivered.iter().map(ToString::to_string).collect();
            let properties = BTreeMap::from([
                ("party".to_string(), party.to_string()),
                ("count".to_string(), ids.len().to_string()),
                ("events".to_string(), ids.join(" ")),
            ]);
            let said = format!("{} Events delivered", ids.len());
            kept(
                audit,
                "event.deliver",
                Severity::Information,
                &said,
                properties,
            );
        }
        if refused > 0 {
            let said = format!("{refused} matching Events refused: the queue held its capacity");
            let properties = BTreeMap::from([
                ("party".to_string(), party.to_string()),
                ("count".to_string(), refused.to_string()),
            ]);
            kept(audit, "event.refuse", Severity::Warning, &said, properties);
        }
    }
}

/// The subscriber and its filter, as audit properties.
pub(crate) fn described(subscriber: &Subscriber, filter: &Filter) -> BTreeMap<String, String> {
    let outcomes: Vec<&str> = filter.outcomes.iter().map(|o| o.word()).collect();
    BTreeMap::from([
        ("party".to_string(), subscriber.party.to_string()),
        ("name".to_string(), subscriber.name.clone()),
        ("identity".to_string(), subscriber.identity.to_string()),
        ("scope".to_string(), filter.reach().to_string()),
        ("types".to_string(), filter.types.join(" ")),
        ("outcomes".to_string(), outcomes.join(" ")),
    ])
}

/// One audit record in the subscriber's program's audit, kept on the
/// keeper's thread.
pub(crate) fn record(
    subscriber: &Subscriber,
    action: &str,
    severity: Severity,
    message: &str,
    properties: BTreeMap<String, String>,
) {
    noted(&subscriber.audit, action, severity, message, properties);
}

/// One audit record in `audit`, kept on the keeper's thread: what a link
/// between nodes records in the node's own audit.
pub(crate) fn noted(
    audit: &ProgramAudit,
    action: &str,
    severity: Severity,
    message: &str,
    properties: BTreeMap<String, String>,
) {
    let audit = audit.clone();
    let action = action.to_string();
    let message = message.to_string();
    later(move || kept(&audit, &action, severity, &message, properties));
}

/// Keep one record now.
fn kept(
    audit: &ProgramAudit,
    action: &str,
    severity: Severity,
    message: &str,
    properties: BTreeMap<String, String>,
) {
    // The capability keeps it, or the operating system's log does; a record
    // neither could keep is all this can lose, and the delivery stands.
    let _ = audit.record(
        action,
        ExecutionPhase::Finished,
        severity,
        Some(message),
        properties,
    );
}
