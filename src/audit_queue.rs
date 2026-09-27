//! Audit off the delivery path: every subscription, delivery and refusal is
//! recorded (ADR-0062), and none of them makes an Event wait for a disk.
//!
//! An Event must reach a subscriber within about a millisecond of being
//! published (the owner, 2026-09-26: near, very near real time), and one
//! audit record is a file append that can take milliseconds. So a record is
//! handed to one thread in the process that keeps them in order, and the
//! subscriber goes on. A subscription's deliveries are noted, not recorded
//! one by one ([`Deliveries`]): at most one record of them waits at a time,
//! and it takes every delivery and refusal noted before it is kept, so a
//! flood of Events costs a few records rather than one each. [`settle`]
//! waits until everything handed over before it is kept; unsubscribing
//! settles, so a program that unsubscribed and exits has lost no record.

use std::collections::BTreeMap;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::thread;

use audit::program_audit::ProgramAudit;
use xcore::{EventId, ExecutionPhase, PartyId, Severity};

use crate::filter::Filter;
use crate::hub::Delivery;
use crate::subscriber::Subscriber;

/// One record to keep.
pub(crate) type Job = Box<dyn FnOnce() + Send>;

/// The keeping thread's inbox, started the first time a record is handed
/// over; `None` where the operating system would not start it.
fn inbox() -> Option<&'static Sender<Job>> {
    static INBOX: OnceLock<Option<Sender<Job>>> = OnceLock::new();
    INBOX
        .get_or_init(|| {
            let (sender, receiver) = mpsc::channel::<Job>();
            thread::Builder::new()
                .name("xmip-event-audit".to_string())
                .spawn(move || receiver.into_iter().for_each(|job| job()))
                .ok()
                .map(|_| sender)
        })
        .as_ref()
}

/// Keep `job` on the keeping thread, or here when there is none.
pub(crate) fn later(job: Job) {
    match inbox() {
        Some(inbox) => {
            if let Err(returned) = inbox.send(job) {
                (returned.0)();
            }
        }
        None => job(),
    }
}

/// Wait until every record handed over before this call is kept.
pub fn settle() {
    let (done, finished) = mpsc::channel();
    later(Box::new(move || {
        let _ = done.send(());
    }));
    // A keeping thread that died has kept what it could; nothing is left
    // to wait for.
    let _ = finished.recv();
}

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
    /// keeping thread unless one is already waiting there.
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
        later(Box::new(move || deliveries.keep(&audit, party)));
    }

    /// Record everything noted so far, on the keeping thread.
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
        ("identity".to_string(), subscriber.identity.to_string()),
        ("scope".to_string(), filter.reach().to_string()),
        ("types".to_string(), filter.types.join(" ")),
        ("outcomes".to_string(), outcomes.join(" ")),
    ])
}

/// One audit record in the subscriber's program's audit, kept on the
/// keeping thread.
pub(crate) fn record(
    subscriber: &Subscriber,
    action: &str,
    severity: Severity,
    message: &str,
    properties: BTreeMap<String, String>,
) {
    let audit = subscriber.audit.clone();
    let action = action.to_string();
    let message = message.to_string();
    later(Box::new(move || {
        kept(&audit, &action, severity, &message, properties);
    }));
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
