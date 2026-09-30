#![forbid(unsafe_code)]

//! The Xmip Event, the one subscription rule, and its delivery — in this
//! process and over the wire (runtime-model section 17, ADR-0065).
//!
//! **Routing** asks where a Message continues; **Eventing** asks who may
//! know that an action completed, and what they may receive. Every
//! completed Receive, Process and Send action produces an [`Event`] for
//! every [`outcome::Outcome`]. A program subscribes with a
//! [`filter::Filter`] — by type, outcome, scope and Party — and
//! [`filter::Filter::matches`] is the one rule that decides what it gets.
//! A subscriber is a Party ([`subscriber::Subscriber`]) whom the
//! authorization gate admits or refuses; the in-process [`hub::Hub`] fans
//! an Event out through a bounded queue per subscription and never waits
//! for one; a [`listener::Listener`] calls back instead of being drained.
//!
//! Over the wire an Event is a [`wire::WireEvent`] (its JSON format in
//! [`json_format`]),
//! carried in either mode of the HTTP, Kafka or AMQP binding
//! ([`binding::Binding`]) by Xmip's own transport of that name, at least
//! once ([`forward::Forwarder`]). Its data is bytes unless its
//! `datacontenttype` declares JSON (ADR-0038). Every
//! subscription, delivery and refusal is audited (ADR-0062).
//!
//! The C boundary is `xmip_operate.h` section 11, forwarded by the
//! runtime's library to [`hub::Hub::process`]; every language binds that.
//!
//! An operator lists what a hub holds ([`hub::Hub::standing`]) and pauses,
//! resumes or removes an Event subscription ([`hub::Hub::act`], by
//! `observe::Act`); a surface that reads a node's publication only leaves
//! the act as an `observe::Order` for the node to take (ADR-0065, amendment
//! 2026-09-29).

pub mod act;
mod audit_trail;
pub mod binding;
pub mod filter;
pub mod forward;
pub mod hub;
pub mod json_format;
pub mod listener;
pub mod outcome;
pub mod subscriber;
pub mod wire;

use std::collections::BTreeMap;

use node::Stage;
use xcore::{
    Clock, EventId, IdGenerator, JourneyId, MessageId, PartyId, StreamId, SystemClock,
    UuidV7Generator,
};

use crate::outcome::Outcome;

xcore::declare_error!(EventError);

/// The prefix every type Xmip raises carries: reverse DNS, as the wire standard
/// recommends and as the Event Grid transport's `se.xmip.stream` already is.
pub const TYPE_PREFIX: &str = "se.xmip.";

/// One Event: what Xmip tells a Party happened.
///
/// It carries its identity and type, when, the action and how it ended,
/// where it happened as an Xmip URI, the Journey, Message and Stream it
/// concerns, the Endpoint, Module and Artifact, the Party it is about, and
/// diagnostics safe to hand outside. **References, never payload copies**:
/// a large Message or a Transfer is named by its identifiers and nothing of
/// its content travels.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    pub id: EventId,
    /// What kind of Event: `se.xmip.receive.success`, or a type a module
    /// raises under its own prefix.
    pub kind: String,
    /// When the action completed, in unix nanoseconds.
    pub time_unix_nanos: i128,
    /// The action that completed: a stage of the message path.
    pub action: Stage,
    pub outcome: Outcome,
    /// Where it happened: an Xmip URI in the one scope tree (ADR-0027).
    pub scope: String,
    pub journey: Option<JourneyId>,
    pub message: Option<MessageId>,
    pub stream: Option<StreamId>,
    /// The Endpoint it concerned, by name.
    pub endpoint: Option<String>,
    /// The Module that acted, by name.
    pub module: Option<String>,
    /// The Artifact: a Receive Location, an Xmip Process, a Send Location.
    pub artifact: Option<String>,
    /// The Party it is about.
    pub party: Option<PartyId>,
    /// Diagnostics safe to hand outside: never a credential, never content.
    pub diagnostics: BTreeMap<String, String>,
}

impl Event {
    /// A completed action at `scope`, minted now, of the type Xmip gives
    /// it: `se.xmip.<action>.<outcome>`.
    #[must_use]
    pub fn completed(action: Stage, outcome: Outcome, scope: impl Into<String>) -> Self {
        Self::raised(Self::type_of(action, outcome), action, outcome, scope)
    }

    /// An Event of `kind`, minted now: a fresh identity and this moment.
    #[must_use]
    pub fn raised(
        kind: impl Into<String>,
        action: Stage,
        outcome: Outcome,
        scope: impl Into<String>,
    ) -> Self {
        Self {
            id: EventId::new(UuidV7Generator.next_u128()),
            kind: kind.into(),
            time_unix_nanos: SystemClock.unix_timestamp_nanos(),
            action,
            outcome,
            scope: scope.into(),
            journey: None,
            message: None,
            stream: None,
            endpoint: None,
            module: None,
            artifact: None,
            party: None,
            diagnostics: BTreeMap::new(),
        }
    }

    /// The type Xmip gives a completed action: `se.xmip.receive.success`.
    #[must_use]
    pub fn type_of(action: Stage, outcome: Outcome) -> String {
        format!("{TYPE_PREFIX}{}.{}", action.name(), outcome.word())
    }

    #[must_use]
    pub const fn in_journey(mut self, journey: JourneyId) -> Self {
        self.journey = Some(journey);
        self
    }

    #[must_use]
    pub const fn of_message(mut self, message: MessageId) -> Self {
        self.message = Some(message);
        self
    }

    #[must_use]
    pub const fn of_stream(mut self, stream: StreamId) -> Self {
        self.stream = Some(stream);
        self
    }

    #[must_use]
    pub fn at_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    #[must_use]
    pub fn by_module(mut self, module: impl Into<String>) -> Self {
        self.module = Some(module.into());
        self
    }

    #[must_use]
    pub fn on_artifact(mut self, artifact: impl Into<String>) -> Self {
        self.artifact = Some(artifact.into());
        self
    }

    #[must_use]
    pub const fn about(mut self, party: PartyId) -> Self {
        self.party = Some(party);
        self
    }

    /// One diagnostic, safe to hand outside.
    #[must_use]
    pub fn saying(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.diagnostics.insert(name.into(), value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_completed_action_is_typed_by_its_stage_and_outcome_and_minted_fresh() {
        let one = Event::completed(Stage::Receive, Outcome::Success, "xmip:///c/node/n");
        let two = Event::completed(Stage::Receive, Outcome::Success, "xmip:///c/node/n");

        assert_eq!(one.kind, "se.xmip.receive.success");
        assert_eq!(
            Event::type_of(Stage::Send, Outcome::ExhaustedRetries),
            "se.xmip.send.exhausted-retries"
        );
        assert_ne!(one.id, two.id, "every Event has its own identity");
        assert!(one.time_unix_nanos > 1_790_000_000_000_000_000);
    }

    #[test]
    fn an_event_carries_references_never_content() {
        let event = Event::completed(Stage::Send, Outcome::Failure, "xmip:///c/node/n/send/b")
            .in_journey(JourneyId::new(1))
            .of_message(MessageId::new(2))
            .of_stream(StreamId::new(3))
            .at_endpoint("https://billing.example/in")
            .by_module("xmip-core-transport-http")
            .on_artifact("billing")
            .about(PartyId::new(4))
            .saying("status", "503");

        assert_eq!(event.journey, Some(JourneyId::new(1)));
        assert_eq!(event.party, Some(PartyId::new(4)));
        assert_eq!(event.diagnostics["status"], "503");
        assert_eq!(
            EventError::new("no subscriber").to_string(),
            "no subscriber"
        );
    }
}
