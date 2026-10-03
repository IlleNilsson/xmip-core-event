//! Who subscribes: a Party, authenticated like every other, authorized by
//! the gate every other attempt goes through (runtime-model section 17,
//! ADR-0065 clause 4).
//!
//! Delivering an Event is Xmip presenting something to a Party, so the
//! question put to the gate is a Send: `authorize::Action::Send`, the
//! artifact the scope the subscription reaches, and the Contract the Event
//! type it asks for — each type once, or no Contract where it asks for
//! every type. The decision is `authorize::authorize`'s and nobody else's;
//! nothing configured is a refusal, as it is at every gate.
//!
//! A subscriber in this process — a C, .NET, Java or Python program that
//! loaded the runtime's library — is recognized as the operating system
//! vouches for a Unix socket's peer: its identity is `peer-credentials`
//! naming this process, resolved to the Party it names. Being here admits
//! it to nothing (ADR-0065, amendment 2026-09-26): its Party is authorized
//! as every other is, by the hub's gate (`gate.rs`).

use audit::program_audit::ProgramAudit;
use authorize::{Action, Attempt, Authorizer, Decision, authorize};
use context::{Alignment, AuthenticatedIdentity, IdentityFacts, OnMisalignment, Verified};
use party::Party;
use xcore::{Clock, Established, PartyId, SystemClock, mechanism};

use crate::filter::Filter;

/// One subscriber: the Party, its name where the Party was declared with
/// one, how it was recognized, and where its deliveries and refusals are
/// audited.
#[derive(Clone, Debug)]
pub struct Subscriber {
    pub party: PartyId,
    /// The Party's name as it was declared (`party::Party::name`), what an
    /// operator reads it by; empty for a subscriber known by its identifier
    /// alone, and never made up from the identifier.
    pub name: String,
    pub identity: IdentityFacts,
    pub audit: ProgramAudit,
}

impl Subscriber {
    #[must_use]
    pub const fn new(party: PartyId, identity: IdentityFacts, audit: ProgramAudit) -> Self {
        Self {
            party,
            name: String::new(),
            identity,
            audit,
        }
    }

    /// A program in this process subscribing as the declared `party`: its
    /// identifier and its name are the declaration's, taken from it and
    /// nowhere else.
    #[must_use]
    pub fn declared(party: &Party, audit: ProgramAudit) -> Self {
        Self {
            name: party.name.clone(),
            ..Self::in_process(party.party_id, audit)
        }
    }

    /// A program in this process, as `party`: `peer-credentials` naming
    /// this process, proven now, resolved to the Party.
    #[must_use]
    pub fn in_process(party: PartyId, audit: ProgramAudit) -> Self {
        let identity = AuthenticatedIdentity::new(
            mechanism::peer_credentials(),
            this_process(),
            Established::Passed,
            Verified::Proven,
        )
        .at(SystemClock.unix_timestamp_nanos())
        .resolving_to(party);

        Self::new(
            party,
            IdentityFacts::evaluate(Alignment::None, identity, None),
            audit,
        )
    }

    /// Whether this subscriber may receive what `filter` asks for, by the
    /// authorization gate over `policies`: every type it names must be
    /// allowed at the scope it reaches.
    #[must_use]
    pub fn authorized(&self, filter: &Filter, policies: &[&dyn Authorizer]) -> Decision {
        let asked =
            Attempt::new(Action::Send, filter.reach()).at(SystemClock.unix_timestamp_nanos());
        let attempts: Vec<Attempt> = if filter.types.is_empty() {
            vec![asked]
        } else {
            filter
                .types
                .iter()
                .map(|kind| asked.clone().on_contract(kind.clone()))
                .collect()
        };

        attempts
            .iter()
            .map(|attempt| authorize(policies, &self.identity, attempt, OnMisalignment::Accept))
            .find(|decision| !decision.allowed())
            .unwrap_or(Decision::Allowed)
    }
}

/// This process, as a `peer-credentials` value names one.
fn this_process() -> String {
    format!("process {}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use xcore::Layer;

    struct Only(&'static str);

    impl Authorizer for Only {
        fn name(&self) -> &'static str {
            "only"
        }

        fn layer(&self) -> Layer {
            Layer::Transport
        }

        fn decide(&self, _: &IdentityFacts, attempt: &Attempt) -> Option<Decision> {
            Some(match attempt.contract.as_deref() {
                Some(kind) if kind == self.0 => Decision::Allowed,
                _ => Decision::denied("only", format!("only {}", self.0)),
            })
        }
    }

    fn audit() -> ProgramAudit {
        let at = std::env::temp_dir().join("xmip-core-event-subscriber-tests");
        ProgramAudit::new("xmip-core-event tests", Some(&at))
    }

    #[test]
    fn nothing_configured_is_a_refusal_as_at_every_gate() {
        let subscriber = Subscriber::in_process(PartyId::new(1), audit());

        let decision = subscriber.authorized(&Filter::everything(), &[]);

        assert!(!decision.allowed());
        assert!(
            decision.to_string().contains("send on 'xmip:///'"),
            "{decision}"
        );
    }

    #[test]
    fn every_type_asked_for_must_be_allowed() {
        let subscriber = Subscriber::in_process(PartyId::new(1), audit());
        let only = Only("se.xmip.send.failure");
        let policies: [&dyn Authorizer; 1] = [&only];
        let one = Filter::everything().of_type("se.xmip.send.failure");

        assert!(subscriber.authorized(&one, &policies).allowed());
        assert!(
            !subscriber
                .authorized(&one.of_type("se.xmip.receive.success"), &policies)
                .allowed()
        );
    }
}
