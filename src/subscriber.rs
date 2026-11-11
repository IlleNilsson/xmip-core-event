//! Who subscribes: a Party, authenticated like every other, authorized by
//! the gate every other attempt goes through (runtime-model section 17,
//! ADR-0065 clause 4).
//!
//! **A subscriber is the caller a gate proved, never a Party it names.**
//! [`Subscriber::proven`] takes the identity the authentication gate
//! answered and the declared Party that identity resolved to, and refuses a
//! caller not proven, or resolved to no Party or to another (the owner,
//! 2026-11-10: a Party is never resolved from what is said, only from what
//! the sender proved; ADR-0019, amendment 2026-11-11). A program in a node's
//! own process is the account the operating system names, proven by the
//! runtime's gate and resolved through that node's Parties (`xmip_operate.h`
//! section 11); a caller no Party holds subscribes as nobody, and that
//! refusal is audited by [`refused`]. This type checks what it is handed and
//! guards nothing beyond that: a proven identity is public to build, so any
//! code linked into a process can make one, and the gate that proves a
//! program's subscriber is the runtime's.
//!
//! Delivering an Event is Xmip presenting something to a Party, so the
//! question put to the gate is a Send: `authorize::Action::Send`, the
//! artifact the scope the subscription reaches, and the Contract the Event
//! type it asks for — each type once, or no Contract where it asks for
//! every type. The decision is `authorize::authorize`'s and nobody else's;
//! nothing configured is a refusal, as it is at every gate. Being in the
//! process admits a subscriber to nothing (ADR-0065, amendment 2026-09-26):
//! its Party is authorized as every other is, by the hub's gate
//! (`gate.rs`).

use audit::program_audit::ProgramAudit;
use authorize::{Action, Attempt, Authorizer, Decision, authorize};
use context::{Alignment, AuthenticatedIdentity, IdentityFacts, Minimum, OnMisalignment};
use party::Party;
use xcore::{Clock, PartyId, SystemClock};

use crate::EventError;
use crate::filter::Filter;

/// One subscriber: the Party its caller proved to be, the Party's name as
/// declared, the identity the gate answered, and where its deliveries and
/// refusals are audited.
#[derive(Clone, Debug)]
pub struct Subscriber {
    party: PartyId,
    /// The Party's name as it was declared (`party::Party::name`), what an
    /// operator reads it by; empty for a Party declared without one, and
    /// never made up from the identifier.
    name: String,
    pub(crate) identity: IdentityFacts,
    pub(crate) audit: ProgramAudit,
}

impl Subscriber {
    /// The subscriber `caller` is: the identity a gate proved, resolved to
    /// `party`, the Party as declared, whose identifier and name it carries;
    /// audited through `audit`.
    ///
    /// # Errors
    /// `caller` was not proven, or resolved to no Party or to another than
    /// `party`, in words.
    pub fn proven(
        caller: AuthenticatedIdentity,
        party: &Party,
        audit: ProgramAudit,
    ) -> Result<Self, EventError> {
        let who = format!("{} '{}'", caller.mechanism.name(), caller.value);
        if !caller.verified.is_proven() {
            return Err(EventError::new(format!(
                "{who} is {}, not proven: a subscriber is the caller a gate proved",
                caller.verified.word()
            )));
        }
        if caller.party_id != Some(party.party_id) {
            return Err(EventError::new(format!(
                "{who} is not the Party '{}' ({}): a subscriber is the Party its proven \
                 caller resolved to",
                party.name, party.party_id
            )));
        }
        Ok(Self {
            party: party.party_id,
            name: party.name.clone(),
            identity: IdentityFacts::evaluate(Alignment::None, caller, None),
            audit,
        })
    }

    /// The Party its caller proved to be.
    #[must_use]
    pub const fn party(&self) -> PartyId {
        self.party
    }

    /// The Party's name as declared; empty where it was declared without
    /// one.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether this subscriber may receive what `filter` asks for, by the
    /// authorization gate over `policies`: every type it names must be
    /// allowed at the scope it reaches.
    #[must_use]
    pub fn authorized(&self, filter: &Filter, policies: &[&dyn Authorizer]) -> Decision {
        let asked = Attempt::new(
            Action::Send,
            filter.reach(),
            SystemClock.unix_timestamp_nanos(),
        );
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
            .map(|attempt| {
                authorize(
                    policies,
                    &self.identity,
                    attempt,
                    OnMisalignment::Accept,
                    Minimum::default(),
                )
            })
            .find(|decision| !decision.allowed())
            .unwrap_or(Decision::Allowed)
    }
}

/// Audit, in `audit`, a subscribe refused before it had a subscriber —
/// `caller`, the identity the gate answered, proved to be no Party, or the
/// operating system named no account (`None`) — asking for `filter`, under
/// the names the hub records its own refusals with
/// (`audit_trail::described`), saying `said`. Kept before it returns, so
/// the refusal is on the record before its caller hears it: a subscribe is
/// no delivery, and no Event waits for it.
pub fn refused(
    audit: &ProgramAudit,
    caller: Option<&AuthenticatedIdentity>,
    filter: &Filter,
    said: &str,
) {
    let mut about = crate::audit_trail::asked(filter);
    if let Some(caller) = caller {
        about.extend(crate::audit_trail::presented(caller));
    }
    crate::audit_trail::kept(
        audit,
        "event.subscribe",
        xcore::Severity::Warning,
        said,
        about,
    );
}

/// Subscribers for tests, built as a gate would have proven them. Behind
/// the `test-support` feature, as `configure::fixture` is, which only a
/// dev-dependency turns on: it keeps a test's helper out of a node's build,
/// and guards nothing.
#[cfg(any(test, feature = "test-support"))]
pub mod fixture {
    use context::{AuthenticatedIdentity, Verified};
    use party::Party;
    use xcore::{Clock, Established, PartyId, SystemClock, mechanism};

    use super::{ProgramAudit, Subscriber};

    /// A Party known by its identifier alone, declared without a name.
    #[must_use]
    pub fn party(id: PartyId) -> Party {
        Party::new(id, None, "")
    }

    /// `party`, its caller proven as the test's own account would be: the
    /// operating system's word, `peer-credentials`, resolved to it.
    ///
    /// # Panics
    /// Never: the caller is proven and resolved to `party`.
    #[must_use]
    pub fn subscriber(party: &Party, audit: ProgramAudit) -> Subscriber {
        let caller = AuthenticatedIdentity::new(
            mechanism::peer_credentials(),
            "tester",
            Established::Detected,
            Verified::Proven,
        )
        .at(SystemClock.unix_timestamp_nanos())
        .resolving_to(party.party_id);
        Subscriber::proven(caller, party, audit).expect("proven and resolved")
    }
}

#[cfg(test)]
mod tests {
    use super::fixture::{party, subscriber};
    use super::*;
    use context::Verified;
    use xcore::{Established, Layer, mechanism};

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
        let subscriber = subscriber(&party(PartyId::new(1)), audit());

        let decision = subscriber.authorized(&Filter::everything(), &[]);

        assert!(!decision.allowed());
        assert!(
            decision.to_string().contains("send on 'xmip:///'"),
            "{decision}"
        );
    }

    #[test]
    fn every_type_asked_for_must_be_allowed() {
        let subscriber = subscriber(&party(PartyId::new(1)), audit());
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

    #[test]
    fn a_subscriber_is_only_the_party_its_proven_caller_resolved_to() {
        let declared = Party::new(PartyId::new(7), None, "operations");
        let caller = |verified, party: Option<u128>| {
            let identity = AuthenticatedIdentity::new(
                mechanism::peer_credentials(),
                r"CORP\jane",
                Established::Detected,
                verified,
            );
            match party {
                Some(id) => identity.resolving_to(PartyId::new(id)),
                None => identity,
            }
        };

        let proven = Subscriber::proven(caller(Verified::Proven, Some(7)), &declared, audit())
            .expect("the Party it proved to be");
        assert_eq!(
            (proven.party(), proven.name()),
            (PartyId::new(7), "operations")
        );

        for (verified, party, said) in [
            (Verified::Proven, None, "is not the Party 'operations'"),
            (Verified::Proven, Some(8), "is not the Party 'operations'"),
            (Verified::Claimed, Some(7), "is claimed, not proven"),
            (Verified::Vouched, Some(7), "is vouched, not proven"),
        ] {
            let refused = Subscriber::proven(caller(verified, party), &declared, audit())
                .expect_err("refused");
            assert!(refused.message.contains(said), "{refused}");
        }
    }

    #[test]
    fn a_subscribe_refused_before_it_had_a_subscriber_is_audited_with_who_and_what() {
        // ADR-0013 clause 1: the claim, its mechanism and how it was
        // established, and what it asked for, as the hub records its own.
        let at =
            std::env::temp_dir().join(format!("xmip-core-event-refused-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&at);
        let audit = ProgramAudit::new("xmip-core-event tests", Some(&at));
        let caller = AuthenticatedIdentity::new(
            mechanism::peer_credentials(),
            "nobody-held",
            Established::Detected,
            Verified::Proven,
        );
        let filter = Filter::everything().ending(crate::outcome::Outcome::Failure);

        refused(&audit, Some(&caller), &filter, "REFUSED: no Party holds it");
        audit::keeper::settle();

        let kept = std::fs::read_to_string(at.join(audit::file_sink::FILE_NAME)).expect("kept");
        for said in [
            "event.subscribe",
            "REFUSED: no Party holds it",
            "peer-credentials",
            "nobody-held",
            "detected",
            "every Event ending failure",
        ] {
            assert!(kept.contains(said), "{said} in {kept}");
        }
        let _ = std::fs::remove_dir_all(&at);
    }
}
