//! What a subscription asks for, and the one rule that decides whether an
//! Event is it (ADR-0065 clause 1).
//!
//! A filter names Event types, outcomes, a scope and a Party. Each it leaves
//! empty means any; each it names must hold. The scope holds when the
//! Event happened at or beneath it, by `observe::Scope::contains` — the one
//! containment rule the operator boundary already forwards — so a
//! subscription to a node sees every Location on it. The Party holds when
//! the Event is about that Party. No binding, no transport and no surface
//! decides this again: the C boundary, the hub and the wire all ask
//! [`Filter::matches`].

use observe::Scope;
use xcore::PartyId;

use crate::Event;
use crate::outcome::Outcome;

/// What a subscription asks for.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Filter {
    /// Event types, exactly; empty is every type.
    pub types: Vec<String>,
    /// Outcomes; empty is every outcome.
    pub outcomes: Vec<Outcome>,
    /// An Xmip URI the Event must have happened at or beneath; `None` is
    /// everywhere.
    pub scope: Option<String>,
    /// The Party the Event must be about; `None` is any Party, or none.
    pub party: Option<PartyId>,
}

impl Filter {
    /// Every Event.
    #[must_use]
    pub fn everything() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn of_type(mut self, kind: impl Into<String>) -> Self {
        self.types.push(kind.into());
        self
    }

    #[must_use]
    pub fn ending(mut self, outcome: Outcome) -> Self {
        self.outcomes.push(outcome);
        self
    }

    #[must_use]
    pub fn beneath(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    #[must_use]
    pub const fn about(mut self, party: PartyId) -> Self {
        self.party = Some(party);
        self
    }

    /// Whether `event` is what this filter asks for. The one rule.
    #[must_use]
    pub fn matches(&self, event: &Event) -> bool {
        (self.types.is_empty() || self.types.contains(&event.kind))
            && (self.outcomes.is_empty() || self.outcomes.contains(&event.outcome))
            && self
                .scope
                .as_deref()
                .is_none_or(|scope| Scope::new(scope).contains(Scope::new(&event.scope)))
            && self.party.is_none_or(|party| event.party == Some(party))
    }

    /// The scope this filter reaches, as the authorization gate is asked
    /// about it: the filter's own, or the whole installation.
    #[must_use]
    pub fn reach(&self) -> &str {
        self.scope.as_deref().unwrap_or("xmip:///")
    }

    /// What this filter subscribes to, in the words a surface lists it by:
    /// the types it names, or every Event, then the outcomes and the Party
    /// where it names them. The scope is [`Self::reach`], said apart.
    #[must_use]
    pub fn said(&self) -> String {
        let mut said = if self.types.is_empty() {
            "every Event".to_string()
        } else {
            self.types.join(", ")
        };
        if !self.outcomes.is_empty() {
            let outcomes: Vec<&str> = self.outcomes.iter().map(|outcome| outcome.word()).collect();
            said.push_str(" ending ");
            said.push_str(&outcomes.join(", "));
        }
        if let Some(party) = self.party {
            said.push_str(" about ");
            said.push_str(&party.to_string());
        }
        said
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use node::Stage;

    fn sent(outcome: Outcome) -> Event {
        Event::completed(Stage::Send, outcome, "xmip:///c/node/n/send/billing")
            .about(PartyId::new(7))
    }

    #[test]
    fn an_empty_filter_asks_for_everything() {
        assert!(Filter::everything().matches(&sent(Outcome::Success)));
    }

    #[test]
    fn every_named_part_must_hold() {
        let filter = Filter::everything()
            .of_type("se.xmip.send.failure")
            .ending(Outcome::Failure)
            .beneath("xmip:///c/node/n")
            .about(PartyId::new(7));

        assert!(filter.matches(&sent(Outcome::Failure)));
        assert!(!filter.matches(&sent(Outcome::Success)), "type and outcome");
        assert!(
            !filter
                .clone()
                .about(PartyId::new(8))
                .matches(&sent(Outcome::Failure)),
            "a Party that is not the one asked for"
        );
    }

    #[test]
    fn a_scope_holds_beneath_itself_and_never_beside() {
        let event = sent(Outcome::Success);

        assert!(Filter::everything().beneath("xmip:///c").matches(&event));
        assert!(Filter::everything().beneath("xmip:///").matches(&event));
        assert!(
            !Filter::everything()
                .beneath("xmip:///c/node/m")
                .matches(&event)
        );
        assert!(
            !Filter::everything()
                .beneath("xmip:///c/node/n/send/billing/x")
                .matches(&event),
            "never above"
        );
    }

    #[test]
    fn a_party_filter_refuses_an_event_about_no_party() {
        let mut event = sent(Outcome::Success);
        event.party = None;

        assert!(!Filter::everything().about(PartyId::new(7)).matches(&event));
        assert_eq!(Filter::everything().reach(), "xmip:///");
    }

    #[test]
    fn a_filter_says_what_it_subscribes_to() {
        assert_eq!(Filter::everything().said(), "every Event");
        assert_eq!(
            Filter::everything()
                .of_type("se.xmip.send.failure")
                .of_type("se.xmip.send.timeout")
                .said(),
            "se.xmip.send.failure, se.xmip.send.timeout"
        );
        assert_eq!(
            Filter::everything()
                .ending(Outcome::Failure)
                .ending(Outcome::ExhaustedRetries)
                .said(),
            "every Event ending failure, exhausted-retries"
        );
    }
}
