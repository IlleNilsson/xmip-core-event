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
use serde_json::{Value, json};
use xcore::PartyId;

use crate::outcome::Outcome;
use crate::{Event, EventError};

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

impl Filter {
    /// This filter as it travels to another node of the cluster, pushed
    /// down so that only what it matches crosses (`cluster.rs`): JSON, the
    /// outcomes by their words and the Party by its identifier.
    #[must_use]
    pub fn json(&self) -> Value {
        let outcomes: Vec<&str> = self.outcomes.iter().map(|outcome| outcome.word()).collect();
        json!({
            "types": self.types,
            "outcomes": outcomes,
            "scope": self.scope,
            "party": self.party.map(|party| party.to_string()),
        })
    }

    /// The filter [`Self::json`] wrote.
    ///
    /// # Errors
    /// Not an object of those members, an outcome no word names, or a
    /// Party that is not an identifier.
    pub fn read_json(value: &Value) -> Result<Self, EventError> {
        let refused = |what: &str| EventError::new(format!("a filter whose {what} is not one"));
        let texts = |name: &str| -> Result<Vec<&str>, EventError> {
            value
                .get(name)
                .and_then(Value::as_array)
                .ok_or_else(|| refused(name))?
                .iter()
                .map(|text| text.as_str().ok_or_else(|| refused(name)))
                .collect()
        };
        let optional = |name: &str| -> Result<Option<&str>, EventError> {
            match value.get(name) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::String(text)) => Ok(Some(text)),
                Some(_) => Err(refused(name)),
            }
        };
        let outcomes = texts("outcomes")?
            .into_iter()
            .map(|word| Outcome::named(word).ok_or_else(|| refused("outcome")))
            .collect::<Result<_, _>>()?;
        let party = optional("party")?
            .map(|party| party.parse::<PartyId>().map_err(|_| refused("party")))
            .transpose()?;
        Ok(Self {
            types: texts("types")?.into_iter().map(str::to_string).collect(),
            outcomes,
            scope: optional("scope")?.map(str::to_string),
            party,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use node::Stage;

    /// The scope of the test cluster's node at `place`.
    fn node(place: usize) -> String {
        configure::fixture::test_cluster().node_scope(place)
    }

    fn sent(outcome: Outcome) -> Event {
        Event::completed(Stage::Send, outcome, format!("{}/send/billing", node(0)))
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
            .beneath(node(0))
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

        let cluster = configure::fixture::test_cluster().scope();
        assert!(Filter::everything().beneath(cluster).matches(&event));
        assert!(Filter::everything().beneath("xmip:///").matches(&event));
        assert!(!Filter::everything().beneath(node(1)).matches(&event));
        assert!(
            !Filter::everything()
                .beneath(format!("{}/send/billing/x", node(0)))
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
    fn a_filter_crosses_to_another_node_as_it_was() {
        let filter = Filter::everything()
            .of_type("se.xmip.send.failure")
            .ending(Outcome::ExhaustedRetries)
            .beneath(node(0))
            .about(PartyId::new(7));

        for each in [Filter::everything(), filter] {
            assert_eq!(Filter::read_json(&each.json()).expect("read"), each);
        }
        assert!(Filter::read_json(&json!({"types": [], "outcomes": ["late"]})).is_err());
        assert!(Filter::read_json(&json!("every Event")).is_err());
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
