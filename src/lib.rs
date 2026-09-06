#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use xcore::{JourneyId, MessageId, PartyId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    pub name: String,
    pub journey_id: JourneyId,
    pub message_id: MessageId,
    pub target_party: PartyId,
    pub properties: BTreeMap<String, String>,
}

xcore::declare_error!(EventError);

pub trait EventPublisher: Send + Sync {
    fn publish(&self, event: Event) -> Result<(), EventError>;
}
