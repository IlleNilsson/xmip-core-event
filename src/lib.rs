#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use xmip_core::{JourneyId, MessageId, PartyId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    pub name: String,
    pub journey_id: JourneyId,
    pub message_id: MessageId,
    pub target_party: PartyId,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug)]
pub struct EventError {
    pub message: String,
}

impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl Error for EventError {}

pub trait EventPublisher: Send + Sync {
    fn publish(&self, event: Event) -> Result<(), EventError>;
}
