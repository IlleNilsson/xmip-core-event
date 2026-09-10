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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Kept(Mutex<Vec<Event>>);

    impl EventPublisher for Kept {
        fn publish(&self, event: Event) -> Result<(), EventError> {
            self.0
                .lock()
                .map_err(|_| EventError::new("poisoned"))?
                .push(event);
            Ok(())
        }
    }

    #[test]
    fn a_published_event_reaches_the_publisher_whole() {
        let kept = Kept(Mutex::new(Vec::new()));
        let event = Event {
            name: "order-received".to_string(),
            journey_id: JourneyId::new(1),
            message_id: MessageId::new(2),
            target_party: PartyId::new(3),
            properties: [("region".to_string(), "eu".to_string())]
                .into_iter()
                .collect(),
        };
        kept.publish(event.clone()).expect("published");
        assert_eq!(kept.0.lock().expect("kept").as_slice(), &[event]);
        assert_eq!(
            EventError::new("no subscriber").to_string(),
            "no subscriber"
        );
    }
}
