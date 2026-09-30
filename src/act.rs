//! What an operator does to a subscription: pause it, resume it, remove it
//! (ADR-0065, amendment 2026-09-29).
//!
//! The words are written here and nowhere else; the C boundary carries the
//! word (`xmip_operate.h` section 11, `xmip_event_subscription_act_v1`), and
//! every surface names an act by it. [`Hub::act`] is the one place an act is
//! applied: paused, a subscription keeps queuing up to its capacity and
//! hands nothing over; resumed, it hands over what queued; removed, it is
//! closed and gone. Every act is recorded in the subscriber's own audit,
//! with who took it, and an act on a subscription that is not there is
//! refused in words. Who may act at all is the surface's to decide by role;
//! the hub applies what reaches it.

use xcore::Severity;

use crate::EventError;
use crate::audit_trail::{described, record};
use crate::hub::Hub;

/// What an operator does to a subscription.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Act {
    /// Hold delivery; the queue keeps filling up to its capacity.
    Pause,
    /// Deliver again, what queued first.
    Resume,
    /// Unsubscribe it.
    Remove,
}

impl Act {
    /// Every act, in the order a surface offers them.
    pub const ALL: [Self; 3] = [Self::Pause, Self::Resume, Self::Remove];

    /// The word the estate names the act by.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Remove => "remove",
        }
    }

    /// The act a word names, exactly.
    #[must_use]
    pub fn named(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|act| act.word() == word)
    }

    /// The action an audit record of it carries.
    #[must_use]
    pub const fn action(self) -> &'static str {
        match self {
            Self::Pause => "event.pause",
            Self::Resume => "event.resume",
            Self::Remove => "event.remove",
        }
    }
}

impl Hub {
    /// Apply `act` to the subscription numbered `id`, by `who`, and say what
    /// came of it. Audited in the subscriber's audit.
    ///
    /// # Errors
    /// REFUSED, in words, when no subscription of that number is held here:
    /// it was removed, or never made.
    pub fn act(&self, id: u64, act: Act, who: &str) -> Result<String, EventError> {
        let Some(slot) = self.slot(id) else {
            return Err(EventError::new(format!(
                "REFUSED: no subscription {id} is held here; it was removed, or never made"
            )));
        };
        let said = match act {
            Act::Pause if slot.hold(true) => format!(
                "subscription {id} paused by {who}; it keeps queuing up to its capacity \
                 and hands nothing over until it is resumed"
            ),
            Act::Pause => format!("subscription {id} was already paused"),
            Act::Resume if slot.hold(false) => {
                format!("subscription {id} resumed by {who}; what queued is handed over")
            }
            Act::Resume => format!("subscription {id} was not paused"),
            Act::Remove => {
                slot.close();
                self.forget(id);
                format!("subscription {id} removed by {who}")
            }
        };

        let mut about = described(&slot.subscriber, &slot.filter);
        about.insert("subscription".to_string(), id.to_string());
        about.insert("by".to_string(), who.to_string());
        record(
            &slot.subscriber,
            act.action(),
            Severity::Information,
            &said,
            about,
        );
        Ok(said)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_act_is_its_word_and_nothing_else_is_one() {
        for act in Act::ALL {
            assert_eq!(Act::named(act.word()), Some(act));
            assert!(act.action().ends_with(act.word()));
        }
        assert_eq!(Act::named("Pause"), None);
        assert_eq!(Act::named("unsubscribe"), None);
    }
}
