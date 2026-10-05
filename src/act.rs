//! What an operator does to an Event subscription: pause it, resume it,
//! remove it (ADR-0065, amendment 2026-09-29).
//!
//! The acts and their words are `observe::Act`'s, written once for every
//! noun an operator acts on; the C boundary carries the word (`xmip_operate.h`
//! section 11, `xmip_event_subscription_act_v1`), and every surface names an
//! act by it. [`Hub::act`] is the one place an act on an Event subscription is
//! applied: paused, a subscription keeps queuing up to its capacity and
//! hands nothing over; resumed, it hands over what queued; removed, it is
//! closed and gone. Every act is recorded in the subscriber's own audit,
//! with who took it, and an act on a subscription that is not there is
//! refused in words. Who may act at all is the surface's to decide by role;
//! the hub applies what reaches it.

use observe::{Act, Noun};
use xcore::Severity;

use crate::EventError;
use crate::audit_trail::{described, record};
use crate::hub::Hub;

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
            Act::Pause if slot.queue.hold(true) => format!(
                "subscription {id} paused by {who}; it keeps queuing up to its capacity \
                 and hands nothing over until it is resumed"
            ),
            Act::Pause => format!("subscription {id} was already paused"),
            Act::Resume if slot.queue.hold(false) => {
                format!("subscription {id} resumed by {who}; what queued is handed over")
            }
            Act::Resume => format!("subscription {id} was not paused"),
            Act::Remove => {
                slot.queue.close();
                self.forget(id);
                format!("subscription {id} removed by {who}")
            }
            // No act an Event subscription takes: refused in observe's words.
            Act::Replay | Act::Retry | Act::Dismiss => {
                return Err(EventError::new(
                    Noun::EventSubscription
                        .act(act.word())
                        .err()
                        .unwrap_or_default(),
                ));
            }
        };

        let mut about = described(&slot.subscriber, &slot.filter);
        about.insert("subscription".to_string(), id.to_string());
        about.insert("by".to_string(), who.to_string());
        record(
            &slot.subscriber,
            &Noun::EventSubscription.action(act),
            Severity::Information,
            &said,
            about,
        );
        Ok(said)
    }
}
