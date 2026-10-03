//! The members of the cluster a hub does not hear now, and why: a node
//! that is down, unreachable or refusing the link is said, never silently
//! missing (ADR-0065, amendment 2026-10-02).
//!
//! A member is unheard from the moment its link could not be made or
//! broke, until its link is made again and carries the subscriptions; the
//! Events it raised meanwhile did not reach this node's subscribers. It is
//! said three ways: every drain of every subscription carries who is
//! unheard (`hub::Delivery`), and a change wakes a waiting drain to say so
//! with no Event; the hub answers it for its node's publication, which
//! every operator surface reads as *not hearing `<node>` since `<time>`:
//! `<why>`* (`observe::Unheard`); and each change is recorded in the
//! node's audit as `event.link`.

use std::sync::PoisonError;

use observe::Unheard;

use crate::hub::Hub;

impl Hub {
    /// The members of the cluster this hub does not hear now, by member.
    #[must_use]
    pub fn unheard(&self) -> Vec<Unheard> {
        self.inner
            .unheard
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    /// `node` is heard; whether it was unheard until now. Every
    /// subscription is told when it was.
    pub(crate) fn heard(&self, node: &str) -> bool {
        let was = self
            .inner
            .unheard
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(node)
            .is_some();
        if was {
            self.tell();
        }
        was
    }

    /// `node` is not heard by `by`, for `why`; whether it was heard until
    /// now. Every subscription is told when it was.
    pub(crate) fn not_heard(&self, by: &str, node: &str, why: &str) -> bool {
        let mut unheard = self
            .inner
            .unheard
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(held) = unheard.get_mut(node) {
            why.clone_into(&mut held.why);
            return false;
        }
        unheard.insert(
            node.to_string(),
            Unheard {
                by: by.to_string(),
                node: node.to_string(),
                since_unix_nanos: observe::now_unix_nanos(),
                why: why.to_string(),
            },
        );
        drop(unheard);
        self.tell();
        true
    }

    /// Wake every subscription's drain to hand over who is unheard.
    fn tell(&self) {
        for slot in self.slots().iter() {
            slot.queue.tell();
        }
    }
}
