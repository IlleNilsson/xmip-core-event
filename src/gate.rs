//! Who may subscribe to a hub: the authorization gate it asks, once, at
//! every subscribe (ADR-0065 clause 4, and its amendment of 2026-09-26: no
//! subscriber is admitted for its place, and every one names its Party and
//! is authorized as one).
//!
//! The gate is one thing: the policies the hub is handed — a node's, as it
//! starts, or those the program hosting the hub builds from the authorize
//! capability ([`Hub::authorize_by`]). Who may subscribe is a policy and
//! nothing beside it; nothing configured is a refusal, as at every gate.

use std::sync::{Arc, PoisonError, RwLock};

use authorize::Authorizer;

use crate::hub::Hub;

/// The policies a hub's subscribers are authorized by.
pub(crate) struct Gate(RwLock<Arc<Vec<Arc<dyn Authorizer>>>>);

impl Gate {
    pub(crate) fn new(policies: Vec<Arc<dyn Authorizer>>) -> Self {
        Self(RwLock::new(Arc::new(policies)))
    }

    /// The policies now.
    pub(crate) fn policies(&self) -> Arc<Vec<Arc<dyn Authorizer>>> {
        Arc::clone(&self.0.read().unwrap_or_else(PoisonError::into_inner))
    }
}

impl Hub {
    /// Authorize every subscriber by `policies` from now on, replacing what
    /// the hub was handed before: what a node starting hands the process's
    /// hub, and what a program hosting a hub of its own hands it.
    pub fn authorize_by(&self, policies: Vec<Arc<dyn Authorizer>>) {
        *self
            .inner
            .gate
            .0
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Arc::new(policies);
    }
}

#[cfg(test)]
mod tests {
    use audit::program_audit::ProgramAudit;
    use authorize_party::PartyPolicy;
    use xcore::PartyId;

    use super::*;
    use crate::filter::Filter;
    use crate::subscriber::Subscriber;

    fn subscriber(party: u128) -> Subscriber {
        let at = std::env::temp_dir().join("xmip-core-event-gate-tests");
        Subscriber::in_process(
            PartyId::new(party),
            ProgramAudit::new("xmip-core-event tests", Some(&at)),
        )
    }

    #[test]
    fn a_subscriber_in_this_process_is_refused_until_a_policy_allows_its_party() {
        let hub = Hub::new(Vec::new());
        let refused = hub.subscribe(subscriber(7), Filter::everything(), 0);
        assert!(refused.is_err(), "being here admits nobody");

        hub.authorize_by(vec![Arc::new(PartyPolicy::new().allow(PartyId::new(7)))]);
        assert!(
            hub.subscribe(subscriber(7), Filter::everything(), 0)
                .is_ok()
        );
        assert!(
            hub.subscribe(subscriber(8), Filter::everything(), 0)
                .is_err(),
            "another Party in the same process is not allowed by it"
        );
    }
}
