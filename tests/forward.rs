//! At least once over the wire: an Event the wire failed is kept, in
//! order, until it is acknowledged; the retry guard decides each attempt;
//! what was carried and what was given up on is audited.

use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use audit::program_audit::ProgramAudit;
use node::Stage;
use resilience::{Failure, Guard};
use retry::Retry;
use xcore::PartyId;
use xmip_core_event::Event;
use xmip_core_event::audit_queue::settle;
use xmip_core_event::binding::{Binding, Carried, Mode};
use xmip_core_event::filter::Filter;
use xmip_core_event::forward::{Forwarder, Wire};
use xmip_core_event::hub::Hub;
use xmip_core_event::outcome::Outcome;
use xmip_core_event::subscriber::{SameProcess, Subscriber};

/// A far end that refuses the first `refusing` attempts, then keeps what
/// it is given and the Party it was carried to.
struct FarEnd {
    refusing: Mutex<u32>,
    kept: Mutex<Vec<(PartyId, Carried)>>,
}

/// The far end as the forwarder holds it, the test holding it too.
struct Shared(Arc<FarEnd>);

impl Wire for Shared {
    fn carry(&self, party: PartyId, carried: &Carried) -> Result<(), Failure> {
        let mut refusing = self.0.refusing.lock().expect("refusing");
        if *refusing > 0 {
            *refusing -= 1;
            return Err(Failure::retryable("the far end is busy"));
        }
        self.0
            .kept
            .lock()
            .expect("kept")
            .push((party, carried.clone()));
        Ok(())
    }
}

fn directory() -> PathBuf {
    let at = std::env::temp_dir().join(format!("xmip-event-forward-{}", std::process::id()));
    let _ = fs::remove_dir_all(&at);
    at
}

#[test]
fn an_event_is_kept_until_the_wire_takes_it_and_arrives_in_order() {
    let at = directory();
    let hub = Hub::new(vec![Arc::new(SameProcess)]);
    let party = PartyId::new(9);
    let subscriber =
        Subscriber::in_process(party, ProgramAudit::new("xmip-core-event tests", Some(&at)));
    let subscription = hub
        .subscribe(subscriber, Filter::everything(), 0)
        .expect("allowed");
    let far = Arc::new(FarEnd {
        refusing: Mutex::new(3),
        kept: Mutex::new(Vec::new()),
    });
    let mut forwarder = Forwarder::new(
        subscription,
        Binding::Kafka,
        Mode::Binary,
        Shared(Arc::clone(&far)),
    );
    let first = Event::completed(Stage::Send, Outcome::Failure, "xmip:///c/node/n/send/b");
    let second = Event::completed(Stage::Send, Outcome::Success, "xmip:///c/node/n/send/b");
    hub.publish(first.clone());
    hub.publish(second.clone());

    let twice = Retry::new(2, Duration::ZERO);
    let guards: [&dyn Guard; 1] = [&twice];
    let gave_up = forwarder.pump(Duration::from_millis(50), &guards);

    assert_eq!(gave_up.carried, 0, "two attempts, both refused");
    assert_eq!(gave_up.pending, 2, "both kept, in order");
    assert_eq!(gave_up.gave_up.as_deref(), Some("the far end is busy"));

    let carried = forwarder.pump(Duration::ZERO, &guards);

    assert_eq!(carried.carried, 2);
    assert_eq!(carried.pending, 0);
    let kept = far.kept.lock().expect("kept");
    let ids: Vec<String> = kept
        .iter()
        .map(|(to, carried)| {
            assert_eq!(*to, party, "carried to the subscriber's Party");
            let wire_event = Binding::Kafka.read(carried).expect("a WireEvent");
            wire_event.id
        })
        .collect();
    assert_eq!(ids, [first.id.to_string(), second.id.to_string()]);
    settle();
    let audit = fs::read_to_string(at.join(audit::file_sink::FILE_NAME)).expect("audited");
    assert!(audit.contains("given up for now"), "{audit}");
    assert!(audit.contains("2 Events carried"), "{audit}");
    let _ = fs::remove_dir_all(&at);
}
