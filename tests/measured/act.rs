//! An operator's acts on a subscription (ADR-0065, amendment 2026-09-29),
//! measured: a resume wakes a drain waiting on the subscription at once.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use authorize_party::PartyPolicy;
use node::Stage;
use observe::Act;
use xcore::PartyId;
use xmip_core_event::Event;
use xmip_core_event::filter::Filter;
use xmip_core_event::hub::Hub;
use xmip_core_event::outcome::Outcome;
use xmip_core_event::subscriber::Subscriber;

fn directory(name: &str) -> PathBuf {
    let at = std::env::temp_dir().join(format!("xmip-event-act-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&at);
    at
}

fn subscriber(at: &Path) -> Subscriber {
    Subscriber::in_process(
        PartyId::new(42),
        ProgramAudit::new("xmip-core-event tests", Some(at)),
    )
}

#[test]
fn a_resume_wakes_a_waiting_drain_within_a_millisecond() {
    let at = directory("resume-measured");
    let hub = Arc::new(Hub::new(vec![Arc::new(
        PartyPolicy::new().allow(PartyId::new(42)),
    )]));
    let subscription = hub
        .subscribe(subscriber(&at), Filter::everything(), 0)
        .expect("allowed");
    let id = subscription.id();
    hub.act(id, Act::Pause, "ilian").expect("paused");
    let node = configure::fixture::test_cluster().node_scope(0);
    let _ = hub.publish(Event::completed(
        Stage::Receive,
        Outcome::Success,
        format!("{node}/receive/a"),
    ));

    let (tell, told) = mpsc::channel();
    let waiting = std::thread::spawn(move || {
        let delivery = subscription.next(Duration::from_secs(5), 16);
        tell.send(Instant::now()).expect("told");
        (delivery, subscription)
    });
    std::thread::sleep(Duration::from_millis(20));
    let resumed_at = Instant::now();
    hub.act(id, Act::Resume, "ilian").expect("resumed");

    let woke = told.recv_timeout(Duration::from_secs(5)).expect("woke");
    let (delivery, subscription) = waiting.join().expect("joined");
    assert_eq!(delivery.events.len(), 1);
    assert!(
        woke.duration_since(resumed_at) < Duration::from_millis(1),
        "resume wakes the drain at once: {:?}",
        woke.duration_since(resumed_at)
    );
    drop(subscription);
    let _ = std::fs::remove_dir_all(&at);
}
