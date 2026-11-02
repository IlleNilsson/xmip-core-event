//! The in-process hub, measured: a publish costs microseconds, to a
//! hundred matching subscriptions and to one.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use authorize_party::PartyPolicy;
use node::Stage;
use xcore::PartyId;
use xmip_core_event::Event;
use xmip_core_event::filter::Filter;
use xmip_core_event::hub::Hub;
use xmip_core_event::outcome::Outcome;
use xmip_core_event::subscriber::Subscriber;

/// A directory of this test's own for the audit it writes.
fn directory(name: &str) -> PathBuf {
    let at = std::env::temp_dir().join(format!("xmip-event-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&at);
    at
}

fn subscriber(at: &Path) -> Subscriber {
    Subscriber::in_process(
        PartyId::new(42),
        ProgramAudit::new("xmip-core-event tests", Some(at)),
    )
}

fn received(outcome: Outcome) -> Event {
    let node = configure::fixture::test_cluster().node_scope(0);
    Event::completed(Stage::Receive, outcome, format!("{node}/receive/orders"))
}

#[test]
fn a_publish_to_a_hundred_subscriptions_takes_microseconds() {
    let at = directory("cost");
    let hub = Hub::new(vec![Arc::new(PartyPolicy::new().allow(PartyId::new(42)))]);
    let cluster = configure::fixture::test_cluster().scope();
    let subscriptions: Vec<_> = (0..100)
        .map(|_| {
            hub.subscribe(
                subscriber(&at),
                Filter::everything().beneath(cluster.clone()),
                4096,
            )
            .expect("allowed")
        })
        .collect();
    let events: Vec<Event> = (0..1000).map(|_| received(Outcome::Success)).collect();

    let started = Instant::now();
    for event in events {
        hub.publish(event);
    }
    let each = started.elapsed() / 1000;

    println!("one publish to 100 matching subscriptions: {each:?}");
    assert!(each < Duration::from_millis(1), "a publish took {each:?}");
    drop(subscriptions);

    let one = hub
        .subscribe(subscriber(&at), Filter::everything(), 4096)
        .expect("allowed");
    let events: Vec<Event> = (0..1000).map(|_| received(Outcome::Success)).collect();
    let started = Instant::now();
    for event in events {
        hub.publish(event);
    }
    let each = started.elapsed() / 1000;
    println!("one publish to 1 matching subscription: {each:?}");
    assert!(each < Duration::from_millis(1), "a publish took {each:?}");
    drop(one);
    let _ = std::fs::remove_dir_all(&at);
}
