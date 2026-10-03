//! The in-process hub: a subscription receives what its filter matches and
//! nothing else, a refused subscriber receives nothing, a full queue never
//! holds the publisher, every delivery and refusal is audited, and a
//! publish costs microseconds — measured here, because anything over a
//! millisecond apart from load is a defect (CONTRIBUTING.md).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use audit::keeper::settle;
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
    let _ = fs::remove_dir_all(&at);
    at
}

fn subscriber(at: &Path) -> Subscriber {
    Subscriber::in_process(
        PartyId::new(42),
        ProgramAudit::new("xmip-core-event tests", Some(at)),
    )
}

fn audited(at: &Path) -> String {
    settle();
    fs::read_to_string(at.join(audit::file_sink::FILE_NAME)).unwrap_or_default()
}

fn hub() -> Hub {
    Hub::new(vec![Arc::new(PartyPolicy::new().allow(PartyId::new(42)))])
}

fn received(outcome: Outcome) -> Event {
    let node = configure::fixture::test_cluster().node_scope(0);
    Event::completed(Stage::Receive, outcome, format!("{node}/receive/orders"))
}

/// The test cluster's scope.
fn cluster() -> String {
    configure::fixture::test_cluster().scope()
}

#[test]
fn a_subscription_receives_what_it_matches_and_the_delivery_is_audited() {
    let at = directory("match");
    let hub = hub();
    let failures = Filter::everything()
        .ending(Outcome::Failure)
        .beneath(cluster());
    let subscription = hub
        .subscribe(subscriber(&at), failures, 0)
        .expect("allowed");

    assert_eq!(hub.publish(received(Outcome::Success)), 0, "not a failure");
    let failed = received(Outcome::Failure);
    assert_eq!(hub.publish(failed.clone()), 1);

    let delivery = subscription.next(Duration::from_secs(1), 16);
    assert_eq!(delivery.events.len(), 1);
    assert_eq!(*delivery.events[0], failed);
    let audit = audited(&at);
    assert!(audit.contains("action = \"event.subscribe\""), "{audit}");
    assert!(audit.contains("action = \"event.deliver\""), "{audit}");
    assert!(audit.contains(&failed.id.to_string()), "{audit}");

    drop(subscription);
    assert_eq!(hub.subscriptions(), 0, "dropping unsubscribes");
    assert!(audited(&at).contains("action = \"event.unsubscribe\""));
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn a_subscriber_the_gate_refuses_gets_no_subscription_and_the_refusal_is_audited() {
    let at = directory("refused");
    let closed = Hub::new(Vec::new());

    let refused = closed.subscribe(subscriber(&at), Filter::everything(), 0);

    let said = refused
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(said.contains("no policy is configured"), "{said}");
    assert_eq!(closed.subscriptions(), 0);
    let audit = audited(&at);
    assert!(audit.contains("severity = \"warning\""), "{audit}");
    assert!(audit.contains("no policy is configured"), "{audit}");
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn a_full_queue_refuses_counts_and_never_holds_the_publisher() {
    let at = directory("full");
    let hub = hub();
    let subscription = hub
        .subscribe(subscriber(&at), Filter::everything(), 2)
        .expect("allowed");

    let taken: usize = (0..5)
        .map(|_| hub.publish(received(Outcome::Success)))
        .sum();

    assert_eq!(taken, 2, "the queue holds two");
    let delivery = subscription.next(Duration::ZERO, 16);
    assert_eq!(delivery.events.len(), 2);
    assert_eq!(delivery.refused, 3);
    assert!(audited(&at).contains("3 matching Events refused"));
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn nothing_published_is_an_empty_delivery_after_the_timeout() {
    let at = directory("empty");
    let hub = hub();
    let subscription = hub
        .subscribe(subscriber(&at), Filter::everything(), 0)
        .expect("allowed");

    let started = Instant::now();
    let delivery = subscription.next(Duration::from_millis(20), 16);

    assert!(delivery.events.is_empty());
    assert!(started.elapsed() >= Duration::from_millis(20));
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn a_listener_is_called_back_on_its_own_thread_until_dropped() {
    let at = directory("listen");
    let hub = hub();
    let subscription = hub
        .subscribe(subscriber(&at), Filter::everything(), 0)
        .expect("allowed");
    let (tell, told) = mpsc::channel();
    let publisher = std::thread::current().id();

    let listener = subscription
        .listen(move |event| {
            let _ = tell.send((event.id, std::thread::current().id()));
        })
        .expect("a thread");
    let event = received(Outcome::Success);
    hub.publish(event.clone());

    let (id, thread) = told
        .recv_timeout(Duration::from_secs(2))
        .expect("called back");
    assert_eq!(id, event.id);
    assert_ne!(thread, publisher, "never on the publisher's thread");
    drop(listener);
    assert_eq!(hub.subscriptions(), 0, "dropping the listener unsubscribes");
    let _ = fs::remove_dir_all(&at);
}

/// The millisecond rule, measured: one publish to a hundred subscriptions,
/// every one of which matches.
#[test]
fn a_publish_to_a_hundred_subscriptions_takes_microseconds() {
    let at = directory("cost");
    let hub = hub();
    let subscriptions: Vec<_> = (0..100)
        .map(|_| {
            hub.subscribe(
                subscriber(&at),
                Filter::everything().beneath(cluster()),
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
    let _ = fs::remove_dir_all(&at);
}
