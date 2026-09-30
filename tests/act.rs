//! An operator's acts on a subscription (ADR-0065, amendment 2026-09-29):
//! paused, it keeps queuing and hands nothing over; resumed, it hands over
//! what queued; removed, it is gone and its holder finds it closed; an act
//! on nothing is refused in words; every act is audited with who took it;
//! and what the hub holds is listed as a node publishes it.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use audit::keeper::settle;
use audit::program_audit::ProgramAudit;
use node::Stage;
use observe::SubscriptionState;
use party::{Party, PartyKind};
use xcore::PartyId;
use xmip_core_event::Event;
use xmip_core_event::act::Act;
use xmip_core_event::filter::Filter;
use xmip_core_event::hub::Hub;
use xmip_core_event::outcome::Outcome;
use xmip_core_event::subscriber::{SameProcess, Subscriber};

const NODE: &str = "xmip:///CT/node/R1";

fn directory(name: &str) -> PathBuf {
    let at = std::env::temp_dir().join(format!("xmip-event-act-{name}-{}", std::process::id()));
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
    Hub::new(vec![Arc::new(SameProcess)])
}

fn received() -> Event {
    Event::completed(
        Stage::Receive,
        Outcome::Success,
        "xmip:///CT/node/R1/receive/a",
    )
}

#[test]
fn a_paused_subscription_keeps_queuing_hands_nothing_over_and_counts_what_it_missed() {
    let at = directory("pause");
    let hub = hub();
    let operations = Party::new(PartyId::new(42), PartyKind::Service, "operations");
    let declared = Subscriber::declared(
        &operations,
        ProgramAudit::new("xmip-core-event tests", Some(&at)),
    );
    let subscription = hub
        .subscribe(declared, Filter::everything(), 2)
        .expect("allowed");
    let id = subscription.id();

    let said = hub.act(id, Act::Pause, "ilian").expect("paused");
    assert!(said.contains("paused by ilian"), "{said}");
    assert_eq!(hub.publish(received()), 1, "a paused queue still takes");
    assert_eq!(hub.publish(received()), 1);
    assert_eq!(hub.publish(received()), 0, "and still holds its capacity");

    let started = Instant::now();
    let held = subscription.next(Duration::from_millis(30), 16);
    assert!(
        held.events.is_empty() && held.refused == 0,
        "nothing handed over"
    );
    assert!(started.elapsed() >= Duration::from_millis(25), "it waited");

    let [standing] = hub.standing(NODE).try_into().expect("one");
    assert_eq!(standing.state, SubscriptionState::Paused);
    assert_eq!(
        (standing.queued, standing.missed, standing.delivered),
        (2, 1, 0)
    );
    assert_eq!(standing.node, NODE);
    assert_eq!(
        standing.subscriber, "operations",
        "the name it was declared with"
    );
    assert_eq!(standing.party, PartyId::new(42).to_string());
    assert_eq!(standing.action, "every Event");
    assert_eq!(standing.capacity, 2);
    assert!(
        hub.act(id, Act::Pause, "ilian")
            .expect("said")
            .contains("already")
    );

    let audit = audited(&at);
    assert!(audit.contains("action = \"event.pause\""), "{audit}");
    assert!(audit.contains("\"by\" = \"ilian\""), "{audit}");
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn resuming_hands_over_what_queued_and_wakes_a_waiting_drain() {
    let at = directory("resume");
    let hub = Arc::new(hub());
    let subscription = hub
        .subscribe(subscriber(&at), Filter::everything(), 0)
        .expect("allowed");
    let id = subscription.id();
    hub.act(id, Act::Pause, "ilian").expect("paused");
    let first = received();
    hub.publish(first.clone());

    let (tell, told) = mpsc::channel();
    let waiting = std::thread::spawn(move || {
        let delivery = subscription.next(Duration::from_secs(5), 16);
        tell.send(Instant::now()).expect("told");
        (delivery, subscription)
    });
    std::thread::sleep(Duration::from_millis(20));
    let resumed_at = Instant::now();
    let said = hub.act(id, Act::Resume, "ilian").expect("resumed");
    assert!(said.contains("resumed by ilian"), "{said}");

    let woke = told.recv_timeout(Duration::from_secs(5)).expect("woke");
    let (delivery, subscription) = waiting.join().expect("joined");
    assert_eq!(delivery.events.len(), 1);
    assert_eq!(*delivery.events[0], first);
    assert!(
        woke.duration_since(resumed_at) < Duration::from_millis(1),
        "resume wakes the drain at once: {:?}",
        woke.duration_since(resumed_at)
    );
    assert_eq!(hub.standing(NODE)[0].delivered, 1);
    assert_eq!(hub.standing(NODE)[0].state, SubscriptionState::Active);
    assert!(audited(&at).contains("action = \"event.resume\""));
    drop(subscription);
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn removing_ends_it_its_holder_finds_it_closed_and_a_second_act_is_refused() {
    let at = directory("remove");
    let hub = hub();
    let subscription = hub
        .subscribe(subscriber(&at), Filter::everything(), 0)
        .expect("allowed");
    let id = subscription.id();

    let said = hub.act(id, Act::Remove, "ilian").expect("removed");
    assert!(said.contains("removed by ilian"), "{said}");
    assert_eq!(hub.subscriptions(), 0);
    assert!(hub.standing(NODE).is_empty());
    assert!(subscription.is_closed());
    assert_eq!(hub.publish(received()), 0, "nothing reaches it");
    assert!(
        subscription
            .next(Duration::from_secs(5), 1)
            .events
            .is_empty()
    );

    for act in Act::ALL {
        let refused = hub.act(id, act, "ilian").expect_err("gone");
        assert!(
            refused.to_string().starts_with("REFUSED: no subscription"),
            "{refused}"
        );
    }
    assert!(audited(&at).contains("action = \"event.remove\""));
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn removing_a_listened_subscription_ends_its_thread() {
    let at = directory("listened");
    let hub = hub();
    let subscription = hub
        .subscribe(subscriber(&at), Filter::everything(), 0)
        .expect("allowed");
    let id = subscription.id();
    let listener = subscription.listen(|_| {}).expect("listening");

    hub.act(id, Act::Remove, "ilian").expect("removed");
    let started = Instant::now();
    drop(listener);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the thread had ended"
    );
    let _ = fs::remove_dir_all(&at);
}
