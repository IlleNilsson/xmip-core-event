//! What crossing to another node adds to an Event (the millisecond rule):
//! raised on the second node, received by a subscriber waiting on the
//! first — the link's queue on the second, its writing thread, Xmip's TLS
//! on loopback, the reading thread on the first, the subscriber's queue —
//! measured from publish to receipt, and the
//! same Event published to a subscriber on its own node beside it, so the
//! difference is what the hop adds. The distribution is printed, because a
//! mean hides the tail.
//!
//! Each is held to the millisecond plus what the machine takes to wake a
//! thread on a plain channel, three times — the hop wakes three threads
//! where the node's own delivery wakes one — measured beside it under the
//! same load: the rule is a millisecond *apart from load*.

#[path = "cluster/test_cluster.rs"]
mod test_cluster;

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use test_cluster::{Authority, Roster, TestNode, failures, name};
use xmip_core_event::outcome::Outcome;

/// How many Events are measured.
const ROUNDS: usize = 500;

/// The bound: about a millisecond.
const BOUND: Duration = Duration::from_millis(1);

/// The median and the 99th percentile, printed.
fn spread(what: &str, mut taken: Vec<Duration>) -> (Duration, Duration) {
    taken.sort();
    let at = |percent: usize| taken[(taken.len() - 1) * percent / 100];
    println!(
        "{what}: median {:?}, p99 {:?}, worst {:?} over {}",
        at(50),
        at(99),
        at(100),
        taken.len()
    );
    (at(50), at(99))
}

/// A plain channel's wake, `ROUNDS` times, beside the Events.
fn plain_wake() -> (Duration, Duration) {
    let (tell, told) = mpsc::channel::<Instant>();
    let waiter = thread::spawn(move || told.iter().map(|sent| sent.elapsed()).collect());
    for _ in 0..ROUNDS {
        tell.send(Instant::now()).expect("the waiter waits");
        thread::sleep(Duration::from_micros(200));
    }
    drop(tell);
    spread("a plain channel beside it", waiter.join().expect("woken"))
}

#[test]
fn an_event_crossing_to_another_node_adds_well_under_a_millisecond() {
    let (authority, roster) = (Authority::new(), Arc::new(Roster::default()));
    let one = TestNode::join(&name(0), &authority, &roster);
    let two = TestNode::join(&name(1), &authority, &roster);
    TestNode::follow(&[&one, &two]);
    let across = one.subscribe(1, failures());
    let here = two.subscribe(2, failures());

    let (mut crossed, mut own) = (Vec::new(), Vec::new());
    for _ in 0..ROUNDS {
        thread::sleep(Duration::from_micros(200));
        let sent = Instant::now();
        two.raise(Outcome::Failure);
        let delivered = here.next(Duration::from_secs(5), 1);
        own.push(sent.elapsed());
        let heard = across.next(Duration::from_secs(5), 1);
        crossed.push(sent.elapsed());
        assert_eq!((delivered.events.len(), heard.events.len()), (1, 1));
    }

    let (median, p99) = spread("raised on the second to a subscriber on the first", crossed);
    let (own_median, _) = spread("raised on the second to a subscriber on itself", own);
    let (usual, tail) = plain_wake();
    println!(
        "the hop adds about {:?} at the median",
        median.saturating_sub(own_median)
    );
    assert!(
        median < BOUND + usual * 3,
        "median {median:?}, the machine's own {usual:?}"
    );
    assert!(
        p99 < BOUND * 2 + tail * 3,
        "p99 {p99:?}, the machine's own {tail:?}"
    );
}
