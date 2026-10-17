//! What crossing to another node adds to an Event (the millisecond rule):
//! raised on the second node, received by a subscriber waiting on the
//! first — the link's queue on the second, its writing thread, Xmip's TLS
//! on loopback, the reading thread on the first, the subscriber's queue —
//! measured from publish to receipt, and the
//! same Event published to a subscriber on its own node beside it, so the
//! difference is what the hop adds. The distribution is printed, because a
//! mean hides the tail.
//!
//! Two things are held, each as the fastest round in every five
//! (`xcore::latency`): a poll or a busy loop is in every round, a wake that
//! waited its turn for a processor in some only. **The wake is a wake**: the
//! crossing is under the millisecond plus what the machine takes to wake a
//! thread on a plain channel, three times — the hop wakes three threads
//! where the node's own delivery wakes one — measured round by round beside
//! it under the same load. **Xmip's own work is under the millisecond**:
//! this process's processor time across a crossing — both nodes, the link's
//! threads and Xmip's TLS (`xcore::processor_time`) — is under the
//! millisecond plus the process's processor time across the plain wake,
//! which counts what the nodes' own threads run beside it. The median and
//! the tail are printed and held by nothing: on a machine busy on every
//! processor they measure the scheduler.

#[path = "cluster/test_cluster.rs"]
mod test_cluster;

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use test_cluster::{Authority, Roster, TestNode, failures, name};
use xcore::latency::{PlainWake, spread};
use xcore::processor_time::ProcessorTime;
use xmip_core_event::outcome::Outcome;

/// How many Events are measured.
const ROUNDS: usize = 500;

/// The bound: about a millisecond.
const BOUND: Duration = Duration::from_millis(1);

#[test]
fn an_event_crossing_to_another_node_adds_well_under_a_millisecond() {
    let (authority, roster) = (Authority::new(), Arc::new(Roster::default()));
    let one = TestNode::join(&name(0), &authority, &roster);
    let two = TestNode::join(&name(1), &authority, &roster);
    TestNode::follow(&[&one, &two]);
    let across = one.subscribe(1, failures());
    let here = two.subscribe(2, failures());
    let plain = PlainWake::start();

    let (mut crossed, mut own, mut worked) = (Vec::new(), Vec::new(), Vec::new());
    let (mut woke, mut idle) = (Vec::new(), Vec::new());
    for _ in 0..ROUNDS {
        thread::sleep(Duration::from_micros(200));
        let from = ProcessorTime::process();
        let sent = Instant::now();
        two.raise(Outcome::Failure);
        let delivered = here.next(Duration::from_secs(5), 1);
        own.push(sent.elapsed());
        let heard = across.next(Duration::from_secs(5), 1);
        crossed.push(sent.elapsed());
        worked.push(from.elapsed());
        assert_eq!((delivered.events.len(), heard.events.len()), (1, 1));
        let (plain_wake, plain_work) = plain.round();
        woke.push(plain_wake);
        idle.push(plain_work);
    }

    let crossed = spread("raised on the second to a subscriber on the first", crossed);
    let own = spread("raised on the second to a subscriber on itself", own);
    let usual = spread("a plain channel beside it", woke).fastest_of_five;
    let worked = spread("the process's processor time for a crossing", worked);
    let idle = spread("the process's processor time for a plain wake", idle);
    println!(
        "the hop adds about {:?}, the fastest of five",
        crossed.fastest_of_five.saturating_sub(own.fastest_of_five)
    );
    assert!(
        crossed.fastest_of_five < BOUND + usual * 3,
        "the fastest of five {:?}, the machine's own {usual:?}",
        crossed.fastest_of_five
    );
    assert!(
        worked.fastest_of_five < BOUND + idle.fastest_of_five,
        "the process's processor time for a crossing {:?}, for a plain wake {:?}",
        worked.fastest_of_five,
        idle.fastest_of_five
    );
}
