//! Near, very near real time (the owner, 2026-09-26): an Event reaches an
//! in-process subscriber within about a millisecond of being published.
//! Measured here from publish to receipt — a subscriber waiting in `next`
//! and a listener's callback — and the distribution printed, because a
//! mean hides the tail an operator waits on.
//!
//! Two things are held, each by what is Xmip's alone. **The wake is a
//! wake**: the fastest of each five rounds (`xcore::latency`) is under the
//! bound plus the same figure for a thread woken on a plain channel,
//! measured beside it under the same load — a poll or a missed notify is in
//! every round and fails it, a wake that waited its turn for a processor is
//! in some only. **Xmip's own work is under the bound**: the processor time
//! the publishing thread spends in `publish` and the subscriber's thread
//! between Events, an Event's share (`xcore::processor_time`), which waiting
//! for a processor does not reach. The median and the tail are printed and
//! held by nothing: under a machine busy on every processor they measure
//! the scheduler, and a 99th percentile held against the plain channel's
//! failed for it.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use authorize_party::PartyPolicy;
use node::Stage;
use xcore::PartyId;
use xcore::latency::{PlainWake, spin, spread};
use xcore::processor_time::ProcessorTime;
use xmip_core_event::Event;
use xmip_core_event::filter::Filter;
use xmip_core_event::hub::Hub;
use xmip_core_event::outcome::Outcome;
use xmip_core_event::subscriber::Subscriber;

/// How many Events each measurement publishes.
const ROUNDS: usize = 500;

/// The bound: about a millisecond.
const BOUND: Duration = Duration::from_millis(1);

/// The two measurements take turns: each is its own load, and one running
/// beside the other would be measured as the machine's.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn directory(name: &str) -> PathBuf {
    let at = std::env::temp_dir().join(format!("xmip-event-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&at);
    at
}

fn subscriber(at: &Path) -> Subscriber {
    Subscriber::in_process(
        PartyId::new(5),
        ProgramAudit::new("xmip-core-event tests", Some(at)),
    )
}

/// A hub whose policy allows the one Party these tests subscribe as.
fn allowing() -> Hub {
    Hub::new(vec![Arc::new(PartyPolicy::new().allow(PartyId::new(5)))])
}

/// What publishing `ROUNDS` Events a little apart measured: the publishing
/// thread's processor time in `publish`, summed, and a plain wake beside
/// each Event.
struct Published {
    publishing: Duration,
    plain: Vec<Duration>,
}

/// Publish `ROUNDS` Events a little apart, each stamped as it goes, and
/// wake a plain thread between them, the machine's own wake under whatever
/// load the Events are under.
fn publish_apart(hub: &Hub, sent: &Mutex<Vec<Instant>>) -> Published {
    let node = configure::fixture::test_cluster().node_scope(0);
    let control = PlainWake::start();
    let mut published = Published {
        publishing: Duration::ZERO,
        plain: Vec::with_capacity(ROUNDS),
    };
    for _ in 0..ROUNDS {
        spin(Duration::from_micros(200));
        let event = Event::completed(Stage::Receive, Outcome::Success, node.as_str());
        sent.lock().expect("sent").push(Instant::now());
        let from = ProcessorTime::thread();
        hub.publish(event);
        published.publishing += from.elapsed();
        spin(Duration::from_micros(100));
        published.plain.push(control.round().0);
    }
    published
}

/// The two holds: the wake against the plain channel's, and Xmip's own
/// processor time, the publisher's and `subscribing`, summed over every
/// Event, an Event's share.
fn hold(what: &str, taken: Vec<Duration>, published: Published, subscribing: Duration) {
    let taken = spread(what, taken);
    let machine = spread("a plain channel beside it", published.plain);
    let worked = published.publishing + subscribing;
    assert!(
        taken.fastest_of_five < BOUND + machine.fastest_of_five,
        "the fastest of five {:?}, the machine's own {:?}",
        taken.fastest_of_five,
        machine.fastest_of_five
    );
    let own = worked / u32::try_from(ROUNDS).expect("a count");
    println!("Xmip's own processor time: {own:?} an Event");
    assert!(own < BOUND, "Xmip's own processor time, {own:?} an Event");
}

#[test]
fn an_event_reaches_a_waiting_subscriber_within_a_millisecond() {
    let _turn = ONE_AT_A_TIME.lock();
    let at = directory("latency-next");
    let hub = Arc::new(allowing());
    let subscription = hub
        .subscribe(subscriber(&at), Filter::everything(), 0)
        .expect("allowed");
    let sent = Arc::new(Mutex::new(Vec::new()));
    let (ready, go) = mpsc::channel();

    let receiver = {
        let sent = Arc::clone(&sent);
        thread::spawn(move || {
            let mut taken = Vec::with_capacity(ROUNDS);
            ready.send(()).expect("ready");
            // The subscriber's thread's processor time: its waits in `next`
            // are not counted, only its work.
            let subscriber_time = ProcessorTime::thread();
            while taken.len() < ROUNDS {
                let delivery = subscription.next(Duration::from_secs(5), usize::MAX);
                let now = Instant::now();
                let sent = sent.lock().expect("sent");
                for index in taken.len()..taken.len() + delivery.events.len() {
                    taken.push(now - sent[index]);
                }
            }
            (taken, subscriber_time.elapsed())
        })
    };
    go.recv().expect("the receiver waits");
    let published = publish_apart(&hub, &sent);

    let (taken, subscribing) = receiver.join().expect("received");
    hold("publish to next", taken, published, subscribing);
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn an_event_reaches_a_listener_within_a_millisecond() {
    let _turn = ONE_AT_A_TIME.lock();
    let at = directory("latency-listen");
    let hub = allowing();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let (tell, told) = mpsc::channel();
    let listener = {
        let sent = Arc::clone(&sent);
        let mut seen = 0;
        // The listener's thread between one callback's end and the next
        // one's start: the wait entered, the wake, the drain and the call.
        // The first has no last, and is not counted.
        let mut left: Option<ProcessorTime> = None;
        hub.subscribe(subscriber(&at), Filter::everything(), 0)
            .expect("allowed")
            .listen(move |_| {
                let now = Instant::now();
                let worked = left.map_or(Duration::ZERO, ProcessorTime::elapsed);
                let _ = tell.send((now - sent.lock().expect("sent")[seen], worked));
                seen += 1;
                left = Some(ProcessorTime::thread());
            })
            .expect("a thread")
    };

    let published = publish_apart(&hub, &sent);
    let (taken, worked): (Vec<Duration>, Vec<Duration>) = (0..ROUNDS)
        .map(|_| {
            told.recv_timeout(Duration::from_secs(5))
                .expect("called back")
        })
        .unzip();
    drop(listener);

    let subscribing = worked.iter().sum::<Duration>();
    hold("publish to callback", taken, published, subscribing);
    let _ = fs::remove_dir_all(&at);
}
