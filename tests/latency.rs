//! Near, very near real time (the owner, 2026-09-26): an Event reaches an
//! in-process subscriber within about a millisecond of being published.
//! Measured here from publish to receipt — a subscriber waiting in `next`
//! and a listener's callback — and the distribution printed, because a
//! mean hides the tail an operator waits on.
//!
//! Each is held to the bound plus what the operating system takes to wake a
//! thread on a plain channel, measured beside it under the same load: the
//! rule is a millisecond *apart from load*, and on a machine busy compiling
//! that wake alone takes milliseconds. On a quiet machine the plain channel
//! is tens of microseconds and the bound is the millisecond itself.

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

/// The median, the 99th percentile and the worst, printed and returned.
fn spread(what: &str, mut taken: Vec<Duration>) -> (Duration, Duration, Duration) {
    taken.sort();
    let at = |percent: usize| taken[(taken.len() - 1) * percent / 100];
    let (median, p99, worst) = (at(50), at(99), at(100));
    println!("{what}: median {median:?}, p99 {p99:?}, worst {worst:?} over {ROUNDS}");
    (median, p99, worst)
}

/// A thread woken on a plain channel, beside the Events: the machine's own
/// wake-up, under whatever load the Events are under.
struct Control {
    tell: mpsc::Sender<Instant>,
    waiter: thread::JoinHandle<Vec<Duration>>,
}

impl Control {
    fn start() -> Self {
        let (tell, told) = mpsc::channel::<Instant>();
        let waiter = thread::spawn(move || {
            told.iter()
                .map(|sent| sent.elapsed())
                .collect::<Vec<Duration>>()
        });
        Self { tell, waiter }
    }

    /// The machine's own median and 99th percentile.
    fn load(self) -> (Duration, Duration) {
        drop(self.tell);
        let (median, p99, _) = spread(
            "a plain channel beside it",
            self.waiter.join().expect("woken"),
        );
        (median, p99)
    }
}

fn spin(pause: Duration) {
    let started = Instant::now();
    while started.elapsed() < pause {
        std::hint::spin_loop();
    }
}

/// Publish `ROUNDS` Events a little apart, each stamped as it goes, and
/// wake the control thread between them.
fn publish_apart(hub: &Hub, sent: &Mutex<Vec<Instant>>, control: &Control) {
    let node = configure::fixture::test_cluster().node_scope(0);
    for _ in 0..ROUNDS {
        spin(Duration::from_micros(200));
        let event = Event::completed(Stage::Receive, Outcome::Success, node.as_str());
        sent.lock().expect("sent").push(Instant::now());
        hub.publish(event);
        spin(Duration::from_micros(100));
        control
            .tell
            .send(Instant::now())
            .expect("the control waits");
    }
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
            while taken.len() < ROUNDS {
                let delivery = subscription.next(Duration::from_secs(5), usize::MAX);
                let now = Instant::now();
                let sent = sent.lock().expect("sent");
                for index in taken.len()..taken.len() + delivery.events.len() {
                    taken.push(now - sent[index]);
                }
            }
            taken
        })
    };
    go.recv().expect("the receiver waits");
    let control = Control::start();
    publish_apart(&hub, &sent, &control);

    let (median, p99, _) = spread("publish to next", receiver.join().expect("received"));
    let (usual, tail) = control.load();
    assert!(
        median < BOUND + usual,
        "median {median:?}, the machine's own {usual:?}"
    );
    assert!(
        p99 < BOUND + tail,
        "p99 {p99:?}, the machine's own {tail:?}"
    );
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
        hub.subscribe(subscriber(&at), Filter::everything(), 0)
            .expect("allowed")
            .listen(move |_| {
                let now = Instant::now();
                let _ = tell.send(now - sent.lock().expect("sent")[seen]);
                seen += 1;
            })
            .expect("a thread")
    };

    let control = Control::start();
    publish_apart(&hub, &sent, &control);
    let taken: Vec<Duration> = (0..ROUNDS)
        .map(|_| {
            told.recv_timeout(Duration::from_secs(5))
                .expect("called back")
        })
        .collect();
    drop(listener);

    let (median, p99, _) = spread("publish to callback", taken);
    let (usual, tail) = control.load();
    assert!(
        median < BOUND + usual,
        "median {median:?}, the machine's own {usual:?}"
    );
    assert!(
        p99 < BOUND + tail,
        "p99 {p99:?}, the machine's own {tail:?}"
    );
    let _ = fs::remove_dir_all(&at);
}
