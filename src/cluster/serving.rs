//! The answering side: a node's sync listener (ADR-0067), where every
//! other member of the cluster that has subscribers opens one link and
//! hears this node's own Events that its subscriptions' filters match.
//!
//! Every connection is mutual TLS through Xmip's own TLS (ADR-0063): the
//! peer presents a certificate reaching the cluster's anchors, so it is a
//! node of the cluster, and it is authorized as one; the Parties behind it
//! were authorized where they subscribed. The protocol is agreed in the
//! handshake as [`ALPN`], so a client that speaks something else is refused
//! before it says anything. A connection is two threads, no async runtime:
//! one reads what the peer wants, one writes what it wants as it is raised.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use xcore::Severity;

use crate::audit_trail::noted;
use crate::cluster::link::{self, ALPN, BEAT, Frame, HANDSHAKE, WATCH, Watched};
use crate::cluster::tap::Tap;
use crate::hub::Hub;

/// How many Events one write carries at most.
const BATCH: usize = 256;

/// How long a stop waits on its own wake-up connection.
const WAKE: Duration = Duration::from_secs(1);

/// The most a write waits on a node that does not read.
const WRITE: Duration = Duration::from_secs(10);

/// The threads serving connections, so a stop can wait for each.
type Served = Arc<Mutex<Vec<JoinHandle<()>>>>;

/// What every connection shares.
struct Serve {
    hub: Hub,
    config: Arc<tls::ServerConfig>,
    stopping: AtomicBool,
    audit: ProgramAudit,
}

/// The sync listener, serving until it is stopped.
pub(crate) struct Serving {
    address: SocketAddr,
    serve: Arc<Serve>,
    accepting: Option<JoinHandle<()>>,
    served: Served,
}

impl Serving {
    /// Serve `hub`'s own Events to every node connecting to `listener`
    /// with a certificate reaching `identity`'s anchors.
    pub(crate) fn start(
        hub: &Hub,
        listener: TcpListener,
        identity: &tls::Identity,
        audit: &ProgramAudit,
    ) -> Result<Self, String> {
        let config = tls::alpn::selecting(identity.server().map_err(|e| e.message)?, &[ALPN]);
        let address = listener.local_addr().map_err(|e| e.to_string())?;
        let serve = Arc::new(Serve {
            hub: hub.clone(),
            config: Arc::new(config),
            stopping: AtomicBool::new(false),
            audit: audit.clone(),
        });
        let served: Served = Arc::default();
        let accepting = {
            let (serve, served) = (Arc::clone(&serve), Arc::clone(&served));
            std::thread::Builder::new()
                .name("xmip-event-accept".to_string())
                .spawn(move || accept(&listener, &serve, &served))
                .map_err(|e| e.to_string())?
        };
        Ok(Self {
            address,
            serve,
            accepting: Some(accepting),
            served,
        })
    }

    pub(crate) const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Stop: nothing accepted after, every link ended, every thread
    /// finished.
    pub(crate) fn stop(&mut self) {
        if self.serve.stopping.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = net::connect(wakeable(self.address), Some(WAKE));
        if let Some(accepting) = self.accepting.take() {
            let _ = accepting.join();
        }
        let served =
            std::mem::take(&mut *self.served.lock().unwrap_or_else(PoisonError::into_inner));
        for thread in served {
            let _ = thread.join();
        }
    }
}

impl Drop for Serving {
    fn drop(&mut self) {
        self.stop();
    }
}

/// An address a connection reaches the listener at: loopback where it
/// listens on every address.
fn wakeable(address: SocketAddr) -> SocketAddr {
    match address.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => (Ipv4Addr::LOCALHOST, address.port()).into(),
        IpAddr::V6(ip) if ip.is_unspecified() => (Ipv6Addr::LOCALHOST, address.port()).into(),
        _ => address,
    }
}

fn accept(listener: &TcpListener, serve: &Arc<Serve>, served: &Served) {
    loop {
        // bounded: a sync listener waits as long as its node runs; a stop wakes it
        let accepted = listener.accept();
        if serve.stopping.load(Ordering::SeqCst) {
            return;
        }
        let Ok((connection, peer)) = accepted else {
            continue;
        };
        let serve = Arc::clone(serve);
        let Ok(thread) = std::thread::Builder::new()
            .name("xmip-event-link".to_string())
            .spawn(move || answer(connection, peer, &serve))
        else {
            continue;
        };
        let mut served = served.lock().unwrap_or_else(PoisonError::into_inner);
        served.retain(|thread| !thread.is_finished());
        served.push(thread);
    }
}

/// One link: the handshake, then what the peer wants read on this thread
/// and what it wants written on another, until either side ends.
fn answer(connection: TcpStream, peer: SocketAddr, serve: &Arc<Serve>) {
    let settled = connection
        .set_nodelay(true)
        .and_then(|()| connection.set_read_timeout(Some(HANDSHAKE)))
        .and_then(|()| connection.set_write_timeout(Some(WRITE)));
    let split = settled
        .map_err(|error| error.to_string())
        .and_then(|()| tls::server(connection, Arc::clone(&serve.config)).map_err(|e| e.message))
        .and_then(|guarded| tls::duplex::split(guarded).map_err(|e| e.message))
        .and_then(|split| {
            let watched = split.reading.socket().set_read_timeout(Some(WATCH));
            watched.map(|()| split).map_err(|error| error.to_string())
        });
    let split = match split {
        Ok(split) if split.agreed.as_deref() == Some(ALPN) => split,
        Ok(_) => {
            return said(
                serve,
                peer,
                Severity::Warning,
                "refused: not the Event link",
            );
        }
        Err(reason) => {
            return said(
                serve,
                peer,
                Severity::Warning,
                &format!("refused: {reason}"),
            );
        }
    };
    said(serve, peer, Severity::Information, "a node listens");

    let tap = serve.hub.tap();
    let out = Arc::new(Mutex::new(split.writing));
    let ending = Arc::new(AtomicBool::new(false));
    let writing = {
        let (tap, out, ending) = (Arc::clone(&tap), Arc::clone(&out), Arc::clone(&ending));
        std::thread::Builder::new()
            .name("xmip-event-link-write".to_string())
            .spawn(move || {
                let reason = carry(&tap, &out);
                ending.store(true, Ordering::SeqCst);
                out.lock().unwrap_or_else(PoisonError::into_inner).close();
                reason
            })
    };
    let reason = match writing {
        Ok(writing) => {
            let heard = listen(serve, &tap, &out, split.reading, &ending);
            ending.store(true, Ordering::SeqCst);
            serve.hub.untap(&tap);
            let wrote = writing.join().unwrap_or_default();
            if wrote.is_empty() { heard } else { wrote }
        }
        Err(error) => error.to_string(),
    };
    serve.hub.untap(&tap);
    said(
        serve,
        peer,
        Severity::Information,
        &format!("ended: {reason}"),
    );
}

/// Read what the peer wants until it, or this side, ends; why it ended.
fn listen<W: Write>(
    serve: &Serve,
    tap: &Tap,
    out: &Mutex<W>,
    reading: impl Read,
    ending: &AtomicBool,
) -> String {
    let mut watched = Watched {
        reading,
        ending: || serve.stopping.load(Ordering::SeqCst) || ending.load(Ordering::SeqCst),
        silence: None,
        heard: Instant::now(),
    };
    loop {
        match link::receive(&mut watched) {
            Ok(Some(Frame::Wants(generation, wants))) => {
                tap.want(wants);
                if let Err(reason) = write(out, &[Frame::Wanted(generation)]) {
                    return reason;
                }
            }
            Ok(Some(_)) => return "the node said what only an answering node says".to_string(),
            Ok(None) => return "the node closed the link".to_string(),
            Err(reason) => return reason,
        }
    }
}

/// Write what the tap queues, as it is queued, and what it refused; a beat
/// when there is nothing. Empty when the tap was closed, else why the
/// write failed.
fn carry<W: Write>(tap: &Tap, out: &Mutex<W>) -> String {
    loop {
        let delivery = tap.queue.take(BEAT, BATCH);
        if tap.queue.is_closed() {
            return String::new();
        }
        let mut frames: Vec<Frame> = delivery.events.into_iter().map(Frame::Event).collect();
        let missed = tap.take_missed();
        if !missed.is_empty() {
            frames.push(Frame::Missed(missed));
        }
        if frames.is_empty() {
            frames.push(Frame::Beat);
        }
        if let Err(reason) = write(out, &frames) {
            return reason;
        }
    }
}

/// `frames` in one write.
fn write<W: Write>(out: &Mutex<W>, frames: &[Frame]) -> Result<(), String> {
    let mut bytes = Vec::new();
    for frame in frames {
        frame
            .append(&mut bytes)
            .map_err(|error| error.to_string())?;
    }
    let mut out = out.lock().unwrap_or_else(PoisonError::into_inner);
    out.write_all(&bytes)
        .and_then(|()| out.flush())
        .map_err(|error| error.to_string())
}

/// One record of the link in the node's audit.
fn said(serve: &Serve, peer: SocketAddr, severity: Severity, message: &str) {
    let properties = BTreeMap::from([("peer".to_string(), peer.to_string())]);
    noted(&serve.audit, "event.link", severity, message, properties);
}
