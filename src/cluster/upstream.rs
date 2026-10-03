//! The asking side: one link from this node to one other member of the
//! cluster, held for as long as the member is followed, carrying this
//! node's subscriptions' filters down so only what they match crosses —
//! nothing, while there are none — and handing what crosses to them.
//!
//! The link is held even while nothing is asked: a subscription then costs
//! one round trip to every member, never a connect and a TLS handshake
//! each, and a member that is down is known to be before anybody asks.
//! What an idle link costs is one connection and a beat a second.
//!
//! The link is made over Xmip's mutual TLS (ADR-0063), presenting this
//! node's certificate: the node it reaches authorizes a node, and the
//! Parties were authorized here, where they subscribed. A member that
//! cannot be reached is tried again, sooner first and then less often, on
//! this link's own thread, so no other member waits for it; meanwhile it
//! is unheard, and said to be (`unheard.rs`, the node's audit).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use xcore::Severity;

use crate::audit_trail::noted;
use crate::cluster::Member;
use crate::cluster::link::{self, ALPN, BEAT, Frame, HANDSHAKE, SILENCE, WATCH, Watched};
use crate::hub::Hub;
use crate::signal::Signal;

/// The most a connect waits.
pub(crate) const CONNECT: Duration = Duration::from_secs(2);

/// The most a write waits on a node that does not read.
const WRITE: Duration = Duration::from_secs(10);

/// The first wait before a member that could not be reached is tried
/// again, doubled each time it still cannot be, up to [`RETRY_MOST`].
const RETRY_FIRST: Duration = Duration::from_millis(50);

/// The longest wait between two tries of a member that is down.
pub(crate) const RETRY_MOST: Duration = Duration::from_secs(1);

/// What follows a member: this node's hub, this node, what the link
/// presents, and the node's audit.
pub(crate) type Following<'a> = (
    &'a Hub,
    &'a str,
    &'a Arc<tls::ClientConfig>,
    &'a ProgramAudit,
);

/// One member's link, on its own thread.
pub(crate) struct Upstream {
    hub: Hub,
    pub(crate) link: u64,
    signal: Arc<Signal>,
    stopping: Arc<AtomicBool>,
    connecting: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

/// What a link's thread holds.
struct Ask {
    hub: Hub,
    /// This node, which does not hear the member when it is down.
    by: String,
    link: u64,
    member: Member,
    host: String,
    config: Arc<tls::ClientConfig>,
    signal: Arc<Signal>,
    stopping: Arc<AtomicBool>,
    /// In a connect, which nothing can cut short.
    connecting: Arc<AtomicBool>,
    audit: ProgramAudit,
}

/// How one attempt at the link ended.
enum Ended {
    /// It was never made.
    NotMade(String),
    /// It was made and broke, or this side ended it.
    Ended(String),
}

impl Upstream {
    /// Follow `member` from `hub`, presenting what `config` presents.
    pub(crate) fn start(following: Following<'_>, member: Member) -> Result<Self, String> {
        let (hub, by, config, audit) = following;
        let signal = Arc::new(Signal::default());
        hub.watch(&signal);
        let stopping = Arc::new(AtomicBool::new(false));
        let connecting = Arc::new(AtomicBool::new(false));
        let host = member
            .address
            .rsplit_once(':')
            .map(|(host, _)| host.trim_start_matches('[').trim_end_matches(']'))
            .ok_or_else(|| format!("'{}' names no port", member.address))?
            .to_string();
        let link = hub.inner.carriers.join();
        let ask = Ask {
            hub: hub.clone(),
            by: by.to_string(),
            link,
            member,
            host,
            config: Arc::clone(config),
            signal: Arc::clone(&signal),
            stopping: Arc::clone(&stopping),
            connecting: Arc::clone(&connecting),
            audit: audit.clone(),
        };
        let thread = std::thread::Builder::new()
            .name("xmip-event-upstream".to_string())
            .spawn(move || ask.run())
            .map_err(|error| {
                hub.inner.carriers.leave(link);
                error.to_string()
            })?;
        Ok(Self {
            hub: hub.clone(),
            link,
            signal,
            stopping,
            connecting,
            thread: Some(thread),
        })
    }

    /// End the link and its thread. A thread in a connect to a member
    /// that does not answer — which on Windows waits out its whole bound
    /// even when refused — is not waited for: it ends when the connect
    /// does, and does nothing more.
    pub(crate) fn stop(&mut self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.signal.raise();
        if let Some(thread) = self.thread.take()
            && !self.connecting.load(Ordering::SeqCst)
        {
            let _ = thread.join();
        }
        self.hub.inner.carriers.leave(self.link);
    }
}

impl Drop for Upstream {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Ask {
    fn run(&self) {
        let mut retry = RETRY_FIRST;
        while !self.stopping.load(Ordering::SeqCst) {
            let seen = self.signal.now();
            match self.link() {
                Ended::NotMade(reason) => {
                    self.unheard(&reason);
                    self.signal.wait(seen, retry);
                    retry = (retry * 2).min(RETRY_MOST);
                }
                Ended::Ended(reason) => {
                    retry = RETRY_FIRST;
                    if !self.stopping.load(Ordering::SeqCst) {
                        self.unheard(&reason);
                        self.signal.wait(self.signal.now(), RETRY_FIRST);
                    }
                }
            }
        }
        // No longer followed: a member that left is not missing.
        self.hub.heard(&self.member.node);
    }

    /// Make the link, carry the filters, and hand on what crosses, until
    /// it breaks or the member is no longer followed.
    fn link(&self) -> Ended {
        self.connecting.store(true, Ordering::SeqCst);
        let connected = net::connect(self.member.address.as_str(), Some(CONNECT));
        self.connecting.store(false, Ordering::SeqCst);
        if self.stopping.load(Ordering::SeqCst) {
            return Ended::Ended("this node stopped following it".to_string());
        }
        let made = connected
            .map_err(|failed| failed.to_string())
            .and_then(|tcp| {
                tcp.set_nodelay(true)
                    .and_then(|()| tcp.set_read_timeout(Some(HANDSHAKE)))
                    .and_then(|()| tcp.set_write_timeout(Some(WRITE)))
                    .map_err(|error| error.to_string())?;
                tls::client_with(&self.host, tcp, Arc::clone(&self.config)).map_err(|e| e.message)
            })
            .and_then(|guarded| tls::duplex::split(guarded).map_err(|e| e.message))
            .and_then(|split| {
                let watched = split.reading.socket().set_read_timeout(Some(WATCH));
                watched.map(|()| split).map_err(|error| error.to_string())
            });
        let split = match made {
            Ok(split) if split.agreed.as_deref() == Some(ALPN) => split,
            Ok(_) => return Ended::NotMade("the node did not agree the Event link".to_string()),
            Err(reason) => return Ended::NotMade(reason),
        };
        let (carriers, link) = (&self.hub.inner.carriers, self.link);
        carriers.made(link);
        let closing = Arc::new(AtomicBool::new(false));
        let broke: Arc<Mutex<Option<String>>> = Arc::default();
        let reading = {
            let (closing, broke) = (Arc::clone(&closing), Arc::clone(&broke));
            let (hub, signal, stopping) = (
                self.hub.clone(),
                Arc::clone(&self.signal),
                Arc::clone(&self.stopping),
            );
            let (node, audit) = (self.member.node.clone(), self.audit.clone());
            let reading = split.reading;
            std::thread::Builder::new()
                .name("xmip-event-upstream-read".to_string())
                .spawn(move || {
                    let mut watched = Watched {
                        reading,
                        ending: || {
                            closing.load(Ordering::SeqCst) || stopping.load(Ordering::SeqCst)
                        },
                        silence: Some(SILENCE),
                        heard: Instant::now(),
                    };
                    let reason = hear(&hub, link, &node, &audit, &mut watched);
                    *broke.lock().unwrap_or_else(PoisonError::into_inner) = Some(reason);
                    signal.raise();
                })
        };
        let Ok(reading) = reading else {
            return Ended::NotMade("the link's reading thread did not start".to_string());
        };

        let mut out = split.writing;
        let mut asked = None;
        let reason = loop {
            let seen = self.signal.now();
            if self.stopping.load(Ordering::SeqCst) {
                break "this node stopped following it".to_string();
            }
            if let Some(reason) = broke.lock().unwrap_or_else(PoisonError::into_inner).take() {
                break reason;
            }
            let (generation, wants) = self.hub.wants();
            if asked != Some(generation) {
                let mut bytes = Vec::new();
                let written = Frame::Wants(generation, wants)
                    .append(&mut bytes)
                    .map_err(|error| error.to_string())
                    .and_then(|()| {
                        std::io::Write::write_all(&mut out, &bytes)
                            .and_then(|()| std::io::Write::flush(&mut out))
                            .map_err(|error| error.to_string())
                    });
                if let Err(reason) = written {
                    break reason;
                }
                asked = Some(generation);
            }
            self.signal.wait(seen, BEAT);
        };
        closing.store(true, Ordering::SeqCst);
        out.close();
        let _ = reading.join();
        Ended::Ended(reason)
    }

    /// The member is not heard, for `reason`: said once, when it stops
    /// being heard.
    fn unheard(&self, reason: &str) {
        self.hub.inner.carriers.unreachable(self.link);
        if self.hub.not_heard(&self.by, &self.member.node, reason) {
            said(
                &self.audit,
                &self.member.node,
                Severity::Warning,
                &format!("unheard: {reason}"),
            );
        }
    }
}

/// Read what the member sends until the link ends: its Events to this
/// node's subscriptions, what it refused counted on them, and how far it
/// carries their filters. Why it ended.
fn hear(
    hub: &Hub,
    link: u64,
    node: &str,
    audit: &ProgramAudit,
    reading: &mut impl std::io::Read,
) -> String {
    loop {
        match link::receive(reading) {
            Ok(Some(Frame::Event(event))) => {
                hub.deliver(&event);
            }
            Ok(Some(Frame::Wanted(generation))) => {
                hub.inner.carriers.carried(link, generation);
                if hub.heard(node) {
                    said(audit, node, Severity::Information, "heard again");
                }
            }
            Ok(Some(Frame::Missed(missed))) => {
                for (id, count) in missed {
                    if let Some(slot) = hub.slot(id) {
                        slot.queue.refused_elsewhere(count);
                    }
                }
            }
            Ok(Some(Frame::Beat)) => {}
            Ok(Some(Frame::Wants(..))) => {
                return "the node asked what only an asking node asks".to_string();
            }
            Ok(None) => return "the node closed the link".to_string(),
            Err(reason) => return reason,
        }
    }
}

/// One record of the link in the node's audit.
fn said(audit: &ProgramAudit, node: &str, severity: Severity, message: &str) {
    let properties = BTreeMap::from([("node".to_string(), node.to_string())]);
    noted(audit, "event.link", severity, message, properties);
}
