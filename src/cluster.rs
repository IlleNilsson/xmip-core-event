//! Any node is the cluster's door (ADR-0065, amendments 2026-09-26 and
//! 2026-10-02): a subscriber on whichever node it reaches receives the
//! matching Events of every node in the cluster, exactly as it would from
//! any other.
//!
//! **One hop, pushed-down filters, no loops.** A node holds one link to
//! each other member of the cluster
//! (`upstream.rs`) over Xmip's mutual TLS (ADR-0063), on the member's sync
//! listener (ADR-0067), and asks it for what its subscriptions' filters
//! match. The member answers from its own Events only (`tap.rs`): what it
//! heard from a third node never crosses again, so every Event crosses at
//! most one hop, comes once, and never returns. What no subscriber's filter
//! wants never leaves the node that raised it. An Event that crosses is
//! pushed the moment it is raised — no node looks for Events, nothing
//! waits a round to send — and it is handed to the subscriptions it
//! matches by the same rule, [`crate::filter::Filter::matches`].
//!
//! **Authorized where it subscribed.** A Party is authorized by the node it
//! connected to, at subscribe, as every subscriber is; a link presents the
//! node's own certificate and the member authorizes a node of the cluster,
//! never a Party.
//!
//! **Followed, retried, said.** Which nodes are members, and where their
//! sync listeners answer, is the cluster's record ([`Membership`]); a
//! member joining gets a link, a member leaving loses it. A member that is
//! down is tried again on its own link's thread, so no other member waits
//! for it, and until it is heard again it is unheard — answered by
//! [`crate::hub::Hub::unheard`] and by each subscription, and recorded in
//! the node's audit as `event.link` — so no Event is missing silently.
//!
//! **Internal.** A link is the cluster's, not a Party's: it is not an Event
//! subscription, is not listed with them and is not paused, resumed or
//! removed by an operator, who would otherwise silence a node for every
//! subscriber at once.

pub(crate) mod carriers;
pub mod link;
mod serving;
pub(crate) mod tap;
pub mod unheard;
mod upstream;

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use audit::program_audit::ProgramAudit;
use xcore::Severity;

use crate::EventError;
use crate::audit_trail::noted;
use crate::hub::Hub;
use crate::signal::Signal;
use serving::Serving;
use upstream::Upstream;

/// One member of the cluster, as a link reaches it.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Member {
    /// The node: `xmip:///<cluster>/node/<name>`.
    pub node: String,
    /// Where its sync listener answers: `host:port`, the host the name its
    /// certificate is issued for.
    pub address: String,
}

/// Where the cluster keeps its members: Xmip Storage's administration
/// database, in a node (`deployment-model.md` section 7).
pub trait Membership: Send + Sync {
    /// Every member now; this node among them or not.
    ///
    /// # Errors
    /// The record could not be read; the members known before are kept.
    fn members(&self) -> Result<Vec<Member>, EventError>;
}

/// How a node takes part in the cluster's Events.
pub struct Joining {
    /// This node: `xmip:///<cluster>/node/<name>`.
    pub node: String,
    /// Its sync listener, bound.
    pub listener: TcpListener,
    /// What it presents to the other nodes, and the anchors their
    /// certificates must reach.
    pub identity: tls::Identity,
    pub membership: Arc<dyn Membership>,
    /// How often the members are read again when nothing says they
    /// changed; [`Cluster::follow`] reads them at once.
    pub follow_every: Duration,
    /// Where the links are audited: the node's own audit.
    pub audit: ProgramAudit,
}

/// A node's part in the cluster's Events: its sync listener answering the
/// other members, and its links to them.
pub struct Cluster {
    address: SocketAddr,
    serving: Serving,
    following: Arc<Follow>,
    thread: Option<JoinHandle<()>>,
}

/// What the following thread holds.
struct Follow {
    hub: Hub,
    node: String,
    membership: Arc<dyn Membership>,
    config: Arc<tls::ClientConfig>,
    every: Duration,
    audit: ProgramAudit,
    signal: Signal,
    stopping: AtomicBool,
    upstreams: Mutex<BTreeMap<String, (Member, Upstream)>>,
}

impl Cluster {
    /// `hub`'s node joins the cluster: its own Events are served on the
    /// sync listener, and it follows the members.
    ///
    /// # Errors
    /// The identity makes no TLS configuration, or a thread did not start.
    pub fn join(hub: &Hub, joining: Joining) -> Result<Self, EventError> {
        let refused = |reason: String| EventError::new(format!("joining the cluster: {reason}"));
        let client = joining.identity.client().map_err(|e| refused(e.message))?;
        let config = Arc::new(tls::alpn::offering(client, &[link::ALPN]));
        let serving = Serving::start(hub, joining.listener, &joining.identity, &joining.audit)
            .map_err(refused)?;
        let following = Arc::new(Follow {
            hub: hub.clone(),
            node: joining.node,
            membership: joining.membership,
            config,
            every: joining.follow_every,
            audit: joining.audit,
            signal: Signal::default(),
            stopping: AtomicBool::new(false),
            upstreams: Mutex::new(BTreeMap::new()),
        });
        let thread = {
            let following = Arc::clone(&following);
            std::thread::Builder::new()
                .name("xmip-event-cluster".to_string())
                .spawn(move || following.run())
                .map_err(|error| refused(error.to_string()))?
        };
        Ok(Self {
            address: serving.address(),
            serving,
            following,
            thread: Some(thread),
        })
    }

    /// Where its sync listener answers.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Read the members again now, on this thread: what a node calls when
    /// the cluster's record says they changed. When it returns, every
    /// member is linked or said to be unheard, and a subscribe made after
    /// waits one round trip.
    pub fn follow(&self) {
        self.following.read();
    }

    /// The members followed now, this node apart.
    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        self.following
            .upstreams
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|(member, _)| member.clone())
            .collect()
    }

    /// Leave: every link ended, the sync listener closed, every thread
    /// finished but one still in a connect, which ends with it.
    pub fn stop(mut self) {
        self.halt();
    }

    fn halt(&mut self) {
        self.following.stopping.store(true, Ordering::SeqCst);
        self.following.signal.raise();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.serving.stop();
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.halt();
    }
}

impl Follow {
    fn run(&self) {
        loop {
            let seen = self.signal.now();
            if self.stopping.load(Ordering::SeqCst) {
                break;
            }
            self.read();
            self.signal.wait(seen, self.every);
        }
        let upstreams = std::mem::take(
            &mut *self
                .upstreams
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        drop(upstreams);
    }

    /// Read the members and follow them.
    fn read(&self) {
        match self.membership.members() {
            Ok(members) => self.reconcile(members),
            Err(error) => self.said(
                Severity::Warning,
                &format!("the members could not be read, those known are kept: {error}"),
            ),
        }
    }

    /// A link to every other member, and none to a node that left.
    fn reconcile(&self, members: Vec<Member>) {
        let wanted: BTreeMap<String, Member> = members
            .into_iter()
            .filter(|member| member.node != self.node)
            .map(|member| (member.node.clone(), member))
            .collect();
        let mut upstreams = self
            .upstreams
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let gone: Vec<String> = upstreams
            .iter()
            .filter(|(node, (member, _))| wanted.get(*node) != Some(member))
            .map(|(node, _)| node.clone())
            .collect();
        for node in gone {
            upstreams.remove(&node);
            self.said(Severity::Information, &format!("{node} left, or moved"));
        }
        let mut started = Vec::new();
        for (node, member) in wanted {
            if upstreams.contains_key(&node) {
                continue;
            }
            let following = (&self.hub, self.node.as_str(), &self.config, &self.audit);
            match Upstream::start(following, member.clone()) {
                Ok(upstream) => {
                    started.push(upstream.link);
                    upstreams.insert(node.clone(), (member, upstream));
                    self.said(Severity::Information, &format!("{node} followed"));
                }
                Err(reason) => {
                    self.hub.not_heard(&self.node, &node, &reason);
                    self.said(
                        Severity::Warning,
                        &format!("{node} cannot be followed: {reason}"),
                    );
                }
            }
        }
        drop(upstreams);
        // A member followed is linked, or said unheard, before this ends.
        self.hub
            .inner
            .carriers
            .made_all(&started, upstream::CONNECT + link::HANDSHAKE);
    }

    fn said(&self, severity: Severity, message: &str) {
        let properties = BTreeMap::from([("node".to_string(), self.node.clone())]);
        noted(&self.audit, "event.cluster", severity, message, properties);
    }
}
