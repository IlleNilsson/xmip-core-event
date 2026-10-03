//! A test cluster: one authority, a certificate per node issued by it for
//! `localhost`, a roster of members a test changes as nodes join and leave,
//! and a node — a hub joined to the cluster on a sync listener of its own.
//! The cluster and its nodes are named by the test cluster's `xmip.toml`,
//! through configure's fixture, as every test run is.

#![allow(dead_code)]

use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use authorize_party::PartyPolicy;
use node::Stage;
use xcore::PartyId;
use xmip_core_event::cluster::{Cluster, Joining, Member, Membership};
use xmip_core_event::filter::Filter;
use xmip_core_event::hub::{EventSubscription, Hub};
use xmip_core_event::outcome::Outcome;
use xmip_core_event::subscriber::Subscriber;
use xmip_core_event::{Event, EventError};

/// The test cluster, read once: a node's scope is asked for on every raise.
fn test_cluster() -> &'static configure::fixture::TestCluster {
    static CLUSTER: std::sync::OnceLock<configure::fixture::TestCluster> =
        std::sync::OnceLock::new();
    CLUSTER.get_or_init(configure::fixture::test_cluster)
}

/// The scope of the cluster every test node is in.
#[must_use]
pub fn cluster() -> String {
    test_cluster().scope()
}

/// The name of the test cluster's node at `place`.
#[must_use]
pub fn name(place: usize) -> String {
    test_cluster().node(place).name.clone()
}

/// A node's scope in the cluster.
#[must_use]
pub fn node(name: &str) -> String {
    format!("{}/node/{name}", cluster())
}

/// The cluster's authority, as PEM, and what issues with it.
pub struct Authority {
    pem: String,
    issuer: rcgen::CertifiedKey,
}

impl Authority {
    #[must_use]
    pub fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().expect("a key");
        let cert = params.self_signed(&key).expect("self-signed");
        Self {
            pem: cert.pem(),
            issuer: rcgen::CertifiedKey {
                cert,
                key_pair: key,
            },
        }
    }

    /// A node's certificate, its key and the anchors, as PEM.
    #[must_use]
    pub fn issue(&self) -> (String, String, String) {
        let key = rcgen::KeyPair::generate().expect("a key");
        let params =
            rcgen::CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                .expect("params");
        let leaf = params
            .signed_by(&key, &self.issuer.cert, &self.issuer.key_pair)
            .expect("issued");
        (leaf.pem(), key.serialize_pem(), self.pem.clone())
    }

    /// A node's identity, as the link presents and checks it.
    #[must_use]
    pub fn identity(&self) -> tls::Identity {
        let (certificate, key, anchors) = self.issue();
        tls::Identity::from_pem(certificate.as_bytes(), key.as_bytes(), anchors.as_bytes())
            .expect("an identity")
    }
}

/// The members, as the cluster's record holds them; a test changes it.
#[derive(Default)]
pub struct Roster(pub Mutex<Vec<Member>>);

impl Roster {
    pub fn add(&self, member: Member) {
        let mut members = self.0.lock().expect("roster");
        members.retain(|held| held.node != member.node);
        members.push(member);
    }

    pub fn remove(&self, node: &str) {
        self.0
            .lock()
            .expect("roster")
            .retain(|held| held.node != node);
    }
}

impl Membership for Roster {
    fn members(&self) -> Result<Vec<Member>, EventError> {
        Ok(self.0.lock().expect("roster").clone())
    }
}

/// A node of the test cluster: its hub, joined.
pub struct TestNode {
    pub name: String,
    pub hub: Hub,
    pub cluster: Option<Cluster>,
    pub audit: ProgramAudit,
    /// The Parties its hub's policy allows: those that subscribed here.
    allowed: Mutex<PartyPolicy>,
}

impl TestNode {
    /// Node `name` joins, on a sync listener of its own, and is added to
    /// `roster`.
    #[must_use]
    pub fn join(name: &str, authority: &Authority, roster: &Arc<Roster>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a sync listener");
        Self::join_on(name, listener, authority, roster)
    }

    /// As [`Self::join`], on `listener`.
    #[must_use]
    pub fn join_on(
        name: &str,
        listener: TcpListener,
        authority: &Authority,
        roster: &Arc<Roster>,
    ) -> Self {
        let port = listener.local_addr().expect("address").port();
        let membership: Arc<dyn Membership> = roster.clone();
        let joined = Self::join_with(name, listener, authority.identity(), membership);
        roster.add(Member {
            node: node(name),
            address: format!("127.0.0.1:{port}"),
        });
        joined
    }

    /// Node `name` joins on `listener`, presenting `identity`, its
    /// members kept by `membership`.
    #[must_use]
    pub fn join_with(
        name: &str,
        listener: TcpListener,
        identity: tls::Identity,
        membership: Arc<dyn Membership>,
    ) -> Self {
        let hub = Hub::new(Vec::new());
        let audit = ProgramAudit::new("xmip-core-event tests", Some(&directory(name)));
        let cluster = Cluster::join(
            &hub,
            Joining {
                node: node(name),
                listener,
                identity,
                membership,
                follow_every: Duration::from_secs(60),
                audit: audit.clone(),
            },
        )
        .expect("joined");
        Self {
            name: name.to_string(),
            hub,
            cluster: Some(cluster),
            audit,
            allowed: Mutex::new(PartyPolicy::new()),
        }
    }

    /// Every node of the cluster reads the members again.
    pub fn follow(nodes: &[&Self]) {
        for each in nodes {
            if let Some(cluster) = &each.cluster {
                cluster.follow();
            }
        }
    }

    /// A subscription on this node, as Party `party`, to `filter`.
    #[must_use]
    pub fn subscribe(&self, party: u128, filter: Filter) -> EventSubscription {
        let mut allowed = self.allowed.lock().expect("allowed");
        *allowed = allowed.clone().allow(PartyId::new(party));
        self.hub.authorize_by(vec![Arc::new(allowed.clone())]);
        drop(allowed);
        let subscriber = Subscriber::in_process(PartyId::new(party), self.audit.clone());
        self.hub.subscribe(subscriber, filter, 0).expect("allowed")
    }

    /// An Event raised on this node.
    pub fn raise(&self, outcome: Outcome) -> Event {
        let event = Event::completed(
            Stage::Receive,
            outcome,
            format!("{}/receive/orders", node(&self.name)),
        );
        self.hub.publish(event.clone());
        event
    }

    /// Leave the cluster.
    pub fn leave(&mut self) {
        if let Some(cluster) = self.cluster.take() {
            cluster.stop();
        }
    }
}

/// A directory of this test's own.
#[must_use]
pub fn directory(name: &str) -> PathBuf {
    let at = std::env::temp_dir().join(format!("xmip-event-cluster-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&at);
    at
}

/// What `subscription` is handed within `within`, until `count` arrived.
#[must_use]
pub fn drained(subscription: &EventSubscription, count: usize, within: Duration) -> Vec<Event> {
    let deadline = Instant::now() + within;
    let mut heard = Vec::new();
    while heard.len() < count {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        heard.extend(
            subscription
                .next(left, usize::MAX)
                .events
                .iter()
                .map(|event| event.as_ref().clone()),
        );
    }
    heard
}

/// Wait up to `within` for `done`.
pub fn until(within: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    done()
}

/// Every failure, beneath the cluster.
#[must_use]
pub fn failures() -> Filter {
    Filter::everything()
        .ending(Outcome::Failure)
        .beneath(cluster())
}
