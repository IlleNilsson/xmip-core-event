//! The cluster's Events across processes: each node its own process — this
//! test binary run again as `node_process` — its own hub and sync
//! listener, the members in a roster file the cluster reads, over Xmip's
//! mutual TLS on loopback. A subscriber on every node hears every node,
//! once each; a node killed is unheard, and heard again when it is started
//! on its port again. Driven through each node's standard input, and heard
//! through its standard output.

#[path = "cluster/test_cluster.rs"]
mod test_cluster;

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use test_cluster::{Authority, TestNode, directory, failures, name, node};
use xmip_core_event::EventError;
use xmip_core_event::cluster::{Member, Membership};
use xmip_core_event::outcome::Outcome;

const WITHIN: Duration = Duration::from_secs(10);

/// What a node process is told by its environment.
const NAME: &str = "XMIP_EVENT_TEST_NODE";
const PORT: &str = "XMIP_EVENT_TEST_PORT";
const AT: &str = "XMIP_EVENT_TEST_AT";

/// The members as the roster file lists them: one `node address` a line.
struct RosterFile(PathBuf);

impl Membership for RosterFile {
    fn members(&self) -> Result<Vec<Member>, EventError> {
        let text = std::fs::read_to_string(&self.0).unwrap_or_default();
        Ok(text
            .lines()
            .filter_map(|line| line.split_once(' '))
            .map(|(node, address)| Member {
                node: node.to_string(),
                address: address.to_string(),
            })
            .collect())
    }
}

/// A node process, when this binary is run as one; otherwise nothing.
#[test]
fn node_process() {
    let (Ok(name), Ok(at)) = (std::env::var(NAME), std::env::var(AT)) else {
        return;
    };
    let at = PathBuf::from(at);
    let port = std::env::var(PORT).unwrap_or_else(|_| "0".to_string());
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).expect("a sync listener");
    let port = listener.local_addr().expect("address").port();
    let read = |file: &str| std::fs::read(at.join(file)).expect("a PEM");
    let identity = tls::Identity::from_pem(
        &read(&format!("{name}.crt")),
        &read(&format!("{name}.key")),
        &read("anchors.crt"),
    )
    .expect("an identity");
    let roster: Arc<dyn Membership> = Arc::new(RosterFile(at.join("roster")));
    let node = TestNode::join_with(&name, listener, identity, roster);
    let out = Arc::new(Mutex::new(std::io::stdout()));
    say(&out, &format!("ready {port}"));

    let mut listening = Vec::new();
    for line in std::io::stdin().lock().lines() {
        let line = line.expect("a command");
        match line.as_str() {
            "follow" => node.cluster.as_ref().expect("joined").follow(),
            "subscribe" => {
                let out = Arc::clone(&out);
                let listener = node
                    .subscribe(1, failures())
                    .listen(move |event| {
                        let late = xcore::Clock::unix_timestamp_nanos(&xcore::SystemClock)
                            - event.time_unix_nanos;
                        say(&out, &format!("heard {} {} {late}", event.id, event.scope));
                    })
                    .expect("listening");
                listening.push(listener);
            }
            "raise" => {
                let event = node.raise(Outcome::Failure);
                say(&out, &format!("raised {}", event.id));
            }
            "unheard" => {
                let unheard: Vec<String> = node
                    .hub
                    .unheard()
                    .into_iter()
                    .map(|gone| gone.node)
                    .collect();
                say(&out, &format!("unheard {}", unheard.join(",")));
            }
            _ => break,
        }
        say(&out, &format!("done {line}"));
    }
}

fn say(out: &Mutex<std::io::Stdout>, line: &str) {
    let mut out = out.lock().expect("stdout");
    let _ = writeln!(out, "xmip: {line}");
    let _ = out.flush();
}

/// A node process as the test drives it.
struct NodeProcess {
    name: String,
    child: Child,
    input: ChildStdin,
    lines: Receiver<String>,
    /// What it heard while the test waited for something else.
    kept: Vec<String>,
    port: u16,
}

impl NodeProcess {
    fn start(name: &str, at: &Path, port: u16) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("this binary"))
            .args(["--exact", "node_process", "--nocapture", "--test-threads=1"])
            .env(NAME, name)
            .env(AT, at)
            .env(PORT, port.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("a node process");
        let input = child.stdin.take().expect("its input");
        let output = child.stdout.take().expect("its output");
        let (tell, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines().map_while(Result::ok) {
                // The harness may have begun the line: "test node_process ... ".
                if let Some((_, said)) = line.split_once("xmip: ") {
                    let _ = tell.send(said.to_string());
                }
            }
        });
        let mut started = Self {
            name: name.to_string(),
            child,
            input,
            lines,
            kept: Vec::new(),
            port: 0,
        };
        let ready = started.expect("ready ");
        started.port = ready.parse().expect("a port");
        started
    }

    /// What follows `start` on the next line starting with it; the Events
    /// heard before it are kept, and other lines passed over.
    fn expect(&mut self, start: &str) -> String {
        let deadline = Instant::now() + WITHIN;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let line = self
                .lines
                .recv_timeout(left)
                .unwrap_or_else(|_| panic!("{} never said '{start}'", self.name));
            if let Some(rest) = line.strip_prefix(start) {
                return rest.to_string();
            }
            if line.starts_with("heard ") {
                self.kept.push(line);
            }
        }
    }

    fn tell(&mut self, command: &str) {
        writeln!(self.input, "{command}").expect("told");
        self.input.flush().expect("flushed");
        self.expect(&format!("done {command}"));
    }

    /// Every Event heard within `within`, by identity, with how late.
    fn heard(&mut self, count: usize, within: Duration) -> Vec<(String, String, i128)> {
        let deadline = Instant::now() + within;
        let mut heard = Vec::new();
        while heard.len() < count {
            let line = if self.kept.is_empty() {
                let left = deadline.saturating_duration_since(Instant::now());
                let Ok(line) = self.lines.recv_timeout(left) else {
                    break;
                };
                line
            } else {
                self.kept.remove(0)
            };
            if let Some(rest) = line.strip_prefix("heard ") {
                let parts: Vec<&str> = rest.split(' ').collect();
                heard.push((
                    parts[0].to_string(),
                    parts[1].to_string(),
                    parts[2].parse().expect("late"),
                ));
            }
        }
        heard
    }

    /// Ask who is unheard until `done` says so of the answer, or time is
    /// out; the last answer.
    fn unheard_until(&mut self, done: impl Fn(&str) -> bool) -> String {
        let deadline = Instant::now() + WITHIN;
        loop {
            writeln!(self.input, "unheard").expect("told");
            self.input.flush().expect("flushed");
            let said = self.expect("unheard ").trim().to_string();
            self.expect("done unheard");
            if done(&said) || Instant::now() > deadline {
                return said;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

/// A cluster directory: the authority's anchors, each node's certificate
/// and key, and the roster.
fn cluster(name: &str, nodes: &[&str]) -> PathBuf {
    let at = directory(name);
    std::fs::create_dir_all(&at).expect("a directory");
    let authority = Authority::new();
    for each in nodes {
        let (certificate, key, anchors) = authority.issue();
        std::fs::write(at.join(format!("{each}.crt")), certificate).expect("written");
        std::fs::write(at.join(format!("{each}.key")), key).expect("written");
        std::fs::write(at.join("anchors.crt"), anchors).expect("written");
    }
    at
}

fn write_roster(at: &Path, processes: &[&NodeProcess]) {
    let roster: Vec<String> = processes
        .iter()
        .map(|each| format!("{} 127.0.0.1:{}", node(&each.name), each.port))
        .collect();
    std::fs::write(at.join("roster"), roster.join("\n")).expect("the roster");
}

#[test]
fn three_node_processes_each_subscriber_hears_every_node_once() {
    let names = [name(0), name(1), name(2)];
    let at = cluster("processes", &names.each_ref().map(String::as_str));
    let mut nodes: Vec<NodeProcess> = names
        .iter()
        .map(|name| NodeProcess::start(name, &at, 0))
        .collect();
    write_roster(&at, &nodes.iter().collect::<Vec<_>>());
    for each in &mut nodes {
        each.tell("follow");
        each.tell("subscribe");
    }
    for each in &mut nodes {
        each.tell("raise");
    }

    let mut late = Vec::new();
    for each in &mut nodes {
        let heard = each.heard(3, WITHIN);
        let scopes: BTreeSet<&str> = heard.iter().map(|(_, scope, _)| scope.as_str()).collect();
        let ids: BTreeSet<&str> = heard.iter().map(|(id, _, _)| id.as_str()).collect();
        assert_eq!(heard.len(), 3, "{}: {heard:?}", each.name);
        assert_eq!(
            (scopes.len(), ids.len()),
            (3, 3),
            "one from each node, none twice"
        );
        assert!(
            each.heard(1, Duration::from_millis(100)).is_empty(),
            "no duplicate"
        );
        late.extend(heard.iter().map(|(_, _, nanos)| *nanos));
    }
    late.sort_unstable();
    println!(
        "raised to heard, across processes: median {} µs over {}",
        late[late.len() / 2] / 1000,
        late.len()
    );
}

#[test]
fn a_node_process_killed_is_unheard_and_heard_again_when_started_again() {
    let (first, second) = (name(0), name(1));
    let at = cluster("restart", &[first.as_str(), second.as_str()]);
    let mut one = NodeProcess::start(&first, &at, 0);
    let mut two = NodeProcess::start(&second, &at, 0);
    write_roster(&at, &[&one, &two]);
    one.tell("follow");
    two.tell("follow");
    one.tell("subscribe");
    two.tell("raise");
    assert_eq!(one.heard(1, WITHIN).len(), 1);

    let port = two.port;
    two.kill();
    let said = one.unheard_until(|unheard| unheard.contains(&node(&second)));
    assert!(
        said.contains(&node(&second)),
        "{second} is said to be unheard: {said}"
    );

    let mut back = NodeProcess::start(&second, &at, port);
    back.tell("follow");
    let said = one.unheard_until(str::is_empty);
    assert!(said.is_empty(), "{second} is heard again: {said}");
    back.tell("raise");
    let heard = one.heard(1, WITHIN);
    assert_eq!(heard.len(), 1, "heard again");
    assert_eq!(heard[0].1, format!("{}/receive/orders", node(&second)));
}
