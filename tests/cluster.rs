//! Any node is the cluster's door (ADR-0065, amendment 2026-10-02): a
//! subscriber on any node hears the matching Events of every node, once
//! each, and nothing its filter refuses crosses; a node that goes down is
//! said to be unheard and is heard again when it is back; a node that
//! joins is followed. Two and three nodes in this process, each its own
//! hub on its own sync listener, over Xmip's mutual TLS.

#[path = "cluster/test_cluster.rs"]
mod test_cluster;

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use test_cluster::{Authority, Roster, TestNode, drained, failures, name, node, until};
use xmip_core_event::filter::Filter;
use xmip_core_event::outcome::Outcome;

const WITHIN: Duration = Duration::from_secs(5);

#[test]
fn a_subscriber_on_either_of_two_nodes_hears_both_once_each() {
    let (authority, roster) = (Authority::new(), Arc::new(Roster::default()));
    let one = TestNode::join(&name(0), &authority, &roster);
    let two = TestNode::join(&name(1), &authority, &roster);
    TestNode::follow(&[&one, &two]);

    let on_one = one.subscribe(1, failures());
    let on_two = two.subscribe(2, failures());
    let raised = [
        one.raise(Outcome::Failure),
        two.raise(Outcome::Failure),
        two.raise(Outcome::Failure),
    ];

    for subscription in [&on_one, &on_two] {
        let heard = drained(subscription, 3, WITHIN);
        let ids: BTreeSet<_> = heard.iter().map(|event| event.id).collect();
        assert_eq!(heard.len(), 3, "{heard:?}");
        assert_eq!(ids, raised.iter().map(|event| event.id).collect());
        assert!(
            drained(subscription, 1, Duration::from_millis(50)).is_empty(),
            "no duplicate"
        );
    }
}

#[test]
fn three_nodes_each_subscriber_hears_every_node_and_nothing_twice() {
    let (authority, roster) = (Authority::new(), Arc::new(Roster::default()));
    let nodes: Vec<TestNode> = (0..3)
        .map(|place| TestNode::join(&name(place), &authority, &roster))
        .collect();
    TestNode::follow(&nodes.iter().collect::<Vec<_>>());

    let subscriptions: Vec<_> = nodes
        .iter()
        .enumerate()
        .map(|(at, each)| each.subscribe(at as u128 + 1, failures()))
        .collect();
    let raised: Vec<_> = nodes
        .iter()
        .map(|each| each.raise(Outcome::Failure))
        .collect();

    for subscription in &subscriptions {
        let heard = drained(subscription, 3, WITHIN);
        let scopes: BTreeSet<_> = heard.iter().map(|event| event.scope.clone()).collect();
        assert_eq!(heard.len(), 3, "{heard:?}");
        assert_eq!(scopes.len(), 3, "one from each node: {scopes:?}");
        assert!(heard.iter().all(|event| raised.contains(event)));
        assert!(
            drained(subscription, 1, Duration::from_millis(50)).is_empty(),
            "no duplicate"
        );
    }
}

#[test]
fn what_no_filter_wants_never_crosses() {
    let (authority, roster) = (Authority::new(), Arc::new(Roster::default()));
    let one = TestNode::join(&name(0), &authority, &roster);
    let two = TestNode::join(&name(1), &authority, &roster);
    TestNode::follow(&[&one, &two]);

    let on_one = one.subscribe(1, failures());
    // A subscriber on the second node itself sees what it raises, so it is
    // raised.
    let on_two = two.subscribe(2, Filter::everything());
    two.raise(Outcome::Success);
    two.raise(Outcome::Failure);

    assert_eq!(drained(&on_two, 2, WITHIN).len(), 2);
    let heard = drained(&on_one, 2, Duration::from_millis(200));
    assert_eq!(heard.len(), 1, "only the failure crossed: {heard:?}");
    assert_eq!(heard[0].outcome, Outcome::Failure);
    // What crossed is what the link carried: the success never left the second.
    assert_eq!(one.hub.unheard(), Vec::new());
}

#[test]
fn a_node_down_is_unheard_and_heard_again_when_it_is_back() {
    let (authority, roster) = (Authority::new(), Arc::new(Roster::default()));
    let one = TestNode::join(&name(0), &authority, &roster);
    let mut two = TestNode::join(&name(1), &authority, &roster);
    let three = TestNode::join(&name(2), &authority, &roster);
    TestNode::follow(&[&one, &two, &three]);
    let on_one = one.subscribe(1, failures());
    let address = roster
        .0
        .lock()
        .expect("roster")
        .iter()
        .find(|member| member.node == node(&name(1)))
        .expect("the second node")
        .address
        .clone();

    two.leave();
    // The subscriber itself is told, with no Event: its drain wakes.
    let told = on_one.next(WITHIN, 1);
    assert!(told.events.is_empty() && told.unheard_changed, "{told:?}");
    assert_eq!(told.unheard.len(), 1, "{told:?}");
    assert_eq!(told.unheard[0].node, node(&name(1)));
    assert_eq!(told.unheard[0].by, node(&name(0)));
    assert!(
        told.unheard[0]
            .said()
            .starts_with(&format!("not hearing {} since ", node(&name(1))))
    );
    // The third is heard throughout: a node down blocks no other.
    three.raise(Outcome::Failure);
    assert_eq!(drained(&on_one, 1, WITHIN).len(), 1);

    let port = address.rsplit_once(':').expect("port").1;
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).expect("the same port");
    let back = TestNode::join_on(&name(1), listener, &authority, &roster);
    assert!(
        until(WITHIN, || one.hub.unheard().is_empty()),
        "the second is heard again: {:?}",
        one.hub.unheard()
    );
    // Told again, with no one unheard.
    let told = on_one.next(WITHIN, 1);
    assert!(told.unheard_changed && told.unheard.is_empty(), "{told:?}");
    back.raise(Outcome::Failure);
    let heard = drained(&on_one, 1, WITHIN);
    assert_eq!(heard.len(), 1);
    assert_eq!(heard[0].scope, format!("{}/receive/orders", node(&name(1))));
}

#[test]
fn a_node_that_joins_is_followed_and_one_that_leaves_is_not_missing() {
    let (authority, roster) = (Authority::new(), Arc::new(Roster::default()));
    let one = TestNode::join(&name(0), &authority, &roster);
    let on_one = one.subscribe(1, failures());

    let two = TestNode::join(&name(1), &authority, &roster);
    TestNode::follow(&[&one, &two]);
    assert!(until(WITHIN, || one
        .cluster
        .as_ref()
        .is_some_and(|cluster| cluster.members().len() == 1)));
    // The follow made the link; a subscribe made now returns once it
    // carries its filter, and the one made before is carried as well.
    let again = one.subscribe(3, failures());
    two.raise(Outcome::Failure);
    assert_eq!(drained(&on_one, 1, WITHIN).len(), 1);
    assert_eq!(drained(&again, 1, WITHIN).len(), 1);

    roster.remove(&node(&name(1)));
    TestNode::follow(&[&one]);
    assert!(until(WITHIN, || one
        .cluster
        .as_ref()
        .is_some_and(|cluster| cluster.members().is_empty())));
    assert!(
        one.hub.unheard().is_empty(),
        "a member that left is not missing"
    );
}

#[test]
fn a_link_is_not_an_event_subscription_an_operator_lists_or_acts_on() {
    let (authority, roster) = (Authority::new(), Arc::new(Roster::default()));
    let one = TestNode::join(&name(0), &authority, &roster);
    let two = TestNode::join(&name(1), &authority, &roster);
    TestNode::follow(&[&one, &two]);
    let on_one = one.subscribe(1, failures());
    two.raise(Outcome::Failure);
    assert_eq!(drained(&on_one, 1, WITHIN).len(), 1);

    // The second serves the first's link, and lists none of it.
    assert_eq!(two.hub.standing(&node(&name(1))), Vec::new());
    assert_eq!(two.hub.subscriptions(), 0);
    let listed = one.hub.standing(&node(&name(0)));
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].delivered, 1);
}
