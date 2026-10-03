# xmip-core-event

The Event: what Xmip tells a Party has happened. Every completed Receive,
Process and Send action produces one for every outcome — success, failure,
rejection, waiting, pause, timeout, exhausted retries, dismissal — carrying
its identity and type, when, the action and how it ended, where it happened
as an Xmip URI, the Journey, Message and Stream it concerns, the Endpoint,
Module and Artifact, the Party it is about, and diagnostics safe to hand
outside. References, never payload copies.

An Event is not an audit record and not a Message. Audit is the durable
record Xmip keeps for itself; an Event leaves Xmip for a Party that asked to
be told. Delivery is not in the message path and no Journey waits for it.

## One model, subscribed two ways (ADR-0065)

- **The rule, once.** `filter::Filter` names Event types, outcomes, a scope
  and a Party; `Filter::matches` is the one rule every way of subscribing
  asks. A scope matches at and beneath itself by `observe::Scope::contains`.
- **Who may see it.** A subscriber is a Party (`subscriber::Subscriber`),
  and the authorization gate decides, once, when it subscribes:
  `authorize::authorize` for a Send at the scope it reaches, each Event type
  as the Contract. Nothing configured is a refusal. A program in this
  process is `peer-credentials` naming the process, resolved to the Party it
  names, and being here admits it to nothing (ADR-0065, amendment
  2026-09-26): the hub's gate (`gate`) is the one policy list it is handed
  by `Hub::authorize_by` — the node's as it starts, or one the program
  hosting the hub takes from the authorize capability (for "these Parties",
  `xmip-core-authorize-party`'s `PartyPolicy`, which this crate's tests use). `Hub::process`
  admits nobody until it is handed one.
- **In process.** `hub::Hub` fans an Event out to every matching
  subscription through a bounded queue each and never waits for a
  subscriber: a full queue refuses the Event for that subscriber and counts
  it. `EventSubscription::next` waits for the first Event and wakes on arrival;
  `EventSubscription::listen` calls back on a thread of its own instead.
  `Hub::process` is the process's hub, and `xmip_operate.h` section 11 is
  its C boundary, forwarded by the runtime's library and bound by C, C++,
  .NET, Java and Python (`xmip-core-abi`).
- **Over the wire.** `wire::WireEvent` is an Event as it travels,
  `json_format` its JSON format; `binding::Binding` writes and reads it in
  either mode of the HTTP, Kafka and AMQP protocol bindings (1.0.2). The
  standard it follows is named once, in `src/wire.rs`.
  The data is bytes (ADR-0038, 2026-09-26): in binary mode the body is read
  and written byte for byte whatever its content type; in the JSON format,
  data whose `datacontenttype` declares JSON (`application/json`, any
  `+json`) is `data` as JSON, text under another type is `data` as a
  string, and bytes otherwise are `data_base64`. `WireEvent::json_data`
  is the one place data is read as JSON, and only where the type declares
  it; bytes declared JSON that are not are refused. `forward::Forwarder`
  carries a subscription's Events through a `forward::Wire` — Xmip's own
  http, kafka and amqp transports each implement one — at least once, in
  order, each attempt judged by the resilience guards.
- **Operated.** `Hub::standing` lists each open Event subscription as the
  `observe::EventSubscription` a node publishes — node and number,
  subscriber, what it asks for (`Filter::said`), state and its queue's
  counts — and `Hub::act` pauses, resumes or removes one (`observe::Act`,
  whose words are written once for every noun an operator acts on): paused,
  it keeps queuing up to its capacity and hands nothing over; resumed, it
  hands over what queued; removed, it is closed and its holder finds it so.
  Each act is audited in the subscriber's audit with who took it. A surface
  that reads a node only through its publication leaves the act as an
  `observe::Order` where the publication says, and the node takes it
  (ADR-0065, amendment 2026-09-29). The handle a subscriber holds is
  `hub::EventSubscription`, so no identifier here reads as a Subscription,
  which picks a published Message up (ADR-0013, amendment 2026-09-30).
- **Any node is the cluster's door** (ADR-0065, amendments 2026-09-26 and
  2026-10-02; `cluster`). A subscriber on whichever node it reaches hears
  the matching Events of every node in the cluster. `cluster::Cluster::join`
  puts a hub's node in the cluster: its sync listener (ADR-0067) answers the
  other members with this node's own Events, and it holds one link to each
  other member, over Xmip's mutual
  TLS (ADR-0063, `tls::duplex`), pushing its subscriptions' filters down so
  only what they match crosses — nothing while there are none (`cluster::link`, agreed as `xmip-event/1`).
  A member answers from its own Events only, so an Event crosses at most
  one hop, once, and never comes back; it is pushed the moment it is
  raised. The Party is authorized where it subscribed; a link presents the
  node's certificate and is authorized as a node. A subscribe returns once
  every member that can be reached carries its filter. Which nodes are
  members, and where they listen, is `cluster::Membership`'s — Xmip
  Storage's administration database in a node — read again by
  `Cluster::follow` and every `follow_every`; a member joining is linked, one
  leaving unlinked. A member that is down is tried again on its own link's
  thread, sooner first and then once a second, and until it is heard again
  it is unheard, and nothing missing is silent: every `Delivery` carries
  who is unheard now — by which node, the member, since when and why, as
  `observe::Unheard` — and a change wakes a waiting drain with no Event
  (`unheard_changed`); `Hub::unheard` answers the same for the node's
  publication, where every operator surface reads it as *not hearing
  `<node>` since `<time>`: `<why>`* (`observe::Unheard::said`); and the
  node's audit records it as `event.link`. What
  a link's queue on the far node refused crosses as a count and is missed
  on the subscriptions it matched. A link is the cluster's and not a
  Party's: it is not an Event subscription, `Hub::standing` does not list it
  and `Hub::act` does not reach it.
- **Audited.** Every subscription, delivery, refusal and forward is recorded
  in the subscriber's program audit (ADR-0062), handed to the audit
  capability's keeper (`audit::keeper`) so no Event waits for a disk and
  unsubscribing never does either; the program's next direct record — its
  stop — is kept after them.

## Near real time

The owner, 2026-09-26: an Event reaches an in-process subscriber within
about a millisecond of being published. `tests/latency.rs` measures publish
to receipt, drained and called back, and asserts it; `tests/hub.rs`
measures what a publish costs the publisher. Measured on the owner's
Windows machine, release build: a publish to one matching subscription
about 0.7 µs, to a hundred about 15 µs; publish to receipt a median of
about 40 µs and a 99th percentile under 100 µs.

`doc/architecture/runtime-model.md` section 17, *Eventing*, governs it, and
ADR-0065 decides it; `architecture.toml` carries the maturity.
