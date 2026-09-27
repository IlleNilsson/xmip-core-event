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
  process is `peer-credentials` naming the process, and `SameProcess` is
  the policy that admits it and nothing else.
- **In process.** `hub::Hub` fans an Event out to every matching
  subscription through a bounded queue each and never waits for a
  subscriber: a full queue refuses the Event for that subscriber and counts
  it. `Subscription::next` waits for the first Event and wakes on arrival;
  `Subscription::listen` calls back on a thread of its own instead.
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
- **Audited.** Every subscription, delivery, refusal and forward is recorded
  in the subscriber's program audit (ADR-0062), handed to one keeping thread
  (`audit_queue`) so no Event waits for a disk; unsubscribing settles it.

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
