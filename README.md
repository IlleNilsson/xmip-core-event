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
be told. Delivery is not in the message path and no Message waits for it.

## One model, subscribed two ways (ADR-0065)

- **The rule, once.** `filter::Filter` names Event types, outcomes, a scope
  and a Party; `Filter::matches` is the one rule every way of subscribing
  asks. A scope matches at and beneath itself by `node::Scope::contains`.
  `Filter::reach` is the scope the authorization gate is asked about, and
  `Filter::said` what a surface lists a subscription by.
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
  it; bytes declared JSON that are not are refused. `wire::Wire` is what
  a transport implements to carry one to a Party, presenting the identity
  the Send side configured for it — Xmip's own http, kafka and amqp
  transports each do (`src/event_wire.rs` in each).

This crate is Foundation: what an Event is, true whether or not anything
runs, depending on `xmip-core`, `xmip-core-node` and
`xmip-core-library-codec` alone (ADR-0058 clause 3). Delivering it is
`xmip-core-cluster`'s, behind its feature `delivery` (ADR-0065, amendment
2026-11-13): the hub every subscriber is admitted to and every Event is
published through, the subscriber and its gate, an operator's acts, the
forwarder that carries a subscription over a `wire::Wire` at least once,
and the Event link's conversation between nodes.

`doc/architecture/runtime-model.md` section 17, *Eventing*, governs it, and
ADR-0065 decides it; `architecture.toml` carries the maturity.
