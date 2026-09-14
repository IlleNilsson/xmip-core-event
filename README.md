# xmip-core-event

The Event: what Xmip tells a Party has happened, named, tied to the Journey
and the Message it concerns, and delivered through an `EventPublisher`.

An Event is not an audit record and not a Message. Audit is the durable
record Xmip keeps for itself; an Event leaves Xmip for a Party that asked to be
told. Delivery is not in the message path and no Journey waits for it.

`doc/architecture/runtime-model.md` section 17, *Eventing*, governs it;
`architecture.toml` carries the maturity.
