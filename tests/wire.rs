//! The wire event against its specification's own examples, the JSON event
//! format's three, and the two modes over a binding's rules: binary data
//! byte for byte, a binding's prefix, matching and escaping used both
//! ways, what is not one event refused, and an Xmip Event round-tripped in
//! both modes. Each protocol binding's own examples are its transport's
//! (`tests/event_wire.rs` in the http, kafka and amqp transports).

use std::borrow::Cow;

use node::Stage;
use serde_json::{Value, json};
use xcore::{JourneyId, MessageId, PartyId};
use xmip_core_event::binding::{Carried, Mode, Rules, STRUCTURED};
use xmip_core_event::json_format::JSON_FORMAT;
use xmip_core_event::outcome::Outcome;
use xmip_core_event::wire::{Data, Extension, WireEvent};
use xmip_core_event::{Event, EventError};

fn read(value: &Value) -> WireEvent {
    WireEvent::read_json(value).expect("a WireEvent")
}

/// A binding with the default rules: its prefix, matched exactly, and
/// every value as it is.
struct Plain;

impl Rules for Plain {
    fn prefix(&self) -> &'static str {
        "ev-"
    }
}

/// A binding that matches a name without regard to case and writes a
/// space as `~`, refusing a value that holds a space of its own.
struct Folding;

impl Rules for Folding {
    fn prefix(&self) -> &'static str {
        "ev-"
    }

    fn attribute<'h>(&self, header: &'h str) -> Option<&'h str> {
        let (head, name) = header.split_at_checked(self.prefix().len())?;
        head.eq_ignore_ascii_case(self.prefix()).then_some(name)
    }

    fn escaped<'v>(&self, value: &'v str) -> Cow<'v, str> {
        if value.contains(' ') {
            Cow::Owned(value.replace(' ', "~"))
        } else {
            Cow::Borrowed(value)
        }
    }

    fn unescaped<'v>(&self, value: &'v str) -> Result<Cow<'v, str>, EventError> {
        if value.contains(' ') {
            return Err(EventError::new("a space this binding never writes"));
        }
        Ok(Cow::Owned(value.replace('~', " ")))
    }
}

/// Both test bindings, as a wire answers its own.
const BINDINGS: [&dyn Rules; 2] = [&Plain, &Folding];

/// JSON format section 5.1, the example with binary data.
#[test]
fn the_json_format_example_with_binary_data_reads_whole() {
    let event = read(&json!({
        "specversion" : "1.0",
        "type" : "com.example.someevent",
        "source" : "/mycontext",
        "id" : "A234-1234-1234",
        "time" : "2018-04-05T17:31:00Z",
        "comexampleextension1" : "value",
        "comexampleothervalue" : 5,
        "datacontenttype" : "application/vnd.apache.thrift.binary",
        "data_base64" : "AQID"
    }));

    assert_eq!(event.id, "A234-1234-1234");
    assert_eq!(event.kind, "com.example.someevent");
    assert_eq!(event.time.as_deref(), Some("2018-04-05T17:31:00Z"));
    assert_eq!(
        event.extensions["comexampleextension1"],
        Extension::Text("value".to_string())
    );
    assert_eq!(
        event.extensions["comexampleothervalue"],
        Extension::Integer(5)
    );
    assert_eq!(event.data, Some(Data::Binary(vec![1, 2, 3])));
    assert_eq!(
        read(&event.json().expect("written")),
        event,
        "written as it was read"
    );
}

/// JSON format section 5.1, the example with JSON data and null attributes.
#[test]
fn the_json_format_example_with_json_data_treats_null_as_absent() {
    let event = read(&json!({
        "specversion" : "1.0",
        "type" : "com.example.someevent",
        "source" : "/mycontext",
        "subject": null,
        "id" : "C234-1234-1234",
        "time" : "2018-04-05T17:31:00Z",
        "comexampleextension1" : "value",
        "comexampleothervalue" : 5,
        "unsetextension": null,
        "datacontenttype" : "application/json",
        "data" : { "appinfoA" : "abc", "appinfoB" : 123, "appinfoC" : true }
    }));

    assert_eq!(event.subject, None);
    assert!(!event.extensions.contains_key("unsetextension"));
    assert_eq!(
        event.data,
        Some(Data::Json(
            json!({ "appinfoA" : "abc", "appinfoB" : 123, "appinfoC" : true })
        ))
    );
}

/// JSON format section 5.1, the example with a string that is XML.
#[test]
fn the_json_format_example_with_text_data_keeps_it_text() {
    let event = read(&json!({
        "specversion" : "1.0",
        "type" : "com.example.someevent",
        "source" : "/mycontext",
        "id" : "B234-1234-1234",
        "time" : "2018-04-05T17:31:00Z",
        "datacontenttype" : "text/xml",
        "data" : "<much wow=\"xml\"/>"
    }));

    assert_eq!(
        event.data,
        Some(Data::Text("<much wow=\"xml\"/>".to_string()))
    );
    assert_eq!(
        event.json().expect("written")["data"],
        "<much wow=\"xml\"/>"
    );
}

/// JSON format section 3.1.1: data declared JSON goes in `data` as JSON,
/// bytes under any other type or none in `data_base64`, and bytes declared
/// JSON that are not JSON are refused, never written some other way.
#[test]
fn the_json_format_writes_json_as_json_and_everything_else_as_bytes() {
    let mut event = WireEvent::new("1", "/s", "t");
    event.data_content_type = Some("application/vnd.example+json".to_string());
    event.data = Some(Data::Binary(br#"{"a":1}"#.to_vec()));
    let written = event.json().expect("written");
    assert_eq!(written["data"], json!({"a": 1}));
    assert!(written.get("data_base64").is_none());
    assert_eq!(read(&written).data, Some(Data::Json(json!({"a": 1}))));

    for media in [
        Some("text/plain; charset=utf-8"),
        Some("application/avro"),
        None,
    ] {
        event.data_content_type = media.map(str::to_string);
        event.data = Some(Data::Binary(vec![b'h', 0xff, 0, b'i']));
        let written = event.json().expect("written");
        assert_eq!(written["data_base64"], "aP8AaQ==", "{media:?}");
        assert!(written.get("data").is_none(), "{media:?}");
        assert_eq!(read(&written), event, "{media:?} byte-exact");
    }

    event.data_content_type = Some("application/json".to_string());
    event.data = Some(Data::Binary(b"not json".to_vec()));
    let refused = event.json().expect_err("declared JSON, is not");
    assert!(refused.to_string().contains("not JSON"), "{refused}");
    event.data_content_type = Some("text/xml".to_string());
    event.data = Some(Data::Json(json!({"a": 1})));
    assert!(
        event.json().is_err(),
        "JSON data under a type that is not JSON"
    );
}

/// JSON format section 3.1.2: under a type that is not JSON, `data` is text
/// and must be a string.
#[test]
fn data_under_a_type_that_is_not_json_must_be_a_string() {
    let refused = WireEvent::read_json(&json!({
        "specversion": "1.0", "type": "t", "source": "/s", "id": "1",
        "datacontenttype": "text/plain", "data": {"a": 1}
    }));
    assert!(refused.is_err());
}

/// Binary mode: the body is the data, byte for byte, whatever its content
/// type says — nothing is decoded on the way in or out.
#[test]
fn binary_mode_data_round_trips_byte_exact_whatever_the_rules() {
    let bodies: [(&str, &[u8]); 3] = [
        ("text/plain; charset=utf-8", &[b'a', 0xc3, 0x28, b'z']),
        ("application/json", b"{ \"a\" : 1 ,}"),
        ("application/octet-stream", &[0, 0xff, 0xfe, 0x80]),
    ];
    for (rules, binding) in BINDINGS.iter().enumerate() {
        for (media, body) in bodies {
            let mut event = WireEvent::new("1", "/s", "t");
            event.data_content_type = Some(media.to_string());
            event.data = Some(Data::Binary(body.to_vec()));
            let carried = binding.write(&event, Mode::Binary).expect("written");
            assert_eq!(carried.body, body, "rules {rules}, {media}");
            let back = binding.read(&carried).expect("read");
            assert_eq!(
                back.data,
                Some(Data::Binary(body.to_vec())),
                "rules {rules}, {media}"
            );
            assert_eq!(back, event);
        }
    }
}

/// Binary mode by a binding's rules: each attribute under its prefix,
/// escaped as the binding writes it, read back by the binding's matching
/// and unescaped, and a value its escaping refuses refused.
#[test]
fn binary_mode_writes_and_reads_by_the_bindings_rules() {
    let mut event = WireEvent::new("1", "/s", "t");
    event.subject = Some("a b".to_string());

    let mut carried = Folding.write(&event, Mode::Binary).expect("written");
    let subject = carried
        .headers
        .iter()
        .find(|(name, _)| name == "ev-subject")
        .map(|(_, value)| value.as_str());
    assert_eq!(subject, Some("a~b"), "escaped as the binding writes it");
    assert!(
        carried
            .headers
            .iter()
            .all(|(name, _)| name.starts_with("ev-"))
    );
    for (name, _) in &mut carried.headers {
        *name = name.to_ascii_uppercase();
    }
    assert_eq!(Folding.read(&carried).expect("read"), event, "EV-, folded");
    assert!(
        Plain.read(&carried).is_err(),
        "EV- is not ev- matched exactly"
    );

    let written = Plain.write(&event, Mode::Binary).expect("written");
    let refused = Folding.read(&written).expect_err("a space unescaped");
    assert!(refused.to_string().contains("never writes"), "{refused}");
    assert_eq!(Plain.read(&written).expect("read"), event, "as it is");
}

#[test]
fn what_the_specification_refuses_is_refused() {
    let base = json!({"specversion": "1.0", "type": "t", "source": "/s", "id": "1"});
    let with = |name: &str, value: Value| {
        let mut changed = base.clone();
        changed[name] = value;
        WireEvent::read_json(&changed)
            .err()
            .map(|error| error.to_string())
    };

    assert!(WireEvent::read_json(&base).is_ok());
    assert!(with("specversion", json!("0.3")).is_some_and(|e| e.contains("0.3")));
    assert!(with("id", json!("")).is_some_and(|e| e.contains("no id")));
    assert!(with("source", json!(5)).is_some_and(|e| e.contains("not a string")));
    assert!(with("Upper", json!("x")).is_some_and(|e| e.contains("a-z0-9")));
    assert!(with("time", json!("yesterday")).is_some_and(|e| e.contains("RFC 3339")));
    let mut both = base.clone();
    both["data"] = json!("x");
    both["data_base64"] = json!("eA==");
    assert!(WireEvent::read_json(&both).is_err());
    assert!(WireEvent::read_json(&json!([base])).is_err());
}

#[test]
fn a_message_without_specversion_or_a_batch_is_no_wire_event() {
    let carried = Carried {
        content_type: Some("text/plain".to_string()),
        headers: vec![("ev-id".to_string(), "1".to_string())],
        body: b"hello".to_vec(),
    };

    assert!(Plain.read(&carried).is_err());
    let batch = Carried {
        content_type: Some("application/cloudevents-batch+json".to_string()),
        ..Carried::default()
    };
    assert!(Plain.read(&batch).is_err());
}

#[test]
fn an_xmip_event_rides_both_modes_whatever_the_rules_and_comes_back_itself() {
    let scope = format!(
        "{}/send/billing",
        configure::fixture::test_cluster().node_scope(0)
    );
    let event = Event::completed(Stage::Send, Outcome::Failure, scope.as_str())
        .in_journey(JourneyId::new(11))
        .of_message(MessageId::new(12))
        .at_endpoint("https://billing.example/in")
        .by_module("xmip-core-transport-http")
        .on_artifact("billing east")
        .about(PartyId::new(13))
        .saying("status", "503");
    let wire_event = WireEvent::from_event(&event);

    assert_eq!(wire_event.kind, "se.xmip.send.failure");
    assert_eq!(wire_event.source, scope);
    assert_eq!(wire_event.subject.as_deref(), Some("billing east"));
    assert_eq!(wire_event.event().expect("an Xmip Event"), event);

    for (rules, binding) in BINDINGS.iter().enumerate() {
        for mode in [Mode::Structured, Mode::Binary] {
            let carried = binding.write(&wire_event, mode).expect("written");
            if mode == Mode::Structured {
                assert_eq!(carried.content_type.as_deref(), Some(STRUCTURED));
                assert!(STRUCTURED.starts_with(JSON_FORMAT));
            }
            let back = binding.read(&carried).expect("read back");
            assert_eq!(
                back.event().expect("the Event"),
                event,
                "rules {rules}, {mode:?}"
            );
        }
    }
}
