//! The wire event against its specification's own examples: the JSON
//! event format's three, the HTTP, Kafka and AMQP bindings' binary-mode
//! examples, the HTTP binding's percent-encoding example, and an Xmip Event
//! round-tripped through every binding in both modes.

use node::Stage;
use serde_json::{Value, json};
use xcore::{JourneyId, MessageId, PartyId};
use xmip_core_event::Event;
use xmip_core_event::binding::{Binding, Carried, Mode, STRUCTURED};
use xmip_core_event::json_format::JSON_FORMAT;
use xmip_core_event::outcome::Outcome;
use xmip_core_event::wire::{Data, Extension, WireEvent};

fn read(value: &Value) -> WireEvent {
    WireEvent::read_json(value).expect("a WireEvent")
}

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
fn binary_mode_data_round_trips_byte_exact_on_every_binding() {
    let bodies: [(&str, &[u8]); 3] = [
        ("text/plain; charset=utf-8", &[b'a', 0xc3, 0x28, b'z']),
        ("application/json", b"{ \"a\" : 1 ,}"),
        ("application/octet-stream", &[0, 0xff, 0xfe, 0x80]),
    ];
    for binding in [Binding::Http, Binding::Kafka, Binding::Amqp] {
        for (media, body) in bodies {
            let mut event = WireEvent::new("1", "/s", "t");
            event.data_content_type = Some(media.to_string());
            event.data = Some(Data::Binary(body.to_vec()));
            let carried = binding.write(&event, Mode::Binary).expect("written");
            assert_eq!(carried.body, body, "{binding:?} {media}");
            let back = binding.read(&carried).expect("read");
            assert_eq!(
                back.data,
                Some(Data::Binary(body.to_vec())),
                "{binding:?} {media}"
            );
            assert_eq!(back, event);
        }
    }
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

/// HTTP binding section 3.1.4, the binary-mode example.
#[test]
fn the_http_binary_example_reads_with_headers_in_any_case() {
    let carried = Carried {
        content_type: Some("application/json; charset=utf-8".to_string()),
        headers: [
            ("ce-specversion", "1.0"),
            ("ce-type", "com.example.someevent"),
            ("CE-Time", "2018-04-05T03:56:24Z"),
            ("ce-id", "1234-1234-1234"),
            ("ce-source", "/mycontext/subcontext"),
            ("Host", "webhook.example.com"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .to_vec(),
        body: br#"{"appinfoA":"abc"}"#.to_vec(),
    };

    let event = Binding::Http.read(&carried).expect("a WireEvent");

    assert_eq!(event.source, "/mycontext/subcontext");
    assert_eq!(event.time.as_deref(), Some("2018-04-05T03:56:24Z"));
    assert_eq!(
        event.data_content_type.as_deref(),
        Some("application/json; charset=utf-8")
    );
    assert_eq!(
        event.data,
        Some(Data::Binary(br#"{"appinfoA":"abc"}"#.to_vec())),
        "the body stays bytes"
    );
    assert_eq!(
        event.json_data().expect("declared JSON"),
        Some(json!({"appinfoA": "abc"}))
    );
    assert!(!event.extensions.contains_key("host"), "not a ce- header");
}

/// HTTP binding section 3.1.3.2: `Euro € 😀` as a header.
#[test]
fn an_http_header_is_percent_encoded_as_the_binding_says() {
    let mut event = WireEvent::new("1", "/s", "t");
    event.subject = Some("Euro € 😀".to_string());

    let carried = Binding::Http.write(&event, Mode::Binary).expect("written");
    let subject = carried
        .headers
        .iter()
        .find(|(name, _)| name == "ce-subject")
        .map(|(_, value)| value.as_str());

    assert_eq!(subject, Some("Euro%20%E2%82%AC%20%F0%9F%98%80"));
    assert_eq!(Binding::Http.read(&carried).expect("read"), event);
}

/// Kafka binding section 3.2.4, the binary-mode example.
#[test]
fn the_kafka_binary_example_reads() {
    let carried = Carried {
        content_type: Some("application/avro".to_string()),
        headers: [
            ("ce_specversion", "1.0"),
            ("ce_type", "com.example.someevent"),
            ("ce_source", "/mycontext/subcontext"),
            ("ce_id", "1234-1234-1234"),
            ("ce_time", "2018-04-05T03:56:24Z"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .to_vec(),
        body: vec![0x02, 0x06],
    };

    let event = Binding::Kafka.read(&carried).expect("a WireEvent");

    assert_eq!(event.kind, "com.example.someevent");
    assert_eq!(event.data, Some(Data::Binary(vec![0x02, 0x06])));
    assert!(
        Binding::Http.read(&carried).is_err(),
        "ce_ is Kafka's, not HTTP's"
    );
}

/// AMQP binding section 3.1.4, the binary-mode example, and the prefix
/// before 1.0.2.
#[test]
fn the_amqp_binary_example_reads_with_either_prefix() {
    for prefix in ["cloudEvents_", "cloudEvents:"] {
        let carried = Carried {
            content_type: Some("application/json; charset=utf-8".to_string()),
            headers: [
                ("specversion", "1.0"),
                ("type", "com.example.someevent"),
                ("time", "2018-04-05T03:56:24Z"),
                ("id", "1234-1234-1234"),
                ("source", "/mycontext/subcontext"),
            ]
            .map(|(name, value)| (format!("{prefix}{name}"), value.to_string()))
            .to_vec(),
            body: b"{}".to_vec(),
        };

        let event = Binding::Amqp.read(&carried).expect("a WireEvent");

        assert_eq!(event.id, "1234-1234-1234", "{prefix}");
    }
}

#[test]
fn a_message_without_specversion_is_no_wire_event() {
    let carried = Carried {
        content_type: Some("text/plain".to_string()),
        headers: vec![("ce-id".to_string(), "1".to_string())],
        body: b"hello".to_vec(),
    };

    assert!(Binding::Http.read(&carried).is_err());
    let batch = Carried {
        content_type: Some("application/cloudevents-batch+json".to_string()),
        ..Carried::default()
    };
    assert!(Binding::Http.read(&batch).is_err());
}

#[test]
fn an_xmip_event_rides_every_binding_in_both_modes_and_comes_back_itself() {
    let scope = format!(
        "{}/send/billing",
        configure::fixture::test_cluster().node_scope(0)
    );
    let event = Event::completed(Stage::Send, Outcome::Failure, scope.as_str())
        .in_journey(JourneyId::new(11))
        .of_message(MessageId::new(12))
        .at_endpoint("https://billing.example/in")
        .by_module("xmip-core-transport-http")
        .on_artifact("billing")
        .about(PartyId::new(13))
        .saying("status", "503");
    let wire_event = WireEvent::from_event(&event);

    assert_eq!(wire_event.kind, "se.xmip.send.failure");
    assert_eq!(wire_event.source, scope);
    assert_eq!(wire_event.subject.as_deref(), Some("billing"));
    assert_eq!(wire_event.event().expect("an Xmip Event"), event);

    for binding in [Binding::Http, Binding::Kafka, Binding::Amqp] {
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
                "{binding:?} {mode:?}"
            );
        }
    }
}
