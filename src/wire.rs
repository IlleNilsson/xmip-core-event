//! A `WireEvent`: an Event as it travels between processes — the envelope
//! of the standard this follows, CNCF `CloudEvents` 1.0 (ADR-0065 clause 3),
//! held and checked here once. Its JSON format and protocol bindings are
//! that standard's; nothing outside this crate names it.
//!
//! The context attributes — `specversion`, `id`, `source`, `type`, the four
//! optional ones and any extension — and the data, as JSON, text or bytes.
//! [`WireEvent::checked`] refuses what the specification refuses: another
//! `specversion`, a missing or empty required attribute, an extension name
//! outside `a-z0-9`. The JSON event format is `json_format.rs`'s; how a
//! protocol carries one is `binding.rs`'s; what an Xmip Event becomes is
//! [`WireEvent::from_event`]'s.

use std::collections::BTreeMap;

use codec::civil::{read_rfc3339, rfc3339_nanos};
use serde_json::{Map, Value};

use crate::{Event, EventError};

/// The one version this reads and writes.
pub const SPEC_VERSION: &str = "1.0";

/// The attributes the specification defines, in the order they are written.
pub const CONTEXT: [&str; 8] = [
    "specversion",
    "id",
    "source",
    "type",
    "datacontenttype",
    "dataschema",
    "subject",
    "time",
];

/// An event's data. What arrives as a body — a binding's binary mode, or
/// `data_base64` — is [`Data::Binary`] and stays bytes (ADR-0038, the
/// 2026-09-26 amendment); [`Data::Json`] and [`Data::Text`] are what the
/// JSON format's `data` member holds, or what a writer built.
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    Json(Value),
    Text(String),
    Binary(Vec<u8>),
}

impl Data {
    /// The data as a body carries it: bytes as they are, text as its UTF-8,
    /// JSON as its serialization.
    #[must_use]
    pub fn bytes(&self) -> Vec<u8> {
        match self {
            Self::Json(value) => value.to_string().into_bytes(),
            Self::Text(text) => text.as_bytes().to_vec(),
            Self::Binary(bytes) => bytes.clone(),
        }
    }
}

/// An extension attribute's value, as the JSON format types it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Extension {
    Text(String),
    Integer(i64),
    Boolean(bool),
}

/// One `WireEvent`.
#[derive(Clone, Debug, PartialEq)]
pub struct WireEvent {
    pub id: String,
    pub source: String,
    /// The `type` attribute.
    pub kind: String,
    pub data_content_type: Option<String>,
    pub data_schema: Option<String>,
    pub subject: Option<String>,
    /// RFC 3339, as the event carries it.
    pub time: Option<String>,
    pub extensions: BTreeMap<String, Extension>,
    pub data: Option<Data>,
}

impl WireEvent {
    /// An event with its three required attributes and nothing else.
    #[must_use]
    pub fn new(id: impl Into<String>, source: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            source: source.into(),
            kind: kind.into(),
            data_content_type: None,
            data_schema: None,
            subject: None,
            time: None,
            extensions: BTreeMap::new(),
            data: None,
        }
    }

    /// Every context attribute but `data`, each as its canonical string:
    /// what a binding in binary mode carries in headers.
    #[must_use]
    pub fn attributes(&self) -> Vec<(String, String)> {
        let optional = [
            ("datacontenttype", &self.data_content_type),
            ("dataschema", &self.data_schema),
            ("subject", &self.subject),
            ("time", &self.time),
        ];
        let mut out = vec![
            ("specversion".to_string(), SPEC_VERSION.to_string()),
            ("id".to_string(), self.id.clone()),
            ("source".to_string(), self.source.clone()),
            ("type".to_string(), self.kind.clone()),
        ];
        out.extend(
            optional
                .into_iter()
                .filter_map(|(name, value)| Some((name.to_string(), value.clone()?))),
        );
        out.extend(self.extensions.iter().map(|(name, value)| {
            let text = match value {
                Extension::Text(text) => text.clone(),
                Extension::Integer(number) => number.to_string(),
                Extension::Boolean(truth) => truth.to_string(),
            };
            (name.clone(), text)
        }));
        out
    }

    /// The event from its attributes as canonical strings — a binding in
    /// binary mode, read — and its data. Every extension is text.
    ///
    /// # Errors
    /// What [`Self::checked`] refuses.
    pub fn from_attributes(
        attributes: impl IntoIterator<Item = (String, String)>,
        data: Option<Data>,
    ) -> Result<Self, EventError> {
        let mut event = Self::new("", "", "");
        let mut version = None;
        for (name, value) in attributes {
            match name.as_str() {
                "specversion" => version = Some(value),
                "id" => event.id = value,
                "source" => event.source = value,
                "type" => event.kind = value,
                "datacontenttype" => event.data_content_type = Some(value),
                "dataschema" => event.data_schema = Some(value),
                "subject" => event.subject = Some(value),
                "time" => event.time = Some(value),
                _ => {
                    event.extensions.insert(name, Extension::Text(value));
                }
            }
        }
        event.data = data;
        event.checked(version.as_deref())
    }

    /// The event, if `version` is this one and every attribute is as the
    /// specification says it must be.
    ///
    /// # Errors
    /// Another `specversion`, an empty required attribute, a `time` that is
    /// not RFC 3339, or an extension name outside `a-z0-9`.
    pub fn checked(self, version: Option<&str>) -> Result<Self, EventError> {
        match version {
            Some(SPEC_VERSION) => {}
            Some(other) => return refused(format!("specversion {other} is not 1.0")),
            None => return refused("an event with no specversion".to_string()),
        }
        for (name, value) in [
            ("id", &self.id),
            ("source", &self.source),
            ("type", &self.kind),
        ] {
            if value.is_empty() {
                return refused(format!("an event with no {name}"));
            }
        }
        if let Some(time) = &self.time
            && read_rfc3339(time).is_none()
        {
            return refused(format!("a time that is not RFC 3339: {time}"));
        }
        if let Some(name) = self.extensions.keys().find(|name| !attribute_name(name)) {
            return refused(format!("an attribute name outside a-z0-9: {name}"));
        }
        Ok(self)
    }

    /// An Xmip Event as a `WireEvent`: the scope is the source, the
    /// Artifact the subject, the time to the nanosecond, the rest as `xmip`
    /// extensions, and the diagnostics as JSON data.
    #[must_use]
    pub fn from_event(event: &Event) -> Self {
        let mut wire_event = Self::new(
            event.id.to_string(),
            event.scope.clone(),
            event.kind.clone(),
        );
        wire_event.subject.clone_from(&event.artifact);
        wire_event.time = Some(rfc3339_nanos(event.time_unix_nanos));
        let identities = [
            ("xmipjourney", event.journey.map(|id| id.to_string())),
            ("xmipmessage", event.message.map(|id| id.to_string())),
            ("xmipstream", event.stream.map(|id| id.to_string())),
            ("xmipparty", event.party.map(|id| id.to_string())),
            ("xmipendpoint", event.endpoint.clone()),
            ("xmipmodule", event.module.clone()),
        ];
        let named = [
            ("xmipaction", Some(event.action.name().to_string())),
            ("xmipoutcome", Some(event.outcome.word().to_string())),
        ];
        for (name, value) in named.into_iter().chain(identities) {
            if let Some(value) = value {
                wire_event
                    .extensions
                    .insert(name.to_string(), Extension::Text(value));
            }
        }
        if !event.diagnostics.is_empty() {
            wire_event.data_content_type = Some("application/json".to_string());
            let object: Map<String, Value> = event
                .diagnostics
                .iter()
                .map(|(name, value)| (name.clone(), Value::String(value.clone())))
                .collect();
            wire_event.data = Some(Data::Json(Value::Object(object)));
        }
        wire_event
    }

    /// The Xmip Event this `WireEvent` carries, as [`Self::from_event`]
    /// wrote it.
    ///
    /// # Errors
    /// An id that is not a UUID, no `xmipaction` or `xmipoutcome` naming a
    /// stage and an outcome, a time that is not RFC 3339, an identifier
    /// extension that is not a UUID, or what [`Self::json_data`] refuses.
    pub fn event(&self) -> Result<Event, EventError> {
        let text = |name: &str| match self.extensions.get(name) {
            Some(Extension::Text(text)) => Some(text.as_str()),
            _ => None,
        };
        let uuid = |name: &str| -> Result<Option<u128>, EventError> {
            text(name)
                .map(|value| value.parse::<xcore::EventId>().map(xcore::EventId::value))
                .transpose()
                .map_err(|error| EventError::new(format!("{name}: {error}")))
        };
        let action = text("xmipaction")
            .and_then(node::Stage::named)
            .ok_or_else(|| EventError::new("no xmipaction naming a stage"))?;
        let outcome = text("xmipoutcome")
            .and_then(crate::outcome::Outcome::named)
            .ok_or_else(|| EventError::new("no xmipoutcome naming an outcome"))?;
        let mut event = Event::raised(self.kind.clone(), action, outcome, self.source.clone());
        event.id = self
            .id
            .parse()
            .map_err(|error| EventError::new(format!("id: {error}")))?;
        if let Some(time) = &self.time {
            event.time_unix_nanos = read_rfc3339(time)
                .ok_or_else(|| EventError::new(format!("a time that is not RFC 3339: {time}")))?;
        }
        event.journey = uuid("xmipjourney")?.map(xcore::JourneyId::new);
        event.message = uuid("xmipmessage")?.map(xcore::MessageId::new);
        event.stream = uuid("xmipstream")?.map(xcore::StreamId::new);
        event.party = uuid("xmipparty")?.map(xcore::PartyId::new);
        event.endpoint = text("xmipendpoint").map(str::to_string);
        event.module = text("xmipmodule").map(str::to_string);
        event.artifact.clone_from(&self.subject);
        if let Some(Value::Object(object)) = self.json_data()? {
            event.diagnostics = object
                .iter()
                .filter_map(|(name, value)| Some((name.clone(), value.as_str()?.to_string())))
                .collect();
        }
        Ok(event)
    }
}

/// A name the specification allows an attribute: `a-z` and `0-9` only.
fn attribute_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

/// A refusal saying `said`.
pub(crate) fn refused<T>(said: String) -> Result<T, EventError> {
    Err(EventError::new(said))
}
