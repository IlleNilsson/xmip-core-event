//! How HTTP, Kafka and AMQP carry a `WireEvent`: the standard's three
//! protocol bindings, 1.0.2, each in its two modes, written and read here
//! once (ADR-0065 clause 3).
//!
//! **Structured** mode carries the whole event as one JSON format document
//! under `application/cloudevents+json`. **Binary** mode carries the data as
//! the body — bytes, read and written as they are, never decoded here —
//! under its own `datacontenttype` and every other attribute as a
//! header: `ce-` on HTTP, percent-encoded where the binding says; `ce_` on
//! Kafka; `cloudEvents_` among AMQP's application properties (the 1.0.2
//! prefix; `cloudEvents:`, the one before it, is read too).
//!
//! What crosses is a [`Carried`]: a content type, headers and a body. Each
//! transport puts the three where its protocol puts them — the HTTP
//! transport in its request, Kafka in its record, AMQP in its content
//! header — and nothing here opens a connection.

use net::percent::{decode_strict, encode_keeping};
use serde_json::Value;

use crate::EventError;
use crate::json_format::{JSON_BATCH_FORMAT, JSON_FORMAT};
use crate::wire::{Data, WireEvent};

/// The content type a structured event travels under.
pub const STRUCTURED: &str = "application/cloudevents+json; charset=UTF-8";

/// A protocol a `WireEvent` rides.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Binding {
    Http,
    Kafka,
    Amqp,
}

/// Which of a binding's two modes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Structured,
    Binary,
}

/// A `WireEvent` as a protocol carries it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Carried {
    /// The content type: the event format's in structured mode, the data's
    /// in binary mode.
    pub content_type: Option<String>,
    /// The attributes as headers, in binary mode; none in structured mode.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Binding {
    /// The prefix an attribute's header carries in binary mode.
    #[must_use]
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::Http => "ce-",
            Self::Kafka => "ce_",
            Self::Amqp => "cloudEvents_",
        }
    }

    /// `event` as this protocol carries it in `mode`.
    ///
    /// In binary mode the data is the body as [`Data::bytes`] gives it.
    ///
    /// # Errors
    /// In structured mode, what `WireEvent::json` refuses.
    pub fn write(self, event: &WireEvent, mode: Mode) -> Result<Carried, EventError> {
        if mode == Mode::Structured {
            let body = event.json()?.to_string().into_bytes();
            return Ok(Carried {
                content_type: Some(STRUCTURED.to_string()),
                headers: Vec::new(),
                body,
            });
        }
        let headers = event
            .attributes()
            .into_iter()
            .filter(|(name, _)| name != "datacontenttype")
            .map(|(name, value)| (format!("{}{name}", self.prefix()), self.escaped(&value)))
            .collect();
        Ok(Carried {
            content_type: event.data_content_type.clone(),
            headers,
            body: event.data.as_ref().map(Data::bytes).unwrap_or_default(),
        })
    }

    /// The `WireEvent` `carried` holds, in whichever mode it came. In
    /// binary mode the body is the data, as bytes, whatever its content
    /// type: [`WireEvent::json_data`] reads JSON out of it where the type
    /// declares JSON.
    ///
    /// # Errors
    /// A batch, which one event is not; a structured body that is not the
    /// JSON format; in binary mode, no `specversion` header — not a
    /// `WireEvent` at all — or a header this binding's escaping refuses;
    /// and what `WireEvent::checked` refuses.
    pub fn read(self, carried: &Carried) -> Result<WireEvent, EventError> {
        let media = carried.content_type.as_deref().unwrap_or("");
        let essence = media
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        if essence == JSON_BATCH_FORMAT {
            return Err(EventError::new("a batch of events, where one was expected"));
        }
        if essence == JSON_FORMAT {
            let value: Value = serde_json::from_slice(&carried.body).map_err(json_error)?;
            return WireEvent::read_json(&value);
        }
        if essence.starts_with("application/cloudevents") {
            return Err(EventError::new(format!(
                "an event format this does not read: {media}"
            )));
        }
        let mut attributes = Vec::new();
        for (name, value) in &carried.headers {
            if let Some(attribute) = self.attribute(name) {
                attributes.push((attribute, self.unescaped(value)?));
            }
        }
        if !attributes.iter().any(|(name, _)| name == "specversion") {
            return Err(EventError::new("no specversion header: not a wire event"));
        }
        if let Some(media) = &carried.content_type {
            attributes.push(("datacontenttype".to_string(), media.clone()));
        }
        let data = (!carried.body.is_empty() || carried.content_type.is_some())
            .then(|| Data::Binary(carried.body.clone()));
        WireEvent::from_attributes(attributes, data)
    }

    /// The attribute a header names, if it is one: the prefix matched as
    /// the protocol matches names — HTTP's without regard to case.
    fn attribute(self, header: &str) -> Option<String> {
        let prefixes: &[&str] = match self {
            Self::Http => &["ce-"],
            Self::Kafka => &["ce_"],
            Self::Amqp => &["cloudEvents_", "cloudEvents:"],
        };
        prefixes.iter().find_map(|prefix| {
            let head = header.get(..prefix.len())?;
            let same = match self {
                Self::Http => head.eq_ignore_ascii_case(prefix),
                Self::Kafka | Self::Amqp => head == *prefix,
            };
            same.then(|| header[prefix.len()..].to_ascii_lowercase())
        })
    }

    /// A header value as the binding writes it: on HTTP, space, `"`, `%`
    /// and anything outside printable ASCII percent-encoded (HTTP binding
    /// section 3.1.3.2); elsewhere as it is.
    fn escaped(self, value: &str) -> String {
        match self {
            Self::Http => encode_keeping(value, |byte| {
                (0x21..=0x7e).contains(&byte) && !matches!(byte, b'"' | b'%')
            }),
            Self::Kafka | Self::Amqp => value.to_string(),
        }
    }

    /// A header value read back.
    fn unescaped(self, value: &str) -> Result<String, EventError> {
        match self {
            Self::Http => {
                let bytes = decode_strict(value.as_bytes())
                    .map_err(|error| EventError::new(error.to_string()))?;
                String::from_utf8(bytes)
                    .map_err(|_| EventError::new("a header that is not UTF-8 once decoded"))
            }
            Self::Kafka | Self::Amqp => Ok(value.to_string()),
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn json_error(error: serde_json::Error) -> EventError {
    EventError::new(format!("JSON: {error}"))
}
