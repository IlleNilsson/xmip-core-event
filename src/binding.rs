//! How a protocol carries a `WireEvent`: the standard's two modes, 1.0.2,
//! written and read here once over the protocol binding's [`Rules`]
//! (ADR-0065 clause 3, amendment 2026-11-14).
//!
//! **Structured** mode carries the whole event as one JSON format document
//! under `application/cloudevents+json`. **Binary** mode carries the data as
//! the body — bytes, read and written as they are, never decoded here —
//! under its own `datacontenttype` and every other attribute as a header,
//! named with the binding's prefix and its value escaped as the binding
//! says.
//!
//! What a protocol adds is its [`Rules`]: the prefix, how a header's name
//! is matched and how a value is escaped. They are the transport's of that
//! name, in its `event_wire.rs`, never this crate's, and a
//! [`Wire`](crate::wire::Wire) answers its own, so no wire carries by
//! another protocol's rules.
//!
//! What crosses is a [`Carried`]: a content type, headers and a body. Each
//! transport puts the three where its protocol puts them — the HTTP
//! transport in its request, Kafka in its record, AMQP in its content
//! header — and nothing here opens a connection.

use std::borrow::Cow;

use serde_json::Value;

use crate::EventError;
use crate::json_format::{JSON_BATCH_FORMAT, JSON_FORMAT};
use crate::wire::{Data, WireEvent};

/// The content type a structured event travels under.
pub const STRUCTURED: &str = "application/cloudevents+json; charset=UTF-8";

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

/// What a protocol binding says of binary mode: the prefix an attribute's
/// header carries, how a header's name is matched, and how its value is
/// escaped. A transport implements it beside its
/// [`Wire`](crate::wire::Wire); [`Rules::write`] and [`Rules::read`] are
/// the two modes over any binding's rules, written once.
pub trait Rules {
    /// The prefix an attribute's header is written with.
    fn prefix(&self) -> &'static str;

    /// The attribute `header` names, if it is one: the name after the
    /// prefix, matched as the protocol matches names. By default the
    /// prefix, exactly.
    fn attribute<'h>(&self, header: &'h str) -> Option<&'h str> {
        header.strip_prefix(self.prefix())
    }

    /// A header value as the protocol writes it: by default as it is.
    fn escaped<'v>(&self, value: &'v str) -> Cow<'v, str> {
        Cow::Borrowed(value)
    }

    /// A header value read back: by default as it came.
    ///
    /// # Errors
    /// What the protocol's escaping refuses.
    fn unescaped<'v>(&self, value: &'v str) -> Result<Cow<'v, str>, EventError> {
        Ok(Cow::Borrowed(value))
    }

    /// `event` as this protocol carries it in `mode`.
    ///
    /// In binary mode the data is the body as [`Data::bytes`] gives it.
    ///
    /// # Errors
    /// In structured mode, what `WireEvent::json` refuses.
    fn write(&self, event: &WireEvent, mode: Mode) -> Result<Carried, EventError> {
        if mode == Mode::Structured {
            let body = event.json()?.to_string().into_bytes();
            return Ok(Carried {
                content_type: Some(STRUCTURED.to_string()),
                headers: Vec::new(),
                body,
            });
        }
        let prefix = self.prefix();
        let headers = event
            .attributes()
            .into_iter()
            .filter(|(name, _)| name != "datacontenttype")
            .map(|(name, value)| {
                let value = match self.escaped(&value) {
                    Cow::Borrowed(_) => value,
                    Cow::Owned(escaped) => escaped,
                };
                (format!("{prefix}{name}"), value)
            })
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
    fn read(&self, carried: &Carried) -> Result<WireEvent, EventError> {
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
                let value = self.unescaped(value)?.into_owned();
                attributes.push((attribute.to_ascii_lowercase(), value));
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
}

#[allow(clippy::needless_pass_by_value)]
fn json_error(error: serde_json::Error) -> EventError {
    EventError::new(format!("JSON: {error}"))
}
