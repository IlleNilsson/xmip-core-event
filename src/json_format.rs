//! The wire event's JSON event format 1.0 (ADR-0065 clause 3), written and
//! read here once: [`WireEvent::json`] writes it, [`WireEvent::read_json`]
//! reads it, and [`WireEvent::json_data`] is the one place data is read as
//! JSON.
//!
//! Data is decoded as JSON only where its `datacontenttype` declares JSON
//! (ADR-0038, the 2026-09-26 amendment: a payload is bytes). Bytes under any
//! other type, or none, stay bytes and travel as `data_base64`; text under a
//! type that is not JSON travels in `data` as a string.

use std::collections::BTreeMap;

use codec::base64;
use serde_json::{Map, Value};

use crate::EventError;
use crate::wire::{CONTEXT, Data, Extension, SPEC_VERSION, WireEvent, refused};

/// The media type of the JSON event format.
pub const JSON_FORMAT: &str = "application/cloudevents+json";

/// The media type of a batch in the JSON event format.
pub const JSON_BATCH_FORMAT: &str = "application/cloudevents-batch+json";

impl WireEvent {
    /// The event in the JSON event format (JSON format section 3.1.1): data
    /// that is JSON — [`Self::json_data`] — in `data` as JSON; text under a
    /// type that is not JSON in `data` as a string; bytes under any other
    /// type, or none, in `data_base64`.
    ///
    /// # Errors
    /// What [`Self::json_data`] refuses.
    pub fn json(&self) -> Result<Value, EventError> {
        let mut object = Map::new();
        for (name, value) in self.attributes() {
            let typed = match self.extensions.get(&name) {
                Some(Extension::Integer(number)) => Value::from(*number),
                Some(Extension::Boolean(truth)) => Value::from(*truth),
                _ => Value::String(value),
            };
            object.insert(name, typed);
        }
        let member = match (self.json_data()?, &self.data) {
            (Some(value), _) => Some(("data", value)),
            (None, Some(Data::Text(text))) => Some(("data", Value::String(text.clone()))),
            (None, Some(Data::Binary(bytes))) => {
                Some(("data_base64", base64::encode(bytes).into()))
            }
            (None, _) => None,
        };
        if let Some((name, value)) = member {
            object.insert(name.to_string(), value);
        }
        Ok(Value::Object(object))
    }

    /// The data as JSON, where it is JSON: [`Data::Json`] as it is, and text
    /// or bytes a JSON `datacontenttype` declares, parsed. Nothing is parsed
    /// that the content type does not declare JSON.
    ///
    /// # Errors
    /// [`Data::Json`] under a `datacontenttype` that is not JSON, or text or
    /// bytes declared JSON that are not.
    pub fn json_data(&self) -> Result<Option<Value>, EventError> {
        let declared = self.data_content_type.as_deref();
        match (&self.data, declared) {
            (Some(Data::Json(_)), Some(media)) if !is_json(media) => refused(format!(
                "JSON data under a datacontenttype that is not JSON: {media}"
            )),
            (Some(Data::Json(value)), _) => Ok(Some(value.clone())),
            (Some(data @ (Data::Text(_) | Data::Binary(_))), Some(media)) if is_json(media) => {
                serde_json::from_slice(&data.bytes())
                    .map(Some)
                    .map_err(|error| {
                        EventError::new(format!("data declared {media} that is not JSON: {error}"))
                    })
            }
            _ => Ok(None),
        }
    }

    /// The event a JSON event format document holds.
    ///
    /// `data_base64` is decoded to bytes; `data` under a JSON
    /// `datacontenttype`, or none, is JSON; `data` under any other type is
    /// text, and must be a JSON string (JSON format section 3.1.2).
    ///
    /// # Errors
    /// Not an object, a context attribute of the wrong JSON type, `data` and
    /// `data_base64` both, `data_base64` that is not base64, `data` that is
    /// not a string under a type that is not JSON, or what
    /// [`Self::checked`] refuses.
    pub fn read_json(value: &Value) -> Result<Self, EventError> {
        let Value::Object(object) = value else {
            return refused("an event that is not a JSON object".to_string());
        };
        let mut attributes = Vec::new();
        let mut extensions = BTreeMap::new();
        for (name, value) in object {
            match (name.as_str(), value) {
                ("data" | "data_base64", _) | (_, Value::Null) => {}
                (_, Value::String(text)) => attributes.push((name.clone(), text.clone())),
                (context, _) if CONTEXT.contains(&context) => {
                    return refused(format!("{context} that is not a string"));
                }
                (_, Value::Bool(truth)) => {
                    extensions.insert(name.clone(), Extension::Boolean(*truth));
                }
                (_, Value::Number(number)) => {
                    let Some(integer) = number.as_i64() else {
                        return refused(format!("{name} that is not an integer"));
                    };
                    extensions.insert(name.clone(), Extension::Integer(integer));
                }
                _ => return refused(format!("{name} that is neither text, integer nor boolean")),
            }
        }
        let data = match (object.get("data"), object.get("data_base64")) {
            (Some(Value::Null) | None, Some(Value::Null) | None) => None,
            (Some(_), Some(_)) => return refused("data and data_base64 both".to_string()),
            (None, Some(Value::String(encoded))) => {
                let bytes = base64::decode(encoded)
                    .map_err(|error| EventError::new(format!("data_base64: {error}")))?;
                Some(Data::Binary(bytes))
            }
            (None, Some(_)) => return refused("data_base64 that is not a string".to_string()),
            (Some(value), None) if json_media(object.get("datacontenttype")) => {
                Some(Data::Json(value.clone()))
            }
            (Some(Value::String(text)), None) => Some(Data::Text(text.clone())),
            (Some(_), None) => {
                return refused("data that is not a string under a type that is not JSON".into());
            }
        };
        let mut event = Self::from_attributes(attributes, data)?;
        event.extensions.extend(extensions);
        event.checked(Some(SPEC_VERSION))
    }
}

/// Whether a `datacontenttype` says JSON: absent, `application/json`, or
/// any `+json` type (JSON format section 3.1).
fn json_media(content_type: Option<&Value>) -> bool {
    match content_type {
        None | Some(Value::Null) => true,
        Some(Value::String(media)) => is_json(media),
        Some(_) => false,
    }
}

/// Whether a media type is JSON.
#[must_use]
pub fn is_json(media: &str) -> bool {
    let essence = media
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    essence == "application/json" || essence.ends_with("+json")
}
