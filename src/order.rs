//! An operator's act left for a node that a surface reaches through its
//! publication only (ADR-0065, amendment 2026-09-29).
//!
//! A surface reading a live node applies an act through the runtime's
//! library, in the node's own process ([`crate::hub::Hub::act`]). A surface
//! reading a snapshot touches no node: the publication says where its
//! publisher takes orders (`observe::Publication::orders`), the surface
//! leaves one there, and the node takes it at its next look and applies it
//! to its hub as any act is applied. The file, its place and its shape are
//! written here once: `<orders>/<node name>/<unix nanos>-<id>-<act>.toml`,
//! written whole beside itself and renamed into place, so a node never
//! reads half of one.

use std::fs;
use std::path::{Path, PathBuf};

use observe::Scope;
use serde::{Deserialize, Serialize};

use crate::EventError;
use crate::act::Act;

/// One act, for one subscription, on one node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Order {
    /// The node whose hub holds the subscription: `xmip:///<cluster>/node/<name>`.
    pub node: String,
    /// The subscription's number in that hub.
    pub id: u64,
    pub act: Act,
    /// Who acted, for the audit record.
    pub who: String,
}

#[derive(Serialize, Deserialize)]
struct Document {
    node: String,
    id: u64,
    act: String,
    who: String,
}

impl Order {
    /// Leave this order in `orders` for its node to take; the file written.
    ///
    /// # Errors
    /// REFUSED when the order names no node; the reason when the file could
    /// not be written.
    pub fn leave(&self, orders: &Path) -> Result<PathBuf, EventError> {
        let place = place(orders, &self.node)?;
        fs::create_dir_all(&place).map_err(|error| failed(&place, &error))?;
        let name = format!(
            "{}-{}-{}.toml",
            observe::now_unix_nanos(),
            self.id,
            self.act.word()
        );
        let document = Document {
            node: self.node.clone(),
            id: self.id,
            act: self.act.word().to_string(),
            who: self.who.clone(),
        };
        let text =
            toml::to_string(&document).map_err(|error| EventError::new(error.to_string()))?;
        let part = place.join(format!("{name}.part"));
        let file = place.join(name);
        fs::write(&part, text).map_err(|error| failed(&part, &error))?;
        fs::rename(&part, &file).map_err(|error| failed(&file, &error))?;
        Ok(file)
    }

    /// Every order left in `orders` for the node at `node`, oldest first,
    /// each file removed as it is taken. A file that is no order is removed
    /// too, and said rather than applied.
    #[must_use]
    pub fn take(orders: &Path, node: &str) -> Vec<Result<Self, String>> {
        let Ok(place) = place(orders, node) else {
            return Vec::new();
        };
        let Ok(entries) = fs::read_dir(&place) else {
            return Vec::new();
        };
        let mut files: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "toml")
            })
            .collect();
        files.sort();

        files
            .into_iter()
            .map(|file| {
                let read = fs::read_to_string(&file);
                let _ = fs::remove_file(&file);
                read.map_err(|error| error.to_string())
                    .and_then(|text| Self::read(&text))
                    .map_err(|problem| format!("{}: {problem}", file.display()))
            })
            .collect()
    }

    fn read(text: &str) -> Result<Self, String> {
        let document: Document = toml::from_str(text).map_err(|error| error.to_string())?;
        let act = Act::named(&document.act)
            .ok_or_else(|| format!("REFUSED: '{}' is no act on a subscription", document.act))?;
        Ok(Self {
            node: document.node,
            id: document.id,
            act,
            who: document.who,
        })
    }
}

/// Where a node's orders lie: beneath `orders`, by the node's name.
fn place(orders: &Path, node: &str) -> Result<PathBuf, EventError> {
    Scope::new(node)
        .node()
        .filter(|name| !name.is_empty())
        .map(|name| orders.join(name))
        .ok_or_else(|| EventError::new(format!("REFUSED: '{node}' names no node")))
}

fn failed(path: &Path, error: &std::io::Error) -> EventError {
    EventError::new(format!("could not write {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn orders(name: &str) -> PathBuf {
        let at =
            std::env::temp_dir().join(format!("xmip-event-order-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&at);
        at
    }

    #[test]
    fn an_order_left_is_taken_once_by_its_node_and_by_no_other() {
        let at = orders("once");
        let pause = Order {
            node: "xmip:///CT/node/R1".to_string(),
            id: 3,
            act: Act::Pause,
            who: "ilian".to_string(),
        };
        let resume = Order {
            act: Act::Resume,
            ..pause.clone()
        };
        pause.leave(&at).expect("left");
        resume.leave(&at).expect("left");

        assert!(Order::take(&at, "xmip:///CT/node/S1").is_empty());
        let taken: Vec<Order> = Order::take(&at, "xmip:///CT/node/R1")
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("orders");
        assert_eq!(taken, vec![pause, resume], "oldest first");
        assert!(
            Order::take(&at, "xmip:///CT/node/R1").is_empty(),
            "taken once"
        );
        let _ = fs::remove_dir_all(&at);
    }

    #[test]
    fn a_file_that_is_no_order_is_said_and_removed_and_no_node_is_refused() {
        let at = orders("stranger");
        let place = at.join("R1");
        fs::create_dir_all(&place).expect("made");
        fs::write(
            place.join("1-1-sulk.toml"),
            "node = \"n\"\nid = 1\nact = \"sulk\"\nwho = \"w\"\n",
        )
        .expect("written");

        let taken = Order::take(&at, "xmip:///CT/node/R1");
        assert_eq!(taken.len(), 1);
        assert!(
            taken[0]
                .as_ref()
                .is_err_and(|said| said.contains("'sulk' is no act"))
        );
        assert!(Order::take(&at, "xmip:///CT/node/R1").is_empty());

        let nowhere = Order {
            node: "xmip:///CT".to_string(),
            id: 1,
            act: Act::Remove,
            who: "w".to_string(),
        };
        assert!(
            nowhere
                .leave(&at)
                .is_err_and(|error| error.to_string().contains("names no node"))
        );
        let _ = fs::remove_dir_all(&at);
    }
}
