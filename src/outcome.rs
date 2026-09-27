//! How an action ended: the outcomes runtime-model section 17 names, every
//! one of which produces an Event.
//!
//! The words are written here and nowhere else. The C boundary carries each
//! as an integer (`xmip_operate.h` section 11, `XmipOutcome`), the wire
//! event as the word (`xmipoutcome`), and a subscription filters by either.

use std::fmt;

/// How a Receive, Process or Send action ended.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Outcome {
    /// It did what it was asked.
    Success,
    /// It could not, and said why.
    Failure,
    /// A gate refused it: authentication, authorization, a contract.
    Rejection,
    /// It waits for something outside it: a human, a correlated Message.
    Waiting,
    /// An operator paused it.
    Pause,
    /// It ran out of time.
    Timeout,
    /// The resilience guards gave up on it.
    ExhaustedRetries,
    /// An operator dismissed it.
    Dismissal,
}

impl Outcome {
    /// Every outcome, in the order the header numbers them (the runtime's
    /// `ffi/event.rs` holds the numbers against `xmip_operate.h`).
    pub const ALL: [Outcome; 8] = [
        Self::Success,
        Self::Failure,
        Self::Rejection,
        Self::Waiting,
        Self::Pause,
        Self::Timeout,
        Self::ExhaustedRetries,
        Self::Dismissal,
    ];

    /// The word an outcome is written as, in a `WireEvent` and a filter.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Rejection => "rejection",
            Self::Waiting => "waiting",
            Self::Pause => "pause",
            Self::Timeout => "timeout",
            Self::ExhaustedRetries => "exhausted-retries",
            Self::Dismissal => "dismissal",
        }
    }

    /// The outcome a word names, exactly, or `None`.
    #[must_use]
    pub fn named(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|outcome| outcome.word() == word)
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.word())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_outcome_reads_back_from_its_word() {
        for outcome in Outcome::ALL {
            assert_eq!(Outcome::named(outcome.word()), Some(outcome));
        }
        assert_eq!(Outcome::named("Success"), None, "exact lowercase only");
        assert_eq!(Outcome::ExhaustedRetries.to_string(), "exhausted-retries");
    }
}
