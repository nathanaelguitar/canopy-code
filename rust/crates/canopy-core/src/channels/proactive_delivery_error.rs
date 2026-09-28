//! Typed classification for failed proactive channel deliveries.
//!
//! Port of `packages/channels/base/src/ChannelProactiveDeliveryError.ts`.

use std::error::Error;
use std::fmt;

pub const CHANNEL_PROACTIVE_DELIVERY_ERROR_CODE: &str = "channel_proactive_delivery_error";
pub const CHANNEL_PROACTIVE_DELIVERY_ERROR_NAME: &str = "ChannelProactiveDeliveryError";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChannelProactiveDeliveryDisposition {
    Permanent,
    Transient,
}

impl ChannelProactiveDeliveryDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Permanent => "permanent",
            Self::Transient => "transient",
        }
    }
}

/// A proactive delivery failure that callers can classify as retryable or
/// permanent without depending on its message text.
#[derive(Debug)]
pub struct ChannelProactiveDeliveryError {
    pub disposition: ChannelProactiveDeliveryDisposition,
    message: String,
    cause: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl ChannelProactiveDeliveryError {
    pub fn new(
        disposition: ChannelProactiveDeliveryDisposition,
        message: impl Into<String>,
    ) -> Self {
        Self {
            disposition,
            message: message.into(),
            cause: None,
        }
    }

    pub fn with_cause<E>(
        disposition: ChannelProactiveDeliveryDisposition,
        message: impl Into<String>,
        cause: E,
    ) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            disposition,
            message: message.into(),
            cause: Some(Box::new(cause)),
        }
    }

    pub const fn code(&self) -> &'static str {
        CHANNEL_PROACTIVE_DELIVERY_ERROR_CODE
    }
}

impl fmt::Display for ChannelProactiveDeliveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ChannelProactiveDeliveryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause.as_deref().map(|source| source as _)
    }
}

/// Type-safe counterpart to the TypeScript structural error guard.
pub fn is_channel_proactive_delivery_error(error: &(dyn Error + 'static)) -> bool {
    error
        .downcast_ref::<ChannelProactiveDeliveryError>()
        .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retains_the_disposition_code_and_message() {
        let error = ChannelProactiveDeliveryError::new(
            ChannelProactiveDeliveryDisposition::Transient,
            "temporary platform outage",
        );
        assert_eq!(error.code(), "channel_proactive_delivery_error");
        assert_eq!(error.disposition.as_str(), "transient");
        assert_eq!(error.to_string(), "temporary platform outage");
        assert!(is_channel_proactive_delivery_error(&error));
    }

    #[test]
    fn exposes_a_source_error_when_provided() {
        let error = ChannelProactiveDeliveryError::with_cause(
            ChannelProactiveDeliveryDisposition::Permanent,
            "recipient rejected the message",
            std::io::Error::other("bad request"),
        );
        assert_eq!(
            error.disposition,
            ChannelProactiveDeliveryDisposition::Permanent
        );
        assert_eq!(error.source().unwrap().to_string(), "bad request");
    }

    #[test]
    fn rejects_other_error_types() {
        let error = std::io::Error::other("unclassified");
        assert!(!is_channel_proactive_delivery_error(&error));
    }
}
