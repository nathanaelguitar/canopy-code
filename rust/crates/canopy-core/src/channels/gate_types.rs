use std::io;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SenderPolicy {
    Allowlist,
    Pairing,
    Open,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupPolicy {
    Disabled,
    Allowlist,
    Pairing,
    Open,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DmPolicy {
    Disabled,
    #[default]
    Open,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GroupConfig {
    /// `None` follows the TypeScript default of requiring a mention.
    pub require_mention: Option<bool>,
}

/// The subset of the TypeScript channel envelope inspected by these gates.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Envelope {
    pub sender_id: String,
    pub sender_name: String,
    pub chat_id: String,
    pub chat_name: Option<String>,
    pub is_group: bool,
    pub is_mentioned: bool,
    pub is_reply_to_bot: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PairingRejection {
    SenderPending,
    CapReached,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreatePairingRequestResult {
    Code(String),
    Rejected(PairingRejection),
}

/// Pairing operations consumed by sender and group gates.
///
/// I/O errors are returned rather than being converted to `cap_reached`, so
/// callers can distinguish a full pending queue from a failed state write.
/// Implementations should preserve the TypeScript `PairingStore` guarantees:
/// approved checks do not create requests, and request creation may return a
/// pending code or either rejection reason.
pub trait PairingStore: Send + Sync {
    fn is_approved(&self, sender_id: &str) -> io::Result<bool>;
    fn is_group_approved(&self, group_id: &str) -> io::Result<bool>;
    fn create_request(
        &self,
        sender_id: &str,
        sender_name: &str,
    ) -> io::Result<Option<CreatePairingRequestResult>>;
    fn create_group_request(
        &self,
        group_id: &str,
        group_name: &str,
        sender_id: &str,
        sender_name: &str,
    ) -> io::Result<Option<CreatePairingRequestResult>>;
}

pub(crate) type SharedPairingStore = Arc<dyn PairingStore>;
