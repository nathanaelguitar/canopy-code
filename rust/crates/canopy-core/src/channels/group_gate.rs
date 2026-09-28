use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use super::{
    CreatePairingRequestResult, Envelope, GroupConfig, GroupPolicy, PairingRejection, PairingStore,
    gate_types::SharedPairingStore,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupDenyReason {
    Disabled,
    NotAllowlisted,
    MentionRequired,
    PairingTriggerRequired,
    PairingRequired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupCheckResult {
    pub allowed: bool,
    pub reason: Option<GroupDenyReason>,
    pub pairing: Option<CreatePairingRequestResult>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GroupCheckOptions {
    /// `Some(false)` suppresses a pairing request even when explicitly
    /// mentioned or replied to. `None` and `Some(true)` permit it.
    pub create_pairing_request: Option<bool>,
}

pub struct GroupGate {
    policy: GroupPolicy,
    groups: HashMap<String, GroupConfig>,
    pairing_store: Option<SharedPairingStore>,
}

impl GroupGate {
    pub fn new(
        policy: GroupPolicy,
        groups: impl IntoIterator<Item = (String, GroupConfig)>,
        pairing_store: Option<Arc<dyn PairingStore>>,
    ) -> Self {
        Self {
            policy,
            groups: groups.into_iter().collect(),
            pairing_store,
        }
    }

    pub fn check(
        &self,
        envelope: &Envelope,
        options: GroupCheckOptions,
    ) -> io::Result<GroupCheckResult> {
        if !envelope.is_group {
            return Ok(allowed());
        }

        if self.policy == GroupPolicy::Disabled {
            return Ok(denied(GroupDenyReason::Disabled, None));
        }

        if self.policy == GroupPolicy::Allowlist && !self.groups.contains_key(&envelope.chat_id) {
            // A `*` group entry supplies config defaults but never grants access.
            return Ok(denied(GroupDenyReason::NotAllowlisted, None));
        }

        if self.policy == GroupPolicy::Pairing {
            let approved = self.pairing_store.as_ref().map_or(Ok(false), |store| {
                store.is_group_approved(&envelope.chat_id)
            })?;
            if !approved {
                if options.create_pairing_request == Some(false)
                    || (!envelope.is_mentioned && !envelope.is_reply_to_bot)
                {
                    return Ok(denied(GroupDenyReason::PairingTriggerRequired, None));
                }

                let group_name = envelope
                    .chat_name
                    .as_deref()
                    .filter(|name| !name.is_empty())
                    .unwrap_or(&envelope.chat_id);
                let pairing = self
                    .pairing_store
                    .as_ref()
                    .map(|store| {
                        store.create_group_request(
                            &envelope.chat_id,
                            group_name,
                            &envelope.sender_id,
                            &envelope.sender_name,
                        )
                    })
                    .transpose()?
                    .flatten()
                    .or(Some(CreatePairingRequestResult::Rejected(
                        PairingRejection::CapReached,
                    )));
                return Ok(denied(GroupDenyReason::PairingRequired, pairing));
            }
        }

        let group_config = self
            .groups
            .get(&envelope.chat_id)
            .or_else(|| self.groups.get("*"));
        let require_mention = group_config
            .and_then(|config| config.require_mention)
            .unwrap_or(true);
        if require_mention && !envelope.is_mentioned && !envelope.is_reply_to_bot {
            return Ok(denied(GroupDenyReason::MentionRequired, None));
        }

        Ok(allowed())
    }

    pub fn is_group_approved(&self, group_id: &str) -> io::Result<bool> {
        self.pairing_store
            .as_ref()
            .map_or(Ok(false), |store| store.is_group_approved(group_id))
    }
}

impl Default for GroupGate {
    fn default() -> Self {
        Self::new(GroupPolicy::Disabled, [], None)
    }
}

fn allowed() -> GroupCheckResult {
    GroupCheckResult {
        allowed: true,
        reason: None,
        pairing: None,
    }
}

fn denied(
    reason: GroupDenyReason,
    pairing: Option<CreatePairingRequestResult>,
) -> GroupCheckResult {
    GroupCheckResult {
        allowed: false,
        reason: Some(reason),
        pairing,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct MockPairingStore {
        group_approved: bool,
        created: AtomicUsize,
        args: std::sync::Mutex<Option<(String, String, String, String)>>,
        result: Option<CreatePairingRequestResult>,
        fail_create: bool,
    }

    impl PairingStore for MockPairingStore {
        fn is_approved(&self, _sender_id: &str) -> io::Result<bool> {
            Ok(false)
        }
        fn is_group_approved(&self, _group_id: &str) -> io::Result<bool> {
            Ok(self.group_approved)
        }
        fn create_request(
            &self,
            _sender_id: &str,
            _sender_name: &str,
        ) -> io::Result<Option<CreatePairingRequestResult>> {
            Ok(None)
        }
        fn create_group_request(
            &self,
            group_id: &str,
            group_name: &str,
            sender_id: &str,
            sender_name: &str,
        ) -> io::Result<Option<CreatePairingRequestResult>> {
            self.created.fetch_add(1, Ordering::SeqCst);
            *self.args.lock().unwrap() = Some((
                group_id.into(),
                group_name.into(),
                sender_id.into(),
                sender_name.into(),
            ));
            if self.fail_create {
                Err(io::Error::other("pairing state write failed"))
            } else {
                Ok(self.result.clone())
            }
        }
    }

    fn envelope() -> Envelope {
        Envelope {
            sender_id: "user1".into(),
            sender_name: "User".into(),
            chat_id: "chat1".into(),
            chat_name: None,
            is_group: false,
            is_mentioned: false,
            is_reply_to_bot: false,
        }
    }

    fn groups(entries: &[(&str, Option<bool>)]) -> HashMap<String, GroupConfig> {
        entries
            .iter()
            .map(|(id, require_mention)| {
                (
                    (*id).to_owned(),
                    GroupConfig {
                        require_mention: *require_mention,
                    },
                )
            })
            .collect()
    }

    fn check(gate: &GroupGate, envelope: &Envelope) -> GroupCheckResult {
        gate.check(envelope, GroupCheckOptions::default()).unwrap()
    }

    #[test]
    fn non_group_messages_bypass_every_policy() {
        let mut envelope = envelope();
        for policy in [
            GroupPolicy::Disabled,
            GroupPolicy::Allowlist,
            GroupPolicy::Open,
            GroupPolicy::Pairing,
        ] {
            let gate = GroupGate::new(policy, [], None);
            assert!(check(&gate, &envelope).allowed);
        }
        envelope.is_group = true;
        assert_eq!(
            check(&GroupGate::default(), &envelope).reason,
            Some(GroupDenyReason::Disabled)
        );
    }

    #[test]
    fn allowlist_requires_explicit_chat_id_and_mention_by_default() {
        let mut envelope = envelope();
        envelope.is_group = true;
        assert_eq!(
            check(
                &GroupGate::new(GroupPolicy::Allowlist, groups(&[("*", Some(false))]), None),
                &envelope
            )
            .reason,
            Some(GroupDenyReason::NotAllowlisted)
        );
        let gate = GroupGate::new(GroupPolicy::Allowlist, groups(&[("chat1", None)]), None);
        assert_eq!(
            check(&gate, &envelope).reason,
            Some(GroupDenyReason::MentionRequired)
        );
        envelope.is_reply_to_bot = true;
        assert!(check(&gate, &envelope).allowed);
    }

    #[test]
    fn per_group_config_wins_and_wildcard_only_supplies_mention_default() {
        let mut envelope = envelope();
        envelope.is_group = true;
        let wildcard = GroupGate::new(GroupPolicy::Open, groups(&[("*", Some(false))]), None);
        assert!(check(&wildcard, &envelope).allowed);
        let specific_wins = GroupGate::new(
            GroupPolicy::Open,
            groups(&[("*", Some(false)), ("chat1", Some(true))]),
            None,
        );
        assert_eq!(
            check(&specific_wins, &envelope).reason,
            Some(GroupDenyReason::MentionRequired)
        );
        envelope.is_mentioned = true;
        assert!(check(&specific_wins, &envelope).allowed);
    }

    #[test]
    fn pairing_requires_a_mention_or_reply_before_creating_request() {
        let mut envelope = envelope();
        envelope.is_group = true;
        let store = Arc::new(MockPairingStore::default());
        let gate = GroupGate::new(
            GroupPolicy::Pairing,
            groups(&[("*", Some(false))]),
            Some(store.clone()),
        );
        assert_eq!(
            check(&gate, &envelope).reason,
            Some(GroupDenyReason::PairingTriggerRequired)
        );
        assert_eq!(store.created.load(Ordering::SeqCst), 0);
        envelope.is_reply_to_bot = true;
        let result = check(&gate, &envelope);
        assert_eq!(result.reason, Some(GroupDenyReason::PairingRequired));
        assert_eq!(
            result.pairing,
            Some(CreatePairingRequestResult::Rejected(
                PairingRejection::CapReached
            ))
        );
        assert_eq!(store.created.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn suppressed_request_precedes_trigger_and_approved_groups_use_mention_gate() {
        let mut envelope = envelope();
        envelope.is_group = true;
        envelope.is_mentioned = true;
        let store = Arc::new(MockPairingStore::default());
        let gate = GroupGate::new(GroupPolicy::Pairing, [], Some(store.clone()));
        assert_eq!(
            gate.check(
                &envelope,
                GroupCheckOptions {
                    create_pairing_request: Some(false)
                }
            )
            .unwrap()
            .reason,
            Some(GroupDenyReason::PairingTriggerRequired)
        );
        assert_eq!(store.created.load(Ordering::SeqCst), 0);
        let approved_store = Arc::new(MockPairingStore {
            group_approved: true,
            ..Default::default()
        });
        let approved = GroupGate::new(GroupPolicy::Pairing, [], Some(approved_store));
        envelope.is_mentioned = false;
        assert_eq!(
            check(&approved, &envelope).reason,
            Some(GroupDenyReason::MentionRequired)
        );
    }

    #[test]
    fn creates_group_request_with_chat_name_or_chat_id() {
        let mut envelope = envelope();
        envelope.is_group = true;
        envelope.is_mentioned = true;
        let store = Arc::new(MockPairingStore {
            result: Some(CreatePairingRequestResult::Code("ABCD1234".into())),
            ..Default::default()
        });
        let gate = GroupGate::new(GroupPolicy::Pairing, [], Some(store.clone()));
        assert_eq!(
            check(&gate, &envelope).pairing,
            Some(CreatePairingRequestResult::Code("ABCD1234".into()))
        );
        assert_eq!(
            *store.args.lock().unwrap(),
            Some((
                "chat1".into(),
                "chat1".into(),
                "user1".into(),
                "User".into()
            ))
        );
        envelope.chat_name = Some("Group name".into());
        let _ = check(&gate, &envelope);
        assert_eq!(store.args.lock().unwrap().as_ref().unwrap().1, "Group name");
    }

    #[test]
    fn request_storage_errors_propagate_through_the_gate() {
        let mut envelope = envelope();
        envelope.is_group = true;
        envelope.is_mentioned = true;
        let store = Arc::new(MockPairingStore {
            fail_create: true,
            ..Default::default()
        });
        let gate = GroupGate::new(GroupPolicy::Pairing, [], Some(store));

        let error = gate
            .check(&envelope, GroupCheckOptions::default())
            .unwrap_err();
        assert_eq!(error.to_string(), "pairing state write failed");
    }
}
