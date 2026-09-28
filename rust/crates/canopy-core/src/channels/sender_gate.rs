use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use super::{
    CreatePairingRequestResult, PairingRejection, PairingStore, SenderPolicy,
    gate_types::SharedPairingStore,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SenderCheckResult {
    pub allowed: bool,
    /// Present only when pairing policy denies the sender.
    pub pairing: Option<CreatePairingRequestResult>,
}

pub struct SenderGate {
    policy: SenderPolicy,
    allowed_users: HashSet<String>,
    pairing_store: Option<SharedPairingStore>,
}

impl SenderGate {
    pub fn new(
        policy: SenderPolicy,
        allowed_users: impl IntoIterator<Item = String>,
        pairing_store: Option<Arc<dyn PairingStore>>,
    ) -> Self {
        Self {
            policy,
            allowed_users: allowed_users.into_iter().collect(),
            pairing_store,
        }
    }

    pub fn replace_allowed_users(&mut self, users: impl IntoIterator<Item = String>) {
        self.allowed_users = users.into_iter().collect();
    }

    /// Check authorization without creating a pairing request.
    pub fn is_allowed(&self, sender_id: &str) -> io::Result<bool> {
        match self.policy {
            SenderPolicy::Open => Ok(true),
            SenderPolicy::Allowlist => Ok(self.allowed_users.contains(sender_id)),
            SenderPolicy::Pairing if self.allowed_users.contains(sender_id) => Ok(true),
            SenderPolicy::Pairing => self
                .pairing_store
                .as_ref()
                .map_or(Ok(false), |store| store.is_approved(sender_id)),
        }
    }

    pub fn check(
        &self,
        sender_id: &str,
        sender_name: Option<&str>,
    ) -> io::Result<SenderCheckResult> {
        Ok(match self.policy {
            SenderPolicy::Open => SenderCheckResult {
                allowed: true,
                pairing: None,
            },
            SenderPolicy::Allowlist => SenderCheckResult {
                allowed: self.allowed_users.contains(sender_id),
                pairing: None,
            },
            SenderPolicy::Pairing => {
                if self.allowed_users.contains(sender_id) {
                    return Ok(SenderCheckResult {
                        allowed: true,
                        pairing: None,
                    });
                }
                if self
                    .pairing_store
                    .as_ref()
                    .map_or(Ok(false), |store| store.is_approved(sender_id))?
                {
                    return Ok(SenderCheckResult {
                        allowed: true,
                        pairing: None,
                    });
                }

                let display_name = sender_name
                    .filter(|name| !name.is_empty())
                    .unwrap_or(sender_id);
                let pairing = self
                    .pairing_store
                    .as_ref()
                    .map(|store| store.create_request(sender_id, display_name))
                    .transpose()?
                    .flatten()
                    .or(Some(CreatePairingRequestResult::Rejected(
                        PairingRejection::CapReached,
                    )));
                SenderCheckResult {
                    allowed: false,
                    pairing,
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct MockPairingStore {
        approved: bool,
        created: AtomicUsize,
        last_name: std::sync::Mutex<Option<String>>,
        result: Option<CreatePairingRequestResult>,
        fail_create: bool,
    }

    impl PairingStore for MockPairingStore {
        fn is_approved(&self, _sender_id: &str) -> io::Result<bool> {
            Ok(self.approved)
        }
        fn is_group_approved(&self, _group_id: &str) -> io::Result<bool> {
            Ok(false)
        }
        fn create_request(
            &self,
            _sender_id: &str,
            sender_name: &str,
        ) -> io::Result<Option<CreatePairingRequestResult>> {
            self.created.fetch_add(1, Ordering::SeqCst);
            *self.last_name.lock().unwrap() = Some(sender_name.to_owned());
            if self.fail_create {
                Err(io::Error::other("pairing state write failed"))
            } else {
                Ok(self.result.clone())
            }
        }
        fn create_group_request(
            &self,
            _group_id: &str,
            _group_name: &str,
            _sender_id: &str,
            _sender_name: &str,
        ) -> io::Result<Option<CreatePairingRequestResult>> {
            Ok(None)
        }
    }

    fn users(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn check(gate: &SenderGate, sender_id: &str, sender_name: Option<&str>) -> SenderCheckResult {
        gate.check(sender_id, sender_name).unwrap()
    }

    #[test]
    fn open_and_allowlist_policies() {
        assert!(
            SenderGate::new(SenderPolicy::Open, [], None)
                .check("any", None)
                .unwrap()
                .allowed
        );
        let gate = SenderGate::new(SenderPolicy::Allowlist, users(&["alice"]), None);
        assert!(check(&gate, "alice", None).allowed);
        assert_eq!(
            check(&gate, "eve", None),
            SenderCheckResult {
                allowed: false,
                pairing: None
            }
        );
        assert!(
            !SenderGate::new(SenderPolicy::Allowlist, [], None)
                .check("any", None)
                .unwrap()
                .allowed
        );
    }

    #[test]
    fn replacement_swaps_the_allowlist_snapshot() {
        let mut gate = SenderGate::new(SenderPolicy::Allowlist, users(&["alice"]), None);
        gate.replace_allowed_users(users(&["10001"]));
        assert!(check(&gate, "10001", None).allowed);
        assert!(!check(&gate, "alice", None).allowed);
    }

    #[test]
    fn pairing_checks_allowlist_before_store_then_dynamic_approval() {
        let store = Arc::new(MockPairingStore {
            approved: true,
            ..Default::default()
        });
        let gate = SenderGate::new(
            SenderPolicy::Pairing,
            users(&["admin"]),
            Some(store.clone()),
        );
        assert!(check(&gate, "admin", None).allowed);
        assert_eq!(store.created.load(Ordering::SeqCst), 0);
        assert!(check(&gate, "approved", None).allowed);
        assert_eq!(store.created.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn unknown_sender_requests_pairing_and_uses_id_for_empty_name() {
        let store = Arc::new(MockPairingStore {
            result: Some(CreatePairingRequestResult::Code("ABCD1234".into())),
            ..Default::default()
        });
        let gate = SenderGate::new(SenderPolicy::Pairing, [], Some(store.clone()));
        assert_eq!(
            check(&gate, "user42", Some("")),
            SenderCheckResult {
                allowed: false,
                pairing: Some(CreatePairingRequestResult::Code("ABCD1234".into())),
            }
        );
        assert_eq!(*store.last_name.lock().unwrap(), Some("user42".into()));
    }

    #[test]
    fn missing_store_and_store_rejections_fall_through_as_in_typescript() {
        let absent = SenderGate::new(SenderPolicy::Pairing, [], None);
        assert_eq!(
            check(&absent, "stranger", None).pairing,
            Some(CreatePairingRequestResult::Rejected(
                PairingRejection::CapReached
            ))
        );
        for rejection in [
            PairingRejection::CapReached,
            PairingRejection::SenderPending,
        ] {
            let store = Arc::new(MockPairingStore {
                result: Some(CreatePairingRequestResult::Rejected(rejection)),
                ..Default::default()
            });
            let gate = SenderGate::new(SenderPolicy::Pairing, [], Some(store));
            assert_eq!(
                check(&gate, "stranger", None).pairing,
                Some(CreatePairingRequestResult::Rejected(rejection))
            );
        }
    }

    #[test]
    fn passive_pairing_check_never_creates_request() {
        let store = Arc::new(MockPairingStore::default());
        let gate = SenderGate::new(
            SenderPolicy::Pairing,
            users(&["admin"]),
            Some(store.clone()),
        );
        assert!(gate.is_allowed("admin").unwrap());
        assert!(!gate.is_allowed("stranger").unwrap());
        assert_eq!(store.created.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn request_storage_errors_propagate_through_the_gate() {
        let store = Arc::new(MockPairingStore {
            fail_create: true,
            ..Default::default()
        });
        let gate = SenderGate::new(SenderPolicy::Pairing, [], Some(store));
        let error = gate.check("stranger", None).unwrap_err();
        assert_eq!(error.to_string(), "pairing state write failed");
    }
}
