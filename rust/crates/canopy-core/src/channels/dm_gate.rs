use super::{DmPolicy, Envelope};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DmDenyReason {
    Disabled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DmCheckResult {
    pub allowed: bool,
    pub reason: Option<DmDenyReason>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DmGate {
    policy: DmPolicy,
}

impl DmGate {
    pub const fn new(policy: DmPolicy) -> Self {
        Self { policy }
    }

    /// Group traffic bypasses the DM gate; group policy is owned by `GroupGate`.
    pub fn check(&self, envelope: &Envelope) -> DmCheckResult {
        if envelope.is_group || self.policy == DmPolicy::Open {
            DmCheckResult {
                allowed: true,
                reason: None,
            }
        } else {
            DmCheckResult {
                allowed: false,
                reason: Some(DmDenyReason::Disabled),
            }
        }
    }
}

impl Default for DmGate {
    fn default() -> Self {
        Self::new(DmPolicy::Open)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(is_group: bool) -> Envelope {
        Envelope {
            is_group,
            ..Default::default()
        }
    }

    #[test]
    fn group_messages_bypass_both_dm_policies() {
        for policy in [DmPolicy::Disabled, DmPolicy::Open] {
            assert_eq!(
                DmGate::new(policy).check(&envelope(true)),
                DmCheckResult {
                    allowed: true,
                    reason: None
                }
            );
        }
    }

    #[test]
    fn disabled_policy_rejects_private_messages() {
        assert_eq!(
            DmGate::new(DmPolicy::Disabled).check(&envelope(false)),
            DmCheckResult {
                allowed: false,
                reason: Some(DmDenyReason::Disabled),
            }
        );
    }

    #[test]
    fn default_policy_is_open() {
        assert!(DmGate::default().check(&envelope(false)).allowed);
    }
}
