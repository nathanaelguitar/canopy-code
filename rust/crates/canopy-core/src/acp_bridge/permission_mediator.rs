//! Multi-client permission mediation for daemon-owned ACP sessions.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::sync::oneshot;

use super::event_bus::BridgeEvent;

pub const CANCEL_VOTE_SENTINEL: &str = "__cancelled__";
const MAX_RESOLVED_PERMISSION_RECORDS: usize = 512;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PermissionPolicy {
    #[default]
    FirstResponder,
    Designated,
    Consensus,
    LocalOnly,
}

#[derive(Clone, Debug)]
pub struct PermissionRequestRecord {
    pub request_id: String,
    pub session_id: String,
    pub prompt_id: Option<String>,
    pub originator_client_id: Option<String>,
    pub allowed_option_ids: HashSet<String>,
    pub issued_at_ms: u64,
    pub voters_at_issue: HashSet<String>,
}

#[derive(Clone, Debug)]
pub struct PermissionVote {
    pub request_id: String,
    pub session_id: String,
    pub client_id: Option<String>,
    pub option_id: String,
    pub received_at_ms: u64,
    pub from_loopback: bool,
    pub metadata: Option<serde_json::Map<String, Value>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PermissionResolution {
    Option {
        option_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Map<String, Value>>,
    },
    Cancelled {
        reason: PermissionCancelReason,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionCancelReason {
    Timeout,
    SessionClosed,
    AgentCancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PermissionVoteOutcome {
    Resolved { resolved_option_id: String },
    Recorded { votes_needed: usize },
    AlreadyResolved { resolved_option_id: String },
    Forbidden { reason: PermissionForbiddenReason },
    UnknownRequest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PermissionForbiddenReason {
    DesignatedMismatch,
    RemoteNotAllowed,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum PermissionMediatorError {
    #[error(
        "Permission {request_id}: optionId \"{option_id}\" is not in the set of options the agent offered."
    )]
    InvalidOption {
        request_id: String,
        option_id: String,
    },
    #[error("Permission {request_id}: allowed option ids contain the cancel-vote sentinel")]
    CancelSentinelCollision { request_id: String },
    #[error("Permission request id already pending: {0}")]
    DuplicateRequest(String),
}

struct Pending {
    record: PermissionRequestRecord,
    policy: PermissionPolicy,
    sender: Option<oneshot::Sender<PermissionResolution>>,
    tallies: HashMap<String, HashSet<String>>,
}

#[derive(Clone)]
struct Resolved {
    session_id: String,
    option_id: String,
}

#[derive(Default)]
struct State {
    pending: HashMap<String, Pending>,
    resolved: HashMap<String, Resolved>,
    resolved_order: VecDeque<String>,
}

struct Inner {
    state: Mutex<State>,
    policy: PermissionPolicy,
    consensus_quorum: Option<usize>,
    events: Option<tokio::sync::mpsc::UnboundedSender<PermissionMediatorEvent>>,
}

#[derive(Clone, Debug)]
pub struct PermissionMediatorEvent {
    pub session_id: String,
    pub event: BridgeEvent,
}

/// Pending registration is synchronous, while each request resolves once via
/// its receiver when a vote, timeout, or session close wins.
pub struct MultiClientPermissionMediator {
    inner: Arc<Inner>,
}

impl MultiClientPermissionMediator {
    pub fn new(policy: PermissionPolicy) -> Self {
        Self::with_consensus_quorum(policy, None)
    }
    pub fn with_consensus_quorum(
        policy: PermissionPolicy,
        consensus_quorum: Option<usize>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                policy,
                consensus_quorum,
                events: None,
            }),
        }
    }
    pub fn with_event_sink(
        mut self,
        events: tokio::sync::mpsc::UnboundedSender<PermissionMediatorEvent>,
    ) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("event sink must be set before sharing the mediator")
            .events = Some(events);
        self
    }
    pub fn policy(&self) -> PermissionPolicy {
        self.inner.policy
    }

    pub fn request(
        &self,
        record: PermissionRequestRecord,
        timeout_ms: u64,
    ) -> Result<oneshot::Receiver<PermissionResolution>, PermissionMediatorError> {
        if record.allowed_option_ids.contains(CANCEL_VOTE_SENTINEL) {
            return Err(PermissionMediatorError::CancelSentinelCollision {
                request_id: record.request_id,
            });
        }
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = lock(&self.inner.state);
            if state.pending.contains_key(&record.request_id) {
                return Err(PermissionMediatorError::DuplicateRequest(record.request_id));
            }
            state.pending.insert(
                record.request_id.clone(),
                Pending {
                    record: record.clone(),
                    policy: self.inner.policy,
                    sender: Some(sender),
                    tallies: HashMap::new(),
                },
            );
        }
        if timeout_ms > 0 {
            let inner = Arc::downgrade(&self.inner);
            let request_id = record.request_id;
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
                if let Some(inner) = inner.upgrade() {
                    resolve_pending(
                        &inner,
                        &request_id,
                        PermissionResolution::Cancelled {
                            reason: PermissionCancelReason::Timeout,
                        },
                    );
                }
            });
        }
        Ok(receiver)
    }

    pub fn vote(
        &self,
        vote: PermissionVote,
    ) -> Result<PermissionVoteOutcome, PermissionMediatorError> {
        let mut state = lock(&self.inner.state);
        let Some(pending) = state.pending.get(&vote.request_id) else {
            return Ok(match state.resolved.get(&vote.request_id) {
                Some(prior) if prior.session_id == vote.session_id => {
                    let outcome = if prior.option_id == CANCEL_VOTE_SENTINEL {
                        json!({"outcome":"cancelled"})
                    } else {
                        json!({"outcome":"selected","optionId":prior.option_id})
                    };
                    emit(
                        &self.inner,
                        prior.session_id.clone(),
                        BridgeEvent::new(
                            "permission_already_resolved",
                            json!({"requestId":&vote.request_id,"sessionId":&prior.session_id,"outcome":outcome}),
                        ),
                    );
                    PermissionVoteOutcome::AlreadyResolved {
                        resolved_option_id: prior.option_id.clone(),
                    }
                }
                _ => PermissionVoteOutcome::UnknownRequest,
            });
        };
        if pending.record.session_id != vote.session_id {
            return Ok(PermissionVoteOutcome::UnknownRequest);
        }
        // Cancellation intentionally bypasses policy and option validation.
        if vote.option_id == CANCEL_VOTE_SENTINEL {
            let resolved_id = vote.option_id.clone();
            resolve_pending_locked(
                &self.inner,
                &mut state,
                &vote.request_id,
                PermissionResolution::Cancelled {
                    reason: PermissionCancelReason::AgentCancelled,
                },
                resolved_id.clone(),
                vote.client_id.clone(),
            );
            return Ok(PermissionVoteOutcome::Resolved {
                resolved_option_id: resolved_id,
            });
        }
        if !pending.record.allowed_option_ids.contains(&vote.option_id) {
            return Err(PermissionMediatorError::InvalidOption {
                request_id: vote.request_id,
                option_id: vote.option_id,
            });
        }
        let policy = pending.policy;
        let originator = pending.record.originator_client_id.clone();
        let voters = pending.record.voters_at_issue.clone();
        match policy {
            PermissionPolicy::FirstResponder => self.resolve_vote_locked(&mut state, &vote),
            PermissionPolicy::Designated => {
                let Some(originator) = originator else {
                    return self.resolve_vote_locked(&mut state, &vote);
                };
                if vote.client_id.as_deref() != Some(originator.as_str()) {
                    emit_forbidden(
                        &self.inner,
                        &vote,
                        PermissionForbiddenReason::DesignatedMismatch,
                    );
                    Ok(PermissionVoteOutcome::Forbidden {
                        reason: PermissionForbiddenReason::DesignatedMismatch,
                    })
                } else {
                    self.resolve_vote_locked(&mut state, &vote)
                }
            }
            PermissionPolicy::LocalOnly => {
                if !vote.from_loopback {
                    emit_forbidden(
                        &self.inner,
                        &vote,
                        PermissionForbiddenReason::RemoteNotAllowed,
                    );
                    Ok(PermissionVoteOutcome::Forbidden {
                        reason: PermissionForbiddenReason::RemoteNotAllowed,
                    })
                } else {
                    self.resolve_vote_locked(&mut state, &vote)
                }
            }
            PermissionPolicy::Consensus => {
                let Some(client_id) = vote.client_id.as_ref().filter(|id| voters.contains(*id))
                else {
                    emit_forbidden(
                        &self.inner,
                        &vote,
                        PermissionForbiddenReason::DesignatedMismatch,
                    );
                    return Ok(PermissionVoteOutcome::Forbidden {
                        reason: PermissionForbiddenReason::DesignatedMismatch,
                    });
                };
                let entry = state
                    .pending
                    .get_mut(&vote.request_id)
                    .expect("pending request remains registered");
                if entry
                    .tallies
                    .values()
                    .any(|voters| voters.contains(client_id))
                {
                    return Ok(PermissionVoteOutcome::Recorded {
                        votes_needed: votes_needed(&self.inner, entry),
                    });
                }
                let tally = entry.tallies.entry(vote.option_id.clone()).or_default();
                tally.insert(client_id.clone());
                let quorum = quorum_for(&self.inner, voters.len());
                let tally_count = tally.len();
                if tally_count < quorum {
                    let needed = quorum.saturating_sub(tally_count).max(1);
                    let votes_received = entry.tallies.values().map(HashSet::len).sum();
                    emit_partial_vote(
                        &self.inner,
                        &vote,
                        votes_received,
                        needed,
                        quorum,
                        &entry.tallies,
                    );
                    return Ok(PermissionVoteOutcome::Recorded {
                        votes_needed: needed,
                    });
                }
                self.resolve_vote_locked(&mut state, &vote)
            }
        }
    }

    fn resolve_vote_locked(
        &self,
        state: &mut State,
        vote: &PermissionVote,
    ) -> Result<PermissionVoteOutcome, PermissionMediatorError> {
        let option_id = vote.option_id.clone();
        let resolution = PermissionResolution::Option {
            option_id: option_id.clone(),
            metadata: vote.metadata.clone(),
        };
        resolve_pending_locked(
            &self.inner,
            state,
            &vote.request_id,
            resolution,
            option_id.clone(),
            vote.client_id.clone(),
        );
        Ok(PermissionVoteOutcome::Resolved {
            resolved_option_id: option_id,
        })
    }

    pub fn forget_session(&self, session_id: &str) {
        let mut state = lock(&self.inner.state);
        let ids: Vec<_> = state
            .pending
            .iter()
            .filter(|(_, p)| p.record.session_id == session_id)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            resolve_pending_locked(
                &self.inner,
                &mut state,
                &id,
                PermissionResolution::Cancelled {
                    reason: PermissionCancelReason::SessionClosed,
                },
                CANCEL_VOTE_SENTINEL.into(),
                None,
            );
        }
    }

    pub fn peek_session_for(&self, request_id: &str) -> Option<String> {
        let state = lock(&self.inner.state);
        state
            .pending
            .get(request_id)
            .map(|p| p.record.session_id.clone())
            .or_else(|| state.resolved.get(request_id).map(|r| r.session_id.clone()))
    }
    pub fn pending_count(&self) -> usize {
        lock(&self.inner.state).pending.len()
    }
}

fn lock(mutex: &Mutex<State>) -> MutexGuard<'_, State> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn resolve_pending(inner: &Inner, request_id: &str, resolution: PermissionResolution) {
    let mut state = lock(&inner.state);
    let option_id = match &resolution {
        PermissionResolution::Option { option_id, .. } => option_id.clone(),
        PermissionResolution::Cancelled { .. } => CANCEL_VOTE_SENTINEL.into(),
    };
    resolve_pending_locked(inner, &mut state, request_id, resolution, option_id, None);
}
fn resolve_pending_locked(
    inner: &Inner,
    state: &mut State,
    request_id: &str,
    resolution: PermissionResolution,
    option_id: String,
    resolver_client_id: Option<String>,
) {
    let Some(mut pending) = state.pending.remove(request_id) else {
        return;
    };
    let outcome = match &resolution {
        PermissionResolution::Option { option_id, .. } => {
            json!({"outcome":"selected","optionId":option_id})
        }
        PermissionResolution::Cancelled { .. } => json!({"outcome":"cancelled"}),
    };
    let mut data = json!({"requestId":pending.record.request_id,"outcome":outcome});
    if let Some(client_id) = &resolver_client_id {
        data["voterClientId"] = json!(client_id);
    }
    let mut event = BridgeEvent::new("permission_resolved", data);
    event.prompt_id = pending.record.prompt_id.clone();
    event.originator_client_id = resolver_client_id;
    emit(inner, pending.record.session_id.clone(), event);
    if let Some(sender) = pending.sender.take() {
        let _ = sender.send(resolution);
    }
    state.resolved.insert(
        request_id.to_owned(),
        Resolved {
            session_id: pending.record.session_id,
            option_id,
        },
    );
    state.resolved_order.push_back(request_id.to_owned());
    while state.resolved_order.len() > MAX_RESOLVED_PERMISSION_RECORDS {
        if let Some(evicted) = state.resolved_order.pop_front() {
            state.resolved.remove(&evicted);
        }
    }
}
fn emit(inner: &Inner, session_id: String, event: BridgeEvent) {
    if let Some(events) = &inner.events {
        let _ = events.send(PermissionMediatorEvent { session_id, event });
    }
}
fn emit_forbidden(inner: &Inner, vote: &PermissionVote, reason: PermissionForbiddenReason) {
    let reason = match reason {
        PermissionForbiddenReason::DesignatedMismatch => "designated_mismatch",
        PermissionForbiddenReason::RemoteNotAllowed => "remote_not_allowed",
    };
    emit(
        inner,
        vote.session_id.clone(),
        BridgeEvent::new(
            "permission_forbidden",
            json!({"requestId":&vote.request_id,"sessionId":&vote.session_id,"clientId":&vote.client_id,"reason":reason}),
        ),
    );
}
fn emit_partial_vote(
    inner: &Inner,
    vote: &PermissionVote,
    votes_received: usize,
    votes_needed: usize,
    quorum: usize,
    tallies: &HashMap<String, HashSet<String>>,
) {
    let option_tallies: Map<String, Value> = tallies
        .iter()
        .map(|(option, clients)| (option.clone(), json!(clients.len())))
        .collect();
    emit(
        inner,
        vote.session_id.clone(),
        BridgeEvent::new(
            "permission_partial_vote",
            json!({"requestId":&vote.request_id,"sessionId":&vote.session_id,"votesReceived":votes_received,"votesNeeded":votes_needed,"quorum":quorum,"optionTallies":option_tallies}),
        ),
    );
}
fn quorum_for(inner: &Inner, voters: usize) -> usize {
    inner
        .consensus_quorum
        .map_or(voters / 2 + 1, |q| q.min(voters.max(1)))
        .max(1)
}
fn votes_needed(inner: &Inner, pending: &Pending) -> usize {
    let max_tally = pending
        .tallies
        .values()
        .map(HashSet::len)
        .max()
        .unwrap_or(0);
    quorum_for(inner, pending.record.voters_at_issue.len())
        .saturating_sub(max_tally)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(voters: &[&str]) -> PermissionRequestRecord {
        PermissionRequestRecord {
            request_id: "r1".into(),
            session_id: "s1".into(),
            prompt_id: None,
            originator_client_id: Some("a".into()),
            allowed_option_ids: ["allow".into(), "deny".into()].into_iter().collect(),
            issued_at_ms: 0,
            voters_at_issue: voters.iter().map(|s| (*s).to_owned()).collect(),
        }
    }
    fn vote(client: &str, option: &str) -> PermissionVote {
        PermissionVote {
            request_id: "r1".into(),
            session_id: "s1".into(),
            client_id: Some(client.into()),
            option_id: option.into(),
            received_at_ms: 1,
            from_loopback: false,
            metadata: None,
        }
    }

    #[tokio::test]
    async fn designated_and_consensus_policies_apply_issue_time_voters() {
        let designated = MultiClientPermissionMediator::new(PermissionPolicy::Designated);
        let result = designated.request(record(&["a", "b"]), 0).unwrap();
        assert_eq!(
            designated.vote(vote("b", "allow")).unwrap(),
            PermissionVoteOutcome::Forbidden {
                reason: PermissionForbiddenReason::DesignatedMismatch
            }
        );
        designated.vote(vote("a", "allow")).unwrap();
        assert!(
            matches!(result.await.unwrap(), PermissionResolution::Option { option_id, .. } if option_id == "allow")
        );

        let consensus = MultiClientPermissionMediator::new(PermissionPolicy::Consensus);
        let result = consensus.request(record(&["a", "b", "c"]), 0).unwrap();
        assert_eq!(
            consensus.vote(vote("a", "allow")).unwrap(),
            PermissionVoteOutcome::Recorded { votes_needed: 1 }
        );
        consensus.vote(vote("b", "allow")).unwrap();
        assert!(
            matches!(result.await.unwrap(), PermissionResolution::Option { option_id, .. } if option_id == "allow")
        );
    }

    #[tokio::test]
    async fn cancel_bypasses_policy_and_close_resolves_pending() {
        let mediator = MultiClientPermissionMediator::new(PermissionPolicy::LocalOnly);
        let result = mediator.request(record(&[]), 0).unwrap();
        let mut cancel = vote("remote", CANCEL_VOTE_SENTINEL);
        cancel.from_loopback = false;
        mediator.vote(cancel).unwrap();
        assert_eq!(
            result.await.unwrap(),
            PermissionResolution::Cancelled {
                reason: PermissionCancelReason::AgentCancelled
            }
        );

        let result = mediator
            .request(
                PermissionRequestRecord {
                    request_id: "r2".into(),
                    ..record(&[])
                },
                0,
            )
            .unwrap();
        mediator.forget_session("s1");
        assert_eq!(
            result.await.unwrap(),
            PermissionResolution::Cancelled {
                reason: PermissionCancelReason::SessionClosed
            }
        );
    }
}
