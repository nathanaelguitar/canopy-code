//! Persistent channel contacts observed from inbound messages.
//!
//! Port of `packages/cli/src/commands/channel/observed-contact-store.ts`.
//! The store keeps recent user/group/topic relationships, validates registry
//! contents before use, and atomically replaces its private JSON file.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};

const REGISTRY_VERSION: u64 = 1;
const MAX_OBSERVATIONS: usize = 500;
const MAX_CHANNEL_NAME_LENGTH: usize = 256;
const MAX_LABEL_LENGTH: usize = 256;
const MAX_ID_LENGTH: usize = 4096;

/// Upper bound accepted by [`ObservedChannelContactStore::list`], in seconds.
pub const OBSERVED_CONTACT_MAX_FRESH_WITHIN_SECONDS: i64 = 365 * 24 * 60 * 60;

static STORE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn store_lock() -> MutexGuard<'static, ()> {
    STORE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A channel user, group, or topic identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservedChannelIdentity {
    pub id: String,
    pub label: String,
}

/// One observed user and its optional group and topic context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservedChannelContactObservation {
    pub user: ObservedChannelIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<ObservedChannelIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<ObservedChannelIdentity>,
}

/// A directly observed channel contact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservedChannelContact {
    pub id: String,
    pub label: String,
    pub channel_name: String,
    pub last_observed_at: String,
}

/// A user related to an observed group or topic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservedChannelRelatedContact {
    pub id: String,
    pub label: String,
    pub last_observed_at: String,
}

/// A topic with the observed users who participated in it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservedChannelTopic {
    pub id: String,
    pub label: String,
    pub last_observed_at: String,
    pub users: Vec<ObservedChannelRelatedContact>,
}

/// A group with its observed users and topics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservedChannelGroup {
    pub id: String,
    pub label: String,
    pub channel_name: String,
    pub last_observed_at: String,
    pub users: Vec<ObservedChannelRelatedContact>,
    pub topics: Vec<ObservedChannelTopic>,
}

/// Graph of direct contacts and observed group membership.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservedChannelContactGraph {
    pub users: Vec<ObservedChannelContact>,
    pub groups: Vec<ObservedChannelGroup>,
}

/// Configuration for an observed contact store.
#[derive(Clone)]
pub struct ObservedChannelContactStoreOptions {
    /// Maximum number of persisted observation rows. Defaults to 500.
    pub max_observations: usize,
    /// Injectable UTC clock. Defaults to the system clock.
    pub now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

impl Default for ObservedChannelContactStoreOptions {
    fn default() -> Self {
        Self {
            max_observations: MAX_OBSERVATIONS,
            now: Arc::new(Utc::now),
        }
    }
}

/// Validation, registry, or filesystem error from the contact store.
#[derive(Debug, Error)]
pub enum ObservedChannelContactError {
    #[error("Invalid observed contact observation.")]
    InvalidObservation,
    #[error("Invalid observed contact freshness.")]
    InvalidFreshness,
    #[error("Invalid observed contact registry.")]
    InvalidRegistry,
    #[error("Unsupported observed contact registry version.")]
    UnsupportedVersion,
    #[error("failed to persist observed contacts: {0}")]
    Io(#[from] io::Error),
    #[error("failed to serialize observed contacts: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedObservedContact {
    channel_name: String,
    user: ObservedChannelIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<ObservedChannelIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<ObservedChannelIdentity>,
    last_observed_at: String,
}

#[derive(Serialize)]
struct ObservedContactRegistryFile<'a> {
    version: u64,
    observations: &'a [PersistedObservedContact],
}

struct MutableGroup {
    value: ObservedChannelGroup,
    user_indices: HashMap<String, usize>,
    topic_indices: HashMap<String, usize>,
    topic_user_indices: Vec<HashMap<String, usize>>,
}

/// File-backed, bounded observations used to enrich channel contact lookups.
pub struct ObservedChannelContactStore {
    file_path: PathBuf,
    max_observations: usize,
    now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

impl ObservedChannelContactStore {
    /// Create a store with the default 500-observation limit and system clock.
    pub fn new(file_path: impl Into<PathBuf>) -> Self {
        Self::with_options(file_path, ObservedChannelContactStoreOptions::default())
    }

    /// Create a store with an explicit observation cap and clock.
    pub fn with_options(
        file_path: impl Into<PathBuf>,
        options: ObservedChannelContactStoreOptions,
    ) -> Self {
        Self {
            file_path: file_path.into(),
            max_observations: options.max_observations,
            now: options.now,
        }
    }

    /// Record or refresh a relationship observation.
    ///
    /// An observation is keyed by channel, user ID, group ID, and topic ID;
    /// observing the same relationship again refreshes its labels and time.
    pub fn observe(
        &self,
        channel_name: &str,
        observation: &ObservedChannelContactObservation,
    ) -> Result<(), ObservedChannelContactError> {
        validate_observation(channel_name, observation)?;
        let _guard = store_lock();
        let observed_at = (self.now)();
        let observed_at_string = canonical_timestamp(observed_at);
        let next = PersistedObservedContact {
            channel_name: channel_name.to_owned(),
            user: normalize_identity(&observation.user),
            group: observation.group.as_ref().map(normalize_identity),
            topic: observation.topic.as_ref().map(normalize_identity),
            last_observed_at: observed_at_string,
        };
        let key = observation_key(&next);
        let cutoff =
            observed_at - chrono::Duration::seconds(OBSERVED_CONTACT_MAX_FRESH_WITHIN_SECONDS);
        let mut observations = self
            .read_observations()?
            .into_iter()
            .filter(|candidate| {
                parse_canonical_timestamp(&candidate.last_observed_at)
                    .is_some_and(|timestamp| timestamp >= cutoff)
                    && observation_key(candidate) != key
            })
            .collect::<Vec<_>>();
        observations.push(next);
        observations.sort_by(|left, right| right.last_observed_at.cmp(&left.last_observed_at));
        observations.truncate(self.max_observations);
        self.persist(&observations)
    }

    /// List a recent contact graph, with freshness in seconds.
    pub fn list(
        &self,
        fresh_within_seconds: i64,
    ) -> Result<ObservedChannelContactGraph, ObservedChannelContactError> {
        if !(1..=OBSERVED_CONTACT_MAX_FRESH_WITHIN_SECONDS).contains(&fresh_within_seconds) {
            return Err(ObservedChannelContactError::InvalidFreshness);
        }
        let cutoff = (self.now)() - chrono::Duration::seconds(fresh_within_seconds);
        let mut observations = self
            .read_observations()?
            .into_iter()
            .filter(|observation| {
                parse_canonical_timestamp(&observation.last_observed_at)
                    .is_some_and(|timestamp| timestamp >= cutoff)
            })
            .collect::<Vec<_>>();
        observations.sort_by(|left, right| right.last_observed_at.cmp(&left.last_observed_at));
        Ok(aggregate_graph(observations))
    }

    fn read_observations(
        &self,
    ) -> Result<Vec<PersistedObservedContact>, ObservedChannelContactError> {
        let bytes = match fs::read(&self.file_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(_) => return Err(ObservedChannelContactError::InvalidRegistry),
        };
        let parsed: Value = serde_json::from_slice(&bytes)
            .map_err(|_| ObservedChannelContactError::InvalidRegistry)?;
        let record = parsed
            .as_object()
            .ok_or(ObservedChannelContactError::InvalidRegistry)?;
        if record.get("version").and_then(Value::as_f64) != Some(REGISTRY_VERSION as f64) {
            return Err(ObservedChannelContactError::UnsupportedVersion);
        }
        let rows = record
            .get("observations")
            .and_then(Value::as_array)
            .filter(|rows| rows.len() <= self.max_observations)
            .ok_or(ObservedChannelContactError::InvalidRegistry)?;
        let observations = rows
            .iter()
            .map(parse_observation)
            .collect::<Result<Vec<_>, _>>()?;
        let keys = observations
            .iter()
            .map(observation_key)
            .collect::<HashSet<_>>();
        if keys.len() != observations.len() {
            return Err(ObservedChannelContactError::InvalidRegistry);
        }
        Ok(observations)
    }

    fn persist(
        &self,
        observations: &[PersistedObservedContact],
    ) -> Result<(), ObservedChannelContactError> {
        let parent = self
            .file_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        let data = ObservedContactRegistryFile {
            version: REGISTRY_VERSION,
            observations,
        };
        let bytes = serde_json::to_vec_pretty(&data)?;
        let options = AtomicWriteOptions {
            mode: Some(0o600),
            force_mode: true,
            symlink_policy: SymlinkPolicy::NoFollow,
            ..AtomicWriteOptions::default()
        };
        atomic_write_file(&self.file_path, &bytes, &options)?;
        Ok(())
    }
}

fn validate_observation(
    channel_name: &str,
    observation: &ObservedChannelContactObservation,
) -> Result<(), ObservedChannelContactError> {
    if !is_bounded_string(channel_name, MAX_CHANNEL_NAME_LENGTH)
        || !is_identity(&observation.user)
        || observation
            .group
            .as_ref()
            .is_some_and(|value| !is_identity(value))
        || observation
            .topic
            .as_ref()
            .is_some_and(|value| !is_identity(value))
        || (observation.topic.is_some() && observation.group.is_none())
    {
        return Err(ObservedChannelContactError::InvalidObservation);
    }
    Ok(())
}

fn is_identity(value: &ObservedChannelIdentity) -> bool {
    is_bounded_string(&value.id, MAX_ID_LENGTH) && is_bounded_string(&value.label, MAX_ID_LENGTH)
}

fn is_bounded_string(value: &str, max_length: usize) -> bool {
    !value.is_empty() && value.encode_utf16().count() <= max_length
}

fn normalize_identity(value: &ObservedChannelIdentity) -> ObservedChannelIdentity {
    ObservedChannelIdentity {
        id: value.id.clone(),
        label: truncate_utf16(&value.label, MAX_LABEL_LENGTH),
    }
}

fn truncate_utf16(value: &str, max_length: usize) -> String {
    let mut result = String::new();
    let mut length = 0;
    for character in value.chars() {
        let character_length = character.len_utf16();
        if length + character_length > max_length {
            break;
        }
        result.push(character);
        length += character_length;
    }
    result
}

fn parse_observation(
    value: &Value,
) -> Result<PersistedObservedContact, ObservedChannelContactError> {
    let record = value
        .as_object()
        .ok_or(ObservedChannelContactError::InvalidRegistry)?;
    let channel_name = record
        .get("channelName")
        .and_then(Value::as_str)
        .filter(|value| is_bounded_string(value, MAX_CHANNEL_NAME_LENGTH))
        .ok_or(ObservedChannelContactError::InvalidRegistry)?;
    let user = parse_identity(record.get("user"))?;
    let group = match record.get("group") {
        None => None,
        Some(value) => Some(parse_identity(Some(value))?),
    };
    let topic = match record.get("topic") {
        None => None,
        Some(value) => Some(parse_identity(Some(value))?),
    };
    let last_observed_at = record
        .get("lastObservedAt")
        .and_then(Value::as_str)
        .filter(|value| parse_canonical_timestamp(value).is_some())
        .ok_or(ObservedChannelContactError::InvalidRegistry)?;
    if topic.is_some() && group.is_none() {
        return Err(ObservedChannelContactError::InvalidRegistry);
    }
    Ok(PersistedObservedContact {
        channel_name: channel_name.to_owned(),
        user,
        group,
        topic,
        last_observed_at: last_observed_at.to_owned(),
    })
}

fn parse_identity(
    value: Option<&Value>,
) -> Result<ObservedChannelIdentity, ObservedChannelContactError> {
    let record = value
        .and_then(Value::as_object)
        .ok_or(ObservedChannelContactError::InvalidRegistry)?;
    let id = record
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| is_bounded_string(value, MAX_ID_LENGTH))
        .ok_or(ObservedChannelContactError::InvalidRegistry)?;
    let label = record
        .get("label")
        .and_then(Value::as_str)
        .filter(|value| is_bounded_string(value, MAX_LABEL_LENGTH))
        .ok_or(ObservedChannelContactError::InvalidRegistry)?;
    Ok(ObservedChannelIdentity {
        id: id.to_owned(),
        label: label.to_owned(),
    })
}

fn parse_canonical_timestamp(value: &str) -> Option<DateTime<Utc>> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .ok()?
        .with_timezone(&Utc);
    (canonical_timestamp(timestamp) == value).then_some(timestamp)
}

fn canonical_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn observation_key(
    observation: &PersistedObservedContact,
) -> (String, String, Option<String>, Option<String>) {
    (
        observation.channel_name.clone(),
        observation.user.id.clone(),
        observation.group.as_ref().map(|group| group.id.clone()),
        observation.topic.as_ref().map(|topic| topic.id.clone()),
    )
}

fn aggregate_graph(observations: Vec<PersistedObservedContact>) -> ObservedChannelContactGraph {
    let mut graph = ObservedChannelContactGraph::default();
    let mut direct_users = HashMap::<(String, String), usize>::new();
    let mut group_indices = HashMap::<(String, String), usize>::new();
    let mut groups = Vec::<MutableGroup>::new();

    for observation in observations {
        let observed_at = observation.last_observed_at;
        let Some(group_identity) = observation.group else {
            let key = (
                observation.channel_name.clone(),
                observation.user.id.clone(),
            );
            if !direct_users.contains_key(&key) {
                direct_users.insert(key, graph.users.len());
                graph.users.push(ObservedChannelContact {
                    id: observation.user.id,
                    label: observation.user.label,
                    channel_name: observation.channel_name,
                    last_observed_at: observed_at,
                });
            }
            continue;
        };

        let group_key = (observation.channel_name.clone(), group_identity.id.clone());
        let group_index = *group_indices.entry(group_key).or_insert_with(|| {
            let index = groups.len();
            groups.push(MutableGroup {
                value: ObservedChannelGroup {
                    id: group_identity.id,
                    label: group_identity.label,
                    channel_name: observation.channel_name.clone(),
                    last_observed_at: observed_at.clone(),
                    users: Vec::new(),
                    topics: Vec::new(),
                },
                user_indices: HashMap::new(),
                topic_indices: HashMap::new(),
                topic_user_indices: Vec::new(),
            });
            index
        });
        let group = &mut groups[group_index];
        if !group.user_indices.contains_key(&observation.user.id) {
            group
                .user_indices
                .insert(observation.user.id.clone(), group.value.users.len());
            group.value.users.push(ObservedChannelRelatedContact {
                id: observation.user.id.clone(),
                label: observation.user.label.clone(),
                last_observed_at: observed_at.clone(),
            });
        }

        if let Some(topic_identity) = observation.topic {
            let topic_index = *group
                .topic_indices
                .entry(topic_identity.id.clone())
                .or_insert_with(|| {
                    let index = group.value.topics.len();
                    group.value.topics.push(ObservedChannelTopic {
                        id: topic_identity.id,
                        label: topic_identity.label,
                        last_observed_at: observed_at.clone(),
                        users: Vec::new(),
                    });
                    group.topic_user_indices.push(HashMap::new());
                    index
                });
            if !group.topic_user_indices[topic_index].contains_key(&observation.user.id) {
                let user_index = group.value.topics[topic_index].users.len();
                group.topic_user_indices[topic_index]
                    .insert(observation.user.id.clone(), user_index);
                group.value.topics[topic_index]
                    .users
                    .push(ObservedChannelRelatedContact {
                        id: observation.user.id,
                        label: observation.user.label,
                        last_observed_at: observed_at,
                    });
            }
        }
    }

    graph.groups = groups.into_iter().map(|group| group.value).collect();
    graph
}
