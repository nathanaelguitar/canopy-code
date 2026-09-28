//! Persistent activation and artifact mutations for installed local extensions.
//!
//! This ports the activation half of `ExtensionStore`: v2 snapshots remain
//! authoritative, `extension-enablement.json` remains the compatibility
//! projection, and writes share the lock-directory protocol used by the
//! TypeScript store, including the recoverable directory-swap journal used by
//! install, update, and uninstall.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::extension_activation::{
    ExtensionActivation, ExtensionIdentity, ExtensionPolicy, ExtensionStoreSnapshot, Override,
    WorkspaceActivation,
};
use crate::extension_inventory::parse_activation_snapshot;
use crate::extension_preferences::ExtensionScope;
use crate::utils::atomic_file_write::{
    AtomicWriteOptions, SymlinkPolicy, atomic_write_file, rename_with_retry,
};

const STATE_FILE: &str = "state.json";
const PREVIOUS_STATE_FILE: &str = "state.previous.json";
const ENABLEMENT_FILE: &str = "extension-enablement.json";
const MAX_STATE_BYTES: u64 = 1024 * 1024;
const MAX_JOURNAL_BYTES: u64 = 4 * 1024 * 1024;
const MAX_IDENTITIES: usize = 128;
const MAX_SAFE_GENERATION: u64 = 9_007_199_254_740_991;
const STALE_AFTER: Duration = Duration::from_secs(60);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const LOCK_RETRIES: usize = 60;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct LegacyEnablement {
    #[serde(default)]
    overrides: Vec<String>,
}

type LegacyProjection = IndexMap<String, LegacyEnablement>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtensionArtifactOperation {
    Install,
    Update,
    Uninstall,
}

#[derive(Clone, Debug)]
pub enum InitialExtensionActivation {
    User,
    Workspace { workspace_path: PathBuf },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TransactionPhase {
    Prepared,
    ArtifactSwapped,
    StateCommitted,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExtensionTransactionJournal {
    version: u32,
    transaction_id: String,
    operation: ExtensionArtifactOperation,
    phase: TransactionPhase,
    destination_directory: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    staging_directory: Option<String>,
    backup_directory: String,
    previous_generation: u64,
    target_generation: u64,
    target_snapshot: ExtensionStoreSnapshot,
}

#[derive(Debug)]
enum StateReadError {
    Io(String),
    Corrupt(String),
}

/// Set an installed extension's default user activation or exact workspace
/// activation. `identity` and `extensions` must come from the current bounded
/// installed-extension scan.
pub fn set_installed_extension_activation(
    extensions_dir: &Path,
    store_dir: &Path,
    identities: &[ExtensionIdentity],
    identity: &ExtensionIdentity,
    scope: ExtensionScope,
    workspace_path: &Path,
    user_home: &Path,
    activation: ExtensionActivation,
) -> Result<u64, String> {
    if !identities.iter().any(|candidate| candidate == identity) {
        return Err(format!(
            "Extension with name {} does not exist.",
            identity.name
        ));
    }
    if identities.len() > MAX_IDENTITIES {
        return Err(format!(
            "More than {MAX_IDENTITIES} installed extensions were found; activation changes are disabled until the inventory is within limits."
        ));
    }
    validate_identity(identity)?;

    let _lock = StoreLock::acquire(store_dir, extensions_dir)?;
    let mut snapshot = ensure_initialized(extensions_dir, store_dir, identities)?;
    let policy = snapshot
        .extensions
        .get_mut(&identity.id)
        .ok_or_else(|| format!("Extension \"{}\" is not installed.", identity.name))?;
    if policy.name != identity.name {
        return Err(format!(
            "Extension id {} belongs to \"{}\", not \"{}\".",
            identity.id, policy.name, identity.name
        ));
    }

    match scope {
        ExtensionScope::Project => {
            let workspace = canonicalize_workspace_path(workspace_path)?;
            policy.workspace_overrides.insert(
                workspace.to_string_lossy().into_owned(),
                match activation {
                    ExtensionActivation::Enabled => WorkspaceActivation::Enabled,
                    ExtensionActivation::Disabled => WorkspaceActivation::Disabled,
                },
            );
        }
        ExtensionScope::User => {
            set_legacy_path_activation(policy, user_home, activation)?;
        }
    }
    snapshot.generation = snapshot
        .generation
        .checked_add(1)
        .ok_or_else(|| "extension store generation is exhausted".to_owned())?;
    write_snapshot(extensions_dir, store_dir, &mut snapshot)?;
    Ok(snapshot.generation)
}

/// Create a fresh transaction directory under the extension store staging
/// root. The caller owns it until [`commit_extension_artifact`] succeeds or
/// the staging directory is removed.
pub fn create_extension_staging_directory(store_dir: &Path) -> Result<PathBuf, String> {
    prepare_store_directories(store_dir)?;
    let staging_root = store_dir.join("staging");
    for _ in 0..16 {
        let staging = staging_root.join(format!("transaction-{}", uuid::Uuid::new_v4()));
        match fs::create_dir(&staging) {
            Ok(()) => return Ok(staging),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(format!(
                    "Could not create extension staging directory: {error}"
                ));
            }
        }
    }
    Err("Could not allocate a unique extension staging directory.".to_owned())
}

/// Commit a prepared extension artifact with the TypeScript store's journal,
/// backup swap, snapshot commit point, rollback, and crash recovery semantics.
/// This API does not download, extract, convert, or validate extension content.
#[allow(clippy::too_many_arguments)]
pub fn commit_extension_artifact(
    extensions_dir: &Path,
    store_dir: &Path,
    operation: ExtensionArtifactOperation,
    identity: &ExtensionIdentity,
    destination_directory: &Path,
    staging_directory: Option<&Path>,
    initial_activation: Option<&InitialExtensionActivation>,
    expected_artifact_generation: Option<u64>,
    known_identities: &[ExtensionIdentity],
) -> Result<ExtensionStoreSnapshot, String> {
    validate_identity(identity)?;
    if known_identities.len() > MAX_IDENTITIES {
        return Err(format!(
            "More than {MAX_IDENTITIES} installed extensions were found; artifact mutations are disabled until the inventory is within limits."
        ));
    }
    assert_artifact_paths(
        extensions_dir,
        store_dir,
        operation,
        destination_directory,
        staging_directory,
    )?;
    let _lock = StoreLock::acquire(store_dir, extensions_dir)?;
    let mut snapshot = ensure_initialized(extensions_dir, store_dir, known_identities)?;
    let transaction_id = uuid::Uuid::new_v4().to_string();
    let transactions_dir = store_dir.join("transactions");
    let backup_directory = store_dir.join("rollback").join(&transaction_id);
    let journal_path = transactions_dir.join(format!("{transaction_id}.json"));
    let destination_exists = path_exists(destination_directory)?;

    if operation == ExtensionArtifactOperation::Install && destination_exists {
        return Err(format!("Extension \"{}\" is installed.", identity.name));
    }
    if operation == ExtensionArtifactOperation::Update && !destination_exists {
        return Err(format!("Extension \"{}\" is not installed.", identity.name));
    }
    if operation == ExtensionArtifactOperation::Install && initial_activation.is_none() {
        return Err("Install requires an initial activation.".to_owned());
    }
    if operation != ExtensionArtifactOperation::Uninstall {
        if snapshot.extensions.iter().any(|(extension_id, policy)| {
            extension_id != &identity.id && policy.name.eq_ignore_ascii_case(&identity.name)
        }) {
            return Err(format!(
                "Extension name \"{}\" conflicts with an installed extension.",
                identity.name
            ));
        }
    }

    let current_policy = snapshot.extensions.get(&identity.id).cloned();
    if let Some(policy) = current_policy.as_ref().filter(|p| p.name != identity.name) {
        return Err(format!(
            "Extension id belongs to \"{}\", not \"{}\".",
            policy.name, identity.name
        ));
    }
    if operation == ExtensionArtifactOperation::Uninstall && current_policy.is_none() {
        if !destination_exists {
            return Ok(snapshot);
        }
        return Err(format!(
            "Extension \"{}\" has no matching policy.",
            identity.name
        ));
    }
    if operation == ExtensionArtifactOperation::Update && current_policy.is_none() {
        return Err(format!("Extension \"{}\" is not installed.", identity.name));
    }
    if operation == ExtensionArtifactOperation::Update {
        if let Some(expected) = expected_artifact_generation {
            if current_policy
                .as_ref()
                .and_then(|policy| policy.artifact_generation)
                .unwrap_or(0)
                != expected
            {
                return Err(format!(
                    "Extension \"{}\" changed while its update was being prepared.",
                    identity.name
                ));
            }
        }
    }

    let mut target_snapshot = snapshot.clone();
    match operation {
        ExtensionArtifactOperation::Install => {
            let initial = initial_activation.expect("validated above");
            let (default_activation, workspace_overrides) = match initial {
                InitialExtensionActivation::User => (ExtensionActivation::Enabled, IndexMap::new()),
                InitialExtensionActivation::Workspace { workspace_path } => {
                    let workspace = canonicalize_workspace_path(workspace_path)?
                        .to_string_lossy()
                        .into_owned();
                    let mut overrides = IndexMap::new();
                    overrides.insert(workspace, WorkspaceActivation::Enabled);
                    (ExtensionActivation::Disabled, overrides)
                }
            };
            let artifact_generation = target_snapshot
                .generation
                .checked_add(1)
                .filter(|generation| *generation <= MAX_SAFE_GENERATION)
                .ok_or_else(|| "extension store generation is exhausted".to_owned())?;
            target_snapshot.extensions.insert(
                identity.id.clone(),
                ExtensionPolicy {
                    name: identity.name.clone(),
                    artifact_generation: Some(artifact_generation),
                    default_activation,
                    workspace_overrides,
                    legacy_path_rules: None,
                },
            );
        }
        ExtensionArtifactOperation::Uninstall => {
            target_snapshot.extensions.shift_remove(&identity.id);
        }
        ExtensionArtifactOperation::Update => {
            let mut policy = target_snapshot
                .extensions
                .get(&identity.id)
                .cloned()
                .ok_or_else(|| format!("Extension \"{}\" is not installed.", identity.name))?;
            if policy.name != identity.name {
                return Err(format!(
                    "Extension update changed name from \"{}\" to \"{}\".",
                    policy.name, identity.name
                ));
            }
            policy.artifact_generation = Some(
                target_snapshot
                    .generation
                    .checked_add(1)
                    .filter(|generation| *generation <= MAX_SAFE_GENERATION)
                    .ok_or_else(|| "extension store generation is exhausted".to_owned())?,
            );
            target_snapshot
                .extensions
                .insert(identity.id.clone(), policy);
        }
    }
    target_snapshot.generation = snapshot
        .generation
        .checked_add(1)
        .filter(|generation| *generation <= MAX_SAFE_GENERATION)
        .ok_or_else(|| "extension store generation is exhausted".to_owned())?;
    target_snapshot.legacy_projection_hash =
        hash_value(&build_legacy_projection(&target_snapshot))?;

    let journal = ExtensionTransactionJournal {
        version: 1,
        transaction_id,
        operation,
        phase: TransactionPhase::Prepared,
        destination_directory: destination_directory.to_string_lossy().into_owned(),
        staging_directory: staging_directory.map(|path| path.to_string_lossy().into_owned()),
        backup_directory: backup_directory.to_string_lossy().into_owned(),
        previous_generation: snapshot.generation,
        target_generation: target_snapshot.generation,
        target_snapshot: target_snapshot.clone(),
    };
    write_atomic_json(&journal_path, &journal)?;

    let mut state_committed = false;
    let transaction_result = (|| -> Result<(), String> {
        if destination_exists {
            rename_transaction_path(destination_directory, &backup_directory)?;
        }
        if operation != ExtensionArtifactOperation::Uninstall {
            rename_transaction_path(
                staging_directory.expect("asserted for install/update"),
                destination_directory,
            )?;
        }
        let mut journal = journal.clone();
        journal.phase = TransactionPhase::ArtifactSwapped;
        write_atomic_json(&journal_path, &journal)?;

        write_snapshot(extensions_dir, store_dir, &mut target_snapshot)?;
        state_committed = true;
        journal.phase = TransactionPhase::StateCommitted;
        // The snapshot generation is authoritative if this advisory phase
        // write fails after the state commit.
        let _ = write_atomic_json(&journal_path, &journal);
        Ok(())
    })();

    if let Err(error) = transaction_result {
        if !state_committed {
            if let Err(rollback_error) = rollback_journal(&journal) {
                return Err(format!(
                    "Extension transaction failed and rollback recovery did not complete: {error}; rollback failed: {rollback_error}"
                ));
            }
            remove_file_force(&journal_path).map_err(|rollback_error| {
                format!(
                    "Extension transaction failed and its journal could not be removed: {error}; {rollback_error}"
                )
            })?;
        }
        return Err(error);
    }

    let _ = cleanup_committed_journal(&journal, &journal_path);
    snapshot = target_snapshot;
    Ok(snapshot)
}

fn ensure_initialized(
    extensions_dir: &Path,
    store_dir: &Path,
    identities: &[ExtensionIdentity],
) -> Result<ExtensionStoreSnapshot, String> {
    let state_path = store_dir.join(STATE_FILE);
    let projection_path = extensions_dir.join(ENABLEMENT_FILE);
    let projection = read_projection(&projection_path)?;
    let projection_hash = hash_value(&projection.value)?;
    let mut snapshot = match read_state(&state_path)? {
        Some(snapshot) => snapshot,
        None => {
            let mut initial = ExtensionStoreSnapshot {
                version: 2,
                generation: 0,
                legacy_projection_hash: String::new(),
                extensions: IndexMap::new(),
            };
            for identity in identities {
                validate_identity(identity)?;
                let rules = projection
                    .policies
                    .get(&identity.name)
                    .map(|policy| policy.overrides.clone())
                    .unwrap_or_default();
                initial.extensions.insert(
                    identity.id.clone(),
                    ExtensionPolicy {
                        name: identity.name.clone(),
                        artifact_generation: None,
                        default_activation: ExtensionActivation::Enabled,
                        workspace_overrides: IndexMap::new(),
                        legacy_path_rules: (!rules.is_empty()).then_some(rules),
                    },
                );
            }
            write_snapshot(extensions_dir, store_dir, &mut initial)?;
            return Ok(initial);
        }
    };

    let legacy_is_newer =
        projection_is_newer(&projection_path, &state_path, &snapshot, &projection_hash)?;
    let mut changed = false;
    let loaded_ids = identities
        .iter()
        .map(|identity| identity.id.as_str())
        .collect::<std::collections::HashSet<_>>();

    for identity in identities {
        validate_identity(identity)?;
        if snapshot.extensions.contains_key(&identity.id) {
            continue;
        }
        let stale_id = snapshot
            .extensions
            .iter()
            .find(|(id, policy)| {
                !loaded_ids.contains(id.as_str())
                    && policy.name.eq_ignore_ascii_case(&identity.name)
            })
            .map(|(id, _)| id.clone());
        if let Some(stale_id) = stale_id {
            if let Some(mut policy) = snapshot.extensions.shift_remove(&stale_id) {
                policy.name.clone_from(&identity.name);
                snapshot.extensions.insert(identity.id.clone(), policy);
                changed = true;
            }
        }
    }

    if legacy_is_newer {
        let mut all_identities = snapshot
            .extensions
            .iter()
            .map(|(id, policy)| ExtensionIdentity {
                id: id.clone(),
                name: policy.name.clone(),
            })
            .collect::<Vec<_>>();
        for identity in identities {
            if !all_identities
                .iter()
                .any(|existing| existing.id == identity.id)
            {
                all_identities.push(identity.clone());
            }
        }
        for identity in all_identities {
            validate_identity(&identity)?;
            if !snapshot.extensions.contains_key(&identity.id) {
                snapshot.extensions.insert(
                    identity.id.clone(),
                    ExtensionPolicy {
                        name: identity.name.clone(),
                        artifact_generation: None,
                        default_activation: ExtensionActivation::Enabled,
                        workspace_overrides: IndexMap::new(),
                        legacy_path_rules: None,
                    },
                );
                changed = true;
            }
            let policy = snapshot
                .extensions
                .get_mut(&identity.id)
                .expect("policy inserted above");
            let incoming = projection
                .policies
                .get(&identity.name)
                .map(|policy| policy.overrides.as_slice())
                .unwrap_or_default();
            let (rules, activation_changed) = import_legacy_rules(policy, incoming);
            let previous_rules = policy.legacy_path_rules.clone().unwrap_or_default();
            if activation_changed || previous_rules != rules || policy.name != identity.name {
                policy.name.clone_from(&identity.name);
                policy.legacy_path_rules = (!rules.is_empty()).then_some(rules);
                changed = true;
            }
        }
    } else {
        if snapshot.legacy_projection_hash != projection_hash {
            // The v2 state wins when it is at least as new as the projection.
            // A projection repair is advisory, as in ExtensionStore.
            let _ = write_projection(&projection_path, &build_legacy_projection(&snapshot));
        }
        for identity in identities {
            if snapshot.extensions.contains_key(&identity.id) {
                continue;
            }
            snapshot.extensions.insert(
                identity.id.clone(),
                ExtensionPolicy {
                    name: identity.name.clone(),
                    artifact_generation: None,
                    default_activation: ExtensionActivation::Enabled,
                    workspace_overrides: IndexMap::new(),
                    legacy_path_rules: None,
                },
            );
            changed = true;
        }
    }

    if changed {
        snapshot.generation = snapshot
            .generation
            .checked_add(1)
            .ok_or_else(|| "extension store generation is exhausted".to_owned())?;
        write_snapshot(extensions_dir, store_dir, &mut snapshot)?;
    } else if legacy_is_newer {
        // Legacy projection has already been imported. The v2 snapshot remains
        // authoritative after migration, so repairing this file is best effort.
        let _ = write_projection(&projection_path, &build_legacy_projection(&snapshot));
    }
    Ok(snapshot)
}

struct ReadProjection {
    policies: LegacyProjection,
    value: Value,
}

fn read_projection(path: &Path) -> Result<ReadProjection, String> {
    let value = match read_json_file(path, MAX_STATE_BYTES)? {
        Some(value) => value,
        None => Value::Object(Map::new()),
    };
    if !value.is_object() {
        return Err(format!(
            "Extension enablement projection is corrupt at {}.",
            path.display()
        ));
    }
    let policies = serde_json::from_value::<LegacyProjection>(value.clone()).map_err(|_| {
        format!(
            "Extension enablement projection is corrupt at {}.",
            path.display()
        )
    })?;
    Ok(ReadProjection { policies, value })
}

fn read_state(path: &Path) -> Result<Option<ExtensionStoreSnapshot>, String> {
    read_state_detailed(path).map_err(|error| match error {
        StateReadError::Io(error) | StateReadError::Corrupt(error) => error,
    })
}

fn read_state_detailed(path: &Path) -> Result<Option<ExtensionStoreSnapshot>, StateReadError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(StateReadError::Io(format!(
                "Could not read {}: {error}",
                path.display()
            )));
        }
    };
    let metadata = file.metadata().map_err(|error| {
        StateReadError::Io(format!("Could not inspect {}: {error}", path.display()))
    })?;
    if metadata.len() > MAX_STATE_BYTES {
        return Err(StateReadError::Corrupt(format!(
            "Extension store state is corrupt at {}: exceeds the {}-byte limit.",
            path.display(),
            MAX_STATE_BYTES
        )));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_STATE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            StateReadError::Io(format!("Could not read {}: {error}", path.display()))
        })?;
    if bytes.len() as u64 > MAX_STATE_BYTES {
        return Err(StateReadError::Corrupt(format!(
            "Extension store state is corrupt at {}: exceeds the {}-byte limit.",
            path.display(),
            MAX_STATE_BYTES
        )));
    }
    let value = serde_json::from_slice::<Value>(&bytes).map_err(|error| {
        StateReadError::Corrupt(format!(
            "Extension store state is corrupt at {}: {error}.",
            path.display()
        ))
    })?;
    parse_activation_snapshot(&value)
        .map(Some)
        .map_err(|error| {
            StateReadError::Corrupt(format!(
                "Extension store state is corrupt at {}: {error}.",
                path.display()
            ))
        })
}

fn recover_extension_store(extensions_dir: &Path, store_dir: &Path) -> Result<(), String> {
    recover_corrupt_state(extensions_dir, store_dir)?;
    recover_transactions(extensions_dir, store_dir)
}

fn recover_corrupt_state(extensions_dir: &Path, store_dir: &Path) -> Result<(), String> {
    let state_path = store_dir.join(STATE_FILE);
    let state_missing = match read_state_detailed(&state_path) {
        Ok(Some(_)) => return Ok(()),
        Ok(None) => true,
        Err(StateReadError::Io(error)) => return Err(error),
        Err(StateReadError::Corrupt(_)) => false,
    };

    let mut candidates = Vec::<(u64, ExtensionStoreSnapshot)>::new();
    for journal_path in transaction_journal_paths(store_dir)? {
        let Some(journal) = read_recoverable_journal(extensions_dir, store_dir, &journal_path)?
        else {
            continue;
        };
        if journal.phase == TransactionPhase::StateCommitted {
            candidates.push((journal.target_generation, journal.target_snapshot));
        }
    }

    let previous_path = store_dir.join(PREVIOUS_STATE_FILE);
    let previous_missing = match read_state_detailed(&previous_path) {
        Ok(Some(previous)) => {
            candidates.push((previous.generation, previous));
            false
        }
        Ok(None) => true,
        Err(StateReadError::Io(error)) => return Err(error),
        Err(StateReadError::Corrupt(_)) => false,
    };

    let mut latest: Option<(u64, ExtensionStoreSnapshot)> = None;
    for candidate in candidates {
        if latest
            .as_ref()
            .is_none_or(|(generation, _)| candidate.0 > *generation)
        {
            latest = Some(candidate);
        }
    }
    if let Some((generation, mut snapshot)) = latest {
        snapshot.generation = generation;
        write_atomic_json(&state_path, &snapshot)?;
        write_projection(
            &extensions_dir.join(ENABLEMENT_FILE),
            &build_legacy_projection(&snapshot),
        )?;
        return Ok(());
    }
    if state_missing && previous_missing {
        return Ok(());
    }
    Err(format!(
        "Extension store state and recovery data are corrupt at {}.",
        store_dir.display()
    ))
}

fn recover_transactions(extensions_dir: &Path, store_dir: &Path) -> Result<(), String> {
    let snapshot = read_state(&store_dir.join(STATE_FILE))?;
    for journal_path in transaction_journal_paths(store_dir)? {
        let Some(journal) = read_recoverable_journal(extensions_dir, store_dir, &journal_path)?
        else {
            continue;
        };
        if journal.phase == TransactionPhase::StateCommitted
            || snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.generation >= journal.target_generation)
        {
            // The committed state is authoritative; a later operation retries
            // cleanup if the backup or advisory journal survived this attempt.
            let _ = cleanup_committed_journal(&journal, &journal_path);
        } else {
            rollback_journal(&journal)?;
            remove_file_force(&journal_path)?;
        }
    }
    Ok(())
}

fn transaction_journal_paths(store_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let directory = store_dir.join("transactions");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(format!(
                "Could not enumerate extension transactions: {error}"
            ));
        }
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry
            .map_err(|error| format!("Could not enumerate extension transactions: {error}"))?;
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            paths.push(entry.path());
        }
    }
    paths.sort();
    Ok(paths)
}

fn read_recoverable_journal(
    extensions_dir: &Path,
    store_dir: &Path,
    journal_path: &Path,
) -> Result<Option<ExtensionTransactionJournal>, String> {
    match read_journal(extensions_dir, store_dir, journal_path) {
        Ok(journal) => Ok(Some(journal)),
        Err(error) => {
            let quarantined =
                append_suffix(journal_path, &format!(".corrupt-{}", uuid::Uuid::new_v4()));
            fs::rename(journal_path, &quarantined).map_err(|quarantine_error| {
                format!(
                    "Extension transaction journal is corrupt and could not be quarantined at {}: {error}; {quarantine_error}",
                    journal_path.display()
                )
            })?;
            eprintln!(
                "[warn] Quarantined corrupt extension transaction journal at {}: {error}",
                quarantined.display()
            );
            Ok(None)
        }
    }
}

fn read_journal(
    extensions_dir: &Path,
    store_dir: &Path,
    journal_path: &Path,
) -> Result<ExtensionTransactionJournal, String> {
    let file =
        File::open(journal_path).map_err(|error| format!("could not read journal: {error}"))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("could not inspect journal: {error}"))?;
    if metadata.len() > MAX_JOURNAL_BYTES {
        return Err(format!(
            "journal exceeds the {MAX_JOURNAL_BYTES}-byte limit"
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_JOURNAL_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("could not read journal: {error}"))?;
    if bytes.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(format!(
            "journal exceeds the {MAX_JOURNAL_BYTES}-byte limit"
        ));
    }
    let journal = serde_json::from_slice::<ExtensionTransactionJournal>(&bytes)
        .map_err(|error| format!("invalid journal JSON: {error}"))?;
    if journal.version != 1
        || journal.transaction_id.is_empty()
        || journal.transaction_id.len() > 128
        || !journal
            .transaction_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || journal.previous_generation > MAX_SAFE_GENERATION
        || journal.previous_generation.checked_add(1) != Some(journal.target_generation)
        || journal.target_generation > MAX_SAFE_GENERATION
    {
        return Err("invalid transaction journal schema".to_owned());
    }
    let target_value = serde_json::to_value(&journal.target_snapshot)
        .map_err(|error| format!("invalid target snapshot: {error}"))?;
    let target_snapshot = parse_activation_snapshot(&target_value)
        .map_err(|error| format!("invalid target snapshot: {error}"))?;
    if target_snapshot.generation != journal.target_generation {
        return Err("transaction target generation does not match".to_owned());
    }
    let journal = ExtensionTransactionJournal {
        target_snapshot,
        ..journal
    };
    assert_recovered_journal_paths(extensions_dir, store_dir, journal_path, &journal)?;
    Ok(journal)
}

fn assert_recovered_journal_paths(
    extensions_dir: &Path,
    store_dir: &Path,
    journal_path: &Path,
    journal: &ExtensionTransactionJournal,
) -> Result<(), String> {
    let transactions_root = resolved_directory(&store_dir.join("transactions"))?;
    let extensions_root = resolved_directory(extensions_dir)?;
    let staging_root = resolved_directory(&store_dir.join("staging"))?;
    let rollback_root = resolved_directory(&store_dir.join("rollback"))?;
    let journal_path = resolved_parent_path(journal_path)?;
    let destination = resolved_parent_path(Path::new(&journal.destination_directory))?;
    let backup = resolved_parent_path(Path::new(&journal.backup_directory))?;
    let staging = journal
        .staging_directory
        .as_deref()
        .map(Path::new)
        .map(resolved_parent_path)
        .transpose()?;
    if journal_path.parent() != Some(transactions_root.as_path())
        || journal_path.file_name().and_then(|name| name.to_str())
            != Some(format!("{}.json", journal.transaction_id).as_str())
        || destination.parent() != Some(extensions_root.as_path())
        || backup.parent() != Some(rollback_root.as_path())
        || staging
            .as_ref()
            .is_some_and(|path| path.parent() != Some(staging_root.as_path()))
        || (journal.operation == ExtensionArtifactOperation::Uninstall && staging.is_some())
        || (journal.operation != ExtensionArtifactOperation::Uninstall && staging.is_none())
    {
        return Err(format!(
            "Extension transaction {} contains unsafe paths.",
            journal.transaction_id
        ));
    }
    Ok(())
}

fn rollback_journal(journal: &ExtensionTransactionJournal) -> Result<(), String> {
    let destination = PathBuf::from(&journal.destination_directory);
    let backup = PathBuf::from(&journal.backup_directory);
    let has_backup = path_exists(&backup)?;
    let staging_exists = journal
        .staging_directory
        .as_deref()
        .map(Path::new)
        .map(path_exists)
        .transpose()?
        .unwrap_or(false);
    if has_backup || (journal.operation == ExtensionArtifactOperation::Install && !staging_exists) {
        remove_tree_force(&destination)?;
    }
    if has_backup {
        rename_transaction_path(&backup, &destination)?;
    }
    if let Some(staging) = journal.staging_directory.as_deref() {
        remove_tree_force(Path::new(staging))?;
    }
    Ok(())
}

fn cleanup_committed_journal(
    journal: &ExtensionTransactionJournal,
    journal_path: &Path,
) -> Result<(), String> {
    remove_tree_force(Path::new(&journal.backup_directory))?;
    if let Some(staging) = journal.staging_directory.as_deref() {
        remove_tree_force(Path::new(staging))?;
    }
    remove_file_force(journal_path)
}

fn assert_artifact_paths(
    extensions_dir: &Path,
    store_dir: &Path,
    operation: ExtensionArtifactOperation,
    destination: &Path,
    staging: Option<&Path>,
) -> Result<(), String> {
    let extensions_root = resolved_directory(extensions_dir)?;
    let destination = resolved_parent_path(destination)?;
    if destination.parent() != Some(extensions_root.as_path()) || destination == extensions_root {
        return Err("Extension destination must be a direct child.".to_owned());
    }
    if operation == ExtensionArtifactOperation::Uninstall {
        if staging.is_some() {
            return Err("Uninstall does not accept a staging directory.".to_owned());
        }
        return Ok(());
    }
    let staging = staging.ok_or_else(|| format!("{operation:?} requires a staging directory."))?;
    let staging_root = resolved_directory(&store_dir.join("staging"))?;
    let staging = resolved_parent_path(staging)?;
    if staging.parent() != Some(staging_root.as_path()) {
        return Err("Extension staging directory is outside the store.".to_owned());
    }
    Ok(())
}

fn path_exists(path: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("Could not inspect {}: {error}", path.display())),
    }
}

fn remove_tree_force(path: &Path) -> Result<(), String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("Could not inspect {}: {error}", path.display())),
    };
    let result = if metadata.file_type().is_symlink() || metadata.is_file() {
        fs::remove_file(path)
    } else {
        fs::remove_dir_all(path)
    };
    result.map_err(|error| format!("Could not remove {}: {error}", path.display()))
}

fn remove_file_force(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("Could not remove {}: {error}", path.display())),
    }
}

fn rename_transaction_path(source: &Path, destination: &Path) -> Result<(), String> {
    rename_with_retry(
        source,
        destination,
        3,
        Duration::from_millis(50),
        |from, to| fs::rename(from, to),
    )
    .map_err(|error| {
        format!(
            "Could not move {} to {}: {error}",
            source.display(),
            destination.display()
        )
    })
}

fn resolved_directory(path: &Path) -> Result<PathBuf, String> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => resolved_path(path),
        Err(error) => Err(format!("Could not resolve {}: {error}", path.display())),
    }
}

fn resolved_parent_path(path: &Path) -> Result<PathBuf, String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("Could not resolve {}.", path.display()))?;
    let filename = path
        .file_name()
        .ok_or_else(|| format!("Could not resolve {}.", path.display()))?;
    if filename == "." || filename == ".." {
        return Err(format!("Could not resolve {}.", path.display()));
    }
    let parent = resolved_directory(parent)?;
    Ok(parent.join(filename))
}

fn resolved_path(path: &Path) -> Result<PathBuf, String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("Could not resolve path: {error}"))?
            .join(path)
    };
    Ok(normalize_absolute(&absolute))
}

fn read_json_file(path: &Path, max_bytes: u64) -> Result<Option<Value>, String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("Could not read {}: {error}", path.display())),
    };
    let metadata = file
        .metadata()
        .map_err(|error| format!("Could not inspect {}: {error}", path.display()))?;
    if metadata.len() > max_bytes {
        return Err(format!(
            "Extension state at {} exceeds the {max_bytes}-byte limit.",
            path.display()
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("Could not read {}: {error}", path.display()))?;
    if bytes.len() as u64 > max_bytes {
        return Err(format!(
            "Extension state at {} exceeds the {max_bytes}-byte limit.",
            path.display()
        ));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| format!("Extension state is corrupt at {}: {error}", path.display()))
}

fn projection_is_newer(
    projection_path: &Path,
    state_path: &Path,
    snapshot: &ExtensionStoreSnapshot,
    projection_hash: &str,
) -> Result<bool, String> {
    if !state_path.exists() {
        return Ok(true);
    }
    if !projection_path.exists() {
        return Ok(false);
    }
    let projection_modified = fs::metadata(projection_path)
        .and_then(|metadata| metadata.modified())
        .map_err(|error| format!("Could not inspect extension projection: {error}"))?;
    let state_modified = fs::metadata(state_path)
        .and_then(|metadata| metadata.modified())
        .map_err(|error| format!("Could not inspect extension store state: {error}"))?;
    if projection_modified == state_modified {
        if projection_hash != snapshot.legacy_projection_hash {
            return Err(format!(
                "Extension store state and projection disagree at the same timestamp in {}.",
                state_path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .display()
            ));
        }
        return Ok(false);
    }
    Ok(projection_modified > state_modified)
}

fn build_legacy_projection(snapshot: &ExtensionStoreSnapshot) -> Value {
    let mut entries = Vec::new();
    for policy in snapshot.extensions.values() {
        let mut overrides = Vec::new();
        if policy.default_activation == ExtensionActivation::Disabled {
            overrides.push(Value::String("!/*".to_owned()));
        }
        overrides.extend(
            policy
                .legacy_path_rules
                .iter()
                .flatten()
                .cloned()
                .map(Value::String),
        );
        for (workspace, activation) in &policy.workspace_overrides {
            let effective = match activation {
                WorkspaceActivation::Inherit => policy.default_activation,
                WorkspaceActivation::Enabled => ExtensionActivation::Enabled,
                WorkspaceActivation::Disabled => ExtensionActivation::Disabled,
            };
            let input = if effective == ExtensionActivation::Disabled {
                format!("!{workspace}")
            } else {
                workspace.clone()
            };
            overrides.push(Value::String(Override::from_input(&input, false).output()));
        }
        if !overrides.is_empty() {
            let mut entry = Map::new();
            entry.insert("overrides".to_owned(), Value::Array(overrides));
            entries.push((policy.name.clone(), Value::Object(entry)));
        }
    }
    // JavaScript JSON.stringify enumerates integer-index keys before other
    // string keys, even when an insertion-ordered object was used.
    let mut indexed = entries
        .iter()
        .filter_map(|(name, value)| array_index_key(name).map(|index| (index, name, value)))
        .collect::<Vec<_>>();
    indexed.sort_by_key(|(index, _, _)| *index);
    let mut projection = Map::new();
    for (_, name, value) in indexed {
        projection.insert(name.clone(), value.clone());
    }
    for (name, value) in entries {
        if array_index_key(&name).is_none() {
            projection.insert(name, value);
        }
    }
    Value::Object(projection)
}

fn import_legacy_rules(
    policy: &mut ExtensionPolicy,
    incoming_rules: &[String],
) -> (Vec<String>, bool) {
    let mut generated_rules = Vec::<(Override, Option<String>)>::new();
    if policy.default_activation == ExtensionActivation::Disabled {
        generated_rules.push((Override::from_file_rule("!/*"), None));
    }
    for (workspace, activation) in &policy.workspace_overrides {
        let effective = match activation {
            WorkspaceActivation::Enabled => ExtensionActivation::Enabled,
            WorkspaceActivation::Disabled => ExtensionActivation::Disabled,
            WorkspaceActivation::Inherit => policy.default_activation,
        };
        let input = if effective == ExtensionActivation::Disabled {
            format!("!{workspace}")
        } else {
            workspace.clone()
        };
        generated_rules.push((Override::from_input(&input, false), Some(workspace.clone())));
    }
    let mut consumed = vec![false; generated_rules.len()];
    let mut imported = Vec::new();
    let mut activation_changed = false;
    for rule in incoming_rules {
        let incoming = Override::from_file_rule(rule);
        if let Some(index) = generated_rules
            .iter()
            .enumerate()
            .find(|(index, (generated, _))| !consumed[*index] && generated.is_equal_to(&incoming))
            .map(|(index, _)| index)
        {
            consumed[index] = true;
            continue;
        }
        if let Some(index) = generated_rules
            .iter()
            .enumerate()
            .find(|(index, (generated, _))| {
                !consumed[*index]
                    && generated.base_rule == incoming.base_rule
                    && generated.include_subdirs == incoming.include_subdirs
                    && generated.is_disable != incoming.is_disable
            })
            .map(|(index, _)| index)
        {
            consumed[index] = true;
            if let Some(workspace) = generated_rules[index].1.as_ref() {
                policy.workspace_overrides.insert(
                    workspace.clone(),
                    if incoming.is_disable {
                        WorkspaceActivation::Disabled
                    } else {
                        WorkspaceActivation::Enabled
                    },
                );
            } else {
                policy.default_activation = ExtensionActivation::Enabled;
            }
            activation_changed = true;
            continue;
        }
        imported.push(rule.clone());
    }
    (imported, activation_changed)
}

fn set_legacy_path_activation(
    policy: &mut ExtensionPolicy,
    scope_path: &Path,
    activation: ExtensionActivation,
) -> Result<(), String> {
    let canonical_scope = canonicalize_workspace_path(scope_path)?
        .to_string_lossy()
        .into_owned();
    let scope = Override::from_input(&canonical_scope, true);
    policy
        .workspace_overrides
        .retain(|workspace, _| !scope.matches_path(&normalize_rule_path(workspace)));
    let next_rule = Override::from_input(
        &if activation == ExtensionActivation::Disabled {
            format!("!{canonical_scope}")
        } else {
            canonical_scope
        },
        true,
    );
    let rules = policy
        .legacy_path_rules
        .take()
        .unwrap_or_default()
        .into_iter()
        .filter(|rule| {
            let existing = Override::from_file_rule(rule);
            !existing.conflicts_with(&next_rule)
                && !existing.is_equal_to(&next_rule)
                && !existing.is_child_of(&next_rule)
        })
        .chain(std::iter::once(next_rule.output()))
        .collect::<Vec<_>>();
    policy.legacy_path_rules = Some(rules);
    Ok(())
}

fn normalize_rule_path(path: &str) -> String {
    let mut normalized = path.replace('\\', "/");
    if !normalized.starts_with('/') {
        normalized.insert(0, '/');
    }
    if !normalized.ends_with('/') {
        normalized.push('/');
    }
    normalized
}

fn canonicalize_workspace_path(path: &Path) -> Result<PathBuf, String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("Could not resolve workspace path: {error}"))?
            .join(path)
    };
    match fs::canonicalize(&absolute) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(normalize_absolute(&absolute)),
        Err(error) => Err(format!("Could not resolve workspace path: {error}")),
    }
}

fn normalize_absolute(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if normalized.file_name().is_some() {
                    normalized.pop();
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn validate_identity(identity: &ExtensionIdentity) -> Result<(), String> {
    if identity.id.len() != 64
        || !identity
            .id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("Invalid extension id \"{}\".", identity.id));
    }
    if identity.name.is_empty()
        || !identity
            .name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
    {
        return Err("Invalid extension name.".to_owned());
    }
    Ok(())
}

fn hash_value(value: &Value) -> Result<String, String> {
    let serialized = serde_json::to_vec(&javascript_json_order(value.clone()))
        .map_err(|error| error.to_string())?;
    Ok(format!("{:x}", Sha256::digest(serialized)))
}

fn javascript_json_order(value: Value) -> Value {
    match value {
        Value::Array(values) => {
            Value::Array(values.into_iter().map(javascript_json_order).collect())
        }
        Value::Object(object) => {
            let mut entries = object
                .into_iter()
                .map(|(key, value)| (key, javascript_json_order(value)))
                .collect::<Vec<_>>();
            let mut indexed = entries
                .iter()
                .filter_map(|(key, value)| array_index_key(key).map(|index| (index, key, value)))
                .collect::<Vec<_>>();
            indexed.sort_by_key(|(index, _, _)| *index);
            let mut ordered = Map::new();
            for (_, key, value) in indexed {
                ordered.insert(key.clone(), value.clone());
            }
            for (key, value) in entries.drain(..) {
                if array_index_key(&key).is_none() {
                    ordered.insert(key, value);
                }
            }
            Value::Object(ordered)
        }
        value => value,
    }
}

fn array_index_key(key: &str) -> Option<u32> {
    let value = key.parse::<u32>().ok()?;
    (value < u32::MAX && value.to_string() == key).then_some(value)
}

fn write_snapshot(
    extensions_dir: &Path,
    store_dir: &Path,
    snapshot: &mut ExtensionStoreSnapshot,
) -> Result<(), String> {
    let projection = build_legacy_projection(snapshot);
    snapshot.legacy_projection_hash = hash_value(&projection)?;
    fs::create_dir_all(extensions_dir)
        .map_err(|error| format!("Could not prepare extension directory: {error}"))?;
    fs::create_dir_all(store_dir)
        .map_err(|error| format!("Could not prepare extension store: {error}"))?;

    let state_path = store_dir.join(STATE_FILE);
    if let Some(previous) = read_json_file(&state_path, MAX_STATE_BYTES)? {
        let previous_state = parse_activation_snapshot(&previous).map_err(|error| {
            format!(
                "Extension store state is corrupt at {}: {error}.",
                state_path.display()
            )
        })?;
        write_atomic_json(&store_dir.join(PREVIOUS_STATE_FILE), &previous_state)?;
    }
    write_atomic_json(&state_path, snapshot)?;
    // state.json is the commit point; TypeScript treats projection repair as
    // best effort after this write for the same reason.
    let _ = write_projection(&extensions_dir.join(ENABLEMENT_FILE), &projection);
    Ok(())
}

fn write_projection(path: &Path, projection: &Value) -> Result<(), String> {
    write_atomic_json(path, projection)
}

fn write_atomic_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    let options = AtomicWriteOptions {
        mode: Some(0o600),
        force_mode: true,
        symlink_policy: SymlinkPolicy::NoFollow,
        ..AtomicWriteOptions::default()
    };
    atomic_write_file(path, &bytes, &options)
        .map_err(|error| format!("Could not write {}: {error}", path.display()))
}

fn prepare_directories(store_dir: &Path, extensions_dir: &Path) -> Result<(), String> {
    prepare_extension_directory(extensions_dir)?;
    prepare_store_directories(store_dir)
}

fn prepare_extension_directory(extensions_dir: &Path) -> Result<(), String> {
    fs::create_dir_all(extensions_dir)
        .map_err(|error| format!("Could not prepare extension directory: {error}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(extensions_dir, fs::Permissions::from_mode(0o700))
            .map_err(|error| format!("Could not secure extension directory: {error}"))?;
    }
    Ok(())
}

fn prepare_store_directories(store_dir: &Path) -> Result<(), String> {
    for directory in [
        store_dir.to_path_buf(),
        store_dir.join("staging"),
        store_dir.join("rollback"),
        store_dir.join("transactions"),
    ] {
        fs::create_dir_all(&directory)
            .map_err(|error| format!("Could not prepare extension store: {error}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
                .map_err(|error| format!("Could not secure extension store: {error}"))?;
        }
    }
    let marker = store_dir.join("lock");
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(&marker)
        .map_err(|error| format!("Could not prepare extension store lock: {error}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("Could not secure extension store lock: {error}"))?;
    }
    Ok(())
}

struct StoreLock {
    directory: PathBuf,
    stop: Option<mpsc::Sender<()>>,
    heartbeat: Option<JoinHandle<()>>,
}

impl StoreLock {
    fn acquire(store_dir: &Path, extensions_dir: &Path) -> Result<Self, String> {
        prepare_directories(store_dir, extensions_dir)?;
        let lock_directory = append_suffix(&store_dir.join("lock"), ".lock");
        for attempt in 0..=LOCK_RETRIES {
            match fs::create_dir(&lock_directory) {
                Ok(()) => {
                    touch_lock_directory(&lock_directory).map_err(|error| {
                        let _ = fs::remove_dir(&lock_directory);
                        format!("Could not initialize extension store lock: {error}")
                    })?;
                    let (stop, receiver) = mpsc::channel();
                    let heartbeat_path = lock_directory.clone();
                    let heartbeat = thread::spawn(move || {
                        loop {
                            match receiver.recv_timeout(HEARTBEAT_INTERVAL) {
                                Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                                Err(RecvTimeoutError::Timeout) => {
                                    if let Err(error) = touch_lock_directory(&heartbeat_path) {
                                        eprintln!(
                                            "[warn] Extension store lock heartbeat failed: {error}"
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    });
                    let lock = Self {
                        directory: lock_directory,
                        stop: Some(stop),
                        heartbeat: Some(heartbeat),
                    };
                    recover_extension_store(extensions_dir, store_dir)?;
                    return Ok(lock);
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    if lock_is_stale(&lock_directory) {
                        let _ = fs::remove_dir_all(&lock_directory);
                    }
                    if attempt == LOCK_RETRIES {
                        break;
                    }
                    thread::sleep(lock_retry_delay(attempt));
                }
                Err(error) => {
                    return Err(format!("Could not acquire extension store lock: {error}"));
                }
            }
        }
        Err(format!(
            "Extension store is busy at {}.",
            store_dir.display()
        ))
    }
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(heartbeat) = self.heartbeat.take() {
            let _ = heartbeat.join();
        }
        let _ = fs::remove_dir(&self.directory);
    }
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

fn touch_lock_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.set_times(std::fs::FileTimes::new().set_modified(SystemTime::now()))
}

fn lock_is_stale(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    SystemTime::now()
        .duration_since(modified)
        .is_ok_and(|age| age > STALE_AFTER)
}

fn lock_retry_delay(attempt: usize) -> Duration {
    let exponent = (attempt as i32).min(16);
    let base = (50.0_f64 * 1.2_f64.powi(exponent)).min(500.0) as u64;
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        % (base + 1);
    Duration::from_millis((base / 2 + jitter).min(500))
}
