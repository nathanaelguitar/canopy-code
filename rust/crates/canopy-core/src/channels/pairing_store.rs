//! File-backed pairing requests and user/group allowlists.
//!
//! Port of `packages/channels/base/src/PairingStore.ts`. A workspace scope
//! gets its own directory under the global `channels/` directory. The first
//! construction in each workspace/channel pair best-effort copies legacy
//! global state into that scope and closes the migration gate with a
//! per-channel sentinel.

use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};
use uuid::Uuid;

use super::{CreatePairingRequestResult, PairingRejection, PairingStore};

const SAFE_ALPHABET: &[u8; 32] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const CODE_LENGTH: usize = 8;
const EXPIRY_MS: f64 = 60.0 * 60.0 * 1000.0;
const MAX_PENDING: usize = 3;

static STORE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn store_lock() -> MutexGuard<'static, ()> {
    STORE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PairingSubjectType {
    User,
    Group,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PairingSubject {
    #[serde(rename = "type")]
    pub subject_type: PairingSubjectType,
    pub id: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairingRequest {
    pub sender_id: String,
    pub sender_name: String,
    pub subject: PairingSubject,
    pub code: String,
    pub created_at: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredPairingRequest {
    sender_id: String,
    sender_name: String,
    #[serde(default)]
    subject: Option<PairingSubject>,
    code: String,
    created_at: f64,
}

impl From<StoredPairingRequest> for PairingRequest {
    fn from(request: StoredPairingRequest) -> Self {
        let subject = request.subject.unwrap_or_else(|| PairingSubject {
            subject_type: PairingSubjectType::User,
            id: request.sender_id.clone(),
            name: request.sender_name.clone(),
        });
        Self {
            sender_id: request.sender_id,
            sender_name: request.sender_name,
            subject,
            code: request.code,
            created_at: request.created_at,
        }
    }
}

/// Persistent pairing state for a single channel and optional workspace.
pub struct FilePairingStore {
    channel_name: String,
    channels_root: PathBuf,
    dir: PathBuf,
    pending_path: PathBuf,
    allowlist_path: PathBuf,
    group_allowlist_path: PathBuf,
    migrated_sentinel_path: PathBuf,
    workspace_scoped: bool,
}

impl FilePairingStore {
    /// Use the current Qwen home (`QWEN_HOME`, then `~/.qwen`) and optionally
    /// scope the channel state to `workspace_cwd`.
    pub fn new(channel_name: impl Into<String>, workspace_cwd: Option<&str>) -> io::Result<Self> {
        Self::with_channels_root(
            channel_name,
            super::paths::global_channels_root()?,
            workspace_cwd,
        )
    }

    /// Construct a store below an explicit `channels/` root.
    ///
    /// This is useful to embed the store with an application-specific home
    /// directory and keeps tests independent of process-global environment
    /// variables. `channels_root` should be the directory named `channels`,
    /// not its parent Qwen home.
    pub fn with_channels_root(
        channel_name: impl Into<String>,
        channels_root: impl Into<PathBuf>,
        workspace_cwd: Option<&str>,
    ) -> io::Result<Self> {
        let channel_name = channel_name.into();
        let channels_root = channels_root.into();
        let dir = match workspace_cwd {
            Some(cwd) => channels_root.join(super::paths::get_workspace_scope_dir_name(cwd)?),
            None => channels_root.clone(),
        };
        let safe_channel_name = encode_component(&channel_name);
        let store = Self {
            channel_name,
            channels_root,
            pending_path: dir.join(format!("{safe_channel_name}-pairing.json")),
            allowlist_path: dir.join(format!("{safe_channel_name}-allowlist.json")),
            group_allowlist_path: dir.join(format!("{safe_channel_name}-groups.json")),
            migrated_sentinel_path: dir.join(format!("{safe_channel_name}.migrated")),
            dir,
            workspace_scoped: workspace_cwd.is_some(),
        };
        if store.workspace_scoped {
            store.migrate_legacy_state();
        }
        Ok(store)
    }

    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }

    pub fn data_dir(&self) -> &Path {
        &self.dir
    }

    /// Create a user request, reusing a live request for the same user ID.
    pub fn create_request(
        &self,
        sender_id: &str,
        sender_name: &str,
    ) -> io::Result<CreatePairingRequestResult> {
        self.create_subject_request(
            PairingSubject {
                subject_type: PairingSubjectType::User,
                id: sender_id.to_owned(),
                name: sender_name.to_owned(),
            },
            sender_id,
            sender_name,
        )
    }

    /// Create a group request, reusing a live request for the same group ID.
    pub fn create_group_request(
        &self,
        group_id: &str,
        group_name: &str,
        sender_id: &str,
        sender_name: &str,
    ) -> io::Result<CreatePairingRequestResult> {
        self.create_subject_request(
            PairingSubject {
                subject_type: PairingSubjectType::Group,
                id: group_id.to_owned(),
                name: group_name.to_owned(),
            },
            sender_id,
            sender_name,
        )
    }

    fn create_subject_request(
        &self,
        subject: PairingSubject,
        sender_id: &str,
        sender_name: &str,
    ) -> io::Result<CreatePairingRequestResult> {
        let _guard = store_lock();
        let pending = self.read_pending();
        let now = now_millis();
        let mut active: Vec<_> = pending
            .into_iter()
            .filter(|request| is_active(request.created_at, now))
            .collect();

        if let Some(existing) = active.iter().find(|request| {
            request.subject.subject_type == subject.subject_type && request.subject.id == subject.id
        }) {
            return Ok(CreatePairingRequestResult::Code(existing.code.clone()));
        }

        if active.iter().any(|request| request.sender_id == sender_id) {
            return Ok(CreatePairingRequestResult::Rejected(
                PairingRejection::SenderPending,
            ));
        }
        if active.len() >= MAX_PENDING {
            return Ok(CreatePairingRequestResult::Rejected(
                PairingRejection::CapReached,
            ));
        }

        let code = generate_code();
        active.push(PairingRequest {
            sender_id: sender_id.to_owned(),
            sender_name: sender_name.to_owned(),
            subject,
            code: code.clone(),
            created_at: now,
        });
        self.write_pending(&active)?;
        Ok(CreatePairingRequestResult::Code(code))
    }

    /// Approve a pending code. The selected allowlist is durably written
    /// before the request is removed, so a failed allowlist update does not
    /// consume a usable code.
    pub fn approve(&self, code: &str) -> io::Result<Option<PairingRequest>> {
        let _guard = store_lock();
        let mut pending = self.read_pending();
        let now = now_millis();
        let code = code.to_uppercase();
        let Some(index) = pending
            .iter()
            .position(|request| request.code == code && is_active(request.created_at, now))
        else {
            return Ok(None);
        };
        let request = pending[index].clone();

        match request.subject.subject_type {
            PairingSubjectType::Group => {
                let mut groups = self.read_group_allowlist(true)?;
                if !groups.contains(&request.subject.id) {
                    groups.push(request.subject.id.clone());
                    self.write_group_allowlist(&groups)?;
                }
            }
            PairingSubjectType::User => {
                let mut users = self.read_allowlist();
                if !users.contains(&request.subject.id) {
                    users.push(request.subject.id.clone());
                    self.write_allowlist(&users)?;
                }
            }
        }

        pending.remove(index);
        self.write_pending(&pending)?;
        Ok(Some(request))
    }

    /// Return live pending requests, ignoring unreadable or malformed files as
    /// the TypeScript implementation does.
    pub fn list_pending(&self) -> Vec<PairingRequest> {
        let _guard = store_lock();
        let now = now_millis();
        self.read_pending()
            .into_iter()
            .filter(|request| is_active(request.created_at, now))
            .collect()
    }

    pub fn get_allowlist(&self) -> Vec<String> {
        let _guard = store_lock();
        self.read_allowlist()
    }

    pub fn get_group_allowlist(&self) -> Vec<String> {
        let _guard = store_lock();
        self.read_group_allowlist(false).unwrap_or_default()
    }

    pub fn revoke(&self, sender_id: &str) -> io::Result<bool> {
        let _guard = store_lock();
        let users = self.read_allowlist();
        let old_len = users.len();
        let next: Vec<_> = users.into_iter().filter(|id| id != sender_id).collect();
        if next.len() == old_len {
            return Ok(false);
        }
        self.write_allowlist(&next)?;
        Ok(true)
    }

    pub fn revoke_group(&self, group_id: &str) -> io::Result<bool> {
        let _guard = store_lock();
        let groups = self.read_group_allowlist(false)?;
        let next: Vec<_> = groups
            .iter()
            .filter(|id| id.as_str() != group_id)
            .cloned()
            .collect();
        if next.len() == groups.len() {
            return Ok(false);
        }
        self.write_group_allowlist(&next)?;
        Ok(true)
    }

    fn migrate_legacy_state(&self) {
        let _guard = store_lock();
        if self.migrated_sentinel_path.exists() {
            return;
        }
        if let Err(error) = ensure_private_dir(&self.dir) {
            eprintln!(
                "[PairingStore] legacy migration failed for channel \"{}\": {error}; scoped store starts empty",
                self.channel_name
            );
            return;
        }

        let legacy_files = [
            (
                format!("{}-pairing.json", self.channel_name),
                &self.pending_path,
            ),
            (
                format!("{}-allowlist.json", self.channel_name),
                &self.allowlist_path,
            ),
            (
                format!("{}-groups.json", self.channel_name),
                &self.group_allowlist_path,
            ),
        ];
        let mut all_succeeded = true;
        for (legacy_name, scoped_path) in legacy_files {
            let legacy_path = self.channels_root.join(legacy_name);
            if !legacy_parent_is_root(&legacy_path, &self.channels_root) {
                continue;
            }
            match legacy_path.try_exists() {
                Ok(false) => continue,
                Err(error) => {
                    all_succeeded = false;
                    eprintln!(
                        "[PairingStore] legacy migration check failed for channel \"{}\": {error}; will retry on next start",
                        self.channel_name
                    );
                    continue;
                }
                Ok(true) => {}
            }
            if scoped_path.exists() {
                continue;
            }
            if let Err(error) = copy_file_without_overwrite(&legacy_path, scoped_path) {
                all_succeeded = false;
                eprintln!(
                    "[PairingStore] legacy migration of {} failed for channel \"{}\": {error}; will retry on next start",
                    legacy_path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy(),
                    self.channel_name
                );
            }
        }

        if all_succeeded {
            let options = private_atomic_options();
            if let Err(error) = crate::utils::atomic_file_write::atomic_write_file(
                &self.migrated_sentinel_path,
                b"",
                &options,
            ) {
                eprintln!(
                    "[PairingStore] legacy migration sentinel failed for channel \"{}\": {error}; will retry on next start",
                    self.channel_name
                );
            }
        }
    }

    fn read_pending(&self) -> Vec<PairingRequest> {
        let Ok(data) = fs::read_to_string(&self.pending_path) else {
            return Vec::new();
        };
        let Ok(requests) = serde_json::from_str::<Vec<StoredPairingRequest>>(&data) else {
            return Vec::new();
        };
        requests.into_iter().map(PairingRequest::from).collect()
    }

    fn write_pending(&self, requests: &[PairingRequest]) -> io::Result<()> {
        self.write_json(&self.pending_path, requests)
    }

    fn read_allowlist(&self) -> Vec<String> {
        read_json_array(&self.allowlist_path).unwrap_or_default()
    }

    fn write_allowlist(&self, list: &[String]) -> io::Result<()> {
        self.write_json(&self.allowlist_path, list)
    }

    fn read_group_allowlist(&self, strict: bool) -> io::Result<Vec<String>> {
        match read_json_array(&self.group_allowlist_path) {
            Ok(groups) => Ok(groups),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) if strict && self.group_allowlist_path.exists() => Err(io::Error::new(
                error.kind(),
                format!(
                    "refusing to rewrite unreadable group allowlist \"{}\": {error}",
                    self.group_allowlist_path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                ),
            )),
            Err(error) => {
                if self.group_allowlist_path.exists() {
                    eprintln!(
                        "[PairingStore] group allowlist \"{}\" is unreadable ({error}); treating as empty — stored group approvals are not in effect and will be lost on the next approve",
                        self.group_allowlist_path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                    );
                }
                Ok(Vec::new())
            }
        }
    }

    fn write_group_allowlist(&self, list: &[String]) -> io::Result<()> {
        self.write_json(&self.group_allowlist_path, list)
    }

    fn write_json<T: Serialize + ?Sized>(&self, path: &Path, value: &T) -> io::Result<()> {
        ensure_private_dir(&self.dir)?;
        let contents = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
        crate::utils::atomic_file_write::atomic_write_file(
            path,
            &contents,
            &private_atomic_options(),
        )
    }
}

impl PairingStore for FilePairingStore {
    fn is_approved(&self, sender_id: &str) -> io::Result<bool> {
        let _guard = store_lock();
        Ok(self.read_allowlist().iter().any(|id| id == sender_id))
    }

    fn is_group_approved(&self, group_id: &str) -> io::Result<bool> {
        let _guard = store_lock();
        Ok(self
            .read_group_allowlist(false)?
            .iter()
            .any(|id| id == group_id))
    }

    fn create_request(
        &self,
        sender_id: &str,
        sender_name: &str,
    ) -> io::Result<Option<CreatePairingRequestResult>> {
        FilePairingStore::create_request(self, sender_id, sender_name).map(Some)
    }

    fn create_group_request(
        &self,
        group_id: &str,
        group_name: &str,
        sender_id: &str,
        sender_name: &str,
    ) -> io::Result<Option<CreatePairingRequestResult>> {
        FilePairingStore::create_group_request(self, group_id, group_name, sender_id, sender_name)
            .map(Some)
    }
}

fn read_json_array(path: &Path) -> io::Result<Vec<String>> {
    let data = fs::read_to_string(path)?;
    serde_json::from_str(&data).map_err(io::Error::other)
}

fn is_active(created_at: f64, now: f64) -> bool {
    now - created_at < EXPIRY_MS
}

fn now_millis() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}

fn generate_code() -> String {
    // UUID v4 provides cryptographic randomness. Skip byte 6, whose high
    // version nibble would make its low five bits non-uniform.
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    first
        .as_bytes()
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != 6)
        .map(|(_, byte)| SAFE_ALPHABET[(byte & 31) as usize])
        .chain(
            second
                .as_bytes()
                .iter()
                .map(|byte| SAFE_ALPHABET[(byte & 31) as usize]),
        )
        .take(CODE_LENGTH)
        .map(char::from)
        .collect()
}

fn encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')'
            )
        {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 15) as usize]));
        }
    }
    encoded
}

const HEX: &[u8; 16] = b"0123456789ABCDEF";

fn private_atomic_options() -> crate::utils::atomic_file_write::AtomicWriteOptions {
    use crate::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy};
    AtomicWriteOptions {
        mode: Some(0o600),
        force_mode: true,
        symlink_policy: SymlinkPolicy::NoFollow,
        ..AtomicWriteOptions::default()
    }
}

fn ensure_private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn legacy_parent_is_root(legacy_path: &Path, channels_root: &Path) -> bool {
    lexical_normalize(legacy_path).parent() == Some(lexical_normalize(channels_root).as_path())
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn copy_file_without_overwrite(source: &Path, destination: &Path) -> io::Result<()> {
    let file_name = destination.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "migration target has no filename",
        )
    })?;
    let (temporary_path, mut output) =
        create_private_temp(destination.parent().unwrap_or(Path::new(".")), file_name)?;
    let mut cleanup = TempCleanup::new(temporary_path.clone());
    let mut input = File::open(source)?;
    io::copy(&mut input, &mut output)?;
    output.sync_all()?;
    drop(output);

    match fs::hard_link(&temporary_path, destination) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Ok(());
        }
        Err(error) => return Err(error),
    }
    fs::remove_file(&temporary_path)?;
    cleanup.persist();
    sync_parent(destination)
}

fn create_private_temp(parent: &Path, name: &std::ffi::OsStr) -> io::Result<(PathBuf, File)> {
    for _ in 0..32 {
        let candidate = parent.join(format!(
            ".{}.{}.migrating",
            name.to_string_lossy(),
            Uuid::new_v4().simple()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        match options.open(&candidate) {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique pairing migration temp file",
    ))
}

fn sync_parent(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path.parent().unwrap_or(Path::new(".")))?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

struct TempCleanup {
    path: PathBuf,
    persisted: bool,
}

impl TempCleanup {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            persisted: false,
        }
    }

    fn persist(&mut self) {
        self.persisted = true;
    }
}

impl Drop for TempCleanup {
    fn drop(&mut self) {
        if !self.persisted {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct Fixture {
        root: PathBuf,
        channels_root: PathBuf,
        workspace_a: PathBuf,
        workspace_b: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("canopy-pairing-{}", Uuid::new_v4()));
            let channels_root = root.join("channels");
            let workspace_a = root.join("workspace-a");
            let workspace_b = root.join("workspace-b");
            fs::create_dir_all(&channels_root).unwrap();
            fs::create_dir_all(&workspace_a).unwrap();
            fs::create_dir_all(&workspace_b).unwrap();
            Self {
                root,
                channels_root,
                workspace_a,
                workspace_b,
            }
        }

        fn store(&self, channel_name: &str, workspace: Option<&Path>) -> FilePairingStore {
            FilePairingStore::with_channels_root(
                channel_name,
                &self.channels_root,
                workspace.map(|path| path.to_str().unwrap()),
            )
            .unwrap()
        }

        fn seed_json(&self, path: &Path, value: &serde_json::Value) {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn code(result: CreatePairingRequestResult) -> String {
        match result {
            CreatePairingRequestResult::Code(code) => code,
            other => panic!("expected code, got {other:?}"),
        }
    }

    #[test]
    fn subject_reuse_sender_limit_shared_cap_and_code_alphabet() {
        let fixture = Fixture::new();
        let store = fixture.store("support-bot", Some(&fixture.workspace_a));
        let first = store
            .create_group_request("release", "Release Team", "alice", "Alice")
            .unwrap();
        let first_code = code(first.clone());
        assert_eq!(first_code.len(), CODE_LENGTH);
        assert!(first_code.bytes().all(|byte| SAFE_ALPHABET.contains(&byte)));

        assert_eq!(
            store
                .create_group_request("release", "Renamed", "bob", "Bob")
                .unwrap(),
            first
        );
        assert_eq!(
            store
                .create_group_request("platform", "Platform", "alice", "Alice")
                .unwrap(),
            CreatePairingRequestResult::Rejected(PairingRejection::SenderPending)
        );
        store
            .create_group_request("platform", "Platform", "bob", "Bob")
            .unwrap();
        store.create_request("carol", "Carol").unwrap();
        assert_eq!(
            store.create_request("dave", "Dave").unwrap(),
            CreatePairingRequestResult::Rejected(PairingRejection::CapReached)
        );
        assert_eq!(store.list_pending().len(), MAX_PENDING);
    }

    #[test]
    fn user_subject_reuses_its_code_and_approval_releases_the_sender_slot() {
        let fixture = Fixture::new();
        let store = fixture.store("support", Some(&fixture.workspace_a));
        let first = store.create_request("alice", "Alice").unwrap();
        let first_code = code(first.clone());
        assert_eq!(
            store.create_request("alice", "Alice Renamed").unwrap(),
            first
        );
        assert_eq!(store.list_pending()[0].sender_name, "Alice");

        store.approve(&first_code).unwrap().unwrap();
        assert!(matches!(
            store.create_request("alice", "Alice").unwrap(),
            CreatePairingRequestResult::Code(new_code) if new_code != first_code
        ));
    }

    #[test]
    fn expiration_releases_subject_sender_and_shared_pending_slots() {
        let fixture = Fixture::new();
        let store = fixture.store("expiry", Some(&fixture.workspace_a));
        fixture.seed_json(
            &store.pending_path,
            &serde_json::json!([{
                "senderId": "old",
                "senderName": "Old",
                "code": "ABCDEFGH",
                "createdAt": now_millis() - EXPIRY_MS - 1.0
            }]),
        );
        assert!(store.list_pending().is_empty());
        assert!(matches!(
            store.create_request("old", "Old").unwrap(),
            CreatePairingRequestResult::Code(_)
        ));
        assert_eq!(store.list_pending().len(), 1);
    }

    #[test]
    fn approval_and_revocation_keep_user_and_group_allowlists_separate() {
        let fixture = Fixture::new();
        let store = fixture.store("channels", Some(&fixture.workspace_a));
        let user_code = code(store.create_request("shared-id", "Alice").unwrap());
        let group_code = code(
            store
                .create_group_request("shared-id", "Team", "alice", "Alice")
                .unwrap(),
        );
        store.approve(&user_code).unwrap().unwrap();
        store.approve(&group_code).unwrap().unwrap();

        assert!(store.is_approved("shared-id").unwrap());
        assert!(store.is_group_approved("shared-id").unwrap());
        assert_eq!(store.get_allowlist(), ["shared-id"]);
        assert_eq!(store.get_group_allowlist(), ["shared-id"]);
        assert!(store.revoke("shared-id").unwrap());
        assert!(!store.is_approved("shared-id").unwrap());
        assert!(store.is_group_approved("shared-id").unwrap());
        assert!(store.revoke_group("shared-id").unwrap());
        assert!(!store.is_group_approved("shared-id").unwrap());
    }

    #[test]
    fn workspace_scopes_isolate_requests_and_approvals_but_canonical_paths_reuse() {
        let fixture = Fixture::new();
        let store_a = fixture.store("support-bot", Some(&fixture.workspace_a));
        let store_b = fixture.store("support-bot", Some(&fixture.workspace_b));
        let code_a = code(store_a.create_request("alice", "Alice").unwrap());
        store_a.approve(&code_a).unwrap();
        assert!(store_a.is_approved("alice").unwrap());
        assert!(!store_b.is_approved("alice").unwrap());
        assert_eq!(store_b.list_pending(), []);

        let equivalent = fixture.workspace_a.join("child/..");
        let same_scope = fixture.store("support-bot", Some(&equivalent));
        assert_eq!(same_scope.data_dir(), store_a.data_dir());
    }

    #[test]
    fn channel_names_are_uri_encoded_and_cannot_escape_workspace_scope() {
        let fixture = Fixture::new();
        let store_a = fixture.store("../support", Some(&fixture.workspace_a));
        let code = code(store_a.create_request("mallory", "Mallory").unwrap());
        store_a.approve(&code).unwrap();
        assert!(store_a.data_dir().starts_with(&fixture.channels_root));
        assert!(
            store_a
                .allowlist_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("%2F")
        );
        let store_b = fixture.store("../support", Some(&fixture.workspace_b));
        assert!(!store_b.is_approved("mallory").unwrap());
        assert!(
            !fixture
                .channels_root
                .join("support-allowlist.json")
                .exists()
        );

        let spaced = fixture.store("my channel", Some(&fixture.workspace_a));
        assert_eq!(
            spaced.pending_path.file_name().unwrap(),
            "my%20channel-pairing.json"
        );
    }

    #[test]
    fn scoped_store_grandfathers_legacy_files_independently_per_workspace() {
        let fixture = Fixture::new();
        fixture.seed_json(
            &fixture.channels_root.join("support-bot-allowlist.json"),
            &serde_json::json!(["legacy-user"]),
        );
        fixture.seed_json(
            &fixture.channels_root.join("support-bot-groups.json"),
            &serde_json::json!(["legacy-group"]),
        );
        fixture.seed_json(
            &fixture.channels_root.join("support-bot-pairing.json"),
            &serde_json::json!([{
                "senderId": "legacy-pending",
                "senderName": "Pending",
                "code": "ABCDEFGH",
                "createdAt": now_millis()
            }]),
        );

        let store_a = fixture.store("support-bot", Some(&fixture.workspace_a));
        let store_b = fixture.store("support-bot", Some(&fixture.workspace_b));
        assert!(store_a.is_approved("legacy-user").unwrap());
        assert!(store_b.is_approved("legacy-user").unwrap());
        assert!(store_a.is_group_approved("legacy-group").unwrap());
        assert_eq!(
            store_a.list_pending()[0].subject.subject_type,
            PairingSubjectType::User
        );
        assert_eq!(store_a.list_pending()[0].subject.id, "legacy-pending");

        assert!(store_a.revoke("legacy-user").unwrap());
        assert!(!store_a.is_approved("legacy-user").unwrap());
        assert!(store_b.is_approved("legacy-user").unwrap());
        assert!(
            fixture
                .channels_root
                .join("support-bot-allowlist.json")
                .exists()
        );
    }

    #[test]
    fn migration_retries_partial_copy_and_does_not_import_late_legacy_state() {
        let fixture = Fixture::new();
        fixture.seed_json(
            &fixture.channels_root.join("partial-pairing.json"),
            &serde_json::json!([]),
        );
        fs::create_dir(fixture.channels_root.join("partial-allowlist.json")).unwrap();
        let first = fixture.store("partial", Some(&fixture.workspace_a));
        assert!(!first.migrated_sentinel_path.exists());

        fs::remove_dir(fixture.channels_root.join("partial-allowlist.json")).unwrap();
        fixture.seed_json(
            &fixture.channels_root.join("partial-allowlist.json"),
            &serde_json::json!(["late-repaired"]),
        );
        let repaired = fixture.store("partial", Some(&fixture.workspace_a));
        assert!(repaired.is_approved("late-repaired").unwrap());
        assert!(repaired.migrated_sentinel_path.exists());

        let first_empty = fixture.store("late", Some(&fixture.workspace_b));
        assert!(!first_empty.is_approved("appeared-later").unwrap());
        fixture.seed_json(
            &fixture.channels_root.join("late-allowlist.json"),
            &serde_json::json!(["appeared-later"]),
        );
        let reopened = fixture.store("late", Some(&fixture.workspace_b));
        assert!(!reopened.is_approved("appeared-later").unwrap());
    }

    #[test]
    fn each_channel_gets_its_own_migration_gate_and_existing_scope_wins() {
        let fixture = Fixture::new();
        let chan_a = fixture.store("chan-a", Some(&fixture.workspace_a));
        assert!(chan_a.migrated_sentinel_path.exists());

        fixture.seed_json(
            &fixture.channels_root.join("chan-b-allowlist.json"),
            &serde_json::json!(["chan-b-user"]),
        );
        let chan_b = fixture.store("chan-b", Some(&fixture.workspace_a));
        assert!(chan_b.is_approved("chan-b-user").unwrap());

        let scoped = fixture.store("existing", Some(&fixture.workspace_a));
        let scoped_code = code(scoped.create_request("scoped-user", "Scoped").unwrap());
        scoped.approve(&scoped_code).unwrap();
        fixture.seed_json(
            &fixture.channels_root.join("existing-allowlist.json"),
            &serde_json::json!(["legacy-user"]),
        );
        let reopened = fixture.store("existing", Some(&fixture.workspace_a));
        assert!(reopened.is_approved("scoped-user").unwrap());
        assert!(!reopened.is_approved("legacy-user").unwrap());

        fs::remove_file(&reopened.allowlist_path).unwrap();
        let after_scoped_deletion = fixture.store("existing", Some(&fixture.workspace_a));
        assert!(!after_scoped_deletion.is_approved("legacy-user").unwrap());
    }

    #[test]
    fn legacy_filename_is_raw_for_migration_even_when_scoped_name_is_encoded() {
        let fixture = Fixture::new();
        fixture.seed_json(
            &fixture.channels_root.join("my channel-allowlist.json"),
            &serde_json::json!(["grandfathered"]),
        );
        let store = fixture.store("my channel", Some(&fixture.workspace_a));
        assert!(store.is_approved("grandfathered").unwrap());
        assert!(
            store
                .allowlist_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("my%20channel-")
        );
    }

    #[test]
    fn corrupt_group_allowlist_fails_closed_and_approval_preserves_pending_code() {
        let fixture = Fixture::new();
        let store = fixture.store("support", Some(&fixture.workspace_a));
        let code = code(
            store
                .create_group_request("group-a", "Group A", "sender", "Sender")
                .unwrap(),
        );
        fs::write(&store.group_allowlist_path, "[\"group-b\"").unwrap();
        assert!(!store.is_group_approved("group-b").unwrap());
        assert!(store.get_group_allowlist().is_empty());

        let error = store.approve(&code).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("refusing to rewrite unreadable group allowlist")
        );
        assert_eq!(store.list_pending().len(), 1);
        assert_eq!(
            fs::read_to_string(&store.group_allowlist_path).unwrap(),
            "[\"group-b\""
        );
    }

    #[test]
    fn group_approval_preserves_existing_entries_and_leaves_no_temporary_file() {
        let fixture = Fixture::new();
        let store = fixture.store("support", Some(&fixture.workspace_a));
        fixture.seed_json(
            &store.group_allowlist_path,
            &serde_json::json!(["existing-group"]),
        );
        let code = code(
            store
                .create_group_request("new-group", "New Group", "sender", "Sender")
                .unwrap(),
        );
        store.approve(&code).unwrap();
        assert_eq!(store.get_group_allowlist(), ["existing-group", "new-group"]);
        assert!(fs::read_dir(&store.dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".tmp")
        }));
    }

    #[test]
    fn failed_allowlist_write_does_not_consume_request() {
        let fixture = Fixture::new();
        let store = fixture.store("support", Some(&fixture.workspace_a));
        let code = code(store.create_request("alice", "Alice").unwrap());
        fs::create_dir(&store.allowlist_path).unwrap();
        assert!(store.approve(&code).is_err());
        assert_eq!(store.list_pending().len(), 1);
    }

    #[test]
    fn updates_are_atomic_private_and_serialized_across_instances() {
        let fixture = Fixture::new();
        let stores: Vec<_> = (0..4)
            .map(|_| Arc::new(fixture.store("support", Some(&fixture.workspace_a))))
            .collect();
        let threads: Vec<_> = (0..8)
            .map(|index| {
                let store = stores[index % stores.len()].clone();
                std::thread::spawn(move || {
                    store
                        .create_request(&format!("sender-{index}"), "Sender")
                        .unwrap()
                })
            })
            .collect();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, CreatePairingRequestResult::Code(_)))
                .count(),
            MAX_PENDING
        );
        assert!(results.contains(&CreatePairingRequestResult::Rejected(
            PairingRejection::CapReached
        )));
        let store = &stores[0];
        assert_eq!(store.list_pending().len(), MAX_PENDING);

        let approval_code = store.list_pending()[0].code.clone();
        store.approve(&approval_code).unwrap();
        assert!(fs::read_dir(&store.dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".tmp")
        }));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&store.dir).unwrap().permissions().mode() & 0o777,
                0o700
            );
            for path in [&store.pending_path, &store.allowlist_path] {
                assert_eq!(
                    fs::metadata(path).unwrap().permissions().mode() & 0o777,
                    0o600
                );
            }
        }
    }

    #[test]
    fn global_layout_remains_available_without_workspace_scope() {
        let fixture = Fixture::new();
        let store = fixture.store("support-bot", None);
        let code = code(store.create_request("alice", "Alice").unwrap());
        store.approve(&code).unwrap();
        assert!(
            fixture
                .channels_root
                .join("support-bot-allowlist.json")
                .exists()
        );
    }
}
