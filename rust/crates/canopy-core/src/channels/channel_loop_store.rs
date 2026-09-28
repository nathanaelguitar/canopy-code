//! Persistent store for channel loops.
//!
//! Port of `packages/channels/base/src/ChannelLoopStore.ts`. Reads validate
//! the top-level JSON strictly, skip invalid individual entries, and normalize
//! legacy missing `runCount` / target group fields. Mutations are serialized
//! per store instance and replace the JSON file through a private same-folder
//! temporary file.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use uuid::Uuid;

const LOOP_FIELDS: &[&str] = &[
    "id",
    "channelName",
    "target",
    "cwd",
    "cron",
    "prompt",
    "label",
    "recurring",
    "enabled",
    "createdBy",
    "createdAt",
    "lastFiredAt",
    "lastFinishedAt",
    "lastResultPreview",
    "lastStatus",
    "lastError",
    "consecutiveFailures",
    "runningSince",
    "runCount",
];
const TARGET_FIELDS: &[&str] = &["channelName", "senderId", "chatId", "threadId", "isGroup"];

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionTarget {
    pub channel_name: String,
    pub sender_id: String,
    pub chat_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_group: Option<bool>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelLoopStatus {
    Ok,
    Error,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelLoop {
    pub id: String,
    pub channel_name: String,
    pub target: SessionTarget,
    pub cwd: String,
    pub cron: String,
    pub prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub recurring: bool,
    pub enabled: bool,
    pub created_by: String,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_fired_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_finished_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_result_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_status: Option<ChannelLoopStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub consecutive_failures: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub running_since: Option<String>,
    pub run_count: f64,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChannelLoopInput {
    pub channel_name: String,
    pub target: SessionTarget,
    pub cwd: String,
    pub cron: String,
    pub prompt: String,
    pub label: Option<String>,
    pub recurring: bool,
    pub created_by: String,
    pub extra: serde_json::Map<String, Value>,
}

/// A field update distinguishes leaving a value untouched from setting an
/// optional field to `None`, which clears it as a JavaScript patch with an
/// `undefined` value would do when serialized.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum PatchField<T> {
    #[default]
    Unchanged,
    Set(T),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChannelLoopPatch {
    pub enabled: PatchField<bool>,
    pub last_fired_at: PatchField<Option<String>>,
    pub last_finished_at: PatchField<Option<String>>,
    pub last_result_preview: PatchField<Option<String>>,
    pub last_status: PatchField<Option<ChannelLoopStatus>>,
    pub last_error: PatchField<Option<String>>,
    pub consecutive_failures: PatchField<f64>,
    pub running_since: PatchField<Option<String>>,
    pub run_count: PatchField<f64>,
    pub extra: serde_json::Map<String, Value>,
}

type NowFactory = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;
type IdFactory = Arc<dyn Fn() -> String + Send + Sync>;

/// JSON-backed channel loop persistence.
///
/// The update mutex is local to this store instance, like the source
/// implementation's promise queue. It prevents same-instance read/modify/write
/// races but does not coordinate separate store instances or processes.
pub struct ChannelLoopStore {
    file_path: PathBuf,
    now: NowFactory,
    id_factory: IdFactory,
    pending_update: Mutex<()>,
}

impl ChannelLoopStore {
    pub fn new(file_path: impl Into<PathBuf>) -> Self {
        Self::with_factories(file_path, Utc::now, || Uuid::new_v4().to_string())
    }

    pub fn with_factories<N, I>(file_path: impl Into<PathBuf>, now: N, id_factory: I) -> Self
    where
        N: Fn() -> DateTime<Utc> + Send + Sync + 'static,
        I: Fn() -> String + Send + Sync + 'static,
    {
        Self {
            file_path: file_path.into(),
            now: Arc::new(now),
            id_factory: Arc::new(id_factory),
            pending_update: Mutex::new(()),
        }
    }

    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    pub async fn list(&self) -> io::Result<Vec<ChannelLoop>> {
        self.read_jobs().await
    }

    pub async fn list_for_target(
        &self,
        channel_name: &str,
        target: &SessionTarget,
    ) -> io::Result<Vec<ChannelLoop>> {
        Ok(self
            .read_jobs()
            .await?
            .into_iter()
            .filter(|job| job.channel_name == channel_name && same_target(&job.target, target))
            .collect())
    }

    pub async fn create(&self, input: ChannelLoopInput) -> io::Result<ChannelLoop> {
        let _guard = self.pending_update.lock().await;
        let mut jobs = self.read_jobs().await?;
        let job = self.build_loop(input, &jobs);
        jobs.push(job.clone());
        self.write_jobs(&jobs).await?;
        Ok(job)
    }

    pub async fn create_for_target(
        &self,
        input: ChannelLoopInput,
        max_enabled_loops: f64,
    ) -> io::Result<Option<ChannelLoop>> {
        let _guard = self.pending_update.lock().await;
        let mut jobs = self.read_jobs().await?;
        let enabled_for_target = jobs
            .iter()
            .filter(|job| {
                job.enabled
                    && job.channel_name == input.channel_name
                    && same_target(&job.target, &input.target)
            })
            .count();
        if enabled_for_target as f64 >= max_enabled_loops {
            self.write_jobs(&jobs).await?;
            return Ok(None);
        }

        let job = self.build_loop(input, &jobs);
        jobs.push(job.clone());
        self.write_jobs(&jobs).await?;
        Ok(Some(job))
    }

    pub async fn update(&self, id: &str, patch: ChannelLoopPatch) -> io::Result<bool> {
        let _guard = self.pending_update.lock().await;
        let mut jobs = self.read_jobs().await?;
        let mut found = false;
        for job in &mut jobs {
            if job.id == id {
                found = true;
                apply_patch(job, &patch);
            }
        }
        self.write_jobs(&jobs).await?;
        Ok(found)
    }

    pub async fn disable(&self, id: &str) -> io::Result<bool> {
        let patch = ChannelLoopPatch {
            enabled: PatchField::Set(false),
            ..ChannelLoopPatch::default()
        };
        self.update(id, patch).await
    }

    fn build_loop(&self, input: ChannelLoopInput, existing: &[ChannelLoop]) -> ChannelLoop {
        let base_id = (self.id_factory)();
        let mut id = base_id.clone();
        let mut suffix = 1_u64;
        while existing.iter().any(|job| job.id == id) {
            id = format!("{base_id}-{suffix}");
            suffix = suffix.saturating_add(1);
        }
        let extra = extra_fields(&input.extra, LOOP_FIELDS);
        ChannelLoop {
            id,
            channel_name: input.channel_name,
            target: normalize_target(input.target),
            cwd: input.cwd,
            cron: input.cron,
            prompt: input.prompt,
            label: input.label,
            recurring: input.recurring,
            enabled: true,
            created_by: input.created_by,
            created_at: (self.now)().to_rfc3339_opts(SecondsFormat::Millis, true),
            last_fired_at: None,
            last_finished_at: None,
            last_result_preview: None,
            last_status: None,
            last_error: None,
            consecutive_failures: 0.0,
            running_since: None,
            run_count: 0.0,
            extra,
        }
    }

    async fn read_jobs(&self) -> io::Result<Vec<ChannelLoop>> {
        let bytes = match fs::read(&self.file_path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let text = String::from_utf8_lossy(&bytes);
        let parsed: Value = serde_json::from_str(&text).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Malformed JSON in {}; fix or delete the file.",
                    self.file_path.display()
                ),
            )
        })?;
        let Some(values) = parsed.as_array() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Expected a JSON array in {}; fix or delete the file.",
                    self.file_path.display()
                ),
            ));
        };

        let mut jobs = Vec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            if let Some(job) = parse_channel_loop(value) {
                jobs.push(normalize_job(job));
            } else {
                let _ = writeln!(
                    io::stderr(),
                    "Invalid channel loop at index {index} in {}: {value}",
                    self.file_path.display()
                );
            }
        }
        Ok(jobs)
    }

    async fn write_jobs(&self, jobs: &[ChannelLoop]) -> io::Result<()> {
        let dir = self
            .file_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        create_private_dir(dir).await?;

        let bytes = serde_json::to_vec_pretty(jobs)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let (tmp_path, file) = create_private_temp(&self.file_path).await?;
        let mut temp = TempFileGuard {
            path: tmp_path.clone(),
            file: Some(file),
        };
        let write_result = async {
            let file = temp.file.as_mut().expect("temp file is open");
            file.write_all(&bytes).await?;
            file.flush().await?;
            Ok::<(), io::Error>(())
        }
        .await;
        drop(temp.file.take());
        match write_result {
            Ok(()) => match fs::rename(&tmp_path, &self.file_path).await {
                Ok(()) => {
                    set_private_mode(&self.file_path, 0o600).await;
                    Ok(())
                }
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        }
    }
}

fn same_target(a: &SessionTarget, b: &SessionTarget) -> bool {
    a.channel_name == b.channel_name
        && a.sender_id == b.sender_id
        && a.chat_id == b.chat_id
        && a.thread_id == b.thread_id
}

fn normalize_target(target: SessionTarget) -> SessionTarget {
    SessionTarget {
        is_group: target.is_group,
        extra: extra_fields(&target.extra, TARGET_FIELDS),
        ..target
    }
}

fn normalize_job(mut job: ChannelLoop) -> ChannelLoop {
    job.target = normalize_target(job.target);
    job
}

fn apply_patch(job: &mut ChannelLoop, patch: &ChannelLoopPatch) {
    macro_rules! apply {
        ($field:ident) => {
            if let PatchField::Set(value) = &patch.$field {
                job.$field = value.clone();
            }
        };
    }
    apply!(enabled);
    apply!(last_fired_at);
    apply!(last_finished_at);
    apply!(last_result_preview);
    apply!(last_status);
    apply!(last_error);
    apply!(consecutive_failures);
    apply!(running_since);
    apply!(run_count);
    job.extra.extend(extra_fields(&patch.extra, LOOP_FIELDS));
}

fn parse_channel_loop(value: &Value) -> Option<ChannelLoop> {
    let job = value.as_object()?;
    let target = parse_target(job.get("target")?)?;
    let last_status = match optional_string(job, "lastStatus")? {
        None => None,
        Some(status) => Some(match status.as_str() {
            "ok" => ChannelLoopStatus::Ok,
            "error" => ChannelLoopStatus::Error,
            _ => return None,
        }),
    };
    Some(ChannelLoop {
        id: required_string(job, "id")?,
        channel_name: required_string(job, "channelName")?,
        target,
        cwd: required_string(job, "cwd")?,
        cron: required_string(job, "cron")?,
        prompt: required_string(job, "prompt")?,
        label: optional_string(job, "label")?,
        recurring: required_bool(job, "recurring")?,
        enabled: required_bool(job, "enabled")?,
        created_by: required_string(job, "createdBy")?,
        created_at: required_string(job, "createdAt")?,
        last_fired_at: optional_string(job, "lastFiredAt")?,
        last_finished_at: optional_string(job, "lastFinishedAt")?,
        last_result_preview: optional_string(job, "lastResultPreview")?,
        last_status,
        last_error: optional_string(job, "lastError")?,
        consecutive_failures: required_number(job, "consecutiveFailures")?,
        running_since: optional_string(job, "runningSince")?,
        run_count: match job.get("runCount") {
            Some(value) => value.as_f64()?,
            None => 0.0,
        },
        extra: extra_fields(job, LOOP_FIELDS),
    })
}

fn parse_target(value: &Value) -> Option<SessionTarget> {
    let target = value.as_object()?;
    Some(SessionTarget {
        channel_name: required_string(target, "channelName")?,
        sender_id: required_string(target, "senderId")?,
        chat_id: required_string(target, "chatId")?,
        thread_id: optional_string(target, "threadId")?,
        is_group: optional_bool(target, "isGroup")?,
        extra: extra_fields(target, TARGET_FIELDS),
    })
}

fn required_string(object: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    object.get(key)?.as_str().map(str::to_owned)
}

fn optional_string(object: &serde_json::Map<String, Value>, key: &str) -> Option<Option<String>> {
    match object.get(key) {
        None => Some(None),
        Some(value) => value.as_str().map(|value| Some(value.to_owned())),
    }
}

fn required_bool(object: &serde_json::Map<String, Value>, key: &str) -> Option<bool> {
    object.get(key)?.as_bool()
}

fn optional_bool(object: &serde_json::Map<String, Value>, key: &str) -> Option<Option<bool>> {
    match object.get(key) {
        None => Some(None),
        Some(value) => value.as_bool().map(Some),
    }
}

fn required_number(object: &serde_json::Map<String, Value>, key: &str) -> Option<f64> {
    object.get(key)?.as_f64()
}

fn extra_fields(
    object: &serde_json::Map<String, Value>,
    known_fields: &[&str],
) -> serde_json::Map<String, Value> {
    object
        .iter()
        .filter(|(key, _)| !known_fields.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

async fn create_private_temp(path: &Path) -> io::Result<(PathBuf, tokio::fs::File)> {
    for _ in 0..32 {
        let temp = PathBuf::from(format!(
            "{}.{}.tmp",
            path.display(),
            Uuid::new_v4().simple()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            options.mode(0o600);
        }
        match options.open(&temp).await {
            Ok(file) => return Ok((temp, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "unable to allocate a unique channel loop temp file",
    ))
}

async fn create_private_dir(path: &Path) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut current = path;
    loop {
        match fs::metadata(current).await {
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(current.to_path_buf());
                let Some(parent) = current.parent() else {
                    break;
                };
                if parent == current {
                    break;
                }
                current = parent;
            }
            Err(error) => return Err(error),
        }
    }
    fs::create_dir_all(path).await?;
    for directory in missing.into_iter().rev() {
        set_private_mode(&directory, 0o700).await;
    }
    // Match the source's chmod call even when the directory already existed.
    set_private_mode(path, 0o700).await;
    Ok(())
}

struct TempFileGuard {
    path: PathBuf,
    file: Option<tokio::fs::File>,
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        // Close before unlinking so cleanup also succeeds on Windows. This
        // runs after a canceled async write as well as after ordinary errors.
        drop(self.file.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

async fn set_private_mode(path: &Path, mode: u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(metadata) = fs::metadata(path).await {
            let mut permissions = metadata.permissions();
            permissions.set_mode(mode);
            let _ = fs::set_permissions(path, permissions).await;
        }
    }
    #[cfg(not(unix))]
    let _ = (path, mode);
}

#[cfg(test)]
mod tests {
    use super::{
        ChannelLoop, ChannelLoopInput, ChannelLoopPatch, ChannelLoopStatus, ChannelLoopStore,
        PatchField, SessionTarget,
    };
    use chrono::{DateTime, Utc};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    fn temp_file() -> PathBuf {
        let directory =
            std::env::temp_dir().join(format!("canopy-channel-loop-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        directory.join("channels").join("loops.json")
    }

    fn input() -> ChannelLoopInput {
        ChannelLoopInput {
            channel_name: "feishu-main".to_owned(),
            target: SessionTarget {
                channel_name: "feishu-main".to_owned(),
                sender_id: "alice".to_owned(),
                chat_id: "chat-1".to_owned(),
                thread_id: Some("thread-1".to_owned()),
                is_group: Some(false),
                extra: serde_json::Map::new(),
            },
            cwd: "/repo".to_owned(),
            cron: "0 9 * * *".to_owned(),
            prompt: "post a daily summary".to_owned(),
            label: Some("daily summary".to_owned()),
            recurring: true,
            created_by: "Alice".to_owned(),
            extra: serde_json::Map::new(),
        }
    }

    fn fixed_store(path: PathBuf, id: &'static str) -> ChannelLoopStore {
        ChannelLoopStore::with_factories(
            path,
            || {
                DateTime::parse_from_rfc3339("2026-06-30T01:02:03.000Z")
                    .unwrap()
                    .with_timezone(&Utc)
            },
            move || id.to_owned(),
        )
    }

    #[tokio::test]
    async fn creates_persists_and_normalizes_default_fields() {
        let path = temp_file();
        let store = fixed_store(path.clone(), "job-1");
        let created = store.create(input()).await.unwrap();
        assert_eq!(created.id, "job-1");
        assert!(created.enabled);
        assert_eq!(created.created_at, "2026-06-30T01:02:03.000Z");
        assert_eq!(created.consecutive_failures, 0.0);
        assert_eq!(created.run_count, 0.0);
        assert_eq!(store.list().await.unwrap(), std::slice::from_ref(&created));
        assert_eq!(fixed_store(path, "other").list().await.unwrap(), [created]);
    }

    #[tokio::test]
    async fn target_filter_ignores_group_flag_and_keeps_sender_isolation() {
        let store = fixed_store(temp_file(), "job");
        let created = store.create(input()).await.unwrap();
        let promoted = SessionTarget {
            is_group: Some(true),
            ..input().target
        };
        assert_eq!(
            store
                .list_for_target("feishu-main", &promoted)
                .await
                .unwrap(),
            std::slice::from_ref(&created)
        );
        let other_sender = SessionTarget {
            sender_id: "bob".to_owned(),
            ..promoted
        };
        assert!(
            store
                .list_for_target("feishu-main", &other_sender)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn disabled_jobs_do_not_count_against_target_cap() {
        let store = fixed_store(temp_file(), "job");
        let first = store.create(input()).await.unwrap();
        assert!(store.disable(&first.id).await.unwrap());
        let second = store
            .create_for_target(input(), 1.0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.id, "job-1");
        assert_eq!(store.list().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn concurrent_target_cap_updates_are_serialized() {
        let path = temp_file();
        let next_id = ArcCounter::new();
        let store = ChannelLoopStore::with_factories(path, Utc::now, move || {
            format!("job-{}", next_id.next())
        });
        let (first, second) = tokio::join!(
            store.create_for_target(input(), 1.0),
            store.create_for_target(input(), 1.0)
        );
        assert_eq!(
            usize::from(first.unwrap().is_some()) + usize::from(second.unwrap().is_some()),
            1
        );
        assert_eq!(store.list().await.unwrap().len(), 1);
    }

    struct ArcCounter(std::sync::Arc<AtomicUsize>);
    impl ArcCounter {
        fn new() -> Self {
            Self(std::sync::Arc::new(AtomicUsize::new(0)))
        }
        fn next(&self) -> usize {
            self.0.fetch_add(1, Ordering::SeqCst) + 1
        }
    }

    #[tokio::test]
    async fn creates_collision_safe_ids() {
        let store = fixed_store(temp_file(), "job-1");
        let first = store.create(input()).await.unwrap();
        let second = store.create(input()).await.unwrap();
        assert_eq!(first.id, "job-1");
        assert_eq!(second.id, "job-1-1");
    }

    #[tokio::test]
    async fn normalizes_legacy_targets_and_run_count() {
        let path = temp_file();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            r#"[{"id":"old","channelName":"feishu-main","target":{"channelName":"feishu-main","senderId":"alice","chatId":"chat-1","threadId":"thread-1"},"cwd":"/repo","cron":"0 9 * * *","prompt":"old","recurring":true,"enabled":true,"createdBy":"Alice","createdAt":"2026-06-30T01:02:03.000Z","consecutiveFailures":0}]"#,
        )
        .unwrap();
        let store = fixed_store(path, "new");
        let jobs = store.list().await.unwrap();
        assert_eq!(jobs[0].target.is_group, None);
        assert_eq!(jobs[0].run_count, 0.0);
        assert_eq!(jobs[0].id, "old");
    }

    #[tokio::test]
    async fn preserves_unknown_fields_across_normalizing_updates() {
        let path = temp_file();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            r#"[{"id":"old","channelName":"feishu-main","target":{"channelName":"feishu-main","senderId":"alice","chatId":"chat-1","platformTarget":"thread"},"cwd":"/repo","cron":"0 9 * * *","prompt":"old","recurring":true,"enabled":true,"createdBy":"Alice","createdAt":"2026-06-30T01:02:03.000Z","consecutiveFailures":0,"platformField":{"keep":true}}]"#,
        )
        .unwrap();
        let store = fixed_store(path, "new");
        assert!(store.disable("old").await.unwrap());
        let reloaded = store.list().await.unwrap();
        assert_eq!(
            reloaded[0].extra.get("platformField"),
            Some(&serde_json::json!({"keep": true}))
        );
        assert_eq!(
            reloaded[0].target.extra.get("platformTarget"),
            Some(&serde_json::json!("thread"))
        );
        assert_eq!(reloaded[0].run_count, 0.0);
    }

    #[tokio::test]
    async fn patch_and_disable_keep_existing_status_and_can_clear_optionals() {
        let store = fixed_store(temp_file(), "job");
        let created = store.create(input()).await.unwrap();
        let patch = ChannelLoopPatch {
            last_status: PatchField::Set(Some(ChannelLoopStatus::Error)),
            last_error: PatchField::Set(Some("adapter failed".to_owned())),
            last_result_preview: PatchField::Set(Some("partial".to_owned())),
            consecutive_failures: PatchField::Set(1.0),
            ..ChannelLoopPatch::default()
        };
        assert!(store.update(&created.id, patch).await.unwrap());
        assert!(store.disable(&created.id).await.unwrap());
        assert!(
            store
                .update(
                    &created.id,
                    ChannelLoopPatch {
                        last_error: PatchField::Set(None),
                        ..ChannelLoopPatch::default()
                    }
                )
                .await
                .unwrap()
        );
        let updated = store.list().await.unwrap().remove(0);
        assert!(!updated.enabled);
        assert_eq!(updated.last_status, Some(ChannelLoopStatus::Error));
        assert_eq!(updated.last_result_preview.as_deref(), Some("partial"));
        assert_eq!(updated.last_error, None);
        assert_eq!(updated.consecutive_failures, 1.0);
    }

    #[tokio::test]
    async fn malformed_or_non_array_json_is_an_error() {
        let path = temp_file();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "{").unwrap();
        let store = fixed_store(path.clone(), "job");
        assert!(
            store
                .list()
                .await
                .unwrap_err()
                .to_string()
                .contains("Malformed JSON")
        );
        fs::write(&path, "{}").unwrap();
        assert!(
            store
                .list()
                .await
                .unwrap_err()
                .to_string()
                .contains("Expected a JSON array")
        );
    }

    #[tokio::test]
    async fn skips_invalid_entries_but_keeps_valid_ones() {
        let path = temp_file();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let valid = serde_json::to_value(ChannelLoop {
            id: "ok".to_owned(),
            channel_name: "feishu-main".to_owned(),
            target: input().target,
            cwd: "/repo".to_owned(),
            cron: "0 9 * * *".to_owned(),
            prompt: "prompt".to_owned(),
            label: None,
            recurring: true,
            enabled: true,
            created_by: "Alice".to_owned(),
            created_at: "now".to_owned(),
            last_fired_at: None,
            last_finished_at: None,
            last_result_preview: None,
            last_status: None,
            last_error: None,
            consecutive_failures: 0.0,
            running_since: None,
            run_count: 0.0,
            extra: serde_json::Map::new(),
        })
        .unwrap();
        fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!([{}, valid])).unwrap(),
        )
        .unwrap();
        let jobs = fixed_store(path, "job").list().await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, "ok");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn writes_private_directory_and_file_modes() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_file();
        fixed_store(path.clone(), "job")
            .create(input())
            .await
            .unwrap();
        assert_eq!(
            fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
