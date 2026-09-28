//! Cached skill discovery, activation, change notifications, and filesystem
//! watching contracts.
//!
//! Port of `packages/core/src/skills/skill-manager.ts`. Configuration and
//! filesystem watcher creation are injected so the core manager does not own
//! process-global settings or a platform-specific watcher implementation.

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Duration;

use futures_util::future::{BoxFuture, join_all};
use indexmap::{IndexMap, IndexSet};
use serde_json::{Map, Value};
use tokio::task::JoinHandle;

use super::activation::{SkillActivationRegistry, split_conditional_skills};
use super::skill_load::{
    SKILL_MANIFEST_FILE, normalize_content, parse_skill_content as parse_basic_skill_content,
};
use super::symlink_scope::{SymlinkTargetCheck, validate_symlink_target};
use super::types::{
    ListSkillsOptions, SkillConfig, SkillError, SkillErrorCode, SkillHooksSettings, SkillLevel,
    SkillValidationResult,
};

pub const SKILLS_CONFIG_DIR: &str = "skills";
pub const WATCHER_MAX_DEPTH: usize = 2;
pub const WATCHER_REFRESH_DEBOUNCE: Duration = Duration::from_millis(150);
pub const CHANGE_LISTENER_TIMEOUT: Duration = Duration::from_secs(30);

/// A single active extension's skill contribution.
#[derive(Clone, Debug, Default)]
pub struct SkillExtension {
    pub name: String,
    pub display_name: Option<String>,
    pub skills: Vec<SkillConfig>,
}

/// Values the TypeScript manager reads from `Config`, `Storage`, `os`, and
/// bundle-path discovery. The native session supplies these once when it
/// constructs the manager.
#[derive(Clone, Debug)]
pub struct SkillManagerConfig {
    pub project_root: PathBuf,
    pub home_dir: PathBuf,
    pub global_canopy_dir: PathBuf,
    pub current_dir: PathBuf,
    pub bundled_skills_dir: PathBuf,
    pub safe_mode: bool,
    pub bare_mode: bool,
    pub disabled_skill_levels: Vec<SkillLevel>,
    /// Custom user skill roots, retaining `~` and relative spelling until
    /// `get_skills_base_dirs` applies the source path rules.
    pub custom_skill_dirs: Vec<String>,
    pub active_extensions: Vec<SkillExtension>,
    /// Injectable to make timeout behavior deterministic in host tests; the
    /// production default matches the source's 30 second cap.
    pub listener_timeout: Duration,
}

impl SkillManagerConfig {
    pub fn new(
        project_root: impl Into<PathBuf>,
        home_dir: impl Into<PathBuf>,
        global_canopy_dir: impl Into<PathBuf>,
        current_dir: impl Into<PathBuf>,
        bundled_skills_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            project_root: project_root.into(),
            home_dir: home_dir.into(),
            global_canopy_dir: global_canopy_dir.into(),
            current_dir: current_dir.into(),
            bundled_skills_dir: bundled_skills_dir.into(),
            safe_mode: false,
            bare_mode: false,
            disabled_skill_levels: Vec::new(),
            custom_skill_dirs: Vec::new(),
            active_extensions: Vec::new(),
            listener_timeout: CHANGE_LISTENER_TIMEOUT,
        }
    }

    fn level_disabled(&self, level: SkillLevel) -> bool {
        self.disabled_skill_levels.contains(&level)
    }
}

/// Listener futures are started concurrently and individually capped by the
/// configured listener timeout. A timeout detaches the listener task, which
/// may finish later, like the source's `Promise.race` behavior.
pub type SkillChangeListener = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatcherEntryKind {
    File,
    Directory,
    Special,
}

/// Chokidar-compatible options required by the manager's shallow skill tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SkillWatchOptions {
    pub ignore_initial: bool,
    pub depth: usize,
}

pub type SkillWatchEventHandler = Arc<dyn Fn() + Send + Sync + 'static>;

/// Handle returned by an injected watcher adapter.
pub trait SkillWatcherHandle: Send + Sync {
    fn close(&self) -> BoxFuture<'_, Result<(), String>>;
}

/// Platform adapter for creating filesystem watchers. Implementations should
/// call `on_change` for any event in the watched tree and apply the provided
/// ignore predicate to special files and `.git` paths.
pub trait SkillWatcherBackend: Send + Sync {
    fn watch(
        &self,
        path: &Path,
        options: SkillWatchOptions,
        ignored: fn(&Path, Option<WatcherEntryKind>) -> bool,
        on_change: SkillWatchEventHandler,
    ) -> Result<Arc<dyn SkillWatcherHandle>, String>;
}

#[derive(Default)]
struct SkillCache {
    levels: Vec<(SkillLevel, Vec<SkillConfig>)>,
}

impl SkillCache {
    fn get(&self, level: SkillLevel) -> Option<&Vec<SkillConfig>> {
        self.levels
            .iter()
            .find_map(|(candidate, skills)| (*candidate == level).then_some(skills))
    }

    fn insert(&mut self, level: SkillLevel, skills: Vec<SkillConfig>) {
        if let Some((_, existing)) = self
            .levels
            .iter_mut()
            .find(|(candidate, _)| *candidate == level)
        {
            *existing = skills;
        } else {
            self.levels.push((level, skills));
        }
    }
}

#[derive(Default)]
struct ManagerState {
    cache: Option<SkillCache>,
    activation_registry: Option<Arc<SkillActivationRegistry>>,
    watchers: HashMap<PathBuf, Arc<dyn SkillWatcherHandle>>,
    watch_started: bool,
}

struct ListenerEntry {
    id: u64,
    listener: SkillChangeListener,
}

/// Manages skill configurations stored as `<skill-name>/SKILL.md`.
pub struct SkillManager {
    config: SkillManagerConfig,
    watcher_backend: Option<Arc<dyn SkillWatcherBackend>>,
    state: RwLock<ManagerState>,
    parse_errors: Mutex<IndexMap<PathBuf, SkillError>>,
    change_listeners: Mutex<Vec<ListenerEntry>>,
    next_listener_id: AtomicU64,
    slash_reload_suppressed: AtomicBool,
    refresh_generation: AtomicU64,
    refresh_timer: Mutex<Option<JoinHandle<()>>>,
}

impl SkillManager {
    pub fn new(config: SkillManagerConfig) -> Self {
        Self::with_watcher_backend(config, None)
    }

    pub fn with_watcher_backend(
        config: SkillManagerConfig,
        watcher_backend: Option<Arc<dyn SkillWatcherBackend>>,
    ) -> Self {
        Self {
            config,
            watcher_backend,
            state: RwLock::new(ManagerState::default()),
            parse_errors: Mutex::new(IndexMap::new()),
            change_listeners: Mutex::new(Vec::new()),
            next_listener_id: AtomicU64::new(1),
            slash_reload_suppressed: AtomicBool::new(false),
            refresh_generation: AtomicU64::new(0),
            refresh_timer: Mutex::new(None),
        }
    }

    /// Register a callback and return an unregister closure.
    pub fn add_change_listener(
        self: &Arc<Self>,
        listener: SkillChangeListener,
    ) -> impl Fn() + Send + Sync + 'static {
        let id = self.next_listener_id.fetch_add(1, Ordering::Relaxed);
        lock(&self.change_listeners).push(ListenerEntry { id, listener });
        let weak = Arc::downgrade(self);
        move || {
            if let Some(manager) = weak.upgrade() {
                lock(&manager.change_listeners).retain(|entry| entry.id != id);
            }
        }
    }

    /// Notify consumers after a non-disk configuration change such as a
    /// change to the disabled-skill set.
    pub async fn notify_config_changed(&self) {
        self.notify_change_listeners().await;
    }

    /// Make the next opted-in listener consume a one-shot reload suppression.
    pub fn suppress_next_slash_reload(&self) {
        self.slash_reload_suppressed.store(true, Ordering::Release);
    }

    /// Read-and-clear the one-shot slash-command reload suppression.
    pub fn consume_slash_reload_suppression(&self) -> bool {
        self.slash_reload_suppressed.swap(false, Ordering::AcqRel)
    }

    async fn notify_change_listeners(&self) {
        let listeners = lock(&self.change_listeners)
            .iter()
            .map(|entry| Arc::clone(&entry.listener))
            .collect::<Vec<_>>();
        let timeout = self.config.listener_timeout;
        let waits = listeners.into_iter().map(|listener| async move {
            let task = tokio::spawn(async move { (listener)().await });
            match tokio::time::timeout(timeout, task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    eprintln!("[SKILL_MANAGER] Skill change listener failed: {error}")
                }
                Err(_) => eprintln!(
                    "[SKILL_MANAGER] Skill change listener timed out after {:?}",
                    timeout
                ),
            }
        });
        join_all(waits).await;
    }

    /// Snapshot parse diagnostics in insertion order.
    pub fn get_parse_errors(&self) -> IndexMap<PathBuf, SkillError> {
        lock(&self.parse_errors).clone()
    }

    /// Read the committed cache without triggering discovery. `None` means
    /// no refresh has committed yet.
    pub fn get_cached_skills(&self, level: Option<SkillLevel>) -> Option<Vec<SkillConfig>> {
        let state = read_lock(&self.state);
        let cache = state.cache.as_ref()?;
        Some(collect_cached_skills(cache, level))
    }

    /// Return skill configs from cache, refreshing on a cold read or when
    /// `force` is set.
    pub async fn list_skills(&self, options: ListSkillsOptions) -> Vec<SkillConfig> {
        let should_refresh = options.force || read_lock(&self.state).cache.is_none();
        if should_refresh {
            self.refresh_cache().await;
        }
        self.get_cached_skills(options.level).unwrap_or_default()
    }

    /// Search one level, populating only that level in a cold/partial cache.
    pub async fn load_skill(&self, name: &str, level: Option<SkillLevel>) -> Option<SkillConfig> {
        let levels = level.map_or_else(all_skill_levels, |level| vec![level]);
        for level in levels {
            self.ensure_level_cache(level).await;
            let found = {
                let state = read_lock(&self.state);
                state
                    .cache
                    .as_ref()
                    .and_then(|cache| cache.get(level))
                    .and_then(|skills| skills.iter().find(|skill| skill.name == name))
                    .cloned()
            };
            if found.is_some() {
                return found;
            }
        }
        None
    }

    /// Return the loaded skill config. The TypeScript implementation currently
    /// returns the same object as `loadSkill`; extra-file runtime hydration is
    /// performed by consumers.
    pub async fn load_skill_for_runtime(
        &self,
        name: &str,
        level: Option<SkillLevel>,
    ) -> Option<SkillConfig> {
        self.load_skill(name, level).await
    }

    pub fn validate_config(&self, config: &SkillConfig) -> SkillValidationResult {
        super::skill_load::validate_config(config)
    }

    /// Parse the manager's SKILL.md variant and record source-compatible
    /// diagnostics on failure.
    pub fn parse_skill_content(
        &self,
        content: &str,
        file_path: impl AsRef<Path>,
        level: SkillLevel,
    ) -> Result<SkillConfig, SkillError> {
        let file_path = file_path.as_ref();
        let mut skill = match parse_basic_skill_content(content, file_path) {
            Ok(skill) => skill,
            Err(error) => {
                let wrapped = SkillError::new(
                    format!("Failed to parse skill file: {}", error.message),
                    SkillErrorCode::ParseError,
                    None,
                );
                self.record_parse_error(file_path, wrapped.clone());
                return Err(wrapped);
            }
        };
        skill.level = level;
        skill.hooks = match parse_hooks_from_content(content) {
            Ok(hooks) => hooks,
            Err(message) => {
                let wrapped = SkillError::new(
                    format!("Failed to parse skill file: {message}"),
                    SkillErrorCode::ParseError,
                    None,
                );
                self.record_parse_error(file_path, wrapped.clone());
                return Err(wrapped);
            }
        };
        Ok(skill)
    }

    /// Read and parse a skill manifest. Read errors are recorded separately
    /// from parse errors, matching `parseSkillFileInternal`.
    pub async fn parse_skill_file(
        &self,
        file_path: impl AsRef<Path>,
        level: SkillLevel,
    ) -> Result<SkillConfig, SkillError> {
        let file_path = file_path.as_ref();
        let content = match tokio::fs::read(file_path).await {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(error) => {
                let skill_error = SkillError::new(
                    format!("Failed to read skill file: {error}"),
                    SkillErrorCode::FileError,
                    None,
                );
                self.record_parse_error(file_path, skill_error.clone());
                return Err(skill_error);
            }
        };
        self.parse_skill_content(&content, file_path, level)
    }

    /// Compute storage roots in provider precedence order.
    pub fn get_skills_base_dirs(&self, level: SkillLevel) -> Result<Vec<PathBuf>, String> {
        match level {
            SkillLevel::Project => Ok([".canopy", ".agents"]
                .into_iter()
                .map(|provider| self.config.project_root.join(provider).join(SKILLS_CONFIG_DIR))
                .collect()),
            SkillLevel::User => {
                let mut dirs = vec![
                    resolve_absolute(&self.config.global_canopy_dir.join(SKILLS_CONFIG_DIR), &self.config.current_dir),
                    resolve_absolute(
                        &self
                            .config
                            .home_dir
                            .join(".agents")
                            .join(SKILLS_CONFIG_DIR),
                        &self.config.current_dir,
                    ),
                ];
                for custom_dir in &self.config.custom_skill_dirs {
                    let expanded = expand_home(custom_dir, &self.config.home_dir);
                    let resolved = resolve_absolute(&expanded, &self.config.current_dir);
                    if !dirs.contains(&resolved) {
                        dirs.push(resolved);
                    }
                }
                Ok(dirs)
            }
            SkillLevel::Bundled => Ok(vec![self.config.bundled_skills_dir.clone()]),
            SkillLevel::Extension => Err(
                "Extension skills do not have a base directory; they are loaded from active extensions."
                    .to_owned(),
            ),
        }
    }

    /// List and parse skills in one provider directory. Missing roots and bad
    /// entries are best-effort omissions, while parse/read failures are kept
    /// in `get_parse_errors`.
    pub async fn load_skills_from_dir(
        &self,
        base_dir: impl AsRef<Path>,
        level: SkillLevel,
    ) -> Vec<SkillConfig> {
        let mut entries = match tokio::fs::read_dir(base_dir.as_ref()).await {
            Ok(entries) => entries,
            Err(_) => return Vec::new(),
        };
        let mut skill_dirs = Vec::new();
        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) | Err(_) => break,
            };
            let kind = match entry.file_type().await {
                Ok(kind) => kind,
                Err(_) => continue,
            };
            if !kind.is_dir() && !kind.is_symlink() {
                continue;
            }
            skill_dirs.push((entry.path(), kind.is_symlink()));
        }
        let loaded = join_all(
            skill_dirs
                .into_iter()
                .map(|(skill_dir, is_symlink)| async move {
                    if is_symlink {
                        match validate_symlink_target(&skill_dir) {
                            SymlinkTargetCheck::Valid { .. } => {}
                            SymlinkTargetCheck::Invalid { .. } => return None,
                        }
                    }
                    let manifest = skill_dir.join(SKILL_MANIFEST_FILE);
                    // `fs.access` failure means "no valid SKILL.md" and is not a
                    // parse diagnostic. A later read failure after metadata succeeds
                    // is recorded by `parse_skill_file`.
                    if tokio::fs::metadata(&manifest).await.is_err() {
                        return None;
                    }
                    self.parse_skill_file(&manifest, level).await.ok()
                }),
        )
        .await;
        loaded.into_iter().flatten().collect()
    }

    /// Rebuild cache and path activation state, then await all change
    /// listeners. Each storage level is isolated from failures at other
    /// levels, and provider directories are folded in configured order.
    pub async fn refresh_cache(&self) {
        lock(&self.parse_errors).clear();
        let levels = if self.config.safe_mode {
            vec![SkillLevel::Bundled]
        } else {
            all_skill_levels()
        };
        let loaded = join_all(
            levels
                .iter()
                .copied()
                .map(|level| async move { (level, self.list_skills_at_level(level).await) }),
        )
        .await;
        let mut cache = SkillCache::default();
        for (level, skills) in loaded {
            cache.insert(level, skills);
        }

        let mut seen_for_activation = HashSet::new();
        let mut eligible_for_activation = Vec::new();
        for level in &levels {
            for skill in cache.get(*level).into_iter().flatten() {
                if !seen_for_activation.insert(skill.name.clone()) {
                    continue;
                }
                if skill.disable_model_invocation == Some(true) {
                    continue;
                }
                eligible_for_activation.push(skill.clone());
            }
        }
        let (_, conditional) = split_conditional_skills(&eligible_for_activation);
        let parse_errors = &self.parse_errors;
        let invalid_pattern = |skill: &SkillConfig, pattern: &str, error: &str| {
            let key = PathBuf::from(format!("{}#paths[{pattern}]", skill.file_path.display()));
            let diagnostic = SkillError::new(
                format!("Invalid glob in \"paths\": {pattern} — {error}"),
                SkillErrorCode::InvalidConfig,
                Some(skill.name.clone()),
            );
            upsert_parse_error(parse_errors, key, diagnostic);
        };
        let registry = Arc::new(SkillActivationRegistry::new(
            &conditional,
            self.config.project_root.clone(),
            Some(&invalid_pattern),
        ));
        {
            let mut state = write_lock(&self.state);
            state.cache = Some(cache);
            state.activation_registry = Some(registry);
        }
        self.notify_change_listeners().await;
    }

    /// Whether a skill is eligible for the model-facing list.
    pub fn is_skill_active(&self, skill: &SkillConfig) -> bool {
        if skill.paths.as_ref().is_none_or(Vec::is_empty) {
            return true;
        }
        read_lock(&self.state)
            .activation_registry
            .as_ref()
            .is_some_and(|registry| registry.is_activated(&skill.name))
    }

    pub async fn match_and_activate_by_path(&self, file_path: impl AsRef<Path>) -> Vec<String> {
        self.match_and_activate_by_paths(&[file_path.as_ref().to_path_buf()])
            .await
    }

    /// Batch activation notifies listeners once for the union of newly
    /// activated skills.
    pub async fn match_and_activate_by_paths(&self, file_paths: &[PathBuf]) -> Vec<String> {
        if file_paths.is_empty() {
            return Vec::new();
        }
        let registry = read_lock(&self.state).activation_registry.clone();
        let Some(registry) = registry else {
            return Vec::new();
        };
        let mut newly = IndexSet::new();
        for file_path in file_paths {
            for name in registry.match_and_consume(file_path).await {
                newly.insert(name);
            }
        }
        if !newly.is_empty() {
            self.notify_change_listeners().await;
        }
        newly.into_iter().collect()
    }

    pub fn get_activated_skill_names(&self) -> IndexSet<String> {
        read_lock(&self.state)
            .activation_registry
            .as_ref()
            .map(|registry| registry.get_activated_names())
            .unwrap_or_default()
    }

    /// Start watching project and user roots through the injected adapter.
    /// Bare mode still refreshes the cache but does not create watchers.
    pub async fn start_watching(self: &Arc<Self>) {
        if read_lock(&self.state).watch_started {
            return;
        }
        if self.config.bare_mode {
            self.refresh_cache().await;
            return;
        }
        write_lock(&self.state).watch_started = true;
        let user_skills_dir = self.config.global_canopy_dir.join(SKILLS_CONFIG_DIR);
        if let Err(error) = tokio::fs::create_dir_all(&user_skills_dir).await {
            eprintln!(
                "[SKILL_MANAGER] Failed to create user skills directory at {}: {error}",
                user_skills_dir.display()
            );
        }
        self.refresh_cache().await;
        self.update_watchers_from_cache().await;
    }

    /// Stop watcher handles and cancel a pending debounced refresh.
    pub fn stop_watching(&self) {
        let watchers = {
            let mut state = write_lock(&self.state);
            state.watch_started = false;
            std::mem::take(&mut state.watchers)
                .into_values()
                .collect::<Vec<_>>()
        };
        self.refresh_generation.fetch_add(1, Ordering::AcqRel);
        if let Some(timer) = lock(&self.refresh_timer).take() {
            timer.abort();
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            for watcher in watchers {
                runtime.spawn(async move {
                    if let Err(error) = watcher.close().await {
                        eprintln!("[SKILL_MANAGER] Failed to close skills watcher: {error}");
                    }
                });
            }
        }
    }

    async fn ensure_level_cache(&self, level: SkillLevel) {
        let missing = {
            let mut state = write_lock(&self.state);
            let cache = state.cache.get_or_insert_with(SkillCache::default);
            cache.get(level).is_none()
        };
        if !missing {
            return;
        }
        let skills = self.list_skills_at_level(level).await;
        let mut state = write_lock(&self.state);
        let cache = state.cache.get_or_insert_with(SkillCache::default);
        if cache.get(level).is_none() {
            cache.insert(level, skills);
        }
    }

    async fn list_skills_at_level(&self, level: SkillLevel) -> Vec<SkillConfig> {
        if self.config.bare_mode || self.config.level_disabled(level) {
            return Vec::new();
        }
        if level == SkillLevel::Project
            && resolve_absolute(&self.config.project_root, &self.config.current_dir)
                == resolve_absolute(&self.config.home_dir, &self.config.current_dir)
        {
            return Vec::new();
        }
        if level == SkillLevel::Extension {
            let mut skills = Vec::new();
            for extension in &self.config.active_extensions {
                for source_skill in &extension.skills {
                    let mut skill = source_skill.clone();
                    let invalid_priority =
                        skill.priority.is_some_and(|priority| !priority.is_finite());
                    if invalid_priority {
                        eprintln!(
                            "[SKILL_MANAGER] Extension \"{}\" skill \"{}\" has invalid priority; treating as 0.",
                            extension.name, skill.name
                        );
                        skill.priority = Some(0.0);
                    }
                    skill.extension_name = Some(
                        extension
                            .display_name
                            .clone()
                            .unwrap_or_else(|| extension.name.clone()),
                    );
                    skill.level = SkillLevel::Extension;
                    skills.push(skill);
                }
            }
            return skills;
        }
        if level == SkillLevel::Bundled {
            if tokio::fs::metadata(&self.config.bundled_skills_dir)
                .await
                .is_err()
            {
                return Vec::new();
            }
            return self
                .load_skills_from_dir(&self.config.bundled_skills_dir, SkillLevel::Bundled)
                .await;
        }

        let base_dirs = match self.get_skills_base_dirs(level) {
            Ok(base_dirs) => base_dirs,
            Err(_) => return Vec::new(),
        };
        let per_dir = join_all(
            base_dirs
                .iter()
                .map(|base_dir| async move { self.load_skills_from_dir(base_dir, level).await }),
        )
        .await;
        let mut seen_names = HashSet::new();
        let mut skills = Vec::new();
        for dir_skills in per_dir {
            for skill in dir_skills {
                if seen_names.insert(skill.name.clone()) {
                    skills.push(skill);
                }
            }
        }
        skills
    }

    async fn update_watchers_from_cache(self: &Arc<Self>) {
        if self.config.bare_mode {
            return;
        }
        let targets = [SkillLevel::Project, SkillLevel::User]
            .into_iter()
            .filter_map(|level| self.get_skills_base_dirs(level).ok())
            .flatten()
            .filter(|path| std::fs::metadata(path).is_ok())
            .collect::<HashSet<_>>();
        let stale = {
            let mut state = write_lock(&self.state);
            let stale_paths = state
                .watchers
                .keys()
                .filter(|path| !targets.contains(*path))
                .cloned()
                .collect::<Vec<_>>();
            stale_paths
                .into_iter()
                .filter_map(|path| state.watchers.remove(&path))
                .collect::<Vec<_>>()
        };
        for watcher in stale {
            if let Err(error) = watcher.close().await {
                eprintln!("[SKILL_MANAGER] Failed to close skills watcher: {error}");
            }
        }
        let Some(backend) = &self.watcher_backend else {
            return;
        };
        for path in targets {
            if read_lock(&self.state).watchers.contains_key(&path) {
                continue;
            }
            let weak = Arc::downgrade(self);
            let on_change: SkillWatchEventHandler = Arc::new(move || {
                if let Some(manager) = weak.upgrade() {
                    manager.schedule_refresh();
                }
            });
            match backend.watch(
                &path,
                SkillWatchOptions {
                    ignore_initial: true,
                    depth: WATCHER_MAX_DEPTH,
                },
                watcher_ignored,
                on_change,
            ) {
                Ok(watcher) => {
                    write_lock(&self.state).watchers.insert(path, watcher);
                }
                Err(error) => eprintln!(
                    "[SKILL_MANAGER] Failed to watch skills directory at {}: {error}",
                    path.display()
                ),
            }
        }
    }

    fn schedule_refresh(self: &Arc<Self>) {
        let generation = self.refresh_generation.fetch_add(1, Ordering::AcqRel) + 1;
        if let Some(previous) = lock(&self.refresh_timer).take() {
            previous.abort();
        }
        let weak = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            tokio::time::sleep(WATCHER_REFRESH_DEBOUNCE).await;
            let Some(manager) = weak.upgrade() else {
                return;
            };
            if manager.refresh_generation.load(Ordering::Acquire) != generation {
                return;
            }
            lock(&manager.refresh_timer).take();
            manager.refresh_cache().await;
            manager.update_watchers_from_cache().await;
        });
        *lock(&self.refresh_timer) = Some(task);
    }

    fn record_parse_error(&self, file_path: &Path, error: SkillError) {
        upsert_parse_error(&self.parse_errors, file_path.to_path_buf(), error);
    }
}

/// Source `watcherIgnored` logic: reject special filesystem nodes and anything
/// below a `.git` path component.
pub fn watcher_ignored(file_path: &Path, kind: Option<WatcherEntryKind>) -> bool {
    if kind == Some(WatcherEntryKind::Special) {
        return true;
    }
    file_path
        .components()
        .any(|component| matches!(component, Component::Normal(name) if name == ".git"))
}

fn collect_cached_skills(cache: &SkillCache, level: Option<SkillLevel>) -> Vec<SkillConfig> {
    let levels = level.map_or_else(all_skill_levels, |level| vec![level]);
    let mut seen_names = HashSet::new();
    let mut skills = Vec::new();
    for level in levels {
        for skill in cache.get(level).into_iter().flatten() {
            if seen_names.insert(skill.name.clone()) {
                skills.push(skill.clone());
            }
        }
    }
    // JS `localeCompare` is locale-sensitive. Rust's stable scalar ordering is
    // used here; ordinary ASCII skill names have the same order.
    skills.sort_by(|left, right| left.name.cmp(&right.name));
    skills
}

fn all_skill_levels() -> Vec<SkillLevel> {
    vec![
        SkillLevel::Project,
        SkillLevel::User,
        SkillLevel::Extension,
        SkillLevel::Bundled,
    ]
}

fn resolve_absolute(path: &Path, current_dir: &Path) -> PathBuf {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        current_dir.join(path)
    };
    let mut result = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::Prefix(prefix) => result.push(prefix.as_os_str()),
            Component::RootDir => result.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !result.pop() && !result.has_root() {
                    result.push(component.as_os_str());
                }
            }
            Component::Normal(part) => result.push(part),
        }
    }
    result
}

fn expand_home(path: &str, home: &Path) -> PathBuf {
    if path == "~" {
        return home.to_path_buf();
    }
    if let Some(rest) = path.strip_prefix("~/").or_else(|| path.strip_prefix("~\\")) {
        return home.join(rest);
    }
    PathBuf::from(path)
}

fn upsert_parse_error(
    errors: &Mutex<IndexMap<PathBuf, SkillError>>,
    path: PathBuf,
    error: SkillError,
) {
    lock(errors).insert(path, error);
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// Parse manager hooks from the same full frontmatter map used by the shared
// skill loader, then apply the manager's HookEventName and hook-type filters.
fn parse_hooks_from_content(content: &str) -> Result<Option<SkillHooksSettings>, String> {
    let normalized = normalize_content(content);
    let Some(frontmatter) = split_manager_frontmatter(&normalized) else {
        return Ok(None);
    };
    let frontmatter = super::skill_load::parse_yaml_frontmatter(frontmatter)?;
    let Some(raw_hooks) = frontmatter.get("hooks") else {
        return Ok(None);
    };
    let Some(events) = raw_hooks.as_object() else {
        return Ok(None);
    };
    Ok(Some(filter_hook_events(events.clone())))
}

fn split_manager_frontmatter(content: &str) -> Option<&str> {
    let rest = content.strip_prefix("---\n")?;
    let mut search_from = 0;
    while let Some(offset) = rest.get(search_from..)?.find("\n---") {
        let delimiter = search_from + offset;
        let after = delimiter + 4;
        if after == rest.len() || rest.as_bytes().get(after) == Some(&b'\n') {
            return Some(&rest[..delimiter]);
        }
        search_from = delimiter + 1;
    }
    None
}

fn filter_hook_events(events: Map<String, Value>) -> SkillHooksSettings {
    let valid_events = [
        "PreToolUse",
        "PostToolUse",
        "PostToolUseFailure",
        "PostToolBatch",
        "Notification",
        "UserPromptSubmit",
        "UserPromptExpansion",
        "SessionStart",
        "Stop",
        "MessageDisplay",
        "SubagentStart",
        "SubagentStop",
        "PreCompact",
        "PostCompact",
        "SessionEnd",
        "SessionDelete",
        "PermissionRequest",
        "PermissionDenied",
        "StopFailure",
        "TodoCreated",
        "TodoCompleted",
        "InstructionsLoaded",
    ]
    .into_iter()
    .collect::<HashSet<_>>();
    let mut output = HashMap::new();
    for (event_name, matchers) in events {
        if !valid_events.contains(event_name.as_str()) {
            continue;
        }
        let Some(matchers) = matchers.as_array() else {
            continue;
        };
        let parsed = matchers
            .iter()
            .filter_map(parse_hook_matcher)
            .collect::<Vec<_>>();
        if !parsed.is_empty() {
            output.insert(event_name, parsed);
        }
    }
    output
}

fn parse_hook_matcher(raw: &Value) -> Option<Value> {
    let matcher = raw.as_object()?;
    let hooks = matcher.get("hooks")?.as_array()?;
    let parsed_hooks = hooks
        .iter()
        .filter_map(|raw_hook| {
            let hook = raw_hook.as_object()?;
            let kind = hook.get("type")?.as_str()?;
            let mut output = Map::new();
            match kind {
                "command" => {
                    output.insert("type".to_owned(), Value::String("command".to_owned()));
                    copy_hook_fields(
                        hook,
                        &mut output,
                        &["command", "timeout", "statusMessage", "shell"],
                    );
                }
                "http" => {
                    output.insert("type".to_owned(), Value::String("http".to_owned()));
                    copy_hook_fields(
                        hook,
                        &mut output,
                        &[
                            "url",
                            "headers",
                            "allowedEnvVars",
                            "timeout",
                            "statusMessage",
                        ],
                    );
                }
                _ => return None,
            }
            Some(Value::Object(output))
        })
        .collect::<Vec<_>>();
    if parsed_hooks.is_empty() {
        return None;
    }
    let mut output = Map::new();
    if let Some(matcher) = matcher.get("matcher") {
        output.insert("matcher".to_owned(), matcher.clone());
    }
    output.insert("hooks".to_owned(), Value::Array(parsed_hooks));
    Some(Value::Object(output))
}

fn copy_hook_fields(source: &Map<String, Value>, target: &mut Map<String, Value>, fields: &[&str]) {
    for field in fields {
        if let Some(value) = source.get(*field) {
            target.insert((*field).to_owned(), value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "canopy-skill-manager-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ))
    }

    fn setup(root: &Path) -> SkillManagerConfig {
        let home = root.join("home");
        SkillManagerConfig::new(
            root.join("project"),
            &home,
            home.join(".canopy"),
            root,
            root.join("bundled"),
        )
    }

    fn write_skill(base: &Path, dir: &str, name: &str, description: &str, extra: &str) {
        let skill_dir = base.join(dir);
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(
            skill_dir.join(SKILL_MANIFEST_FILE),
            format!(
                "---\nname: {name}\ndescription: {description}\n{extra}---\n\nBody for {name}\n"
            ),
        )
        .unwrap();
    }

    fn names(skills: &[SkillConfig]) -> Vec<String> {
        skills.iter().map(|skill| skill.name.clone()).collect()
    }

    #[tokio::test]
    async fn cold_cached_reads_do_not_scan_and_force_refresh_publishes_snapshot() {
        let root = temp_root("cache");
        let config = setup(&root);
        let project_skills = config.project_root.join(".canopy/skills");
        write_skill(&project_skills, "one", "one", "First", "");
        let manager = Arc::new(SkillManager::new(config.clone()));
        assert_eq!(manager.get_cached_skills(None), None);
        assert_eq!(manager.get_cached_skills(None), None);

        let first = manager.list_skills(ListSkillsOptions::default()).await;
        assert_eq!(names(&first), ["one"]);
        write_skill(&project_skills, "two", "two", "Second", "");
        assert_eq!(
            names(
                &manager
                    .get_cached_skills(Some(SkillLevel::Project))
                    .unwrap()
            ),
            ["one"]
        );
        let refreshed = manager
            .list_skills(ListSkillsOptions {
                level: Some(SkillLevel::Project),
                force: true,
            })
            .await;
        assert_eq!(names(&refreshed), ["one", "two"]);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cache_deduplicates_provider_and_level_precedence_then_sorts_names() {
        let root = temp_root("precedence");
        let config = setup(&root);
        write_skill(
            &config.project_root.join(".canopy/skills"),
            "shared",
            "shared",
            "Canopy wins",
            "",
        );
        write_skill(
            &config.project_root.join(".agents/skills"),
            "shared",
            "shared",
            "Agent loses",
            "",
        );
        write_skill(
            &config.project_root.join(".agents/skills"),
            "alpha",
            "alpha",
            "Alpha",
            "",
        );
        write_skill(
            &config.global_canopy_dir.join("skills"),
            "shared",
            "shared",
            "User loses",
            "",
        );
        let manager = SkillManager::new(config);
        let all = manager.list_skills(ListSkillsOptions::default()).await;
        assert_eq!(names(&all), ["alpha", "shared"]);
        let shared = all.iter().find(|skill| skill.name == "shared").unwrap();
        assert_eq!(shared.description, "Canopy wins");
        assert_eq!(shared.level, SkillLevel::Project);
        assert_eq!(
            names(&manager.get_cached_skills(Some(SkillLevel::User)).unwrap()),
            ["shared"]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn load_skill_searches_precedence_and_only_populates_requested_levels() {
        let root = temp_root("load");
        let config = setup(&root);
        write_skill(
            &config.global_canopy_dir.join("skills"),
            "shared",
            "shared",
            "User",
            "",
        );
        let manager = SkillManager::new(config.clone());
        assert_eq!(manager.get_cached_skills(None), None);
        let loaded = manager.load_skill("shared", None).await.unwrap();
        assert_eq!(loaded.level, SkillLevel::User);
        assert_eq!(
            manager.get_cached_skills(Some(SkillLevel::Project)),
            Some(Vec::new())
        );
        assert_eq!(
            manager.get_cached_skills(Some(SkillLevel::Bundled)),
            Some(Vec::new())
        );
        let not_found = manager
            .load_skill("missing", Some(SkillLevel::Bundled))
            .await;
        assert!(not_found.is_none());
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn safe_mode_only_loads_bundled_and_bare_mode_loads_nothing() {
        let root = temp_root("modes");
        let mut config = setup(&root);
        write_skill(
            &config.project_root.join(".canopy/skills"),
            "project",
            "project",
            "Project",
            "",
        );
        write_skill(
            &config.bundled_skills_dir,
            "built-in",
            "built-in",
            "Bundled",
            "",
        );
        config.safe_mode = true;
        let safe = SkillManager::new(config.clone());
        let skills = safe.list_skills(ListSkillsOptions::default()).await;
        assert_eq!(names(&skills), ["built-in"]);

        config.safe_mode = false;
        config.bare_mode = true;
        let bare = SkillManager::new(config);
        assert!(
            bare.list_skills(ListSkillsOptions::default())
                .await
                .is_empty()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn disabled_levels_and_extension_skills_follow_source_normalization() {
        let root = temp_root("extension");
        let mut config = setup(&root);
        write_skill(
            &config.project_root.join(".canopy/skills"),
            "disabled",
            "disabled",
            "Disabled project skill",
            "",
        );
        let extension_skill = SkillConfig {
            name: "extension-helper".to_owned(),
            description: "From extension".to_owned(),
            allowed_tools: None,
            hooks: None,
            model: None,
            level: SkillLevel::Extension,
            file_path: root.join("extension/helper/SKILL.md"),
            skill_root: Some(root.join("extension/helper")),
            body: "Extension body".to_owned(),
            extension_name: None,
            argument_hint: None,
            when_to_use: None,
            disable_model_invocation: None,
            user_invocable: None,
            paths: None,
            priority: Some(f64::INFINITY),
        };
        config.disabled_skill_levels = vec![SkillLevel::Project];
        config.active_extensions = vec![SkillExtension {
            name: "extension-id".to_owned(),
            display_name: Some("Pretty Extension".to_owned()),
            skills: vec![extension_skill],
        }];
        let manager = SkillManager::new(config);
        let skills = manager.list_skills(ListSkillsOptions::default()).await;
        assert_eq!(names(&skills), ["extension-helper"]);
        assert_eq!(
            skills[0].extension_name.as_deref(),
            Some("Pretty Extension")
        );
        assert_eq!(skills[0].priority, Some(0.0));
        assert!(
            manager
                .get_cached_skills(Some(SkillLevel::Project))
                .unwrap()
                .is_empty()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn base_dirs_expand_home_deduplicate_custom_roots_and_reject_extension_level() {
        let root = temp_root("base-dirs");
        let mut config = setup(&root);
        config.custom_skill_dirs = vec!["~/custom".to_owned(), "./relative".to_owned()];
        let default_user_root = config.global_canopy_dir.join("skills");
        config
            .custom_skill_dirs
            .push(default_user_root.to_string_lossy().into_owned());
        let manager = SkillManager::new(config.clone());
        let dirs = manager.get_skills_base_dirs(SkillLevel::User).unwrap();
        assert_eq!(dirs.len(), 4);
        assert_eq!(dirs[2], config.home_dir.join("custom"));
        assert_eq!(dirs[3], root.join("relative"));
        assert!(manager.get_skills_base_dirs(SkillLevel::Extension).is_err());
    }

    #[tokio::test]
    async fn manager_parser_tracks_errors_and_parses_command_and_http_hooks() {
        let root = temp_root("parse");
        let manager = SkillManager::new(setup(&root));
        let content = "---\nname: hooked\ndescription: Hook support\nhooks:\n  PreToolUse:\n    - matcher: \"Bash\"\n      hooks:\n        - type: command\n          command: 'echo checking'\n          timeout: 5\n    - matcher: \"Write\"\n      hooks:\n        - type: http\n          url: 'https://example.test/hook'\n          headers:\n            Authorization: 'Bearer token'\n          allowedEnvVars:\n            - API_KEY\n          timeout: 10\n  UnknownEvent:\n    - matcher: \"*\"\n      hooks:\n        - type: command\n          command: 'ignored'\n---\nBody\n";
        let skill = manager
            .parse_skill_content(content, root.join("hooked/SKILL.md"), SkillLevel::User)
            .unwrap();
        assert_eq!(skill.level, SkillLevel::User);
        let matchers = &skill.hooks.as_ref().unwrap()["PreToolUse"];
        assert_eq!(matchers.len(), 2);
        assert_eq!(matchers[0]["matcher"], "Bash");
        assert_eq!(matchers[0]["hooks"][0]["command"], "echo checking");
        assert_eq!(
            matchers[1]["hooks"][0]["headers"]["Authorization"],
            "Bearer token"
        );
        assert!(!skill.hooks.as_ref().unwrap().contains_key("UnknownEvent"));

        let flow_content = "---\nname: flow-hook\ndescription: Flow hook\nhooks: {PreToolUse: [{matcher: Bash, hooks: [{type: command, command: 'echo flow'}]}]}\n---\nBody\n";
        let flow_skill = manager
            .parse_skill_content(
                flow_content,
                root.join("flow-hook/SKILL.md"),
                SkillLevel::Project,
            )
            .unwrap();
        assert_eq!(
            flow_skill.hooks.as_ref().unwrap()["PreToolUse"][0]["hooks"][0]["command"],
            "echo flow"
        );

        assert!(
            manager
                .parse_skill_content("broken", root.join("bad/SKILL.md"), SkillLevel::Project)
                .is_err()
        );
        let errors = manager.get_parse_errors();
        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors.get_index(0).unwrap().1.code,
            SkillErrorCode::ParseError
        );
        fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn listeners_are_removable_isolated_and_suppression_is_one_shot() {
        let root = temp_root("listeners");
        let manager = Arc::new(SkillManager::new(setup(&root)));
        let observed = Arc::new(AtomicBool::new(false));
        let observed_clone = observed.clone();
        let removed = manager.add_change_listener(Arc::new(|| Box::pin(async {})));
        let _unregister_observed = manager.add_change_listener(Arc::new(move || {
            let observed = observed_clone.clone();
            Box::pin(async move { observed.store(true, Ordering::Release) })
        }));
        removed();
        let _unregister_panicking = manager.add_change_listener(Arc::new(|| {
            Box::pin(async { panic!("isolated listener panic") })
        }));
        manager.notify_config_changed().await;
        assert!(observed.load(Ordering::Acquire));
        manager.suppress_next_slash_reload();
        assert!(manager.consume_slash_reload_suppression());
        assert!(!manager.consume_slash_reload_suppression());
    }

    #[tokio::test]
    async fn path_activation_uses_visible_precedence_and_batches_notifications() {
        let root = temp_root("activation");
        let config = setup(&root);
        write_skill(
            &config.project_root.join(".canopy/skills"),
            "helper",
            "helper",
            "Project helper",
            "paths:\n  - \"src/**\"\n",
        );
        write_skill(
            &config.global_canopy_dir.join("skills"),
            "helper",
            "helper",
            "User helper",
            "paths:\n  - \"lib/**\"\n",
        );
        write_skill(
            &config.project_root.join(".agents/skills"),
            "hidden",
            "hidden",
            "Hidden skill",
            "paths:\n  - \"src/**\"\ndisable-model-invocation: true\n",
        );
        let manager = Arc::new(SkillManager::new(config));
        manager.refresh_cache().await;
        let activated = Arc::new(AtomicU64::new(0));
        let counter = activated.clone();
        let _unregister_counter = manager.add_change_listener(Arc::new(move || {
            let counter = counter.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::AcqRel);
            })
        }));
        assert!(
            manager
                .match_and_activate_by_path(root.join("project/lib/file.rs"))
                .await
                .is_empty()
        );
        let activated_names = manager
            .match_and_activate_by_paths(&[
                root.join("project/src/a.rs"),
                root.join("project/src/b.rs"),
            ])
            .await;
        assert_eq!(activated_names, ["helper"]);
        assert_eq!(activated.load(Ordering::Acquire), 1);
        assert!(manager.get_activated_skill_names().contains("helper"));
        assert!(!manager.get_activated_skill_names().contains("hidden"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn watcher_policy_matches_special_file_git_and_depth_contract() {
        assert!(watcher_ignored(
            Path::new("/skills/socket"),
            Some(WatcherEntryKind::Special)
        ));
        assert!(watcher_ignored(
            Path::new("/skills/.git/config"),
            Some(WatcherEntryKind::File)
        ));
        assert!(!watcher_ignored(
            Path::new("/skills/a/SKILL.md"),
            Some(WatcherEntryKind::File)
        ));
        assert_eq!(WATCHER_MAX_DEPTH, 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn load_skills_from_dir_accepts_directory_symlinks_and_skips_broken_targets() {
        use std::os::unix::fs::symlink;
        let root = temp_root("symlink");
        let base = root.join("skills");
        let external = root.join("external");
        write_skill(&external, "linked", "linked", "External skill", "");
        fs::create_dir_all(&base).unwrap();
        symlink(external.join("linked"), base.join("linked")).unwrap();
        symlink(root.join("missing"), base.join("broken")).unwrap();
        let manager = SkillManager::new(setup(&root));
        let skills = manager
            .load_skills_from_dir(&base, SkillLevel::Project)
            .await;
        assert_eq!(names(&skills), ["linked"]);
        fs::remove_dir_all(root).unwrap();
    }
}
