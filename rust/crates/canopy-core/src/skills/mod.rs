//! Skill filesystem contracts shared by the Rust runtime.

mod activation;
mod curator;
mod manager;
mod paths;
mod skill_load;
mod symlink_scope;
mod types;

pub use activation::{
    InvalidPatternHandler, MAX_SKILL_ACTIVATION_PATTERN_CODE_UNITS, SkillActivationRegistry,
    resolve_project_relative_path, resolve_symlink_aware_relative_paths, split_conditional_skills,
};
pub use curator::{
    AUTO_SKILL_ARCHIVE_AFTER_MS, AUTO_SKILL_CURATOR_INTERVAL_MS, AUTO_SKILL_STALE_AFTER_MS,
    AutoSkillCuratorAutomaticResult, AutoSkillCuratorEntry, AutoSkillCuratorRunResult,
    AutoSkillCuratorRuntime, AutoSkillCuratorStatus, AutoSkillState, CuratorDirectoryEntry,
    CuratorError, FsAutoSkillCuratorRuntime, FsCuratorLockGuard, MAX_MANIFEST_BYTES,
    MAX_STATE_FILE_BYTES, get_auto_skill_curator_status, maybe_run_auto_skill_curator,
    record_auto_skill_usage, restore_archived_auto_skill, run_auto_skill_curator,
    set_auto_skill_pinned,
};
pub use manager::{
    CHANGE_LISTENER_TIMEOUT, SKILLS_CONFIG_DIR, SkillChangeListener, SkillExtension, SkillManager,
    SkillManagerConfig, SkillWatchEventHandler, SkillWatchOptions, SkillWatcherBackend,
    SkillWatcherHandle, WATCHER_MAX_DEPTH, WATCHER_REFRESH_DEBOUNCE, WatcherEntryKind,
    watcher_ignored,
};
pub use paths::{
    ARCHIVED_SKILLS_RELATIVE_DIR, PENDING_SKILLS_RELATIVE_DIR, PROJECT_SKILLS_RELATIVE_DIR,
    SKILL_FILE_NAME, assert_project_skill_path, assert_real_project_skill_path,
    get_archived_skills_root, get_pending_skills_root, get_project_skills_root,
    is_project_skill_path, sanitize_skill_name,
};
pub use skill_load::{
    SKILL_MANIFEST_FILE, load_skills_from_dir, normalize_content, parse_skill_content,
    validate_config,
};
pub use symlink_scope::{SymlinkTargetCheck, SymlinkTargetFailure, validate_symlink_target};
pub use types::{
    ListSkillsOptions, SKILL_NAME_PATTERN, SkillConfig, SkillError, SkillErrorCode,
    SkillHooksSettings, SkillLevel, SkillRuntimeConfig, SkillValidationResult,
    normalize_skill_priority, parse_allowed_tools_field, parse_model_field, parse_paths_field,
    parse_priority_field, parse_user_invocable_field, validate_skill_name,
};
