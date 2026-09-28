//! Configuration models and settings-file loading.
//!
//! This module is intentionally isolated from the crate root while the Rust
//! port is being integrated. Add `pub mod config;` to `lib.rs` to expose it.

pub mod environment;
pub mod image_generation;
pub mod loader;
pub mod migrations;
pub mod schema;
pub mod warnings;

pub use environment::{
    EnvFileReadFailure, RuntimeEnvironmentSnapshot, build_runtime_environment, find_env_files,
    load_environment,
};
pub use loader::{
    LoadSettingsOptions, LoadedSettings, SettingScope, SettingsError, SettingsFile,
    SettingsLoadError, SettingsPaths, load_settings, load_settings_from_paths, merge_settings,
    update_setting_value,
};
pub use migrations::{
    MigrationResult, is_v2_container_key, run_migrations, settings_need_migration,
    v1_to_v2_migration_map,
};
pub use schema::{
    SettingDefinition, SettingType, SettingsSchema, merge_strategy_for_path, setting_definition,
    settings_schema, validate_setting_value,
};
pub use warnings::{get_settings_warnings, unknown_setting_keys};
