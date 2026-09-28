// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//!
//! Provider installation and rollback behavior ported from
//! `packages/core/src/providers/install.ts`.
//!
//! Settings, process environment, and runtime callbacks are injected so the
//! host can adapt its own persistence and model reload lifecycle. The module
//! owns the ordering and rollback contract shared by those adapters.

use crate::providers::presets::{self, ProviderModelConfig, ProviderModelIdentity};
use crate::utils::error_parsing::AuthType;
use indexmap::IndexMap;
use serde_json::Value;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub type InstallCause = Box<dyn Error + Send + Sync + 'static>;
pub type ModelProvidersConfig = IndexMap<String, Vec<ProviderModelConfig>>;

const DENY_ENV_KEYS: &[&str] = &[
    "NODE_OPTIONS",
    "NODE_PATH",
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
    "DYLD_INSERT_LIBRARIES",
    "DYLD_LIBRARY_PATH",
    "PATH",
    "HOME",
    "TMPDIR",
    "TMP",
    "TEMP",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergeStrategy {
    PrependAndRemoveOwned,
    ReplaceOwned,
    Append,
}

impl MergeStrategy {
    fn from_source(value: &str) -> Self {
        match value {
            "append" => Self::Append,
            "replace-owned" => Self::ReplaceOwned,
            // The TypeScript implementation treats every non-append,
            // non-replace-owned value as prepend-and-remove-owned.
            _ => Self::PrependAndRemoveOwned,
        }
    }
}

/// Ownership rule used by a provider patch. `Predicate` is the host-facing
/// equivalent of TypeScript's arbitrary `ownsModel` callback. `Preset` lets a
/// plan produced by the Rust preset catalog keep using its registered rule.
#[derive(Clone)]
pub enum ModelOwnership {
    Preset(&'static str),
    Predicate(Arc<dyn Fn(&ProviderModelConfig) -> bool + Send + Sync>),
}

impl fmt::Debug for ModelOwnership {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Preset(id) => formatter.debug_tuple("Preset").field(id).finish(),
            Self::Predicate(_) => formatter.write_str("Predicate(..)"),
        }
    }
}

impl ModelOwnership {
    fn owns(&self, model: &ProviderModelConfig) -> bool {
        match self {
            Self::Predicate(predicate) => predicate(model),
            Self::Preset(provider_id) => {
                presets::find_provider_by_id(provider_id).is_some_and(|preset| {
                    presets::owns_model(
                        preset,
                        ProviderModelIdentity {
                            id: &model.id,
                            name: Some(&model.name),
                            base_url: model.base_url.as_deref(),
                            env_key: model.env_key.as_deref(),
                        },
                    )
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct InstallModelProvidersPatch {
    pub auth_type: AuthType,
    pub models: Vec<ProviderModelConfig>,
    pub merge_strategy: MergeStrategy,
    pub owns_model: Option<ModelOwnership>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LegacyCredentials {
    pub api_key: Option<String>,
    pub base_url: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProviderModelSelection {
    pub model_id: String,
    pub base_url: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ProviderInstallPlan {
    pub provider_id: String,
    pub auth_type: AuthType,
    /// An ordered list mirrors JavaScript `Object.entries` for env setup.
    pub env: Vec<(String, String)>,
    pub legacy_credentials: Option<LegacyCredentials>,
    pub model_selection: Option<ProviderModelSelection>,
    pub model_providers: Vec<InstallModelProvidersPatch>,
    pub provider_state: IndexMap<String, IndexMap<String, String>>,
}

impl From<&presets::ProviderInstallPlan> for ProviderInstallPlan {
    fn from(plan: &presets::ProviderInstallPlan) -> Self {
        Self {
            provider_id: plan.provider_id.to_owned(),
            auth_type: plan.auth_type,
            env: plan
                .env
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            legacy_credentials: None,
            model_selection: plan.model_selection.as_ref().map(|selection| {
                ProviderModelSelection {
                    model_id: selection.model_id.clone(),
                    base_url: selection.base_url.clone(),
                }
            }),
            model_providers: plan
                .model_providers
                .iter()
                .map(|patch| InstallModelProvidersPatch {
                    auth_type: patch.auth_type,
                    models: patch.models.clone(),
                    merge_strategy: MergeStrategy::from_source(patch.merge_strategy),
                    owns_model: patch.owns_model_preset_id.map(ModelOwnership::Preset),
                })
                .collect(),
            provider_state: plan
                .provider_state
                .as_ref()
                .map(|state| {
                    state
                        .iter()
                        .map(|(key, entries)| {
                            (
                                key.clone(),
                                entries
                                    .iter()
                                    .map(|(field, value)| (field.clone(), value.clone()))
                                    .collect(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ApplyProviderInstallOptions {
    /// TypeScript defaults this option to true.
    pub do_refresh_auth: bool,
}

impl Default for ApplyProviderInstallOptions {
    fn default() -> Self {
        Self {
            do_refresh_auth: true,
        }
    }
}

#[derive(Debug)]
pub struct ApplyProviderInstallResult {
    pub updated_model_providers: ModelProvidersConfig,
    /// Environment keys whose preexisting process values differ from the
    /// value being installed and may shadow the settings fallback after restart.
    pub shadowed_env_keys: Vec<String>,
}

/// Host settings adapter. The host may persist eagerly from `set_value`; the
/// explicit backup/restore and persist methods preserve the source contract.
pub trait ProviderSettingsAdapter {
    fn get_value(&self, key: &str) -> Result<Option<Value>, InstallCause>;
    fn set_value(&mut self, key: &str, value: Value) -> Result<(), InstallCause>;
    fn get_model_providers(&self) -> ModelProvidersConfig;

    fn backup(&mut self) -> Result<(), InstallCause> {
        Ok(())
    }
    fn restore(&mut self) -> Result<(), InstallCause> {
        Ok(())
    }
    fn persist(&mut self) -> Result<(), InstallCause>;
    fn cleanup_backup(&mut self) -> Result<(), InstallCause> {
        Ok(())
    }
}

/// Abstracts the environment store used by the host. The installer snapshots
/// and restores values through this adapter; it does not mutate Rust's global
/// process environment directly, whose writes require unsafe code in edition
/// 2024. A host can route this adapter to its serialized environment registry.
pub trait ProcessEnvironment {
    fn get(&self, key: &str) -> Option<String>;
    fn set(&mut self, key: &str, value: &str) -> Result<(), InstallCause>;
    fn remove(&mut self, key: &str) -> Result<(), InstallCause>;
}

/// Optional live-runtime callbacks. Implementations should reload the
/// in-memory provider map, sync auth selection, and refresh provider auth.
pub trait ProviderInstallRuntime {
    fn reload_model_providers(
        &mut self,
        _providers: &ModelProvidersConfig,
    ) -> Result<(), InstallCause> {
        Ok(())
    }

    fn sync_auth_state(
        &mut self,
        _auth_type: AuthType,
        _model_id: &str,
        _base_url: Option<&str>,
    ) -> Result<(), InstallCause> {
        Ok(())
    }

    fn refresh_auth<'a>(
        &'a mut self,
        _auth_type: AuthType,
    ) -> Pin<Box<dyn Future<Output = Result<(), InstallCause>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn warn_shadowed_env(&mut self, keys: &[String]) {
        let noun = if keys.len() == 1 { "is" } else { "are" };
        eprintln!(
            "[auth] Warning: {} {noun} also set in your shell environment or .env file. The shell/file value will take priority on restart. To ensure your new key is used, update or remove the variable from your shell profile or .env file.",
            keys.join(", ")
        );
    }

    /// Best-effort rollback diagnostics. Failure here must not stop later
    /// rollback steps or replace the original install error.
    fn report_rollback_error(&mut self, stage: &'static str, message: &str) {
        eprintln!("[applyProviderInstallPlan] {stage} failed during rollback: {message}");
    }
}

pub struct ProviderInstallError {
    pub message: String,
    pub step: &'static str,
    pub auth_type: AuthType,
    cause: Option<InstallCause>,
}

impl ProviderInstallError {
    fn new(cause: InstallCause, step: &'static str, auth_type: AuthType) -> Self {
        Self {
            message: cause.to_string(),
            step,
            auth_type,
            cause: Some(cause),
        }
    }

    pub fn cause(&self) -> Option<&(dyn Error + Send + Sync + 'static)> {
        self.cause.as_deref()
    }
}

impl fmt::Debug for ProviderInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderInstallError")
            .field("message", &self.message)
            .field("step", &self.step)
            .field("auth_type", &self.auth_type)
            .field("cause", &self.cause.as_ref().map(ToString::to_string))
            .finish()
    }
}

impl fmt::Display for ProviderInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ProviderInstallError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause
            .as_deref()
            .map(|cause| cause as &(dyn Error + 'static))
    }
}

#[derive(Debug)]
struct InstallMessageError(String);

impl fmt::Display for InstallMessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for InstallMessageError {}

fn message_cause(message: impl Into<String>) -> InstallCause {
    Box::new(InstallMessageError(message.into()))
}

fn same_model_identity(left: &ProviderModelConfig, right: &ProviderModelConfig) -> bool {
    left.id == right.id
        && left.base_url.as_deref().unwrap_or("") == right.base_url.as_deref().unwrap_or("")
}

fn apply_model_providers_patch(
    existing: &ModelProvidersConfig,
    patch: &InstallModelProvidersPatch,
) -> ModelProvidersConfig {
    let key = patch.auth_type.as_str();
    let previous = existing.get(key).cloned().unwrap_or_default();
    let next = match patch.merge_strategy {
        MergeStrategy::Append => previous
            .into_iter()
            .chain(patch.models.iter().cloned())
            .collect(),
        strategy => {
            let preserved: Vec<_> = previous
                .into_iter()
                .filter(|model| {
                    if let Some(owns_model) = &patch.owns_model {
                        !owns_model.owns(model)
                    } else {
                        !patch
                            .models
                            .iter()
                            .any(|new_model| same_model_identity(new_model, model))
                    }
                })
                .collect();
            if strategy == MergeStrategy::ReplaceOwned {
                preserved
                    .into_iter()
                    .chain(patch.models.iter().cloned())
                    .collect()
            } else {
                patch.models.iter().cloned().chain(preserved).collect()
            }
        }
    };

    let mut updated = existing.clone();
    updated.insert(key.to_owned(), next);
    updated
}

fn is_reserved_env_key(key: &str) -> bool {
    let key = key.to_ascii_uppercase();
    DENY_ENV_KEYS.contains(&key.as_str())
}

fn validate_env_assignment(key: &str, value: &str) -> Result<(), InstallCause> {
    if key.is_empty() || key.contains('=') || key.contains('\0') || value.contains('\0') {
        return Err(message_cause(format!(
            "Invalid process environment variable assignment for {key:?}"
        )));
    }
    Ok(())
}

fn should_keep_current_selection(
    settings: &dyn ProviderSettingsAdapter,
    plan: &ProviderInstallPlan,
) -> Result<bool, InstallCause> {
    let Some(selection) = plan.model_selection.as_ref() else {
        return Ok(false);
    };
    if selection.model_id.is_empty() {
        return Ok(false);
    }

    let current_model_id = settings.get_value("model.name")?;
    let Some(current_model_id) =
        current_model_id.and_then(|value| value.as_str().map(str::to_owned))
    else {
        return Ok(false);
    };
    if current_model_id.is_empty() {
        return Ok(false);
    }

    let current_base_url = settings.get_value("model.baseUrl")?;
    let id_only = match current_base_url.as_ref() {
        None => true,
        Some(Value::String(value)) => value.is_empty(),
        Some(_) => false,
    };
    let normalized_current_base_url = match current_base_url.as_ref() {
        None | Some(Value::Null) => Some(""),
        Some(Value::String(value)) => Some(value.as_str()),
        // The TypeScript identity helper compares this value directly with
        // model.baseUrl. Non-string settings therefore cannot match a model.
        Some(_) => None,
    };

    Ok(plan.model_providers.iter().any(|patch| {
        patch.models.iter().any(|model| {
            if id_only {
                model.id == current_model_id
            } else {
                normalized_current_base_url.is_some_and(|base_url| {
                    model.id == current_model_id
                        && base_url == model.base_url.as_deref().unwrap_or("")
                })
            }
        })
    }))
}

fn set_setting<S: ProviderSettingsAdapter>(
    settings: &mut S,
    key: &str,
    value: impl serde::Serialize,
) -> Result<(), InstallCause> {
    let value = serde_json::to_value(value).map_err(|error| Box::new(error) as InstallCause)?;
    settings.set_value(key, value)
}

/// Apply an installation plan and restore settings, env values, and runtime
/// provider state if any step fails. `settings.get_model_providers()` is
/// snapshotted before the guarded sequence, matching the TypeScript adapter.
pub async fn apply_provider_install_plan<S, E, R>(
    plan: &ProviderInstallPlan,
    settings: &mut S,
    process_env: &mut E,
    runtime: &mut R,
    options: ApplyProviderInstallOptions,
) -> Result<ApplyProviderInstallResult, ProviderInstallError>
where
    S: ProviderSettingsAdapter,
    E: ProcessEnvironment,
    R: ProviderInstallRuntime,
{
    let previous_runtime_providers = settings.get_model_providers();
    let mut previous_env_values: IndexMap<String, Option<String>> = IndexMap::new();
    let mut current_step = "init";

    let result = async {
        current_step = "backup";
        settings.backup()?;

        current_step = "env";
        let mut shadowed_env_keys = Vec::new();
        for (key, value) in &plan.env {
            if is_reserved_env_key(key) {
                return Err(message_cause(format!(
                    "Install plan must not set reserved environment variable: {key}"
                )));
            }
            validate_env_assignment(key, value)?;
            let previous = process_env.get(key);
            if previous
                .as_ref()
                .is_some_and(|previous| !previous.is_empty() && previous != value)
            {
                shadowed_env_keys.push(key.clone());
            }
            previous_env_values.entry(key.clone()).or_insert(previous);
            set_setting(settings, &format!("env.{key}"), value)?;
            process_env.set(key, value)?;
        }
        if !shadowed_env_keys.is_empty() {
            runtime.warn_shadowed_env(&shadowed_env_keys);
        }

        current_step = "modelProviders";
        let mut updated_model_providers = previous_runtime_providers.clone();
        for patch in &plan.model_providers {
            updated_model_providers = apply_model_providers_patch(&updated_model_providers, patch);
            let key = format!("modelProviders.{}", patch.auth_type.as_str());
            let models = updated_model_providers
                .get(patch.auth_type.as_str())
                .cloned()
                .unwrap_or_default();
            set_setting(settings, &key, models)?;
        }

        current_step = "authType";
        set_setting(
            settings,
            "security.auth.selectedType",
            plan.auth_type.as_str(),
        )?;

        current_step = "legacyCredentials";
        if let Some(credentials) = &plan.legacy_credentials {
            if let Some(api_key) = &credentials.api_key {
                set_setting(settings, "security.auth.apiKey", api_key)?;
            }
            if let Some(base_url) = &credentials.base_url {
                set_setting(settings, "security.auth.baseUrl", base_url)?;
            }
        }

        current_step = "modelSelection";
        let retain_selection = should_keep_current_selection(settings, plan)?;
        let effective_selection = if retain_selection {
            None
        } else {
            plan.model_selection
                .as_ref()
                .filter(|selection| !selection.model_id.is_empty())
        };
        if let Some(selection) = effective_selection {
            set_setting(settings, "model.name", &selection.model_id)?;
            if let Some(base_url) = selection.base_url.as_deref().filter(|url| !url.is_empty()) {
                set_setting(settings, "model.baseUrl", base_url)?;
            } else {
                // Empty string is a tombstone that overrides lower-scope
                // model URLs when settings are merged.
                set_setting(settings, "model.baseUrl", "")?;
            }
        }

        current_step = "providerState";
        for (key, entries) in &plan.provider_state {
            for (field, value) in entries {
                set_setting(settings, &format!("{key}.{field}"), value)?;
            }
        }

        current_step = "persist";
        settings.persist()?;

        current_step = "reloadModelProviders";
        runtime.reload_model_providers(&updated_model_providers)?;
        if let Some(selection) = effective_selection {
            current_step = "syncAuthState";
            runtime.sync_auth_state(
                plan.auth_type,
                &selection.model_id,
                selection.base_url.as_deref(),
            )?;
        }
        if options.do_refresh_auth {
            current_step = "refreshAuth";
            runtime.refresh_auth(plan.auth_type).await?;
        }

        current_step = "cleanupBackup";
        settings.cleanup_backup()?;

        Ok(ApplyProviderInstallResult {
            updated_model_providers,
            shadowed_env_keys,
        })
    }
    .await;

    match result {
        Ok(result) => Ok(result),
        Err(cause) => {
            if let Err(rollback_error) = settings.restore() {
                runtime.report_rollback_error("settings.restore", &rollback_error.to_string());
            }
            for (key, previous) in &previous_env_values {
                let rollback_result = match previous {
                    Some(value) => process_env.set(key, value),
                    None => process_env.remove(key),
                };
                if let Err(rollback_error) = rollback_result {
                    runtime.report_rollback_error("environment", &rollback_error.to_string());
                    break;
                }
            }
            if let Err(rollback_error) = runtime.reload_model_providers(&previous_runtime_providers)
            {
                runtime.report_rollback_error("reloadModelProviders", &rollback_error.to_string());
            }
            Err(ProviderInstallError::new(
                cause,
                current_step,
                plan.auth_type,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[derive(Debug)]
    struct TestError(String);

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(&self.0)
        }
    }

    impl Error for TestError {}

    fn test_error(message: impl Into<String>) -> InstallCause {
        Box::new(TestError(message.into()))
    }

    fn model(id: &str, base_url: Option<&str>, env_key: Option<&str>) -> ProviderModelConfig {
        ProviderModelConfig {
            id: id.to_owned(),
            name: id.to_owned(),
            description: None,
            base_url: base_url.map(str::to_owned),
            env_key: env_key.map(str::to_owned),
            image_only: None,
            generation_config: None,
        }
    }

    fn empty_plan() -> ProviderInstallPlan {
        ProviderInstallPlan {
            provider_id: "test-provider".to_owned(),
            auth_type: AuthType::OpenAi,
            env: Vec::new(),
            legacy_credentials: None,
            model_selection: None,
            model_providers: Vec::new(),
            provider_state: IndexMap::new(),
        }
    }

    #[derive(Default)]
    struct FakeSettings {
        values: IndexMap<String, Value>,
        model_providers: ModelProvidersConfig,
        set_calls: Vec<(String, Value)>,
        restore_calls: usize,
        persist_calls: usize,
        cleanup_calls: usize,
        snapshot: Option<(IndexMap<String, Value>, ModelProvidersConfig)>,
        fail_backup: bool,
        fail_restore: bool,
    }

    impl ProviderSettingsAdapter for FakeSettings {
        fn get_value(&self, key: &str) -> Result<Option<Value>, InstallCause> {
            Ok(self.values.get(key).cloned())
        }

        fn set_value(&mut self, key: &str, value: Value) -> Result<(), InstallCause> {
            self.set_calls.push((key.to_owned(), value.clone()));
            self.values.insert(key.to_owned(), value);
            Ok(())
        }

        fn get_model_providers(&self) -> ModelProvidersConfig {
            self.model_providers.clone()
        }

        fn backup(&mut self) -> Result<(), InstallCause> {
            if self.fail_backup {
                return Err(test_error("backup failed"));
            }
            self.snapshot = Some((self.values.clone(), self.model_providers.clone()));
            Ok(())
        }

        fn restore(&mut self) -> Result<(), InstallCause> {
            self.restore_calls += 1;
            if self.fail_restore {
                return Err(test_error("restore failed"));
            }
            if let Some((values, providers)) = self.snapshot.clone() {
                self.values = values;
                self.model_providers = providers;
            }
            Ok(())
        }

        fn persist(&mut self) -> Result<(), InstallCause> {
            self.persist_calls += 1;
            Ok(())
        }

        fn cleanup_backup(&mut self) -> Result<(), InstallCause> {
            self.cleanup_calls += 1;
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeEnvironment {
        values: HashMap<String, String>,
    }

    impl ProcessEnvironment for FakeEnvironment {
        fn get(&self, key: &str) -> Option<String> {
            self.values.get(key).cloned()
        }

        fn set(&mut self, key: &str, value: &str) -> Result<(), InstallCause> {
            self.values.insert(key.to_owned(), value.to_owned());
            Ok(())
        }

        fn remove(&mut self, key: &str) -> Result<(), InstallCause> {
            self.values.remove(key);
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeRuntime {
        reload_calls: Vec<ModelProvidersConfig>,
        sync_calls: Vec<(AuthType, String, Option<String>)>,
        refresh_calls: Vec<AuthType>,
        warning_keys: Vec<Vec<String>>,
        rollback_errors: Vec<(&'static str, String)>,
        fail_refresh: Option<String>,
    }

    impl ProviderInstallRuntime for FakeRuntime {
        fn reload_model_providers(
            &mut self,
            providers: &ModelProvidersConfig,
        ) -> Result<(), InstallCause> {
            self.reload_calls.push(providers.clone());
            Ok(())
        }

        fn sync_auth_state(
            &mut self,
            auth_type: AuthType,
            model_id: &str,
            base_url: Option<&str>,
        ) -> Result<(), InstallCause> {
            self.sync_calls
                .push((auth_type, model_id.to_owned(), base_url.map(str::to_owned)));
            Ok(())
        }

        fn refresh_auth<'a>(
            &'a mut self,
            auth_type: AuthType,
        ) -> Pin<Box<dyn Future<Output = Result<(), InstallCause>> + Send + 'a>> {
            self.refresh_calls.push(auth_type);
            let failure = self.fail_refresh.clone();
            Box::pin(async move {
                if let Some(message) = failure {
                    return Err(test_error(message));
                }
                Ok(())
            })
        }

        fn warn_shadowed_env(&mut self, keys: &[String]) {
            self.warning_keys.push(keys.to_vec());
        }

        fn report_rollback_error(&mut self, stage: &'static str, message: &str) {
            self.rollback_errors.push((stage, message.to_owned()));
        }
    }

    #[tokio::test]
    async fn rejects_reserved_environment_names_without_writing_them() {
        let mut plan = empty_plan();
        plan.env.push(("pAtH".to_owned(), "/tmp/evil".to_owned()));
        let mut settings = FakeSettings::default();
        let mut env = FakeEnvironment::default();
        let mut runtime = FakeRuntime::default();

        let error = apply_provider_install_plan(
            &plan,
            &mut settings,
            &mut env,
            &mut runtime,
            ApplyProviderInstallOptions::default(),
        )
        .await
        .unwrap_err();

        assert_eq!(error.step, "env");
        assert_eq!(error.auth_type, AuthType::OpenAi);
        assert_eq!(
            error.message,
            "Install plan must not set reserved environment variable: pAtH"
        );
        assert!(settings.set_calls.is_empty());
        assert!(!env.values.contains_key("pAtH"));
        assert_eq!(settings.restore_calls, 1);
        assert_eq!(runtime.reload_calls.len(), 1);
    }

    #[test]
    fn merge_strategies_keep_identity_ownership_and_other_protocols() {
        let mut existing = ModelProvidersConfig::new();
        existing.insert(
            "openai".to_owned(),
            vec![
                model("shared", Some("https://proxy.example/v1"), None),
                model("shared", Some("https://api.example/v1"), None),
                model("old-owned", None, Some("TEST_KEY")),
            ],
        );
        existing.insert("gemini".to_owned(), vec![model("other", None, None)]);

        let identity_patch = InstallModelProvidersPatch {
            auth_type: AuthType::OpenAi,
            models: vec![model("shared", Some("https://api.example/v1/"), None)],
            merge_strategy: MergeStrategy::PrependAndRemoveOwned,
            owns_model: None,
        };
        let after_identity = apply_model_providers_patch(&existing, &identity_patch);
        // Identity uses id + baseUrl exactly; trailing slashes are not normalized.
        assert_eq!(after_identity["openai"].len(), 4);
        assert_eq!(
            after_identity["openai"][0].base_url.as_deref(),
            Some("https://api.example/v1/")
        );

        let owner_patch = InstallModelProvidersPatch {
            auth_type: AuthType::OpenAi,
            models: vec![model("new", None, Some("TEST_KEY"))],
            merge_strategy: MergeStrategy::ReplaceOwned,
            owns_model: Some(ModelOwnership::Predicate(Arc::new(|model| {
                model.env_key.as_deref() == Some("TEST_KEY")
            }))),
        };
        let after_owner = apply_model_providers_patch(&after_identity, &owner_patch);
        assert_eq!(after_owner["openai"].last().unwrap().id, "new");
        assert_eq!(after_owner["gemini"], existing["gemini"]);

        let append_patch = InstallModelProvidersPatch {
            auth_type: AuthType::OpenAi,
            models: vec![model("tail", None, None)],
            merge_strategy: MergeStrategy::Append,
            owns_model: None,
        };
        let after_append = apply_model_providers_patch(&after_owner, &append_patch);
        assert_eq!(after_append["openai"].last().unwrap().id, "tail");
    }

    #[tokio::test]
    async fn retains_a_current_model_offered_by_a_patch() {
        let mut plan = empty_plan();
        plan.model_selection = Some(ProviderModelSelection {
            model_id: "default-model".to_owned(),
            base_url: Some("https://new.example/v1".to_owned()),
        });
        plan.model_providers.push(InstallModelProvidersPatch {
            auth_type: AuthType::OpenAi,
            models: vec![model(
                "current-model",
                Some("https://another.example/v1"),
                None,
            )],
            merge_strategy: MergeStrategy::Append,
            owns_model: None,
        });
        let mut settings = FakeSettings::default();
        settings.values.insert(
            "model.name".to_owned(),
            Value::String("current-model".to_owned()),
        );
        settings
            .values
            .insert("model.baseUrl".to_owned(), Value::String(String::new()));
        let mut env = FakeEnvironment::default();
        let mut runtime = FakeRuntime::default();

        apply_provider_install_plan(
            &plan,
            &mut settings,
            &mut env,
            &mut runtime,
            ApplyProviderInstallOptions {
                do_refresh_auth: false,
            },
        )
        .await
        .unwrap();

        assert!(
            !settings
                .set_calls
                .iter()
                .any(|(key, _)| key == "model.name")
        );
        assert!(
            !settings
                .set_calls
                .iter()
                .any(|(key, _)| key == "model.baseUrl")
        );
        assert!(runtime.sync_calls.is_empty());
        assert!(runtime.refresh_calls.is_empty());
    }

    #[tokio::test]
    async fn model_selection_retention_respects_a_nonempty_base_url() {
        let mut plan = empty_plan();
        plan.model_selection = Some(ProviderModelSelection {
            model_id: "new-default".to_owned(),
            base_url: Some("https://new.example/v1".to_owned()),
        });
        plan.model_providers.push(InstallModelProvidersPatch {
            auth_type: AuthType::OpenAi,
            models: vec![model("shared-id", Some("https://other.example/v1"), None)],
            merge_strategy: MergeStrategy::Append,
            owns_model: None,
        });
        let mut settings = FakeSettings::default();
        settings.values.insert(
            "model.name".to_owned(),
            Value::String("shared-id".to_owned()),
        );
        settings.values.insert(
            "model.baseUrl".to_owned(),
            Value::String("https://current.example/v1".to_owned()),
        );
        let mut env = FakeEnvironment::default();
        let mut runtime = FakeRuntime::default();

        apply_provider_install_plan(
            &plan,
            &mut settings,
            &mut env,
            &mut runtime,
            ApplyProviderInstallOptions {
                do_refresh_auth: false,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            settings.values["model.name"],
            Value::String("new-default".to_owned())
        );
        assert_eq!(
            settings.values["model.baseUrl"],
            Value::String("https://new.example/v1".to_owned())
        );
        assert_eq!(runtime.sync_calls.len(), 1);
    }

    #[tokio::test]
    async fn reports_shadowed_shell_environment_values() {
        let mut plan = empty_plan();
        plan.env
            .push(("SHADOW_KEY".to_owned(), "installed".to_owned()));
        let mut settings = FakeSettings::default();
        let mut env = FakeEnvironment::default();
        env.values
            .insert("SHADOW_KEY".to_owned(), "from-shell".to_owned());
        let mut runtime = FakeRuntime::default();

        let result = apply_provider_install_plan(
            &plan,
            &mut settings,
            &mut env,
            &mut runtime,
            ApplyProviderInstallOptions {
                do_refresh_auth: false,
            },
        )
        .await
        .unwrap();

        assert_eq!(result.shadowed_env_keys, vec!["SHADOW_KEY".to_owned()]);
        assert_eq!(runtime.warning_keys, vec![vec!["SHADOW_KEY".to_owned()]]);
    }

    #[tokio::test]
    async fn refresh_failure_restores_env_and_runtime_and_keeps_failure_context() {
        let mut previous = ModelProvidersConfig::new();
        previous.insert("openai".to_owned(), vec![model("previous", None, None)]);
        let mut settings = FakeSettings {
            model_providers: previous.clone(),
            ..FakeSettings::default()
        };
        let mut env = FakeEnvironment::default();
        env.values.insert("TEST_KEY".to_owned(), "old".to_owned());
        let mut runtime = FakeRuntime {
            fail_refresh: Some("endpoint unreachable".to_owned()),
            ..FakeRuntime::default()
        };
        let mut plan = empty_plan();
        plan.env.push(("TEST_KEY".to_owned(), "new".to_owned()));
        plan.env
            .push(("BRAND_NEW_KEY".to_owned(), "new".to_owned()));
        plan.model_providers.push(InstallModelProvidersPatch {
            auth_type: AuthType::OpenAi,
            models: vec![model("installed", None, Some("TEST_KEY"))],
            merge_strategy: MergeStrategy::PrependAndRemoveOwned,
            owns_model: None,
        });

        let error = apply_provider_install_plan(
            &plan,
            &mut settings,
            &mut env,
            &mut runtime,
            ApplyProviderInstallOptions::default(),
        )
        .await
        .unwrap_err();

        assert_eq!(error.step, "refreshAuth");
        assert_eq!(error.auth_type, AuthType::OpenAi);
        assert_eq!(error.message, "endpoint unreachable");
        assert_eq!(error.cause().unwrap().to_string(), "endpoint unreachable");
        assert_eq!(env.get("TEST_KEY").as_deref(), Some("old"));
        assert!(!env.values.contains_key("BRAND_NEW_KEY"));
        assert_eq!(settings.restore_calls, 1);
        assert_eq!(runtime.reload_calls.len(), 2);
        assert_eq!(runtime.reload_calls[0]["openai"][0].id, "installed");
        assert_eq!(runtime.reload_calls[1], previous);
    }

    #[tokio::test]
    async fn backup_failure_keeps_step_context_and_rollback_continues() {
        let mut settings = FakeSettings {
            fail_backup: true,
            fail_restore: true,
            ..FakeSettings::default()
        };
        let mut env = FakeEnvironment::default();
        env.values.insert("TEST_KEY".to_owned(), "old".to_owned());
        let mut runtime = FakeRuntime::default();
        let mut plan = empty_plan();
        plan.env.push(("TEST_KEY".to_owned(), "new".to_owned()));

        let error = apply_provider_install_plan(
            &plan,
            &mut settings,
            &mut env,
            &mut runtime,
            ApplyProviderInstallOptions::default(),
        )
        .await
        .unwrap_err();

        assert_eq!(error.step, "backup");
        assert_eq!(error.message, "backup failed");
        assert_eq!(env.get("TEST_KEY").as_deref(), Some("old"));
        assert_eq!(runtime.reload_calls.len(), 1);
        assert_eq!(runtime.rollback_errors[0].0, "settings.restore");
    }

    #[tokio::test]
    async fn writes_legacy_credentials_and_provider_state_and_cleans_backup() {
        let mut plan = empty_plan();
        plan.legacy_credentials = Some(LegacyCredentials {
            api_key: Some("legacy-key".to_owned()),
            base_url: Some("https://example.test/v1".to_owned()),
        });
        plan.provider_state.insert(
            "codingPlan".to_owned(),
            IndexMap::from([("version".to_owned(), "v1".to_owned())]),
        );
        let mut settings = FakeSettings::default();
        let mut env = FakeEnvironment::default();
        let mut runtime = FakeRuntime::default();

        apply_provider_install_plan(
            &plan,
            &mut settings,
            &mut env,
            &mut runtime,
            ApplyProviderInstallOptions {
                do_refresh_auth: false,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            settings.values["security.auth.apiKey"],
            Value::String("legacy-key".to_owned())
        );
        assert_eq!(
            settings.values["security.auth.baseUrl"],
            Value::String("https://example.test/v1".to_owned())
        );
        assert_eq!(
            settings.values["codingPlan.version"],
            Value::String("v1".to_owned())
        );
        assert_eq!(settings.persist_calls, 1);
        assert_eq!(settings.cleanup_calls, 1);
    }
}
