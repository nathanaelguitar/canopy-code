use std::fs::File;
use std::io::{Read, Take};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};
use url::Url;

const MAX_CONFIG_BYTES: u64 = 64 * 1024;
const DEFAULT_TIMEOUT_MS: u64 = 5_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigurationError(pub String);

impl std::fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigurationError {}

#[derive(Clone)]
pub struct ExternalContextConfig {
    pub version: u8,
    pub timeout_ms: u64,
    pub provider: ProviderConfig,
    pub write_enabled: bool,
    pub auto_recall: Option<AutoRecallConfig>,
}

#[derive(Clone, Debug)]
pub struct AutoRecallConfig {
    pub repository_root: PathBuf,
    pub timeout_ms: u64,
}

#[derive(Clone)]
pub enum ProviderConfig {
    Mem0PlatformV3 { api_key: String, app_id: String },
    GenericHttpSearchV1 { base_url: Url, token: String },
}

impl ProviderConfig {
    pub fn can_write(&self) -> bool {
        matches!(self, Self::Mem0PlatformV3 { .. })
    }
}

pub fn load_config() -> Result<ExternalContextConfig, ConfigurationError> {
    load_config_with_env(|name| std::env::var(name).ok())
}

fn load_config_with_env(
    mut read_env: impl FnMut(&str) -> Option<String>,
) -> Result<ExternalContextConfig, ConfigurationError> {
    let Some(config_path) =
        read_env("QWEN_EXTERNAL_CONTEXT_CONFIG").filter(|path| !path.is_empty())
    else {
        return Err(ConfigurationError(
            "QWEN_EXTERNAL_CONTEXT_CONFIG must name an absolute file path.".into(),
        ));
    };
    let path = Path::new(&config_path);
    if !path.is_absolute() {
        return Err(ConfigurationError(
            "QWEN_EXTERNAL_CONTEXT_CONFIG must name an absolute file path.".into(),
        ));
    }

    let source = read_config_file(path)?;
    let parsed: Value = serde_json::from_slice(&source)
        .map_err(|_| ConfigurationError("External context config is not valid JSON.".into()))?;
    let object = parsed.as_object().ok_or_else(invalid_config)?;
    let version = object
        .get("version")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && value.fract() == 0.0)
        .ok_or_else(invalid_config)?;
    let version = version as u64;
    match version {
        1 => reject_unknown_keys(object, &["version", "timeoutMs", "provider", "write"])?,
        2 => reject_unknown_keys(object, &["version", "timeoutMs", "provider", "autoRecall"])?,
        _ => return Err(invalid_config()),
    }
    let timeout_ms = optional_bounded_integer(object, "timeoutMs", DEFAULT_TIMEOUT_MS, 1, 30_000)?;
    let raw_provider = object
        .get("provider")
        .and_then(Value::as_object)
        .ok_or_else(invalid_config)?;
    let mem0_provider = validate_provider_shape(raw_provider)?;

    let auto_recall_shape = if version == 2 {
        let raw = object
            .get("autoRecall")
            .and_then(Value::as_object)
            .ok_or_else(invalid_config)?;
        reject_unknown_keys(raw, &["repositoryRoot", "timeoutMs"])?;
        let root = raw
            .get("repositoryRoot")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(invalid_config)?;
        let timeout_ms = optional_bounded_integer(raw, "timeoutMs", 1_500, 1, 5_000)?;
        Some((root.to_owned(), timeout_ms))
    } else {
        None
    };

    let write_enabled = match object.get("write") {
        None => false,
        Some(value) => {
            let write = value.as_object().ok_or_else(invalid_config)?;
            reject_unknown_keys(write, &["enabled"])?;
            if write.get("enabled").and_then(Value::as_bool) != Some(true) {
                return Err(invalid_config());
            }
            if !mem0_provider {
                return Err(ConfigurationError(
                    "External context memory writes require a Mem0 provider.".into(),
                ));
            }
            true
        }
    };

    let provider = resolve_provider(raw_provider, &mut read_env)?;
    let auto_recall = if let Some((root, timeout_ms)) = auto_recall_shape {
        let root_path = Path::new(&root);
        if !root_path.is_absolute() {
            return Err(ConfigurationError(
                "External context repository root is invalid.".into(),
            ));
        }
        let resolved = std::fs::canonicalize(root_path).map_err(|_| invalid_repository_root())?;
        let metadata = std::fs::metadata(&resolved).map_err(|_| invalid_repository_root())?;
        if !metadata.is_dir() || resolved.file_name().is_none() {
            return Err(invalid_repository_root());
        }
        Some(AutoRecallConfig {
            repository_root: resolved,
            timeout_ms,
        })
    } else {
        None
    };

    Ok(ExternalContextConfig {
        version: version as u8,
        timeout_ms,
        provider,
        write_enabled,
        auto_recall,
    })
}

fn read_config_file(path: &Path) -> Result<Vec<u8>, ConfigurationError> {
    let file = File::open(path)
        .map_err(|_| ConfigurationError("External context config could not be read.".into()))?;
    let metadata = file
        .metadata()
        .map_err(|_| ConfigurationError("External context config could not be read.".into()))?;
    if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES {
        return Err(ConfigurationError(
            "External context config is not a valid file.".into(),
        ));
    }
    let mut bytes = Vec::new();
    let mut bounded: Take<File> = file.take(MAX_CONFIG_BYTES + 1);
    bounded
        .read_to_end(&mut bytes)
        .map_err(|_| ConfigurationError("External context config could not be read.".into()))?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err(ConfigurationError(
            "External context config is not a valid file.".into(),
        ));
    }
    Ok(bytes)
}

fn resolve_provider(
    provider: &Map<String, Value>,
    read_env: &mut impl FnMut(&str) -> Option<String>,
) -> Result<ProviderConfig, ConfigurationError> {
    match provider.get("type").and_then(Value::as_str) {
        Some("mem0-platform-v3") => {
            reject_unknown_keys(provider, &["type", "apiKeyEnv", "appId"])?;
            let api_key_env = valid_env_name(provider.get("apiKeyEnv").and_then(Value::as_str))?;
            let app_id = provider
                .get("appId")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty() && utf16_len(value) <= 256)
                .ok_or_else(invalid_config)?;
            let api_key = read_credential(read_env, api_key_env)?;
            Ok(ProviderConfig::Mem0PlatformV3 {
                api_key,
                app_id: app_id.to_owned(),
            })
        }
        Some("generic-http-search-v1") => {
            reject_unknown_keys(provider, &["type", "baseUrl", "tokenEnv"])?;
            let base_url = provider
                .get("baseUrl")
                .and_then(Value::as_str)
                .ok_or_else(invalid_config)?;
            let base_url = validate_provider_base_url(base_url)?;
            let token_env = valid_env_name(provider.get("tokenEnv").and_then(Value::as_str))?;
            let token = read_credential(read_env, token_env)?;
            Ok(ProviderConfig::GenericHttpSearchV1 { base_url, token })
        }
        _ => Err(invalid_config()),
    }
}

fn validate_provider_shape(provider: &Map<String, Value>) -> Result<bool, ConfigurationError> {
    match provider.get("type").and_then(Value::as_str) {
        Some("mem0-platform-v3") => {
            reject_unknown_keys(provider, &["type", "apiKeyEnv", "appId"])?;
            valid_env_name(provider.get("apiKeyEnv").and_then(Value::as_str))?;
            let app_id = provider
                .get("appId")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty() && utf16_len(value) <= 256)
                .ok_or_else(invalid_config)?;
            let _ = app_id;
            Ok(true)
        }
        Some("generic-http-search-v1") => {
            reject_unknown_keys(provider, &["type", "baseUrl", "tokenEnv"])?;
            let base_url = provider
                .get("baseUrl")
                .and_then(Value::as_str)
                .ok_or_else(invalid_config)?;
            Url::parse(base_url).map_err(|_| invalid_config())?;
            valid_env_name(provider.get("tokenEnv").and_then(Value::as_str))?;
            Ok(false)
        }
        _ => Err(invalid_config()),
    }
}

fn validate_provider_base_url(value: &str) -> Result<Url, ConfigurationError> {
    let url =
        Url::parse(value).map_err(|_| ConfigurationError("Provider URL is invalid.".into()))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigurationError(
            "Provider URL must not contain credentials, path, query, or fragment.".into(),
        ));
    }
    if url.scheme() == "https" {
        return Ok(url);
    }
    let host = url.host_str().unwrap_or_default();
    if url.scheme() == "http" && matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]") {
        return Ok(url);
    }
    Err(ConfigurationError(
        "Provider URL must use HTTPS or loopback HTTP.".into(),
    ))
}

fn valid_env_name(value: Option<&str>) -> Result<&str, ConfigurationError> {
    let value = value.ok_or_else(invalid_config)?;
    let mut bytes = value.bytes();
    let valid_first = bytes
        .next()
        .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic());
    if !valid_first || !bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric()) {
        return Err(invalid_config());
    }
    Ok(value)
}

fn read_credential(
    read_env: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<String, ConfigurationError> {
    read_env(name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ConfigurationError("Configured external context credential is unavailable.".into())
        })
}

fn optional_bounded_integer(
    object: &Map<String, Value>,
    key: &str,
    default: u64,
    minimum: u64,
    maximum: u64,
) -> Result<u64, ConfigurationError> {
    let Some(value) = object.get(key) else {
        return Ok(default);
    };
    let value = value
        .as_f64()
        .filter(|value| value.is_finite() && value.fract() == 0.0)
        .ok_or_else(invalid_config)?;
    if value < minimum as f64 || value > maximum as f64 {
        return Err(invalid_config());
    }
    Ok(value as u64)
}

fn reject_unknown_keys(
    object: &Map<String, Value>,
    allowed: &[&str],
) -> Result<(), ConfigurationError> {
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid_config());
    }
    Ok(())
}

fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

fn invalid_config() -> ConfigurationError {
    ConfigurationError("External context config is invalid.".into())
}

fn invalid_repository_root() -> ConfigurationError {
    ConfigurationError("External context repository root is invalid.".into())
}
