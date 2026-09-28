// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Native `canopy update` availability check and installation guidance.

use std::path::Path;
use std::time::Duration;

use futures_util::StreamExt;
use semver::Version;
use serde_json::Value;
use tokio::runtime::Builder;

#[path = "standalone_update_command.rs"]
mod standalone_update_command;

const USAGE: &str = "Usage: canopy update";
const PACKAGE_NAME: &str = "@canopy-code/canopy-code";
const REGISTRY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_DIST_TAGS_BYTES: u64 = 128 * 1024;

pub(super) enum StandaloneRollbackOutcome {
    NotStandalone,
    WindowsManual,
    RolledBack,
}

pub(super) fn rollback_current_standalone_install() -> Result<StandaloneRollbackOutcome, String> {
    let Some((standalone_dir, _)) = standalone_update_command::find_current_standalone_install()
    else {
        return Ok(StandaloneRollbackOutcome::NotStandalone);
    };

    #[cfg(windows)]
    {
        let _ = standalone_dir;
        Ok(StandaloneRollbackOutcome::WindowsManual)
    }
    #[cfg(not(windows))]
    {
        standalone_update_command::rollback_standalone_update(&standalone_dir)?;
        Ok(StandaloneRollbackOutcome::RolledBack)
    }
}

pub fn run(args: &[String]) -> Result<(), String> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        println!("{USAGE}");
        println!("Check for Canopy Code updates and print installation instructions.");
        return Ok(());
    }
    if !args.is_empty() {
        return Err(format!("{USAGE}\nupdate accepts no arguments."));
    }

    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Could not start update check runtime: {error}"))?;
    runtime.block_on(run_update())
}

async fn run_update() -> Result<(), String> {
    if std::env::var("DEV").ok().as_deref() == Some("true") {
        return Err("Unable to check for updates: development mode.".to_owned());
    }

    let standalone_install = standalone_update_command::find_current_standalone_install();
    let current_version = standalone_install
        .as_ref()
        .map(|(_, version)| version.as_str())
        .unwrap_or(env!("CARGO_PKG_VERSION"));
    let current = Version::parse(current_version)
        .map_err(|_| "The current Canopy Code version is invalid.".to_owned())?;
    let current_text = current.to_string();
    let tags = fetch_dist_tags().await?;
    let latest = choose_latest(&tags, &current)?;

    if latest <= current {
        println!("Canopy Code {current_text} is up to date!");
        return Ok(());
    }

    println!("Canopy Code update available! {current_text} → {latest}");
    if let Some((standalone_dir, _)) = standalone_install {
        println!("Downloading update...");
        match standalone_update_command::perform_standalone_update(
            &standalone_dir,
            &latest.to_string(),
        )
        .await
        {
            Ok(standalone_update_command::StandaloneUpdateResult::Done) => {
                println!("Update successful! The new version will be used on your next run.");
            }
            Ok(standalone_update_command::StandaloneUpdateResult::Deferred) => {
                println!("Update downloaded. It will be applied after you exit this session.");
            }
            Err(error) => return Err(format!("Update failed: {error}")),
        }
        return Ok(());
    }
    for line in format_installation_instructions(&latest.to_string()) {
        println!("{line}");
    }
    Ok(())
}

async fn fetch_dist_tags() -> Result<Value, String> {
    let registry = std::env::var("npm_config_registry")
        .or_else(|_| std::env::var("NPM_CONFIG_REGISTRY"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "https://registry.npmjs.org".to_owned());
    let url = format!(
        "{}/-/package/@canopy-code%2fcanopy-code/dist-tags",
        registry.trim_end_matches('/')
    );
    let client = reqwest::Client::builder()
        .timeout(REGISTRY_TIMEOUT)
        .user_agent(format!("CanopyCode/{}", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|_| update_check_error("registry error"))?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|error| update_check_error(classify_request_error(&error)))?;
    if !response.status().is_success() {
        return Err(update_check_error("registry error"));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_DIST_TAGS_BYTES)
    {
        return Err(update_check_error("registry error"));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| update_check_error(classify_request_error(&error)))?;
        if bytes.len().saturating_add(chunk.len()) as u64 > MAX_DIST_TAGS_BYTES {
            return Err(update_check_error("registry error"));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| update_check_error("registry error"))
}

fn classify_request_error(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "registry did not respond within 5s"
    } else if error.is_connect() {
        "registry unreachable"
    } else {
        "registry error"
    }
}

fn update_check_error(reason: &str) -> String {
    format!(
        "Failed to check for updates ({reason}). Please check your network or registry configuration."
    )
}

fn choose_latest(tags: &Value, current: &Version) -> Result<Version, String> {
    let parse_tag = |name: &str| -> Result<Option<Version>, String> {
        let Some(value) = tags.get(name).and_then(Value::as_str) else {
            return Ok(None);
        };
        if value.is_empty() {
            return Ok(None);
        }
        Version::parse(value)
            .map(Some)
            .map_err(|_| update_check_error("registry error"))
    };

    let stable = parse_tag("latest")?.unwrap_or_else(|| current.clone());
    if current.to_string().contains("nightly") {
        let nightly = parse_tag("nightly")?.unwrap_or_else(|| current.clone());
        if (stable.major, stable.minor, stable.patch)
            == (nightly.major, nightly.minor, nightly.patch)
        {
            return Ok(nightly);
        }
        return Ok(if stable > nightly { stable } else { nightly });
    }
    Ok(stable)
}

fn format_installation_instructions(latest: &str) -> Vec<String> {
    let executable = std::env::current_exe().ok();
    let cwd = std::env::current_dir().ok();
    let Some(executable) = executable else {
        return vec!["Manual update required. Please reinstall Canopy Code.".to_owned()];
    };
    let normalized = executable.to_string_lossy().replace('\\', "/");

    if is_git_checkout_binary(&executable, cwd.as_deref()) {
        return vec!["Running from a local git clone. Please update with \"git pull\".".to_owned()];
    }
    if normalized.contains("/.npm/_npx/") || normalized.contains("/npm/_npx/") {
        return vec!["Running via npx, update not applicable.".to_owned()];
    }
    if normalized.contains("/.pnpm/_pnpx/") {
        return vec!["Running via pnpx, update not applicable.".to_owned()];
    }
    if normalized.contains("/.bun/install/cache/") {
        return vec!["Running via bunx, update not applicable.".to_owned()];
    }
    if normalized.contains("/Cellar/canopy-code/") {
        return vec!["Installed via Homebrew. Please update with \"brew upgrade\".".to_owned()];
    }
    if let Some(manager) = global_package_manager(&normalized) {
        let command = manager.update_command(latest);
        return vec![
            "Run the following to update:".to_owned(),
            format!("  {command}"),
        ];
    }
    if cwd
        .as_deref()
        .is_some_and(|root| executable.starts_with(root.join("node_modules")))
    {
        return vec![
            "Locally installed. Please update via your project's package.json.".to_owned(),
        ];
    }
    let command = format!("npm install -g {PACKAGE_NAME}@{}", update_tag(latest));
    vec![
        "Run the following to update:".to_owned(),
        format!("  {command}"),
    ]
}

fn is_git_checkout_binary(executable: &Path, cwd: Option<&Path>) -> bool {
    let Some(cwd) = cwd else {
        return false;
    };
    executable.starts_with(cwd)
        && !executable.to_string_lossy().contains("/node_modules/")
        && cwd.join(".git").exists()
}

#[derive(Clone, Copy)]
enum PackageManager {
    Pnpm,
    Yarn,
    Bun,
}

impl PackageManager {
    fn update_command(self, latest: &str) -> String {
        let package = format!("{PACKAGE_NAME}@{}", update_tag(latest));
        match self {
            Self::Pnpm => format!("pnpm add -g {package}"),
            Self::Yarn => format!("yarn global add {package}"),
            Self::Bun => format!("bun add -g {package}"),
        }
    }
}

fn global_package_manager(path: &str) -> Option<PackageManager> {
    if path.contains("/.pnpm/global/") {
        Some(PackageManager::Pnpm)
    } else if path.contains("/.yarn/global/") {
        Some(PackageManager::Yarn)
    } else if path.contains("/.bun/bin/") {
        Some(PackageManager::Bun)
    } else {
        None
    }
}

fn update_tag(version: &str) -> &str {
    if version.contains("nightly") {
        "nightly"
    } else {
        version
    }
}
