// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Native extension update command.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use base64::Engine;
use canopy_core::config::{LoadSettingsOptions, load_settings};
use canopy_core::extension_activation::{ExtensionIdentity, ExtensionStoreSnapshot};
use canopy_core::extension_activation_store::{
    ExtensionArtifactOperation, commit_extension_artifact,
};
use canopy_core::extension_install_source::parse_github_repo_for_releases;
use canopy_core::extension_inventory::{
    InstalledExtensionListOptions, InstalledLocalExtension, load_installed_local_extensions,
};
use canopy_core::extension_network_policy::ExtensionNetworkPolicy;
use canopy_core::storage::Storage;
use canopy_core::utils::terminal_safe::strip_terminal_control_sequences;
use futures_util::StreamExt;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderValue, USER_AGENT};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command as TokioCommand;
use tokio::runtime::Builder;

use crate::extensions_install_command::{
    InstallMetadata, PreparedUpdateArtifact, ensure_public_git_supported,
    prepare_extension_update_artifact, public_git_config_arguments, public_git_environment,
    resolve_extension_network_target,
};

const USAGE: &str = "Usage: canopy extensions update <name> | --all";
const MAX_STORE_STATE_BYTES: u64 = 1024 * 1024;
const MAX_INSTALL_METADATA_BYTES: u64 = 64 * 1024;
const SOURCE_PROBE_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_SOURCE_PROBE_BYTES: usize = 10 * 1024 * 1024;

#[derive(Clone, Debug)]
struct UpdateArgs {
    name: Option<String>,
}

#[derive(Clone, Debug)]
struct UpdateInfo {
    name: String,
    original_version: String,
    updated_version: String,
    warnings: Vec<(String, String)>,
}

struct UpdateContext {
    workspace: PathBuf,
    home: PathBuf,
    extensions_dir: PathBuf,
    store_dir: PathBuf,
}

pub fn run(args: &[String]) -> Result<(), String> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        println!("{USAGE}");
        return Ok(());
    }
    let parsed = parse_args(args)?;
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Could not start extension update runtime: {error}"))?;
    runtime.block_on(update(parsed))
}

fn parse_args(args: &[String]) -> Result<UpdateArgs, String> {
    let mut name = None;
    let mut all = false;
    for argument in args {
        match argument.as_str() {
            "--all" | "--all=true" => all = true,
            "--all=false" => all = false,
            value if value.starts_with('-') => {
                return Err(format!("{USAGE}\nUnknown option: {value}"));
            }
            value => {
                if name.replace(value.to_owned()).is_some() {
                    return Err(format!("{USAGE}\nExpected one extension name."));
                }
            }
        }
    }
    if name.is_some() && all {
        return Err(format!(
            "{USAGE}\nThe extension name and --all cannot be used together."
        ));
    }
    if name.is_none() && !all {
        return Err(format!(
            "{USAGE}\nEither an extension name or --all must be provided."
        ));
    }
    Ok(UpdateArgs { name })
}

async fn update(args: UpdateArgs) -> Result<(), String> {
    let context = load_context()?;
    let inventory = load_inventory(&context);
    refuse_incomplete_inventory(&inventory.diagnostics)?;
    for diagnostic in &inventory.diagnostics {
        eprintln!("[CANOPY] {}", sanitize(diagnostic));
    }

    if let Some(name) = args.name {
        let Some(extension) = inventory
            .extensions
            .iter()
            .find(|extension| extension.name == name)
        else {
            println!("Extension \"{}\" not found.", sanitize(&name));
            return Ok(());
        };
        let Some(metadata) = read_metadata(extension)? else {
            println!(
                "Unable to install extension \"{}\" due to missing install metadata",
                sanitize(&name)
            );
            return Ok(());
        };
        if !is_updateable(&metadata) {
            println!("Extension \"{}\" is already up to date.", sanitize(&name));
            return Ok(());
        }
        let known_identities = identities(&inventory.extensions);
        match update_one(extension, &context, &known_identities).await {
            Ok(Some(info)) => print_named_result(&info),
            Ok(None) => println!("Extension \"{}\" is already up to date.", sanitize(&name)),
            Err(error) => eprintln!("{}", sanitize(&error)),
        }
        return Ok(());
    }

    let known_identities = identities(&inventory.extensions);
    let mut updated = Vec::new();
    for extension in &inventory.extensions {
        match read_metadata(extension) {
            Ok(Some(metadata)) if is_updateable(&metadata) => {}
            Ok(_) => continue,
            Err(error) => {
                eprintln!("{}", sanitize(&error));
                continue;
            }
        }
        match update_one(extension, &context, &known_identities).await {
            Ok(Some(info)) => updated.push(info),
            Ok(None) => {}
            Err(error) => eprintln!("{}", sanitize(&error)),
        }
    }

    let changed_versions = updated
        .into_iter()
        .filter(|info| info.original_version != info.updated_version)
        .collect::<Vec<_>>();
    if changed_versions.is_empty() {
        println!("No extensions to update.");
        return Ok(());
    }
    for info in &changed_versions {
        println!(
            "Extension \"{}\" successfully updated: {} → {}.",
            sanitize(&info.name),
            sanitize(&info.original_version),
            sanitize(&info.updated_version)
        );
    }
    for info in &changed_versions {
        print_warnings(info);
    }
    Ok(())
}

fn load_context() -> Result<UpdateContext, String> {
    let workspace = std::env::current_dir()
        .map_err(|error| format!("could not determine current directory: {error}"))?;
    let home =
        home_directory().ok_or_else(|| "could not determine the user home directory".to_owned())?;
    let settings = load_settings(workspace.clone(), &mut LoadSettingsOptions::default())
        .map_err(|error| error.to_string())?;
    if !settings.is_trusted {
        return Err(format!(
            "Could not update extensions from untrusted folder at {}",
            workspace.display()
        ));
    }
    Ok(UpdateContext {
        workspace,
        home,
        extensions_dir: Storage::get_user_extensions_dir(),
        store_dir: Storage::get_global_canopy_dir().join("extension-store"),
    })
}

fn load_inventory(
    context: &UpdateContext,
) -> canopy_core::extension_inventory::InstalledExtensionList {
    load_installed_local_extensions(InstalledExtensionListOptions {
        workspace_root: &context.workspace,
        user_home: &context.home,
        user_extensions_dir: &context.extensions_dir,
        extension_store_dir: &context.store_dir,
    })
}

fn refuse_incomplete_inventory(diagnostics: &[String]) -> Result<(), String> {
    if diagnostics.iter().any(|diagnostic| {
        diagnostic.starts_with("More than 128 installed extensions")
            || diagnostic.starts_with("Extension directory contains more than 256 entries")
    }) {
        return Err(
            "The installed-extension scan was truncated; refusing to update from an incomplete inventory."
                .to_owned(),
        );
    }
    Ok(())
}

fn identities(extensions: &[InstalledLocalExtension]) -> Vec<ExtensionIdentity> {
    extensions
        .iter()
        .map(|extension| ExtensionIdentity {
            id: extension.id.clone(),
            name: extension.name.clone(),
        })
        .collect()
}

fn read_metadata(extension: &InstalledLocalExtension) -> Result<Option<InstallMetadata>, String> {
    let path = extension
        .install_slot
        .join(".canopy-extension-install.json");
    let file_metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "Could not read extension install metadata: {error}"
            ));
        }
    };
    if !file_metadata.is_file() || file_metadata.file_type().is_symlink() {
        return Ok(None);
    }
    if file_metadata.len() > MAX_INSTALL_METADATA_BYTES {
        return Err("Extension install metadata exceeds the 64 KiB limit.".to_owned());
    }
    let bytes = fs::read(&path)
        .map_err(|error| format!("Could not read extension install metadata: {error}"))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| format!("Could not parse extension install metadata: {error}"))
}

fn is_updateable(metadata: &InstallMetadata) -> bool {
    if metadata.external_content == Some(true)
        || (metadata.external_content.is_none()
            && metadata.origin_source.as_deref() == Some("Claude")
            && metadata.plugin_name.is_some())
    {
        return false;
    }
    match metadata.install_type.as_str() {
        "local" => !metadata.source.starts_with("upload:"),
        "archive-url" | "npm" | "github-release" => true,
        "git" => {
            !(metadata.git_commit.is_none()
                && matches!(metadata.origin_source.as_deref(), Some("Claude" | "Qoder")))
        }
        _ => false,
    }
}

async fn update_one(
    extension: &InstalledLocalExtension,
    context: &UpdateContext,
    known_identities: &[ExtensionIdentity],
) -> Result<Option<UpdateInfo>, String> {
    // Probe refs and registry metadata before downloading and converting an
    // artifact. A failed or inconclusive probe falls through to the existing
    // full preparation path, which still validates stable identity before a
    // transaction can commit.
    if let Ok(Some(metadata)) = read_metadata(extension)
        && is_updateable(&metadata)
        && probe_update_availability(extension, &metadata).await == Some(false)
    {
        return Ok(None);
    }

    let mut prepared = prepare_extension_update_artifact(
        extension,
        &context.workspace,
        &context.home,
        &context.extensions_dir,
        &context.store_dir,
    )
    .await?;

    let update_available = match update_is_available(extension, &prepared) {
        Ok(update_available) => update_available,
        Err(error) => {
            discard_prepared(&prepared);
            return Err(error);
        }
    };
    if !update_available {
        discard_prepared(&prepared);
        return Ok(None);
    }

    let expected_generation =
        match expected_artifact_generation(&context.store_dir, &prepared.identity.id) {
            Ok(generation) => generation,
            Err(error) => {
                discard_prepared(&prepared);
                return Err(error);
            }
        };
    let destination = context.extensions_dir.join(&prepared.identity.name);
    if let Err(error) = commit_extension_artifact(
        &context.extensions_dir,
        &context.store_dir,
        ExtensionArtifactOperation::Update,
        &prepared.identity,
        &destination,
        Some(&prepared.staging_directory),
        None,
        expected_generation,
        known_identities,
    ) {
        let cleanup_error = remove_staging(&prepared.staging_directory).err();
        cleanup_source_paths(&prepared.cleanup_paths, false);
        return Err(match cleanup_error {
            Some(cleanup_error) => {
                format!("{error}; staging cleanup also failed: {cleanup_error}")
            }
            None => error,
        });
    }

    let mut warnings = std::mem::take(&mut prepared.warnings);
    warnings.extend(cleanup_source_paths(&prepared.cleanup_paths, true));
    let refreshed = load_inventory(context);
    let refreshed_extension = refreshed.extensions.iter().find(|candidate| {
        candidate.id == prepared.identity.id && candidate.name == prepared.identity.name
    });
    let updated_version = match refreshed_extension {
        Some(extension) if !extension.version.is_empty() => extension.version.clone(),
        _ => {
            let error = refreshed.diagnostics.first().cloned().unwrap_or_else(|| {
                "updated extension was not found in the refreshed inventory".to_owned()
            });
            warnings.push(("extension_inventory_refresh_failed".to_owned(), error));
            prepared.updated_version.clone()
        }
    };
    Ok(Some(UpdateInfo {
        name: prepared.identity.name,
        original_version: prepared.original_version,
        updated_version,
        warnings,
    }))
}

fn update_is_available(
    extension: &InstalledLocalExtension,
    prepared: &PreparedUpdateArtifact,
) -> Result<bool, String> {
    let previous = &prepared.previous_metadata;
    let next = &prepared.install_metadata;
    if previous.install_type != next.install_type {
        if next.install_type == "git" {
            let current = current_git_commit(&extension.install_slot)?;
            return Ok(next.git_commit.as_deref() != Some(current.as_str()));
        }
        return Ok(true);
    }
    let available = match previous.install_type.as_str() {
        "local" | "archive-url" => prepared.original_version != prepared.updated_version,
        "npm" => previous.release_tag != next.release_tag,
        "github-release" => previous.release_tag != next.release_tag,
        "git" => {
            let old_commit = match previous.git_commit.as_deref() {
                Some(commit) => commit.to_owned(),
                None => current_git_commit(&extension.install_slot)?,
            };
            next.git_commit.as_deref() != Some(old_commit.as_str())
        }
        _ => return Err("Extension source type cannot be updated.".to_owned()),
    };
    Ok(available)
}

/// Returns `Some(true)` when a source probe sees an update, `Some(false)` when
/// it can prove the installed source is current, or `None` when acquisition
/// must decide. Probe errors deliberately fall through to acquisition.
async fn probe_update_availability(
    extension: &InstalledLocalExtension,
    metadata: &InstallMetadata,
) -> Option<bool> {
    match metadata.install_type.as_str() {
        "git" => probe_git_update(extension, metadata).await,
        "github-release" => probe_github_release_update(metadata).await,
        "npm" => probe_npm_update(metadata).await,
        // Local trees and archive URLs have no persisted revision, digest, or
        // HTTP validator to compare. They still need acquisition and
        // conversion before their version can be checked.
        _ => None,
    }
}

async fn probe_git_update(
    extension: &InstalledLocalExtension,
    metadata: &InstallMetadata,
) -> Option<bool> {
    let current_commit = match metadata.git_commit.as_deref() {
        Some(commit) if !commit.is_empty() => commit.to_owned(),
        _ => current_git_commit(&extension.install_slot).ok()?,
    };
    let reference = metadata.source_ref.as_deref().unwrap_or("HEAD");
    let peeled_reference = format!("{reference}^{{}}");
    let output = git_ls_remote(
        &metadata.source,
        reference,
        &peeled_reference,
        metadata.network_policy,
    )
    .await?;
    let remote_commit = output
        .lines()
        .find(|line| {
            line.split('\t')
                .nth(1)
                .is_some_and(|name| name.ends_with("^{}"))
        })
        .or_else(|| output.lines().next())?
        .split('\t')
        .next()?;
    if !matches!(remote_commit.len(), 40 | 64)
        || !remote_commit.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(remote_commit != current_commit)
}

async fn git_ls_remote(
    source: &str,
    reference: &str,
    peeled_reference: &str,
    network_policy: Option<ExtensionNetworkPolicy>,
) -> Option<String> {
    tokio::time::timeout(SOURCE_PROBE_TIMEOUT, async {
        let pin = if network_policy == Some(ExtensionNetworkPolicy::Public) {
            ensure_public_git_supported().ok()?;
            Some(
                resolve_extension_network_target(source, network_policy)
                    .await
                    .ok()?
                    .pin?,
            )
        } else {
            None
        };
        let mut command = TokioCommand::new("git");
        if let Some(pin) = &pin {
            command.env_clear().envs(public_git_environment());
            command.args(public_git_config_arguments(pin));
        }
        command
            .arg("ls-remote")
            .arg("--")
            .arg(source)
            .arg(reference)
            .arg(peeled_reference)
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Ok(token) = std::env::var("GITHUB_TOKEN")
            && !token.is_empty()
        {
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{token}"));
            command
                .env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", "http.https://github.com/.extraheader")
                .env(
                    "GIT_CONFIG_VALUE_0",
                    format!("AUTHORIZATION: basic {encoded}"),
                );
        }
        let mut child = command.spawn().ok()?;
        let stdout = child.stdout.take()?;
        let mut bounded_stdout = stdout.take((MAX_SOURCE_PROBE_BYTES + 1) as u64);
        let mut output = Vec::new();
        bounded_stdout.read_to_end(&mut output).await.ok()?;
        if output.len() > MAX_SOURCE_PROBE_BYTES {
            return None;
        }
        let status = child.wait().await.ok()?;
        if !status.success() {
            return None;
        }
        String::from_utf8(output).ok()
    })
    .await
    .ok()?
}

#[derive(Deserialize)]
struct GithubReleaseProbe {
    tag_name: String,
}

async fn probe_github_release_update(metadata: &InstallMetadata) -> Option<bool> {
    let installed_tag = metadata.release_tag.as_deref()?;
    let repository = parse_github_repo_for_releases(&metadata.source).ok()?;
    let mut endpoint = Url::parse("https://api.github.com/").ok()?;
    {
        let mut path = endpoint.path_segments_mut().ok()?;
        path.pop_if_empty()
            .push("repos")
            .push(&repository.owner)
            .push(&repository.repo)
            .push("releases");
        match metadata.source_ref.as_deref() {
            Some(reference) => {
                path.push("tags").push(reference);
            }
            None => {
                // Match TypeScript's checkForExtensionUpdate, which probes
                // `/latest` even for sources installed with prereleases on.
                path.push("latest");
            }
        }
    }
    let body = get_source_probe_body(
        endpoint.as_str(),
        github_probe_headers().ok()?,
        metadata.network_policy,
    )
    .await?;
    let latest_tag = serde_json::from_slice::<GithubReleaseProbe>(&body)
        .ok()?
        .tag_name;
    Some(latest_tag != installed_tag)
}

fn github_probe_headers() -> Result<reqwest::header::HeaderMap, String> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/vnd.github+json"),
    );
    if let Ok(token) = std::env::var("GITHUB_TOKEN")
        && !token.is_empty()
    {
        let value = HeaderValue::from_str(&format!("token {token}"))
            .map_err(|_| "GITHUB_TOKEN contains invalid header characters.".to_owned())?;
        headers.insert(AUTHORIZATION, value);
    }
    Ok(headers)
}

#[derive(Deserialize)]
struct NpmUpdateProbe {
    #[serde(rename = "dist-tags", default)]
    dist_tags: std::collections::HashMap<String, String>,
}

async fn probe_npm_update(metadata: &InstallMetadata) -> Option<bool> {
    let installed_tag = metadata.release_tag.as_deref()?;
    let (package, requested_version) = parse_scoped_npm_source(&metadata.source)?;
    let configured_registry = resolve_update_npm_registry(&package);
    let registry = Url::parse(
        metadata
            .registry_url
            .as_deref()
            .unwrap_or(&configured_registry),
    )
    .ok()?;
    if registry.scheme() != "https"
        || !registry.username().is_empty()
        || registry.password().is_some()
    {
        return None;
    }
    let mut metadata_url = registry;
    metadata_url
        .path_segments_mut()
        .ok()?
        .pop_if_empty()
        .push(&package);
    let headers = npm_probe_auth_headers(
        metadata
            .registry_url
            .as_deref()
            .unwrap_or(&configured_registry),
        &configured_registry,
    )?;
    let body =
        get_source_probe_body(metadata_url.as_str(), headers, metadata.network_policy).await?;
    let packument = serde_json::from_slice::<NpmUpdateProbe>(&body).ok()?;
    if let Some(requested) = requested_version.as_deref()
        && requested != "latest"
        && !packument.dist_tags.contains_key(requested)
    {
        // This matches checkNpmUpdate: an exact pinned version without a
        // matching dist-tag is considered current without a package download.
        return Some(false);
    }
    let target_tag = requested_version
        .as_deref()
        .filter(|requested| packument.dist_tags.contains_key(*requested))
        .unwrap_or("latest");
    let latest_version = packument.dist_tags.get(target_tag)?;
    Some(latest_version != installed_tag)
}

fn resolve_update_npm_registry(package: &str) -> String {
    let scope = package
        .strip_prefix('@')
        .and_then(|package| package.split('/').next())
        .unwrap_or_default();
    let mut scoped_registry = None;
    let mut default_registry = None;
    for path in [
        std::env::current_dir().unwrap_or_default().join(".npmrc"),
        home_directory().unwrap_or_default().join(".npmrc"),
    ] {
        let Ok(content) = fs::read_to_string(path) else {
            continue;
        };
        for line in content.lines().map(str::trim) {
            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim();
                let value = value.trim().trim_end_matches('/');
                if key == format!("{scope}:registry") && scoped_registry.is_none() {
                    scoped_registry = Some(value.to_owned());
                }
                if key == "registry" && default_registry.is_none() {
                    default_registry = Some(value.to_owned());
                }
            }
        }
    }
    scoped_registry
        .or(default_registry)
        .unwrap_or_else(|| "https://registry.npmjs.org".to_owned())
}

fn npm_probe_auth_headers(
    registry: &str,
    configured_registry: &str,
) -> Option<reqwest::header::HeaderMap> {
    let mut headers = reqwest::header::HeaderMap::new();
    let Some(token) = resolve_update_npm_auth_token(registry, configured_registry) else {
        return Some(headers);
    };
    let value = HeaderValue::from_str(&format!("Bearer {token}")).ok()?;
    headers.insert(AUTHORIZATION, value);
    Some(headers)
}

fn resolve_update_npm_auth_token(registry: &str, configured_registry: &str) -> Option<String> {
    let registry_url = Url::parse(registry).ok()?;
    if let Ok(token) = std::env::var("NPM_TOKEN")
        && !token.is_empty()
        && Url::parse(configured_registry)
            .ok()
            .is_some_and(|configured| configured.origin() == registry_url.origin())
    {
        return Some(token);
    }

    let authority = registry_url
        .port()
        .map(|port| format!("{}:{port}", registry_url.host_str().unwrap_or_default()))
        .unwrap_or_else(|| registry_url.host_str().unwrap_or_default().to_owned());
    let mut prefixes = Vec::new();
    let mut path_segments = registry_url
        .path()
        .trim_end_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    loop {
        let path = path_segments.join("/");
        prefixes.push(if path.is_empty() {
            authority.clone()
        } else {
            format!("{authority}/{path}")
        });
        if path_segments.pop().is_none() {
            break;
        }
    }
    for path in [
        std::env::current_dir().unwrap_or_default().join(".npmrc"),
        home_directory().unwrap_or_default().join(".npmrc"),
    ] {
        let Ok(contents) = fs::read_to_string(path) else {
            continue;
        };
        for line in contents.lines().map(str::trim) {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let Some(prefix) = key
                .trim()
                .strip_prefix("//")
                .and_then(|key| key.strip_suffix(":_authToken"))
            else {
                continue;
            };
            if prefixes
                .iter()
                .any(|candidate| candidate == prefix.trim_end_matches('/'))
            {
                let token = value.trim();
                if !token.is_empty() {
                    return Some(token.to_owned());
                }
            }
        }
    }
    None
}

fn parse_scoped_npm_source(source: &str) -> Option<(String, Option<String>)> {
    let rest = source.strip_prefix('@')?;
    let (scope, package_and_version) = rest.split_once('/')?;
    let (package, version) = match package_and_version.split_once('@') {
        Some((package, version)) if !version.is_empty() => (package, Some(version.to_owned())),
        Some(_) => return None,
        None => (package_and_version, None),
    };
    if scope.is_empty() || package.is_empty() {
        return None;
    }
    Some((format!("@{scope}/{package}"), version))
}

async fn get_source_probe_body(
    url: &str,
    mut headers: reqwest::header::HeaderMap,
    network_policy: Option<ExtensionNetworkPolicy>,
) -> Option<Vec<u8>> {
    headers.insert(USER_AGENT, HeaderValue::from_static("canopy-code"));
    tokio::time::timeout(SOURCE_PROBE_TIMEOUT, async {
        let target = resolve_extension_network_target(url, network_policy)
            .await
            .ok()?;
        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(SOURCE_PROBE_TIMEOUT);
        if let Some(pin) = &target.pin {
            builder = pin.apply_to_client_builder(builder.no_proxy());
        }
        let client = builder.build().ok()?;
        let response = client.get(target.url).headers(headers).send().await.ok()?;
        if response.status() != StatusCode::OK
            || response
                .content_length()
                .is_some_and(|length| length > MAX_SOURCE_PROBE_BYTES as u64)
        {
            return None;
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.ok()?;
            if body.len().saturating_add(chunk.len()) > MAX_SOURCE_PROBE_BYTES {
                return None;
            }
            body.extend_from_slice(&chunk);
        }
        Some(body)
    })
    .await
    .ok()?
}

fn current_git_commit(extension_slot: &Path) -> Result<String, String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(extension_slot)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .map_err(|error| format!("Could not inspect installed Git extension: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "Could not inspect installed Git extension: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|error| error.to_string())
}

fn expected_artifact_generation(store_dir: &Path, identity: &str) -> Result<Option<u64>, String> {
    let path = store_dir.join("state.json");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Some(0)),
        Err(error) => return Err(format!("Could not read extension store state: {error}")),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err("Extension store state is not a regular file.".to_owned());
    }
    if metadata.len() > MAX_STORE_STATE_BYTES {
        return Err("Extension store state exceeds the 1 MiB limit.".to_owned());
    }
    let bytes = fs::read(&path)
        .map_err(|error| format!("Could not read extension store state: {error}"))?;
    let snapshot: ExtensionStoreSnapshot = serde_json::from_slice(&bytes)
        .map_err(|error| format!("Could not parse extension store state: {error}"))?;
    Ok(Some(
        snapshot
            .extensions
            .get(identity)
            .and_then(|policy| policy.artifact_generation)
            .unwrap_or(0),
    ))
}

fn discard_prepared(prepared: &PreparedUpdateArtifact) {
    let _ = remove_staging(&prepared.staging_directory);
    cleanup_source_paths(&prepared.cleanup_paths, false);
}

fn remove_staging(path: &Path) -> Result<(), String> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("Could not remove staged extension: {error}")),
    }
}

fn cleanup_source_paths(paths: &[PathBuf], report_warnings: bool) -> Vec<(String, String)> {
    let mut warnings = Vec::new();
    for path in paths {
        let result = fs::remove_dir_all(path);
        let Err(error) = result else {
            continue;
        };
        if error.kind() == io::ErrorKind::NotFound {
            continue;
        }
        if report_warnings {
            warnings.push((
                "extension_temp_cleanup_failed".to_owned(),
                format!(
                    "Could not remove temporary update directory {}: {error}",
                    path.display()
                ),
            ));
        }
    }
    warnings
}

fn print_named_result(info: &UpdateInfo) {
    if info.original_version != info.updated_version {
        println!(
            "Extension \"{}\" successfully updated: {} → {}.",
            sanitize(&info.name),
            sanitize(&info.original_version),
            sanitize(&info.updated_version)
        );
    } else {
        println!(
            "Extension \"{}\" is already up to date.",
            sanitize(&info.name)
        );
    }
    print_warnings(info);
}

fn print_warnings(info: &UpdateInfo) {
    for (code, error) in &info.warnings {
        eprintln!(
            "Extension \"{}\" updated with warning {}: {}",
            sanitize(&info.name),
            sanitize(code),
            sanitize(error)
        );
    }
}

fn sanitize(value: &str) -> String {
    strip_terminal_control_sequences(value)
}

fn home_directory() -> Option<PathBuf> {
    std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            ["HOME", "USERPROFILE"]
                .into_iter()
                .filter_map(std::env::var_os)
                .find(|value| !value.is_empty())
                .map(PathBuf::from)
        })
        .or_else(|| {
            let drive = std::env::var_os("HOMEDRIVE")?;
            let path = std::env::var_os("HOMEPATH")?;
            Some(PathBuf::from(drive).join(path))
        })
}
