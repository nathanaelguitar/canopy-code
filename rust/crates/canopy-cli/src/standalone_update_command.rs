// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Verified update and staged replacement for managed standalone installs.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use flate2::read::GzDecoder;
use ring::signature::{ED25519, UnparsedPublicKey};
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::timeout;
use uuid::Uuid;
use zip::ZipArchive;

const OSS_BASE: &str = "https://qwen-code-assets.oss-cn-hangzhou.aliyuncs.com/releases/qwen-code";
const GITHUB_BASE: &str = "https://github.com/QwenLM/canopy-code/releases/download";
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const ARCHIVE_TIMEOUT: Duration = Duration::from_secs(300);
const SMOKE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_DOWNLOAD_BYTES: u64 = 512 * 1024 * 1024;
const MAX_EXTRACTED_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_CHECKSUMS_BYTES: usize = 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const MAX_ARCHIVE_ENTRIES: usize = 100_000;
const MAX_ARCHIVE_PATH_BYTES: usize = 64 * 1024 * 1024;
const MAX_SMOKE_OUTPUT_BYTES: usize = 64 * 1024;
const PACKAGE_NAME: &str = "@canopy-code/canopy-code";
// Release builds must embed the production Ed25519 SPKI DER public key through
// CANOPY_RELEASE_PUBLIC_KEY_DER_B64. Development builds without one fail
// closed instead of trusting an unsigned or test-key-signed archive.
const RELEASE_PUBLIC_KEY_DER_B64: Option<&str> = option_env!("CANOPY_RELEASE_PUBLIC_KEY_DER_B64");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StandaloneUpdateResult {
    Done,
    Deferred,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    name: Option<String>,
    target: Option<String>,
    version: Option<String>,
}

/// Finds a managed Node standalone install containing the currently running
/// executable. Native Rust installs do not match this layout and are left to
/// package-manager guidance until native release artifacts are defined.
pub(super) fn find_current_standalone_install() -> Option<(PathBuf, String)> {
    let executable = std::env::current_exe().ok()?.canonicalize().ok()?;
    let current_target = detect_target().ok()?;
    let mut candidate = executable.parent();

    while let Some(directory) = candidate {
        let manifest_path = directory.join("manifest.json");
        if let Ok(bytes) = read_limited_file(&manifest_path, MAX_MANIFEST_BYTES, "manifest.json") {
            if let Ok(manifest) = serde_json::from_slice::<Manifest>(&bytes) {
                let Some(version) = manifest.version.as_deref() else {
                    candidate = directory.parent();
                    continue;
                };
                if manifest.name.as_deref() == Some(PACKAGE_NAME)
                    && manifest.target.as_deref() == Some(current_target.as_str())
                    && normalize_version(version).is_ok()
                    && standalone_runtime_is_valid(directory, &current_target)
                    && bundled_node_path(directory, &current_target)
                        .canonicalize()
                        .ok()
                        .is_some_and(|node| node == executable)
                {
                    let normalized = normalize_version(version).ok()?;
                    return Some((
                        directory.to_path_buf(),
                        normalized.trim_start_matches('v').to_owned(),
                    ));
                }
            }
        }
        candidate = directory.parent();
    }
    None
}

fn standalone_runtime_is_valid(directory: &Path, target: &str) -> bool {
    let windows = target.starts_with("win-");
    let node = bundled_node_path(directory, target);
    let launcher = if windows {
        directory.join("bin").join("canopy.cmd")
    } else {
        directory.join("bin").join("canopy")
    };
    let cli = directory.join("lib").join("cli.js");

    is_regular_runtime_file(&node, windows)
        && is_regular_runtime_file(&launcher, windows)
        && cli.is_file()
}

fn bundled_node_path(directory: &Path, target: &str) -> PathBuf {
    if target.starts_with("win-") {
        directory.join("node").join("node.exe")
    } else {
        directory.join("node").join("bin").join("node")
    }
}

fn is_regular_runtime_file(path: &Path, windows: bool) -> bool {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return false;
    }
    #[cfg(unix)]
    {
        windows || metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        let _ = windows;
        true
    }
}

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

pub(super) async fn perform_standalone_update(
    standalone_dir: &Path,
    new_version: &str,
) -> Result<StandaloneUpdateResult, String> {
    let version_path = normalize_version(new_version)?;
    let target = detect_target()?;
    let manifest_path = standalone_dir.join("manifest.json");
    let manifest_bytes = read_limited_file(&manifest_path, MAX_MANIFEST_BYTES, "manifest.json")?;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| format!("Invalid standalone manifest: {error}"))?;
    if manifest.name.as_deref() != Some(PACKAGE_NAME) {
        return Err(format!(
            "{} is not a Canopy Code standalone install",
            standalone_dir.display()
        ));
    }
    let installed_target = manifest.target.as_deref().unwrap_or(&target);
    validate_target(installed_target)?;
    if installed_target != target {
        return Err(format!(
            "Standalone target {installed_target} does not match this platform ({target})"
        ));
    }
    let installed_version = manifest
        .version
        .as_deref()
        .ok_or_else(|| "Standalone manifest is missing its version".to_owned())?;
    let installed_version = normalize_version(installed_version)?;
    let requested_version = Version::parse(version_path.trim_start_matches('v'))
        .map_err(|error| format!("Invalid update version: {error}"))?;
    let installed_semver = Version::parse(installed_version.trim_start_matches('v'))
        .map_err(|error| format!("Invalid installed version: {error}"))?;
    if requested_version <= installed_semver {
        return Err(format!(
            "Refusing standalone update to non-newer version {version_path} from {installed_version}",
            version_path = version_path.trim_start_matches('v')
        ));
    }

    let filename = archive_filename(&target);
    let parent = standalone_dir
        .parent()
        .ok_or_else(|| "Standalone installation has no parent directory".to_owned())?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("Could not create update directory: {error}"))?;

    let lock_path = parent.join(".canopy-update.lock");
    let mut lock = UpdateLock::acquire(&lock_path, standalone_dir)?;
    let temp_dir = TemporaryDirectory::create(&std::env::temp_dir(), "canopy-code-update-")?;
    let extract_dir = TemporaryDirectory::create(parent, ".canopy-code-update-")?;

    let result = async {
        let archive_path = temp_dir.path.join(&filename);
        let archive_hash = download_archive(&version_path, &filename, &archive_path).await?;
        let checksums = download_small_file(&version_path, "SHA256SUMS", FETCH_TIMEOUT).await?;
        verify_checksums(&version_path, &checksums, &archive_hash, &filename).await?;

        extract_archive(&archive_path, &extract_dir.path, &target)?;
        let new_install = extract_dir.path.join("canopy-code");
        validate_new_install(&new_install, &target, &version_path)?;
        smoke_test(&new_install, &target, &version_path).await?;

        let swap = atomic_replace(standalone_dir, &new_install, &lock_path, &version_path)?;
        if swap == StandaloneUpdateResult::Done {
            write_rollback_metadata(standalone_dir, &version_path);
        }
        Ok(swap)
    }
    .await;

    match result {
        Ok(StandaloneUpdateResult::Deferred) => {
            lock.retain_for_deferred_swap();
            Ok(StandaloneUpdateResult::Deferred)
        }
        Ok(done) => Ok(done),
        Err(error) => {
            let pending = sibling_with_suffix(standalone_dir, ".new");
            let _ = remove_any(&pending);
            let pending_metadata = sibling_with_suffix(standalone_dir, ".rollback-info.pending");
            let _ = remove_any(&pending_metadata);
            Err(error)
        }
    }
}

/// Restores the preserved `.old` install for `/doctor rollback`.
#[allow(dead_code)]
pub(super) fn rollback_standalone_update(standalone_dir: &Path) -> Result<(), String> {
    let parent = standalone_dir
        .parent()
        .ok_or_else(|| "Standalone installation has no parent directory".to_owned())?;
    let lock_path = parent.join(".canopy-update.lock");
    match fs::read_to_string(&lock_path) {
        Ok(contents) => {
            let pid = contents
                .trim()
                .parse::<u32>()
                .map_err(|_| "Could not determine whether an update is active".to_owned())?;
            if process_is_alive(pid) {
                return Err(
                    "An auto-update is currently in progress. Wait for it to finish before rolling back."
                        .to_owned(),
                );
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("Could not inspect update lock: {error}")),
    }

    let deferred = sibling_with_suffix(standalone_dir, ".deferred");
    if let Ok(contents) = fs::read_to_string(&deferred) {
        if contents
            .trim()
            .parse::<u32>()
            .ok()
            .is_some_and(process_is_alive)
        {
            return Err(
                "A deferred update is still being applied. Wait for it to finish before rolling back."
                    .to_owned(),
            );
        }
    }

    let old_dir = sibling_with_suffix(standalone_dir, ".old");
    if !old_dir.exists() {
        return Err(format!("{} does not exist", old_dir.display()));
    }
    let old_manifest: Manifest = serde_json::from_slice(
        &read_limited_file(
            &old_dir.join("manifest.json"),
            MAX_MANIFEST_BYTES,
            "preserved manifest.json",
        )
        .map_err(|_| {
            format!(
                "{}/manifest.json missing — .old may be corrupt",
                old_dir.display()
            )
        })?,
    )
    .map_err(|error| format!("The preserved standalone manifest is invalid: {error}"))?;
    if old_manifest.name.as_deref() != Some(PACKAGE_NAME) {
        return Err("The preserved directory is not a Canopy Code standalone install".to_owned());
    }
    let expected_target = detect_target()?;
    if old_manifest.target.as_deref() != Some(expected_target.as_str()) {
        return Err("The preserved install targets a different platform".to_owned());
    }

    let failed_dir = sibling_with_suffix(standalone_dir, ".failed");
    remove_any(&failed_dir)
        .map_err(|error| format!("Could not clear previous failed install: {error}"))?;
    fs::rename(standalone_dir, &failed_dir)
        .map_err(|error| format!("Could not move current install aside: {error}"))?;
    if let Err(error) = fs::rename(&old_dir, standalone_dir) {
        if !standalone_dir.exists() && failed_dir.exists() {
            if fs::rename(&failed_dir, standalone_dir).is_ok() {
                return Err(format!(
                    "Rollback failed; the current installation was restored automatically: {error}"
                ));
            }
        }
        return Err(format!(
            "Rollback failed: {error}. Manual recovery: mv \"{}\" \"{}\"",
            old_dir.display(),
            standalone_dir.display()
        ));
    }
    let _ = remove_any(&failed_dir);
    Ok(())
}

fn normalize_version(version: &str) -> Result<String, String> {
    let normalized = version.strip_prefix('v').unwrap_or(version);
    let mut pieces = normalized.splitn(2, '-');
    let core = pieces.next().unwrap_or_default();
    let core_parts: Vec<_> = core.split('.').collect();
    let prerelease = pieces.next();
    if core_parts.len() != 3
        || core_parts
            .iter()
            .any(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
        || prerelease.is_some_and(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.')
        })
        || Version::parse(normalized).is_err()
    {
        return Err(format!("Invalid version format: {version}"));
    }
    Ok(format!("v{normalized}"))
}

fn read_limited_file(path: &Path, limit: usize, label: &str) -> Result<Vec<u8>, String> {
    let file = File::open(path).map_err(|error| format!("Could not read {label}: {error}"))?;
    let read_limit = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    let mut bytes = Vec::with_capacity(limit.min(8192));
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("Could not read {label}: {error}"))?;
    if bytes.len() > limit {
        return Err(format!("{label} exceeds the {limit} byte limit"));
    }
    Ok(bytes)
}

fn detect_target() -> Result<String, String> {
    let target = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => "darwin-arm64",
        ("macos", "x86_64") => "darwin-x64",
        ("linux", "aarch64") => "linux-arm64",
        ("linux", "x86_64") => "linux-x64",
        ("windows", _) => "win-x64",
        _ => {
            return Err(format!(
                "Unsupported platform: {}-{}",
                std::env::consts::OS,
                std::env::consts::ARCH
            ));
        }
    };
    Ok(target.to_owned())
}

fn validate_target(target: &str) -> Result<(), String> {
    match target {
        "darwin-arm64" | "darwin-x64" | "linux-arm64" | "linux-x64" | "win-x64" => Ok(()),
        _ => Err(format!("Unknown standalone target: {target}")),
    }
}

fn archive_filename(target: &str) -> String {
    if target.starts_with("win-") {
        format!("canopy-code-{target}.zip")
    } else {
        format!("canopy-code-{target}.tar.gz")
    }
}

fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .user_agent(format!("CanopyCode/{}", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|error| format!("Could not initialize update client: {error}"))
}

async fn fetch_response(
    client: &reqwest::Client,
    version: &str,
    filename: &str,
    timeout: Duration,
) -> Result<reqwest::Response, String> {
    let mut failures = Vec::new();
    for base in [OSS_BASE, GITHUB_BASE] {
        let url = format!("{base}/{version}/{filename}");
        match client
            .get(url)
            .timeout(timeout)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
        {
            Ok(response) => return Ok(response),
            Err(error) => failures.push(error.to_string()),
        }
    }
    Err(format!(
        "Failed to download {filename}: {}",
        failures.join("; ")
    ))
}

async fn download_small_file(
    version: &str,
    filename: &str,
    request_timeout: Duration,
) -> Result<Vec<u8>, String> {
    let client = client()?;
    let mut response = fetch_response(&client, version, filename, request_timeout).await?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_CHECKSUMS_BYTES as u64)
    {
        return Err(format!("{filename} exceeds the size limit"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("Could not download {filename}: {error}"))?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_CHECKSUMS_BYTES {
            return Err(format!("{filename} exceeds the size limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn download_archive(
    version: &str,
    filename: &str,
    destination: &Path,
) -> Result<String, String> {
    let client = client()?;
    let mut response = fetch_response(&client, version, filename, ARCHIVE_TIMEOUT).await?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_DOWNLOAD_BYTES)
    {
        return Err(format!(
            "Download too large: exceeds {MAX_DOWNLOAD_BYTES} byte limit"
        ));
    }

    let mut file = tokio::fs::File::create(destination)
        .await
        .map_err(|error| format!("Could not create archive file: {error}"))?;
    let mut hash = Sha256::new();
    let mut total_bytes = 0_u64;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("Could not download archive: {error}"))?
    {
        total_bytes = total_bytes.saturating_add(chunk.len() as u64);
        if total_bytes > MAX_DOWNLOAD_BYTES {
            return Err(format!("Download exceeded {MAX_DOWNLOAD_BYTES} byte limit"));
        }
        hash.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|error| format!("Could not write archive: {error}"))?;
    }
    file.flush()
        .await
        .map_err(|error| format!("Could not flush archive: {error}"))?;
    file.sync_all()
        .await
        .map_err(|error| format!("Could not sync downloaded archive: {error}"))?;
    drop(file);
    if total_bytes == 0 {
        return Err("Empty response body".to_owned());
    }
    if let Some(parent) = destination.parent() {
        sync_directory_for_update(parent)?;
    }
    Ok(format!("{:x}", hash.finalize()))
}

async fn verify_checksums(
    version: &str,
    content: &[u8],
    actual_hash: &str,
    filename: &str,
) -> Result<(), String> {
    let text =
        std::str::from_utf8(content).map_err(|_| "SHA256SUMS is not valid UTF-8".to_owned())?;
    let signature = download_small_file(version, "SHA256SUMS.sig", FETCH_TIMEOUT)
        .await
        .map_err(|error| format!("Signed standalone updates are required; {error}"))?;
    verify_signature(text.as_bytes(), &signature)?;

    let expected = text.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let hash = fields.next()?;
        let name = fields.last()?.trim_start_matches('*');
        (name == filename).then_some(hash)
    });
    let expected =
        expected.ok_or_else(|| format!("No checksum found for {filename} in SHA256SUMS"))?;
    if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("Invalid checksum for {filename} in SHA256SUMS"));
    }
    if !actual_hash.eq_ignore_ascii_case(expected) {
        return Err(format!(
            "Checksum mismatch: expected {expected}, got {actual_hash}"
        ));
    }
    Ok(())
}

fn verify_signature(message: &[u8], signature_base64: &[u8]) -> Result<(), String> {
    let signature_text = std::str::from_utf8(signature_base64)
        .map_err(|_| "SHA256SUMS.sig is not valid UTF-8".to_owned())?
        .trim();
    let signature = BASE64
        .decode(signature_text)
        .map_err(|_| "SHA256SUMS.sig is not valid base64".to_owned())?;
    if signature.len() != 64 {
        return Err(format!(
            "Invalid signature length: expected 64 bytes, got {}",
            signature.len()
        ));
    }
    let public_key = RELEASE_PUBLIC_KEY_DER_B64.ok_or_else(|| {
        "Standalone updater is disabled: this build has no production release verification key"
            .to_owned()
    })?;
    let der = BASE64
        .decode(public_key)
        .map_err(|_| "Embedded release public key is invalid".to_owned())?;
    const ED25519_SPKI_PREFIX: &[u8] = &[
        0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
    ];
    if der.len() != ED25519_SPKI_PREFIX.len() + 32 || !der.starts_with(ED25519_SPKI_PREFIX) {
        return Err("Embedded release public key has invalid Ed25519 DER".to_owned());
    }
    UnparsedPublicKey::new(&ED25519, &der[ED25519_SPKI_PREFIX.len()..])
        .verify(message, &signature)
        .map_err(|_| {
            "SHA256SUMS signature verification failed — possible tampering detected".to_owned()
        })
}

fn extract_archive(archive_path: &Path, destination: &Path, target: &str) -> Result<(), String> {
    fs::create_dir_all(destination)
        .map_err(|error| format!("Could not create extraction directory: {error}"))?;
    if target.starts_with("win-") {
        extract_zip(archive_path, destination)?;
    } else {
        extract_tar_gz(archive_path, destination)?;
    }
    validate_extracted_tree(destination)?;
    Ok(())
}

fn normalized_archive_parts(raw: &str) -> Result<Vec<String>, String> {
    if raw.is_empty()
        || raw.starts_with('/')
        || raw.starts_with('\\')
        || raw.as_bytes().contains(&0)
        || raw.as_bytes().get(1) == Some(&b':')
    {
        return Err(format!("Unsafe archive path: {raw}"));
    }
    let mut parts = Vec::new();
    for part in raw.split(['/', '\\']) {
        match part {
            "" | "." => continue,
            ".." => return Err(format!("Unsafe archive path: {raw}")),
            _ if part.contains(':') || part.contains('*') || part.contains('?') => {
                return Err(format!("Unsafe archive path: {raw}"));
            }
            _ => parts.push(part.to_owned()),
        }
    }
    if parts.first().map(String::as_str) != Some("canopy-code") {
        return Err(format!("Unexpected archive root for entry: {raw}"));
    }
    Ok(parts)
}

fn destination_path(root: &Path, parts: &[String]) -> PathBuf {
    parts
        .iter()
        .fold(root.to_path_buf(), |path, part| path.join(part))
}

fn ensure_no_symlink_parents(root: &Path, destination: &Path) -> Result<(), String> {
    let relative = destination
        .strip_prefix(root)
        .map_err(|_| "Archive destination escaped extraction root".to_owned())?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            return Err("Unsafe archive destination component".to_owned());
        };
        current.push(part);
        if let Ok(metadata) = fs::symlink_metadata(&current) {
            if metadata.file_type().is_symlink() {
                return Err(format!(
                    "Archive path traverses a symlink: {}",
                    current.display()
                ));
            }
        }
    }
    Ok(())
}

fn extract_tar_gz(archive_path: &Path, destination: &Path) -> Result<(), String> {
    let file =
        File::open(archive_path).map_err(|error| format!("Could not open archive: {error}"))?;
    let decoder = GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    let mut total_size = 0_u64;
    let mut entry_count = 0_usize;
    let mut total_path_bytes = 0_usize;
    let mut symlinks = Vec::new();
    let entries = archive
        .entries()
        .map_err(|error| format!("Could not read tar archive: {error}"))?;

    for entry_result in entries {
        entry_count = entry_count.saturating_add(1);
        if entry_count > MAX_ARCHIVE_ENTRIES {
            return Err("Archive exceeds the maximum entry count".to_owned());
        }
        let mut entry = entry_result.map_err(|error| format!("Invalid tar entry: {error}"))?;
        let path_bytes = entry.path_bytes();
        total_path_bytes = total_path_bytes.saturating_add(path_bytes.len());
        if total_path_bytes > MAX_ARCHIVE_PATH_BYTES {
            return Err("Archive paths exceed the cumulative path size limit".to_owned());
        }
        let raw_path = std::str::from_utf8(&path_bytes)
            .map_err(|_| "Tar entry contains a non-UTF-8 path".to_owned())?
            .to_owned();
        let parts = normalized_archive_parts(&raw_path)?;
        let entry_type = entry.header().entry_type();
        let output = destination_path(destination, &parts);

        if entry_type.is_dir() {
            ensure_no_symlink_parents(destination, &output)?;
            fs::create_dir_all(&output)
                .map_err(|error| format!("Could not create archive directory: {error}"))?;
        } else if entry_type.is_file() {
            let size = entry
                .header()
                .size()
                .map_err(|error| format!("Invalid tar file size: {error}"))?;
            total_size = total_size.saturating_add(size);
            if total_size > MAX_EXTRACTED_BYTES {
                return Err("Extracted archive exceeds the size limit".to_owned());
            }
            ensure_no_symlink_parents(destination, &output)?;
            let parent = output
                .parent()
                .ok_or_else(|| "Invalid archive path".to_owned())?;
            fs::create_dir_all(parent)
                .map_err(|error| format!("Could not create archive directory: {error}"))?;
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output)
                .map_err(|error| format!("Could not create extracted file: {error}"))?;
            let mut limited = (&mut entry).take(size.saturating_add(1));
            let copied = io::copy(&mut limited, &mut file)
                .map_err(|error| format!("Could not extract archive file: {error}"))?;
            if copied != size {
                return Err(format!("Truncated archive entry: {raw_path}"));
            }
            #[cfg(unix)]
            if let Ok(mode) = entry.header().mode() {
                let _ = fs::set_permissions(&output, fs::Permissions::from_mode(mode & 0o777));
            }
        } else if entry_type.is_symlink() {
            let link_bytes = entry
                .link_name_bytes()
                .ok_or_else(|| format!("Symlink has no target: {raw_path}"))?;
            let target = std::str::from_utf8(&link_bytes)
                .map_err(|_| format!("Symlink target is not UTF-8: {raw_path}"))?
                .to_owned();
            safe_symlink_target(&parts, &target)?;
            symlinks.push((output, target));
        } else {
            return Err(format!("Unsupported tar entry type: {raw_path}"));
        }
    }

    for (link_path, link_target) in symlinks {
        ensure_no_symlink_parents(destination, &link_path)?;
        let parent = link_path
            .parent()
            .ok_or_else(|| "Invalid symlink path".to_owned())?;
        fs::create_dir_all(parent)
            .map_err(|error| format!("Could not create symlink parent: {error}"))?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&link_target, &link_path)
            .map_err(|error| format!("Could not create archive symlink: {error}"))?;
        #[cfg(not(unix))]
        return Err("Tar symlinks are unsupported on this platform".to_owned());
    }
    Ok(())
}

fn safe_symlink_target(parts: &[String], target: &str) -> Result<(), String> {
    if target.is_empty()
        || target.starts_with('/')
        || target.starts_with('\\')
        || target.as_bytes().get(1) == Some(&b':')
        || target.as_bytes().contains(&0)
    {
        return Err(format!("Unsafe symlink target: {target}"));
    }
    let parent_parts = parts.get(1..parts.len().saturating_sub(1)).unwrap_or(&[]);
    let mut resolved: Vec<String> = parent_parts.to_vec();
    for part in target.split(['/', '\\']) {
        match part {
            "" | "." => {}
            ".." => {
                if resolved.pop().is_none() {
                    return Err(format!("Symlink target escapes canopy-code: {target}"));
                }
            }
            _ if part.contains(':') => return Err(format!("Unsafe symlink target: {target}")),
            _ => resolved.push(part.to_owned()),
        }
    }
    Ok(())
}

fn extract_zip(archive_path: &Path, destination: &Path) -> Result<(), String> {
    let file =
        File::open(archive_path).map_err(|error| format!("Could not open archive: {error}"))?;
    let mut archive =
        ZipArchive::new(file).map_err(|error| format!("Invalid zip archive: {error}"))?;
    if archive.len() > MAX_ARCHIVE_ENTRIES {
        return Err("Archive exceeds the maximum entry count".to_owned());
    }
    let mut total_size = 0_u64;
    let mut total_path_bytes = 0_usize;
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| format!("Invalid zip entry: {error}"))?;
        let raw_path = entry.name().to_owned();
        total_path_bytes = total_path_bytes.saturating_add(raw_path.len());
        if total_path_bytes > MAX_ARCHIVE_PATH_BYTES {
            return Err("Archive paths exceed the cumulative path size limit".to_owned());
        }
        let parts = normalized_archive_parts(&raw_path)?;
        let mode = entry.unix_mode().unwrap_or(0);
        let file_type = mode & 0o170000;
        if file_type != 0 && file_type != 0o100000 && file_type != 0o040000 {
            return Err(format!("Unsupported zip entry type: {raw_path}"));
        }
        let output = destination_path(destination, &parts);
        if entry.is_dir() || file_type == 0o040000 {
            ensure_no_symlink_parents(destination, &output)?;
            fs::create_dir_all(&output)
                .map_err(|error| format!("Could not create archive directory: {error}"))?;
            continue;
        }

        let expected_size = entry.size();
        total_size = total_size.saturating_add(expected_size);
        if total_size > MAX_EXTRACTED_BYTES {
            return Err("Extracted archive exceeds the size limit".to_owned());
        }
        ensure_no_symlink_parents(destination, &output)?;
        let parent = output
            .parent()
            .ok_or_else(|| "Invalid archive path".to_owned())?;
        fs::create_dir_all(parent)
            .map_err(|error| format!("Could not create archive directory: {error}"))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&output)
            .map_err(|error| format!("Could not create extracted file: {error}"))?;
        let mut limited = (&mut entry).take(expected_size.saturating_add(1));
        let copied = io::copy(&mut limited, &mut file)
            .map_err(|error| format!("Could not extract archive file: {error}"))?;
        if copied != expected_size {
            return Err(format!("Truncated archive entry: {raw_path}"));
        }
    }
    Ok(())
}

fn validate_extracted_tree(destination: &Path) -> Result<(), String> {
    let root = destination.join("canopy-code");
    if !root.is_dir() {
        return Err("Extracted archive does not contain expected canopy-code directory".to_owned());
    }
    let resolved_root = root
        .canonicalize()
        .map_err(|error| format!("Could not resolve extracted directory: {error}"))?;
    for entry in walkdir::WalkDir::new(&root).follow_links(false) {
        let entry =
            entry.map_err(|error| format!("Could not inspect extracted archive: {error}"))?;
        if entry.file_type().is_symlink() {
            let resolved = entry
                .path()
                .canonicalize()
                .map_err(|error| format!("Invalid archive symlink: {error}"))?;
            if !resolved.starts_with(&resolved_root) {
                return Err(format!(
                    "Path traversal detected in archive: {}",
                    entry.path().display()
                ));
            }
        }
    }
    Ok(())
}

fn validate_new_install(
    directory: &Path,
    target: &str,
    requested_version: &str,
) -> Result<(), String> {
    let manifest: Manifest = serde_json::from_slice(
        &read_limited_file(
            &directory.join("manifest.json"),
            MAX_MANIFEST_BYTES,
            "Extracted manifest.json",
        )
        .map_err(|_| "Extracted archive has no valid-size manifest.json".to_owned())?,
    )
    .map_err(|error| format!("Extracted archive has an invalid manifest: {error}"))?;
    if manifest.name.as_deref() != Some(PACKAGE_NAME) || manifest.target.as_deref() != Some(target)
    {
        return Err("Extracted archive manifest does not match this standalone target".to_owned());
    }
    let manifest_version = manifest
        .version
        .as_deref()
        .ok_or_else(|| "Extracted archive manifest is missing its version".to_owned())?;
    if normalize_version(manifest_version)? != requested_version {
        return Err(format!(
            "Extracted archive version does not match requested version {}",
            requested_version.trim_start_matches('v')
        ));
    }
    if !standalone_runtime_is_valid(directory, target) {
        return Err("Extracted archive is missing standalone runtime files".to_owned());
    }
    Ok(())
}

async fn smoke_test(directory: &Path, target: &str, requested_version: &str) -> Result<(), String> {
    let node = if target.starts_with("win-") {
        directory.join("node").join("node.exe")
    } else {
        directory.join("node").join("bin").join("node")
    };
    let cli = directory.join("lib").join("cli.js");
    let mut child = Command::new(&node)
        .arg(&cli)
        .arg("--version")
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("Smoke test could not start bundled Node: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Smoke test stdout was not captured".to_owned())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Smoke test stderr was not captured".to_owned())?;
    let stdout_task = tokio::spawn(read_capped_output(stdout));
    let stderr_task = tokio::spawn(read_capped_output(stderr));
    let status = match timeout(SMOKE_TIMEOUT, child.wait()).await {
        Ok(status) => status
            .map_err(|error| format!("Smoke test could not wait for bundled Node: {error}"))?,
        Err(_) => {
            let _ = child.kill().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            return Err("Smoke test timed out".to_owned());
        }
    };
    let stdout = stdout_task
        .await
        .map_err(|error| format!("Smoke test stdout reader failed: {error}"))?
        .map_err(|error| format!("Smoke test stdout read failed: {error}"))?;
    let stderr = stderr_task
        .await
        .map_err(|error| format!("Smoke test stderr reader failed: {error}"))?
        .map_err(|error| format!("Smoke test stderr read failed: {error}"))?;
    let stderr = String::from_utf8_lossy(&stderr).trim().to_owned();
    if !status.success() {
        let detail = if stderr.is_empty() {
            String::new()
        } else {
            format!(": {stderr}")
        };
        return Err(format!(
            "Smoke test failed: new binary exited with code {}{detail}",
            status.code().unwrap_or(1)
        ));
    }
    let version = String::from_utf8_lossy(&stdout).trim().to_owned();
    let normalized = normalize_version(&version)
        .map_err(|_| format!("Smoke test failed: unexpected version output \"{version}\""))?;
    if normalized != requested_version {
        return Err(format!(
            "Smoke test reported version {}, expected {}",
            normalized.trim_start_matches('v'),
            requested_version.trim_start_matches('v')
        ));
    }
    Ok(())
}

async fn read_capped_output<R>(mut reader: R) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::with_capacity(MAX_SMOKE_OUTPUT_BYTES);
    let mut buffer = [0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        let remaining = MAX_SMOKE_OUTPUT_BYTES.saturating_sub(output.len());
        output.extend_from_slice(&buffer[..count.min(remaining)]);
    }
    Ok(output)
}

fn atomic_replace(
    standalone_dir: &Path,
    new_install: &Path,
    _lock_path: &Path,
    _updated_to: &str,
) -> Result<StandaloneUpdateResult, String> {
    let old_dir = sibling_with_suffix(standalone_dir, ".old");
    remove_any(&old_dir).map_err(|error| format!("Could not remove previous rollback: {error}"))?;

    #[cfg(windows)]
    {
        let pending_dir = sibling_with_suffix(standalone_dir, ".new");
        ensure_batch_safe(standalone_dir)?;
        remove_any(&pending_dir)
            .map_err(|error| format!("Could not remove pending update: {error}"))?;
        fs::rename(new_install, &pending_dir)
            .map_err(|error| format!("Could not stage pending update: {error}"))?;
        let rollback_metadata_path = sibling_with_suffix(standalone_dir, ".rollback-info.pending");
        let _ = write_rollback_metadata_from(standalone_dir, &rollback_metadata_path, _updated_to);
        spawn_deferred_swap(
            standalone_dir,
            &old_dir,
            &pending_dir,
            &rollback_metadata_path,
            _lock_path,
        )?;
        return Ok(StandaloneUpdateResult::Deferred);
    }

    #[cfg(not(windows))]
    {
        sync_staged_tree(new_install)?;
        let source_parent = new_install
            .parent()
            .ok_or_else(|| "Staged installation has no parent directory".to_owned())?;
        let install_parent = standalone_dir
            .parent()
            .ok_or_else(|| "Standalone installation has no parent directory".to_owned())?;
        fs::rename(new_install, &old_dir).map_err(|error| {
            format!("Could not stage new installation for atomic exchange: {error}")
        })?;
        if let Err(error) = sync_directory_for_update(source_parent)
            .and_then(|()| sync_directory_for_update(install_parent))
        {
            let cleanup = remove_any(&old_dir);
            let _ = sync_directory_for_update(source_parent);
            let _ = sync_directory_for_update(install_parent);
            return Err(match cleanup {
                Ok(()) => format!(
                    "Could not sync staged directory entries; current install remains active: {error}"
                ),
                Err(cleanup_error) => format!(
                    "Could not sync staged directory entries; current install remains active. Remove the uncommitted candidate at {} manually ({cleanup_error}); sync error: {error}",
                    old_dir.display()
                ),
            });
        }
        if let Err(error) = atomic_exchange_directories(standalone_dir, &old_dir) {
            let cleanup = remove_any(&old_dir);
            let _ = sync_directory_for_update(install_parent);
            let _ = sync_directory_for_update(source_parent);
            return Err(match cleanup {
                Ok(()) => format!(
                    "Filesystem does not support atomic standalone replacement; current install remains active: {error}"
                ),
                Err(cleanup_error) => format!(
                    "Filesystem does not support atomic standalone replacement; current install remains active. Remove the uncommitted candidate at {} manually ({cleanup_error}); exchange error: {error}",
                    old_dir.display()
                ),
            });
        }
        if let Err(error) = sync_directory_for_update(install_parent) {
            return Err(format!(
                "The update is active, but syncing the install directory failed; durability across a sudden shutdown is uncertain. The previous install remains at {} for recovery: {error}",
                old_dir.display()
            ));
        }
        Ok(StandaloneUpdateResult::Done)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn sync_staged_tree(root: &Path) -> Result<(), String> {
    for entry in walkdir::WalkDir::new(root)
        .contents_first(true)
        .follow_links(false)
    {
        let entry = entry.map_err(|error| format!("Could not sync staged tree: {error}"))?;
        let path = entry.path();
        let file_type = entry.file_type();
        if file_type.is_file() {
            File::open(path)
                .and_then(|file| file.sync_all())
                .map_err(|error| {
                    format!("Could not sync staged file {}: {error}", path.display())
                })?;
        } else if file_type.is_dir() {
            sync_directory_for_update(path)?;
        } else if !file_type.is_symlink() {
            return Err(format!("Unsupported staged file type: {}", path.display()));
        }
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn sync_staged_tree(_root: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn sync_directory_for_update(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("Could not sync directory {}: {error}", path.display()))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn sync_directory_for_update(_path: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(all(unix, target_os = "linux"))]
fn atomic_exchange_directories(left: &Path, right: &Path) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::raw::{c_char, c_int, c_uint};
    use std::os::unix::ffi::OsStrExt;

    unsafe extern "C" {
        fn renameat2(
            olddirfd: c_int,
            oldpath: *const c_char,
            newdirfd: c_int,
            newpath: *const c_char,
            flags: c_uint,
        ) -> c_int;
    }

    let left = CString::new(left.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    let right = CString::new(right.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    // Linux AT_FDCWD is -100 and RENAME_EXCHANGE is 2.
    let result = unsafe { renameat2(-100, left.as_ptr(), -100, right.as_ptr(), 2) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(all(unix, target_os = "macos"))]
fn atomic_exchange_directories(left: &Path, right: &Path) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::raw::{c_char, c_int, c_uint};
    use std::os::unix::ffi::OsStrExt;

    unsafe extern "C" {
        fn renamex_np(from: *const c_char, to: *const c_char, flags: c_uint) -> c_int;
    }

    let left = CString::new(left.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    let right = CString::new(right.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    // macOS RENAME_SWAP atomically exchanges two existing paths.
    let result = unsafe { renamex_np(left.as_ptr(), right.as_ptr(), 2) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn atomic_exchange_directories(_left: &Path, _right: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic directory exchange is not implemented for this operating system",
    ))
}

fn write_rollback_metadata(standalone_dir: &Path, updated_to: &str) {
    let old_dir = sibling_with_suffix(standalone_dir, ".old");
    let destination = old_dir.join(".canopy-rollback-info.json");
    let _ = write_rollback_metadata_from(&old_dir, &destination, updated_to);
}

fn write_rollback_metadata_from(
    preserved_dir: &Path,
    destination: &Path,
    updated_to: &str,
) -> Result<(), String> {
    let old_manifest = read_limited_file(
        &preserved_dir.join("manifest.json"),
        MAX_MANIFEST_BYTES,
        "preserved manifest.json",
    )
    .ok()
    .and_then(|bytes| serde_json::from_slice::<Manifest>(&bytes).ok());
    let info = serde_json::json!({
        "preservedVersion": old_manifest.and_then(|manifest| manifest.version).unwrap_or_else(|| "unknown".to_owned()),
        "updatedTo": updated_to,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "reason": "auto-update",
    });
    let bytes = serde_json::to_vec_pretty(&info)
        .map_err(|error| format!("Could not encode rollback metadata: {error}"))?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(destination)
        .map_err(|error| format!("Could not stage rollback metadata: {error}"))?;
    file.write_all(&bytes)
        .map_err(|error| format!("Could not write rollback metadata: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("Could not sync rollback metadata: {error}"))?;
    drop(file);
    if let Some(parent) = destination.parent() {
        sync_directory_for_update(parent)?;
    }
    Ok(())
}

fn sibling_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn remove_any(path: &Path) -> io::Result<()> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn create(parent: &Path, prefix: &str) -> Result<Self, String> {
        fs::create_dir_all(parent)
            .map_err(|error| format!("Could not create temporary directory parent: {error}"))?;
        for _ in 0..10 {
            let path = parent.join(format!("{prefix}{}", Uuid::new_v4().simple()));
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            builder.mode(0o700);
            match builder.create(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(format!("Could not create temporary directory: {error}")),
            }
        }
        Err("Could not allocate a unique temporary directory".to_owned())
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct UpdateLock {
    path: PathBuf,
    retain: bool,
}

impl UpdateLock {
    fn acquire(path: &Path, standalone_dir: &Path) -> Result<Self, String> {
        check_deferred_swap(standalone_dir)?;
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut file) => {
                if let Err(error) = writeln!(file, "{}", std::process::id()) {
                    let _ = fs::remove_file(path);
                    return Err(format!("Could not write update lock: {error}"));
                }
                Ok(Self {
                    path: path.to_path_buf(),
                    retain: false,
                })
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let pid = fs::read_to_string(path)
                    .ok()
                    .and_then(|text| text.trim().parse::<u32>().ok());
                if let Some(pid) = pid.filter(|pid| !process_is_alive(*pid)) {
                    Err(format!(
                        "A stale update lock from process {pid} remains at {}. Verify no updater is running, then remove the lock file and retry.",
                        path.display()
                    ))
                } else {
                    Err("Another update is already in progress".to_owned())
                }
            }
            Err(error) => Err(format!("Could not acquire update lock: {error}")),
        }
    }

    fn retain_for_deferred_swap(&mut self) {
        self.retain = true;
    }
}

impl Drop for UpdateLock {
    fn drop(&mut self) {
        if !self.retain {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn check_deferred_swap(standalone_dir: &Path) -> Result<(), String> {
    let deferred = sibling_with_suffix(standalone_dir, ".deferred");
    if let Ok(text) = fs::read_to_string(&deferred) {
        if text
            .trim()
            .parse::<u32>()
            .ok()
            .is_some_and(process_is_alive)
        {
            return Err(
                "A previous update is still being applied. Please wait a moment and try again."
                    .to_owned(),
            );
        }
        let _ = fs::remove_file(&deferred);
    }
    let pending = sibling_with_suffix(standalone_dir, ".new");
    if pending.exists() {
        return Err(format!(
            "A previous update left a pending swap at {}. Remove the pending swap and .canopy-update.lock after checking for an active updater.",
            pending.display()
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    match kill(Pid::from_raw(pid as i32), None) {
        Ok(()) | Err(Errno::EPERM) => true,
        Err(_) => false,
    }
}

#[cfg(windows)]
fn process_is_alive(pid: u32) -> bool {
    let filter = format!("PID eq {pid}");
    let Ok(output) = std::process::Command::new("tasklist")
        .args(["/FI", filter.as_str(), "/NH"])
        .output()
    else {
        return true;
    };
    String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
}

#[cfg(not(any(unix, windows)))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

#[cfg(windows)]
fn ensure_batch_safe(path: &Path) -> Result<(), String> {
    let value = path.to_string_lossy();
    if value.chars().any(|character| {
        matches!(
            character,
            '&' | '|' | '<' | '>' | '^' | '%' | '!' | '"' | '`' | '\n' | '\r'
        )
    }) {
        return Err(
            "Installation path contains characters unsafe for deferred update script".to_owned(),
        );
    }
    Ok(())
}

#[cfg(windows)]
fn spawn_deferred_swap(
    standalone_dir: &Path,
    old_dir: &Path,
    pending_dir: &Path,
    rollback_metadata_path: &Path,
    lock_path: &Path,
) -> Result<(), String> {
    use std::os::windows::process::CommandExt;

    let parent = standalone_dir
        .parent()
        .ok_or_else(|| "Standalone installation has no parent directory".to_owned())?;
    let marker = sibling_with_suffix(standalone_dir, ".deferred");
    let script_path = parent.join("canopy-update.bat");
    let log_path = parent.join("canopy-update.log");
    for path in [
        old_dir,
        pending_dir,
        rollback_metadata_path,
        lock_path,
        &marker,
        &script_path,
        &log_path,
    ] {
        ensure_batch_safe(path)?;
    }
    let pid = std::process::id();
    let launcher_pid = std::env::var("CANOPY_CODE_LAUNCHER_PID")
        .ok()
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()));
    let mut lines = vec![
        "@echo off".to_owned(),
        "set /a TRIES=0".to_owned(),
        ":wait".to_owned(),
        "set /a TRIES+=1".to_owned(),
        "if %TRIES% GTR 30 goto proceed".to_owned(),
        format!(
            "tasklist /FI \"PID eq {pid}\" 2>nul | find \"{pid}\" >nul && (timeout /t 1 >nul & goto wait)"
        ),
    ];
    if let Some(launcher_pid) = launcher_pid {
        lines.extend([
            "set /a LAUNCHER_TRIES=0".to_owned(),
            ":wait_launcher".to_owned(),
            "set /a LAUNCHER_TRIES+=1".to_owned(),
            "if %LAUNCHER_TRIES% GTR 30 goto proceed".to_owned(),
            format!("tasklist /FI \"PID eq {launcher_pid}\" 2>nul | find \"{launcher_pid}\" >nul && (timeout /t 1 >nul & goto wait_launcher)"),
        ]);
    }
    lines.extend([
        ":proceed".to_owned(),
        format!(
            "echo [%DATE% %TIME%] starting swap >> \"{}\"",
            log_path.display()
        ),
        format!(
            "move /Y \"{}\" \"{}\"",
            standalone_dir.display(),
            old_dir.display()
        ),
        "if errorlevel 1 goto move1_failed".to_owned(),
        format!(
            "move /Y \"{}\" \"{}\"",
            pending_dir.display(),
            standalone_dir.display()
        ),
        "if errorlevel 1 goto move2_failed".to_owned(),
        format!(
            "if exist \"{}\" move /Y \"{}\" \"{}\\.canopy-rollback-info.json\" >nul 2>nul",
            rollback_metadata_path.display(),
            rollback_metadata_path.display(),
            old_dir.display()
        ),
        format!(
            "echo [%DATE% %TIME%] swap completed >> \"{}\"",
            log_path.display()
        ),
        "goto cleanup".to_owned(),
        ":move1_failed".to_owned(),
        format!(
            "echo [%DATE% %TIME%] ERROR: failed to rename install >> \"{}\"",
            log_path.display()
        ),
        "goto cleanup".to_owned(),
        ":move2_failed".to_owned(),
        format!(
            "echo [%DATE% %TIME%] ERROR: failed to promote .new; rolling back >> \"{}\"",
            log_path.display()
        ),
        format!(
            "move /Y \"{}\" \"{}\"",
            old_dir.display(),
            standalone_dir.display()
        ),
        "if errorlevel 1 (".to_owned(),
        format!(
            "echo [%DATE% %TIME%] CRITICAL: rollback failed; manual recovery required >> \"{}\"",
            log_path.display()
        ),
        ") else (".to_owned(),
        format!(
            "echo [%DATE% %TIME%] rollback succeeded >> \"{}\"",
            log_path.display()
        ),
        ")".to_owned(),
        ":cleanup".to_owned(),
        format!("del /F /Q \"{}\" 2>nul", marker.display()),
        format!("del /F /Q \"{}\" 2>nul", lock_path.display()),
        format!("del /F /Q \"{}\" 2>nul", rollback_metadata_path.display()),
        "del \"%~f0\"".to_owned(),
    ]);
    let script = lines.join("\r\n");
    fs::write(&script_path, script)
        .map_err(|error| format!("Could not create deferred update script: {error}"))?;
    fs::write(&marker, pid.to_string())
        .map_err(|error| format!("Could not write deferred update marker: {error}"))?;
    let script_string = script_path.to_string_lossy().into_owned();
    let child = std::process::Command::new("cmd.exe")
        .args(["/C", script_string.as_str()])
        .creation_flags(0x0000_0008 | 0x0800_0000)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("Failed to start deferred update script: {error}"))?;
    let _ = fs::write(&marker, child.id().to_string());
    Ok(())
}
