//! Download and install the pinned `cua-driver-rs` release.
//!
//! This is a Rust port of `packages/core/src/tools/computer-use/downloader.ts`.
//! It keeps the source order (download-host override, OSS mirror, GitHub),
//! verifies `checksums.txt` before extraction, streams the asset to disk while
//! hashing, and publishes the extracted directory through a staging rename.
//! The checksum is fetched from the same source order as the asset, so it
//! protects against truncation and corruption in transit, not a compromised
//! source. macOS quarantine removal and LaunchServices registration are
//! best-effort, matching the TypeScript implementation. One platform gap is
//! that extraction uses native `tar`/PowerShell rather than Node's `tar`
//! package; archive edge-case handling and error text can therefore differ.

use super::constants::{
    AssetTarget, CUA_DRIVER_VERSION, binary_path, resolve_asset_target, resolve_asset_urls,
    resolve_checksum_urls, version_dir,
};
use reqwest::{Client, Response};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::ffi::OsString;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const HEADER_TIMEOUT: Duration = Duration::from_secs(30);
const BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_CHECKSUM_BODY_BYTES: usize = 1024 * 1024;
const DOWNLOAD_HOST_ENV: &str = "CANOPY_COMPUTER_USE_DOWNLOAD_HOST";
const LSREGISTER: &str = "/System/Library/Frameworks/CoreServices.framework/Versions/A/Frameworks/LaunchServices.framework/Versions/A/Support/lsregister";

/// Injectable archive extraction boundary. The production implementation uses
/// the host's `tar`/PowerShell tools; tests can materialize a tiny fake driver.
pub trait ArchiveExtractor: Send + Sync {
    fn extract<'a>(
        &'a self,
        archive_path: &'a Path,
        destination: &'a Path,
        asset: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

/// OS-tools based archive extractor. macOS and Linux use `tar`; Windows tries
/// bsdtar, GNU tar with `--force-local`, then PowerShell `Expand-Archive`.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemArchiveExtractor;

impl ArchiveExtractor for SystemArchiveExtractor {
    fn extract<'a>(
        &'a self,
        archive_path: &'a Path,
        destination: &'a Path,
        asset: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move { extract_archive(archive_path, destination, asset).await })
    }
}

/// Options for downloading and installing the Computer Use driver.
pub type InstallProgress = Arc<dyn Fn(&str) + Send + Sync>;

pub struct InstallOptions {
    pub home: PathBuf,
    pub platform: String,
    pub arch: String,
    pub version: String,
    pub download_host: Option<String>,
    pub client: Client,
    pub extractor: Arc<dyn ArchiveExtractor>,
    /// Bootstrap status callback, e.g. "Downloading…" or fallback notice.
    pub on_progress: Option<InstallProgress>,
}

impl InstallOptions {
    /// Build options for the current host, honoring the source download-host
    /// override environment variable.
    pub fn for_host(home: impl Into<PathBuf>) -> Result<Self, String> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .map_err(|error| format!("Computer Use: could not create HTTP client: {error}"))?;
        Ok(Self {
            home: home.into(),
            platform: host_platform().to_owned(),
            arch: host_arch().to_owned(),
            version: CUA_DRIVER_VERSION.to_owned(),
            download_host: std::env::var(DOWNLOAD_HOST_ENV).ok(),
            client,
            extractor: Arc::new(SystemArchiveExtractor),
            on_progress: None,
        })
    }

    pub fn with_extractor(mut self, extractor: Arc<dyn ArchiveExtractor>) -> Self {
        self.extractor = extractor;
        self
    }

    pub fn with_progress(mut self, callback: Arc<dyn Fn(&str) + Send + Sync>) -> Self {
        self.on_progress = Some(callback);
        self
    }
}

/// Parse sha256sum-format lines into a filename-to-lowercase-SHA256 map.
/// Malformed lines and comments are ignored, and later duplicate filenames
/// replace earlier entries, matching JavaScript `Map.set` behavior.
pub fn parse_checksums(body: &str) -> HashMap<String, String> {
    let mut checksums = HashMap::new();
    for line in body.lines() {
        let Some((hash, filename)) = parse_checksum_line(line) else {
            continue;
        };
        checksums.insert(filename, hash);
    }
    checksums
}

fn parse_checksum_line(line: &str) -> Option<(String, String)> {
    let line = line.trim();
    let bytes = line.as_bytes();
    let hash_bytes = bytes.get(..64)?;
    if !hash_bytes.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let mut rest = line.get(64..)?;
    let whitespace_len = rest
        .char_indices()
        .take_while(|(_, character)| character.is_whitespace())
        .map(|(index, character)| index + character.len_utf8())
        .last()?;
    rest = &rest[whitespace_len..];
    if let Some(filename) = rest.strip_prefix('*') {
        rest = filename;
    }
    let filename = rest.trim();
    if filename.is_empty() {
        return None;
    }
    Some((line[..64].to_ascii_lowercase(), filename.to_owned()))
}

/// Return the installed driver path if it currently names a regular file.
pub async fn find_installed(
    home: impl AsRef<Path>,
    platform: &str,
    arch: &str,
    version: &str,
) -> Option<PathBuf> {
    let path = binary_path(home, platform, arch, version).ok()?;
    tokio::fs::metadata(&path)
        .await
        .ok()
        .filter(|metadata| metadata.is_file())
        .map(|_| path)
}

/// Ensure the pinned driver is installed, downloading, verifying, and
/// extracting it when absent. Existing binaries short-circuit without network
/// access. The install is idempotent and returns the spawnable binary path.
pub async fn ensure_installed(options: &InstallOptions) -> Result<PathBuf, String> {
    if let Some(path) = find_installed(
        &options.home,
        &options.platform,
        &options.arch,
        &options.version,
    )
    .await
    {
        return Ok(path);
    }

    let target = resolve_asset_target(&options.platform, &options.arch, &options.version)?;
    validate_asset_filename(&target.asset)?;
    progress(
        options,
        "Downloading Computer Use driver (~20MB, one time)...",
    );

    // Fetch expected digest from the first reachable checksum source.
    let checksum_urls = resolve_checksum_urls(options.download_host.as_deref(), &options.version);
    let (_, checksum_response) = fetch_first(&checksum_urls, &options.client, None).await?;
    let checksum_body = read_checksum_body(checksum_response).await?;
    let checksums = parse_checksums(&checksum_body);
    let expected_sha = checksums
        .get(&target.asset)
        .ok_or_else(|| format!("Computer Use: {} missing from checksums.txt.", target.asset))?;

    // Fetch the archive from the first reachable source and hash it as each
    // response chunk is streamed to the extension-preserving temporary file.
    let asset_urls = resolve_asset_urls(
        &target.asset,
        options.download_host.as_deref(),
        &options.version,
    );
    let (_, asset_response) =
        fetch_first(&asset_urls, &options.client, options.on_progress.as_deref()).await?;
    let temp_dir = computer_use_temp_dir(&options.home);
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .map_err(|error| format!("Computer Use: cannot create download directory: {error}"))?;
    let temp_file = temp_dir.join(&target.asset);
    let actual_sha = match stream_archive(asset_response, &temp_file).await {
        Ok(hash) => hash,
        Err(error) => {
            let _ = tokio::fs::remove_file(&temp_file).await;
            return Err(error);
        }
    };
    if actual_sha != *expected_sha {
        let _ = tokio::fs::remove_file(&temp_file).await;
        return Err(format!(
            "Computer Use: checksum mismatch for {} (expected {}…, got {}…).",
            target.asset,
            &expected_sha[..expected_sha.len().min(12)],
            &actual_sha[..actual_sha.len().min(12)]
        ));
    }

    // Extract into a sibling staging directory. Check that the expected file
    // exists before replacing a prior installation, then rename the complete
    // tree into place.
    let install_dir = version_dir(&options.home, &options.version);
    let staging_dir = append_suffix(&install_dir, ".staging");
    if let Err(error) = remove_path_if_exists(&staging_dir).await {
        return Err(format!(
            "Computer Use: cannot clear staging directory: {error}"
        ));
    }
    tokio::fs::create_dir_all(&staging_dir)
        .await
        .map_err(|error| format!("Computer Use: cannot create staging directory: {error}"))?;
    options
        .extractor
        .extract(&temp_file, &staging_dir, &target.asset)
        .await
        .map_err(|error| format!("Computer Use: failed to extract {}: {error}", target.asset))?;
    let staged_binary = staged_binary_path(&staging_dir, &target);
    validate_extracted_binary(&staging_dir, &staged_binary).await?;
    tokio::fs::remove_file(&temp_file)
        .await
        .map_err(|error| format!("Computer Use: cannot remove verified archive: {error}"))?;

    if options.platform != "win32" {
        set_executable(&staged_binary)?;
    }
    if let Err(error) = remove_path_if_exists(&install_dir).await {
        return Err(format!(
            "Computer Use: cannot replace prior installation: {error}"
        ));
    }
    tokio::fs::rename(&staging_dir, &install_dir)
        .await
        .map_err(|error| format!("Computer Use: cannot publish driver installation: {error}"))?;

    if options.platform == "darwin" && target.has_app {
        let extract_root = extract_root(&install_dir, &target);
        let app_dir = extract_root.join("CuaDriver.app");
        strip_quarantine(&extract_root).await;
        register_launch_services(&app_dir).await;
    }

    progress(options, "Computer Use driver ready.");
    binary_path(
        &options.home,
        &options.platform,
        &options.arch,
        &options.version,
    )
}

fn progress(options: &InstallOptions, message: &str) {
    if let Some(callback) = &options.on_progress {
        callback(message);
    }
}

fn validate_asset_filename(asset: &str) -> Result<(), String> {
    let path = Path::new(asset);
    if asset.is_empty()
        || path.file_name().and_then(|name| name.to_str()) != Some(asset)
        || asset == "."
        || asset == ".."
        || asset.contains('/')
        || asset.contains('\\')
    {
        return Err("Computer Use: invalid release asset filename.".to_owned());
    }
    Ok(())
}

async fn fetch_first(
    urls: &[String],
    client: &Client,
    on_progress: Option<&(dyn Fn(&str) + Send + Sync)>,
) -> Result<(String, Response), String> {
    let mut last_error = "no download URLs were configured".to_owned();
    for url in urls {
        let request = client.get(url).send();
        let response = match tokio::time::timeout(HEADER_TIMEOUT, request).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                last_error = error.to_string();
                if let Some(callback) = on_progress {
                    callback("Source unreachable, trying fallback…");
                }
                continue;
            }
            Err(_) => {
                last_error = format!("headers timeout after 30s for {url}");
                if let Some(callback) = on_progress {
                    callback("Source unreachable, trying fallback…");
                }
                continue;
            }
        };
        if response.status().is_success() {
            return Ok((url.clone(), response));
        }
        last_error = format!("HTTP {} for {url}", response.status());
        // Dropping a non-success response cancels its body, as in the source.
        drop(response);
    }
    Err(format!(
        "Computer Use: all download sources failed. Last error: {last_error}"
    ))
}

async fn read_checksum_body(mut response: Response) -> Result<String, String> {
    let mut body = Vec::new();
    loop {
        let next = tokio::time::timeout(BODY_IDLE_TIMEOUT, response.chunk())
            .await
            .map_err(|_| "Computer Use: checksum download stalled: no data for 60s".to_owned())?
            .map_err(|error| format!("Computer Use: failed to read checksums.txt: {error}"))?;
        let Some(chunk) = next else { break };
        if body.len().saturating_add(chunk.len()) > MAX_CHECKSUM_BODY_BYTES {
            return Err("Computer Use: checksums.txt exceeds 1 MiB.".to_owned());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

async fn stream_archive(mut response: Response, path: &Path) -> Result<String, String> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await
        .map_err(|error| format!("Computer Use: cannot open temporary archive: {error}"))?;
    let result = async {
        let mut hash = Sha256::new();
        loop {
            let next = tokio::time::timeout(BODY_IDLE_TIMEOUT, response.chunk())
                .await
                .map_err(|_| "Computer Use: download stalled: no data for 60s".to_owned())?
                .map_err(|error| format!("Computer Use: download failed: {error}"))?;
            let Some(chunk) = next else { break };
            hash.update(&chunk);
            file.write_all(&chunk)
                .await
                .map_err(|error| format!("Computer Use: cannot write archive: {error}"))?;
        }
        file.flush()
            .await
            .map_err(|error| format!("Computer Use: cannot flush archive: {error}"))?;
        Ok(format!("{:x}", hash.finalize()))
    }
    .await;
    drop(file);
    result
}

async fn extract_archive(archive: &Path, destination: &Path, asset: &str) -> Result<(), String> {
    if asset.ends_with(".zip") {
        extract_zip_windows(archive, destination).await
    } else {
        let output = run_process(
            "tar",
            &[
                OsString::from("-xzf"),
                archive.as_os_str().to_owned(),
                OsString::from("-C"),
                destination.as_os_str().to_owned(),
            ],
            Duration::from_secs(180),
        )
        .await?;
        if output.success {
            Ok(())
        } else {
            Err(format!("tar extraction failed: {}", output.message))
        }
    }
}

async fn extract_zip_windows(archive: &Path, destination: &Path) -> Result<(), String> {
    let attempts = zip_extraction_attempts(archive, destination);
    let mut errors = Vec::with_capacity(attempts.len());
    for (program, args) in attempts {
        match run_process(&program, &args, Duration::from_secs(180)).await {
            Ok(output) if output.success => return Ok(()),
            Ok(output) => errors.push(format!(
                "{program} {}: {}",
                args[0].to_string_lossy(),
                output.message
            )),
            Err(error) => errors.push(format!("{program} {}: {error}", args[0].to_string_lossy())),
        }
    }
    Err(format!(
        "Computer Use: failed to unzip {} on Windows ({}).",
        archive.display(),
        errors.join("; ")
    ))
}

fn zip_extraction_attempts(archive: &Path, destination: &Path) -> Vec<(String, Vec<OsString>)> {
    let archive = archive.as_os_str().to_owned();
    let destination = destination.as_os_str().to_owned();
    let powershell = format!(
        "Expand-Archive -LiteralPath {} -DestinationPath {} -Force",
        powershell_single_quote(&archive.to_string_lossy()),
        powershell_single_quote(&destination.to_string_lossy())
    );
    vec![
        (
            "tar".to_owned(),
            vec![
                "-xf".into(),
                archive.clone(),
                "-C".into(),
                destination.clone(),
            ],
        ),
        (
            "tar".to_owned(),
            vec![
                "--force-local".into(),
                "-xf".into(),
                archive,
                "-C".into(),
                destination,
            ],
        ),
        (
            "powershell".to_owned(),
            vec![
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-Command".into(),
                powershell.into(),
            ],
        ),
    ]
}

fn powershell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

struct ProcessOutput {
    success: bool,
    message: String,
}

async fn run_process(
    program: &str,
    args: &[OsString],
    timeout: Duration,
) -> Result<ProcessOutput, String> {
    let command = Command::new(program).args(args).kill_on_drop(true).output();
    let output = tokio::time::timeout(timeout, command)
        .await
        .map_err(|_| format!("process timed out after {}s", timeout.as_secs()))?
        .map_err(|error| error.to_string())?;
    let message = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    Ok(ProcessOutput {
        success: output.status.success(),
        message: if message.is_empty() {
            format!("exit status {}", output.status)
        } else {
            message
        },
    })
}

fn computer_use_temp_dir(home: &Path) -> PathBuf {
    let encoded = home.to_string_lossy();
    // The source uses only the first four UTF-8 bytes of `home`; that makes
    // ordinary sibling temp homes collide. Hash the complete path so separate
    // accounts/worktrees cannot overwrite each other's in-flight archives.
    let prefix = format!("{:x}", Sha256::digest(encoded.as_bytes()));
    std::env::temp_dir()
        .join("canopy-computer-use-dl")
        .join(prefix)
}

fn staged_binary_path(staging_dir: &Path, target: &AssetTarget) -> PathBuf {
    let mut root = extract_root(staging_dir, target);
    root.push(&target.binary_rel_path);
    root
}

fn extract_root(install_dir: &Path, target: &AssetTarget) -> PathBuf {
    if target.extract_dir == "." {
        install_dir.to_owned()
    } else {
        install_dir.join(&target.extract_dir)
    }
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

async fn remove_path_if_exists(path: &Path) -> std::io::Result<()> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.is_dir() => tokio::fs::remove_dir_all(path).await,
        Ok(_) => tokio::fs::remove_file(path).await,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

async fn validate_extracted_binary(staging_dir: &Path, binary: &Path) -> Result<(), String> {
    let canonical_root = tokio::fs::canonicalize(staging_dir)
        .await
        .map_err(|error| format!("Computer Use: cannot resolve staging directory: {error}"))?;
    let canonical_binary = tokio::fs::canonicalize(binary)
        .await
        .map_err(|error| format!("Computer Use: extracted driver is missing: {error}"))?;
    if !canonical_binary.starts_with(&canonical_root) {
        return Err(
            "Computer Use: extracted driver resolves outside staging directory.".to_owned(),
        );
    }
    let metadata = tokio::fs::symlink_metadata(binary)
        .await
        .map_err(|error| format!("Computer Use: cannot inspect extracted driver: {error}"))?;
    if !metadata.file_type().is_file() {
        return Err("Computer Use: extracted driver is not a regular file.".to_owned());
    }

    // Reject symlinked parent components too, even if they happen to resolve
    // inside staging. This keeps chmod and later driver launch on the extracted
    // tree itself instead of following archive-controlled links.
    let relative = binary
        .strip_prefix(staging_dir)
        .map_err(|_| "Computer Use: extracted driver path escaped staging.".to_owned())?;
    let mut current = staging_dir.to_owned();
    for component in relative.components() {
        current.push(component.as_os_str());
        let metadata = tokio::fs::symlink_metadata(&current)
            .await
            .map_err(|error| format!("Computer Use: cannot inspect extracted path: {error}"))?;
        if metadata.file_type().is_symlink() {
            return Err("Computer Use: extracted driver path contains a symlink.".to_owned());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = std::fs::metadata(path)
        .map_err(|error| format!("Computer Use: cannot inspect extracted driver: {error}"))?;
    let mut permissions = metadata.permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions)
        .map_err(|error| format!("Computer Use: cannot make driver executable: {error}"))
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<(), String> {
    // The executable bit has no meaning on Windows, matching Node's chmod
    // skip for a win32 target.
    Ok(())
}

async fn strip_quarantine(path: &Path) {
    let _ = run_process(
        "xattr",
        &[
            "-dr".into(),
            "com.apple.quarantine".into(),
            path.as_os_str().to_owned(),
        ],
        Duration::from_secs(10),
    )
    .await;
}

async fn register_launch_services(app_path: &Path) {
    let _ = run_process(
        LSREGISTER,
        &["-f".into(), app_path.as_os_str().to_owned()],
        Duration::from_secs(15),
    )
    .await;
}

fn host_platform() -> &'static str {
    if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "windows") {
        "win32"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else {
        std::env::consts::OS
    }
}

fn host_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        arch => arch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use uuid::Uuid;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("canopy-cua-downloader-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&path).expect("make test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct FakeExtractor {
        calls: Mutex<Vec<String>>,
        payload: Vec<u8>,
    }

    impl ArchiveExtractor for FakeExtractor {
        fn extract<'a>(
            &'a self,
            _archive_path: &'a Path,
            destination: &'a Path,
            asset: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("calls lock")
                    .push(asset.to_owned());
                let target = resolve_asset_target("linux", "x64", CUA_DRIVER_VERSION)?;
                let path = staged_binary_path(destination, &target);
                tokio::fs::create_dir_all(path.parent().expect("binary parent"))
                    .await
                    .map_err(|error| error.to_string())?;
                tokio::fs::write(path, &self.payload)
                    .await
                    .map_err(|error| error.to_string())
            })
        }
    }

    async fn serve_responses(
        responses: Vec<(u16, Vec<u8>)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local server");
        let address = listener.local_addr().expect("local server address");
        let task = tokio::spawn(async move {
            let mut paths = Vec::with_capacity(responses.len());
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.expect("accept request");
                let mut request = Vec::new();
                let mut chunk = [0; 1024];
                loop {
                    let read = stream.read(&mut chunk).await.expect("read request");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let first_line = String::from_utf8_lossy(&request)
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .to_owned();
                paths.push(
                    first_line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("")
                        .to_owned(),
                );
                let reason = if (200..300).contains(&status) {
                    "OK"
                } else {
                    "Error"
                };
                let headers = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream
                    .write_all(headers.as_bytes())
                    .await
                    .expect("write response headers");
                stream.write_all(&body).await.expect("write response body");
                stream.shutdown().await.expect("close response");
            }
            paths
        });
        (format!("http://{address}"), task)
    }

    fn client() -> Client {
        Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("create test client")
    }

    fn options(home: &Path, host: String, extractor: Arc<dyn ArchiveExtractor>) -> InstallOptions {
        InstallOptions {
            home: home.to_owned(),
            platform: "linux".to_owned(),
            arch: "x64".to_owned(),
            version: CUA_DRIVER_VERSION.to_owned(),
            download_host: Some(host),
            client: client(),
            extractor,
            on_progress: None,
        }
    }

    #[test]
    fn parses_valid_sha256sum_lines_and_ignores_bad_rows() {
        let map = parse_checksums(&format!(
            "{}  driver.tar.gz\n{} *other.zip\n# ignored\nnot-a-hash file\n",
            "A".repeat(64),
            "b".repeat(64)
        ));
        assert_eq!(map.get("driver.tar.gz"), Some(&"a".repeat(64)));
        assert_eq!(map.get("other.zip"), Some(&"b".repeat(64)));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn later_duplicate_checksum_line_wins_and_filename_is_trimmed() {
        let checksums = parse_checksums(&format!(
            "{}  asset.tar.gz\n{}   * asset.tar.gz  \n",
            "a".repeat(64),
            "c".repeat(64)
        ));
        assert_eq!(checksums.get("asset.tar.gz"), Some(&"c".repeat(64)));
    }

    #[tokio::test]
    async fn find_installed_only_returns_regular_driver_file() {
        let directory = TestDirectory::new();
        let path = binary_path(&directory.0, "linux", "x64", CUA_DRIVER_VERSION).unwrap();
        assert_eq!(
            find_installed(&directory.0, "linux", "x64", CUA_DRIVER_VERSION).await,
            None
        );
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, b"driver").await.unwrap();
        assert_eq!(
            find_installed(&directory.0, "linux", "x64", CUA_DRIVER_VERSION).await,
            Some(path)
        );
    }

    #[tokio::test]
    async fn fetch_first_falls_back_after_non_success_and_preserves_url_order() {
        let (host, server) =
            serve_responses(vec![(503, b"unavailable".to_vec()), (200, b"ok".to_vec())]).await;
        let urls = vec![format!("{host}/first"), format!("{host}/second")];
        let (chosen, response) = fetch_first(&urls, &client(), None).await.unwrap();
        assert_eq!(chosen, urls[1]);
        assert_eq!(response.text().await.unwrap(), "ok");
        assert_eq!(server.await.unwrap(), vec!["/first", "/second"]);
    }

    #[tokio::test]
    async fn streams_verifies_extracts_and_publishes_then_reports_progress() {
        let directory = TestDirectory::new();
        let payload = b"streamed archive bytes".to_vec();
        let asset = format!("cua-driver-rs-{CUA_DRIVER_VERSION}-linux-x86_64-binary.tar.gz");
        let digest = format!("{:x}", Sha256::digest(&payload));
        let checksums = format!("{digest}  {asset}\n").into_bytes();
        let (host, server) = serve_responses(vec![(200, checksums), (200, payload.clone())]).await;
        let extractor = Arc::new(FakeExtractor {
            calls: Mutex::new(Vec::new()),
            payload: payload.clone(),
        });
        let messages = Arc::new(Mutex::new(Vec::new()));
        let seen_messages = Arc::clone(&messages);
        let mut install = options(&directory.0, host, extractor.clone());
        install.on_progress = Some(Arc::new(move |message| {
            seen_messages.lock().unwrap().push(message.to_owned());
        }));

        let installed = ensure_installed(&install).await.unwrap();
        assert_eq!(
            installed,
            binary_path(&directory.0, "linux", "x64", CUA_DRIVER_VERSION).unwrap()
        );
        assert_eq!(tokio::fs::read(&installed).await.unwrap(), payload);
        assert_eq!(
            extractor.calls.lock().unwrap().as_slice(),
            std::slice::from_ref(&asset)
        );
        assert_eq!(server.await.unwrap().len(), 2);
        assert_eq!(
            messages.lock().unwrap().as_slice(),
            [
                "Downloading Computer Use driver (~20MB, one time)...",
                "Computer Use driver ready."
            ]
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(installed).unwrap().permissions().mode() & 0o777,
                0o755
            );
        }
    }

    #[tokio::test]
    async fn checksum_mismatch_does_not_extract_or_publish_partial_driver() {
        let directory = TestDirectory::new();
        let payload = b"tampered archive".to_vec();
        let asset = format!("cua-driver-rs-{CUA_DRIVER_VERSION}-linux-x86_64-binary.tar.gz");
        let checksums = format!("{}  {asset}\n", "0".repeat(64)).into_bytes();
        let (host, server) = serve_responses(vec![(200, checksums), (200, payload)]).await;
        let extractor = Arc::new(FakeExtractor::default());
        let error = ensure_installed(&options(&directory.0, host, extractor.clone()))
            .await
            .unwrap_err();
        assert!(error.contains("checksum mismatch"));
        assert!(extractor.calls.lock().unwrap().is_empty());
        assert_eq!(server.await.unwrap().len(), 2);
        assert!(
            find_installed(&directory.0, "linux", "x64", CUA_DRIVER_VERSION)
                .await
                .is_none()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_an_extracted_binary_symlink_that_resolves_outside_staging() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new();
        let staging = directory.0.join("version.staging");
        let outside = directory.0.join("outside-driver");
        tokio::fs::create_dir_all(&staging).await.unwrap();
        tokio::fs::write(&outside, b"outside").await.unwrap();
        let target = resolve_asset_target("linux", "x64", CUA_DRIVER_VERSION).unwrap();
        symlink(&outside, staged_binary_path(&staging, &target)).unwrap();

        let error = validate_extracted_binary(&staging, &staged_binary_path(&staging, &target))
            .await
            .unwrap_err();
        assert!(error.contains("outside staging"));
    }

    #[test]
    fn windows_zip_fallback_order_and_powershell_quoting_match_source() {
        let attempts = zip_extraction_attempts(
            Path::new("C:\\My O'Reilly\\driver.zip"),
            Path::new("C:\\target dir"),
        );
        assert_eq!(attempts[0].0, "tar");
        assert_eq!(attempts[0].1[0], "-xf");
        assert_eq!(attempts[1].1[0], "--force-local");
        assert_eq!(attempts[2].0, "powershell");
        let command = attempts[2].1[3].to_string_lossy();
        assert!(command.contains("'C:\\My O''Reilly\\driver.zip'"));
        assert!(command.contains("'C:\\target dir'"));
    }

    #[test]
    fn rejects_asset_filenames_that_could_escape_download_directory() {
        assert!(validate_asset_filename("../driver.tar.gz").is_err());
        assert!(validate_asset_filename("subdir\\driver.zip").is_err());
        assert!(validate_asset_filename("driver.tar.gz").is_ok());
    }

    #[test]
    fn host_architecture_names_match_node_arch_labels() {
        assert_eq!(
            host_arch(),
            match std::env::consts::ARCH {
                "x86_64" => "x64",
                "aarch64" => "arm64",
                arch => arch,
            }
        );
    }

    #[tokio::test]
    async fn already_installed_binary_short_circuits_before_network() {
        let directory = TestDirectory::new();
        let path = binary_path(&directory.0, "linux", "x64", CUA_DRIVER_VERSION).unwrap();
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, b"present").await.unwrap();
        let install = InstallOptions {
            home: directory.0.clone(),
            platform: "linux".into(),
            arch: "x64".into(),
            version: CUA_DRIVER_VERSION.into(),
            download_host: None,
            client: client(),
            extractor: Arc::new(FakeExtractor::default()),
            on_progress: None,
        };
        assert_eq!(ensure_installed(&install).await.unwrap(), path);
    }

    #[test]
    fn checksum_parser_handles_carriage_returns_and_rejects_empty_filename() {
        assert_eq!(
            parse_checksums(&format!("{}  asset.zip\r\n", "d".repeat(64))).len(),
            1
        );
        assert!(parse_checksum_line(&format!("{}  ", "e".repeat(64))).is_none());
    }
}
