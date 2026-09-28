//! Per-simulator WebDriverAgent setup used by the TypeScript mobile-mcp server.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::Value;
use zip::ZipArchive;

use crate::devices::MobileCli;
use crate::logger;
use crate::runner::{CommandOutput, CommandRunner};

const MAX_SIMULATOR_APP_UNCOMPRESSED_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_SIMULATOR_APP_EXTRACTION_TIME: Duration = Duration::from_secs(30);

/// Ensure the simulator's WebDriverAgent is installed before using its
/// MobileDevice adapter. A failed status command or invalid JSON is surfaced;
/// installation is only attempted when the parsed status is explicitly
/// `fail`, as in Mobilecli.agentStatus.
pub fn ensure_webdriver_agent(mobilecli: &MobileCli, device_id: &str) -> Result<(), String> {
    let status_output = mobilecli.text(&[
        "agent".to_owned(),
        "status".to_owned(),
        "--device".to_owned(),
        device_id.to_owned(),
    ])?;
    let status: Value = serde_json::from_str(&status_output)
        .map_err(|error| format!("Invalid mobilecli agent status response: {error}"))?;
    if status.get("status").and_then(Value::as_str) == Some("fail") {
        mobilecli.text(&[
            "agent".to_owned(),
            "install".to_owned(),
            "--device".to_owned(),
            device_id.to_owned(),
        ])?;
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimctlError {
    Actionable(String),
    Failure(String),
}

impl std::fmt::Display for SimctlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Actionable(message) | Self::Failure(message) => formatter.write_str(message),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulatorApp {
    pub package_name: Option<String>,
    pub app_name: Option<String>,
}

/// The simulator app-management subset of the TypeScript `Simctl` robot.
/// Every xcrun invocation goes through the bounded shared process runner.
pub struct Simctl {
    simulator_id: String,
    runner: CommandRunner,
}

impl Simctl {
    pub fn new(simulator_id: impl Into<String>, runner: CommandRunner) -> Self {
        Self {
            simulator_id: simulator_id.into(),
            runner,
        }
    }

    pub fn launch_app(&self, package_name: &str, locale: Option<&str>) -> Result<(), SimctlError> {
        validate_package_name(package_name)?;
        let mut args = vec![
            "launch".to_owned(),
            self.simulator_id.clone(),
            package_name.to_owned(),
        ];
        if let Some(locale) = locale.filter(|locale| !locale.is_empty()) {
            validate_locale(locale)?;
            let locales = locale.split(',').map(str::trim).collect::<Vec<_>>();
            args.push("-AppleLanguages".to_owned());
            args.push(format!("({})", locales.join(", ")));
            args.push("-AppleLocale".to_owned());
            args.push(locales[0].to_owned());
        }
        self.simctl(args).map_err(SimctlError::Failure)?;
        Ok(())
    }

    pub fn terminate_app(&self, package_name: &str) -> Result<(), SimctlError> {
        validate_package_name(package_name)?;
        self.simctl([
            "terminate".to_owned(),
            self.simulator_id.clone(),
            package_name.to_owned(),
        ])
        .map_err(SimctlError::Failure)?;
        Ok(())
    }

    pub fn install_app(&self, path: &str) -> Result<(), SimctlError> {
        let mut extraction = None;
        let install_path = if Path::new(path)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
        {
            logger::trace("Detected .zip file, validating contents");
            let temp_dir = extract_app_archive(path).map_err(SimctlError::Actionable)?;
            let app_bundle = find_app_bundle(temp_dir.path()).map_err(SimctlError::Actionable)?;
            if let Some(name) = app_bundle.file_name() {
                logger::trace(&format!("Found .app bundle at: {}", name.to_string_lossy()));
            }
            extraction = Some(temp_dir);
            app_bundle
        } else {
            PathBuf::from(path)
        };

        self.simctl([
            "install".to_owned(),
            self.simulator_id.clone(),
            install_path.to_string_lossy().into_owned(),
        ])
        .map_err(|error| SimctlError::Actionable(error))?;
        drop(extraction);
        Ok(())
    }

    pub fn uninstall_app(&self, bundle_id: &str) -> Result<(), SimctlError> {
        self.simctl([
            "uninstall".to_owned(),
            self.simulator_id.clone(),
            bundle_id.to_owned(),
        ])
        .map_err(SimctlError::Actionable)?;
        Ok(())
    }

    pub fn list_apps(&self) -> Result<Vec<SimulatorApp>, SimctlError> {
        let plist = self
            .simctl(["listapps".to_owned(), self.simulator_id.clone()])
            .map_err(SimctlError::Failure)?;
        let temp_dir = TempDirGuard::create("mobile-mcp-plist")
            .map_err(|error| SimctlError::Failure(error.to_string()))?;
        let plist_path = temp_dir.path().join("apps.plist");
        fs::write(&plist_path, plist).map_err(|error| SimctlError::Failure(error.to_string()))?;
        let output = self
            .runner
            .run(
                "plutil",
                [
                    "-convert".to_owned(),
                    "json".to_owned(),
                    "-o".to_owned(),
                    "-".to_owned(),
                    "-r".to_owned(),
                    plist_path.to_string_lossy().into_owned(),
                ],
            )
            .map_err(|error| SimctlError::Failure(error.to_string()))?;
        if !output.status.success() {
            return Err(SimctlError::Failure(command_output(&output)));
        }
        let value: Value = serde_json::from_slice(&output.stdout).map_err(|error| {
            SimctlError::Failure(format!("Invalid simulator app list: {error}"))
        })?;
        let apps = value
            .as_object()
            .ok_or_else(|| SimctlError::Failure("Invalid simulator app list".to_owned()))?;
        Ok(apps
            .values()
            .map(|app| SimulatorApp {
                package_name: app
                    .get("CFBundleIdentifier")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                app_name: app
                    .get("CFBundleDisplayName")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
            .collect())
    }

    fn simctl<I, S>(&self, args: I) -> Result<Vec<u8>, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<std::ffi::OsString>,
    {
        #[cfg(not(target_os = "macos"))]
        {
            let _ = args;
            Err("iOS simulator app management is only available on macOS".to_owned())
        }
        #[cfg(target_os = "macos")]
        {
            let args = std::iter::once(std::ffi::OsString::from("simctl"))
                .chain(args.into_iter().map(Into::into));
            let output = self
                .runner
                .run("xcrun", args)
                .map_err(|error| error.to_string())?;
            if output.status.success() {
                Ok(output.stdout)
            } else {
                Err(command_output(&output))
            }
        }
    }
}

struct TempDirGuard {
    path: PathBuf,
    trace_cleanup: bool,
}

impl TempDirGuard {
    fn create(prefix: &str) -> io::Result<Self> {
        for _ in 0..32 {
            let path = std::env::temp_dir().join(format!(
                "{prefix}-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            match fs::create_dir(&path) {
                Ok(()) => {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        if let Err(error) =
                            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                        {
                            let _ = fs::remove_dir_all(&path);
                            return Err(error);
                        }
                    }
                    return Ok(Self {
                        path,
                        trace_cleanup: prefix == "ios-app",
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a unique temporary directory",
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        if self.trace_cleanup {
            logger::trace("Cleaning up temporary directory");
        }
        if let Err(error) = fs::remove_dir_all(&self.path) {
            if error.kind() != io::ErrorKind::NotFound && self.trace_cleanup {
                logger::trace(&format!(
                    "Warning: Failed to cleanup temporary directory: {error}"
                ));
            }
        }
    }
}

fn extract_app_archive(path: &str) -> Result<TempDirGuard, String> {
    let archive_file = File::open(path).map_err(|error| error.to_string())?;
    let mut archive =
        ZipArchive::new(archive_file).map_err(|error| format!("Invalid zip archive: {error}"))?;
    let mut uncompressed_bytes = 0_u64;
    for index in 0..archive.len() {
        let file = archive
            .by_index(index)
            .map_err(|error| format!("Invalid zip archive: {error}"))?;
        let relative = validated_entry_path(file.name(), file.is_dir())?;
        if relative.as_os_str().is_empty() && !file.is_dir() {
            return Err(format!(
                "Security violation: File path '{}' contains invalid characters",
                file.name().trim()
            ));
        }
        if file.unix_mode().is_some_and(|mode| {
            let file_type = mode & 0o170000;
            file_type != 0 && file_type != 0o100000 && file_type != 0o040000
        }) {
            return Err(format!(
                "Security violation: File path '{}' contains an unsupported link or special file",
                file.name()
            ));
        }
        uncompressed_bytes = uncompressed_bytes.saturating_add(file.size());
        if uncompressed_bytes > MAX_SIMULATOR_APP_UNCOMPRESSED_BYTES {
            return Err(format!(
                "App archive expands beyond the {MAX_SIMULATOR_APP_UNCOMPRESSED_BYTES} byte limit"
            ));
        }
    }

    let temp_dir = TempDirGuard::create("ios-app").map_err(|error| error.to_string())?;
    let extraction_started = Instant::now();
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| format!("Failed to unzip file: {error}"))?;
        let relative = validated_entry_path(entry.name(), entry.is_dir())?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        let destination = temp_dir.path().join(relative);
        if entry.is_dir() {
            fs::create_dir_all(&destination)
                .map_err(|error| format!("Failed to unzip file: {error}"))?;
            continue;
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| format!("Failed to unzip file: {error}"))?;
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
            .map_err(|error| format!("Failed to unzip file: {error}"))?;
        #[cfg(unix)]
        if let Some(mode) = entry.unix_mode() {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&destination, fs::Permissions::from_mode(mode & 0o777))
                .map_err(|error| format!("Failed to unzip file: {error}"))?;
        }
        let mut copied = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            if extraction_started.elapsed() > MAX_SIMULATOR_APP_EXTRACTION_TIME {
                return Err("Failed to unzip file: timed out after 30.00 seconds".to_owned());
            }
            let count = entry
                .read(&mut buffer)
                .map_err(|error| format!("Failed to unzip file: {error}"))?;
            if count == 0 {
                break;
            }
            output
                .write_all(&buffer[..count])
                .map_err(|error| format!("Failed to unzip file: {error}"))?;
            copied = copied.saturating_add(count as u64);
        }
        if copied != entry.size() {
            return Err(
                "Failed to unzip file: extracted size did not match archive metadata".to_owned(),
            );
        }
    }
    Ok(temp_dir)
}

fn validated_entry_path(path: &str, is_dir: bool) -> Result<PathBuf, String> {
    let trimmed = path.trim();
    if trimmed.starts_with('/')
        || trimmed.starts_with('\\')
        || trimmed.contains("..")
        || trimmed.contains('\\')
        || has_windows_drive_prefix(trimmed)
    {
        return Err(format!(
            "Security violation: File path '{trimmed}' contains invalid characters"
        ));
    }
    let relative = Path::new(path)
        .components()
        .filter(|component| !matches!(component, std::path::Component::CurDir))
        .collect::<PathBuf>();
    if relative.as_os_str().is_empty() && is_dir {
        return Ok(relative);
    }
    if relative.as_os_str().is_empty()
        || Path::new(path).components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(format!(
            "Security violation: File path '{trimmed}' contains invalid characters"
        ));
    }
    Ok(relative)
}

fn has_windows_drive_prefix(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn find_app_bundle(directory: &Path) -> Result<PathBuf, String> {
    let entries = fs::read_dir(directory).map_err(|error| error.to_string())?;
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        let is_app = entry
            .file_type()
            .map(|file_type| file_type.is_dir())
            .unwrap_or(false)
            && entry.file_name().to_string_lossy().ends_with(".app");
        if is_app {
            return Ok(entry.path());
        }
    }
    Err("No .app bundle found in the .zip file, please visit wiki at https://github.com/mobile-next/mobile-mcp/wiki for assistance.".to_owned())
}

fn validate_package_name(package_name: &str) -> Result<(), SimctlError> {
    if !package_name.is_empty()
        && package_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_'))
    {
        Ok(())
    } else {
        Err(SimctlError::Actionable(format!(
            "Invalid package name: \"{package_name}\""
        )))
    }
}

fn validate_locale(locale: &str) -> Result<(), SimctlError> {
    if !locale.is_empty()
        && locale
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b',' | b'-' | b' '))
    {
        Ok(())
    } else {
        Err(SimctlError::Actionable(format!(
            "Invalid locale: \"{locale}\""
        )))
    }
}

fn command_output(output: &CommandOutput) -> String {
    let mut detail = String::from_utf8_lossy(&output.stdout).into_owned();
    detail.push_str(&String::from_utf8_lossy(&output.stderr));
    let detail = detail.trim();
    if detail.is_empty() {
        format!("xcrun exited with {}", output.status)
    } else {
        detail.to_owned()
    }
}
