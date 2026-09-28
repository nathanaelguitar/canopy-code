//! Secure, best-effort browser opening for authentication and local reports.

use std::env;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};

use url::Url;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BrowserLaunchOptions {
    pub allow_file: bool,
    pub allowed_file_paths: Vec<PathBuf>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BrowserLaunchEnvironmentOptions {
    pub ignore_browser_blocklist: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BrowserLaunchError {
    InvalidUrl(String),
    UnsafeProtocol { protocol: String, allowed: String },
    AllowedFilePathsRequired,
    FileNotAllowed,
    InvalidFileUrl(String),
    CurrentDirectory(String),
    InvalidUrlCharacters,
    UnsupportedPlatform(String),
}

impl fmt::Display for BrowserLaunchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl(url) => write!(formatter, "Invalid URL: {url}"),
            Self::UnsafeProtocol { protocol, allowed } => {
                write!(
                    formatter,
                    "Unsafe protocol: {protocol}. Only {allowed} are allowed."
                )
            }
            Self::AllowedFilePathsRequired => {
                formatter.write_str("allowedFilePaths is required when allowFile is true")
            }
            Self::FileNotAllowed => formatter.write_str("File URL is not in the allowed file set"),
            Self::InvalidFileUrl(url) => write!(formatter, "Invalid file URL: {url}"),
            Self::CurrentDirectory(error) => {
                write!(
                    formatter,
                    "Could not resolve the current directory: {error}"
                )
            }
            Self::InvalidUrlCharacters => formatter.write_str("URL contains invalid characters"),
            Self::UnsupportedPlatform(platform) => {
                write!(formatter, "Unsupported platform: {platform}")
            }
        }
    }
}

impl std::error::Error for BrowserLaunchError {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BrowserCommand {
    command: String,
    args: Vec<String>,
}

/// Open a browser-safe URL using the current environment and platform.
///
/// URL validation errors are returned. Launch failures are reported to stderr
/// with a manual-open instruction and resolve successfully, as in the source
/// TypeScript helper.
pub fn open_browser_securely(url: &str) -> Result<(), BrowserLaunchError> {
    open_browser_securely_with_options(url, BrowserLaunchOptions::default())
}

/// Open a URL, optionally allowing one exact `file://` path from the caller's
/// allow-list. File paths are resolved lexically; symlinks are not followed.
pub fn open_browser_securely_with_options(
    url: &str,
    options: BrowserLaunchOptions,
) -> Result<(), BrowserLaunchError> {
    validate_url(url, &options)?;

    let platform = current_platform();
    let browser_env = env::var("BROWSER")
        .ok()
        .map(|value| trim_javascript_whitespace(&value).to_owned());
    let browser_env = browser_env.filter(|value| !value.is_empty());
    let browser_command = if platform == Platform::Windows {
        None
    } else if let Some(browser_env) = browser_env.as_deref() {
        match parse_browser_command(browser_env) {
            Some(command) => build_browser_command(command, url),
            None => {
                eprintln!(
                    "Invalid BROWSER environment variable, falling back to platform default."
                );
                None
            }
        }
    } else {
        None
    };

    if let Some(browser_command) = browser_command {
        match launch_detached(&browser_command.command, &browser_command.args) {
            Ok(()) => return Ok(()),
            Err(error) => eprintln!(
                "Failed to open BROWSER command {}: {error}. Falling back to the platform browser opener.",
                browser_command.command
            ),
        }
    }

    if !should_attempt_browser_launch_for_platform(
        BrowserLaunchEnvironmentOptions {
            ignore_browser_blocklist: true,
        },
        platform,
    ) {
        warn_manual_open(url);
        return Ok(());
    }

    let (command, args) = platform_open_command(platform, url)?;
    if run_platform_command(&command, &args, true).is_ok() {
        return Ok(());
    }

    if platform.is_unix_like() && command == "xdg-open" {
        let fallbacks = [
            ("gnome-open", false),
            ("kde-open", false),
            ("firefox", true),
            ("chromium", true),
            ("google-chrome", true),
            ("microsoft-edge", true),
        ];
        for (fallback, detached) in fallbacks {
            let args = [url.to_owned()];
            let result = if detached {
                launch_detached(fallback, &args)
            } else {
                run_platform_command(fallback, &args, true)
            };
            if result.is_ok() {
                return Ok(());
            }
        }
    }

    warn_manual_open(url);
    Ok(())
}

/// Whether the current environment normally permits an automatic browser
/// launch. An explicit BROWSER command is attempted before this gate by
/// `open_browser_securely`, matching the TypeScript helper.
pub fn should_launch_browser() -> bool {
    should_attempt_browser_launch()
}

/// Browser launch policy used by both the secure launcher and callers that
/// need to decide whether to offer an automatic browser flow.
pub fn should_attempt_browser_launch() -> bool {
    should_attempt_browser_launch_with_options(BrowserLaunchEnvironmentOptions::default())
}

/// Browser launch policy with an explicit blocklist override.
pub fn should_attempt_browser_launch_with_options(
    options: BrowserLaunchEnvironmentOptions,
) -> bool {
    should_attempt_browser_launch_for_platform(options, current_platform())
}

/// Whether a browser command is blocklisted by executable basename.
pub fn is_browser_command_blocked(command: &str) -> bool {
    let normalized = command.replace('\\', "/");
    normalized.rsplit('/').next() == Some("www-browser")
}

fn validate_url(url: &str, options: &BrowserLaunchOptions) -> Result<(), BrowserLaunchError> {
    let parsed = Url::parse(url).map_err(|_| BrowserLaunchError::InvalidUrl(url.to_owned()))?;
    let allowed = if options.allow_file {
        "http:, https:, file:"
    } else {
        "http:, https:"
    };
    if parsed.scheme() != "http"
        && parsed.scheme() != "https"
        && !(options.allow_file && parsed.scheme() == "file")
    {
        return Err(BrowserLaunchError::UnsafeProtocol {
            protocol: format!("{}:", parsed.scheme()),
            allowed: allowed.to_owned(),
        });
    }

    if parsed.scheme() == "file" && options.allow_file {
        if options.allowed_file_paths.is_empty() {
            return Err(BrowserLaunchError::AllowedFilePathsRequired);
        }
        let requested = parsed
            .to_file_path()
            .map_err(|_| BrowserLaunchError::InvalidFileUrl(url.to_owned()))?;
        let current_dir = env::current_dir()
            .map_err(|error| BrowserLaunchError::CurrentDirectory(error.to_string()))?;
        let requested = resolve_path(&requested, &current_dir);
        let allowed = options
            .allowed_file_paths
            .iter()
            .map(|path| resolve_path(path, &current_dir))
            .collect::<Vec<_>>();
        if !allowed.iter().any(|path| path == &requested) {
            return Err(BrowserLaunchError::FileNotAllowed);
        }
    }

    if url.chars().any(|character| (character as u32) <= 0x1f) {
        return Err(BrowserLaunchError::InvalidUrlCharacters);
    }
    Ok(())
}

fn resolve_path(path: &Path, current_dir: &Path) -> PathBuf {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        current_dir.join(path)
    };
    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            Component::RootDir => resolved.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                let popped = resolved.pop();
                if !popped && !resolved.has_root() {
                    resolved.push(component.as_os_str());
                }
            }
            Component::Normal(value) => resolved.push(value),
        }
    }
    resolved
}

fn should_attempt_browser_launch_for_platform(
    options: BrowserLaunchEnvironmentOptions,
    platform: Platform,
) -> bool {
    let browser_env = env::var("BROWSER").unwrap_or_default();
    let browser_command = trim_javascript_whitespace(&browser_env)
        .chars()
        .take_while(|character| !is_javascript_whitespace(*character))
        .collect::<String>();
    if !options.ignore_browser_blocklist
        && platform != Platform::Windows
        && !browser_command.is_empty()
        && is_browser_command_blocked(&browser_command)
    {
        return false;
    }

    if env_is_truthy("CI")
        || env::var("DEBIAN_FRONTEND").is_ok_and(|value| value == "noninteractive")
    {
        return false;
    }
    let is_ssh = env_is_truthy("SSH_CONNECTION");
    if platform == Platform::Linux
        && !["DISPLAY", "WAYLAND_DISPLAY", "MIR_SOCKET"]
            .iter()
            .any(|variable| env_is_truthy(variable))
    {
        return false;
    }
    if is_ssh && platform != Platform::Linux {
        return false;
    }
    true
}

fn env_is_truthy(key: &str) -> bool {
    env::var_os(key).is_some_and(|value| !value.is_empty())
}

fn parse_browser_command(browser_env: &str) -> Option<BrowserCommand> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    for character in browser_env.chars() {
        if let Some(active_quote) = quote {
            if character == active_quote {
                quote = None;
            } else {
                current.push(character);
            }
            continue;
        }
        if character == '"' || character == '\'' {
            quote = Some(character);
        } else if is_javascript_whitespace(character) {
            if !current.is_empty() {
                parts.push(std::mem::take(&mut current));
            }
        } else {
            current.push(character);
        }
    }
    if quote.is_some() {
        return None;
    }
    if !current.is_empty() {
        parts.push(current);
    }
    let mut parts = parts.into_iter();
    let command = parts.next()?;
    Some(BrowserCommand {
        command,
        args: parts.collect(),
    })
}

fn build_browser_command(mut command: BrowserCommand, url: &str) -> Option<BrowserCommand> {
    if is_browser_command_blocked(&command.command) {
        return None;
    }
    let mut used_placeholder = false;
    for argument in &mut command.args {
        if argument.contains("%s") {
            *argument = argument.replace("%s", url);
            used_placeholder = true;
        }
    }
    if !used_placeholder {
        command.args.push(url.to_owned());
    }
    Some(command)
}

fn launch_detached(command: &str, args: &[String]) -> Result<(), String> {
    let mut child = process_command(command, args, true)
        .spawn()
        .map_err(|error| error.to_string())?;
    let _ = std::thread::Builder::new()
        .name("browser-child-reaper".into())
        .spawn(move || {
            let _ = child.wait();
        });
    Ok(())
}

fn run_platform_command(command: &str, args: &[String], detached: bool) -> Result<(), String> {
    let status = process_command(command, args, detached)
        .status()
        .map_err(|error| error.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format_status(status))
    }
}

fn process_command(command: &str, args: &[String], detached: bool) -> Command {
    let mut process = Command::new(command);
    process
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .env_remove("SHELL");
    #[cfg(unix)]
    if detached {
        use std::os::unix::process::CommandExt;
        process.process_group(0);
    }
    #[cfg(windows)]
    if detached {
        use std::os::windows::process::CommandExt;
        process.creation_flags(0x0000_0008 | 0x0000_0200 | 0x0800_0000);
    }
    process
}

fn platform_open_command(
    platform: Platform,
    url: &str,
) -> Result<(String, Vec<String>), BrowserLaunchError> {
    let result = match platform {
        Platform::MacOs => ("open".to_owned(), vec![url.to_owned()]),
        Platform::Windows => {
            let escaped = url.replace('\'', "''");
            (
                "powershell.exe".to_owned(),
                vec![
                    "-NoProfile".into(),
                    "-NonInteractive".into(),
                    "-WindowStyle".into(),
                    "Hidden".into(),
                    "-Command".into(),
                    format!("Start-Process '{escaped}'"),
                ],
            )
        }
        Platform::Linux | Platform::FreeBsd | Platform::OpenBsd => {
            ("xdg-open".to_owned(), vec![url.to_owned()])
        }
        Platform::Other(name) => {
            return Err(BrowserLaunchError::UnsupportedPlatform(name.to_owned()));
        }
    };
    Ok(result)
}

fn format_status(status: ExitStatus) -> String {
    match status.code() {
        Some(code) => format!("process exited with status {code}"),
        None => "process terminated by signal".into(),
    }
}

fn warn_manual_open(url: &str) {
    eprintln!("Failed to open browser automatically. Please open this URL manually: {url}");
}

fn is_javascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

fn trim_javascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_javascript_whitespace)
}

// The complete match is shared across target builds; variants for other
// operating systems are unused on the current host target.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Platform {
    MacOs,
    Windows,
    Linux,
    FreeBsd,
    OpenBsd,
    Other(&'static str),
}

impl Platform {
    fn is_unix_like(self) -> bool {
        matches!(self, Self::Linux | Self::FreeBsd | Self::OpenBsd)
    }
}

fn current_platform() -> Platform {
    #[cfg(target_os = "macos")]
    {
        Platform::MacOs
    }
    #[cfg(target_os = "windows")]
    {
        Platform::Windows
    }
    #[cfg(target_os = "linux")]
    {
        Platform::Linux
    }
    #[cfg(target_os = "freebsd")]
    {
        Platform::FreeBsd
    }
    #[cfg(target_os = "openbsd")]
    {
        Platform::OpenBsd
    }
    #[cfg(not(any(
        target_os = "macos",
        target_os = "windows",
        target_os = "linux",
        target_os = "freebsd",
        target_os = "openbsd"
    )))]
    {
        Platform::Other(std::env::consts::OS)
    }
}
