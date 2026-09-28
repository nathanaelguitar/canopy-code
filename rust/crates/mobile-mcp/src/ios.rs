//! Physical iOS app-management commands backed by bounded `go-ios` calls.

use crate::devices::go_ios_path;
use crate::runner::{CommandError, CommandOutput, CommandRunner};
use crate::wda::WdaClient;

const IOS_TUNNEL_PORT: u16 = 60105;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IosAppError {
    Actionable(String),
    Failure(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IosApp {
    pub package_name: String,
    pub app_name: String,
}

#[derive(Clone, Debug)]
pub struct PhysicalIosRobot {
    device_id: String,
    version: String,
    runner: CommandRunner,
}

impl PhysicalIosRobot {
    pub fn new(device_id: String, version: String, runner: CommandRunner) -> Self {
        Self {
            device_id,
            version,
            runner,
        }
    }

    pub fn list_apps(&self) -> Result<Vec<IosApp>, IosAppError> {
        self.assert_tunnel_running()?;
        let output = self.run_ios_checked(["apps", "--all", "--list"])?;
        Ok(String::from_utf8_lossy(&output.stdout)
            .split('\n')
            .map(|line| {
                let mut fields = line.split(' ');
                IosApp {
                    package_name: fields.next().unwrap_or("undefined").to_owned(),
                    app_name: fields.next().unwrap_or("undefined").to_owned(),
                }
            })
            .collect())
    }

    pub fn launch_app(&self, package_name: &str, locale: Option<&str>) -> Result<(), IosAppError> {
        validate_package_name(package_name)?;
        self.assert_tunnel_running()?;
        let mut args = vec!["launch".to_owned(), package_name.to_owned()];
        if let Some(locale) = locale.filter(|locale| !locale.is_empty()) {
            validate_locale(locale)?;
            let locales = locale.split(',').map(str::trim).collect::<Vec<_>>();
            let apple_languages = format!("({})", locales.join(", "));
            args.extend([
                "-AppleLanguages".to_owned(),
                apple_languages,
                "-AppleLocale".to_owned(),
                locales.first().copied().unwrap_or_default().to_owned(),
            ]);
        }
        self.run_ios_checked(args).map(|_| ())
    }

    pub fn terminate_app(&self, package_name: &str) -> Result<(), IosAppError> {
        validate_package_name(package_name)?;
        self.assert_tunnel_running()?;
        self.run_ios_checked(["kill", package_name]).map(|_| ())
    }

    pub fn install_app(&self, path: &str) -> Result<(), IosAppError> {
        self.assert_tunnel_running()?;
        self.run_ios_install_action(["install", "--path", path])
    }

    pub fn uninstall_app(&self, bundle_id: &str) -> Result<(), IosAppError> {
        self.assert_tunnel_running()?;
        self.run_ios_install_action(["uninstall", "--bundleid", bundle_id])
    }

    fn assert_tunnel_running(&self) -> Result<(), IosAppError> {
        if ios_major_version(&self.version).is_some_and(|major| major >= 17)
            && !WdaClient::port_is_listening(IOS_TUNNEL_PORT)
        {
            return Err(IosAppError::Actionable(
                "iOS tunnel is not running, please see https://github.com/mobile-next/mobile-mcp/wiki/"
                    .into(),
            ));
        }
        Ok(())
    }

    fn run_ios_checked<I, S>(&self, args: I) -> Result<CommandOutput, IosAppError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        self.runner
            .run_checked(go_ios_path(), self.with_device_id(args))
            .map_err(|error| IosAppError::Failure(error.to_string()))
    }

    fn run_ios_install_action<I, S>(&self, args: I) -> Result<(), IosAppError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        match self.runner.run(go_ios_path(), self.with_device_id(args)) {
            Ok(output) if output.status.success() => Ok(()),
            Ok(output) => Err(IosAppError::Actionable(command_output_error(
                output.stdout,
                output.stderr,
                format!("ios exited with {}", output.status),
            ))),
            Err(error) => Err(IosAppError::Actionable(command_error_message(error))),
        }
    }

    fn with_device_id<I, S>(&self, args: I) -> impl Iterator<Item = String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        std::iter::once("--udid".to_owned())
            .chain(std::iter::once(self.device_id.clone()))
            .chain(
                args.into_iter()
                    .map(|arg| arg.as_ref().to_string_lossy().into_owned()),
            )
    }
}

fn validate_package_name(package_name: &str) -> Result<(), IosAppError> {
    if package_name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_'))
        && !package_name.is_empty()
    {
        Ok(())
    } else {
        Err(IosAppError::Actionable(format!(
            "Invalid package name: \"{package_name}\""
        )))
    }
}

fn validate_locale(locale: &str) -> Result<(), IosAppError> {
    if !locale.is_empty()
        && locale
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b',' | b'-' | b' '))
    {
        Ok(())
    } else {
        Err(IosAppError::Actionable(format!(
            "Invalid locale: \"{locale}\""
        )))
    }
}

fn ios_major_version(version: &str) -> Option<u32> {
    let version = version.trim_start();
    let version = version.strip_prefix('+').unwrap_or(version);
    let digit_count = version.bytes().take_while(u8::is_ascii_digit).count();
    (digit_count > 0)
        .then(|| version[..digit_count].parse().ok())
        .flatten()
}

fn command_output_error(stdout: Vec<u8>, stderr: Vec<u8>, fallback: String) -> String {
    let mut output = String::from_utf8_lossy(&stdout).into_owned();
    output.push_str(&String::from_utf8_lossy(&stderr));
    let output = output.trim();
    if output.is_empty() {
        fallback
    } else {
        output.to_owned()
    }
}

fn command_error_message(error: CommandError) -> String {
    let fallback = error.to_string();
    command_output_error(error.stdout, error.stderr, fallback)
}
