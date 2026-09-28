//! Device discovery and mobilecli command resolution.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::android::{AndroidDeviceType, AndroidRobot};
use crate::runner::CommandRunner;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Android,
    Ios,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DeviceType {
    Real,
    Emulator,
    Simulator,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DeviceState {
    Online,
    Offline,
}

fn online_state() -> DeviceState {
    DeviceState::Online
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Device {
    pub id: String,
    pub name: String,
    pub platform: Platform,
    #[serde(rename = "type")]
    pub device_type: DeviceType,
    pub version: String,
    #[serde(default = "online_state")]
    pub state: DeviceState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AndroidDevice {
    pub device_id: String,
    pub device_type: AndroidDeviceType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AndroidDeviceDetails {
    pub device_id: String,
    pub device_type: AndroidDeviceType,
    pub version: String,
    pub name: String,
}

/// ADB-backed equivalent of the TypeScript AndroidDeviceManager.
#[derive(Clone, Debug)]
pub struct AndroidDeviceManager {
    runner: CommandRunner,
}

impl AndroidDeviceManager {
    pub fn new(runner: CommandRunner) -> Self {
        Self { runner }
    }

    /// List devices that ADB reports as online, classifying TV devices from
    /// their package-manager feature list. Feature-query failures fall back to
    /// mobile, matching the TypeScript manager.
    pub fn get_connected_devices(&self) -> Vec<AndroidDevice> {
        self.connected_device_ids()
            .into_iter()
            .map(|device_id| {
                let device_type = AndroidRobot::new(device_id.clone(), self.runner.clone())
                    .get_device_type()
                    .unwrap_or(AndroidDeviceType::Mobile);
                AndroidDevice {
                    device_id,
                    device_type,
                }
            })
            .collect()
    }

    /// List online devices with Android release and human-readable name.
    pub fn get_connected_devices_with_details(&self) -> Vec<AndroidDeviceDetails> {
        self.get_connected_devices()
            .into_iter()
            .map(|device| {
                let avd_name =
                    adb_property(&self.runner, &device.device_id, "ro.boot.qemu.avd_name");
                let name = match avd_name {
                    Some(avd_name) if !avd_name.is_empty() => avd_name.replace('_', " "),
                    Some(_) => adb_property(&self.runner, &device.device_id, "ro.product.model")
                        .unwrap_or_else(|| device.device_id.clone()),
                    None => device.device_id.clone(),
                };
                let version =
                    adb_property(&self.runner, &device.device_id, "ro.build.version.release")
                        .unwrap_or_else(|| "unknown".to_owned());
                AndroidDeviceDetails {
                    device_id: device.device_id,
                    device_type: device.device_type,
                    version,
                    name,
                }
            })
            .collect()
    }

    fn connected_device_ids(&self) -> Vec<String> {
        let Ok(output) = self.runner.run(adb_path(), ["devices"]) else {
            return Vec::new();
        };
        if !output.status.success() {
            return Vec::new();
        }
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with("List of devices attached"))
            .filter_map(|line| {
                let (device_id, state) = line.split_once('\t')?;
                (state.trim() == "device").then(|| device_id.to_owned())
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IosDevice {
    pub device_id: String,
    pub device_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IosDeviceDetails {
    pub device_id: String,
    pub device_name: String,
    pub version: String,
}

/// go-ios-backed equivalent of the TypeScript IosManager discovery methods.
#[derive(Clone, Debug)]
pub struct IosDeviceManager {
    runner: CommandRunner,
}

impl IosDeviceManager {
    pub fn new(runner: CommandRunner) -> Self {
        Self { runner }
    }

    pub fn is_go_ios_installed(&self) -> bool {
        let Ok(output) = self.runner.run(go_ios_path(), ["version"]) else {
            return false;
        };
        if !output.status.success() {
            return false;
        }
        serde_json::from_slice::<Value>(&output.stdout)
            .ok()
            .and_then(|value| {
                value
                    .get("version")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .is_some_and(|version| is_go_ios_version(&version))
    }

    pub fn list_devices(&self) -> Result<Vec<IosDevice>, String> {
        self.list_devices_with_details().map(|devices| {
            devices
                .into_iter()
                .map(|device| IosDevice {
                    device_id: device.device_id,
                    device_name: device.device_name,
                })
                .collect()
        })
    }

    /// Read device names and release versions. If listing or reading any
    /// device fails, return an error so the caller can omit the physical iOS
    /// group as the TypeScript server does around `listDevicesWithDetails`.
    pub fn list_devices_with_details(&self) -> Result<Vec<IosDeviceDetails>, String> {
        if !self.is_go_ios_installed() {
            return Ok(Vec::new());
        }
        let list = self
            .runner
            .run_checked(go_ios_path(), ["list"])
            .map_err(|error| error.to_string())?;
        let value: Value = serde_json::from_slice(&list.stdout)
            .map_err(|error| format!("Invalid go-ios list response: {error}"))?;
        let ids = value
            .get("deviceList")
            .and_then(Value::as_array)
            .ok_or_else(|| "Invalid go-ios list response: missing deviceList".to_owned())?;

        ids.iter()
            .filter_map(Value::as_str)
            .map(|device_id| {
                let info = self
                    .runner
                    .run_checked(go_ios_path(), ["info", "--udid", device_id])
                    .map_err(|error| error.to_string())?;
                let info: Value = serde_json::from_slice(&info.stdout)
                    .map_err(|error| format!("Invalid go-ios info response: {error}"))?;
                Ok(IosDeviceDetails {
                    device_id: device_id.to_owned(),
                    device_name: info
                        .get("DeviceName")
                        .and_then(Value::as_str)
                        .unwrap_or(device_id)
                        .to_owned(),
                    version: info
                        .get("ProductVersion")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_owned(),
                })
            })
            .collect()
    }
}

pub fn adb_path() -> PathBuf {
    let exe_name = if cfg!(windows) { "adb.exe" } else { "adb" };
    if let Some(home) = std::env::var_os("ANDROID_HOME") {
        return PathBuf::from(home).join("platform-tools").join(exe_name);
    }
    #[cfg(target_os = "windows")]
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        let candidate = PathBuf::from(local)
            .join("Android")
            .join("Sdk")
            .join("platform-tools")
            .join(exe_name);
        if candidate.exists() {
            return candidate;
        }
    }
    #[cfg(target_os = "macos")]
    if let Some(home) = std::env::var_os("HOME") {
        let candidate = PathBuf::from(home).join("Library/Android/sdk/platform-tools/adb");
        if candidate.exists() {
            return candidate;
        }
    }
    PathBuf::from(exe_name)
}

pub fn go_ios_path() -> PathBuf {
    std::env::var_os("GO_IOS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("ios"))
}

fn is_go_ios_version(version: &str) -> bool {
    if version == "local-build" {
        return true;
    }
    let version = version.strip_prefix('v').unwrap_or(version);
    let mut remaining = version;
    for component in 0..3 {
        let digits = remaining.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return false;
        }
        remaining = &remaining[digits..];
        if component < 2 {
            let Some(rest) = remaining.strip_prefix('.') else {
                return false;
            };
            remaining = rest;
        }
    }
    true
}

pub fn mobilecli_path() -> PathBuf {
    resolve_mobilecli_path().path
}

struct MobileCliPathResolution {
    path: PathBuf,
    error: Option<String>,
}

fn resolve_mobilecli_path() -> MobileCliPathResolution {
    if let Some(path) = std::env::var_os("MOBILECLI_PATH").filter(|path| !path.is_empty()) {
        return MobileCliPathResolution {
            path: PathBuf::from(path),
            error: None,
        };
    }

    let (node_platform, binary_platform) = mobilecli_platform_names();
    let arch = if std::env::consts::ARCH == "aarch64" {
        "arm64"
    } else {
        // mobilecli.ts maps only Node's arm64 architecture specially; every
        // other process architecture selects the amd64 artifact.
        "amd64"
    };
    let ext = if binary_platform == "windows" {
        ".exe"
    } else {
        ""
    };
    let binary_name = format!("mobilecli-{binary_platform}-{arch}{ext}");

    // `__filename` is the TypeScript source's package location. For the native
    // executable, its installed path supplies the equivalent package location.
    // Match the source order: the nearest node_modules sibling first, followed
    // by the package-local node_modules fallback.
    let executable = std::env::current_exe().ok();
    let mut candidates = Vec::new();
    if let Some(executable) = executable.as_deref() {
        if let Some(node_modules) = executable.ancestors().find(|ancestor| {
            ancestor
                .file_name()
                .is_some_and(|name| name == "node_modules")
        }) {
            candidates.push(node_modules.join("mobilecli/bin").join(&binary_name));
        }

        let script_dir = executable.parent();
        let package_dir = script_dir.and_then(Path::parent);
        if let Some(package_dir) = package_dir {
            candidates.push(
                package_dir
                    .join("node_modules/mobilecli/bin")
                    .join(&binary_name),
            );
        }
    }

    if let Some(path) = candidates.iter().find(|path| path.is_file()) {
        return MobileCliPathResolution {
            path: path.clone(),
            error: None,
        };
    }

    let path = candidates.last().cloned().unwrap_or_else(|| {
        std::env::current_dir()
            .unwrap_or_default()
            .join("node_modules/mobilecli/bin")
            .join(&binary_name)
    });
    MobileCliPathResolution {
        path,
        error: Some(format!(
            "Could not find mobilecli binary for platform: {node_platform}"
        )),
    }
}

fn mobilecli_platform_names() -> (&'static str, &'static str) {
    match std::env::consts::OS {
        "windows" => ("win32", "windows"),
        "macos" => ("darwin", "darwin"),
        "solaris" | "illumos" => ("sunos", "sunos"),
        platform => (platform, platform),
    }
}

#[derive(Clone, Debug)]
pub struct MobileCli {
    path: PathBuf,
    path_error: Option<String>,
    runner: CommandRunner,
}

impl MobileCli {
    pub fn new(runner: CommandRunner) -> Self {
        let resolution = resolve_mobilecli_path();
        Self {
            path: resolution.path,
            path_error: resolution.error,
            runner,
        }
    }
    pub fn with_path(path: impl Into<PathBuf>, runner: CommandRunner) -> Self {
        Self {
            path: path.into(),
            path_error: None,
            runner,
        }
    }
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn execute(&self, args: &[String]) -> Result<Vec<u8>, String> {
        self.check_path_resolution()?;
        let output = self
            .runner
            .run(&self.path, args)
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            let detail = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            let detail = if detail.is_empty() {
                String::from_utf8_lossy(&output.stdout).trim().to_owned()
            } else {
                detail
            };
            return Err(if detail.is_empty() {
                format!("mobilecli exited with {}", output.status)
            } else {
                detail
            });
        }
        Ok(output.stdout)
    }

    pub fn spawn(&self, args: Vec<String>) -> Result<std::process::Child, String> {
        self.check_path_resolution()?;
        self.runner
            .spawn(self.path.clone(), args)
            .map_err(|error| error.to_string())
    }

    fn check_path_resolution(&self) -> Result<(), String> {
        match &self.path_error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub fn text(&self, args: &[String]) -> Result<String, String> {
        self.execute(args)
            .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_owned())
    }

    pub fn check(&self) -> Result<(), String> {
        let output = self.text(&["--version".into()])?;
        if output.starts_with("mobilecli version ") {
            Ok(())
        } else {
            Err("mobilecli version check failed".to_owned())
        }
    }

    pub fn get_devices(&self, options: &[(&str, &str)]) -> Result<Vec<Device>, String> {
        let mut args = vec!["devices".to_owned()];
        for (key, value) in options {
            args.push(key.to_string());
            args.push(value.to_string());
        }
        let output = self.text(&args)?;
        let value: Value = serde_json::from_str(&output)
            .map_err(|error| format!("Invalid mobilecli devices response: {error}"))?;
        let data = value
            .get("data")
            .and_then(|data| data.get("devices"))
            .and_then(Value::as_array)
            .ok_or_else(|| "Invalid mobilecli devices response: missing data.devices".to_owned())?;
        let devices = data
            .iter()
            .filter_map(parse_mobilecli_device)
            .collect::<Vec<_>>();
        Ok(devices)
    }
}

fn parse_mobilecli_device(value: &Value) -> Option<Device> {
    let id = value.get("id")?.as_str()?.to_owned();
    let name = value.get("name")?.as_str()?.to_owned();
    let version = value
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let platform = match value.get("platform")?.as_str()? {
        "android" => Platform::Android,
        "ios" => Platform::Ios,
        _ => return None,
    };
    let device_type = match value.get("type")?.as_str()? {
        "real" => DeviceType::Real,
        "emulator" => DeviceType::Emulator,
        "simulator" => DeviceType::Simulator,
        _ => return None,
    };
    // This query always supplies includeOffline=false and the TypeScript
    // server reports every returned simulator as online regardless of any
    // additional state fields from mobilecli.
    Some(Device {
        id,
        name,
        platform,
        device_type,
        version,
        state: DeviceState::Online,
    })
}

pub fn discover_devices(runner: &CommandRunner, mobilecli: &MobileCli) -> Vec<Device> {
    let mut devices = AndroidDeviceManager::new(runner.clone())
        .get_connected_devices_with_details()
        .into_iter()
        .map(|device| Device {
            id: device.device_id,
            name: device.name,
            platform: Platform::Android,
            // The TypeScript MCP listing intentionally represents every
            // Android ADB device with mobilecli's `emulator` type, even though
            // the AndroidDeviceManager also exposes its TV/mobile form factor.
            device_type: DeviceType::Emulator,
            version: device.version,
            state: DeviceState::Online,
        })
        .collect::<Vec<_>>();

    if let Ok(ios_devices) = IosDeviceManager::new(runner.clone()).list_devices_with_details() {
        devices.extend(ios_devices.into_iter().map(|device| Device {
            id: device.device_id,
            name: device.device_name,
            platform: Platform::Ios,
            device_type: DeviceType::Real,
            version: device.version,
            state: DeviceState::Online,
        }));
    }

    if let Ok(simulators) = mobilecli.get_devices(&[("--platform", "ios"), ("--type", "simulator")])
    {
        devices.extend(
            simulators
                .into_iter()
                .filter(|device| device.state == DeviceState::Online),
        );
    }
    devices
}

pub fn find_device(runner: &CommandRunner, mobilecli: &MobileCli, id: &str) -> Option<Device> {
    discover_devices(runner, mobilecli)
        .into_iter()
        .find(|device| device.id == id)
}

fn adb_property(runner: &CommandRunner, device: &str, property: &str) -> Option<String> {
    // AndroidDeviceManager uses a 5-second timeout for model and version
    // lookups. Keep a caller's shorter timeout, and retain its output bound.
    let bounded_runner = CommandRunner {
        timeout: runner.timeout.min(Duration::from_secs(5)),
        max_output_bytes: runner.max_output_bytes,
    };
    let output = bounded_runner
        .run(adb_path(), ["-s", device, "shell", "getprop", property])
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
