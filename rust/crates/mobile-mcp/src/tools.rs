//! MCP tool catalog and mobile device tool dispatch.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use serde_json::{Map, Value, json};
use wait_timeout::ChildExt;

use crate::android::{AndroidError, AndroidRobot};
use crate::coord::{self, ScreenSize};
use crate::devices::{self, Device, DeviceType, IosDeviceManager, MobileCli, Platform};
use crate::ios::{IosAppError, PhysicalIosRobot};
use crate::ios_simulator::{Simctl, SimctlError};
use crate::runner::{CommandError, CommandRunner};
use crate::wda::{WdaClient, WdaError};

const SCREENSHOT_EXTENSIONS: &[&str] = &[".png", ".jpg", ".jpeg"];
const DEVICE_DESC: &str = "The device identifier to use. Use mobile_list_available_devices to find which devices are available to you.";
const IOS_TUNNEL_PORT: u16 = 60105;
const IOS_WDA_PORT: u16 = 8100;
const IOS_WDA_BUNDLE_ID: &str = "com.facebook.WebDriverAgentRunner.xctrunner";
const IOS_SETUP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolError {
    Actionable(String),
    Failure(String),
}

impl ToolError {
    fn actionable(message: impl Into<String>) -> Self {
        Self::Actionable(message.into())
    }
    fn failure(message: impl Into<String>) -> Self {
        Self::Failure(message.into())
    }
}

fn param(kind: &str, description: &str) -> Value {
    json!({"type":kind,"description":description})
}

fn tool_spec(
    name: &str,
    title: &str,
    description: &str,
    properties: Map<String, Value>,
    required: &[&str],
    annotations: Value,
) -> Value {
    json!({
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": { "type":"object", "properties": properties, "required": required, "additionalProperties": false },
        "annotations": annotations,
    })
}

fn fields(fields: impl IntoIterator<Item = (String, Value)>) -> Map<String, Value> {
    fields.into_iter().collect()
}

/// MCP tools/list schema. Names and argument keys match mobile-mcp's Zod registry.
pub fn tool_specs(normalized: bool, scale: u32, remote: bool) -> Vec<Value> {
    let device = || param("string", DEVICE_DESC);
    let swipe_distance_description = if normalized {
        format!(
            "The distance to swipe in 0-{scale} normalized coordinates. If not provided, defaults to a platform-appropriate value."
        )
    } else {
        "The distance to swipe in pixels. Defaults to 400 pixels for iOS or 30% of screen dimension for Android".to_owned()
    };
    let readonly = json!({"readOnlyHint":true});
    let destructive = json!({"destructiveHint":true});
    let mut result = vec![
        tool_spec("mobile_list_available_devices", "List Devices", "List all available devices. This includes both physical mobile devices and mobile simulators and emulators. It returns both Android and iOS devices.", fields([]), &[], readonly.clone()),
        tool_spec("mobile_list_apps", "List Apps", "List all the installed apps on the device", fields([("device".into(), device())]), &["device"], readonly.clone()),
        tool_spec("mobile_launch_app", "Launch App", "Launch an app on mobile device. Use this to open a specific app. You can find the package name of the app by calling list_apps_on_device.", fields([("device".into(), device()), ("packageName".into(), param("string", "The package name of the app to launch")), ("locale".into(), param("string", "Comma-separated BCP 47 locale tags to launch the app with (e.g., fr-FR,en-GB)"))]), &["device", "packageName"], destructive.clone()),
        tool_spec("mobile_terminate_app", "Terminate App", "Stop and terminate an app on mobile device", fields([("device".into(), device()), ("packageName".into(), param("string", "The package name of the app to terminate"))]), &["device", "packageName"], destructive.clone()),
        tool_spec("mobile_install_app", "Install App", "Install an app on mobile device", fields([("device".into(), device()), ("path".into(), param("string", "The path to the app file to install. For iOS simulators, provide a .zip file or a .app directory. For Android provide an .apk file. For iOS real devices provide an .ipa file")), ("grant_permissions".into(), param("boolean", "(Android only) Grant all runtime permissions on install (-g flag). Defaults to false.")), ("replace".into(), param("boolean", "(Android only) Replace existing application (-r flag). Defaults to true.")), ("allow_downgrade".into(), param("boolean", "(Android only) Allow version code downgrade (-d flag). Defaults to false.")), ("allow_test".into(), param("boolean", "(Android only) Allow test APKs (-t flag). Defaults to false."))]), &["device", "path"], destructive.clone()),
        tool_spec("mobile_uninstall_app", "Uninstall App", "Uninstall an app from mobile device", fields([("device".into(), device()), ("bundle_id".into(), param("string", "Bundle identifier (iOS) or package name (Android) of the app to be uninstalled"))]), &["device", "bundle_id"], destructive.clone()),
        tool_spec("mobile_get_screen_size", "Get Screen Size", "Get the screen size of the mobile device (returns width and height in device pixels).", fields([("device".into(), device())]), &["device"], readonly.clone()),
        tool_spec("mobile_click_on_screen_at_coordinates", "Click Screen", if normalized { format!("Click on the screen at given x,y coordinates. Note: mobile_list_elements_on_screen returns coordinates in device pixels — convert them to 0-{scale} normalized coordinates before passing to this tool.") } else { "Click on the screen at given x,y coordinates. If clicking on an element, use the list_elements_on_screen tool to find the coordinates.".to_owned() }.as_str(), fields([("device".into(), device()), ("x".into(), param("number", &coord_description("The x coordinate to click on the screen, in pixels", normalized, scale))), ("y".into(), param("number", &coord_description("The y coordinate to click on the screen, in pixels", normalized, scale)))]), &["device", "x", "y"], destructive.clone()),
        tool_spec("mobile_double_tap_on_screen", "Double Tap Screen", "Double-tap on the screen at given x,y coordinates.", fields([("device".into(), device()), ("x".into(), param("number", &coord_description("The x coordinate to double-tap, in pixels", normalized, scale))), ("y".into(), param("number", &coord_description("The y coordinate to double-tap, in pixels", normalized, scale)))]), &["device", "x", "y"], destructive.clone()),
        tool_spec("mobile_long_press_on_screen_at_coordinates", "Long Press Screen", if normalized { format!("Long press on the screen at given x,y coordinates. Note: mobile_list_elements_on_screen returns coordinates in device pixels — convert them to 0-{scale} normalized coordinates before passing to this tool.") } else { "Long press on the screen at given x,y coordinates. If long pressing on an element, use the list_elements_on_screen tool to find the coordinates.".to_owned() }.as_str(), fields([("device".into(), device()), ("x".into(), param("number", &coord_description("The x coordinate to long press on the screen, in pixels", normalized, scale))), ("y".into(), param("number", &coord_description("The y coordinate to long press on the screen, in pixels", normalized, scale))), ("duration".into(), json!({"type":"number","minimum":1,"maximum":10000,"description":"Duration of the long press in milliseconds. Defaults to 500ms."}))]), &["device", "x", "y"], destructive.clone()),
        tool_spec("mobile_list_elements_on_screen", "List Screen Elements", "List elements on screen and their coordinates (in device pixels), with display text or accessibility label. Do not cache this result.", fields([("device".into(), device())]), &["device"], readonly.clone()),
        tool_spec("mobile_press_button", "Press Button", "Press a button on device", fields([("device".into(), device()), ("button".into(), param("string", "The button to press. Supported buttons: BACK (android only), HOME, VOLUME_UP, VOLUME_DOWN, ENTER, DPAD_CENTER (android tv only), DPAD_UP (android tv only), DPAD_DOWN (android tv only), DPAD_LEFT (android tv only), DPAD_RIGHT (android tv only)"))]), &["device", "button"], destructive.clone()),
        tool_spec("mobile_open_url", "Open URL", "Open a URL in browser on device", fields([("device".into(), device()), ("url".into(), param("string", "The URL to open"))]), &["device", "url"], destructive.clone()),
        tool_spec("mobile_swipe_on_screen", "Swipe Screen", "Swipe on the screen", fields([("device".into(), device()), ("direction".into(), json!({"type":"string","enum":["up","down","left","right"],"description":"The direction to swipe"})), ("x".into(), param("number", &coord_description("The x coordinate to start the swipe from, in pixels. If not provided, uses center of screen", normalized, scale))), ("y".into(), param("number", &coord_description("The y coordinate to start the swipe from, in pixels. If not provided, uses center of screen", normalized, scale))), ("distance".into(), param("number", &swipe_distance_description))]), &["device", "direction"], destructive.clone()),
        tool_spec("mobile_type_keys", "Type Text", "Type text into the focused element", fields([("device".into(), device()), ("text".into(), param("string", "The text to type")), ("submit".into(), param("boolean", "Whether to submit the text. If true, the text will be submitted as if the user pressed the enter key."))]), &["device", "text", "submit"], destructive.clone()),
        tool_spec("mobile_save_screenshot", "Save Screenshot", "Save a screenshot of the mobile device to a file", fields([("device".into(), device()), ("saveTo".into(), param("string", "The path to save the screenshot to. Filename must end with .png, .jpg, or .jpeg"))]), &["device", "saveTo"], destructive.clone()),
        tool_spec("mobile_take_screenshot", "Take Screenshot", "Take a screenshot of the mobile device. Use this to understand what's on screen. Do not cache this result.", fields([("device".into(), device())]), &["device"], readonly.clone()),
        tool_spec("mobile_set_orientation", "Set Orientation", "Change the screen orientation of the device", fields([("device".into(), device()), ("orientation".into(), json!({"type":"string","enum":["portrait","landscape"],"description":"The desired orientation"}))]), &["device", "orientation"], destructive.clone()),
        tool_spec("mobile_get_orientation", "Get Orientation", "Get the current screen orientation of the device", fields([("device".into(), device())]), &["device"], readonly.clone()),
        tool_spec("mobile_start_screen_recording", "Start Screen Recording", "Start recording the screen of a mobile device. The recording runs in the background until stopped with mobile_stop_screen_recording. Returns the path where the recording will be saved.", fields([("device".into(), device()), ("output".into(), json!({"type":"string","description":"The file path to save the recording to. Filename must end with .mp4. If not provided, a temporary path will be used."})), ("timeLimit".into(), json!({"type":"number","description":"Maximum recording duration in seconds. The recording will stop automatically after this time."}))]), &["device"], destructive.clone()),
        tool_spec("mobile_stop_screen_recording", "Stop Screen Recording", "Stop an active screen recording on a mobile device. Returns the file path, size, and approximate duration of the recording.", fields([("device".into(), device())]), &["device"], destructive.clone()),
        tool_spec("mobile_list_crashes", "List Crash Reports", "List crash reports available on the device", fields([("device".into(), device())]), &["device"], readonly.clone()),
        tool_spec("mobile_get_crash", "Get Crash Report", "Get the full content of a crash report by its ID. Use mobile_list_crashes to find available crash IDs.", fields([("device".into(), device()), ("id".into(), param("string", "The crash report ID to retrieve"))]), &["device", "id"], readonly.clone()),
        tool_spec("mobile_ui_dump", "UI Hierarchy Dump", "(Android only) Dump the full UI hierarchy as raw XML using uiautomator. Unlike mobile_list_elements_on_screen which returns a filtered flat list of interactive elements as JSON, this returns the complete unfiltered XML tree preserving parent-child hierarchy and all node attributes (class, resource-id, absolute bounds in device pixels, clickable, scrollable, enabled, etc.). Use when you need the full view tree for debugging, or when mobile_list_elements_on_screen misses an element you can see on screen. Supports --compressed to reduce output size.", fields([("device".into(), device()), ("compressed".into(), json!({"type":"boolean","description":"Whether to use --compressed flag to reduce XML output size. Defaults to false."})), ("output_path".into(), json!({"type":"string","description":"If provided, save the XML to this local file path instead of returning it in the response."}))]), &["device"], readonly.clone()),
        tool_spec("mobile_adb_pull", "ADB Pull File", "(Android only) Pull (download) a file from the Android device to the local filesystem.", fields([("device".into(), device()), ("remote_path".into(), param("string", "The file path on the Android device to pull from")), ("local_path".into(), param("string", "The local file path to save the pulled file to."))]), &["device", "remote_path", "local_path"], destructive.clone()),
        tool_spec("mobile_adb_push", "ADB Push File", "(Android only) Push (upload) a file from local filesystem to the Android device. By default only allows pushing to /sdcard/. Set force=true to push to other paths.", fields([("device".into(), device()), ("local_path".into(), param("string", "The local file path to push to the device")), ("remote_path".into(), param("string", "The target file path on the device")), ("force".into(), json!({"type":"boolean","description":"Set to true to allow pushing to paths outside /sdcard/. Defaults to false."}))]), &["device", "local_path", "remote_path"], destructive.clone()),
    ];
    if remote {
        result.extend([
            tool_spec("mobile_list_remote_devices", "List Remote Devices", "List devices available in the remote fleet", fields([]), &[], readonly),
            tool_spec("mobile_allocate_remote_device", "Allocate Remote Device", "Reserve a device from the remote fleet", fields([("platform".into(), json!({"type":"string","enum":["ios","android"],"description":"The platform to allocate a device for"}))]), &["platform"], destructive.clone()),
            tool_spec("mobile_release_remote_device", "Release Remote Device", "Release a device back to the remote fleet", fields([("device".into(), param("string", "The device identifier to release back to the remote fleet"))]), &["device"], destructive),
        ]);
    }
    result
}

fn coord_description(text: &str, normalized: bool, scale: u32) -> String {
    if normalized {
        text.replace("in pixels", &format!("in 0-{scale} normalized coordinates"))
    } else {
        text.to_owned()
    }
}

#[derive(Debug)]
struct Recording {
    child: Child,
    path: PathBuf,
    started: SystemTime,
}

#[derive(Default)]
struct PhysicalIosProcesses {
    tunnel: Option<Child>,
    wda: Option<Child>,
}

pub struct MobileServer {
    runner: CommandRunner,
    mobilecli: MobileCli,
    recordings: HashMap<String, Recording>,
    physical_ios_processes: HashMap<String, PhysicalIosProcesses>,
    verified_simulators: HashMap<String, bool>,
    remote_enabled: bool,
    client_name: Option<String>,
}

impl Drop for MobileServer {
    fn drop(&mut self) {
        for (_, mut recording) in self.recordings.drain() {
            #[cfg(unix)]
            {
                // Use the same graceful stop as the explicit MCP tool, then
                // force termination after a short shutdown grace period.
                let _ = unsafe { libc::kill(recording.child.id() as i32, libc::SIGINT) };
            }
            #[cfg(windows)]
            {
                let _ = recording.child.kill();
            }
            match recording.child.wait_timeout(Duration::from_secs(2)) {
                Ok(Some(_)) => {}
                _ => {
                    let _ = recording.child.kill();
                    let _ = recording.child.wait();
                }
            }
        }
        for (_, mut processes) in self.physical_ios_processes.drain() {
            if let Some(child) = processes.wda.take() {
                stop_child(child);
            }
            if let Some(child) = processes.tunnel.take() {
                stop_child(child);
            }
        }
    }
}

impl Default for MobileServer {
    fn default() -> Self {
        let runner = CommandRunner::default();
        let server = Self {
            mobilecli: MobileCli::new(runner.clone()),
            runner,
            recordings: HashMap::new(),
            physical_ios_processes: HashMap::new(),
            verified_simulators: HashMap::new(),
            remote_enabled: std::env::var("MOBILEFLEET_ENABLE").is_ok_and(|value| value == "1"),
            client_name: None,
        };
        crate::telemetry::launch();
        server
    }
}

impl MobileServer {
    pub fn new(runner: CommandRunner, mobilecli: MobileCli) -> Self {
        let server = Self {
            runner,
            mobilecli,
            recordings: HashMap::new(),
            physical_ios_processes: HashMap::new(),
            verified_simulators: HashMap::new(),
            remote_enabled: false,
            client_name: None,
        };
        crate::telemetry::launch();
        server
    }

    pub(crate) fn set_client_name(&mut self, name: Option<&str>) {
        self.client_name = name.filter(|name| !name.is_empty()).map(str::to_owned);
    }

    pub fn tools(&self) -> Vec<Value> {
        tool_specs(
            coord::normalized_enabled(),
            coord::coordinate_scale(),
            self.remote_enabled,
        )
    }

    pub fn call_tool(&mut self, name: &str, mut args: Value) -> Result<Value, ToolError> {
        self.reap_finished_recordings();
        coerce_tool_numbers(name, &mut args)?;
        validate_tool_arguments(name, &mut args, &self.tools())?;
        let description = self
            .tools()
            .into_iter()
            .find(|tool| tool.get("name").and_then(Value::as_str) == Some(name))
            .and_then(|tool| {
                tool.get("description")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| name.to_owned());
        if name != "mobile_take_screenshot" {
            crate::logger::trace(&format!("Invoking {name} with args: {}", args));
        }

        let started = Instant::now();
        let result = (|| {
            let normalized = coord::normalized_enabled();
            let scale = coord::coordinate_scale();
            if normalized && has_device(name) {
                let device = get_string(&args, "device")?;
                let size = match coord::get_cached_screen_size(device) {
                    Some(size) => Some(size),
                    None => match self.screen_size(device) {
                        Ok(size) => {
                            coord::cache_screen_size(device, size.width, size.height);
                            Some(ScreenSize {
                                width: size.width,
                                height: size.height,
                            })
                        }
                        Err(error) => {
                            let message = match error {
                                ToolError::Actionable(message) | ToolError::Failure(message) => {
                                    message
                                }
                            };
                            crate::logger::trace(&format!(
                                "[coord-norm] Failed to get screen size for {device}: {message}. Coordinates will pass through without normalization."
                            ));
                            None
                        }
                    },
                };
                if let Some(size) = size {
                    coord::denormalize_args(name, &mut args, size, scale)
                        .map_err(ToolError::actionable)?;
                } else if coord::has_coord_fields(name) {
                    return Err(ToolError::actionable(
                        "Screen size unknown. Call mobile_get_screen_size first so coordinates can be converted correctly.",
                    ));
                }
            }

            match name {
                "mobile_list_available_devices" => self.list_devices(),
                "mobile_list_remote_devices" => self.remote("list-devices", &[]),
                "mobile_allocate_remote_device" => {
                    let platform = get_string(&args, "platform")?;
                    self.remote("allocate", &["--platform", platform])
                }
                "mobile_release_remote_device" => {
                    let device = get_string(&args, "device")?;
                    self.remote("release", &["--device", device])
                }
                "mobile_get_screen_size" => {
                    let device = get_string(&args, "device")?;
                    let size = self.screen_size(device)?;
                    let response = format!("Screen size is {}x{} pixels", size.width, size.height);
                    coord::ingest_screen_size(device, &response);
                    Ok(text_result(response))
                }
                "mobile_list_apps" => {
                    let device = get_string(&args, "device")?;
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        let apps = robot.list_apps().map_err(android_tool_error)?;
                        let labels = apps
                            .iter()
                            .map(|package| format!("{package} ({package})"))
                            .collect::<Vec<_>>()
                            .join(", ");
                        return Ok(text_result(format!("Found these apps on device: {labels}")));
                    }
                    if let Some(version) = self.physical_ios_version(device) {
                        self.ensure_ios_tunnel(device, &version)?;
                        let apps =
                            PhysicalIosRobot::new(device.to_owned(), version, self.runner.clone())
                                .list_apps()
                                .map_err(ios_app_tool_error)?;
                        let labels = apps
                            .iter()
                            .map(|app| format!("{} ({})", app.app_name, app.package_name))
                            .collect::<Vec<_>>()
                            .join(", ");
                        return Ok(text_result(format!("Found these apps on device: {labels}")));
                    }
                    let target = self.ensure_device(device)?;
                    if target.platform == Platform::Ios
                        && target.device_type == DeviceType::Simulator
                    {
                        let apps = Simctl::new(device.to_owned(), self.runner.clone())
                            .list_apps()
                            .map_err(simctl_tool_error)?;
                        let labels = apps
                            .iter()
                            .map(|app| {
                                format!(
                                    "{} ({})",
                                    app.app_name.as_deref().unwrap_or("undefined"),
                                    app.package_name.as_deref().unwrap_or("undefined")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        return Ok(text_result(format!("Found these apps on device: {labels}")));
                    }
                    let output = self.mobilecli_text(&["apps", "list", "--device", device])?;
                    let parsed = parse_data_array(&output, "apps")?;
                    let labels = parsed
                        .iter()
                        .map(|item| {
                            format!(
                                "{} ({})",
                                item.get("appName")
                                    .and_then(Value::as_str)
                                    .or_else(|| item.get("packageName").and_then(Value::as_str))
                                    .unwrap_or(""),
                                item.get("packageName")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    Ok(text_result(format!("Found these apps on device: {labels}")))
                }
                "mobile_launch_app" => {
                    let device = get_string(&args, "device")?;
                    let package = get_string(&args, "packageName")?;
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        robot
                            .launch_app(package, args.get("locale").and_then(Value::as_str))
                            .map_err(android_tool_error)?;
                        return Ok(text_result(format!("Launched app {package}")));
                    }
                    if let Some(version) = self.physical_ios_version(device) {
                        self.ensure_ios_tunnel(device, &version)?;
                        PhysicalIosRobot::new(device.to_owned(), version, self.runner.clone())
                            .launch_app(package, args.get("locale").and_then(Value::as_str))
                            .map_err(ios_app_tool_error)?;
                        return Ok(text_result(format!("Launched app {package}")));
                    }
                    let target = self.ensure_device(device)?;
                    if target.platform == Platform::Ios
                        && target.device_type == DeviceType::Simulator
                    {
                        Simctl::new(device.to_owned(), self.runner.clone())
                            .launch_app(package, args.get("locale").and_then(Value::as_str))
                            .map_err(simctl_tool_error)?;
                        return Ok(text_result(format!("Launched app {package}")));
                    }
                    let mut argv = vec!["apps", "launch", package];
                    if let Some(locale) = args.get("locale").and_then(Value::as_str) {
                        argv.extend(["--locale", locale]);
                    }
                    argv.extend(["--device", device]);
                    self.mobilecli_text(&argv)?;
                    Ok(text_result(format!("Launched app {package}")))
                }
                "mobile_terminate_app" => {
                    let device = get_string(&args, "device")?;
                    let package = get_string(&args, "packageName")?;
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        robot.terminate_app(package).map_err(android_tool_error)?;
                        return Ok(text_result(format!("Terminated app {package}")));
                    }
                    if let Some(version) = self.physical_ios_version(device) {
                        self.ensure_ios_tunnel(device, &version)?;
                        PhysicalIosRobot::new(device.to_owned(), version, self.runner.clone())
                            .terminate_app(package)
                            .map_err(ios_app_tool_error)?;
                        return Ok(text_result(format!("Terminated app {package}")));
                    }
                    let target = self.ensure_device(device)?;
                    if target.platform == Platform::Ios
                        && target.device_type == DeviceType::Simulator
                    {
                        Simctl::new(device.to_owned(), self.runner.clone())
                            .terminate_app(package)
                            .map_err(simctl_tool_error)?;
                        return Ok(text_result(format!("Terminated app {package}")));
                    }
                    self.mobilecli_text(&["apps", "terminate", package, "--device", device])?;
                    Ok(text_result(format!("Terminated app {package}")))
                }
                "mobile_install_app" => {
                    let device = get_string(&args, "device")?;
                    let path = get_string(&args, "path")?;
                    self.install_app(device, path, &args)?;
                    Ok(text_result(format!("Installed app from {path}")))
                }
                "mobile_uninstall_app" => {
                    let device = get_string(&args, "device")?;
                    let bundle = get_string(&args, "bundle_id")?;
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        robot.uninstall_app(bundle).map_err(android_tool_error)?;
                        return Ok(text_result(format!("Uninstalled app {bundle}")));
                    }
                    if let Some(version) = self.physical_ios_version(device) {
                        self.ensure_ios_tunnel(device, &version)?;
                        PhysicalIosRobot::new(device.to_owned(), version, self.runner.clone())
                            .uninstall_app(bundle)
                            .map_err(ios_app_tool_error)?;
                        return Ok(text_result(format!("Uninstalled app {bundle}")));
                    }
                    let target = self.ensure_device(device)?;
                    if target.platform == Platform::Ios
                        && target.device_type == DeviceType::Simulator
                    {
                        Simctl::new(device.to_owned(), self.runner.clone())
                            .uninstall_app(bundle)
                            .map_err(simctl_tool_error)?;
                        return Ok(text_result(format!("Uninstalled app {bundle}")));
                    }
                    self.mobilecli_text(&["apps", "uninstall", bundle, "--device", device])?;
                    Ok(text_result(format!("Uninstalled app {bundle}")))
                }
                "mobile_click_on_screen_at_coordinates" | "mobile_double_tap_on_screen" => {
                    let device = get_string(&args, "device")?;
                    let (x, y) = coordinates(&args)?;
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        if name == "mobile_double_tap_on_screen" {
                            robot.double_tap(x, y).map_err(android_tool_error)?;
                        } else {
                            robot.tap(x, y).map_err(android_tool_error)?;
                        }
                        let label = if name == "mobile_double_tap_on_screen" {
                            "Double-tapped"
                        } else {
                            "Clicked"
                        };
                        return Ok(text_result(format!(
                            "{label} on screen at coordinates: {x}, {y}"
                        )));
                    }
                    if name == "mobile_click_on_screen_at_coordinates" {
                        if let Some(wda) = self.ios_wda_for_id(device)? {
                            wda.tap(x, y).map_err(wda_tool_error)?;
                            return Ok(text_result(format!(
                                "Clicked on screen at coordinates: {x}, {y}"
                            )));
                        }
                    } else if let Some(wda) = self.ios_wda_for_id(device)? {
                        wda.double_tap(x, y).map_err(wda_tool_error)?;
                        return Ok(text_result(format!(
                            "Double-tapped on screen at coordinates: {x}, {y}"
                        )));
                    }
                    self.mobilecli_text(&["io", "tap", &format!("{x},{y}"), "--device", device])?;
                    if name == "mobile_double_tap_on_screen" {
                        self.mobilecli_text(&[
                            "io",
                            "tap",
                            &format!("{x},{y}"),
                            "--device",
                            device,
                        ])?;
                    }
                    let label = if name == "mobile_double_tap_on_screen" {
                        "Double-tapped"
                    } else {
                        "Clicked"
                    };
                    Ok(text_result(format!(
                        "{label} on screen at coordinates: {x}, {y}"
                    )))
                }
                "mobile_long_press_on_screen_at_coordinates" => {
                    let device = get_string(&args, "device")?;
                    let (x, y) = coordinates(&args)?;
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        let duration = args
                            .get("duration")
                            .and_then(Value::as_f64)
                            .unwrap_or(500.0);
                        if !(1.0..=10_000.0).contains(&duration) {
                            return Err(ToolError::failure("duration must be between 1 and 10000"));
                        }
                        robot
                            .long_press(x, y, duration)
                            .map_err(android_tool_error)?;
                        return Ok(text_result(format!(
                            "Long pressed on screen at coordinates: {x}, {y} for {duration}ms"
                        )));
                    }
                    if let Some(wda) = self.ios_wda_for_id(device)? {
                        let duration = args
                            .get("duration")
                            .and_then(Value::as_f64)
                            .unwrap_or(500.0);
                        if !(1.0..=10_000.0).contains(&duration) {
                            return Err(ToolError::failure("duration must be between 1 and 10000"));
                        }
                        wda.long_press(x, y, duration).map_err(wda_tool_error)?;
                        return Ok(text_result(format!(
                            "Long pressed on screen at coordinates: {x}, {y} for {duration}ms"
                        )));
                    }
                    let duration = args
                        .get("duration")
                        .and_then(Value::as_f64)
                        .unwrap_or(500.0);
                    if !(1.0..=10_000.0).contains(&duration) {
                        return Err(ToolError::failure("duration must be between 1 and 10000"));
                    }
                    self.mobilecli_text(&[
                        "io",
                        "longpress",
                        &format!("{x},{y}"),
                        "--duration",
                        &duration.to_string(),
                        "--device",
                        device,
                    ])?;
                    Ok(text_result(format!(
                        "Long pressed on screen at coordinates: {x}, {y} for {duration}ms"
                    )))
                }
                "mobile_list_elements_on_screen" => {
                    let device = get_string(&args, "device")?;
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        let elements =
                            robot.get_elements_on_screen().map_err(android_tool_error)?;
                        let elements = format_android_screen_elements(elements);
                        return Ok(text_result(format!(
                            "Found these elements on screen: {elements}"
                        )));
                    }
                    if let Some(wda) = self.ios_wda_for_id(device)? {
                        let elements =
                            Value::Array(wda.get_elements_on_screen().map_err(wda_tool_error)?);
                        return Ok(text_result(format!(
                            "Found these elements on screen: {elements}"
                        )));
                    }
                    self.ensure_device(device)?;
                    let output = self.mobilecli_text(&["dump", "ui", "--device", device])?;
                    let parsed: Value = serde_json::from_str(&output).map_err(|e| {
                        ToolError::failure(format!("Invalid UI dump response: {e}"))
                    })?;
                    let elements = parsed
                        .pointer("/data/elements")
                        .or_else(|| parsed.get("elements"))
                        .cloned()
                        .unwrap_or_else(|| json!([]));
                    Ok(text_result(format!(
                        "Found these elements on screen: {}",
                        elements
                    )))
                }
                "mobile_press_button" => {
                    let device = get_string(&args, "device")?;
                    let button = get_string(&args, "button")?;
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        robot.press_button(button).map_err(android_tool_error)?;
                        return Ok(text_result(format!("Pressed the button: {button}")));
                    }
                    if let Some(wda) = self.ios_wda_for_id(device)? {
                        wda.press_button(button).map_err(wda_tool_error)?;
                        return Ok(text_result(format!("Pressed the button: {button}")));
                    }
                    self.mobilecli_text(&["io", "button", button, "--device", device])?;
                    Ok(text_result(format!("Pressed the button: {button}")))
                }
                "mobile_open_url" => {
                    let device = get_string(&args, "device")?;
                    let url = get_string(&args, "url")?;
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        if !unsafe_urls_allowed()
                            && !url.starts_with("http://")
                            && !url.starts_with("https://")
                        {
                            return Err(ToolError::actionable(
                                "Only http:// and https:// URLs are allowed. Set MOBILEMCP_ALLOW_UNSAFE_URLS=1 to allow other URL schemes.",
                            ));
                        }
                        robot.open_url(url).map_err(android_tool_error)?;
                        return Ok(text_result(format!("Opened URL: {url}")));
                    }
                    if !unsafe_urls_allowed()
                        && !url.starts_with("http://")
                        && !url.starts_with("https://")
                    {
                        return Err(ToolError::actionable(
                            "Only http:// and https:// URLs are allowed. Set MOBILEMCP_ALLOW_UNSAFE_URLS=1 to allow other URL schemes.",
                        ));
                    }
                    if let Some(wda) = self.ios_wda_for_id(device)? {
                        wda.open_url(url).map_err(wda_tool_error)?;
                        return Ok(text_result(format!("Opened URL: {url}")));
                    }
                    self.mobilecli_text(&["url", url, "--device", device])?;
                    Ok(text_result(format!("Opened URL: {url}")))
                }
                "mobile_swipe_on_screen" => self.swipe(&args),
                "mobile_type_keys" => {
                    let device = get_string(&args, "device")?;
                    let text = get_string(&args, "text")?;
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        robot.send_keys(text).map_err(android_tool_error)?;
                        if args.get("submit").and_then(Value::as_bool).unwrap_or(false) {
                            robot.press_button("ENTER").map_err(android_tool_error)?;
                        }
                        return Ok(text_result(format!("Typed text: {text}")));
                    }
                    if let Some(wda) = self.ios_wda_for_id(device)? {
                        wda.send_keys(text).map_err(wda_tool_error)?;
                        if args.get("submit").and_then(Value::as_bool).unwrap_or(false) {
                            wda.press_button("ENTER").map_err(wda_tool_error)?;
                        }
                        return Ok(text_result(format!("Typed text: {text}")));
                    }
                    self.mobilecli_text(&["io", "text", text, "--device", device])?;
                    if args.get("submit").and_then(Value::as_bool).unwrap_or(false) {
                        self.mobilecli_text(&["io", "button", "ENTER", "--device", device])?;
                    }
                    Ok(text_result(format!("Typed text: {text}")))
                }
                "mobile_save_screenshot" => {
                    let device = get_string(&args, "device")?;
                    let path = get_string(&args, "saveTo")?;
                    validate_extension(path, SCREENSHOT_EXTENSIONS, "save_screenshot")?;
                    validate_output_path(path)?;
                    let screenshot = self.screenshot(device)?;
                    fs::write(path, screenshot).map_err(|e| {
                        ToolError::failure(format!("Failed writing screenshot: {e}"))
                    })?;
                    Ok(text_result(format!("Screenshot saved to: {path}")))
                }
                "mobile_take_screenshot" => {
                    let device = get_string(&args, "device").map_err(as_failure)?;
                    self.take_screenshot(device).map_err(as_failure)
                }
                "mobile_set_orientation" => {
                    let device = get_string(&args, "device")?;
                    let orientation = get_string(&args, "orientation")?;
                    if !matches!(orientation, "portrait" | "landscape") {
                        return Err(ToolError::failure(
                            "orientation must be portrait or landscape",
                        ));
                    }
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        robot
                            .set_orientation(orientation)
                            .map_err(android_tool_error)?;
                        coord::invalidate_screen_size(device);
                        return Ok(text_result(format!(
                            "Changed device orientation to {orientation}"
                        )));
                    }
                    if let Some(wda) = self.ios_wda_for_id(device)? {
                        wda.set_orientation(orientation).map_err(wda_tool_error)?;
                        coord::invalidate_screen_size(device);
                        return Ok(text_result(format!(
                            "Changed device orientation to {orientation}"
                        )));
                    }
                    self.mobilecli_text(&[
                        "device",
                        "orientation",
                        "set",
                        orientation,
                        "--device",
                        device,
                    ])?;
                    coord::invalidate_screen_size(device);
                    Ok(text_result(format!(
                        "Changed device orientation to {orientation}"
                    )))
                }
                "mobile_get_orientation" => {
                    let device = get_string(&args, "device")?;
                    if let Some(robot) = self.android_robot_if_connected(device) {
                        let orientation = robot.get_orientation().map_err(android_tool_error)?;
                        return Ok(text_result(format!(
                            "Current device orientation is {orientation}"
                        )));
                    }
                    if let Some(wda) = self.ios_wda_for_id(device)? {
                        let orientation = wda.get_orientation().map_err(wda_tool_error)?;
                        return Ok(text_result(format!(
                            "Current device orientation is {orientation}"
                        )));
                    }
                    let output =
                        self.mobilecli_text(&["device", "orientation", "get", "--device", device])?;
                    let orientation = parse_orientation(&output);
                    Ok(text_result(format!(
                        "Current device orientation is {orientation}"
                    )))
                }
                "mobile_start_screen_recording" => self.start_recording(&args),
                "mobile_stop_screen_recording" => self.stop_recording(get_string(&args, "device")?),
                "mobile_list_crashes" => {
                    let device = get_string(&args, "device")?;
                    self.ensure_mobilecli()?;
                    let output =
                        self.mobilecli_text(&["device", "crashes", "list", "--device", device])?;
                    let value: Value = serde_json::from_str(&output).map_err(|e| {
                        ToolError::failure(format!("Invalid crash list response: {e}"))
                    })?;
                    Ok(text_result(
                        value.get("data").cloned().unwrap_or(value).to_string(),
                    ))
                }
                "mobile_get_crash" => {
                    let device = get_string(&args, "device")?;
                    let id = get_string(&args, "id")?;
                    self.ensure_mobilecli()?;
                    let output =
                        self.mobilecli_text(&["device", "crashes", "get", id, "--device", device])?;
                    let value: Value = serde_json::from_str(&output)
                        .map_err(|e| ToolError::failure(format!("Invalid crash response: {e}")))?;
                    Ok(text_result(
                        value
                            .pointer("/data/content")
                            .and_then(Value::as_str)
                            .unwrap_or(&output)
                            .to_owned(),
                    ))
                }
                "mobile_ui_dump" => self.ui_dump(&args),
                "mobile_adb_pull" => self.adb_pull(&args),
                "mobile_adb_push" => self.adb_push(&args),
                _ => Err(ToolError::failure(format!(
                    "Unknown mobile-mcp tool: {name}"
                ))),
            }
        })();

        if name != "mobile_take_screenshot" {
            match &result {
                Ok(_) => crate::telemetry::tool_invoked(
                    name,
                    started.elapsed().as_millis(),
                    self.client_name.as_deref(),
                ),
                Err(_) => crate::telemetry::tool_failed(name, self.client_name.as_deref()),
            }
        }

        match &result {
            Ok(response) => {
                if let Some(text) = response.pointer("/content/0/text").and_then(Value::as_str) {
                    crate::logger::trace(&format!("=> {text}"));
                }
            }
            Err(ToolError::Actionable(_)) => {}
            Err(ToolError::Failure(error)) => {
                if name == "mobile_take_screenshot" {
                    crate::logger::error(&format!("Error taking screenshot: {error}"));
                } else {
                    crate::logger::trace(&format!("Tool '{description}' failed: {error}"));
                }
            }
        }
        result
    }

    fn ensure_mobilecli(&self) -> Result<(), ToolError> {
        self.mobilecli.check().map_err(|_| ToolError::actionable("mobilecli is not available or not working properly. Please review the documentation at https://github.com/mobile-next/mobile-mcp/wiki for installation instructions"))
    }

    fn ensure_device(&mut self, id: &str) -> Result<Device, ToolError> {
        self.ensure_mobilecli()?;
        let device = devices::find_device(&self.runner, &self.mobilecli, id).ok_or_else(|| ToolError::actionable(format!("Device \"{id}\" not found. Use the mobile_list_available_devices tool to see available devices.")))?;
        if device.device_type == DeviceType::Simulator && !self.verified_simulators.contains_key(id)
        {
            crate::ios_simulator::ensure_webdriver_agent(&self.mobilecli, id)
                .map_err(ToolError::failure)?;
            self.verified_simulators.insert(id.to_owned(), true);
        }
        let (platform, device_type) = match (&device.platform, &device.device_type) {
            (Platform::Android, _) => ("android", None),
            (Platform::Ios, DeviceType::Real) => ("ios", Some("real")),
            (Platform::Ios, DeviceType::Simulator | DeviceType::Emulator) => {
                ("ios", Some("simulator"))
            }
        };
        crate::telemetry::robot_selected(platform, device_type, self.client_name.as_deref());
        Ok(device)
    }

    fn physical_ios_version(&self, id: &str) -> Option<String> {
        let Ok(devices) = IosDeviceManager::new(self.runner.clone()).list_devices_with_details()
        else {
            return None;
        };
        devices
            .into_iter()
            .find(|device| device.device_id == id)
            .map(|device| device.version)
    }

    fn ios_wda_for_id(&mut self, id: &str) -> Result<Option<WdaClient>, ToolError> {
        // A missing physical device means this remains on the existing
        // simulator/mobilecli path, without triggering simulator WDA setup.
        let Some(version) = self.physical_ios_version(id) else {
            return Ok(None);
        };
        self.ios_wda_for_version(id, &version)
    }

    fn ios_wda_for_device(&mut self, device: &Device) -> Result<Option<WdaClient>, ToolError> {
        if device.platform != Platform::Ios || device.device_type != DeviceType::Real {
            return Ok(None);
        }
        self.ios_wda_for_version(&device.id, &device.version)
    }

    fn ensure_ios_tunnel(&mut self, id: &str, version: &str) -> Result<(), ToolError> {
        if !ios_major_version(version).is_some_and(|major| major >= 17)
            || WdaClient::port_is_listening(IOS_TUNNEL_PORT)
        {
            return Ok(());
        }

        let processes = self
            .physical_ios_processes
            .entry(id.to_owned())
            .or_default();
        reap_child(&mut processes.tunnel);
        if processes.tunnel.is_none() {
            let args = vec![
                "--udid".to_owned(),
                id.to_owned(),
                "tunnel".to_owned(),
                "start".to_owned(),
            ];
            processes.tunnel = Some(
                self.runner
                    .spawn(devices::go_ios_path(), args)
                    .map_err(|error| {
                        ToolError::actionable(format!(
                            "Could not start the iOS tunnel for device \"{id}\": {error}. Run `sudo ios tunnel start --udid {id}` (or start the tunnel with GO_IOS_PATH) and retry. See https://github.com/mobile-next/mobile-mcp/wiki/"
                        ))
                    })?,
            );
        }

        if wait_for_port(IOS_TUNNEL_PORT, &mut processes.tunnel, IOS_SETUP_TIMEOUT) {
            return Ok(());
        }
        if let Some(child) = processes.tunnel.take() {
            stop_child(child);
        }
        Err(ToolError::actionable(format!(
            "Could not start the iOS 17+ tunnel for device \"{id}\" within {} seconds. The go-ios tunnel may require elevated privileges; run `sudo ios tunnel start --udid {id}` and retry. See https://github.com/mobile-next/mobile-mcp/wiki/",
            IOS_SETUP_TIMEOUT.as_secs()
        )))
    }

    fn ensure_wda_installed(&self, id: &str) -> Result<(), ToolError> {
        let output = self
            .runner
            .run_checked(
                devices::go_ios_path(),
                [
                    "--udid".to_owned(),
                    id.to_owned(),
                    "apps".to_owned(),
                    "--all".to_owned(),
                    "--list".to_owned(),
                ],
            )
            .map_err(|error| ToolError::actionable(command_error_detail(error)))?;
        let installed = String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .any(|bundle_id| bundle_id == IOS_WDA_BUNDLE_ID);
        if installed {
            return Ok(());
        }

        let (Some(p12), Some(profile)) = (
            std::env::var_os("GO_IOS_WDA_P12"),
            std::env::var_os("GO_IOS_WDA_PROFILE"),
        ) else {
            return Err(ToolError::actionable(format!(
                "WebDriverAgent is not installed on device \"{id}\". To install it automatically, configure GO_IOS_WDA_P12 and GO_IOS_WDA_PROFILE with an iOS signing certificate and provisioning profile; otherwise install WDA manually. See https://github.com/mobile-next/mobile-mcp/wiki/"
            )));
        };

        let mut args = vec![
            "--udid".to_owned(),
            id.to_owned(),
            "ui".to_owned(),
            "install".to_owned(),
            "wda".to_owned(),
            format!("--p12file={}", p12.to_string_lossy()),
            format!("--profile={}", profile.to_string_lossy()),
        ];
        if let Some(password) = std::env::var_os("GO_IOS_WDA_P12_PASSWORD") {
            args.push(format!("--p12password={}", password.to_string_lossy()));
        }
        self.runner
            .run_checked(devices::go_ios_path(), args)
            .map_err(|error| {
                ToolError::actionable(format!(
                    "Failed to install WebDriverAgent on device \"{id}\": {}. Check the signing inputs and device provisioning, then retry. See https://github.com/mobile-next/mobile-mcp/wiki/",
                    command_error_detail(error)
                ))
            })?;
        Ok(())
    }

    fn ios_wda_for_version(
        &mut self,
        id: &str,
        version: &str,
    ) -> Result<Option<WdaClient>, ToolError> {
        self.ensure_ios_tunnel(id, version)?;
        let wda = WdaClient::new();
        if wda.is_running() {
            return Ok(Some(wda));
        }
        if WdaClient::port_is_listening(IOS_WDA_PORT) {
            return Err(ToolError::actionable(
                "Port 8100 is occupied but WebDriverAgent is not ready (tunnel okay), please see https://github.com/mobile-next/mobile-mcp/wiki/",
            ));
        }

        self.ensure_wda_installed(id)?;
        let processes = self
            .physical_ios_processes
            .entry(id.to_owned())
            .or_default();
        reap_child(&mut processes.wda);
        if processes.wda.is_none() {
            let args = vec![
                "--udid".to_owned(),
                id.to_owned(),
                "ui".to_owned(),
                "run".to_owned(),
                "wda".to_owned(),
            ];
            processes.wda = Some(
                self.runner
                    .spawn(devices::go_ios_path(), args)
                    .map_err(|error| {
                        ToolError::actionable(format!(
                            "Could not start WebDriverAgent for device \"{id}\": {error}. Start WDA and its port forward manually, then retry. See https://github.com/mobile-next/mobile-mcp/wiki/"
                        ))
                    })?,
            );
        }

        if wait_for_wda(&mut processes.wda, IOS_SETUP_TIMEOUT) {
            return Ok(Some(wda));
        }
        if let Some(child) = processes.wda.take() {
            stop_child(child);
        }
        Err(ToolError::actionable(format!(
            "WebDriverAgent did not become ready on device \"{id}\" within {} seconds. Check that the WDA runner is installed, signed for this device, and supported by the installed go-ios version. See https://github.com/mobile-next/mobile-mcp/wiki/",
            IOS_SETUP_TIMEOUT.as_secs()
        )))
    }

    fn android_robot_if_connected(&self, id: &str) -> Option<AndroidRobot> {
        let output = self.runner.run(devices::adb_path(), ["devices"]).ok()?;
        if !output.status.success()
            || !String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|line| line.split_once('\t'))
                .any(|(device, state)| device == id && state.trim() == "device")
        {
            return None;
        }
        crate::telemetry::robot_selected("android", None, self.client_name.as_deref());
        Some(AndroidRobot::new(id.to_owned(), self.runner.clone()))
    }

    fn mobilecli_text(&self, args: &[&str]) -> Result<String, ToolError> {
        self.mobilecli
            .text(&args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>())
            .map_err(ToolError::failure)
    }

    fn list_devices(&self) -> Result<Value, ToolError> {
        self.ensure_mobilecli()?;
        let devices = devices::discover_devices(&self.runner, &self.mobilecli);
        Ok(json!({"content":[{"type":"text","text":json!({"devices":devices}).to_string()}]}))
    }

    fn remote(&self, action: &str, args: &[&str]) -> Result<Value, ToolError> {
        if !self.remote_enabled {
            return Err(ToolError::failure("Remote device fleet is disabled"));
        }
        self.ensure_mobilecli()?;
        let mut argv = vec!["remote".to_owned(), action.to_owned()];
        argv.extend(args.iter().map(|arg| (*arg).to_owned()));
        Ok(text_result(
            self.mobilecli.text(&argv).map_err(ToolError::failure)?,
        ))
    }

    fn screen_size(&mut self, id: &str) -> Result<ScreenSize, ToolError> {
        self.screen_info(id).map(|(size, _scale)| size)
    }

    fn screen_info(&mut self, id: &str) -> Result<(ScreenSize, f64), ToolError> {
        if let Some(robot) = self.android_robot_if_connected(id) {
            let size = robot.get_screen_size().map_err(android_tool_error)?;
            return Ok((size, 1.0));
        }
        let device = self.ensure_device(id)?;
        if device.platform == Platform::Android {
            let size = AndroidRobot::new(id.to_owned(), self.runner.clone())
                .get_screen_size()
                .map_err(android_tool_error)?;
            return Ok((size, 1.0));
        }
        if let Some(wda) = self.ios_wda_for_device(&device)? {
            let (width, height, scale) = wda.screen_size().map_err(wda_tool_error)?;
            return Ok((ScreenSize { width, height }, scale));
        }
        let output = self.mobilecli_text(&["device", "info", "--device", id])?;
        let value: Value = serde_json::from_str(&output).map_err(|error| {
            ToolError::failure(format!("Invalid device info response: {error}"))
        })?;
        let size = value
            .pointer("/data/device/screenSize")
            .ok_or_else(|| ToolError::actionable("Screen size is unavailable for this device"))?;
        let width = size.get("width").and_then(Value::as_u64).unwrap_or(0) as u32;
        let height = size.get("height").and_then(Value::as_u64).unwrap_or(0) as u32;
        let scale = size
            .get("scale")
            .and_then(Value::as_f64)
            .filter(|scale| *scale > 0.0)
            .unwrap_or(1.0);
        Ok((ScreenSize { width, height }, scale))
    }

    fn install_app(&mut self, id: &str, path: &str, args: &Value) -> Result<(), ToolError> {
        if let Some(robot) = self.android_robot_if_connected(id) {
            robot
                .install_app(
                    path,
                    args.get("replace").and_then(Value::as_bool) != Some(false),
                    args.get("grant_permissions").and_then(Value::as_bool) == Some(true),
                    args.get("allow_downgrade").and_then(Value::as_bool) == Some(true),
                    args.get("allow_test").and_then(Value::as_bool) == Some(true),
                )
                .map_err(android_tool_error)?;
            return Ok(());
        }
        if let Some(version) = self.physical_ios_version(id) {
            self.ensure_ios_tunnel(id, &version)?;
            PhysicalIosRobot::new(id.to_owned(), version, self.runner.clone())
                .install_app(path)
                .map_err(ios_app_tool_error)?;
            return Ok(());
        }
        let device = self.ensure_device(id)?;
        if device.platform == Platform::Android {
            AndroidRobot::new(id.to_owned(), self.runner.clone())
                .install_app(
                    path,
                    args.get("replace").and_then(Value::as_bool) != Some(false),
                    args.get("grant_permissions").and_then(Value::as_bool) == Some(true),
                    args.get("allow_downgrade").and_then(Value::as_bool) == Some(true),
                    args.get("allow_test").and_then(Value::as_bool) == Some(true),
                )
                .map_err(android_tool_error)?;
        } else if device.platform == Platform::Ios && device.device_type == DeviceType::Simulator {
            Simctl::new(id.to_owned(), self.runner.clone())
                .install_app(path)
                .map_err(simctl_tool_error)?;
        } else {
            self.mobilecli_text(&["apps", "install", path, "--device", id])?;
        }
        Ok(())
    }

    fn swipe(&mut self, args: &Value) -> Result<Value, ToolError> {
        let id = get_string(args, "device")?;
        let direction = get_string(args, "direction")?;
        if !matches!(direction, "up" | "down" | "left" | "right") {
            return Err(ToolError::failure(
                "direction must be up, down, left, or right",
            ));
        }
        if let Some(robot) = self.android_robot_if_connected(id) {
            let x = args.get("x").and_then(Value::as_f64);
            let y = args.get("y").and_then(Value::as_f64);
            let distance = args.get("distance").and_then(Value::as_f64);
            if let (Some(x), Some(y)) = (x, y) {
                robot
                    .swipe_from_coordinate(x, y, direction, distance)
                    .map_err(android_tool_error)?;
                let detail = distance
                    .filter(|distance| *distance != 0.0)
                    .map(|distance| format!(" {distance} pixels"))
                    .unwrap_or_default();
                return Ok(text_result(format!(
                    "Swiped {direction}{detail} from coordinates: {x}, {y}"
                )));
            }
            robot.swipe(direction).map_err(android_tool_error)?;
            return Ok(text_result(format!("Swiped {direction} on screen")));
        }
        if let Some(wda) = self.ios_wda_for_id(id)? {
            if let (Some(x), Some(y)) = (
                args.get("x").and_then(Value::as_f64),
                args.get("y").and_then(Value::as_f64),
            ) {
                let distance = args
                    .get("distance")
                    .and_then(Value::as_f64)
                    .unwrap_or(400.0);
                wda.swipe_from_coordinate(x, y, direction, distance)
                    .map_err(wda_tool_error)?;
                let detail = if distance != 0.0 {
                    format!(" {distance} pixels")
                } else {
                    String::new()
                };
                return Ok(text_result(format!(
                    "Swiped {direction}{detail} from coordinates: {x}, {y}"
                )));
            }
            wda.swipe(direction).map_err(wda_tool_error)?;
            return Ok(text_result(format!("Swiped {direction} on screen")));
        }
        let (screen, _scale) = self.screen_info(id)?;
        let x = args.get("x").and_then(Value::as_f64);
        let y = args.get("y").and_then(Value::as_f64);
        let distance = args.get("distance").and_then(Value::as_f64);
        if let (Some(x), Some(y)) = (x, y) {
            let show_distance = distance.is_some_and(|distance| distance != 0.0);
            let default = if direction == "up" || direction == "down" {
                f64::from(screen.height) * 0.3
            } else {
                f64::from(screen.width) * 0.3
            };
            let distance = distance.filter(|d| *d != 0.0).unwrap_or(default);
            let (ex, ey) = match direction {
                "up" => (x, (y - distance).max(0.0)),
                "down" => (x, (y + distance).min(f64::from(screen.height))),
                "left" => ((x - distance).max(0.0), y),
                _ => ((x + distance).min(f64::from(screen.width)), y),
            };
            self.mobilecli_text(&["io", "swipe", &format!("{x},{y},{ex},{ey}"), "--device", id])?;
            let detail = if show_distance {
                format!(" {distance} pixels")
            } else {
                String::new()
            };
            Ok(text_result(format!(
                "Swiped {direction}{detail} from coordinates: {x}, {y}"
            )))
        } else {
            self.mobilecli_text(&["io", "swipe", direction, "--device", id])?;
            Ok(text_result(format!("Swiped {direction} on screen")))
        }
    }

    fn screenshot(&mut self, id: &str) -> Result<Vec<u8>, ToolError> {
        if let Some(robot) = self.android_robot_if_connected(id) {
            return robot.get_screenshot().map_err(android_tool_error);
        }
        let device = self.ensure_device(id)?;
        if device.platform == Platform::Android {
            return AndroidRobot::new(id.to_owned(), self.runner.clone())
                .get_screenshot()
                .map_err(android_tool_error);
        }
        if let Some(wda) = self.ios_wda_for_device(&device)? {
            return wda.screenshot().map_err(wda_tool_error);
        }
        self.mobilecli
            .execute(&[
                "screenshot".into(),
                "--device".into(),
                id.into(),
                "--format".into(),
                "png".into(),
                "--output".into(),
                "-".into(),
            ])
            .map_err(ToolError::failure)
    }

    fn take_screenshot(&mut self, id: &str) -> Result<Value, ToolError> {
        let (_screen, scale) = self.screen_info(id)?;
        let mut image = self.screenshot(id)?;
        let (width, height) =
            png_dimensions(&image).ok_or_else(|| ToolError::failure("Not a valid PNG file"))?;
        if width == 0 || height == 0 {
            return Err(ToolError::actionable(
                "Screenshot is invalid. Please try again.",
            ));
        }
        let mut mime = "image/png";
        let before_size = image.len();
        if let Some(scaled) = resize_screenshot(
            &self.runner,
            &image,
            (f64::from(width) / scale).floor().max(1.0) as u32,
        )
        .map_err(ToolError::failure)?
        {
            image = scaled;
            mime = "image/jpeg";
            crate::logger::trace(&format!(
                "Screenshot resized from {before_size} bytes to {} bytes",
                image.len()
            ));
        }
        crate::logger::trace(&format!("Screenshot taken: {} bytes", image.len()));
        let screenshot64 = base64::engine::general_purpose::STANDARD.encode(&image);
        crate::telemetry::screenshot_taken(
            screenshot64.len(),
            mime,
            width,
            height,
            self.client_name.as_deref(),
        );
        let mut content = vec![json!({"type":"image","data":screenshot64,"mimeType":mime})];
        if coord::normalized_enabled() {
            content.push(json!({"type":"text","text":format!("Use 0-{} normalized coordinates when clicking on positions from this screenshot. The actual image size may differ from the coordinate space.",coord::coordinate_scale())}));
        }
        Ok(json!({"content":content}))
    }

    fn ui_dump(&mut self, args: &Value) -> Result<Value, ToolError> {
        let id = get_string(args, "device")?;
        let robot = match self.android_robot_if_connected(id) {
            Some(robot) => robot,
            None => {
                let device = self.ensure_device(id)?;
                if device.platform != Platform::Android {
                    return Err(ToolError::actionable(
                        "mobile_ui_dump is only supported on Android devices.",
                    ));
                }
                AndroidRobot::new(id.to_owned(), self.runner.clone())
            }
        };
        let xml = robot
            .dump_ui_hierarchy(args.get("compressed").and_then(Value::as_bool) == Some(true))
            .map_err(android_tool_error)?;
        if let Some(path) = args.get("output_path").and_then(Value::as_str) {
            validate_output_path(path)?;
            fs::write(path, &xml).map_err(|e| ToolError::failure(e.to_string()))?;
            return Ok(text_result(format!("UI hierarchy XML saved to: {path}")));
        }
        Ok(text_result(xml))
    }

    fn adb(&self, args: &[String]) -> Result<Vec<u8>, String> {
        let output = self
            .runner
            .run(devices::adb_path(), args)
            .map_err(|e| e.to_string())?;
        if output.status.success() {
            Ok(output.stdout)
        } else {
            let msg = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            Err(if msg.is_empty() {
                format!("adb exited with {}", output.status)
            } else {
                msg
            })
        }
    }

    fn adb_pull(&mut self, args: &Value) -> Result<Value, ToolError> {
        let id = get_string(args, "device")?;
        let remote = get_string(args, "remote_path")?;
        let local = get_string(args, "local_path")?;
        if let Some(robot) = self.android_robot_if_connected(id) {
            validate_output_path(local)?;
            robot.pull_file(remote, local).map_err(android_tool_error)?;
            return Ok(text_result(format!(
                "Successfully pulled {remote} to {local}"
            )));
        }
        self.require_android(id, "mobile_adb_pull")?;
        validate_output_path(local)?;
        self.adb(&[
            "-s".into(),
            id.into(),
            "pull".into(),
            remote.into(),
            local.into(),
        ])
        .map_err(ToolError::actionable)?;
        Ok(text_result(format!(
            "Successfully pulled {remote} to {local}"
        )))
    }

    fn adb_push(&mut self, args: &Value) -> Result<Value, ToolError> {
        let id = get_string(args, "device")?;
        let local = get_string(args, "local_path")?;
        let remote = get_string(args, "remote_path")?;
        if let Some(robot) = self.android_robot_if_connected(id) {
            if args.get("force").and_then(Value::as_bool) != Some(true) {
                let resolved = posix_resolve(remote);
                if !resolved.starts_with("/sdcard/") {
                    return Err(ToolError::actionable(format!(
                        "Push target path must resolve under /sdcard/ for safety. Got: \"{remote}\" (resolved: \"{resolved}\"). Set force=true to override."
                    )));
                }
            }
            if !Path::new(local).exists() {
                return Err(ToolError::actionable(format!(
                    "Local file not found: \"{local}\""
                )));
            }
            robot.push_file(local, remote).map_err(android_tool_error)?;
            return Ok(text_result(format!(
                "Successfully pushed {local} to {remote}"
            )));
        }
        self.require_android(id, "mobile_adb_push")?;
        if args.get("force").and_then(Value::as_bool) != Some(true) {
            let resolved = posix_resolve(remote);
            if !resolved.starts_with("/sdcard/") {
                return Err(ToolError::actionable(format!(
                    "Push target path must resolve under /sdcard/ for safety. Got: \"{remote}\" (resolved: \"{resolved}\"). Set force=true to override."
                )));
            }
        }
        if !Path::new(local).exists() {
            return Err(ToolError::actionable(format!(
                "Local file not found: \"{local}\""
            )));
        }
        self.adb(&[
            "-s".into(),
            id.into(),
            "push".into(),
            local.into(),
            remote.into(),
        ])
        .map_err(ToolError::actionable)?;
        Ok(text_result(format!(
            "Successfully pushed {local} to {remote}"
        )))
    }

    fn require_android(&mut self, id: &str, tool: &str) -> Result<Device, ToolError> {
        let device = self.ensure_device(id)?;
        if device.platform != Platform::Android {
            return Err(ToolError::actionable(format!(
                "{tool} is only supported on Android devices."
            )));
        }
        Ok(device)
    }

    fn start_recording(&mut self, args: &Value) -> Result<Value, ToolError> {
        let id = get_string(args, "device")?;
        let output = args.get("output").and_then(Value::as_str);
        if let Some(path) = output {
            validate_extension(path, &[".mp4"], "start_screen_recording")?;
            validate_output_path(path)?;
        }
        self.ensure_device(id)?;
        if self.recordings.contains_key(id) {
            return Err(ToolError::actionable(format!(
                "Device \"{id}\" is already being recorded. Stop the current recording first with mobile_stop_screen_recording."
            )));
        }
        let path = output.map(PathBuf::from).unwrap_or_else(|| {
            std::env::temp_dir().join(format!("screen-recording-{}.mp4", now_millis()))
        });
        let mut argv = vec![
            "screenrecord".to_owned(),
            "--device".to_owned(),
            id.to_owned(),
            "--output".to_owned(),
            path.to_string_lossy().into_owned(),
            "--silent".to_owned(),
        ];
        if let Some(limit) = args.get("timeLimit").and_then(Value::as_f64) {
            argv.extend(["--time-limit".into(), limit.to_string()]);
        }
        // TypeScript uses this shared mobilecli path for physical iOS too;
        // screen recording does not require the WDA tunnel/session path.
        let child = self
            .mobilecli
            .spawn(argv)
            .map_err(|error| ToolError::actionable(error.to_string()))?;
        self.recordings.insert(
            id.to_owned(),
            Recording {
                child,
                path: path.clone(),
                started: SystemTime::now(),
            },
        );
        Ok(text_result(format!(
            "Screen recording started. Output will be saved to: {}",
            path.display()
        )))
    }

    fn reap_finished_recordings(&mut self) {
        let finished = self
            .recordings
            .iter_mut()
            .filter_map(|(device, recording)| match recording.child.try_wait() {
                Ok(Some(_)) => Some(device.clone()),
                Ok(None) | Err(_) => None,
            })
            .collect::<Vec<_>>();
        for device in finished {
            self.recordings.remove(&device);
        }
    }

    fn stop_recording(&mut self, id: &str) -> Result<Value, ToolError> {
        let mut recording=self.recordings.remove(id).ok_or_else(||ToolError::actionable(format!("No active recording found for device \"{id}\". Start a recording first with mobile_start_screen_recording.")))?;
        #[cfg(unix)]
        {
            let _ = self.runner.run(
                "kill",
                ["-INT".to_owned(), recording.child.id().to_string()],
            );
        }
        #[cfg(windows)]
        {
            let _ = recording.child.kill();
        }
        match recording
            .child
            .wait_timeout(Duration::from_secs(300))
            .map_err(|e| ToolError::failure(e.to_string()))?
        {
            Some(_) => {}
            None => {
                let _ = recording.child.kill();
                let _ = recording.child.wait();
            }
        }
        let duration = SystemTime::now()
            .duration_since(recording.started)
            .unwrap_or_default()
            .as_secs_f64()
            .round() as u64;
        if !recording.path.exists() {
            return Ok(text_result(format!(
                "Recording stopped after ~{duration}s but the output file was not found at: {}",
                recording.path.display()
            )));
        }
        let size = fs::metadata(&recording.path)
            .map(|metadata| metadata.len())
            .unwrap_or(0) as f64
            / 1024.0
            / 1024.0;
        Ok(text_result(format!(
            "Recording stopped. File: {} ({size:.2} MB, ~{duration}s)",
            recording.path.display()
        )))
    }
}

fn has_device(name: &str) -> bool {
    name != "mobile_list_available_devices"
        && !name.starts_with("mobile_list_remote")
        && name != "mobile_allocate_remote_device"
}

fn unsafe_urls_allowed() -> bool {
    std::env::var("MOBILEMCP_ALLOW_UNSAFE_URLS").is_ok_and(|value| value == "1")
}

fn coerce_tool_numbers(name: &str, args: &mut Value) -> Result<(), ToolError> {
    let fields: &[&str] = match name {
        "mobile_click_on_screen_at_coordinates" | "mobile_double_tap_on_screen" => &["x", "y"],
        "mobile_long_press_on_screen_at_coordinates" => &["x", "y", "duration"],
        "mobile_swipe_on_screen" => &["x", "y", "distance"],
        "mobile_start_screen_recording" => &["timeLimit"],
        _ => return Ok(()),
    };
    let Some(object) = args.as_object_mut() else {
        return Ok(());
    };
    for field in fields {
        let Some(value) = object.get(*field) else {
            continue;
        };
        let number = js_number(value)
            .filter(|number| number.is_finite())
            .ok_or_else(|| ToolError::failure(format!("{field} must be a number")))?;
        let number = serde_json::Number::from_f64(number)
            .ok_or_else(|| ToolError::failure(format!("{field} must be a finite number")))?;
        object.insert((*field).to_owned(), Value::Number(number));
    }
    Ok(())
}

fn js_number(value: &Value) -> Option<f64> {
    match value {
        Value::Null => Some(0.0),
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::Number(value) => value.as_f64(),
        Value::String(value) => parse_js_number(value),
        Value::Array(values) => parse_js_number(&js_array_string(values)),
        Value::Object(_) => None,
    }
}

fn js_array_string(values: &[Value]) -> String {
    values
        .iter()
        .map(|value| match value {
            Value::Null => String::new(),
            Value::Bool(value) => value.to_string(),
            Value::Number(value) => value.to_string(),
            Value::String(value) => value.clone(),
            Value::Array(values) => js_array_string(values),
            Value::Object(_) => "[object Object]".to_owned(),
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_js_number(value: &str) -> Option<f64> {
    let value =
        value.trim_matches(|character: char| character.is_whitespace() || character == '\u{FEFF}');
    if value.is_empty() {
        return Some(0.0);
    }
    for (prefix, radix) in [("0x", 16), ("0b", 2), ("0o", 8)] {
        if let Some(digits) = value
            .strip_prefix(prefix)
            .or_else(|| value.strip_prefix(&prefix.to_ascii_uppercase()))
        {
            if digits.is_empty() {
                return None;
            }
            return u64::from_str_radix(digits, radix)
                .ok()
                .map(|number| number as f64);
        }
    }
    value.parse::<f64>().ok()
}

fn validate_tool_arguments(name: &str, args: &mut Value, specs: &[Value]) -> Result<(), ToolError> {
    let Some(spec) = specs
        .iter()
        .find(|spec| spec.get("name").and_then(Value::as_str) == Some(name))
    else {
        return Ok(());
    };
    let Some(object) = args.as_object_mut() else {
        return Err(ToolError::failure("Tool arguments must be an object"));
    };
    let properties = spec
        .pointer("/inputSchema/properties")
        .and_then(Value::as_object)
        .ok_or_else(|| ToolError::failure("Tool schema is invalid"))?;
    let required = spec
        .pointer("/inputSchema/required")
        .and_then(Value::as_array)
        .ok_or_else(|| ToolError::failure("Tool schema is invalid"))?;

    for field in required.iter().filter_map(Value::as_str) {
        if !object.contains_key(field) {
            return Err(ToolError::failure(format!(
                "Missing required argument: {field}"
            )));
        }
    }
    object.retain(|field, _| properties.contains_key(field));

    for (field, value) in object.iter() {
        let schema = properties
            .get(field)
            .and_then(Value::as_object)
            .ok_or_else(|| ToolError::failure("Tool schema is invalid"))?;
        let expected = schema.get("type").and_then(Value::as_str);
        let valid_type = match expected {
            Some("string") => value.is_string(),
            Some("number") => value.is_number(),
            Some("boolean") => value.is_boolean(),
            _ => true,
        };
        if !valid_type {
            return Err(ToolError::failure(format!(
                "Invalid type for argument '{field}'; expected {}",
                expected.unwrap_or("value")
            )));
        }
        if let Some(values) = schema.get("enum").and_then(Value::as_array) {
            if !values.contains(value) {
                return Err(ToolError::failure(format!(
                    "Invalid value for argument '{field}'"
                )));
            }
        }
        if let Some(number) = value.as_f64() {
            if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64) {
                if number < minimum {
                    return Err(ToolError::failure(format!(
                        "Argument '{field}' must be at least {minimum}"
                    )));
                }
            }
            if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64) {
                if number > maximum {
                    return Err(ToolError::failure(format!(
                        "Argument '{field}' must be at most {maximum}"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn get_string<'a>(args: &'a Value, key: &str) -> Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::failure(format!("Missing or invalid required argument: {key}")))
}
fn coordinates(args: &Value) -> Result<(f64, f64), ToolError> {
    let x = args
        .get("x")
        .and_then(Value::as_f64)
        .ok_or_else(|| ToolError::failure("x must be a number"))?;
    let y = args
        .get("y")
        .and_then(Value::as_f64)
        .ok_or_else(|| ToolError::failure("y must be a number"))?;
    Ok((x, y))
}
fn text_result(text: impl Into<String>) -> Value {
    json!({"content":[{"type":"text","text":text.into()}]})
}
fn as_failure(error: ToolError) -> ToolError {
    match error {
        ToolError::Actionable(message) | ToolError::Failure(message) => ToolError::Failure(message),
    }
}

fn android_tool_error(error: AndroidError) -> ToolError {
    match error {
        AndroidError::Actionable(message) => ToolError::actionable(message),
        AndroidError::Failure(message) => ToolError::failure(message),
    }
}

fn simctl_tool_error(error: SimctlError) -> ToolError {
    match error {
        SimctlError::Actionable(message) => ToolError::actionable(message),
        SimctlError::Failure(message) => ToolError::failure(message),
    }
}

fn ios_app_tool_error(error: IosAppError) -> ToolError {
    match error {
        IosAppError::Actionable(message) => ToolError::actionable(message),
        IosAppError::Failure(message) => ToolError::failure(message),
    }
}

fn wda_tool_error(error: WdaError) -> ToolError {
    match error {
        WdaError::Actionable(message) => ToolError::actionable(message),
        WdaError::Failure(message) => ToolError::failure(message),
    }
}

fn ios_major_version(version: &str) -> Option<u32> {
    let version = version.trim_start();
    let version = version.strip_prefix('+').unwrap_or(version);
    let digits = version.bytes().take_while(u8::is_ascii_digit).count();
    (digits > 0)
        .then(|| version[..digits].parse().ok())
        .flatten()
}

fn command_error_detail(error: CommandError) -> String {
    let fallback = error.to_string();
    let mut output = String::from_utf8_lossy(&error.stdout).into_owned();
    output.push_str(&String::from_utf8_lossy(&error.stderr));
    let output = output.trim();
    if output.is_empty() {
        fallback
    } else {
        output.to_owned()
    }
}

fn reap_child(slot: &mut Option<Child>) {
    let Some(child) = slot.as_mut() else {
        return;
    };
    match child.try_wait() {
        Ok(Some(_)) => {
            slot.take();
        }
        Ok(None) => {}
        Err(_) => {
            if let Some(child) = slot.take() {
                stop_child(child);
            }
        }
    }
}

fn wait_for_port(port: u16, child: &mut Option<Child>, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if WdaClient::port_is_listening(port) {
            return true;
        }
        reap_child(child);
        if child.is_none() || Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_wda(child: &mut Option<Child>, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let wda = WdaClient::new();
    loop {
        if wda.is_running() {
            return true;
        }
        reap_child(child);
        if child.is_none() || Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn stop_child(mut child: Child) {
    // Do not signal an already-exited child: its numeric PID may have been
    // reused by the time the server is dropped.
    if !matches!(child.try_wait(), Ok(None)) {
        return;
    }
    #[cfg(unix)]
    {
        // go-ios responds to SIGINT by tearing down its USB forwarding and
        // tunnel listeners. Fall back to kill if it does not stop promptly.
        let _ = unsafe { libc::kill(child.id() as i32, libc::SIGINT) };
    }
    #[cfg(windows)]
    {
        let _ = child.kill();
    }
    match child.wait_timeout(Duration::from_secs(2)) {
        Ok(Some(_)) => {}
        _ => {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn format_android_screen_elements(elements: Vec<Value>) -> Value {
    Value::Array(
        elements
            .into_iter()
            .map(|element| {
                let mut formatted = Map::new();
                if let Some(value) = element.get("type") {
                    formatted.insert("type".to_owned(), value.clone());
                }
                if let Some(value) = element.get("text") {
                    formatted.insert("text".to_owned(), value.clone());
                }
                if let Some(value) = element.get("label") {
                    formatted.insert("label".to_owned(), value.clone());
                }
                if let Some(value) = element.get("identifier") {
                    formatted.insert("identifier".to_owned(), value.clone());
                }
                if let Some(value) = element.get("rect") {
                    formatted.insert("coordinates".to_owned(), value.clone());
                }
                if element.get("focused").and_then(Value::as_bool) == Some(true) {
                    formatted.insert("focused".to_owned(), json!(true));
                }
                Value::Object(formatted)
            })
            .collect(),
    )
}

fn parse_data_array(output: &str, key: &str) -> Result<Vec<Value>, ToolError> {
    let value: Value = serde_json::from_str(output)
        .map_err(|e| ToolError::failure(format!("Invalid mobilecli response: {e}")))?;
    value
        .pointer(&format!("/data/{key}"))
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| {
            ToolError::failure(format!("Invalid mobilecli response: missing data.{key}"))
        })
}
fn parse_orientation(output: &str) -> String {
    if output.contains("landscape") {
        "landscape".into()
    } else {
        "portrait".into()
    }
}
fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn validate_extension(path: &str, extensions: &[&str], tool: &str) -> Result<(), ToolError> {
    let ext = Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| format!(".{}", ext.to_ascii_lowercase()))
        .unwrap_or_default();
    if extensions.contains(&ext.as_str()) {
        Ok(())
    } else {
        Err(ToolError::actionable(format!(
            "{tool} requires a {} file extension, got: \"{}\"",
            extensions.join(", "),
            if ext.is_empty() { "(none)" } else { &ext }
        )))
    }
}

fn validate_output_path(path: &str) -> Result<(), ToolError> {
    let target = PathBuf::from(path);
    let absolute = if target.is_absolute() {
        target
    } else {
        std::env::current_dir()
            .map_err(|e| ToolError::failure(e.to_string()))?
            .join(target)
    };
    let parent = absolute
        .parent()
        .ok_or_else(|| ToolError::actionable("Output path must have a parent directory"))?;
    let resolved_parent = parent
        .canonicalize()
        .unwrap_or_else(|_| parent.to_path_buf());
    let resolved = resolved_parent.join(
        absolute
            .file_name()
            .ok_or_else(|| ToolError::actionable("Output path requires a filename"))?,
    );
    let cwd = std::env::current_dir()
        .unwrap_or_default()
        .canonicalize()
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
    let temp = std::env::temp_dir()
        .canonicalize()
        .unwrap_or_else(|_| std::env::temp_dir());
    let allowed = [
        cwd,
        temp,
        PathBuf::from("/tmp"),
        PathBuf::from("/private/tmp"),
    ]
    .iter()
    .any(|root| {
        root.canonicalize().unwrap_or_else(|_| root.clone()) != resolved
            && resolved.starts_with(root.canonicalize().unwrap_or_else(|_| root.clone()))
    });
    if allowed {
        Ok(())
    } else {
        Err(ToolError::actionable(format!(
            "\"{}\" is not in the list of allowed directories. Allowed directories include the current directory and the temp directory on this host.",
            resolved.parent().unwrap_or(Path::new("")).display()
        )))
    }
}

fn posix_resolve(path: &str) -> String {
    let mut components = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                components.pop();
            }
            value => components.push(value),
        }
    }
    format!("/{}", components.join("/"))
}

fn png_dimensions(image: &[u8]) -> Option<(u32, u32)> {
    if image.get(..8)? != [137, 80, 78, 71, 13, 10, 26, 10] {
        return None;
    }
    Some((
        u32::from_be_bytes(image.get(16..20)?.try_into().ok()?),
        u32::from_be_bytes(image.get(20..24)?.try_into().ok()?),
    ))
}

fn resize_screenshot(
    runner: &CommandRunner,
    image: &[u8],
    width: u32,
) -> Result<Option<Vec<u8>>, String> {
    let mut sips_installed = false;
    #[cfg(target_os = "macos")]
    {
        let sips = PathBuf::from("/usr/bin/sips");
        if runner
            .run(&sips, ["--version"])
            .is_ok_and(|output| output.status.success())
        {
            sips_installed = true;
            crate::logger::trace("Image scaling is available, resizing screenshot");
            if let Some(bytes) = resize_with_sips(runner, &sips, image, width) {
                return Ok(Some(bytes));
            }
            crate::logger::trace(
                "Sips failed, falling back to ImageMagick: failed to resize screenshot",
            );
        }
    }
    let magick_installed = runner
        .run("magick", ["--version"])
        .is_ok_and(|output| output.status.success());
    if magick_installed {
        if !sips_installed {
            crate::logger::trace("Image scaling is available, resizing screenshot");
        }
        return resize_with_magick(runner, image, width)
            .map(Some)
            .ok_or_else(|| {
                crate::logger::trace("ImageMagick failed: failed to resize screenshot");
                "Image scaling unavailable (requires Sips or ImageMagick).".to_owned()
            });
    }
    if sips_installed {
        return Err("Image scaling unavailable (requires Sips or ImageMagick).".to_owned());
    }
    Ok(None)
}

fn resize_with_sips(
    runner: &CommandRunner,
    sips: &Path,
    image: &[u8],
    width: u32,
) -> Option<Vec<u8>> {
    let dir = unique_temp_dir("mobile-mcp-image")?;
    let result = (|| {
        let input = dir.join("input.png");
        let output = dir.join("output.jpg");
        fs::write(&input, image).ok()?;
        let width = width.to_string();
        crate::logger::trace(&format!(
            "Running sips command: {} -s format jpeg -s formatOptions high -Z {width} --out {} {}",
            sips.display(),
            output.display(),
            input.display()
        ));
        let command = runner
            .run(
                sips,
                [
                    "-s",
                    "format",
                    "jpeg",
                    "-s",
                    "formatOptions",
                    "high",
                    "-Z",
                    width.as_str(),
                    "--out",
                    output.to_str()?,
                    input.to_str()?,
                ],
            )
            .ok()?;
        if !command.status.success() {
            return None;
        }
        let bytes = read_bounded_file(&output)?;
        crate::logger::trace(&format!("Sips returned buffer of size: {}", bytes.len()));
        Some(bytes)
    })();
    let _ = fs::remove_dir_all(dir);
    result
}

fn resize_with_magick(runner: &CommandRunner, image: &[u8], width: u32) -> Option<Vec<u8>> {
    let dir = unique_temp_dir("mobile-mcp-image")?;
    let result = (|| {
        let input = dir.join("input.png");
        let output = dir.join("output.jpg");
        fs::write(&input, image).ok()?;
        let resize = format!("{width}x");
        let destination = format!("jpeg:{}", output.display());
        crate::logger::trace(&format!(
            "Running magick command: magick {} -resize {resize} -quality 75 {destination}",
            input.display()
        ));
        let command = runner
            .run(
                "magick",
                [
                    input.to_str()?,
                    "-resize",
                    resize.as_str(),
                    "-quality",
                    "75",
                    destination.as_str(),
                ],
            )
            .ok()?;
        command
            .status
            .success()
            .then(|| read_bounded_file(&output))
            .flatten()
    })();
    let _ = fs::remove_dir_all(dir);
    result
}

fn read_bounded_file(path: &Path) -> Option<Vec<u8>> {
    let metadata = fs::metadata(path).ok()?;
    if metadata.len() > crate::runner::MAX_OUTPUT_BYTES as u64 {
        return None;
    }
    let bytes = fs::read(path).ok()?;
    (!bytes.is_empty()).then_some(bytes)
}

fn unique_temp_dir(prefix: &str) -> Option<PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);
    for _ in 0..100 {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "{prefix}-{}-{}-{id}",
            std::process::id(),
            now_millis()
        ));
        match fs::create_dir(&path) {
            Ok(()) => return Some(path),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return None,
        }
    }
    None
}
