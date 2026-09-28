//! Android actions implemented through the same ADB Robot commands as the
//! TypeScript mobile-mcp adapter.

use std::collections::HashMap;
use std::ffi::OsString;
use std::thread;
use std::time::Duration;

use base64::Engine;
use regex::Regex;
use serde_json::{Value, json};

use crate::coord::ScreenSize;
use crate::devices;
use crate::runner::{CommandOutput, CommandRunner};

const DEVICE_KIT_PACKAGE: &str = "com.mobilenext.devicekit";
const DEVICE_KIT_RECEIVER: &str = "com.mobilenext.devicekit/.ClipboardBroadcastReceiver";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AndroidDeviceType {
    Tv,
    Mobile,
}

struct UiNodeFrame {
    attributes: HashMap<String, String>,
    children: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AndroidError {
    Actionable(String),
    Failure(String),
}

impl std::fmt::Display for AndroidError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Actionable(message) | Self::Failure(message) => formatter.write_str(message),
        }
    }
}

pub struct AndroidRobot {
    device_id: String,
    runner: CommandRunner,
}

impl AndroidRobot {
    pub fn new(device_id: impl Into<String>, runner: CommandRunner) -> Self {
        Self {
            device_id: device_id.into(),
            runner,
        }
    }

    fn adb<I, S>(&self, args: I) -> Result<Vec<u8>, AndroidError>
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        let args = std::iter::once(OsString::from("-s"))
            .chain(std::iter::once(OsString::from(self.device_id.as_str())))
            .chain(args.into_iter().map(Into::into));
        let output = self
            .runner
            .run(devices::adb_path(), args)
            .map_err(|error| AndroidError::Failure(error.to_string()))?;
        if !output.status.success() {
            return Err(AndroidError::Failure(command_output(&output)));
        }
        Ok(output.stdout)
    }

    /// Return the package-manager feature names exposed by the device.
    pub fn get_system_features(&self) -> Result<Vec<String>, AndroidError> {
        let output = self.adb(["shell", "pm", "list", "features"])?;
        Ok(String::from_utf8_lossy(&output)
            .lines()
            .map(str::trim)
            .filter_map(|line| line.strip_prefix("feature:"))
            .map(str::to_owned)
            .collect())
    }

    /// Classify Android TV devices using the same feature checks as the
    /// TypeScript AndroidDeviceManager. Discovery callers should fall back to
    /// `Mobile` when querying features fails.
    pub fn get_device_type(&self) -> Result<AndroidDeviceType, AndroidError> {
        let features = self.get_system_features()?;
        Ok(
            if features.iter().any(|feature| {
                matches!(
                    feature.as_str(),
                    "android.software.leanback" | "android.hardware.type.television"
                )
            }) {
                AndroidDeviceType::Tv
            } else {
                AndroidDeviceType::Mobile
            },
        )
    }

    pub fn get_screen_size(&self) -> Result<ScreenSize, AndroidError> {
        let output = self.adb(["shell", "wm", "size"])?;
        let output = String::from_utf8_lossy(&output);
        let size = output.split(' ').next_back().unwrap_or_default().trim();
        let Some((width, height)) = size.split_once('x') else {
            return Err(AndroidError::Failure(
                "Failed to get screen size".to_owned(),
            ));
        };
        let (Ok(width), Ok(height)) = (width.parse::<u32>(), height.parse::<u32>()) else {
            return Err(AndroidError::Failure(
                "Failed to get screen size".to_owned(),
            ));
        };
        Ok(ScreenSize { width, height })
    }

    pub fn list_running_processes(&self) -> Result<Vec<String>, AndroidError> {
        let output = self.adb(["shell", "ps", "-e"])?;
        Ok(String::from_utf8_lossy(&output)
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with('u'))
            .filter_map(|line| line.split_whitespace().nth(8).map(str::to_owned))
            .collect())
    }

    pub fn set_orientation(&self, orientation: &str) -> Result<(), AndroidError> {
        let rotation = match orientation {
            "portrait" => "0",
            "landscape" => "1",
            _ => {
                return Err(AndroidError::Actionable(format!(
                    "Orientation \"{orientation}\" is not supported"
                )));
            }
        };
        self.adb([
            "shell",
            "settings",
            "put",
            "system",
            "accelerometer_rotation",
            "0",
        ])?;
        self.adb([
            "shell",
            "content",
            "insert",
            "--uri",
            "content://settings/system",
            "--bind",
            "name:s:user_rotation",
            "--bind",
            &format!("value:i:{rotation}"),
        ])?;
        Ok(())
    }

    pub fn get_orientation(&self) -> Result<&'static str, AndroidError> {
        let output = self.adb(["shell", "settings", "get", "system", "user_rotation"])?;
        if String::from_utf8_lossy(&output).trim() == "0" {
            Ok("portrait")
        } else {
            Ok("landscape")
        }
    }

    /// Only launcher activities are returned, matching AndroidRobot.listApps.
    pub fn list_apps(&self) -> Result<Vec<String>, AndroidError> {
        let output = self.adb([
            "shell",
            "cmd",
            "package",
            "query-activities",
            "-a",
            "android.intent.action.MAIN",
            "-c",
            "android.intent.category.LAUNCHER",
        ])?;
        let output = String::from_utf8_lossy(&output);
        let mut apps = Vec::new();
        for line in output.lines().map(str::trim) {
            if let Some(package) = line.strip_prefix("packageName=") {
                if !apps.iter().any(|seen: &String| seen.as_str() == package) {
                    apps.push(package.to_owned());
                }
            }
        }
        Ok(apps)
    }

    pub fn launch_app(&self, package_name: &str, locale: Option<&str>) -> Result<(), AndroidError> {
        validate_package_name(package_name)?;
        if let Some(locale) = locale.filter(|locale| !locale.is_empty()) {
            validate_locale(locale)?;
            let _ = self.adb([
                "shell".to_owned(),
                "cmd".to_owned(),
                "locale".to_owned(),
                "set-app-locales".to_owned(),
                package_name.to_owned(),
                "--locales".to_owned(),
                locale.to_owned(),
            ]);
        }
        if self
            .adb([
                "shell".to_owned(),
                "monkey".to_owned(),
                "-p".to_owned(),
                package_name.to_owned(),
                "-c".to_owned(),
                "android.intent.category.LAUNCHER".to_owned(),
                "1".to_owned(),
            ])
            .is_err()
        {
            return Err(AndroidError::Actionable(format!(
                "Failed launching app with package name \"{package_name}\", please make sure it exists"
            )));
        }
        Ok(())
    }

    pub fn terminate_app(&self, package_name: &str) -> Result<(), AndroidError> {
        validate_package_name(package_name)?;
        self.adb([
            "shell".to_owned(),
            "am".to_owned(),
            "force-stop".to_owned(),
            package_name.to_owned(),
        ])?;
        Ok(())
    }

    pub fn install_app(
        &self,
        path: &str,
        replace: bool,
        grant_permissions: bool,
        allow_downgrade: bool,
        allow_test: bool,
    ) -> Result<(), AndroidError> {
        let mut args = vec![OsString::from("install")];
        if replace {
            args.push(OsString::from("-r"));
        }
        if grant_permissions {
            args.push(OsString::from("-g"));
        }
        if allow_downgrade {
            args.push(OsString::from("-d"));
        }
        if allow_test {
            args.push(OsString::from("-t"));
        }
        args.push(OsString::from(path));
        self.adb(args).map_err(actionable_error)?;
        Ok(())
    }

    pub fn uninstall_app(&self, package_name: &str) -> Result<(), AndroidError> {
        self.adb(["uninstall", package_name])
            .map_err(actionable_error)?;
        Ok(())
    }

    pub fn pull_file(&self, remote_path: &str, local_path: &str) -> Result<(), AndroidError> {
        self.adb(["pull", remote_path, local_path])
            .map_err(actionable_error)?;
        Ok(())
    }

    pub fn push_file(&self, local_path: &str, remote_path: &str) -> Result<(), AndroidError> {
        self.adb(["push", local_path, remote_path])
            .map_err(actionable_error)?;
        Ok(())
    }

    pub fn get_screenshot(&self) -> Result<Vec<u8>, AndroidError> {
        if self.display_count()? <= 1 {
            return self.adb(["exec-out", "screencap", "-p"]);
        }
        let Some(display_id) = self.first_display_id() else {
            return self.adb(["exec-out", "screencap", "-p"]);
        };
        self.adb([
            "exec-out".to_owned(),
            "screencap".to_owned(),
            "-p".to_owned(),
            "-d".to_owned(),
            display_id,
        ])
    }

    pub fn dump_ui_hierarchy(&self, compressed: bool) -> Result<String, AndroidError> {
        for _ in 0..10 {
            let mut args = vec![
                "exec-out".to_owned(),
                "uiautomator".to_owned(),
                "dump".to_owned(),
            ];
            if compressed {
                args.push("--compressed".to_owned());
            }
            args.push("/dev/tty".to_owned());
            let output = self.adb(args)?;
            let dump = String::from_utf8_lossy(&output);
            if dump.contains("null root node returned by UiTestAutomationBridge") {
                continue;
            }
            if let Some(start) = dump.find("<?xml") {
                return Ok(dump[start..].to_owned());
            }
        }
        Err(AndroidError::Actionable(
            "Failed to get UIAutomator XML dump".to_owned(),
        ))
    }

    pub fn get_elements_on_screen(&self) -> Result<Vec<Value>, AndroidError> {
        let xml = self.get_ui_automator_dump()?;
        extract_screen_elements(&xml)
    }

    fn get_ui_automator_dump(&self) -> Result<String, AndroidError> {
        for _ in 0..10 {
            let output = self.adb(["exec-out", "uiautomator", "dump", "/dev/tty"])?;
            let dump = String::from_utf8_lossy(&output);
            if dump.contains("null root node returned by UiTestAutomationBridge") {
                continue;
            }
            return Ok(dump
                .find("<?xml")
                .map(|start| dump[start..].to_owned())
                .unwrap_or_else(|| dump.into_owned()));
        }
        Err(AndroidError::Actionable(
            "Failed to get UIAutomator XML".to_owned(),
        ))
    }

    fn display_count(&self) -> Result<usize, AndroidError> {
        let output = self.adb(["shell", "dumpsys", "SurfaceFlinger", "--display-id"])?;
        Ok(String::from_utf8_lossy(&output)
            .lines()
            .filter(|line| line.starts_with("Display "))
            .count())
    }

    fn first_display_id(&self) -> Option<String> {
        if let Ok(output) = self.adb(["shell", "cmd", "display", "get-displays"]) {
            let output = String::from_utf8_lossy(&output);
            if let Some(line) = output.lines().find(|line| {
                line.starts_with("Display id ")
                    && line.contains(", state ON,")
                    && line.contains(", uniqueId ")
            }) {
                if let Some((_, value)) = line.split_once("uniqueId \"") {
                    if let Some((unique_id, _)) = value.split_once('"') {
                        return Some(
                            unique_id
                                .strip_prefix("local:")
                                .unwrap_or(unique_id)
                                .to_owned(),
                        );
                    }
                }
            }
        }

        let output = self
            .adb(["shell", "dumpsys", "display"])
            .ok()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())?;
        let viewport =
            Regex::new(r"DisplayViewport\{type=INTERNAL[^}]*isActive=true[^}]*uniqueId='([^']+)'")
                .ok()?;
        if let Some(captures) = viewport.captures(&output) {
            if let Some(unique_id) = captures.get(1) {
                let unique_id = unique_id.as_str();
                return Some(
                    unique_id
                        .strip_prefix("local:")
                        .unwrap_or(unique_id)
                        .to_owned(),
                );
            }
        }

        Regex::new(r"(?s)Display Id=(\d+).*?Display State=ON")
            .ok()?
            .captures(&output)
            .and_then(|captures| captures.get(1).map(|id| id.as_str().to_owned()))
    }

    pub fn tap(&self, x: f64, y: f64) -> Result<(), AndroidError> {
        self.adb([
            "shell".to_owned(),
            "input".to_owned(),
            "tap".to_owned(),
            x.to_string(),
            y.to_string(),
        ])?;
        Ok(())
    }

    pub fn double_tap(&self, x: f64, y: f64) -> Result<(), AndroidError> {
        self.tap(x, y)?;
        thread::sleep(Duration::from_millis(100));
        self.tap(x, y)
    }

    pub fn long_press(&self, x: f64, y: f64, duration_ms: f64) -> Result<(), AndroidError> {
        self.adb([
            "shell".to_owned(),
            "input".to_owned(),
            "swipe".to_owned(),
            x.to_string(),
            y.to_string(),
            x.to_string(),
            y.to_string(),
            duration_ms.to_string(),
        ])?;
        Ok(())
    }

    pub fn swipe(&self, direction: &str) -> Result<(), AndroidError> {
        let size = self.get_screen_size()?;
        let center_x = size.width >> 1;
        let (x0, y0, x1, y1) = match direction {
            "up" => (
                center_x,
                (f64::from(size.height) * 0.8).floor() as u32,
                center_x,
                (f64::from(size.height) * 0.2).floor() as u32,
            ),
            "down" => (
                center_x,
                (f64::from(size.height) * 0.2).floor() as u32,
                center_x,
                (f64::from(size.height) * 0.8).floor() as u32,
            ),
            "left" => (
                (f64::from(size.width) * 0.8).floor() as u32,
                (f64::from(size.height) * 0.5).floor() as u32,
                (f64::from(size.width) * 0.2).floor() as u32,
                (f64::from(size.height) * 0.5).floor() as u32,
            ),
            "right" => (
                (f64::from(size.width) * 0.2).floor() as u32,
                (f64::from(size.height) * 0.5).floor() as u32,
                (f64::from(size.width) * 0.8).floor() as u32,
                (f64::from(size.height) * 0.5).floor() as u32,
            ),
            _ => {
                return Err(AndroidError::Actionable(format!(
                    "Swipe direction \"{direction}\" is not supported"
                )));
            }
        };
        self.adb([
            "shell".to_owned(),
            "input".to_owned(),
            "swipe".to_owned(),
            x0.to_string(),
            y0.to_string(),
            x1.to_string(),
            y1.to_string(),
            "1000".to_owned(),
        ])?;
        Ok(())
    }

    pub fn swipe_from_coordinate(
        &self,
        x: f64,
        y: f64,
        direction: &str,
        distance: Option<f64>,
    ) -> Result<(), AndroidError> {
        let size = self.get_screen_size()?;
        let default_y = (f64::from(size.height) * 0.3).floor();
        let default_x = (f64::from(size.width) * 0.3).floor();
        let swipe_y = distance
            .filter(|distance| *distance != 0.0)
            .unwrap_or(default_y);
        let swipe_x = distance
            .filter(|distance| *distance != 0.0)
            .unwrap_or(default_x);
        let (x0, y0, x1, y1) = match direction {
            "up" => (x, y, x, (y - swipe_y).max(0.0)),
            "down" => (x, y, x, (y + swipe_y).min(f64::from(size.height))),
            "left" => (x, y, (x - swipe_x).max(0.0), y),
            "right" => (x, y, (x + swipe_x).min(f64::from(size.width)), y),
            _ => {
                return Err(AndroidError::Actionable(format!(
                    "Swipe direction \"{direction}\" is not supported"
                )));
            }
        };
        self.adb([
            "shell".to_owned(),
            "input".to_owned(),
            "swipe".to_owned(),
            x0.to_string(),
            y0.to_string(),
            x1.to_string(),
            y1.to_string(),
            "1000".to_owned(),
        ])?;
        Ok(())
    }

    pub fn press_button(&self, button: &str) -> Result<(), AndroidError> {
        let key = match button {
            "BACK" => "KEYCODE_BACK",
            "HOME" => "KEYCODE_HOME",
            "VOLUME_UP" => "KEYCODE_VOLUME_UP",
            "VOLUME_DOWN" => "KEYCODE_VOLUME_DOWN",
            "ENTER" => "KEYCODE_ENTER",
            "DPAD_CENTER" => "KEYCODE_DPAD_CENTER",
            "DPAD_UP" => "KEYCODE_DPAD_UP",
            "DPAD_DOWN" => "KEYCODE_DPAD_DOWN",
            "DPAD_LEFT" => "KEYCODE_DPAD_LEFT",
            "DPAD_RIGHT" => "KEYCODE_DPAD_RIGHT",
            _ => {
                return Err(AndroidError::Actionable(format!(
                    "Button \"{button}\" is not supported"
                )));
            }
        };
        self.adb(["shell", "input", "keyevent", key])?;
        Ok(())
    }

    pub fn open_url(&self, url: &str) -> Result<(), AndroidError> {
        let escaped_url = escape_shell_text(url);
        self.adb([
            "shell".to_owned(),
            "am".to_owned(),
            "start".to_owned(),
            "-a".to_owned(),
            "android.intent.action.VIEW".to_owned(),
            "-d".to_owned(),
            escaped_url,
        ])?;
        Ok(())
    }

    pub fn send_keys(&self, text: &str) -> Result<(), AndroidError> {
        if text.is_empty() {
            return Ok(());
        }
        if text.is_ascii() {
            let escaped_text = escape_shell_text(text);
            self.adb([
                "shell".to_owned(),
                "input".to_owned(),
                "text".to_owned(),
                escaped_text,
            ])?;
            return Ok(());
        }
        let packages = self.adb(["shell", "pm", "list", "packages"])?;
        if !String::from_utf8_lossy(&packages)
            .lines()
            .map(str::trim)
            .filter_map(|line| line.strip_prefix("package:"))
            .any(|package| package == DEVICE_KIT_PACKAGE)
        {
            return Err(AndroidError::Actionable(
                "Non-ASCII text is not supported on Android, please install mobilenext devicekit, see https://github.com/mobile-next/devicekit-android".to_owned(),
            ));
        }

        let encoded = base64::engine::general_purpose::STANDARD.encode(text.as_bytes());
        self.adb([
            "shell".to_owned(),
            "am".to_owned(),
            "broadcast".to_owned(),
            "-a".to_owned(),
            "devicekit.clipboard.set".to_owned(),
            "-e".to_owned(),
            "encoding".to_owned(),
            "base64".to_owned(),
            "-e".to_owned(),
            "text".to_owned(),
            encoded,
            "-n".to_owned(),
            DEVICE_KIT_RECEIVER.to_owned(),
        ])?;
        self.adb(["shell", "input", "keyevent", "KEYCODE_PASTE"])?;
        self.adb([
            "shell".to_owned(),
            "am".to_owned(),
            "broadcast".to_owned(),
            "-a".to_owned(),
            "devicekit.clipboard.clear".to_owned(),
            "-n".to_owned(),
            DEVICE_KIT_RECEIVER.to_owned(),
        ])?;
        Ok(())
    }
}

fn extract_screen_elements(xml: &str) -> Result<Vec<Value>, AndroidError> {
    let mut stack = Vec::<UiNodeFrame>::new();
    let mut elements = Vec::new();
    let mut saw_node = false;
    let mut cursor = 0;

    while let Some(offset) = xml[cursor..].find('<') {
        let start = cursor + offset;
        let remaining = &xml[start..];
        if remaining.starts_with("<!--") {
            let Some(end) = remaining.find("-->") else {
                return Err(AndroidError::Failure(
                    "Failed to parse UIAutomator XML".to_owned(),
                ));
            };
            cursor = start + end + 3;
            continue;
        }
        if remaining.starts_with("</node")
            && remaining
                .as_bytes()
                .get(6)
                .is_some_and(|byte| byte.is_ascii_whitespace() || *byte == b'>')
        {
            let Some(end) = xml_tag_end(xml, start) else {
                return Err(AndroidError::Failure(
                    "Failed to parse UIAutomator XML".to_owned(),
                ));
            };
            if let Some(frame) = stack.pop() {
                append_completed_node(frame, &mut stack, &mut elements);
            }
            cursor = end + 1;
            continue;
        }
        if remaining.starts_with("<node")
            && remaining
                .as_bytes()
                .get(5)
                .is_some_and(|byte| byte.is_ascii_whitespace() || *byte == b'/' || *byte == b'>')
        {
            let Some(end) = xml_tag_end(xml, start) else {
                return Err(AndroidError::Failure(
                    "Failed to parse UIAutomator XML".to_owned(),
                ));
            };
            let tag = &xml[start + 1..end];
            let attributes = parse_xml_attributes(tag);
            saw_node = true;
            if tag.trim_end().ends_with('/') {
                append_completed_node(
                    UiNodeFrame {
                        attributes,
                        children: Vec::new(),
                    },
                    &mut stack,
                    &mut elements,
                );
            } else {
                stack.push(UiNodeFrame {
                    attributes,
                    children: Vec::new(),
                });
            }
            cursor = end + 1;
            continue;
        }

        let Some(end) = xml_tag_end(xml, start) else {
            return Err(AndroidError::Failure(
                "Failed to parse UIAutomator XML".to_owned(),
            ));
        };
        cursor = end + 1;
    }

    if !saw_node || !stack.is_empty() {
        return Err(AndroidError::Failure(
            "Failed to parse UIAutomator XML".to_owned(),
        ));
    }
    Ok(elements)
}

fn append_completed_node(
    mut frame: UiNodeFrame,
    stack: &mut [UiNodeFrame],
    elements: &mut Vec<Value>,
) {
    if let Some(element) = screen_element(&frame.attributes) {
        frame.children.push(element);
    }
    if let Some(parent) = stack.last_mut() {
        parent.children.extend(frame.children);
    } else {
        elements.extend(frame.children);
    }
}

fn screen_element(attributes: &HashMap<String, String>) -> Option<Value> {
    let text = attributes.get("text");
    let description = attributes.get("content-desc");
    let hint = attributes.get("hint");
    let resource_id = attributes.get("resource-id");
    let is_checkable = attributes
        .get("checkable")
        .is_some_and(|value| value == "true");
    if !text.is_some_and(|value| !value.is_empty())
        && !description.is_some_and(|value| !value.is_empty())
        && !hint.is_some_and(|value| !value.is_empty())
        && !resource_id.is_some_and(|value| !value.is_empty())
        && !is_checkable
    {
        return None;
    }

    let Some(bounds) = attributes
        .get("bounds")
        .and_then(|value| parse_bounds(value))
    else {
        return None;
    };
    let (left, top, right, bottom) = bounds;
    let width = right - left;
    let height = bottom - top;
    if width <= 0 || height <= 0 {
        return None;
    }

    let mut element = serde_json::Map::new();
    element.insert(
        "type".to_owned(),
        json!(
            attributes
                .get("class")
                .filter(|class| !class.is_empty())
                .map(String::as_str)
                .unwrap_or("text")
        ),
    );
    if let Some(text) = text {
        element.insert("text".to_owned(), json!(text));
    }
    element.insert(
        "label".to_owned(),
        json!(
            description
                .filter(|value| !value.is_empty())
                .or_else(|| hint.filter(|value| !value.is_empty()))
                .map(String::as_str)
                .unwrap_or("")
        ),
    );
    element.insert(
        "rect".to_owned(),
        json!({"x":left,"y":top,"width":width,"height":height}),
    );
    if attributes
        .get("focused")
        .is_some_and(|value| value == "true")
    {
        element.insert("focused".to_owned(), json!(true));
    }
    if let Some(identifier) = resource_id.filter(|value| !value.is_empty()) {
        element.insert("identifier".to_owned(), json!(identifier));
    }
    Some(Value::Object(element))
}

fn parse_bounds(bounds: &str) -> Option<(i64, i64, i64, i64)> {
    let inner = bounds.strip_prefix('[')?.strip_suffix(']')?;
    let (first, second) = inner.split_once("][")?;
    let (left, top) = first.split_once(',')?;
    let (right, bottom) = second.split_once(',')?;
    if [left, top, right, bottom].iter().any(|coordinate| {
        coordinate.is_empty() || !coordinate.bytes().all(|byte| byte.is_ascii_digit())
    }) {
        return None;
    }
    Some((
        left.parse().ok()?,
        top.parse().ok()?,
        right.parse().ok()?,
        bottom.parse().ok()?,
    ))
}

fn xml_tag_end(xml: &str, start: usize) -> Option<usize> {
    let mut quote = None;
    for (offset, byte) in xml.as_bytes()[start..].iter().enumerate() {
        if let Some(active_quote) = quote {
            if *byte == active_quote {
                quote = None;
            }
        } else if matches!(*byte, b'\'' | b'"') {
            quote = Some(*byte);
        } else if *byte == b'>' {
            return Some(start + offset);
        }
    }
    None
}

fn parse_xml_attributes(tag: &str) -> HashMap<String, String> {
    let Some(rest) = tag.strip_prefix("node") else {
        return HashMap::new();
    };
    let bytes = rest.as_bytes();
    let mut attributes = HashMap::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        while cursor < bytes.len() && (bytes[cursor].is_ascii_whitespace() || bytes[cursor] == b'/')
        {
            cursor += 1;
        }
        if cursor >= bytes.len() || bytes[cursor] == b'>' {
            break;
        }
        let name_start = cursor;
        while cursor < bytes.len()
            && !bytes[cursor].is_ascii_whitespace()
            && !matches!(bytes[cursor], b'=' | b'/' | b'>')
        {
            cursor += 1;
        }
        if name_start == cursor {
            cursor += 1;
            continue;
        }
        let name = &rest[name_start..cursor];
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor >= bytes.len() || bytes[cursor] != b'=' {
            continue;
        }
        cursor += 1;
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        let Some(quote) = bytes
            .get(cursor)
            .copied()
            .filter(|byte| matches!(*byte, b'\'' | b'"'))
        else {
            continue;
        };
        cursor += 1;
        let value_start = cursor;
        while cursor < bytes.len() && bytes[cursor] != quote {
            cursor += 1;
        }
        if cursor >= bytes.len() {
            break;
        }
        attributes.insert(
            name.to_owned(),
            decode_xml_entities(&rest[value_start..cursor]),
        );
        cursor += 1;
    }
    attributes
}

fn decode_xml_entities(value: &str) -> String {
    let mut decoded = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(ampersand) = rest.find('&') {
        decoded.push_str(&rest[..ampersand]);
        let entity_start = ampersand + 1;
        let Some(semicolon_offset) = rest[entity_start..].find(';') else {
            decoded.push_str(&rest[ampersand..]);
            return decoded;
        };
        let entity_end = entity_start + semicolon_offset;
        let entity = &rest[entity_start..entity_end];
        let replacement = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                u32::from_str_radix(&entity[2..], 16)
                    .ok()
                    .and_then(char::from_u32)
            }
            _ if entity.starts_with('#') => entity[1..].parse().ok().and_then(char::from_u32),
            _ => None,
        };
        if let Some(character) = replacement {
            decoded.push(character);
        } else {
            decoded.push_str(&rest[ampersand..=entity_end]);
        }
        rest = &rest[entity_end + 1..];
    }
    decoded.push_str(rest);
    decoded
}

fn validate_package_name(package_name: &str) -> Result<(), AndroidError> {
    if !package_name.is_empty()
        && package_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_'))
    {
        Ok(())
    } else {
        Err(AndroidError::Actionable(format!(
            "Invalid package name: \"{package_name}\""
        )))
    }
}

fn validate_locale(locale: &str) -> Result<(), AndroidError> {
    if !locale.is_empty()
        && locale
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b',' | b'-' | b' '))
    {
        Ok(())
    } else {
        Err(AndroidError::Actionable(format!(
            "Invalid locale: \"{locale}\""
        )))
    }
}

fn escape_shell_text(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        if matches!(
            character,
            '\\' | '\''
                | '"'
                | '`'
                | ' '
                | '\t'
                | '\n'
                | '\r'
                | '|'
                | '&'
                | ';'
                | '('
                | ')'
                | '<'
                | '>'
                | '{'
                | '}'
                | '['
                | ']'
                | '$'
                | '*'
                | '?'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn actionable_error(error: AndroidError) -> AndroidError {
    AndroidError::Actionable(error.to_string())
}

fn command_output(output: &CommandOutput) -> String {
    let mut detail = String::from_utf8_lossy(&output.stdout).into_owned();
    detail.push_str(&String::from_utf8_lossy(&output.stderr));
    let detail = detail.trim();
    if detail.is_empty() {
        format!("adb exited with {}", output.status)
    } else {
        detail.to_owned()
    }
}
