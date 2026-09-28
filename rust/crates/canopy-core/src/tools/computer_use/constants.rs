use std::path::{Path, PathBuf};

/// Exact `cua-driver-rs` release against which the schemas are pinned.
pub const CUA_DRIVER_VERSION: &str = "0.5.2";
pub const OSS_MIRROR_BASE: &str =
    "https://qwen-code-assets.oss-cn-hangzhou.aliyuncs.com/computer-use";
pub const GITHUB_RELEASE_BASE: &str = "https://github.com/trycua/cua/releases/download";
pub const MAX_IMAGE_DIMENSION_ENV: &str = "CANOPY_COMPUTER_USE_MAX_IMAGE_DIMENSION";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssetTarget {
    pub asset: String,
    pub extract_dir: String,
    pub binary_rel_path: String,
    pub has_app: bool,
}

/// Map a Node platform/architecture pair to its pinned release asset.
///
/// The fallback architecture behavior intentionally follows the TypeScript
/// implementation: macOS and Windows map any architecture other than
/// `arm64` to `x86_64`; Linux accepts only `x64`.
pub fn resolve_asset_target(
    platform: &str,
    arch: &str,
    version: &str,
) -> Result<AssetTarget, String> {
    match platform {
        "darwin" => {
            let slug = if arch == "arm64" {
                "darwin-arm64"
            } else {
                "darwin-x86_64"
            };
            let extract_dir = format!("cua-driver-rs-{version}-{slug}");
            Ok(AssetTarget {
                asset: format!("{extract_dir}.tar.gz"),
                extract_dir,
                // The app-contained executable attributes TCC grants to the
                // driver bundle instead of the launching terminal.
                binary_rel_path: "CuaDriver.app/Contents/MacOS/cua-driver".to_owned(),
                has_app: true,
            })
        }
        "linux" => {
            if arch != "x64" {
                return Err(format!(
                    "Computer Use: unsupported Linux arch '{arch}' (only x64)."
                ));
            }
            Ok(AssetTarget {
                asset: format!("cua-driver-rs-{version}-linux-x86_64-binary.tar.gz"),
                extract_dir: ".".to_owned(),
                binary_rel_path: "cua-driver".to_owned(),
                has_app: false,
            })
        }
        "win32" => {
            let slug = if arch == "arm64" {
                "windows-arm64"
            } else {
                "windows-x86_64"
            };
            let extract_dir = format!("cua-driver-rs-{version}-{slug}");
            Ok(AssetTarget {
                asset: format!("{extract_dir}.zip"),
                extract_dir,
                binary_rel_path: "cua-driver.exe".to_owned(),
                has_app: false,
            })
        }
        _ => Err(format!("Computer Use: unsupported platform '{platform}'.")),
    }
}

/// Resolve download sources in override, Canopy OSS, GitHub order.
pub fn resolve_asset_urls(asset: &str, download_host: Option<&str>, version: &str) -> Vec<String> {
    let mut urls = Vec::with_capacity(3);
    if let Some(host) = download_host.filter(|host| !host.is_empty()) {
        urls.push(format!(
            "{}/cua-driver-rs/v{version}/{asset}",
            host.strip_suffix('/').unwrap_or(host)
        ));
    }
    urls.push(format!(
        "{OSS_MIRROR_BASE}/cua-driver-rs/v{version}/{asset}"
    ));
    urls.push(format!(
        "{GITHUB_RELEASE_BASE}/cua-driver-rs-v{version}/{asset}"
    ));
    urls
}

/// Resolve checksum locations using the same source precedence as assets.
pub fn resolve_checksum_urls(download_host: Option<&str>, version: &str) -> Vec<String> {
    resolve_asset_urls("checksums.txt", download_host, version)
}

/// Resolve the screenshot longest-edge override.
///
/// A valid environment value takes precedence over a setting. Invalid values
/// fall through to the setting. `0` disables resizing; negative, fractional,
/// non-finite, and blank values mean no override at that layer.
pub fn resolve_max_image_dimension(
    setting_value: Option<f64>,
    env_value: Option<&str>,
) -> Option<f64> {
    env_value
        .and_then(coerce_image_dimension_string)
        .or_else(|| setting_value.and_then(coerce_image_dimension))
}

fn coerce_image_dimension(value: f64) -> Option<f64> {
    (value.is_finite() && value >= 0.0 && value.fract() == 0.0).then_some(value)
}

fn coerce_image_dimension_string(value: &str) -> Option<f64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let parsed = parse_javascript_number(value)?;
    coerce_image_dimension(parsed)
}

/// The environment value is a string, while the source uses JavaScript's
/// `Number(value)`. Cover its ordinary numeric forms plus unsigned radix
/// prefixes so valid values like `0x400` keep their source meaning.
fn parse_javascript_number(value: &str) -> Option<f64> {
    for (prefix, radix) in [
        ("0x", 16),
        ("0X", 16),
        ("0b", 2),
        ("0B", 2),
        ("0o", 8),
        ("0O", 8),
    ] {
        if let Some(digits) = value.strip_prefix(prefix) {
            if digits.is_empty() {
                return None;
            }
            return digits.chars().try_fold(0.0_f64, |number, digit| {
                let digit = digit.to_digit(radix)?;
                Some(number * radix as f64 + f64::from(digit))
            });
        }
    }
    value.parse::<f64>().ok()
}

pub fn approval_key(version: &str) -> String {
    format!("cua-driver-rs@{version}")
}

pub fn computer_use_root(home: impl AsRef<Path>) -> PathBuf {
    home.as_ref().join(".canopy").join("computer-use")
}

pub fn version_dir(home: impl AsRef<Path>, version: &str) -> PathBuf {
    computer_use_root(home).join(format!("cua-driver-rs-{version}"))
}

pub fn binary_path(
    home: impl AsRef<Path>,
    platform: &str,
    arch: &str,
    version: &str,
) -> Result<PathBuf, String> {
    let target = resolve_asset_target(platform, arch, version)?;
    let mut path = version_dir(home, version);
    if target.extract_dir != "." {
        path.push(target.extract_dir);
    }
    Ok(path.join(target.binary_rel_path))
}

pub fn install_state_path(home: impl AsRef<Path>) -> PathBuf {
    computer_use_root(home).join("installed.json")
}

/// Read the source's download host override from the process environment.
/// Core callers can instead pass an explicit value to `resolve_asset_urls`.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_supported_asset_targets() {
        let mac = resolve_asset_target("darwin", "arm64", CUA_DRIVER_VERSION).unwrap();
        assert_eq!(mac.asset, "cua-driver-rs-0.5.2-darwin-arm64.tar.gz");
        assert_eq!(
            mac.binary_rel_path,
            "CuaDriver.app/Contents/MacOS/cua-driver"
        );
        assert!(mac.has_app);

        let linux = resolve_asset_target("linux", "x64", CUA_DRIVER_VERSION).unwrap();
        assert_eq!(linux.extract_dir, ".");
        assert_eq!(linux.binary_rel_path, "cua-driver");

        let windows = resolve_asset_target("win32", "x64", CUA_DRIVER_VERSION).unwrap();
        assert_eq!(windows.asset, "cua-driver-rs-0.5.2-windows-x86_64.zip");
        assert_eq!(windows.binary_rel_path, "cua-driver.exe");
    }

    #[test]
    fn rejects_unsupported_host_targets() {
        assert!(resolve_asset_target("linux", "arm64", CUA_DRIVER_VERSION).is_err());
        assert!(resolve_asset_target("aix", "x64", CUA_DRIVER_VERSION).is_err());
    }

    #[test]
    fn preserves_download_precedence_and_single_slash_trim() {
        let urls = resolve_asset_urls("driver.tar.gz", Some("https://mirror/"), "1.2.3");
        assert_eq!(urls.len(), 3);
        assert_eq!(urls[0], "https://mirror/cua-driver-rs/v1.2.3/driver.tar.gz");
        assert!(urls[1].contains("aliyuncs.com/computer-use"));
        assert!(urls[2].contains("github.com/trycua/cua/releases/download"));
    }

    #[test]
    fn image_dimension_precedence_and_validation_match_typescript() {
        assert_eq!(
            resolve_max_image_dimension(Some(1024.0), Some("768")),
            Some(768.0)
        );
        assert_eq!(
            resolve_max_image_dimension(Some(1024.0), Some("-1")),
            Some(1024.0)
        );
        assert_eq!(resolve_max_image_dimension(Some(0.0), None), Some(0.0));
        assert_eq!(
            resolve_max_image_dimension(None, Some("0x400")),
            Some(1024.0)
        );
        assert_eq!(resolve_max_image_dimension(None, Some("12.5")), None);
        assert_eq!(resolve_max_image_dimension(Some(12.5), None), None);
    }

    #[test]
    fn derives_paths_and_versioned_approval_key() {
        assert_eq!(
            binary_path("/home/u", "darwin", "arm64", CUA_DRIVER_VERSION)
                .unwrap()
                .to_string_lossy(),
            "/home/u/.canopy/computer-use/cua-driver-rs-0.5.2/cua-driver-rs-0.5.2-darwin-arm64/CuaDriver.app/Contents/MacOS/cua-driver"
        );
        assert_eq!(approval_key("9.9.9"), "cua-driver-rs@9.9.9");
    }
}
