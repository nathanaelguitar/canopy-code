//! Native implementation of `canopy review meta`.
//!
//! This command is read-only and delegates GitHub identity lookup to the
//! installed `gh` CLI, matching the TypeScript review platform's current
//! registry (which contains only GitHub).

use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::io::{self, Write};
use std::process::{Command, Output};
use std::thread;
use std::time::Duration;

const USAGE: &str = "Usage: canopy review meta [pr_number] [--repo owner/repo] [--host hostname]";
const MAX_GH_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
const GH_TRANSIENT_RETRIES: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorClass {
    Usage,
    Runtime,
}

#[derive(Debug)]
pub struct ReviewMetaError {
    class: ErrorClass,
    message: String,
}

impl ReviewMetaError {
    fn usage(message: impl Into<String>) -> Self {
        Self {
            class: ErrorClass::Usage,
            message: message.into(),
        }
    }

    fn runtime(message: impl Into<String>) -> Self {
        Self {
            class: ErrorClass::Runtime,
            message: message.into(),
        }
    }

    pub fn class(&self) -> ErrorClass {
        self.class
    }

    pub fn exit_code(&self) -> i32 {
        match self.class {
            ErrorClass::Usage => 2,
            ErrorClass::Runtime => 1,
        }
    }

    pub fn diagnostic(&self) -> String {
        format!("meta: {}", self.message)
    }
}

impl Display for ReviewMetaError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ReviewMetaError {}

#[derive(Debug, Deserialize)]
struct GhLogin {
    login: String,
}

#[derive(Debug, Deserialize)]
struct GhRepoParent {
    owner: GhLogin,
    name: String,
}

#[derive(Debug, Deserialize)]
struct GhRepoView {
    owner: GhLogin,
    name: String,
    url: String,
    #[serde(default)]
    parent: Option<GhRepoParent>,
}

#[derive(Debug, Deserialize)]
struct GhPrView {
    #[serde(rename = "headRefOid")]
    head_ref_oid: Option<String>,
    url: Option<String>,
}

#[derive(Debug, Serialize)]
struct MetaResult {
    platform: &'static str,
    host: String,
    #[serde(rename = "ownerRepo")]
    owner_repo: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    number: Option<u64>,
    #[serde(rename = "headSha", skip_serializing_if = "Option::is_none")]
    head_sha: Option<String>,
    #[serde(rename = "webUrl", skip_serializing_if = "Option::is_none")]
    web_url: Option<String>,
}

#[derive(Default)]
struct MetaArgs {
    pr_number_raw: Option<String>,
    repo: Option<String>,
    host: Option<String>,
}

/// Run the `review meta` subcommand. `args` begins after the `meta` token.
///
/// Errors retain their TypeScript exit distinction: malformed invocation is
/// exit 2 and authentication, GitHub, or environment failures are exit 1.
/// The dispatcher should print `error.diagnostic()` and use `error.exit_code()`.
pub fn run(args: &[String]) -> Result<(), ReviewMetaError> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        println!("{USAGE}");
        println!(
            "Print repository identity and optional pull request head metadata as one JSON object."
        );
        return Ok(());
    }

    let args = parse_args(args)?;
    let pr_number = parse_pr_number(args.pr_number_raw.as_deref())?;

    // The source handler validates the flag before runMeta validates --repo;
    // both checks happen before the auth gate.
    let flag_host = validate_host_flag(args.host.as_deref())?;
    if let Some(repo) = args.repo.as_deref() {
        if !is_owner_repo(repo) {
            return Err(ReviewMetaError::usage(format!(
                "expected owner/repo, got {}",
                json_string(repo)
            )));
        }
    }

    let result = resolve_meta(
        pr_number,
        args.repo.as_deref(),
        args.host.as_deref(),
        flag_host,
    )?;
    let json = serde_json::to_string(&result)
        .map_err(|error| ReviewMetaError::runtime(format!("could not encode metadata: {error}")))?;
    writeln!(io::stdout().lock(), "{json}")
        .map_err(|error| ReviewMetaError::runtime(format!("could not write metadata: {error}")))
}

fn parse_args(args: &[String]) -> Result<MetaArgs, ReviewMetaError> {
    let mut parsed = MetaArgs::default();
    let mut positional_seen = false;
    let mut index = 0;

    while index < args.len() {
        let argument = &args[index];
        if let Some(value) = argument.strip_prefix("--repo=") {
            parsed.repo = Some(value.to_owned());
        } else if argument == "--repo" {
            index += 1;
            let value = args.get(index).ok_or_else(|| {
                ReviewMetaError::usage(format!("{USAGE}\n--repo requires owner/repo."))
            })?;
            parsed.repo = Some(value.clone());
        } else if let Some(value) = argument.strip_prefix("--host=") {
            parsed.host = Some(value.to_owned());
        } else if argument == "--host" {
            index += 1;
            let value = args.get(index).ok_or_else(|| {
                ReviewMetaError::usage(format!("{USAGE}\n--host requires a hostname."))
            })?;
            parsed.host = Some(value.clone());
        } else if argument.starts_with('-') && argument.parse::<f64>().is_err() {
            return Err(ReviewMetaError::usage(format!(
                "{USAGE}\nunknown option: {argument}"
            )));
        } else if !positional_seen {
            parsed.pr_number_raw = Some(argument.clone());
            positional_seen = true;
        } else {
            return Err(ReviewMetaError::usage(format!(
                "{USAGE}\naccepts at most one pull request number."
            )));
        }
        index += 1;
    }

    Ok(parsed)
}

fn parse_pr_number(raw: Option<&str>) -> Result<Option<u64>, ReviewMetaError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let parsed = if raw.trim().is_empty() {
        Some(0.0)
    } else {
        raw.trim().parse::<f64>().ok()
    };
    let Some(number) = parsed.filter(|number| {
        number.is_finite() && *number > 0.0 && number.fract() == 0.0 && *number < u64::MAX as f64
    }) else {
        return Err(ReviewMetaError::usage(format!(
            "pr_number must be a positive integer, got {}",
            json_number_for_error(raw)
        )));
    };
    Ok(Some(number as u64))
}

fn json_number_for_error(raw: &str) -> String {
    if raw.trim().is_empty() {
        return "0".to_owned();
    }
    match raw.trim().parse::<f64>() {
        Ok(value) if value.is_finite() => js_number_string(value),
        _ => "null".to_owned(),
    }
}

fn js_number_string(value: f64) -> String {
    if value == 0.0 {
        "0".to_owned()
    } else if value.fract() == 0.0 && value.abs() < 1e21 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn validate_host_flag(host: Option<&str>) -> Result<Option<String>, ReviewMetaError> {
    let Some(host) = host else {
        return Ok(None);
    };
    if host.is_empty() {
        return Ok(None);
    }
    let trimmed = host.trim();
    if !is_hostname(trimmed) {
        return Err(ReviewMetaError::usage(format!(
            "--host must be a hostname (optionally :port), got {}",
            json_string(host)
        )));
    }
    Ok(Some(trimmed.to_owned()))
}

fn is_hostname(host: &str) -> bool {
    let name = match host.split_once(':') {
        Some((name, port))
            if !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            name
        }
        Some(_) => return false,
        None => host,
    };
    !name.is_empty()
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
}

fn is_owner_repo(repo: &str) -> bool {
    let mut parts = repo.split('/');
    let Some(owner) = parts.next() else {
        return false;
    };
    let Some(name) = parts.next() else {
        return false;
    };
    if parts.next().is_some() || owner.is_empty() || name.is_empty() {
        return false;
    }
    let valid_segment = |segment: &str| {
        segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    };
    !owner.starts_with('-')
        && valid_segment(owner)
        && valid_segment(name)
        && owner != "."
        && owner != ".."
        && name != "."
        && name != ".."
}

fn resolve_meta(
    pr_number: Option<u64>,
    explicit_repo: Option<&str>,
    host_flag: Option<&str>,
    host_override: Option<String>,
) -> Result<MetaResult, ReviewMetaError> {
    ensure_authenticated(host_override.as_deref())?;

    let (owner_repo, output_host, pr_route_host) = if let Some(repo) = explicit_repo {
        let effective =
            resolve_gh_host(host_flag, env_gh_host()).unwrap_or_else(|| "github.com".to_owned());
        validate_routed_host(&effective, host_flag.is_some())?;
        (repo.to_owned(), effective, host_override)
    } else {
        let view: GhRepoView = gh_json(
            &[
                "repo".to_owned(),
                "view".to_owned(),
                "--json".to_owned(),
                "owner,name,url,parent".to_owned(),
            ],
            host_override.as_deref(),
            "repository",
        )?;
        let output_host = host_of_repo_url(&view.url);
        let target = view.parent;
        let (owner, name) = target
            .map(|parent| (parent.owner.login, parent.name))
            .unwrap_or((view.owner.login, view.name));
        let routed =
            resolve_gh_host(host_flag, env_gh_host()).unwrap_or_else(|| output_host.clone());
        validate_routed_host(&routed, host_flag.is_some())?;
        (format!("{owner}/{name}"), output_host, Some(routed))
    };

    let (number, head_sha, web_url) = if let Some(number) = pr_number {
        let view: GhPrView = gh_json(
            &[
                "pr".to_owned(),
                "view".to_owned(),
                number.to_string(),
                "--repo".to_owned(),
                owner_repo.clone(),
                "--json".to_owned(),
                "headRefOid,url".to_owned(),
            ],
            pr_route_host.as_deref(),
            "pull request",
        )?;
        (Some(number), view.head_ref_oid, view.url)
    } else {
        (None, None, None)
    };

    Ok(MetaResult {
        platform: "github",
        host: output_host,
        owner_repo,
        number,
        head_sha,
        web_url,
    })
}

fn env_gh_host() -> Option<String> {
    std::env::var("GH_HOST")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn resolve_gh_host(flag_host: Option<&str>, env_host: Option<String>) -> Option<String> {
    match flag_host {
        Some(host) if !host.is_empty() => Some(host.trim().to_owned()),
        _ => env_host,
    }
}

fn validate_routed_host(host: &str, flag_was_provided: bool) -> Result<(), ReviewMetaError> {
    if is_hostname(host) {
        return Ok(());
    }
    let source = if flag_was_provided {
        "--host flag"
    } else {
        "GH_HOST environment"
    };
    Err(ReviewMetaError::runtime(format!(
        "cannot route at the {source} {} — not a hostname the review subcommands accept",
        json_string(host)
    )))
}

fn host_of_repo_url(url: &str) -> String {
    let authority_source = url
        .split_once("://")
        .filter(|(scheme, _)| {
            !scheme.is_empty() && scheme.bytes().all(|byte| byte.is_ascii_alphabetic())
        })
        .map(|(_, remainder)| remainder)
        .unwrap_or(url);
    authority_source
        .split('/')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn ensure_authenticated(host_override: Option<&str>) -> Result<(), ReviewMetaError> {
    for attempt in 0..=1 {
        let mut command = Command::new("gh");
        command.args(["auth", "status"]);
        if let Some(host) = host_override {
            command.env("GH_HOST", host);
        }
        match command.output() {
            Ok(output) if output.status.success() => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => break,
            Ok(_) | Err(_) if attempt == 0 => thread::sleep(Duration::from_secs(2)),
            Ok(_) | Err(_) => break,
        }
    }
    Err(ReviewMetaError::runtime(
        "gh CLI is not authenticated. Run `gh auth login` and retry.",
    ))
}

fn gh_json<T: for<'de> Deserialize<'de>>(
    args: &[String],
    host_override: Option<&str>,
    label: &str,
) -> Result<T, ReviewMetaError> {
    let output = run_gh(args, host_override)?;
    serde_json::from_str(&output).map_err(|error| {
        ReviewMetaError::runtime(format!("could not parse {label} metadata from gh: {error}"))
    })
}

fn run_gh(args: &[String], host_override: Option<&str>) -> Result<String, ReviewMetaError> {
    for attempt in 0..=GH_TRANSIENT_RETRIES {
        let mut command = Command::new("gh");
        command.args(args);
        if let Some(host) = host_override {
            command.env("GH_HOST", host);
        }
        let output = command
            .output()
            .map_err(|error| ReviewMetaError::runtime(format!("could not run gh: {error}")))?;
        if output.stdout.len().saturating_add(output.stderr.len()) > MAX_GH_OUTPUT_BYTES {
            return Err(ReviewMetaError::runtime(
                "gh command output exceeded the 64 MiB limit",
            ));
        }
        if output.status.success() {
            return Ok(String::from_utf8_lossy(&output.stdout)
                .replace("\r\n", "\n")
                .trim()
                .to_owned());
        }

        let failure = gh_failure_message(args, &output);
        if attempt < GH_TRANSIENT_RETRIES && is_transient_gh_error(&failure) {
            thread::sleep(Duration::from_secs(3 * (attempt as u64 + 1)));
            continue;
        }
        return Err(ReviewMetaError::runtime(failure));
    }
    unreachable!("gh retry loop returns on success or final failure")
}

fn gh_failure_message(args: &[String], output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        format!(
            "Command failed: gh {} (exit status {})",
            args.join(" "),
            output
                .status
                .code()
                .map_or_else(|| "signal".to_owned(), |code| code.to_string())
        )
    } else {
        format!("Command failed: gh {}: {stderr}", args.join(" "))
    }
}

fn is_transient_gh_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    (0..=9).any(|first| (0..=9).any(|second| lower.contains(&format!("http 5{first}{second}"))))
        || [
            "server is currently unavailable",
            "service unavailable",
            "bad gateway",
            "internal server error",
        ]
        .iter()
        .any(|needle| lower.contains(needle))
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned())
}
