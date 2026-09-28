//! Native interactive token usage summaries and exports.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use canopy_core::services::token_usage::{
    TokenUsageExportFormat, TokenUsageGroupSummary, TokenUsagePeriod, TokenUsageQuery,
    TokenUsageSummary, format_token_usage_summary_as_csv, format_token_usage_summary_as_json,
    query_token_usage,
};
use canopy_core::services::usage_history::{
    ApiMetrics, ModelMetrics, SessionMetrics, TokenMetrics,
};
use chrono::Utc;
use uuid::Uuid;

const USAGE: &str = "Usage: /stats [model|tools|skills|daily [YYYY-MM-DD]|monthly [YYYY-MM]|export <daily|monthly> [date|month] [--format csv|json] [--output path]]";

pub fn execute(runtime_base_dir: &Path, project_root: &Path, args: &str) -> Result<String, String> {
    let tokens = tokenize_args(args)?;
    let Some(command) = tokens.first().map(String::as_str) else {
        return Ok(USAGE.to_owned());
    };

    match command {
        "daily" | "day" => {
            if tokens.len() > 2 {
                return Err(format!("Unexpected argument: {}", tokens[2]));
            }
            let summary = query_token_usage(
                runtime_base_dir,
                TokenUsageQuery {
                    period: TokenUsagePeriod::Day,
                    value: tokens.get(1).cloned(),
                },
                Utc::now(),
            )?;
            Ok(format_summary(&summary))
        }
        "monthly" | "month" => {
            if tokens.len() > 2 {
                return Err(format!("Unexpected argument: {}", tokens[2]));
            }
            let summary = query_token_usage(
                runtime_base_dir,
                TokenUsageQuery {
                    period: TokenUsagePeriod::Month,
                    value: tokens.get(1).cloned(),
                },
                Utc::now(),
            )?;
            Ok(format_summary(&summary))
        }
        "export" => export(runtime_base_dir, project_root, &tokens[1..]),
        _ => Err(format!("{USAGE}\nUnknown stats command: {command}")),
    }
}

/// Dispatch stats subcommands that need the current session's in-memory
/// metrics, falling back to the token-usage history commands for daily,
/// monthly, and export views.
pub fn execute_with_session_metrics(
    runtime_base_dir: &Path,
    project_root: &Path,
    args: &str,
    metrics: Option<&SessionMetrics>,
) -> Result<String, String> {
    let tokens = tokenize_args(args)?;
    match tokens.first().map(String::as_str) {
        Some("model") => {
            reject_view_arguments(&tokens)?;
            let metrics = metrics.ok_or_else(session_metrics_unavailable)?;
            Ok(format_model_stats(metrics))
        }
        Some("tools") => {
            reject_view_arguments(&tokens)?;
            let metrics = metrics.ok_or_else(session_metrics_unavailable)?;
            Ok(format_tool_stats(metrics))
        }
        Some("skills") => {
            reject_view_arguments(&tokens)?;
            Ok(metrics.map_or_else(
                || "Skill statistics are not available in the native runtime.".to_owned(),
                format_skill_stats,
            ))
        }
        _ => execute(runtime_base_dir, project_root, args),
    }
}

fn session_metrics_unavailable() -> String {
    "Session metrics are not available for this session.".to_owned()
}

fn reject_view_arguments(tokens: &[String]) -> Result<(), String> {
    if let Some(argument) = tokens.get(1) {
        return Err(format!("Unexpected argument: {argument}"));
    }
    Ok(())
}

/// Render the model breakdown shown by the TypeScript `/stats model` view.
pub fn format_model_stats(metrics: &SessionMetrics) -> String {
    let entries = model_source_entries(&metrics.models);
    if entries.is_empty() {
        return "No API calls have been made in this session.".to_owned();
    }

    let mut lines = vec![
        "Model Stats For Nerds".to_owned(),
        String::new(),
        "Model | Requests | Errors | Avg latency".to_owned(),
    ];
    for (label, api, _) in &entries {
        let requests = api.total_requests;
        let error_rate = if requests > 0.0 {
            api.total_errors / requests * 100.0
        } else {
            0.0
        };
        let average_latency = if requests > 0.0 {
            api.total_latency_ms / requests
        } else {
            0.0
        };
        lines.push(format!(
            "{label} | {} | {} ({error_rate:.1}%) | {}",
            format_metric(requests),
            format_metric(api.total_errors),
            format_duration_ms(average_latency),
        ));
    }

    let has_token_counts = entries.iter().any(|(_, _, tokens)| {
        tokens.total_tokens > 0.0
            || tokens.prompt_tokens > 0.0
            || tokens.candidates > 0.0
            || tokens.cached_tokens > 0.0
            || tokens.thoughts_tokens > 0.0
    });
    lines.push(String::new());
    if has_token_counts {
        lines.push("Tokens by model:".to_owned());
        for (label, _, tokens) in entries {
            let cache_rate = if tokens.prompt_tokens > 0.0 {
                tokens.cached_tokens / tokens.prompt_tokens * 100.0
            } else {
                0.0
            };
            lines.push(format!(
                "{label}: total={}, prompt={}, cached={} ({cache_rate:.1}%), thoughts={}, output={}",
                format_metric(tokens.total_tokens),
                format_metric(tokens.prompt_tokens),
                format_metric(tokens.cached_tokens),
                format_metric(tokens.thoughts_tokens),
                format_metric(tokens.candidates),
            ));
        }
    } else {
        lines.push("No token counts were reported by the provider.".to_owned());
    }
    lines.join("\n")
}

/// Render tool calls, success rates, and average durations for `/stats tools`.
pub fn format_tool_stats(metrics: &SessionMetrics) -> String {
    let tools = metrics
        .tools
        .by_name
        .iter()
        .filter(|(_, stats)| stats.count > 0.0)
        .collect::<Vec<_>>();
    if tools.is_empty() {
        return "No tool calls have been made in this session.".to_owned();
    }
    let mut lines = vec![
        "Tool Stats For Nerds".to_owned(),
        String::new(),
        format!(
            "Tool calls: {} ({} ok, {} fail)",
            format_metric(metrics.tools.total_calls),
            format_metric(metrics.tools.total_success),
            format_metric(metrics.tools.total_fail),
        ),
        format!(
            "Total tool duration: {}",
            format_duration_ms(metrics.tools.total_duration_ms),
        ),
        "Tool name | Calls | Success rate | Avg duration".to_owned(),
    ];
    for (name, stats) in tools {
        let rate = success_rate(stats.count, stats.success);
        let average_duration = if stats.count > 0.0 {
            stats.duration_ms / stats.count
        } else {
            0.0
        };
        lines.push(format!(
            "{name} | {} | {rate:.1}% | {}",
            format_metric(stats.count),
            format_duration_ms(average_duration),
        ));
    }
    lines.join("\n")
}

/// Render call counts and success rates for `/stats skills`.
pub fn format_skill_stats(metrics: &SessionMetrics) -> String {
    let Some(skills) = metrics.skills.as_ref() else {
        return "Skill statistics are not available in the native runtime.".to_owned();
    };
    let mut entries = skills
        .by_name
        .iter()
        .filter(|(_, stats)| stats.count > 0.0)
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return "No skill calls have been made in this session.".to_owned();
    }
    entries.sort_by(|(left_name, left), (right_name, right)| {
        right
            .count
            .total_cmp(&left.count)
            .then_with(|| left_name.cmp(right_name))
    });

    let mut lines = vec![
        "Skill Stats For Nerds".to_owned(),
        String::new(),
        format!(
            "Skill calls: {} ({} ok, {} fail)",
            format_metric(skills.total_calls),
            format_metric(skills.total_success),
            format_metric(skills.total_fail),
        ),
        "Skill name | Calls | OK | Fail | Success rate".to_owned(),
    ];
    for (name, stats) in entries {
        let rate = success_rate(stats.count, stats.success);
        lines.push(format!(
            "{name} | {} | {} | {} | {rate:.1}%",
            format_metric(stats.count),
            format_metric(stats.success),
            format_metric(stats.fail),
        ));
    }
    lines.join("\n")
}

fn model_source_entries<'a>(
    models: impl IntoIterator<Item = (&'a String, &'a ModelMetrics)>,
) -> Vec<(String, ApiMetrics, TokenMetrics)> {
    let models = models.into_iter().collect::<Vec<_>>();
    let has_non_main_source = models
        .iter()
        .map(|(_, metrics)| *metrics)
        .any(|metrics| metrics.by_source.keys().any(|source| source != "main"));
    let mut entries = Vec::new();

    for (model, metrics) in models {
        if metrics.api.total_requests <= 0.0 {
            continue;
        }
        let display_model = model.strip_suffix("-001").unwrap_or(model);
        if metrics.by_source.is_empty() {
            entries.push((
                display_model.to_owned(),
                metrics.api.clone(),
                metrics.tokens.clone(),
            ));
            continue;
        }
        if !has_non_main_source {
            let source = metrics.by_source.get("main");
            let api = source.map_or_else(|| metrics.api.clone(), |value| value.api.clone());
            let tokens =
                source.map_or_else(|| metrics.tokens.clone(), |value| value.tokens.clone());
            entries.push((display_model.to_owned(), api, tokens));
            continue;
        }

        let mut sources = metrics.by_source.iter().collect::<Vec<_>>();
        sources.sort_by(
            |(left, _), (right, _)| match (*left == "main", *right == "main") {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => left.cmp(right),
            },
        );
        for (source, value) in sources {
            entries.push((
                format!("{display_model} ({source})"),
                value.api.clone(),
                value.tokens.clone(),
            ));
        }
    }
    entries
}

fn success_rate(count: f64, success: f64) -> f64 {
    if count > 0.0 {
        success / count * 100.0
    } else {
        0.0
    }
}

fn format_metric(value: f64) -> String {
    if !value.is_finite() || value <= 0.0 {
        return "0".to_owned();
    }
    let rounded = value.round();
    if (value - rounded).abs() < 0.000_001 {
        let digits = format!("{rounded:.0}");
        return group_digits(&digits);
    }
    format!("{value:.1}")
}

fn group_digits(digits: &str) -> String {
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(character);
    }
    grouped
}

fn format_duration_ms(milliseconds: f64) -> String {
    if !milliseconds.is_finite() || milliseconds <= 0.0 {
        return "0s".to_owned();
    }
    if milliseconds < 1_000.0 {
        return format!("{}ms", milliseconds.round());
    }
    let seconds = milliseconds / 1_000.0;
    if seconds < 60.0 {
        let rounded = (seconds * 10.0).round() / 10.0;
        return if rounded >= 60.0 {
            "1m".to_owned()
        } else {
            format!("{rounded:.1}s")
        };
    }
    let total_seconds = seconds.floor() as u64;
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    let mut parts = Vec::new();
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 {
        parts.push(format!("{minutes}m"));
    }
    if seconds > 0 {
        parts.push(format!("{seconds}s"));
    }
    if parts.is_empty() {
        return if hours > 0 {
            format!("{hours}h")
        } else if minutes > 0 {
            format!("{minutes}m")
        } else {
            format!("{seconds}s")
        };
    }
    parts.join(" ")
}

fn format_summary(summary: &TokenUsageSummary) -> String {
    let label = match summary.period {
        TokenUsagePeriod::Day => "Daily",
        TokenUsagePeriod::Month => "Monthly",
    };
    let mut lines = vec![
        format!("{label} token usage for {}", summary.value),
        format!("Total: {} tokens", summary.totals.total_tokens),
        format!("Requests: {}", summary.totals.requests),
        String::new(),
        "Breakdown:".to_owned(),
        format!("  Input: {}", summary.totals.input_tokens),
        format!("  Output: {}", summary.totals.output_tokens),
        format!(
            "  Cached (included in Input): {}",
            summary.totals.cached_tokens
        ),
        format!("  Thoughts: {}", summary.totals.thoughts_tokens),
        String::new(),
    ];
    append_group_lines(&mut lines, "By model:", &summary.by_model);
    lines.push(String::new());
    append_group_lines(&mut lines, "By auth type:", &summary.by_auth_type);
    lines.push(String::new());
    append_group_lines(
        &mut lines,
        "By model/auth type:",
        &summary.by_model_and_auth_type,
    );
    lines.push(String::new());
    append_group_lines(&mut lines, "By source:", &summary.by_source);
    lines.push(String::new());
    lines.push("Note: generation timing (TTFT/TPS) belongs to generation metrics.".to_owned());
    lines.join("\n")
}

fn append_group_lines(lines: &mut Vec<String>, title: &str, groups: &[TokenUsageGroupSummary]) {
    lines.push(title.to_owned());
    if groups.is_empty() {
        lines.push("  No usage data.".to_owned());
        return;
    }
    lines.extend(groups.iter().map(|group| {
        let label = match (
            group.model.as_deref(),
            group.auth_type.as_deref(),
            group.source.as_deref(),
        ) {
            (Some(model), Some(auth_type), _) => format!("{model} ({auth_type})"),
            (Some(model), None, _) => model.to_owned(),
            (None, Some(auth_type), _) => auth_type.to_owned(),
            (None, None, Some(source)) => source.to_owned(),
            (None, None, None) => group.key.clone(),
        };
        format!(
            "  {label}: {} tokens ({} requests)",
            group.totals.total_tokens, group.totals.requests
        )
    }));
}

struct ParsedExportArgs {
    period: TokenUsagePeriod,
    value: Option<String>,
    format: TokenUsageExportFormat,
    output_path: Option<PathBuf>,
}

fn export(runtime_base_dir: &Path, project_root: &Path, args: &[String]) -> Result<String, String> {
    let parsed = parse_export_args(args)?;
    let summary = query_token_usage(
        runtime_base_dir,
        TokenUsageQuery {
            period: parsed.period,
            value: parsed.value,
        },
        Utc::now(),
    )?;
    let contents = match parsed.format {
        TokenUsageExportFormat::Json => format_token_usage_summary_as_json(&summary)?,
        TokenUsageExportFormat::Csv => {
            format!("{}\n", format_token_usage_summary_as_csv(&summary))
        }
    };
    let extension = match parsed.format {
        TokenUsageExportFormat::Json => "json",
        TokenUsageExportFormat::Csv => "csv",
    };
    let period_name = match summary.period {
        TokenUsagePeriod::Day => "day",
        TokenUsagePeriod::Month => "month",
    };
    let default_name = format!(
        "canopy-token-usage-{period_name}-{}.{}",
        summary.value, extension
    );
    let requested = parsed.output_path.unwrap_or_else(|| default_name.into());
    let written = write_export_atomically(project_root, &requested, &contents)?;
    let root = project_root
        .canonicalize()
        .unwrap_or_else(|_| project_root.to_path_buf());
    let display = written.strip_prefix(root).unwrap_or(&written);
    Ok(format!(
        "Token usage exported to {}: {}",
        extension.to_uppercase(),
        display.display()
    ))
}

fn parse_export_args(args: &[String]) -> Result<ParsedExportArgs, String> {
    let mut period = None;
    let mut value = None;
    let mut format = TokenUsageExportFormat::Csv;
    let mut output_path = None;
    let mut index = 0;
    while index < args.len() {
        let token = &args[index];
        if token == "--format" || token == "-f" {
            index += 1;
            let Some(value) = args.get(index) else {
                return Err("Expected --format csv or --format json.".to_owned());
            };
            format = parse_format(value)?;
        } else if let Some(value) = token.strip_prefix("--format=") {
            format = parse_format(value)?;
        } else if token == "--output" || token == "-o" {
            index += 1;
            let Some(path) = args.get(index) else {
                return Err("Expected a file path after --output.".to_owned());
            };
            if path.is_empty() {
                return Err("Expected a file path after --output.".to_owned());
            }
            output_path = Some(PathBuf::from(path));
        } else if let Some(path) = token.strip_prefix("--output=") {
            if path.is_empty() {
                return Err("Expected a file path after --output.".to_owned());
            }
            output_path = Some(PathBuf::from(path));
        } else if period.is_none() && matches!(token.as_str(), "daily" | "day") {
            period = Some(TokenUsagePeriod::Day);
        } else if period.is_none() && matches!(token.as_str(), "monthly" | "month") {
            period = Some(TokenUsagePeriod::Month);
        } else if value.is_none() {
            value = Some(token.clone());
        } else if output_path.is_none() {
            output_path = Some(PathBuf::from(token));
        } else {
            return Err(format!("Unexpected argument: {token}"));
        }
        index += 1;
    }
    let Some(period) = period else {
        return Err(USAGE.to_owned());
    };
    Ok(ParsedExportArgs {
        period,
        value,
        format,
        output_path,
    })
}

fn parse_format(value: &str) -> Result<TokenUsageExportFormat, String> {
    match value {
        "csv" => Ok(TokenUsageExportFormat::Csv),
        "json" => Ok(TokenUsageExportFormat::Json),
        _ => Err("Expected --format csv or --format json.".to_owned()),
    }
}

fn tokenize_args(args: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut chars = args.chars().peekable();
    while let Some(character) = chars.next() {
        if let Some(quote_char) = quote {
            if character == '\\' && chars.peek() == Some(&quote_char) {
                current.push(quote_char);
                chars.next();
            } else if character == quote_char {
                quote = None;
            } else {
                current.push(character);
            }
        } else if matches!(character, '"' | '\'') {
            quote = Some(character);
        } else if character.is_whitespace() {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
        } else {
            current.push(character);
        }
    }
    if quote.is_some() {
        return Err("Unclosed quote in arguments.".to_owned());
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    Ok(tokens)
}

fn write_export_atomically(
    project_root: &Path,
    requested_path: &Path,
    contents: &str,
) -> Result<PathBuf, String> {
    let root = project_root
        .canonicalize()
        .map_err(|error| format!("could not resolve project working directory: {error}"))?;
    let candidate = if requested_path.is_absolute() {
        normalize_absolute(requested_path)?
    } else {
        normalize_absolute(&root.join(requested_path))?
    };
    if !candidate.starts_with(&root) {
        return path_error();
    }
    #[cfg(windows)]
    if candidate
        .file_name()
        .is_some_and(|name| name.to_string_lossy().contains(':'))
    {
        return path_error();
    }
    let requested_directory = candidate
        .parent()
        .ok_or_else(|| "Cannot resolve export path within the working directory.".to_owned())?;
    let filename = candidate
        .file_name()
        .ok_or_else(|| "Export path must name a file.".to_owned())?;
    let existing_parent = nearest_existing_parent(requested_directory, &root)?;
    let real_existing_parent = existing_parent
        .canonicalize()
        .map_err(|error| format!("could not resolve export directory: {error}"))?;
    if !real_existing_parent.starts_with(&root) {
        return path_error();
    }

    fs::create_dir_all(requested_directory)
        .map_err(|error| format!("could not create export directory: {error}"))?;
    let output_directory = requested_directory
        .canonicalize()
        .map_err(|error| format!("could not resolve export directory: {error}"))?;
    if !output_directory.starts_with(&root) {
        return path_error();
    }
    let target_path = output_directory.join(filename);
    validate_target_file(&target_path)?;

    for _ in 0..10 {
        let temp_path = output_directory.join(format!(
            ".{}.{}.{}.tmp",
            filename.to_string_lossy(),
            std::process::id(),
            Uuid::new_v4()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = match options.open(&temp_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("could not create temporary export file: {error}")),
        };
        let write_result = file
            .write_all(contents.as_bytes())
            .and_then(|()| file.sync_all());
        drop(file);
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temp_path);
            return Err(format!("could not write token usage export: {error}"));
        }

        let canonical_temp = match temp_path.canonicalize() {
            Ok(path) => path,
            Err(error) => {
                let _ = fs::remove_file(&temp_path);
                return Err(format!("could not resolve temporary export file: {error}"));
            }
        };
        if !canonical_temp.starts_with(&root) {
            let _ = fs::remove_file(&temp_path);
            return path_error();
        }
        let current_directory = match requested_directory.canonicalize() {
            Ok(path) => path,
            Err(error) => {
                let _ = fs::remove_file(&temp_path);
                return Err(format!("could not resolve export directory: {error}"));
            }
        };
        if current_directory != output_directory || !current_directory.starts_with(&root) {
            let _ = fs::remove_file(&temp_path);
            return path_error();
        }
        if let Err(error) = validate_target_file(&target_path) {
            let _ = fs::remove_file(&temp_path);
            return Err(error);
        }
        if let Err(error) = fs::rename(&temp_path, &target_path) {
            let _ = fs::remove_file(&temp_path);
            return Err(format!("could not finalize token usage export: {error}"));
        }
        let metadata = fs::symlink_metadata(&target_path)
            .map_err(|error| format!("could not verify exported file: {error}"))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return path_error();
        }
        let real_target = target_path
            .canonicalize()
            .map_err(|error| format!("could not resolve exported file: {error}"))?;
        if !real_target.starts_with(&root) {
            return path_error();
        }
        return Ok(real_target);
    }
    Err("Could not create a temporary export file.".to_owned())
}

fn path_error<T>() -> Result<T, String> {
    Err("Token usage export path must be within the project working directory.".to_owned())
}

fn validate_target_file(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => path_error(),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("could not inspect export target: {error}")),
    }
}

fn nearest_existing_parent(path: &Path, root: &Path) -> Result<PathBuf, String> {
    let mut current = path.to_path_buf();
    while current.starts_with(root) {
        match fs::symlink_metadata(&current) {
            Ok(_) => return Ok(current),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(parent) = current.parent() else {
                    break;
                };
                current = parent.to_path_buf();
            }
            Err(error) => return Err(format!("could not inspect export directory: {error}")),
        }
    }
    Err("Cannot resolve export path within the working directory.".to_owned())
}

fn normalize_absolute(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err("Cannot resolve export path within the working directory.".to_owned());
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return path_error();
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    Ok(normalized)
}
