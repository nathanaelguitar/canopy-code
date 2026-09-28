//! Pure HTML validation and document wrapping for interactive artifacts.

use std::sync::OnceLock;

use regex::Regex;

pub const MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_TITLE: &str = "Artifact";
const CSS_RESET: &str = "*,*::before,*::after{box-sizing:border-box}\nhtml{-webkit-text-size-adjust:100%}\nbody{margin:0;padding:1.5rem;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;line-height:1.5;color:#1a1a1a;background:#fff}\nimg,svg,video,canvas{max-width:100%;height:auto}\npre,table{max-width:100%;overflow-x:auto}\n:where(a){color:#0969da}";
const CSP: &str = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:; font-src data:; media-src data:; connect-src 'none'; form-action 'none'; base-uri 'none'; frame-ancestors 'none'; sandbox allow-scripts;";

static EXTERNAL_RESOURCE: OnceLock<Regex> = OnceLock::new();
static EXTERNAL_LINK: OnceLock<Regex> = OnceLock::new();
static JAVASCRIPT_URI: OnceLock<Regex> = OnceLock::new();
static EXTERNAL_SCRIPT: OnceLock<Regex> = OnceLock::new();
static META_REFRESH: OnceLock<Regex> = OnceLock::new();
static EXTERNAL_CSS: OnceLock<Regex> = OnceLock::new();
static DOCUMENT_WRAPPER: OnceLock<Regex> = OnceLock::new();
static DOUBLE_QUOTE_ENTITY: OnceLock<Regex> = OnceLock::new();
static SINGLE_QUOTE_ENTITY: OnceLock<Regex> = OnceLock::new();

/// Normalize a user title to one line and clamp it to 120 UTF-16 code units.
pub fn sanitize_artifact_title(raw: Option<&str>) -> String {
    let collapsed = raw
        .unwrap_or_default()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let units = collapsed.encode_utf16().take(120).collect::<Vec<_>>();
    let title = String::from_utf16_lossy(&units);
    if title.is_empty() {
        DEFAULT_TITLE.to_owned()
    } else {
        title
    }
}

/// Return an actionable model-facing error when the fragment references
/// external resources, browser egress, or a complete HTML document.
pub fn validate_self_contained(fragment: &str) -> Option<String> {
    if fragment.trim().is_empty() {
        return Some(
            "Artifact file is empty — write the page content (a body-only HTML fragment) first."
                .to_owned(),
        );
    }
    let scan = normalize_attribute_quotes(fragment);
    let head = strip_leading_comments_and_whitespace(fragment);
    let wrapper_regex = DOCUMENT_WRAPPER.get_or_init(|| {
        Regex::new(r"(?i)^(?:<!doctype\b|<html[\s>]|<head[\s>]|<body[\s>])")
            .expect("artifact wrapper regex is valid")
    });
    if let Some(found) = wrapper_regex.find(head) {
        return Some(format!(
            "Write a body-only fragment — it starts with a full-document tag ({}). Omit <!doctype>, <html>, <head>, and <body>; they are added at publish time.",
            found.as_str().trim()
        ));
    }

    let external_resource = EXTERNAL_RESOURCE.get_or_init(|| {
        Regex::new(r#"(?i)\b(?:src|srcset|poster)\s*=\s*["']?\s*(?:https?:)?//"#)
            .expect("external resource regex is valid")
    });
    let external_link = EXTERNAL_LINK.get_or_init(|| {
        Regex::new(r#"(?i)<link\b[^>]*\bhref\s*=\s*["']?\s*(?:https?:)?//"#)
            .expect("external link regex is valid")
    });
    if let Some(found) = external_resource
        .find(&scan)
        .or_else(|| external_link.find(&scan))
    {
        return Some(format!(
            "Artifact must be self-contained — found an external reference ({}). Inline scripts/styles and embed assets as data: URIs.",
            truncate(found.as_str(), 60)
        ));
    }

    let javascript_uri = JAVASCRIPT_URI.get_or_init(|| {
        Regex::new(r#"(?i)\b(?:href|src)\s*=\s*["']?\s*javascript\s*:"#)
            .expect("javascript URI regex is valid")
    });
    if let Some(found) = javascript_uri.find(&scan) {
        return Some(format!(
            "Artifact must be self-contained — found a javascript: URI ({}). Use inline <script> blocks instead.",
            truncate(found.as_str(), 60)
        ));
    }

    let external_script = EXTERNAL_SCRIPT.get_or_init(|| {
        Regex::new(concat!(
            r#"(?i)(?:\b(?:fetch|WebSocket|XMLHttpRequest)\s*\(\s*["']\s*(?:https?|wss?)://"#,
            r#"|\bimport\s*\(\s*["'](?:https?:)?//"#,
            r#"|\bwindow\.open\s*\("#,
            r#"|\blocation\.\w+\s*[=(]"#,
            r#"|\bnavigator\.sendBeacon\s*\(\s*["']\s*(?:https?:)?//)"#,
        ))
        .expect("external script regex is valid")
    });
    if let Some(found) = external_script.find(&scan) {
        return Some(format!(
            "Artifact must be self-contained — found browser network egress ({}). Embed data in the artifact instead of fetching it at runtime.",
            truncate(found.as_str(), 60)
        ));
    }

    let meta_refresh = META_REFRESH.get_or_init(|| {
        Regex::new(
            r#"(?i)<meta\b[^>]*http-equiv\s*=\s*["']?refresh["']?[^>]*\burl\s*=\s*(?:https?:)?//"#,
        )
        .expect("meta refresh regex is valid")
    });
    if let Some(found) = meta_refresh.find(&scan) {
        return Some(format!(
            "Artifact must be self-contained — found a meta refresh redirect ({}).",
            truncate(found.as_str(), 60)
        ));
    }

    let external_css = EXTERNAL_CSS.get_or_init(|| {
        Regex::new(r#"(?i)(?:@import\s+(?:url\()?|url\()\s*["']?\s*(?:https?:)?//"#)
            .expect("external CSS regex is valid")
    });
    if let Some(found) = external_css.find(&scan) {
        return Some(format!(
            "Artifact must be self-contained — found an external CSS reference ({}). Inline CSS and embed fonts/images as data: URIs.",
            truncate(found.as_str(), 60)
        ));
    }
    None
}

/// Wrap a body fragment in the same minimal self-contained HTML document used
/// by the TypeScript artifact publisher.
pub fn wrap_artifact_html(body_fragment: &str, title: Option<&str>) -> String {
    let safe_title = escape_for_title(&sanitize_artifact_title(title));
    format!(
        "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<meta http-equiv=\"Content-Security-Policy\" content=\"{CSP}\">\n<title>{safe_title}</title>\n<style>{CSS_RESET}</style>\n</head>\n<body>\n{body_fragment}\n</body>\n</html>\n"
    )
}

pub fn byte_length(value: &str) -> usize {
    value.len()
}

fn strip_leading_comments_and_whitespace(mut input: &str) -> &str {
    loop {
        input = input.trim_start_matches(char::is_whitespace);
        let Some(comment) = input.strip_prefix("<!--") else {
            return input;
        };
        let Some(end) = comment.find("-->") else {
            return input;
        };
        input = &comment[end + 3..];
    }
}

fn normalize_attribute_quotes(value: &str) -> String {
    let double_quote = DOUBLE_QUOTE_ENTITY.get_or_init(|| {
        Regex::new(r"(?i)&(?:quot|#34|#x22);").expect("quote entity regex is valid")
    });
    let single_quote = SINGLE_QUOTE_ENTITY.get_or_init(|| {
        Regex::new(r"(?i)&(?:apos|#39|#x27);").expect("quote entity regex is valid")
    });
    single_quote
        .replace_all(&double_quote.replace_all(value, "\""), "'")
        .into_owned()
}

fn truncate(value: &str, max: usize) -> String {
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = collapsed.chars();
    let prefix = chars.by_ref().take(max).collect::<String>();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

fn escape_for_title(title: &str) -> String {
    title
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
