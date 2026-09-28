//! Explain the removal of the legacy `canopy auth` command.

use std::io::{self, IsTerminal};

pub fn run() {
    let no_color = std::env::var("NO_COLOR").is_ok_and(|value| !value.is_empty());
    let tty = io::stdout().is_terminal() && !no_color;
    let cyan = |value: &str| color(value, "36", tty);
    let yellow = |value: &str| color(value, "33", tty);
    println!();
    println!("{}", yellow("⚠  canopy auth has been removed."));
    println!();
    println!(
        "  {}   →  run canopy and use /auth to configure providers",
        cyan("Interactive")
    );
    println!(
        "  {} → set provider environment variables, for example OPENAI_API_KEY + OPENAI_BASE_URL + OPENAI_MODEL",
        cyan("CI / Headless")
    );
    println!("                     or pass --openai-api-key, --openai-base-url, --model");
    println!(
        "  {}   → set BAILIAN_CODING_PLAN_API_KEY and use the Coding Plan base URL for your region",
        cyan("Coding Plan")
    );
    println!("                     China: https://coding.dashscope.aliyuncs.com/v1");
    println!("                     International: https://coding-intl.dashscope.aliyuncs.com/v1");
    println!(
        "  {}    → set OPENROUTER_API_KEY and OPENAI_BASE_URL=https://openrouter.ai/api/v1",
        cyan("OpenRouter")
    );
    println!(
        "  {}      → set REQUESTY_API_KEY and OPENAI_BASE_URL=https://router.requesty.ai/v1",
        cyan("Requesty")
    );
    println!(
        "  {} → run canopy interactively and use /auth; OAuth cannot be configured with env vars alone",
        cyan("Canopy OAuth")
    );
    println!(
        "  {}      → edit ~/.canopy/settings.json, or run canopy interactively once",
        cyan("Scripted")
    );
    println!();
    println!("  Check auth status → {}", cyan("/doctor"));
    println!();
}

fn color(value: &str, code: &str, enabled: bool) -> String {
    if enabled {
        format!("\x1b[{code}m{value}\x1b[0m")
    } else {
        value.to_owned()
    }
}
