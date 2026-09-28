//! Small stderr and optional file logger matching mobile-mcp's TypeScript
//! `trace` and `error` helpers.

use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_LOG_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LOG_RECORD_BYTES: usize = 64 * 1024;
const MAX_LOG_MESSAGE_BYTES: usize = MAX_LOG_RECORD_BYTES - 33;

static FILE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Write a trace message to stderr and, when `LOG_FILE` is set, to that file.
pub fn trace(message: &str) {
    write_log(message);
}

/// Write an error message to stderr and, when `LOG_FILE` is set, to that file.
pub fn error(message: &str) {
    write_log(message);
}

fn write_log(message: &str) {
    append_to_log_file(message);
    let _ = writeln!(std::io::stderr().lock(), "{message}");
}

fn append_to_log_file(message: &str) {
    let Some(path) = env::var_os("LOG_FILE").filter(|path| !path.is_empty()) else {
        return;
    };
    let lock = FILE_LOCK.get_or_init(|| Mutex::new(()));
    let Ok(_guard) = lock.lock() else {
        return;
    };

    let message = bounded_message(message);
    let line = format!("[{}] INFO {message}\n", iso_timestamp());
    if line.len() > MAX_LOG_RECORD_BYTES {
        return;
    }
    let Ok(options) = open_log_options() else {
        return;
    };
    let Ok(mut file) = options.open(path) else {
        return;
    };
    let Ok(metadata) = file.metadata() else {
        return;
    };
    if !metadata.is_file() || metadata.len().saturating_add(line.len() as u64) > MAX_LOG_FILE_BYTES
    {
        return;
    }
    let _ = file.write_all(line.as_bytes());
}

fn open_log_options() -> std::io::Result<OpenOptions> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    Ok(options)
}

fn bounded_message(message: &str) -> String {
    if message.len() <= MAX_LOG_MESSAGE_BYTES {
        return message.to_owned();
    }
    const SUFFIX: &str = " [truncated]";
    let end_limit = MAX_LOG_MESSAGE_BYTES.saturating_sub(SUFFIX.len());
    let mut end = end_limit.min(message.len());
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{SUFFIX}", &message[..end])
}

fn iso_timestamp() -> String {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = elapsed.as_secs();
    let days = (seconds / 86_400) as i64;
    let day_seconds = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = day_seconds / 3_600;
    let minute = (day_seconds % 3_600) / 60;
    let second = day_seconds % 60;
    let millis = elapsed.subsec_millis();
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

// Convert days since 1970-01-01 to a proleptic Gregorian calendar date.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}
