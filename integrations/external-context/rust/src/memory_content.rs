use regex::Regex;
use std::sync::LazyLock;

pub const MAX_MEMORY_CONTENT_CHARACTERS: usize = 4_000;

static NON_CONTENT_CHARACTER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[\p{White_Space}\p{Cc}\p{Cf}]$").expect("valid Unicode categories")
});

pub fn is_valid_memory_content(value: &str) -> bool {
    let mut has_visible = false;
    let mut characters = 0usize;
    for character in value.chars() {
        characters += 1;
        if characters > MAX_MEMORY_CONTENT_CHARACTERS {
            return false;
        }
        if !NON_CONTENT_CHARACTER.is_match(&character.to_string()) {
            has_visible = true;
        }
    }
    has_visible
}
