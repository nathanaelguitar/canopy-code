//! Deterministic relevance selection for channel memory entries.
//!
//! This mirrors `packages/channels/base/src/channel-memory-recall.ts`. The
//! tokenizer, scoring, ordering, fallback, and text budgets follow that module.

use regex::Regex;
use std::collections::HashSet;
use std::sync::OnceLock;
use unicode_normalization::UnicodeNormalization;

pub const CHANNEL_MEMORY_RECALL_MAX_ENTRIES: usize = 3;
pub const CHANNEL_MEMORY_RECALL_MAX_CODE_POINTS: usize = 1_200;
pub const CHANNEL_MEMORY_RECALL_FALLBACK_CODE_POINTS: usize = 120;

const TRUNCATION_SUFFIX: &str = " [truncated]";

/// A channel memory item accepted by the recall selector.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChannelMemoryEntry {
    pub id: String,
    pub text: String,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub created_by: Option<String>,
}

impl ChannelMemoryEntry {
    pub fn new(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            text: text.into(),
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug)]
struct IndexedCandidate {
    entry: ChannelMemoryEntry,
    index: usize,
    entry_terms: HashSet<String>,
    normalized_length: usize,
}

/// An immutable, reusable snapshot of channel memory entries for recall.
#[derive(Clone, Debug, Default)]
pub struct ChannelMemoryRecallIndex {
    candidates: Vec<IndexedCandidate>,
}

fn term_pattern() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r"(?P<latin>\p{Script=Latin}+)|(?P<number>\p{Decimal_Number}+)|(?P<han>\p{Script_Extensions=Han}+)|(?P<hiragana>\p{Script_Extensions=Hiragana}+)|(?P<katakana>\p{Script_Extensions=Katakana}+)|(?P<hangul>\p{Script_Extensions=Hangul}+)",
        )
        .expect("channel memory recall term pattern is valid")
    })
}

/// Same unsafe-invisible set and replacement behavior as
/// `PROMPT_UNSAFE_INVISIBLES` in the TypeScript channel sanitizer. The Rust
/// sanitizer keeps its regex private, so this exact filter remains local.
fn unsafe_invisibles() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"[\p{Cf}\p{Variation_Selector}\u{0080}-\u{009F}\u{2028}\u{2029}]")
            .expect("channel memory recall invisible pattern is valid")
    })
}

/// Apply Unicode NFKC, Unicode lowercase, and then the channel invisible filter.
fn normalize_for_recall(text: &str) -> String {
    let nfkc: String = text.nfkc().collect();
    let lowercased = nfkc.to_lowercase();
    unsafe_invisibles()
        .replace_all(&lowercased, " ")
        .into_owned()
}

fn terms(normalized: &str) -> HashSet<String> {
    let mut result = HashSet::new();
    for captures in term_pattern().captures_iter(normalized) {
        let (namespace, run) = if let Some(run) = captures.name("latin") {
            ("latin", run.as_str())
        } else if let Some(run) = captures.name("number") {
            ("number", run.as_str())
        } else if let Some(run) = captures.name("han") {
            ("han", run.as_str())
        } else if let Some(run) = captures.name("hiragana") {
            ("hiragana", run.as_str())
        } else if let Some(run) = captures.name("katakana") {
            ("katakana", run.as_str())
        } else if let Some(run) = captures.name("hangul") {
            ("hangul", run.as_str())
        } else {
            continue;
        };
        let characters: Vec<char> = run.chars().collect();
        match namespace {
            "latin" | "number" => {
                if characters.len() >= 2 {
                    result.insert(format!("{namespace}:{run}"));
                }
            }
            _ => {
                for pair in characters.windows(2) {
                    result.insert(format!("{namespace}:{}{}", pair[0], pair[1]));
                }
            }
        }
    }
    result
}

fn truncate_entry_to_recall_budget(entry: &ChannelMemoryEntry) -> ChannelMemoryEntry {
    let suffix_length = TRUNCATION_SUFFIX.chars().count();
    let keep = CHANNEL_MEMORY_RECALL_MAX_CODE_POINTS - suffix_length;
    let mut text: String = entry.text.chars().take(keep).collect();
    text.push_str(TRUNCATION_SUFFIX);
    let mut truncated = entry.clone();
    truncated.text = text;
    truncated
}

/// Build an immutable recall snapshot. Entries and their metadata are cloned,
/// so later caller mutations do not affect this index.
pub fn create_channel_memory_recall_index(
    entries: &[ChannelMemoryEntry],
) -> ChannelMemoryRecallIndex {
    let candidates = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let normalized = normalize_for_recall(&entry.text);
            IndexedCandidate {
                entry: entry.clone(),
                index,
                entry_terms: terms(&normalized),
                normalized_length: normalized.chars().count(),
            }
        })
        .collect();
    ChannelMemoryRecallIndex { candidates }
}

/// Select relevant memories from a prepared immutable index.
pub fn select_relevant_channel_memory_from_index(
    message: &str,
    recall_index: &ChannelMemoryRecallIndex,
) -> Vec<ChannelMemoryEntry> {
    let message_terms = terms(&normalize_for_recall(message));
    let mut positives: Vec<(&IndexedCandidate, usize)> = Vec::new();
    let mut fallbacks: Vec<&IndexedCandidate> = Vec::new();

    for candidate in &recall_index.candidates {
        let score = candidate
            .entry_terms
            .iter()
            .filter(|term| message_terms.contains(*term))
            .count();
        if score > 0 {
            positives.push((candidate, score));
        } else if candidate.normalized_length <= CHANNEL_MEMORY_RECALL_FALLBACK_CODE_POINTS {
            fallbacks.push(candidate);
        }
    }

    // Use original position as an explicit stable tiebreaker.
    positives.sort_by(|(left, left_score), (right, right_score)| {
        right_score
            .cmp(left_score)
            .then_with(|| left.index.cmp(&right.index))
    });

    let mut selected = Vec::new();
    let mut used_code_points = 0usize;
    for (candidate, score) in positives
        .into_iter()
        .chain(fallbacks.into_iter().map(|candidate| (candidate, 0)))
    {
        if selected.len() >= CHANNEL_MEMORY_RECALL_MAX_ENTRIES {
            break;
        }
        let entry_code_points = candidate.entry.text.chars().count();
        if used_code_points + entry_code_points > CHANNEL_MEMORY_RECALL_MAX_CODE_POINTS {
            if score > 0
                && selected.is_empty()
                && entry_code_points > CHANNEL_MEMORY_RECALL_MAX_CODE_POINTS
            {
                selected.push(truncate_entry_to_recall_budget(&candidate.entry));
                break;
            }
            continue;
        }
        selected.push(candidate.entry.clone());
        used_code_points += entry_code_points;
    }
    selected
}

/// Select relevant memories and prepare an index for this call.
pub fn select_relevant_channel_memory(
    message: &str,
    entries: &[ChannelMemoryEntry],
) -> Vec<ChannelMemoryEntry> {
    select_relevant_channel_memory_from_index(message, &create_channel_memory_recall_index(entries))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, text: impl Into<String>) -> ChannelMemoryEntry {
        ChannelMemoryEntry::new(id, text)
    }

    fn long_fact(text: &str) -> String {
        format!(
            "{text} {}",
            "z".repeat(CHANNEL_MEMORY_RECALL_FALLBACK_CODE_POINTS + 1)
        )
    }

    #[test]
    fn prepared_index_is_immutable_and_matches_one_shot_selection() {
        let mut entries = vec![
            entry("fallback", "short preference"),
            entry("relevant", long_fact("deploy staging")),
        ];
        let index = create_channel_memory_recall_index(&entries);
        assert_eq!(
            select_relevant_channel_memory_from_index("deploy staging", &index),
            select_relevant_channel_memory("deploy staging", &entries)
        );

        entries[0].text = "changed after indexing".to_owned();
        entries[1].text = "production only".to_owned();
        assert_eq!(
            select_relevant_channel_memory_from_index("deploy staging", &index),
            vec![
                entry("relevant", long_fact("deploy staging")),
                entry("fallback", "short preference")
            ]
        );
    }

    #[test]
    fn matches_nfkc_compatibility_forms_and_lowercases_latin() {
        let matching = entry("matching", long_fact("deploy staging"));
        assert_eq!(
            select_relevant_channel_memory(
                "ＤＥＰＬＯＹ to STAGING",
                std::slice::from_ref(&matching)
            ),
            vec![matching]
        );

        let normalized = entry("normalized", long_fact("café release 12"));
        assert_eq!(
            select_relevant_channel_memory(
                "cafe\u{301} release ①②",
                std::slice::from_ref(&normalized)
            ),
            vec![normalized]
        );
    }

    #[test]
    fn ignores_latin_and_decimal_terms_shorter_than_two_code_points() {
        let short_terms = entry("short", long_fact("x 7"));
        let decimal_term = entry("decimal", long_fact("42"));
        assert_eq!(
            select_relevant_channel_memory("x 7 42", &[short_terms, decimal_term.clone()]),
            vec![decimal_term]
        );
    }

    #[test]
    fn matches_sliding_bigrams_for_han_hiragana_katakana_and_hangul() {
        for (message, fact) in [
            ("数据治理", "项目治理规范"),
            ("たのしい", "うれしいこと"),
            ("カタカナ", "ナカナミ"),
            ("데이터관리", "품질관리자"),
        ] {
            let matching = entry("matching", long_fact(fact));
            assert_eq!(
                select_relevant_channel_memory(message, std::slice::from_ref(&matching)),
                vec![matching],
                "failed for {message}"
            );
        }
    }

    #[test]
    fn single_cjk_character_does_not_score() {
        assert!(
            select_relevant_channel_memory("数", &[entry("single", long_fact("数"))]).is_empty()
        );
    }

    #[test]
    fn script_extensions_join_katakana_runs() {
        let matching = entry("matching", long_fact("コーヒー"));
        assert_eq!(
            select_relevant_channel_memory("コーヒー", std::slice::from_ref(&matching)),
            vec![matching]
        );
    }

    #[test]
    fn punctuation_whitespace_and_invisibles_separate_terms() {
        let joined = entry(
            "joined",
            long_fact("deploytarget buildnext releasecandidate"),
        );
        let separated = entry(
            "separated",
            long_fact("deploy target build next release candidate"),
        );
        assert_eq!(
            select_relevant_channel_memory(
                "deploy,target\tbuild\u{200b}next\u{2028}release-candidate",
                &[joined, separated.clone()]
            ),
            vec![separated]
        );
    }

    #[test]
    fn ranks_by_unique_overlap_count_and_keeps_stable_ties() {
        let one_term = entry("one", long_fact("alpha alpha alpha"));
        let first_tie = entry("first-tie", long_fact("alpha beta"));
        let second_tie = entry("second-tie", long_fact("beta gamma"));
        assert_eq!(
            select_relevant_channel_memory(
                "alpha alpha beta gamma",
                &[one_term.clone(), first_tie.clone(), second_tie.clone()]
            ),
            vec![first_tie, second_tie, one_term]
        );
    }

    #[test]
    fn places_short_fallbacks_after_positives_and_excludes_long_ones() {
        let fallback = entry(
            "fallback",
            "x".repeat(CHANNEL_MEMORY_RECALL_FALLBACK_CODE_POINTS),
        );
        let long_unrelated = entry(
            "long-unrelated",
            "y".repeat(CHANNEL_MEMORY_RECALL_FALLBACK_CODE_POINTS + 1),
        );
        let positive = entry("positive", long_fact("deploy"));
        assert_eq!(
            select_relevant_channel_memory(
                "deploy",
                &[fallback.clone(), long_unrelated, positive.clone()]
            ),
            vec![positive, fallback]
        );
    }

    #[test]
    fn fallback_length_uses_normalized_code_points() {
        let at_limit = entry(
            "at-limit",
            "ﬃ".repeat(CHANNEL_MEMORY_RECALL_FALLBACK_CODE_POINTS / 3),
        );
        let over_limit = entry(
            "over-limit",
            "ﬃ".repeat(CHANNEL_MEMORY_RECALL_FALLBACK_CODE_POINTS / 3 + 1),
        );
        assert_eq!(
            select_relevant_channel_memory("unrelated", &[at_limit.clone(), over_limit]),
            vec![at_limit]
        );
    }

    #[test]
    fn returns_at_most_three_entries() {
        let entries = vec![
            entry("one", "fallback one"),
            entry("two", "fallback two"),
            entry("three", "fallback three"),
            entry("four", "fallback four"),
        ];
        assert_eq!(
            select_relevant_channel_memory("unrelated", &entries),
            entries[..CHANNEL_MEMORY_RECALL_MAX_ENTRIES]
        );
    }

    #[test]
    fn skips_nonfitting_entry_and_keeps_later_entry_that_fits() {
        let first = entry("first", format!("aa {}", "😀".repeat(697)));
        let does_not_fit = entry("does-not-fit", format!("bb {}", "😀".repeat(498)));
        let later_fit = entry("later-fit", format!("cc {}", "😀".repeat(497)));
        let selected = select_relevant_channel_memory(
            "aa bb cc",
            &[first.clone(), does_not_fit, later_fit.clone()],
        );
        assert_eq!(selected, vec![first.clone(), later_fit.clone()]);
        assert_eq!(
            first.text.chars().count() + later_fit.text.chars().count(),
            CHANNEL_MEMORY_RECALL_MAX_CODE_POINTS
        );
    }

    #[test]
    fn truncates_an_oversized_relevant_entry_with_the_expected_suffix() {
        let relevant = entry(
            "relevant",
            format!(
                "deploy runbook {}",
                "x".repeat(CHANNEL_MEMORY_RECALL_MAX_CODE_POINTS)
            ),
        );
        let selected = select_relevant_channel_memory("deploy", std::slice::from_ref(&relevant));
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, relevant.id);
        assert!(selected[0].text.starts_with("deploy runbook"));
        assert!(selected[0].text.ends_with(TRUNCATION_SUFFIX));
        assert_eq!(
            selected[0].text.chars().count(),
            CHANNEL_MEMORY_RECALL_MAX_CODE_POINTS
        );
        assert!(!relevant.text.ends_with("[truncated]"));
    }

    #[test]
    fn leaves_fitting_entries_and_the_input_unchanged() {
        let fallback = entry("fallback", "short preference");
        let relevant = entry("relevant", long_fact("deploy"));
        let entries = vec![fallback.clone(), relevant.clone()];
        let before = entries.clone();
        assert_eq!(
            select_relevant_channel_memory("deploy", &entries),
            vec![relevant, fallback]
        );
        assert_eq!(entries, before);
    }
}
