//! Recognition of internal background-operation prompt IDs.

const INTERNAL_PROMPT_IDS: &[&str] = &["prompt_suggestion", "forked_query", "speculation"];
const SIDE_QUERY_PROMPT_PREFIX: &str = "side-query:";

/// Return whether a prompt ID belongs to an internal background operation.
///
/// `None` and the empty string are not internal IDs. Side-query IDs are
/// recognized by an exact, case-sensitive prefix match.
pub fn is_internal_prompt_id(prompt_id: Option<&str>) -> bool {
    let Some(prompt_id) = prompt_id else {
        return false;
    };
    if prompt_id.is_empty() {
        return false;
    }

    INTERNAL_PROMPT_IDS.contains(&prompt_id) || prompt_id.starts_with(SIDE_QUERY_PROMPT_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::is_internal_prompt_id;

    #[test]
    fn recognizes_prompt_suggestion() {
        assert!(is_internal_prompt_id(Some("prompt_suggestion")));
    }

    #[test]
    fn recognizes_forked_query() {
        assert!(is_internal_prompt_id(Some("forked_query")));
    }

    #[test]
    fn recognizes_speculation() {
        assert!(is_internal_prompt_id(Some("speculation")));
    }

    #[test]
    fn does_not_recognize_user_queries() {
        assert!(!is_internal_prompt_id(Some("user_query")));
    }

    #[test]
    fn does_not_recognize_an_empty_string() {
        assert!(!is_internal_prompt_id(Some("")));
    }

    #[test]
    fn does_not_recognize_arbitrary_prompt_ids() {
        assert!(!is_internal_prompt_id(Some("btw-prompt-id")));
        assert!(!is_internal_prompt_id(Some("context-prompt-id")));
    }

    #[test]
    fn does_not_recognize_a_missing_prompt_id() {
        assert!(!is_internal_prompt_id(None));
    }

    #[test]
    fn recognizes_any_side_query_prefixed_id() {
        assert!(is_internal_prompt_id(Some("side-query:chat-compression")));
        assert!(is_internal_prompt_id(Some("side-query:session-recap")));
        assert!(is_internal_prompt_id(Some("side-query:")));
    }

    #[test]
    fn requires_the_side_query_prefix_at_the_start() {
        assert!(!is_internal_prompt_id(Some("my-side-query:test")));
        assert!(!is_internal_prompt_id(Some(" side-query:leading-space")));
    }
}
