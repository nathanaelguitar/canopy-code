//! Shared managed auto-memory data contracts.
//!
//! Port of `packages/core/src/memory/types.ts`. Persistence already owns the
//! Rust definitions so JSON format and scaffold code use a single source of
//! truth; this module exposes the focused source-level API.

pub use super::store::{
    AUTO_MEMORY_SCHEMA_VERSION, AUTO_MEMORY_TYPES, AutoMemoryExtractCursor, AutoMemoryMetadata,
    AutoMemorySourceRef, AutoMemoryStatus, AutoMemoryType,
};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn metadata_and_cursor_keep_the_typescript_json_field_names() {
        let now = Utc.with_ymd_and_hms(2026, 9, 25, 12, 0, 0).unwrap();
        let metadata = super::super::store::create_default_auto_memory_metadata(now);
        let value = serde_json::to_value(metadata).unwrap();
        assert_eq!(value["version"], AUTO_MEMORY_SCHEMA_VERSION);
        assert_eq!(value["createdAt"], "2026-09-25T12:00:00.000Z");
        assert_eq!(value["updatedAt"], "2026-09-25T12:00:00.000Z");

        let cursor = AutoMemoryExtractCursor {
            session_id: Some("session-1".to_owned()),
            processed_offset: Some(42),
            updated_at: "2026-09-25T12:00:00.000Z".to_owned(),
        };
        let value = serde_json::to_value(cursor).unwrap();
        assert_eq!(value["sessionId"], "session-1");
        assert_eq!(value["processedOffset"], 42);
        assert_eq!(value["updatedAt"], "2026-09-25T12:00:00.000Z");
    }

    #[test]
    fn memory_topics_follow_the_source_order() {
        assert_eq!(
            AUTO_MEMORY_TYPES,
            ["user", "feedback", "project", "reference"]
        );
        assert_eq!(
            AutoMemoryType::ALL.map(AutoMemoryType::as_str),
            AUTO_MEMORY_TYPES
        );
    }
}
