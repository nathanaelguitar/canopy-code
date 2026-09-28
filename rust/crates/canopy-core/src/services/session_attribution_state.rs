//! Restore the latest commit-attribution snapshot from an active transcript.

use serde_json::Value;

use crate::transcript::TranscriptRecord;

/// Returns the last object-valued attribution snapshot in the supplied
/// branch-ordered transcript records. The commit-attribution service performs
/// schema validation and applies its reset/coercion rules when restoring it.
pub fn restored_attribution_snapshot(records: &[TranscriptRecord]) -> Option<Value> {
    records
        .iter()
        .filter(|record| record.subtype.as_deref() == Some("attribution_snapshot"))
        .filter_map(|record| {
            record
                .extra
                .get("systemPayload")
                .and_then(|payload| payload.get("snapshot"))
                .filter(|snapshot| snapshot.is_object())
                .cloned()
        })
        .last()
}
