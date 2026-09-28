//! Bounded, session-scoped image blob storage used by ACP content blocks.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, OnceCell};
use uuid::Uuid;

pub const SESSION_MEDIA_MAX_ITEM_BYTES: usize = 8 * 1024 * 1024;
pub const SESSION_MEDIA_MAX_TOTAL_BYTES: usize = 100 * 1024 * 1024;
pub const SESSION_MEDIA_MAX_ITEMS: usize = 256;
pub const SESSION_MEDIA_UNAVAILABLE_TEXT: &str = "[Attached media is no longer available]";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionMediaReference {
    #[serde(rename = "type")]
    pub media_type: String,
    #[serde(rename = "mediaId")]
    pub media_id: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
    pub size: usize,
}

impl SessionMediaReference {
    pub fn is_valid(&self) -> bool {
        self.media_type == "image"
            && !self.media_id.is_empty()
            && self.mime_type.starts_with("image/")
            && self.size > 0
    }
}

#[derive(Debug, Error)]
pub enum SessionMediaError {
    #[error("Invalid session media reference")]
    InvalidReference,
    #[error("Unknown or unavailable session media: {0}")]
    Gone(String),
    #[error("Session media store is closed")]
    Closed,
    #[error("Session media must be image/*")]
    InvalidMimeType,
    #[error("Session media must be between 1 and {SESSION_MEDIA_MAX_ITEM_BYTES} bytes")]
    InvalidSize,
    #[error("Session media exceeds the {SESSION_MEDIA_MAX_TOTAL_BYTES}-byte session limit")]
    TotalLimit,
    #[error("Session media exceeds the {SESSION_MEDIA_MAX_ITEMS}-item session limit")]
    ItemLimit,
    #[error("Session media referenced more than once: {0}")]
    Duplicate(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
struct StoredMedia {
    reference: SessionMediaReference,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionMediaDegradedContent {
    pub retained_blocks: Vec<Value>,
    pub resolved_blocks: Vec<Value>,
    pub degraded: usize,
}

/// Caller-owned memo for sharing successful media reads and encodes across
/// concurrent content-resolution calls. Failed initializations are not cached.
#[derive(Default)]
pub struct SessionMediaResolveMemo {
    entries: Mutex<HashMap<String, Arc<OnceCell<Value>>>>,
}

impl SessionMediaResolveMemo {
    pub fn new() -> Self {
        Self::default()
    }

    async fn cell(&self, media_id: &str) -> Arc<OnceCell<Value>> {
        let mut entries = self.entries.lock().await;
        entries
            .entry(media_id.to_owned())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    }

    async fn remove_failed(&self, media_id: &str, cell: &Arc<OnceCell<Value>>) {
        let mut entries = self.entries.lock().await;
        if Arc::strong_count(cell) == 2
            && entries
                .get(media_id)
                .is_some_and(|current| Arc::ptr_eq(current, cell))
        {
            entries.remove(media_id);
        }
    }
}

#[derive(Default)]
struct StoreState {
    records: std::collections::HashMap<String, StoredMedia>,
    total_bytes: usize,
    pending_items: usize,
    closed: bool,
}

/// One store belongs to one session; blobs are private files removed on close.
pub struct SessionMediaStore {
    state: Arc<Mutex<StoreState>>,
    directory: OnceCell<PathBuf>,
}

impl Default for SessionMediaStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionMediaStore {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(StoreState::default())),
            directory: OnceCell::new(),
        }
    }

    pub async fn put(
        &self,
        data: &[u8],
        mime_type: &str,
    ) -> Result<SessionMediaReference, SessionMediaError> {
        if !mime_type.starts_with("image/") {
            return Err(SessionMediaError::InvalidMimeType);
        }
        if data.is_empty() || data.len() > SESSION_MEDIA_MAX_ITEM_BYTES {
            return Err(SessionMediaError::InvalidSize);
        }
        {
            let mut state = self.state.lock().await;
            if state.closed {
                return Err(SessionMediaError::Closed);
            }
            if state.total_bytes + data.len() > SESSION_MEDIA_MAX_TOTAL_BYTES {
                return Err(SessionMediaError::TotalLimit);
            }
            if state.records.len() + state.pending_items >= SESSION_MEDIA_MAX_ITEMS {
                return Err(SessionMediaError::ItemLimit);
            }
            state.total_bytes += data.len();
            state.pending_items += 1;
        }
        let media_id = Uuid::new_v4().to_string();
        let reference = SessionMediaReference {
            media_type: "image".into(),
            media_id: media_id.clone(),
            mime_type: mime_type.into(),
            size: data.len(),
        };
        let result = async {
            let dir = self
                .directory
                .get_or_try_init(|| async {
                    let base = std::env::temp_dir().join("qwen-session-media-");
                    tokio::fs::create_dir_all(&base).await?;
                    let path = base.join(Uuid::new_v4().to_string());
                    tokio::fs::create_dir(&path).await?;
                    Ok::<_, std::io::Error>(path)
                })
                .await?;
            let path = dir.join(&media_id);
            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .await?;
            file.write_all(data).await?;
            file.flush().await?;
            let mut state = self.state.lock().await;
            if state.closed {
                drop(state);
                let _ = tokio::fs::remove_file(path).await;
                return Err(SessionMediaError::Closed);
            }
            state.records.insert(
                media_id.clone(),
                StoredMedia {
                    reference: reference.clone(),
                },
            );
            Ok(reference.clone())
        }
        .await;
        let mut state = self.state.lock().await;
        if result.is_err() {
            state.total_bytes = state.total_bytes.saturating_sub(data.len());
        }
        state.pending_items = state.pending_items.saturating_sub(1);
        result
    }

    pub async fn assert_reference(&self, value: &Value) -> Result<(), SessionMediaError> {
        let Some(obj) = value.as_object() else {
            return Ok(());
        };
        if !obj.contains_key("mediaId") {
            return Ok(());
        }
        let reference: SessionMediaReference = serde_json::from_value(value.clone())
            .map_err(|_| SessionMediaError::InvalidReference)?;
        if !reference.is_valid() {
            return Err(SessionMediaError::InvalidReference);
        }
        let state = self.state.lock().await;
        let stored = state
            .records
            .get(&reference.media_id)
            .ok_or_else(|| SessionMediaError::Gone(reference.media_id.clone()))?;
        if stored.reference != reference {
            return Err(SessionMediaError::Gone(reference.media_id));
        }
        Ok(())
    }

    pub async fn assert_references(&self, content: &[Value]) -> Result<(), SessionMediaError> {
        let mut seen = HashSet::new();
        for block in content {
            if block.get("mediaId").is_none() {
                continue;
            }
            let reference: SessionMediaReference = serde_json::from_value(block.clone())
                .map_err(|_| SessionMediaError::InvalidReference)?;
            if !reference.is_valid() {
                return Err(SessionMediaError::InvalidReference);
            }
            if !seen.insert(reference.media_id.clone()) {
                return Err(SessionMediaError::Duplicate(reference.media_id));
            }
            self.assert_reference(block).await?;
        }
        Ok(())
    }

    pub async fn read(
        &self,
        media_id: &str,
    ) -> Result<Option<(Vec<u8>, String)>, SessionMediaError> {
        let record = {
            let state = self.state.lock().await;
            state.records.get(media_id).cloned()
        };
        let Some(record) = record else {
            return Ok(None);
        };
        let path = self
            .directory
            .get()
            .ok_or_else(|| SessionMediaError::Gone(media_id.to_owned()))?
            .join(media_id);
        match tokio::fs::read(path).await {
            Ok(bytes) => Ok(Some((bytes, record.reference.mime_type))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut state = self.state.lock().await;
                if let Some(removed) = state.records.remove(media_id) {
                    state.total_bytes = state.total_bytes.saturating_sub(removed.reference.size);
                }
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn resolve_content(
        &self,
        content: &[Value],
    ) -> Result<Vec<Value>, SessionMediaError> {
        let memo = SessionMediaResolveMemo::new();
        self.resolve_content_with_memo(content, &memo).await
    }

    /// Resolve content with a caller-owned memo that can be shared by
    /// concurrent calls for the same session media store.
    pub async fn resolve_content_with_memo(
        &self,
        content: &[Value],
        memo: &SessionMediaResolveMemo,
    ) -> Result<Vec<Value>, SessionMediaError> {
        let mut out = Vec::with_capacity(content.len());
        for block in content {
            let Some(media_id) = block
                .get("mediaId")
                .and_then(Value::as_str)
                .map(str::to_owned)
            else {
                out.push(block.clone());
                continue;
            };
            let cell = memo.cell(&media_id).await;
            let result = cell
                .get_or_try_init(|| async {
                    self.assert_reference(block).await?;
                    let (bytes, mime_type) = self
                        .read(&media_id)
                        .await?
                        .ok_or_else(|| SessionMediaError::Gone(media_id.clone()))?;
                    Ok(json!({
                        "type": "image",
                        "data": base64::engine::general_purpose::STANDARD.encode(bytes),
                        "mimeType": mime_type
                    }))
                })
                .await
                .map(|resolved| resolved.clone());
            match result {
                Ok(resolved) => out.push(resolved),
                Err(error) => {
                    memo.remove_failed(&media_id, &cell).await;
                    return Err(error);
                }
            }
        }
        Ok(out)
    }

    /// Resolve media independently so an unavailable attachment does not
    /// discard the surrounding text and media blocks. Only reference errors
    /// degrade; filesystem and other operational errors still propagate.
    pub async fn resolve_content_degrading(
        &self,
        content: &[Value],
    ) -> Result<SessionMediaDegradedContent, SessionMediaError> {
        let memo = SessionMediaResolveMemo::new();
        self.resolve_content_degrading_with_memo(content, &memo)
            .await
    }

    /// Degrading variant that shares successful resolutions across calls.
    pub async fn resolve_content_degrading_with_memo(
        &self,
        content: &[Value],
        memo: &SessionMediaResolveMemo,
    ) -> Result<SessionMediaDegradedContent, SessionMediaError> {
        let mut retained_blocks = Vec::with_capacity(content.len());
        let mut resolved_blocks = Vec::with_capacity(content.len());
        let mut degraded = 0;

        for block in content {
            if !is_session_media_reference(block) {
                retained_blocks.push(block.clone());
                resolved_blocks.push(block.clone());
                continue;
            }

            match self
                .resolve_content_with_memo(std::slice::from_ref(block), memo)
                .await
            {
                Ok(mut resolved) => {
                    let Some(resolved) = resolved.pop() else {
                        return Err(SessionMediaError::Gone(
                            block
                                .get("mediaId")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                        ));
                    };
                    retained_blocks.push(block.clone());
                    resolved_blocks.push(resolved);
                }
                Err(SessionMediaError::InvalidReference | SessionMediaError::Gone(_)) => {
                    degraded += 1;
                }
                Err(error) => return Err(error),
            }
        }

        Ok(SessionMediaDegradedContent {
            retained_blocks,
            resolved_blocks,
            degraded,
        })
    }

    pub async fn remove(&self, media_id: &str) -> Result<bool, SessionMediaError> {
        let record = { self.state.lock().await.records.remove(media_id) };
        let Some(record) = record else {
            return Ok(false);
        };
        {
            let mut state = self.state.lock().await;
            state.total_bytes = state.total_bytes.saturating_sub(record.reference.size);
        }
        if let Some(dir) = self.directory.get() {
            match tokio::fs::remove_file(dir.join(media_id)).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(true)
    }

    pub async fn size_bytes(&self) -> usize {
        self.state.lock().await.total_bytes
    }

    pub async fn close(&self) -> Result<(), SessionMediaError> {
        {
            let mut state = self.state.lock().await;
            if state.closed {
                return Ok(());
            }
            state.closed = true;
            state.records.clear();
            state.total_bytes = 0;
            state.pending_items = 0;
        }
        if let Some(dir) = self.directory.get() {
            tokio::fs::remove_dir_all(dir).await?;
        }
        Ok(())
    }
}

pub fn is_session_media_reference(value: &Value) -> bool {
    serde_json::from_value::<SessionMediaReference>(value.clone()).is_ok_and(|r| r.is_valid())
}

/// Add the unavailable marker to the last text block or append a new block.
pub fn with_media_degradation_marker(blocks: &[Value]) -> Vec<Value> {
    let mut result = blocks.to_vec();
    if let Some(index) = result
        .iter()
        .rposition(|block| block.get("type").and_then(Value::as_str) == Some("text"))
    {
        let text = result[index]
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if text.ends_with(SESSION_MEDIA_UNAVAILABLE_TEXT) {
            return result;
        }
        result[index] =
            json!({"type":"text", "text":format!("{text}\n{SESSION_MEDIA_UNAVAILABLE_TEXT}")});
    } else {
        result.push(json!({"type":"text", "text":SESSION_MEDIA_UNAVAILABLE_TEXT}));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn media_is_bounded_resolved_and_removed_with_store() {
        let store = SessionMediaStore::new();
        let reference = store.put(b"image", "image/png").await.unwrap();
        assert_eq!(store.size_bytes().await, 5);
        store
            .assert_reference(&serde_json::to_value(&reference).unwrap())
            .await
            .unwrap();
        let resolved = store
            .resolve_content(&[serde_json::to_value(&reference).unwrap()])
            .await
            .unwrap();
        assert_eq!(resolved[0]["data"], "aW1hZ2U=");
        assert!(store.remove(&reference.media_id).await.unwrap());
        assert_eq!(store.read(&reference.media_id).await.unwrap(), None);
        store.close().await.unwrap();
    }

    #[test]
    fn degradation_marker_preserves_blocks_and_deduplicates_marker() {
        let original = vec![json!({"type":"text","text":"hello"})];
        let once = with_media_degradation_marker(&original);
        assert!(
            once[0]["text"]
                .as_str()
                .unwrap()
                .ends_with(SESSION_MEDIA_UNAVAILABLE_TEXT)
        );
        assert_eq!(with_media_degradation_marker(&once), once);
    }
}
