//! Bounded, shared IDE workspace context.
//!
//! This ports `packages/core/src/ide/ideContext.ts` and the corresponding
//! `IdeContext` JSON shape. Context updates retain only the ten most-recent
//! open files and cap selected text before storing or notifying subscribers.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

use indexmap::IndexMap;

pub const IDE_MAX_OPEN_FILES: usize = 10;
pub const IDE_MAX_SELECTED_TEXT_LENGTH: usize = 16_384;
const SELECTED_TEXT_TRUNCATION_MARKER: &str = "... [TRUNCATED]";

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IdeCursor {
    pub line: f64,
    pub character: f64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IdeOpenFile {
    pub path: String,
    pub timestamp: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_active: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<IdeCursor>,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IdeWorkspaceState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_files: Option<Vec<IdeOpenFile>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_trusted: Option<bool>,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IdeContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_state: Option<IdeWorkspaceState>,
}

type Subscriber = Arc<dyn Fn(Option<IdeContext>) + Send + Sync + 'static>;

#[derive(Default)]
struct Subscribers {
    next_id: AtomicU64,
    listeners: Mutex<IndexMap<u64, Subscriber>>,
}

/// Thread-safe replacement for the source's process-wide IDE context store.
pub struct IdeContextStore {
    state: RwLock<Option<IdeContext>>,
    subscribers: Arc<Subscribers>,
}

impl Default for IdeContextStore {
    fn default() -> Self {
        Self {
            state: RwLock::new(None),
            subscribers: Arc::new(Subscribers::default()),
        }
    }
}

pub struct IdeContextSubscription {
    subscribers: Weak<Subscribers>,
    id: u64,
}

impl Drop for IdeContextSubscription {
    fn drop(&mut self) {
        if let Some(subscribers) = self.subscribers.upgrade() {
            subscribers
                .listeners
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .shift_remove(&self.id);
        }
    }
}

impl IdeContextStore {
    pub fn set(&self, mut context: IdeContext) {
        if let Some(workspace) = context.workspace_state.as_mut() {
            if let Some(open_files) = workspace.open_files.take() {
                workspace.open_files = Some(normalize_open_files(open_files));
            }
        }
        *self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(context);
        self.notify_subscribers();
    }

    pub fn clear(&self) {
        *self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        self.notify_subscribers();
    }

    pub fn get(&self) -> Option<IdeContext> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn subscribe(
        &self,
        subscriber: impl Fn(Option<IdeContext>) + Send + Sync + 'static,
    ) -> IdeContextSubscription {
        let id = self
            .subscribers
            .next_id
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.subscribers
            .listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, Arc::new(subscriber));
        IdeContextSubscription {
            subscribers: Arc::downgrade(&self.subscribers),
            id,
        }
    }

    fn notify_subscribers(&self) {
        let current = self.get();
        let listeners = self
            .subscribers
            .listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for listener in listeners {
            let value = current.clone();
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| listener(value)));
        }
    }
}

/// Process-wide context instance used by provider, trust, and UI adapters.
pub fn ide_context_store() -> &'static IdeContextStore {
    static STORE: OnceLock<IdeContextStore> = OnceLock::new();
    STORE.get_or_init(IdeContextStore::default)
}

fn normalize_open_files(mut input: Vec<IdeOpenFile>) -> Vec<IdeOpenFile> {
    // Retain only the best ten entries with stable timestamp ordering. This
    // avoids allocating a second full-size sorted vector for an IDE payload.
    let mut newest = Vec::with_capacity(IDE_MAX_OPEN_FILES);
    for (original_index, file) in input.drain(..).enumerate() {
        let position = newest
            .iter()
            .position(|(seen_index, seen_file): &(usize, IdeOpenFile)| {
                match file.timestamp.partial_cmp(&seen_file.timestamp) {
                    Some(Ordering::Greater) => true,
                    Some(Ordering::Less) => false,
                    Some(Ordering::Equal) | None => original_index < *seen_index,
                }
            })
            .unwrap_or(newest.len());
        newest.insert(position, (original_index, file));
        if newest.len() > IDE_MAX_OPEN_FILES {
            newest.pop();
        }
    }
    let mut files = newest.into_iter().map(|(_, file)| file).collect::<Vec<_>>();

    let Some(most_recent) = files.first() else {
        return files;
    };
    if most_recent.is_active != Some(true) {
        for file in &mut files {
            file.is_active = Some(false);
            file.cursor = None;
            file.selected_text = None;
        }
        return files;
    }

    if files[0]
        .selected_text
        .as_ref()
        .is_some_and(|selected_text| {
            selected_text
                .encode_utf16()
                .take(IDE_MAX_SELECTED_TEXT_LENGTH + 1)
                .count()
                > IDE_MAX_SELECTED_TEXT_LENGTH
        })
    {
        if let Some(selected_text) = files[0].selected_text.as_mut() {
            let mut truncated = truncate_utf16_safely(selected_text, IDE_MAX_SELECTED_TEXT_LENGTH);
            truncated.push_str(SELECTED_TEXT_TRUNCATION_MARKER);
            *selected_text = truncated;
        }
    }
    for file in files.iter_mut().skip(1) {
        file.is_active = Some(false);
        file.cursor = None;
        file.selected_text = None;
    }
    files
}

fn truncate_utf16_safely(input: &str, limit: usize) -> String {
    let mut end = 0;
    let mut units = 0;
    for (index, character) in input.char_indices() {
        let width = character.len_utf16();
        if units + width > limit {
            break;
        }
        units += width;
        end = index + character.len_utf8();
    }
    input[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, timestamp: f64, is_active: Option<bool>) -> IdeOpenFile {
        IdeOpenFile {
            path: path.to_owned(),
            timestamp,
            is_active,
            selected_text: Some("selected".to_owned()),
            cursor: Some(IdeCursor {
                line: 3.0,
                character: 5.0,
            }),
        }
    }

    #[test]
    fn sorts_stably_and_keeps_only_the_ten_newest_files() {
        let store = IdeContextStore::default();
        let open_files = (0..15)
            .map(|index| file(&format!("/file-{index}"), index as f64, Some(true)))
            .collect();
        store.set(IdeContext {
            workspace_state: Some(IdeWorkspaceState {
                open_files: Some(open_files),
                is_trusted: Some(true),
            }),
        });
        let context = store.get().unwrap();
        let workspace = context.workspace_state.unwrap();
        let files = workspace.open_files.unwrap();
        assert_eq!(files.len(), IDE_MAX_OPEN_FILES);
        assert_eq!(files[0].path, "/file-14");
        assert_eq!(files[9].path, "/file-5");
        assert_eq!(workspace.is_trusted, Some(true));
    }

    #[test]
    fn inactive_most_recent_file_clears_selection_and_cursor_for_all() {
        let store = IdeContextStore::default();
        store.set(IdeContext {
            workspace_state: Some(IdeWorkspaceState {
                open_files: Some(vec![
                    file("/older", 1.0, Some(true)),
                    file("/newer", 2.0, Some(false)),
                ]),
                is_trusted: None,
            }),
        });
        let files = store
            .get()
            .unwrap()
            .workspace_state
            .unwrap()
            .open_files
            .unwrap();
        assert_eq!(files[0].path, "/newer");
        assert!(files.iter().all(|file| file.is_active == Some(false)));
        assert!(files.iter().all(|file| file.cursor.is_none()));
        assert!(files.iter().all(|file| file.selected_text.is_none()));
    }

    #[test]
    fn only_the_newest_file_remains_active_and_selected_text_is_bounded() {
        let store = IdeContextStore::default();
        store.set(IdeContext {
            workspace_state: Some(IdeWorkspaceState {
                open_files: Some(vec![
                    file("/older", 1.0, Some(true)),
                    IdeOpenFile {
                        selected_text: Some("a".repeat(IDE_MAX_SELECTED_TEXT_LENGTH + 1)),
                        ..file("/newer", 2.0, Some(true))
                    },
                ]),
                is_trusted: Some(false),
            }),
        });
        let files = store
            .get()
            .unwrap()
            .workspace_state
            .unwrap()
            .open_files
            .unwrap();
        assert_eq!(files[0].is_active, Some(true));
        assert!(
            files[0]
                .selected_text
                .as_ref()
                .unwrap()
                .ends_with(SELECTED_TEXT_TRUNCATION_MARKER)
        );
        assert_eq!(
            files[0]
                .selected_text
                .as_ref()
                .unwrap()
                .trim_end_matches(SELECTED_TEXT_TRUNCATION_MARKER)
                .encode_utf16()
                .count(),
            IDE_MAX_SELECTED_TEXT_LENGTH
        );
        assert_eq!(files[1].is_active, Some(false));
        assert!(files[1].selected_text.is_none());
        assert!(files[1].cursor.is_none());
    }

    #[test]
    fn subscribers_receive_updates_and_unsubscribe() {
        let store = IdeContextStore::default();
        let calls = Arc::new(AtomicU64::new(0));
        let calls_by_subscriber = Arc::clone(&calls);
        let subscription = store.subscribe(move |_| {
            calls_by_subscriber.fetch_add(1, AtomicOrdering::Relaxed);
        });
        store.set(IdeContext::default());
        assert_eq!(calls.load(AtomicOrdering::Relaxed), 1);
        drop(subscription);
        store.clear();
        assert_eq!(calls.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn serde_uses_the_source_camel_case_contract() {
        let context: IdeContext = serde_json::from_value(serde_json::json!({
            "workspaceState": {
                "isTrusted": false,
                "openFiles": [{
                    "path": "/repo/main.rs",
                    "timestamp": 12,
                    "isActive": true,
                    "cursor": {"line": 2, "character": 9}
                }]
            }
        }))
        .unwrap();
        assert_eq!(context.workspace_state.unwrap().is_trusted, Some(false));
    }
}
