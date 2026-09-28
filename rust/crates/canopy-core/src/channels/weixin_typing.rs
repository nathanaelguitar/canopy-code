//! Best-effort Weixin typing tickets and lifecycle state.
//!
//! Port of `typingTickets`, `startTyping`, `stopTyping`, and `setTyping` from
//! `packages/channels/weixin/src/WeixinAdapter.ts`. ChannelBase event types and
//! the monitor's context-token cache are supplied through narrow caller seams.

use crate::channels::weixin_api;
use crate::channels::weixin_types::{GetConfigResp, SendTypingReq, TypingStatus};
use reqwest::Client;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

pub type TypingFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type ContextTokenLookup = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// Async seam around the existing Weixin API helpers. Errors are intentionally
/// reduced to strings because typing failures are always best-effort.
pub trait WeixinTypingApi: Send + Sync {
    fn get_config<'a>(
        &'a self,
        user_id: &'a str,
        context_token: Option<&'a str>,
    ) -> TypingFuture<'a, Result<GetConfigResp, String>>;

    fn send_typing<'a>(&'a self, request: SendTypingReq) -> TypingFuture<'a, Result<(), String>>;
}

/// Production API adapter backed by the shared retrying Weixin API client.
pub struct ReqwestWeixinTypingApi {
    client: Client,
    base_url: String,
    token: String,
}

impl ReqwestWeixinTypingApi {
    pub fn new(client: Client, base_url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            client,
            base_url: base_url.into(),
            token: token.into(),
        }
    }
}

impl WeixinTypingApi for ReqwestWeixinTypingApi {
    fn get_config<'a>(
        &'a self,
        user_id: &'a str,
        context_token: Option<&'a str>,
    ) -> TypingFuture<'a, Result<GetConfigResp, String>> {
        Box::pin(async move {
            let response = weixin_api::get_config(
                &self.client,
                &self.base_url,
                &self.token,
                user_id,
                context_token,
            )
            .await
            .map_err(|error| error.to_string())?;
            serde_json::from_value(response).map_err(|error| error.to_string())
        })
    }

    fn send_typing<'a>(&'a self, request: SendTypingReq) -> TypingFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let body: Value = serde_json::to_value(request).map_err(|error| error.to_string())?;
            weixin_api::send_typing(&self.client, &self.base_url, &self.token, body)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }
}

/// Process-wide cache keyed only by Weixin user ID, matching the module-level
/// TypeScript `Map` that is shared by all channel instances.
#[derive(Clone, Default)]
pub struct TypingTicketCache {
    entries: Arc<Mutex<HashMap<String, String>>>,
}

static GLOBAL_TYPING_TICKETS: LazyLock<TypingTicketCache> =
    LazyLock::new(TypingTicketCache::default);

pub fn global_typing_ticket_cache() -> TypingTicketCache {
    GLOBAL_TYPING_TICKETS.clone()
}

impl TypingTicketCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, user_id: &str) -> Option<String> {
        self.entries
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(user_id)
            .cloned()
    }

    pub fn insert(&self, user_id: impl Into<String>, ticket: impl Into<String>) {
        self.entries
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(user_id.into(), ticket.into());
    }

    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .is_empty()
    }
}

#[derive(Debug)]
struct LifecycleToken {
    id: u64,
    aborted: AtomicBool,
}

/// Maps the adapter's start/terminal notifications onto typing state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypingLifecycleEvent {
    Started,
    Terminal,
    Other,
}

/// In-memory typing lifecycle for one Weixin channel instance.
pub struct WeixinTypingLifecycle {
    api: Arc<dyn WeixinTypingApi>,
    context_token: ContextTokenLookup,
    tickets: TypingTicketCache,
    active_chats: Mutex<HashSet<String>>,
    connection: Mutex<Option<Arc<LifecycleToken>>>,
    next_connection_id: AtomicU64,
}

impl WeixinTypingLifecycle {
    /// Construct a channel controller using the process-wide ticket cache.
    pub fn new(api: Arc<dyn WeixinTypingApi>, context_token: ContextTokenLookup) -> Self {
        Self::with_ticket_cache(api, context_token, global_typing_ticket_cache())
    }

    /// Construct with an isolated or shared ticket cache, useful for tests and
    /// multi-channel hosts that explicitly own process-wide adapter state.
    pub fn with_ticket_cache(
        api: Arc<dyn WeixinTypingApi>,
        context_token: ContextTokenLookup,
        tickets: TypingTicketCache,
    ) -> Self {
        Self {
            api,
            context_token,
            tickets,
            active_chats: Mutex::new(HashSet::new()),
            connection: Mutex::new(None),
            next_connection_id: AtomicU64::new(1),
        }
    }

    /// Mark a successful connect/reconnect with a new identity token. Late
    /// starts from a previous connection are ignored when they resolve.
    pub fn connect(&self) -> u64 {
        let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *connection = Some(Arc::new(LifecycleToken {
            id,
            aborted: AtomicBool::new(false),
        }));
        id
    }

    /// Abort the current lifecycle identity and clear deduplicated active
    /// chats. In-flight HTTP calls are best-effort and are not cancelled.
    pub fn disconnect(&self) {
        if let Some(connection) = self
            .connection
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
        {
            connection.aborted.store(true, Ordering::SeqCst);
        }
        self.active_chats
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clear();
    }

    /// Start typing once for the user while a start request is in flight.
    /// Returns `false` when this chat was already active.
    pub fn start_typing(
        self: &Arc<Self>,
        user_id: impl Into<String>,
    ) -> Result<bool, TypingScheduleError> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| TypingScheduleError::NoTokioRuntime)?;
        let user_id = user_id.into();
        {
            let mut active = self
                .active_chats
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if !active.insert(user_id.clone()) {
                return Ok(false);
            }
        }
        let captured = self.current_connection();
        let lifecycle = Arc::clone(self);
        handle.spawn(async move {
            let started = lifecycle.set_typing(&user_id, true).await;
            if !lifecycle.connection_is_current(captured.as_ref()) {
                return;
            }
            if !started {
                lifecycle
                    .active_chats
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .remove(&user_id);
                return;
            }
            if !lifecycle.is_active(&user_id) {
                let _ = lifecycle.set_typing(&user_id, false).await;
            }
        });
        Ok(true)
    }

    /// Stop typing only if this chat had a corresponding active start.
    pub fn stop_typing(
        self: &Arc<Self>,
        user_id: impl Into<String>,
    ) -> Result<bool, TypingScheduleError> {
        let user_id = user_id.into();
        let was_active = self
            .active_chats
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&user_id);
        if !was_active {
            return Ok(false);
        }
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| TypingScheduleError::NoTokioRuntime)?;
        let lifecycle = Arc::clone(self);
        handle.spawn(async move {
            let _ = lifecycle.set_typing(&user_id, false).await;
        });
        Ok(true)
    }

    /// Fire-and-forget lifecycle mapping used by a channel adapter.
    pub fn on_prompt_start(self: &Arc<Self>, user_id: impl Into<String>) {
        let _ = self.start_typing(user_id);
    }

    pub fn on_prompt_end(self: &Arc<Self>, user_id: impl Into<String>) {
        let _ = self.stop_typing(user_id);
    }

    pub fn on_task_lifecycle(
        self: &Arc<Self>,
        user_id: impl Into<String>,
        event: TypingLifecycleEvent,
    ) {
        match event {
            TypingLifecycleEvent::Started => self.on_prompt_start(user_id),
            TypingLifecycleEvent::Terminal => self.on_prompt_end(user_id),
            TypingLifecycleEvent::Other => {}
        }
    }

    /// Fetch and cache a ticket on first use, then send one typing status.
    /// All API errors are converted to `false` and never escape into the
    /// message lifecycle.
    pub async fn set_typing(&self, user_id: &str, typing: bool) -> bool {
        let mut ticket = self.tickets.get(user_id);
        if ticket.is_none() {
            let context_token = (self.context_token)(user_id);
            let config = match self.api.get_config(user_id, context_token.as_deref()).await {
                Ok(config) => config,
                Err(_) => return false,
            };
            if let Some(config_ticket) = config
                .typing_ticket
                .filter(|config_ticket| !config_ticket.is_empty())
            {
                self.tickets.insert(user_id, config_ticket.clone());
                ticket = Some(config_ticket);
            }
        }
        let Some(ticket) = ticket else {
            return false;
        };

        let request = SendTypingReq {
            ilink_user_id: Some(user_id.to_owned()),
            typing_ticket: Some(ticket),
            status: Some(if typing {
                TypingStatus::TYPING
            } else {
                TypingStatus::CANCEL
            }),
            base_info: None,
        };
        self.api.send_typing(request).await.is_ok()
    }

    pub fn active_typing_chats(&self) -> HashSet<String> {
        self.active_chats
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn is_active(&self, user_id: &str) -> bool {
        self.active_chats
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .contains(user_id)
    }

    fn current_connection(&self) -> Option<Arc<LifecycleToken>> {
        self.connection
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn connection_is_current(&self, captured: Option<&Arc<LifecycleToken>>) -> bool {
        let current = self
            .connection
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        match (captured, current.as_ref()) {
            (None, None) => true,
            (Some(captured), Some(current)) => {
                captured.id == current.id
                    && Arc::ptr_eq(captured, current)
                    && !captured.aborted.load(Ordering::SeqCst)
            }
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypingScheduleError {
    NoTokioRuntime,
}

impl fmt::Display for TypingScheduleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoTokioRuntime => {
                formatter.write_str("typing lifecycle requires a Tokio runtime")
            }
        }
    }
}

impl Error for TypingScheduleError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channels::weixin_types::GetConfigResp;
    use std::collections::VecDeque;
    use tokio::sync::oneshot;

    type DeferredSend = (oneshot::Sender<()>, oneshot::Receiver<Result<(), String>>);

    #[derive(Default)]
    struct FakeApi {
        ticket: Mutex<Option<String>>,
        config_error: AtomicBool,
        config_requests: Mutex<Vec<(String, Option<String>)>>,
        send_requests: Mutex<Vec<SendTypingReq>>,
        send_results: Mutex<VecDeque<Result<(), String>>>,
        first_send_deferred: AtomicBool,
        first_send_gate: Mutex<Option<DeferredSend>>,
    }

    impl FakeApi {
        fn with_ticket(ticket: &str) -> Self {
            Self {
                ticket: Mutex::new(Some(ticket.to_owned())),
                ..Self::default()
            }
        }

        fn config_requests(&self) -> Vec<(String, Option<String>)> {
            self.config_requests
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
        }

        fn send_requests(&self) -> Vec<SendTypingReq> {
            self.send_requests
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
        }

        fn fail_next_send(&self) {
            self.send_results
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push_back(Err("send failed".to_owned()));
        }

        fn defer_first_send(&self) -> (oneshot::Receiver<()>, oneshot::Sender<Result<(), String>>) {
            let (started_tx, started_rx) = oneshot::channel();
            let (finish_tx, finish_rx) = oneshot::channel();
            self.first_send_deferred.store(true, Ordering::SeqCst);
            *self
                .first_send_gate
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = Some((started_tx, finish_rx));
            (started_rx, finish_tx)
        }
    }

    impl WeixinTypingApi for FakeApi {
        fn get_config<'a>(
            &'a self,
            user_id: &'a str,
            context_token: Option<&'a str>,
        ) -> TypingFuture<'a, Result<GetConfigResp, String>> {
            self.config_requests
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push((user_id.to_owned(), context_token.map(str::to_owned)));
            if self.config_error.load(Ordering::SeqCst) {
                return Box::pin(async { Err("config failed".to_owned()) });
            }
            let ticket = self
                .ticket
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone();
            Box::pin(async move {
                Ok(GetConfigResp {
                    typing_ticket: ticket,
                    ..GetConfigResp::default()
                })
            })
        }

        fn send_typing<'a>(
            &'a self,
            request: SendTypingReq,
        ) -> TypingFuture<'a, Result<(), String>> {
            self.send_requests
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(request);
            if self.first_send_deferred.swap(false, Ordering::SeqCst) {
                let gate = self
                    .first_send_gate
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .take();
                if let Some((started, finish)) = gate {
                    return Box::pin(async move {
                        let _ = started.send(());
                        finish
                            .await
                            .unwrap_or_else(|_| Err("deferred send dropped".to_owned()))
                    });
                }
            }
            let result = self
                .send_results
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .pop_front()
                .unwrap_or(Ok(()));
            Box::pin(async move { result })
        }
    }

    fn context_lookup(user_id: &str) -> Option<String> {
        Some(format!("context-{user_id}"))
    }

    fn lifecycle(api: Arc<FakeApi>, tickets: TypingTicketCache) -> Arc<WeixinTypingLifecycle> {
        Arc::new(WeixinTypingLifecycle::with_ticket_cache(
            api,
            Arc::new(context_lookup),
            tickets,
        ))
    }

    async fn wait_for_sends(api: &FakeApi, count: usize) {
        for _ in 0..200 {
            if api.send_requests().len() >= count {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!("timed out waiting for {count} typing requests");
    }

    #[tokio::test]
    async fn set_typing_fetches_ticket_with_user_context_then_reuses_it() {
        let api = Arc::new(FakeApi::with_ticket("ticket-1"));
        let cache = TypingTicketCache::new();
        let typing = lifecycle(Arc::clone(&api), cache.clone());

        assert!(typing.set_typing("user-1", true).await);
        assert!(typing.set_typing("user-1", false).await);
        assert_eq!(
            api.config_requests(),
            [("user-1".to_owned(), Some("context-user-1".to_owned()))]
        );
        assert_eq!(cache.get("user-1").as_deref(), Some("ticket-1"));
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
        let requests = api.send_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].ilink_user_id.as_deref(), Some("user-1"));
        assert_eq!(requests[0].typing_ticket.as_deref(), Some("ticket-1"));
        assert_eq!(requests[0].status, Some(TypingStatus::TYPING));
        assert_eq!(requests[1].status, Some(TypingStatus::CANCEL));
    }

    #[tokio::test]
    async fn ticket_cache_is_shared_between_lifecycle_instances_by_user() {
        let cache = TypingTicketCache::new();
        let first_api = Arc::new(FakeApi::with_ticket("global-ticket"));
        let second_api = Arc::new(FakeApi::with_ticket("ignored-ticket"));
        let first = lifecycle(Arc::clone(&first_api), cache.clone());
        let second = lifecycle(Arc::clone(&second_api), cache);

        assert!(first.set_typing("same-user", true).await);
        assert!(second.set_typing("same-user", true).await);
        assert_eq!(first_api.config_requests().len(), 1);
        assert!(second_api.config_requests().is_empty());
        assert_eq!(
            second_api.send_requests()[0].typing_ticket.as_deref(),
            Some("global-ticket")
        );
    }

    #[tokio::test]
    async fn default_lifecycles_use_the_process_wide_ticket_cache() {
        static NEXT_TEST_USER: AtomicU64 = AtomicU64::new(1);
        let user_id = format!(
            "global-cache-test-{}",
            NEXT_TEST_USER.fetch_add(1, Ordering::Relaxed)
        );
        let first_api = Arc::new(FakeApi::with_ticket("process-ticket"));
        let second_api = Arc::new(FakeApi::with_ticket("unused-ticket"));
        let first = Arc::new(WeixinTypingLifecycle::new(
            first_api.clone(),
            Arc::new(context_lookup),
        ));
        let second = Arc::new(WeixinTypingLifecycle::new(
            second_api.clone(),
            Arc::new(context_lookup),
        ));

        assert!(first.set_typing(&user_id, true).await);
        assert!(second.set_typing(&user_id, false).await);
        assert_eq!(first_api.config_requests().len(), 1);
        assert!(second_api.config_requests().is_empty());
        assert_eq!(
            global_typing_ticket_cache().get(&user_id).as_deref(),
            Some("process-ticket")
        );
    }

    #[tokio::test]
    async fn missing_ticket_and_api_failures_are_best_effort() {
        let api = Arc::new(FakeApi::default());
        let typing = lifecycle(Arc::clone(&api), TypingTicketCache::new());
        assert!(!typing.set_typing("no-ticket", true).await);
        assert!(api.send_requests().is_empty());

        *api.ticket.lock().unwrap() = Some("ticket".to_owned());
        api.config_error.store(true, Ordering::SeqCst);
        assert!(!typing.set_typing("config-error", true).await);
        api.config_error.store(false, Ordering::SeqCst);
        api.fail_next_send();
        assert!(!typing.set_typing("send-error", true).await);
        assert_eq!(api.send_requests().len(), 1);
    }

    #[tokio::test]
    async fn lifecycle_deduplicates_starts_stops_and_retries_failed_start() {
        let api = Arc::new(FakeApi::with_ticket("ticket"));
        api.fail_next_send();
        let typing = lifecycle(Arc::clone(&api), TypingTicketCache::new());
        typing.connect();
        assert!(typing.start_typing("user-retry").unwrap());
        assert!(!typing.start_typing("user-retry").unwrap());
        wait_for_sends(&api, 1).await;
        for _ in 0..200 {
            if !typing.active_typing_chats().contains("user-retry") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(!typing.active_typing_chats().contains("user-retry"));

        assert!(typing.start_typing("user-retry").unwrap());
        wait_for_sends(&api, 2).await;
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        assert!(typing.stop_typing("user-retry").unwrap());
        assert!(!typing.stop_typing("user-retry").unwrap());
        wait_for_sends(&api, 3).await;
        assert_eq!(
            api.send_requests()
                .iter()
                .map(|request| request.status.unwrap())
                .collect::<Vec<_>>(),
            [
                TypingStatus::TYPING,
                TypingStatus::TYPING,
                TypingStatus::CANCEL
            ]
        );
    }

    #[tokio::test]
    async fn late_start_after_terminal_repeats_cancel() {
        let api = Arc::new(FakeApi::with_ticket("ticket"));
        let (started, finish) = api.defer_first_send();
        let typing = lifecycle(Arc::clone(&api), TypingTicketCache::new());
        typing.connect();
        typing.on_task_lifecycle("late-user", TypingLifecycleEvent::Started);
        started.await.unwrap();
        typing.on_task_lifecycle("late-user", TypingLifecycleEvent::Terminal);
        wait_for_sends(&api, 2).await;
        finish.send(Ok(())).unwrap();
        wait_for_sends(&api, 3).await;
        assert_eq!(
            api.send_requests()
                .iter()
                .map(|request| request.status.unwrap())
                .collect::<Vec<_>>(),
            [
                TypingStatus::TYPING,
                TypingStatus::CANCEL,
                TypingStatus::CANCEL
            ]
        );
    }

    #[tokio::test]
    async fn disconnect_reconnect_invalidates_late_start_and_clears_active_set() {
        let api = Arc::new(FakeApi::with_ticket("ticket"));
        let (started, finish) = api.defer_first_send();
        let typing = lifecycle(Arc::clone(&api), TypingTicketCache::new());
        typing.connect();
        typing.start_typing("reconnect-user").unwrap();
        started.await.unwrap();
        typing.disconnect();
        assert!(typing.active_typing_chats().is_empty());
        typing.connect();
        typing.start_typing("reconnect-user").unwrap();
        wait_for_sends(&api, 2).await;
        finish.send(Ok(())).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(api.send_requests().len(), 2);
        assert!(typing.active_typing_chats().contains("reconnect-user"));
    }

    #[tokio::test]
    async fn disconnect_clears_active_chats_without_sending_cancel() {
        let api = Arc::new(FakeApi::with_ticket("ticket"));
        let typing = lifecycle(Arc::clone(&api), TypingTicketCache::new());
        typing.connect();
        typing.start_typing("disconnect-user").unwrap();
        wait_for_sends(&api, 1).await;
        typing.disconnect();
        assert!(typing.active_typing_chats().is_empty());
        assert_eq!(api.send_requests().len(), 1);
    }

    #[tokio::test]
    async fn monitor_style_lifecycle_events_ignore_other_events() {
        let api = Arc::new(FakeApi::with_ticket("ticket"));
        let typing = lifecycle(Arc::clone(&api), TypingTicketCache::new());
        typing.connect();
        typing.on_task_lifecycle("event-user", TypingLifecycleEvent::Other);
        assert!(api.send_requests().is_empty());
        typing.on_prompt_start("event-user");
        wait_for_sends(&api, 1).await;
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        typing.on_prompt_end("event-user");
        wait_for_sends(&api, 2).await;
        assert_eq!(
            api.send_requests()
                .iter()
                .map(|request| request.status.unwrap())
                .collect::<Vec<_>>(),
            [TypingStatus::TYPING, TypingStatus::CANCEL]
        );
    }
}
