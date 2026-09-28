//! Feishu user-question presentation and callback lifecycle controller.
//!
//! The channel-base request context is not yet available as a Rust type, so this
//! module defines a narrow local context and callback seam for adapter wiring.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::sync::oneshot;
use tokio::time::sleep;

use super::feishu_question_card::{
    FeishuQuestion, FeishuQuestionAction, FeishuQuestionTerminalState, build_question_card,
    build_question_terminal_card, parse_question_action, parse_question_answers,
};

pub type FeishuQuestionFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
pub type FeishuQuestionSettlementListener =
    Arc<dyn Fn(FeishuUserInputSettlementReason) + Send + Sync + 'static>;
pub type FeishuQuestionUnsubscribe = Box<dyn FnOnce() + Send + 'static>;
pub type FeishuQuestionExecute = Box<dyn FnOnce() -> FeishuQuestionFuture<()> + Send + 'static>;

pub type FeishuQuestionOnSettled = Arc<
    dyn Fn(FeishuQuestionSettlementListener) -> FeishuQuestionUnsubscribe + Send + Sync + 'static,
>;
pub type FeishuQuestionRespond = Arc<
    dyn Fn(FeishuUserInputResponse) -> FeishuQuestionFuture<Result<bool, String>>
        + Send
        + Sync
        + 'static,
>;

#[derive(Clone)]
pub struct FeishuQuestionContext {
    pub request_id: String,
    pub session_id: String,
    pub run_id: String,
    pub owner_id: String,
    pub target_chat_id: String,
    pub submit_option_id: String,
    pub questions: Vec<FeishuQuestion>,
    pub on_settled: FeishuQuestionOnSettled,
    pub respond: FeishuQuestionRespond,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeishuUserInputSettlementReason {
    ResolvedOutsidePresenter,
    Cancelled,
    RunCancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FeishuUserInputOutcome {
    Selected { option_id: String },
    Cancelled,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FeishuUserInputResponse {
    pub outcome: FeishuUserInputOutcome,
    pub answers: Option<Map<String, Value>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeishuQuestionPresentationResult {
    Presented,
    Handled,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeishuQuestionCancelRunState {
    Cancelled,
    Expired,
}

pub type FeishuQuestionSendCard = Arc<
    dyn Fn(String, Value) -> FeishuQuestionFuture<Result<String, String>> + Send + Sync + 'static,
>;
pub type FeishuQuestionPatchCard = Arc<
    dyn Fn(String, Value) -> FeishuQuestionFuture<Result<bool, String>> + Send + Sync + 'static,
>;
pub type FeishuQuestionSendFallback =
    Arc<dyn Fn(String, String) -> FeishuQuestionFuture<Result<(), String>> + Send + Sync + 'static>;
pub type FeishuQuestionOnError = Arc<dyn Fn(&'static str, String) + Send + Sync + 'static>;

#[derive(Clone)]
pub struct FeishuQuestionCardControllerOptions {
    pub timeout: Duration,
    pub send_card: FeishuQuestionSendCard,
    pub patch_card: FeishuQuestionPatchCard,
    pub send_fallback: FeishuQuestionSendFallback,
    pub on_error: Option<FeishuQuestionOnError>,
}

pub enum FeishuQuestionCallbackResult {
    Unhandled,
    Handled {
        response: Value,
        execute: Option<FeishuQuestionExecute>,
    },
}

#[derive(Clone)]
pub struct FeishuQuestionCardController {
    core: Arc<ControllerCore>,
}

struct ControllerCore {
    options: FeishuQuestionCardControllerOptions,
    registry: Mutex<Registry>,
}

#[derive(Default)]
struct Registry {
    by_request: HashMap<String, Arc<QuestionRecord>>,
    active_by_scope: HashMap<String, Arc<QuestionRecord>>,
    disposed: bool,
}

struct QuestionRecord {
    request_id: String,
    run_id: String,
    owner_id: String,
    chat_id: String,
    scope_key: String,
    context: FeishuQuestionContext,
    data: Mutex<RecordData>,
    projections: Mutex<ProjectionQueue>,
}

struct RecordData {
    state: QuestionState,
    message_id: Option<String>,
    timer_cancel: Option<oneshot::Sender<()>>,
    unsubscribe: Option<FeishuQuestionUnsubscribe>,
    responding: bool,
    ignore_response_projection: bool,
    terminal_delivered_by_claim: bool,
    terminal_state: Option<FeishuQuestionTerminalState>,
    terminal_answers: Option<Map<String, Value>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QuestionState {
    Reserved,
    Pending,
    Claimed,
    Terminal,
}

#[derive(Default)]
struct ProjectionQueue {
    requests: std::collections::VecDeque<ProjectionRequest>,
    running: bool,
}

struct ProjectionRequest {
    message_id: String,
    chat_id: String,
    card: Value,
    fallback_text: String,
    completed: oneshot::Sender<()>,
}

struct ProjectionTicket {
    receiver: oneshot::Receiver<()>,
    start_worker: bool,
}

struct TerminalEffects {
    timer_cancel: Option<oneshot::Sender<()>>,
    unsubscribe: Option<FeishuQuestionUnsubscribe>,
    projection: Option<ProjectionTicket>,
}

impl FeishuQuestionCardController {
    pub fn new(options: FeishuQuestionCardControllerOptions) -> Self {
        Self {
            core: Arc::new(ControllerCore {
                options,
                registry: Mutex::new(Registry::default()),
            }),
        }
    }

    pub async fn present(
        &self,
        context: FeishuQuestionContext,
    ) -> FeishuQuestionPresentationResult {
        let scope_key = format!("{}\0{}", context.session_id, context.owner_id);
        let record = Arc::new(QuestionRecord {
            request_id: context.request_id.clone(),
            run_id: context.run_id.clone(),
            owner_id: context.owner_id.clone(),
            chat_id: context.target_chat_id.clone(),
            scope_key: scope_key.clone(),
            context: context.clone(),
            data: Mutex::new(RecordData {
                state: QuestionState::Reserved,
                message_id: None,
                timer_cancel: None,
                unsubscribe: None,
                responding: false,
                ignore_response_projection: false,
                terminal_delivered_by_claim: false,
                terminal_state: None,
                terminal_answers: None,
            }),
            projections: Mutex::new(ProjectionQueue::default()),
        });

        {
            let mut registry = lock(&self.core.registry);
            if registry.disposed {
                return FeishuQuestionPresentationResult::Unsupported;
            }
            if registry
                .active_by_scope
                .get(&scope_key)
                .is_some_and(|active| lock(&active.data).state != QuestionState::Terminal)
            {
                return FeishuQuestionPresentationResult::Unsupported;
            }
            registry
                .by_request
                .insert(context.request_id.clone(), record.clone());
            registry.active_by_scope.insert(scope_key, record.clone());
        }

        let weak_core = Arc::downgrade(&self.core);
        let weak_record = Arc::downgrade(&record);
        let listener: FeishuQuestionSettlementListener = Arc::new(move |reason| {
            let (Some(core), Some(record)) = (weak_core.upgrade(), weak_record.upgrade()) else {
                return;
            };
            core.handle_settlement(record, reason);
        });
        let unsubscribe = (context.on_settled)(listener);

        let settled_during_registration = {
            let mut data = lock(&record.data);
            data.unsubscribe = Some(unsubscribe);
            if data.state == QuestionState::Terminal {
                data.unsubscribe.take()
            } else {
                None
            }
        };
        if let Some(unsubscribe) = settled_during_registration {
            unsubscribe();
            return FeishuQuestionPresentationResult::Presented;
        }

        let send_result = (self.core.options.send_card)(
            record.chat_id.clone(),
            build_question_card(&record.request_id, &record.context.questions),
        )
        .await
        .and_then(|message_id| {
            if message_id.is_empty() {
                Err("Feishu card delivery returned no message id".to_owned())
            } else {
                Ok(message_id)
            }
        });

        let message_id = match send_result {
            Ok(message_id) => message_id,
            Err(error) => {
                if lock(&record.data).state == QuestionState::Terminal {
                    return FeishuQuestionPresentationResult::Presented;
                }
                self.core.report_error("question card delivery", error);
                if !self
                    .core
                    .finalize_and_wait(&record, FeishuQuestionTerminalState::Cancelled)
                    .await
                {
                    return FeishuQuestionPresentationResult::Presented;
                }
                if let Err(error) = (self.core.options.send_fallback)(
                    record.chat_id.clone(),
                    fallback_text(&record.context),
                )
                .await
                {
                    self.core.report_error("question fallback delivery", error);
                }
                if let Err(error) = (record.context.respond)(cancelled_response()).await {
                    self.core
                        .report_error("question delivery cancellation", error);
                }
                return FeishuQuestionPresentationResult::Handled;
            }
        };

        let mut timer_to_start = None;
        let projection = {
            let registry = lock(&self.core.registry);
            let mut data = lock(&record.data);
            let still_current = registry
                .by_request
                .get(&record.request_id)
                .is_some_and(|current| Arc::ptr_eq(current, &record))
                && registry
                    .active_by_scope
                    .get(&record.scope_key)
                    .is_some_and(|active| Arc::ptr_eq(active, &record));

            if data.state == QuestionState::Reserved && still_current {
                data.message_id = Some(message_id);
                data.state = QuestionState::Pending;
                let (cancel_tx, cancel_rx) = oneshot::channel();
                data.timer_cancel = Some(cancel_tx);
                timer_to_start = Some(cancel_rx);
                None
            } else {
                enqueue_projection_locked(&record, &data, Some(message_id))
            }
        };
        if let Some(cancel_rx) = timer_to_start {
            self.core.start_timeout(record.clone(), cancel_rx);
        }
        if let Some(ticket) = projection {
            self.core
                .start_projection_if_needed(&record, ticket.start_worker);
            let _ = ticket.receiver.await;
        }
        FeishuQuestionPresentationResult::Presented
    }

    pub fn claim(&self, data: &Value) -> FeishuQuestionCallbackResult {
        let action = parse_question_action(data);
        let request_id = match &action {
            FeishuQuestionAction::Unhandled => return FeishuQuestionCallbackResult::Unhandled,
            FeishuQuestionAction::Submit { request_id, .. }
            | FeishuQuestionAction::Cancel { request_id, .. } => request_id,
        };
        let registry = lock(&self.core.registry);
        if registry.disposed {
            return expired_response();
        }
        let record = registry.by_request.get(request_id).cloned();
        let Some(record) = record else {
            return expired_response();
        };

        let (operator_id, chat_id, message_id, form_value, is_submit) = match action {
            FeishuQuestionAction::Submit {
                operator_id,
                chat_id,
                message_id,
                form_value,
                ..
            } => (operator_id, chat_id, message_id, form_value, true),
            FeishuQuestionAction::Cancel {
                operator_id,
                chat_id,
                message_id,
                ..
            } => (operator_id, chat_id, message_id, None, false),
            FeishuQuestionAction::Unhandled => unreachable!(),
        };

        let mut data = lock(&record.data);
        if data.state != QuestionState::Pending
            || operator_id.as_deref() != Some(record.owner_id.as_str())
            || chat_id.as_deref() != Some(record.chat_id.as_str())
            || message_id.as_deref() != data.message_id.as_deref()
            || operator_id.is_none()
            || chat_id.is_none()
            || message_id.is_none()
        {
            return expired_response();
        }

        if is_submit {
            let Some(answers) =
                parse_question_answers(&record.context.questions, form_value.as_ref())
            else {
                return FeishuQuestionCallbackResult::Handled {
                    response: json!({
                        "toast": { "type": "warning", "content": "请完整选择有效答案。" }
                    }),
                    execute: None,
                };
            };
            clear_timer(&mut data);
            data.state = QuestionState::Claimed;
            let response = json!({
                "toast": { "type": "success", "content": "答案已提交，正在处理。" },
                "card": {
                    "type": "raw",
                    "data": build_question_terminal_card(
                        &record.context.questions,
                        FeishuQuestionTerminalState::Processing,
                        Some(&answers),
                    ),
                },
            });
            drop(data);
            drop(registry);
            let core = self.core.clone();
            let execute_record = record.clone();
            return FeishuQuestionCallbackResult::Handled {
                response,
                execute: Some(Box::new(move || {
                    Box::pin(async move {
                        core.respond_to_claim(
                            execute_record,
                            FeishuQuestionTerminalState::Submitted,
                            Some(answers),
                        )
                        .await;
                    })
                })),
            };
        }

        clear_timer(&mut data);
        data.state = QuestionState::Claimed;
        data.terminal_delivered_by_claim = true;
        let response = json!({
            "toast": { "type": "info", "content": "已取消。" },
            "card": {
                "type": "raw",
                "data": build_question_terminal_card(
                    &record.context.questions,
                    FeishuQuestionTerminalState::Cancelled,
                    None,
                ),
            },
        });
        drop(data);
        drop(registry);
        let core = self.core.clone();
        FeishuQuestionCallbackResult::Handled {
            response,
            execute: Some(Box::new(move || {
                Box::pin(async move {
                    core.respond_to_claim(record, FeishuQuestionTerminalState::Cancelled, None)
                        .await;
                })
            })),
        }
    }

    /// Return contexts for questions whose cards are currently actionable.
    pub fn pending_contexts(&self) -> Vec<FeishuQuestionContext> {
        let registry = lock(&self.core.registry);
        registry
            .by_request
            .values()
            .filter(|record| lock(&record.data).state == QuestionState::Pending)
            .map(|record| record.context.clone())
            .collect()
    }

    /// Cancel one exact pending question and relay its cancelled response.
    pub async fn cancel_request(
        &self,
        request_id: &str,
        session_id: &str,
        run_id: &str,
        owner_id: &str,
        target_chat_id: &str,
    ) -> bool {
        let record = {
            let registry = lock(&self.core.registry);
            let Some(record) = registry.by_request.get(request_id).cloned() else {
                return false;
            };
            if record.context.session_id != session_id
                || record.context.run_id != run_id
                || record.context.owner_id != owner_id
                || record.context.target_chat_id != target_chat_id
            {
                return false;
            }
            let mut data = lock(&record.data);
            if data.state != QuestionState::Pending {
                return false;
            }
            clear_timer(&mut data);
            data.state = QuestionState::Claimed;
            drop(data);
            record
        };
        self.core
            .clone()
            .respond_to_claim(record, FeishuQuestionTerminalState::Cancelled, None)
            .await
    }

    pub fn cancel_run(&self, run_id: &str) {
        self.cancel_run_with_state(run_id, FeishuQuestionCancelRunState::Cancelled);
    }

    pub fn cancel_run_with_state(
        &self,
        run_id: &str,
        terminal_state: FeishuQuestionCancelRunState,
    ) {
        let terminal_state = match terminal_state {
            FeishuQuestionCancelRunState::Cancelled => FeishuQuestionTerminalState::Cancelled,
            FeishuQuestionCancelRunState::Expired => FeishuQuestionTerminalState::Expired,
        };
        let effects = {
            let mut registry = lock(&self.core.registry);
            let records = registry
                .by_request
                .values()
                .filter(|record| record.run_id == run_id)
                .cloned()
                .collect::<Vec<_>>();
            records
                .into_iter()
                .filter_map(|record| {
                    transition_record_locked(&mut registry, &record, terminal_state, None, false)
                        .map(|effects| (record, effects))
                })
                .collect::<Vec<_>>()
        };
        for (record, effects) in effects {
            self.core.finish_terminal_effects(&record, effects);
        }
    }

    pub fn dispose(&self) {
        let effects = {
            let mut registry = lock(&self.core.registry);
            registry.disposed = true;
            let records = registry.by_request.values().cloned().collect::<Vec<_>>();
            records
                .into_iter()
                .filter_map(|record| {
                    lock(&record.data).ignore_response_projection = true;
                    transition_record_locked(
                        &mut registry,
                        &record,
                        FeishuQuestionTerminalState::Expired,
                        None,
                        false,
                    )
                    .map(|effects| (record, effects))
                })
                .collect::<Vec<_>>()
        };
        for (record, effects) in effects {
            self.core.finish_terminal_effects(&record, effects);
        }
    }
}

impl ControllerCore {
    fn report_error(&self, operation: &'static str, error: impl Into<String>) {
        if let Some(on_error) = &self.options.on_error {
            on_error(operation, error.into());
        }
    }

    fn handle_settlement(
        self: &Arc<Self>,
        record: Arc<QuestionRecord>,
        reason: FeishuUserInputSettlementReason,
    ) {
        let terminal_state = match reason {
            FeishuUserInputSettlementReason::ResolvedOutsidePresenter => {
                FeishuQuestionTerminalState::Expired
            }
            FeishuUserInputSettlementReason::Cancelled
            | FeishuUserInputSettlementReason::RunCancelled => {
                FeishuQuestionTerminalState::Cancelled
            }
        };
        if let Some(effects) = self.transition_to_terminal(&record, terminal_state, None, true) {
            self.finish_terminal_effects(&record, effects);
        }
    }

    fn transition_to_terminal(
        self: &Arc<Self>,
        record: &Arc<QuestionRecord>,
        terminal_state: FeishuQuestionTerminalState,
        expected_state: Option<QuestionState>,
        skip_responding_claim: bool,
    ) -> Option<TerminalEffects> {
        let mut registry = lock(&self.registry);
        transition_record_locked(
            &mut registry,
            record,
            terminal_state,
            expected_state,
            skip_responding_claim,
        )
    }

    fn finish_terminal_effects(
        self: &Arc<Self>,
        record: &Arc<QuestionRecord>,
        effects: TerminalEffects,
    ) -> Option<oneshot::Receiver<()>> {
        if let Some(timer_cancel) = effects.timer_cancel {
            let _ = timer_cancel.send(());
        }
        if let Some(unsubscribe) = effects.unsubscribe {
            unsubscribe();
        }
        if let Some(ticket) = effects.projection {
            self.start_projection_if_needed(record, ticket.start_worker);
            return Some(ticket.receiver);
        }
        None
    }

    async fn finalize_and_wait(
        self: &Arc<Self>,
        record: &Arc<QuestionRecord>,
        terminal_state: FeishuQuestionTerminalState,
    ) -> bool {
        if let Some(effects) = self.transition_to_terminal(record, terminal_state, None, false) {
            let receiver = self.finish_terminal_effects(record, effects);
            if let Some(receiver) = receiver {
                let _ = receiver.await;
            }
            true
        } else {
            false
        }
    }

    fn start_timeout(
        self: &Arc<Self>,
        record: Arc<QuestionRecord>,
        mut cancel: oneshot::Receiver<()>,
    ) {
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                self.report_error(
                    "question timeout",
                    "Question presentation requires an active Tokio runtime",
                );
                return;
            }
        };
        let core = self.clone();
        let timeout = self.options.timeout;
        runtime.spawn(async move {
            tokio::select! {
                biased;
                _ = &mut cancel => return,
                _ = sleep(timeout) => {}
            }
            core.expire_record(record).await;
        });
    }

    async fn expire_record(self: &Arc<Self>, record: Arc<QuestionRecord>) {
        let Some(effects) = self.transition_to_terminal(
            &record,
            FeishuQuestionTerminalState::Expired,
            Some(QuestionState::Pending),
            false,
        ) else {
            return;
        };
        let receiver = self.finish_terminal_effects(&record, effects);
        if let Some(receiver) = receiver {
            let _ = receiver.await;
        }
        if let Err(error) = (record.context.respond)(cancelled_response()).await {
            self.report_error("expired question cancellation", error);
        }
    }

    async fn respond_to_claim(
        self: Arc<Self>,
        record: Arc<QuestionRecord>,
        terminal_state: FeishuQuestionTerminalState,
        answers: Option<Map<String, Value>>,
    ) -> bool {
        {
            let mut data = lock(&record.data);
            if data.state != QuestionState::Claimed {
                return false;
            }
            data.responding = true;
        }

        let response = match terminal_state {
            FeishuQuestionTerminalState::Submitted => FeishuUserInputResponse {
                outcome: FeishuUserInputOutcome::Selected {
                    option_id: record.context.submit_option_id.clone(),
                },
                answers: answers.clone(),
            },
            _ => cancelled_response(),
        };

        let accepted = match (record.context.respond)(response).await {
            Ok(accepted) => {
                if !accepted {
                    self.report_error(
                        "question response",
                        "Question response was no longer accepted",
                    );
                }
                accepted
            }
            Err(error) => {
                self.report_error("question response", error);
                false
            }
        };
        let final_state = if accepted {
            terminal_state
        } else {
            FeishuQuestionTerminalState::Expired
        };
        if let Some(ticket) =
            self.complete_response(&record, final_state, accepted.then_some(answers).flatten())
        {
            self.start_projection_if_needed(&record, ticket.start_worker);
            let _ = ticket.receiver.await;
        }
        accepted
    }

    fn complete_response(
        self: &Arc<Self>,
        record: &Arc<QuestionRecord>,
        terminal_state: FeishuQuestionTerminalState,
        answers: Option<Map<String, Value>>,
    ) -> Option<ProjectionTicket> {
        let mut registry = lock(&self.registry);
        let mut data = lock(&record.data);
        data.responding = false;
        if terminal_state == FeishuQuestionTerminalState::Submitted {
            data.terminal_answers = answers;
        }

        if data.state != QuestionState::Terminal {
            data.state = QuestionState::Terminal;
            data.terminal_state = Some(terminal_state);
            let timer_cancel = data.timer_cancel.take();
            let unsubscribe = data.unsubscribe.take();
            registry.by_request.remove(&record.request_id);
            if registry
                .active_by_scope
                .get(&record.scope_key)
                .is_some_and(|active| Arc::ptr_eq(active, record))
            {
                registry.active_by_scope.remove(&record.scope_key);
            }
            let ticket = enqueue_projection_locked(record, &data, None);
            drop(data);
            drop(registry);
            if let Some(timer_cancel) = timer_cancel {
                let _ = timer_cancel.send(());
            }
            if let Some(unsubscribe) = unsubscribe {
                unsubscribe();
            }
            return ticket;
        }

        if data.ignore_response_projection {
            return None;
        }
        if terminal_state == FeishuQuestionTerminalState::Expired && data.terminal_state.is_some() {
            return None;
        }
        data.terminal_state = Some(terminal_state);
        enqueue_projection_locked(record, &data, None)
    }

    fn start_projection_if_needed(self: &Arc<Self>, record: &Arc<QuestionRecord>, start: bool) {
        if !start {
            return;
        }
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                self.report_error(
                    "question card finalization",
                    "Terminal card projection requires an active Tokio runtime",
                );
                let mut queue = lock(&record.projections);
                queue.running = false;
                queue.requests.clear();
                return;
            }
        };
        let core = self.clone();
        let record = record.clone();
        runtime.spawn(async move { core.run_projection_queue(record).await });
    }

    async fn run_projection_queue(self: Arc<Self>, record: Arc<QuestionRecord>) {
        loop {
            let request = {
                let mut queue = lock(&record.projections);
                match queue.requests.pop_front() {
                    Some(request) => request,
                    None => {
                        queue.running = false;
                        return;
                    }
                }
            };

            let patched = match (self.options.patch_card)(request.message_id, request.card).await {
                Ok(true) => true,
                Ok(false) => {
                    self.report_error(
                        "question card finalization",
                        "Feishu card patch was not accepted",
                    );
                    false
                }
                Err(error) => {
                    self.report_error("question card finalization", error);
                    false
                }
            };
            if !patched
                && let Err(error) =
                    (self.options.send_fallback)(request.chat_id, request.fallback_text).await
            {
                self.report_error("question terminal fallback delivery", error);
            }
            let _ = request.completed.send(());
        }
    }
}

fn transition_record_locked(
    registry: &mut Registry,
    record: &Arc<QuestionRecord>,
    terminal_state: FeishuQuestionTerminalState,
    expected_state: Option<QuestionState>,
    skip_responding_claim: bool,
) -> Option<TerminalEffects> {
    let mut data = lock(&record.data);
    if data.state == QuestionState::Terminal
        || expected_state.is_some_and(|expected| expected != data.state)
        || (skip_responding_claim && data.state == QuestionState::Claimed && data.responding)
    {
        return None;
    }

    data.state = QuestionState::Terminal;
    data.terminal_state = Some(terminal_state);
    let timer_cancel = data.timer_cancel.take();
    let unsubscribe = data.unsubscribe.take();
    registry.by_request.remove(&record.request_id);
    if registry
        .active_by_scope
        .get(&record.scope_key)
        .is_some_and(|active| Arc::ptr_eq(active, record))
    {
        registry.active_by_scope.remove(&record.scope_key);
    }
    let projection = enqueue_projection_locked(record, &data, None);
    Some(TerminalEffects {
        timer_cancel,
        unsubscribe,
        projection,
    })
}

fn enqueue_projection_locked(
    record: &Arc<QuestionRecord>,
    data: &RecordData,
    message_id_override: Option<String>,
) -> Option<ProjectionTicket> {
    let message_id = message_id_override.or_else(|| data.message_id.clone())?;
    let terminal_state = data.terminal_state?;
    if data.terminal_delivered_by_claim {
        return None;
    }
    let card = build_question_terminal_card(
        &record.context.questions,
        terminal_state,
        data.terminal_answers.as_ref(),
    );
    let fallback_text = terminal_fallback_text(
        &record.context.questions,
        terminal_state,
        data.terminal_answers.as_ref(),
    );
    let (completed, receiver) = oneshot::channel();
    let mut queue = lock(&record.projections);
    queue.requests.push_back(ProjectionRequest {
        message_id,
        chat_id: record.chat_id.clone(),
        card,
        fallback_text,
        completed,
    });
    let start_worker = !queue.running;
    if start_worker {
        queue.running = true;
    }
    Some(ProjectionTicket {
        receiver,
        start_worker,
    })
}

fn terminal_fallback_text(
    questions: &[FeishuQuestion],
    terminal_state: FeishuQuestionTerminalState,
    answers: Option<&Map<String, Value>>,
) -> String {
    let details = questions
        .iter()
        .map(|question| {
            let answer = answers
                .and_then(|answers| answers.get(&question.answer_key))
                .and_then(Value::as_str)
                .unwrap_or(&question.question);
            format!("{}: {}", question.header, answer)
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("{}\n{}", terminal_label(terminal_state), details)
}

fn terminal_label(terminal_state: FeishuQuestionTerminalState) -> &'static str {
    match terminal_state {
        FeishuQuestionTerminalState::Processing => "正在处理...",
        FeishuQuestionTerminalState::Submitted => "已提交",
        FeishuQuestionTerminalState::Cancelled => "已取消",
        FeishuQuestionTerminalState::Expired => "已过期",
    }
}

fn fallback_text(context: &FeishuQuestionContext) -> String {
    let questions = context
        .questions
        .iter()
        .map(|question| {
            let options = question
                .options
                .iter()
                .map(|option| option.label.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("- {}: {}", question.question, options)
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("互动问题卡片投递失败，该请求已取消，请重试。\n{questions}")
}

fn expired_response() -> FeishuQuestionCallbackResult {
    FeishuQuestionCallbackResult::Handled {
        response: json!({
            "toast": { "type": "warning", "content": "该问题已过期或已处理。" }
        }),
        execute: None,
    }
}

fn cancelled_response() -> FeishuUserInputResponse {
    FeishuUserInputResponse {
        outcome: FeishuUserInputOutcome::Cancelled,
        answers: None,
    }
}

fn clear_timer(data: &mut RecordData) {
    if let Some(timer_cancel) = data.timer_cancel.take() {
        let _ = timer_cancel.send(());
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::time::{sleep, timeout};

    struct Calls {
        sent: Mutex<Vec<(String, Value)>>,
        patched: Mutex<Vec<(String, Value)>>,
        fallback: Mutex<Vec<(String, String)>>,
        errors: Mutex<Vec<(&'static str, String)>>,
        send_result: Mutex<Result<String, String>>,
        patch_result: Mutex<Result<bool, String>>,
        send_gate: Mutex<Option<Arc<Notify>>>,
        send_started: Mutex<Option<Arc<Notify>>>,
        patch_gate: Mutex<Option<Arc<Notify>>>,
        patch_started: Mutex<Option<Arc<Notify>>>,
        patch_count: AtomicUsize,
    }

    impl Default for Calls {
        fn default() -> Self {
            Self {
                sent: Mutex::new(Vec::new()),
                patched: Mutex::new(Vec::new()),
                fallback: Mutex::new(Vec::new()),
                errors: Mutex::new(Vec::new()),
                send_result: Mutex::new(Ok("om_1".into())),
                patch_result: Mutex::new(Ok(true)),
                send_gate: Mutex::new(None),
                send_started: Mutex::new(None),
                patch_gate: Mutex::new(None),
                patch_started: Mutex::new(None),
                patch_count: AtomicUsize::new(0),
            }
        }
    }

    struct TestContext {
        context: FeishuQuestionContext,
        listeners: Arc<Mutex<Vec<FeishuQuestionSettlementListener>>>,
        respond_count: Arc<AtomicUsize>,
    }

    fn question_context(request_id: &str) -> TestContext {
        let reply: FeishuQuestionRespond = Arc::new(|_| Box::pin(async { Ok(true) }));
        question_context_with_reply(request_id, reply)
    }

    fn question_context_with_reply(request_id: &str, reply: FeishuQuestionRespond) -> TestContext {
        let listeners = Arc::new(Mutex::new(Vec::<FeishuQuestionSettlementListener>::new()));
        let respond_count = Arc::new(AtomicUsize::new(0));
        let respond_count_clone = respond_count.clone();
        let respond: FeishuQuestionRespond = Arc::new(move |response| {
            respond_count_clone.fetch_add(1, Ordering::SeqCst);
            reply(response)
        });
        let on_settled_listeners = listeners.clone();
        let on_settled: FeishuQuestionOnSettled = Arc::new(move |listener| {
            lock(&on_settled_listeners).push(listener.clone());
            let weak = Arc::downgrade(&on_settled_listeners);
            Box::new(move || {
                if let Some(listeners) = weak.upgrade() {
                    lock(&listeners).retain(|existing| !Arc::ptr_eq(existing, &listener));
                }
            })
        });
        TestContext {
            context: FeishuQuestionContext {
                request_id: request_id.to_owned(),
                session_id: "session-1".into(),
                run_id: "run-1".into(),
                owner_id: "owner-1".into(),
                target_chat_id: "oc_1".into(),
                submit_option_id: "allow-once".into(),
                questions: vec![FeishuQuestion {
                    answer_key: "0".into(),
                    header: "Region".into(),
                    question: "Which region?".into(),
                    options: vec![super::super::feishu_question_card::FeishuQuestionOption {
                        label: "Beijing".into(),
                        description: "Use Beijing.".into(),
                    }],
                    multi_select: false,
                }],
                on_settled,
                respond,
            },
            listeners,
            respond_count,
        }
    }

    fn harness(timeout_ms: u64) -> (FeishuQuestionCardController, Arc<Calls>) {
        let calls = Arc::new(Calls::default());
        let sent = calls.clone();
        let send_card: FeishuQuestionSendCard = Arc::new(move |chat_id, card| {
            let sent = sent.clone();
            Box::pin(async move {
                let send_started = lock(&sent.send_started).clone();
                if let Some(started) = send_started {
                    started.notify_one();
                }
                let send_gate = lock(&sent.send_gate).clone();
                if let Some(gate) = send_gate {
                    gate.notified().await;
                }
                let result = lock(&sent.send_result).clone();
                if result.is_ok() {
                    lock(&sent.sent).push((chat_id, card));
                }
                result
            })
        });
        let patched = calls.clone();
        let patch_card: FeishuQuestionPatchCard = Arc::new(move |message_id, card| {
            let patched = patched.clone();
            Box::pin(async move {
                let index = patched.patch_count.fetch_add(1, Ordering::SeqCst);
                lock(&patched.patched).push((message_id, card));
                if index == 0 {
                    let patch_started = lock(&patched.patch_started).clone();
                    if let Some(started) = patch_started {
                        started.notify_one();
                    }
                    let patch_gate = lock(&patched.patch_gate).clone();
                    if let Some(gate) = patch_gate {
                        gate.notified().await;
                    }
                }
                lock(&patched.patch_result).clone()
            })
        });
        let fallbacks = calls.clone();
        let send_fallback: FeishuQuestionSendFallback = Arc::new(move |chat_id, text| {
            let fallbacks = fallbacks.clone();
            Box::pin(async move {
                lock(&fallbacks.fallback).push((chat_id, text));
                Ok(())
            })
        });
        let errors = calls.clone();
        let on_error: FeishuQuestionOnError = Arc::new(move |operation, error| {
            lock(&errors.errors).push((operation, error));
        });
        (
            FeishuQuestionCardController::new(FeishuQuestionCardControllerOptions {
                timeout: Duration::from_millis(timeout_ms),
                send_card,
                patch_card,
                send_fallback,
                on_error: Some(on_error),
            }),
            calls,
        )
    }

    fn submit_action(request_id: &str, answer: &str) -> Value {
        json!({
            "operator": { "open_id": "owner-1" },
            "context": { "open_chat_id": "oc_1", "open_message_id": "om_1" },
            "action": {
                "name": format!("qwen_ask_submit_{request_id}"),
                "value": { "action": "qwen_ask_submit", "operation_id": request_id },
                "form_value": { "0": answer },
            },
        })
    }

    fn cancel_action(request_id: &str) -> Value {
        json!({
            "operator": { "open_id": "owner-1" },
            "context": { "open_chat_id": "oc_1", "open_message_id": "om_1" },
            "action": {
                "name": format!("qwen_ask_cancel_{request_id}"),
                "value": { "action": "qwen_ask_cancel", "operation_id": request_id },
            },
        })
    }

    fn take_execute(result: FeishuQuestionCallbackResult) -> (Value, FeishuQuestionExecute) {
        let FeishuQuestionCallbackResult::Handled {
            response,
            execute: Some(execute),
        } = result
        else {
            panic!("expected handled callback with execute closure");
        };
        (response, execute)
    }

    fn fire_settlement(context: &TestContext, reason: FeishuUserInputSettlementReason) {
        let listeners = lock(&context.listeners).clone();
        for listener in listeners {
            listener(reason);
        }
    }

    #[tokio::test]
    async fn reserves_during_delivery_and_projects_settlement_after_late_send() {
        let (controller, calls) = harness(1_000);
        let gate = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        *lock(&calls.send_gate) = Some(gate.clone());
        *lock(&calls.send_started) = Some(started.clone());
        let context = question_context("late");
        let presenting_controller = controller.clone();
        let context_for_present = context.context.clone();
        let presenting =
            tokio::spawn(async move { presenting_controller.present(context_for_present).await });
        timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        fire_settlement(
            &context,
            FeishuUserInputSettlementReason::ResolvedOutsidePresenter,
        );
        gate.notify_one();
        assert_eq!(
            presenting.await.unwrap(),
            FeishuQuestionPresentationResult::Presented
        );
        assert!(lock(&calls.patched)[0].1.to_string().contains("已过期"));
        assert!(matches!(
            controller.claim(&cancel_action("late")),
            FeishuQuestionCallbackResult::Handled { execute: None, .. }
        ));
    }

    #[tokio::test]
    async fn unsubscribes_and_skips_delivery_when_settled_during_registration() {
        let (controller, calls) = harness(1_000);
        let unsubscriptions = Arc::new(AtomicUsize::new(0));
        let unsubscribe_count = unsubscriptions.clone();
        let mut context = question_context("settled-before-send").context;
        context.on_settled = Arc::new(move |listener| {
            listener(FeishuUserInputSettlementReason::Cancelled);
            let unsubscribe_count = unsubscribe_count.clone();
            Box::new(move || {
                unsubscribe_count.fetch_add(1, Ordering::SeqCst);
            })
        });

        assert_eq!(
            controller.present(context).await,
            FeishuQuestionPresentationResult::Presented
        );
        assert_eq!(unsubscriptions.load(Ordering::SeqCst), 1);
        assert!(lock(&calls.sent).is_empty());
        assert!(lock(&calls.patched).is_empty());
    }

    #[tokio::test]
    async fn failed_or_empty_delivery_cancels_and_releases_scope_for_retry() {
        for result in [Err("delivery failed".to_owned()), Ok(String::new())] {
            let (controller, calls) = harness(1_000);
            *lock(&calls.send_result) = result;
            let failed = question_context("delivery-failed");
            assert_eq!(
                controller.present(failed.context).await,
                FeishuQuestionPresentationResult::Handled
            );
            assert_eq!(failed.respond_count.load(Ordering::SeqCst), 1);
            assert!(lock(&calls.fallback)[0].1.contains("互动问题卡片投递失败"));
            assert!(lock(&calls.fallback)[0].1.contains("Which region?"));

            *lock(&calls.send_result) = Ok("om_retry".into());
            assert_eq!(
                controller
                    .present(question_context("delivery-retry").context)
                    .await,
                FeishuQuestionPresentationResult::Presented
            );
        }
    }

    #[tokio::test]
    async fn claims_only_valid_owner_actions_and_executes_response_later() {
        let (controller, calls) = harness(1_000);
        let context = question_context("submit");
        assert_eq!(
            controller.present(context.context.clone()).await,
            FeishuQuestionPresentationResult::Presented
        );
        let result = controller.claim(&submit_action("submit", "Beijing"));
        let (response, execute) = take_execute(result);
        assert!(response.to_string().contains("正在处理..."));
        assert_eq!(context.respond_count.load(Ordering::SeqCst), 0);
        assert!(matches!(
            controller.claim(&cancel_action("submit")),
            FeishuQuestionCallbackResult::Handled { execute: None, .. }
        ));
        execute().await;
        assert_eq!(context.respond_count.load(Ordering::SeqCst), 1);
        assert_eq!(lock(&calls.patched).len(), 1);
        assert!(lock(&calls.patched)[0].1.to_string().contains("已提交"));
        assert!(lock(&calls.patched)[0].1.to_string().contains("Beijing"));
    }

    #[tokio::test]
    async fn cancellation_delivers_terminal_card_in_callback_without_repatching() {
        let (controller, calls) = harness(1_000);
        let context = question_context("cancel");
        controller.present(context.context.clone()).await;
        let (response, execute) = take_execute(controller.claim(&cancel_action("cancel")));
        assert!(response.to_string().contains("已取消"));
        execute().await;
        assert_eq!(context.respond_count.load(Ordering::SeqCst), 1);
        assert!(lock(&calls.patched).is_empty());
        assert!(lock(&calls.fallback).is_empty());
    }

    #[tokio::test]
    async fn invalid_answer_and_bad_correlation_leave_request_pending() {
        let (controller, calls) = harness(1_000);
        let context = question_context("validate");
        controller.present(context.context.clone()).await;
        let mut wrong_owner = submit_action("validate", "Beijing");
        wrong_owner["operator"]["open_id"] = json!("elsewhere");
        assert!(matches!(
            controller.claim(&wrong_owner),
            FeishuQuestionCallbackResult::Handled { execute: None, .. }
        ));
        let invalid = submit_action("validate", "Unknown");
        let FeishuQuestionCallbackResult::Handled { response, execute } =
            controller.claim(&invalid)
        else {
            panic!()
        };
        assert!(response.to_string().contains("请完整选择有效答案"));
        assert!(execute.is_none());
        let (_, execute) = take_execute(controller.claim(&submit_action("validate", "Beijing")));
        execute().await;
        assert_eq!(lock(&calls.patched).len(), 1);
    }

    #[tokio::test]
    async fn enforces_session_owner_scope_and_uses_delivery_chat_for_fallback() {
        let (controller, calls) = harness(1_000);
        *lock(&calls.patch_result) = Ok(false);
        let mut first = question_context("scope-first");
        assert_eq!(
            controller.present(first.context.clone()).await,
            FeishuQuestionPresentationResult::Presented
        );
        assert_eq!(
            controller
                .present(question_context("scope-blocked").context)
                .await,
            FeishuQuestionPresentationResult::Unsupported
        );

        let mut other_owner = question_context("scope-other-owner").context;
        other_owner.owner_id = "owner-2".into();
        other_owner.target_chat_id = "oc_2".into();
        assert_eq!(
            controller.present(other_owner).await,
            FeishuQuestionPresentationResult::Presented
        );

        first.context.target_chat_id = "oc_changed".into();
        let (_, execute) = take_execute(controller.claim(&submit_action("scope-first", "Beijing")));
        execute().await;
        assert_eq!(lock(&calls.fallback)[0].0, "oc_1");
        assert!(lock(&calls.fallback)[0].1.contains("Region: Beijing"));

        let mut retry = question_context("scope-retry").context;
        retry.target_chat_id = "oc_changed".into();
        assert_eq!(
            controller.present(retry).await,
            FeishuQuestionPresentationResult::Presented
        );
    }

    #[tokio::test]
    async fn timeout_expires_before_cancelling_and_releases_scope() {
        let (controller, calls) = harness(15);
        let context = question_context("timeout");
        controller.present(context.context.clone()).await;
        timeout(Duration::from_secs(1), async {
            loop {
                if context.respond_count.load(Ordering::SeqCst) > 0 {
                    break;
                }
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(context.respond_count.load(Ordering::SeqCst), 1);
        assert!(lock(&calls.patched)[0].1.to_string().contains("已过期"));
        assert_eq!(
            controller
                .present(question_context("replacement").context)
                .await,
            FeishuQuestionPresentationResult::Presented
        );
    }

    #[tokio::test]
    async fn cancel_run_terminalizes_all_matching_records_once() {
        let (controller, calls) = harness(1_000);
        let first = question_context("run-one");
        let mut second = question_context("run-two");
        second.context.session_id = "session-2".into();
        second.context.owner_id = "owner-2".into();
        second.context.target_chat_id = "oc_2".into();
        let mut other_run = question_context("other-run");
        other_run.context.session_id = "session-3".into();
        other_run.context.owner_id = "owner-3".into();
        other_run.context.target_chat_id = "oc_3".into();
        other_run.context.run_id = "run-2".into();
        controller.present(first.context).await;
        controller.present(second.context).await;
        controller.present(other_run.context).await;

        controller.cancel_run("run-1");
        timeout(Duration::from_secs(1), async {
            while lock(&calls.patched).len() != 2 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        controller.cancel_run("run-1");
        sleep(Duration::from_millis(1)).await;

        assert_eq!(lock(&calls.patched).len(), 2);
        assert!(lock(&calls.patched)[0].1.to_string().contains("已取消"));
        assert!(lock(&calls.patched)[1].1.to_string().contains("已取消"));
        let mut other_action = cancel_action("other-run");
        other_action["operator"]["open_id"] = json!("owner-3");
        other_action["context"]["open_chat_id"] = json!("oc_3");
        assert!(matches!(
            controller.claim(&other_action),
            FeishuQuestionCallbackResult::Handled {
                execute: Some(_),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn cancellation_then_accepted_in_flight_submit_projects_in_order() {
        let (controller, calls) = harness(1_000);
        let (response_tx, response_rx) = oneshot::channel::<Result<bool, String>>();
        let response_rx = Arc::new(Mutex::new(Some(response_rx)));
        let reply: FeishuQuestionRespond = Arc::new(move |_| {
            let receiver = lock(&response_rx).take().expect("responder called once");
            Box::pin(async move { receiver.await.unwrap() })
        });
        let context = question_context_with_reply("race", reply);
        controller.present(context.context.clone()).await;
        let (_, execute) = take_execute(controller.claim(&submit_action("race", "Beijing")));
        let executing = tokio::spawn(execute());
        timeout(Duration::from_secs(1), async {
            while context.respond_count.load(Ordering::SeqCst) == 0 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        let patch_gate = Arc::new(Notify::new());
        let patch_started = Arc::new(Notify::new());
        *lock(&calls.patch_gate) = Some(patch_gate.clone());
        *lock(&calls.patch_started) = Some(patch_started.clone());
        controller.cancel_run("run-1");
        timeout(Duration::from_secs(1), patch_started.notified())
            .await
            .unwrap();
        assert_eq!(lock(&calls.patched).len(), 1);
        assert!(lock(&calls.patched)[0].1.to_string().contains("已取消"));

        response_tx.send(Ok(true)).unwrap();
        sleep(Duration::from_millis(1)).await;
        patch_gate.notify_one();
        executing.await.unwrap();

        let patched = lock(&calls.patched);
        assert_eq!(patched.len(), 2);
        assert!(patched[0].1.to_string().contains("已取消"));
        assert!(patched[1].1.to_string().contains("已提交"));
        assert!(patched[1].1.to_string().contains("Beijing"));
    }

    #[tokio::test]
    async fn not_accepted_in_flight_response_keeps_run_cancelled_projection() {
        let (controller, calls) = harness(1_000);
        let (response_tx, response_rx) = oneshot::channel::<Result<bool, String>>();
        let response_rx = Arc::new(Mutex::new(Some(response_rx)));
        let reply: FeishuQuestionRespond = Arc::new(move |_| {
            let receiver = lock(&response_rx).take().expect("responder called once");
            Box::pin(async move { receiver.await.unwrap() })
        });
        let context = question_context_with_reply("race-rejected", reply);
        controller.present(context.context.clone()).await;
        let (_, execute) =
            take_execute(controller.claim(&submit_action("race-rejected", "Beijing")));
        let executing = tokio::spawn(execute());
        timeout(Duration::from_secs(1), async {
            while context.respond_count.load(Ordering::SeqCst) == 0 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        controller.cancel_run("run-1");
        response_tx.send(Ok(false)).unwrap();
        executing.await.unwrap();

        assert_eq!(lock(&calls.patched).len(), 1);
        assert!(lock(&calls.patched)[0].1.to_string().contains("已取消"));
        assert!(
            lock(&calls.errors)
                .iter()
                .any(|(operation, _)| *operation == "question response")
        );
    }

    #[tokio::test]
    async fn settlement_echo_while_responding_waits_for_acceptance() {
        let (controller, calls) = harness(1_000);
        let (response_tx, response_rx) = oneshot::channel::<Result<bool, String>>();
        let response_rx = Arc::new(Mutex::new(Some(response_rx)));
        let reply: FeishuQuestionRespond = Arc::new(move |_| {
            let receiver = lock(&response_rx).take().expect("responder called once");
            Box::pin(async move { receiver.await.unwrap() })
        });
        let context = question_context_with_reply("echo", reply);
        controller.present(context.context.clone()).await;
        let (_, execute) = take_execute(controller.claim(&submit_action("echo", "Beijing")));
        let executing = tokio::spawn(execute());
        timeout(Duration::from_secs(1), async {
            while context.respond_count.load(Ordering::SeqCst) == 0 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        fire_settlement(
            &context,
            FeishuUserInputSettlementReason::ResolvedOutsidePresenter,
        );
        assert!(lock(&calls.patched).is_empty());

        response_tx.send(Ok(true)).unwrap();
        executing.await.unwrap();
        assert_eq!(lock(&calls.patched).len(), 1);
        assert!(lock(&calls.patched)[0].1.to_string().contains("已提交"));
    }

    #[tokio::test]
    async fn settlement_before_claim_execute_prevents_responding() {
        let (controller, calls) = harness(1_000);
        let context = question_context("settled-claim");
        controller.present(context.context.clone()).await;
        let (_, execute) = take_execute(controller.claim(&cancel_action("settled-claim")));
        fire_settlement(&context, FeishuUserInputSettlementReason::RunCancelled);
        execute().await;
        assert_eq!(context.respond_count.load(Ordering::SeqCst), 0);
        assert!(lock(&calls.patched).is_empty());
    }

    #[tokio::test]
    async fn disposal_during_delivery_patches_late_card_as_expired() {
        let (controller, calls) = harness(1_000);
        let gate = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        *lock(&calls.send_gate) = Some(gate.clone());
        *lock(&calls.send_started) = Some(started.clone());
        let context = question_context("dispose-late");
        let presenting_controller = controller.clone();
        let context_for_present = context.context.clone();
        let presenting =
            tokio::spawn(async move { presenting_controller.present(context_for_present).await });
        timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        controller.dispose();
        gate.notify_one();
        assert_eq!(
            presenting.await.unwrap(),
            FeishuQuestionPresentationResult::Presented
        );
        assert!(lock(&calls.patched)[0].1.to_string().contains("已过期"));
        assert_eq!(context.respond_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rejected_submission_expires_and_remains_closed() {
        let (controller, calls) = harness(1_000);
        let reply: FeishuQuestionRespond = Arc::new(|_| Box::pin(async { Ok(false) }));
        let context = question_context_with_reply("rejected", reply);
        controller.present(context.context.clone()).await;
        let (_, execute) = take_execute(controller.claim(&submit_action("rejected", "Beijing")));
        execute().await;
        assert!(lock(&calls.patched)[0].1.to_string().contains("已过期"));
        assert!(
            lock(&calls.errors)
                .iter()
                .any(|(operation, _)| *operation == "question response")
        );
        assert!(matches!(
            controller.claim(&submit_action("rejected", "Beijing")),
            FeishuQuestionCallbackResult::Handled { execute: None, .. }
        ));
    }

    #[tokio::test]
    async fn patch_failure_sends_terminal_text_fallback_with_answer() {
        let (controller, calls) = harness(1_000);
        *lock(&calls.patch_result) = Ok(false);
        let context = question_context("fallback");
        controller.present(context.context.clone()).await;
        let (_, execute) = take_execute(controller.claim(&submit_action("fallback", "Beijing")));
        execute().await;
        assert!(lock(&calls.fallback)[0].1.contains("已提交"));
        assert!(lock(&calls.fallback)[0].1.contains("Region: Beijing"));
    }

    #[tokio::test]
    async fn rejected_terminal_patch_reports_error_and_falls_back() {
        let (controller, calls) = harness(1_000);
        *lock(&calls.patch_result) = Err("patch failed".into());
        let context = question_context("patch-rejected");
        controller.present(context.context.clone()).await;
        controller.cancel_run("run-1");
        timeout(Duration::from_secs(1), async {
            while lock(&calls.fallback).is_empty() {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(lock(&calls.fallback)[0].1.contains("已取消"));
        assert!(
            lock(&calls.errors)
                .iter()
                .any(
                    |(operation, error)| *operation == "question card finalization"
                        && error == "patch failed"
                )
        );
    }

    #[tokio::test]
    async fn disposal_ignores_claimed_execute_and_rejects_new_presentations() {
        let (controller, calls) = harness(1_000);
        let context = question_context("dispose");
        controller.present(context.context.clone()).await;
        let (_, execute) = take_execute(controller.claim(&cancel_action("dispose")));
        controller.dispose();
        execute().await;
        assert_eq!(context.respond_count.load(Ordering::SeqCst), 0);
        assert_eq!(
            controller
                .present(question_context("after-dispose").context)
                .await,
            FeishuQuestionPresentationResult::Unsupported
        );
        assert!(lock(&calls.patched).is_empty());
    }
}
