//! Framework-neutral shared inbound slash commands.
//!
//! This ports the common `/help`, `/new` (plus `/clear` and `/reset`),
//! `/cancel`, `/status`, `/who`, `/approve`, `/approve-always`, and `/deny` handling from
//! `packages/channels/base/src/ChannelBase.ts`.
//! Session mutation, running-turn cancellation, command discovery, `/who`
//! identity/workspace data, pending permission lookup and response, authorization
//! policy, and platform message delivery are supplied by [`InboundCommandHost`].

use super::sanitize::sanitize_quoted_text;
use std::future::Future;
use std::pin::Pin;

pub type InboundCommandFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Routing identity and message context needed by shared command callbacks.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InboundCommandContext {
    pub channel_name: String,
    pub sender_id: String,
    pub chat_id: String,
    pub thread_id: Option<String>,
    pub is_group: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedInboundCommand {
    /// Lowercase command name for local, case-insensitive dispatch.
    pub command: String,
    /// Original command spelling, useful to callers forwarding agent commands.
    pub raw_command: String,
    pub args: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InboundAgentCommand {
    pub name: String,
    pub description: String,
}

/// Values needed to render `/status`. Identity and memory are omitted together
/// when a channel has not opted into the channel boundary prompt.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InboundStatusInfo {
    pub has_session: bool,
    pub access_policy: String,
    pub identity_id: Option<String>,
    pub memory_mode: Option<String>,
}

/// Channel routing scope used to describe who shares the resolved session in
/// `/who`. This mirrors `SessionScope` from the shared TypeScript channel base.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum InboundSessionScope {
    #[default]
    User,
    Thread,
    ChatThread,
    Single,
}

/// Optional channel identity details shown by `/who` when the host enables its
/// channel-boundary prompt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundWhoIdentity {
    pub display_name: String,
    pub memory_namespace: String,
}

/// Values needed to render `/who`. The host resolves `has_session` against the
/// exact sender/chat/thread target without creating a session, and supplies its
/// configured workspace path and routing scope.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InboundWhoInfo {
    pub has_session: bool,
    pub workspace_cwd: String,
    pub session_scope: InboundSessionScope,
    /// Omit both identity and memory lines when the channel boundary prompt is
    /// disabled, matching ChannelBase's opt-in behavior.
    pub identity: Option<InboundWhoIdentity>,
}

/// Permission option kind used by the shared `/approve*` and `/deny` commands.
/// `None` is retained for legacy bridge options identified by their option ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboundPermissionOptionKind {
    AllowOnce,
    AllowAlways,
    RejectOnce,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundPermissionOption {
    pub option_id: String,
    pub kind: Option<InboundPermissionOptionKind>,
}

/// A pending request snapshot needed by shared permission commands. The host
/// resolves `shared_session_target` from its configured session scope, matching
/// ChannelBase's per-target sharing rules.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundPendingPermission {
    pub request_id: String,
    pub target_sender_id: String,
    pub target_chat_id: String,
    pub target_thread_id: Option<String>,
    pub shared_session_target: bool,
    pub user_input_presented: bool,
    pub tool_call_title: Option<String>,
    pub options: Vec<InboundPermissionOption>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InboundPermissionOutcome {
    Selected { option_id: String },
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundPermissionResponse {
    pub outcome: InboundPermissionOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboundCommandResult {
    /// The message was not a parsed local command or names an unsupported command.
    Unhandled,
    Handled,
}

/// Runtime hooks required by the shared command dispatcher.
///
/// `agent_commands` should return the commands for the current session when one
/// exists, otherwise the bridge's global command list, matching ChannelBase's
/// `/help` behavior. `clear_session` owns the full clear lifecycle, including
/// cancellation, persistence removal, and per-session state cleanup. `who_info`
/// resolves the existing sender/chat/thread session without creating one.
pub trait InboundCommandHost: Send + Sync {
    fn is_shared_session(&self, context: &InboundCommandContext) -> bool;
    fn is_authorized_for_shared_session(&self, context: &InboundCommandContext) -> bool;
    fn has_running_request(&self, context: &InboundCommandContext) -> bool;
    fn status_info(&self, context: &InboundCommandContext) -> InboundStatusInfo;
    /// Resolve `/who` data for this sender/chat/thread without creating a
    /// session. Return identity details only when the channel-boundary prompt
    /// is enabled.
    fn who_info(&self, context: &InboundCommandContext) -> InboundWhoInfo;
    fn registered_command_names(&self, context: &InboundCommandContext) -> Vec<String>;
    fn agent_commands(&self, context: &InboundCommandContext) -> Vec<InboundAgentCommand>;

    /// Find a pending permission by explicit request ID, or return the live
    /// requests for this context's exact chat/thread when no ID is supplied.
    /// For an omitted ID, preserve request arrival order. The dispatcher applies
    /// sender and shared-session visibility checks to these candidate records.
    fn pending_permission_requests<'a>(
        &'a self,
        context: InboundCommandContext,
        request_id: Option<String>,
    ) -> InboundCommandFuture<'a, Vec<InboundPendingPermission>>;

    /// Whether a permission response relay is configured for this session.
    fn permission_relay_available(&self, context: &InboundCommandContext) -> bool;

    /// Submit a permission response and retire the local pending entry whether
    /// the bridge accepts, rejects as stale, or errors. `Ok(false)` means stale.
    fn respond_to_permission<'a>(
        &'a self,
        context: InboundCommandContext,
        request_id: String,
        response: InboundPermissionResponse,
    ) -> InboundCommandFuture<'a, bool>;

    fn clear_session<'a>(
        &'a self,
        context: InboundCommandContext,
    ) -> InboundCommandFuture<'a, bool>;

    fn cancel_running_request<'a>(
        &'a self,
        context: InboundCommandContext,
    ) -> InboundCommandFuture<'a, bool>;

    fn send_thread_message<'a>(
        &'a self,
        context: InboundCommandContext,
        text: String,
    ) -> InboundCommandFuture<'a, ()>;

    /// Send to the chat without targeting a thread. Used for questions already
    /// presented through an interactive user-input surface.
    fn send_chat_message<'a>(
        &'a self,
        context: InboundCommandContext,
        text: String,
    ) -> InboundCommandFuture<'a, ()>;
}

/// Parse the leading slash token used by ChannelBase local command dispatch.
///
/// Leading/trailing whitespace is ignored. Command names accept ASCII letters,
/// digits, `_`, `:`, and `-`; Telegram-style `@botname` suffixes are consumed.
/// Command names are lowercased for local dispatch while their original case is
/// retained in `raw_command`.
pub fn parse_inbound_command(text: &str) -> Option<ParsedInboundCommand> {
    let trimmed = text.trim();
    let after_slash = trimmed.strip_prefix('/')?;
    let command_len = after_slash
        .char_indices()
        .take_while(|(_, ch)| is_command_char(*ch))
        .map(|(index, ch)| index + ch.len_utf8())
        .last()?;
    let raw_command = &after_slash[..command_len];
    let mut remainder = &after_slash[command_len..];

    if let Some(after_at) = remainder.strip_prefix('@') {
        let bot_suffix_len = after_at
            .char_indices()
            .take_while(|(_, ch)| !ch.is_whitespace())
            .map(|(index, ch)| index + ch.len_utf8())
            .last();
        if let Some(suffix_len) = bot_suffix_len {
            remainder = &after_at[suffix_len..];
        }
    }

    Some(ParsedInboundCommand {
        command: raw_command.to_ascii_lowercase(),
        raw_command: raw_command.to_owned(),
        args: remainder.trim().to_owned(),
    })
}

fn is_command_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | ':' | '-')
}

/// Dispatch one inbound message to the framework-neutral shared command set.
/// Unknown commands return [`InboundCommandResult::Unhandled`] for the caller
/// to forward to the agent.
pub async fn handle_inbound_command<H: InboundCommandHost + ?Sized>(
    host: &H,
    context: &InboundCommandContext,
    text: &str,
) -> Result<InboundCommandResult, String> {
    let Some(parsed) = parse_inbound_command(text) else {
        return Ok(InboundCommandResult::Unhandled);
    };

    match parsed.command.as_str() {
        "help" => {
            send_help(host, context).await?;
            Ok(InboundCommandResult::Handled)
        }
        "clear" | "reset" | "new" => {
            clear_session(host, context, &parsed.args).await?;
            Ok(InboundCommandResult::Handled)
        }
        "cancel" => {
            cancel_request(host, context).await?;
            Ok(InboundCommandResult::Handled)
        }
        "status" => {
            send_status(host, context).await?;
            Ok(InboundCommandResult::Handled)
        }
        "who" => {
            send_who(host, context).await?;
            Ok(InboundCommandResult::Handled)
        }
        "approve" | "approve-always" | "deny" => {
            respond_to_permission(host, context, &parsed.args, parsed.command.as_str()).await?;
            Ok(InboundCommandResult::Handled)
        }
        _ => Ok(InboundCommandResult::Unhandled),
    }
}

async fn send_help<H: InboundCommandHost + ?Sized>(
    host: &H,
    context: &InboundCommandContext,
) -> Result<(), String> {
    let clear_help = if host.is_shared_session(context) {
        "/clear confirm — Clear the shared session (aliases: /reset, /new)"
    } else {
        "/clear — Clear your session (aliases: /reset, /new)"
    };
    let mut lines = vec![
        "Commands:".to_owned(),
        "/help — Show this help".to_owned(),
        clear_help.to_owned(),
        "/who — Show current session & workspace".to_owned(),
        "/status — Show session info".to_owned(),
        "/approve [request-id] — Approve a pending permission request".to_owned(),
        "/approve-always [request-id] — Always approve a pending permission request".to_owned(),
        "/deny [request-id] — Deny a pending permission request".to_owned(),
    ];

    // Mirror ChannelBase's shared-command set. `/cancel` is intentionally not
    // in this set there, so registered cancel handlers appear with platform
    // commands in help output.
    const SHARED_COMMANDS: &[&str] = &[
        "help",
        "clear",
        "reset",
        "new",
        "who",
        "status",
        "approve",
        "approve-always",
        "deny",
    ];
    for command in host.registered_command_names(context) {
        if !SHARED_COMMANDS
            .iter()
            .any(|shared| command.eq_ignore_ascii_case(shared))
        {
            lines.push(format!("/{command}"));
        }
    }

    let agent_commands = host.agent_commands(context);
    if !agent_commands.is_empty() {
        lines.push(String::new());
        lines.push("Agent commands (forwarded to Qwen Code):".to_owned());
        for command in agent_commands {
            lines.push(format!("/{} — {}", command.name, command.description));
        }
    }
    lines.push(String::new());
    lines.push("Send any text to chat with the agent.".to_owned());
    host.send_thread_message(context.clone(), lines.join("\n"))
        .await
}

async fn clear_session<H: InboundCommandHost + ?Sized>(
    host: &H,
    context: &InboundCommandContext,
    args: &str,
) -> Result<(), String> {
    if !host.is_authorized_for_shared_session(context) {
        return host
            .send_thread_message(
                context.clone(),
                "Only authorized members can clear this shared session.".to_owned(),
            )
            .await;
    }
    if host.is_shared_session(context) && !args.eq_ignore_ascii_case("confirm") {
        return host
            .send_thread_message(
                context.clone(),
                "This clears the shared session for everyone who shares it. Re-send with \"confirm\" (e.g. /clear confirm) to proceed.".to_owned(),
            )
            .await;
    }

    let cleared = host.clear_session(context.clone()).await?;
    let message = if cleared {
        "Session cleared. The next message starts a fresh conversation."
    } else {
        "No active session to clear."
    };
    host.send_thread_message(context.clone(), message.to_owned())
        .await
}

async fn cancel_request<H: InboundCommandHost + ?Sized>(
    host: &H,
    context: &InboundCommandContext,
) -> Result<(), String> {
    if !host.is_authorized_for_shared_session(context) {
        return host
            .send_thread_message(
                context.clone(),
                "Only authorized members can cancel requests in this shared session.".to_owned(),
            )
            .await;
    }
    if !host.has_running_request(context) {
        return host
            .send_thread_message(
                context.clone(),
                "No request is currently running.".to_owned(),
            )
            .await;
    }

    let cancelled = host.cancel_running_request(context.clone()).await?;
    let message = if cancelled {
        "Cancelled current request."
    } else {
        "Failed to cancel current request."
    };
    host.send_thread_message(context.clone(), message.to_owned())
        .await
}

async fn respond_to_permission<H: InboundCommandHost + ?Sized>(
    host: &H,
    context: &InboundCommandContext,
    args: &str,
    decision: &str,
) -> Result<(), String> {
    if !host.is_authorized_for_shared_session(context) {
        return host
            .send_thread_message(
                context.clone(),
                "Only authorized members can answer permission requests in this shared session."
                    .to_owned(),
            )
            .await;
    }

    let explicit_id = (!args.trim().is_empty()).then(|| args.trim().to_owned());
    let candidates = host
        .pending_permission_requests(context.clone(), explicit_id.clone())
        .await?;
    let mut matching = candidates
        .into_iter()
        .filter(|pending| {
            explicit_id
                .as_deref()
                .is_none_or(|request_id| pending.request_id == request_id)
                && pending.target_chat_id == context.chat_id
                && pending.target_thread_id == context.thread_id
                && (!pending.user_input_presented || pending.target_sender_id == context.sender_id)
                && (pending.shared_session_target || pending.target_sender_id == context.sender_id)
        })
        .collect::<Vec<_>>();

    if let Some(request_id) = explicit_id.as_deref() {
        matching.retain(|pending| pending.request_id == request_id);
    }
    if matching.len() > 1 {
        let request_list = matching
            .iter()
            .take(6)
            .map(|pending| {
                let id = sanitize_quoted_text(&pending.request_id, 128);
                let title = pending
                    .tool_call_title
                    .as_deref()
                    .filter(|title| !title.is_empty())
                    .unwrap_or("Tool use");
                format!("- {id}: {}", sanitize_quoted_text(title, 160))
            })
            .collect::<Vec<_>>()
            .join("\n");
        return host
            .send_thread_message(
                context.clone(),
                format!(
                    "Multiple permission requests are pending for this chat. Reply with /{decision} <request-id>.\n{request_list}"
                ),
            )
            .await;
    }
    let Some(pending) = matching.into_iter().next() else {
        let message = if explicit_id.is_some() {
            "No pending permission request with that id for this chat."
        } else {
            "No pending permission request for this chat."
        };
        return host
            .send_thread_message(context.clone(), message.to_owned())
            .await;
    };

    if !host.permission_relay_available(context) {
        return host
            .send_thread_message(
                context.clone(),
                "Permission relay is not available for this session.".to_owned(),
            )
            .await;
    }

    if pending.user_input_presented && decision != "deny" {
        return host
            .send_chat_message(
                context.clone(),
                "Submit this question through its interactive card, or use /deny [request-id] to cancel it."
                    .to_owned(),
            )
            .await;
    }

    let response = match decision {
        "deny" => InboundPermissionResponse {
            outcome: denial_permission_outcome(&pending),
        },
        "approve" => {
            let Some(option_id) = approval_option_id(&pending) else {
                return host
                    .send_thread_message(
                        context.clone(),
                        "This permission request has no approvable option.".to_owned(),
                    )
                    .await;
            };
            InboundPermissionResponse {
                outcome: InboundPermissionOutcome::Selected { option_id },
            }
        }
        "approve-always" => {
            let Some(option_id) = approval_always_option_id(&pending) else {
                return host
                    .send_thread_message(
                        context.clone(),
                        "This permission request has no always-allow option.".to_owned(),
                    )
                    .await;
            };
            InboundPermissionResponse {
                outcome: InboundPermissionOutcome::Selected { option_id },
            }
        }
        _ => return Ok(()),
    };

    let accepted = match host
        .respond_to_permission(context.clone(), pending.request_id, response)
        .await
    {
        Ok(accepted) => accepted,
        Err(_) => {
            return host
                .send_thread_message(
                    context.clone(),
                    "Failed to answer the permission request.".to_owned(),
                )
                .await;
        }
    };
    let message = if accepted {
        match decision {
            "approve" => "Permission approved.",
            "approve-always" => "Permission approved always.",
            _ => "Permission denied.",
        }
    } else {
        "Permission request is no longer pending."
    };
    host.send_thread_message(context.clone(), message.to_owned())
        .await
}

fn approval_option_id(pending: &InboundPendingPermission) -> Option<String> {
    pending
        .options
        .iter()
        .find(|option| option.kind == Some(InboundPermissionOptionKind::AllowOnce))
        .or_else(|| {
            pending
                .options
                .iter()
                .find(|option| option.option_id == "proceed_once" && option.kind.is_none())
        })
        .map(|option| option.option_id.clone())
}

fn approval_always_option_id(pending: &InboundPendingPermission) -> Option<String> {
    let options = pending
        .options
        .iter()
        .filter(|option| option.kind == Some(InboundPermissionOptionKind::AllowAlways));
    let options = options.collect::<Vec<_>>();
    options
        .iter()
        .find(|option| option.option_id == "proceed_always_project")
        .or_else(|| {
            options
                .iter()
                .find(|option| option.option_id == "proceed_always_user")
        })
        .or_else(|| options.first())
        .map(|option| option.option_id.clone())
}

fn denial_permission_outcome(pending: &InboundPendingPermission) -> InboundPermissionOutcome {
    let option = pending
        .options
        .iter()
        .find(|option| option.kind == Some(InboundPermissionOptionKind::RejectOnce))
        .or_else(|| {
            pending
                .options
                .iter()
                .find(|option| option.option_id == "cancel" && option.kind.is_none())
        });
    option.map_or(InboundPermissionOutcome::Cancelled, |option| {
        InboundPermissionOutcome::Selected {
            option_id: option.option_id.clone(),
        }
    })
}

async fn send_status<H: InboundCommandHost + ?Sized>(
    host: &H,
    context: &InboundCommandContext,
) -> Result<(), String> {
    if !host.is_authorized_for_shared_session(context) {
        return host
            .send_thread_message(
                context.clone(),
                "Only authorized members can view this shared session.".to_owned(),
            )
            .await;
    }

    let status = host.status_info(context);
    let mut lines = vec![
        format!(
            "Session: {}",
            if status.has_session { "active" } else { "none" }
        ),
        format!("Access: {}", status.access_policy),
        format!("Channel: {}", context.channel_name),
    ];
    if let (Some(identity_id), Some(memory_mode)) =
        (status.identity_id.as_deref(), status.memory_mode.as_deref())
    {
        lines.push(format!(
            "Identity: {}",
            sanitize_quoted_text(identity_id, 128)
        ));
        lines.push(format!("Memory: {memory_mode}"));
    }
    host.send_thread_message(context.clone(), lines.join("\n"))
        .await
}

async fn send_who<H: InboundCommandHost + ?Sized>(
    host: &H,
    context: &InboundCommandContext,
) -> Result<(), String> {
    // /who exposes the workspace basename. Keep it behind the same shared
    // session authorization policy used by /clear and /status.
    if !host.is_authorized_for_shared_session(context) {
        return host
            .send_thread_message(
                context.clone(),
                "Only authorized members can view this shared session.".to_owned(),
            )
            .await;
    }

    let who = host.who_info(context);
    let scope_note = match who.session_scope {
        InboundSessionScope::Single => " (shared channel-wide)",
        InboundSessionScope::Thread | InboundSessionScope::ChatThread if context.is_group => {
            " (shared by this group)"
        }
        InboundSessionScope::User if context.is_group => " (private to you)",
        InboundSessionScope::User
        | InboundSessionScope::Thread
        | InboundSessionScope::ChatThread => "",
    };

    let mut lines = vec![format!("Channel: {}", context.channel_name)];
    if let Some(identity) = who.identity {
        lines.push(format!(
            "Identity: {}",
            sanitize_quoted_text(&identity.display_name, 128)
        ));
        lines.push(format!(
            "Memory: {}",
            sanitize_quoted_text(&identity.memory_namespace, 128)
        ));
    }
    // Only the basename is reported, never the absolute workspace path.
    let workspace = std::path::Path::new(&who.workspace_cwd)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    lines.push(format!("Workspace: {workspace}"));
    lines.push(format!(
        "Session: {}{scope_note}",
        if who.has_session { "active" } else { "none" }
    ));

    host.send_thread_message(context.clone(), lines.join("\n"))
        .await
}
