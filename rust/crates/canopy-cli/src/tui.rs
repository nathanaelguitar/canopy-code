use std::collections::{HashSet, VecDeque};
use std::io::{self, IsTerminal, Write};
use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use canopy_core::agent_runtime::AgentRunEvent;
use canopy_core::services::at_resource_references::LocalExtensionReference;
use canopy_core::turn::TurnEvent;
use crossterm::event::{
    self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    Event, KeyCode, KeyModifiers, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::{Terminal, TerminalOptions, Viewport};

mod command_completion;
mod help;
mod hooks;
mod markdown;
mod memory;
mod stats;
mod voice;

use command_completion::Completion as SlashCompletion;
pub(crate) use hooks::HooksDialogModel;
pub(crate) use memory::MemoryDialogModel;
use voice::{VoiceDictation, VoiceResult};

const MAX_TRANSCRIPT_BYTES: usize = 512 * 1024;
const MAX_TRANSCRIPT_MESSAGES: usize = 1_000;
const MAX_RENDERED_CONVERSATION_LINES: usize = 16_384;
const MAX_RESTORED_HISTORY_BYTES: usize = 256 * 1024;
const MAX_INPUT_BYTES: usize = 64 * 1024;
const MAX_INPUT_HISTORY_ENTRIES: usize = 100;
const MAX_INPUT_HISTORY_BYTES: usize = 64 * 1024;
const MAX_VISIBLE_INPUT_LINES: usize = 5;

struct ChatMessage {
    role: String,
    content: String,
}

pub struct ChatTerminal {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
    session_id: String,
    messages: VecDeque<ChatMessage>,
    transcript_bytes: usize,
    input_history: VecDeque<String>,
    input_history_bytes: usize,
    conversation_scroll: Option<usize>,
    status: String,
    in_alternate_screen: bool,
    active_tools: usize,
    completion_query: Option<String>,
    completion_index: usize,
    skill_commands: Vec<String>,
    file_commands: Vec<String>,
    prompt_commands: Vec<command_completion::PromptCommand>,
    voice: VoiceDictation,
}

static INTERRUPT_REQUESTED: AtomicBool = AtomicBool::new(false);
static TERMINAL_ACTIVE: AtomicBool = AtomicBool::new(false);

struct RawMode;

impl RawMode {
    fn enter() -> Result<Self, String> {
        enable_raw_mode().map_err(|error| format!("could not enable terminal input: {error}"))?;
        Ok(Self)
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}

pub fn supports_fullscreen() -> bool {
    io::stdin().is_terminal()
        && io::stdout().is_terminal()
        && io::stderr().is_terminal()
        && std::env::var("TERM").as_deref() != Ok("dumb")
}

impl ChatTerminal {
    pub fn new(session_id: &str, history: &[serde_json::Value]) -> Result<Self, String> {
        install_interrupt_cleanup()?;
        let mut terminal = Terminal::with_options(
            CrosstermBackend::new(io::stdout()),
            TerminalOptions {
                viewport: Viewport::Fullscreen,
            },
        )
        .map_err(|error| format!("could not initialize terminal UI: {error}"))?;
        if let Err(error) = execute!(
            terminal.backend_mut(),
            EnterAlternateScreen,
            crossterm::cursor::Hide,
            EnableBracketedPaste,
            EnableMouseCapture
        ) {
            let _ = execute!(
                terminal.backend_mut(),
                DisableBracketedPaste,
                DisableMouseCapture,
                LeaveAlternateScreen,
                crossterm::cursor::Show
            );
            return Err(format!("could not enter terminal UI: {error}"));
        }
        let voice = VoiceDictation::load();
        let mut ui = Self {
            terminal,
            session_id: session_id.to_owned(),
            messages: VecDeque::new(),
            transcript_bytes: 0,
            input_history: VecDeque::new(),
            input_history_bytes: 0,
            conversation_scroll: None,
            status: voice.initial_status(),
            in_alternate_screen: true,
            active_tools: 0,
            completion_query: None,
            completion_index: 0,
            skill_commands: Vec::new(),
            file_commands: Vec::new(),
            prompt_commands: Vec::new(),
            voice,
        };
        INTERRUPT_REQUESTED.store(false, Ordering::SeqCst);
        TERMINAL_ACTIVE.store(true, Ordering::SeqCst);
        ui.seed_history(history);
        ui.draw(None)?;
        Ok(ui)
    }

    pub fn read_prompt(&mut self) -> Result<Option<String>, String> {
        let mut input = Vec::<char>::new();
        let mut input_bytes = 0usize;
        let mut cursor = 0usize;
        let mut history_index: Option<usize> = None;
        let mut draft: Option<(Vec<char>, usize)> = None;
        let _raw_mode = RawMode::enter()?;
        self.draw_input(&input, cursor)?;
        loop {
            self.voice.tick();
            if let Some(result) = self.voice.take_result() {
                match result {
                    VoiceResult::Transcript { text, submit } => {
                        insert_voice_transcript(&mut input, &mut cursor, &mut input_bytes, &text);
                        self.status = if text.trim().is_empty() {
                            "No speech detected".to_owned()
                        } else {
                            "Voice transcript added · Enter to send".to_owned()
                        };
                        if submit {
                            let prompt = input.iter().collect::<String>();
                            self.remember_input(&prompt);
                            return Ok(Some(prompt));
                        }
                    }
                    VoiceResult::Failed(error) => {
                        self.status = format!("Voice transcription failed: {error}");
                    }
                }
                self.draw_input(&input, cursor)?;
            }
            if !event::poll(Duration::from_millis(80))
                .map_err(|error| format!("could not read terminal event: {error}"))?
            {
                continue;
            }
            let event =
                event::read().map_err(|error| format!("could not read terminal event: {error}"))?;
            let Event::Key(key) = event else {
                match event {
                    Event::Paste(pasted) => {
                        insert_pasted_chars(&mut input, &mut cursor, &mut input_bytes, &pasted);
                        self.draw_input(&input, cursor)?;
                    }
                    Event::Resize(_, _) => self.draw_input(&input, cursor)?,
                    Event::Mouse(mouse) => match mouse.kind {
                        MouseEventKind::ScrollUp => {
                            self.scroll_conversation(true, Some(3), &input, cursor)?;
                            self.draw_input(&input, cursor)?;
                        }
                        MouseEventKind::ScrollDown => {
                            self.scroll_conversation(false, Some(3), &input, cursor)?;
                            self.draw_input(&input, cursor)?;
                        }
                        _ => {}
                    },
                    _ => {}
                }
                continue;
            };
            if key.kind == crossterm::event::KeyEventKind::Release {
                if key.code == KeyCode::Char(' ')
                    && self.voice.mode_is_hold()
                    && self.voice.is_recording()
                {
                    self.voice.finish_recording(false);
                    self.status = self.voice.status().to_owned();
                    self.draw_input(&input, cursor)?;
                    continue;
                }
                continue;
            }
            if key.code == KeyCode::Esc && self.voice.is_active() {
                self.voice.cancel();
                self.status = "Voice recording cancelled".to_owned();
                self.draw_input(&input, cursor)?;
                continue;
            }
            if key.code == KeyCode::Char('c')
                && key.modifiers.contains(KeyModifiers::CONTROL)
                && self.voice.is_active()
            {
                self.voice.cancel();
                self.status = "Voice recording cancelled".to_owned();
                self.draw_input(&input, cursor)?;
                continue;
            }
            if key.code == KeyCode::Char(' ')
                && self.voice.is_enabled()
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
            {
                self.voice.handle_space(key.kind);
                self.status = self.voice.status().to_owned();
                self.draw_input(&input, cursor)?;
                continue;
            }

            let completion = command_completion::for_input(
                &input,
                cursor,
                &self.skill_commands,
                &self.file_commands,
            );
            if let Some(completion) = completion.as_ref() {
                if completion.suggestions.len() > 1
                    && matches!(key.code, KeyCode::Up | KeyCode::Down)
                {
                    let count = completion.suggestions.len();
                    self.completion_index = match key.code {
                        KeyCode::Up => (self.completion_index + count - 1) % count,
                        KeyCode::Down => (self.completion_index + 1) % count,
                        _ => self.completion_index,
                    };
                    self.draw_input(&input, cursor)?;
                    continue;
                }

                if key.code == KeyCode::Tab && !key.modifiers.contains(KeyModifiers::SHIFT) {
                    self.accept_slash_completion(
                        &mut input,
                        &mut cursor,
                        &mut input_bytes,
                        completion,
                    );
                    self.draw_input(&input, cursor)?;
                    continue;
                }

                if key.code == KeyCode::Enter
                    && !key.modifiers.contains(KeyModifiers::CONTROL)
                    && !completion.is_perfect_match()
                    && !completion.suggestions.is_empty()
                {
                    self.accept_slash_completion(
                        &mut input,
                        &mut cursor,
                        &mut input_bytes,
                        completion,
                    );
                    self.draw_input(&input, cursor)?;
                    continue;
                }
            }

            match key.code {
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    return Ok(None);
                }
                KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    if input.is_empty() {
                        return Ok(None);
                    }
                    if cursor < input.len() {
                        input_bytes = input_bytes.saturating_sub(input.remove(cursor).len_utf8());
                    }
                }
                KeyCode::Enter if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    if input_bytes < MAX_INPUT_BYTES {
                        input.insert(cursor, '\n');
                        input_bytes += 1;
                        cursor += 1;
                    }
                }
                KeyCode::Enter => {
                    let prompt = input.into_iter().collect::<String>();
                    self.remember_input(&prompt);
                    return Ok(Some(prompt));
                }
                KeyCode::Backspace if cursor > 0 => {
                    cursor -= 1;
                    input_bytes = input_bytes.saturating_sub(input.remove(cursor).len_utf8());
                }
                KeyCode::Delete if cursor < input.len() => {
                    input_bytes = input_bytes.saturating_sub(input.remove(cursor).len_utf8());
                }
                KeyCode::Left if cursor > 0 => cursor -= 1,
                KeyCode::Right if cursor < input.len() => cursor += 1,
                KeyCode::Home => cursor = current_line_start(&input, cursor),
                KeyCode::End => cursor = current_line_end(&input, cursor),
                KeyCode::Up => {
                    if !move_cursor_vertical(&input, &mut cursor, true)
                        && !self.input_history.is_empty()
                    {
                        let next_index = match history_index {
                            Some(index) => index.saturating_sub(1),
                            None => {
                                draft = Some((input.clone(), cursor));
                                self.input_history.len() - 1
                            }
                        };
                        history_index = Some(next_index);
                        input = self.input_history[next_index].chars().collect();
                        input_bytes = self.input_history[next_index].len();
                        cursor = input.len();
                    }
                }
                KeyCode::Down => {
                    if !move_cursor_vertical(&input, &mut cursor, false) {
                        if let Some(index) = history_index {
                            if index + 1 < self.input_history.len() {
                                let next_index = index + 1;
                                history_index = Some(next_index);
                                input = self.input_history[next_index].chars().collect();
                                input_bytes = self.input_history[next_index].len();
                                cursor = input.len();
                            } else {
                                (input, cursor) = draft.take().unwrap_or_default();
                                input_bytes =
                                    input.iter().map(|character| character.len_utf8()).sum();
                                history_index = None;
                            }
                        }
                    }
                }
                KeyCode::PageUp => self.scroll_conversation(true, None, &input, cursor)?,
                KeyCode::PageDown => self.scroll_conversation(false, None, &input, cursor)?,
                KeyCode::Char(character)
                    if !key
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                        && input_bytes.saturating_add(character.len_utf8()) <= MAX_INPUT_BYTES =>
                {
                    input.insert(cursor, character);
                    input_bytes += character.len_utf8();
                    cursor += 1;
                }
                _ => {}
            }
            self.draw_input(&input, cursor)?;
        }
    }

    pub fn begin_turn(&mut self, prompt: &str) -> Result<(), String> {
        self.conversation_scroll = None;
        self.push_message("You", prompt.to_owned());
        self.push_message("Canopy", String::new());
        self.status = "Thinking… · Ctrl-C to interrupt".to_owned();
        self.draw(None)
    }

    pub fn add_session_reference_card(
        &mut self,
        mention: &str,
        description: &str,
        result_display: Option<&str>,
        is_error: bool,
    ) -> Result<(), String> {
        let mut content = format!("{mention}\n{description}");
        if let Some(result_display) = result_display {
            content.push('\n');
            content.push_str(result_display);
        }
        self.insert_transcript_message(
            if is_error {
                "ReferenceError"
            } else {
                "ReferenceSuccess"
            },
            content,
        )
    }

    pub fn add_file_reference_card(
        &mut self,
        mention: &str,
        description: &str,
        result_display: Option<&str>,
        is_directory: bool,
        is_error: bool,
    ) -> Result<(), String> {
        let mut content = format!("{mention}\n{description}");
        if let Some(result_display) = result_display {
            content.push('\n');
            content.push_str(result_display);
        }
        let role = match (is_directory, is_error) {
            (true, false) => "DirectoryReferenceSuccess",
            (true, true) => "DirectoryReferenceError",
            (false, false) => "FileReferenceSuccess",
            (false, true) => "FileReferenceError",
        };
        self.insert_transcript_message(role, content)
    }

    pub fn add_session_reference_debug_message(&mut self, message: &str) -> Result<(), String> {
        self.insert_transcript_message("Debug", message.to_owned())
    }

    pub fn add_command_warning(&mut self, message: &str) -> Result<(), String> {
        self.insert_transcript_message("CommandWarning", message.to_owned())
    }

    pub fn set_skill_commands(&mut self, skill_commands: Vec<String>) {
        self.skill_commands = skill_commands;
    }

    pub fn set_prompt_commands(
        &mut self,
        user_command_dir: &Path,
        workspace_command_dir: &Path,
        active_extensions: &[LocalExtensionReference],
        safe_mode: bool,
        bare_mode: bool,
        workspace_trusted: bool,
        disabled_names: &HashSet<String>,
    ) {
        self.prompt_commands = command_completion::load_prompt_commands(
            user_command_dir,
            workspace_command_dir,
            active_extensions,
            safe_mode,
            bare_mode,
            workspace_trusted,
            disabled_names,
            &self.skill_commands,
        );
        self.file_commands = self
            .prompt_commands
            .iter()
            .map(|command| command.name.clone())
            .collect();
    }

    /// Expand a loaded user, workspace, or extension command when the input
    /// invokes one. Errors must be displayed without submitting a model turn.
    pub fn expand_prompt_command(&self, input: &str) -> Option<Result<String, String>> {
        command_completion::expand_prompt_command(&self.prompt_commands, input)
    }

    pub fn prompt_command_shadows_native_alias(&self, input: &str) -> bool {
        command_completion::shadows_native_alias(&self.prompt_commands, input)
    }

    pub fn add_stats_command_result(
        &mut self,
        command: &str,
        result: &str,
        is_error: bool,
    ) -> Result<(), String> {
        self.insert_transcript_message(
            if is_error { "StatsError" } else { "Stats" },
            format!("{command}\n{result}"),
        )
    }

    pub fn add_memory_command_result(
        &mut self,
        result: &str,
        is_error: bool,
    ) -> Result<(), String> {
        self.insert_transcript_message(
            if is_error { "MemoryError" } else { "Memory" },
            result.to_owned(),
        )
    }

    pub fn add_doctor_command_result(
        &mut self,
        result: &str,
        is_error: bool,
    ) -> Result<(), String> {
        self.insert_transcript_message(
            if is_error { "DoctorError" } else { "Doctor" },
            result.to_owned(),
        )
    }

    pub fn begin_resume(&mut self) -> Result<(), String> {
        self.conversation_scroll = None;
        self.push_message("Canopy", String::new());
        self.status = "Continuing the interrupted turn… · Ctrl-C to interrupt".to_owned();
        self.draw(None)
    }

    pub fn handle_agent_event(&mut self, event: AgentRunEvent) -> Result<(), String> {
        if INTERRUPT_REQUESTED.load(Ordering::SeqCst) {
            return Err("interrupted by Ctrl-C".to_owned());
        }
        match event {
            AgentRunEvent::Turn(TurnEvent::Content { value, .. }) => {
                self.append_assistant(&value);
                self.draw(None)
            }
            AgentRunEvent::Turn(TurnEvent::Finished { .. }) => {
                self.status = "Ready for your next message".to_owned();
                self.draw(None)
            }
            AgentRunEvent::Turn(TurnEvent::Retry { .. }) => {
                self.status = "Retrying the model request…".to_owned();
                self.draw(None)
            }
            AgentRunEvent::Turn(TurnEvent::ModelFallback {
                from_model,
                to_model,
                ..
            }) => {
                self.status = format!("Switching from {from_model} to {to_model}…");
                self.draw(None)
            }
            AgentRunEvent::Turn(TurnEvent::ChatCompressed { .. }) => {
                self.status = "Conversation context was compacted".to_owned();
                self.draw(None)
            }
            AgentRunEvent::Turn(TurnEvent::UserCancelled) => {
                self.status = "Response cancelled".to_owned();
                self.draw(None)
            }
            AgentRunEvent::ToolExecutionStarted { name, .. } => {
                self.active_tools += 1;
                if self.active_tools == 1 {
                    self.suspend_for_tool()?;
                }
                eprintln!("Running tool: {name}");
                Ok(())
            }
            AgentRunEvent::ToolExecutionFinished {
                name,
                was_truncated,
                display,
                ..
            } => {
                if was_truncated {
                    eprintln!("Tool output for {name} was shortened and saved to session storage.");
                }
                if let Some(display) = display {
                    print_todo_summary(&display);
                }
                self.active_tools = self.active_tools.saturating_sub(1);
                if self.active_tools == 0 && !self.in_alternate_screen {
                    self.resume_from_tool()?;
                }
                Ok(())
            }
            AgentRunEvent::Turn(_) => Ok(()),
        }
    }

    fn seed_history(&mut self, history: &[serde_json::Value]) {
        let mut visible = Vec::new();
        for message in history.iter().rev() {
            let role = match message.get("role").and_then(serde_json::Value::as_str) {
                Some("user") => "You",
                Some("assistant" | "model") => "Canopy",
                _ => continue,
            };
            let Some(content) = visible_content(message) else {
                continue;
            };
            visible.push((role, content));
        }
        let mut bytes = 0usize;
        let mut recent = Vec::new();
        for (role, content) in visible {
            if bytes.saturating_add(content.len()) > MAX_RESTORED_HISTORY_BYTES {
                break;
            }
            bytes += content.len();
            recent.push((role, content));
        }
        for (role, content) in recent.into_iter().rev() {
            self.push_message(role, content);
        }
    }

    fn push_message(&mut self, role: &str, mut content: String) {
        sanitize_terminal_text(&mut content);
        if content.len() > MAX_TRANSCRIPT_BYTES {
            content.truncate(utf8_boundary(&content, MAX_TRANSCRIPT_BYTES));
        }
        self.transcript_bytes = self.transcript_bytes.saturating_add(content.len());
        self.messages.push_back(ChatMessage {
            role: role.to_owned(),
            content,
        });
        self.trim_transcript();
    }

    fn insert_transcript_message(&mut self, role: &str, mut content: String) -> Result<(), String> {
        sanitize_terminal_text(&mut content);
        if content.len() > MAX_TRANSCRIPT_BYTES {
            content.truncate(utf8_boundary(&content, MAX_TRANSCRIPT_BYTES));
        }
        self.transcript_bytes = self.transcript_bytes.saturating_add(content.len());
        let pending_assistant = self
            .messages
            .back()
            .is_some_and(|message| message.role == "Canopy");
        let insertion_index = self
            .messages
            .len()
            .saturating_sub(usize::from(pending_assistant));
        self.messages.insert(
            insertion_index,
            ChatMessage {
                role: role.to_owned(),
                content,
            },
        );
        self.trim_transcript();
        self.draw(None)
    }

    fn remember_input(&mut self, prompt: &str) {
        if prompt.trim().is_empty()
            || prompt.len() > MAX_INPUT_HISTORY_BYTES
            || self
                .input_history
                .back()
                .is_some_and(|previous| previous == prompt)
        {
            return;
        }
        while self.input_history.len() >= MAX_INPUT_HISTORY_ENTRIES
            || self.input_history_bytes.saturating_add(prompt.len()) > MAX_INPUT_HISTORY_BYTES
        {
            let Some(oldest) = self.input_history.pop_front() else {
                break;
            };
            self.input_history_bytes = self.input_history_bytes.saturating_sub(oldest.len());
        }
        self.input_history.push_back(prompt.to_owned());
        self.input_history_bytes = self.input_history_bytes.saturating_add(prompt.len());
    }

    fn scroll_conversation(
        &mut self,
        up: bool,
        amount: Option<usize>,
        input: &[char],
        cursor: usize,
    ) -> Result<(), String> {
        let size = self
            .terminal
            .size()
            .map_err(|error| format!("could not read terminal size: {error}"))?;
        let rows = self.render_rows();
        let width = size.width.saturating_sub(2);
        let suggestion_count =
            command_completion::for_input(input, cursor, &self.skill_commands, &self.file_commands)
                .map(|completion| completion.suggestions.len())
                .unwrap_or_default();
        let prompt_height = prompt_height(
            size.height,
            input.iter().filter(|character| **character == '\n').count() + 1,
            suggestion_count,
        );
        let height = size.height.saturating_sub(prompt_height.saturating_add(6));
        let max_scroll = max_body_scroll(&rows, width, height);
        let current = self
            .conversation_scroll
            .unwrap_or(max_scroll)
            .min(max_scroll);
        let page = amount.unwrap_or_else(|| usize::from(height.max(1)));
        let next = if up {
            current.saturating_sub(page)
        } else {
            current.saturating_add(page).min(max_scroll)
        };
        self.conversation_scroll = (next < max_scroll).then_some(next);
        Ok(())
    }

    fn append_assistant(&mut self, chunk: &str) {
        let mut safe_chunk = chunk.to_owned();
        sanitize_terminal_text(&mut safe_chunk);
        let remaining = MAX_TRANSCRIPT_BYTES.saturating_sub(self.transcript_bytes);
        let take = utf8_boundary(&safe_chunk, remaining);
        if let Some(message) = self
            .messages
            .back_mut()
            .filter(|item| item.role == "Canopy")
        {
            message.content.push_str(&safe_chunk[..take]);
            self.transcript_bytes = self.transcript_bytes.saturating_add(take);
        }
        if take < safe_chunk.len() {
            self.status =
                "Response display capped; the full response is saved in session history".to_owned();
        }
    }

    fn trim_transcript(&mut self) {
        while self.transcript_bytes > MAX_TRANSCRIPT_BYTES
            || self.messages.len() > MAX_TRANSCRIPT_MESSAGES
        {
            let Some(message) = self.messages.pop_front() else {
                break;
            };
            self.transcript_bytes = self.transcript_bytes.saturating_sub(message.content.len());
        }
    }

    fn draw_input(&mut self, input: &[char], cursor: usize) -> Result<(), String> {
        self.sync_completion_selection(input, cursor);
        self.draw(Some((input, cursor)))
    }

    fn sync_completion_selection(&mut self, input: &[char], cursor: usize) {
        let completion =
            command_completion::for_input(input, cursor, &self.skill_commands, &self.file_commands);
        let query = completion
            .as_ref()
            .map(|completion| completion.query.clone());
        if self.completion_query != query {
            self.completion_query = query;
            self.completion_index = 0;
        } else if let Some(completion) = completion {
            if !completion.suggestions.is_empty() {
                self.completion_index %= completion.suggestions.len();
            } else {
                self.completion_index = 0;
            }
        } else {
            self.completion_index = 0;
        }
    }

    fn accept_slash_completion(
        &mut self,
        input: &mut Vec<char>,
        cursor: &mut usize,
        input_bytes: &mut usize,
        completion: &SlashCompletion,
    ) -> bool {
        let Some(suggestion) = completion.suggestions.get(
            self.completion_index
                .min(completion.suggestions.len().saturating_sub(1)),
        ) else {
            return false;
        };
        let has_space_after = input
            .get(completion.end)
            .is_some_and(|character| character.is_whitespace());
        let replacement_text = if completion.is_root_command {
            format!("/{}", suggestion.name)
        } else {
            suggestion.name.to_owned()
        };
        let mut replacement = replacement_text.chars().collect::<Vec<_>>();
        if !has_space_after {
            replacement.push(' ');
        }
        let removed_bytes = input[completion.start..completion.end]
            .iter()
            .map(|character| character.len_utf8())
            .sum::<usize>();
        let inserted_bytes = replacement
            .iter()
            .map(|character| character.len_utf8())
            .sum::<usize>();
        let new_bytes = input_bytes
            .saturating_sub(removed_bytes)
            .saturating_add(inserted_bytes);
        if new_bytes > MAX_INPUT_BYTES {
            return false;
        }

        input.splice(
            completion.start..completion.end,
            replacement.iter().copied(),
        );
        *cursor = completion.start + replacement.len();
        *input_bytes = new_bytes;
        true
    }

    fn draw(&mut self, input: Option<(&[char], usize)>) -> Result<(), String> {
        let session_id = self.session_id.clone();
        let status = self.status.clone();
        let rows = self.render_rows();
        let conversation_scroll = self.conversation_scroll;
        let (prompt_text, cursor_line, cursor_column) = prompt_content(input);
        let completion = input.and_then(|(input, cursor)| {
            command_completion::for_input(input, cursor, &self.skill_commands, &self.file_commands)
        });
        let suggestion_count = completion
            .as_ref()
            .map(|completion| completion.suggestions.len())
            .unwrap_or_default();
        let completion_index = self.completion_index;
        let input_line_count = input
            .map(|(input, _)| input.iter().filter(|character| **character == '\n').count() + 1)
            .unwrap_or(1);
        let voice_hint = self.voice.prompt_hint();
        self.terminal
            .draw(|frame| {
                let prompt_height = prompt_height(
                    frame.area().height,
                    input_line_count,
                    suggestion_count,
                );
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(3),
                        Constraint::Min(4),
                        Constraint::Length(prompt_height),
                        Constraint::Length(1),
                    ])
                    .split(frame.area());
                let title = Paragraph::new(Line::from(vec![
                    Span::styled(
                        " Canopy ",
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(format!("· session {} ", session_id)),
                ]))
                .block(Block::default().borders(Borders::ALL));
                frame.render_widget(title, chunks[0]);

                let body_block = Block::default()
                    .title(" Conversation ")
                    .borders(Borders::ALL);
                let body_area = body_block.inner(chunks[1]);
                let scroll = body_scroll(
                    &rows,
                    body_area.width,
                    body_area.height,
                    conversation_scroll,
                );
                let body = Paragraph::new(Text::from(rows))
                    .block(body_block)
                    .wrap(Wrap { trim: false })
                    .scroll((scroll, 0));
                frame.render_widget(body, chunks[1]);

                let completion_hint = if suggestion_count > 0 {
                    " · Tab complete · ↑/↓ select"
                } else {
                    ""
                };
                let prompt_block = Block::default()
                    .title(format!(" Message · Ctrl+Enter for newline · Enter to send · {voice_hint}{completion_hint} · /exit to quit "))
                    .borders(Borders::ALL);
                let prompt_area = prompt_block.inner(chunks[2]);
                let menu_height = suggestion_count
                    .min(command_completion::max_visible_suggestions())
                    .min(usize::from(prompt_area.height.saturating_sub(1)))
                    as u16;
                let input_height = prompt_area.height.saturating_sub(menu_height).max(1);
                let input_area = Rect {
                    height: input_height,
                    ..prompt_area
                };
                let menu_area = Rect {
                    y: prompt_area.y.saturating_add(input_height),
                    height: menu_height,
                    ..prompt_area
                };
                let visible_input_lines = usize::from(input_area.height).max(1);
                let prompt_vertical_scroll = cursor_line
                    .saturating_sub(visible_input_lines.saturating_sub(1))
                    .min(u16::MAX as usize) as u16;
                frame.render_widget(prompt_block, chunks[2]);
                let prompt = Paragraph::new(prompt_text).scroll((
                    prompt_vertical_scroll,
                    horizontal_scroll(cursor_column, chunks[2].width),
                ));
                frame.render_widget(prompt, input_area);

                if menu_height > 0
                    && let Some(completion) = completion.as_ref()
                {
                    let include_aliases = completion.query.is_empty();
                    let visible_suggestion_count = usize::from(menu_height);
                    let suggestion_start = completion_index
                        .saturating_add(1)
                        .saturating_sub(visible_suggestion_count);
                    let lines = completion
                        .suggestions
                        .iter()
                        .enumerate()
                        .skip(suggestion_start)
                        .take(visible_suggestion_count)
                        .map(|(index, suggestion)| {
                            let selected = index == completion_index;
                            let marker = if selected { "> " } else { "  " };
                            let label_style = if selected {
                                Style::default()
                                    .fg(Color::Yellow)
                                    .add_modifier(Modifier::BOLD)
                            } else {
                                Style::default().fg(Color::Cyan)
                            };
                            Line::from(vec![
                                Span::styled(
                                    marker,
                                    Style::default()
                                        .fg(Color::Cyan)
                                        .add_modifier(Modifier::BOLD),
                                ),
                                Span::styled(
                                    suggestion.label(include_aliases, completion.is_root_command),
                                    label_style,
                                ),
                                Span::raw("  "),
                                Span::styled(
                                    suggestion.description.as_str(),
                                    Style::default().fg(Color::Gray),
                                ),
                            ])
                        })
                        .collect::<Vec<_>>();
                    frame.render_widget(Paragraph::new(lines), menu_area);
                }

                let footer =
                    Paragraph::new(terminal_safe(&status)).style(Style::default().fg(Color::Gray));
                frame.render_widget(footer, chunks[3]);
            })
            .map_err(|error| format!("could not render terminal UI: {error}"))?;
        Ok(())
    }

    fn render_rows(&self) -> Vec<Line<'static>> {
        let mut rows = Vec::new();
        let mut processed_messages = 0usize;
        let mut omitted = false;
        for message in self.messages.iter().rev() {
            let remaining = MAX_RENDERED_CONVERSATION_LINES.saturating_sub(rows.len());
            if remaining <= 3 {
                omitted = true;
                break;
            }
            let content_limit = remaining - 3;
            let (content_rows, content_omitted) =
                markdown::render_suffix(&message.content, content_limit);
            let (color, label) = match message.role.as_str() {
                "You" => (Color::Green, "You"),
                "ReferenceSuccess" => (Color::Green, "Referenced Session · success"),
                "ReferenceError" => (Color::Red, "Referenced Session · error"),
                "FileReferenceSuccess" => (Color::Green, "Referenced File · success"),
                "FileReferenceError" => (Color::Red, "Referenced File · error"),
                "DirectoryReferenceSuccess" => (Color::Green, "Referenced Directory · success"),
                "DirectoryReferenceError" => (Color::Red, "Referenced Directory · error"),
                "Stats" => (Color::Cyan, "Usage"),
                "StatsError" => (Color::Red, "Usage error"),
                "Memory" => (Color::Cyan, "Memory"),
                "MemoryError" => (Color::Red, "Memory error"),
                "Doctor" => (Color::Cyan, "Doctor"),
                "DoctorError" => (Color::Red, "Doctor error"),
                "CommandWarning" => (Color::Yellow, "Command warning"),
                "Debug" => (Color::Yellow, "Debug"),
                _ => (Color::Cyan, "Canopy"),
            };
            rows.push(Line::raw(""));
            rows.extend(content_rows.into_iter().rev());
            rows.push(Line::from(Span::styled(
                format!("{label}"),
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            )));
            processed_messages += 1;
            if content_omitted {
                omitted = true;
                break;
            }
        }
        if processed_messages < self.messages.len() {
            omitted = true;
        }
        if omitted {
            rows.push(Line::styled(
                "… Older conversation rows omitted by the display limit …",
                Style::default().fg(Color::DarkGray),
            ));
        }
        rows.reverse();
        if rows.is_empty() {
            rows.push(Line::raw(
                "Ask Canopy to inspect, explain, or change something in this workspace.",
            ));
        }
        rows
    }

    fn suspend_for_tool(&mut self) -> Result<(), String> {
        if !self.in_alternate_screen {
            return Ok(());
        }
        disable_raw_mode()
            .map_err(|error| format!("could not switch to approval prompt: {error}"))?;
        execute!(
            self.terminal.backend_mut(),
            DisableBracketedPaste,
            DisableMouseCapture,
            LeaveAlternateScreen
        )
        .map_err(|error| format!("could not switch to approval prompt: {error}"))?;
        self.in_alternate_screen = false;
        Ok(())
    }

    fn resume_from_tool(&mut self) -> Result<(), String> {
        execute!(
            self.terminal.backend_mut(),
            EnterAlternateScreen,
            EnableBracketedPaste,
            EnableMouseCapture
        )
        .map_err(|error| format!("could not resume terminal UI: {error}"))?;
        self.in_alternate_screen = true;
        self.terminal
            .clear()
            .map_err(|error| format!("could not redraw terminal UI: {error}"))?;
        self.status = "Thinking… · Ctrl-C to interrupt".to_owned();
        self.draw(None)
    }
}

impl Drop for ChatTerminal {
    fn drop(&mut self) {
        TERMINAL_ACTIVE.store(false, Ordering::SeqCst);
        let _ = disable_raw_mode();
        let _ = execute!(
            self.terminal.backend_mut(),
            DisableBracketedPaste,
            DisableMouseCapture,
            LeaveAlternateScreen,
            crossterm::cursor::Show
        );
        let _ = self.terminal.backend_mut().flush();
    }
}

fn install_interrupt_cleanup() -> Result<(), String> {
    static INSTALLED: OnceLock<Result<(), String>> = OnceLock::new();
    INSTALLED
        .get_or_init(|| {
            ctrlc::set_handler(|| {
                INTERRUPT_REQUESTED.store(true, Ordering::SeqCst);
                std::thread::spawn(|| {
                    std::thread::sleep(Duration::from_secs(2));
                    if TERMINAL_ACTIVE.swap(false, Ordering::SeqCst) {
                        let _ = disable_raw_mode();
                        let mut stdout = io::stdout();
                        let _ = write!(
                            stdout,
                            "\x1b[?1006l\x1b[?1015l\x1b[?1003l\x1b[?1002l\x1b[?1000l\x1b[?2004l\x1b[?1049l\x1b[?25h\x1b[0m"
                        );
                        let _ = stdout.flush();
                        std::process::exit(130);
                    }
                });
            })
            .map_err(|error| format!("could not install terminal interrupt cleanup: {error}"))
        })
        .clone()
}

fn print_todo_summary(display: &serde_json::Value) {
    if display.get("type").and_then(serde_json::Value::as_str) != Some("todo_list") {
        return;
    }
    let Some(todos) = display.get("todos").and_then(serde_json::Value::as_array) else {
        return;
    };
    eprintln!("Todo list updated:");
    for todo in todos.iter().take(50) {
        let status = todo
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let content = todo
            .get("content")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        eprintln!("  [{status}] {}", terminal_safe(content));
    }
    if todos.len() > 50 {
        eprintln!("  … {} more item(s)", todos.len() - 50);
    }
}

fn visible_content(message: &serde_json::Value) -> Option<String> {
    content_text(message.get("content")).or_else(|| content_text(message.get("parts")))
}

fn content_text(content: Option<&serde_json::Value>) -> Option<String> {
    let content = content?;
    if let Some(text) = content.as_str() {
        return Some(text.to_owned());
    }
    let parts = content.as_array()?;
    let text = parts
        .iter()
        .filter_map(|part| part.get("text").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>()
        .join("");
    (!text.is_empty()).then_some(text)
}

fn current_line_start(input: &[char], cursor: usize) -> usize {
    input[..cursor]
        .iter()
        .rposition(|character| *character == '\n')
        .map(|newline| newline + 1)
        .unwrap_or(0)
}

fn current_line_end(input: &[char], cursor: usize) -> usize {
    input[cursor..]
        .iter()
        .position(|character| *character == '\n')
        .map(|newline| cursor + newline)
        .unwrap_or(input.len())
}

fn move_cursor_vertical(input: &[char], cursor: &mut usize, up: bool) -> bool {
    let line_start = current_line_start(input, *cursor);
    let column = cursor.saturating_sub(line_start);
    if up {
        if line_start == 0 {
            return false;
        }
        let previous_end = line_start - 1;
        let previous_start = input[..previous_end]
            .iter()
            .rposition(|character| *character == '\n')
            .map(|newline| newline + 1)
            .unwrap_or(0);
        *cursor = previous_start + column.min(previous_end - previous_start);
        true
    } else {
        let line_end = current_line_end(input, *cursor);
        if line_end == input.len() {
            return false;
        }
        let next_start = line_end + 1;
        let next_end = input[next_start..]
            .iter()
            .position(|character| *character == '\n')
            .map(|newline| next_start + newline)
            .unwrap_or(input.len());
        *cursor = next_start + column.min(next_end - next_start);
        true
    }
}

fn prompt_height(terminal_height: u16, input_lines: usize, suggestion_count: usize) -> u16 {
    let requested = input_lines.min(MAX_VISIBLE_INPUT_LINES) as u16
        + 2
        + suggestion_count.min(command_completion::max_visible_suggestions()) as u16;
    requested
        .min(terminal_height.saturating_sub(8).max(3))
        .max(3)
}

fn prompt_content(input: Option<(&[char], usize)>) -> (Text<'static>, usize, usize) {
    let Some((input, cursor)) = input else {
        return (Text::from(vec![Line::default()]), 0, 0);
    };
    let before = input[..cursor].iter().collect::<String>();
    let after = input[cursor..].iter().collect::<String>();
    let before_lines = before.split('\n').collect::<Vec<_>>();
    let after_lines = after.split('\n').collect::<Vec<_>>();
    let cursor_line = before_lines.len().saturating_sub(1);
    let cursor_column = before_lines
        .last()
        .map(|line| line.chars().count())
        .unwrap_or_default();
    let mut lines = Vec::with_capacity(cursor_line + after_lines.len());
    lines.extend(
        before_lines
            .iter()
            .take(cursor_line)
            .map(|line| Line::raw((*line).to_owned())),
    );
    lines.push(Line::from(vec![
        Span::raw(before_lines.last().copied().unwrap_or_default().to_owned()),
        Span::styled("▏", Style::default().fg(Color::Cyan)),
        Span::raw(after_lines.first().copied().unwrap_or_default().to_owned()),
    ]));
    lines.extend(
        after_lines
            .iter()
            .skip(1)
            .map(|line| Line::raw((*line).to_owned())),
    );
    (Text::from(lines), cursor_line, cursor_column)
}

fn insert_pasted_chars(
    input: &mut Vec<char>,
    cursor: &mut usize,
    input_bytes: &mut usize,
    pasted: &str,
) {
    let mut characters = pasted.chars().peekable();
    while let Some(character) = characters.next() {
        let character = if character == '\r' {
            if characters.peek() == Some(&'\n') {
                characters.next();
            }
            '\n'
        } else {
            character
        };
        if character.is_control() && character != '\n' {
            continue;
        }
        if input_bytes.saturating_add(character.len_utf8()) > MAX_INPUT_BYTES {
            break;
        }
        input.insert(*cursor, character);
        *input_bytes += character.len_utf8();
        *cursor += 1;
    }
}

fn insert_voice_transcript(
    input: &mut Vec<char>,
    cursor: &mut usize,
    input_bytes: &mut usize,
    transcript: &str,
) {
    let text = transcript
        .chars()
        .filter(|character| !character.is_control() || *character == '\n')
        .collect::<String>()
        .trim()
        .to_owned();
    if text.is_empty() {
        return;
    }
    if *cursor > 0 && !input[*cursor - 1].is_whitespace() {
        insert_pasted_chars(input, cursor, input_bytes, " ");
    }
    insert_pasted_chars(input, cursor, input_bytes, &text);
}

fn sanitize_terminal_text(text: &mut String) {
    if text
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
    {
        *text = terminal_safe(text);
    }
}

fn terminal_safe(text: &str) -> String {
    text.chars()
        .filter(|character| !character.is_control() || matches!(character, '\n' | '\t'))
        .collect()
}

fn utf8_boundary(text: &str, max_bytes: usize) -> usize {
    let mut boundary = max_bytes.min(text.len());
    while !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    boundary
}

fn body_scroll(rows: &[Line<'_>], width: u16, height: u16, scroll_from_top: Option<usize>) -> u16 {
    let max_scroll = max_body_scroll(rows, width, height);
    scroll_from_top
        .map(|scroll| scroll.min(max_scroll))
        .unwrap_or(max_scroll)
        .min(u16::MAX as usize) as u16
}

fn max_body_scroll(rows: &[Line<'_>], width: u16, height: u16) -> usize {
    if width == 0 || height == 0 {
        return 0;
    }
    let columns = usize::from(width);
    let visible_lines = rows
        .iter()
        .map(|row| {
            let chars = row
                .spans
                .iter()
                .map(|span| span.content.chars().count())
                .sum::<usize>();
            chars.max(1).div_ceil(columns)
        })
        .sum::<usize>();
    visible_lines.saturating_sub(usize::from(height))
}

fn horizontal_scroll(cursor_column: usize, width: u16) -> u16 {
    let available = usize::from(width.saturating_sub(4)).max(1);
    cursor_column
        .saturating_sub(available.saturating_sub(1))
        .min(u16::MAX as usize) as u16
}
