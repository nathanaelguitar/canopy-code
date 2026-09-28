//! Memory document, auto-memory, prompt, persistence, and indexing contracts.
//!
//! These modules port focused source files from `packages/core/src/memory`;
//! runtime orchestration and caller wiring remain separate.

pub mod channel_memory;
pub mod channel_memory_document;
pub mod context_filenames;
pub mod discovery;
pub mod dream;
pub mod dream_agent_planner;
pub mod entries;
pub mod extraction;
pub mod extraction_agent_planner;
pub mod forget;
pub mod indexer;
pub mod learn_skill_agent;
pub mod manager;
pub mod memory_age;
pub mod memory_scoped_agent_config;
pub mod paths;
pub mod pending_skills;
pub mod prompt;
pub mod recall;
pub mod refresh;
pub mod relevance_selector;
pub mod remember;
pub mod rules_discovery;
pub mod scan;
pub mod scopes;
pub mod secret_scanner;
pub mod skill_review_agent_planner;
pub mod status;
pub mod store;
pub mod team_memory_git_status;
pub mod team_memory_secret_guard;
pub mod team_memory_sync;
pub mod types;
pub mod write_context_file;

pub use channel_memory::{
    AddChannelMemoryResult, CHANNEL_MEMORY_FILE_NAME, ChannelMemoryError,
    ChannelMemoryMutationResult, ChannelMemoryStore, ChannelMemoryTarget,
    LEGACY_CHANNEL_MEMORY_FILE_NAME, MAX_CHANNEL_MEMORY_BYTES, RemoveChannelMemoryResult,
    UpdateChannelMemoryResult, add_channel_memory_entries, append_channel_memory,
    clear_channel_memory, get_channel_memory_file_path, get_channel_memory_revision,
    get_legacy_channel_memory_file_path, list_channel_memory_entries, read_channel_memory,
    remove_channel_memory_entries, update_channel_memory_entry,
};
pub use channel_memory_document::{
    CHANNEL_MEMORY_DOCUMENT_VERSION, CHANNEL_MEMORY_ID_PATTERN, ChannelMemoryDocument,
    ChannelMemoryDocumentError, ChannelMemoryEntry, ChannelMemoryMigration,
    MAX_CHANNEL_MEMORY_ENTRIES, MAX_CHANNEL_MEMORY_ENTRIES_PER_REQUEST,
    MAX_CHANNEL_MEMORY_ENTRY_CODE_POINTS, NewChannelMemoryEntry, create_channel_memory_entry,
    is_channel_memory_id, normalize_channel_memory_text, parse_channel_memory_document,
    parse_legacy_channel_memory, render_channel_memory_recall, serialize_channel_memory_document,
};
pub use context_filenames::{
    AGENT_CONTEXT_FILENAME, ContextFilenameInput, DEFAULT_CONTEXT_FILENAME, LOCAL_CONTEXT_FILENAME,
    get_all_gemini_md_filenames, get_current_gemini_md_filename, set_gemini_md_filename,
};
pub use dream::{
    AutoMemoryDreamError, AutoMemoryDreamResult, DreamOrchestratorRuntime,
    MemoryDreamTelemetryEvent, MemoryDreamTrigger, RunDreamOptions, infer_touched_topics,
    run_managed_auto_memory_dream, update_dream_metadata_result,
    write_dream_manual_run_to_metadata,
};
pub use dream_agent_planner::{
    DEFAULT_DREAM_AGENT_MAX_TURNS, DEFAULT_DREAM_AGENT_TIMEOUT_MINUTES, DREAM_AGENT_SYSTEM_PROMPT,
    DREAM_AGENT_TOOLS, DreamAgentFuture, DreamAgentRequest, DreamAgentRunResult, DreamAgentRuntime,
    DreamAgentScopedPaths, DreamAgentStatus, DreamPlannerError, DreamPlannerOptions,
    DreamShellFlavor, MANAGED_AUTO_MEMORY_DREAM_AGENT_NAME, build_consolidation_task_prompt,
    build_dream_agent_request, get_transcript_dir, plan_managed_auto_memory_dream_by_agent,
};
pub use entries::{
    ManagedAutoMemoryEntry, build_auto_memory_entry_search_text, get_auto_memory_body_heading,
    merge_auto_memory_entry, parse_auto_memory_entries, render_auto_memory_body,
};
pub use extraction::{
    AutoMemoryExtractError, AutoMemoryExtractResult, AutoMemoryExtractSkippedReason,
    run_auto_memory_extract,
};
pub use extraction_agent_planner::{
    AutoMemoryExtractionExecutionResult, AutoMemoryExtractionRuntime, DEFAULT_EXTRACTION_MAX_TURNS,
    DEFAULT_EXTRACTION_TIMEOUT_MINUTES, EXTRACTION_AGENT_TOOLS, ExtractionAgentFuture,
    ExtractionAgentPlannerError, ExtractionAgentRequest, ExtractionAgentRunResult,
    ExtractionAgentStatus, ExtractionMessage, ExtractionRefreshFuture, ExtractionScopedPaths,
    MANAGED_AUTO_MEMORY_EXTRACTOR_AGENT_NAME, MAX_EXTRACTION_TOPIC_SUMMARY_UTF16_UNITS,
    TouchedExtractionTopics, build_extraction_agent_history, build_extraction_task_prompt,
    build_topic_summary_block, extraction_agent_system_prompt, run_auto_memory_extraction_by_agent,
    touched_topics_from_file_paths,
};
pub use forget::{
    AutoMemoryForgetError, AutoMemoryForgetMatch, AutoMemoryForgetResult,
    AutoMemoryForgetSelectionResult, AutoMemoryForgetSideQuery, AutoMemoryForgetStrategy,
    AutoMemoryStorageScope, DEFAULT_FORGET_SELECTION_LIMIT, FORGET_SELECTION_TIMEOUT,
    ForgetApplyOptions, ForgetOperationOptions, ForgetSelectionContent, ForgetSelectionOptions,
    ForgetSelectionRequest, UNBOUNDED_FORGET_SELECTION_LIMIT, forget_managed_auto_memory_entries,
    forget_managed_auto_memory_matches, select_managed_auto_memory_forget_candidates,
};
pub use indexer::{
    RebuildTeamMemoryIndexError, TeamMemoryRootSecurityError, build_managed_auto_memory_index,
    build_team_auto_memory_index, rebuild_managed_auto_memory_index,
    rebuild_team_auto_memory_index, rebuild_user_auto_memory_index,
};
pub use learn_skill_agent::{
    LEARNED_SKILL_DIR_PREFIX, LearnSkillError, LearnVideoInput, LearnVideoKind,
    build_learn_skill_prompt, build_learn_video_skill_request, parse_learn_video_input,
};
pub use manager::{
    AUTO_SKILL_THRESHOLD, DEFAULT_AUTO_DREAM_MIN_HOURS, DEFAULT_AUTO_DREAM_MIN_SESSIONS,
    DREAM_TASK_TYPE, DreamResult, DreamScheduleResult, DreamSkipReason, EXTRACT_TASK_TYPE,
    ExtractResult, ExtractSkipReason, MemoryManager, MemoryManagerFuture, MemoryManagerRuntime,
    MemorySubscription, MemoryTaskListener, MemoryTurn, SKILL_REVIEW_TASK_TYPE,
    ScheduleDreamParams, ScheduleExtractParams, ScheduleSkillReviewParams, SkillReviewResult,
    SkillReviewScheduleResult, SkillReviewSkipReason,
};
pub use memory_scoped_agent_config::{
    MemoryScopedAgentConfig, MemoryScopedAgentConfigOptions, MemoryScopedBasePermissionManager,
    MemoryShellReadOnlyChecker, ShellReadOnlyFuture,
};
pub use paths::{
    AUTO_MEMORY_CONSOLIDATION_LOCK_FILENAME, AUTO_MEMORY_DIRNAME,
    AUTO_MEMORY_EXTRACT_CURSOR_FILENAME, AUTO_MEMORY_INDEX_FILENAME, AUTO_MEMORY_METADATA_FILENAME,
    AUTO_MEMORY_PINNED_DIRNAME, AutoMemoryPaths, MEMORY_PROJECT_SCOPES, MemoryPathInputs,
    MemoryProjectScope, TEAM_AUTO_MEMORY_DIRNAME, USER_AUTO_MEMORY_DIRNAME,
    clear_auto_memory_root_cache, get_auto_memory_topic_filename, is_inside_memory_root,
    resolve_memory_project_scope,
};
pub use pending_skills::{
    PendingSkill, PendingSkillError, accept_pending_skill, reject_pending_skill, stage_skill_dirs,
};
pub use prompt::{
    BuildMemoryPromptOptions, CONDENSED_DO_NOT_SAVE_SECTION, CONDENSED_TEAM_GUIDANCE,
    CONDENSED_TYPES_SECTION, CONDENSED_WHEN_TO_ACCESS_SECTION, MAX_MANAGED_AUTO_MEMORY_INDEX_BYTES,
    MAX_MANAGED_AUTO_MEMORY_INDEX_LINES, MEMORY_DRIFT_CAVEAT, MEMORY_FRONTMATTER_EXAMPLE,
    TRUSTING_RECALL_SECTION, TYPES_SECTION_INDIVIDUAL, TeamAutoMemorySection,
    UserAutoMemorySection, WHAT_NOT_TO_SAVE_SECTION, WHEN_TO_ACCESS_SECTION,
    build_managed_auto_memory_prompt, truncate_managed_auto_memory_index,
};
pub use recall::{
    AutoMemoryRecallResolveError, AutoMemoryRecallStrategy, AutoMemoryRecallTelemetry,
    AutoMemoryRecallTelemetryEvent, MAX_DOC_BODY_CODE_UNITS, MAX_RELEVANT_DOCS,
    RelevantAutoMemoryPromptResult, ResolveRelevantAutoMemoryPromptOptions,
    build_relevant_auto_memory_prompt, build_relevant_auto_memory_prompt_at, memory_age,
    memory_age_at, memory_age_days, memory_age_days_at, memory_freshness_note,
    memory_freshness_note_at, memory_freshness_text, memory_freshness_text_at,
    resolve_relevant_auto_memory_prompt_for_query, select_fallback_auto_memory_documents,
    select_relevant_auto_memory_documents, select_relevant_auto_memory_documents_default,
};
pub use refresh::{
    EDIT_TOOL_NAME, LEGACY_EDIT_TOOL_NAME, MemoryWriteCandidate, RefreshCallback, RefreshFuture,
    RefreshMemoryAfterWriteOptions, RefreshRuntimeCallbacks, RefreshWarningCallback,
    WRITE_FILE_TOOL_NAME, WrittenMemoryScope, candidate_file_path, canonical_write_tool_name,
    classify_written_memory_scope, did_write_managed_memory, did_write_project_context_file,
    is_successful_write, refresh_memory_after_managed_write, refresh_memory_instruction,
};
pub use relevance_selector::{
    AUTO_MEMORY_RECALL_SELECTION_TIMEOUT, AUTO_MEMORY_RECALL_SYSTEM_INSTRUCTION,
    AutoMemoryRecallContent, AutoMemoryRecallRequest, AutoMemoryRecallSelectionError,
    AutoMemoryRecallSelector, build_auto_memory_recall_request,
    select_relevant_auto_memory_documents_by_model,
};
pub use remember::{
    DEFAULT_REMEMBER_MAX_TURNS, DEFAULT_REMEMBER_TIMEOUT_MINUTES, MANAGED_REMEMBER_AGENT_NAME,
    ManagedRememberResult, REMEMBER_AGENT_TOOLS, RememberAgentFuture, RememberAgentRequest,
    RememberAgentRunResult, RememberAgentRuntime, RememberAgentStatus, RememberError,
    RememberHistoryPolicy, RememberScopedPaths, WorkspaceRememberContextMode,
    WorkspaceRememberScope, build_bare_remember_prompt, build_clean_memory_system_prompt,
    build_managed_remember_prompt, build_remember_system_prompt, classify_touched_scopes,
    run_managed_remember_by_agent,
};
pub use scan::{
    MAX_SCANNED_MEMORY_FILES, ScannedAutoMemoryDocument, parse_auto_memory_topic_document,
    scan_auto_memory_topic_documents, scan_team_auto_memory_topic_documents,
    scan_user_auto_memory_topic_documents,
};
pub use secret_scanner::{SecretMatch, scan_for_secrets};
pub use skill_review_agent_planner::{
    AUTO_SKILL_DIR_PREFIX, DEFAULT_AUTO_SKILL_MAX_TURNS, DEFAULT_AUTO_SKILL_TIMEOUT_MS,
    SKILL_REVIEW_AGENT_NAME, SKILL_REVIEW_AGENT_TOOLS, SKILL_REVIEW_SYSTEM_PROMPT,
    SkillReviewAgentFuture, SkillReviewAgentRequest, SkillReviewAgentRunResult,
    SkillReviewAgentRuntime, SkillReviewAgentStatus, SkillReviewBasePermissionManager,
    SkillReviewError, SkillReviewExecutionResult, SkillReviewOptions, SkillReviewPermissionContext,
    SkillScopedPermissionPolicy, build_agent_history, build_task_prompt, evaluate_scoped_decision,
    get_scoped_deny_rule, has_auto_skill_source, is_archived_skill_directory_reserved,
    list_archived_skill_dir_names, list_existing_skill_dir_names, run_skill_review_by_agent,
};
pub use status::{
    ManagedAutoMemoryStatus, ManagedAutoMemoryTopicStatus, ManagedMemoryTaskType, MemoryTaskRecord,
    MemoryTaskSource, MemoryTaskStatus, get_managed_auto_memory_status,
};
pub use store::{
    AUTO_MEMORY_SCHEMA_VERSION, AUTO_MEMORY_TYPES, AutoMemoryExtractCursor, AutoMemoryFileStats,
    AutoMemoryIndexRead, AutoMemoryMetadata, AutoMemorySourceRef, AutoMemoryStatus, AutoMemoryType,
    create_default_auto_memory_extract_cursor, create_default_auto_memory_index,
    create_default_auto_memory_metadata, ensure_auto_memory_scaffold,
    ensure_auto_memory_scaffold_at, ensure_user_auto_memory_scaffold, read_auto_memory_index,
    read_auto_memory_index_with_stats, read_user_auto_memory_index,
    read_user_auto_memory_index_with_stats,
};
pub use team_memory_git_status::{
    GitIgnoreProbe, ProcessGitIgnoreProbe, get_team_memory_shareability_warning,
    get_team_memory_shareability_warning_with_probe,
};
pub use team_memory_secret_guard::check_team_memory_secrets;
pub use team_memory_sync::{
    GitCommandRequest, GitCommandRunner, GitTerminationBehavior, ProcessGitCommandRunner,
    TeamMemoryGitAuthor, TeamMemorySyncEnvironment, TeamMemorySyncOptions, TeamMemorySyncResult,
    TeamMemorySyncSkippedReason, sync_team_memory, sync_team_memory_with,
};
pub use write_context_file::{
    ContextFileCommitGuard, FILE_LOCK_TIMEOUT_MS, MAX_EXISTING_FILE_BYTES, MEMORY_SECTION_HEADER,
    WorkspaceMemoryFileTooLargeError, WorkspaceMemoryWriteTimeoutError, WriteContextFileError,
    WriteContextFileMode, WriteContextFileOptions, WriteContextFileResult, WriteContextFileScope,
    write_context_file,
};
