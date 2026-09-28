//! Prompt construction for the managed auto-memory layers.
//!
//! Port of `packages/core/src/memory/prompt.ts`; index bounds use ECMAScript
//! UTF-16 code units, as JavaScript string `.length` and `.slice` do.

pub const MAX_MANAGED_AUTO_MEMORY_INDEX_LINES: usize = 200;
pub const MAX_MANAGED_AUTO_MEMORY_INDEX_BYTES: usize = 25_000;

const DIR_EXISTS_GUIDANCE: &str = "This directory already exists — write to it directly with the write_file tool (do not run mkdir or check for its existence).";
const NUMBER_WORDS: [&str; 5] = ["zero", "one", "two", "three", "four"];

pub const MEMORY_FRONTMATTER_EXAMPLE: &[&str] = &[
    "```markdown",
    "---",
    "name: {{memory name}}",
    "description: {{one-line description — used to decide relevance in future conversations, so be specific}}",
    "type: {{user, feedback, project, reference}}",
    "---",
    "",
    "{{memory content — for feedback/project types, structure as: rule/fact, then **Why:** and **How to apply:** lines}}",
    "```",
];

pub const TYPES_SECTION_INDIVIDUAL: &[&str] = &[
    "## Types of memory",
    "",
    "There are several discrete types of memory that you can store in your memory system. Each type carries a `<scope>` that decides which memory directory it belongs to when both a user (cross-project) and a project (this-project-only) directory are available:",
    "",
    "<types>",
    "<type>",
    "    <name>user</name>",
    "    <scope>always user (cross-project)</scope>",
    "    <description>Contain information about the user's role, goals, responsibilities, and knowledge. Great user memories help you tailor your future behavior to the user's preferences and perspective. Your goal in reading and writing these memories is to build up an understanding of who the user is and how you can be most helpful to them specifically. For example, you should collaborate with a senior software engineer differently than a student who is coding for the very first time. Keep in mind, that the aim here is to be helpful to the user. Avoid writing memories about the user that could be viewed as a negative judgement or that are not relevant to the work you're trying to accomplish together.</description>",
    "    <when_to_save>When you learn any details about the user's role, preferences, responsibilities, or knowledge</when_to_save>",
    "    <how_to_use>When your work should be informed by the user's profile or perspective. For example, if the user is asking you to explain a part of the code, you should answer that question in a way that is tailored to the specific details that they will find most valuable or that helps them build their mental model in relation to domain knowledge they already have.</how_to_use>",
    "    <examples>",
    "    user: I'm a data scientist investigating what logging we have in place",
    "    assistant: [saves user memory: user is a data scientist, currently focused on observability/logging]",
    "",
    "    user: I've been writing Go for ten years but this is my first time touching the React side of this repo",
    "    assistant: [saves user memory: deep Go expertise, new to React and this project's frontend — frame frontend explanations in terms of backend analogues]",
    "    </examples>",
    "</type>",
    "<type>",
    "    <name>feedback</name>",
    "    <scope>default user; save under project ONLY when the guidance is clearly a project-wide convention every contributor must follow (e.g., a testing policy, a build invariant), not a personal style preference.</scope>",
    "    <description>Guidance the user has given you about how to approach work — both what to avoid and what to keep doing. These are a very important type of memory to read and write as they allow you to remain coherent and responsive to the way you should approach work in the project. Record from failure AND success: if you only save corrections, you will avoid past mistakes but drift away from approaches the user has already validated, and may grow overly cautious.</description>",
    "    <when_to_save>Any time the user corrects your approach (\"no not that\", \"don't\", \"stop doing X\") OR confirms a non-obvious approach worked (\"yes exactly\", \"perfect, keep doing that\", accepting an unusual choice without pushback). Corrections are easy to notice; confirmations are quieter — watch for them. In both cases, save what is applicable to future conversations, especially if surprising or not obvious from the code. Include *why* so you can judge edge cases later.</when_to_save>",
    "    <how_to_use>Let these memories guide your behavior so that the user does not need to offer the same guidance twice.</how_to_use>",
    "    <body_structure>Lead with the rule itself, then a **Why:** line (the reason the user gave — often a past incident or strong preference) and a **How to apply:** line (when/where this guidance kicks in). Knowing *why* lets you judge edge cases instead of blindly following the rule.</body_structure>",
    "    <examples>",
    "    user: don't mock the database in these tests — we got burned last quarter when mocked tests passed but the prod migration failed",
    "    assistant: [saves feedback memory: integration tests must hit a real database, not mocks. Reason: prior incident where mock/prod divergence masked a broken migration]",
    "",
    "    user: stop summarizing what you just did at the end of every response, I can read the diff",
    "    assistant: [saves feedback memory: this user wants terse responses with no trailing summaries]",
    "",
    "    user: yeah the single bundled PR was the right call here, splitting this one would've just been churn",
    "    assistant: [saves feedback memory: for refactors in this area, user prefers one bundled PR over many small ones. Confirmed after I chose this approach — a validated judgment call, not a correction]",
    "    </examples>",
    "</type>",
    "<type>",
    "    <name>project</name>",
    "    <scope>always project (this-project-only)</scope>",
    "    <description>Information that you learn about ongoing work, goals, initiatives, bugs, or incidents within the project that is not otherwise derivable from the code or git history. Project memories help you understand the broader context and motivation behind the work the user is doing within this working directory.</description>",
    "    <when_to_save>When you learn who is doing what, why, or by when. These states change relatively quickly so try to keep your understanding of this up to date. Always convert relative dates in user messages to absolute dates when saving (e.g., \"Thursday\" → \"2026-03-05\"), so the memory remains interpretable after time passes.</when_to_save>",
    "    <how_to_use>Use these memories to more fully understand the details and nuance behind the user's request and make better informed suggestions.</how_to_use>",
    "    <body_structure>Lead with the fact or decision, then a **Why:** line (the motivation — often a constraint, deadline, or stakeholder ask) and a **How to apply:** line (how this should shape your suggestions). Project memories decay fast, so the why helps future-you judge whether the memory is still load-bearing.</body_structure>",
    "    <examples>",
    "    user: we're freezing all non-critical merges after Thursday — mobile team is cutting a release branch",
    "    assistant: [saves project memory: merge freeze begins 2026-03-05 for mobile release cut. Flag any non-critical PR work scheduled after that date]",
    "",
    "    user: the reason we're ripping out the old auth middleware is that legal flagged it for storing session tokens in a way that doesn't meet the new compliance requirements",
    "    assistant: [saves project memory: auth middleware rewrite is driven by legal/compliance requirements around session token storage, not tech-debt cleanup — scope decisions should favor compliance over ergonomics]",
    "    </examples>",
    "</type>",
    "<type>",
    "    <name>reference</name>",
    "    <scope>default project (this project's Linear, Slack channel, Grafana board, etc.); save under user when the resource is user-scoped rather than project-scoped (e.g., the company-wide wiki the user always consults).</scope>",
    "    <description>Stores pointers to where information can be found in external systems. These memories allow you to remember where to look to find up-to-date information outside of the project directory.</description>",
    "    <when_to_save>When you learn about resources in external systems and their purpose. For example, that bugs are tracked in a specific project in Linear or that feedback can be found in a specific Slack channel.</when_to_save>",
    "    <how_to_use>When the user references an external system or information that may be in an external system.</how_to_use>",
    "    <examples>",
    "    user: check the Linear project \"INGEST\" if you want context on these tickets, that's where we track all pipeline bugs",
    "    assistant: [saves reference memory: pipeline bugs are tracked in Linear project \"INGEST\"]",
    "",
    "    user: the Grafana board at grafana.internal/d/api-latency is what oncall watches — if you're touching request handling, that's the thing that'll page someone",
    "    assistant: [saves reference memory: grafana.internal/d/api-latency is the oncall latency dashboard — check it when editing request-path code]",
    "    </examples>",
    "</type>",
    "</types>",
    "",
];

pub const WHAT_NOT_TO_SAVE_SECTION: &[&str] = &[
    "## What NOT to save in memory",
    "",
    "- Code patterns, conventions, architecture, file paths, or project structure — these can be derived by reading the current project state.",
    "- Git history, recent changes, or who-changed-what — `git log` / `git blame` are authoritative.",
    "- Debugging solutions or fix recipes — the fix is in the code; the commit message has the context.",
    "- MCP tool names, parameter schemas, field mappings, guessed tool-call formats, or raw failed tool-call transcripts — live tool definitions are authoritative and may change. Save a tool-related note only when it captures a confirmed durable workaround, warning, owner, or escalation path.",
    "- Anything already documented in CANOPY.md or AGENTS.md files.",
    "- Ephemeral task details: in-progress work, temporary state, current conversation context.",
    "",
    "These exclusions apply even when the user explicitly asks you to save. If they ask you to save a PR list or activity summary, ask what was *surprising* or *non-obvious* about it — that is the part worth keeping.",
];

pub const MEMORY_DRIFT_CAVEAT: &str = "- Memory records can become stale over time. Use memory as context for what was true at a given point in time. Before answering the user or building assumptions based solely on information in memory records, verify that the memory is still correct and up-to-date by reading the current state of the files or resources. If a recalled memory conflicts with current information, trust what you observe now — and update or remove the stale memory rather than acting on it.";

pub const WHEN_TO_ACCESS_SECTION: &[&str] = &[
    "## When to access memories",
    "- When memories seem relevant, or the user references prior-conversation work.",
    "- You MUST access memory when the user explicitly asks you to check, recall, or remember.",
    "- If the user says to *ignore* or *not use* memory: proceed as if MEMORY.md were empty. Do not apply remembered facts, cite, compare against, or mention memory content.",
    "- Memory records can become stale over time. Use memory as context for what was true at a given point in time. Before answering the user or building assumptions based solely on information in memory records, verify that the memory is still correct and up-to-date by reading the current state of the files or resources. If a recalled memory conflicts with current information, trust what you observe now — and update or remove the stale memory rather than acting on it.",
];

pub const CONDENSED_WHEN_TO_ACCESS_SECTION: &[&str] = &[
    "## Accessing memories",
    "",
    "- Access memory when relevant or when user references prior-conversation work.",
    "- You MUST access memory when the user explicitly asks you to check, recall, or remember.",
    "- If the user says to ignore memory, proceed as if empty.",
    "- Memory records can become stale. If a recalled memory conflicts with current information, trust what you observe now — and update or remove the stale memory rather than acting on it.",
    "- Before recommending a memory that names a file, function, or flag, verify it still exists in the current code.",
];

pub const CONDENSED_DO_NOT_SAVE_SECTION: &[&str] = &[
    "## Do not save",
    "",
    "- Code patterns, conventions, architecture, file paths, or project structure (read the project instead)",
    "- Git history, recent changes, or who-changed-what",
    "- Debugging solutions or fix recipes (the fix is in the code; the commit message has context)",
    "- MCP tool names, schemas, field mappings, guessed tool-call formats, or failed call transcripts (save only confirmed durable workarounds, warnings, owner, or escalation path)",
    "- Ephemeral task state or current conversation context",
    "- Content already in CANOPY.md or AGENTS.md",
    "",
    "These exclusions apply even when the user explicitly asks you to save.",
    "If the user asks you to save a PR list or activity summary, ask what was *surprising* or *non-obvious* about it — that is the part worth keeping.",
];

pub const CONDENSED_TYPES_SECTION: &[&str] = &[
    "## Memory types",
    "",
    "- **user** — the user's role, goals, responsibilities, and knowledge (always user-scoped). Avoid writing memories that could be viewed as a negative judgement.",
    "- **feedback** — guidance on how to approach work: corrections AND confirmed approaches. Record from both failure and success — if you only save corrections, you drift from validated approaches (default user; project only for project-wide conventions).",
    "- **project** — ongoing work, goals, initiatives, bugs, or incidents not derivable from code/git (always project-scoped). Always convert relative dates to absolute dates when saving. Include *why* — project memories decay fast, so the why helps assess staleness.",
    "- **reference** — pointers to where information lives in external systems (default project; user when the resource is personal).",
];

pub const TRUSTING_RECALL_SECTION: &[&str] = &[
    "## Before recommending from memory",
    "",
    "A memory that names a specific function, file, or flag is a claim that it existed when the memory was written. It may have been renamed, removed, or never merged. Before recommending it:",
    "",
    "- If the memory names a file path: check the file exists.",
    "- If the memory names a function or flag: grep for it.",
    "- If the user is about to act on your recommendation (not just asking about history), verify first.",
    "",
    "\"The memory says X exists\" is not the same as \"X exists now.\"",
    "",
    "A memory that summarizes repo state (activity logs, architecture snapshots) is frozen in time. If the user asks about *recent* or *current* state, prefer `git log` or reading the code over recalling the snapshot.",
];

pub const CONDENSED_TEAM_GUIDANCE: &[&str] = &[
    "When a team directory is available, route project-wide conventions and shared references to TEAM instead of PROJECT. You MUST NOT save sensitive data to TEAM memory — never API keys, tokens, or credentials; it is visible to everyone who can read the repository. `user` memories are always private — never save them to TEAM. For TEAM memory, only write the file (Step 1) — its index is auto-generated; do NOT hand-edit the team `MEMORY.md`.",
];

const MAX_MANAGED_AUTO_MEMORY_INDEX_UNITS: usize = MAX_MANAGED_AUTO_MEMORY_INDEX_BYTES;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UserAutoMemorySection<'a> {
    pub memory_dir: &'a str,
    pub index_content: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TeamAutoMemorySection<'a> {
    pub memory_dir: &'a str,
    pub index_content: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BuildMemoryPromptOptions {
    pub force_full_protocol: bool,
}

/// Truncate the loaded project/user/team MEMORY.md index using JavaScript's
/// string-length and slice units. The limit is named `BYTES` in the source,
/// but the source measures UTF-16 code units, not encoded UTF-8 bytes.
pub fn truncate_managed_auto_memory_index(index_content: &str) -> String {
    let trimmed = trim_js_whitespace(index_content);
    let lines = trimmed.split('\n').collect::<Vec<_>>();
    let line_count = lines.len();
    let code_units = js_utf16_len(trimmed);
    let was_line_truncated = line_count > MAX_MANAGED_AUTO_MEMORY_INDEX_LINES;
    let was_unit_truncated = code_units > MAX_MANAGED_AUTO_MEMORY_INDEX_UNITS;

    if !was_line_truncated && !was_unit_truncated {
        return trimmed.to_owned();
    }

    let mut truncated = if was_line_truncated {
        lines[..MAX_MANAGED_AUTO_MEMORY_INDEX_LINES].join("\n")
    } else {
        trimmed.to_owned()
    };

    if js_utf16_len(&truncated) > MAX_MANAGED_AUTO_MEMORY_INDEX_UNITS {
        let cut_at =
            last_index_of_newline_at_or_before(&truncated, MAX_MANAGED_AUTO_MEMORY_INDEX_UNITS);
        let end = cut_at
            .filter(|index| *index > 0)
            .unwrap_or(MAX_MANAGED_AUTO_MEMORY_INDEX_UNITS);
        truncated = slice_utf16(&truncated, end);
    }

    let reason = if was_unit_truncated && !was_line_truncated {
        format!(
            "{} KB (limit: {} KB) — index entries are too long",
            js_to_fixed_one(code_units as f64 / 1024.0),
            js_to_fixed_one(MAX_MANAGED_AUTO_MEMORY_INDEX_UNITS as f64 / 1024.0),
        )
    } else if was_line_truncated && !was_unit_truncated {
        format!("{line_count} lines (limit: {MAX_MANAGED_AUTO_MEMORY_INDEX_LINES})")
    } else {
        format!(
            "{line_count} lines and {} KB",
            js_to_fixed_one(code_units as f64 / 1024.0),
        )
    };

    format!(
        "{truncated}\n\n> WARNING: MEMORY.md is {reason}. Only part of it was loaded. Keep index entries to one line under ~200 chars; move detail into topic files."
    )
}

/// Build the full or condensed prompt for the available private and team
/// memory layers. Set `force_full_protocol` to retain the verbose guidance
/// even while all indexes are empty.
pub fn build_managed_auto_memory_prompt(
    memory_dir: &str,
    index_content: Option<&str>,
    user_section: Option<&UserAutoMemorySection<'_>>,
    team_section: Option<&TeamAutoMemorySection<'_>>,
    options: BuildMemoryPromptOptions,
) -> String {
    let mut tier_lines = Vec::new();
    if let Some(user) = user_section {
        tier_lines.push(format!(
            "- USER memory (cross-project, durable knowledge about who the user is): `{}`",
            user.memory_dir
        ));
    }
    tier_lines.push(format!(
        "- PROJECT memory (this project only, private to you): `{memory_dir}`"
    ));
    if let Some(team) = team_section {
        tier_lines.push(format!(
            "- TEAM memory (this project, shared with every collaborator through the repository — just save the file; the repo's normal git workflow carries it to teammates, so don't run git yourself): `{}`",
            team.memory_dir
        ));
    }
    let multi_tier = tier_lines.len() > 1;

    if all_indexes_empty(index_content, user_section, team_section) && !options.force_full_protocol
    {
        let condensed_intro = if multi_tier {
            let tier_count = NUMBER_WORDS
                .get(tier_lines.len())
                .map(|word| (*word).to_owned())
                .unwrap_or_else(|| tier_lines.len().to_string());
            let mut lines = vec![
                format!(
                    "You have {tier_count} persistent, file-based memory directories. {DIR_EXISTS_GUIDANCE}"
                ),
                String::new(),
            ];
            lines.extend(tier_lines.iter().cloned());
            lines
        } else {
            vec![format!(
                "You have a persistent, file-based memory system at `{memory_dir}`. {DIR_EXISTS_GUIDANCE}"
            )]
        };

        let condensed_maintenance_bullets = [
            String::new(),
            "- Keep the name, description, and type fields in memory files up-to-date with the content.".to_owned(),
            "- Organize memories semantically by topic, not chronologically.".to_owned(),
            "- Update or remove memories that turn out to be wrong or outdated.".to_owned(),
            format!("- Every `MEMORY.md` index is always loaded into your conversation context — lines after {MAX_MANAGED_AUTO_MEMORY_INDEX_LINES} will be truncated, so keep each index concise."),
        ];

        let condensed_save = if multi_tier {
            let mut lines = vec![
                "## How to save memories".to_owned(),
                String::new(),
                "Two-step process:".to_owned(),
                String::new(),
                "**Step 1** — write the memory to its own file (e.g., `user/role.md`, `feedback/testing.md`) inside the directory chosen by its type scope, using this frontmatter format:".to_owned(),
                String::new(),
            ];
            extend_lines(&mut lines, MEMORY_FRONTMATTER_EXAMPLE);
            lines.extend([
                String::new(),
                "**Step 2** — add a pointer to that file in the `MEMORY.md` index that lives in the SAME directory you wrote to (each directory has its own index — never cross-reference). Each entry: one line, under ~150 chars: `- [Title](file.md) — one-line hook`.".to_owned(),
                "- Never write memory content directly into `MEMORY.md` — it is an index of one-line pointers, not a memory file.".to_owned(),
                "- Do not write duplicate memories. First check if there is an existing memory in any of your memory directories you can update before writing a new one.".to_owned(),
            ]);
            lines.extend(condensed_maintenance_bullets.iter().cloned());
            if team_section.is_some() {
                lines.push(String::new());
                extend_lines(&mut lines, CONDENSED_TEAM_GUIDANCE);
            }
            lines
        } else {
            let mut lines = vec![
                "## How to save memories".to_owned(),
                String::new(),
                "Two-step process:".to_owned(),
                String::new(),
                "**Step 1** — write the memory to its own file (e.g., `user/role.md`, `feedback/testing.md`) using this frontmatter format:".to_owned(),
                String::new(),
            ];
            extend_lines(&mut lines, MEMORY_FRONTMATTER_EXAMPLE);
            lines.extend([
                String::new(),
                format!("**Step 2** — add a pointer to that file in `{memory_dir}/MEMORY.md`. Each entry: one line, under ~150 chars: `- [Title](file.md) — one-line hook`."),
                "- Never write memory content directly into `MEMORY.md` — it is an index of one-line pointers, not a memory file. Do not write duplicate memories.".to_owned(),
            ]);
            lines.extend(condensed_maintenance_bullets.iter().cloned());
            lines
        };

        let index_sections =
            build_index_sections(memory_dir, index_content, user_section, team_section);
        let mut lines = vec!["# auto memory".to_owned(), String::new()];
        lines.extend(condensed_intro);
        lines.extend([
            String::new(),
            "Your memory is currently empty. When you learn something worth remembering across conversations, save it using the process below.".to_owned(),
            "If the user explicitly asks you to remember something, save it immediately as whichever type fits best. If they ask you to forget something, find and remove the relevant entry.".to_owned(),
            String::new(),
        ]);
        extend_lines(&mut lines, CONDENSED_TYPES_SECTION);
        lines.push(String::new());
        extend_lines(&mut lines, CONDENSED_DO_NOT_SAVE_SECTION);
        lines.push(String::new());
        extend_lines(&mut lines, CONDENSED_WHEN_TO_ACCESS_SECTION);
        lines.push(String::new());
        lines.extend(condensed_save);
        lines.extend([
            String::new(),
            "- Use plans and tasks for in-conversation work; reserve memory for durable cross-conversation knowledge.".to_owned(),
            String::new(),
        ]);
        lines.extend(index_sections);
        return lines.join("\n");
    }

    let intro = if multi_tier {
        let tier_count = NUMBER_WORDS
            .get(tier_lines.len())
            .map(|word| (*word).to_owned())
            .unwrap_or_else(|| tier_lines.len().to_string());
        let mut lines = vec![
            format!(
                "You have {tier_count} persistent, file-based memory directories. {DIR_EXISTS_GUIDANCE}"
            ),
            String::new(),
        ];
        lines.extend(tier_lines.iter().cloned());
        lines.extend([
            String::new(),
            "For every memory you save, decide which directory it belongs in using the per-type `<scope>` guidance below.".to_owned(),
        ]);
        lines
    } else {
        vec![format!(
            "You have a persistent, file-based memory system at `{memory_dir}`. {DIR_EXISTS_GUIDANCE}"
        )]
    };

    let how_to_save = if multi_tier {
        let mut lines = vec![
            "## How to save memories".to_owned(),
            String::new(),
            "Saving a memory is a two-step process:".to_owned(),
            String::new(),
            "**Step 1** — write the memory to its own file inside the directory chosen by its `<scope>`, organising it under the matching type subdirectory (e.g., `user/role.md`, `feedback/testing.md`) using this frontmatter format:".to_owned(),
            String::new(),
        ];
        extend_lines(&mut lines, MEMORY_FRONTMATTER_EXAMPLE);
        lines.extend([
            String::new(),
            "**Step 2** — add a pointer to that file in the `MEMORY.md` index that lives in the SAME directory you wrote to (each directory has its own index — never cross-reference). Each entry should be one line, under ~150 characters: `- [Title](file.md) — one-line hook`. It has no frontmatter. Never write memory content directly into `MEMORY.md`.".to_owned(),
            String::new(),
            format!("- Every `MEMORY.md` index is always loaded into your conversation context — lines after {MAX_MANAGED_AUTO_MEMORY_INDEX_LINES} will be truncated, so keep each index concise"),
            "- Keep the name, description, and type fields in memory files up-to-date with the content".to_owned(),
            "- Organize memory semantically by topic, not chronologically.".to_owned(),
            "- Update or remove memories that turn out to be wrong or outdated.".to_owned(),
            "- Do not write duplicate memories. First check if there is an existing memory in any of your memory directories you can update before writing a new one.".to_owned(),
        ]);
        lines
    } else {
        let mut lines = vec![
            "## How to save memories".to_owned(),
            String::new(),
            "Saving a memory is a two-step process:".to_owned(),
            String::new(),
            "**Step 1** — write the memory to its own file under the matching type subdirectory (e.g., `user/role.md`, `feedback/testing.md`) using this frontmatter format:".to_owned(),
            String::new(),
        ];
        extend_lines(&mut lines, MEMORY_FRONTMATTER_EXAMPLE);
        lines.extend([
            String::new(),
            format!("**Step 2** — add a pointer to that file in `{memory_dir}/MEMORY.md` (the full absolute path). This index file is an index, not a memory — each entry should be one line, under ~150 characters: `- [Title](file.md) — one-line hook`. It has no frontmatter. Never write memory content directly into `{memory_dir}/MEMORY.md`."),
            String::new(),
            format!("- `{memory_dir}/MEMORY.md` is always loaded into your conversation context — lines after {MAX_MANAGED_AUTO_MEMORY_INDEX_LINES} will be truncated, so keep the index concise"),
            "- Keep the name, description, and type fields in memory files up-to-date with the content".to_owned(),
            "- Organize memory semantically by topic, not chronologically.".to_owned(),
            "- Update or remove memories that turn out to be wrong or outdated.".to_owned(),
            "- Do not write duplicate memories. First check if there is an existing memory you can update before writing a new one.".to_owned(),
        ]);
        lines
    };

    let index_sections =
        build_index_sections(memory_dir, index_content, user_section, team_section);
    let mut lines = vec!["# auto memory".to_owned(), String::new()];
    lines.extend(intro);
    lines.extend([
        String::new(),
        "You should build up this memory system over time so that future conversations can have a complete picture of who the user is, how they'd like to collaborate with you, what behaviors to avoid or repeat, and the context behind the work the user gives you.".to_owned(),
        String::new(),
        "If the user explicitly asks you to remember something, save it immediately as whichever type fits best. If they ask you to forget something, find and remove the relevant entry.".to_owned(),
        String::new(),
    ]);
    extend_lines(&mut lines, TYPES_SECTION_INDIVIDUAL);
    if team_section.is_some() {
        extend_lines(&mut lines, &team_scope_section());
    }
    extend_lines(&mut lines, WHAT_NOT_TO_SAVE_SECTION);
    lines.push(String::new());
    lines.extend(how_to_save);
    lines.push(String::new());
    extend_lines(&mut lines, WHEN_TO_ACCESS_SECTION);
    lines.push(String::new());
    extend_lines(&mut lines, TRUSTING_RECALL_SECTION);
    lines.extend([
        String::new(),
        "## Memory and other forms of persistence".to_owned(),
        "Memory is one of several persistence mechanisms available to you as you assist the user in a given conversation. The distinction is often that memory can be recalled in future conversations and should not be used for persisting information that is only useful within the scope of the current conversation.".to_owned(),
        "- When to use or update a plan instead of memory: If you are about to start a non-trivial implementation task and would like to reach alignment with the user on your approach you should use a Plan rather than saving this information to memory. Similarly, if you already have a plan within the conversation and you have changed your approach persist that change by updating the plan rather than saving a memory.".to_owned(),
        "- When to use or update tasks instead of memory: When you need to break your work in current conversation into discrete steps or keep track of your progress use tasks instead of saving to memory. Tasks are great for persisting information about the work that needs to be done in the current conversation, but memory should be reserved for information that will be useful in future conversations.".to_owned(),
        String::new(),
    ]);
    lines.extend(index_sections);
    lines.join("\n")
}

fn team_scope_section() -> Vec<&'static str> {
    [
        "## Saving to team memory",
        "",
        "TEAM memory is shared with every collaborator through the repository, so it refines the `<scope>` guidance above:",
        "",
        "- A `feedback` memory that is a project-wide convention every contributor must follow (a testing policy, a build invariant) → save to TEAM instead of the project directory.",
        "- A `reference` pointer the whole team relies on (issue tracker, dashboard, channel) → save to TEAM instead of the project directory.",
        "- `user` memories are always private — never save them to TEAM.",
        "- `project` memories stay private to you by default. Save to TEAM only for durable shared facts every contributor needs — not time-bound state like freezes or in-flight task status, which stays private and decays.",
        "- You MUST NOT save sensitive data to TEAM memory — never API keys, tokens, or credentials. It is visible to everyone who can read the repository, and such writes are rejected automatically.",
        "- For TEAM memory you only write the memory file (Step 1). Its `MEMORY.md` index is generated automatically from the saved files — do NOT hand-edit the team index (that two-step rule applies only to the private directories).",
        "",
    ]
    .into()
}

fn render_index_block(memory_dir: &str, index_content: Option<&str>) -> Vec<String> {
    let trimmed = index_content
        .map(trim_js_whitespace)
        .filter(|value| !value.is_empty());
    vec![
        format!("## {memory_dir}/MEMORY.md"),
        String::new(),
        trimmed
            .map(truncate_managed_auto_memory_index)
            .unwrap_or_else(|| "Your MEMORY.md is currently empty. When you save new memories, they will appear here.".to_owned()),
    ]
}

fn build_index_sections(
    memory_dir: &str,
    index_content: Option<&str>,
    user_section: Option<&UserAutoMemorySection<'_>>,
    team_section: Option<&TeamAutoMemorySection<'_>>,
) -> Vec<String> {
    let mut sections = Vec::new();
    if let Some(user) = user_section {
        sections.extend(render_index_block(user.memory_dir, user.index_content));
        sections.push(String::new());
    }
    sections.extend(render_index_block(memory_dir, index_content));
    if let Some(team) = team_section {
        sections.push(String::new());
        sections.extend(render_index_block(team.memory_dir, team.index_content));
    }
    sections
}

fn all_indexes_empty(
    index_content: Option<&str>,
    user_section: Option<&UserAutoMemorySection<'_>>,
    team_section: Option<&TeamAutoMemorySection<'_>>,
) -> bool {
    let is_empty =
        |content: Option<&str>| content.map(trim_js_whitespace).is_none_or(str::is_empty);
    is_empty(index_content)
        && user_section.is_none_or(|section| is_empty(section.index_content))
        && team_section.is_none_or(|section| is_empty(section.index_content))
}

fn extend_lines(target: &mut Vec<String>, lines: &[&str]) {
    target.extend(lines.iter().map(|line| (*line).to_owned()));
}

fn trim_js_whitespace(text: &str) -> &str {
    let start = text
        .char_indices()
        .find(|(_, character)| !is_ecmascript_whitespace(*character))
        .map(|(index, _)| index)
        .unwrap_or(text.len());
    let end = text
        .char_indices()
        .rev()
        .find(|(_, character)| !is_ecmascript_whitespace(*character))
        .map(|(index, character)| index + character.len_utf8())
        .unwrap_or(start);
    &text[start..end]
}

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000a}'
            | '\u{000b}'
            | '\u{000c}'
            | '\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

fn js_utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn last_index_of_newline_at_or_before(text: &str, max_index: usize) -> Option<usize> {
    text.encode_utf16()
        .enumerate()
        .take(max_index.saturating_add(1))
        .filter_map(|(index, unit)| (unit == b'\n' as u16).then_some(index))
        .last()
}

/// Slice in UTF-16 code units. Rust `String` cannot retain an unpaired
/// surrogate, so a boundary that splits a pair is represented as U+FFFD.
fn slice_utf16(text: &str, end: usize) -> String {
    let units = text.encode_utf16().take(end).collect::<Vec<_>>();
    String::from_utf16_lossy(&units)
}

/// Match ECMAScript `Number.prototype.toFixed(1)` for the nonnegative values
/// used in memory-index size warnings. The values here are integer code-unit
/// counts divided by 1024, so multiplying by ten is exact in binary64.
fn js_to_fixed_one(value: f64) -> String {
    let scaled = value * 10.0;
    let floor = scaled.floor();
    let rounded = (if scaled - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }) as u64;
    format!("{}.{:01}", rounded / 10, rounded % 10)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_condensed_prompt_for_empty_indexes_and_full_prompt_when_requested() {
        let condensed = build_managed_auto_memory_prompt(
            "/tmp/project/.canopy/memory",
            None,
            None,
            None,
            BuildMemoryPromptOptions::default(),
        );
        assert!(
            condensed
                .starts_with("# auto memory\n\nYou have a persistent, file-based memory system")
        );
        assert!(condensed.contains("## Memory types"));
        assert!(condensed.contains("currently empty"));
        assert!(!condensed.contains("## What NOT to save in memory"));
        assert!(!condensed.contains("## Memory and other forms of persistence"));

        let full = build_managed_auto_memory_prompt(
            "/tmp/project/.canopy/memory",
            None,
            None,
            None,
            BuildMemoryPromptOptions {
                force_full_protocol: true,
            },
        );
        assert!(full.contains("## Types of memory"));
        assert!(full.contains("## What NOT to save in memory"));
        assert!(full.contains("## When to access memories"));
        assert!(full.contains("## Before recommending from memory"));
        assert!(full.contains("## Memory and other forms of persistence"));
    }

    #[test]
    fn renders_two_and_three_tier_sections_in_directory_order() {
        let user = UserAutoMemorySection {
            memory_dir: "/home/u/.canopy/memories",
            index_content: Some("- [Preference](user/short.md) — concise"),
        };
        let team = TeamAutoMemorySection {
            memory_dir: "/tmp/project/.canopy/team-memory",
            index_content: Some("- [Convention](feedback/tests.md) — shared"),
        };
        let prompt = build_managed_auto_memory_prompt(
            "/tmp/project/.canopy/memory",
            Some("- [Project](project/current.md) — underway"),
            Some(&user),
            Some(&team),
            BuildMemoryPromptOptions::default(),
        );
        assert!(prompt.contains("three persistent, file-based memory directories"));
        assert!(prompt.contains("## Saving to team memory"));
        assert!(prompt.contains("MUST NOT save sensitive data to TEAM memory"));
        let user_index = prompt
            .find("## /home/u/.canopy/memories/MEMORY.md")
            .unwrap();
        let project_index = prompt
            .find("## /tmp/project/.canopy/memory/MEMORY.md")
            .unwrap();
        let team_index = prompt
            .find("## /tmp/project/.canopy/team-memory/MEMORY.md")
            .unwrap();
        assert!(user_index < project_index && project_index < team_index);
        assert!(prompt.contains("[Preference](user/short.md)"));
        assert!(prompt.contains("[Project](project/current.md)"));
        assert!(prompt.contains("[Convention](feedback/tests.md)"));
    }

    #[test]
    fn truncation_uses_utf16_length_slice_and_newline_boundaries() {
        let oversized = format!("{}🦀", "a".repeat(24_999));
        let output = truncate_managed_auto_memory_index(&oversized);
        let content = output.split("\n\n> WARNING:").next().unwrap();
        assert_eq!(js_utf16_len(content), 25_000);
        assert!(content.ends_with('\u{fffd}'));
        assert!(output.contains("24.4 KB"));
        assert!(output.contains("limit: 24.4 KB"));

        let line_boundary = format!("{}\n{}", "a".repeat(24_998), "b".repeat(50));
        let output = truncate_managed_auto_memory_index(&line_boundary);
        let content = output.split("\n\n> WARNING:").next().unwrap();
        assert_eq!(content, "a".repeat(24_998));
    }

    #[test]
    fn line_limit_and_utf16_byte_count_match_source_warning_selection() {
        let lines = (0..250)
            .map(|index| format!("line {index}"))
            .collect::<Vec<_>>();
        let output = truncate_managed_auto_memory_index(&lines.join("\n"));
        assert!(output.contains("250 lines (limit: 200)"));
        assert_eq!(
            output
                .split("\n\n> WARNING:")
                .next()
                .unwrap()
                .split('\n')
                .count(),
            200
        );

        let both = (0..250)
            .map(|_| "x".repeat(150))
            .collect::<Vec<_>>()
            .join("\n");
        let output = truncate_managed_auto_memory_index(&both);
        assert!(output.contains("250 lines and 36.9 KB"));
        assert!(js_utf16_len(output.split("\n\n> WARNING:").next().unwrap()) <= 25_000);

        assert_eq!(js_to_fixed_one(25_344.0 / 1024.0), "24.8");
    }

    #[test]
    fn javascript_whitespace_controls_empty_index_selection_and_rendering() {
        let prompt = build_managed_auto_memory_prompt(
            "/tmp/project/.canopy/memory",
            Some("\u{feff}\u{00a0}"),
            None,
            None,
            BuildMemoryPromptOptions::default(),
        );
        assert!(prompt.contains("## Memory types"));
        assert!(prompt.contains("Your MEMORY.md is currently empty."));
    }
}
