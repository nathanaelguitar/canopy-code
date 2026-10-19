/**
 * Host-provided agent persona (for example CanopyChat's "Flame").
 *
 * Off unless the host asks for it (the `agentPersona` prop, or `?agent=<name>`
 * in the standalone shell). When on, assistant replies carry the agent's name
 * and avatar, the composer says "Message <name>…", and a status bubble at the
 * live end of the transcript says what the agent is doing right now.
 *
 * The status lines follow the persona's SOUL.md table: short, truthful, calm.
 */
import type { DaemonTranscriptBlock } from '@qwen-code/sdk/daemon';

export type AgentPersonaLanguage = 'en' | 'es';

export interface WebShellAgentPersona {
  /** Display name, already normalized by `normalizeAgentName`. */
  name: string;
  /** Language of the status lines and the composer copy. */
  language: AgentPersonaLanguage;
  /**
   * Base URL of the avatar loops, without a trailing slash. The host serves
   * `<base>/<mood>.webm` (VP9 with alpha) and `<base>/<mood>.png` (frame 0)
   * for every mood. Omitted: a monogram is shown instead.
   */
  avatarBaseUrl?: string;
}

export const DEFAULT_AGENT_NAME = 'Flame';
export const MAX_AGENT_NAME_LENGTH = 20;

/** Trimmed, single-line, at most 20 characters; blank falls back to "Flame". */
export function normalizeAgentName(raw: string | null | undefined): string {
  const collapsed = (raw ?? '').replace(/[\r\n]+/g, ' ').trim();
  if (!collapsed) return DEFAULT_AGENT_NAME;
  return Array.from(collapsed).slice(0, MAX_AGENT_NAME_LENGTH).join('').trim();
}

export function normalizeAgentLanguage(
  raw: string | null | undefined,
): AgentPersonaLanguage {
  return (raw ?? '').toLowerCase().startsWith('es') ? 'es' : 'en';
}

/** Which loop the avatar plays. Every loop has a poster (its frame 0). */
export type AgentMood = 'idle' | 'working' | 'thinking' | 'listening' | 'snag';

export type AgentActivity =
  | { kind: 'working' }
  | { kind: 'thinking' }
  | { kind: 'reading' }
  | { kind: 'searching' }
  | { kind: 'editing' }
  | { kind: 'runningTests' }
  | { kind: 'runningCommand' }
  | { kind: 'building' }
  | { kind: 'waiting'; tool: string }
  | { kind: 'listening' }
  | { kind: 'needsApproval' }
  | { kind: 'done' }
  | { kind: 'snag' };

export function moodFor(activity: AgentActivity | null | undefined): AgentMood {
  switch (activity?.kind) {
    case undefined:
    case 'needsApproval':
    case 'done':
      return 'idle';
    case 'thinking':
      return 'thinking';
    case 'listening':
      return 'listening';
    case 'snag':
      return 'snag';
    default:
      return 'working';
  }
}

const RUNNING_STATUSES = new Set(['', 'pending', 'in_progress', 'running']);

export function isRunningToolStatus(
  status: string | null | undefined,
): boolean {
  return RUNNING_STATUSES.has((status ?? '').toLowerCase());
}

/**
 * Maps a tool kind (or, when there is none, the title's first word) to a
 * status. A plain shell command is "running a command", never "running tests",
 * unless the title says tests: the status line has to be true.
 */
export function activityForTool(
  kind: string | null | undefined,
  title: string,
): AgentActivity {
  const lowerTitle = title.toLowerCase();
  const verb = lowerTitle.split(/[^a-z_]+/).find(Boolean) ?? '';
  let resolved = (kind ?? '').toLowerCase();
  if (!resolved || resolved === 'other') {
    switch (verb) {
      case 'read':
      case 'view':
      case 'open':
      case 'cat':
      case 'read_file':
      case 'readfile':
        resolved = 'read';
        break;
      case 'search':
      case 'grep':
      case 'glob':
      case 'find':
      case 'list':
      case 'ls':
      case 'list_directory':
        resolved = 'search';
        break;
      case 'edit':
      case 'write':
      case 'apply':
      case 'patch':
      case 'create':
      case 'delete':
      case 'move':
      case 'replace':
      case 'write_file':
      case 'apply_patch':
        resolved = 'edit';
        break;
      case 'run':
      case 'shell':
      case 'bash':
      case 'exec':
      case 'execute':
      case 'command':
      case 'run_shell_command':
        resolved = 'execute';
        break;
      case 'fetch':
      case 'web_fetch':
      case 'webfetch':
        resolved = 'fetch';
        break;
      case 'think':
      case 'thinking':
      case 'plan':
        resolved = 'think';
        break;
      default:
        resolved = 'other';
    }
  }
  switch (resolved) {
    case 'read':
      return { kind: 'reading' };
    case 'search':
      return { kind: 'searching' };
    case 'edit':
    case 'delete':
    case 'move':
      return { kind: 'editing' };
    case 'think':
      return { kind: 'thinking' };
    case 'execute':
      if (lowerTitle.includes('test')) return { kind: 'runningTests' };
      if (lowerTitle.includes('build') || lowerTitle.includes('compile')) {
        return { kind: 'building' };
      }
      return { kind: 'runningCommand' };
    case 'fetch':
      return { kind: 'waiting', tool: title.split(' ')[0] || 'fetch' };
    default:
      return { kind: 'working' };
  }
}

/** Blocks that say nothing about what the agent is doing. */
function isBookkeeping(block: DaemonTranscriptBlock): boolean {
  return block.kind === 'debug' || block.kind === 'status';
}

/**
 * What the agent is doing right now, from the transcript. A pending approval
 * wins; then a running tool; then an open thought. Null means at rest.
 */
export function currentAgentActivity(
  blocks: readonly DaemonTranscriptBlock[],
  { isWorking, needsApproval }: { isWorking: boolean; needsApproval: boolean },
): AgentActivity | null {
  if (needsApproval) return { kind: 'needsApproval' };
  let last: DaemonTranscriptBlock | undefined;
  for (let i = blocks.length - 1; i >= 0; i--) {
    if (!isBookkeeping(blocks[i]!)) {
      last = blocks[i];
      break;
    }
  }
  if (!isWorking) {
    return last?.kind === 'error' ? { kind: 'snag' } : null;
  }
  for (let i = blocks.length - 1; i >= 0; i--) {
    const block = blocks[i]!;
    if (block.kind === 'user') break; // only the current turn
    if (block.kind === 'tool' && isRunningToolStatus(block.status)) {
      return activityForTool(block.toolKind, block.title);
    }
  }
  if (last?.kind === 'thought') return { kind: 'thinking' };
  return { kind: 'working' };
}

/** The file a running tool works on, for the bubble ("Read src/App.tsx" → "App.tsx"). */
export function currentToolSubject(
  blocks: readonly DaemonTranscriptBlock[],
): string | undefined {
  for (let i = blocks.length - 1; i >= 0; i--) {
    const block = blocks[i]!;
    if (block.kind === 'user') return undefined;
    if (block.kind === 'tool' && isRunningToolStatus(block.status)) {
      return subjectFromToolTitle(block.title);
    }
  }
  return undefined;
}

export function subjectFromToolTitle(title: string): string | undefined {
  const tokens = title.split(/\s+/).filter(Boolean).reverse();
  for (const token of tokens) {
    const cleaned = token.replace(/^[`'"(),:;]+|[`'"(),:;]+$/g, '');
    const looksLikePath =
      cleaned.includes('/') ||
      (cleaned.includes('.') &&
        !cleaned.endsWith('.') &&
        !cleaned.startsWith('.'));
    if (!looksLikePath || !/[A-Za-z]/.test(cleaned)) continue;
    const last = cleaned.split('/').filter(Boolean).pop();
    return last || undefined;
  }
  return undefined;
}

/** The words after the name ("is reading the code…"). Empty for "Done.". */
export function statusPredicate(
  activity: AgentActivity,
  language: AgentPersonaLanguage,
): string {
  if (language === 'es') {
    switch (activity.kind) {
      case 'working':
        return 'está trabajando…';
      case 'thinking':
        return 'está pensando…';
      case 'reading':
        return 'está leyendo el código…';
      case 'searching':
        return 'está buscando…';
      case 'editing':
        return 'está editando…';
      case 'runningTests':
        return 'está corriendo las pruebas…';
      case 'runningCommand':
        return 'está ejecutando un comando…';
      case 'building':
        return 'está compilando…';
      case 'waiting':
        return `espera a ${activity.tool}…`;
      case 'listening':
        return 'está escuchando…';
      case 'needsApproval':
        return 'necesita su visto bueno';
      case 'done':
        return '';
      case 'snag':
        return 'se topó con un problema';
    }
  }
  switch (activity.kind) {
    case 'working':
      return 'is working…';
    case 'thinking':
      return 'is thinking…';
    case 'reading':
      return 'is reading the code…';
    case 'searching':
      return 'is looking around…';
    case 'editing':
      return 'is editing files…';
    case 'runningTests':
      return 'is running tests…';
    case 'runningCommand':
      return 'is running a command…';
    case 'building':
      return 'is building…';
    case 'waiting':
      return `is waiting on ${activity.tool}…`;
    case 'listening':
      return 'is listening…';
    case 'needsApproval':
      return 'needs your OK';
    case 'done':
      return '';
    case 'snag':
      return 'hit a snag';
  }
}

export interface AgentStatusLine {
  activity: AgentActivity;
  mood: AgentMood;
  /** "Flame is reading the code…", "Done.". */
  sentence: string;
  /** Without the name, for a row under the name: "Reading the code…". */
  short: string;
  /** "is reading the code…" (empty for Done). */
  predicate: string;
}

export function agentStatusLine(
  name: string,
  activity: AgentActivity,
  language: AgentPersonaLanguage,
): AgentStatusLine {
  const predicate = statusPredicate(activity, language);
  const mood = moodFor(activity);
  if (activity.kind === 'done') {
    return {
      activity,
      mood,
      predicate,
      sentence: language === 'es' ? 'Listo.' : 'Done.',
      short: language === 'es' ? 'Listo' : 'Done',
    };
  }
  let short = predicate;
  for (const prefix of ['is ', 'está ']) {
    if (short.startsWith(prefix)) {
      short = short.slice(prefix.length);
      break;
    }
  }
  short = short.charAt(0).toUpperCase() + short.slice(1);
  return { activity, mood, predicate, sentence: `${name} ${predicate}`, short };
}

/** Composer copy: "Message Flame…" / "Escríbale a Flame…". */
export function agentComposerPlaceholder(
  persona: WebShellAgentPersona,
): string {
  return persona.language === 'es'
    ? `Escríbale a ${persona.name}…`
    : `Message ${persona.name}…`;
}

/**
 * Reads `?agent=<name>` (and `agentLang`, `agentAvatar`) for the standalone
 * shell. `agentAvatar` is a same-origin path; it defaults to
 * `/assets/persona`, where a host puts the loops next to the shell's assets.
 */
export function agentPersonaFromSearch(
  search: string,
  fallbackLanguage = typeof navigator === 'undefined'
    ? 'en'
    : navigator.language,
): WebShellAgentPersona | undefined {
  const params = new URLSearchParams(search);
  const raw = params.get('agent');
  if (raw === null) return undefined;
  const avatar = params.get('agentAvatar');
  const avatarBaseUrl =
    avatar === 'none'
      ? undefined
      : avatar && avatar.startsWith('/') && !avatar.startsWith('//')
        ? avatar.replace(/\/+$/, '')
        : '/assets/persona';
  return {
    name: normalizeAgentName(raw),
    language: normalizeAgentLanguage(
      params.get('agentLang') ?? fallbackLanguage,
    ),
    avatarBaseUrl,
  };
}

/**
 * The agent's state for an embedding host that cannot reach into the page
 * (for example a native webview): `document.title` carries it as
 * `canopy-agent:` + JSON. Only set while a persona is on.
 */
export const AGENT_TITLE_PREFIX = 'canopy-agent:';

export function agentStatusTitle(
  name: string,
  line: AgentStatusLine | null,
): string {
  return (
    AGENT_TITLE_PREFIX +
    JSON.stringify(
      line
        ? {
            name,
            mood: line.mood,
            state: line.activity.kind,
            short: line.short,
            sentence: line.sentence,
          }
        : { name, mood: 'idle', state: 'idle' },
    )
  );
}
