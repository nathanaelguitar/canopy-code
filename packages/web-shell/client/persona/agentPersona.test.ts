import { describe, expect, it } from 'vitest';
import type { DaemonTranscriptBlock } from '@qwen-code/sdk/daemon';
import {
  activityForTool,
  agentComposerPlaceholder,
  agentPersonaFromSearch,
  agentStatusLine,
  agentStatusTitle,
  currentAgentActivity,
  currentToolSubject,
  moodFor,
  normalizeAgentName,
  subjectFromToolTitle,
} from './agentPersona';

const base = { clientReceivedAt: 0, createdAt: 0, updatedAt: 0 };
const user = (id: string): DaemonTranscriptBlock => ({
  ...base,
  id,
  kind: 'user',
  text: 'hi',
});
const thought = (id: string): DaemonTranscriptBlock => ({
  ...base,
  id,
  kind: 'thought',
  text: '…',
});
const tool = (
  id: string,
  title: string,
  status: string,
  toolKind?: string,
): DaemonTranscriptBlock =>
  ({
    ...base,
    id,
    kind: 'tool',
    toolCallId: id,
    title,
    status,
    toolKind,
    preview: { kind: 'none' },
  }) as unknown as DaemonTranscriptBlock;
const error = (id: string): DaemonTranscriptBlock => ({
  ...base,
  id,
  kind: 'error',
  text: 'boom',
});

describe('agent persona', () => {
  it('normalizes names like the Mac app', () => {
    expect(normalizeAgentName('  Ember  ')).toBe('Ember');
    expect(normalizeAgentName('')).toBe('Flame');
    expect(normalizeAgentName(null)).toBe('Flame');
    expect(normalizeAgentName('a\nb')).toBe('a b');
    expect(normalizeAgentName('x'.repeat(40))).toHaveLength(20);
  });

  it('is off without ?agent=', () => {
    expect(agentPersonaFromSearch('?theme=dark')).toBeUndefined();
    expect(agentPersonaFromSearch('?agent=Ember&agentLang=es-CO')).toEqual({
      name: 'Ember',
      language: 'es',
      avatarBaseUrl: '/assets/persona',
    });
    expect(
      agentPersonaFromSearch('?agent=&agentAvatar=//evil.example/x', 'en'),
    ).toMatchObject({ name: 'Flame', avatarBaseUrl: '/assets/persona' });
    expect(
      agentPersonaFromSearch('?agent=Flame&agentAvatar=none', 'en')
        ?.avatarBaseUrl,
    ).toBeUndefined();
  });

  it('maps tools to truthful status lines', () => {
    expect(activityForTool('read', 'Read App.tsx')).toEqual({
      kind: 'reading',
    });
    expect(activityForTool(undefined, 'grep foo')).toEqual({
      kind: 'searching',
    });
    expect(activityForTool('execute', 'npm test')).toEqual({
      kind: 'runningTests',
    });
    expect(activityForTool('execute', 'cargo build')).toEqual({
      kind: 'building',
    });
    expect(activityForTool('execute', 'ls -la')).toEqual({
      kind: 'runningCommand',
    });
    expect(activityForTool('other', 'edit src/a.ts')).toEqual({
      kind: 'editing',
    });
  });

  it('reads the current activity from the transcript', () => {
    expect(
      currentAgentActivity([user('u'), tool('t', 'Read a.ts', 'completed')], {
        isWorking: false,
        needsApproval: false,
      }),
    ).toBeNull();
    expect(
      currentAgentActivity([user('u'), error('e')], {
        isWorking: false,
        needsApproval: false,
      }),
    ).toEqual({ kind: 'snag' });
    expect(
      currentAgentActivity([user('u')], {
        isWorking: true,
        needsApproval: true,
      }),
    ).toEqual({ kind: 'needsApproval' });
    expect(
      currentAgentActivity(
        [user('u'), tool('t', 'Read src/App.tsx', 'in_progress', 'read')],
        { isWorking: true, needsApproval: false },
      ),
    ).toEqual({ kind: 'reading' });
    expect(
      currentAgentActivity([user('u'), thought('th')], {
        isWorking: true,
        needsApproval: false,
      }),
    ).toEqual({ kind: 'thinking' });
    // A tool still "running" in an earlier turn does not count.
    expect(
      currentAgentActivity([tool('old', 'Read a.ts', 'running'), user('u')], {
        isWorking: true,
        needsApproval: false,
      }),
    ).toEqual({ kind: 'working' });
  });

  it('finds the subject of a running tool', () => {
    expect(
      currentToolSubject([user('u'), tool('t', 'Read src/App.tsx', 'running')]),
    ).toBe('App.tsx');
    expect(subjectFromToolTitle('Search codeSessions =')).toBeUndefined();
  });

  it('writes the SOUL.md lines in English and Spanish', () => {
    const en = agentStatusLine('Flame', { kind: 'reading' }, 'en');
    expect(en.sentence).toBe('Flame is reading the code…');
    expect(en.short).toBe('Reading the code…');
    expect(en.mood).toBe('working');
    const es = agentStatusLine('Flame', { kind: 'needsApproval' }, 'es');
    expect(es.sentence).toBe('Flame necesita su visto bueno');
    expect(agentStatusLine('Flame', { kind: 'done' }, 'es').sentence).toBe(
      'Listo.',
    );
    expect(moodFor({ kind: 'snag' })).toBe('snag');
    expect(agentComposerPlaceholder({ name: 'Ember', language: 'en' })).toBe(
      'Message Ember…',
    );
  });

  it('encodes the status for a native host in the title', () => {
    const title = agentStatusTitle(
      'Flame',
      agentStatusLine('Flame', { kind: 'thinking' }, 'en'),
    );
    expect(title.startsWith('canopy-agent:')).toBe(true);
    expect(JSON.parse(title.slice('canopy-agent:'.length))).toMatchObject({
      mood: 'thinking',
      short: 'Thinking…',
    });
  });
});
