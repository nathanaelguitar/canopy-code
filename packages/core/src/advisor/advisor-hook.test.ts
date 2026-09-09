/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { vi, describe, it, expect, beforeEach } from 'vitest';
import {
  ADVISOR_HOOK_TIMEOUT_MS,
  buildAdvisorGuidancePrompt,
  registerAdvisorHook,
  unregisterAdvisorHook,
  isAdvisorHookRegistered,
} from './advisor-hook.js';
import { HookEventName } from '../hooks/types.js';
import type { Config } from '../config/config.js';

const mockRunForkedAgent = vi.hoisted(() => vi.fn());
const mockBuildBtwCacheSafeParams = vi.hoisted(() =>
  vi.fn().mockReturnValue({
    generationConfig: {},
    history: [{ role: 'user', parts: [{ text: 'hello' }] }],
    model: 'test-model',
    version: 0,
  }),
);

vi.mock('../utils/forkedAgent.js', () => ({
  runForkedAgent: mockRunForkedAgent,
}));
vi.mock('../utils/btwUtils.js', () => ({
  buildBtwCacheSafeParams: mockBuildBtwCacheSafeParams,
}));

function createConfig(withHistory = true) {
  const hooks = new Map<string, { event: HookEventName; matcher: string }>();
  const system = {
    addFunctionHook: vi.fn(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (...args: any[]) => {
        const [, event, matcher] = args as [string, HookEventName, string];
        const id = `hook-${hooks.size}`;
        hooks.set(id, { event, matcher });
        return id;
      },
    ),
    removeFunctionHook: vi.fn((_sessionId: string, _event: HookEventName, hookId: string) =>
      hooks.delete(hookId),
    ),
    hooks,
  };
  const config = {
    getHookSystem: () => system,
    getGeminiClient: () => ({
      getHistoryForForkWindow: () =>
        withHistory ? [{ role: 'user', parts: [{ text: 'hi' }] }] : [],
    }),
  } as unknown as Config;
  return { config, system };
}

let sessionCounter = 0;
// Session ids must be unique per test: the module keeps a session->hookId map
// across tests so register() can idempotently unregister the previous hook.
let SESSION: string;
const INPUT = { prompt: 'ship it?' };

beforeEach(() => {
  vi.clearAllMocks();
  SESSION = `session-${++sessionCounter}`;
});


describe('buildAdvisorGuidancePrompt', () => {
  it('includes the user prompt and no-tools constraint', () => {
    const prompt = buildAdvisorGuidancePrompt('ship it?');
    expect(prompt).toContain('ship it?');
    expect(prompt).toContain('NO tools');
  });
});

describe('registerAdvisorHook', () => {
  it('registers a UserPromptSubmit function hook with the advisor timeout', () => {
    const { config, system } = createConfig();
    registerAdvisorHook({ config, sessionId: SESSION });

    expect(system.addFunctionHook).toHaveBeenCalledWith(
      SESSION,
      HookEventName.UserPromptSubmit,
      '*',
      expect.any(Function),
      'Advisor guidance failed',
      expect.objectContaining({ timeout: ADVISOR_HOOK_TIMEOUT_MS }),
    );
    expect(isAdvisorHookRegistered(config, SESSION)).toBe(true);
  });

  it('re-registers idempotently (one hook per session)', () => {
    const { config, system } = createConfig();
    registerAdvisorHook({ config, sessionId: SESSION });
    registerAdvisorHook({ config, sessionId: SESSION });

    expect(system.hooks.size).toBe(1);
  });
});

describe('advisor callback', () => {
  async function runCallback(opts: {
    withHistory?: boolean;
    advisorModel?: string;
    rejectWith?: Error;
    text?: string;
    aborted?: boolean;
  }) {
    const { config, system } = createConfig(opts.withHistory ?? true);
    registerAdvisorHook({
      config,
      sessionId: SESSION,
      advisorModel: opts.advisorModel,
    });
    const callback = system.addFunctionHook.mock.calls[0]![3] as (
      input: unknown,
      context?: { signal?: AbortSignal },
    ) => Promise<object>;
    if (opts.rejectWith) {
      mockRunForkedAgent.mockRejectedValue(opts.rejectWith);
    } else {
      mockRunForkedAgent.mockResolvedValue({ text: opts.text ?? 'Watch out.' });
    }
    return callback(INPUT, {
      signal: opts.aborted ? AbortSignal.abort() : new AbortController().signal,
    });
  }

  it('returns guidance as additionalContext', async () => {
    const output = await runCallback({ text: 'Verify before merging.' });
    expect(output).toEqual({
      hookSpecificOutput: {
        hookEventName: 'UserPromptSubmit',
        additionalContext: expect.stringContaining('Verify before merging.'),
      },
    });
    expect(mockRunForkedAgent).toHaveBeenCalledWith(
      expect.objectContaining({
        disableModelFallbacks: true,
        userMessage: expect.stringContaining('ship it?'),
      }),
    );
  });

  it('passes the advisorModel override as model', async () => {
    await runCallback({ advisorModel: 'big-model' });
    expect(mockRunForkedAgent).toHaveBeenCalledWith(
      expect.objectContaining({ model: 'big-model' }),
    );
  });

  it('fails silently ({}) when the advisor call rejects', async () => {
    const output = await runCallback({ rejectWith: new Error('boom') });
    expect(output).toEqual({});
  });

  it('returns {} when there is no conversation context', async () => {
    const output = await runCallback({ withHistory: false });
    expect(output).toEqual({});
    expect(mockRunForkedAgent).not.toHaveBeenCalled();
  });

  it('returns {} when cache params are unavailable', async () => {
    mockBuildBtwCacheSafeParams.mockReturnValueOnce(null);
    const output = await runCallback({});
    expect(output).toEqual({});
    expect(mockRunForkedAgent).not.toHaveBeenCalled();
  });

  it('returns {} when aborted before the response lands', async () => {
    const output = await runCallback({ aborted: true });
    expect(output).toEqual({});
  });

  it('returns {} when guidance text is empty', async () => {
    const output = await runCallback({ text: '   ' });
    expect(output).toEqual({});
  });
});

describe('unregisterAdvisorHook', () => {
  it('removes the hook and is idempotent', () => {
    const { config, system } = createConfig();
    registerAdvisorHook({ config, sessionId: SESSION });

    expect(unregisterAdvisorHook(config, SESSION)).toBe(true);
    expect(isAdvisorHookRegistered(config, SESSION)).toBe(false);
    expect(unregisterAdvisorHook(config, SESSION)).toBe(false);
    expect(system.removeFunctionHook).toHaveBeenCalledTimes(1);
  });
});
