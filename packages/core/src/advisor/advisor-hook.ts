/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import type { Config } from '../config/config.js';
import {
  HookEventName,
  type FunctionHookContext,
  type HookInput,
  type UserPromptSubmitInput,
} from '../hooks/types.js';
import { runForkedAgent } from '../utils/forkedAgent.js';
import { buildBtwCacheSafeParams } from '../utils/btwUtils.js';
import { createDebugLogger } from '../utils/debugLogger.js';

const debugLogger = createDebugLogger('ADVISOR_HOOK');

export const ADVISOR_HOOK_ID = 'advisor-mode-hook';
/** Advisor calls are one forked model call; 60s guarantees a hang never blocks the turn. */
export const ADVISOR_HOOK_TIMEOUT_MS = 60_000;

// Session -> hookId so unregister can find it without a scan.
const registeredHooks = new Map<string, string>();

/**
 * Short guidance prompt for advisor mode. Distinct from the /advisor one-shot
 * structured review: this is free-text guidance injected into the turn via the
 * UserPromptSubmit additionalContext path, not a user-facing report.
 */
export function buildAdvisorGuidancePrompt(userPrompt: string): string {
  return [
    'You are an ADVISOR reviewing the conversation above. The user just submitted a new message:',
    '',
    userPrompt,
    '',
    'Give brief guidance for the main agent\'s next response (2-4 sentences): what to prioritize, likely pitfalls, missing considerations. Be direct; do not answer the user yourself. You have NO tools — base guidance only on what is visible in the conversation.',
  ].join('\n');
}

function createAdvisorCallback(
  config: Config,
  advisorModel?: string,
): (input: HookInput, context?: FunctionHookContext) => Promise<object> {
  return async (input, context) => {
    try {
      const cacheSafeParams = buildBtwCacheSafeParams(config);
      if (
        !cacheSafeParams ||
        config.getGeminiClient().getHistoryForForkWindow().length === 0
      ) {
        return {};
      }

      const prompt =
        (input as UserPromptSubmitInput).prompt ??
        (input as UserPromptSubmitInput).submitted_prompt ??
        '';

      const result = await runForkedAgent({
        config,
        userMessage: buildAdvisorGuidancePrompt(prompt),
        cacheSafeParams,
        ...(advisorModel ? { model: advisorModel } : {}),
        abortSignal: context?.signal,
        disableModelFallbacks: true,
      });

      if (context?.signal?.aborted) return {};

      const guidance = result.text?.trim();
      if (!guidance) return {};

      return {
        hookSpecificOutput: {
          hookEventName: 'UserPromptSubmit',
          additionalContext: `Advisor guidance (non-binding):\n${guidance}`,
        },
      };
    } catch (error) {
      // Advisor mode must never block or break a user turn.
      debugLogger.debug('Advisor guidance call failed', error);
      return {};
    }
  };
}

/**
 * Registers (or replaces) the advisor-mode UserPromptSubmit hook for this
 * session. Callers gate on `Config.getHookSystem()` before invoking.
 *
 * The hook fires only for genuine user prompt submissions — Retry/Cron/Steer
 * turns are excluded upstream in client.ts — and its guidance is injected as
 * `<canopy:user-prompt-submit-context>` context by the existing client path.
 */
export function registerAdvisorHook(args: {
  config: Config;
  sessionId: string;
  advisorModel?: string;
}): string {
  const { config, sessionId, advisorModel } = args;
  const system = config.getHookSystem();
  if (!system) {
    throw new Error('Hook system is not initialized; cannot register advisor mode');
  }

  unregisterAdvisorHook(config, sessionId);

  const hookId = system.addFunctionHook(
    sessionId,
    HookEventName.UserPromptSubmit,
    '*',
    createAdvisorCallback(config, advisorModel),
    'Advisor guidance failed',
    {
      name: ADVISOR_HOOK_ID,
      description: 'Advisory guidance injected into each user turn',
      statusMessage: 'Consulting advisor…',
      timeout: ADVISOR_HOOK_TIMEOUT_MS,
    },
  );
  registeredHooks.set(sessionId, hookId);
  return hookId;
}

/** Removes the advisor-mode hook for the session (idempotent). */
export function unregisterAdvisorHook(config: Config, sessionId: string): boolean {
  const hookId = registeredHooks.get(sessionId);
  registeredHooks.delete(sessionId);
  if (!hookId) return false;
  const system = config.getHookSystem();
  if (!system) return false;
  return system.removeFunctionHook(
    sessionId,
    HookEventName.UserPromptSubmit,
    hookId,
  );
}

export function isAdvisorHookRegistered(config: Config, sessionId: string): boolean {
  return registeredHooks.has(sessionId) && !!config.getHookSystem();
}
