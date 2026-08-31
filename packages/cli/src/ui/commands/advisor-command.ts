/**
 * @license
 * Copyright 2025 Canopy Code
 * SPDX-License-Identifier: Apache-2.0
 */

import type {
  CommandContext,
  SlashCommand,
  SlashCommandActionReturn,
} from './types.js';
import type { Content } from '@google/genai';
import { CommandKind } from './types.js';
import { MessageType } from '../types.js';
import { t } from '../../i18n/index.js';
import {
  BTW_MAX_INPUT_LENGTH,
  buildBtwCacheSafeParams,
  runForkedAgent,
  type CacheSafeParams,
} from '@canopy-code/canopy-code-core';

const ADVISOR_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    verdict: { type: 'string', minLength: 1 },
    risks: { type: 'string', minLength: 1 },
    missingEvidence: { type: 'string', minLength: 1 },
    recommendation: { type: 'string', minLength: 1 },
  },
  required: ['verdict', 'risks', 'missingEvidence', 'recommendation'],
} as const;

interface AdvisorReview {
  verdict: string;
  risks: string;
  missingEvidence: string;
  recommendation: string;
}

const ADVISOR_REASONING_EFFORTS = new Set([
  'none',
  'low',
  'medium',
  'high',
  'xhigh',
  'max',
]);

/**
 * Parse the compact advisor setting form `<model> <effort>` and migrate the
 * old ChatGPT family alias. The model ID and reasoning effort are independent
 * wire fields; keeping them separate prevents `gpt-5.6-luna high` from being
 * sent as an invalid model name.
 */
function parseAdvisorModelSetting(raw: string): {
  model: string;
  reasoningEffort?: string;
} {
  const tokens = raw.trim().split(/\s+/);
  const last = tokens.at(-1)?.toLowerCase();
  const reasoningEffort =
    last && ADVISOR_REASONING_EFFORTS.has(last) ? last : undefined;
  const model = (reasoningEffort ? tokens.slice(0, -1) : tokens).join(' ');
  const migratedModel = model.replace(
    /^(chatgpt-oauth:)?gpt-5\.6$/,
    '$1gpt-5.6-sol',
  );
  return {
    model: migratedModel,
    ...(reasoningEffort ? { reasoningEffort } : {}),
  };
}

function buildAdvisorPrompt(focus: string): string {
  return [
    '<system-reminder>',
    'You are acting as an ADVISOR — an independent senior reviewer giving a second opinion on the conversation so far. The transcript above may be truncated to the most recent turns; treat what is shown as the evidence available to you.',
    '',
    'CRITICAL CONSTRAINTS:',
    '- You have NO tools. Base every claim strictly on evidence present in the transcript; never claim to have verified something you could not observe.',
    '- Do not perform the task or write the implementation. Review only.',
    '- Be direct about problems: flawed assumptions, premature conclusions, unverified claims, risky next steps.',
    '- The main conversation is NOT interrupted; your review is shown to the user only.',
    '',
    'Return exactly one JSON object with these string fields and no markdown fence, preamble, extra key, or commentary:',
    '- verdict: one short paragraph stating whether the current approach or conclusion is sound.',
    '- risks: concrete risks or flawed assumptions, each citing transcript evidence. Write "None found" if none.',
    '- missingEvidence: claims asserted but not verified in the visible transcript (earlier verification may exist outside the shown window).',
    '- recommendation: the single most valuable next action.',
    '</system-reminder>',
    '',
    focus || 'Review the conversation above.',
  ].join('\n');
}

function formatAdvisorReview(
  value: Record<string, unknown> | undefined,
): string {
  const fields = ['verdict', 'risks', 'missingEvidence', 'recommendation'];
  if (
    !value ||
    Object.keys(value).length !== fields.length ||
    fields.some((field) => {
      const fieldValue = value[field];
      return typeof fieldValue !== 'string' || fieldValue.trim().length === 0;
    })
  ) {
    throw new Error('Advisor returned invalid structured output.');
  }

  const review = value as unknown as AdvisorReview;

  return [
    '## Verdict',
    review.verdict.trim(),
    '## Risks',
    review.risks.trim(),
    '## Missing evidence',
    review.missingEvidence.trim(),
    '## Recommendation',
    review.recommendation.trim(),
  ].join('\n\n');
}

function formatAdvisorError(error: unknown): string {
  return t('Advisor review failed: {{error}}', {
    error:
      error instanceof Error ? error.message : String(error || 'Unknown error'),
  });
}

/**
 * An attached terminal has a display-only Config: the daemon owns the real
 * GeminiChat and therefore the local client's fork window is intentionally
 * empty.  The daemon event stream still populates the UI history, so build a
 * small, text-only cache snapshot from that history for /advisor.  The same
 * fallback also covers resumed sessions whose local cache snapshot was lost;
 * startup-only UI rows are ignored because only user/assistant text is copied.
 */
function buildAttachedAdvisorCacheSafeParams(
  context: CommandContext,
  config: NonNullable<CommandContext['services']['config']>,
): CacheSafeParams | null {
  const history: Content[] = [];
  for (const item of context.ui.history) {
    if (
      (item.type === 'user' ||
        item.type === 'gemini' ||
        item.type === 'gemini_content') &&
      typeof item.text === 'string' &&
      item.text.trim()
    ) {
      history.push({
        role: item.type === 'user' ? 'user' : 'model',
        parts: [{ text: item.text }],
      });
    }
  }

  if (history.length === 0) return null;

  try {
    const generationConfig = config
      .getGeminiClient()
      .getChat()
      .getGenerationConfig();
    if (!generationConfig) return null;
    return {
      generationConfig: structuredClone(generationConfig),
      // Keep the advisor bounded just like the normal cache snapshot.  The
      // newest turns carry the useful evidence and avoid an oversized fork.
      history: history.slice(-40),
      model: config.getModel() ?? '',
      version: 0,
    };
  } catch {
    // A display-only attached Config may not have a local chat at all.  In
    // that case the daemon must expose a normal local history before review
    // can run; preserve the explicit, actionable error below.
    return null;
  }
}

async function askAdvisor(
  context: CommandContext,
  focus: string,
  abortSignal: AbortSignal,
  selectedAdvisor?: { model: string; reasoningEffort?: string },
): Promise<{ text: string; model: string }> {
  const { config } = context.services;
  if (!config) throw new Error(t('Config not loaded.'));

  const localCacheSafeParams = buildBtwCacheSafeParams(config);
  const localHasHistory =
    config.getGeminiClient().getHistoryForForkWindow().length > 0;
  const cacheSafeParams =
    localCacheSafeParams && localHasHistory
      ? localCacheSafeParams
      : buildAttachedAdvisorCacheSafeParams(context, config);
  if (!cacheSafeParams) {
    throw new Error(t('No conversation context available for /advisor'));
  }

  const advisorModelSetting =
    context.services.settings.merged.advisorModel?.trim() || undefined;
  const advisorSelection =
    selectedAdvisor ??
    (advisorModelSetting
      ? parseAdvisorModelSetting(advisorModelSetting)
      : undefined);

  // Tools are always stripped (NO_TOOLS), matching /btw and the "You have NO
  // tools" framing of the advisor prompt. This accepts a cache-prefix miss in
  // exchange for guaranteeing the reviewer cannot answer with tool calls that
  // would be discarded and surface as an empty review.
  const result = await runForkedAgent({
    config,
    userMessage: buildAdvisorPrompt(focus),
    cacheSafeParams,
    jsonSchema: ADVISOR_SCHEMA,
    ...(advisorSelection
      ? {
          model: advisorSelection.model,
          ...(advisorSelection.reasoningEffort
            ? { reasoningEffort: advisorSelection.reasoningEffort }
            : {}),
        }
      : {}),
    abortSignal,
    disableModelFallbacks: true,
  });

  return {
    text: formatAdvisorReview(result.jsonResult),
    model: result.model,
  };
}

async function runInteractiveAdvisor(
  context: CommandContext,
  focus: string,
  abortSignal: AbortSignal,
  selectedAdvisor?: { model: string; reasoningEffort?: string },
): Promise<void> {
  if (abortSignal.aborted) return;
  const { ui } = context;
  ui.setPendingItem({
    type: MessageType.INFO,
    text: t('Consulting advisor...'),
  });
  try {
    const review = await askAdvisor(
      context,
      focus,
      abortSignal,
      selectedAdvisor,
    );
    if (abortSignal.aborted) return;
    ui.addItem(
      { type: MessageType.ADVISOR, text: review.text, model: review.model },
      Date.now(),
    );
  } catch (error) {
    if (abortSignal.aborted) return;
    ui.addItem(
      { type: MessageType.ERROR, text: formatAdvisorError(error) },
      Date.now(),
    );
  } finally {
    if (!abortSignal.aborted) ui.setPendingItem(null);
  }
}

export const advisorCommand: SlashCommand = {
  name: 'advisor',
  get description() {
    return t(
      'Get a second opinion on the current conversation from a reviewer model',
    );
  },
  kind: CommandKind.BUILT_IN,
  supportedModes: ['interactive', 'acp'] as const,
  action: async (
    context: CommandContext,
    args: string,
  ): Promise<void | SlashCommandActionReturn> => {
    const focus = args.trim();

    if (focus.length > BTW_MAX_INPUT_LENGTH) {
      return {
        type: 'message',
        messageType: 'error',
        content: t('Focus too long (max {{max}} chars)', {
          max: String(BTW_MAX_INPUT_LENGTH),
        }),
      };
    }

    const { config } = context.services;
    const { ui } = context;

    if (!config) {
      return {
        type: 'message',
        messageType: 'error',
        content: t('Config not loaded.'),
      };
    }

    if (!config.getModel()) {
      return {
        type: 'message',
        messageType: 'error',
        content: t('No model configured.'),
      };
    }

    const abortSignal = context.abortSignal ?? new AbortController().signal;
    const executionMode = context.executionMode ?? 'interactive';

    if (executionMode !== 'interactive') {
      try {
        const review = await askAdvisor(context, focus, abortSignal);
        return { type: 'message', messageType: 'info', content: review.text };
      } catch (error) {
        return {
          type: 'message',
          messageType: 'error',
          content: formatAdvisorError(error),
        };
      }
    }

    // Mirror /recap's guard: pendingItem alone misses an in-flight main turn,
    // which isIdleRef covers. Without it the advisor would review a stale
    // snapshot and park a blocking pendingItem on top of a live turn.
    const turnInFlight = !ui.isIdleRef.current || ui.pendingItem !== null;
    if (turnInFlight) {
      return {
        type: 'message',
        messageType: 'error',
        content: t(
          'Another operation is in progress, wait for it to complete before running /advisor',
        ),
      };
    }

    // The slash-command processor supplies the canonical invocation name. A
    // direct programmatic caller (used by integrations and unit tests) has no
    // picker surface, so retain the immediate execution behavior there.
    if (context.invocation?.name !== 'advisor') {
      await runInteractiveAdvisor(context, focus, abortSignal);
      return;
    }

    const persistedAdvisorSetting =
      context.services.settings.merged.advisorModel?.trim() || undefined;
    const persistedAdvisorSelection = persistedAdvisorSetting
      ? parseAdvisorModelSetting(persistedAdvisorSetting)
      : undefined;

    return {
      type: 'advisor_picker',
      initialModel: persistedAdvisorSelection?.model || config.getModel(),
      initialReasoningEffort: persistedAdvisorSelection?.reasoningEffort,
      onSelect: async (model: string, reasoningEffort: string) => {
        // Keep the last picker choice as the default for the next /advisor.
        context.services.settings.setValue(
          'User' as Parameters<typeof context.services.settings.setValue>[0],
          'advisorModel',
          `${model} ${reasoningEffort}`,
        );
        await runInteractiveAdvisor(context, focus, abortSignal, {
          model,
          reasoningEffort,
        });
      },
      onCancel: () => undefined,
    };
  },
};
