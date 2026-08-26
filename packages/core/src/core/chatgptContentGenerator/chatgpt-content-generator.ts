/**
 * @license
 * Copyright 2026 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

import type {
  CountTokensParameters,
  CountTokensResponse,
  EmbedContentParameters,
  EmbedContentResponse,
  GenerateContentParameters,
  Part,
} from '@google/genai';
import type { GenerateContentResponse } from '@google/genai';
import type { Config } from '../../config/config.js';
import type {
  ContentGenerator,
  ContentGeneratorConfig,
} from '../contentGenerator.js';
import { randomUUID } from 'node:crypto';
import { RequestTokenEstimator } from '../../utils/request-tokenizer/index.js';
import { createDebugLogger } from '../../utils/debugLogger.js';
import { createChildAbortController } from '../../utils/abortController.js';
import {
  buildRuntimeFetchOptions,
  redactProxyError,
} from '../../utils/runtimeFetchOptions.js';
import { ensureFreshChatgptCredentials } from '../../canopy/chatgpt-oauth.js';
import {
  ResponsesStreamAccumulator,
  convertContentsToResponsesInput,
  convertToolChoice,
  convertToolsToResponsesFormat,
  systemInstructionToInstructions,
} from './converter.js';

const debugLogger = createDebugLogger('CHATGPT');

// ChatGPT Codex backend (mirrors codex CHATGPT_CODEX_BASE_URL).
const CHATGPT_CODEX_BASE_URL = 'https://chatgpt.com/backend-api/codex';

const CHATGPT_OAUTH_ORIGINATOR =
  process.env['CANOPY_CHATGPT_ORIGINATOR'] || 'codex_cli_rs';

interface ResponsesApiRequest {
  model: string;
  instructions: string;
  input: unknown[];
  tools?: Array<Record<string, unknown>>;
  tool_choice?: string;
  reasoning?: { effort?: string; summary?: string };
  store?: boolean;
  stream?: boolean;
  prompt_cache_key?: string;
}

export class ChatgptContentGenerator implements ContentGenerator {
  constructor(
    private contentGeneratorConfig: ContentGeneratorConfig,
    private readonly cliConfig: Config,
  ) {}

  async generateContent(
    request: GenerateContentParameters,
    userPromptId: string,
  ): Promise<GenerateContentResponse> {
    const { accumulator, stream } = await this.startStream(
      request,
      userPromptId,
    );
    for await (const chunk of stream) {
      void chunk;
    }
    // The `response.completed` event has been consumed by now, so the
    // accumulator holds the full output items + usage.
    return accumulator.toFinalResponse();
  }

  async generateContentStream(
    request: GenerateContentParameters,
    userPromptId: string,
  ): Promise<AsyncGenerator<GenerateContentResponse>> {
    const { stream } = await this.startStream(request, userPromptId);
    return stream;
  }

  private async startStream(
    request: GenerateContentParameters,
    userPromptId: string,
  ): Promise<{
    accumulator: ResponsesStreamAccumulator;
    stream: AsyncGenerator<GenerateContentResponse>;
  }> {
    const credentials = await ensureFreshChatgptCredentials();
    const responsesRequest = this.buildRequest(request, userPromptId);

    const parentSignal = request.config?.abortSignal;
    const perRequestAc = parentSignal
      ? createChildAbortController(parentSignal)
      : undefined;

    // Raw fetch (no SDK), so wire the bundled-undici dispatcher through by
    // hand: use the runtime's pinned fetch whenever a custom dispatcher is
    // installed so both share one undici version.
    const runtimeOptions = buildRuntimeFetchOptions(
      'openai',
      this.cliConfig.getProxy(),
    );
    const doFetch =
      (runtimeOptions?.fetch as typeof fetch | undefined) ?? fetch;
    const dispatcher = (
      runtimeOptions?.fetchOptions as { dispatcher?: unknown } | undefined
    )?.dispatcher;

    let response: Response;
    try {
      response = await doFetch(`${CHATGPT_CODEX_BASE_URL}/responses`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${credentials.accessToken}`,
          ...(credentials.accountId
            ? { 'chatgpt-account-id': credentials.accountId }
            : {}),
          'OpenAI-Beta': 'responses=experimental',
          originator: CHATGPT_OAUTH_ORIGINATOR,
          session_id: userPromptId || randomUUID(),
          'User-Agent': `canopy-code (${CHATGPT_OAUTH_ORIGINATOR})`,
        },
        body: JSON.stringify(responsesRequest),
        signal: perRequestAc?.signal,
        ...(dispatcher ? { dispatcher } : {}),
      } as RequestInit);
    } catch (error) {
      perRequestAc?.abort();
      throw redactProxyError(error);
    }

    if (!response.ok || !response.body) {
      const text = await response.text().catch(() => '');
      perRequestAc?.abort();
      throw new Error(
        `ChatGPT Codex backend returned status ${response.status}: ${text}`,
      );
    }

    const model = this.contentGeneratorConfig.model;
    const accumulator = new ResponsesStreamAccumulator(model);

    async function* iterate(): AsyncGenerator<GenerateContentResponse> {
      try {
        for await (const event of parseSseEvents(response.body!)) {
          const converted = accumulator.consumeEvent(event);
          if (converted) {
            yield converted;
          }
        }
      } finally {
        perRequestAc?.abort();
      }
    }
    return { accumulator, stream: iterate() };
  }

  async countTokens(
    request: CountTokensParameters,
  ): Promise<CountTokensResponse> {
    try {
      const estimator = new RequestTokenEstimator();
      const result = await estimator.calculateTokens(request);
      return { totalTokens: result.totalTokens };
    } catch (error) {
      debugLogger.warn(
        'Failed to calculate tokens with tokenizer, falling back to estimate:',
        error,
      );
      return { totalTokens: 0 };
    }
  }

  async embedContent(
    _request: EmbedContentParameters,
  ): Promise<EmbedContentResponse> {
    throw new Error('ChatGPT Codex backend does not support embeddings.');
  }

  useSummarizedThinking(): boolean {
    return false;
  }

  private buildRequest(
    request: GenerateContentParameters,
    userPromptId: string,
  ): ResponsesApiRequest {
    const config = this.contentGeneratorConfig;
    const contents = toContents(request.contents);
    const input = convertContentsToResponsesInput(contents);
    const instructions = systemInstructionToInstructions(
      request.config?.systemInstruction as Part[] | Part | string | undefined,
    );

    const tools = convertToolsToResponsesFormat(request.config?.tools);

    // Codex models reason by default; the Responses API controls it via
    // `reasoning.effort`. Honor the internal thinkingConfig opt-out, and
    // allow an effort override through extra_body.reasoning_effort.
    const reasoningDisabled =
      request.config?.thinkingConfig?.includeThoughts === false;
    const configuredEffort = (
      config.extra_body as Record<string, unknown> | undefined
    )?.['reasoning_effort'] as string | undefined;
    const reasoningEffort = reasoningDisabled
      ? undefined
      : (configuredEffort ?? 'medium');

    const body: ResponsesApiRequest = {
      model: config.model,
      instructions,
      input,
      store: false,
      stream: true,
      prompt_cache_key: userPromptId,
    };

    if (tools.length > 0) {
      body.tools = tools;
      const toolChoice = convertToolChoice(request.config);
      if (toolChoice) {
        body.tool_choice = toolChoice;
      }
    }

    if (reasoningEffort) {
      body.reasoning = { effort: reasoningEffort, summary: 'auto' };
    }

    return body;
  }
}

function toContents(value: unknown): Array<import('@google/genai').Content> {
  if (!value) return [];
  if (Array.isArray(value)) {
    return value.flatMap((item) => toContents(item));
  }
  const candidate = value as { role?: string; parts?: Part[] };
  if (candidate.role && candidate.parts) {
    return [candidate as import('@google/genai').Content];
  }
  // Single Part or string shorthand.
  if (typeof value === 'string') {
    return [{ role: 'user', parts: [{ text: value }] }];
  }
  return [{ role: 'user', parts: [value as Part] }];
}

/**
 * Parses an SSE byte stream into JSON event payloads. Handles multi-line
 * `data:` fields and ignores comments / keep-alives. Terminates on `[DONE]`.
 */
async function* parseSseEvents(
  body: ReadableStream<Uint8Array>,
): AsyncGenerator<Record<string, unknown>> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let dataLines: string[] = [];

  const flush = (): Record<string, unknown> | null => {
    if (dataLines.length === 0) return null;
    const raw = dataLines.join('\n');
    dataLines = [];
    if (!raw || raw === '[DONE]') return null;
    try {
      return JSON.parse(raw) as Record<string, unknown>;
    } catch (error) {
      debugLogger.warn(
        'Dropping unparseable SSE event:',
        error instanceof Error ? error.message : error,
      );
      return null;
    }
  };

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      let newlineIndex: number;
      while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, newlineIndex).replace(/\r$/, '');
        buffer = buffer.slice(newlineIndex + 1);
        if (line.startsWith('data:')) {
          dataLines.push(line.slice(5).trimStart());
        } else if (line === '') {
          const event = flush();
          if (event) yield event;
        }
      }
    }
    const trailing = flush();
    if (trailing) yield trailing;
  } finally {
    reader.releaseLock();
  }
}
