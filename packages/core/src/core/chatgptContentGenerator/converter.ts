/**
 * @license
 * Copyright 2026 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Wire-format conversions between the internal Gemini-style request/response
 * types and the OpenAI Responses API used by the ChatGPT Codex backend.
 */

import type {
  Content,
  GenerateContentResponseUsageMetadata,
  Part,
} from '@google/genai';
import { FinishReason, GenerateContentResponse } from '@google/genai';

// ---------------------------------------------------------------------------
// Request conversion
// ---------------------------------------------------------------------------

export interface ResponsesInputItem {
  type?: string;
  role?: string;
  content?: Array<Record<string, unknown>>;
  name?: string;
  call_id?: string;
  output?: string;
  arguments?: string;
  status?: string;
}

export function systemInstructionToInstructions(
  systemInstruction: Part[] | Part | string | undefined,
): string {
  if (!systemInstruction) return '';
  if (typeof systemInstruction === 'string') {
    return systemInstruction;
  }
  const parts: Array<Part | { text?: string }> = Array.isArray(
    systemInstruction,
  )
    ? systemInstruction
    : [systemInstruction as unknown as Part];
  const texts = parts
    .map((part) =>
      part && typeof part === 'object' && 'text' in part
        ? ((part as { text?: string }).text ?? '')
        : '',
    )
    .filter(Boolean);
  return texts.join('\n');
}

function partToInputContent(
  part: Part,
  role: string,
): Array<Record<string, unknown>> {
  if (typeof part.text === 'string') {
    return [
      {
        type: role === 'assistant' ? 'output_text' : 'input_text',
        text: part.text,
      },
    ];
  }
  if (part.inlineData?.mimeType?.startsWith('image/') && part.inlineData.data) {
    return [
      {
        type: 'input_image',
        image_url: `data:${part.inlineData.mimeType};base64,${part.inlineData.data}`,
      },
    ];
  }
  return [];
}

export function convertContentsToResponsesInput(
  contents: Content[],
): ResponsesInputItem[] {
  const input: ResponsesInputItem[] = [];
  for (const content of contents) {
    const role = content.role === 'model' ? 'assistant' : 'user';
    const messageContent: Array<Record<string, unknown>> = [];

    for (const part of content.parts ?? []) {
      if (part.thought) continue;
      if (part.functionCall) {
        if (messageContent.length > 0) {
          input.push({ role, content: messageContent });
          messageContent.length = 0;
        }
        input.push({
          type: 'function_call',
          call_id: part.functionCall.id || part.functionCall.name || '',
          name: part.functionCall.name || '',
          arguments: JSON.stringify(part.functionCall.args ?? {}),
        });
        continue;
      }
      if (part.functionResponse) {
        if (messageContent.length > 0) {
          input.push({ role, content: messageContent });
          messageContent.length = 0;
        }
        input.push({
          type: 'function_call_output',
          call_id:
            (part.functionResponse.id as string | undefined) ||
            part.functionResponse.name ||
            '',
          output:
            typeof part.functionResponse.response === 'string'
              ? part.functionResponse.response
              : JSON.stringify(part.functionResponse.response ?? {}),
        });
        continue;
      }
      messageContent.push(...partToInputContent(part, role));
    }

    if (messageContent.length > 0) {
      input.push({
        role,
        content: messageContent,
        ...(role === 'assistant' ? { type: 'message' } : {}),
      });
    }
  }
  return input;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type InternalTool = any;

export function convertToolsToResponsesFormat(
  tools: InternalTool[] | undefined,
): Array<Record<string, unknown>> {
  const converted: Array<Record<string, unknown>> = [];
  for (const tool of tools ?? []) {
    for (const declaration of tool.functionDeclarations ?? []) {
      converted.push({
        type: 'function',
        name: declaration.name,
        ...(declaration.description
          ? { description: declaration.description }
          : {}),
        parameters: declaration.parameters ?? {
          type: 'object',
          properties: {},
        },
        strict: false,
      });
    }
  }
  return converted;
}

export function convertToolChoice(
  config:
    | { toolConfig?: { functionCallingConfig?: { mode?: string } } }
    | undefined,
): 'auto' | 'required' | 'none' | undefined {
  const mode = config?.toolConfig?.functionCallingConfig?.mode;
  switch (mode?.toUpperCase()) {
    case 'ANY':
      return 'required';
    case 'NONE':
      return 'none';
    case 'AUTO':
      return 'auto';
    default:
      return undefined;
  }
}

// ---------------------------------------------------------------------------
// Response (SSE event stream) conversion
// ---------------------------------------------------------------------------

interface UsageTokens {
  input_tokens?: number;
  output_tokens?: number;
  total_tokens?: number;
  output_tokens_details?: { reasoning_tokens?: number };
}

/** Mutable state threaded across the SSE events of one streamed response. */
export class ResponsesStreamAccumulator {
  private readonly model: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private finalOutputItems: any[] = [];
  private usage: UsageTokens | undefined;

  constructor(model: string) {
    this.model = model;
  }

  /**
   * Converts one SSE `data:` payload into zero or one response chunks.
   * Returns null for events that map to nothing user-visible.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  consumeEvent(event: Record<string, any>): GenerateContentResponse | null {
    const type = event['type'] as string | undefined;
    let parts: Part[] | undefined;

    switch (type) {
      case 'response.output_text.delta':
        if (typeof event['delta'] === 'string' && event['delta'].length > 0) {
          parts = [{ text: event['delta'] }];
        }
        break;
      case 'response.reasoning_summary_text.delta':
      case 'response.reasoning_text.delta':
        if (typeof event['delta'] === 'string' && event['delta'].length > 0) {
          parts = [{ text: event['delta'], thought: true }];
        }
        break;
      case 'response.output_item.done': {
        const item = event['item'];
        if (item?.type === 'function_call') {
          let args: Record<string, unknown> = {};
          try {
            args = JSON.parse(item.arguments ?? '{}') as Record<
              string,
              unknown
            >;
          } catch {
            args = {};
          }
          parts = [
            {
              functionCall: {
                id: item.call_id || item.name,
                name: item.name,
                args,
              },
            },
          ];
        }
        break;
      }
      case 'response.completed':
      case 'response.incomplete': {
        const response = event['response'] ?? {};
        this.finalOutputItems = response.output ?? [];
        this.usage = response.usage;
        parts = [];
        break;
      }
      default:
        return null;
    }

    if (!parts) return null;

    const response = new GenerateContentResponse();
    response.candidates = [
      {
        content: { role: 'model', parts },
        index: 0,
        ...(type === 'response.completed' || type === 'response.incomplete'
          ? {
              finishReason:
                type === 'response.completed'
                  ? FinishReason.STOP
                  : FinishReason.MAX_TOKENS,
            }
          : {}),
      },
    ];
    if (type === 'response.completed' || type === 'response.incomplete') {
      response.usageMetadata = this.buildUsageMetadata();
    }
    return response;
  }

  private buildUsageMetadata(): GenerateContentResponseUsageMetadata {
    const usage = this.usage ?? {};
    return {
      promptTokenCount: usage.input_tokens ?? 0,
      candidatesTokenCount: usage.output_tokens ?? 0,
      totalTokenCount: usage.total_tokens ?? 0,
      thoughtsTokenCount: usage.output_tokens_details?.reasoning_tokens ?? 0,
    };
  }

  /**
   * Builds the full non-streaming response from the buffered
   * `response.completed` payload. Only valid after that event has been
   * consumed.
   */
  toFinalResponse(): GenerateContentResponse {
    const parts: Part[] = [];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    for (const item of this.finalOutputItems as any[]) {
      if (item.type === 'message' && item.role === 'assistant') {
        for (const contentItem of item.content ?? []) {
          if (contentItem.type === 'output_text' && contentItem.text) {
            parts.push({ text: contentItem.text });
          }
        }
      } else if (item.type === 'reasoning') {
        for (const summary of item.summary ?? []) {
          if (summary.text) {
            parts.push({ text: summary.text, thought: true });
          }
        }
      } else if (item.type === 'function_call') {
        let args: Record<string, unknown> = {};
        try {
          args = JSON.parse(item.arguments ?? '{}') as Record<string, unknown>;
        } catch {
          args = {};
        }
        parts.push({
          functionCall: {
            id: item.call_id || item.name,
            name: item.name,
            args,
          },
        });
      }
    }
    const response = new GenerateContentResponse();
    response.candidates = [
      {
        content: { role: 'model', parts },
        index: 0,
        finishReason: FinishReason.STOP,
      },
    ];
    response.usageMetadata = this.buildUsageMetadata();
    response.modelVersion = this.model;
    return response;
  }
}
