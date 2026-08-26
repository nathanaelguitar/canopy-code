/**
 * @license
 * Copyright 2026 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, it, expect } from 'vitest';
import {
  convertContentsToResponsesInput,
  convertToolsToResponsesFormat,
  systemInstructionToInstructions,
  ResponsesStreamAccumulator,
} from './converter.js';

describe('convertContentsToResponsesInput', () => {
  it('maps user text to input_text items and model text to output_text items', () => {
    const input = convertContentsToResponsesInput([
      { role: 'user', parts: [{ text: 'hello' }] },
      { role: 'model', parts: [{ text: 'hi there' }] },
    ]);
    expect(input).toEqual([
      { role: 'user', content: [{ type: 'input_text', text: 'hello' }] },
      {
        role: 'assistant',
        type: 'message',
        content: [{ type: 'output_text', text: 'hi there' }],
      },
    ]);
  });

  it('emits function_call / function_call_output pairs with call ids', () => {
    const input = convertContentsToResponsesInput([
      { role: 'user', parts: [{ text: 'run it' }] },
      {
        role: 'model',
        parts: [
          { functionCall: { id: 'call_1', name: 'ls', args: { path: '/' } } },
        ],
      },
      {
        role: 'user',
        parts: [
          {
            functionResponse: {
              id: 'call_1',
              name: 'ls',
              response: { output: 'files...' },
            },
          },
        ],
      },
    ]);
    expect(input[1]).toEqual({
      type: 'function_call',
      call_id: 'call_1',
      name: 'ls',
      arguments: '{"path":"/"}',
    });
    expect(input[2]).toEqual({
      type: 'function_call_output',
      call_id: 'call_1',
      output: '{"output":"files..."}',
    });
  });

  it('skips thought parts', () => {
    const input = convertContentsToResponsesInput([
      {
        role: 'model',
        parts: [{ text: 'thinking...', thought: true }, { text: 'answer' }],
      },
    ]);
    expect(input).toHaveLength(1);
    expect((input[0]?.content as Array<Record<string, unknown>>)[0]).toEqual({
      type: 'output_text',
      text: 'answer',
    });
  });

  it('converts inline images to input_image data URIs', () => {
    const input = convertContentsToResponsesInput([
      {
        role: 'user',
        parts: [{ inlineData: { mimeType: 'image/png', data: 'QUJD' } }],
      },
    ]);
    expect(input[0]?.content).toEqual([
      { type: 'input_image', image_url: 'data:image/png;base64,QUJD' },
    ]);
  });
});

describe('convertToolsToResponsesFormat', () => {
  it('flattens function declarations into function tools', () => {
    const tools = convertToolsToResponsesFormat([
      {
        functionDeclarations: [
          {
            name: 'read_file',
            description: 'Reads a file',
            parameters: { type: 'object', properties: {} },
          },
        ],
      },
    ]);
    expect(tools).toEqual([
      {
        type: 'function',
        name: 'read_file',
        description: 'Reads a file',
        parameters: { type: 'object', properties: {} },
        strict: false,
      },
    ]);
  });

  it('returns empty for undefined tools', () => {
    expect(convertToolsToResponsesFormat(undefined)).toEqual([]);
  });
});

describe('systemInstructionToInstructions', () => {
  it('joins text parts', () => {
    expect(
      systemInstructionToInstructions([{ text: 'a' }, { text: 'b' }]),
    ).toBe('a\nb');
  });
  it('handles plain strings', () => {
    expect(systemInstructionToInstructions('sys')).toBe('sys');
  });
});

describe('ResponsesStreamAccumulator', () => {
  it('emits text delta chunks as parts', () => {
    const acc = new ResponsesStreamAccumulator('gpt-5.2-codex');
    const chunk = acc.consumeEvent({
      type: 'response.output_text.delta',
      delta: 'Hello',
    })!;
    expect(chunk.candidates![0]!.content!.parts![0]).toEqual({ text: 'Hello' });
  });

  it('emits reasoning deltas as thought parts', () => {
    const acc = new ResponsesStreamAccumulator('m');
    const chunk = acc.consumeEvent({
      type: 'response.reasoning_summary_text.delta',
      delta: 'pondering',
    })!;
    expect(chunk.candidates![0]!.content!.parts![0]).toEqual({
      text: 'pondering',
      thought: true,
    });
  });

  it('emits function calls from output_item.done and finishes on completed', () => {
    const acc = new ResponsesStreamAccumulator('m');
    const callChunk = acc.consumeEvent({
      type: 'response.output_item.done',
      item: {
        type: 'function_call',
        call_id: 'call_9',
        name: 'run',
        arguments: '{"cmd":"ls"}',
      },
    })!;
    expect(callChunk.candidates![0]!.content!.parts![0]).toEqual({
      functionCall: { id: 'call_9', name: 'run', args: { cmd: 'ls' } },
    });

    const doneChunk = acc.consumeEvent({
      type: 'response.completed',
      response: {
        output: [],
        usage: {
          input_tokens: 10,
          output_tokens: 5,
          total_tokens: 15,
          output_tokens_details: { reasoning_tokens: 3 },
        },
      },
    })!;
    expect(doneChunk.candidates![0]!.finishReason).toBe('STOP');
    expect(doneChunk.usageMetadata).toEqual({
      promptTokenCount: 10,
      candidatesTokenCount: 5,
      totalTokenCount: 15,
      thoughtsTokenCount: 3,
    });
  });

  it('builds a full non-streaming response from buffered items', () => {
    const acc = new ResponsesStreamAccumulator('gpt-5.2-codex');
    acc.consumeEvent({ type: 'response.output_text.delta', delta: 'ignored' });
    acc.consumeEvent({
      type: 'response.completed',
      response: {
        output: [
          {
            type: 'message',
            role: 'assistant',
            content: [{ type: 'output_text', text: 'final answer' }],
          },
        ],
        usage: { input_tokens: 1, output_tokens: 2, total_tokens: 3 },
      },
    });
    const final = acc.toFinalResponse();
    expect(final.modelVersion).toBe('gpt-5.2-codex');
    expect(final.candidates![0]!.content!.parts).toEqual([
      { text: 'final answer' },
    ]);
  });
});
