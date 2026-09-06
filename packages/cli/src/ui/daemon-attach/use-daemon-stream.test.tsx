/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

const daemonMocks = vi.hoisted(() => ({
  answerDaemonPermission: vi.fn(),
  detachDaemonSession: vi.fn(),
  loadDaemonSession: vi.fn(),
  streamDaemonSessionEvents: vi.fn(),
  submitDaemonPrompt: vi.fn(),
}));

vi.mock('./daemon-session-events.js', async () => {
  const actual = await vi.importActual<
    typeof import('./daemon-session-events.js')
  >('./daemon-session-events.js');
  return { ...actual, ...daemonMocks };
});

import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { ToolConfirmationOutcome } from '@canopy-code/canopy-code-core';
import type { UseHistoryManagerReturn } from '../hooks/useHistoryManager.js';
import {
  createDaemonConfirmation,
  useDaemonStream,
  type PendingDaemonPermission,
} from './use-daemon-stream.js';
import type { DaemonSessionEvent } from './daemon-session-events.js';

describe('useDaemonStream permission rendering', () => {
  let onEvent: ((event: DaemonSessionEvent) => void) | undefined;
  let onResyncRequired: ((reason?: string) => void) | undefined;

  beforeEach(() => {
    onEvent = undefined;
    onResyncRequired = undefined;
    daemonMocks.answerDaemonPermission.mockReset().mockResolvedValue(undefined);
    daemonMocks.submitDaemonPrompt.mockReset();
    daemonMocks.detachDaemonSession.mockReset().mockResolvedValue(undefined);
    daemonMocks.loadDaemonSession.mockReset();
    daemonMocks.streamDaemonSessionEvents
      .mockReset()
      .mockImplementation(
        (options: {
          onEvent: typeof onEvent;
          onResyncRequired?: typeof onResyncRequired;
        }) => {
          onEvent = options.onEvent;
          onResyncRequired = options.onResyncRequired;
          return Promise.resolve();
        },
      );
  });

  it('deduplicates replayed ids and shows a catching-up indicator', async () => {
    daemonMocks.loadDaemonSession.mockResolvedValue({
      clientId: 'client-after-resync',
      lastEventId: 3,
      eventEpoch: 'epoch-1',
      compactedReplay: [
        {
          id: 1,
          event: 'session_update',
          data: {
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: 'already-seen' },
            },
          },
        },
      ],
      liveJournal: [
        {
          id: 1,
          event: 'session_update',
          data: {
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: 'already-seen' },
            },
          },
        },
        {
          id: 2,
          event: 'session_update',
          data: {
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: 'recovered' },
            },
          },
        },
      ],
    });
    const addItem = vi.fn() as unknown as UseHistoryManagerReturn['addItem'];
    const clearHistory = vi.fn() as UseHistoryManagerReturn['clearItems'];
    const session = {
      baseUrl: 'http://daemon.test',
      sessionId: 'session-1',
      clientId: 'client-1',
    };
    const { result } = renderHook(() =>
      useDaemonStream(session, addItem, clearHistory),
    );

    await waitFor(() => expect(onEvent).toBeDefined());
    vi.useFakeTimers();
    try {
      act(() => {
        onEvent?.({
          id: 1,
          event: 'session_update',
          data: {
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: 'already-seen' },
            },
          },
        });
        onResyncRequired?.('ring_evicted');
      });
      expect(result.current.pendingHistoryItems).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ text: 'Catching up with daemon session…' }),
        ]),
      );

      await act(async () => {
        vi.advanceTimersByTime(250);
        await vi.runAllTimersAsync();
      });

      const pendingText = result.current.pendingHistoryItems.find(
        (item) => item.type === 'gemini_content',
      );
      expect(clearHistory).toHaveBeenCalledOnce();
      expect(pendingText).toMatchObject({
        text: 'already-seenrecovered',
      });
      expect(result.current.pendingHistoryItems).not.toEqual(
        expect.arrayContaining([
          expect.objectContaining({ text: 'already-seenalready-seen' }),
        ]),
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it('renders daemon questions through the shared TUI confirmation dialog', async () => {
    const addItem = vi.fn() as unknown as UseHistoryManagerReturn['addItem'];
    const session = {
      baseUrl: 'http://daemon.test',
      sessionId: 'session-1',
      clientId: 'client-1',
    };
    const { result } = renderHook(() => useDaemonStream(session, addItem));

    await waitFor(() => expect(onEvent).toBeDefined());

    act(() => {
      onEvent?.({
        id: 1,
        event: 'permission_request',
        data: {
          data: {
            requestId: 'permission-1',
            toolCall: {
              title: 'Answer deployment questions',
              _meta: {
                canopyInteractionKind: 'user_question',
                canopyQuestions: [
                  {
                    header: 'Target',
                    question: 'Where should this deploy?',
                    options: [
                      { label: 'Staging', description: 'Safe preview' },
                      { label: 'Production', description: 'Live traffic' },
                    ],
                  },
                ],
              },
            },
            options: [
              { optionId: 'answer', name: 'Answer', kind: 'allow_once' },
            ],
          },
        },
      });
    });

    const [pendingItem] = result.current.pendingHistoryItems;
    if (!pendingItem || pendingItem.type !== 'tool_group') {
      throw new Error('Expected a pending daemon tool group');
    }
    const confirmationDetails = pendingItem.tools[0]?.confirmationDetails;
    if (
      !confirmationDetails ||
      confirmationDetails.type !== 'ask_user_question'
    ) {
      throw new Error('Expected the shared ask-user-question confirmation');
    }

    expect(confirmationDetails.questions).toEqual([
      {
        header: 'Target',
        question: 'Where should this deploy?',
        options: [
          { label: 'Staging', description: 'Safe preview' },
          { label: 'Production', description: 'Live traffic' },
        ],
      },
    ]);

    await act(async () => {
      await confirmationDetails.onConfirm(ToolConfirmationOutcome.ProceedOnce, {
        answers: { '0': 'Staging' },
      });
    });

    expect(daemonMocks.answerDaemonPermission).toHaveBeenCalledWith(
      session.baseUrl,
      session.sessionId,
      session.clientId,
      'permission-1',
      {
        outcome: { outcome: 'selected', optionId: 'answer' },
        answers: { '0': 'Staging' },
      },
    );
    expect(result.current.pendingPermission).toBeUndefined();
  });

  it('keeps non-question daemon permissions visible in the shared confirmation UI', () => {
    const permission: PendingDaemonPermission = {
      requestId: 'permission-2',
      toolCall: { title: 'Run command', rawInput: { command: 'npm test' } },
      options: [{ optionId: 'allow', kind: 'allow_once' }],
    };
    const answer = vi.fn().mockResolvedValue(undefined);

    const confirmation = createDaemonConfirmation(permission, answer);

    expect(confirmation).toMatchObject({
      type: 'info',
      title: 'Run command',
      prompt: 'Command: npm test',
    });
  });

  it('exposes pollable daemon connection health through daemonHealthRef', async () => {
    daemonMocks.loadDaemonSession.mockResolvedValue({
      clientId: 'client-after-resync',
      lastEventId: 8,
      eventEpoch: 'epoch-9',
      liveJournal: [],
    });
    const addItem = vi.fn() as unknown as UseHistoryManagerReturn['addItem'];
    const session = {
      baseUrl: 'http://daemon.test',
      sessionId: 'session-1',
      clientId: 'client-1',
    };
    const { result } = renderHook(() => useDaemonStream(session, addItem));

    await waitFor(() => expect(onEvent).toBeDefined());
    expect(result.current.daemonHealthRef.current).toMatchObject({
      isResyncing: false,
      resyncAttempts: 0,
      lastEventId: undefined,
      eventEpoch: undefined,
      lastFrameAtMs: 0,
      activeClientId: 'client-1',
    });

    act(() => {
      onEvent?.({
        id: 7,
        event: 'session_update',
        data: {
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: 'hello' },
          },
        },
      });
    });
    const afterEvent = result.current.daemonHealthRef.current;
    expect(afterEvent.lastEventId).toBe(7);
    expect(afterEvent.lastFrameAtMs).toBeGreaterThan(0);

    vi.useFakeTimers();
    try {
      act(() => {
        onResyncRequired?.('ring_evicted');
      });
      expect(result.current.daemonHealthRef.current).toMatchObject({
        isResyncing: true,
        resyncAttempts: 1,
      });
      await act(async () => {
        await vi.runAllTimersAsync();
      });
      expect(result.current.daemonHealthRef.current).toMatchObject({
        isResyncing: false,
        eventEpoch: 'epoch-9',
        activeClientId: 'client-after-resync',
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it('retries detaching the superseded client after resync', async () => {
    daemonMocks.loadDaemonSession.mockResolvedValue({
      clientId: 'client-after-resync',
      lastEventId: 2,
      liveJournal: [],
    });
    daemonMocks.detachDaemonSession
      .mockRejectedValueOnce(new Error('boom-1'))
      .mockRejectedValueOnce(new Error('boom-2'))
      .mockResolvedValue(undefined);
    const addItem = vi.fn() as unknown as UseHistoryManagerReturn['addItem'];
    const session = {
      baseUrl: 'http://daemon.test',
      sessionId: 'session-1',
      clientId: 'client-old',
    };
    renderHook(() => useDaemonStream(session, addItem));

    await waitFor(() => expect(onEvent).toBeDefined());
    vi.useFakeTimers();
    try {
      act(() => {
        onResyncRequired?.('ring_evicted');
      });
      await act(async () => {
        await vi.runAllTimersAsync();
      });
      // The stream-effect cleanup also detaches on client rotation, so assert
      // on the superseded-client calls rather than an exact total.
      const oldClientCalls = daemonMocks.detachDaemonSession.mock.calls.filter(
        (call) => call[2] === 'client-old',
      );
      expect(oldClientCalls.length).toBeGreaterThanOrEqual(3);
      expect(oldClientCalls[0]).toEqual([
        'http://daemon.test',
        'session-1',
        'client-old',
      ]);
    } finally {
      vi.useRealTimers();
    }
  });
});
