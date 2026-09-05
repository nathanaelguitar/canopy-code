/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

const daemonMocks = vi.hoisted(() => ({
  answerDaemonPermission: vi.fn(),
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

  beforeEach(() => {
    onEvent = undefined;
    daemonMocks.answerDaemonPermission.mockReset().mockResolvedValue(undefined);
    daemonMocks.submitDaemonPrompt.mockReset();
    daemonMocks.streamDaemonSessionEvents
      .mockReset()
      .mockImplementation((options: { onEvent: typeof onEvent }) => {
        onEvent = options.onEvent;
        return Promise.resolve();
      });
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
});
