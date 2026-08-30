/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import type { PartListUnion } from '@google/genai';
import type { UseHistoryManagerReturn } from '../hooks/useHistoryManager.js';
import { StreamingState } from '../types.js';
import type { HistoryItemToolGroup, HistoryItemWithoutId } from '../types.js';
import type { useGeminiStream } from '../hooks/useGeminiStream.js';
import {
  createDaemonTuiReducerState,
  reduceDaemonEventToTuiUpdates,
} from '../daemon/daemon-tui-adapter.js';
import {
  DaemonEventStreamHttpError,
  streamDaemonSessionEvents,
  submitDaemonPrompt,
  cancelDaemonSession,
  answerDaemonPermission,
  resumeDaemonSession,
  type DaemonSessionEvent,
} from './daemon-session-events.js';

/**
 * Renders a live daemon-managed session in the interactive TUI instead of
 * running a local agent loop. See docs/design/2026-08-26-remote-control.md.
 *
 * Deliberately not full parity with {@link useGeminiStream}: the daemon-
 * spawned `canopy --acp` child is the sole execution engine (tool
 * scheduling, compression, vision bridging, goal turns all happen there),
 * so this hook only needs to display the resulting event stream and
 * forward input — it is a display+input adapter, not a second execution
 * engine. Fields with no daemon-mode equivalent yet (goal turns, loop
 * detection, PTY tracking, approval-mode push) are typed and present, but
 * inert, so the hook return stays structurally assignable to
 * `ReturnType<typeof useGeminiStream>` and the rest of `AppContainer.tsx`
 * keeps working unmodified. Extending them is iteration, not this pass.
 *
 * ACP `sessionUpdate` kinds and the `permission_request` SSE event shape
 * used below were confirmed by direct reads of
 * `packages/acp-bridge/src/bridgeClient.ts` (permission_request publish
 * call) and `packages/acp-bridge/src/compactionEngine.ts` (sessionUpdate
 * kind switch), not assumed — see the design doc's Validated Facts.
 */

export interface PendingDaemonPermission {
  requestId: string;
  toolCall: unknown;
  options: Array<{ optionId: string; name?: string; kind?: string }>;
}

export interface UseDaemonStreamExtra {
  /** Answer a pending permission request. Present in addition to, not part
   * of, the useGeminiStream-shaped return — callers that only need local
   * rendering parity can ignore it. */
  answerPermission: (
    requestId: string,
    outcome:
      | { outcome: 'selected'; optionId: string }
      | { outcome: 'cancelled' },
  ) => Promise<void>;
  pendingPermission: PendingDaemonPermission | undefined;
  /**
   * The durable session name reported by the daemon. The ACP child owns
   * auto-title generation, so an attached terminal must consume this event
   * instead of relying on its local (non-recording) Config instance.
   */
  sessionTitle: string | undefined;
}

/**
 * Some OpenAI-compatible providers emit their internal tool wire markers as
 * ordinary text while also sending a proper structured ACP tool event. The
 * structured event is authoritative and renders a ToolGroupMessage; marker-
 * only chunks would otherwise create a wall of `tool_result` noise.
 */
function isToolProtocolArtifact(text: string): boolean {
  return /^(?:tool_(?:call|result)|function_(?:call|response))$/i.test(
    text.trim(),
  );
}

export function useDaemonStream(
  session: { baseUrl: string; sessionId: string; clientId: string } | undefined,
  addItem: UseHistoryManagerReturn['addItem'],
): ReturnType<typeof useGeminiStream> & UseDaemonStreamExtra {
  const baseUrl = session?.baseUrl;
  const sessionId = session?.sessionId;
  const clientId = session?.clientId;
  const [activeClientId, setActiveClientId] = useState(clientId);
  const [streamingState, setStreamingState] = useState<StreamingState>(
    StreamingState.Idle,
  );
  const [initError, setInitError] = useState<string | null>(null);
  const [pendingText, setPendingText] = useState('');
  const [isReceivingContent, setIsReceivingContent] = useState(false);
  const [pendingPermission, setPendingPermission] = useState<
    PendingDaemonPermission | undefined
  >(undefined);
  const [pendingToolGroup, setPendingToolGroup] = useState<
    HistoryItemToolGroup | undefined
  >(undefined);
  const [daemonSessionTitle, setDaemonSessionTitle] = useState<
    string | undefined
  >(undefined);
  const streamingResponseLengthRef = useRef(0);
  const activePromptIdRef = useRef<string | undefined>(undefined);
  const toolReducerStateRef = useRef(createDaemonTuiReducerState());
  const pendingToolGroupRef = useRef<HistoryItemToolGroup | undefined>(
    undefined,
  );
  const recoveryInFlightRef = useRef(false);

  const clearPendingState = useCallback(() => {
    setPendingText('');
    pendingToolGroupRef.current = undefined;
    setPendingToolGroup(undefined);
    streamingResponseLengthRef.current = 0;
    setIsReceivingContent(false);
  }, []);

  const commitPendingText = useCallback(() => {
    const toolGroup = pendingToolGroupRef.current;
    if (toolGroup) {
      addItem(toolGroup, Date.now());
      pendingToolGroupRef.current = undefined;
      setPendingToolGroup(undefined);
    }
    setPendingText((current) => {
      if (current) {
        addItem({ type: 'gemini', text: current }, Date.now());
      }
      return '';
    });
    streamingResponseLengthRef.current = 0;
    setIsReceivingContent(false);
  }, [addItem]);

  const handleEvent = useCallback(
    (evt: DaemonSessionEvent) => {
      switch (evt.event) {
        case 'session_update': {
          const payload = evt.data as {
            promptId?: string;
            data?: {
              update?: {
                sessionUpdate?: string;
                content?: { type: string; text?: string };
                toolCallId?: string;
              };
            };
            update?: {
              sessionUpdate?: string;
              content?: { type: string; text?: string };
              toolCallId?: string;
            };
          };
          // The daemon's typed SSE envelope carries the ACP update under
          // `data.update`; tolerate the older flat shape for compatibility.
          const update = payload.data?.update ?? payload.update;
          const kind = update?.sessionUpdate;
          const text = update?.content?.text;
          if (kind === 'user_message_chunk') {
            // The local submitter already renders its text optimistically;
            // every other co-driver (including the phone) must appear in the
            // terminal transcript from this daemon echo.
            if (text && payload.promptId !== activePromptIdRef.current) {
              addItem({ type: 'user', text }, Date.now());
            }
            break;
          }
          if (kind === 'agent_message_chunk' && text) {
            if (isToolProtocolArtifact(text)) {
              break;
            }
            setIsReceivingContent(true);
            streamingResponseLengthRef.current += text.length;
            setPendingText((current) => current + text);
            break;
          }
          if (kind === 'tool_call' || kind === 'tool_call_update') {
            // The daemon already emits the full ACP tool-call lifecycle; the
            // attached terminal used to discard it while the Web Shell showed
            // it. Reuse the established daemon→TUI reducer so MCP calls,
            // shell output, status, and errors render identically here.
            const updates = reduceDaemonEventToTuiUpdates(
              {
                id: evt.id,
                v: 1,
                type: 'session_update',
                data: { update },
              },
              toolReducerStateRef.current,
            );
            for (const toolUpdate of updates) {
              if (toolUpdate.type !== 'tool_group_update') continue;
              pendingToolGroupRef.current = toolUpdate.item;
              setPendingToolGroup(toolUpdate.item);
            }
          }
          break;
        }
        case 'session_metadata_updated': {
          // Title changes are emitted by the ACP child on the daemon event
          // bus, not through the attached terminal's local Config. Capture
          // them here so the terminal tag and Remote Control delivery both
          // use the meaningful auto/manual session title.
          const payload = evt.data as {
            sessionId?: string;
            displayName?: string;
            data?: { sessionId?: string; displayName?: string };
          };
          const metadata = payload.data ?? payload;
          if (
            metadata.sessionId === sessionId &&
            typeof metadata.displayName === 'string' &&
            metadata.displayName.trim()
          ) {
            setDaemonSessionTitle(metadata.displayName.trim());
          }
          break;
        }
        case 'permission_request': {
          const payload = evt.data as {
            data?: {
              requestId: string;
              toolCall: unknown;
              options: Array<{
                optionId: string;
                name?: string;
                kind?: string;
              }>;
            };
          };
          if (payload.data) {
            setStreamingState(StreamingState.WaitingForConfirmation);
            setPendingPermission({
              requestId: payload.data.requestId,
              toolCall: payload.data.toolCall,
              options: payload.data.options,
            });
          }
          break;
        }
        case 'turn_error': {
          const payload = evt.data as { data?: { message?: string } };
          commitPendingText();
          addItem(
            {
              type: 'error',
              text: payload.data?.message ?? 'Remote turn failed.',
            },
            Date.now(),
          );
          setStreamingState(StreamingState.Idle);
          break;
        }
        case 'prompt_cancelled': {
          commitPendingText();
          setStreamingState(StreamingState.Idle);
          break;
        }
        case 'turn_complete':
        case 'turn_finished':
        case 'stop': {
          commitPendingText();
          setStreamingState(StreamingState.Idle);
          break;
        }
        default:
          break;
      }
    },
    [addItem, commitPendingText, sessionId],
  );

  useEffect(() => {
    setDaemonSessionTitle(undefined);
    setActiveClientId(clientId);
    setPendingPermission(undefined);
    recoveryInFlightRef.current = false;
  }, [clientId, sessionId]);

  useEffect(() => {
    if (!baseUrl || !sessionId || !activeClientId) return;
    const controller = new AbortController();
    let disposed = false;
    void streamDaemonSessionEvents({
      baseUrl,
      sessionId,
      clientId: activeClientId,
      signal: controller.signal,
      onEvent: handleEvent,
      onError: (error) => {
        if (
          error instanceof DaemonEventStreamHttpError &&
          error.status === 404 &&
          !recoveryInFlightRef.current
        ) {
          recoveryInFlightRef.current = true;
          const requestedClientId = `terminal-${globalThis.crypto.randomUUID()}`;
          void resumeDaemonSession(baseUrl, sessionId, requestedClientId)
            .then(({ clientId: resumedClientId }) => {
              if (disposed) return;
              setInitError(null);
              setActiveClientId(resumedClientId);
            })
            .catch((resumeError) => {
              if (disposed) return;
              setStreamingState(StreamingState.Idle);
              setInitError(
                `The Canopy daemon restarted and this session could not be restored: ${
                  resumeError instanceof Error
                    ? resumeError.message
                    : String(resumeError)
                }`,
              );
            })
            .finally(() => {
              recoveryInFlightRef.current = false;
            });
          return;
        }
        setInitError(error.message);
      },
    });
    return () => {
      disposed = true;
      controller.abort();
    };
  }, [activeClientId, baseUrl, sessionId, handleEvent]);

  const submitQuery = useCallback(
    async (query: PartListUnion) => {
      if (!baseUrl || !sessionId || !activeClientId) return;
      const text =
        typeof query === 'string'
          ? query
          : Array.isArray(query)
            ? query
                .map((part) =>
                  typeof part === 'string'
                    ? part
                    : ((part as { text?: string }).text ?? ''),
                )
                .join('')
            : '';
      if (!text) return;
      addItem({ type: 'user', text }, Date.now());
      setStreamingState(StreamingState.Responding);
      try {
        const result = (await submitDaemonPrompt(
          baseUrl,
          sessionId,
          activeClientId,
          [{ type: 'text', text }],
        )) as { promptId?: string };
        activePromptIdRef.current = result.promptId;
      } catch (error) {
        setStreamingState(StreamingState.Idle);
        addItem(
          {
            type: 'error',
            text: `Failed to submit prompt to remote session: ${
              error instanceof Error ? error.message : String(error)
            }`,
          },
          Date.now(),
        );
      }
    },
    [activeClientId, addItem, baseUrl, sessionId],
  );

  const answerPermission = useCallback(
    async (
      requestId: string,
      outcome:
        | { outcome: 'selected'; optionId: string }
        | { outcome: 'cancelled' },
    ) => {
      if (!baseUrl || !sessionId || !activeClientId) return;
      await answerDaemonPermission(
        baseUrl,
        sessionId,
        activeClientId,
        requestId,
        outcome,
      );
      setPendingPermission((current) =>
        current?.requestId === requestId ? undefined : current,
      );
      setStreamingState(StreamingState.Responding);
    },
    [activeClientId, baseUrl, sessionId],
  );

  const cancelOngoingRequest = useCallback(() => {
    if (!baseUrl || !sessionId || !activeClientId) return;
    // Esc is synchronous at the TUI boundary, while cancellation is a daemon
    // RPC. Keep the turn marked active until its prompt_cancelled/turn_complete
    // SSE event arrives; that preserves the queue hand-off semantics.
    void cancelDaemonSession(baseUrl, sessionId, activeClientId).catch(
      (error) => {
        addItem(
          {
            type: 'error',
            text: `Failed to interrupt remote turn: ${
              error instanceof Error ? error.message : 'daemon rejected cancel'
            }`,
          },
          Date.now(),
        );
      },
    );
  }, [activeClientId, addItem, baseUrl, sessionId]);

  const noopAsync = useCallback(async () => {}, []);
  const noop = useCallback(() => {}, []);

  const pendingHistoryItems: HistoryItemWithoutId[] = [
    ...(pendingToolGroup ? [pendingToolGroup] : []),
    ...(pendingText
      ? [{ type: 'gemini_content' as const, text: pendingText }]
      : []),
  ];

  return {
    streamingState,
    submitQuery: submitQuery as unknown as ReturnType<
      typeof useGeminiStream
    >['submitQuery'],
    initError,
    pendingHistoryItems,
    clearPendingState,
    thought: null,
    cancelOngoingRequest,
    preemptGoalTurn: noop as unknown as ReturnType<
      typeof useGeminiStream
    >['preemptGoalTurn'],
    retryLastPrompt: noopAsync,
    pendingToolCalls: [],
    handleApprovalModeChange: noopAsync as unknown as ReturnType<
      typeof useGeminiStream
    >['handleApprovalModeChange'],
    activePtyId: undefined,
    loopDetectionConfirmationRequest: null,
    streamingResponseLengthRef,
    isReceivingContent,
    answerPermission,
    pendingPermission,
    sessionTitle: daemonSessionTitle,
  };
}
