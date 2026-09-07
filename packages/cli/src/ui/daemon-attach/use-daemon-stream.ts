/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import type { RefObject } from 'react';
import type { PartListUnion } from '@google/genai';
import {
  createDebugLogger,
  ToolConfirmationOutcome,
  type ToolAskUserQuestionConfirmationDetails,
  type ToolCallConfirmationDetails,
  type ToolConfirmationPayload,
} from '@canopy-code/canopy-code-core';
import type { UseHistoryManagerReturn } from '../hooks/useHistoryManager.js';
import { StreamingState } from '../types.js';
import type { HistoryItemToolGroup, HistoryItemWithoutId } from '../types.js';
import type { useGeminiStream } from '../hooks/useGeminiStream.js';
import {
  createDaemonTuiReducerState,
  reduceDaemonEventToTuiUpdates,
} from '../daemon/daemon-tui-adapter.js';
import { sanitizeTerminalText } from '../utils/textUtils.js';
import {
  DaemonEventStreamHttpError,
  streamDaemonSessionEvents,
  submitDaemonPrompt,
  cancelDaemonSession,
  answerDaemonPermission,
  detachDaemonSession,
  loadDaemonSession,
  resumeDaemonSession,
  type DaemonPermissionResponse,
  type DaemonSessionEvent,
} from './daemon-session-events.js';

const MAX_RESYNC_ATTEMPTS = 3;
const RESYNC_BASE_DELAY_MS = 250;
const RESYNC_MAX_DELAY_MS = 4_000;
const MAX_TRACKED_DAEMON_EVENT_IDS = 8_192;
const debugLogger = createDebugLogger('DAEMON_STREAM');

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

type DaemonQuestion =
  ToolAskUserQuestionConfirmationDetails['questions'][number];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function stringField(
  value: Record<string, unknown>,
  ...keys: string[]
): string | undefined {
  for (const key of keys) {
    if (typeof value[key] === 'string' && value[key].trim()) {
      return value[key] as string;
    }
  }
  return undefined;
}

function getToolCallInput(toolCall: unknown): Record<string, unknown> {
  if (!isRecord(toolCall)) return {};
  const nested = toolCall['rawInput'] ?? toolCall['input'] ?? toolCall['args'];
  return isRecord(nested) ? nested : toolCall;
}

function extractDaemonQuestions(toolCall: unknown): DaemonQuestion[] {
  const toolCallRecord = isRecord(toolCall) ? toolCall : undefined;
  const input = getToolCallInput(toolCall);
  const metadata = isRecord(toolCallRecord?.['_meta'])
    ? toolCallRecord['_meta']
    : undefined;
  const rawQuestions =
    metadata?.['canopyQuestions'] ??
    metadata?.['qwenQuestions'] ??
    input['questions'] ??
    toolCallRecord?.['questions'];
  if (!Array.isArray(rawQuestions)) return [];

  return rawQuestions.flatMap((rawQuestion, index) => {
    if (!isRecord(rawQuestion)) return [];
    const rawOptions = rawQuestion['options'];
    const options = Array.isArray(rawOptions)
      ? rawOptions.flatMap((rawOption) => {
          if (!isRecord(rawOption)) return [];
          return [
            {
              label:
                stringField(rawOption, 'label', 'title', 'value') ?? 'Option',
              description:
                stringField(rawOption, 'description', 'detail', 'text') ?? '',
            },
          ];
        })
      : [];

    return [
      {
        header:
          stringField(rawQuestion, 'header', 'title', 'label') ??
          `Question ${index + 1}`,
        question:
          stringField(rawQuestion, 'question', 'prompt', 'text') ?? 'Question',
        options,
        ...(typeof rawQuestion['multiSelect'] === 'boolean'
          ? { multiSelect: rawQuestion['multiSelect'] }
          : {}),
      },
    ];
  });
}

function getPermissionOptionId(
  permission: PendingDaemonPermission,
  outcome: ToolConfirmationOutcome,
): string | undefined {
  return (
    permission.options.find((option) => option.optionId === outcome)
      ?.optionId ??
    permission.options.find((option) => option.kind === 'allow_once')
      ?.optionId ??
    permission.options[0]?.optionId
  );
}

export function createDaemonConfirmation(
  permission: PendingDaemonPermission,
  answer: (
    requestId: string,
    response: DaemonPermissionResponse,
  ) => Promise<void>,
): ToolCallConfirmationDetails {
  const questions = extractDaemonQuestions(permission.toolCall);
  const toolCall = isRecord(permission.toolCall) ? permission.toolCall : {};
  const title =
    stringField(toolCall, 'title', 'name', 'toolName') ?? 'Permission required';

  const onConfirm = async (
    outcome: ToolConfirmationOutcome,
    payload?: ToolConfirmationPayload,
  ) => {
    if (outcome === ToolConfirmationOutcome.Cancel) {
      await answer(permission.requestId, { outcome: { outcome: 'cancelled' } });
      return;
    }

    const optionId = getPermissionOptionId(permission, outcome);
    if (!optionId) {
      await answer(permission.requestId, { outcome: { outcome: 'cancelled' } });
      return;
    }

    await answer(permission.requestId, {
      outcome: { outcome: 'selected', optionId },
      ...(payload?.answers ? { answers: payload.answers } : {}),
    });
  };

  if (questions.length > 0) {
    return {
      type: 'ask_user_question',
      title,
      questions,
      onConfirm,
    };
  }

  const input = getToolCallInput(permission.toolCall);
  const command = stringField(input, 'command', 'cmd');
  return {
    type: 'info',
    title,
    prompt: command
      ? `Command: ${command}`
      : `${title} is requesting permission.`,
    renderPromptAsPlainText: true,
    hideAlwaysAllow: true,
    onConfirm,
  };
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
    answers?: Record<string, string>,
  ) => Promise<void>;
  pendingPermission: PendingDaemonPermission | undefined;
  /**
   * The durable session name reported by the daemon. The ACP child owns
   * auto-title generation, so an attached terminal must consume this event
   * instead of relying on its local (non-recording) Config instance.
   */
  sessionTitle: string | undefined;
  /**
   * Pollable daemon connection health. The ref identity is stable and the
   * object is mutated in place on every applied event, so polling it never
   * triggers renders by itself — consumers read `.current` on their own
   * animation tick, mirroring `streamingResponseLengthRef`. Powers the
   * catching-up / stalled / degraded-cursor indicators.
   */
  daemonHealthRef: RefObject<DaemonHealth>;
}

/**
 * Live connection health for a daemon-attached TUI. All fields describe the
 * attached stream; `lastFrameAtMs` drives the stalled-stream detector.
 */
export interface DaemonHealth {
  isResyncing: boolean;
  resyncAttempts: number;
  lastEventId: number | undefined;
  eventEpoch: string | undefined;
  /** Wall-clock ms of the last applied sequenced event (0 = none yet). */
  lastFrameAtMs: number;
  activeClientId: string | undefined;
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
  clearHistory?: UseHistoryManagerReturn['clearItems'],
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
  const [isResyncing, setIsResyncing] = useState(false);
  const streamingResponseLengthRef = useRef(0);
  const activePromptIdRef = useRef<string | undefined>(undefined);
  const toolReducerStateRef = useRef(createDaemonTuiReducerState());
  const pendingToolGroupRef = useRef<HistoryItemToolGroup | undefined>(
    undefined,
  );
  const recoveryInFlightRef = useRef(false);
  const pendingTextRef = useRef('');
  const pendingRenderTimerRef = useRef<
    ReturnType<typeof setTimeout> | undefined
  >(undefined);
  const resyncCursorRef = useRef<number | undefined>(undefined);
  const isResyncingRef = useRef(false);
  const rebuildingHistoryRef = useRef(false);
  const eventEpochRef = useRef<string | undefined>(undefined);
  const processedEventIdsRef = useRef(new Set<number>());
  const pendingPermissionRef = useRef<PendingDaemonPermission | undefined>(
    undefined,
  );
  const resyncSawTerminalRef = useRef(false);
  const resyncResolvedPermissionIdsRef = useRef(new Set<string>());
  const resyncAttemptsRef = useRef(0);
  const resyncDelayTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(
    undefined,
  );
  const detachInFlightRef = useRef(new Map<string, Promise<void>>());
  const daemonHealthRef = useRef<DaemonHealth>({
    isResyncing: false,
    resyncAttempts: 0,
    lastEventId: undefined,
    eventEpoch: undefined,
    lastFrameAtMs: 0,
    activeClientId: clientId,
  });

  const updateEventEpoch = useCallback((epoch: string) => {
    if (
      eventEpochRef.current !== undefined &&
      eventEpochRef.current !== epoch
    ) {
      processedEventIdsRef.current.clear();
    }
    eventEpochRef.current = epoch;
    daemonHealthRef.current.eventEpoch = epoch;
  }, []);

  const setResyncing = useCallback((value: boolean) => {
    isResyncingRef.current = value;
    daemonHealthRef.current.isResyncing = value;
    setIsResyncing(value);
  }, []);

  const detachClientWithRetry = useCallback(
    (clientToDetach: string) => {
      const existing = detachInFlightRef.current.get(clientToDetach);
      if (existing) return existing;
      const operation = (async () => {
        let lastError: unknown;
        for (let attempt = 0; attempt < 3; attempt++) {
          try {
            await detachDaemonSession(
              baseUrl ?? '',
              sessionId ?? '',
              clientToDetach,
            );
            return;
          } catch (error) {
            lastError = error;
            if (attempt < 2) {
              await new Promise((resolve) =>
                setTimeout(resolve, 250 * 2 ** attempt),
              );
            }
          }
        }
        debugLogger.warn(
          `[Canopy] Failed to detach daemon client ${clientToDetach}: ${
            lastError instanceof Error ? lastError.message : String(lastError)
          }`,
        );
      })().finally(() => {
        detachInFlightRef.current.delete(clientToDetach);
      });
      detachInFlightRef.current.set(clientToDetach, operation);
      return operation;
    },
    [baseUrl, sessionId],
  );

  const flushPendingRender = useCallback(() => {
    pendingRenderTimerRef.current = undefined;
    setPendingText(pendingTextRef.current);
    setPendingToolGroup(pendingToolGroupRef.current);
  }, []);

  const schedulePendingRender = useCallback(() => {
    if (pendingRenderTimerRef.current !== undefined) return;
    // Tool progress can arrive many times per second. Rendering every SSE
    // frame makes Ink retain a long chain of intermediate trees and can make
    // long browser-driven turns unnecessarily expensive for the TUI.
    pendingRenderTimerRef.current = setTimeout(flushPendingRender, 50);
  }, [flushPendingRender]);

  const clearPendingState = useCallback(() => {
    if (pendingRenderTimerRef.current !== undefined) {
      clearTimeout(pendingRenderTimerRef.current);
      pendingRenderTimerRef.current = undefined;
    }
    pendingTextRef.current = '';
    setPendingText('');
    pendingToolGroupRef.current = undefined;
    setPendingToolGroup(undefined);
    pendingPermissionRef.current = undefined;
    setPendingPermission(undefined);
    setStreamingState(StreamingState.Idle);
    setResyncing(false);
    streamingResponseLengthRef.current = 0;
    setIsReceivingContent(false);
  }, [setResyncing]);

  const commitPendingText = useCallback(() => {
    if (pendingRenderTimerRef.current !== undefined) {
      clearTimeout(pendingRenderTimerRef.current);
      pendingRenderTimerRef.current = undefined;
    }
    const toolGroup = pendingToolGroupRef.current;
    if (toolGroup) {
      addItem(toolGroup, Date.now());
      pendingToolGroupRef.current = undefined;
      setPendingToolGroup(undefined);
    }
    const text = pendingTextRef.current;
    pendingTextRef.current = '';
    if (text) addItem({ type: 'gemini', text }, Date.now());
    setPendingText('');
    streamingResponseLengthRef.current = 0;
    setIsReceivingContent(false);
  }, [addItem]);

  const handleEvent = useCallback(
    (evt: DaemonSessionEvent) => {
      if (evt.id !== undefined) {
        if (processedEventIdsRef.current.has(evt.id)) return;
        processedEventIdsRef.current.add(evt.id);
        daemonHealthRef.current.lastEventId = evt.id;
        daemonHealthRef.current.lastFrameAtMs = Date.now();
        while (
          processedEventIdsRef.current.size > MAX_TRACKED_DAEMON_EVENT_IDS
        ) {
          const oldest = processedEventIdsRef.current.values().next().value;
          if (typeof oldest !== 'number') break;
          processedEventIdsRef.current.delete(oldest);
        }
        if (!isResyncingRef.current) resyncAttemptsRef.current = 0;
      }
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
          const text =
            typeof update?.content?.text === 'string'
              ? sanitizeTerminalText(update.content.text)
              : undefined;
          if (kind === 'user_message_chunk') {
            // The local submitter already renders its text optimistically;
            // every other co-driver (including the phone) must appear in the
            // terminal transcript from this daemon echo.
            if (
              text &&
              (rebuildingHistoryRef.current ||
                payload.promptId !== activePromptIdRef.current)
            ) {
              addItem({ type: 'user', text }, Date.now());
            }
            break;
          }
          if (kind === 'agent_message_chunk' && text) {
            if (isToolProtocolArtifact(text)) {
              break;
            }
            setStreamingState(StreamingState.Responding);
            setIsReceivingContent(true);
            streamingResponseLengthRef.current += text.length;
            pendingTextRef.current += text;
            schedulePendingRender();
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
              schedulePendingRender();
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
            const permission = {
              requestId: payload.data.requestId,
              toolCall: payload.data.toolCall,
              options: payload.data.options,
            };
            pendingPermissionRef.current = permission;
            setStreamingState(StreamingState.WaitingForConfirmation);
            setPendingPermission(permission);
          }
          break;
        }
        case 'permission_resolved': {
          const payload = evt.data as {
            data?: { requestId?: string };
            requestId?: string;
          };
          const requestId = payload.data?.requestId ?? payload.requestId;
          if (requestId) {
            if (isResyncingRef.current) {
              resyncResolvedPermissionIdsRef.current.add(requestId);
            }
            if (pendingPermissionRef.current?.requestId === requestId) {
              pendingPermissionRef.current = undefined;
            }
            setPendingPermission((current) =>
              current?.requestId === requestId ? undefined : current,
            );
            setStreamingState((current) =>
              current === StreamingState.WaitingForConfirmation
                ? StreamingState.Responding
                : current,
            );
          }
          break;
        }
        case 'turn_error': {
          if (isResyncingRef.current) resyncSawTerminalRef.current = true;
          const payload = evt.data as { data?: { message?: string } };
          commitPendingText();
          pendingPermissionRef.current = undefined;
          setPendingPermission(undefined);
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
          if (isResyncingRef.current) resyncSawTerminalRef.current = true;
          commitPendingText();
          pendingPermissionRef.current = undefined;
          setPendingPermission(undefined);
          setStreamingState(StreamingState.Idle);
          break;
        }
        case 'turn_complete':
        case 'turn_finished':
        case 'stop': {
          if (isResyncingRef.current) resyncSawTerminalRef.current = true;
          commitPendingText();
          pendingPermissionRef.current = undefined;
          setPendingPermission(undefined);
          setStreamingState(StreamingState.Idle);
          break;
        }
        case 'replay_complete':
          setResyncing(false);
          break;
        case 'client_evicted':
        case 'stream_error': {
          const payload = evt.data as {
            reason?: unknown;
            message?: unknown;
            data?: { reason?: unknown; message?: unknown };
          };
          const detail = payload.data ?? payload;
          setResyncing(false);
          setStreamingState(StreamingState.Idle);
          setInitError(
            `Daemon event stream ended: ${
              typeof detail.message === 'string'
                ? detail.message
                : typeof detail.reason === 'string'
                  ? detail.reason
                  : evt.event
            }`,
          );
          break;
        }
        default:
          break;
      }
    },
    [
      addItem,
      commitPendingText,
      schedulePendingRender,
      sessionId,
      setResyncing,
    ],
  );

  useEffect(() => {
    setDaemonSessionTitle(undefined);
    setActiveClientId(clientId);
    resyncCursorRef.current = undefined;
    eventEpochRef.current = undefined;
    processedEventIdsRef.current.clear();
    pendingTextRef.current = '';
    pendingPermissionRef.current = undefined;
    resyncResolvedPermissionIdsRef.current.clear();
    resyncAttemptsRef.current = 0;
    isResyncingRef.current = false;
    setIsResyncing(false);
    Object.assign(daemonHealthRef.current, {
      isResyncing: false,
      resyncAttempts: 0,
      lastEventId: undefined,
      eventEpoch: undefined,
      lastFrameAtMs: 0,
      activeClientId: clientId,
    });
    setPendingPermission(undefined);
    recoveryInFlightRef.current = false;
  }, [clientId, sessionId]);

  useEffect(
    () => () => {
      if (pendingRenderTimerRef.current !== undefined) {
        clearTimeout(pendingRenderTimerRef.current);
      }
      if (resyncDelayTimerRef.current !== undefined) {
        clearTimeout(resyncDelayTimerRef.current);
      }
    },
    [],
  );

  useEffect(() => {
    if (!baseUrl || !sessionId || !activeClientId) return;
    const controller = new AbortController();
    let disposed = false;
    void streamDaemonSessionEvents({
      baseUrl,
      sessionId,
      clientId: activeClientId,
      ...(resyncCursorRef.current !== undefined
        ? { lastEventId: resyncCursorRef.current }
        : {}),
      ...(eventEpochRef.current !== undefined
        ? { eventEpoch: eventEpochRef.current }
        : {}),
      signal: controller.signal,
      onEvent: handleEvent,
      onEpoch: updateEventEpoch,
      onResyncRequired: (reason) => {
        if (disposed || recoveryInFlightRef.current) return;
        recoveryInFlightRef.current = true;
        const oldClientId = activeClientId;
        const requestedClientId = `terminal-${globalThis.crypto.randomUUID()}`;
        const attempt = resyncAttemptsRef.current + 1;
        resyncAttemptsRef.current = attempt;
        daemonHealthRef.current.resyncAttempts = attempt;
        setResyncing(true);
        debugLogger.warn(
          `[Canopy] Daemon session resync requested${
            reason ? ` (${reason})` : ''
          }; attempt ${attempt}/${MAX_RESYNC_ATTEMPTS}`,
        );
        controller.abort();
        if (attempt > MAX_RESYNC_ATTEMPTS) {
          setResyncing(false);
          setStreamingState(StreamingState.Idle);
          setInitError(
            `The daemon session could not resync after ${MAX_RESYNC_ATTEMPTS} attempts${
              reason ? ` (${reason})` : ''
            }. Restart Canopy to reconnect.`,
          );
          recoveryInFlightRef.current = false;
          return;
        }
        const delayMs = Math.min(
          RESYNC_BASE_DELAY_MS * 2 ** (attempt - 1),
          RESYNC_MAX_DELAY_MS,
        );
        resyncDelayTimerRef.current = setTimeout(() => {
          resyncDelayTimerRef.current = undefined;
          void (async () => {
            try {
              const resync = await loadDaemonSession(
                baseUrl,
                sessionId,
                requestedClientId,
              );
              // Always release the old attachment, including when the TUI
              // unmounted while /load was in flight.
              void detachClientWithRetry(oldClientId);
              if (disposed) return;
              if (resync.eventEpoch) updateEventEpoch(resync.eventEpoch);

              const previousPermission = pendingPermissionRef.current;
              resyncSawTerminalRef.current = false;
              resyncResolvedPermissionIdsRef.current.clear();
              const replayEvents = [
                ...(resync.compactedReplay ?? []),
                ...(resync.liveJournal ?? []),
              ];
              const rebuildHistory =
                Boolean(clearHistory) &&
                (resync.compactedReplay?.length ?? 0) > 0;
              if (rebuildHistory) {
                clearHistory?.();
                processedEventIdsRef.current.clear();
              } else if (resync.compactedReplay?.length) {
                // Even without a history setter (for example in an embedded
                // consumer), the reducer must see the complete snapshot.
                processedEventIdsRef.current.clear();
              }
              clearPendingState();
              setResyncing(true);
              toolReducerStateRef.current = createDaemonTuiReducerState();
              rebuildingHistoryRef.current = rebuildHistory;
              try {
                for (const event of replayEvents) {
                  handleEvent(event);
                }
              } finally {
                rebuildingHistoryRef.current = false;
              }
              setResyncing(false);
              if (
                previousPermission &&
                !pendingPermissionRef.current &&
                !resyncSawTerminalRef.current &&
                !resyncResolvedPermissionIdsRef.current.has(
                  previousPermission.requestId,
                )
              ) {
                pendingPermissionRef.current = previousPermission;
                setPendingPermission(previousPermission);
                setStreamingState(StreamingState.WaitingForConfirmation);
              }
              resyncCursorRef.current = resync.lastEventId;
              setInitError(null);
              daemonHealthRef.current.activeClientId = resync.clientId;
              setActiveClientId(resync.clientId);
            } catch (resyncError) {
              if (disposed) return;
              setResyncing(false);
              setStreamingState(StreamingState.Idle);
              setInitError(
                `The daemon lost part of this session stream and could not resync: ${
                  resyncError instanceof Error
                    ? resyncError.message
                    : String(resyncError)
                }`,
              );
            } finally {
              recoveryInFlightRef.current = false;
            }
          })();
        }, delayMs);
      },
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
              eventEpochRef.current = undefined;
              processedEventIdsRef.current.clear();
              setInitError(null);
              daemonHealthRef.current.activeClientId = resumedClientId;
              daemonHealthRef.current.eventEpoch = undefined;
              daemonHealthRef.current.lastEventId = undefined;
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
      if (resyncDelayTimerRef.current !== undefined) {
        clearTimeout(resyncDelayTimerRef.current);
        resyncDelayTimerRef.current = undefined;
      }
      void detachClientWithRetry(activeClientId);
    };
  }, [
    activeClientId,
    baseUrl,
    clearPendingState,
    clearHistory,
    detachClientWithRetry,
    handleEvent,
    sessionId,
    setResyncing,
    updateEventEpoch,
  ]);

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
      // Baseline the stall detector: the daemon may take a while to emit
      // the first event of a turn, which must not read as a stall.
      daemonHealthRef.current.lastFrameAtMs = Date.now();
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
      answers?: Record<string, string>,
    ) => {
      if (!baseUrl || !sessionId || !activeClientId) return;
      await answerDaemonPermission(
        baseUrl,
        sessionId,
        activeClientId,
        requestId,
        {
          outcome,
          ...(answers ? { answers } : {}),
        },
      );
      pendingPermissionRef.current = undefined;
      setPendingPermission((current) =>
        current?.requestId === requestId ? undefined : current,
      );
      daemonHealthRef.current.lastFrameAtMs = Date.now();
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
    ...(isResyncing
      ? [{ type: 'info' as const, text: 'Catching up with daemon session…' }]
      : []),
    ...(pendingToolGroup ? [pendingToolGroup] : []),
    ...(pendingText
      ? [{ type: 'gemini_content' as const, text: pendingText }]
      : []),
  ];

  // Pending daemon permissions are rendered by DialogManager via
  // DaemonPermissionDialog. Do not also inject a ToolConfirmationMessage into
  // the pending transcript: that creates two competing approval prompts (the
  // generic "Do you want to proceed?" plus the real daemon request) and makes
  // the question/options ambiguous in the TUI.

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
    daemonHealthRef,
  };
}
