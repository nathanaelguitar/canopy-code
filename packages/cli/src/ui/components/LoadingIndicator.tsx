/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import type React from 'react';
import { useRef, type RefObject } from 'react';
import { Box, Text } from 'ink';
import { theme } from '../semantic-colors.js';
import { useStreamingContext } from '../contexts/StreamingContext.js';
import type { DaemonHealth } from '../daemon-attach/use-daemon-stream.js';
import { StreamingState } from '../types.js';
import { GeminiRespondingSpinner } from './GeminiRespondingSpinner.js';
import { formatDuration, formatTokenCount } from '../utils/formatters.js';
import { useTerminalSize } from '../hooks/useTerminalSize.js';
import { useAnimationFrame } from '../hooks/useAnimationFrame.js';
import { isNarrowWidth } from '../utils/isNarrowWidth.js';
import { t } from '../../i18n/index.js';

interface LoadingIndicatorProps {
  currentLoadingPhrase?: string;
  elapsedTime: number;
  rightContent?: React.ReactNode;
  candidatesTokens?: number;
  taskStartTokens?: number;
  taskStartStreamingChars?: number;
  /**
   * Live-updating character counter for the streaming response. When provided
   * together with `isStreaming`, the indicator animates a token estimate
   * (chars / 4) internally, so the animation never re-renders `Composer` or
   * the input prompt.
   */
  streamingCharsRef?: React.RefObject<number>;
  /** Whether to poll `streamingCharsRef` (true during Responding/WaitingForConfirmation). */
  isStreaming?: boolean;
  /** Show live response speed next to the token counter. */
  showResponseTokensPerSecond?: boolean;
  /**
   * Daemon-attached connection health (absent in local mode). Read on the
   * existing animation tick below — it never triggers renders by itself.
   * Powers the catching-up override and the stalled-stream warning.
   */
  daemonHealthRef?: RefObject<DaemonHealth>;
  /**
   * True when receiving content (shows ↓ arrow), false when waiting for API
   * response (shows ↑ arrow).
   * @default true
   */
  isReceivingContent?: boolean;
}

/** No daemon SSE frames for this long during a turn reads as a stall. */
const STALLED_AFTER_MS = 10_000;

export const LoadingIndicator: React.FC<LoadingIndicatorProps> = ({
  currentLoadingPhrase,
  elapsedTime,
  rightContent,
  candidatesTokens,
  taskStartTokens = 0,
  taskStartStreamingChars = 0,
  streamingCharsRef,
  isStreaming,
  showResponseTokensPerSecond = false,
  daemonHealthRef,
  isReceivingContent = true,
}) => {
  const streamingState = useStreamingContext();
  const { columns: terminalWidth } = useTerminalSize();
  const isNarrow = isNarrowWidth(terminalWidth);

  // Animate the streaming-chars counter locally so only this component
  // re-renders on each animation frame (100ms ≈ spinner cadence). Siblings
  // like InputPrompt / Footer stay static, which eliminates terminal flicker
  // during streaming output.
  const fallbackRef = useRef(0);
  const animatedChars = useAnimationFrame(
    streamingCharsRef ?? fallbackRef,
    streamingCharsRef && isStreaming ? 100 : null,
  );

  const health = daemonHealthRef?.current;
  const catchingUp = health?.isResyncing === true;

  if (streamingState === StreamingState.Idle && !catchingUp) {
    return null;
  }

  // Daemon-attached overlays. `animatedChars` above re-renders this component
  // ~10x/s while streaming, so the stall clock stays fresh without its own
  // timer. A silent spinner is how a dead-attached TUI looks identical to a
  // working one — the stalled warning must be loud instead.
  const frameGapMs =
    !catchingUp &&
    streamingState === StreamingState.Responding &&
    (health?.lastFrameAtMs ?? 0) > 0
      ? Date.now() - (health?.lastFrameAtMs ?? 0)
      : 0;
  const stalled = frameGapMs > STALLED_AFTER_MS;

  // The spinner row shows status only: phrase, timer, token estimate, and the
  // cancel affordance. Model reasoning lives in the collapsible thinking block
  // in history, not here.
  const primaryText = catchingUp
    ? t('Catching up with daemon session…')
    : currentLoadingPhrase;

  const streamingTokens = streamingCharsRef ? Math.round(animatedChars / 4) : 0;
  const outputTokens = (candidatesTokens ?? 0) + streamingTokens;
  const taskStartStreamingTokens = streamingCharsRef
    ? Math.round(taskStartStreamingChars / 4)
    : 0;
  const outputTokensSinceTimerStart = Math.max(
    0,
    outputTokens - taskStartTokens - taskStartStreamingTokens,
  );
  const showTokens = !isNarrow && outputTokens > 0;
  const tokenArrow = isReceivingContent ? '↓' : '↑';

  // Keep the timer's sub-second precision for rate calculations, but display
  // only completed whole seconds in the status line.
  const timeStr =
    elapsedTime < 60
      ? `${Math.floor(Math.max(0, elapsedTime))}s`
      : formatDuration(elapsedTime * 1000);

  const tokenStr = showTokens
    ? ` · ${tokenArrow} ${formatTokenCount(outputTokens)} tokens`
    : '';
  const tokenRateStr =
    showTokens &&
    showResponseTokensPerSecond &&
    isReceivingContent &&
    elapsedTime > 0
      ? ` · ${formatTokensPerSecond(outputTokensSinceTimerStart / elapsedTime)}`
      : '';

  const cancelAndTimerContent =
    streamingState !== StreamingState.WaitingForConfirmation
      ? t('({{time}}{{tokens}} · esc to cancel)', {
          time: timeStr,
          tokens: `${tokenStr}${tokenRateStr}`,
        })
      : null;

  if (stalled) {
    const cursorBits = [
      health?.lastEventId !== undefined ? `#${health.lastEventId}` : '',
      health?.eventEpoch ? `epoch ${health.eventEpoch.slice(0, 8)}` : '',
    ].filter(Boolean);
    return (
      <Box paddingLeft={2} flexDirection="column">
        <Box width="100%" flexDirection="row" alignItems="center">
          <Box marginRight={1}>
            <GeminiRespondingSpinner nonRespondingDisplay="" />
          </Box>
          <Text color={theme.status.warning} wrap="truncate-end">
            {t(
              'Stalled — no daemon frames for {{seconds}}s{{cursor}} · esc to cancel',
              {
                seconds: String(Math.floor(frameGapMs / 1000)),
                cursor:
                  cursorBits.length > 0 ? ` · ${cursorBits.join(' · ')}` : '',
              },
            )}
          </Text>
          {!isNarrow && <Box flexGrow={1}>{/* Spacer */}</Box>}
          {!isNarrow && rightContent && <Box>{rightContent}</Box>}
        </Box>
        {isNarrow && rightContent && <Box>{rightContent}</Box>}
      </Box>
    );
  }

  return (
    <Box paddingLeft={2} flexDirection="column">
      {/* Main loading line */}
      <Box
        width="100%"
        flexDirection={isNarrow ? 'column' : 'row'}
        alignItems={isNarrow ? 'flex-start' : 'center'}
      >
        <Box>
          <Box marginRight={1}>
            <GeminiRespondingSpinner
              nonRespondingDisplay={
                streamingState === StreamingState.WaitingForConfirmation
                  ? '⠏'
                  : ''
              }
            />
          </Box>
          {primaryText && (
            <Text color={theme.text.accent} wrap="truncate-end">
              {primaryText}
            </Text>
          )}
          {!isNarrow && cancelAndTimerContent && (
            <Text color={theme.text.secondary}> {cancelAndTimerContent}</Text>
          )}
        </Box>
        {!isNarrow && <Box flexGrow={1}>{/* Spacer */}</Box>}
        {!isNarrow && rightContent && <Box>{rightContent}</Box>}
      </Box>
      {isNarrow && cancelAndTimerContent && (
        <Box>
          <Text color={theme.text.secondary}>{cancelAndTimerContent}</Text>
        </Box>
      )}
      {isNarrow && rightContent && <Box>{rightContent}</Box>}
    </Box>
  );
};

function formatTokensPerSecond(tokensPerSecond: number): string {
  if (!Number.isFinite(tokensPerSecond) || tokensPerSecond <= 0) {
    return '0 t/s';
  }

  const rounded =
    tokensPerSecond >= 10
      ? Math.round(tokensPerSecond).toString()
      : tokensPerSecond.toFixed(1);

  return `${rounded} t/s`;
}
