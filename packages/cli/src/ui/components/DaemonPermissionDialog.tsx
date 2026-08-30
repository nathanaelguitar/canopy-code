/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { Box, Text } from 'ink';
import { useMemo, useState } from 'react';
import type { PendingDaemonPermission } from '../daemon-attach/use-daemon-stream.js';
import { useKeypress } from '../hooks/useKeypress.js';
import { theme } from '../semantic-colors.js';
import {
  RadioButtonSelect,
  type RadioSelectItem,
} from './shared/RadioButtonSelect.js';

type PermissionOutcome =
  | { outcome: 'selected'; optionId: string }
  | { outcome: 'cancelled' };

interface DaemonPermissionDialogProps {
  request: PendingDaemonPermission;
  onAnswer: (requestId: string, outcome: PermissionOutcome) => Promise<void>;
}

function toolLabel(toolCall: unknown): string {
  if (!toolCall || typeof toolCall !== 'object') return 'A remote tool';
  const record = toolCall as Record<string, unknown>;
  for (const key of ['title', 'name', 'toolName', 'toolCallId']) {
    const value = record[key];
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  return 'A remote tool';
}

export function DaemonPermissionDialog({
  request,
  onAnswer,
}: DaemonPermissionDialogProps) {
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string>();

  const answer = async (outcome: PermissionOutcome) => {
    if (submitting) return;
    setSubmitting(true);
    setError(undefined);
    try {
      await onAnswer(request.requestId, outcome);
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : 'The daemon rejected this permission response.',
      );
      setSubmitting(false);
    }
  };

  useKeypress(
    (key) => {
      if (key.name === 'escape') void answer({ outcome: 'cancelled' });
    },
    { isActive: !submitting },
  );

  const options = useMemo<Array<RadioSelectItem<PermissionOutcome>>>(() => {
    const choices: Array<RadioSelectItem<PermissionOutcome>> =
      request.options.map((option) => ({
        key: option.optionId,
        value: {
          outcome: 'selected' as const,
          optionId: option.optionId,
        },
        label: option.name ?? option.kind ?? option.optionId,
      }));
    choices.push({
      key: 'cancel',
      value: { outcome: 'cancelled' as const },
      label: 'Cancel (esc)',
    });
    return choices;
  }, [request.options]);

  return (
    <Box
      flexDirection="column"
      borderStyle="round"
      borderColor={theme.status.warning}
      paddingX={1}
      marginLeft={1}
      width="100%"
    >
      <Text bold color={theme.text.primary}>
        Permission required
      </Text>
      <Text color={theme.text.primary}>
        {toolLabel(request.toolCall)} is waiting for your approval.
      </Text>
      {error ? <Text color={theme.status.error}>{error}</Text> : null}
      {submitting ? (
        <Text color={theme.text.secondary}>Sending selection…</Text>
      ) : (
        <RadioButtonSelect
          items={options}
          onSelect={(outcome) => void answer(outcome)}
          isFocused
        />
      )}
    </Box>
  );
}
