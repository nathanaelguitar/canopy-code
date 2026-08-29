/**
 * @license
 * Copyright 2025 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

import type React from 'react';
import { useMemo, useState } from 'react';
import { Box, Text } from 'ink';
import {
  REASONING_EFFORT_TIERS,
  type Config,
  type ReasoningEffort,
} from '@canopy-code/canopy-code-core';
import { useConfig } from '../contexts/ConfigContext.js';
import { useKeypress } from '../hooks/useKeypress.js';
import { theme } from '../semantic-colors.js';
import { DescriptiveRadioButtonSelect } from './shared/DescriptiveRadioButtonSelect.js';
import { RadioButtonSelect } from './shared/RadioButtonSelect.js';
import type { AdvisorDialogSelection } from '../hooks/use-advisor-dialog.js';

interface AdvisorDialogProps {
  initialModel?: string;
  initialReasoningEffort?: string;
  onSelect: (selection: AdvisorDialogSelection) => void;
  onCancel: () => void;
}

interface AdvisorModelOption {
  value: string;
  label: string;
  description: string;
  key: string;
}

const EFFORT_DESCRIPTIONS: Record<ReasoningEffort, string> = {
  low: 'Fastest and cheapest; least reasoning.',
  medium: 'Balanced speed, cost, and reasoning.',
  high: 'Strong reasoning for difficult reviews.',
  xhigh: 'Extended reasoning for complex reviews.',
  max: 'Maximum reasoning; highest cost and latency.',
};

function modelSelectorValue(model: { authType: string; id: string }): string {
  return `${model.authType}:${model.id}`;
}

function configuredModels(config: Config | null): AdvisorModelOption[] {
  const models = config?.getAllConfiguredModels?.() ?? [];
  const seen = new Set<string>();
  const options: AdvisorModelOption[] = [];

  for (const model of models) {
    if (model.imageOnly) continue;
    const value = modelSelectorValue(model);
    if (seen.has(value)) continue;
    seen.add(value);
    options.push({
      value,
      label: `[${model.authType}] ${model.label || model.id}`,
      description: model.description || model.id,
      key: value,
    });
  }

  return options.sort((a, b) => a.label.localeCompare(b.label));
}

function findInitialModel(
  options: AdvisorModelOption[],
  initialModel: string | undefined,
): number {
  if (!initialModel) return 0;
  const normalized = initialModel.replace(
    /^(chatgpt-oauth:)?gpt-5\.6$/,
    '$1gpt-5.6-sol',
  );
  const index = options.findIndex(
    (option) =>
      option.value === normalized || option.value.endsWith(`:${normalized}`),
  );
  return index >= 0 ? index : 0;
}

export function AdvisorDialog({
  initialModel,
  initialReasoningEffort,
  onSelect,
  onCancel,
}: AdvisorDialogProps): React.JSX.Element {
  const config = useConfig();
  const [step, setStep] = useState<'model' | 'effort'>('model');
  const modelOptions = useMemo(() => {
    const options = configuredModels(config);
    if (options.length > 0) return options;
    const fallback = initialModel?.trim();
    return fallback
      ? [{ value: fallback, label: fallback, description: '', key: fallback }]
      : [];
  }, [config, initialModel]);
  const [selectedModel, setSelectedModel] = useState(
    () =>
      modelOptions[findInitialModel(modelOptions, initialModel)]?.value ?? '',
  );

  const effortInitialIndex = Math.max(
    0,
    REASONING_EFFORT_TIERS.indexOf(initialReasoningEffort as ReasoningEffort),
  );

  useKeypress(
    (key) => {
      if (key.name !== 'escape') return;
      if (step === 'effort') {
        setStep('model');
      } else {
        onCancel();
      }
    },
    { isActive: true },
  );

  if (step === 'effort') {
    const effortItems = REASONING_EFFORT_TIERS.map((effort) => ({
      value: effort,
      label: `${effort} — ${EFFORT_DESCRIPTIONS[effort]}`,
      key: effort,
    }));
    return (
      <Box
        borderStyle="round"
        borderColor={theme.border.default}
        flexDirection="column"
        padding={1}
        width="100%"
      >
        <Text bold>{'> Choose advisor reasoning effort'}</Text>
        <Box height={1} />
        <RadioButtonSelect
          items={effortItems}
          initialIndex={effortInitialIndex}
          onSelect={(reasoningEffort) =>
            onSelect({ model: selectedModel, reasoningEffort })
          }
          isFocused
          showNumbers
        />
        <Box marginTop={1}>
          <Text color={theme.text.secondary}>
            {'Enter to use · Esc to return to model list'}
          </Text>
        </Box>
      </Box>
    );
  }

  return (
    <Box
      borderStyle="round"
      borderColor={theme.border.default}
      flexDirection="column"
      padding={1}
      width="100%"
    >
      <Text bold>{'> Choose advisor model'}</Text>
      <Box height={1} />
      {modelOptions.length > 0 ? (
        <DescriptiveRadioButtonSelect
          items={modelOptions.map((option) => ({
            value: option.value,
            title: option.label,
            description: option.description,
            key: option.key,
          }))}
          initialIndex={findInitialModel(modelOptions, initialModel)}
          onSelect={(model) => {
            setSelectedModel(model);
            setStep('effort');
          }}
          isFocused
          showNumbers
          showScrollArrows
          maxItemsToShow={10}
        />
      ) : (
        <Text color={theme.status.error}>
          No configured advisor models are available. Sign in or configure a
          provider first.
        </Text>
      )}
      <Box marginTop={1}>
        <Text color={theme.text.secondary}>
          {'↑/↓ to scroll · Enter to choose · Esc to cancel'}
        </Text>
      </Box>
    </Box>
  );
}
