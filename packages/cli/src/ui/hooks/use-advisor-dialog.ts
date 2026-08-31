/**
 * @license
 * Copyright 2025 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

import { useCallback, useRef, useState } from 'react';
import type { SubmitPromptResult } from '../types.js';

export interface AdvisorDialogSelection {
  model: string;
  reasoningEffort: string;
}

export interface AdvisorDialogOptions {
  initialModel?: string;
  initialReasoningEffort?: string;
  onSelect: (
    selection: AdvisorDialogSelection,
  ) => void | Promise<void | SubmitPromptResult>;
  onCancel: () => void;
}

export interface UseAdvisorDialogReturn {
  advisorDialog: AdvisorDialogOptions | null;
  openAdvisorDialog: (options: AdvisorDialogOptions) => void;
  closeAdvisorDialog: () => void;
  selectAdvisorDialog: (selection: AdvisorDialogSelection) => void;
}

export function useAdvisorDialog(): UseAdvisorDialogReturn {
  const [advisorDialog, setAdvisorDialog] =
    useState<AdvisorDialogOptions | null>(null);
  const advisorDialogRef = useRef<AdvisorDialogOptions | null>(null);

  const openAdvisorDialog = useCallback((options: AdvisorDialogOptions) => {
    advisorDialogRef.current = options;
    setAdvisorDialog(options);
  }, []);

  const closeAdvisorDialog = useCallback(() => {
    const current = advisorDialogRef.current;
    advisorDialogRef.current = null;
    setAdvisorDialog(null);
    current?.onCancel();
  }, []);

  const selectAdvisorDialog = useCallback(
    (selection: AdvisorDialogSelection) => {
      const current = advisorDialogRef.current;
      advisorDialogRef.current = null;
      setAdvisorDialog(null);
      if (current) void current.onSelect(selection);
    },
    [],
  );

  return {
    advisorDialog,
    openAdvisorDialog,
    closeAdvisorDialog,
    selectAdvisorDialog,
  };
}
