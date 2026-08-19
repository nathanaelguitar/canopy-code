/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import {
  SessionService,
  Storage,
  type SessionServiceOptions,
} from '@canopy-code/canopy-code-core';
import type { WorkspaceRuntime } from './workspace-registry.js';

export function runWithWorkspaceRuntimeStorage<T>(
  runtime: WorkspaceRuntime,
  fn: () => T,
): T {
  return Storage.runWithResolvedRuntimeBaseDir(
    runtime.sessionRuntimeBaseDir,
    fn,
  );
}

export function createWorkspaceRuntimeSessionService(
  runtime: WorkspaceRuntime,
  options: Omit<SessionServiceOptions, 'runtimeBaseDir'> = {},
): SessionService {
  return new SessionService(runtime.workspaceCwd, {
    ...options,
    runtimeBaseDir: runtime.sessionRuntimeBaseDir,
  });
}
