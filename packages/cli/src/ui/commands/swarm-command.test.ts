/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, it, vi } from 'vitest';
import type {
  Config,
  GoalRuntime,
  GoalSnapshotV2,
} from '@canopy-code/canopy-code-core';
import { swarmCommand } from './swarm-command.js';
import { createMockCommandContext } from '../../test-utils/mockCommandContext.js';

const emptySnapshot: GoalSnapshotV2 = { v: 2, goal: null, activity: 'idle' };

function makeContext({ trusted = true } = {}) {
  const dispatch = vi.fn().mockResolvedValue({ snapshot: emptySnapshot });
  const runtime = {
    getSnapshot: vi.fn(() => emptySnapshot),
    dispatch,
  } as unknown as GoalRuntime;
  const config = {
    getGoalRuntimeReady: vi.fn().mockResolvedValue(runtime),
    isTrustedFolder: vi.fn(() => trusted),
  } as unknown as Config;
  return {
    context: createMockCommandContext({ services: { config } }),
    dispatch,
  };
}

describe('swarmCommand', () => {
  it('starts a persistent Goal instead of a one-shot prompt', async () => {
    const { context, dispatch } = makeContext();

    const result = await swarmCommand.action!(context, 'add a health endpoint');

    expect(result).toMatchObject({
      type: 'goal_control',
      cause: 'create',
      operation: { kind: 'set' },
    });
    expect(dispatch).toHaveBeenCalledWith(
      expect.objectContaining({
        action: 'create',
        objective: expect.stringContaining('add a health endpoint'),
      }),
    );
  });

  it('requires a trusted workspace', async () => {
    const { context, dispatch } = makeContext({ trusted: false });

    await expect(
      swarmCommand.action!(context, 'fix the test'),
    ).resolves.toEqual(
      expect.objectContaining({ type: 'message', messageType: 'error' }),
    );
    expect(dispatch).not.toHaveBeenCalled();
  });
});
