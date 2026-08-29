/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import type {
  GoalControlRequest,
  GoalRuntime,
} from '@canopy-code/canopy-code-core';
import {
  CommandKind,
  type CommandContext,
  type GoalControlActionReturn,
  type GoalCommandOperation,
  type MessageActionReturn,
  type SlashCommand,
} from './types.js';

const SWARM_DIRECTIVE = `
You are the swarm coordinator. Finish the coding task below using the native
Agent tool to delegate only independent, bounded work streams. Keep ownership
exclusive: no two agents may edit the same files, and the coordinator remains
responsible for integration, tests, and the final answer.

This DGX Spark currently serves its local coding model with one inference slot.
That means many simultaneous code-generating agents reduce throughput rather
than increase it. Start with one focused implementation agent. Add at most one
additional agent only when it can make independent progress while the first is
blocked on tool I/O or when there is a clearly separate read-only investigation.
Do not spawn agents just to satisfy the word "swarm". Use worktree isolation
for any concurrent edits, collect results, integrate them yourself, and run the
relevant tests before declaring the task complete.
`;

function errorMessage(content: string): MessageActionReturn {
  return { type: 'message', messageType: 'error', content };
}

function goalControl(
  operation: GoalCommandOperation,
  response: Awaited<ReturnType<GoalRuntime['dispatch']>>,
  cause: 'create' | 'replace',
): GoalControlActionReturn {
  return { type: 'goal_control', operation, response, cause };
}

export const swarmCommand: SlashCommand = {
  name: 'swarm',
  description: 'Coordinate an efficient subagent swarm for a coding task',
  kind: CommandKind.BUILT_IN,
  supportedModes: ['interactive'] as const,
  action: async (
    context: CommandContext,
    args,
  ): Promise<GoalControlActionReturn | MessageActionReturn> => {
    const task = args.trim();
    if (!task) {
      return {
        type: 'message',
        messageType: 'info',
        content:
          'Usage: /swarm <coding task>. The coordinator will choose the smallest useful set of subagents for this DGX Spark.',
      };
    }

    const { config } = context.services;
    if (!config) return errorMessage('Configuration is not available.');
    if (!config.isTrustedFolder()) {
      return errorMessage(
        '/swarm is only available in trusted workspaces. Trust this folder via `/trust` and try again.',
      );
    }

    try {
      const runtime = await config.getGoalRuntimeReady();
      const current = runtime.getSnapshot().goal;
      const objective = `${SWARM_DIRECTIVE}\n\nCoding task:\n${task}`;
      const request: GoalControlRequest = current
        ? {
            action: 'replace',
            objective,
            expectedGoalId: current.goalId,
            expectedRevision: current.revision,
          }
        : { action: 'create', objective };
      const operation: GoalCommandOperation = { kind: 'set', objective };
      return goalControl(
        operation,
        await runtime.dispatch(request),
        request.action,
      );
    } catch (error) {
      return errorMessage(
        error instanceof Error ? error.message : String(error),
      );
    }
  },
};
