/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import {
  type CommandContext,
  type SlashCommand,
  CommandKind,
} from './types.js';
import { MessageType } from '../types.js';
import { enableRemoteControl } from '../daemon-attach/enable-remote-control.js';
import {
  disconnectRemoteControl,
  finishRemoteControlPairing,
  startRemoteControlPairing,
} from '../daemon-attach/remote-control-pairing.js';

function workspaceNameOf(context: CommandContext): string {
  return (
    context.services.config?.getWorkingDir()?.split('/').pop() ??
    'canopy session'
  );
}

export const remoteControlCommand: SlashCommand = {
  name: 'remote-control',
  description:
    'Share this session over Tailscale so it can be co-driven from your phone',
  kind: CommandKind.BUILT_IN,
  supportedModes: ['interactive'] as const,
  action: async (context: CommandContext) => {
    const daemonSession = context.services.daemonSession;
    if (!daemonSession) {
      return {
        type: 'message' as const,
        messageType: 'error' as const,
        content:
          'Remote Control requires this session to be daemon-attached. ' +
          'Restart without --no-remote-control to enable it.',
      };
    }

    const outcome = await enableRemoteControl(
      daemonSession,
      workspaceNameOf(context),
    );

    if (outcome.status === 'unavailable') {
      return {
        type: 'message' as const,
        messageType: 'error' as const,
        content:
          'No Tailscale address is available on this host. Install ' +
          'Tailscale and run `tailscale up`, then retry /remote-control.',
      };
    }
    if (outcome.status === 'error') {
      return {
        type: 'message' as const,
        messageType: 'error' as const,
        content: `Remote Control could not start: ${outcome.message}`,
      };
    }

    const lines = [
      'Remote Control is on. Scan this QR code from any device on your tailnet:',
      '',
      outcome.pairingUrl,
    ];
    if (outcome.qrText) {
      lines.push('', outcome.qrText);
    }
    lines.push(
      '',
      outcome.deliveryStatus === 'queued'
        ? 'A pairing notification is being sent to CanopyChat.'
        : 'Connect your phone once with /remote-control connect to receive notifications automatically.',
      'Turn Remote Control off from the Web Shell Settings card, or press Ctrl+C to exit.',
    );

    context.ui.addItem(
      { type: MessageType.INFO, text: lines.join('\n') },
      Date.now(),
    );
    return;
  },
  subCommands: [
    {
      name: 'connect',
      description: 'Pair this computer with your signed-in CanopyChat app',
      kind: CommandKind.BUILT_IN,
      supportedModes: ['interactive'] as const,
      action: async (context: CommandContext) => {
        try {
          const challenge = await startRemoteControlPairing();
          context.ui.addItem(
            {
              type: MessageType.INFO,
              text: `Open this on your signed-in phone to approve this computer:\n\n${challenge.pairingUrl}\n\nWaiting for approval…`,
            },
            Date.now(),
          );
          await finishRemoteControlPairing(challenge, context.abortSignal);
          return {
            type: 'message' as const,
            messageType: 'info' as const,
            content:
              'This computer is connected. Future /remote-control sessions will notify your phone automatically.',
          };
        } catch (error) {
          return {
            type: 'message' as const,
            messageType: 'error' as const,
            content:
              error instanceof Error
                ? error.message
                : 'CanopyChat pairing failed.',
          };
        }
      },
    },
    {
      name: 'disconnect',
      description: 'Remove this computer’s CanopyChat remote-control access',
      kind: CommandKind.BUILT_IN,
      supportedModes: ['interactive'] as const,
      action: async () => {
        await disconnectRemoteControl();
        return {
          type: 'message' as const,
          messageType: 'info' as const,
          content:
            'This computer is disconnected from CanopyChat Remote Control.',
        };
      },
    },
  ],
};
