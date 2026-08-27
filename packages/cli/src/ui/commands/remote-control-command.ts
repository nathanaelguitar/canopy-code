/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { readFile } from 'node:fs/promises';
import {
  type CommandContext,
  type SlashCommand,
  CommandKind,
} from './types.js';
import { MessageType } from '../types.js';

/**
 * The daemon's stdio stays bound to its log file for its whole lifetime
 * (see ensure-workspace-daemon.ts), so when `/workspace/local-control/
 * enable` redacts the pairing URL from an unauthenticated caller's response
 * body (correct, deliberate behavior — see workspace-local-control.ts's
 * `presentStatus`), the URL is still recoverable from that file for
 * whichever process actually spawned the daemon. A short poll: the log
 * write and this read are two different processes, so there's no ordering
 * guarantee the line has landed by the time the HTTP response comes back.
 */
async function readPairingUrlFromLog(
  logPath: string,
): Promise<string | undefined> {
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline) {
    let text: string;
    try {
      text = await readFile(logPath, 'utf-8');
    } catch {
      return undefined;
    }
    const match = text.match(/canopy serve: Local Control pairing URL: (\S+)/);
    if (match) return match[1];
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  return undefined;
}

interface LocalControlEnableResponse {
  active?: boolean;
  url?: string;
  urlRedacted?: boolean;
  qrText?: string;
  error?: string;
  code?: string;
}

/**
 * Fire-and-forget webhook so the CanopyChat iOS app can push a notification
 * with a deep link. Config presence is the opt-in — no configured URL means
 * no network call at all, silently. Never blocks or fails the command:
 * a webhook delivery problem must not stop the operator from getting their
 * QR code.
 */
async function notifyCanopyChat(
  webhookUrl: string,
  payload: {
    url: string;
    sessionId: string;
    workspaceName: string;
    title?: string;
  },
): Promise<void> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 5000);
    try {
      await fetch(webhookUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }
  } catch {
    // Best-effort. The pairing URL is still valid and printed regardless of
    // whether the phone gets a push notification about it.
  }
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
          'Restart with CANOPY_CODE_DAEMON_ATTACH=1 to enable it.',
      };
    }

    let response: LocalControlEnableResponse;
    try {
      const res = await fetch(
        new URL('/workspace/local-control/enable', daemonSession.baseUrl),
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Canopy-Client-Id': daemonSession.clientId,
          },
          body: JSON.stringify({
            network: 'tailscale',
            target: `/session/${encodeURIComponent(daemonSession.sessionId)}`,
          }),
        },
      );
      response = (await res
        .json()
        .catch(() => ({}))) as LocalControlEnableResponse;
      if (!res.ok) {
        return {
          type: 'message' as const,
          messageType: 'error' as const,
          content: `Remote Control could not start: ${
            response.error ?? `HTTP ${res.status}`
          }`,
        };
      }
    } catch (error) {
      return {
        type: 'message' as const,
        messageType: 'error' as const,
        content: `Remote Control could not reach the local daemon: ${
          error instanceof Error ? error.message : String(error)
        }`,
      };
    }

    let pairingUrl = response.url;
    if (!pairingUrl && response.urlRedacted) {
      // Enabling is an unauthenticated call from this process's point of
      // view (no bearer token — see the module doc comment), so the HTTP
      // response deliberately withholds the URL. Recover it from the
      // daemon's own log when this process is the one that spawned it.
      pairingUrl = daemonSession.daemonLogPath
        ? await readPairingUrlFromLog(daemonSession.daemonLogPath)
        : undefined;
    }

    if (!pairingUrl) {
      context.ui.addItem(
        {
          type: MessageType.INFO,
          text:
            'Remote Control is on, but the pairing URL could not be ' +
            'recovered automatically (this daemon was not spawned by this ' +
            "terminal, so its log is not visible here). Check the daemon's " +
            'own log for a line starting "canopy serve: Local Control ' +
            'pairing URL:".',
        },
        Date.now(),
      );
      return;
    }

    const workspaceName =
      context.services.config?.getWorkingDir()?.split('/').pop() ??
      'canopy session';
    const webhookUrl = process.env['CANOPY_CHAT_WEBHOOK_URL'];
    if (webhookUrl) {
      void notifyCanopyChat(webhookUrl, {
        url: pairingUrl,
        sessionId: daemonSession.sessionId,
        workspaceName,
        title: context.services.config?.getSessionId(),
      });
    }

    const lines = [
      'Remote Control is on. Scan this QR code from any device on your tailnet:',
      '',
      pairingUrl,
    ];
    // qrText is redacted alongside url for the same unauthenticated-caller
    // reason (presentStatus strips both) — generate it locally from the
    // now-recovered URL rather than leaving the operator with text only.
    let qrText = response.qrText;
    if (!qrText) {
      try {
        const { default: qrcode } = (await import('qrcode-terminal')) as {
          default: typeof import('qrcode-terminal');
        };
        qrcode.setErrorLevel('Q');
        qrcode.generate(pairingUrl, { small: true }, (code) => {
          qrText = code.trimEnd();
        });
      } catch {
        // Best-effort — the raw URL above is still usable without a QR.
      }
    }
    if (qrText) {
      lines.push('', qrText);
    }
    lines.push(
      '',
      webhookUrl
        ? 'A pairing notification was sent to CanopyChat.'
        : 'Set CANOPY_CHAT_WEBHOOK_URL to also push a notification to CanopyChat.',
      'Turn Remote Control off from the Web Shell Settings card, or press Ctrl+C to exit.',
    );

    context.ui.addItem(
      { type: MessageType.INFO, text: lines.join('\n') },
      Date.now(),
    );
    return;
  },
};
