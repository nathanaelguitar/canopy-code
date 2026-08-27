/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { readFile } from 'node:fs/promises';
import type { DaemonAttachedSession } from './attach-daemon-session.js';

interface LocalControlEnableResponse {
  active?: boolean;
  url?: string;
  urlRedacted?: boolean;
  qrText?: string;
  error?: string;
  code?: string;
}

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
  sessionId: string,
): Promise<string | undefined> {
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline) {
    let text: string;
    try {
      text = await readFile(logPath, 'utf-8');
    } catch {
      return undefined;
    }
    const urls = text.matchAll(
      /canopy serve: Local Control pairing URL: (\S+)/g,
    );
    for (const match of Array.from(urls).reverse()) {
      try {
        if (new URL(match[1]).pathname === `/session/${sessionId}`) {
          return match[1];
        }
      } catch {
        // Ignore malformed diagnostic lines and keep looking for this session.
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  return undefined;
}

/**
 * Fire-and-forget webhook so the CanopyChat iOS app can push a notification
 * with a deep link. Config presence is the opt-in — no configured URL means
 * no network call at all, silently. Never blocks or fails the caller: a
 * webhook delivery problem must not stop the operator from getting their
 * QR code, and must not stop startup either (the auto-enable caller).
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
    // Best-effort.
  }
}

export type RemoteControlOutcome =
  | {
      status: 'enabled';
      pairingUrl: string;
      qrText?: string;
      webhookSent: boolean;
    }
  | { status: 'unavailable'; reason: string }
  | { status: 'error'; message: string };

/**
 * Enable Tailscale pairing on the daemon backing `daemonSession` and fire
 * the CanopyChat webhook if configured. Shared by the explicit
 * `/remote-control` command and the Stage C startup auto-enable — the
 * command always reports its outcome to the user; the startup path only
 * reports success (an `unavailable` "no tailnet interface" result is the
 * expected common case for anyone without Tailscale installed and must not
 * read as an error at every session start).
 */
export async function enableRemoteControl(
  daemonSession: DaemonAttachedSession,
  workspaceName: string,
): Promise<RemoteControlOutcome> {
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
      if (response.code === 'no_tailscale_interface') {
        return { status: 'unavailable', reason: response.error ?? '' };
      }
      return {
        status: 'error',
        message: response.error ?? `HTTP ${res.status}`,
      };
    }
  } catch (error) {
    return {
      status: 'error',
      message: error instanceof Error ? error.message : String(error),
    };
  }

  let pairingUrl = response.url;
  if (!pairingUrl && response.urlRedacted) {
    pairingUrl = daemonSession.daemonLogPath
      ? await readPairingUrlFromLog(
          daemonSession.daemonLogPath,
          daemonSession.sessionId,
        )
      : undefined;
  }
  if (!pairingUrl) {
    return {
      status: 'error',
      message:
        'Remote Control is on, but the pairing URL could not be recovered ' +
        "(this daemon was not spawned by this terminal). Check the daemon's " +
        'own log for a line starting "canopy serve: Local Control pairing URL:".',
    };
  }

  const webhookUrl = process.env['CANOPY_CHAT_WEBHOOK_URL'];
  if (webhookUrl) {
    void notifyCanopyChat(webhookUrl, {
      url: pairingUrl,
      sessionId: daemonSession.sessionId,
      workspaceName,
    });
  }

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
      // Best-effort — the raw URL is still usable without a QR.
    }
  }

  return {
    status: 'enabled',
    pairingUrl,
    qrText,
    webhookSent: webhookUrl !== undefined,
  };
}
