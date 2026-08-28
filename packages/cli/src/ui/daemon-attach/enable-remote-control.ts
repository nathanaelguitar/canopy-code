/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { readFile } from 'node:fs/promises';
import { hostname } from 'node:os';
import { HybridTokenStorage } from '@canopy-code/canopy-code-core';
import type { DaemonAttachedSession } from './attach-daemon-session.js';

// Deliberately fixed to the private beta Worker. This is not a user-configured
// webhook and is not read from the shell environment.
const REMOTE_CONTROL_API = 'https://founding-api.canopychat.app/v1/remote-control';
const REMOTE_CONTROL_SECRET = 'private-remote-control-device';

interface LocalControlEnableResponse {
  active?: boolean;
  url?: string;
  urlRedacted?: boolean;
  qrText?: string;
  error?: string;
  code?: string;
}

interface PairingStartResponse {
  pairing_id: string;
  pairing_url: string;
  polling_token: string;
  expires_at: string;
}

const deviceStorage = new HybridTokenStorage('Canopy Code');

async function apiRequest(path: string, init: RequestInit): Promise<Response> {
  return fetch(new URL(path, `${REMOTE_CONTROL_API}/`), {
    ...init,
    headers: { Accept: 'application/json', ...(init.headers ?? {}) },
  });
}

async function sendSession(accessToken: string, session: {
  sessionId: string;
  workspaceName: string;
  url: string;
}): Promise<'sent' | 'unauthorized' | 'unavailable'> {
  const response = await apiRequest('/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${accessToken}` },
    body: JSON.stringify({
      session_id: session.sessionId,
      workspace_name: session.workspaceName,
      url: session.url,
    }),
  });
  if (response.status === 401 || response.status === 403) return 'unauthorized';
  if (!response.ok) return 'unavailable';
  return 'sent';
}

async function pairAndSend(session: {
  sessionId: string;
  workspaceName: string;
  url: string;
}): Promise<PairingStartResponse | undefined> {
  const response = await apiRequest('/pairings', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ device_name: hostname().slice(0, 120) || 'Canopy Code computer' }),
  });
  if (!response.ok) return undefined;
  const pairing = (await response.json()) as PairingStartResponse;
  void (async () => {
    const deadline = Date.parse(pairing.expires_at);
    while (Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 2000));
      const poll = await apiRequest(`/pairings/${encodeURIComponent(pairing.pairing_id)}`, {
        headers: { Authorization: `Bearer ${pairing.polling_token}` },
      });
      if (poll.status === 202) continue;
      if (!poll.ok) return;
      const result = (await poll.json()) as { access_token?: string; status: string };
      if (result.status !== 'approved' || !result.access_token) return;
      await deviceStorage.setSecret(REMOTE_CONTROL_SECRET, result.access_token);
      await sendSession(result.access_token, session);
      return;
    }
  })();
  return pairing;
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

/**
 * Fire-and-forget webhook so the CanopyChat iOS app can push a notification
 * with a deep link. Config presence is the opt-in — no configured URL means
 * no network call at all, silently. Never blocks or fails the caller: a
 * webhook delivery problem must not stop the operator from getting their
 * QR code, and must not stop startup either (the auto-enable caller).
 */
export type RemoteControlOutcome =
  | {
      status: 'enabled';
      pairingUrl: string;
      qrText?: string;
      pairingPending: boolean;
    }
  | { status: 'unavailable'; reason: string }
  | { status: 'error'; message: string };

/**
 * Enable Tailscale pairing on the daemon backing `daemonSession` and deliver
 * the session through the private authenticated Worker API. Shared by the explicit
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
      ? await readPairingUrlFromLog(daemonSession.daemonLogPath)
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

  const session = { sessionId: daemonSession.sessionId, workspaceName, url: pairingUrl };
  let pairingPending = false;
  let accessToken: string | null = null;
  try { accessToken = await deviceStorage.getSecret(REMOTE_CONTROL_SECRET); } catch { accessToken = null; }
  if (accessToken) {
    const sent = await sendSession(accessToken, session);
    if (sent === 'unauthorized') {
      try { await deviceStorage.deleteSecret(REMOTE_CONTROL_SECRET); } catch { /* already absent */ }
      accessToken = null;
    }
  }
  let publicPairingUrl: string;
  if (!accessToken) {
    const pairing = await pairAndSend(session);
    if (!pairing) {
      return {
        status: 'error',
        message: 'CanopyChat pairing is unavailable. No localhost fallback was created.',
      };
    }
    publicPairingUrl = pairing.pairing_url;
    pairingPending = true;
  } else {
    publicPairingUrl = pairingUrl;
  }

  let qrText: string | undefined;
  try {
    const { default: qrcode } = (await import('qrcode-terminal')) as {
      default: typeof import('qrcode-terminal');
    };
    qrcode.setErrorLevel('Q');
    qrcode.generate(publicPairingUrl, { small: true }, (code) => {
      qrText = code.trimEnd();
    });
  } catch {
    // Best-effort — the raw URL is still usable without a QR.
  }

  return {
    status: 'enabled',
    pairingUrl: publicPairingUrl,
    qrText,
    pairingPending,
  };
}
