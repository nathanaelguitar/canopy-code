/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { randomUUID } from 'node:crypto';
import { ensureWorkspaceDaemon } from '../../serve/ensure-workspace-daemon.js';

export interface DaemonAttachedSession {
  baseUrl: string;
  sessionId: string;
  clientId: string;
}

async function post(
  baseUrl: string,
  path: string,
  clientId: string,
  body: Record<string, unknown> = {},
): Promise<Record<string, unknown>> {
  const response = await fetch(new URL(path, baseUrl), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Canopy-Client-Id': clientId,
    },
    body: JSON.stringify(body),
  });
  const json = (await response.json().catch(() => undefined)) as
    | Record<string, unknown>
    | undefined;
  if (!response.ok) {
    throw new Error(
      `Daemon session setup failed: HTTP ${response.status}${
        typeof json?.['error'] === 'string' ? ` — ${json['error']}` : ''
      }`,
    );
  }
  return json ?? {};
}

/**
 * Start (or restore) the daemon-owned session backing one interactive TUI.
 * The caller's Config already chose the session id, so requesting that same
 * id preserves the existing chat-recording and --resume identity contract.
 */
export async function attachDaemonSession(options: {
  workspaceCwd: string;
  sessionId: string;
  resume: boolean;
}): Promise<DaemonAttachedSession> {
  const daemon = await ensureWorkspaceDaemon(options.workspaceCwd);
  const clientId = `terminal-${randomUUID()}`;
  const path = options.resume
    ? `/session/${encodeURIComponent(options.sessionId)}/resume`
    : '/session';
  const body = options.resume ? {} : { sessionId: options.sessionId };
  const result = await post(daemon.baseUrl, path, clientId, body);
  const sessionId =
    typeof result['sessionId'] === 'string'
      ? result['sessionId']
      : options.sessionId;
  // `spawnOrAttach` allocates the registered per-session client identity.
  // The request header identifies this caller during setup, but it is not the
  // id accepted by prompt admission; use the daemon's returned value for all
  // later SSE and mutation requests.
  const registeredClientId =
    typeof result['clientId'] === 'string' ? result['clientId'] : clientId;
  return { baseUrl: daemon.baseUrl, sessionId, clientId: registeredClientId };
}
