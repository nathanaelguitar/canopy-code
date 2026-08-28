/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { HybridTokenStorage } from '@canopy-code/canopy-code-core';

const REMOTE_CONTROL_API =
  'https://founding-api.canopychat.app/v1/remote-control';
const TOKEN_SERVICE = 'Canopy Code Remote Control';
const TOKEN_KEY = 'paired-device-access-token';
const POLL_INTERVAL_MS = 2000;

const tokenStorage = new HybridTokenStorage(TOKEN_SERVICE);

export interface PairingChallenge {
  pairingId: string;
  pairingUrl: string;
  pollingToken: string;
  expiresAt: string;
}

interface PairingStatus {
  status: 'pending' | 'approved' | 'expired';
  access_token?: string;
}

export type RemoteDeliveryStatus = 'queued' | 'unpaired';

/** Start a short-lived QR/device-code pairing. The URL has no session secret. */
export async function startRemoteControlPairing(): Promise<PairingChallenge> {
  const response = await fetch(`${REMOTE_CONTROL_API}/pairings`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  });
  const body = (await response.json().catch(() => null)) as Record<
    string,
    unknown
  > | null;
  if (
    !response.ok ||
    !body ||
    typeof body.pairing_id !== 'string' ||
    typeof body.pairing_url !== 'string' ||
    typeof body.polling_token !== 'string' ||
    typeof body.expires_at !== 'string'
  ) {
    throw new Error('CanopyChat pairing is temporarily unavailable.');
  }
  return {
    pairingId: body.pairing_id,
    pairingUrl: body.pairing_url,
    pollingToken: body.polling_token,
    expiresAt: body.expires_at,
  };
}

/** Wait for the signed-in phone to approve a pairing and keep the credential
 * in the OS keychain (or Canopy's encrypted fallback), never in settings/env. */
export async function finishRemoteControlPairing(
  challenge: PairingChallenge,
  signal?: AbortSignal,
): Promise<void> {
  const deadline = Date.parse(challenge.expiresAt);
  while (Number.isFinite(deadline) && Date.now() < deadline) {
    if (signal?.aborted) throw new Error('Pairing cancelled.');
    const response = await fetch(
      `${REMOTE_CONTROL_API}/pairings/${encodeURIComponent(challenge.pairingId)}`,
      {
        headers: { Authorization: `Bearer ${challenge.pollingToken}` },
        signal,
      },
    );
    const body = (await response
      .json()
      .catch(() => null)) as PairingStatus | null;
    if (
      response.ok &&
      body?.status === 'approved' &&
      typeof body.access_token === 'string'
    ) {
      await tokenStorage.setSecret(TOKEN_KEY, body.access_token);
      return;
    }
    if (body?.status === 'expired' || response.status === 410) {
      throw new Error(
        'That pairing code expired. Run /remote-control connect again.',
      );
    }
    if (!response.ok && response.status !== 202) {
      throw new Error('CanopyChat pairing is temporarily unavailable.');
    }
    await delay(POLL_INTERVAL_MS, signal);
  }
  throw new Error(
    'That pairing code expired. Run /remote-control connect again.',
  );
}

export async function disconnectRemoteControl(): Promise<void> {
  const token = await tokenStorage.getSecret(TOKEN_KEY);
  if (token) {
    await fetch(`${REMOTE_CONTROL_API}/devices/current`, {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${token}` },
    }).catch(() => undefined);
  }
  await tokenStorage.deleteSecret(TOKEN_KEY).catch(() => undefined);
}

/** Queue best-effort delivery without delaying an interactive session. */
export async function queueRemoteControlSession(payload: {
  url: string;
  sessionId: string;
  workspaceName: string;
}): Promise<RemoteDeliveryStatus> {
  const token = await tokenStorage.getSecret(TOKEN_KEY);
  if (!token) return 'unpaired';
  void sendRemoteControlSession(token, payload);
  return 'queued';
}

async function sendRemoteControlSession(
  token: string,
  payload: { url: string; sessionId: string; workspaceName: string },
): Promise<void> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 5000);
  try {
    const response = await fetch(`${REMOTE_CONTROL_API}/sessions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        session_id: payload.sessionId,
        workspace_name: payload.workspaceName,
        url: payload.url,
      }),
      signal: controller.signal,
    });
    if (response.status === 401 || response.status === 403) {
      await tokenStorage.deleteSecret(TOKEN_KEY).catch(() => undefined);
      return;
    }
    return;
  } catch {
    return;
  } finally {
    clearTimeout(timer);
  }
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        reject(new Error('Pairing cancelled.'));
      },
      { once: true },
    );
  });
}
