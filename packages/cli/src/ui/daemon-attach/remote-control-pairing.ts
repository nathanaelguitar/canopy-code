/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import {
  apiRequest,
  deviceStorage,
  REMOTE_CONTROL_SECRET,
} from './remote-control-shared.js';

const POLL_INTERVAL_MS = 2000;

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

/** Start a short-lived QR/device-code pairing. The URL has no session secret. */
export async function startRemoteControlPairing(): Promise<PairingChallenge> {
  const response = await apiRequest('/pairings', {
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
    typeof body['pairing_id'] !== 'string' ||
    typeof body['pairing_url'] !== 'string' ||
    typeof body['polling_token'] !== 'string' ||
    typeof body['expires_at'] !== 'string'
  ) {
    throw new Error('CanopyChat pairing is temporarily unavailable.');
  }
  return {
    pairingId: body['pairing_id'],
    pairingUrl: body['pairing_url'],
    pollingToken: body['polling_token'],
    expiresAt: body['expires_at'],
  };
}

/**
 * Wait for the signed-in phone to approve a pairing and store the credential
 * under the same key enableRemoteControl's inline auto-pairing reads, so a
 * one-time /remote-control connect makes every later /remote-control call
 * skip the QR step and send directly.
 */
export async function finishRemoteControlPairing(
  challenge: PairingChallenge,
  signal?: AbortSignal,
): Promise<void> {
  const deadline = Date.parse(challenge.expiresAt);
  while (Number.isFinite(deadline) && Date.now() < deadline) {
    if (signal?.aborted) throw new Error('Pairing cancelled.');
    const response = await apiRequest(
      `/pairings/${encodeURIComponent(challenge.pairingId)}`,
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
      await deviceStorage.setSecret(REMOTE_CONTROL_SECRET, body.access_token);
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
  const token = await deviceStorage.getSecret(REMOTE_CONTROL_SECRET);
  if (token) {
    await apiRequest('/devices/current', {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${token}` },
    }).catch(() => undefined);
  }
  await deviceStorage
    .deleteSecret(REMOTE_CONTROL_SECRET)
    .catch(() => undefined);
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
