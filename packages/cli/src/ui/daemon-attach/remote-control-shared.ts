/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { HybridTokenStorage } from '@canopy-code/canopy-code-core';

// Deliberately fixed to the private beta Worker. This is not a user-configured
// webhook and is not read from the shell environment.
export const REMOTE_CONTROL_API =
  'https://founding-api.canopychat.app/v1/remote-control';
export const REMOTE_CONTROL_SECRET = 'private-remote-control-device';

// Shared with remote-control-pairing.ts so /remote-control connect writes the
// same credential enableRemoteControl's inline auto-pairing already reads —
// pre-pairing here means later /remote-control calls skip the QR step.
export const deviceStorage = new HybridTokenStorage('Canopy Code');

export async function apiRequest(
  path: string,
  init: RequestInit,
): Promise<Response> {
  return fetch(new URL(path.replace(/^\/+/, ''), `${REMOTE_CONTROL_API}/`), {
    ...init,
    headers: { Accept: 'application/json', ...(init.headers ?? {}) },
  });
}
