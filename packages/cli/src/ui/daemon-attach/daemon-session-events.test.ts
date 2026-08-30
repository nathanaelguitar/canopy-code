/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  answerDaemonPermission,
  resumeDaemonSession,
  streamDaemonSessionEvents,
} from './daemon-session-events.js';
import type { DaemonEventStreamHttpError } from './daemon-session-events.js';

describe('daemon session events', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('stops retrying a stale event stream after HTTP 404', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(new Response(null, { status: 404 }));
    vi.stubGlobal('fetch', fetchMock);
    const onError = vi.fn();

    await streamDaemonSessionEvents({
      baseUrl: 'http://daemon.test',
      sessionId: 'session-1',
      clientId: 'client-1',
      signal: new AbortController().signal,
      onEvent: vi.fn(),
      onError,
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining<Partial<DaemonEventStreamHttpError>>({
        status: 404,
      }),
    );
  });

  it('restores a durable session with a newly registered client id', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ clientId: 'registered-client' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }),
    );
    vi.stubGlobal('fetch', fetchMock);

    await expect(
      resumeDaemonSession(
        'http://daemon.test',
        'session/with slash',
        'requested-client',
      ),
    ).resolves.toEqual({ clientId: 'registered-client' });

    const [, init] = fetchMock.mock.calls[0] as [URL, RequestInit];
    expect(init.headers).toMatchObject({
      'X-Canopy-Client-Id': 'requested-client',
    });
  });

  it('authenticates permission votes as the attached terminal client', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response('{}', {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }),
    );
    vi.stubGlobal('fetch', fetchMock);

    await answerDaemonPermission(
      'http://daemon.test',
      'session-1',
      'terminal-client',
      'request-1',
      { outcome: 'selected', optionId: 'allow-once' },
    );

    const [, init] = fetchMock.mock.calls[0] as [URL, RequestInit];
    expect(init.headers).toMatchObject({
      'X-Canopy-Client-Id': 'terminal-client',
    });
    expect(JSON.parse(String(init.body))).toEqual({
      outcome: { outcome: 'selected', optionId: 'allow-once' },
    });
  });
});
