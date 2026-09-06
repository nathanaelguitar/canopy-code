/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  answerDaemonPermission,
  loadDaemonSession,
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

  it('delivers id-less state resync control frames to the consumer', async () => {
    const controller = new AbortController();
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        new Response(
          'event: state_resync_required\ndata: {"reason":"ring_evicted"}\n\n',
          { status: 200, headers: { 'Content-Type': 'text/event-stream' } },
        ),
      );
    vi.stubGlobal('fetch', fetchMock);
    const onEvent = vi.fn();
    const onResyncRequired = vi.fn(() => controller.abort());

    await streamDaemonSessionEvents({
      baseUrl: 'http://daemon.test',
      sessionId: 'session-1',
      clientId: 'client-1',
      signal: controller.signal,
      onEvent,
      onResyncRequired,
    });

    expect(onResyncRequired).toHaveBeenCalledOnce();
    expect(onEvent).toHaveBeenCalledWith({
      event: 'state_resync_required',
      data: { reason: 'ring_evicted' },
    });
  });

  it('latches after resync and does not deliver buffered replay frames', async () => {
    const controller = new AbortController();
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        new Response(
          'event: state_resync_required\ndata: {"reason":"ring_evicted"}\n\n' +
            'id: 42\nevent: session_update\ndata: {"after":true}\n\n',
          { status: 200, headers: { 'Content-Type': 'text/event-stream' } },
        ),
      );
    vi.stubGlobal('fetch', fetchMock);
    const onEvent = vi.fn();

    await streamDaemonSessionEvents({
      baseUrl: 'http://daemon.test',
      sessionId: 'session-1',
      clientId: 'client-1',
      signal: controller.signal,
      onEvent,
      onResyncRequired: () => controller.abort(),
    });

    expect(onEvent).toHaveBeenCalledTimes(1);
    expect(onEvent).not.toHaveBeenCalledWith(
      expect.objectContaining({ event: 'session_update' }),
    );
  });

  it('pairs the resume cursor with the daemon event epoch and refreshes it', async () => {
    const controller = new AbortController();
    const fetchMock = vi.fn().mockResolvedValue(
      new Response('id: 6\nevent: session_update\ndata: {}\n\n', {
        status: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'X-Canopy-Event-Epoch': 'epoch-new',
        },
      }),
    );
    vi.stubGlobal('fetch', fetchMock);
    const onEpoch = vi.fn();

    await streamDaemonSessionEvents({
      baseUrl: 'http://daemon.test',
      sessionId: 'session-1',
      clientId: 'client-1',
      lastEventId: 5,
      eventEpoch: 'epoch-old',
      signal: controller.signal,
      onEvent: () => controller.abort(),
      onEpoch,
    });

    const [, init] = fetchMock.mock.calls[0] as [URL, RequestInit];
    expect(init.headers).toMatchObject({
      'Last-Event-ID': '5',
      'X-Canopy-Event-Epoch': 'epoch-old',
    });
    expect(onEpoch).toHaveBeenCalledWith('epoch-new');
  });

  it('maps the load replay journal into stream events', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          clientId: 'registered-client',
          lastEventId: 91,
          eventEpoch: 'epoch-91',
          compactedReplay: [
            { id: 88, type: 'session_update', data: { update: {} } },
          ],
          liveJournal: [
            { id: 90, type: 'session_update', data: { update: {} } },
            { malformed: true },
          ],
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      ),
    );
    vi.stubGlobal('fetch', fetchMock);

    await expect(
      loadDaemonSession('http://daemon.test', 'session-1', 'requested-client'),
    ).resolves.toEqual({
      clientId: 'registered-client',
      lastEventId: 91,
      eventEpoch: 'epoch-91',
      compactedReplay: [
        { id: 88, event: 'session_update', data: { update: {} } },
      ],
      liveJournal: [{ id: 90, event: 'session_update', data: { update: {} } }],
    });
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
