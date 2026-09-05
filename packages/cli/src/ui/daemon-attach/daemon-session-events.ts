/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Hand-rolled SSE client for `GET /session/:id/events`, deliberately not a
 * dependency on `@qwen-code/sdk-typescript`'s SSE transport
 * (`packages/sdk-typescript/src/daemon/sse.ts` and friends) — that package
 * has real, unrelated, uncommitted work in progress on its SSE transport as
 * of this writing (see docs/design/2026-08-26-remote-control.md,
 * Landmines). Depending on it here would mean building on top of code that
 * is actively changing out from under this feature. The wire format is
 * plain SSE (`id:`/`event:`/`data:` lines, blank-line-terminated), small
 * enough to parse directly.
 */

export interface DaemonSessionEvent {
  id: number;
  event: string;
  data: unknown;
}

export interface DaemonSessionEventStreamOptions {
  baseUrl: string;
  sessionId: string;
  clientId: string;
  /** Resume from this event id on (re)connect, via the `Last-Event-ID` header. */
  lastEventId?: number;
  signal: AbortSignal;
  onEvent: (event: DaemonSessionEvent) => void;
  /** Called on a connection error before an automatic reconnect attempt. */
  onError?: (error: Error) => void;
}

export class DaemonEventStreamHttpError extends Error {
  readonly status: number;

  constructor(status: number) {
    super(`SSE connect failed: HTTP ${status}`);
    this.name = 'DaemonEventStreamHttpError';
    this.status = status;
  }
}

export type DaemonPermissionOutcome =
  | { outcome: 'selected'; optionId: string }
  | { outcome: 'cancelled' };

export interface DaemonPermissionResponse {
  outcome: DaemonPermissionOutcome;
  answers?: Record<string, string>;
}

type LegacyDaemonPermissionResponse = DaemonPermissionOutcome;

const RECONNECT_DELAY_MS = 1000;

/**
 * Parses one SSE frame (a blank-line-terminated block of `field: value`
 * lines) into an event. Returns undefined for frames with no `data:` line
 * (e.g. a bare `retry:` frame or a comment-only heartbeat frame starting
 * with `:`).
 */
function parseFrame(frame: string): DaemonSessionEvent | undefined {
  let id: number | undefined;
  let event = 'message';
  let dataRaw: string | undefined;
  for (const line of frame.split('\n')) {
    if (line.startsWith(':')) continue; // comment/heartbeat
    const colonIndex = line.indexOf(':');
    if (colonIndex === -1) continue;
    const field = line.slice(0, colonIndex);
    const value = line.slice(colonIndex + 1).replace(/^ /, '');
    if (field === 'id') id = Number(value);
    else if (field === 'event') event = value;
    else if (field === 'data') dataRaw = (dataRaw ?? '') + value;
  }
  if (dataRaw === undefined || id === undefined) return undefined;
  try {
    return { id, event, data: JSON.parse(dataRaw) };
  } catch {
    return undefined;
  }
}

/**
 * Consumes `GET /session/:id/events` until `signal` aborts, reconnecting
 * (with `Last-Event-ID`) on a dropped connection. Runs as a background
 * loop; callers await this to know when it has stopped for good (aborted),
 * not to know when a single connection ended.
 */
export async function streamDaemonSessionEvents(
  options: DaemonSessionEventStreamOptions,
): Promise<void> {
  let lastEventId = options.lastEventId;

  while (!options.signal.aborted) {
    try {
      const url = new URL(
        `/session/${encodeURIComponent(options.sessionId)}/events`,
        options.baseUrl,
      );
      url.searchParams.set('clientId', options.clientId);
      const headers: Record<string, string> = {};
      headers['X-Canopy-Client-Id'] = options.clientId;
      if (lastEventId !== undefined) {
        headers['Last-Event-ID'] = String(lastEventId);
      }
      const res = await fetch(url, { headers, signal: options.signal });
      if (!res.ok || !res.body) {
        throw new DaemonEventStreamHttpError(res.status);
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffered = '';
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buffered += decoder.decode(value, { stream: true });
        let boundary: number;
        // SSE frames are separated by a blank line (\n\n). CRLF inputs are
        // normalized first so the same split works either way.
        buffered = buffered.replace(/\r\n/g, '\n');
        while ((boundary = buffered.indexOf('\n\n')) !== -1) {
          const frame = buffered.slice(0, boundary);
          buffered = buffered.slice(boundary + 2);
          const parsed = parseFrame(frame);
          if (parsed) {
            lastEventId = parsed.id;
            options.onEvent(parsed);
          }
        }
      }
    } catch (error) {
      if (options.signal.aborted) return;
      const normalized =
        error instanceof Error ? error : new Error(String(error));
      options.onError?.(normalized);
      // A daemon restart loses the in-memory session runtime. Retrying the
      // same stale subscription can never succeed; let the caller resume the
      // durable session and reconnect with its newly registered client id.
      if (
        normalized instanceof DaemonEventStreamHttpError &&
        normalized.status === 404
      ) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, RECONNECT_DELAY_MS));
    }
  }
}

export interface DaemonRequestError {
  status: number;
  body: unknown;
}

async function postJson(
  baseUrl: string,
  path: string,
  body: unknown,
  clientId?: string,
): Promise<unknown> {
  const res = await fetch(new URL(path, baseUrl), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(clientId ? { 'X-Canopy-Client-Id': clientId } : {}),
    },
    body: JSON.stringify(body),
  });
  const json = await res.json().catch(() => undefined);
  if (!res.ok) {
    const error: DaemonRequestError = { status: res.status, body: json };
    throw error;
  }
  return json;
}

/** Restore a durable session after the workspace daemon has restarted. */
export async function resumeDaemonSession(
  baseUrl: string,
  sessionId: string,
  requestedClientId: string,
): Promise<{ clientId: string }> {
  const result = (await postJson(
    baseUrl,
    `/session/${encodeURIComponent(sessionId)}/resume`,
    {},
    requestedClientId,
  )) as { clientId?: unknown };
  return {
    clientId:
      typeof result.clientId === 'string' ? result.clientId : requestedClientId,
  };
}

/** `POST /session/:id/prompt` — submit a user turn. */
export function submitDaemonPrompt(
  baseUrl: string,
  sessionId: string,
  clientId: string,
  prompt: Array<{ type: 'text'; text: string }>,
): Promise<unknown> {
  return fetch(
    new URL(`/session/${encodeURIComponent(sessionId)}/prompt`, baseUrl),
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Canopy-Client-Id': clientId,
      },
      body: JSON.stringify({ clientId, prompt }),
    },
  ).then(async (res) => {
    const json = await res.json().catch(() => undefined);
    if (!res.ok)
      throw { status: res.status, body: json } satisfies DaemonRequestError;
    return json;
  });
}

/** `POST /session/:id/cancel` — interrupt the active daemon turn. */
export function cancelDaemonSession(
  baseUrl: string,
  sessionId: string,
  clientId: string,
): Promise<unknown> {
  return fetch(
    new URL(`/session/${encodeURIComponent(sessionId)}/cancel`, baseUrl),
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Canopy-Client-Id': clientId,
      },
      body: JSON.stringify({ clientId, reason: 'user_interrupt' }),
    },
  ).then(async (res) => {
    const json = await res.json().catch(() => undefined);
    if (!res.ok)
      throw { status: res.status, body: json } satisfies DaemonRequestError;
    return json;
  });
}

/** `POST /session/:id/permission/:requestId` — answer a permission prompt. */
export function answerDaemonPermission(
  baseUrl: string,
  sessionId: string,
  clientId: string,
  requestId: string,
  response: DaemonPermissionResponse | LegacyDaemonPermissionResponse,
): Promise<unknown> {
  const normalizedResponse: DaemonPermissionResponse =
    typeof response.outcome === 'string'
      ? { outcome: response }
      : response;
  return postJson(
    baseUrl,
    `/session/${encodeURIComponent(sessionId)}/permission/${encodeURIComponent(requestId)}`,
    normalizedResponse,
    clientId,
  );
}
