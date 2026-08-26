/**
 * @license
 * Copyright 2026 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * ChatGPT sign-in for Canopy Code.
 *
 * Ported from openai/codex `codex-rs/login` (PKCE authorization-code flow with
 * a localhost callback server). Credentials are stored separately from every
 * other auth method (`~/.canopy/chatgpt_auth.json`) so signing in with ChatGPT
 * never disturbs a configured API key and vice versa.
 */

import crypto from 'crypto';
import http from 'node:http';
import path from 'node:path';
import { promises as fs } from 'node:fs';
import type { AddressInfo } from 'node:net';
import type { Config } from '../config/config.js';
import { createDebugLogger } from '../utils/debugLogger.js';
import { combineAbortSignals } from '../utils/abortController.js';
import { openBrowserSecurely } from '../utils/secure-browser-launcher.js';
import { Storage } from '../config/storage.js';

const debugLogger = createDebugLogger('CHATGPT_OAUTH');

// OAuth endpoints (issuer mirrors codex DEFAULT_ISSUER).
const CHATGPT_OAUTH_ISSUER = 'https://auth.openai.com';

// Public client id from the open-source Codex CLI.
const CHATGPT_OAUTH_CLIENT_ID = 'app_EMoamEEZ73f0CkXaXp7hrann';

const CHATGPT_OAUTH_DEFAULT_PORT = 1455;
const CHATGPT_OAUTH_FALLBACK_PORT = 1457;

const CHATGPT_OAUTH_SCOPE =
  'openid profile email offline_access api.connectors.read api.connectors.invoke';

// Matches the originator Codex CLI sends; the backend gates on known values.
const CHATGPT_OAUTH_ORIGINATOR =
  process.env['CANOPY_CHATGPT_ORIGINATOR'] || 'codex_cli_rs';

const REFRESH_TIMEOUT_MS = 30_000;
const LOGIN_TIMEOUT_MS = 10 * 60_000;
// Refresh when the access token expires within this window.
const EXPIRY_SAFETY_MARGIN_MS = 5 * 60_000;

const CHATGPT_CREDENTIAL_FILENAME = 'chatgpt_auth.json';
export const CHATGPT_CREDENTIAL_FILE_MODE = 0o600;

export class ChatgptCredentialsClearRequiredError extends Error {
  constructor(
    message: string,
    public originalError?: unknown,
  ) {
    super(message);
    this.name = 'ChatgptCredentialsClearRequiredError';
  }
}

/** Flat subset of useful claims parsed out of the id_token JWT. */
export interface ChatgptIdTokenInfo {
  email?: string;
  chatgpt_plan_type?: string;
  chatgpt_user_id?: string;
  chatgpt_account_id?: string;
  /** Unix ms when the access token expires, if the JWT carries `exp`. */
  expires_at_ms?: number;
}

export interface ChatgptCredentials {
  id_token: string;
  access_token: string;
  refresh_token: string;
  account_id?: string;
  last_refresh: string;
}

interface ParsedChatgptCredentials extends ChatgptCredentials {
  parsedIdToken?: ChatgptIdTokenInfo;
}

// ---------------------------------------------------------------------------
// JWT helpers (port of token_data.rs)
// ---------------------------------------------------------------------------

function decodeJwtPayload(jwt: string): Record<string, unknown> {
  const parts = jwt.split('.');
  if (parts.length !== 3 || !parts[0] || !parts[1] || !parts[2]) {
    throw new Error('Invalid ID token format');
  }
  const payload = Buffer.from(parts[1], 'base64url').toString('utf8');
  return JSON.parse(payload) as Record<string, unknown>;
}

export function parseChatgptJwtClaims(jwt: string): ChatgptIdTokenInfo {
  const claims = decodeJwtPayload(jwt);
  const profile = claims['https://api.openai.com/profile'] as
    | { email?: string }
    | undefined;
  const auth = claims['https://api.openai.com/auth'] as
    | {
        chatgpt_plan_type?: string;
        chatgpt_user_id?: string;
        user_id?: string;
        chatgpt_account_id?: string;
      }
    | undefined;
  const exp = typeof claims['exp'] === 'number' ? claims['exp'] : undefined;

  return {
    email:
      (typeof claims['email'] === 'string' && claims['email']) ||
      profile?.email ||
      undefined,
    chatgpt_plan_type: auth?.chatgpt_plan_type,
    chatgpt_user_id: auth?.chatgpt_user_id ?? auth?.user_id,
    chatgpt_account_id: auth?.chatgpt_account_id,
    expires_at_ms: exp !== undefined ? exp * 1000 : undefined,
  };
}

// ---------------------------------------------------------------------------
// PKCE + state (port of pkce.rs / generate_state)
// ---------------------------------------------------------------------------

export interface PkceCodes {
  code_verifier: string;
  code_challenge: string;
}

export function generatePkce(): PkceCodes {
  const code_verifier = crypto.randomBytes(48).toString('base64url');
  const code_challenge = crypto
    .createHash('sha256')
    .update(code_verifier)
    .digest('base64url');
  return { code_verifier, code_challenge };
}

function generateState(): string {
  return crypto.randomBytes(32).toString('base64url');
}

// ---------------------------------------------------------------------------
// Authorize URL (port of build_authorize_url)
// ---------------------------------------------------------------------------

export interface AuthorizeUrlOptions {
  redirectUri: string;
  pkce: PkceCodes;
  state: string;
}

export function buildAuthorizeUrl({
  redirectUri,
  pkce,
  state,
}: AuthorizeUrlOptions): string {
  const query = new URLSearchParams({
    response_type: 'code',
    client_id: CHATGPT_OAUTH_CLIENT_ID,
    redirect_uri: redirectUri,
    scope: CHATGPT_OAUTH_SCOPE,
    code_challenge: pkce.code_challenge,
    code_challenge_method: 'S256',
    id_token_add_organizations: 'true',
    codex_cli_simplified_flow: 'true',
    state,
    originator: CHATGPT_OAUTH_ORIGINATOR,
  });
  return `${CHATGPT_OAUTH_ISSUER}/oauth/authorize?${query.toString()}`;
}

// ---------------------------------------------------------------------------
// Token exchange + refresh (ports of exchange_code_for_tokens /
// request_chatgpt_token_refresh)
// ---------------------------------------------------------------------------

export interface ExchangedTokens {
  id_token: string;
  access_token: string;
  refresh_token: string;
}

export async function exchangeCodeForTokens(opts: {
  code: string;
  redirectUri: string;
  pkce: PkceCodes;
  signal?: AbortSignal;
}): Promise<ExchangedTokens> {
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code: opts.code,
    redirect_uri: opts.redirectUri,
    client_id: CHATGPT_OAUTH_CLIENT_ID,
    code_verifier: opts.pkce.code_verifier,
  });
  const response = await fetch(`${CHATGPT_OAUTH_ISSUER}/oauth/token`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: body.toString(),
    ...(opts.signal ? { signal: opts.signal } : {}),
  });

  const text = await response.text();
  if (!response.ok) {
    throw new Error(
      `Token exchange failed with status ${response.status}: ${text}`,
    );
  }
  const parsed = JSON.parse(text) as Partial<ExchangedTokens>;
  if (!parsed.id_token || !parsed.access_token || !parsed.refresh_token) {
    throw new Error('Token endpoint returned an incomplete token set');
  }
  return {
    id_token: parsed.id_token,
    access_token: parsed.access_token,
    refresh_token: parsed.refresh_token,
  };
}

interface RefreshResponse {
  access_token: string;
  refresh_token?: string;
  id_token?: string;
}

async function requestTokenRefresh(
  refreshToken: string,
): Promise<RefreshResponse> {
  const { signal, cleanup } = combineAbortSignals([], {
    timeoutMs: REFRESH_TIMEOUT_MS,
  });
  try {
    const response = await fetch(`${CHATGPT_OAUTH_ISSUER}/oauth/token`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        client_id: CHATGPT_OAUTH_CLIENT_ID,
        grant_type: 'refresh_token',
        refresh_token: refreshToken,
      }),
      signal,
    });

    const text = await response.text();
    if (response.ok) {
      return JSON.parse(text) as RefreshResponse;
    }

    // Terminal failures mean the stored refresh token can never be used
    // again — drop the credential file so the next attempt re-runs login.
    let errorCode = '';
    try {
      const parsed = JSON.parse(text) as {
        error?: string | { code?: string };
        code?: string;
      };
      errorCode =
        (typeof parsed.error === 'string'
          ? parsed.error
          : parsed.error?.code) ??
        parsed.code ??
        '';
    } catch {
      // Non-JSON body — fall through to status-based classification.
    }
    const normalizedCode = errorCode.toLowerCase();
    const isPermanent =
      response.status === 401 ||
      normalizedCode === 'invalid_grant' ||
      normalizedCode === 'refresh_token_expired' ||
      normalizedCode === 'refresh_token_reused' ||
      normalizedCode === 'refresh_token_invalidated';
    if (isPermanent) {
      await clearChatgptCredentials();
      throw new ChatgptCredentialsClearRequiredError(
        'Your ChatGPT session could not be refreshed. Please sign in again.',
        { status: response.status, errorCode },
      );
    }
    throw new Error(`Failed to refresh token: ${response.status}: ${text}`);
  } finally {
    cleanup();
  }
}

// ---------------------------------------------------------------------------
// Credential persistence (~/.canopy/chatgpt_auth.json)
// ---------------------------------------------------------------------------

function getChatgptCredentialPath(): string {
  return path.join(Storage.getGlobalCanopyDir(), CHATGPT_CREDENTIAL_FILENAME);
}

export async function cacheChatgptCredentials(
  tokens: ExchangedTokens,
): Promise<ChatgptCredentials> {
  const claims = parseChatgptJwtClaims(tokens.id_token);
  const credentials: ChatgptCredentials = {
    ...tokens,
    account_id: claims.chatgpt_account_id,
    last_refresh: new Date().toISOString(),
  };

  const filePath = getChatgptCredentialPath();
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const tempPath = `${filePath}.tmp.${process.pid}.${crypto.randomUUID()}`;
  try {
    await fs.writeFile(tempPath, JSON.stringify(credentials, null, 2), {
      mode: CHATGPT_CREDENTIAL_FILE_MODE,
    });
    try {
      await fs.chmod(tempPath, CHATGPT_CREDENTIAL_FILE_MODE);
    } catch (chmodErr) {
      if (process.platform !== 'win32') {
        throw chmodErr;
      }
    }
    await fs.rename(tempPath, filePath);
  } catch (error) {
    try {
      await fs.unlink(tempPath);
    } catch {
      // best-effort
    }
    throw new Error(
      `Failed to cache ChatGPT credentials to \`${filePath}\`: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
  return credentials;
}

export async function loadChatgptCredentials(): Promise<ParsedChatgptCredentials> {
  const filePath = getChatgptCredentialPath();
  let raw: string;
  try {
    raw = await fs.readFile(filePath, 'utf8');
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      throw new Error(
        'No ChatGPT credentials found. Sign in with ChatGPT first.',
      );
    }
    throw error;
  }
  const parsed = JSON.parse(raw) as ChatgptCredentials;
  if (!parsed.access_token || !parsed.refresh_token || !parsed.id_token) {
    throw new Error(
      `ChatGPT credential file \`${filePath}\` is incomplete. Please sign in again.`,
    );
  }
  let parsedIdToken: ChatgptIdTokenInfo | undefined;
  try {
    parsedIdToken = parseChatgptJwtClaims(parsed.id_token);
  } catch (error) {
    debugLogger.warn(
      'Failed to parse ChatGPT id_token claims:',
      error instanceof Error ? error.message : error,
    );
  }
  return { ...parsed, parsedIdToken };
}

export async function clearChatgptCredentials(): Promise<void> {
  try {
    await fs.unlink(getChatgptCredentialPath());
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return;
    }
    debugLogger.warn(
      'Warning: Failed to clear cached ChatGPT credentials:',
      error,
    );
  }
}

/**
 * Returns usable ChatGPT credentials, refreshing (and persisting the rotated
 * result) when the current access token is expired or about to expire.
 */
export async function ensureFreshChatgptCredentials(): Promise<{
  accessToken: string;
  accountId?: string;
  credentials: ChatgptCredentials;
}> {
  let credentials = await loadChatgptCredentials();
  // The access token's own `exp` governs usability — the id_token exp can
  // differ, so decode the access token directly.
  let expiresAtMs: number | undefined;
  try {
    const accessClaims = decodeJwtPayload(credentials.access_token);
    expiresAtMs =
      typeof accessClaims['exp'] === 'number'
        ? accessClaims['exp'] * 1000
        : undefined;
  } catch {
    expiresAtMs = undefined;
  }
  expiresAtMs ??= Date.parse(credentials.last_refresh) + 28 * 24 * 3600_000;
  if (Date.now() >= expiresAtMs - EXPIRY_SAFETY_MARGIN_MS) {
    debugLogger.debug('ChatGPT access token stale; refreshing');
    const refreshed = await requestTokenRefresh(credentials.refresh_token);
    credentials = await cacheChatgptCredentials({
      id_token: refreshed.id_token ?? credentials.id_token,
      access_token: refreshed.access_token,
      refresh_token: refreshed.refresh_token ?? credentials.refresh_token,
    });
  }
  return {
    accessToken: credentials.access_token,
    accountId:
      credentials.account_id ?? credentials.parsedIdToken?.chatgpt_account_id,
    credentials,
  };
}

export async function hasChatgptCredentials(): Promise<boolean> {
  try {
    await loadChatgptCredentials();
    return true;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// Interactive login (port of server.rs run_login_server)
// ---------------------------------------------------------------------------

export interface LoginWithChatgptResult {
  credentials: ChatgptCredentials;
  email?: string;
  planType?: string;
}

export class LoginCancelledError extends Error {
  constructor(message = 'Login cancelled') {
    super(message);
    this.name = 'LoginCancelledError';
  }
}

interface CallbackServerHandle {
  authUrl: string;
  port: number;
  done: Promise<ExchangedTokens>;
  /** Resolves once the browser has fetched the /success page (or timed out). */
  successShown: Promise<void>;
  cancel: () => void;
  close: () => void;
}

function escapeHtml(input: string): string {
  return input.replace(/[&<>"']/g, (ch) => {
    switch (ch) {
      case '&':
        return '&amp;';
      case '<':
        return '&lt;';
      case '>':
        return '&gt;';
      case '"':
        return '&quot;';
      default:
        return '&#39;';
    }
  });
}

function renderSuccessPage(email?: string): string {
  return `<!DOCTYPE html>
<html>
  <head><title>Canopy Code</title></head>
  <body style="font-family: sans-serif; display: grid; place-items: center; height: 100vh; margin: 0;">
    <div style="text-align: center;">
      <h1>You're signed in!</h1>
      <p>Return to Canopy Code to continue.${email ? `<br/>Signed in as <strong>${escapeHtml(email)}</strong>` : ''}</p>
    </div>
  </body>
</html>`;
}

function renderErrorPage(message: string): string {
  return `<!DOCTYPE html>
<html>
  <head><title>Canopy Code</title></head>
  <body style="font-family: sans-serif; display: grid; place-items: center; height: 100vh; margin: 0;">
    <div style="text-align: center;">
      <h1>Sign-in could not be completed</h1>
      <p>${escapeHtml(message)}</p>
      <p>Return to Canopy Code to retry.</p>
    </div>
  </body>
</html>`;
}

/**
 * Runs the localhost callback server and resolves with exchanged tokens once
 * the browser completes the flow. Port preference mirrors codex: 1455 with a
 * 1457 fallback.
 */
function startCallbackServer(
  pkce: PkceCodes,
  state: string,
): Promise<CallbackServerHandle> {
  return new Promise((resolveServer, rejectServer) => {
    let settled = false;
    let resolvedPort = CHATGPT_OAUTH_DEFAULT_PORT;
    let cancelRequested = false;

    const server = http.createServer((req, res) => {
      void handleRequest(req, res);
    });

    let resolveDone!: (tokens: ExchangedTokens) => void;
    let rejectDone!: (error: Error) => void;
    const done = new Promise<ExchangedTokens>((resolve, reject) => {
      resolveDone = resolve;
      rejectDone = reject;
      server.on('close', () => {
        if (!settled) {
          settled = true;
          clearTimeout(timeoutHandle);
          reject(
            cancelRequested
              ? new LoginCancelledError()
              : new Error('Login was not completed'),
          );
        }
      });
    });

    let resolveSuccessShown!: () => void;
    const successShown = new Promise<void>((resolve) => {
      resolveSuccessShown = resolve;
    });

    function close(): void {
      server.close(() => {
        /* noop — 'close' listener handles settle */
      });
      // Unref any keep-alive sockets so the process can exit.
      server.closeAllConnections?.();
    }

    function settleError(error: Error): void {
      if (settled) return;
      settled = true;
      clearTimeout(timeoutHandle);
      close();
      rejectDone(error);
    }

    async function handleRequest(
      req: http.IncomingMessage,
      res: http.ServerResponse,
    ): Promise<void> {
      const url = new URL(req.url ?? '/', `http://127.0.0.1:${resolvedPort}`);
      if (url.pathname !== '/auth/callback') {
        if (url.pathname === '/cancel') {
          cancelRequested = true;
          res.end('Login cancelled');
          close();
          return;
        }
        if (url.pathname === '/success') {
          res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
          res.end(renderSuccessPage());
          close();
          resolveSuccessShown();
          return;
        }
        res.statusCode = 404;
        res.end('Not Found');
        return;
      }

      const callbackState = url.searchParams.get('state');
      if (!callbackState || callbackState !== state) {
        debugLogger.warn('login callback state mismatch');
        res.statusCode = 400;
        res.end('State mismatch');
        return;
      }

      const errorCode = url.searchParams.get('error');
      if (errorCode) {
        const description = url.searchParams.get('error_description') ?? '';
        const message = description
          ? `Sign-in failed: ${description}`
          : `Sign-in failed: ${errorCode}`;
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderErrorPage(message));
        settleError(new Error(message));
        return;
      }

      const code = url.searchParams.get('code');
      if (!code) {
        const message = 'Missing authorization code.';
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderErrorPage(message));
        settleError(new Error(message));
        return;
      }

      try {
        const tokens = await exchangeCodeForTokens({
          code,
          redirectUri: `http://localhost:${resolvedPort}/auth/callback`,
          pkce,
        });
        res.writeHead(302, { Location: `/success` });
        res.end();
        settled = true;
        clearTimeout(timeoutHandle);
        resolveDone(tokens);
      } catch (error) {
        const message = `Token exchange failed: ${
          error instanceof Error ? error.message : String(error)
        }`;
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderErrorPage(message));
        settleError(new Error(message));
      }
    }

    const timeoutHandle: NodeJS.Timeout = setTimeout(() => {
      settleError(new Error('Login timed out. Please try again.'));
    }, LOGIN_TIMEOUT_MS);
    timeoutHandle.unref();

    const tryListen = (port: number, isFallback: boolean): void => {
      const onError = async (err: NodeJS.ErrnoException): Promise<void> => {
        if (err.code !== 'EADDRINUSE' || isFallback) {
          clearTimeout(timeoutHandle);
          rejectServer(err);
          return;
        }
        await new Promise((r) => setTimeout(r, 200));
        tryListen(CHATGPT_OAUTH_FALLBACK_PORT, true);
      };
      server.once('error', onError);
      server.listen(port, '127.0.0.1', () => {
        server.off('error', onError);
        resolvedPort = (server.address() as AddressInfo).port;
        resolveServer({
          authUrl: buildAuthorizeUrl({
            redirectUri: `http://localhost:${resolvedPort}/auth/callback`,
            pkce,
            state,
          }),
          port: resolvedPort,
          done: done as Promise<ExchangedTokens>,
          successShown,
          cancel: () => {
            cancelRequested = true;
            close();
          },
          close,
        });
      });
    };

    tryListen(CHATGPT_OAUTH_DEFAULT_PORT, false);
  });
}

/**
 * Guarantees usable ChatGPT credentials: returns immediately when cached
 * tokens are fresh; otherwise refreshes them, or (when interactive) runs the
 * full browser sign-in flow.
 */
export async function ensureChatgptAuthenticated(
  config?: Config,
): Promise<void> {
  try {
    await ensureFreshChatgptCredentials();
    return;
  } catch (error) {
    if (!(error instanceof ChatgptCredentialsClearRequiredError)) {
      // Network/transport failure during refresh — surface it rather than
      // silently starting an interactive login.
      if (await hasChatgptCredentials()) {
        throw error;
      }
    }
  }

  if (config && !config.isInteractive()) {
    throw new Error(
      'ChatGPT credentials are missing or expired. Run Canopy Code interactively and sign in with ChatGPT via /auth.',
    );
  }
  await loginWithChatgpt(config as Config);
}

/**
 * Full interactive ChatGPT login: opens the browser, waits for the callback,
 * exchanges the code, and caches credentials to disk.
 */
export async function loginWithChatgpt(
  config: Config,
): Promise<LoginWithChatgptResult> {
  const pkce = generatePkce();
  const state = generateState();

  const server = await startCallbackServer(pkce, state);

  if (config.isBrowserLaunchSuppressed()) {
    process.stderr.write(
      `\nSign in with ChatGPT:\nPlease visit the following URL to authenticate:\n${server.authUrl}\n\n`,
    );
  } else {
    try {
      await openBrowserSecurely(server.authUrl);
    } catch (err) {
      debugLogger.warn(
        `Failed to open browser automatically: ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
      process.stderr.write(
        `Please open this URL manually to sign in:\n${server.authUrl}\n`,
      );
    }
  }

  try {
    const tokens = await server.done;
    const credentials = await cacheChatgptCredentials(tokens);
    const claims = parseChatgptJwtClaims(tokens.id_token);
    debugLogger.debug(
      `ChatGPT login complete for ${claims.email ?? 'unknown user'}`,
    );
    // Give the browser a moment to load the /success page before tearing
    // down the server — otherwise it gets a connection error after the
    // redirect. Never block longer than 2s on it.
    await Promise.race([
      server.successShown,
      new Promise((r) => setTimeout(r, 2000).unref()),
    ]);
    return {
      credentials,
      email: claims.email,
      planType: claims.chatgpt_plan_type,
    };
  } finally {
    server.cancel();
  }
}
