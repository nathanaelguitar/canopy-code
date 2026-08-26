/**
 * @license
 * Copyright 2026 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import crypto from 'crypto';
import {
  generatePkce,
  buildAuthorizeUrl,
  parseChatgptJwtClaims,
  exchangeCodeForTokens,
  ChatgptCredentialsClearRequiredError,
} from './chatgpt-oauth.js';

function makeJwt(claims: Record<string, unknown>): string {
  const header = Buffer.from(
    JSON.stringify({ alg: 'RS256', typ: 'JWT' }),
  ).toString('base64url');
  const payload = Buffer.from(JSON.stringify(claims)).toString('base64url');
  const signature = Buffer.from('sig').toString('base64url');
  return `${header}.${payload}.${signature}`;
}

describe('generatePkce', () => {
  it('produces a base64url verifier and matching S256 challenge', () => {
    const { code_verifier, code_challenge } = generatePkce();
    expect(code_verifier.length).toBeGreaterThanOrEqual(43);
    expect(code_verifier).not.toMatch(/[+/=]/);
    const expected = crypto
      .createHash('sha256')
      .update(code_verifier)
      .digest('base64url');
    expect(code_challenge).toBe(expected);
  });

  it('generates unique pairs', () => {
    const a = generatePkce();
    const b = generatePkce();
    expect(a.code_verifier).not.toBe(b.code_verifier);
  });
});

describe('buildAuthorizeUrl', () => {
  it('includes the codex client id, PKCE and state parameters', () => {
    const url = new URL(
      buildAuthorizeUrl({
        redirectUri: 'http://localhost:1455/auth/callback',
        pkce: { code_verifier: 'v', code_challenge: 'c' },
        state: 's3cret',
      }),
    );
    expect(url.origin + url.pathname).toBe(
      'https://auth.openai.com/oauth/authorize',
    );
    expect(url.searchParams.get('response_type')).toBe('code');
    expect(url.searchParams.get('client_id')).toBe(
      'app_EMoamEEZ73f0CkXaXp7hrann',
    );
    expect(url.searchParams.get('redirect_uri')).toBe(
      'http://localhost:1455/auth/callback',
    );
    expect(url.searchParams.get('code_challenge')).toBe('c');
    expect(url.searchParams.get('code_challenge_method')).toBe('S256');
    expect(url.searchParams.get('state')).toBe('s3cret');
    expect(url.searchParams.get('scope')).toContain('offline_access');
    expect(url.searchParams.get('originator')).toBeTruthy();
  });
});

describe('parseChatgptJwtClaims', () => {
  it('extracts email and auth claims', () => {
    const jwt = makeJwt({
      email: 'user@example.com',
      'https://api.openai.com/auth': {
        chatgpt_plan_type: 'plus',
        chatgpt_account_id: 'acct-1',
        chatgpt_user_id: 'user-1',
      },
      exp: 1700000000,
    });
    const claims = parseChatgptJwtClaims(jwt);
    expect(claims.email).toBe('user@example.com');
    expect(claims.chatgpt_plan_type).toBe('plus');
    expect(claims.chatgpt_account_id).toBe('acct-1');
    expect(claims.chatgpt_user_id).toBe('user-1');
    expect(claims.expires_at_ms).toBe(1700000000_000);
  });

  it('falls back to profile claim for email and user_id for user id', () => {
    const jwt = makeJwt({
      'https://api.openai.com/profile': { email: 'profile@example.com' },
      'https://api.openai.com/auth': { user_id: 'uid' },
    });
    const claims = parseChatgptJwtClaims(jwt);
    expect(claims.email).toBe('profile@example.com');
    expect(claims.chatgpt_user_id).toBe('uid');
  });

  it('throws on malformed tokens', () => {
    expect(() => parseChatgptJwtClaims('not-a-jwt')).toThrow();
  });
});

describe('exchangeCodeForTokens', () => {
  beforeEach(() => {
    vi.unstubAllGlobals();
  });

  it('posts a form-encoded authorization_code grant', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          id_token: 'i',
          access_token: 'a',
          refresh_token: 'r',
        }),
        { status: 200 },
      ),
    );
    vi.stubGlobal('fetch', fetchMock);

    const tokens = await exchangeCodeForTokens({
      code: 'abc',
      redirectUri: 'http://localhost:1455/auth/callback',
      pkce: { code_verifier: 'v', code_challenge: 'c' },
    });
    expect(tokens.access_token).toBe('a');

    const [url, init] = fetchMock.mock.calls[0] as [
      string,
      RequestInit & { body: string },
    ];
    expect(url).toBe('https://auth.openai.com/oauth/token');
    const params = new URLSearchParams(init.body);
    expect(params.get('grant_type')).toBe('authorization_code');
    expect(params.get('code')).toBe('abc');
    expect(params.get('client_id')).toBe('app_EMoamEEZ73f0CkXaXp7hrann');
    expect(params.get('code_verifier')).toBe('v');
  });

  it('surfaces non-200 responses as errors', async () => {
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValue(
          new Response('{"error":"invalid_grant"}', { status: 400 }),
        ),
    );
    await expect(
      exchangeCodeForTokens({
        code: 'bad',
        redirectUri: 'http://localhost:1455/auth/callback',
        pkce: { code_verifier: 'v', code_challenge: 'c' },
      }),
    ).rejects.toThrow(/status 400/);
  });
});

describe('ChatgptCredentialsClearRequiredError', () => {
  it('is named and carries original error', () => {
    const cause = new Error('boom');
    const err = new ChatgptCredentialsClearRequiredError('msg', cause);
    expect(err.name).toBe('ChatgptCredentialsClearRequiredError');
    expect(err.originalError).toBe(cause);
  });
});
