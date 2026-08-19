/**
 * @license
 * Copyright 2025 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { afterEach, describe, expect, it, vi } from 'vitest';
import { _setSandboxMountExistsForTest } from '@qwen-code/acp-bridge/workspacePaths';
import { validateAndCanonicalizeWorkspaceInput } from './run-canopy-serve.js';

// #7139 wiring for the PRIMARY reproduction path: `canopy serve --workspace
// C:\canopy-repro` relaunched into a Linux Docker sandbox. The boot validator
// must translate to the bind mount BEFORE its absolute-path guard. statSync
// is mocked so the (root-level, uncreatable) translated mount stats as a
// directory; acp-bridge's canonicalizeWorkspace then falls back to the
// resolved path on ENOENT, matching a real container where the mount exists.
vi.mock('node:fs', async (importOriginal) => {
  const actual = (await importOriginal()) as typeof import('node:fs');
  return {
    ...actual,
    statSync: ((p: unknown, ...rest: unknown[]) =>
      p === '/c/canopy-repro'
        ? ({ isDirectory: () => true } as ReturnType<typeof actual.statSync>)
        : (actual.statSync as (...a: unknown[]) => unknown)(
            p,
            ...rest,
          )) as typeof actual.statSync,
  };
});

describe('validateAndCanonicalizeWorkspaceInput inside a POSIX container sandbox (#7139)', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    _setSandboxMountExistsForTest(undefined);
  });

  it.skipIf(process.platform === 'win32')(
    'boots a Windows-shaped --workspace via its bind-mount location',
    () => {
      vi.stubEnv('SANDBOX', 'canopy-code-sandbox-0');
      _setSandboxMountExistsForTest((p) => p === '/c/canopy-repro');
      expect(validateAndCanonicalizeWorkspaceInput('C:\\canopy-repro')).toBe(
        '/c/canopy-repro',
      );
    },
  );

  it.skipIf(process.platform === 'win32')(
    'still rejects a Windows-shaped --workspace outside a sandbox',
    () => {
      vi.stubEnv('SANDBOX', '');
      expect(() =>
        validateAndCanonicalizeWorkspaceInput('C:\\canopy-repro'),
      ).toThrow('must be an absolute path');
    },
  );

  it('echoes the operator-typed input in the rejection, not the translation result', () => {
    vi.stubEnv('SANDBOX', '');
    // The extraction renamed the parameter; the message must interpolate
    // the RAW input ("relative/path"), never the null translation result.
    expect(() =>
      validateAndCanonicalizeWorkspaceInput('relative/path'),
    ).toThrow('Invalid --workspace "relative/path": must be an absolute path.');
  });
});
