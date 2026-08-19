/**
 * @license
 * Copyright 2025 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { afterEach, describe, expect, it, vi } from 'vitest';
import { _setSandboxMountExistsForTest } from '@qwen-code/acp-bridge/workspacePaths';
import { parseOptionalWorkspaceCwd } from './dispatch.js';

// #7139 wiring: the ACP JSON-RPC `cwd` entry point must translate a
// Windows-shaped path to its bind mount BEFORE its absolute-path guard —
// this is the dispatch-side sibling of request-helpers.sandbox.test.ts.
describe('ACP dispatch parseOptionalWorkspaceCwd inside a POSIX container sandbox (#7139)', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    _setSandboxMountExistsForTest(undefined);
  });

  it.skipIf(process.platform === 'win32')(
    'accepts a Windows-shaped cwd and returns its bind-mount location',
    () => {
      vi.stubEnv('SANDBOX', 'canopy-code-sandbox-0');
      _setSandboxMountExistsForTest((p) => p === '/c/canopy-repro');
      expect(
        parseOptionalWorkspaceCwd(
          { cwd: 'C:\\canopy-repro' },
          '/c/canopy-repro',
        ),
      ).toBe('/c/canopy-repro');
    },
  );

  it.skipIf(process.platform === 'win32')(
    'still rejects a Windows-shaped cwd outside a sandbox',
    () => {
      vi.stubEnv('SANDBOX', '');
      expect(() =>
        parseOptionalWorkspaceCwd({ cwd: 'C:\\canopy-repro' }, '/tmp'),
      ).toThrow('`cwd` must be an absolute path when provided');
    },
  );
});
