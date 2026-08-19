/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import * as path from 'node:path';
import { homedir } from 'node:os';
import { getUserSettingsDir, getUserSettingsPath } from './settings.js';
import { getTrustedFoldersPath } from './trustedFolders.js';

// Regression guard: `QWEN_HOME` is resolved by `preResolveHomeEnvOverrides()`
// AFTER any module that imports a settings/trustedFolders path has loaded.
// A top-level `const` would freeze the pre-bootstrap value and split state
// across callers. Each test mutates `process.env.QWEN_HOME` post-load and
// asserts the exported path getters reflect the new value.

describe('settings/trustedFolders path getters are lazy', () => {
  let originalCanopyHome: string | undefined;
  let originalTrustedPath: string | undefined;

  beforeEach(() => {
    originalCanopyHome = process.env['QWEN_HOME'];
    originalTrustedPath = process.env['CANOPY_CODE_TRUSTED_FOLDERS_PATH'];
    delete process.env['QWEN_HOME'];
    delete process.env['CANOPY_CODE_TRUSTED_FOLDERS_PATH'];
  });

  afterEach(() => {
    if (originalCanopyHome === undefined) delete process.env['QWEN_HOME'];
    else process.env['QWEN_HOME'] = originalCanopyHome;
    if (originalTrustedPath === undefined)
      delete process.env['CANOPY_CODE_TRUSTED_FOLDERS_PATH'];
    else process.env['CANOPY_CODE_TRUSTED_FOLDERS_PATH'] = originalTrustedPath;
  });

  it('getUserSettingsPath() reflects QWEN_HOME set after module load', () => {
    const defaultPath = getUserSettingsPath();
    expect(defaultPath).toBe(path.join(homedir(), '.canopy', 'settings.json'));

    process.env['QWEN_HOME'] = '/tmp/canopy-lazy-test';
    expect(getUserSettingsPath()).toBe(
      path.join('/tmp/canopy-lazy-test', 'settings.json'),
    );
  });

  it('getUserSettingsDir() reflects QWEN_HOME set after module load', () => {
    expect(getUserSettingsDir()).toBe(path.join(homedir(), '.canopy'));

    process.env['QWEN_HOME'] = '/tmp/canopy-lazy-test';
    expect(getUserSettingsDir()).toBe(path.normalize('/tmp/canopy-lazy-test'));
  });

  it('getTrustedFoldersPath() reflects QWEN_HOME set after module load', () => {
    expect(getTrustedFoldersPath()).toBe(
      path.join(homedir(), '.canopy', 'trustedFolders.json'),
    );

    process.env['QWEN_HOME'] = '/tmp/canopy-lazy-test';
    expect(getTrustedFoldersPath()).toBe(
      path.join('/tmp/canopy-lazy-test', 'trustedFolders.json'),
    );
  });
});
