import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import path from 'node:path';
import { Storage } from '@canopy-code/canopy-code-core';
import type { LoadedSettings } from '../config/settings.js';
import { runWithAcpRuntimeOutputDir } from './runtimeOutputDirContext.js';

describe('runWithAcpRuntimeOutputDir', () => {
  beforeEach(() => {
    Storage.setRuntimeBaseDir(null);
    delete process.env['CANOPY_RUNTIME_DIR'];
  });

  afterEach(() => {
    Storage.setRuntimeBaseDir(null);
    delete process.env['CANOPY_RUNTIME_DIR'];
  });

  it('uses the merged runtimeOutputDir relative to cwd within the async context', async () => {
    const cwd = path.resolve('workspace', 'project-a');
    const settings = {
      merged: {
        advanced: {
          runtimeOutputDir: '.canopy-runtime',
        },
      },
    } as LoadedSettings;

    await runWithAcpRuntimeOutputDir(settings, cwd, async () => {
      expect(Storage.getRuntimeBaseDir()).toBe(
        path.join(cwd, '.canopy-runtime'),
      );
    });

    expect(Storage.getRuntimeBaseDir()).toBe(Storage.getGlobalCanopyDir());
  });
});
