/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { buildCurrentCanopyCliArgv } from './current-cli-argv.js';

const originalArgv = [...process.argv];
const originalDev = process.env['DEV'];

describe('buildCurrentCanopyCliArgv', () => {
  afterEach(() => {
    process.argv.splice(0, process.argv.length, ...originalArgv);
    if (originalDev === undefined) {
      delete process.env['DEV'];
    } else {
      process.env['DEV'] = originalDev;
    }
  });

  it('uses the current JavaScript entrypoint by default', () => {
    process.argv[1] = '/tmp/canopy/dist/index.js';
    delete process.env['DEV'];

    expect(buildCurrentCanopyCliArgv(['agents'])).toEqual([
      process.execPath,
      '/tmp/canopy/dist/index.js',
      'agents',
    ]);
  });

  it('falls back to canopy when process.argv[1] is undefined', () => {
    process.argv.splice(1, 1);
    delete process.env['DEV'];

    expect(buildCurrentCanopyCliArgv(['agents'])).toEqual(['canopy', 'agents']);
  });

  it('uses local tsx for dev TypeScript entrypoints', async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'canopy-dev-argv-'));
    const entrypoint = path.join(root, 'packages', 'cli', 'index.ts');
    const tsxCli = path.join(root, 'node_modules', 'tsx', 'dist', 'cli.mjs');
    await fs.mkdir(path.dirname(entrypoint), { recursive: true });
    await fs.mkdir(path.dirname(tsxCli), { recursive: true });
    await fs.writeFile(tsxCli, '');
    process.argv[1] = entrypoint;
    process.env['DEV'] = 'true';

    try {
      expect(buildCurrentCanopyCliArgv(['--bg', 'hello'])).toEqual([
        process.execPath,
        tsxCli,
        entrypoint,
        '--bg',
        'hello',
      ]);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it('throws when DEV=true with a TypeScript entrypoint but tsx is missing', async () => {
    const entryRoot = await fs.mkdtemp(
      path.join(os.tmpdir(), 'canopy-entry-argv-'),
    );
    const cwdRoot = await fs.mkdtemp(
      path.join(os.tmpdir(), 'canopy-cwd-argv-'),
    );
    const entrypoint = path.join(entryRoot, 'packages', 'cli', 'index.ts');
    const cwdTsxCli = path.join(
      cwdRoot,
      'node_modules',
      'tsx',
      'dist',
      'cli.mjs',
    );
    await fs.mkdir(path.dirname(entrypoint), { recursive: true });
    await fs.mkdir(path.dirname(cwdTsxCli), { recursive: true });
    await fs.writeFile(cwdTsxCli, '');
    process.argv[1] = entrypoint;
    process.env['DEV'] = 'true';
    const originalCwd = process.cwd();

    try {
      process.chdir(cwdRoot);

      expect(() => buildCurrentCanopyCliArgv(['agents'])).toThrow(
        /tsx was not found/,
      );
    } finally {
      process.chdir(originalCwd);
      await fs.rm(entryRoot, { recursive: true, force: true });
      await fs.rm(cwdRoot, { recursive: true, force: true });
    }
  });
});
