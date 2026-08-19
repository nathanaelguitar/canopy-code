/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as os from 'node:os';
import * as path from 'node:path';
import { Storage } from './storage.js';

const mockRealpathSync = vi.hoisted(() => vi.fn());

vi.mock('node:fs', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs')>();
  const mocked = {
    ...actual,
    realpathSync: mockRealpathSync,
  };
  return {
    ...mocked,
    default: mocked,
  };
});

const actualFs = await vi.importActual<typeof import('node:fs')>('node:fs');

function createEnoent(pathToResolve: string): NodeJS.ErrnoException {
  const error = new Error(
    `ENOENT: no such file or directory, realpath '${pathToResolve}'`,
  ) as NodeJS.ErrnoException;
  error.code = 'ENOENT';
  return error;
}

function mockRealpath(
  resolutions: Map<string, string>,
  missingPaths = new Set<string>(),
): void {
  mockRealpathSync.mockImplementation((pathToResolve) => {
    const resolvedPath = pathToResolve.toString();
    if (missingPaths.has(resolvedPath)) {
      throw createEnoent(resolvedPath);
    }
    return resolutions.get(resolvedPath) ?? resolvedPath;
  });
}

describe('Storage – getGlobalSettingsPath', () => {
  it('returns path to ~/.canopy/settings.json', () => {
    const expected = path.join(os.homedir(), '.canopy', 'settings.json');
    expect(Storage.getGlobalSettingsPath()).toBe(expected);
  });
});

describe('Storage – additional helpers', () => {
  const projectRoot = '/tmp/project';
  const storage = new Storage(projectRoot);

  it('getWorkspaceSettingsPath returns project/.canopy/settings.json', () => {
    const expected = path.join(projectRoot, '.canopy', 'settings.json');
    expect(storage.getWorkspaceSettingsPath()).toBe(expected);
  });

  it('getUserCommandsDir returns ~/.canopy/commands', () => {
    const expected = path.join(os.homedir(), '.canopy', 'commands');
    expect(Storage.getUserCommandsDir()).toBe(expected);
  });

  it('getProjectCommandsDir returns project/.canopy/commands', () => {
    const expected = path.join(projectRoot, '.canopy', 'commands');
    expect(storage.getProjectCommandsDir()).toBe(expected);
  });

  it('getMcpOAuthTokensPath returns ~/.canopy/mcp-oauth-tokens.json', () => {
    const expected = path.join(
      os.homedir(),
      '.canopy',
      'mcp-oauth-tokens.json',
    );
    expect(Storage.getMcpOAuthTokensPath()).toBe(expected);
  });
});

describe('Storage – getRuntimeBaseDir / setRuntimeBaseDir', () => {
  const originalEnv = process.env['CANOPY_RUNTIME_DIR'];

  beforeEach(() => {
    // Reset state before each test
    Storage.setRuntimeBaseDir(null);
    delete process.env['CANOPY_RUNTIME_DIR'];
  });

  afterEach(() => {
    // Restore original env
    Storage.setRuntimeBaseDir(null);
    if (originalEnv !== undefined) {
      process.env['CANOPY_RUNTIME_DIR'] = originalEnv;
    } else {
      delete process.env['CANOPY_RUNTIME_DIR'];
    }
  });

  it('defaults to getGlobalCanopyDir() when nothing is configured', () => {
    expect(Storage.getRuntimeBaseDir()).toBe(Storage.getGlobalCanopyDir());
  });

  it('uses setRuntimeBaseDir value when set with absolute path', () => {
    const runtimeDir = path.resolve('custom', 'runtime');
    Storage.setRuntimeBaseDir(runtimeDir);
    expect(Storage.getRuntimeBaseDir()).toBe(runtimeDir);
  });

  it('env var CANOPY_RUNTIME_DIR takes priority over setRuntimeBaseDir', () => {
    const settingsDir = path.resolve('from-settings');
    const envDir = path.resolve('from-env');
    Storage.setRuntimeBaseDir(settingsDir);
    process.env['CANOPY_RUNTIME_DIR'] = envDir;
    expect(Storage.getRuntimeBaseDir()).toBe(envDir);
  });

  it('expands tilde (~) in setRuntimeBaseDir', () => {
    Storage.setRuntimeBaseDir('~/custom-runtime');
    const expected = path.join(os.homedir(), 'custom-runtime');
    expect(Storage.getRuntimeBaseDir()).toBe(expected);
  });

  it('expands Windows-style tilde paths in setRuntimeBaseDir', () => {
    Storage.setRuntimeBaseDir('~\\custom-runtime');
    const expected = path.join(os.homedir(), 'custom-runtime');
    expect(Storage.getRuntimeBaseDir()).toBe(expected);
  });

  it('expands tilde (~) in CANOPY_RUNTIME_DIR env var', () => {
    process.env['CANOPY_RUNTIME_DIR'] = '~/env-runtime';
    const expected = path.join(os.homedir(), 'env-runtime');
    expect(Storage.getRuntimeBaseDir()).toBe(expected);
  });

  it('resolves relative paths in setRuntimeBaseDir using process.cwd by default', () => {
    Storage.setRuntimeBaseDir('relative/path');
    const expected = path.resolve('relative/path');
    expect(Storage.getRuntimeBaseDir()).toBe(expected);
  });

  it('resolves relative paths in setRuntimeBaseDir using explicit cwd', () => {
    const cwd = path.resolve('workspace', 'projectA');
    Storage.setRuntimeBaseDir('.canopy', cwd);
    expect(Storage.getRuntimeBaseDir()).toBe(path.join(cwd, '.canopy'));
  });

  it('ignores cwd when path is absolute', () => {
    const absolutePath = path.resolve('absolute', 'path');
    const cwd = path.resolve('workspace', 'projectA');
    Storage.setRuntimeBaseDir(absolutePath, cwd);
    expect(Storage.getRuntimeBaseDir()).toBe(absolutePath);
  });

  it('ignores cwd when path starts with tilde', () => {
    Storage.setRuntimeBaseDir(
      '~/runtime',
      path.resolve('workspace', 'projectA'),
    );
    const expected = path.join(os.homedir(), 'runtime');
    expect(Storage.getRuntimeBaseDir()).toBe(expected);
  });

  it('resolves relative paths in CANOPY_RUNTIME_DIR env var', () => {
    process.env['CANOPY_RUNTIME_DIR'] = 'relative/env-path';
    const expected = path.resolve('relative/env-path');
    expect(Storage.getRuntimeBaseDir()).toBe(expected);
  });

  it('resets to default when setRuntimeBaseDir is called with null', () => {
    const customDir = path.resolve('custom');
    Storage.setRuntimeBaseDir(customDir);
    expect(Storage.getRuntimeBaseDir()).toBe(customDir);

    Storage.setRuntimeBaseDir(null);
    expect(Storage.getRuntimeBaseDir()).toBe(Storage.getGlobalCanopyDir());
  });

  it('resets to default when setRuntimeBaseDir is called with undefined', () => {
    Storage.setRuntimeBaseDir(path.resolve('custom'));
    Storage.setRuntimeBaseDir(undefined);
    expect(Storage.getRuntimeBaseDir()).toBe(Storage.getGlobalCanopyDir());
  });

  it('resets to default when setRuntimeBaseDir is called with empty string', () => {
    Storage.setRuntimeBaseDir(path.resolve('custom'));
    Storage.setRuntimeBaseDir('');
    expect(Storage.getRuntimeBaseDir()).toBe(Storage.getGlobalCanopyDir());
  });

  it('handles bare tilde (~) as home directory', () => {
    Storage.setRuntimeBaseDir('~');
    expect(Storage.getRuntimeBaseDir()).toBe(path.normalize(os.homedir()));
  });
});

describe('Storage – getPlansDir', () => {
  const projectRoot = path.resolve('workspace', 'project');

  beforeEach(() => {
    mockRealpathSync.mockImplementation((pathToResolve) =>
      actualFs.realpathSync(pathToResolve),
    );
  });

  afterEach(() => {
    mockRealpathSync.mockReset();
  });

  it('defaults to ~/.canopy/plans when plansDirectory is not configured', () => {
    expect(Storage.getPlansDir(projectRoot)).toBe(
      path.join(Storage.getGlobalCanopyDir(), 'plans'),
    );
  });

  it('resolves relative plansDirectory values against the project root', () => {
    expect(Storage.getPlansDir(projectRoot, './project-plans')).toBe(
      path.join(projectRoot, 'project-plans'),
    );
  });

  it('allows project subdirectories whose names start with two dots', () => {
    expect(Storage.getPlansDir(projectRoot, './..plans')).toBe(
      path.join(projectRoot, '..plans'),
    );
  });

  it('expands tilde in configured plansDirectory values', () => {
    const projectInHome = path.join(os.homedir(), 'workspace', 'project');
    expect(
      Storage.getPlansDir(projectInHome, '~/workspace/project/plans'),
    ).toBe(path.join(projectInHome, 'plans'));
  });

  it('allows absolute plansDirectory values inside the project root', () => {
    const plansDir = path.join(projectRoot, 'nested', 'plans');
    expect(Storage.getPlansDir(projectRoot, plansDir)).toBe(plansDir);
  });

  it('rejects relative plansDirectory values that escape the project root', () => {
    expect(() => Storage.getPlansDir(projectRoot, '../plans')).toThrow(
      'plansDirectory must resolve within the project root',
    );
  });

  it('rejects absolute plansDirectory values outside the project root', () => {
    const outsideProject = path.join(path.dirname(projectRoot), 'plans');
    expect(() => Storage.getPlansDir(projectRoot, outsideProject)).toThrow(
      'plansDirectory must resolve within the project root',
    );
  });

  it('requires projectRoot when plansDirectory is configured', () => {
    expect(() => Storage.getPlansDir(undefined, './plans')).toThrow(
      'projectRoot is required when plansDirectory is configured',
    );
    expect(() => Storage.getPlansDir(null, './plans')).toThrow(
      'projectRoot is required when plansDirectory is configured',
    );
  });

  it('rejects Windows-style absolute path outside the project root', () => {
    // Simulate project root on C: drive and plansDirectory on D: drive
    const projectOnC = path.resolve('C:', 'work', 'project');
    const plansOnD = path.resolve('D:', 'plans');
    expect(() => Storage.getPlansDir(projectOnC, plansOnD)).toThrow(
      'plansDirectory must resolve within the project root',
    );
  });

  it('rejects path with mixed separators that escapes project root', () => {
    // On Windows, path.resolve normalizes backslashes as path separators.
    // On POSIX, backslashes are literal characters, so this traversal
    // is inherently Windows-specific and should be guarded.
    if (process.platform !== 'win32') {
      return;
    }
    const tricky = '..\\..\\plans'; // backslashes with traversal
    expect(() => Storage.getPlansDir(projectRoot, tricky)).toThrow(
      'plansDirectory must resolve within the project root',
    );
  });

  it('rejects symlink pointing outside the project root', () => {
    const project = path.resolve('tmp', 'project');
    const outside = path.resolve('tmp', 'outside');
    const symlink = path.join(project, 'escape-link');
    mockRealpath(
      new Map([
        [project, project],
        [symlink, outside],
      ]),
    );

    expect(() => Storage.getPlansDir(project, './escape-link')).toThrow(
      'plansDirectory must resolve within the project root',
    );
  });

  it('allows legitimate symlink that stays within project root', () => {
    const project = path.resolve('tmp', 'project');
    const target = path.join(project, 'plans-target');
    const symlink = path.join(project, 'plans-link');
    mockRealpath(
      new Map([
        [project, project],
        [symlink, target],
      ]),
    );

    const result = Storage.getPlansDir(project, './plans-link');
    // The configured symlink path is accepted as long as it stays inside
    // the project root.
    expect(result).toBe(symlink);
  });

  it('rejects missing nested path under symlink that escapes project root', () => {
    const project = path.resolve('tmp', 'project');
    const outside = path.resolve('tmp', 'outside');
    const dataSymlink = path.join(project, 'data');
    const missingSubdir = path.join(dataSymlink, 'subdir');
    const missingPlans = path.join(missingSubdir, 'plans');
    mockRealpath(
      new Map([
        [project, project],
        [dataSymlink, outside],
      ]),
      new Set([missingPlans, missingSubdir]),
    );

    expect(() => Storage.getPlansDir(project, './data/subdir/plans')).toThrow(
      'plansDirectory must resolve within the project root',
    );
  });

  it('uses configured plansDirectory when building plan file paths', () => {
    expect(Storage.getPlanFilePath('session-123', projectRoot, './plans')).toBe(
      path.join(projectRoot, 'plans', 'session-123.md'),
    );
  });

  it('sanitizes session IDs when building plan file paths', () => {
    expect(
      Storage.getPlanFilePath('../../../escape', projectRoot, './plans'),
    ).toBe(path.join(projectRoot, 'plans', 'escape.md'));
  });
});

describe('Storage – runtime path methods use getRuntimeBaseDir', () => {
  const originalEnv = process.env['CANOPY_RUNTIME_DIR'];

  beforeEach(() => {
    Storage.setRuntimeBaseDir(null);
    delete process.env['CANOPY_RUNTIME_DIR'];
  });

  afterEach(() => {
    Storage.setRuntimeBaseDir(null);
    if (originalEnv !== undefined) {
      process.env['CANOPY_RUNTIME_DIR'] = originalEnv;
    } else {
      delete process.env['CANOPY_RUNTIME_DIR'];
    }
  });

  it('getGlobalTempDir uses custom runtime base dir', () => {
    const customDir = path.resolve('custom');
    Storage.setRuntimeBaseDir(customDir);
    expect(Storage.getGlobalTempDir()).toBe(path.join(customDir, 'tmp'));
  });

  it('getGlobalDebugDir uses custom runtime base dir', () => {
    const customDir = path.resolve('custom');
    Storage.setRuntimeBaseDir(customDir);
    expect(Storage.getGlobalDebugDir()).toBe(path.join(customDir, 'debug'));
  });

  it('getDebugLogPath uses custom runtime base dir', () => {
    const customDir = path.resolve('custom');
    Storage.setRuntimeBaseDir(customDir);
    expect(Storage.getDebugLogPath('session-123')).toBe(
      path.join(customDir, 'debug', 'session-123.txt'),
    );
  });

  it('getGlobalIdeDir is anchored to the global Canopy dir, not runtime base dir', () => {
    const customDir = path.resolve('custom');
    Storage.setRuntimeBaseDir(customDir);
    // IDE lock files are discovery anchors shared with the VS Code companion,
    // which can only see env vars (not settings-based runtimeOutputDir), so
    // getGlobalIdeDir must follow getGlobalCanopyDir to keep both sides aligned.
    expect(Storage.getGlobalIdeDir()).toBe(
      path.join(Storage.getGlobalCanopyDir(), 'ide'),
    );
  });

  it('getProjectDir uses custom runtime base dir', () => {
    const customDir = path.resolve('custom');
    Storage.setRuntimeBaseDir(customDir);
    const storage = new Storage('/tmp/project');
    expect(storage.getProjectDir()).toContain(path.join(customDir, 'projects'));
  });

  it('getProjectTempDir uses custom runtime base dir', () => {
    const customDir = path.resolve('custom');
    Storage.setRuntimeBaseDir(customDir);
    const storage = new Storage('/tmp/project');
    expect(storage.getProjectTempDir()).toContain(path.join(customDir, 'tmp'));
  });

  it('getProjectTempCheckpointsDir uses custom runtime base dir', () => {
    const customDir = path.resolve('custom');
    Storage.setRuntimeBaseDir(customDir);
    const storage = new Storage('/tmp/project');
    expect(storage.getProjectTempCheckpointsDir()).toContain(
      path.join(customDir, 'tmp'),
    );
    expect(storage.getProjectTempCheckpointsDir()).toMatch(/checkpoints$/);
  });

  it('getHistoryFilePath uses custom runtime base dir', () => {
    const customDir = path.resolve('custom');
    Storage.setRuntimeBaseDir(customDir);
    const storage = new Storage('/tmp/project');
    expect(storage.getHistoryFilePath()).toContain(path.join(customDir, 'tmp'));
    expect(storage.getHistoryFilePath()).toMatch(/shell_history$/);
  });
});

describe('Storage – config paths remain at ~/.canopy regardless of runtime dir', () => {
  const originalEnv = process.env['CANOPY_RUNTIME_DIR'];
  const globalCanopyDir = Storage.getGlobalCanopyDir();

  beforeEach(() => {
    Storage.setRuntimeBaseDir(path.resolve('custom-runtime'));
    process.env['CANOPY_RUNTIME_DIR'] = path.resolve('env-runtime');
  });

  afterEach(() => {
    Storage.setRuntimeBaseDir(null);
    if (originalEnv !== undefined) {
      process.env['CANOPY_RUNTIME_DIR'] = originalEnv;
    } else {
      delete process.env['CANOPY_RUNTIME_DIR'];
    }
  });

  it('getGlobalSettingsPath still uses ~/.canopy', () => {
    expect(Storage.getGlobalSettingsPath()).toBe(
      path.join(globalCanopyDir, 'settings.json'),
    );
  });

  it('getInstallationIdPath still uses ~/.canopy', () => {
    expect(Storage.getInstallationIdPath()).toBe(
      path.join(globalCanopyDir, 'installation_id'),
    );
  });

  it('getGoogleAccountsPath still uses ~/.canopy', () => {
    expect(Storage.getGoogleAccountsPath()).toBe(
      path.join(globalCanopyDir, 'google_accounts.json'),
    );
  });

  it('getMcpOAuthTokensPath still uses ~/.canopy', () => {
    expect(Storage.getMcpOAuthTokensPath()).toBe(
      path.join(globalCanopyDir, 'mcp-oauth-tokens.json'),
    );
  });

  it('getOAuthCredsPath still uses ~/.canopy', () => {
    expect(Storage.getOAuthCredsPath()).toBe(
      path.join(globalCanopyDir, 'oauth_creds.json'),
    );
  });

  it('getUserCommandsDir still uses ~/.canopy', () => {
    expect(Storage.getUserCommandsDir()).toBe(
      path.join(globalCanopyDir, 'commands'),
    );
  });

  it('getGlobalMemoryFilePath still uses ~/.canopy', () => {
    expect(Storage.getGlobalMemoryFilePath()).toBe(
      path.join(globalCanopyDir, 'memory.md'),
    );
  });

  it('getGlobalBinDir still uses ~/.canopy', () => {
    expect(Storage.getGlobalBinDir()).toBe(path.join(globalCanopyDir, 'bin'));
  });

  it('getUserSkillsDirs still includes ~/.canopy/skills', () => {
    const storage = new Storage('/tmp/project');
    const skillsDirs = storage.getUserSkillsDirs();
    expect(
      skillsDirs.some((dir) => dir === path.join(globalCanopyDir, 'skills')),
    ).toBe(true);
  });
});

describe('Storage – QWEN_HOME env var', () => {
  const originalEnv = process.env['QWEN_HOME'];

  afterEach(() => {
    if (originalEnv !== undefined) {
      process.env['QWEN_HOME'] = originalEnv;
    } else {
      delete process.env['QWEN_HOME'];
    }
  });

  it('defaults to ~/.canopy when QWEN_HOME is not set', () => {
    delete process.env['QWEN_HOME'];
    const expected = path.join(os.homedir(), '.canopy');
    expect(Storage.getGlobalCanopyDir()).toBe(expected);
  });

  it('uses QWEN_HOME when set to absolute path', () => {
    const configDir = path.resolve('/tmp/custom-canopy');
    process.env['QWEN_HOME'] = configDir;
    expect(Storage.getGlobalCanopyDir()).toBe(configDir);
  });

  it('resolves relative QWEN_HOME to absolute path', () => {
    process.env['QWEN_HOME'] = 'relative/config';
    const expected = path.resolve('relative/config');
    expect(Storage.getGlobalCanopyDir()).toBe(expected);
  });

  it('config paths follow QWEN_HOME', () => {
    const configDir = path.resolve('/tmp/custom-canopy');
    process.env['QWEN_HOME'] = configDir;
    expect(Storage.getGlobalSettingsPath()).toBe(
      path.join(configDir, 'settings.json'),
    );
    expect(Storage.getInstallationIdPath()).toBe(
      path.join(configDir, 'installation_id'),
    );
    expect(Storage.getUserCommandsDir()).toBe(path.join(configDir, 'commands'));
    expect(Storage.getMcpOAuthTokensPath()).toBe(
      path.join(configDir, 'mcp-oauth-tokens.json'),
    );
    expect(Storage.getOAuthCredsPath()).toBe(
      path.join(configDir, 'oauth_creds.json'),
    );
    expect(Storage.getGlobalBinDir()).toBe(path.join(configDir, 'bin'));
    expect(Storage.getGlobalMemoryFilePath()).toBe(
      path.join(configDir, 'memory.md'),
    );
  });

  it('project-level paths are NOT affected by QWEN_HOME', () => {
    const configDir = path.resolve('/tmp/custom-canopy');
    const projectDir = path.resolve('/tmp/project');
    process.env['QWEN_HOME'] = configDir;
    const storage = new Storage(projectDir);
    expect(storage.getWorkspaceSettingsPath()).toBe(
      path.join(projectDir, '.canopy', 'settings.json'),
    );
    expect(storage.getProjectCommandsDir()).toBe(
      path.join(projectDir, '.canopy', 'commands'),
    );
  });

  it('expands tilde (~) in QWEN_HOME', () => {
    process.env['QWEN_HOME'] = '~/custom-canopy';
    const expected = path.join(os.homedir(), 'custom-canopy');
    expect(Storage.getGlobalCanopyDir()).toBe(expected);
  });

  it('expands Windows-style tilde in QWEN_HOME', () => {
    process.env['QWEN_HOME'] = '~\\custom-canopy';
    const expected = path.join(os.homedir(), 'custom-canopy');
    expect(Storage.getGlobalCanopyDir()).toBe(expected);
  });

  it('handles bare tilde (~) as home directory in QWEN_HOME', () => {
    process.env['QWEN_HOME'] = '~';
    expect(Storage.getGlobalCanopyDir()).toBe(path.normalize(os.homedir()));
  });

  it('QWEN_HOME and CANOPY_RUNTIME_DIR are independent', () => {
    const configDir = path.resolve('/tmp/config');
    const runtimeDir = path.resolve('/tmp/runtime');
    process.env['QWEN_HOME'] = configDir;
    process.env['CANOPY_RUNTIME_DIR'] = runtimeDir;
    expect(Storage.getGlobalCanopyDir()).toBe(configDir);
    expect(Storage.getRuntimeBaseDir()).toBe(runtimeDir);
    expect(Storage.getGlobalSettingsPath()).toBe(
      path.join(configDir, 'settings.json'),
    );
    expect(Storage.getGlobalTempDir()).toBe(path.join(runtimeDir, 'tmp'));
    expect(Storage.getGlobalDebugDir()).toBe(path.join(runtimeDir, 'debug'));
    delete process.env['CANOPY_RUNTIME_DIR'];
  });
});

describe('Storage – runtime base dir async context isolation', () => {
  const originalEnv = process.env['CANOPY_RUNTIME_DIR'];

  beforeEach(() => {
    Storage.setRuntimeBaseDir(null);
    delete process.env['CANOPY_RUNTIME_DIR'];
  });

  afterEach(() => {
    Storage.setRuntimeBaseDir(null);
    if (originalEnv !== undefined) {
      process.env['CANOPY_RUNTIME_DIR'] = originalEnv;
    } else {
      delete process.env['CANOPY_RUNTIME_DIR'];
    }
  });

  it('uses contextual runtime dir inside runWithRuntimeBaseDir', async () => {
    Storage.setRuntimeBaseDir(path.resolve('global-runtime'));
    const cwd = path.resolve('workspace', 'project-a');

    await Storage.runWithRuntimeBaseDir('.canopy', cwd, async () => {
      expect(Storage.getRuntimeBaseDir()).toBe(path.join(cwd, '.canopy'));
    });
  });

  it('keeps concurrent contexts isolated', async () => {
    const cwdA = path.resolve('workspace', 'a');
    const cwdB = path.resolve('workspace', 'b');

    const runA = Storage.runWithRuntimeBaseDir('.canopy-a', cwdA, async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      return Storage.getRuntimeBaseDir();
    });

    const runB = Storage.runWithRuntimeBaseDir('.canopy-b', cwdB, async () => {
      await new Promise((resolve) => setTimeout(resolve, 1));
      return Storage.getRuntimeBaseDir();
    });

    const [a, b] = await Promise.all([runA, runB]);
    expect(a).toBe(path.join(cwdA, '.canopy-a'));
    expect(b).toBe(path.join(cwdB, '.canopy-b'));
  });

  it('lets a resolved runtime pin override later process env changes', async () => {
    const pinned = path.resolve('workspace', 'pinned-runtime');
    process.env['CANOPY_RUNTIME_DIR'] = path.resolve(
      'workspace',
      'ambient-runtime',
    );

    await Storage.runWithResolvedRuntimeBaseDir(pinned, async () => {
      expect(Storage.getRuntimeBaseDir()).toBe(pinned);
      await Promise.resolve();
      expect(new Storage('/workspace').getRuntimeBaseDir()).toBe(pinned);
    });
  });

  it('keeps a resolved runtime pin across nested configurable contexts', () => {
    const pinned = path.resolve('workspace', 'pinned-runtime');

    Storage.runWithResolvedRuntimeBaseDir(pinned, () => {
      Storage.runWithRuntimeBaseDir(
        path.resolve('workspace', 'nested-runtime'),
        undefined,
        () => {
          expect(Storage.getRuntimeBaseDir()).toBe(pinned);
          expect(new Storage('/workspace').getRuntimeBaseDir()).toBe(pinned);
        },
      );
    });
  });

  it('pins an instance to the runtime dir where it was created', () => {
    const cwd = path.resolve('workspace', 'pinned');
    const runtimeDir = path.join(cwd, '.canopy-a');
    const storage = Storage.runWithRuntimeBaseDir(
      '.canopy-a',
      cwd,
      () => new Storage(cwd),
    );

    Storage.runWithRuntimeBaseDir('.canopy-b', cwd, () => {
      expect(storage.getRuntimeBaseDir()).toBe(runtimeDir);
      expect(storage.getProjectDir()).toContain(
        path.join(runtimeDir, 'projects'),
      );
      expect(storage.getProjectTempDir()).toContain(
        path.join(runtimeDir, 'tmp'),
      );
    });
  });
});
