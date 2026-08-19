/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { FileDiscoveryService } from './fileDiscoveryService.js';

describe('FileDiscoveryService', () => {
  let testRootDir: string;
  let projectRoot: string;

  async function createTestFile(filePath: string, content = '') {
    const fullPath = path.join(projectRoot, filePath);
    await fs.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.writeFile(fullPath, content);
    return fullPath;
  }

  beforeEach(async () => {
    testRootDir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'file-discovery-test-'),
    );
    projectRoot = path.join(testRootDir, 'project');
    await fs.mkdir(projectRoot, { recursive: true });
  });

  afterEach(async () => {
    await fs.rm(testRootDir, { recursive: true, force: true });
  });

  describe('initialization', () => {
    it('should initialize git ignore parser by default in a git repo', async () => {
      await fs.mkdir(path.join(projectRoot, '.git'));
      await createTestFile('.gitignore', 'node_modules/');

      const service = new FileDiscoveryService(projectRoot);
      // Let's check the effect of the parser instead of mocking it.
      expect(service.shouldGitIgnoreFile('node_modules/foo.js')).toBe(true);
      expect(service.shouldGitIgnoreFile('src/foo.js')).toBe(false);
    });

    it('should not load git repo patterns when not in a git repo', async () => {
      // No .git directory
      await createTestFile('.gitignore', 'node_modules/');
      const service = new FileDiscoveryService(projectRoot);

      // .gitignore is not loaded in non-git repos
      expect(service.shouldGitIgnoreFile('node_modules/foo.js')).toBe(false);
    });

    it('should load .canopyignore patterns even when not in a git repo', async () => {
      await createTestFile('.canopyignore', 'secrets.txt');
      const service = new FileDiscoveryService(projectRoot);

      expect(service.shouldCanopyIgnoreFile('secrets.txt')).toBe(true);
      expect(service.getCanopyIgnoreFileDisplayForPath('secrets.txt')).toBe(
        '.canopyignore',
      );
      expect(service.shouldCanopyIgnoreFile('src/index.js')).toBe(false);
    });

    it('should load .agentignore and .aiignore patterns', async () => {
      await createTestFile('.agentignore', 'agent-secret.txt');
      await createTestFile('.aiignore', 'ai-secret.txt');
      const service = new FileDiscoveryService(projectRoot);

      expect(service.shouldCanopyIgnoreFile('agent-secret.txt')).toBe(true);
      expect(
        service.getCanopyIgnoreFileDisplayForPath('agent-secret.txt'),
      ).toBe('.agentignore');
      expect(service.shouldCanopyIgnoreFile('ai-secret.txt')).toBe(true);
      expect(service.getCanopyIgnoreFileDisplayForPath('ai-secret.txt')).toBe(
        '.aiignore',
      );
      expect(service.shouldCanopyIgnoreFile('src/index.js')).toBe(false);
    });

    it('should load configured custom canopy ignore file patterns', async () => {
      await createTestFile('.cursorignore', 'cursor-secret.txt');
      await createTestFile('.agentignore', 'agent-secret.txt');
      const service = new FileDiscoveryService(projectRoot, ['.cursorignore']);

      expect(service.getCanopyIgnoreFileNamesDisplay()).toBe(
        '.canopyignore, .cursorignore',
      );
      expect(service.shouldCanopyIgnoreFile('cursor-secret.txt')).toBe(true);
      expect(
        service.getCanopyIgnoreFileDisplayForPath('cursor-secret.txt'),
      ).toBe('.cursorignore');
      expect(service.shouldCanopyIgnoreFile('agent-secret.txt')).toBe(false);
      expect(service.shouldCanopyIgnoreFile('src/index.js')).toBe(false);
    });
  });

  describe('filterFiles', () => {
    beforeEach(async () => {
      await fs.mkdir(path.join(projectRoot, '.git'));
      await createTestFile('.gitignore', 'node_modules/\n.git/\ndist');
      await createTestFile('.canopyignore', 'logs/');
    });

    it('should filter out git-ignored and canopy-ignored files by default', () => {
      const files = [
        'src/index.ts',
        'node_modules/package/index.js',
        'README.md',
        '.git/config',
        'dist/bundle.js',
        'logs/latest.log',
      ].map((f) => path.join(projectRoot, f));

      const service = new FileDiscoveryService(projectRoot);

      expect(service.filterFiles(files)).toEqual(
        ['src/index.ts', 'README.md'].map((f) => path.join(projectRoot, f)),
      );
    });

    it('should not filter files when respectGitIgnore is false', () => {
      const files = [
        'src/index.ts',
        'node_modules/package/index.js',
        '.git/config',
        'logs/latest.log',
      ].map((f) => path.join(projectRoot, f));

      const service = new FileDiscoveryService(projectRoot);

      const filtered = service.filterFiles(files, {
        respectGitIgnore: false,
        respectCanopyIgnore: true, // still respect this one
      });

      expect(filtered).toEqual(
        ['src/index.ts', 'node_modules/package/index.js', '.git/config'].map(
          (f) => path.join(projectRoot, f),
        ),
      );
    });

    it('should not filter files when respectCanopyIgnore is false', () => {
      const files = [
        'src/index.ts',
        'node_modules/package/index.js',
        'logs/latest.log',
      ].map((f) => path.join(projectRoot, f));

      const service = new FileDiscoveryService(projectRoot);

      const filtered = service.filterFiles(files, {
        respectGitIgnore: true,
        respectCanopyIgnore: false,
      });

      expect(filtered).toEqual(
        ['src/index.ts', 'logs/latest.log'].map((f) =>
          path.join(projectRoot, f),
        ),
      );
    });

    it('should handle empty file list', () => {
      const service = new FileDiscoveryService(projectRoot);

      expect(service.filterFiles([])).toEqual([]);
    });
  });

  describe('shouldGitIgnoreFile & shouldCanopyIgnoreFile', () => {
    beforeEach(async () => {
      await fs.mkdir(path.join(projectRoot, '.git'));
      await createTestFile('.gitignore', 'node_modules/');
      await createTestFile('.canopyignore', '*.log');
    });

    it('should return true for git-ignored files', () => {
      const service = new FileDiscoveryService(projectRoot);

      expect(
        service.shouldGitIgnoreFile(
          path.join(projectRoot, 'node_modules/package/index.js'),
        ),
      ).toBe(true);
    });

    it('should return false for non-git-ignored files', () => {
      const service = new FileDiscoveryService(projectRoot);

      expect(
        service.shouldGitIgnoreFile(path.join(projectRoot, 'src/index.ts')),
      ).toBe(false);
    });

    it('should return true for canopy-ignored files', () => {
      const service = new FileDiscoveryService(projectRoot);

      expect(
        service.shouldCanopyIgnoreFile(path.join(projectRoot, 'debug.log')),
      ).toBe(true);
    });

    it('should return false for non-canopy-ignored files', () => {
      const service = new FileDiscoveryService(projectRoot);

      expect(
        service.shouldCanopyIgnoreFile(path.join(projectRoot, 'src/index.ts')),
      ).toBe(false);
    });
  });

  describe('edge cases', () => {
    it('should handle relative project root paths', async () => {
      await fs.mkdir(path.join(projectRoot, '.git'));
      await createTestFile('.gitignore', 'ignored.txt');
      const service = new FileDiscoveryService(
        path.relative(process.cwd(), projectRoot),
      );

      expect(
        service.shouldGitIgnoreFile(path.join(projectRoot, 'ignored.txt')),
      ).toBe(true);
      expect(
        service.shouldGitIgnoreFile(path.join(projectRoot, 'not-ignored.txt')),
      ).toBe(false);
    });

    it('should handle filterFiles with undefined options', async () => {
      await fs.mkdir(path.join(projectRoot, '.git'));
      await createTestFile('.gitignore', 'ignored.txt');
      const service = new FileDiscoveryService(projectRoot);

      const files = ['src/index.ts', 'ignored.txt'].map((f) =>
        path.join(projectRoot, f),
      );

      expect(service.filterFiles(files, undefined)).toEqual([
        path.join(projectRoot, 'src/index.ts'),
      ]);
    });
  });
});
