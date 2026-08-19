/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  formatCanopyIgnoreFileNames,
  getCanopyIgnoreFileNames,
  normalizeCanopyCustomIgnoreFileNames,
  CanopyIgnoreParser,
} from './canopy-ignore-parser.js';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import * as os from 'node:os';

describe('CanopyIgnoreParser', () => {
  let projectRoot: string;

  async function createTestFile(filePath: string, content = '') {
    const fullPath = path.join(projectRoot, filePath);
    await fs.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.writeFile(fullPath, content);
  }

  beforeEach(async () => {
    projectRoot = await fs.mkdtemp(
      path.join(os.tmpdir(), 'canopyignore-test-'),
    );
  });

  afterEach(async () => {
    await fs.rm(projectRoot, { recursive: true, force: true });
    vi.restoreAllMocks();
  });

  describe('when .canopyignore exists', () => {
    beforeEach(async () => {
      await createTestFile(
        '.canopyignore',
        'ignored.txt\n# A comment\n/ignored_dir/\n',
      );
      await createTestFile('ignored.txt', 'ignored');
      await createTestFile('not_ignored.txt', 'not ignored');
      await createTestFile(
        path.join('ignored_dir', 'file.txt'),
        'in ignored dir',
      );
      await createTestFile(
        path.join('subdir', 'not_ignored.txt'),
        'not ignored',
      );
    });

    it('should ignore files specified in .canopyignore', () => {
      const parser = new CanopyIgnoreParser(projectRoot);
      expect(parser.getPatterns()).toEqual(['ignored.txt', '/ignored_dir/']);
      expect(parser.isIgnored('ignored.txt')).toBe(true);
      expect(parser.getIgnoreFileNameForPath('ignored.txt')).toBe(
        '.canopyignore',
      );
      expect(parser.isIgnored('not_ignored.txt')).toBe(false);
      expect(parser.getIgnoreFileNameForPath('not_ignored.txt')).toBe(
        undefined,
      );
      expect(parser.isIgnored(path.join('ignored_dir', 'file.txt'))).toBe(true);
      expect(parser.isIgnored(path.join('subdir', 'not_ignored.txt'))).toBe(
        false,
      );
    });

    it('should still evaluate files whose names start with two dots', async () => {
      await createTestFile('.canopyignore', '..secret.log');

      const parser = new CanopyIgnoreParser(projectRoot);

      expect(parser.isIgnored('..secret.log')).toBe(true);
    });

    it('should not evaluate paths outside the project root', () => {
      const parser = new CanopyIgnoreParser(projectRoot);

      expect(parser.isIgnored(path.join('..', '..secret.log'))).toBe(false);
    });
  });

  describe('when compatibility agent ignore files exist', () => {
    beforeEach(async () => {
      await createTestFile('.agentignore', 'agent-secret.txt\n');
      await createTestFile('.aiignore', 'ai-secret.txt\n');
      await createTestFile('agent-secret.txt', 'agent secret');
      await createTestFile('ai-secret.txt', 'ai secret');
      await createTestFile('visible.txt', 'visible');
    });

    it('should ignore files specified in .agentignore and .aiignore', () => {
      const parser = new CanopyIgnoreParser(projectRoot);
      expect(parser.getPatterns()).toEqual([
        'agent-secret.txt',
        'ai-secret.txt',
      ]);
      expect(parser.isIgnored('agent-secret.txt')).toBe(true);
      expect(parser.getIgnoreFileNameForPath('agent-secret.txt')).toBe(
        '.agentignore',
      );
      expect(parser.isIgnored('ai-secret.txt')).toBe(true);
      expect(parser.getIgnoreFileNameForPath('ai-secret.txt')).toBe(
        '.aiignore',
      );
      expect(parser.isIgnored('visible.txt')).toBe(false);
    });
  });

  describe('when compatibility ignore files contain negations', () => {
    beforeEach(async () => {
      await createTestFile('.canopyignore', 'secrets/**\n');
      await createTestFile('.agentignore', '!secrets/**\n');
      await createTestFile(path.join('secrets', 'token.txt'), 'secret');
    });

    it('should not let custom ignore negations unignore .canopyignore matches', () => {
      const parser = new CanopyIgnoreParser(projectRoot);

      expect(parser.isIgnored(path.join('secrets', 'token.txt'))).toBe(true);
      expect(
        parser.getIgnoreFileNameForPath(path.join('secrets', 'token.txt')),
      ).toBe('.canopyignore');
    });
  });

  describe('when custom ignore files are configured', () => {
    beforeEach(async () => {
      await createTestFile('.cursorignore', 'cursor-secret.txt\n');
      await createTestFile('.agentignore', 'agent-secret.txt\n');
      await createTestFile('cursor-secret.txt', 'cursor secret');
      await createTestFile('agent-secret.txt', 'agent secret');
      await createTestFile('visible.txt', 'visible');
    });

    it('should use configured custom ignore files instead of defaults', () => {
      const parser = new CanopyIgnoreParser(projectRoot, ['.cursorignore']);

      expect(parser.getIgnoreFileNames()).toEqual([
        '.canopyignore',
        '.cursorignore',
      ]);
      expect(parser.getPatterns()).toEqual(['cursor-secret.txt']);
      expect(parser.isIgnored('cursor-secret.txt')).toBe(true);
      expect(parser.getIgnoreFileNameForPath('cursor-secret.txt')).toBe(
        '.cursorignore',
      );
      expect(parser.isIgnored('agent-secret.txt')).toBe(false);
      expect(parser.isIgnored('visible.txt')).toBe(false);
    });
  });

  describe('custom ignore file name normalization', () => {
    it('should keep safe relative ignore files and skip unsafe paths', () => {
      expect(
        normalizeCanopyCustomIgnoreFileNames([
          ' .cursorignore ',
          '.cursorignore',
          'nested\\.ignore',
          '.canopyignore',
          '',
          '/absolute',
          '../escape',
          'nested/../escape',
          'bad\0file',
        ]),
      ).toEqual(['.cursorignore', 'nested/.ignore']);
    });

    it('should include .canopyignore plus default custom ignore files by default', () => {
      expect(getCanopyIgnoreFileNames()).toEqual([
        '.canopyignore',
        '.agentignore',
        '.aiignore',
      ]);
    });

    it('should keep .canopyignore when custom ignore files are empty', () => {
      expect(getCanopyIgnoreFileNames([])).toEqual(['.canopyignore']);
    });

    it('should format ignore file names for user-facing messages', () => {
      expect(formatCanopyIgnoreFileNames(['.cursorignore'])).toBe(
        '.canopyignore, .cursorignore',
      );
    });
  });

  describe('when no supported ignore file exists', () => {
    it('should not load any patterns and not ignore any files', () => {
      const parser = new CanopyIgnoreParser(projectRoot);
      expect(parser.getPatterns()).toEqual([]);
      expect(parser.isIgnored('any_file.txt')).toBe(false);
    });
  });

  // These files use gitignore syntax, so they inherit gitignore whitespace
  // rules — see the matching suite in gitIgnoreParser.test.ts.
  describe('pattern whitespace', () => {
    it('keeps leading whitespace and strips only a trailing CR', async () => {
      await createTestFile(
        '.canopyignore',
        ' leading.txt\r\n  #hash.txt\r\n# real comment\r\n',
      );
      const parser = new CanopyIgnoreParser(projectRoot);

      // `getPatterns()` is public and feeds FileDiscoveryService, so the
      // stored text is asserted as well as the match result.
      expect(parser.getPatterns()).toEqual([' leading.txt', '  #hash.txt']);
      expect(parser.isIgnored(' leading.txt')).toBe(true);
      expect(parser.isIgnored('leading.txt')).toBe(false);
    });
  });
});
