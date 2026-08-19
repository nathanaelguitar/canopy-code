/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import ignore from 'ignore';
import { createDebugLogger } from './debugLogger.js';
import { isPathWithinRoot } from './workspaceContext.js';

const CANOPY_IGNORE_FILE_NAME = '.canopyignore';
const debugLogger = createDebugLogger('CANOPY_IGNORE');

export const DEFAULT_CANOPY_CUSTOM_IGNORE_FILE_NAMES = [
  '.agentignore',
  '.aiignore',
] as const;

export function normalizeCanopyCustomIgnoreFileNames(
  ignoreFileNames: readonly string[] = DEFAULT_CANOPY_CUSTOM_IGNORE_FILE_NAMES,
): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();

  for (const ignoreFileName of ignoreFileNames) {
    const candidate = ignoreFileName.trim().replace(/\\/g, '/');
    const skipReason = getCustomIgnoreFileNameSkipReason(candidate);
    if (skipReason) {
      debugLogger.debug(
        `Skipping customIgnoreFiles entry "${ignoreFileName}": ${skipReason}`,
      );
      continue;
    }
    if (seen.has(candidate)) {
      debugLogger.debug(
        `Skipping customIgnoreFiles entry "${ignoreFileName}": duplicate`,
      );
      continue;
    }
    normalized.push(candidate);
    seen.add(candidate);
  }

  return normalized;
}

function getCustomIgnoreFileNameSkipReason(candidate: string): string | null {
  if (candidate === '') {
    return 'empty path';
  }
  if (path.isAbsolute(candidate) || candidate.startsWith('/')) {
    return 'absolute paths are not allowed';
  }
  if (candidate.includes('\0')) {
    return 'null bytes are not allowed';
  }
  if (candidate === CANOPY_IGNORE_FILE_NAME) {
    return '.canopyignore is always included';
  }
  if (candidate.split('/').includes('..')) {
    return 'parent directory segments are not allowed';
  }
  return null;
}

export function getCanopyIgnoreFileNames(
  customIgnoreFileNames?: readonly string[],
): string[] {
  return [
    CANOPY_IGNORE_FILE_NAME,
    ...normalizeCanopyCustomIgnoreFileNames(customIgnoreFileNames),
  ];
}

export function formatCanopyIgnoreFileNames(
  customIgnoreFileNames?: readonly string[],
): string {
  return getCanopyIgnoreFileNames(customIgnoreFileNames).join(', ');
}

export interface CanopyIgnoreFilter {
  isIgnored(filePath: string): boolean;
  getIgnoreFileNameForPath(filePath: string): string | undefined;
  getPatterns(): string[];
}

export class CanopyIgnoreParser implements CanopyIgnoreFilter {
  private projectRoot: string;
  private patterns: string[] = [];
  private readonly ignoreFileNames: string[];
  private readonly sourceIgnorers: Array<{
    ignoreFileName: string;
    ignorer: ReturnType<typeof ignore>;
  }> = [];

  constructor(projectRoot: string, customIgnoreFileNames?: readonly string[]) {
    this.projectRoot = path.resolve(projectRoot);
    this.ignoreFileNames = getCanopyIgnoreFileNames(customIgnoreFileNames);
    this.loadPatterns();
  }

  private loadPatterns(): void {
    for (const ignoreFileName of this.ignoreFileNames) {
      const patternsFilePath = path.join(this.projectRoot, ignoreFileName);
      let content: string;
      try {
        content = fs.readFileSync(patternsFilePath, 'utf-8');
      } catch (_error) {
        const error = _error as NodeJS.ErrnoException;
        if (error.code !== 'ENOENT') {
          debugLogger.debug(
            `Failed to read ${patternsFilePath}: ${error.message}`,
          );
        }
        continue;
      }

      // These files use gitignore syntax, so they follow gitignore whitespace
      // rules: only a trailing CR is stripped here, leading whitespace is part
      // of the pattern, and unescaped trailing whitespace is dropped by the
      // `ignore` library itself. See the same fix in `gitIgnoreParser.ts` for
      // why `trim()` inverted the match.
      const patterns = (content ?? '')
        .split('\n')
        .map((p) => (p.endsWith('\r') ? p.slice(0, -1) : p))
        .filter((p) => p.trim() !== '' && !p.startsWith('#'));
      if (patterns.length > 0) {
        const sourceIgnorer = ignore();
        sourceIgnorer.add(patterns);
        this.sourceIgnorers.push({
          ignoreFileName,
          ignorer: sourceIgnorer,
        });
      }
      this.patterns.push(...patterns);
    }
  }

  isIgnored(filePath: string): boolean {
    if (this.patterns.length === 0) {
      return false;
    }

    const normalizedPath = this.normalizePathForIgnore(filePath);
    if (!normalizedPath) {
      return false;
    }

    return this.sourceIgnorers.some(({ ignorer }) =>
      ignorer.ignores(normalizedPath),
    );
  }

  getIgnoreFileNameForPath(filePath: string): string | undefined {
    const normalizedPath = this.normalizePathForIgnore(filePath);
    if (!normalizedPath) {
      return undefined;
    }

    return this.sourceIgnorers.find(({ ignorer }) =>
      ignorer.ignores(normalizedPath),
    )?.ignoreFileName;
  }

  private normalizePathForIgnore(filePath: string): string | null {
    if (!filePath || typeof filePath !== 'string') {
      return null;
    }

    if (
      filePath.startsWith('\\') ||
      filePath === '/' ||
      filePath.includes('\0')
    ) {
      return null;
    }

    const isDir = filePath.endsWith('/');
    const resolved = path.resolve(this.projectRoot, filePath);
    const relativePath = path.relative(this.projectRoot, resolved);

    if (relativePath === '' || !isPathWithinRoot(resolved, this.projectRoot)) {
      return null;
    }

    // Even in windows, Ignore expects forward slashes.
    let normalizedPath = relativePath.replace(/\\/g, '/');
    // Preserve trailing '/' so directory-only patterns (e.g. `node_modules/`)
    // are matched correctly by the ignore library.
    if (isDir && !normalizedPath.endsWith('/')) {
      normalizedPath += '/';
    }

    if (normalizedPath.startsWith('/') || normalizedPath === '') {
      return null;
    }

    return normalizedPath;
  }

  getPatterns(): string[] {
    return this.patterns;
  }

  getIgnoreFileNames(): string[] {
    return this.ignoreFileNames;
  }
}
