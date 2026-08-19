/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import type { GitIgnoreFilter } from '../utils/gitIgnoreParser.js';
import type { CanopyIgnoreFilter } from '../utils/canopy-ignore-parser.js';
import { GitIgnoreParser } from '../utils/gitIgnoreParser.js';
import {
  formatCanopyIgnoreFileNames,
  CanopyIgnoreParser,
} from '../utils/canopy-ignore-parser.js';
import { isGitRepository } from '../utils/gitUtils.js';
import * as path from 'node:path';

export interface FilterFilesOptions {
  respectGitIgnore?: boolean;
  respectCanopyIgnore?: boolean;
}

export interface FilterReport {
  filteredPaths: string[];
  gitIgnoredCount: number;
  canopyIgnoredCount: number;
}

export class FileDiscoveryService {
  private gitIgnoreFilter: GitIgnoreFilter | null = null;
  private canopyIgnoreFilter: CanopyIgnoreFilter | null = null;
  private projectRoot: string;

  constructor(
    projectRoot: string,
    private readonly customIgnoreFiles?: string[],
  ) {
    this.projectRoot = path.resolve(projectRoot);
    if (isGitRepository(this.projectRoot)) {
      this.gitIgnoreFilter = new GitIgnoreParser(this.projectRoot);
    }
    this.canopyIgnoreFilter = new CanopyIgnoreParser(
      this.projectRoot,
      customIgnoreFiles,
    );
  }

  /**
   * Filters a list of file paths based on git and AI ignore rules.
   */
  filterFiles(
    filePaths: string[],
    options: FilterFilesOptions = {
      respectGitIgnore: true,
      respectCanopyIgnore: true,
    },
  ): string[] {
    return filePaths.filter((filePath) => {
      if (options.respectGitIgnore && this.shouldGitIgnoreFile(filePath)) {
        return false;
      }
      if (
        options.respectCanopyIgnore &&
        this.shouldCanopyIgnoreFile(filePath)
      ) {
        return false;
      }
      return true;
    });
  }

  /**
   * Filters a list of file paths based on git ignore rules and returns a report
   * with counts of ignored files.
   */
  filterFilesWithReport(
    filePaths: string[],
    opts: FilterFilesOptions = {
      respectGitIgnore: true,
      respectCanopyIgnore: true,
    },
  ): FilterReport {
    const filteredPaths: string[] = [];
    let gitIgnoredCount = 0;
    let canopyIgnoredCount = 0;

    for (const filePath of filePaths) {
      if (opts.respectGitIgnore && this.shouldGitIgnoreFile(filePath)) {
        gitIgnoredCount++;
        continue;
      }

      if (opts.respectCanopyIgnore && this.shouldCanopyIgnoreFile(filePath)) {
        canopyIgnoredCount++;
        continue;
      }

      filteredPaths.push(filePath);
    }

    return {
      filteredPaths,
      gitIgnoredCount,
      canopyIgnoredCount,
    };
  }

  /**
   * Checks if a single file should be git-ignored
   */
  shouldGitIgnoreFile(filePath: string): boolean {
    if (this.gitIgnoreFilter) {
      return this.gitIgnoreFilter.isIgnored(filePath);
    }
    return false;
  }

  /**
   * Checks if a single file should be ignored by Canopy/agent ignore files.
   */
  shouldCanopyIgnoreFile(filePath: string): boolean {
    if (this.canopyIgnoreFilter) {
      return this.canopyIgnoreFilter.isIgnored(filePath);
    }
    return false;
  }

  /**
   * Unified method to check if a file should be ignored based on filtering options.
   *
   * Convention: append a trailing `/` to `filePath` to signal that the path
   * refers to a directory. This allows directory-only ignore patterns (e.g.
   * `node_modules/`) to match correctly during traversal pruning. Both the
   * GitIgnoreParser and CanopyIgnoreParser preserve the trailing slash through
   * their internal path normalization.
   */
  shouldIgnoreFile(
    filePath: string,
    options: FilterFilesOptions = {},
  ): boolean {
    const {
      respectGitIgnore = true,
      respectCanopyIgnore: respectCanopyIgnore = true,
    } = options;

    if (respectGitIgnore && this.shouldGitIgnoreFile(filePath)) {
      return true;
    }
    if (respectCanopyIgnore && this.shouldCanopyIgnoreFile(filePath)) {
      return true;
    }
    return false;
  }

  /**
   * Returns loaded patterns from Canopy/agent ignore files.
   */
  getCanopyIgnorePatterns(): string[] {
    return this.canopyIgnoreFilter?.getPatterns() ?? [];
  }

  getCanopyIgnoreFileDisplayForPath(filePath: string): string {
    return (
      this.canopyIgnoreFilter?.getIgnoreFileNameForPath(filePath) ??
      this.getCanopyIgnoreFileNamesDisplay()
    );
  }

  getCanopyIgnoreFileNamesDisplay(): string {
    return formatCanopyIgnoreFileNames(this.customIgnoreFiles);
  }
}
