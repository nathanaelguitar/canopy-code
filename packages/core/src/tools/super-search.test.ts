/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { Config } from '../config/config.js';
import { partListUnionToString } from '../core/geminiRequest.js';
import { createMockWorkspaceContext } from '../test-utils/mockWorkspaceContext.js';
import {
  buildDefaultSuperSearchRoots,
  SuperSearchTool,
} from './super-search.js';

describe('SuperSearchTool', () => {
  let tempRoot: string;
  let tool: SuperSearchTool;

  beforeEach(async () => {
    tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), 'super-search-tool-'));
    const mockConfig = {
      getTargetDir: () => tempRoot,
      getWorkspaceContext: () => createMockWorkspaceContext(tempRoot),
    } as unknown as Config;
    tool = new SuperSearchTool(mockConfig);
  });

  afterEach(async () => {
    await fs.rm(tempRoot, { recursive: true, force: true });
  });

  it('prioritizes user-facing roots and keeps the home fallback shallow', () => {
    const roots = buildDefaultSuperSearchRoots('/Users/tester', [
      '/Users/tester/repos/example',
    ]);
    expect(roots.map((root) => root.label)).toEqual([
      'Downloads',
      'Claude downloads',
      'Desktop',
      'Documents',
      'Projects',
      'repos',
      'Workspace',
      'Home folder (shallow)',
    ]);
    expect(roots.at(-1)?.maxDepth).toBe(1);
    expect(roots.find((root) => root.label === 'Downloads')?.pruneJunk).toBe(
      true,
    );
  });

  it('ranks a resume filename and returns a bounded candidate list', async () => {
    const downloads = path.join(tempRoot, 'Downloads');
    await fs.mkdir(downloads, { recursive: true });
    const resume = path.join(downloads, 'Nathan_Gill_Resume.md');
    await fs.writeFile(resume, '# Nathan Gill\nSoftware engineer\n');
    await fs.writeFile(path.join(downloads, 'old-notes.txt'), 'unrelated');

    const result = await tool
      .build({ query: 'Nathan Gill resume', path: 'Downloads', max_results: 1 })
      .execute(new AbortController().signal);

    expect(result.error).toBeUndefined();
    expect(result.resultFilePaths).toEqual([resume]);
    expect(result.llmContent).toContain(resume);
    expect(result.llmContent).toContain('Use read_file on an exact path');
    expect(partListUnionToString(result.llmContent).length).toBeLessThanOrEqual(
      12_000,
    );
  });

  it('finds a file by a bounded content-header match when its name is generic', async () => {
    const downloads = path.join(tempRoot, 'Downloads');
    await fs.mkdir(downloads, { recursive: true });
    const notes = path.join(downloads, 'career-notes.txt');
    await fs.writeFile(
      notes,
      'Private reference: Nathan Gill resume and employment history.\n',
    );

    const result = await tool
      .build({
        query: 'Nathan Gill resume',
        path: 'Downloads',
        include_content: true,
      })
      .execute(new AbortController().signal);

    expect(result.resultFilePaths).toContain(notes);
    expect(result.llmContent).toContain('content match');
    expect(result.llmContent).toContain('Private reference');
  });
});
