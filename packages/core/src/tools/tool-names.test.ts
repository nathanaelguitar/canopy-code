/**
 * @license
 * Copyright 2026 Qwen Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, it } from 'vitest';
import { canonicalToolName } from './tool-names.js';

describe('canonicalToolName', () => {
  it('resolves declared legacy aliases', () => {
    expect(canonicalToolName('replace')).toBe('edit');
    expect(canonicalToolName('search_file_content')).toBe('grep_search');
    expect(canonicalToolName('task')).toBe('agent');
  });

  it('does not resolve inherited object properties as aliases', () => {
    for (const name of [
      'constructor',
      'toString',
      'hasOwnProperty',
      '__proto__',
    ]) {
      expect(canonicalToolName(name)).toBe(name);
    }
  });
});
