/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, it, vi } from 'vitest';
import type { Config } from '@canopy-code/canopy-code-core';
import { AuthType } from '@canopy-code/canopy-code-core';
import { getConfiguredAdvisorModels } from './AdvisorDialog.js';

describe('getConfiguredAdvisorModels', () => {
  it('exposes ChatGPT OAuth GPT models as qualified advisor selectors', () => {
    const config = {
      getAllConfiguredModels: vi.fn(() => [
        {
          authType: AuthType.CHATGPT_OAUTH,
          id: 'gpt-5.6-luna',
          label: 'GPT-5.6 Luna',
          description: 'ChatGPT OAuth model',
        },
      ]),
    } as unknown as Config;

    expect(getConfiguredAdvisorModels(config)).toEqual([
      {
        value: 'chatgpt-oauth:gpt-5.6-luna',
        label: '[chatgpt-oauth] GPT-5.6 Luna',
        description: 'ChatGPT OAuth model',
        key: 'chatgpt-oauth:gpt-5.6-luna',
      },
    ]);
  });
});
