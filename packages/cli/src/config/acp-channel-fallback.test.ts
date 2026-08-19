/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, it, expect } from 'vitest';
import {
  CANOPY_CODE_DESKTOP_ENV,
  CANOPY_CODE_SERVE_ENV,
  resolveAcpChannelFallback,
} from './acp-channel-fallback.js';

describe('resolveAcpChannelFallback', () => {
  it('falls back to ACP for a direct launch without daemon markers', () => {
    expect(resolveAcpChannelFallback({})).toBe('ACP');
  });

  it('reports daemon for daemon-spawned children', () => {
    expect(resolveAcpChannelFallback({ [CANOPY_CODE_SERVE_ENV]: '1' })).toBe(
      'daemon',
    );
  });

  it('reports desktop for the Tauri desktop shell', () => {
    expect(resolveAcpChannelFallback({ [CANOPY_CODE_DESKTOP_ENV]: '1' })).toBe(
      'desktop',
    );
    // Tauri sessions are daemon-spawned too; the launcher identity wins.
    expect(
      resolveAcpChannelFallback({
        [CANOPY_CODE_SERVE_ENV]: '1',
        [CANOPY_CODE_DESKTOP_ENV]: '1',
      }),
    ).toBe('desktop');
  });

  it.each(['', '0', 'false'])('ignores marker value %j', (value) => {
    expect(
      resolveAcpChannelFallback({
        [CANOPY_CODE_SERVE_ENV]: value,
        [CANOPY_CODE_DESKTOP_ENV]: value,
      }),
    ).toBe('ACP');
  });
});
