/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, it } from 'vitest';
import { resolveRemoteControlComputerName } from './enable-remote-control.js';

describe('resolveRemoteControlComputerName', () => {
  it('uses an explicit configured name', () => {
    expect(
      resolveRemoteControlComputerName('  Studio Mac  ', 'spark-1b8a', 'linux'),
    ).toBe('Studio Mac');
  });

  it('limits explicit names to the remote-control payload limit', () => {
    expect(
      resolveRemoteControlComputerName('x'.repeat(100), 'spark-1b8a', 'linux'),
    ).toHaveLength(80);
  });

  it('labels DGX and Spark machines as Spark', () => {
    expect(
      resolveRemoteControlComputerName(undefined, 'spark-1b8a', 'linux'),
    ).toBe('Spark');
    expect(
      resolveRemoteControlComputerName(undefined, 'DGX-host', 'linux'),
    ).toBe('Spark');
  });

  it('labels Darwin hosts as Mac', () => {
    expect(
      resolveRemoteControlComputerName(
        undefined,
        'Nathanaels-MacBook-Pro',
        'darwin',
      ),
    ).toBe('Mac');
  });

  it('uses the hostname label for other machines', () => {
    expect(
      resolveRemoteControlComputerName(
        undefined,
        'build-host.internal',
        'linux',
      ),
    ).toBe('build-host');
    expect(resolveRemoteControlComputerName(undefined, '  ', 'linux')).toBe(
      'Computer',
    );
  });
});
