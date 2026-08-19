/**
 * @license
 * Copyright 2025 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, it } from 'vitest';
import { AuthType } from '@canopy-code/canopy-code-core';
import {
  buildAuthMethods,
  pickAuthMethodsForAuthRequired,
} from './authMethods.js';

describe('ACP auth methods', () => {
  it('does not advertise discontinued Canopy OAuth', () => {
    const authMethods = buildAuthMethods();

    expect(authMethods.map((method) => method.id)).toEqual([
      AuthType.USE_OPENAI,
    ]);
  });

  it('falls back to working methods for a stored discontinued Canopy OAuth selection', () => {
    const authMethods = pickAuthMethodsForAuthRequired('canopy-oauth');

    expect(authMethods.map((method) => method.id)).toEqual([
      AuthType.USE_OPENAI,
    ]);
  });
});
