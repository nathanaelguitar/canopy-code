/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, it } from 'vitest';
import { pairingUrlForSession } from './remote-control-url.js';

describe('pairingUrlForSession', () => {
  it('selects the URL for the requested session from a reused daemon log', () => {
    const log = [
      'canopy serve: Local Control pairing URL: http://100.64.0.1:5173/session/old-session#token=old',
      'canopy serve: Local Control pairing URL: http://100.64.0.1:5173/session/new-session#token=new',
    ].join('\n');

    expect(pairingUrlForSession(log, 'new-session')).toBe(
      'http://100.64.0.1:5173/session/new-session#token=new',
    );
  });

  it('does not mistake a newer URL for another session as the requested URL', () => {
    const log = [
      'canopy serve: Local Control pairing URL: http://100.64.0.1:5173/session/target#token=target',
      'canopy serve: Local Control pairing URL: http://100.64.0.1:5173/session/other#token=other',
    ].join('\n');

    expect(pairingUrlForSession(log, 'target')).toBe(
      'http://100.64.0.1:5173/session/target#token=target',
    );
  });

  it('returns the newest matching URL when the session was re-enabled', () => {
    const log = [
      'canopy serve: Local Control pairing URL: http://100.64.0.1:5173/session/same#token=old',
      'canopy serve: Local Control pairing URL: http://100.64.0.1:5173/session/same#token=new',
    ].join('\n');

    expect(pairingUrlForSession(log, 'same')).toBe(
      'http://100.64.0.1:5173/session/same#token=new',
    );
  });

  it('ignores malformed lines and returns undefined when there is no match', () => {
    const log = [
      'canopy serve: Local Control pairing URL: not a url',
      'canopy serve: Local Control pairing URL: http://100.64.0.1:5173/session/other#token=other',
    ].join('\n');

    expect(pairingUrlForSession(log, 'missing')).toBeUndefined();
  });
});
