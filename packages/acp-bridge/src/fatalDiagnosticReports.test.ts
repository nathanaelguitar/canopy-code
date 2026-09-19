/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, it, vi } from 'vitest';
import type { mkdirSync } from 'node:fs';
import {
  configureFatalDiagnosticReports,
  getFatalDiagnosticReportExecArgs,
  resolveFatalDiagnosticReportDirectory,
} from './fatalDiagnosticReports.js';

describe('fatal diagnostic reports', () => {
  it('uses a private per-user default directory', () => {
    expect(
      resolveFatalDiagnosticReportDirectory({
        env: {},
        homeDirectory: '/tmp/canopy-home',
      }),
    ).toBe('/tmp/canopy-home/.canopy/fatal-reports');
  });

  it('supports an isolated report directory override', () => {
    expect(
      getFatalDiagnosticReportExecArgs({
        env: { CANOPY_FATAL_REPORT_DIR: '/tmp/canopy-crashes' },
        homeDirectory: '/tmp/unused',
      }),
    ).toEqual([
      '--report-on-fatalerror',
      '--report-exclude-env',
      '--report-exclude-network',
      '--report-directory=/tmp/canopy-crashes',
    ]);
  });

  it('configures the current process without making startup depend on reports', () => {
    const report = {
      directory: '',
      reportOnFatalError: false,
    };
    const mkdir = vi.fn() as unknown as typeof mkdirSync;

    const directory = configureFatalDiagnosticReports({
      env: {},
      homeDirectory: '/tmp/canopy-home',
      report,
      mkdir,
    });

    expect(directory).toBe('/tmp/canopy-home/.canopy/fatal-reports');
    expect(report).toEqual({
      directory: '/tmp/canopy-home/.canopy/fatal-reports',
      reportOnFatalError: true,
    });
    expect(mkdir).toHaveBeenCalledWith(
      '/tmp/canopy-home/.canopy/fatal-reports',
      { recursive: true, mode: 0o700 },
    );
  });

  it('can be disabled for constrained or privacy-sensitive environments', () => {
    const report = {
      directory: '',
      reportOnFatalError: false,
    };
    const mkdir = vi.fn() as unknown as typeof mkdirSync;

    expect(
      configureFatalDiagnosticReports({
        env: { CANOPY_DISABLE_FATAL_REPORTS: '1' },
        report,
        mkdir,
      }),
    ).toBeUndefined();
    expect(report).toEqual({ directory: '', reportOnFatalError: false });
    expect(mkdir).not.toHaveBeenCalled();
    expect(
      getFatalDiagnosticReportExecArgs({
        env: { CANOPY_DISABLE_FATAL_REPORTS: '1' },
      }),
    ).toEqual([]);
  });
});
