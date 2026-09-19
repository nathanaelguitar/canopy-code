/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { chmodSync, mkdirSync } from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

const FATAL_REPORT_DIR_ENV = 'CANOPY_FATAL_REPORT_DIR';
const DISABLE_FATAL_REPORTS_ENV = 'CANOPY_DISABLE_FATAL_REPORTS';

/** The small subset of `process.report` used by this module. */
export interface FatalDiagnosticReportTarget {
  directory: string;
  reportOnFatalError: boolean;
}

export interface FatalDiagnosticReportOptions {
  env?: NodeJS.ProcessEnv;
  homeDirectory?: string;
  report?: FatalDiagnosticReportTarget;
  mkdir?: typeof mkdirSync;
}

function isDisabled(env: NodeJS.ProcessEnv): boolean {
  return env[DISABLE_FATAL_REPORTS_ENV] === '1';
}

/**
 * Resolve the private directory used for fatal Node diagnostic reports.
 *
 * A central per-user directory makes it possible to find reports from both
 * the daemon and its ACP children. The override is useful for a disposable
 * reproduction or a CI artifact directory.
 */
export function resolveFatalDiagnosticReportDirectory(
  options: Pick<FatalDiagnosticReportOptions, 'env' | 'homeDirectory'> = {},
): string {
  const env = options.env ?? process.env;
  const configured = env[FATAL_REPORT_DIR_ENV];
  const directory =
    configured && configured.trim().length > 0
      ? configured
      : path.join(
          options.homeDirectory ?? os.homedir(),
          '.canopy',
          'fatal-reports',
        );
  return path.resolve(directory);
}

/**
 * Return Node CLI flags for child processes.
 *
 * `process.report` exposes `reportOnFatalError` and `directory` at runtime,
 * but the privacy switches are startup-only Node flags. The ACP child gets
 * these flags before its entrypoint is evaluated, so an OOM in the actual
 * session worker produces the same private report shape as the daemon.
 */
export function getFatalDiagnosticReportExecArgs(
  options: Pick<FatalDiagnosticReportOptions, 'env' | 'homeDirectory'> = {},
): string[] {
  const env = options.env ?? process.env;
  if (isDisabled(env)) return [];
  const directory = resolveFatalDiagnosticReportDirectory(options);
  return [
    '--report-on-fatalerror',
    '--report-exclude-env',
    '--report-exclude-network',
    `--report-directory=${directory}`,
  ];
}

/**
 * Enable fatal reports in the current Node process.
 *
 * This is deliberately best-effort: inability to create the diagnostic
 * directory must not prevent Canopy from starting. The returned path is
 * useful to callers that want to surface where reports will land.
 */
export function configureFatalDiagnosticReports(
  options: FatalDiagnosticReportOptions = {},
): string | undefined {
  const env = options.env ?? process.env;
  if (isDisabled(env)) return undefined;

  const directory = resolveFatalDiagnosticReportDirectory(options);
  const mkdir = options.mkdir ?? mkdirSync;
  try {
    mkdir(directory, { recursive: true, mode: 0o700 });
    try {
      chmodSync(directory, 0o700);
    } catch {
      // A read-only or ACL-managed directory can still be usable for reports;
      // do not make diagnostics a startup dependency.
    }
    const report = options.report ?? process.report;
    if (!report) return undefined;
    report.directory = directory;
    report.reportOnFatalError = true;
    return directory;
  } catch {
    return undefined;
  }
}
