/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { spawn } from 'node:child_process';
import {
  realpathSync,
  existsSync,
  openSync,
  closeSync,
  readFileSync,
  mkdirSync,
  mkdtempSync,
  renameSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { getGlobalCanopyDirLite } from '../config/storage-paths-lite.js';

/**
 * Default `canopy serve` bind port. The daemon auto-increments on
 * EADDRINUSE (observed live: 4170 -> 4171 -> 4172...), so a daemon for an
 * unrelated workspace already sitting on 4170 is a normal case, not an
 * error — {@link ensureWorkspaceDaemon} spawns a fresh one and lets it find
 * its own port rather than treating the probe miss as fatal.
 *
 * There is no daemon discovery registry in this codebase (the small
 * log-path registry below exists only to recover a spawned daemon's terminal
 * output, not to discover arbitrary ports). We checked
 * `daemon-logger.ts`'s stable-log lock — that's log-file contention, not a
 * "which port is workspace X's daemon on" index; and the IDE companion's
 * port lockfiles — unrelated MCP discovery for a different subsystem). So
 * reuse is best-effort: only the default port is probed. A daemon this
 * function itself spawned for a workspace, on a non-default port because
 * 4170 was taken by something else, will not be found by a later probe
 * from a second terminal tab in the same workspace and a second daemon
 * will be spawned instead. Documented as a known v1 limitation rather than
 * solved with a new registry — see docs/design/2026-08-26-remote-control.md.
 */
export const DEFAULT_DAEMON_PORT = 4170;

const HEALTH_PROBE_TIMEOUT_MS = 800;
const SPAWN_READY_TIMEOUT_MS = 10_000;
const DAEMON_LOG_REGISTRY_VERSION = 1;

interface DaemonLogRegistryEntry {
  port: number;
  pid: number;
  logPath: string;
}

interface DaemonLogRegistry {
  version: number;
  entries: Record<string, DaemonLogRegistryEntry>;
}

function canonicalWorkspace(workspaceCwd: string): string {
  try {
    return realpathSync(workspaceCwd);
  } catch {
    return workspaceCwd;
  }
}

function daemonLogRegistryPath(): string {
  return path.join(getGlobalCanopyDirLite(), 'daemon-spawn-logs.json');
}

function readDaemonLogRegistry(): DaemonLogRegistry {
  try {
    const parsed = JSON.parse(readFileSync(daemonLogRegistryPath(), 'utf-8'));
    if (
      parsed?.version === DAEMON_LOG_REGISTRY_VERSION &&
      parsed.entries &&
      typeof parsed.entries === 'object'
    ) {
      return parsed as DaemonLogRegistry;
    }
  } catch {
    // A missing or malformed registry only prevents recovering an optional
    // terminal-owned pairing URL; daemon reuse itself must still work.
  }
  return { version: DAEMON_LOG_REGISTRY_VERSION, entries: {} };
}

function rememberDaemonLogPath(
  workspaceCwd: string,
  port: number,
  pid: number | undefined,
  logPath: string,
): void {
  if (!pid) return;
  try {
    const registryPath = daemonLogRegistryPath();
    mkdirSync(path.dirname(registryPath), { recursive: true, mode: 0o700 });
    const registry = readDaemonLogRegistry();
    registry.entries[canonicalWorkspace(workspaceCwd)] = { port, pid, logPath };
    const stagingPath = `${registryPath}.${process.pid}.tmp`;
    writeFileSync(stagingPath, JSON.stringify(registry), { mode: 0o600 });
    renameSync(stagingPath, registryPath);
  } catch {
    // Pairing can still be recovered by the terminal that spawned the daemon.
  }
}

function rememberedDaemonLogPath(
  workspaceCwd: string,
  port: number,
): string | undefined {
  const entry =
    readDaemonLogRegistry().entries[canonicalWorkspace(workspaceCwd)];
  if (!entry || entry.port !== port || !existsSync(entry.logPath))
    return undefined;
  try {
    process.kill(entry.pid, 0);
  } catch {
    return undefined;
  }
  return entry.logPath;
}

export interface EnsureWorkspaceDaemonResult {
  baseUrl: string;
  port: number;
  /** True if this call spawned the daemon; false if an existing one was reused. */
  spawned: boolean;
  /**
   * Path to the daemon's stdout/stderr log, present when this call spawned
   * the daemon or when a live daemon's spawn record can be recovered. The
   * daemon's stdio stays bound to this
   * file for its entire lifetime (Node's spawn redirection isn't just for
   * startup), so it's where to find anything the daemon prints later that
   * an HTTP response won't carry for an unauthenticated caller — e.g. the
   * Local Control pairing URL, which `/workspace/local-control/enable`
   * deliberately withholds from unauthenticated response bodies and prints
   * here instead (see workspace-local-control.ts's `presentStatus`).
   */
  logPath?: string;
}

export class DaemonSpawnTimeoutError extends Error {
  constructor() {
    super(
      `canopy serve did not become ready within ${SPAWN_READY_TIMEOUT_MS}ms.`,
    );
    this.name = 'DaemonSpawnTimeoutError';
  }
}

async function probeCapabilities(
  port: number,
): Promise<{ workspaceCwd?: string } | undefined> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), HEALTH_PROBE_TIMEOUT_MS);
  try {
    const res = await fetch(`http://127.0.0.1:${port}/capabilities`, {
      signal: controller.signal,
    });
    if (!res.ok) return undefined;
    return (await res.json()) as { workspaceCwd?: string };
  } catch {
    return undefined;
  } finally {
    clearTimeout(timer);
  }
}

function sameWorkspace(a: string, b: string): boolean {
  try {
    return realpathSync(a) === realpathSync(b);
  } catch {
    return a === b;
  }
}

/**
 * Spawn `canopy serve` detached, bound to `workspaceCwd`, and resolve once
 * it reports itself ready.
 *
 * The child's stdout/stderr are redirected to a real log file, not a pipe.
 * A pipe requires something to keep draining it for the life of the child;
 * this function's caller (e.g. the interactive TUI at startup) exits as
 * soon as it resolves. Piping and then destroying the parent's read end
 * once done reading (the first version of this function) let the process
 * exit promptly, but closes the pipe out from under the still-running
 * daemon — its next log write hits a closed pipe and the daemon crashes
 * (confirmed live: the "reused" daemon from a prior call was gone by the
 * next probe). A file has no such reader-lifetime coupling.
 */
function spawnDaemon(
  workspaceCwd: string,
): Promise<EnsureWorkspaceDaemonResult> {
  return new Promise((resolve, reject) => {
    const logDir = mkdtempSync(path.join(tmpdir(), 'canopy-serve-spawn-'));
    const logPath = path.join(logDir, 'daemon.log');
    const logFd = openSync(logPath, 'a');

    const child = spawn(
      process.execPath,
      [
        process.argv[1] ?? '',
        'serve',
        '--port',
        String(DEFAULT_DAEMON_PORT),
        '--hostname',
        '127.0.0.1',
        '--workspace',
        workspaceCwd,
      ],
      {
        detached: true,
        stdio: ['ignore', logFd, logFd],
        env: process.env,
      },
    );
    closeSync(logFd); // The child holds its own duplicated fd; ours is done.
    child.unref();

    let settled = false;
    const timeout = setTimeout(() => {
      clearInterval(poll);
      if (settled) return;
      settled = true;
      reject(new DaemonSpawnTimeoutError());
    }, SPAWN_READY_TIMEOUT_MS);

    const poll = setInterval(() => {
      if (settled) return;
      let text: string;
      try {
        text = readFileSync(logPath, 'utf-8');
      } catch {
        return;
      }
      const match = text.match(
        /canopy serve listening on http:\/\/127\.0\.0\.1:(\d+)/,
      );
      if (!match) return;
      settled = true;
      clearTimeout(timeout);
      clearInterval(poll);
      rememberDaemonLogPath(workspaceCwd, Number(match[1]), child.pid, logPath);
      resolve({
        baseUrl: `http://127.0.0.1:${match[1]}`,
        port: Number(match[1]),
        spawned: true,
        logPath,
      });
    }, 100);

    child.on('error', (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      clearInterval(poll);
      reject(err);
    });
  });
}

/**
 * Find or start the `canopy serve` daemon for `workspaceCwd`. One daemon
 * per workspace is the intended steady state — see {@link DEFAULT_DAEMON_PORT}
 * for the reuse limitation.
 */
export async function ensureWorkspaceDaemon(
  workspaceCwd: string,
): Promise<EnsureWorkspaceDaemonResult> {
  const existing = await probeCapabilities(DEFAULT_DAEMON_PORT);
  if (
    existing?.workspaceCwd &&
    sameWorkspace(existing.workspaceCwd, workspaceCwd)
  ) {
    return {
      baseUrl: `http://127.0.0.1:${DEFAULT_DAEMON_PORT}`,
      port: DEFAULT_DAEMON_PORT,
      spawned: false,
      logPath: rememberedDaemonLogPath(workspaceCwd, DEFAULT_DAEMON_PORT),
    };
  }
  return spawnDaemon(workspaceCwd);
}
