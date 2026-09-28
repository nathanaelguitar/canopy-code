/**
 * @license
 * Copyright 2025 Qwen
 * SPDX-License-Identifier: Apache-2.0
 */

// Native capture is optional because the CLI retains its SoX/arecord voice
// recorder fallback. Prefer a shipped Rust prebuild; in a source checkout,
// attempt a local Rust build. Missing Rust or a failed build must not break
// installation for users who do not use native voice input.
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)));
const glibcVersionRuntime =
  process.platform === 'linux'
    ? process.report?.getReport?.().header?.glibcVersionRuntime
    : undefined;
const prebuildTarget =
  process.platform === 'linux' && !glibcVersionRuntime
    ? `${process.platform}-${process.arch}-musl`
    : `${process.platform}-${process.arch}`;
const rustPrebuild = join(
  packageRoot,
  'prebuilds',
  prebuildTarget,
  'audio_capture_rust.node',
);
const localRustWorkspace = resolve(packageRoot, '../../rust/Cargo.toml');

if (existsSync(rustPrebuild)) {
  process.exit(0);
}

if (existsSync(localRustWorkspace)) {
  const rustBuild = spawnSync(
    process.execPath,
    [join(packageRoot, 'scripts', 'build-rust-addon.js')],
    { stdio: 'inherit' },
  );
  if (rustBuild.status === 0) {
    process.exit(0);
  }
  if (rustBuild.error) {
    process.stderr.write(
      `[audio-capture] Could not start the Rust addon build: ${rustBuild.error.message}\n`,
    );
  }
}

process.stderr.write(
  '[audio-capture] Rust microphone backend unavailable; ' +
    'voice input will fall back to SoX/arecord.\n',
);

// Keep this package optional at install time. The CLI selects its command-line
// recorder if the N-API addon cannot be loaded when voice input is requested.
process.exit(0);
