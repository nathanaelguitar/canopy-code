/**
 * @license
 * Copyright 2025 Qwen
 * SPDX-License-Identifier: Apache-2.0
 */

import { copyFileSync, existsSync, mkdirSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { dirname, join, resolve } from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const rustManifest = resolve(packageRoot, '../../rust/Cargo.toml');
if (!existsSync(rustManifest)) {
  process.stderr.write(
    '[audio-capture] Rust workspace is unavailable; cannot build the Rust addon.\n',
  );
  process.exit(2);
}

const cargoArgs = [
  'build',
  '--manifest-path',
  rustManifest,
  '-p',
  'canopy-audio-capture',
  '--features',
  'node-addon',
  '--release',
  '--locked',
];
const build = spawnSync('cargo', cargoArgs, {
  cwd: packageRoot,
  stdio: 'inherit',
});
if (build.error) {
  process.stderr.write(`[audio-capture] ${build.error.message}\n`);
}
if (build.status !== 0) {
  process.exit(build.status ?? 1);
}

const metadata = spawnSync(
  'cargo',
  [
    'metadata',
    '--manifest-path',
    rustManifest,
    '--no-deps',
    '--format-version',
    '1',
  ],
  { cwd: packageRoot, encoding: 'utf8' },
);
if (metadata.status !== 0) {
  process.stderr.write(metadata.stderr ?? 'cargo metadata failed\n');
  process.exit(metadata.status ?? 1);
}
const targetDirectory = JSON.parse(metadata.stdout).target_directory;
const targetPrefix = process.env.CARGO_BUILD_TARGET
  ? join(targetDirectory, process.env.CARGO_BUILD_TARGET)
  : targetDirectory;
const artifactDirectory = join(targetPrefix, 'release');
const artifactNames =
  process.platform === 'win32'
    ? ['canopy_audio_capture.dll']
    : process.platform === 'darwin'
      ? ['libcanopy_audio_capture.dylib']
      : ['libcanopy_audio_capture.so'];
const artifact = artifactNames
  .map((name) => join(artifactDirectory, name))
  .find(existsSync);
if (!artifact) {
  process.stderr.write(
    `[audio-capture] Cargo succeeded, but the addon was not found in ${artifactDirectory}.\n`,
  );
  process.exit(1);
}

const outputDirectory = join(packageRoot, 'build', 'Release');
mkdirSync(outputDirectory, { recursive: true });
copyFileSync(artifact, join(outputDirectory, 'audio_capture_rust.node'));
