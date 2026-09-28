/**
 * @license
 * Copyright 2025 Qwen
 * SPDX-License-Identifier: Apache-2.0
 */

import { copyFileSync, existsSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const source = join(packageRoot, 'build', 'Release', 'audio_capture_rust.node');
if (!existsSync(source)) {
  process.stderr.write(
    '[audio-capture] build the Rust addon before packaging a prebuild.\n',
  );
  process.exit(1);
}

const libc =
  process.platform === 'linux' &&
  !process.report?.getReport().header.glibcVersionRuntime
    ? 'musl'
    : 'glibc';
const target = `${process.platform}-${process.arch}${process.platform === 'linux' && libc === 'musl' ? '-musl' : ''}`;
const outputDirectory = join(packageRoot, 'prebuilds', target);
mkdirSync(outputDirectory, { recursive: true });
copyFileSync(source, join(outputDirectory, 'audio_capture_rust.node'));
