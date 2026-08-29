/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { execSync } from 'node:child_process';
import { chmodSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

if (!process.cwd().includes('packages')) {
  console.error('must be invoked from a package directory');
  process.exit(1);
}

// Clean this package's stale outputs first to avoid TS5055 when tsbuildinfo
// is out of sync with sources (e.g. after a version bump or branch switch)
// under composite project references. We delete files directly rather than
// using `tsc --build --clean`, because the latter walks project references
// and would wipe upstream packages already built by scripts/build.js.
rmSync('dist', { recursive: true, force: true });
rmSync('tsconfig.tsbuildinfo', { force: true });

// build typescript files
execSync('tsc --build', { stdio: 'inherit' });

// copy .{md,json} files
execSync('node ../../scripts/copy_files.js', { stdio: 'inherit' });

// The CLI package exposes dist/index.js as its executable bin entry. TypeScript
// emits it using the process umask (commonly 0644), which makes a development
// install that symlinks directly into this workspace fail with EACCES. Keep
// the generated entrypoint executable after every package rebuild.
if (process.env.npm_package_name === '@canopy-code/canopy-code') {
  chmodSync(join(process.cwd(), 'dist', 'index.js'), 0o755);
}

// touch dist/.last_build
writeFileSync(join(process.cwd(), 'dist', '.last_build'), '');
process.exit(0);
