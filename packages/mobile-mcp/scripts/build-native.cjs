'use strict';

const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const targets = {
  'darwin-arm64': 'aarch64-apple-darwin',
  'darwin-x64': 'x86_64-apple-darwin',
  'linux-arm64': 'aarch64-unknown-linux-gnu',
  'linux-x64': 'x86_64-unknown-linux-gnu',
  'win32-arm64': 'aarch64-pc-windows-msvc',
  'win32-x64': 'x86_64-pc-windows-msvc',
};

const packageRoot = path.resolve(__dirname, '..');
const workspaceRoot = path.resolve(packageRoot, '..', '..');
const manifest = path.join(workspaceRoot, 'rust', 'Cargo.toml');
const cargo = process.env.CARGO || 'cargo';
const requestedTargets = process.env.MOBILE_MCP_TARGETS
  ? process.env.MOBILE_MCP_TARGETS.split(',').map((target) => target.trim())
  : [`${process.platform}-${process.arch}`];

if (!fs.existsSync(manifest)) {
  console.error(
    '[mobile-mcp] Native builds require the qwen-code source checkout with rust/Cargo.toml.',
  );
  process.exit(1);
}

if (!requestedTargets.length || requestedTargets.some((target) => !target)) {
  console.error(
    '[mobile-mcp] MOBILE_MCP_TARGETS must be a comma-separated target list.',
  );
  process.exit(1);
}

const targetDir = process.env.CARGO_TARGET_DIR
  ? path.resolve(workspaceRoot, process.env.CARGO_TARGET_DIR)
  : path.join(workspaceRoot, 'rust', 'target');

for (const target of requestedTargets) {
  const rustTarget = targets[target];
  if (!rustTarget) {
    console.error(
      `[mobile-mcp] Unsupported target '${target}'. Supported targets: ${Object.keys(targets).join(', ')}.`,
    );
    process.exit(1);
  }

  const binaryName = target.startsWith('win32-')
    ? 'mcp-server-mobile.exe'
    : 'mcp-server-mobile';

  console.error(`[mobile-mcp] Building ${target} (${rustTarget})`);
  const result = spawnSync(
    cargo,
    [
      'build',
      '--locked',
      '--release',
      '--manifest-path',
      manifest,
      '--target',
      rustTarget,
      '-p',
      'mobile-mcp',
      '--bin',
      'mcp-server-mobile',
    ],
    { cwd: workspaceRoot, stdio: 'inherit' },
  );

  if (result.error) {
    console.error(
      `[mobile-mcp] Could not start Cargo: ${result.error.message}`,
    );
    process.exit(1);
  }
  if (result.status !== 0) {
    process.exit(result.status || 1);
  }

  const builtBinary = path.join(targetDir, rustTarget, 'release', binaryName);
  if (!fs.existsSync(builtBinary)) {
    console.error(
      `[mobile-mcp] Cargo succeeded but did not produce ${builtBinary}.`,
    );
    process.exit(1);
  }

  const artifactDir = path.join(packageRoot, 'native', target);
  fs.mkdirSync(artifactDir, { recursive: true });
  const artifact = path.join(artifactDir, binaryName);
  fs.copyFileSync(builtBinary, artifact);
  if (process.platform !== 'win32') {
    fs.chmodSync(artifact, 0o755);
  }
  console.error(`[mobile-mcp] Wrote ${path.relative(packageRoot, artifact)}`);
}
