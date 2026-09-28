#!/usr/bin/env node
'use strict';

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const executableName =
  process.platform === 'win32' ? 'mcp-server-mobile.exe' : 'mcp-server-mobile';
const supportedTargets = {
  'darwin-arm64': 'aarch64-apple-darwin',
  'darwin-x64': 'x86_64-apple-darwin',
  'linux-arm64': 'aarch64-unknown-linux-gnu',
  'linux-x64': 'x86_64-unknown-linux-gnu',
  'win32-arm64': 'aarch64-pc-windows-msvc',
  'win32-x64': 'x86_64-pc-windows-msvc',
};

function getPackageRoot() {
  try {
    return fs.realpathSync(path.resolve(__dirname, '..'));
  } catch {
    return path.resolve(__dirname, '..');
  }
}

function finishWithChild(child, description) {
  const forwardedSignals = ['SIGINT', 'SIGTERM', 'SIGHUP'];
  const signalHandlers = new Map();

  for (const signal of forwardedSignals) {
    const handler = () => {
      if (child.exitCode === null && child.signalCode === null) {
        try {
          child.kill(signal);
        } catch {
          // The child may be exiting while the signal is forwarded.
        }
      }
    };
    signalHandlers.set(signal, handler);
    process.on(signal, handler);
  }

  child.once('error', (error) => {
    console.error(
      `[mcp-server-mobile] Could not start ${description}: ${error.message}`,
    );
    process.exitCode = 1;
  });
  child.once('close', (code, signal) => {
    for (const [forwardedSignal, handler] of signalHandlers) {
      process.removeListener(forwardedSignal, handler);
    }
    if (signal) {
      process.exitCode = 128 + (os.constants.signals[signal] || 1);
    } else {
      process.exitCode = code ?? 1;
    }
  });
}

const packageRoot = getPackageRoot();
const targetKey = `${process.platform}-${process.arch}`;
const bundledBinary = path.join(
  packageRoot,
  'native',
  targetKey,
  executableName,
);

if (fs.existsSync(bundledBinary)) {
  finishWithChild(
    spawn(bundledBinary, process.argv.slice(2), { stdio: 'inherit' }),
    bundledBinary,
  );
} else {
  const workspaceRoot = path.resolve(packageRoot, '..', '..');
  const cargoManifest = path.join(workspaceRoot, 'rust', 'Cargo.toml');
  const sourceCheckout = fs.existsSync(cargoManifest);

  if (sourceCheckout) {
    const cargo = process.env.CARGO || 'cargo';
    const cargoArgs = [
      'run',
      '--quiet',
      '--release',
      '--locked',
      '--manifest-path',
      cargoManifest,
      '-p',
      'mobile-mcp',
      '--bin',
      'mcp-server-mobile',
      '--',
      ...process.argv.slice(2),
    ];
    const child = spawn(cargo, cargoArgs, {
      cwd: workspaceRoot,
      stdio: 'inherit',
      env: process.env,
    });
    finishWithChild(child, 'Cargo source-checkout fallback');
  } else {
    const supported = Object.keys(supportedTargets).join(', ');
    if (supportedTargets[targetKey]) {
      console.error(
        `[mcp-server-mobile] No native executable is bundled for ${targetKey}. Available package targets: ${supported}. Install a release that includes this target, or run from a source checkout with Rust/Cargo installed.`,
      );
    } else {
      console.error(
        `[mcp-server-mobile] Unsupported host ${process.platform}/${process.arch}. Available package targets: ${supported}.`,
      );
    }
    process.exitCode = 1;
  }
}
