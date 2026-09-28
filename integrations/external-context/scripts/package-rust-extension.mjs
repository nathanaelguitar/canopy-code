import { spawnSync } from 'node:child_process';
import {
  chmod,
  copyFile,
  mkdir,
  readFile,
  rename,
  rm,
  writeFile,
} from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const integrationRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '..',
);
const repositoryRoot = path.resolve(integrationRoot, '../..');
const rustRoot = path.join(integrationRoot, 'rust');
const targetDir = path.join(rustRoot, 'target');
const distDir = path.join(integrationRoot, 'dist');
const profiles = {
  externalContext: {
    binary: 'canopy-external-context',
    extension: 'external-context',
    manifest: 'qwen-extension.json',
    metadata: 'rust-extension-target.json',
    output: 'rust-extension',
    server: 'external-context',
    title: 'External Context Rust Extension',
  },
  providerContext: {
    binary: 'provider-context-local-example',
    extension: 'provider-context-local-example',
    manifest: 'examples/provider-extension-local/qwen-extension.json',
    metadata: 'rust-provider-context-target.json',
    output: 'rust-provider-context',
    server: 'provider-context-local-example',
    title: 'Provider Context Local Rust Extension',
  },
};

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: integrationRoot,
    encoding: 'utf8',
    stdio: 'inherit',
    windowsHide: true,
    ...options,
  });
  if (result.error) {
    throw new Error(`Could not run ${command}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(
      `${command} exited with status ${result.status ?? 'unknown'}`,
    );
  }
  return result;
}

function configuredBuild() {
  const args = process.argv.slice(2);
  let target = process.env.RUST_TARGET;
  let providerContext = false;
  for (let index = 0; index < args.length; index += 1) {
    if (args[index] === '--provider-context') {
      providerContext = true;
    } else if (args[index] === '--target' && args[index + 1]) {
      target = args[index + 1];
      index += 1;
    } else {
      throw new Error(
        'Usage: package-rust-extension.mjs [--provider-context] [--target <triple>]',
      );
    }
  }

  if (target) {
    if (/[\\/]/.test(target)) {
      throw new Error('The Rust target must be a target triple, not a path.');
    }
    return {
      profile: providerContext
        ? profiles.providerContext
        : profiles.externalContext,
      target,
    };
  }

  const version = run('rustc', ['-vV'], { stdio: 'pipe' }).stdout;
  const host = version.match(/^host: (.+)$/m)?.[1];
  if (!host)
    throw new Error('rustc -vV did not report its host target triple.');
  return {
    profile: providerContext
      ? profiles.providerContext
      : profiles.externalContext,
    target: host,
  };
}

async function packageExtension() {
  const { profile, target } = configuredBuild();
  const executable = `${profile.binary}${target.includes('windows') ? '.exe' : ''}`;
  const buildOutput = path.join(targetDir, target, 'release', executable);
  const outputDir = path.join(distDir, `${profile.output}-${target}`);
  const stagingDir = path.join(
    distDir,
    `.${profile.output}-${target}-${process.pid}.staging`,
  );

  run('cargo', [
    'build',
    '--manifest-path',
    path.join(rustRoot, 'Cargo.toml'),
    '--release',
    '--locked',
    '--bin',
    profile.binary,
    '--target',
    target,
    '--target-dir',
    targetDir,
  ]);

  await rm(stagingDir, { recursive: true, force: true });
  await mkdir(path.join(stagingDir, 'bin'), { recursive: true });
  await copyFile(buildOutput, path.join(stagingDir, 'bin', executable));
  if (!target.includes('windows')) {
    await chmod(path.join(stagingDir, 'bin', executable), 0o755);
  }

  const sourceManifest = JSON.parse(
    await readFile(path.join(integrationRoot, profile.manifest), 'utf8'),
  );
  const sourceServer = sourceManifest.mcpServers?.[profile.server];
  if (!sourceServer || typeof sourceServer !== 'object') {
    throw new Error(`The source manifest has no ${profile.server} MCP server.`);
  }

  const extensionPathVariable = '${extensionPath}';
  const pathSeparatorVariable = '${/}';
  const typescriptEntrypoint = `${extensionPathVariable}${pathSeparatorVariable}dist${pathSeparatorVariable}main.js`;
  const nativeArgs = Array.isArray(sourceServer.args)
    ? sourceServer.args.filter((argument) => argument !== typescriptEntrypoint)
    : [];
  const nativeManifest = {
    ...sourceManifest,
    mcpServers: {
      ...sourceManifest.mcpServers,
      [profile.server]: {
        ...sourceServer,
        command: `${extensionPathVariable}${pathSeparatorVariable}bin${pathSeparatorVariable}${executable}`,
        args: nativeArgs,
        cwd: '${extensionPath}',
      },
    },
  };
  await writeFile(
    path.join(stagingDir, 'canopy-extension.json'),
    `${JSON.stringify(nativeManifest, null, 2)}\n`,
  );
  await writeFile(
    path.join(stagingDir, profile.metadata),
    `${JSON.stringify(
      {
        extension: profile.extension,
        runtime: 'rust',
        target,
        executable: `bin/${executable}`,
        manifest: 'canopy-extension.json',
      },
      null,
      2,
    )}\n`,
  );
  await copyFile(
    path.join(repositoryRoot, 'LICENSE'),
    path.join(stagingDir, 'LICENSE'),
  );
  await writeFile(
    path.join(stagingDir, 'README.md'),
    profile.extension === 'external-context'
      ? `# ${profile.title}\n\n` +
          `This package contains the native MCP server for target \`${target}\`. ` +
          `Set \`QWEN_EXTERNAL_CONTEXT_CONFIG\` to an absolute version 1 ` +
          `configuration path and provide any credential variable referenced by ` +
          `that configuration before starting Qwen Code.\n\n` +
          `Link this directory with \`qwen extensions link <path>\`. The package ` +
          `exposes only \`context_search\`. It does not include the Auto-recall ` +
          `Hook or the optional Mem0 write confirmation Hook.\n\n` +
          `Use the administrator-managed MCP examples in the source repository ` +
          `when you need a pinned server path and configuration.\n`
      : `# ${profile.title}\n\n` +
          `This package contains the native local-provider MCP server for target ` +
          `\`${target}\`. Set \`PROVIDER_CONTEXT_BASE_URL\` and ` +
          `\`PROVIDER_CONTEXT_TOKEN\` in the trusted Qwen environment before ` +
          `starting Qwen Code. The extension preserves the source manifest's ` +
          `environment-variable names and MCP server settings. The TypeScript ` +
          `entrypoint argument is removed because the Rust binary launches ` +
          `directly; any remaining manifest arguments are retained.\n\n` +
          `Link this directory with \`qwen extensions link <path>\`. The package ` +
          `exposes only \`context_search\`. The base URL must use HTTPS, except ` +
          `for the example's loopback-only HTTP allowance.\n`,
  );

  await mkdir(distDir, { recursive: true });
  await rm(outputDir, { recursive: true, force: true });
  await rename(stagingDir, outputDir);
  console.log(
    `Built native Rust extension "${profile.extension}" for ${target}: ${outputDir}`,
  );
}

packageExtension().catch((error) => {
  console.error(`Rust extension packaging failed: ${error.message}`);
  process.exitCode = 1;
});
