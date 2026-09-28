import { readdir, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const integrationRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '..',
);
const distDirectory = path.join(integrationRoot, 'dist');
const preservedPackagePrefixes = ['rust-extension-', 'rust-provider-context-'];

try {
  const entries = await readdir(distDirectory);
  for (const entry of entries) {
    if (preservedPackagePrefixes.some((prefix) => entry.startsWith(prefix))) {
      continue;
    }
    await rm(path.join(distDirectory, entry), { recursive: true, force: true });
  }
} catch (error) {
  if (error.code !== 'ENOENT') throw error;
}

await rm(path.join(integrationRoot, 'tsconfig.tsbuildinfo'), { force: true });
