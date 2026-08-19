/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { promises as fs } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { Storage } from '../../config/storage.js';
import type {
  ArtifactPublisher,
  PublishArtifactInput,
  PublishedArtifact,
} from './publisher.js';

/**
 * Option B: writes the artifact to the local Canopy home and returns a file://
 * URL. No network, no sharing — the page opens directly in the browser. Keyed
 * by id under `~/.canopy/artifacts/{id}/index.html`, so redeploys overwrite in
 * place and keep the same URL.
 */
export class LocalPublisher implements ArtifactPublisher {
  readonly kind = 'local';

  /** @param baseDir Override the output root (defaults to ~/.canopy/artifacts). */
  constructor(private readonly baseDir?: string) {}

  private getBaseDir(): string {
    return this.baseDir ?? path.join(Storage.getGlobalCanopyDir(), 'artifacts');
  }

  async publish(input: PublishArtifactInput): Promise<PublishedArtifact> {
    const dir = path.join(this.getBaseDir(), input.id);
    await fs.mkdir(dir, { recursive: true });
    const filePath = path.join(dir, 'index.html');
    await fs.writeFile(filePath, input.html, 'utf8');
    return {
      id: input.id,
      url: pathToFileURL(filePath).href,
      filePath,
    };
  }
}
