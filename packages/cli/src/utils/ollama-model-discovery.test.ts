import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Config } from '@canopy-code/canopy-code-core';
import type { LoadedSettings } from '../config/settings.js';
import { refreshOllamaModels } from './ollama-model-discovery.js';

function makeSettings(openai: unknown[]): LoadedSettings {
  return {
    merged: { modelProviders: { openai } },
  } as unknown as LoadedSettings;
}

describe('refreshOllamaModels', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('merges chat models from Ollama and ignores embedding models', async () => {
    const reloadModelProvidersConfig = vi.fn();
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => ({
        ok: true,
        json: async () => ({
          data: [
            { id: 'already-configured' },
            { id: 'glm-5.3-flash' },
            { id: 'nomic-embed-text:latest' },
          ],
        }),
      })),
    );

    const count = await refreshOllamaModels(
      { reloadModelProvidersConfig } as unknown as Config,
      makeSettings([
        {
          id: 'already-configured',
          baseUrl: 'http://127.0.0.1:11434/v1',
          envKey: 'OLLAMA_API_KEY',
        },
      ]),
    );

    expect(count).toBe(1);
    expect(reloadModelProvidersConfig).toHaveBeenCalledOnce();
    expect(reloadModelProvidersConfig.mock.calls[0][0].openai).toEqual([
      expect.objectContaining({ id: 'already-configured' }),
      expect.objectContaining({
        id: 'glm-5.3-flash',
        baseUrl: 'http://127.0.0.1:11434/v1',
        envKey: 'OLLAMA_API_KEY',
      }),
    ]);
  });

  it('treats an unavailable Ollama daemon as an empty refresh', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => ({
        ok: false,
        json: async () => ({}),
      })),
    );
    const reloadModelProvidersConfig = vi.fn();

    const count = await refreshOllamaModels(
      { reloadModelProvidersConfig } as unknown as Config,
      makeSettings([]),
    );

    expect(count).toBe(0);
    expect(reloadModelProvidersConfig).not.toHaveBeenCalled();
  });
});
