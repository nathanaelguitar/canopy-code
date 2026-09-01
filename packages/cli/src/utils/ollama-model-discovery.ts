/**
 * Discover chat-capable models exposed by an Ollama OpenAI-compatible server.
 *
 * Ollama's /v1/models endpoint is intentionally used here instead of the
 * Ollama-specific API so the discovered entries use the exact same transport
 * as the configured model. Discovery is best-effort: a stopped or remote
 * server must never prevent the model picker from opening.
 */

import type {
  Config,
  ModelProvidersConfig,
} from '@canopy-code/canopy-code-core';
import type { LoadedSettings } from '../config/settings.js';

type ConfiguredModel = ModelProvidersConfig[string][number];

const DEFAULT_OLLAMA_BASE_URL = 'http://127.0.0.1:11434/v1';
const DISCOVERY_TIMEOUT_MS = 1500;

interface OllamaModelListing {
  data?: Array<{ id?: unknown }>;
}

interface OllamaEndpoint {
  baseUrl: string;
  envKey?: string;
}

function isOllamaBaseUrl(value: unknown): value is string {
  if (typeof value !== 'string' || value.trim().length === 0) return false;
  try {
    const url = new URL(value);
    return (
      url.port === '11434' ||
      url.hostname === 'ollama' ||
      url.hostname === 'ollama.com' ||
      url.hostname.endsWith('.ollama.com')
    );
  } catch {
    return false;
  }
}

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, '');
}

function modelName(id: string): string {
  return `${id} (Ollama)`;
}

function isChatModel(id: string): boolean {
  const normalized = id.toLowerCase();
  return !(
    normalized.includes('embed') ||
    normalized.includes('rerank') ||
    normalized.includes('embedding')
  );
}

function getConfiguredProviders(
  settings: LoadedSettings,
): ModelProvidersConfig {
  const configured = settings.merged.modelProviders;
  if (!configured || typeof configured !== 'object') return {};
  return configured as ModelProvidersConfig;
}

function getEndpoints(providers: ModelProvidersConfig): OllamaEndpoint[] {
  const endpoints = new Map<string, OllamaEndpoint>();
  const openaiModels = Array.isArray(providers['openai'])
    ? providers['openai']
    : [];

  for (const model of openaiModels) {
    if (!isOllamaBaseUrl(model.baseUrl)) continue;
    const baseUrl = normalizeBaseUrl(model.baseUrl);
    if (!endpoints.has(baseUrl)) {
      endpoints.set(baseUrl, { baseUrl, envKey: model.envKey });
    }
  }

  const configuredBaseUrl = process.env['OLLAMA_BASE_URL'];
  if (isOllamaBaseUrl(configuredBaseUrl)) {
    const baseUrl = normalizeBaseUrl(configuredBaseUrl);
    if (!endpoints.has(baseUrl)) {
      endpoints.set(baseUrl, { baseUrl, envKey: 'OLLAMA_API_KEY' });
    }
  }

  // Keep the common local install discoverable even before a model has been
  // configured. This is harmless when Ollama is not running (the request is
  // bounded by DISCOVERY_TIMEOUT_MS).
  if (!endpoints.has(DEFAULT_OLLAMA_BASE_URL)) {
    endpoints.set(DEFAULT_OLLAMA_BASE_URL, {
      baseUrl: DEFAULT_OLLAMA_BASE_URL,
      envKey: 'OLLAMA_API_KEY',
    });
  }

  return [...endpoints.values()];
}

async function fetchModelIds(endpoint: OllamaEndpoint): Promise<string[]> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), DISCOVERY_TIMEOUT_MS);
  try {
    const headers: Record<string, string> = {};
    const apiKey = endpoint.envKey ? process.env[endpoint.envKey] : undefined;
    if (apiKey) headers['Authorization'] = `Bearer ${apiKey}`;

    const response = await fetch(`${endpoint.baseUrl}/models`, {
      headers,
      signal: controller.signal,
    });
    if (!response.ok) return [];
    const payload = (await response.json()) as OllamaModelListing;
    if (!Array.isArray(payload.data)) return [];
    return payload.data
      .map((entry) => (typeof entry.id === 'string' ? entry.id.trim() : ''))
      .filter((id) => id.length > 0 && isChatModel(id));
  } catch {
    return [];
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * Refresh the in-memory model registry from configured Ollama endpoints.
 * Returns the number of new models registered. No settings are written: the
 * user's explicit model entries remain authoritative, while discovered models
 * are available for the current process and can be selected immediately.
 */
export async function refreshOllamaModels(
  config: Config,
  settings: LoadedSettings | undefined,
): Promise<number> {
  if (!settings) return 0;

  const providers = getConfiguredProviders(settings);
  const openaiModels = Array.isArray(providers['openai'])
    ? providers['openai']
    : [];
  const existingKeys = new Set(
    openaiModels.map(
      (model) => `${model.id}\0${normalizeBaseUrl(model.baseUrl ?? '')}`,
    ),
  );

  const discoveredByEndpoint = await Promise.all(
    getEndpoints(providers).map(async (endpoint) => ({
      endpoint,
      ids: await fetchModelIds(endpoint),
    })),
  );

  const discovered: ConfiguredModel[] = [];
  for (const { endpoint, ids } of discoveredByEndpoint) {
    for (const id of ids) {
      const key = `${id}\0${endpoint.baseUrl}`;
      if (existingKeys.has(key)) continue;
      existingKeys.add(key);
      discovered.push({
        id,
        name: modelName(id),
        description: `Discovered from Ollama at ${endpoint.baseUrl}.`,
        baseUrl: endpoint.baseUrl,
        envKey: endpoint.envKey,
      });
    }
  }

  if (discovered.length === 0) return 0;

  const updatedProviders: ModelProvidersConfig = {
    ...providers,
    openai: [...openaiModels, ...discovered],
  };
  config.reloadModelProvidersConfig(updatedProviders);
  return discovered.length;
}
