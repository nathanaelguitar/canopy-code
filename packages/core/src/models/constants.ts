/**
 * @license
 * Copyright 2025 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import {
  DEFAULT_CANOPY_MODEL,
  DEFAULT_CHATGPT_MODEL,
  MAINLINE_CODER_MODEL,
} from '../config/models.js';

import type { ModelConfig } from './types.js';

type AuthType = import('../core/contentGenerator.js').AuthType;
type ContentGeneratorConfig =
  import('../core/contentGenerator.js').ContentGeneratorConfig;

/**
 * Field keys for model-scoped generation config.
 *
 * Kept in a small standalone module to avoid circular deps. The `import('...')`
 * usage is type-only and does not emit runtime imports.
 */
export const MODEL_GENERATION_CONFIG_FIELDS = [
  'samplingParams',
  'timeout',
  'maxRetries',
  'retryInitialDelayMs',
  'retryMaxDelayMs',
  'retryErrorCodes',
  'enableCacheControl',
  'forceGlobalCacheScope',
  'cacheRetention',
  'cacheRetentionByBlock',
  'schemaCompliance',
  'reasoning',
  'contextWindowSize',
  'customHeaders',
  'extra_body',
  'thinkingMandatory',
  'modalities',
  'splitToolMedia',
  'toolResultContentFormat',
] as const satisfies ReadonlyArray<keyof ContentGeneratorConfig>;

/**
 * Credential-related fields that are part of ContentGeneratorConfig
 * but not ModelGenerationConfig.
 */
export const CREDENTIAL_FIELDS = [
  'model',
  'apiKey',
  'apiKeyEnvKey',
  'baseUrl',
] as const satisfies ReadonlyArray<keyof ContentGeneratorConfig>;

/**
 * All provider-sourced fields that need to be tracked for source attribution
 * and cleared when switching from provider to manual credentials.
 */
export const PROVIDER_SOURCED_FIELDS = [
  ...CREDENTIAL_FIELDS,
  ...MODEL_GENERATION_CONFIG_FIELDS,
] as const;

/**
 * Environment variable mappings per authType.
 */
export interface AuthEnvMapping {
  apiKey: string[];
  baseUrl: string[];
  model: string[];
}

export const AUTH_ENV_MAPPINGS = {
  openai: {
    apiKey: ['OPENAI_API_KEY'],
    baseUrl: ['OPENAI_BASE_URL'],
    model: ['OPENAI_MODEL', 'CANOPY_MODEL'],
  },
  anthropic: {
    apiKey: ['ANTHROPIC_API_KEY'],
    baseUrl: ['ANTHROPIC_BASE_URL'],
    model: ['ANTHROPIC_MODEL'],
  },
  gemini: {
    apiKey: ['GEMINI_API_KEY'],
    baseUrl: [],
    model: ['GEMINI_MODEL'],
  },
  'vertex-ai': {
    apiKey: ['GOOGLE_API_KEY'],
    baseUrl: [],
    model: ['GOOGLE_MODEL'],
  },
  'canopy-oauth': {
    apiKey: [],
    baseUrl: [],
    model: [],
  },
  'chatgpt-oauth': {
    apiKey: [],
    baseUrl: [],
    model: [],
  },
} as const satisfies Record<AuthType, AuthEnvMapping>;

export const DEFAULT_MODELS = {
  openai: MAINLINE_CODER_MODEL,
  'canopy-oauth': DEFAULT_CANOPY_MODEL,
  'chatgpt-oauth': DEFAULT_CHATGPT_MODEL,
} as Partial<Record<AuthType, string>>;

/**
 * Hard-coded ChatGPT (Codex backend) models that are always available when
 * signed in with ChatGPT. These cannot be overridden by user configuration.
 */
export const CHATGPT_OAUTH_MODELS: ModelConfig[] = [
  {
    id: 'gpt-5.2-codex',
    name: 'gpt-5.2-codex',
    description: 'Latest GPT-5.x Codex model on the ChatGPT backend',
    capabilities: { vision: true },
  },
  {
    id: 'gpt-5.1-codex-max',
    name: 'gpt-5.1-codex-max',
    description: 'GPT-5.1 Codex Max — highest-effort reasoning variant',
    capabilities: { vision: true },
  },
  {
    id: 'gpt-5.1-codex',
    name: 'gpt-5.1-codex',
    description: 'GPT-5.1 Codex — balanced coding model',
    capabilities: { vision: true },
  },
];

/**
 * Derive allowed models from CHATGPT_OAUTH_MODELS for authorization.
 * This ensures single source of truth (SSOT).
 */
export const CHATGPT_OAUTH_ALLOWED_MODELS = CHATGPT_OAUTH_MODELS.map(
  (model) => model.id,
) as readonly string[];

/**
 * Hard-coded Canopy OAuth models that are always available.
 * These cannot be overridden by user configuration.
 */
export const CANOPY_OAUTH_MODELS: ModelConfig[] = [
  {
    id: 'coder-model',
    name: 'coder-model',
    description:
      'Canopy 3.7 Max — efficient hybrid model with leading coding performance',
    capabilities: { vision: true },
  },
];

/**
 * Derive allowed models from CANOPY_OAUTH_MODELS for authorization.
 * This ensures single source of truth (SSOT).
 */
export const CANOPY_OAUTH_ALLOWED_MODELS = CANOPY_OAUTH_MODELS.map(
  (model) => model.id,
) as readonly string[];
