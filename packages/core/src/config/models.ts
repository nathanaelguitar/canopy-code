/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

export const DEFAULT_CANOPY_MODEL = 'coder-model';
export const DEFAULT_CANOPY_FLASH_MODEL = 'coder-model';
export const DEFAULT_CANOPY_EMBEDDING_MODEL = 'text-embedding-v4';
export const MAINLINE_CODER_MODEL = 'qwen3.7-max';
// The ChatGPT Codex endpoint rejects the family alias `gpt-5.6` even though
// the public API documents it as an alias for Sol. Use the concrete slug on
// the OAuth transport; it is semantically equivalent and accepted there.
export const DEFAULT_CHATGPT_MODEL = 'gpt-5.6-sol';
