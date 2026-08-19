/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { DEFAULT_CANOPY_CUSTOM_IGNORE_FILE_NAMES } from '../utils/canopy-ignore-parser.js';

export interface FileFilteringOptions {
  respectGitIgnore: boolean;
  respectCanopyIgnore: boolean;
  customIgnoreFiles?: string[];
}

// For memory files
export const DEFAULT_MEMORY_FILE_FILTERING_OPTIONS: FileFilteringOptions = {
  respectGitIgnore: false,
  respectCanopyIgnore: true,
  customIgnoreFiles: [...DEFAULT_CANOPY_CUSTOM_IGNORE_FILE_NAMES],
};

// For all other files
export const DEFAULT_FILE_FILTERING_OPTIONS: FileFilteringOptions = {
  respectGitIgnore: true,
  respectCanopyIgnore: true,
  customIgnoreFiles: [...DEFAULT_CANOPY_CUSTOM_IGNORE_FILE_NAMES],
};
