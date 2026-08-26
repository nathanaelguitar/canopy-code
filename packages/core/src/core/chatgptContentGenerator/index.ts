/**
 * @license
 * Copyright 2026 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

import type {
  ContentGenerator,
  ContentGeneratorConfig,
} from '../contentGenerator.js';
import type { Config } from '../../config/config.js';
import { ChatgptContentGenerator } from './chatgpt-content-generator.js';

export { ChatgptContentGenerator } from './chatgpt-content-generator.js';

export function createChatgptContentGenerator(
  contentGeneratorConfig: ContentGeneratorConfig,
  cliConfig: Config,
): ContentGenerator {
  return new ChatgptContentGenerator(contentGeneratorConfig, cliConfig);
}
