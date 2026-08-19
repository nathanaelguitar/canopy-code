/**
 * @license
 * Copyright 2025 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * @fileoverview Skills feature implementation
 *
 * This module provides the foundation for the skills feature, which allows
 * users to define reusable skill configurations that can be loaded by the
 * model via a dedicated Skills tool.
 *
 * Skills are stored as directories containing a SKILL.md file with YAML
 * frontmatter for metadata. They can be loaded from four levels
 * (precedence: project > user > extension > bundled):
 * - Project-level: `.canopy/skills/`
 * - User-level: `~/.canopy/skills/`
 * - Extension-level: provided by installed extensions
 * - Bundled: built-in skills shipped with canopy-code
 */

// Core types and interfaces
export type {
  SkillConfig,
  SkillLevel,
  SkillValidationResult,
  ListSkillsOptions,
  SkillErrorCode,
} from './types.js';

export { SkillError } from './types.js';

// Main management class
export { SkillManager } from './skill-manager.js';

// Priority normalization, shared with the `/skills` display sort
export { normalizeSkillPriority } from './skill-load.js';

// Path-based conditional skill activation
export {
  SkillActivationRegistry,
  splitConditionalSkills,
} from './skill-activation.js';

// Project auto-skill lifecycle maintenance
export * from './skill-curator.js';
