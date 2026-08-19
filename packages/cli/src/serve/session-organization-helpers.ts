/**
 * @license
 * Copyright 2025 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { SessionOrganizationService } from '@canopy-code/canopy-code-core';
import { writeStderrLine } from '../utils/stdioHelpers.js';

export function createSessionOrganizationService(
  workspaceCwd: string,
): SessionOrganizationService {
  return new SessionOrganizationService(workspaceCwd, (message) => {
    writeStderrLine(`canopy serve: session-org: ${message}`);
  });
}
