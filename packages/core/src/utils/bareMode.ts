/**
 * @license
 * Copyright 2025 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

export const CANOPY_CODE_SIMPLE_ENV_VAR = 'QWEN_CODE_SIMPLE';

export function isTruthy(value: string | undefined): boolean {
  if (!value) {
    return false;
  }

  return ['1', 'true', 'yes', 'on'].includes(value.toLowerCase().trim());
}

export function isBareMode(cliFlag?: boolean): boolean {
  return cliFlag === true || isTruthy(process.env[CANOPY_CODE_SIMPLE_ENV_VAR]);
}
