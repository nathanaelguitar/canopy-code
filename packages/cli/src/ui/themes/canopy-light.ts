/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { type ColorsTheme, Theme } from './theme.js';
import { lightSemanticColors } from './semantic-tokens.js';

const canopyLightColors: ColorsTheme = {
  type: 'light',
  Background: '#f8f9fa',
  Foreground: '#5c6166',
  LightBlue: '#55b4d4',
  AccentBlue: '#399ee6',
  AccentPurple: '#a37acc',
  AccentCyan: '#4cbf99',
  AccentGreen: '#86b300',
  AccentYellow: '#f2ae49',
  AccentRed: '#f07171',
  AccentYellowDim: '#8B7000',
  AccentRedDim: '#993333',
  DiffAdded: '#86b300',
  DiffRemoved: '#f07171',
  Comment: '#ABADB1',
  Gray: '#CCCFD3',
  GradientColors: ['#399ee6', '#86b300'],
};

export const CanopyLight: Theme = new Theme(
  'Canopy Light',
  'light',
  {
    hljs: {
      display: 'block',
      overflowX: 'auto',
      padding: '0.5em',
      background: canopyLightColors.Background,
      color: canopyLightColors.Foreground,
    },
    'hljs-comment': {
      color: canopyLightColors.Comment,
      fontStyle: 'italic',
    },
    'hljs-quote': {
      color: canopyLightColors.AccentCyan,
      fontStyle: 'italic',
    },
    'hljs-string': {
      color: canopyLightColors.AccentGreen,
    },
    'hljs-constant': {
      color: canopyLightColors.AccentCyan,
    },
    'hljs-number': {
      color: canopyLightColors.AccentPurple,
    },
    'hljs-keyword': {
      color: canopyLightColors.AccentYellow,
    },
    'hljs-selector-tag': {
      color: canopyLightColors.AccentYellow,
    },
    'hljs-attribute': {
      color: canopyLightColors.AccentYellow,
    },
    'hljs-variable': {
      color: canopyLightColors.Foreground,
    },
    'hljs-variable.language': {
      color: canopyLightColors.LightBlue,
      fontStyle: 'italic',
    },
    'hljs-title': {
      color: canopyLightColors.AccentBlue,
    },
    'hljs-section': {
      color: canopyLightColors.AccentGreen,
      fontWeight: 'bold',
    },
    'hljs-type': {
      color: canopyLightColors.LightBlue,
    },
    'hljs-class .hljs-title': {
      color: canopyLightColors.AccentBlue,
    },
    'hljs-tag': {
      color: canopyLightColors.LightBlue,
    },
    'hljs-name': {
      color: canopyLightColors.AccentBlue,
    },
    'hljs-builtin-name': {
      color: canopyLightColors.AccentYellow,
    },
    'hljs-meta': {
      color: canopyLightColors.AccentYellow,
    },
    'hljs-symbol': {
      color: canopyLightColors.AccentRed,
    },
    'hljs-bullet': {
      color: canopyLightColors.AccentYellow,
    },
    'hljs-regexp': {
      color: canopyLightColors.AccentCyan,
    },
    'hljs-link': {
      color: canopyLightColors.LightBlue,
    },
    'hljs-deletion': {
      color: canopyLightColors.AccentRed,
    },
    'hljs-addition': {
      color: canopyLightColors.AccentGreen,
    },
    'hljs-emphasis': {
      fontStyle: 'italic',
    },
    'hljs-strong': {
      fontWeight: 'bold',
    },
    'hljs-literal': {
      color: canopyLightColors.AccentCyan,
    },
    'hljs-built_in': {
      color: canopyLightColors.AccentRed,
    },
    'hljs-doctag': {
      color: canopyLightColors.AccentRed,
    },
    'hljs-template-variable': {
      color: canopyLightColors.AccentCyan,
    },
    'hljs-selector-id': {
      color: canopyLightColors.AccentRed,
    },
  },
  canopyLightColors,
  lightSemanticColors,
);
