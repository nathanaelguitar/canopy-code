/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { type ColorsTheme, Theme } from './theme.js';
import { darkSemanticColors } from './semantic-tokens.js';

const canopyDarkColors: ColorsTheme = {
  type: 'dark',
  Background: '#0b0e14',
  Foreground: '#bfbdb6',
  LightBlue: '#59C2FF',
  AccentBlue: '#39BAE6',
  AccentPurple: '#D2A6FF',
  AccentCyan: '#95E6CB',
  AccentGreen: '#AAD94C',
  AccentYellow: '#FFD700',
  AccentRed: '#F26D78',
  AccentYellowDim: '#8B7530',
  AccentRedDim: '#8B3A4A',
  DiffAdded: '#AAD94C',
  DiffRemoved: '#F26D78',
  Comment: '#646A71',
  Gray: '#3D4149',
  GradientColors: ['#FFD700', '#da7959'],
};

export const CanopyDark: Theme = new Theme(
  'Canopy Dark',
  'dark',
  {
    hljs: {
      display: 'block',
      overflowX: 'auto',
      padding: '0.5em',
      background: canopyDarkColors.Background,
      color: canopyDarkColors.Foreground,
    },
    'hljs-keyword': {
      color: canopyDarkColors.AccentYellow,
    },
    'hljs-literal': {
      color: canopyDarkColors.AccentPurple,
    },
    'hljs-symbol': {
      color: canopyDarkColors.AccentCyan,
    },
    'hljs-name': {
      color: canopyDarkColors.LightBlue,
    },
    'hljs-link': {
      color: canopyDarkColors.AccentBlue,
    },
    'hljs-function .hljs-keyword': {
      color: canopyDarkColors.AccentYellow,
    },
    'hljs-subst': {
      color: canopyDarkColors.Foreground,
    },
    'hljs-string': {
      color: canopyDarkColors.AccentGreen,
    },
    'hljs-title': {
      color: canopyDarkColors.AccentYellow,
    },
    'hljs-type': {
      color: canopyDarkColors.AccentBlue,
    },
    'hljs-attribute': {
      color: canopyDarkColors.AccentYellow,
    },
    'hljs-bullet': {
      color: canopyDarkColors.AccentYellow,
    },
    'hljs-addition': {
      color: canopyDarkColors.AccentGreen,
    },
    'hljs-variable': {
      color: canopyDarkColors.Foreground,
    },
    'hljs-template-tag': {
      color: canopyDarkColors.AccentYellow,
    },
    'hljs-template-variable': {
      color: canopyDarkColors.AccentYellow,
    },
    'hljs-comment': {
      color: canopyDarkColors.Comment,
      fontStyle: 'italic',
    },
    'hljs-quote': {
      color: canopyDarkColors.AccentCyan,
      fontStyle: 'italic',
    },
    'hljs-deletion': {
      color: canopyDarkColors.AccentRed,
    },
    'hljs-meta': {
      color: canopyDarkColors.AccentYellow,
    },
    'hljs-doctag': {
      fontWeight: 'bold',
    },
    'hljs-strong': {
      fontWeight: 'bold',
    },
    'hljs-emphasis': {
      fontStyle: 'italic',
    },
  },
  canopyDarkColors,
  darkSemanticColors,
);
