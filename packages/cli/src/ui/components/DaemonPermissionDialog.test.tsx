/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { render } from 'ink-testing-library';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useKeypress } from '../hooks/useKeypress.js';
import { RadioButtonSelect } from './shared/RadioButtonSelect.js';
import { DaemonPermissionDialog } from './DaemonPermissionDialog.js';

vi.mock('../hooks/useKeypress.js', () => ({ useKeypress: vi.fn() }));
vi.mock('./shared/RadioButtonSelect.js', () => ({
  RadioButtonSelect: vi.fn(() => null),
}));

const mockedUseKeypress = vi.mocked(useKeypress);
const mockedRadioButtonSelect = vi.mocked(RadioButtonSelect);

describe('DaemonPermissionDialog', () => {
  beforeEach(() => vi.clearAllMocks());

  it('renders daemon choices and submits the selected option', () => {
    const onAnswer = vi.fn().mockResolvedValue(undefined);
    const { lastFrame } = render(
      <DaemonPermissionDialog
        request={{
          requestId: 'request-1',
          toolCall: { title: 'Run browser action' },
          options: [
            { optionId: 'once', name: 'Allow once' },
            { optionId: 'always', name: 'Always allow' },
          ],
        }}
        onAnswer={onAnswer}
        availableWidth={80}
      />,
    );

    expect(lastFrame()).toContain('Run browser action');
    expect(lastFrame()).toContain(
      'Do you want to allow this action to continue?',
    );
    const props = mockedRadioButtonSelect.mock.calls[0]?.[0];
    expect(props?.items.map((item) => item.label)).toEqual([
      'Allow once',
      'Always allow',
      'Cancel (esc)',
    ]);
    props?.onSelect({ outcome: 'selected', optionId: 'once' });
    expect(onAnswer).toHaveBeenCalledWith('request-1', {
      outcome: 'selected',
      optionId: 'once',
    });
  });

  it('sends cancellation when Escape is pressed', () => {
    const onAnswer = vi.fn().mockResolvedValue(undefined);
    render(
      <DaemonPermissionDialog
        request={{ requestId: 'request-1', toolCall: {}, options: [] }}
        onAnswer={onAnswer}
        availableWidth={80}
      />,
    );
    const keyHandler = mockedUseKeypress.mock.calls[0]?.[0];
    keyHandler?.({ name: 'escape' } as never);
    expect(onAnswer).toHaveBeenCalledWith('request-1', {
      outcome: 'cancelled',
    });
  });

  it('renders structured daemon questions', () => {
    const onAnswer = vi.fn().mockResolvedValue(undefined);
    const { lastFrame } = render(
      <DaemonPermissionDialog
        request={{
          requestId: 'request-question',
          toolCall: {
            title: 'Choose a deployment target',
            _meta: {
              canopyQuestions: [
                {
                  header: 'Target',
                  question: 'Where should this deploy?',
                  options: [
                    { label: 'Staging', description: 'Safe preview' },
                    { label: 'Production', description: 'Live traffic' },
                  ],
                },
              ],
            },
          },
          options: [{ optionId: 'answer', kind: 'allow_once' }],
        }}
        onAnswer={onAnswer}
        availableWidth={80}
      />,
    );

    expect(lastFrame()).toContain('Where should this deploy?');
    expect(lastFrame()).toContain('Staging');
    expect(lastFrame()).toContain('Production');
  });
});
