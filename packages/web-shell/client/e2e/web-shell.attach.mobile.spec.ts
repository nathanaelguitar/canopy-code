/**
 * @license
 * Copyright 2026 Qwen Team
 * SPDX-License-Identifier: Apache-2.0
 */

// The composer's "+" button on a daemon that cannot take workspace uploads
// (the Canopy remote-control gateway: no `workspace_file_upload`, no
// `workspaces[]`). The button must be there whatever the model can see, open
// the system file picker, and send a chosen picture inline with the prompt.

import { expect, test, type Page, type TestInfo } from '@playwright/test';
import {
  createWebShellDaemonScenario,
  installMockDaemon,
  replayCompleteEvent,
  type DaemonRequestRecord,
  type MockDaemonController,
  type WebShellDaemonScenario,
} from './utils/mockDaemon';

const COMPOSER_TEXTAREA = 'textarea[data-web-shell-composer-editor]';
const PLUS = '[data-web-shell-attachment-button]';

// A 1x1 PNG.
const PNG_BASE64 =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==';

for (const [label, image] of [
  ['a model that sees pictures', true],
  ['a text-only model', false],
] as const) {
  test(`offers "+" with ${label}, opens the picker and sends the picture inline`, async ({
    page,
  }, testInfo) => {
    const scenario = createWebShellDaemonScenario();
    for (const provider of scenario.providers.providers) {
      for (const model of provider.models) model.modalities = { image };
    }
    expect(scenario.capabilities.features).not.toContain(
      'workspace_file_upload',
    );
    const daemon = await installScenario(page, scenario, testInfo);
    await gotoSession(page, scenario, daemon);

    const plus = page.locator(PLUS);
    await expect(plus).toBeVisible();
    await expect(plus).toBeEnabled();

    const [chooser] = await Promise.all([
      page.waitForEvent('filechooser'),
      plus.tap(),
    ]);
    expect(chooser.isMultiple()).toBe(true);
    await chooser.setFiles({
      name: 'photo.png',
      mimeType: 'image/png',
      buffer: Buffer.from(PNG_BASE64, 'base64'),
    });
    await expect(
      page.locator('[data-web-shell-composer-attachments] img'),
    ).toHaveCount(1);

    await page.locator(COMPOSER_TEXTAREA).fill('what is in this picture?');
    await page.locator('[data-web-shell-composer-submit]').tap();

    await expect.poll(() => daemon.promptRequests().length).toBe(1);
    const prompt = promptBlocks(daemon.promptRequests()[0]!);
    expect(prompt).toContainEqual(
      expect.objectContaining({
        type: 'text',
        text: 'what is in this picture?',
      }),
    );
    expect(prompt).toContainEqual(
      expect.objectContaining({
        type: 'image',
        mimeType: 'image/png',
        data: PNG_BASE64,
      }),
    );
  });
}

test('attaches a chosen text file inline', async ({ page }, testInfo) => {
  const scenario = createWebShellDaemonScenario();
  const daemon = await installScenario(page, scenario, testInfo);
  await gotoSession(page, scenario, daemon);

  const [chooser] = await Promise.all([
    page.waitForEvent('filechooser'),
    page.locator(PLUS).tap(),
  ]);
  await chooser.setFiles({
    name: 'notes.md',
    mimeType: 'text/markdown',
    buffer: Buffer.from('# notes\nremember the milk'),
  });
  await expect(
    page.locator('[data-web-shell-composer-attachments]'),
  ).toContainText('notes.md');

  await page.locator(COMPOSER_TEXTAREA).fill('summarize');
  await page.locator('[data-web-shell-composer-submit]').tap();
  await expect.poll(() => daemon.promptRequests().length).toBe(1);
  expect(JSON.stringify(promptBlocks(daemon.promptRequests()[0]!))).toContain(
    'remember the milk',
  );
});

function promptBlocks(request: DaemonRequestRecord): unknown[] {
  const body = request.body as { prompt?: unknown } | undefined;
  expect(Array.isArray(body?.prompt)).toBe(true);
  return body!.prompt as unknown[];
}

async function installScenario(
  page: Page,
  scenario: WebShellDaemonScenario,
  testInfo: TestInfo,
): Promise<MockDaemonController> {
  return installMockDaemon(page, scenario, {
    baseURL: String(testInfo.project.use.baseURL),
  });
}

async function gotoSession(
  page: Page,
  scenario: WebShellDaemonScenario,
  daemon: MockDaemonController,
): Promise<void> {
  await page.goto(`/session/${encodeURIComponent(scenario.sessionId)}`);
  await expect(page.locator('[data-web-shell-root]')).toBeVisible();
  const connection = await daemon.sse.waitForConnection(scenario.sessionId);
  await daemon.sendEvent(
    replayCompleteEvent({
      sessionId: connection.sessionId,
      replayedCount: 0,
    }),
  );
  await expect(page.getByText('Loading...')).toHaveCount(0);
}
