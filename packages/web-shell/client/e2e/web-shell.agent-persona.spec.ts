import fs from 'node:fs';
import path from 'node:path';
import { expect, test, type Page, type TestInfo } from '@playwright/test';
import {
  assistantTextEvent,
  createWebShellDaemonScenario,
  installMockDaemon,
  replayCompleteEvent,
  type DaemonEvent,
  type MockDaemonController,
  type WebShellDaemonScenario,
} from './utils/mockDaemon';

// Optional: a directory with `<mood>.png` / `<mood>.webm` avatar art, served at
// /assets/persona. Without it the persona shows a monogram.
const ASSETS = process.env['AGENT_PERSONA_ASSETS'];
// Optional: where to write screenshots of each state.
const SHOTS = process.env['AGENT_PERSONA_SCREENSHOTS'];

function thoughtEvent(text: string): DaemonEvent {
  return {
    v: 1,
    type: 'session_update',
    data: {
      update: {
        sessionUpdate: 'agent_thought_chunk',
        content: { type: 'text', text },
      },
    },
  };
}

function readToolEvent(status: 'in_progress' | 'completed'): DaemonEvent {
  return {
    v: 1,
    type: 'session_update',
    data: {
      update: {
        sessionUpdate:
          status === 'in_progress' ? 'tool_call' : 'tool_call_update',
        toolCallId: 'call-read-1',
        toolName: 'read_file',
        title: 'Read src/AppState.ts',
        kind: 'read',
        status,
        rawInput: { path: 'src/AppState.ts' },
      },
    },
  };
}

async function shot(page: Page, name: string): Promise<void> {
  if (!SHOTS) return;
  fs.mkdirSync(SHOTS, { recursive: true });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(SHOTS, `${name}.png`) });
}

test('agent persona: off by default', async ({ page }, testInfo) => {
  const scenario = createWebShellDaemonScenario();
  const daemon = await install(page, scenario, testInfo);
  await gotoSession(page, scenario, daemon, '');
  await expect(page.locator('[data-testid="agent-reply-label"]')).toHaveCount(
    0,
  );
  await expect(page.locator('[data-testid="agent-presence"]')).toHaveCount(0);
});

for (const theme of ['light', 'dark'] as const) {
  test(`agent persona: name, avatar, status bubble (${theme})`, async ({
    page,
  }, testInfo) => {
    const scenario = createWebShellDaemonScenario();
    const daemon = await install(page, scenario, testInfo);
    await gotoSession(
      page,
      scenario,
      daemon,
      `?agent=Flame&agentLang=en&theme=${theme}${ASSETS ? '' : '&agentAvatar=none'}`,
    );
    const editor = page.locator('[data-web-shell-composer-editor]');
    await expect(editor).toContainText('Message Flame…');

    await editor.locator('.cm-content').click();
    await page.keyboard.type('The session list flickers when a turn ends.');
    await page.locator('[data-web-shell-composer-submit]').click();
    await expect.poll(() => daemon.promptRequests().length).toBe(1);

    const presence = page.locator('[data-testid="agent-presence"]');
    await daemon.sse.split(thoughtEvent('Check the refresh path first.'));
    await expect(presence).toHaveAttribute('aria-label', 'Flame is thinking…');
    await expect.poll(() => page.title()).toContain('"mood":"thinking"');

    await daemon.sse.split(readToolEvent('in_progress'));
    await expect(presence).toHaveAttribute(
      'aria-label',
      'Flame is reading the code…',
    );
    await expect(presence).toContainText('AppState.ts');
    await shot(page, `agent-persona-working-${theme}`);

    await daemon.sse.split(readToolEvent('completed'));
    await daemon.sse.split(
      assistantTextEvent(
        'Found it: the list is replaced on every turn end, so rows lose their identity. I will merge by id instead.',
      ),
    );
    await expect(
      page.locator('[data-testid="agent-reply-label"]').first(),
    ).toContainText('Flame');
    await shot(page, `agent-persona-reply-${theme}`);
  });
}

test('agent persona: a loop that cannot play as WebM falls back to WebP', async ({
  page,
}, testInfo) => {
  test.skip(!ASSETS, 'needs AGENT_PERSONA_ASSETS');
  const scenario = createWebShellDaemonScenario();
  const daemon = await install(page, scenario, testInfo);
  // A server without byte ranges breaks WebM in WebKitGTK; simulate the
  // failure. Registered after install() so this route wins.
  await page.route('**/assets/persona/*.webm', (route) =>
    route.fulfill({ status: 404 }),
  );
  await gotoSession(page, scenario, daemon, '?agent=Flame&agentLang=en');
  const editor = page.locator('[data-web-shell-composer-editor]');
  await editor.locator('.cm-content').click();
  await page.keyboard.type('Look around.');
  await page.locator('[data-web-shell-composer-submit]').click();
  await expect.poll(() => daemon.promptRequests().length).toBe(1);
  await daemon.sse.split(readToolEvent('in_progress'));
  const loop = page.locator('[data-testid="agent-presence"] [data-agent-loop]');
  await expect(loop).toHaveAttribute('data-agent-loop', 'webp');
});

test('agent persona: Spanish status lines', async ({ page }, testInfo) => {
  const scenario = createWebShellDaemonScenario();
  const daemon = await install(page, scenario, testInfo);
  await gotoSession(
    page,
    scenario,
    daemon,
    '?agent=Ember&agentLang=es&agentAvatar=none',
  );
  const editor = page.locator('[data-web-shell-composer-editor]');
  await expect(editor).toContainText('Escríbale a Ember…');
  await editor.locator('.cm-content').click();
  await page.keyboard.type('Revise las pruebas.');
  await page.locator('[data-web-shell-composer-submit]').click();
  await expect.poll(() => daemon.promptRequests().length).toBe(1);
  await daemon.sse.split(readToolEvent('in_progress'));
  await expect(page.locator('[data-testid="agent-presence"]')).toHaveAttribute(
    'aria-label',
    'Ember está leyendo el código…',
  );
});

async function install(
  page: Page,
  scenario: WebShellDaemonScenario,
  testInfo: TestInfo,
): Promise<MockDaemonController> {
  if (ASSETS) {
    await page.route('**/assets/persona/*', async (route) => {
      const file = path.join(
        ASSETS,
        path.basename(new URL(route.request().url()).pathname),
      );
      if (!fs.existsSync(file)) return route.fulfill({ status: 404 });
      await route.fulfill({
        body: fs.readFileSync(file),
        contentType: file.endsWith('.webm') ? 'video/webm' : 'image/png',
      });
    });
  }
  return installMockDaemon(page, scenario, {
    baseURL: String(testInfo.project.use.baseURL),
  });
}

async function gotoSession(
  page: Page,
  scenario: WebShellDaemonScenario,
  daemon: MockDaemonController,
  search: string,
): Promise<void> {
  await page.goto(
    `/session/${encodeURIComponent(scenario.sessionId)}${search}`,
  );
  await expect(page.locator('[data-web-shell-root]')).toBeVisible();
  const connection = await daemon.sse.waitForConnection(scenario.sessionId);
  await daemon.sendEvent(
    replayCompleteEvent({
      sessionId: connection.sessionId,
      replayedCount: scenario.events.length,
    }),
  );
  await expect(page.getByText('Loading...')).toHaveCount(0);
}
