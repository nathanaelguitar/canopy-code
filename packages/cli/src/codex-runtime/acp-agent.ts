/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { createHash, randomUUID } from 'node:crypto';
import { spawn, type ChildProcessByStdio } from 'node:child_process';
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
  Readable,
  Writable,
  type Readable as ReadableStreamNode,
  type Writable as WritableStreamNode,
} from 'node:stream';
import {
  AgentSideConnection,
  PROTOCOL_VERSION,
  type Agent,
  type CancelNotification,
  type InitializeRequest,
  type InitializeResponse,
  type LoadSessionRequest,
  type LoadSessionResponse,
  type McpServer,
  type NewSessionRequest,
  type NewSessionResponse,
  type PromptRequest,
  type PromptResponse,
  type RequestPermissionRequest,
  type RequestPermissionResponse,
  type ResumeSessionRequest,
  type ResumeSessionResponse,
  type SessionConfigOption,
  type SessionModeState,
  type SessionModelState,
  type SessionUpdate,
  type SetSessionConfigOptionRequest,
  type SetSessionConfigOptionResponse,
  type SetSessionModeRequest,
  type SetSessionModeResponse,
} from '@agentclientprotocol/sdk';
import { ndJsonStream } from '@qwen-code/acp-bridge/ndJsonStream';

const CANOPY_SESSION_ID_META_KEY = 'qwen-code/sessionId';
const MAX_APP_SERVER_LINE_BYTES = 16 * 1024 * 1024;
const MAX_TOOL_OUTPUT_BYTES = 128 * 1024;
const MODE_OPTIONS = [
  { id: 'plan', name: 'Plan', description: 'Analyze without modifying files.' },
  {
    id: 'default',
    name: 'Default',
    description: 'Ask for approval when Codex needs it.',
  },
  {
    id: 'auto-edit',
    name: 'Auto Edit',
    description: 'Allow workspace edits and review untrusted commands.',
  },
  {
    id: 'auto',
    name: 'Auto',
    description: 'Use Codex untrusted-command approval policy.',
  },
  {
    id: 'yolo',
    name: 'YOLO',
    description: 'Run without sandboxing or approval prompts.',
  },
] as const;

type JsonObject = Record<string, unknown>;
type JsonRpcId = string | number | null;
type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
};
type CodexModel = {
  id: string;
  model: string;
  displayName: string;
  description: string;
  supportedReasoningEfforts: Array<{
    reasoningEffort: string;
    description: string;
  }>;
  defaultReasoningEffort: string;
};
type CanopyCodexSession = {
  sessionId: string;
  threadId: string;
  cwd: string;
  model: string;
  mode: (typeof MODE_OPTIONS)[number]['id'];
  reasoningEffort?: string;
};
type CanopyCodexSessionMapping = {
  threadId: string;
  model?: string;
  mode?: CanopyCodexSession['mode'];
  reasoningEffort?: string;
};
type ActiveTurn = {
  sessionId: string;
  threadId: string;
  turnId?: string;
  cancelRequested: boolean;
  resolve: (response: PromptResponse) => void;
};
type ActiveTool = {
  sessionId: string;
  title: string;
  kind: 'execute' | 'edit' | 'other' | 'fetch';
  rawInput: unknown;
  output: string;
};
type CodexChildProcess = ChildProcessByStdio<
  WritableStreamNode,
  ReadableStreamNode,
  null
>;

function isRecord(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function stringField(value: unknown, key: string): string | undefined {
  const field = isRecord(value) ? value[key] : undefined;
  return typeof field === 'string' ? field : undefined;
}

function formatError(error: unknown): Error {
  if (error instanceof Error) return error;
  return new Error(String(error));
}

function writeDiagnostic(message: string): void {
  process.stderr.write(`[canopy codex] ${message}\n`);
}

class CodexAppServer {
  private child?: CodexChildProcess;
  private nextRequestId = 1;
  private readonly pending = new Map<JsonRpcId, PendingRequest>();
  private readonly exitedPromise: Promise<void>;
  private resolveExited!: () => void;
  private lineBuffer = '';
  private stopping = false;
  private notificationHandler?: (method: string, params: JsonObject) => void;
  private serverRequestHandler?: (
    method: string,
    params: JsonObject,
  ) => Promise<unknown>;

  constructor(private readonly cwd: string) {
    this.exitedPromise = new Promise<void>((resolve) => {
      this.resolveExited = resolve;
    });
  }

  get exited(): Promise<void> {
    return this.exitedPromise;
  }

  onNotification(handler: (method: string, params: JsonObject) => void): void {
    this.notificationHandler = handler;
  }

  onServerRequest(
    handler: (method: string, params: JsonObject) => Promise<unknown>,
  ): void {
    this.serverRequestHandler = handler;
  }

  async start(): Promise<void> {
    const executable = process.env['CANOPY_CODEX_CLI_PATH']?.trim() || 'codex';
    let child: CodexChildProcess;
    try {
      child = spawn(executable, ['app-server'], {
        cwd: this.cwd,
        env: process.env,
        stdio: ['pipe', 'pipe', 'inherit'],
        windowsHide: true,
      });
    } catch (error) {
      throw new Error(
        `Could not start Codex CLI at ${JSON.stringify(executable)}: ${formatError(error).message}`,
      );
    }
    this.child = child;
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk: string) => this.readChunk(chunk));
    child.on('error', (error) => this.failPending(error));
    child.on('exit', (code, signal) => {
      this.resolveExited();
      if (!this.stopping) {
        this.failPending(
          new Error(
            `Codex app-server exited (code=${String(code)}, signal=${String(signal)})`,
          ),
        );
      }
    });
    await this.request('initialize', {
      clientInfo: {
        name: 'canopy',
        title: 'Canopy Code',
        version: process.env['CANOPY_CODE_VERSION'] ?? '0.21.11',
      },
      capabilities: null,
    });
    this.notify('initialized', {});
  }

  async request<T>(method: string, params: JsonObject): Promise<T> {
    if (this.pending.size >= 128) {
      throw new Error('Codex app-server request queue is full.');
    }
    const id = this.nextRequestId++;
    const promise = new Promise<unknown>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
    try {
      this.write({ id, method, params });
    } catch (error) {
      this.pending.delete(id);
      throw error;
    }
    return (await promise) as T;
  }

  notify(method: string, params: JsonObject): void {
    this.write({ method, params });
  }

  async stop(): Promise<void> {
    const child = this.child;
    if (!child || child.exitCode !== null || child.signalCode !== null) return;
    this.stopping = true;
    child.stdin.end();
    child.kill('SIGTERM');
    const timeout = setTimeout(() => child.kill('SIGKILL'), 1500);
    timeout.unref();
    await Promise.race([
      this.exitedPromise,
      new Promise<void>((resolve) => {
        const fallback = setTimeout(resolve, 2000);
        fallback.unref();
      }),
    ]);
    clearTimeout(timeout);
  }

  private write(message: JsonObject): void {
    const child = this.child;
    if (!child || child.stdin.destroyed) {
      throw new Error('Codex app-server is not available.');
    }
    child.stdin.write(`${JSON.stringify(message)}\n`, (error) => {
      if (error) this.failPending(error);
    });
  }

  private readChunk(chunk: string): void {
    this.lineBuffer += chunk;
    if (Buffer.byteLength(this.lineBuffer) > MAX_APP_SERVER_LINE_BYTES) {
      this.failPending(
        new Error('Codex app-server emitted an oversized JSONL message.'),
      );
      this.child?.kill('SIGKILL');
      return;
    }
    while (true) {
      const newline = this.lineBuffer.indexOf('\n');
      if (newline < 0) return;
      const line = this.lineBuffer.slice(0, newline).trim();
      this.lineBuffer = this.lineBuffer.slice(newline + 1);
      if (!line) continue;
      try {
        this.handleMessage(JSON.parse(line) as unknown);
      } catch (error) {
        writeDiagnostic(
          `ignoring invalid app-server JSONL: ${formatError(error).message}`,
        );
      }
    }
  }

  private handleMessage(value: unknown): void {
    if (!isRecord(value)) return;
    const method = stringField(value, 'method');
    if (method) {
      const params = isRecord(value['params']) ? value['params'] : {};
      if ('id' in value) {
        void this.handleServerRequest(value['id'] as JsonRpcId, method, params);
      } else {
        this.notificationHandler?.(method, params);
      }
      return;
    }
    if (!('id' in value)) return;
    const pending = this.pending.get(value['id'] as JsonRpcId);
    if (!pending) return;
    this.pending.delete(value['id'] as JsonRpcId);
    if ('error' in value && isRecord(value['error'])) {
      pending.reject(
        new Error(
          stringField(value['error'], 'message') ??
            'Codex app-server request failed.',
        ),
      );
    } else {
      pending.resolve(value['result']);
    }
  }

  private async handleServerRequest(
    id: JsonRpcId,
    method: string,
    params: JsonObject,
  ): Promise<void> {
    try {
      if (!this.serverRequestHandler) {
        throw new Error('Codex app-server request handler is not ready.');
      }
      const result = await this.serverRequestHandler(method, params);
      this.write({ id, result });
    } catch (error) {
      this.write({
        id,
        error: { code: -32000, message: formatError(error).message },
      });
    }
  }

  private failPending(error: Error): void {
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
  }
}

function getCanopyStorageDir(): string {
  const configured = process.env['QWEN_HOME']?.trim();
  if (!configured) return path.join(os.homedir(), '.canopy');
  if (configured === '~') return os.homedir();
  if (configured.startsWith('~/')) {
    return path.resolve(os.homedir(), configured.slice(2));
  }
  return path.resolve(configured);
}

function sessionMapPath(cwd: string, sessionId: string): string {
  const workspaceKey = createHash('sha256')
    .update(path.resolve(cwd))
    .digest('hex');
  const sessionKey = createHash('sha256').update(sessionId).digest('hex');
  return path.join(
    getCanopyStorageDir(),
    'codex-sessions',
    workspaceKey,
    `${sessionKey}.json`,
  );
}

async function writeSessionMapping(session: CanopyCodexSession): Promise<void> {
  const file = sessionMapPath(session.cwd, session.sessionId);
  await mkdir(path.dirname(file), { recursive: true, mode: 0o700 });
  const tempFile = `${file}.${randomUUID()}.tmp`;
  const payload = JSON.stringify({
    version: 1,
    cwd: path.resolve(session.cwd),
    sessionId: session.sessionId,
    threadId: session.threadId,
    model: session.model,
    mode: session.mode,
    ...(session.reasoningEffort
      ? { reasoningEffort: session.reasoningEffort }
      : {}),
  });
  await writeFile(tempFile, payload, {
    encoding: 'utf8',
    mode: 0o600,
    flag: 'wx',
  });
  await rename(tempFile, file);
}

async function readSessionMapping(
  cwd: string,
  sessionId: string,
): Promise<CanopyCodexSessionMapping | undefined> {
  try {
    const parsed: unknown = JSON.parse(
      await readFile(sessionMapPath(cwd, sessionId), 'utf8'),
    );
    if (
      isRecord(parsed) &&
      parsed['version'] === 1 &&
      parsed['cwd'] === path.resolve(cwd) &&
      parsed['sessionId'] === sessionId &&
      typeof parsed['threadId'] === 'string'
    ) {
      const mode = MODE_OPTIONS.find(({ id }) => id === parsed['mode'])?.id;
      return {
        threadId: parsed['threadId'],
        ...(typeof parsed['model'] === 'string'
          ? { model: parsed['model'] }
          : {}),
        ...(mode ? { mode } : {}),
        ...(typeof parsed['reasoningEffort'] === 'string'
          ? { reasoningEffort: parsed['reasoningEffort'] }
          : {}),
      };
    }
  } catch (error) {
    if (isRecord(error) && error['code'] !== 'ENOENT') throw error;
  }
  return undefined;
}

function codexMcpServers(servers: McpServer[]): JsonObject {
  const mapped: JsonObject = {};
  for (const server of servers) {
    if ('command' in server) {
      mapped[server.name] = {
        command: server.command,
        args: server.args,
        ...(server.env.length > 0
          ? {
              env_vars: Object.fromEntries(
                server.env.map(({ name, value }) => [name, value]),
              ),
            }
          : {}),
      };
    } else {
      mapped[server.name] = {
        url: server.url,
        ...(server.headers.length > 0
          ? {
              http_headers: Object.fromEntries(
                server.headers.map(({ name, value }) => [name, value]),
              ),
            }
          : {}),
      };
    }
  }
  return mapped;
}

function approvalPolicy(mode: CanopyCodexSession['mode']): string {
  switch (mode) {
    case 'plan':
    case 'yolo':
      return 'never';
    case 'auto-edit':
    case 'auto':
      return 'untrusted';
    default:
      return 'on-request';
  }
}

function sandboxMode(mode: CanopyCodexSession['mode']): string {
  if (mode === 'plan') return 'read-only';
  if (mode === 'yolo') return 'danger-full-access';
  return 'workspace-write';
}

function sandboxPolicy(
  mode: CanopyCodexSession['mode'],
  cwd: string,
): JsonObject {
  if (mode === 'plan') {
    return { type: 'readOnly', networkAccess: true };
  }
  if (mode === 'yolo') return { type: 'dangerFullAccess' };
  const workspaceWrite = {
    type: 'workspaceWrite',
    writableRoots: [path.resolve(cwd)],
    networkAccess: true,
    excludeTmpdirEnvVar: false,
    excludeSlashTmp: false,
  };
  return workspaceWrite;
}

function modelInfo(value: unknown): CodexModel | undefined {
  if (!isRecord(value)) return undefined;
  const id = stringField(value, 'id');
  const model = stringField(value, 'model');
  const displayName = stringField(value, 'displayName');
  if (!id || !model || !displayName) return undefined;
  const rawEfforts = Array.isArray(value['supportedReasoningEfforts'])
    ? value['supportedReasoningEfforts']
    : [];
  const supportedReasoningEfforts = rawEfforts.flatMap((effort) => {
    const reasoningEffort = stringField(effort, 'reasoningEffort');
    if (!reasoningEffort) return [];
    return [
      {
        reasoningEffort,
        description: stringField(effort, 'description') ?? '',
      },
    ];
  });
  return {
    id,
    model,
    displayName,
    description: stringField(value, 'description') ?? '',
    supportedReasoningEfforts,
    defaultReasoningEffort: stringField(value, 'defaultReasoningEffort') ?? '',
  };
}

function statusToToolCall(
  value: unknown,
): 'completed' | 'failed' | 'in_progress' {
  if (value === 'completed') return 'completed';
  if (value === 'failed' || value === 'declined') return 'failed';
  return 'in_progress';
}

function textInput(text: string): JsonObject {
  return { type: 'text', text, text_elements: [] };
}

function toCodexInput(request: PromptRequest): JsonObject[] {
  const input: JsonObject[] = [];
  for (const block of request.prompt) {
    if (block.type === 'text') {
      input.push(textInput(block.text));
    } else if (block.type === 'image') {
      input.push({
        type: 'image',
        url: `data:${block.mimeType};base64,${block.data}`,
      });
    } else if (block.type === 'resource_link') {
      input.push(textInput(`${block.title ?? block.name}: ${block.uri}`));
    } else if (block.type === 'resource') {
      const resource = block.resource;
      if ('text' in resource && typeof resource.text === 'string') {
        input.push(textInput(resource.text));
      } else {
        input.push(textInput(`Resource: ${resource.uri}`));
      }
    } else {
      throw new Error(
        `Codex runtime does not support ACP ${block.type} input yet.`,
      );
    }
  }
  if (input.length === 0) input.push(textInput(''));
  return input;
}

function threadItems(value: unknown): JsonObject[] {
  if (!isRecord(value) || !Array.isArray(value['turns'])) return [];
  return value['turns'].flatMap((turn) =>
    isRecord(turn) && Array.isArray(turn['items'])
      ? turn['items'].filter(isRecord)
      : [],
  );
}

class CodexAcpAgent implements Agent {
  private readonly sessions = new Map<string, CanopyCodexSession>();
  private readonly activeTurns = new Map<string, ActiveTurn>();
  private readonly activeTools = new Map<string, ActiveTool>();
  private notificationQueue = Promise.resolve();
  private modelCatalogPromise?: Promise<CodexModel[]>;

  constructor(
    private readonly appServer: CodexAppServer,
    private readonly connection: AgentSideConnection,
    private readonly defaultModel?: string,
  ) {
    appServer.onNotification((method, params) => {
      this.notificationQueue = this.notificationQueue.then(async () => {
        try {
          await this.onAppServerNotification(method, params);
        } catch (error) {
          writeDiagnostic(
            `event forwarding failed: ${formatError(error).message}`,
          );
        }
      });
    });
    appServer.onServerRequest((method, params) =>
      this.onAppServerRequest(method, params),
    );
  }

  async initialize(_params: InitializeRequest): Promise<InitializeResponse> {
    return {
      protocolVersion: PROTOCOL_VERSION,
      agentInfo: {
        name: 'Canopy Codex Runtime',
        title: 'Canopy Code',
        version: process.env['CANOPY_CODE_VERSION'] ?? '0.21.11',
      },
      agentCapabilities: {
        loadSession: true,
        sessionCapabilities: { resume: {} },
        promptCapabilities: { image: true, embeddedContext: true },
      },
    };
  }

  async newSession(params: NewSessionRequest): Promise<NewSessionResponse> {
    const cwd = path.resolve(params.cwd);
    const mode: CanopyCodexSession['mode'] = 'default';
    const startParams: JsonObject = {
      cwd,
      approvalPolicy: approvalPolicy(mode),
      sandbox: sandboxMode(mode),
      ...(this.defaultModel ? { model: this.defaultModel } : {}),
    };
    const mcpServers = codexMcpServers(params.mcpServers);
    if (Object.keys(mcpServers).length > 0) {
      startParams['config'] = { mcp_servers: mcpServers };
    }
    const response = await this.appServer.request<unknown>(
      'thread/start',
      startParams,
    );
    const thread = isRecord(response) ? response['thread'] : undefined;
    const threadId = stringField(thread, 'id');
    if (!threadId)
      throw new Error('Codex app-server did not return a thread id.');

    const requestedId = params._meta?.[CANOPY_SESSION_ID_META_KEY];
    if (
      typeof requestedId === 'string' &&
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
        requestedId,
      )
    ) {
      throw new Error('Canopy requested an invalid ACP session ID.');
    }
    const sessionId =
      typeof requestedId === 'string' ? requestedId : randomUUID();
    const session: CanopyCodexSession = {
      sessionId,
      threadId,
      cwd,
      model: stringField(response, 'model') ?? this.defaultModel ?? '',
      mode,
    };
    await writeSessionMapping(session);
    this.sessions.set(sessionId, session);
    const models = await this.getModels();
    return {
      sessionId,
      modes: this.modeState(session),
      models: this.modelState(session, models),
      configOptions: this.configOptions(session, models),
    };
  }

  async loadSession(params: LoadSessionRequest): Promise<LoadSessionResponse> {
    const session = await this.resume(params.sessionId, params.cwd);
    const response = await this.appServer.request<unknown>(
      'thread/resume',
      this.resumeParams(session),
    );
    session.model = stringField(response, 'model') ?? session.model;
    await writeSessionMapping(session);
    const thread = isRecord(response) ? response['thread'] : undefined;
    for (const item of threadItems(thread)) {
      await this.replayItem(session.sessionId, item);
    }
    const models = await this.getModels();
    return {
      modes: this.modeState(session),
      models: this.modelState(session, models),
      configOptions: this.configOptions(session, models),
    };
  }

  async authenticate(): Promise<void> {}

  async unstable_resumeSession(
    params: ResumeSessionRequest,
  ): Promise<ResumeSessionResponse> {
    const session = await this.resume(params.sessionId, params.cwd);
    const response = await this.appServer.request<unknown>(
      'thread/resume',
      this.resumeParams(session),
    );
    session.model = stringField(response, 'model') ?? session.model;
    await writeSessionMapping(session);
    return {
      modes: this.modeState(session),
      models: this.modelState(session, await this.getModels()),
      configOptions: this.configOptions(session, await this.getModels()),
    };
  }

  async setSessionMode(
    params: SetSessionModeRequest,
  ): Promise<SetSessionModeResponse> {
    const session = this.requireSession(params.sessionId);
    if (!MODE_OPTIONS.some((mode) => mode.id === params.modeId)) {
      throw new Error(`Unsupported Canopy approval mode: ${params.modeId}`);
    }
    const mode = params.modeId as CanopyCodexSession['mode'];
    await this.appServer.request('thread/settings/update', {
      threadId: session.threadId,
      approvalPolicy: approvalPolicy(mode),
      sandboxPolicy: sandboxPolicy(mode, session.cwd),
    });
    session.mode = mode;
    await writeSessionMapping(session);
    await this.sendUpdate(session.sessionId, {
      sessionUpdate: 'current_mode_update',
      currentModeId: mode,
    });
    return {};
  }

  async setSessionConfigOption(
    params: SetSessionConfigOptionRequest,
  ): Promise<SetSessionConfigOptionResponse> {
    const session = this.requireSession(params.sessionId);
    const models = await this.getModels();
    if (params.configId === 'mode') {
      await this.setSessionMode({
        sessionId: session.sessionId,
        modeId: params.value,
      });
    } else if (params.configId === 'model') {
      await this.setModel(session, params.value, models);
    } else if (params.configId === 'reasoning_effort') {
      await this.appServer.request('thread/settings/update', {
        threadId: session.threadId,
        effort: params.value,
      });
      session.reasoningEffort = params.value;
      await writeSessionMapping(session);
    } else {
      throw new Error(`Unsupported Codex session option: ${params.configId}`);
    }
    return { configOptions: this.configOptions(session, models) };
  }

  async unstable_setSessionModel(params: {
    sessionId: string;
    modelId: string;
  }): Promise<void> {
    const session = this.requireSession(params.sessionId);
    await this.setModel(session, params.modelId, await this.getModels());
  }

  async prompt(params: PromptRequest): Promise<PromptResponse> {
    const session = this.requireSession(params.sessionId);
    if (this.activeTurns.has(session.threadId)) {
      throw new Error('Codex already has an active turn for this session.');
    }
    const completion = new Promise<PromptResponse>((resolve) => {
      this.activeTurns.set(session.threadId, {
        sessionId: session.sessionId,
        threadId: session.threadId,
        cancelRequested: false,
        resolve,
      });
    });
    const active = this.activeTurns.get(session.threadId)!;
    try {
      const response = await this.appServer.request<unknown>('turn/start', {
        threadId: session.threadId,
        cwd: session.cwd,
        input: toCodexInput(params),
        ...(session.model ? { model: session.model } : {}),
        ...(session.reasoningEffort ? { effort: session.reasoningEffort } : {}),
      });
      const turn = isRecord(response) ? response['turn'] : undefined;
      active.turnId = stringField(turn, 'id');
      if (active.cancelRequested && active.turnId) {
        await this.appServer.request('turn/interrupt', {
          threadId: session.threadId,
          turnId: active.turnId,
        });
      }
      return await completion;
    } finally {
      this.activeTurns.delete(session.threadId);
    }
  }

  async cancel(params: CancelNotification): Promise<void> {
    const session = this.sessions.get(params.sessionId);
    if (!session) return;
    const active = this.activeTurns.get(session.threadId);
    if (!active) return;
    active.cancelRequested = true;
    if (active.turnId) {
      await this.appServer.request('turn/interrupt', {
        threadId: session.threadId,
        turnId: active.turnId,
      });
    }
  }

  private async resume(
    sessionId: string,
    cwd: string,
  ): Promise<CanopyCodexSession> {
    const existing = this.sessions.get(sessionId);
    if (existing) return existing;
    const resolvedCwd = path.resolve(cwd);
    const mapping = await readSessionMapping(resolvedCwd, sessionId);
    const session: CanopyCodexSession = {
      sessionId,
      threadId: mapping?.threadId ?? sessionId,
      cwd: resolvedCwd,
      model: mapping?.model ?? '',
      mode: mapping?.mode ?? 'default',
      ...(mapping?.reasoningEffort
        ? { reasoningEffort: mapping.reasoningEffort }
        : {}),
    };
    this.sessions.set(sessionId, session);
    return session;
  }

  private resumeParams(session: CanopyCodexSession): JsonObject {
    return {
      threadId: session.threadId,
      cwd: session.cwd,
      approvalPolicy: approvalPolicy(session.mode),
      sandbox: sandboxMode(session.mode),
    };
  }

  private requireSession(sessionId: string): CanopyCodexSession {
    const session = this.sessions.get(sessionId);
    if (!session) throw new Error(`Codex session ${sessionId} is not loaded.`);
    return session;
  }

  private async setModel(
    session: CanopyCodexSession,
    modelId: string,
    models: CodexModel[],
  ): Promise<void> {
    const model = models.find(
      (candidate) => candidate.id === modelId || candidate.model === modelId,
    );
    const codexModel = model?.model ?? modelId;
    await this.appServer.request('thread/settings/update', {
      threadId: session.threadId,
      model: codexModel,
    });
    session.model = codexModel;
    await writeSessionMapping(session);
  }

  private async getModels(): Promise<CodexModel[]> {
    this.modelCatalogPromise ??= this.appServer
      .request<unknown>('model/list', { includeHidden: false })
      .then((result) => {
        if (!isRecord(result) || !Array.isArray(result['data'])) return [];
        return result['data'].flatMap((value) => {
          const parsed = modelInfo(value);
          return parsed ? [parsed] : [];
        });
      })
      .catch((error: unknown) => {
        this.modelCatalogPromise = undefined;
        writeDiagnostic(
          `model list unavailable: ${formatError(error).message}`,
        );
        return [];
      });
    return await this.modelCatalogPromise;
  }

  private modeState(session: CanopyCodexSession): SessionModeState {
    return {
      currentModeId: session.mode,
      availableModes: MODE_OPTIONS.map(({ id, name, description }) => ({
        id,
        name,
        description,
      })),
    };
  }

  private modelState(
    session: CanopyCodexSession,
    models: CodexModel[],
  ): SessionModelState {
    const current = models.find(
      (model) => model.model === session.model || model.id === session.model,
    );
    return {
      currentModelId: current?.id ?? session.model,
      availableModels: models.map((model) => ({
        modelId: model.id,
        name: model.displayName,
        description: model.description,
      })),
    };
  }

  private configOptions(
    session: CanopyCodexSession,
    models: CodexModel[],
  ): SessionConfigOption[] {
    const current = models.find(
      (model) => model.model === session.model || model.id === session.model,
    );
    const options: SessionConfigOption[] = [
      {
        id: 'mode',
        name: 'Mode',
        category: 'mode',
        type: 'select',
        currentValue: session.mode,
        options: MODE_OPTIONS.map(({ id, name, description }) => ({
          value: id,
          name,
          description,
        })),
      },
    ];
    if (models.length > 0) {
      options.push({
        id: 'model',
        name: 'Model',
        category: 'model',
        type: 'select',
        currentValue: current?.id ?? session.model,
        options: models.map((model) => ({
          value: model.id,
          name: model.displayName,
          description: model.description,
        })),
      });
    }
    if (current?.supportedReasoningEfforts.length) {
      options.push({
        id: 'reasoning_effort',
        name: 'Reasoning effort',
        category: 'thought_level',
        type: 'select',
        currentValue: session.reasoningEffort ?? current.defaultReasoningEffort,
        options: current.supportedReasoningEfforts.map((effort) => ({
          value: effort.reasoningEffort,
          name: effort.reasoningEffort,
          description: effort.description,
        })),
      });
    }
    return options;
  }

  private async sendUpdate(
    sessionId: string,
    update: SessionUpdate,
  ): Promise<void> {
    await this.connection.sessionUpdate({ sessionId, update });
  }

  private async replayItem(sessionId: string, item: JsonObject): Promise<void> {
    const type = stringField(item, 'type');
    if (type === 'userMessage' && Array.isArray(item['content'])) {
      for (const content of item['content']) {
        if (isRecord(content) && content['type'] === 'text') {
          const text = stringField(content, 'text');
          if (text) {
            await this.sendUpdate(sessionId, {
              sessionUpdate: 'user_message_chunk',
              content: { type: 'text', text },
            });
          }
        }
      }
    } else if (type === 'agentMessage') {
      const text = stringField(item, 'text');
      if (text) {
        await this.sendUpdate(sessionId, {
          sessionUpdate: 'agent_message_chunk',
          content: { type: 'text', text },
        });
      }
    }
  }

  private async onAppServerNotification(
    method: string,
    params: JsonObject,
  ): Promise<void> {
    if (method === 'item/agentMessage/delta') {
      const session = this.sessionForThread(params['threadId']);
      const delta = stringField(params, 'delta');
      if (session && delta) {
        await this.sendUpdate(session.sessionId, {
          sessionUpdate: 'agent_message_chunk',
          content: { type: 'text', text: delta },
        });
      }
      return;
    }
    if (method === 'item/started') {
      const session = this.sessionForThread(params['threadId']);
      const item = isRecord(params['item']) ? params['item'] : undefined;
      if (!session || !item) return;
      const tool = this.makeTool(session, item);
      if (!tool) return;
      const toolCallId = stringField(item, 'id');
      if (!toolCallId) return;
      this.activeTools.set(toolCallId, tool);
      await this.sendUpdate(session.sessionId, {
        sessionUpdate: 'tool_call',
        toolCallId,
        title: tool.title,
        kind: tool.kind,
        status: 'in_progress',
        rawInput: tool.rawInput,
      });
      return;
    }
    if (method === 'item/commandExecution/outputDelta') {
      const itemId = stringField(params, 'itemId');
      const delta = stringField(params, 'delta');
      const tool = itemId ? this.activeTools.get(itemId) : undefined;
      if (tool && delta) {
        tool.output = `${tool.output}${delta}`.slice(-MAX_TOOL_OUTPUT_BYTES);
        await this.sendUpdate(tool.sessionId, {
          sessionUpdate: 'tool_call_update',
          toolCallId: itemId!,
          rawOutput: tool.output,
        });
      }
      return;
    }
    if (method === 'item/completed') {
      const session = this.sessionForThread(params['threadId']);
      const item = isRecord(params['item']) ? params['item'] : undefined;
      const itemId = item ? stringField(item, 'id') : undefined;
      const tool = itemId ? this.activeTools.get(itemId) : undefined;
      if (!session || !item || !itemId || !tool) return;
      const itemType = stringField(item, 'type');
      const status = statusToToolCall(item['status']);
      const rawOutput =
        stringField(item, 'aggregatedOutput') ?? tool.output ?? undefined;
      await this.sendUpdate(session.sessionId, {
        sessionUpdate: 'tool_call_update',
        toolCallId: itemId,
        status,
        ...(rawOutput
          ? { rawOutput: rawOutput.slice(-MAX_TOOL_OUTPUT_BYTES) }
          : {}),
        ...(itemType === 'commandExecution' &&
        typeof item['exitCode'] === 'number'
          ? { title: `${tool.title} (exit ${String(item['exitCode'])})` }
          : {}),
      });
      this.activeTools.delete(itemId);
      return;
    }
    if (method === 'turn/completed') {
      const threadId = stringField(params, 'threadId');
      const turn = isRecord(params['turn']) ? params['turn'] : undefined;
      const active = threadId ? this.activeTurns.get(threadId) : undefined;
      if (!active || !turn) return;
      const status = stringField(turn, 'status');
      if (status === 'failed') {
        const error = isRecord(turn['error'])
          ? stringField(turn['error'], 'message')
          : undefined;
        if (error) {
          await this.sendUpdate(active.sessionId, {
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: `Codex error: ${error}` },
          });
        }
      }
      active.resolve({
        stopReason: status === 'interrupted' ? 'cancelled' : 'end_turn',
      });
    }
  }

  private sessionForThread(threadId: unknown): CanopyCodexSession | undefined {
    if (typeof threadId !== 'string') return undefined;
    return [...this.sessions.values()].find(
      (session) => session.threadId === threadId,
    );
  }

  private makeTool(
    session: CanopyCodexSession,
    item: JsonObject,
  ): ActiveTool | undefined {
    const type = stringField(item, 'type');
    if (type === 'commandExecution') {
      const command = stringField(item, 'command') ?? 'Run command';
      return {
        sessionId: session.sessionId,
        title: command.slice(0, 200),
        kind: 'execute',
        rawInput: { command, cwd: stringField(item, 'cwd') ?? session.cwd },
        output: stringField(item, 'aggregatedOutput') ?? '',
      };
    }
    if (type === 'fileChange') {
      return {
        sessionId: session.sessionId,
        title: 'Update files',
        kind: 'edit',
        rawInput: item['changes'] ?? [],
        output: '',
      };
    }
    if (type === 'mcpToolCall') {
      return {
        sessionId: session.sessionId,
        title: `${stringField(item, 'server') ?? 'MCP'}: ${stringField(item, 'tool') ?? 'tool'}`,
        kind: 'other',
        rawInput: item['arguments'] ?? {},
        output: '',
      };
    }
    if (type === 'webSearch') {
      return {
        sessionId: session.sessionId,
        title: 'Web search',
        kind: 'fetch',
        rawInput: item,
        output: '',
      };
    }
    return undefined;
  }

  private async onAppServerRequest(
    method: string,
    params: JsonObject,
  ): Promise<unknown> {
    if (
      method !== 'item/commandExecution/requestApproval' &&
      method !== 'item/fileChange/requestApproval'
    ) {
      throw new Error(`Unsupported Codex approval request: ${method}`);
    }
    const threadId = stringField(params, 'threadId');
    const itemId = stringField(params, 'itemId');
    const session = threadId ? this.sessionForThread(threadId) : undefined;
    if (!session || !itemId) return { decision: 'cancel' };
    const existingTool = this.activeTools.get(itemId);
    const command = stringField(params, 'command');
    const tool: ActiveTool = existingTool ?? {
      sessionId: session.sessionId,
      title:
        command?.slice(0, 200) ??
        (method === 'item/fileChange/requestApproval'
          ? 'Update files'
          : 'Run command'),
      kind: method === 'item/fileChange/requestApproval' ? 'edit' : 'execute',
      rawInput:
        method === 'item/fileChange/requestApproval'
          ? { reason: stringField(params, 'reason') ?? '' }
          : {
              command: command ?? '',
              cwd: stringField(params, 'cwd') ?? session.cwd,
            },
      output: '',
    };
    if (!existingTool) {
      await this.sendUpdate(session.sessionId, {
        sessionUpdate: 'tool_call',
        toolCallId: itemId,
        title: tool.title,
        kind: tool.kind,
        status: 'pending',
        rawInput: tool.rawInput,
      });
    }
    const available = Array.isArray(params['availableDecisions'])
      ? params['availableDecisions']
      : ['accept', 'acceptForSession', 'decline', 'cancel'];
    const options = available.flatMap(
      (decision): RequestPermissionRequest['options'] => {
        if (decision === 'accept') {
          return [
            { optionId: 'accept', name: 'Allow once', kind: 'allow_once' },
          ];
        }
        if (decision === 'acceptForSession') {
          return [
            {
              optionId: 'acceptForSession',
              name: 'Allow for this session',
              kind: 'allow_always',
            },
          ];
        }
        if (decision === 'decline') {
          return [{ optionId: 'decline', name: 'Reject', kind: 'reject_once' }];
        }
        if (decision === 'cancel') {
          return [
            { optionId: 'cancel', name: 'Cancel turn', kind: 'reject_always' },
          ];
        }
        return [];
      },
    );
    if (options.length === 0) {
      options.push({
        optionId: 'decline',
        name: 'Reject',
        kind: 'reject_once',
      });
    }
    const response: RequestPermissionResponse =
      await this.connection.requestPermission({
        sessionId: session.sessionId,
        toolCall: {
          toolCallId: itemId,
          title: tool.title,
          kind: tool.kind,
          status: 'pending',
          rawInput: tool.rawInput,
        },
        options,
      });
    if (response.outcome.outcome === 'cancelled') return { decision: 'cancel' };
    switch (response.outcome.optionId) {
      case 'accept':
        return { decision: 'accept' };
      case 'acceptForSession':
        return { decision: 'acceptForSession' };
      case 'decline':
      case 'cancel':
      default:
        return {
          decision:
            response.outcome.optionId === 'cancel' ? 'cancel' : 'decline',
        };
    }
  }
}

export async function runCodexAcpAgent(model?: string): Promise<void> {
  const appServer = new CodexAppServer(process.cwd());
  try {
    await appServer.start();
  } catch (error) {
    await appServer.stop();
    throw error;
  }
  const stdout = Writable.toWeb(process.stdout) as WritableStream<Uint8Array>;
  const stdin = Readable.toWeb(process.stdin) as ReadableStream<Uint8Array>;
  const stream = ndJsonStream(stdout, stdin);
  let agent: CodexAcpAgent | undefined;
  const connection = new AgentSideConnection((client) => {
    agent = new CodexAcpAgent(appServer, client, model);
    return agent;
  }, stream);
  const shutdown = new Promise<void>((resolve) => {
    process.once('SIGTERM', resolve);
    process.once('SIGINT', resolve);
  });
  try {
    const outcome = await Promise.race([
      connection.closed.then(() => 'acp-closed' as const),
      appServer.exited.then(() => 'codex-exited' as const),
      shutdown.then(() => 'shutdown' as const),
    ]);
    if (outcome === 'codex-exited') {
      writeDiagnostic(
        'Codex app-server stopped unexpectedly; closing the ACP channel.',
      );
      process.exitCode = 1;
    }
  } finally {
    await appServer.stop();
    process.stdin.destroy();
    process.stdout.end();
    await connection.closed.catch(() => {});
    void agent;
  }
}
