/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import fsPromises from 'node:fs/promises';
import type { Dirent } from 'node:fs';
import type { FileHandle } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import type { ToolInvocation, ToolResult } from './tools.js';
import { BaseDeclarativeTool, BaseToolInvocation, Kind } from './tools.js';
import { ToolNames, ToolDisplayNames } from './tool-names.js';
import type { Config } from '../config/config.js';
import type { PermissionDecision } from '../permissions/types.js';
import {
  isSubpath,
  resolveAndValidatePath,
  resolvePath,
} from '../utils/paths.js';
import { getMemoryBaseDir } from '../memory/paths.js';
import { getErrorMessage } from '../utils/errors.js';
import { ToolErrorType } from './tool-error.js';

const DEFAULT_MAX_RESULTS = 12;
const MAX_RESULTS = 24;
const MAX_FILES_SCANNED = 6_000;
const MAX_DIRECTORIES_SCANNED = 1_500;
const MAX_CONTENT_FILES = 320;
const MAX_CONTENT_BYTES = 128 * 1024;
const MAX_SCAN_TIME_MS = 2_500;
const MAX_OUTPUT_CHARS = 12_000;

const TEXT_EXTENSIONS = new Set([
  '.c',
  '.cc',
  '.cfg',
  '.conf',
  '.css',
  '.csv',
  '.env',
  '.go',
  '.h',
  '.hpp',
  '.html',
  '.ini',
  '.java',
  '.js',
  '.json',
  '.jsx',
  '.log',
  '.md',
  '.markdown',
  '.mjs',
  '.py',
  '.rb',
  '.rs',
  '.rtf',
  '.sh',
  '.sql',
  '.swift',
  '.toml',
  '.ts',
  '.tsx',
  '.txt',
  '.xml',
  '.yaml',
  '.yml',
  '.zsh',
]);

const QUERY_STOPWORDS = new Set([
  'a',
  'an',
  'and',
  'computer',
  'document',
  'file',
  'find',
  'for',
  'from',
  'in',
  'look',
  'my',
  'of',
  'on',
  'please',
  'search',
  'the',
  'this',
  'to',
]);

const ALWAYS_PRUNED_DIRECTORIES = new Set([
  '.git',
  '.hg',
  '.svn',
  'node_modules',
  '__pycache__',
  '.venv',
  'Pods',
  'DerivedData',
  'vendor',
  'target',
  'coverage',
]);

const DEFAULT_PRUNED_DIRECTORIES = new Set([
  ...ALWAYS_PRUNED_DIRECTORIES,
  '.cache',
  '.npm',
  '.pnpm-store',
  'Library',
  'Caches',
  '.Trash',
]);

const HOME_FALLBACK_ROOTS = new Set([
  'Downloads',
  'Desktop',
  'Documents',
  'Projects',
  'repos',
]);

export interface SuperSearchParams {
  /** Natural-language file/name query, for example "my resume". */
  query: string;
  /** Optional directory to search instead of the curated user-facing roots. */
  path?: string;
  /** Maximum number of ranked candidates to return. */
  max_results?: number;
  /** Inspect small text files for the query when the name is not enough. */
  include_content?: boolean;
}

export interface SuperSearchRoot {
  path: string;
  label: string;
  priority: number;
  maxDepth: number;
  pruneJunk: boolean;
}

interface ScannedFile {
  fullPath: string;
  root: SuperSearchRoot;
  depth: number;
  filenameScore: number;
  textLike: boolean;
}

interface Candidate {
  fullPath: string;
  rootLabel: string;
  score: number;
  matchedBy: string[];
  snippet?: string;
  sizeBytes?: number;
  mtimeMs?: number;
}

interface SearchStats {
  directories: number;
  files: number;
  truncated: boolean;
}

function normalizeForMatch(value: string): string {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
}

function tokenizeQuery(query: string): string[] {
  const tokens = normalizeForMatch(query)
    .split(/[^a-z0-9]+/)
    .filter((token) => token.length >= 2 && !QUERY_STOPWORDS.has(token));
  if (tokens.length > 0) return [...new Set(tokens)];
  return normalizeForMatch(query)
    .split(/[^a-z0-9]+/)
    .filter((token) => token.length >= 2);
}

function compactMatch(value: string): string {
  return normalizeForMatch(value).replace(/[^a-z0-9]+/g, '');
}

function isTextLike(filePath: string): boolean {
  const extension = path.extname(filePath).toLowerCase();
  return TEXT_EXTENSIONS.has(extension) || extension === '';
}

function scoreFilename(
  filePath: string,
  query: string,
  tokens: readonly string[],
): number {
  const baseName = normalizeForMatch(path.basename(filePath));
  const stem = normalizeForMatch(
    path.basename(filePath, path.extname(filePath)),
  );
  const compactBase = compactMatch(stem);
  const compactQuery = compactMatch(query);
  const tokenHits = tokens.filter((token) => stem.includes(token));
  let score = 0;

  if (compactQuery.length > 0 && compactBase === compactQuery) score += 240;
  else if (compactQuery.length > 0 && compactBase.includes(compactQuery)) {
    score += 165;
  }
  if (tokens.length > 0 && tokenHits.length === tokens.length) score += 115;
  score += tokenHits.length * 28;

  // A resume/CV is a common open-ended discovery request. This small bonus
  // helps a generic query such as "my resume" beat unrelated text files.
  if (
    tokenHits.some((token) =>
      ['resume', 'cv', 'curriculum', 'portfolio'].includes(token),
    )
  ) {
    score += 18;
  }
  if (baseName.endsWith('.md') || baseName.endsWith('.pdf')) score += 4;
  return score;
}

function scoreContent(
  content: string,
  query: string,
  tokens: readonly string[],
) {
  const normalized = normalizeForMatch(content);
  const phrase = normalizeForMatch(query).trim();
  if (phrase.length > 2 && normalized.includes(phrase)) return 100;
  const tokenHits = tokens.filter((token) => normalized.includes(token));
  if (tokenHits.length === 0) return 0;
  const requiredHits = Math.min(2, Math.max(1, tokens.length));
  return tokenHits.length >= requiredHits ? 55 + tokenHits.length * 8 : 0;
}

function makeSnippet(
  content: string,
  tokens: readonly string[],
): string | undefined {
  const normalized = normalizeForMatch(content);
  const hit = tokens
    .map((token) => normalized.indexOf(token))
    .filter((index) => index >= 0)
    .sort((a, b) => a - b)[0];
  if (hit === undefined) return undefined;

  const start = Math.max(0, hit - 90);
  const end = Math.min(content.length, hit + 190);
  const snippet = content
    .slice(start, end)
    .replace(/\0/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!snippet) return undefined;
  return `${start > 0 ? '…' : ''}${snippet}${end < content.length ? '…' : ''}`;
}

function shouldPruneDirectory(name: string, pruneJunk: boolean): boolean {
  return (
    pruneJunk ? DEFAULT_PRUNED_DIRECTORIES : ALWAYS_PRUNED_DIRECTORIES
  ).has(name);
}

function dedupeRoots(roots: SuperSearchRoot[]): SuperSearchRoot[] {
  const seen = new Set<string>();
  return roots.filter((root) => {
    const key =
      process.platform === 'darwin' ? root.path.toLowerCase() : root.path;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/** Build the bounded default search roots used by open-ended file discovery. */
export function buildDefaultSuperSearchRoots(
  homeDirectory: string,
  workspaceDirectories: readonly string[],
): SuperSearchRoot[] {
  const userRoots: SuperSearchRoot[] = [
    {
      path: path.join(homeDirectory, 'Downloads'),
      label: 'Downloads',
      priority: 100,
      maxDepth: 8,
      pruneJunk: true,
    },
    {
      path: path.join(homeDirectory, '.claude', 'downloads'),
      label: 'Claude downloads',
      priority: 96,
      maxDepth: 8,
      pruneJunk: true,
    },
    {
      path: path.join(homeDirectory, 'Desktop'),
      label: 'Desktop',
      priority: 92,
      maxDepth: 8,
      pruneJunk: true,
    },
    {
      path: path.join(homeDirectory, 'Documents'),
      label: 'Documents',
      priority: 88,
      maxDepth: 10,
      pruneJunk: true,
    },
    {
      path: path.join(homeDirectory, 'Projects'),
      label: 'Projects',
      priority: 74,
      maxDepth: 10,
      pruneJunk: true,
    },
    {
      path: path.join(homeDirectory, 'repos'),
      label: 'repos',
      priority: 72,
      maxDepth: 10,
      pruneJunk: true,
    },
    {
      path: homeDirectory,
      label: 'Home folder (shallow)',
      priority: 24,
      maxDepth: 1,
      pruneJunk: true,
    },
  ];
  const workspaces = workspaceDirectories.map((workspacePath, index) => ({
    path: workspacePath,
    label: `Workspace${workspaceDirectories.length > 1 ? ` ${index + 1}` : ''}`,
    priority: 80,
    maxDepth: 12,
    pruneJunk: true,
  }));
  return dedupeRoots([
    ...userRoots.slice(0, -1),
    ...workspaces,
    userRoots.at(-1)!,
  ]);
}

async function readFileHead(filePath: string): Promise<string | undefined> {
  let handle: FileHandle | undefined;
  try {
    handle = await fsPromises.open(filePath, 'r');
    const buffer = Buffer.allocUnsafe(MAX_CONTENT_BYTES);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    const head = buffer.subarray(0, bytesRead);
    if (head.includes(0)) return undefined;
    return head.toString('utf8');
  } catch {
    return undefined;
  } finally {
    await handle?.close().catch(() => undefined);
  }
}

async function statFile(filePath: string): Promise<
  | {
      sizeBytes: number;
      mtimeMs: number;
    }
  | undefined
> {
  try {
    const stat = await fsPromises.stat(filePath);
    if (!stat.isFile()) return undefined;
    return { sizeBytes: stat.size, mtimeMs: stat.mtimeMs };
  } catch {
    return undefined;
  }
}

async function scanRoot(
  root: SuperSearchRoot,
  query: string,
  tokens: readonly string[],
  signal: AbortSignal,
  startedAt: number,
  stats: SearchStats,
): Promise<ScannedFile[]> {
  const files: ScannedFile[] = [];
  const pending: Array<{ directory: string; depth: number }> = [
    { directory: root.path, depth: 0 },
  ];

  while (pending.length > 0) {
    if (signal.aborted) break;
    if (
      Date.now() - startedAt >= MAX_SCAN_TIME_MS ||
      stats.files >= MAX_FILES_SCANNED ||
      stats.directories >= MAX_DIRECTORIES_SCANNED
    ) {
      stats.truncated = true;
      break;
    }

    const current = pending.pop()!;
    let directory: AsyncIterable<Dirent>;
    try {
      directory = await fsPromises.opendir(current.directory);
    } catch {
      continue;
    }
    stats.directories += 1;

    try {
      for await (const entry of directory) {
        if (signal.aborted) break;
        const fullPath = path.join(current.directory, entry.name);
        if (entry.isSymbolicLink()) continue;

        if (entry.isDirectory()) {
          if (
            root.label === 'Home folder (shallow)' &&
            current.depth === 0 &&
            HOME_FALLBACK_ROOTS.has(entry.name)
          ) {
            continue;
          }
          if (
            current.depth < root.maxDepth &&
            !shouldPruneDirectory(entry.name, root.pruneJunk)
          ) {
            if (pending.length < MAX_DIRECTORIES_SCANNED) {
              pending.push({ directory: fullPath, depth: current.depth + 1 });
            } else {
              stats.truncated = true;
            }
          }
          continue;
        }
        if (!entry.isFile()) continue;

        stats.files += 1;
        files.push({
          fullPath,
          root,
          depth: current.depth,
          filenameScore: scoreFilename(fullPath, query, tokens),
          textLike: isTextLike(fullPath),
        });
        if (stats.files >= MAX_FILES_SCANNED) {
          stats.truncated = true;
          break;
        }
      }
    } catch {
      continue;
    }
  }
  return files;
}

function formatSize(sizeBytes: number | undefined): string {
  if (sizeBytes === undefined) return 'size unknown';
  if (sizeBytes < 1024) return `${sizeBytes} B`;
  if (sizeBytes < 1024 * 1024) return `${Math.round(sizeBytes / 1024)} KB`;
  return `${(sizeBytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatModified(mtimeMs: number | undefined): string {
  if (mtimeMs === undefined) return 'modified time unknown';
  return `modified ${new Date(mtimeMs).toISOString().slice(0, 10)}`;
}

class SuperSearchToolInvocation extends BaseToolInvocation<
  SuperSearchParams,
  ToolResult
> {
  constructor(
    private readonly config: Config,
    params: SuperSearchParams,
  ) {
    super(params);
  }

  getDescription(): string {
    const location = this.params.path ? ` in ${this.params.path}` : '';
    return `Find likely files for '${this.params.query}'${location}`;
  }

  override toolLocations() {
    return this.params.path ? [{ path: this.params.path }] : [];
  }

  override async getDefaultPermission(): Promise<PermissionDecision> {
    // The no-path mode is intentionally limited to user-facing roots and is
    // the safe default for an explicit request such as "find my resume".
    if (!this.params.path) return 'allow';
    const resolvedPath = resolvePath(
      this.config.getTargetDir(),
      this.params.path,
    );
    const workspaceContext = this.config.getWorkspaceContext();
    if (
      workspaceContext.isPathWithinWorkspace(resolvedPath) ||
      isSubpath(getMemoryBaseDir(), resolvedPath)
    ) {
      return 'allow';
    }
    return 'ask';
  }

  async execute(signal: AbortSignal): Promise<ToolResult> {
    const query = this.params.query.trim();
    const tokens = tokenizeQuery(query);
    const maxResults = this.params.max_results ?? DEFAULT_MAX_RESULTS;
    const startedAt = Date.now();
    const stats: SearchStats = { directories: 0, files: 0, truncated: false };

    try {
      const roots = this.params.path
        ? [
            {
              path: resolveAndValidatePath(this.config, this.params.path, {
                allowExternalPaths: true,
              }),
              label: this.params.path,
              priority: 110,
              maxDepth: 14,
              pruneJunk: false,
            },
          ]
        : buildDefaultSuperSearchRoots(
            os.homedir(),
            this.config.getWorkspaceContext().getDirectories(),
          );

      const existingRoots: SuperSearchRoot[] = [];
      for (const root of roots) {
        try {
          const rootStat = await fsPromises.stat(root.path);
          if (rootStat.isDirectory()) existingRoots.push(root);
        } catch {
          // A missing conventional root is expected; keep searching the rest.
        }
      }

      const scannedFiles: ScannedFile[] = [];
      for (const root of existingRoots) {
        scannedFiles.push(
          ...(await scanRoot(root, query, tokens, signal, startedAt, stats)),
        );
        if (stats.truncated || signal.aborted) break;
      }

      if (signal.aborted) {
        return {
          llmContent: 'File search cancelled.',
          returnDisplay: 'Cancelled',
        };
      }

      scannedFiles.sort(
        (a, b) =>
          b.filenameScore +
            b.root.priority -
            (a.filenameScore + a.root.priority) ||
          a.depth - b.depth ||
          a.fullPath.localeCompare(b.fullPath),
      );

      const candidates = new Map<string, Candidate>();
      const addCandidate = async (
        file: ScannedFile,
        score: number,
        matchedBy: string[],
        snippet?: string,
      ): Promise<void> => {
        const fileStat = await statFile(file.fullPath);
        if (!fileStat) return;
        const existing = candidates.get(file.fullPath);
        const candidate: Candidate = {
          fullPath: file.fullPath,
          rootLabel: file.root.label,
          score,
          matchedBy,
          ...(snippet ? { snippet } : {}),
          sizeBytes: fileStat.sizeBytes,
          mtimeMs: fileStat.mtimeMs,
        };
        if (!existing || candidate.score > existing.score) {
          candidates.set(file.fullPath, candidate);
        }
      };

      for (const file of scannedFiles) {
        if (file.filenameScore > 0) {
          await addCandidate(file, file.root.priority + file.filenameScore, [
            'filename match',
          ]);
        }
      }

      if (this.params.include_content !== false) {
        let contentFiles = 0;
        for (const file of scannedFiles) {
          if (!file.textLike || contentFiles >= MAX_CONTENT_FILES) break;
          if (Date.now() - startedAt >= MAX_SCAN_TIME_MS) {
            stats.truncated = true;
            break;
          }
          contentFiles += 1;
          const content = await readFileHead(file.fullPath);
          if (!content) continue;
          const contentScore = scoreContent(content, query, tokens);
          if (contentScore <= 0) continue;
          await addCandidate(
            file,
            file.root.priority + file.filenameScore + contentScore,
            [
              ...(file.filenameScore > 0 ? ['filename match'] : []),
              'content match',
            ],
            makeSnippet(content, tokens),
          );
        }
      }

      const ranked = [...candidates.values()]
        .sort(
          (a, b) =>
            b.score - a.score ||
            (b.mtimeMs ?? 0) - (a.mtimeMs ?? 0) ||
            a.fullPath.localeCompare(b.fullPath),
        )
        .slice(0, maxResults);

      const searchedDescription = existingRoots
        .map((root) => root.label)
        .join(', ');
      if (ranked.length === 0) {
        const truncation = stats.truncated
          ? ' The bounded search stopped before exhausting every file; search a named folder for a deeper pass.'
          : '';
        return {
          llmContent:
            `No likely files found for "${query}" in ${searchedDescription || 'the configured search roots'}.` +
            ` Searched user-facing folders and skipped system, cache, and dependency trees.${truncation}`,
          returnDisplay: 'No likely files found',
        };
      }

      const lines = ranked.map((candidate, index) => {
        const reason = candidate.matchedBy.join(' + ');
        const details = `${reason}; ${formatSize(candidate.sizeBytes)}; ${formatModified(candidate.mtimeMs)}`;
        const snippet = candidate.snippet ? `\n   ${candidate.snippet}` : '';
        return `${index + 1}. ${candidate.fullPath} (${details})${snippet}`;
      });
      const truncation = stats.truncated
        ? '\n\nSearch was bounded by time/volume; these are the best candidates found so far.'
        : '';
      const more =
        candidates.size > ranked.length
          ? `\n${candidates.size - ranked.length} lower-ranked candidate(s) omitted.`
          : '';
      const llmContent =
        `Found ${candidates.size} likely file(s) for "${query}" across ${searchedDescription || 'the configured search roots'}:\n---\n` +
        lines.join('\n') +
        `\n---\nUse read_file on an exact path above to inspect it; do not run a broad filesystem search.${more}${truncation}`;
      return {
        llmContent,
        returnDisplay: `Found ${ranked.length} likely file(s)`,
        resultFilePaths: ranked.map((candidate) => candidate.fullPath),
      };
    } catch (error) {
      const errorMessage = getErrorMessage(error);
      return {
        llmContent: `Error during bounded file search: ${errorMessage}`,
        returnDisplay: `Error: ${errorMessage}`,
        error: {
          message: errorMessage,
          type: ToolErrorType.EXECUTION_FAILED,
        },
      };
    }
  }
}

export class SuperSearchTool extends BaseDeclarativeTool<
  SuperSearchParams,
  ToolResult
> {
  static readonly Name = ToolNames.SUPER_SEARCH;

  constructor(private readonly config: Config) {
    super(
      SuperSearchTool.Name,
      ToolDisplayNames.SUPER_SEARCH,
      'Bounded, ranked local file discovery for open-ended requests such as "find my resume" or "look in Downloads". Searches user-facing roots (workspace, Downloads, Claude downloads, Desktop, Documents, Projects, and repos), skips system/cache/dependency trees, and optionally checks small text-file headers. Use this before shell/find/glob when the user asks to locate a file and the folder is uncertain. It returns a short candidate list; use read_file on an exact result. For directed codebase searches, use glob or grep_search instead.',
      Kind.Search,
      {
        properties: {
          query: {
            description:
              'Natural-language filename or content query, e.g. "my resume" or "Nathan Gill resume".',
            type: 'string',
          },
          path: {
            description:
              'Optional directory to search. Use this when the user names a folder such as ~/Downloads. If omitted, search curated user-facing roots.',
            type: 'string',
          },
          max_results: {
            description: `Maximum ranked candidates to return (1-${MAX_RESULTS}; default ${DEFAULT_MAX_RESULTS}).`,
            type: 'integer',
            minimum: 1,
            maximum: MAX_RESULTS,
          },
          include_content: {
            description:
              'Whether to inspect bounded headers of small text files when the filename does not match. Defaults to true.',
            type: 'boolean',
          },
        },
        required: ['query'],
        type: 'object',
      },
      true,
      false,
      false,
      false,
      'file search find resume downloads desktop documents filename content ranked candidates',
    );
  }

  override get maxOutputChars(): number {
    return MAX_OUTPUT_CHARS;
  }

  protected override validateToolParamValues(
    params: SuperSearchParams,
  ): string | null {
    if (typeof params.query !== 'string' || params.query.trim().length < 2) {
      return "The 'query' parameter must contain at least two characters.";
    }
    if (
      params.max_results !== undefined &&
      (!Number.isInteger(params.max_results) ||
        params.max_results < 1 ||
        params.max_results > MAX_RESULTS)
    ) {
      return `The 'max_results' parameter must be an integer from 1 to ${MAX_RESULTS}.`;
    }
    if (params.path !== undefined) {
      params.path = params.path.trim();
      if (params.path.length === 0)
        return "The 'path' parameter cannot be empty.";
      try {
        resolveAndValidatePath(this.config, params.path, {
          allowExternalPaths: true,
        });
      } catch (error) {
        return getErrorMessage(error);
      }
    }
    return null;
  }

  protected createInvocation(
    params: SuperSearchParams,
  ): ToolInvocation<SuperSearchParams, ToolResult> {
    return new SuperSearchToolInvocation(this.config, params);
  }
}
