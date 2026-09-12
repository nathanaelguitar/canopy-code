/**
 * @license
 * Copyright 2026 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

const PAIRING_URL_PATTERN = /canopy serve: Local Control pairing URL: (\S+)/g;

/**
 * Select the pairing URL that targets a particular daemon session.
 *
 * A workspace daemon can outlive the interactive TUI that enabled Local
 * Control, so its stdout may contain URLs for several sessions. The most
 * recently logged URL is not sufficient on its own: a URL for another session
 * can be newer than the one we need. The session path is the authoritative
 * discriminator because it is supplied to Local Control as the target.
 */
export function pairingUrlForSession(
  logText: string,
  sessionId: string,
): string | undefined {
  const targetPath = `/session/${encodeURIComponent(sessionId)}`;
  const urls = Array.from(
    logText.matchAll(PAIRING_URL_PATTERN),
    (match) => match[1],
  );

  for (const candidate of urls.reverse()) {
    try {
      if (new URL(candidate).pathname === targetPath) return candidate;
    } catch {
      // Ignore a partially written or malformed log line.
    }
  }
  return undefined;
}
