/**
 * @license
 * Copyright 2025 Canopy Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { networkInterfaces } from 'node:os';
import type { LanCandidate } from './lan-interfaces.js';

/**
 * Tailscale (and every other CGNAT-numbered overlay it interops with, e.g.
 * Headscale) hands out addresses from the shared carrier-grade NAT block
 * 100.64.0.0/10 (100.64.0.0 - 100.127.255.255, RFC 6598). `lan-interfaces.ts`
 * deliberately excludes this range and the `tailscale*`/`utun*` interface
 * names it rides on, because Local Control is scoped to "same physical
 * network" — a tailnet address reaches the host from anywhere, which is
 * exactly the opposite guarantee. Remote Control wants that reach, so it
 * looks for the same interfaces Local Control throws away.
 */
function isTailscaleIpv4(address: string): boolean {
  const octets = address.split('.').map((part) => Number(part));
  if (octets.length !== 4 || octets.some((o) => !Number.isInteger(o))) {
    return false;
  }
  const [a, b] = octets;
  return a === 100 && b >= 64 && b <= 127;
}

/** Every tailnet IPv4 address the host currently has, sorted for stable output. */
export function listTailscaleCandidates(
  interfaces = networkInterfaces(),
): LanCandidate[] {
  const candidates: LanCandidate[] = [];
  for (const [interfaceName, addresses] of Object.entries(interfaces).sort()) {
    for (const address of addresses ?? []) {
      if (address.family !== 'IPv4' || address.internal) continue;
      if (!isTailscaleIpv4(address.address)) continue;
      candidates.push({ interfaceName, address: address.address });
    }
  }
  return candidates;
}

export class NoTailscaleInterfaceError extends Error {
  readonly code = 'no_tailscale_interface';
  constructor() {
    super(
      'No Tailscale address is available. Install Tailscale and run ' +
        '`tailscale up` on this host, then retry.',
    );
    this.name = 'NoTailscaleInterfaceError';
  }
}

export class AmbiguousTailscaleInterfaceError extends Error {
  readonly code = 'ambiguous_tailscale_interface';
  readonly candidates: readonly LanCandidate[];
  constructor(candidates: readonly LanCandidate[]) {
    super(
      'Multiple Tailscale addresses are available; choose which one to expose: ' +
        candidates.map((c) => `${c.interfaceName} (${c.address})`).join(', '),
    );
    this.name = 'AmbiguousTailscaleInterfaceError';
    this.candidates = candidates;
  }
}

export class UnknownTailscaleInterfaceError extends Error {
  readonly code = 'unknown_tailscale_interface';
  constructor(requested: string) {
    super(
      `${requested} is not a Tailscale address on this host right now. ` +
        'The tailnet may have changed since the list was fetched.',
    );
    this.name = 'UnknownTailscaleInterfaceError';
  }
}

/** Pick the tailnet address to bind, mirroring {@link selectLanAddress}. */
export function selectTailscaleAddress(
  preferredAddress?: string,
  interfaces = networkInterfaces(),
): LanCandidate {
  const candidates = listTailscaleCandidates(interfaces);
  if (candidates.length === 0) throw new NoTailscaleInterfaceError();
  if (preferredAddress !== undefined) {
    const match = candidates.find((c) => c.address === preferredAddress);
    if (!match) throw new UnknownTailscaleInterfaceError(preferredAddress);
    return match;
  }
  if (candidates.length > 1)
    throw new AmbiguousTailscaleInterfaceError(candidates);
  return candidates[0];
}
