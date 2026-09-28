/**
 * @license
 * Copyright 2025 Qwen
 * SPDX-License-Identifier: Apache-2.0
 */

import { createRequire } from 'node:module';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { getPlatformBackendName } from './platform.js';

export type MicrophoneAuthorizationStatus =
  | 'granted'
  | 'denied'
  | 'prompt'
  | 'unknown';

export interface AudioCaptureOptions {
  sampleRate: number;
  channels: number;
  /** Flag sustained silence so the caller can auto-stop (tap mode). */
  silenceDetection?: boolean;
}

export interface NativeAudioCaptureBackend {
  startRecording: (options: AudioCaptureOptions) => void;
  stopRecording: () => Uint8Array;
  isRecording: () => boolean;
  /** True once sustained silence was detected. Absent on older addons. */
  silenceDetected?: () => boolean;
  /** Return & clear PCM captured since the last call (for streaming uploads). */
  drainAudio?: () => Uint8Array;
  /** Recent input level 0..1 (for waveform display). */
  audioLevel?: () => number;
  microphoneAuthorizationStatus: () => MicrophoneAuthorizationStatus;
}

interface NativeBinding {
  startRecording: (options?: Partial<AudioCaptureOptions>) => void;
  stopRecording: () => Uint8Array;
  isRecording: () => boolean;
  silenceDetected?: () => boolean;
  drainAudio?: () => Uint8Array;
  audioLevel?: () => number;
  microphoneAuthorizationStatus?: () => MicrophoneAuthorizationStatus;
}

const nativeRequire = createRequire(import.meta.url);
// dist/index.js → package root, which holds prebuilds/ and build/.
const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));

function loadBinding(): NativeBinding {
  try {
    // Throws on unsupported platforms before touching the native layer.
    getPlatformBackendName();
    const rustBinding = loadRustBinding();
    if (rustBinding) {
      return rustBinding;
    }
    throw new Error(
      'No compatible Rust N-API addon was found for this platform and architecture.',
    );
  } catch (error) {
    throw new Error(
      'Native audio capture addon could not be loaded. Reinstall ' +
        '@qwen-code/audio-capture, or run "npm run build" in packages/audio-capture. ' +
        `(${error instanceof Error ? error.message : String(error)})`,
    );
  }
}

function loadRustBinding(): NativeBinding | undefined {
  const prebuildDirectory = join(
    packageRoot,
    'prebuilds',
    `${process.platform}-${process.arch}`,
  );
  const candidates = [
    join(packageRoot, 'build', 'Release', 'audio_capture_rust.node'),
    join(prebuildDirectory, 'audio_capture_rust.node'),
    // Linux distributions using musl have their own N-API prebuild directory.
    join(
      packageRoot,
      'prebuilds',
      `${process.platform}-${process.arch}-musl`,
      'audio_capture_rust.node',
    ),
  ];

  for (const candidate of candidates) {
    if (!existsSync(candidate)) {
      continue;
    }
    try {
      const addon = nativeRequire(candidate) as {
        NativeAudioCaptureBackend: new () => NativeBinding;
      };
      return new addon.NativeAudioCaptureBackend();
    } catch {
      // Keep the package's optional voice fallback available when a prebuild
      // does not match the current Node ABI or host runtime.
    }
  }
  return undefined;
}

export function createNativeAudioCaptureBackend(
  binding: NativeBinding = loadBinding(),
): NativeAudioCaptureBackend {
  const silenceDetected = binding.silenceDetected;
  const drainAudio = binding.drainAudio;
  const audioLevel = binding.audioLevel;
  return {
    startRecording: (options) => {
      binding.startRecording(options);
    },
    stopRecording: () => binding.stopRecording(),
    isRecording: () => binding.isRecording(),
    ...(drainAudio ? { drainAudio: () => drainAudio.call(binding) } : {}),
    ...(audioLevel ? { audioLevel: () => audioLevel.call(binding) } : {}),
    ...(silenceDetected
      ? { silenceDetected: () => silenceDetected.call(binding) }
      : {}),
    microphoneAuthorizationStatus: () =>
      binding.microphoneAuthorizationStatus?.() ?? 'unknown',
  };
}

export { getPlatformBackendName } from './platform.js';
