# `packages/audio-capture` Rust port status

The crate contains the microphone capture engine, WAV encoding, bounded PCM
collection, streaming PCM drain, waveform level, silence detection, and
platform authorization query. The Rust engine uses the vendored miniaudio
v0.11.22 header through a small C++ device shim. The header is MIT-0 or
Unlicense; the Canopy crate is Apache-2.0.

## Package integration

The optional `node-addon` Cargo feature exposes the Rust API through N-API.
`packages/audio-capture/src/index.ts` keeps the existing TypeScript API and
loads this binding first from `build/Release` or a platform prebuild. The CLI's
existing `native-audio-recorder.ts` caller therefore uses Rust without a
caller-side API change when a Rust addon is present. The package no longer
loads a C++ Node addon or invokes node-gyp; the Rust crate's small C++
miniaudio device shim remains part of the Rust engine build.

`npm run build` compiles the Rust N-API library and TypeScript facade.
`npm run prebuildify` packages the N-API binary under the package's platform
prebuild directory. The install hook uses a matching prebuild or tries to
compile Rust in a source checkout. A missing Rust workspace, unavailable
toolchain, or failed build is non-fatal; the CLI can use its SoX/arecord
recorder fallback. The TypeScript entry point only locates the Rust N-API
binary and adapts its methods; if no compatible binary is present, it reports
an error for the CLI recorder to handle.

The binding preserves the package's method names and return values, the
16 kHz/mono/no-silence-detection defaults for omitted options, platform-name
errors, and the existing user-facing error text for existing capture errors.
The addon's `getPlatformBackendName(platform?)` also matches the TypeScript
utility's optional platform override; when omitted, it reports the current
native target.

The Rust interactive CLI TUI also links the crate directly for push-to-talk.
When `general.voice.enabled` is true and `voiceModel` is selected, Space starts
capture in the configured hold or tap mode. Capture uses the native engine
first, then SoX, then `arecord` on Linux. Hold mode inserts a completed
transcript into the prompt; tap mode inserts it and submits.

The TUI supports three ASR transports:

- `qwen3-asr-flash` and dated `qwen3-asr-flash-YYYY-MM-DD` models use the
  Canopy-ASR batch chat contract. WAV requests carry configured language and
  trusted keyterms.
- `qwen3-asr-flash-realtime` models use the Qwen realtime WebSocket session
  protocol. PCM is sent live as base64 append events. The configured language
  and trusted keyterms are sent as the session language and corpus context.
- `fun-asr` and `paraformer` model IDs matching the TypeScript realtime
  selector use the DashScope duplex task WebSocket protocol. PCM is sent as
  binary frames, with configured language hints. This transport does not
  receive keyterm context, matching the TypeScript caller.

Realtime transcription requires the native capture backend so PCM can be
drained during recording. If native capture is unavailable, the TUI gives the
same guidance as the TypeScript path: install or rebuild the audio-capture
package, or select the batch Qwen model. Realtime audio uses 16 kHz, mono,
little-endian signed 16-bit PCM. The upload queue holds at most 32 aggregated
frames of 32 KiB each; when full, the TUI drops frames and logs one
backpressure warning until a frame can be queued again. WebSocket server
messages/frames and accumulated transcript text are each capped at 1 MiB.

Realtime setup has an 8-second timeout and retries one retryable open failure
after 200 ms. Authentication, model, and rate-limit errors are not retried.
After recording stops, the TUI waits up to 60 seconds for the final provider
event. Esc and Ctrl-C cancel capture and abort the WebSocket task. The TUI
inserts finalized transcript text; live interim text is not rendered.

Voice endpoint requests reject redirects, cap batch audio/response bodies, and
pin validated DNS addresses for both HTTP and WebSocket connections. Unless
`general.voice.refineTranscript` is false, a configured OpenAI-compatible
`fastModel` uses the existing Rust completion client for best-effort transcript
cleanup; failures retain the raw transcript. Keyterm-echo filtering applies to
Qwen batch and realtime transcripts.

## Validation and remaining work

The Rust engine previously passed its crate compile and focused unit coverage
on Apple Silicon macOS. The new N-API feature passes a host `cargo check`, a
locked optimized addon build, the package TypeScript build, and crate formatting
checks. The Rust CLI voice integration passes `cargo check -p canopy-cli
--locked --offline` and formatting. No tests were added or run for this
realtime transport change.

The capture crate and its `node-addon` feature both pass locked offline
`cargo check`. No tests were added or run for the platform-name API parity
update.

The package's TypeScript native-recorder caller continues to reach Rust through
the N-API facade. The Rust TUI now links the same capture crate directly. The
non-fullscreen TypeScript prompt and other TypeScript UI consumers still use
the TypeScript recorder/transcriber path.

Live microphone capture and teardown, the macOS permission prompt, and Linux
and Windows device/permission behavior remain unvalidated. The current host
check does not prove that cross-platform binaries load or that hardware capture
works. The Rust TUI does not display interim realtime transcript callbacks.
Transcript cleanup currently uses only a resolvable OpenAI-compatible fast
model; other provider protocols fall back to the raw ASR transcript. The C++
addon and node-gyp compatibility sources have been removed from the package.
Release builds still need compatible Rust N-API prebuilds for each supported
OS, architecture, and Linux libc variant; users without a prebuild or Rust
toolchain retain the SoX/arecord recorder fallback.
