# `@qwen-code/audio-capture`

This package exposes the synchronous microphone API used by the CLI voice
recorder. Its capture engine lives in
[`rust/crates/canopy-audio-capture`](../../rust/crates/canopy-audio-capture),
and the package loads that engine through its Rust N-API addon.

## Building and installing

In a source checkout, `npm run build` compiles the Rust addon and the TypeScript
facade. `npm run prebuildify` packages a platform-specific N-API prebuild for
publishing; run it once per supported OS and architecture. The package install
hook uses a matching prebuild or attempts a Rust build in a source checkout.
That build is nonfatal: when it is unavailable or fails, CLI voice input can
use the existing SoX/arecord recorder fallback. No node-gyp addon is built or
loaded.

The public API returns WAV bytes from `stopRecording`, PCM bytes from
`drainAudio`, reports `audioLevel` and `silenceDetected`, and exposes
`microphoneAuthorizationStatus`. Omitted native binding options default to
16 kHz, mono, and silence detection off. Unsupported host platforms retain the
existing platform error.

The TypeScript entry point only loads and adapts the N-API methods. If no
compatible Rust addon is installed, it reports that error to the CLI recorder,
which selects SoX or `arecord` where available. The Rust crate uses a small C++
miniaudio device shim internally; the removed C++ addon was a separate legacy
Node binding.

## Current limits

Rust capture code and the Node binding compile on the development host. Actual
microphone capture, macOS permission prompts, and Linux and Windows runtime
behavior have not been validated across those platforms. Release prebuilds must
be generated for each supported OS and architecture; a successful host build
does not prove cross-platform device behavior.
