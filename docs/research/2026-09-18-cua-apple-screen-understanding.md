# Cua Driver and Apple Screen Understanding for Canopy Code

**Date:** 2026-09-18
**Status:** Exploratory research
**Canopy revision inspected:** [`403250f`](https://github.com/nathanaelguitar/canopy-code/commit/403250faed1aac3fb6db39b4d421e94377282502)

## Executive conclusion

Yes, Cua is useful for Canopy Code—but it is not a missing dependency that should simply be added. The inspected Canopy fork already vendors and exposes Cua Driver as its built-in Computer Use implementation. Cua is currently the right layer for reliable desktop actions, accessibility-tree grounding, permissions, and post-action verification.

The most promising incremental work is an optional Apple-native perception backend on macOS:

```text
ScreenCaptureKit / Cua window state
        -> OCR and/or Apple Foundation Models image understanding
        -> structured observation
        -> Canopy agent chooses an action
        -> Cua Driver executes and re-observes to verify
```

Apple’s Foundation Models framework should be treated as a local model and tool-calling layer, not as a complete computer-use runtime. Screen capture, action execution, permissions, and the agent loop still belong to Canopy and Cua. Apple’s Visual Intelligence and App Intents APIs provide app-context integration, but they are not a general API for giving an agent an arbitrary Mac desktop.

**Recommendation:** preserve Cua as the action and verification substrate; prototype Apple OCR/Foundation Models behind Canopy’s existing vision bridge; start with window-scoped, read-only perception and bounded actions; do not make Apple-only code or Jev a dependency of the cross-platform core.

**Clarification:** Cua is new to this investigation, but it is not new to this checkout. [`trycua/cua`](https://github.com/trycua/cua) is a broader monorepo; the Qwen/Canopy codebase contains the Cua Driver subset and a Canopy-owned wrapper/distribution, not necessarily every Cua project such as Lume, Fleets, Cua-Bench, or Jev. The migration to Cua Driver landed in the local history in [`e8342715e`](https://github.com/nathanaelguitar/canopy-code/commit/e8342715e5a627e9709311b7122afdf0aa421fc8) on 2026-06-14, followed by a vendored `v0.17.0` sync in [`67d128715`](https://github.com/nathanaelguitar/canopy-code/commit/67d128715e2964803429e6aa697c5ed9da0da07c) on 2026-08-05. So the upstream Cua repository is still relevant for updates and adjacent components, but basic desktop computer use is already implemented in Canopy.

## What Canopy Code already has

Canopy already has a substantial computer-use implementation:

- [`docs/users/features/computer-use.md`](../users/features/computer-use.md) documents desktop actions including clicking, typing, scrolling, application launch, window inspection, accessibility trees, screenshots, and recordings.
- [`packages/core/src/tools/computer-use`](../../packages/core/src/tools/computer-use) registers Cua Driver tools lazily as `computer_use__*` tools. Lazy registration keeps them compatible with the permission manager and tool allowlists.
- The wrapper assigns confirmation requirements per action and marks higher-risk operations such as launching or killing apps, recording, configuration changes, and JavaScript execution for stronger approval.
- Tool results preserve image/audio blocks and structured content, including window IDs and bounds. This is important because a screenshot-only result would lose the stable identifiers needed for reliable follow-up actions.
- The current settings schema defaults `tools.computerUse.enabled` to `false`, because Canopy’s browser-control path is preferred for normal browser automation. Computer Use is intended for desktop-level/native-app cases.
- The feature page still contains older Qwen-era wording that says Computer Use is enabled by default and refers to `~/.qwen`; the settings schema is the authoritative behavior for this Canopy revision.
- [`docs/design/2026-07-21-tool-result-vision-bridge.md`](../design/2026-07-21-tool-result-vision-bridge.md) already defines a bridge for images returned by tools. It can pass images to an image-capable model or turn them into bounded, untrusted machine descriptions for a text-only model.

The current fork also includes a vendored Cua Driver distribution in [`packages/cua-driver`](../../packages/cua-driver). At the inspected revision, its README identifies the vendored source as `cua-driver-rs-v0.17.0`, while the runtime download constants pin `CUA_DRIVER_VERSION = '0.5.2'`. These may represent different packaging/version lines, but they should be explicitly reconciled before relying on features documented only in newer upstream Cua releases.

This changes the answer to “should we integrate Cua?” into “how should we harden and extend the existing Cua integration?”

## What upstream Cua adds

The [Cua repository](https://github.com/trycua/cua) is broader than a mouse-control library:

- **Cua Driver** is the relevant component for Canopy. It drives native macOS, Windows, and Linux applications through accessibility APIs, keyboard/mouse input, screenshots, and structured window state. It can be used through MCP, CLI, Python, or TypeScript.
- Its preferred flow is accessibility-first: call `get_window_state`, use the returned accessibility tree and stable `element_index`, then re-observe after the action. Pixel coordinates are a fallback for cases where accessibility information is missing or ambiguous.
- A window-state response combines an accessibility tree with a screenshot by default. The [capture and delivery documentation](https://cua.ai/docs/concepts/capture-and-delivery-modalities) describes the tree as the ground truth for actionable elements and the image as useful for layout, color, repeated labels, and visual context.
- Its [permission policies](https://cua.ai/docs/reference/cua-driver/permission-policies) and bounded manifests can restrict tools, applications, text lengths, scroll ranges, files, and display capture. This is a useful model for Canopy’s approval system.
- **Lume** provides local macOS/Linux virtual machines and **Cua Fleets** provides isolated cloud desktops. These are useful for sandboxing and evaluation, but are not necessary for the first Canopy integration because Canopy already has its own sandbox and worktree concepts.
- **Cua-Bench** provides task setup, reference trajectories, and evaluators. It is a good source of ideas for measuring grounding, recovery, and refusal behavior.

Cua deliberately does not choose the model or run the agent loop. The separation is effectively:

```text
model -> agent harness / Canopy -> Cua Driver -> operating system and applications
```

That separation fits Canopy’s architecture well.

## Apple’s role: perception, not computer use

### Screen capture

[ScreenCaptureKit](https://developer.apple.com/documentation/ScreenCaptureKit) is the Apple-native capture layer. It can enumerate displays, running applications, and windows, then produce display/window frames as `CVPixelBuffer` data through `SCStream` or screenshot APIs. It requires macOS Screen Recording permission and an appropriate usage description.

For Canopy, this is most useful as a narrow capture adapter:

- capture one selected window or application instead of the entire display;
- crop or redact sensitive regions before sending pixels to a model;
- attach a capture ID and window identity to every observation;
- avoid continuous capture unless the user explicitly enables it.

Cua’s own window-state calls should remain the first choice when they provide both the accessibility tree and a sufficient screenshot. ScreenCaptureKit is valuable when Canopy needs a separate native capture pipeline, a full-display view, or Apple-specific preprocessing.

### Foundation Models and OCR

Apple’s [Foundation Models image documentation](https://developer.apple.com/documentation/FoundationModels/analyzing-images-with-multimodal-prompting) describes image attachments in multimodal prompts, structured output, and tool calling. Apple also provides a Vision-backed [`OCRTool`](https://developer.apple.com/documentation/Vision/OCRTool) and barcode tools for use in a `LanguageModelSession`.

This makes Apple’s stack a plausible local perception backend for:

- extracting text from dialogs, menus, and canvas-like applications;
- identifying visual state that is absent from an accessibility tree;
- classifying a screen or region into a small structured schema;
- supplying OCR or visual evidence to the existing Canopy vision bridge.

It does not by itself provide a general-purpose click/type/drag loop. Canopy still needs to decide whether an observation is sufficient, choose an allowed action, ask for approval when appropriate, call Cua, and verify the result.

### Visual Intelligence and on-screen awareness

Apple’s [Visual Intelligence framework](https://developer.apple.com/documentation/VisualIntelligence) and App Intents on-screen context are primarily mechanisms for an app to make its own content and entities understandable to Apple’s system experiences. They should not be assumed to expose arbitrary third-party desktop applications as a structured screen feed.

The practical distinction is:

| Need                                                               | Appropriate layer                                                       |
| ------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| Capture a Mac window or display                                    | ScreenCaptureKit or Cua Driver                                          |
| Read visible text                                                  | Vision OCR / Foundation Models `OCRTool`                                |
| Understand an image or classify a UI state                         | Foundation Models multimodal prompt or Canopy’s configured vision model |
| Choose and execute a desktop action                                | Canopy agent + Cua Driver                                               |
| Make a specific app’s own entities available to Apple Intelligence | App Intents / view annotations                                          |

Apple’s newer `fm` command and Python Foundation Models SDK could make a small process bridge practical on supported macOS versions, but this should be treated as an optional adapter. Canopy is TypeScript/Node, and Apple model availability depends on OS, device, region, and Apple Intelligence availability. A Swift helper or Python/CLI bridge must fail cleanly back to the existing vision provider.

The on-device model is particularly attractive for privacy and latency, but it should initially be used for OCR, extraction, classification, and bounded visual judgments—not assumed to be a strong long-horizon UI planner. Apple’s Foundation Models availability and model behavior can also change with OS updates.

## Jev and bounded action selection

The [Cua Jev integration](https://cua.ai/docs/how-to-guides/driver/jev-use) is relevant to the user’s OCR/perception idea, but it is not a replacement for Canopy’s agent. Jev chooses among application-supplied action IDs; Cua Driver executes the selected action and the application re-observes to verify the postcondition. The visual variant uses a capture ID and bounded visual regions rather than allowing an untrusted chooser to invent arbitrary driver calls or coordinates.

That pattern is attractive for narrow, high-risk workflows:

```text
Canopy creates candidate action IDs
    -> perception/chooser selects one candidate or abstains
    -> Canopy validates policy and approval
    -> Cua executes the candidate
    -> Canopy re-observes and checks the postcondition
```

Jev should remain optional. The documented integration is in public preview, requires a TypeSafe API key for live calls, and adds another external service and billing/privacy boundary. A first Canopy experiment can implement the same bounded-candidate contract locally without adopting Jev.

## Recommended Canopy architecture

```text
User request
    |
    v
Canopy agent
    |-- use normal API / MCP / browser-control when available
    |
    `-- native or visually ambiguous desktop task
          |
          v
      Cua Driver: list apps -> select window -> get_window_state
          |                         |
          |                         `-- accessibility tree + screenshot
          |
          `-- optional Apple perception path on macOS
                ScreenCaptureKit or Cua image
                    -> crop/redact
                    -> Vision OCR / Foundation Models image prompt
                    -> bounded structured observation
          |
          v
      Canopy selects an approved action
          |-- accessibility element_index first
          |-- capture-bound pixel action only as fallback
          `-- optional bounded chooser such as Jev
          |
          v
      Cua executes -> Canopy re-observes -> postcondition check
```

Important design rules:

1. Keep accessibility-first actions as the default. A model should not be asked to rediscover coordinates when Cua already exposes a stable element.
2. Treat screenshots, OCR, and model-produced regions as untrusted observations, not instructions. The existing vision bridge’s untrusted-image policy should apply here too.
3. Use capture-bound coordinates with an exact capture ID when a visual click is unavoidable. Do not accept free-floating coordinates from a perception model.
4. Preserve Canopy’s existing permission and confirmation flow. Screen capture itself should be disclosed, and actions such as typing secrets, sending messages, purchases, destructive operations, app launch, and recording should remain gated.
5. Keep Apple code behind a capability check and provider interface. The core should continue to work on Windows, Linux, non-Apple macOS, and Macs without Apple Intelligence.

## Suggested experiment plan

### Phase 0: audit the existing integration

- Run the current Cua tools in read-only mode: enumerate applications, inspect one window, and inspect the accessibility tree.
- Confirm the runtime binary, vendored source, update behavior, telemetry behavior, and permissions used by the shipped Canopy build.
- Reconcile the `0.5.2` runtime pin with the `0.17.0` vendored-source note and document the supported upstream baseline.
- Add a small compatibility test so an upstream Cua update cannot silently change structured payloads, element IDs, or image handling.

### Phase 1: Apple perception proof of concept

On an Apple Silicon Mac with the required OS/runtime:

- capture one selected window, not the full desktop;
- run OCR and one small structured extraction prompt locally;
- return a schema such as `{ captureId, windowId, text, regions, uncertainty }`;
- feed that result through the existing vision bridge;
- perform no actions yet.

Measure latency, OCR quality, memory use, availability failures, and whether the local path stays offline.

### Phase 2: bounded action loop

- Generate a short candidate list from known Cua actions or application-defined action IDs.
- Let the perception/model layer select a candidate or abstain.
- Require Canopy policy/approval before execution.
- Execute through Cua using `element_index` where possible, otherwise a capture-bound visual action.
- Re-observe and require a postcondition before continuing.

### Phase 3: evaluation

Build a small fixture set of native-app tasks: open a dialog, locate a menu item, read a value, fill a non-sensitive form, recover from a changed layout, and refuse an unsafe action. Track:

- task success and postcondition correctness;
- accessibility-first versus visual-fallback rate;
- OCR and structured-observation accuracy;
- recovery after stale screenshots or changed windows;
- approval/refusal correctness;
- screen-data egress, retention, and telemetry;
- latency and CPU/memory cost.

Cua-Bench can provide ideas for task/evaluator structure, but Canopy should also maintain local deterministic fixtures for regression testing.

## Privacy and security notes

- Screen Recording permission is sensitive. Prefer a selected window and explicit user-visible capture state over silent full-display polling.
- The upstream [Cua telemetry documentation](https://cua.ai/docs/reference/cua-driver/telemetry) currently describes content-free telemetry enabled by default, including bounded operational events and a pseudonymous installation identifier. The vendored Canopy Cua README describes telemetry as disabled by default. This distribution difference must be verified in the actual shipped binary; local-first deployments should explicitly disable telemetry and update checks where supported.
- Apple’s on-device model is the preferred default for local perception. Private Cloud Compute or another remote vision model should be treated as an explicit egress path and surfaced through Canopy’s existing vision-bridge disclosure.
- OCR and multimodal outputs can contain secrets and prompt injection. They need the same untrusted-data handling, size limits, redaction, and audit treatment as screenshots returned by other tools.
- Cua’s bounded manifests and Canopy’s confirmation manager are complementary. A bounded manifest should enforce hard limits; user confirmation should handle contextual risk.

## Decision

**Go:** prototype an optional macOS Apple perception adapter on top of the existing Cua and vision-bridge architecture.

**Do not do yet:** replace Cua with raw screenshots, make full-screen capture the default, add Jev as a mandatory dependency, or put Apple-specific code in the cross-platform core.

The highest-value near-term work is likely Cua integration hardening and evaluation, followed by a narrow Apple local-perception adapter. Canopy already has the core ingredients for computer use; the opportunity is to make perception more private and robust on macOS while keeping action execution structured, permissioned, and verifiable.

## Sources

- [Canopy Code repository](https://github.com/nathanaelguitar/canopy-code)
- [Cua repository](https://github.com/trycua/cua)
- [Cua Driver README](https://raw.githubusercontent.com/trycua/cua/main/libs/cua-driver/README.md)
- [Cua capture and delivery modalities](https://cua.ai/docs/concepts/capture-and-delivery-modalities)
- [Cua action-selection policy](https://cua.ai/docs/reference/cua-driver/action-selection-policy)
- [Cua permission policies](https://cua.ai/docs/reference/cua-driver/permission-policies)
- [Cua Jev integration](https://cua.ai/docs/how-to-guides/driver/jev-use)
- [Cua computer-use model](https://cua.ai/docs/concepts/what-is-computer-use)
- [Cua telemetry](https://cua.ai/docs/reference/cua-driver/telemetry)
- [Apple Foundation Models image understanding](https://developer.apple.com/documentation/FoundationModels/analyzing-images-with-multimodal-prompting)
- [Apple OCRTool](https://developer.apple.com/documentation/Vision/OCRTool)
- [Apple ScreenCaptureKit](https://developer.apple.com/documentation/ScreenCaptureKit)
- [Apple Visual Intelligence](https://developer.apple.com/documentation/VisualIntelligence)
- [Apple Foundation Models framework](https://developer.apple.com/documentation/FoundationModels/)
- [Apple WWDC: What’s new in Foundation Models framework](https://developer.apple.com/videos/play/wwdc2026/241/)
