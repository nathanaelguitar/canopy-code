# Image generation configuration port status

`image_generation.rs` ports the config-side selection behavior from
`parseVisionModelSetting`, `Config.resolveImageGenerationModel`, and
`Config.getImageGenerationConfig`.

Implemented:

- NUL-delimited model selector and optional base URL parsing.
- Known auth-type prefixes, bare model IDs, and exact unique-route matching.
- Image-only filtering, fast/voice-only exclusion, optional auth/base URL
  disambiguation, and required nonblank API-key environment variable plus
  explicit registry endpoint.
- Safe-mode and bare-mode gates for the public image-generation config getter.
- HTTPS URL parsing, credential/query/fragment rejection, and trailing slash
  normalization.

Remaining integration gap:

- The host must adapt its model registry into
  `ImageGenerationModelCandidate` records, including both the selected
  `base_url` and explicit `registry_base_url`. This module intentionally does
  not own provider configuration, model defaults, or runtime model discovery.
- No ambient fast-model or current-model selector context is supplied here;
  `fast` and `inherit` therefore resolve to no image-generation config.
