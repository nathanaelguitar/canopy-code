# Interactive artifact publisher Rust port status

Sources: packages/core/src/tools/artifact/{artifact-tool,html,publisher,create-publisher,local-publisher,host-publisher,oss-publisher}.ts.

The standalone tools::artifact module now provides an ArtifactTool, a
backend-neutral publisher contract, and local, custom-command, and Aliyun OSS
implementations. The tool reads at most 16 MiB plus one byte from the source,
requires an absolute path, rejects empty/full-document/external-resource
fragments, wraps accepted body fragments in the matching CSP and reset, checks
the wrapped UTF-8 size cap, and publishes to a stable 16-character SHA-1
identity derived from a lexically resolved source path. Success returns model
text, display text, result_file_paths for local output, and first-class HTML
artifact metadata.

The local publisher atomically replaces
Storage::get_global_canopy_dir()/artifacts/<id>/index.html, uses a no-follow
destination and mode 0600, and returns its file:// URL. The host publisher parses quotes into argv,
substitutes {file} and {key} without invoking a shell, caps captured
stdout/stderr at 10 MiB, kills its child when the cancellation token fires, and
cleans its private temporary directory. The OSS publisher implements OSS V1
Content-MD5 and HMAC-SHA1 signing, supports both environment credential
families and optional STS tokens, and uses a 60-second cancellable PUT.

ArtifactTool::default_permission() is ask; confirmation_prompt() explains
when remote publishing uploads the page. Auto-open honors the explicit
CANOPY_ARTIFACT_NO_AUTO_OPEN=1 switch and the shared browser-launch policy.
Browser launch is best-effort and uses the secure URL opener with the local
artifact path allow-listed.

Host integration status:

- `tools/mod.rs` exports the module. The interactive CLI registers the tool
  only for interactive sessions, honors `experimental.artifact` and the
  `CANOPY_CODE_{DISABLE,ENABLE}_ARTIFACT` switches, and resolves the
  `artifact.autoOpen`, publisher, host, and OSS settings into
  `ArtifactToolConfig`. It applies configured permission rules and prompts
  before publishing unless the user explicitly allowed the tool.
- The ACP host registers the publishing `artifact` tool when the experimental
  setting and environment gates enable it. It builds the same publisher
  configuration as the interactive CLI and requests
  `session/request_permission` before publishing when configured rules require
  confirmation. The separate `record_artifact` metadata tool remains available
  independently.

Remaining parity gaps:

- The HTML checks intentionally mirror the source's heuristic scanner rather
  than parse HTML, JavaScript, or CSS. CSP remains the browser-side egress
  guard.
- Source text is decoded as UTF-8 with replacement for invalid sequences;
  TypeScript's filesystem service can apply broader encoding detection.
- The current core output type has no dedicated tool-error category. Invalid
  input and publish failures return their actionable message as an error
  string; cancellation returns a normal text result.
- OSS endpoint validation targets Aliyun OSS only. The OSS response body is
  not read, matching the source behavior.

No tests were added or run.
