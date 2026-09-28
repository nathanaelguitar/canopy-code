# Rust Anthropic Runtime Slice

## Shipped behavior in ACP and `canopy run`

`canopy --acp --provider anthropic` and
`canopy run --provider anthropic` route primary model turns through the native
Rust Anthropic Messages API adapter. The selection is explicit; omitting
`--provider` keeps the existing OpenAI-compatible runtime. `canopy run` uses
the selected route for one-shot prompts, resumed sessions, line mode, and the
Ratatui chat loop.

Both hosts resolve the model from `--model` or `ANTHROPIC_MODEL`, the endpoint
from `--base-url` or `ANTHROPIC_BASE_URL` (defaulting to
`https://api.anthropic.com`), and credentials from `ANTHROPIC_API_KEY`. The
shared configured proxy is applied to either transport. Anthropic response
chunks enter the same Canopy turn, tool execution, transcript, and continuation
flow used by the OpenAI-compatible client. Request and response limits, SSE
event limits, stream idle/lifetime limits, and request cancellation come from
the native adapter.

Example:

```sh
ANTHROPIC_API_KEY=... ANTHROPIC_MODEL='your-model-id' canopy --acp --provider anthropic
# or
ANTHROPIC_API_KEY=... ANTHROPIC_MODEL='your-model-id' canopy run --provider anthropic
```

## Current boundaries

- Neither host resolves `modelProviders` or `providerProtocol` settings,
  exposes provider switching through ACP model-selection requests, or reads
  stored Anthropic login credentials. The flag and environment variables are
  the only native Anthropic routes in this slice.
- Web-fetch side queries remain on the OpenAI-compatible client, configured
  with `OPENAI_BASE_URL` and `OPENAI_API_KEY` (or `CANOPY_API_KEY`). Auto-memory
  recall is disabled for Anthropic primary turns because its selector still
  uses that OpenAI-compatible client. Other settings-backed model routes have
  not been migrated.
- The Telegram host still uses its existing OpenAI-compatible primary runtime
  constructor.
- Existing ACP image/audio prompt capability remains unchanged and those
  content blocks are still rejected before reaching either provider.
- Provider-specific Anthropic behavior is exposed by the adapter, but full
  source-runtime parity for every setting, login flow, and auxiliary request
  is not claimed by this runtime integration.

The `AgentRuntime::new` constructor remains OpenAI-compatible by default.
`AgentRuntime::new_anthropic` is the explicit native provider entry point for
hosts that have resolved an Anthropic model and credentials.
