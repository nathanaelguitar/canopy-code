# Canopy Code Node memory-pressure RCA

**Date:** 2026-09-18
**Status:** Investigation documented; remediation not yet enabled

## Incident

On a 16 GB Apple Silicon Mac, Canopy Code became visibly slow and the terminal
session eventually stopped making progress while a long-running remote-control
and browser-control workflow was active. Activity Monitor showed two `node`
processes near 4.3 GB each, high swap use, and a yellow memory-pressure graph.

The observed snapshot was:

| Metric                           |      Observed value |
| -------------------------------- | ------------------: |
| Physical memory                  |            16.00 GB |
| Memory used                      |            14.28 GB |
| Swap used                        |            12.34 GB |
| Compressed memory                |             6.89 GB |
| Largest displayed Node processes | 4.38 GB and 4.26 GB |

The process names alone do not prove that V8 itself leaked. Activity Monitor's
memory column includes more than the JavaScript heap, and the values changed
substantially during the investigation. A later `ps` sample identified the
largest active application process as:

```text
/Users/nathanaelguitar/.local/node-v22.14.0-darwin-arm64/bin/node
  /Users/nathanaelguitar/repos/qwen-code/packages/cli/dist/index.js
  --resume 655b44b7-0610-4dc1-a25b-9ab7d6cce45e
```

That process was using about 2.1 GiB RSS and 184% CPU in the sample. A second
CLI process belonged to the same resumed session through the local `serve`
bridge. The TypeScript build running at the same time was a separate Node
process and is not part of the long-running agent's baseline.

## Root-cause assessment

### Proven contributors

1. **Severe system-wide memory pressure.** The machine was already paging
   heavily. This alone explains a large part of the UI and token-throughput
   degradation; swap latency makes an otherwise healthy process look frozen.
2. **A large, long-lived agent session.** The active session had a long browser
   and remote-control transcript, large browser snapshots/tool payloads, and a
   256K-context model selected. The daemon log recorded repeated inbound ACP
   frames of approximately 456 KB, which is a concrete indication that large
   command/update payloads were being retained or transferred.
3. **More than one CLI process for the same session path.** The process tree
   included the resumed CLI plus the local `serve`/ACP bridge and another
   session-side process. Even if only one process owns the main model state,
   serialization across the bridge creates additional transient buffers.
4. **A stalled generation held the session open.** The daemon recorded a
   request that produced no stream chunks for 240 seconds and failed with the
   configured stream-idle timeout. That is a failure-recovery symptom, not
   proof of a memory leak, but it lengthens the lifetime of all request state.

### Likely mechanism

The most likely mechanism is **unbounded or insufficiently bounded retention of
large session/tool payloads in the Node orchestration layer**, amplified by
duplicate JSON/string buffers during ACP/daemon transport and by the model's
large context. Browser-control HTML snapshots, tool results, transcript copies,
and serialized daemon frames can each exist simultaneously as JavaScript
strings/objects. V8 may reclaim dead objects eventually, but it cannot reclaim
objects still referenced by session history, event buffers, pending requests, or
logging/recording services. Under swap pressure, this creates the observed
feedback loop:

```text
large transcript/tool payloads
  -> duplicate serialized buffers and retained history
  -> larger Node RSS / slower GC
  -> system compression and swap
  -> slower model/tool stream
  -> pending requests live longer
  -> more memory pressure
```

This is a stronger explanation than “Node.js is inherently 4 GB.” Codex and
Claude's lower RSS is useful comparative evidence, but their implementations,
session retention policies, bridge topology, and active transcript sizes are
not identical, so it is not a controlled runtime benchmark.

## What is not established yet

- A V8 heap leak versus native/external memory growth has not been separated.
- The exact retaining object—history, browser snapshot, ACP frame, logger,
  cache, or a combination—has not been identified with a heap snapshot.
- The two Activity Monitor rows have not been proven to be two independent
  copies of the full transcript; one may be a bridge/worker with mostly native
  or compressed memory.
- Changing the Node heap cap alone is not a fix. It may turn paging into an
  earlier out-of-memory crash without reducing retained state.

## Safe next investigation

Do this on a reproducible test session, not the user's important live session:

1. Record `process.memoryUsage()` and `v8.getHeapStatistics()` at startup, after
   each prompt, after each browser-control call, after each ACP frame, and after
   each completed turn.
2. Track serialized byte counts for transcript history, tool results, pending
   events, and daemon frames. Log counts and sizes, never sensitive contents.
3. Capture two V8 heap snapshots: after startup and after a fixed browser task
   repeated ten times. Compare retained paths for strings, arrays, JSON objects,
   and session/history containers.
4. Compare one direct CLI session with one daemon/ACP session at the same prompt
   and context size. This isolates bridge duplication from application state.
5. Reproduce at 16K, 32K, and 256K context settings. Keep browser snapshots and
   tool outputs bounded so the model context size is not confused with an
   unlimited in-process transcript.

## Remediation direction

The eventual fix should be layered:

- bound retained browser snapshots and tool-result text by bytes and age;
- store resumable history on disk and keep only the active window in memory;
- avoid repeated `JSON.stringify`/`JSON.parse` copies for large ACP frames;
- discard completed request/event buffers promptly and cancel stalled streams;
- ensure one owner process manages a session, with attach clients remaining
  thin;
- add a memory-pressure watchdog that reports the retaining category before
  the process becomes unresponsive;
- add a bounded-session regression test that runs repeated large browser/tool
  turns and asserts memory reaches a plateau rather than growing linearly.

The existing compression work helps prevent an over-limit model request, but it
does not by itself solve in-process retention. Treat context fitting and memory
retention as separate controls.

## Conclusion

**Root cause:** system-wide swap pressure amplified by a long-lived, large
browser/remote-control session and multiple Node/ACP buffering layers.

**Confidence:** medium. The process topology, payload sizes, idle timeout, and
memory-pressure state support this diagnosis. A heap snapshot is still required
before labeling it a confirmed V8 memory leak.

**Action taken in this RCA:** documentation only. No Canopy process was killed,
no swap was cleared, and no runtime limit was changed.
