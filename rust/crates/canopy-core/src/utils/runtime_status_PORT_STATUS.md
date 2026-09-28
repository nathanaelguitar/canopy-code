# runtimeStatus.ts Rust port status

Implemented in runtime_status.rs, reusing atomic_file_write for durable
replacement and CancellationToken for cancellable reads. The reader requires
the same seven on-disk fields, accepts only schema version 1, rejects missing
or wrongly typed fields without coercion, ignores unknown fields, and returns
None for filesystem and parse failures after Node-style lossy UTF-8 decoding.
Cleanup swallows every remove error.
Focused retained Rust unit tests cover writing/defaults, parent creation,
atomic overwrite, Unicode, valid and invalid records, UTF-8 replacement,
cancellation, and idempotent cleanup.

Rust callers pass WriteRuntimeStatusFields and an optional CancellationToken;
a cancelled read returns a typed error carrying the token's optional
CancellationReason. JavaScript permits arbitrary abort reason values and
preserves object identity, which Rust cannot represent. The PID and timestamps
use f64 to retain JavaScript number validation and rounding behavior. Rust
returns a PathBuf from writes instead of the exact input string.

Unix hostname lookup uses gethostname. Windows uses COMPUTERNAME, which can
differ from Node's native os.hostname() if that environment variable is
missing or overridden. The shared Rust atomic writer creates a temp file and
renames it, but unlike the TypeScript writer it has no ownership-different
in-place or cross-device fallback. Writer failures propagate as io::Error
rather than the TypeScript writer's annotated Error rejection. Parent
directories are created before the atomic write; the shared atomic operation
itself is synchronous within the async wrapper.

Cargo tests have not been run for this slice because the consolidated Cargo
validation window is being managed by the parent task.
