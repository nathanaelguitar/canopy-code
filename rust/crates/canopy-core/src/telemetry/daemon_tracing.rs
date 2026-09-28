//! Workspace identifiers shared by daemon-owned state paths.
//!
//! Port of `hashDaemonWorkspace` from `packages/core/src/telemetry/daemon-tracing.ts`.

use sha2::{Digest, Sha256};

/// Hash the exact workspace string into the 16-character directory key used
/// by daemon-scoped channel state.
pub fn hash_daemon_workspace(workspace: &str) -> String {
    let digest = Sha256::digest(workspace.as_bytes());
    let mut hash = String::with_capacity(16);
    for byte in &digest[..8] {
        use std::fmt::Write as _;
        write!(&mut hash, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hash
}
