use serde::{Deserialize, Serialize};
use serde_json::Value;

/// On-disk install approval contract. The legacy field name is retained for
/// compatibility with `~/.canopy/computer-use/installed.json`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstallState {
    pub approved_package_spec: String,
    pub approved_at_iso: String,
}

impl InstallState {
    pub fn approves(&self, package_spec: &str) -> bool {
        self.approved_package_spec == package_spec
    }
}

/// Parse a persisted state record. Missing, malformed, or wrongly typed
/// fields mean "not approved", just as in the TypeScript loader. Unknown
/// fields are ignored for forward/backward compatibility.
pub fn install_state_from_json(text: &str) -> Option<InstallState> {
    let value: Value = serde_json::from_str(text).ok()?;
    Some(InstallState {
        approved_package_spec: value.get("approvedPackageSpec")?.as_str()?.to_owned(),
        approved_at_iso: value.get("approvedAtIso")?.as_str()?.to_owned(),
    })
}

pub fn install_state_to_json(state: &InstallState) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(state)
}
