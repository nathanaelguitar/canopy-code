//! Validates the optional child-side startup profile carried on ACP initialize.

use std::collections::BTreeMap;

use serde_json::Value;

pub const CHANNEL_STARTUP_PROFILE_META_KEY: &str = "qwen.channel.startupProfile";
pub const CHANNEL_STARTUP_PROFILE_VERSION: u8 = 1;
const MAX_PROFILE_DURATION_MS: f64 = 600_000.0;
const ATTRIBUTE_PREFIX: &str = "qwen-code.daemon.acp_startup";

const PHASE_KEYS: &[&str] = &[
    "processToProfilerReadyMs",
    "geminiImportMs",
    "argsParseMs",
    "settingsLoadMs",
    "configConstructionMs",
    "appInitializationMs",
    "acpImportMs",
    "bootstrapConfigInitializationMs",
    "transportSetupMs",
    "initializeHandlerMs",
    "unattributedMs",
];
const CONFIG_KEYS: &[&str] = &[
    "extensionsInitialMs",
    "hooksMs",
    "skillsMs",
    "extensionsFinalMs",
    "hierarchicalMemoryMs",
    "toolRegistryMs",
    "ripgrepProbeMs",
    "toolWarmupMs",
    "otherMs",
];

pub fn get_channel_startup_profile_attributes(
    response: &Value,
    received_at_epoch_ms: f64,
    initialize_timeout_ms: f64,
) -> Option<BTreeMap<String, Value>> {
    let profile = response
        .get("_meta")?
        .get(CHANNEL_STARTUP_PROFILE_META_KEY)?;
    if profile.get("v")?.as_u64()? != CHANNEL_STARTUP_PROFILE_VERSION as u64 {
        return None;
    }
    let phases = read_durations(profile.get("phases"), PHASE_KEYS);
    let config = read_durations(profile.get("config"), CONFIG_KEYS);
    let process_to_response = read_duration(profile.get("processToResponseMs"));
    let response_built = profile
        .get("responseBuiltAtEpochMs")
        .and_then(Value::as_f64)
        .filter(|v| v.is_finite() && *v >= 0.0);
    let child_complete = profile.get("complete").and_then(Value::as_bool) == Some(true);
    let effective_complete = child_complete
        && phases.1
        && config.1
        && process_to_response.is_some()
        && response_built.is_some();
    let mut attrs = BTreeMap::new();
    attrs.insert(
        format!("{ATTRIBUTE_PREFIX}.profile.version"),
        Value::from(CHANNEL_STARTUP_PROFILE_VERSION),
    );
    attrs.insert(
        format!("{ATTRIBUTE_PREFIX}.profile.complete"),
        Value::from(effective_complete),
    );
    if let Some(duration) = process_to_response {
        attrs.insert(
            format!("{ATTRIBUTE_PREFIX}.child.process_to_response_ms"),
            Value::from(duration),
        );
    }
    if let Some(duration) = phases.0.get("unattributedMs") {
        attrs.insert(
            format!("{ATTRIBUTE_PREFIX}.child.unattributed_ms"),
            Value::from(*duration),
        );
    }
    add_duration_attrs(&mut attrs, "phase", &phases.0);
    add_duration_attrs(&mut attrs, "config", &config.0);
    if let Some(response_built) = response_built {
        let transport = received_at_epoch_ms - response_built;
        if transport.is_finite() && transport >= 0.0 && transport <= initialize_timeout_ms {
            attrs.insert(
                format!("{ATTRIBUTE_PREFIX}.response_transport_ms"),
                Value::from(transport),
            );
        }
    }
    Some(attrs)
}

fn read_duration(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && (0.0..=MAX_PROFILE_DURATION_MS).contains(value))
}
fn read_durations(source: Option<&Value>, keys: &[&str]) -> (BTreeMap<String, f64>, bool) {
    let mut values = BTreeMap::new();
    let Some(source) = source.and_then(Value::as_object) else {
        return (values, false);
    };
    let mut complete = true;
    for key in keys {
        if let Some(value) = read_duration(source.get(*key)) {
            values.insert((*key).into(), value);
        } else {
            complete = false;
        }
    }
    (values, complete)
}
fn add_duration_attrs(
    attrs: &mut BTreeMap<String, Value>,
    group: &str,
    values: &BTreeMap<String, f64>,
) {
    for (key, value) in values {
        if group == "phase" && key == "unattributedMs" {
            continue;
        }
        let mut snake = String::new();
        for ch in key.chars() {
            if ch.is_ascii_uppercase() {
                snake.push('_');
                snake.push(ch.to_ascii_lowercase());
            } else {
                snake.push(ch);
            }
        }
        attrs.insert(
            format!("{ATTRIBUTE_PREFIX}.{group}.{snake}"),
            Value::from(*value),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn profile_completeness_requires_every_declared_duration() {
        let input = serde_json::json!({"_meta":{"qwen.channel.startupProfile":{"v":1,"complete":true,"processToResponseMs":10,"responseBuiltAtEpochMs":100,"phases":{"processToProfilerReadyMs":2,"unattributedMs":1},"config":{}}}});
        let attrs = get_channel_startup_profile_attributes(&input, 110.0, 1000.0).unwrap();
        assert_eq!(
            attrs["qwen-code.daemon.acp_startup.profile.complete"],
            false
        );
        assert_eq!(
            attrs["qwen-code.daemon.acp_startup.response_transport_ms"],
            10.0
        );
    }
}
