//! Migration helpers for legacy Canopy settings.

use serde_json::{Map, Value};

/// Migrates legacy tool permission arrays into `permissions.allow` and
/// `permissions.deny`.
///
/// Returns `None` when `tools.core`, `tools.allowed`, and `tools.exclude` are
/// all absent or are not arrays. The input is never modified.
pub fn migrate_legacy_permissions(settings: &Value) -> Option<Value> {
    let tools = settings.as_object()?.get("tools")?.as_object()?;
    let allowed = tools.get("allowed").and_then(Value::as_array).cloned();
    let excluded = tools.get("exclude").and_then(Value::as_array).cloned();
    let core = tools.get("core").and_then(Value::as_array).cloned();

    if allowed.is_none() && excluded.is_none() && core.is_none() {
        return None;
    }

    let mut result = settings.clone();
    let result_object = result.as_object_mut()?;

    {
        let permissions = result_object
            .entry("permissions".to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        if !permissions.is_object() {
            *permissions = Value::Object(Map::new());
        }
        let permissions = permissions.as_object_mut()?;

        if let Some(values) = &allowed {
            merge_values(permissions, "allow", values);
        }
        if let Some(values) = &excluded {
            merge_values(permissions, "deny", values);
        }
        if let Some(values) = &core {
            merge_values(permissions, "allow", values);
        }
    }

    if let Some(tools) = result_object
        .get_mut("tools")
        .and_then(Value::as_object_mut)
    {
        if allowed.is_some() {
            tools.remove("allowed");
        }
        if excluded.is_some() {
            tools.remove("exclude");
        }
        if core.is_some() {
            tools.remove("core");
        }
    }

    Some(result)
}

fn merge_values(permissions: &mut Map<String, Value>, key: &str, additions: &[Value]) {
    let mut merged = permissions
        .get(key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for value in additions {
        if !merged
            .iter()
            .any(|existing| js_same_value_zero(existing, value))
        {
            merged.push(value.clone());
        }
    }

    // These property names are fixed, so input keys cannot become assignment
    // targets (including JavaScript's special `__proto__` property).
    permissions.insert(key.to_owned(), Value::Array(merged));
}

/// JSON has no shared object references, so only primitive values can be
/// equal under JavaScript `Set` semantics. Numbers are compared as JavaScript
/// numbers, which also makes `1` and `1.0` duplicates and treats `-0` as `0`.
fn js_same_value_zero(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(left), Value::Bool(right)) => left == right,
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        (Value::String(left), Value::String(right)) => left == right,
        (Value::Array(_), _) | (Value::Object(_), _) => false,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::migrate_legacy_permissions;

    #[test]
    fn migrates_legacy_arrays_and_preserves_other_settings() {
        let settings = json!({
            "theme": "dark",
            "tools": {
                "allowed": ["read", "write"],
                "exclude": ["shell"],
                "core": ["read", "search"],
                "sandbox": true
            },
            "permissions": {
                "allow": ["read", "edit"],
                "deny": ["network"],
                "ask": ["shell"]
            }
        });

        let result = migrate_legacy_permissions(&settings).expect("migration applies");

        assert_eq!(
            result,
            json!({
                "theme": "dark",
                "tools": {"sandbox": true},
                "permissions": {
                    "allow": ["read", "edit", "write", "search"],
                    "deny": ["network", "shell"],
                    "ask": ["shell"]
                }
            })
        );
        assert_eq!(settings["tools"]["allowed"], json!(["read", "write"]));
        assert_eq!(settings["permissions"]["allow"], json!(["read", "edit"]));
    }

    #[test]
    fn returns_none_unless_a_legacy_setting_is_an_array() {
        for settings in [
            json!({}),
            json!({"tools": {}}),
            json!({"tools": {"core": "read", "allowed": null, "exclude": false}}),
            json!({"tools": ["read"]}),
        ] {
            assert_eq!(migrate_legacy_permissions(&settings), None, "{settings}");
        }
    }

    #[test]
    fn removes_only_legacy_keys_that_are_arrays() {
        let settings = json!({
            "tools": {
                "allowed": ["read"],
                "exclude": "shell",
                "core": null,
                "other": ["keep"]
            }
        });

        let result = migrate_legacy_permissions(&settings).expect("migration applies");

        assert_eq!(
            result,
            json!({
                "tools": {
                    "exclude": "shell",
                    "core": null,
                    "other": ["keep"]
                },
                "permissions": {"allow": ["read"]}
            })
        );
    }

    #[test]
    fn deduplicates_primitives_using_javascript_set_number_semantics() {
        let settings = json!({
            "tools": {"allowed": ["read", 1, 1.0, -0.0, true, null]},
            "permissions": {"allow": [1.0, 0, "read", false]}
        });

        let result = migrate_legacy_permissions(&settings).expect("migration applies");
        let allow = result["permissions"]["allow"].as_array().unwrap();

        assert_eq!(allow.len(), 6);
        assert_eq!(allow[0], json!(1.0));
        assert_eq!(allow[1], json!(0));
        assert_eq!(allow[2], json!("read"));
        assert_eq!(allow[3], json!(false));
        assert_eq!(allow[4], json!(true));
        assert_eq!(allow[5], Value::Null);
    }

    #[test]
    fn keeps_distinct_json_objects_and_prototype_names_as_data() {
        let settings: Value = serde_json::from_str(
            r#"{"__proto__":{"safe":true},"tools":{"allowed":["__proto__","constructor","prototype",{"x":1},{"x":1}]},"permissions":{"__proto__":"preserved"}}"#,
        )
        .unwrap();

        let result = migrate_legacy_permissions(&settings).expect("migration applies");

        assert_eq!(result["__proto__"]["safe"], true);
        assert_eq!(result["permissions"]["__proto__"], "preserved");
        assert_eq!(
            result["permissions"]["allow"],
            json!(["__proto__", "constructor", "prototype", {"x": 1}, {"x": 1}])
        );
    }

    #[test]
    fn replaces_non_array_permission_lists_and_non_object_permissions() {
        let settings = json!({
            "tools": {"exclude": ["shell"]},
            "permissions": {"deny": "invalid", "ask": ["read"]}
        });

        let result = migrate_legacy_permissions(&settings).expect("migration applies");

        assert_eq!(result["permissions"]["deny"], json!(["shell"]));
        assert_eq!(result["permissions"]["ask"], json!(["read"]));

        let settings = json!({"tools": {"core": ["read"]}, "permissions": false});
        let result = migrate_legacy_permissions(&settings).expect("migration applies");
        assert_eq!(result["permissions"], json!({"allow": ["read"]}));
    }
}
