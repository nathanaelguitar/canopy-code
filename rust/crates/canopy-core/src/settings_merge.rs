use serde_json::{Map, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergeStrategy {
    Replace,
    Concat,
    Union,
    ShallowMerge,
}

const UNSAFE_KEYS: [&str; 3] = ["__proto__", "constructor", "prototype"];

pub fn custom_deep_merge<F>(
    mut get_merge_strategy_for_path: F,
    sources: &[Map<String, Value>],
) -> Map<String, Value>
where
    F: FnMut(&[String]) -> Option<MergeStrategy>,
{
    let mut result = Map::new();
    let mut path = Vec::new();
    for source in sources {
        merge_object(
            &mut result,
            source,
            &mut get_merge_strategy_for_path,
            &mut path,
        );
    }
    result
}

fn merge_object<F>(
    target: &mut Map<String, Value>,
    source: &Map<String, Value>,
    get_merge_strategy_for_path: &mut F,
    path: &mut Vec<String>,
) where
    F: FnMut(&[String]) -> Option<MergeStrategy>,
{
    for (key, source_value) in source {
        if UNSAFE_KEYS.contains(&key.as_str()) {
            continue;
        }

        path.push(key.clone());
        let strategy = get_merge_strategy_for_path(path);
        let target_value = target.get(key);

        if strategy == Some(MergeStrategy::ShallowMerge)
            && target_value.is_some_and(js_truthy)
            && js_truthy(source_value)
        {
            let mut spread = shallow_spread(target_value.expect("checked above"));
            spread.extend(shallow_spread(source_value));
            target.insert(key.clone(), Value::Object(spread));
        } else if let Some(Value::Array(target_array)) = target_value {
            match strategy {
                Some(MergeStrategy::Concat) => {
                    let mut merged = target_array.clone();
                    merged.extend(array_argument(source_value));
                    target.insert(key.clone(), Value::Array(merged));
                }
                Some(MergeStrategy::Union) => {
                    let mut merged = Vec::new();
                    for candidate in target_array
                        .iter()
                        .cloned()
                        .chain(array_argument(source_value))
                    {
                        if !merged
                            .iter()
                            .any(|existing| same_value_zero(existing, &candidate))
                        {
                            merged.push(candidate);
                        }
                    }
                    target.insert(key.clone(), Value::Array(merged));
                }
                _ => merge_non_array_value(
                    target,
                    key,
                    source_value,
                    get_merge_strategy_for_path,
                    path,
                ),
            }
        } else {
            merge_non_array_value(target, key, source_value, get_merge_strategy_for_path, path);
        }

        path.pop();
    }
}

fn merge_non_array_value<F>(
    target: &mut Map<String, Value>,
    key: &str,
    source_value: &Value,
    get_merge_strategy_for_path: &mut F,
    path: &mut Vec<String>,
) where
    F: FnMut(&[String]) -> Option<MergeStrategy>,
{
    if let Value::Object(source_object) = source_value {
        if let Some(Value::Object(target_object)) = target.get_mut(key) {
            merge_object(
                target_object,
                source_object,
                get_merge_strategy_for_path,
                path,
            );
        } else {
            let mut merged = Map::new();
            merge_object(
                &mut merged,
                source_object,
                get_merge_strategy_for_path,
                path,
            );
            target.insert(key.to_owned(), Value::Object(merged));
        }
    } else {
        target.insert(key.to_owned(), source_value.clone());
    }
}

fn array_argument(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values.clone(),
        _ => vec![value.clone()],
    }
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn shallow_spread(value: &Value) -> Map<String, Value> {
    match value {
        Value::Object(object) => object.clone(),
        Value::Array(array) => array
            .iter()
            .enumerate()
            .map(|(index, value)| (index.to_string(), value.clone()))
            .collect(),
        Value::String(string) => string
            .encode_utf16()
            .enumerate()
            .map(|(index, code_unit)| {
                (
                    index.to_string(),
                    Value::String(String::from_utf16_lossy(&[code_unit])),
                )
            })
            .collect(),
        Value::Null | Value::Bool(_) | Value::Number(_) => Map::new(),
    }
}

fn same_value_zero(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(left), Value::Bool(right)) => left == right,
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        (Value::String(left), Value::String(right)) => left == right,
        // JavaScript Set compares objects by identity. JSON trees have no shared
        // object identity, so each object or array element remains distinct.
        (Value::Array(_) | Value::Object(_), _) | (_, Value::Array(_) | Value::Object(_)) => false,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{MergeStrategy, custom_deep_merge};
    use serde_json::{Map, Value, json};

    fn object(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    fn merge(sources: &[Map<String, Value>]) -> Value {
        Value::Object(custom_deep_merge(|_| None, sources))
    }

    #[test]
    fn merges_simple_and_nested_objects() {
        assert_eq!(
            merge(&[
                object(json!({"a": 1, "b": 2})),
                object(json!({"b": 3, "c": 4}))
            ]),
            json!({"a": 1, "b": 3, "c": 4})
        );
        assert_eq!(
            merge(&[
                object(json!({"a": {"x": 1}, "b": 2})),
                object(json!({"a": {"y": 2}, "c": 3}))
            ]),
            json!({"a": {"x": 1, "y": 2}, "b": 2, "c": 3})
        );
    }

    #[test]
    fn replaces_arrays_by_default_and_supports_concat_and_union() {
        let sources = [object(json!({"a": [1, 2]})), object(json!({"a": [3, 4]}))];
        assert_eq!(merge(&sources), json!({"a": [3, 4]}));

        let concat = custom_deep_merge(
            |path| (path.len() == 1 && path[0] == "a").then_some(MergeStrategy::Concat),
            &sources,
        );
        assert_eq!(concat["a"], json!([1, 2, 3, 4]));

        let union = custom_deep_merge(
            |path| (path.len() == 1 && path[0] == "a").then_some(MergeStrategy::Union),
            &[
                object(json!({"a": [1, 2, 3]})),
                object(json!({"a": [3, 4, 5]})),
            ],
        );
        assert_eq!(union["a"], json!([1, 2, 3, 4, 5]));
    }

    #[test]
    fn shallow_merges_objects_and_matches_js_spread_for_other_values() {
        let sources = [
            object(json!({"a": {"x": 1, "y": 1}})),
            object(json!({"a": {"y": 2, "z": 2}})),
        ];
        let merged = custom_deep_merge(
            |path| (path.len() == 1 && path[0] == "a").then_some(MergeStrategy::ShallowMerge),
            &sources,
        );
        assert_eq!(merged["a"], json!({"x": 1, "y": 2, "z": 2}));

        let spread = custom_deep_merge(
            |path| (path.len() == 1 && path[0] == "a").then_some(MergeStrategy::ShallowMerge),
            &[object(json!({"a": ["old"]})), object(json!({"a": "new"}))],
        );
        assert_eq!(spread["a"], json!({"0": "n", "1": "e", "2": "w"}));
    }

    #[test]
    fn merges_multiple_sources_and_empty_source_list() {
        assert_eq!(
            merge(&[
                object(json!({"a": 1})),
                object(json!({"b": 2})),
                object(json!({"c": 3})),
            ]),
            json!({"a": 1, "b": 2, "c": 3})
        );
        assert_eq!(merge(&[]), json!({}));
    }

    #[test]
    fn clones_the_first_source_and_does_not_mutate_any_source() {
        let first = object(json!({"a": {"b": 1}, "items": [1, 2]}));
        let second = object(json!({"a": {"c": 2}, "items": [3, 4]}));
        let first_before = first.clone();
        let second_before = second.clone();

        let mut result = custom_deep_merge(|_| None, std::slice::from_ref(&first));
        assert_eq!(result, first);
        result.get_mut("a").unwrap()["b"] = json!(9);
        assert_eq!(first["a"]["b"], json!(1));

        let _ = custom_deep_merge(|_| None, &[first.clone(), second.clone()]);
        assert_eq!(first, first_before);
        assert_eq!(second, second_before);
    }

    #[test]
    fn applies_nested_strategies_by_full_path() {
        let result = custom_deep_merge(
            |path| match path
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .as_slice()
            {
                ["level1", "arr1"] => Some(MergeStrategy::Concat),
                ["level1", "arr2"] => Some(MergeStrategy::Union),
                ["level1", "obj1"] => Some(MergeStrategy::ShallowMerge),
                _ => None,
            },
            &[
                object(json!({"level1": {"arr1": [1, 2], "arr2": [1, 2], "obj1": {"a": 1}}})),
                object(json!({"level1": {"arr1": [3, 4], "arr2": [2, 3], "obj1": {"b": 2}}})),
            ],
        );
        assert_eq!(
            result["level1"],
            json!({"arr1": [1, 2, 3, 4], "arr2": [1, 2, 3], "obj1": {"a": 1, "b": 2}})
        );
    }

    #[test]
    fn union_uses_js_primitive_equality_and_object_identity() {
        let merged = custom_deep_merge(
            |path| (path.len() == 1 && path[0] == "values").then_some(MergeStrategy::Union),
            &[
                object(json!({"values": [1, 1.0, {"x": 1}]})),
                object(json!({"values": [1.0, {"x": 1}, null, null]})),
            ],
        );
        assert_eq!(merged["values"], json!([1, {"x": 1}, {"x": 1}, null]));
    }

    #[test]
    fn drops_prototype_sensitive_keys_at_every_depth() {
        let malicious = object(serde_json::from_str(
            r#"{"__proto__":{"polluted":true},"constructor":1,"prototype":2,"nested":{"__proto__":3,"ok":4}}"#,
        ).unwrap());
        let merged = custom_deep_merge(|_| None, &[malicious]);
        assert_eq!(merged, object(json!({"nested": {"ok": 4}})));
    }

    #[test]
    fn replace_strategy_still_recurses_for_object_pairs() {
        let merged = custom_deep_merge(
            |path| (path.len() == 1 && path[0] == "a").then_some(MergeStrategy::Replace),
            &[
                object(json!({"a": {"x": 1}})),
                object(json!({"a": {"y": 2}})),
            ],
        );
        assert_eq!(merged["a"], json!({"x": 1, "y": 2}));
    }
}
