//! JSON Schema conversion used by Canopy's OpenAI-compatible tool adapters.
//!
//! These functions port the tool-parameter conversion and the OpenAPI 3.0
//! compatibility mode from `core/openaiContentGenerator/converter.ts` and
//! `utils/schemaConverter.ts`.

use serde_json::{Map, Number, Value};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SchemaComplianceMode {
    #[default]
    Auto,
    OpenApi30,
}

/// Convert Gemini schema type names to lowercase and coerce numeric bounds
/// represented as strings. The source value is left untouched.
pub fn convert_gemini_tool_parameters_to_openai(parameters: &Value) -> Value {
    convert_gemini_types(parameters)
}

fn convert_gemini_types(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(convert_gemini_types).collect()),
        Value::Object(values) => {
            let mut converted = Map::with_capacity(values.len());
            for (key, value) in values {
                let converted_value = if value.is_object() || value.is_array() {
                    convert_gemini_types(value)
                } else if key == "type" {
                    value
                        .as_str()
                        .map(|value| Value::String(value.to_lowercase()))
                        .unwrap_or_else(|| value.clone())
                } else if matches!(key.as_str(), "minimum" | "maximum" | "multipleOf") {
                    coerce_js_number(value).unwrap_or_else(|| value.clone())
                } else if matches!(
                    key.as_str(),
                    "minLength" | "maxLength" | "minItems" | "maxItems"
                ) {
                    coerce_js_integer(value).unwrap_or_else(|| value.clone())
                } else {
                    value.clone()
                };
                converted.insert(key.clone(), converted_value);
            }
            Value::Object(converted)
        }
        _ => value.clone(),
    }
}

fn coerce_js_number(value: &Value) -> Option<Value> {
    let string = value.as_str()?;
    let trimmed = string.trim();
    let number = if trimmed.is_empty() {
        0.0
    } else {
        trimmed.parse::<f64>().ok()?
    };
    js_number_value(number)
}

fn coerce_js_integer(value: &Value) -> Option<Value> {
    let string = value.as_str()?;
    let trimmed = string.trim();
    if trimmed.is_empty() {
        return None;
    }
    let number = trimmed.parse::<f64>().ok()?;
    if !number.is_finite() || number.fract() != 0.0 {
        return None;
    }
    js_number_value(number)
}

fn js_number_value(number: f64) -> Option<Value> {
    if !number.is_finite() {
        return None;
    }
    if number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_991.0 {
        return Some(Value::Number(Number::from(number as i64)));
    }
    Number::from_f64(number).map(Value::Number)
}

/// Convert a modern JSON Schema to the subset used by OpenAPI 3.0.
pub fn convert_schema(schema: &Value, mode: SchemaComplianceMode) -> Value {
    match mode {
        SchemaComplianceMode::Auto => schema.clone(),
        SchemaComplianceMode::OpenApi30 => to_openapi_30(schema),
    }
}

fn to_openapi_30(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(to_openapi_30).collect()),
        Value::Object(source) => {
            let mut target = Map::new();

            if let Some(types) = source.get("type").and_then(Value::as_array) {
                let contains_null = types.iter().any(|value| value == "null");
                let selected = if types.len() == 2 && contains_null {
                    types.iter().find(|value| *value != "null")
                } else {
                    types
                        .iter()
                        .find(|value| *value != "null")
                        .or_else(|| types.first())
                };
                if let Some(selected) = selected {
                    target.insert("type".to_owned(), selected.clone());
                }
                if contains_null {
                    target.insert("nullable".to_owned(), Value::Bool(true));
                }
            } else if let Some(schema_type) = source.get("type") {
                target.insert("type".to_owned(), schema_type.clone());
            }

            if let Some(constant) = source.get("const") {
                target.insert("enum".to_owned(), Value::Array(vec![js_string(constant)]));
            }

            for (exclusive_key, bound_key) in [
                ("exclusiveMinimum", "minimum"),
                ("exclusiveMaximum", "maximum"),
            ] {
                if let Some(bound) = source.get(exclusive_key).filter(|value| value.is_number()) {
                    target.insert(bound_key.to_owned(), bound.clone());
                    target.insert(exclusive_key.to_owned(), Value::Bool(true));
                }
            }

            match source.get("items") {
                Some(Value::Array(_)) => {}
                Some(Value::Object(_)) => {
                    if let Some(items) = source.get("items") {
                        target.insert("items".to_owned(), to_openapi_30(items));
                    }
                }
                _ => {}
            }

            if let Some(values) = source.get("enum").and_then(Value::as_array) {
                target.insert(
                    "enum".to_owned(),
                    Value::Array(values.iter().map(js_string).collect()),
                );
            }

            for (key, value) in source {
                if matches!(key.as_str(), "properties" | "$defs" | "definitions") {
                    if let Some(properties) = value.as_object() {
                        let mut converted_properties = Map::with_capacity(properties.len());
                        for (property_name, property_schema) in properties {
                            converted_properties
                                .insert(property_name.clone(), to_openapi_30(property_schema));
                        }
                        target.insert(key.clone(), Value::Object(converted_properties));
                    } else {
                        target.insert(key.clone(), to_openapi_30(value));
                    }
                    continue;
                }

                if matches!(
                    key.as_str(),
                    "type"
                        | "const"
                        | "items"
                        | "enum"
                        | "$schema"
                        | "$id"
                        | "default"
                        | "dependencies"
                        | "patternProperties"
                ) {
                    continue;
                }

                if matches!(key.as_str(), "exclusiveMinimum" | "exclusiveMaximum")
                    && value.is_number()
                {
                    continue;
                }

                target.insert(key.clone(), to_openapi_30(value));
            }

            Value::Object(target)
        }
        _ => value.clone(),
    }
}

fn js_string(value: &Value) -> Value {
    let string = match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value
            .as_f64()
            .map(js_number_string)
            .unwrap_or_else(|| value.to_string()),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| match value {
                Value::Null => String::new(),
                Value::String(value) => value.clone(),
                _ => js_string(value).as_str().unwrap_or_default().to_owned(),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    };
    Value::String(string)
}

/// Format an f64 using JavaScript's `String(number)` notation thresholds.
/// Rust's debug formatter supplies the shortest round-tripping significand;
/// this expands or exponentiates it using ECMAScript's fixed range of 1e-6
/// through values below 1e21.
fn js_number_string(number: f64) -> String {
    if number == 0.0 {
        // JavaScript String(-0) is "0".
        return "0".to_owned();
    }

    let raw = format!("{number:?}");
    let (negative, unsigned) = match raw.strip_prefix('-') {
        Some(unsigned) => (true, unsigned),
        None => (false, raw.as_str()),
    };
    let (mantissa, exponent) = raw
        .split_once('e')
        .or_else(|| raw.split_once('E'))
        .map(|(mantissa, exponent)| (mantissa.strip_prefix('-').unwrap_or(mantissa), exponent))
        .unwrap_or((unsigned, "0"));
    let exponent = exponent.parse::<i32>().unwrap_or(0);
    let decimal_position = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let raw_digits = mantissa.replace('.', "");
    let leading_zeroes = raw_digits.bytes().take_while(|byte| *byte == b'0').count() as i32;
    let digits = raw_digits.trim_start_matches('0').trim_end_matches('0');
    if digits.is_empty() {
        return "0".to_owned();
    }

    let decimal_position = decimal_position + exponent - leading_zeroes;
    let scientific_exponent = decimal_position - 1;
    let sign = if negative { "-" } else { "" };

    if scientific_exponent >= 21 || scientific_exponent <= -7 {
        let coefficient = if digits.len() == 1 {
            digits.to_owned()
        } else {
            format!("{}.{}", &digits[..1], &digits[1..])
        };
        format!(
            "{sign}{coefficient}e{}{scientific_exponent}",
            if scientific_exponent >= 0 { "+" } else { "" }
        )
    } else if decimal_position <= 0 {
        format!(
            "{sign}0.{}{}",
            "0".repeat((-decimal_position) as usize),
            digits
        )
    } else if decimal_position as usize >= digits.len() {
        format!(
            "{sign}{digits}{}",
            "0".repeat(decimal_position as usize - digits.len())
        )
    } else {
        let split_at = decimal_position as usize;
        format!("{sign}{}.{}", &digits[..split_at], &digits[split_at..])
    }
}

/// Drop schema metadata rejected by some gateways and relax
/// `additionalProperties: false` only where the object declares optional
/// properties. Client-side validation remains responsible for source-schema
/// enforcement.
pub fn relax_schema_for_function_calling(schema: &Value) -> Value {
    match schema {
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(relax_schema_for_function_calling)
                .collect(),
        ),
        Value::Object(source) => {
            let properties = source.get("properties").and_then(Value::as_object);
            let required = source
                .get("required")
                .and_then(Value::as_array)
                .map(|required| {
                    required
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let has_optional_properties = properties.is_some_and(|properties| {
                properties
                    .keys()
                    .any(|property| !required.contains(&property.as_str()))
            });

            let mut target = Map::with_capacity(source.len());
            for (key, value) in source {
                if matches!(key.as_str(), "$schema" | "$id") {
                    continue;
                }
                if key == "additionalProperties"
                    && value == &Value::Bool(false)
                    && has_optional_properties
                {
                    continue;
                }
                if matches!(key.as_str(), "properties" | "$defs" | "definitions") {
                    if let Some(map) = value.as_object() {
                        let mut converted = Map::with_capacity(map.len());
                        for (map_key, map_value) in map {
                            converted.insert(
                                map_key.clone(),
                                relax_schema_for_function_calling(map_value),
                            );
                        }
                        target.insert(key.clone(), Value::Object(converted));
                    } else {
                        target.insert(key.clone(), relax_schema_for_function_calling(value));
                    }
                    continue;
                }
                target.insert(key.clone(), relax_schema_for_function_calling(value));
            }
            Value::Object(target)
        }
        _ => schema.clone(),
    }
}

/// Convert a Gemini `ToolListUnion` value into OpenAI Chat Completions tool
/// definitions. Callable tools must be resolved by the Rust tool registry
/// before they reach this serialization boundary.
pub fn convert_gemini_tools_to_openai(
    tools: &Value,
    schema_compliance: SchemaComplianceMode,
) -> Vec<Value> {
    let Some(tools) = tools.as_array() else {
        return Vec::new();
    };
    let mut converted = Vec::new();

    for tool in tools {
        let Some(functions) = tool.get("functionDeclarations").and_then(Value::as_array) else {
            continue;
        };
        for function in functions {
            let Some(name) = function.get("name").and_then(Value::as_str) else {
                continue;
            };
            if name.is_empty() {
                continue;
            }

            let mut parameters = function
                .get("parametersJsonSchema")
                .filter(|value| !value.is_null())
                .map(|value| {
                    value
                        .as_object()
                        .map(|object| Value::Object(object.clone()))
                        .unwrap_or_else(|| value.clone())
                })
                .or_else(|| {
                    function
                        .get("parameters")
                        .filter(|value| !value.is_null())
                        .map(convert_gemini_tool_parameters_to_openai)
                });

            if let Some(schema) = parameters.take() {
                let schema = convert_schema(&schema, schema_compliance);
                let schema = relax_schema_for_function_calling(&schema);
                parameters = Some(schema);
            }

            let mut function_value = Map::new();
            function_value.insert("name".to_owned(), Value::String(name.to_owned()));
            function_value.insert(
                "description".to_owned(),
                function
                    .get("description")
                    .filter(|value| !value.is_null())
                    .cloned()
                    .unwrap_or_else(|| Value::String(String::new())),
            );
            if let Some(parameters) = parameters {
                function_value.insert("parameters".to_owned(), parameters);
            }

            converted.push(serde_json::json!({
                "type": "function",
                "function": function_value,
            }));
        }
    }
    converted
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn converts_gemini_types_and_numeric_constraints_recursively() {
        let original = json!({
            "type": "OBJECT",
            "properties": {
                "maximum": {"type": "INTEGER", "minimum": "5"},
                "minItems": {"type": "STRING"},
                "amount": {"type": "number", "minimum": "0", "multipleOf": "0.5"},
                "count": {"type": "integer", "minLength": "1e2"},
                "bad": {"type": "string", "maxLength": "1.5"}
            }
        });
        let converted = convert_gemini_tool_parameters_to_openai(&original);
        assert_eq!(
            converted,
            json!({
                "type": "object",
                "properties": {
                    "maximum": {"type": "integer", "minimum": 5},
                    "minItems": {"type": "string"},
                    "amount": {"type": "number", "minimum": 0, "multipleOf": 0.5},
                    "count": {"type": "integer", "minLength": 100},
                    "bad": {"type": "string", "maxLength": "1.5"}
                }
            })
        );
        assert_eq!(original["type"], "OBJECT");
    }

    #[test]
    fn converts_openapi_30_union_const_enum_and_tuple_schema() {
        let schema = json!({
            "$schema": "draft",
            "type": ["string", "null"],
            "const": "ready",
            "enum": ["ready", 2, true],
            "items": [{"type": "string"}, {"type": "integer"}],
            "properties": {
                "type": {"type": ["integer", "null"]},
                "default": {"const": 4},
                "nested": {"type": "object", "default": "discard", "properties": {"x": {"type": "string"}}}
            }
        });
        assert_eq!(
            convert_schema(&schema, SchemaComplianceMode::OpenApi30),
            json!({
                "type": "string",
                "nullable": true,
                "enum": ["ready", "2", "true"],
                "properties": {
                    "type": {"type": "integer", "nullable": true},
                    "default": {"enum": ["4"]},
                    "nested": {"type": "object", "properties": {"x": {"type": "string"}}}
                }
            })
        );
    }

    #[test]
    fn convert_schema_auto_preserves_all_schema_values() {
        let schema = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": ["string", "null"],
            "items": [{"type": "string"}, {"type": "number"}],
            "enum": [1, 2, "3"],
            "exclusiveMinimum": 10,
            "default": "kept in auto mode"
        });
        assert_eq!(convert_schema(&schema, SchemaComplianceMode::Auto), schema);
    }

    #[test]
    fn converts_openapi_30_union_fallbacks_exclusive_limits_and_const_values() {
        let schema = json!({
            "type": "object",
            "properties": {
                "fallback": {"type": ["string", "number"]},
                "nullable_fallback": {"type": ["null", "object", "string"]},
                "all_null": {"type": ["null"]},
                "minimum": {"type": "number", "exclusiveMinimum": 10},
                "maximum": {"type": "number", "exclusiveMaximum": 20},
                "draft4_min": {"type": "number", "minimum": 2, "exclusiveMinimum": true},
                "draft4_max": {"type": "number", "maximum": 3, "exclusiveMaximum": true},
                "const_string": {"const": "foo"},
                "const_number": {"const": 5},
                "const_boolean": {"const": true},
                "const_zero": {"type": "integer", "const": 0},
                "const_negative_zero": {"const": -0.0},
                "const_large_fixed": {"const": 1e20},
                "const_large_exponent": {"const": 1e21},
                "const_small_fixed": {"const": 1e-6},
                "const_small_exponent": {"const": 1e-7},
                "enum": {"enum": [1, 2, "3"]}
            }
        });
        let expected = json!({
            "type": "object",
            "properties": {
                "fallback": {"type": "string"},
                "nullable_fallback": {"type": "object", "nullable": true},
                "all_null": {"type": "null", "nullable": true},
                "minimum": {"type": "number", "minimum": 10, "exclusiveMinimum": true},
                "maximum": {"type": "number", "maximum": 20, "exclusiveMaximum": true},
                "draft4_min": {"type": "number", "minimum": 2, "exclusiveMinimum": true},
                "draft4_max": {"type": "number", "maximum": 3, "exclusiveMaximum": true},
                "const_string": {"enum": ["foo"]},
                "const_number": {"enum": ["5"]},
                "const_boolean": {"enum": ["true"]},
                "const_zero": {"type": "integer", "enum": ["0"]},
                "const_negative_zero": {"enum": ["0"]},
                "const_large_fixed": {"enum": ["100000000000000000000"]},
                "const_large_exponent": {"enum": ["1e+21"]},
                "const_small_fixed": {"enum": ["0.000001"]},
                "const_small_exponent": {"enum": ["1e-7"]},
                "enum": {"enum": ["1", "2", "3"]}
            }
        });
        let converted = convert_schema(&schema, SchemaComplianceMode::OpenApi30);
        assert_eq!(converted, expected);
        assert_eq!(
            convert_schema(&converted, SchemaComplianceMode::OpenApi30),
            converted
        );
    }

    #[test]
    fn converts_schema_maps_and_filters_unsupported_fields_at_schema_levels() {
        let schema = json!({
            "$schema": "draft-07",
            "$id": "root",
            "type": "object",
            "default": "discard",
            "dependencies": {"x": ["y"]},
            "patternProperties": {"^x": {"type": "string"}},
            "properties": {
                "type": {"type": ["string", "null"]},
                "const": {"type": "string", "enum": [1, 2]},
                "default": {"type": "boolean"},
                "enum": {"type": ["integer", "null"]},
                "items": {"type": "string"},
                "$schema": {"type": "string"},
                "$id": {"type": "string"},
                "dependencies": {"type": "string"},
                "patternProperties": {"type": "string"}
            },
            "$defs": {"const": {"type": ["string", "null"]}},
            "definitions": {"default": {"type": ["number", "null"]}}
        });
        assert_eq!(
            convert_schema(&schema, SchemaComplianceMode::OpenApi30),
            json!({
                "type": "object",
                "properties": {
                    "type": {"type": "string", "nullable": true},
                    "const": {"type": "string", "enum": ["1", "2"]},
                    "default": {"type": "boolean"},
                    "enum": {"type": "integer", "nullable": true},
                    "items": {"type": "string"},
                    "$schema": {"type": "string"},
                    "$id": {"type": "string"},
                    "dependencies": {"type": "string"},
                    "patternProperties": {"type": "string"}
                },
                "$defs": {"const": {"type": "string", "nullable": true}},
                "definitions": {"default": {"type": "number", "nullable": true}}
            })
        );
    }

    #[test]
    fn relaxes_only_object_levels_with_optional_properties() {
        let schema = json!({
            "$schema": "draft",
            "$id": "schema-id",
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "optional": {"type": "string", "additionalProperties": false},
                "additionalProperties": {"type": "boolean"}
            },
            "required": ["additionalProperties"]
        });
        assert_eq!(
            relax_schema_for_function_calling(&schema),
            json!({
                "type": "object",
                "properties": {
                    "optional": {"type": "string", "additionalProperties": false},
                    "additionalProperties": {"type": "boolean"}
                },
                "required": ["additionalProperties"]
            })
        );
    }

    #[test]
    fn schema_relaxation_keeps_strict_and_empty_objects_and_handles_nested_levels() {
        let strict = json!({
            "type": "object",
            "properties": {"a": {"type": "string"}, "b": {"type": "number"}},
            "required": ["a", "b"],
            "additionalProperties": false
        });
        assert_eq!(
            relax_schema_for_function_calling(&strict)["additionalProperties"],
            false
        );
        let empty = json!({"type": "object", "additionalProperties": false});
        assert_eq!(
            relax_schema_for_function_calling(&empty)["additionalProperties"],
            false
        );

        let nested = json!({
            "type": "object",
            "properties": {
                "optional_inner": {
                    "type": "object",
                    "properties": {"x": {"type": "string"}, "y": {"type": "string"}},
                    "required": ["x"],
                    "additionalProperties": false
                },
                "strict_inner": {
                    "type": "object",
                    "properties": {"z": {"type": "string"}},
                    "required": ["z"],
                    "additionalProperties": false
                }
            },
            "required": ["optional_inner", "strict_inner"],
            "additionalProperties": false
        });
        let relaxed = relax_schema_for_function_calling(&nested);
        assert_eq!(relaxed["additionalProperties"], false);
        assert!(
            relaxed["properties"]["optional_inner"]
                .get("additionalProperties")
                .is_none()
        );
        assert_eq!(
            relaxed["properties"]["strict_inner"]["additionalProperties"],
            false
        );
    }

    #[test]
    fn schema_relaxation_preserves_schema_maps_and_recurses_into_additional_schemas() {
        let schema = json!({
            "$schema": "draft-07",
            "$id": "https://example.com/tool.schema.json",
            "type": "object",
            "properties": {
                "$schema": {"type": "string"},
                "$id": {"type": "string"},
                "additionalProperties": {"type": "boolean"}
            },
            "required": ["$schema", "$id", "additionalProperties"],
            "additionalProperties": false,
            "$defs": {"$schema": {"type": "number"}},
            "definitions": {"$id": {"type": "integer"}}
        });
        let original = schema.clone();
        let relaxed = relax_schema_for_function_calling(&schema);
        assert_eq!(schema, original);
        assert!(relaxed.get("$schema").is_none());
        assert!(relaxed.get("$id").is_none());
        assert!(relaxed["properties"].get("$schema").is_some());
        assert!(relaxed["properties"].get("$id").is_some());
        assert!(relaxed["properties"].get("additionalProperties").is_some());
        assert_eq!(relaxed["additionalProperties"], false);
        assert!(relaxed["$defs"].get("$schema").is_some());
        assert!(relaxed["definitions"].get("$id").is_some());

        let schema_with_additional_schema = json!({
            "type": "object",
            "properties": {"a": {"type": "string"}},
            "additionalProperties": {
                "type": "object",
                "properties": {"inner": {"type": "string"}},
                "required": [],
                "additionalProperties": false
            }
        });
        let relaxed = relax_schema_for_function_calling(&schema_with_additional_schema);
        assert!(relaxed["additionalProperties"].is_object());
        assert!(
            relaxed["additionalProperties"]
                .get("additionalProperties")
                .is_none()
        );
    }

    #[test]
    fn converts_gemini_and_mcp_function_declarations_without_mutating_source() {
        let tools = json!([
            {"functionDeclarations": [
                {"name": "read", "parameters": {"type": "OBJECT", "properties": {"path": {"type": "STRING"}}}},
                {"name": "schema", "description": "MCP", "parametersJsonSchema": {"type": "object", "properties": {"x": {"type": "string"}}}},
                {"name": "no_schema"},
                {"name": ""}
            ]},
            {"functionDeclarations": [{"description": "missing name"}]},
            {"codeExecution": {}}
        ]);
        let converted = convert_gemini_tools_to_openai(&tools, SchemaComplianceMode::Auto);
        assert_eq!(converted.len(), 3);
        assert_eq!(converted[0]["function"]["name"], "read");
        assert_eq!(converted[0]["function"]["description"], "");
        assert_eq!(converted[0]["function"]["parameters"]["type"], "object");
        assert_eq!(converted[1]["function"]["description"], "MCP");
        assert_eq!(
            converted[1]["function"]["parameters"],
            json!({"type": "object", "properties": {"x": {"type": "string"}}})
        );
        assert_eq!(converted[2]["function"]["parameters"], Value::Null);
        assert_eq!(
            tools[0]["functionDeclarations"][0]["parameters"]["type"],
            "OBJECT"
        );
    }
}
