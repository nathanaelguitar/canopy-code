//! Restore the most recent authoritative or estimated prompt-token count.
//!
//! This mirrors `packages/core/src/services/session-resume-token-counts.ts`.

use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResumeTokenCounts {
    pub prompt_token_count: f64,
    pub output_token_count: f64,
    pub is_estimated: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ResumeTokenCountsAccumulator {
    value: Option<ResumeTokenCounts>,
}

fn js_truthy_number(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_f64)
        .is_some_and(|number| number != 0.0 && !number.is_nan())
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null | Value::Bool(false) => false,
        Value::Bool(true) | Value::Array(_) | Value::Object(_) => true,
        Value::String(value) => !value.is_empty(),
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
    }
}

fn numeric(value: Option<&Value>) -> Option<f64> {
    value.and_then(Value::as_f64)
}

/// Compute output tokens while avoiding candidate/thought overlap when total
/// usage is unavailable.
pub fn usage_output_token_count_for_prompt_estimate(usage: Option<&Value>) -> f64 {
    let Some(usage) = usage else {
        return 0.0;
    };
    let Some(prompt_count) = usage.get("promptTokenCount") else {
        return 0.0;
    };
    let prompt_count = numeric(Some(prompt_count)).unwrap_or_default();
    if let Some(total_count) = usage.get("totalTokenCount") {
        return (numeric(Some(total_count)).unwrap_or_default() - prompt_count).max(0.0);
    }

    let candidates = numeric(usage.get("candidatesTokenCount"))
        .unwrap_or_default()
        .max(0.0);
    let thoughts = numeric(usage.get("thoughtsTokenCount"))
        .unwrap_or_default()
        .max(0.0);
    if candidates > thoughts {
        candidates
    } else {
        candidates + thoughts
    }
}

impl ResumeTokenCountsAccumulator {
    pub fn add(&mut self, record: &Value) {
        match record.get("type").and_then(Value::as_str) {
            Some("assistant") => {
                let usage = record.get("usageMetadata");
                // Nullish coalescing: a zero prompt count does not fall back
                // to totalTokenCount, even though zero itself is not a
                // candidate for replacing the prior resume estimate.
                let candidate = usage
                    .and_then(|usage| usage.get("promptTokenCount"))
                    .filter(|value| !value.is_null())
                    .or_else(|| usage.and_then(|usage| usage.get("totalTokenCount")));
                if js_truthy_number(candidate) {
                    let prompt_token_count = numeric(candidate).unwrap_or_default();
                    self.value = Some(ResumeTokenCounts {
                        prompt_token_count,
                        output_token_count: usage_output_token_count_for_prompt_estimate(usage),
                        is_estimated: false,
                    });
                }
            }
            Some("system")
                if record.get("subtype").and_then(Value::as_str) == Some("chat_compression") =>
            {
                let Some(info) = record
                    .get("systemPayload")
                    .and_then(|payload| payload.get("info"))
                    .filter(|info| js_truthy(info))
                else {
                    return;
                };
                let Some(prompt_token_count) = numeric(info.get("newTokenCount")) else {
                    return;
                };
                let is_estimated = info
                    .get("newTokenCountIsEstimated")
                    .filter(|value| !value.is_null())
                    .and_then(Value::as_bool)
                    .unwrap_or(true);
                self.value = Some(ResumeTokenCounts {
                    prompt_token_count,
                    output_token_count: 0.0,
                    is_estimated,
                });
            }
            _ => {}
        }
    }

    pub fn finish(&self) -> Option<ResumeTokenCounts> {
        self.value
    }
}

/// Whether a record can update the last known token count during a scan.
pub fn is_resume_token_counts_candidate(record: &Value) -> bool {
    match record.get("type").and_then(Value::as_str) {
        Some("assistant") => {
            let usage = record.get("usageMetadata");
            let candidate = usage
                .and_then(|usage| usage.get("promptTokenCount"))
                .filter(|value| !value.is_null())
                .or_else(|| usage.and_then(|usage| usage.get("totalTokenCount")));
            js_truthy_number(candidate)
        }
        Some("system")
            if record.get("subtype").and_then(Value::as_str) == Some("chat_compression") =>
        {
            record
                .get("systemPayload")
                .and_then(|payload| payload.get("info"))
                .is_some()
        }
        _ => false,
    }
}

pub fn get_resume_token_counts(records: &[Value]) -> Option<ResumeTokenCounts> {
    let mut accumulator = ResumeTokenCountsAccumulator::default();
    for record in records {
        accumulator.add(record);
    }
    accumulator.finish()
}

pub fn get_resume_prompt_token_count(records: &[Value]) -> Option<f64> {
    get_resume_token_counts(records).map(|counts| counts.prompt_token_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn compression(estimated: Option<bool>) -> Value {
        let mut info = json!({"newTokenCount":300});
        if let Some(estimated) = estimated {
            info["newTokenCountIsEstimated"] = json!(estimated);
        }
        json!({"type":"system","subtype":"chat_compression","systemPayload":{"info":info}})
    }

    #[test]
    fn assistant_usage_supersedes_checkpoint_and_prefers_prompt_count() {
        let records = vec![
            compression(Some(true)),
            json!({"type":"assistant","usageMetadata":{"promptTokenCount":200,"totalTokenCount":450}}),
        ];
        assert_eq!(
            get_resume_token_counts(&records),
            Some(ResumeTokenCounts {
                prompt_token_count: 200.0,
                output_token_count: 250.0,
                is_estimated: false,
            })
        );
    }

    #[test]
    fn total_count_is_used_when_prompt_count_is_missing() {
        let records = vec![json!({"type":"assistant","usageMetadata":{"totalTokenCount":450}})];
        assert_eq!(get_resume_prompt_token_count(&records), Some(450.0));
        assert_eq!(
            get_resume_token_counts(&records)
                .unwrap()
                .output_token_count,
            0.0
        );
    }

    #[test]
    fn zero_usage_does_not_replace_the_compression_checkpoint() {
        let records = vec![
            compression(None),
            json!({"type":"assistant","usageMetadata":{"totalTokenCount":0,"promptTokenCount":0}}),
        ];
        assert_eq!(
            get_resume_token_counts(&records),
            Some(ResumeTokenCounts {
                prompt_token_count: 300.0,
                output_token_count: 0.0,
                is_estimated: true,
            })
        );
    }

    #[test]
    fn restores_candidate_and_reasoning_output_without_double_counting() {
        assert_eq!(
            usage_output_token_count_for_prompt_estimate(Some(&json!({
                "promptTokenCount":100,
                "candidatesTokenCount":150,
                "thoughtsTokenCount":120
            }))),
            150.0
        );
        assert_eq!(
            usage_output_token_count_for_prompt_estimate(Some(&json!({
                "promptTokenCount":100,
                "candidatesTokenCount":50,
                "thoughtsTokenCount":120
            }))),
            170.0
        );
    }

    #[test]
    fn legacy_checkpoint_defaults_to_estimated_but_explicit_false_is_authoritative() {
        assert!(
            get_resume_token_counts(&[compression(None)])
                .unwrap()
                .is_estimated
        );
        assert!(
            !get_resume_token_counts(&[compression(Some(false))])
                .unwrap()
                .is_estimated
        );
        assert!(is_resume_token_counts_candidate(&compression(None)));
    }
}
