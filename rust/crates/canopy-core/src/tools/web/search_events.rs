// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Typed response items and pure WebSearch result collection.
//!
//! Provider stream parsing and transport are owned by the host. This module
//! deserializes the final response/item shapes and applies the item projection
//! rules shared by completed and recoverable partial responses.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WsAction {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queries: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<WsSource>>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WsSource {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WsContentPart {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

/// A Responses API output item. Unknown provider-specific fields are ignored.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WsOutputItem {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<WsAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub urls: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Vec<WsContentPart>>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WsToolUsage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WsToolUsageBreakdown {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub web_search: Option<WsToolUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub web_extractor: Option<WsToolUsage>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WsUsage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x_tools: Option<WsToolUsageBreakdown>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Vec<WsOutputItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<WsUsage>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CollectedSearchData {
    pub executed_queries: Vec<String>,
    pub candidate_urls: Vec<String>,
    pub opened_urls: Vec<String>,
    pub answer_text: String,
    /// Number of non-failed `web_search_call` items in the output.
    pub search_call_count: usize,
    pub usage: Option<WsUsage>,
}

impl CollectedSearchData {
    /// Provider usage is authoritative when present; otherwise report the
    /// number of successful-or-unknown-status search items we collected.
    pub fn reported_search_call_count(&self) -> u64 {
        self.usage
            .as_ref()
            .and_then(|usage| usage.x_tools.as_ref())
            .and_then(|tools| tools.web_search.as_ref())
            .and_then(|search| search.count)
            .unwrap_or(self.search_call_count as u64)
    }

    /// Convert the collected fields to the existing bounded formatter's
    /// input type.
    pub fn into_projection(self) -> super::WebSearchProjection {
        super::WebSearchProjection {
            executed_queries: self.executed_queries,
            candidate_urls: self.candidate_urls,
            opened_urls: self.opened_urls,
            answer_text: self.answer_text,
        }
    }
}

/// Query precedence used for search items and live progress events: a
/// nonempty batch wins, then a truthy singular query, then the caller's
/// fallback (usually the invocation query for progress updates).
pub fn extract_queries(action: Option<&WsAction>, fallback: &[String]) -> Vec<String> {
    if let Some(queries) = action.and_then(|action| action.queries.as_ref())
        && !queries.is_empty()
    {
        return queries.clone();
    }
    if let Some(query) = action
        .and_then(|action| action.query.as_ref())
        .filter(|query| !query.is_empty())
    {
        return vec![query.clone()];
    }
    fallback.to_vec()
}

/// Collect query, URL, and answer evidence from output items. Only an
/// explicitly failed search/extractor is discounted; unknown statuses retain
/// the TypeScript implementation's conservative accounting behavior.
pub fn collect_from_items(
    items: &[WsOutputItem],
    usage: Option<WsUsage>,
    fallback_text: &str,
) -> CollectedSearchData {
    let mut executed_queries = Vec::new();
    let mut candidate_urls = Vec::new();
    let mut opened_urls = Vec::new();
    let mut message_parts = Vec::new();
    let mut extracted_parts = Vec::new();
    let mut search_call_count = 0;

    for item in items {
        match item.r#type.as_deref() {
            Some("web_search_call") => {
                if item.status.as_deref() == Some("failed") {
                    continue;
                }
                search_call_count += 1;
                executed_queries.extend(extract_queries(item.action.as_ref(), &[]));
                if let Some(sources) = item
                    .action
                    .as_ref()
                    .and_then(|action| action.sources.as_ref())
                {
                    candidate_urls.extend(
                        sources
                            .iter()
                            .filter_map(|source| source.url.as_ref())
                            .filter(|url| !url.is_empty())
                            .cloned(),
                    );
                }
            }
            Some("web_extractor_call") => {
                if item.status.as_deref() == Some("failed") {
                    continue;
                }
                if let Some(urls) = &item.urls {
                    opened_urls.extend(urls.iter().cloned());
                }
                if let Some(output) = item.output.as_ref().filter(|output| !output.is_empty()) {
                    let prefix = item
                        .goal
                        .as_ref()
                        .filter(|goal| !goal.is_empty())
                        .map(|goal| format!("[Extracted content — goal: {goal}]\n"))
                        .unwrap_or_default();
                    extracted_parts.push(format!("{prefix}{output}"));
                }
            }
            Some("message") => {
                let text = item
                    .content
                    .as_ref()
                    .into_iter()
                    .flatten()
                    .filter_map(|part| part.text.as_deref())
                    .collect::<String>();
                if !text.is_empty() {
                    message_parts.push(text);
                }
            }
            // Reasoning and unknown output-item types are intentionally
            // ignored, matching the TypeScript item collector.
            _ => {}
        }
    }

    CollectedSearchData {
        executed_queries: unique_in_order(executed_queries),
        candidate_urls: unique_in_order(candidate_urls),
        opened_urls: unique_in_order(opened_urls),
        answer_text: if !message_parts.is_empty() {
            message_parts.join("\n")
        } else if !fallback_text.is_empty() {
            fallback_text.to_owned()
        } else {
            extracted_parts.join("\n\n")
        },
        search_call_count,
        usage,
    }
}

impl WsResponse {
    /// Collect this final response, using partial stream text only when no
    /// narration item exists.
    pub fn collect(&self, fallback_text: &str) -> CollectedSearchData {
        self.collect_with_streamed_items(&[], fallback_text)
    }

    /// Use nonempty final response output when present; otherwise salvage the
    /// output items already received from the stream. This mirrors the TS
    /// collector's defensive handling of terminal responses with omitted or
    /// empty `output` arrays without parsing stream events here.
    pub fn collect_with_streamed_items(
        &self,
        streamed_items: &[WsOutputItem],
        fallback_text: &str,
    ) -> CollectedSearchData {
        let items = self
            .output
            .as_deref()
            .filter(|items| !items.is_empty())
            .unwrap_or(streamed_items);
        collect_from_items(items, self.usage.clone(), fallback_text)
    }
}

fn unique_in_order(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserializes_response_items_and_collects_provider_fixture() {
        let response: WsResponse = serde_json::from_value(json!({
            "status": "completed",
            "output": [
                {
                    "type": "web_search_call",
                    "status": "completed",
                    "action": {
                        "type": "search",
                        "query": "singular query",
                        "queries": ["first query", "second query"],
                        "sources": [
                            {"type": "url", "url": "https://example.com/a"},
                            {"type": "url", "url": "https://example.com/b"}
                        ]
                    },
                    "provider_extension": true
                },
                {
                    "type": "web_extractor_call",
                    "status": "completed",
                    "urls": ["https://example.com/a"],
                    "goal": "verify facts",
                    "output": "page content"
                },
                {
                    "type": "message",
                    "content": [{"type": "output_text", "text": "The answer is 42."}]
                }
            ],
            "usage": {"x_tools": {"web_search": {"count": 3}, "web_extractor": {"count": 1}}}
        }))
        .expect("TS-shaped response fixture should deserialize");

        let data = response.collect("");
        assert_eq!(
            data.executed_queries,
            vec!["first query".to_owned(), "second query".to_owned()]
        );
        assert_eq!(
            data.candidate_urls,
            vec![
                "https://example.com/a".to_owned(),
                "https://example.com/b".to_owned()
            ]
        );
        assert_eq!(data.opened_urls, vec!["https://example.com/a".to_owned()]);
        assert_eq!(data.answer_text, "The answer is 42.");
        assert_eq!(data.search_call_count, 1);
        assert_eq!(data.reported_search_call_count(), 3);
    }

    #[test]
    fn query_fallback_prefers_batch_then_singular_then_invocation_query() {
        let fallback = vec!["invocation query".to_owned()];
        let batch = WsAction {
            query: Some("singular".into()),
            queries: Some(vec!["batch one".into(), "batch two".into()]),
            ..WsAction::default()
        };
        assert_eq!(
            extract_queries(Some(&batch), &fallback),
            vec!["batch one".to_owned(), "batch two".to_owned()]
        );

        let singular = WsAction {
            query: Some("singular".into()),
            queries: Some(Vec::new()),
            ..WsAction::default()
        };
        assert_eq!(
            extract_queries(Some(&singular), &fallback),
            vec!["singular".to_owned()]
        );

        let empty = WsAction {
            query: Some(String::new()),
            ..WsAction::default()
        };
        assert_eq!(extract_queries(Some(&empty), &fallback), fallback);
    }

    #[test]
    fn failed_items_are_excluded_and_unique_values_keep_first_occurrence() {
        let items: Vec<WsOutputItem> = serde_json::from_value(json!([
            {
                "type": "web_search_call", "status": "failed",
                "action": {"queries": ["discard query"], "sources": [{"url": "https://failed.test"}]}
            },
            {
                "type": "web_search_call",
                "action": {"queries": ["query", "query"], "sources": [
                    {"url": "https://example.com/a"}, {"url": "https://example.com/a"}
                ]}
            },
            {
                "type": "web_extractor_call", "status": "failed",
                "urls": ["https://example.com/failed"], "output": "ignore me"
            },
            {
                "type": "web_extractor_call", "status": "completed",
                "urls": ["https://example.com/open", "https://example.com/open"]
            }
        ]))
        .expect("TS-shaped output items should deserialize");

        let data = collect_from_items(&items, None, "");
        assert_eq!(data.search_call_count, 1);
        assert_eq!(data.executed_queries, vec!["query".to_owned()]);
        assert_eq!(
            data.candidate_urls,
            vec!["https://example.com/a".to_owned()]
        );
        assert_eq!(
            data.opened_urls,
            vec!["https://example.com/open".to_owned()]
        );
        assert!(data.answer_text.is_empty());
        assert_eq!(data.reported_search_call_count(), 1);
    }

    #[test]
    fn narration_then_partial_text_then_extracted_content_is_the_answer_fallback_order() {
        let items: Vec<WsOutputItem> = serde_json::from_value(json!([
            {"type": "web_extractor_call", "goal": "check claim", "output": "page text"},
            {"type": "message", "content": [{"text": "narrated"}]},
            {"type": "message", "content": [{"text": " answer"}]}
        ]))
        .expect("TS-shaped output items should deserialize");

        assert_eq!(
            collect_from_items(&items, None, "partial").answer_text,
            "narrated\n answer"
        );

        let without_narration = &items[..1];
        assert_eq!(
            collect_from_items(without_narration, None, "partial").answer_text,
            "partial"
        );
        assert_eq!(
            collect_from_items(without_narration, None, "").answer_text,
            "[Extracted content — goal: check claim]\npage text"
        );
    }

    #[test]
    fn empty_final_output_falls_back_to_streamed_items_but_nonempty_output_wins() {
        let streamed: Vec<WsOutputItem> = serde_json::from_value(json!([
            {"type": "web_search_call", "action": {"query": "streamed query"}}
        ]))
        .expect("streamed item should deserialize");
        let empty_response = WsResponse {
            output: Some(Vec::new()),
            ..WsResponse::default()
        };
        assert_eq!(
            empty_response
                .collect_with_streamed_items(&streamed, "")
                .executed_queries,
            vec!["streamed query".to_owned()]
        );

        let final_items: Vec<WsOutputItem> = serde_json::from_value(json!([
            {"type": "web_search_call", "action": {"query": "final query"}}
        ]))
        .expect("final item should deserialize");
        let final_response = WsResponse {
            output: Some(final_items),
            ..WsResponse::default()
        };
        assert_eq!(
            final_response
                .collect_with_streamed_items(&streamed, "")
                .executed_queries,
            vec!["final query".to_owned()]
        );
    }
}
