# Feishu question card Rust port

`feishu_question_card.rs` ports the pure card construction, terminal-state
projection, callback action parsing, correlation extraction, and answer
validation from `packages/channels/feishu/src/question-card.ts`. Card generation
preserves the Card V2 form/select/button JSON, Chinese button and terminal labels,
Markdown descriptions, and ordering. Callback parsing supports Feishu's omitted
button-value fallback from the action name, nested context precedence, and
optional operator/chat/message/form fields. Answer parsing checks the exact key
set, accepted labels, multi-select arrays or JSON-array strings, and duplicates,
then normalizes multi-select answers with `", "`.

Focused unit tests cover the source test cases and malformed action/value forms.
The Feishu adapter, callback ingress, question lifecycle/response wiring, and
remote Card API calls remain outside this pure helper port. The Rust API uses
typed `FeishuQuestion` data and `serde_json::Value`; callers must adapt the shared
channel context/types. As in JavaScript, this helper assumes JSON-compatible
callback values; non-JSON values and duplicate object keys cannot be represented
after deserialization.
