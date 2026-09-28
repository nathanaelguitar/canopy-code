use std::collections::HashMap;

use serde_json::{Value, json};

use crate::tool_response_finalizer::ToolExecutionOutput;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuestionOption {
    pub label: String,
    pub description: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Question {
    pub question: String,
    pub header: String,
    pub options: Vec<QuestionOption>,
    pub multi_select: bool,
}

pub fn parse_questions(args: &Value) -> Result<Vec<Question>, String> {
    let values = args
        .get("questions")
        .and_then(Value::as_array)
        .ok_or_else(|| "Parameter \"questions\" must be an array.".to_owned())?;
    if !(1..=4).contains(&values.len()) {
        return Err("Parameter \"questions\" must contain between 1 and 4 questions.".to_owned());
    }

    let mut questions = Vec::with_capacity(values.len());
    for (question_index, value) in values.iter().enumerate() {
        let question_number = question_index + 1;
        let object = value
            .as_object()
            .ok_or_else(|| format!("Question {question_number}: must be an object."))?;
        let question = non_empty_string(object.get("question")).ok_or_else(|| {
            format!("Question {question_number}: \"question\" must be a non-empty string.")
        })?;
        let header = non_empty_string(object.get("header")).ok_or_else(|| {
            format!("Question {question_number}: \"header\" must be a non-empty string.")
        })?;
        let options_value = object
            .get("options")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("Question {question_number}: \"options\" must be an array."))?;
        if !(2..=4).contains(&options_value.len()) {
            return Err(format!(
                "Question {question_number}: \"options\" must contain between 2 and 4 options."
            ));
        }
        let mut options = Vec::with_capacity(options_value.len());
        for (option_index, value) in options_value.iter().enumerate() {
            let option_number = option_index + 1;
            let option = value.as_object().ok_or_else(|| {
                format!("Question {question_number}, Option {option_number}: must be an object.")
            })?;
            let label = non_empty_string(option.get("label")).ok_or_else(|| {
                format!(
                    "Question {question_number}, Option {option_number}: \"label\" must be a non-empty string."
                )
            })?;
            let description = non_empty_string(option.get("description")).ok_or_else(|| {
                format!(
                    "Question {question_number}, Option {option_number}: \"description\" must be a non-empty string."
                )
            })?;
            options.push(QuestionOption {
                label: label.to_owned(),
                description: description.to_owned(),
            });
        }
        let multi_select = match object.get("multiSelect") {
            None => false,
            Some(Value::Bool(value)) => *value,
            Some(_) => {
                return Err(format!(
                    "Question {question_number}: \"multiSelect\" must be a boolean."
                ));
            }
        };
        questions.push(Question {
            question: question.to_owned(),
            header: header.to_owned(),
            options,
            multi_select,
        });
    }
    Ok(questions)
}

pub fn answer_result(
    questions: &[Question],
    answers: &HashMap<String, String>,
    answered: bool,
) -> ToolExecutionOutput {
    if !answered {
        return ToolExecutionOutput::text("User declined to answer the questions.");
    }
    let answers_content = questions
        .iter()
        .enumerate()
        .filter_map(|(index, question)| {
            let value = answers.get(&index.to_string())?;
            let header = if question.header.is_empty() {
                format!("Question {}", index + 1)
            } else {
                question.header.clone()
            };
            Some(format!("**{header}**: {value}"))
        })
        .collect::<Vec<_>>()
        .join("\n");
    let body = if answers_content.is_empty() {
        "No valid answers were provided.".to_owned()
    } else {
        answers_content
    };
    ToolExecutionOutput::text(format!(
        "User has provided the following answers:\n\n{body}"
    ))
}

fn non_empty_string(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

pub fn function_declaration() -> Value {
    json!({
        "name":"ask_user_question",
        "description":"Use this tool when you need to ask the user questions during execution. This allows you to:\n1. Gather user preferences or requirements\n2. Clarify ambiguous instructions\n3. Get decisions on implementation choices as you work\n4. Offer choices to the user about what direction to take.\n\nUsage notes:\n- Users will always be able to select \"Other\" to provide custom text input\n- Use multiSelect: true to allow multiple answers to be selected for a question\n- If you recommend a specific option, make that the first option in the list and add \"(Recommended)\" at the end of the label\n\nPlan mode note: In plan mode, use this tool to clarify requirements or choose between approaches BEFORE finalizing your plan. Do NOT use this tool to ask \"Is this plan ready?\" or \"Should I proceed?\" - use ExitPlanMode for plan approval.",
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "questions":{
                    "description":"Questions to ask the user (1-4 questions)",
                    "minItems":1,
                    "maxItems":4,
                    "type":"ARRAY",
                    "items":{
                        "type":"OBJECT",
                        "properties":{
                            "question":{
                                "description":"The complete question to ask the user. It should be clear, specific, and end with a question mark. If multiSelect is true, phrase it accordingly.",
                                "type":"STRING"
                            },
                            "header":{
                                "description":"A very short label displayed as a chip or tag. Keep it within 12 characters when practical.",
                                "type":"STRING",
                                "maxLength":12
                            },
                            "options":{
                                "description":"Two to four distinct choices. The UI adds an Other option for custom text.",
                                "type":"ARRAY",
                                "minItems":2,
                                "maxItems":4,
                                "items":{
                                    "type":"OBJECT",
                                    "properties":{
                                        "label":{"description":"Concise display text for the choice.","type":"STRING"},
                                        "description":{"description":"Explain the option's effect or tradeoff.","type":"STRING"}
                                    },
                                    "required":["label","description"],
                                    "additionalProperties":false
                                }
                            },
                            "multiSelect":{
                                "description":"Allow the user to select multiple options when the choices are not mutually exclusive.",
                                "default":false,
                                "type":"BOOLEAN"
                            }
                        },
                        "required":["question","header","options"],
                        "additionalProperties":false
                    }
                },
                "metadata":{
                    "description":"Optional metadata for tracking and analytics. It is not shown to the user.",
                    "type":"OBJECT",
                    "properties":{"source":{"type":"STRING"}},
                    "additionalProperties":false
                }
            },
            "required":["questions"],
            "additionalProperties":false
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_question() -> Value {
        json!({
            "questions":[{
                "question":"Choose an approach?",
                "header":"Approach",
                "options":[
                    {"label":"A","description":"First choice"},
                    {"label":"B","description":"Second choice"}
                ]
            }]
        })
    }

    #[test]
    fn validates_question_counts_fields_and_option_counts() {
        assert_eq!(parse_questions(&valid_question()).unwrap().len(), 1);
        for invalid in [
            json!({"questions":[]}),
            json!({"questions":[{"question":"Q?","header":"H","options":[{"label":"A","description":"A"}]}]}),
            json!({"questions":[{"question":"Q?","header":"H","options":[{"label":"A","description":"A"},{"label":"B","description":"B"}],"multiSelect":"yes"}]}),
        ] {
            assert!(parse_questions(&invalid).is_err());
        }
        let too_many = json!({"questions": [
            {"question":"Q?","header":"H","options":[{"label":"A","description":"A"},{"label":"B","description":"B"}]},
            {"question":"Q?","header":"H","options":[{"label":"A","description":"A"},{"label":"B","description":"B"}]},
            {"question":"Q?","header":"H","options":[{"label":"A","description":"A"},{"label":"B","description":"B"}]},
            {"question":"Q?","header":"H","options":[{"label":"A","description":"A"},{"label":"B","description":"B"}]},
            {"question":"Q?","header":"H","options":[{"label":"A","description":"A"},{"label":"B","description":"B"}]}
        ]});
        assert!(
            parse_questions(&too_many)
                .unwrap_err()
                .contains("between 1 and 4")
        );
    }

    #[test]
    fn answer_formatter_accepts_only_canonical_in_range_indexes() {
        let questions = parse_questions(&json!({"questions":[
            {"question":"One?","header":"First","options":[{"label":"A","description":"A"},{"label":"B","description":"B"}]},
            {"question":"Two?","header":"Second","options":[{"label":"C","description":"C"},{"label":"D","description":"D"}]}
        ]})).unwrap();
        let valid = HashMap::from([
            ("0".to_owned(), "A".to_owned()),
            ("1".to_owned(), "D".to_owned()),
        ]);
        let output = answer_result(&questions, &valid, true);
        assert!(output.output.contains("**First**: A"));
        assert!(output.output.contains("**Second**: D"));
        let malformed = HashMap::from([("01".to_owned(), "D".to_owned())]);
        assert!(
            answer_result(&questions, &malformed, true)
                .output
                .contains("No valid answers were provided.")
        );
        assert_eq!(
            answer_result(&questions, &HashMap::new(), false).output,
            "User declined to answer the questions."
        );
    }

    #[test]
    fn declaration_keeps_the_tool_name_and_question_schema() {
        let declaration = function_declaration();
        assert_eq!(declaration["name"], "ask_user_question");
        assert_eq!(
            declaration["parameters"]["properties"]["questions"]["minItems"],
            1
        );
        assert_eq!(
            declaration["parameters"]["properties"]["questions"]["maxItems"],
            4
        );
    }
}
