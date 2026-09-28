//! Model-family input modality and wire-capability defaults.

use std::sync::OnceLock;

use regex::Regex;

use crate::providers::openai_request::InputModalities;
use crate::token_limits::normalize;

const FULL_MULTIMODAL: InputModalities = InputModalities {
    image: true,
    pdf: true,
    audio: true,
    video: true,
};

fn modality_rules() -> &'static [(Regex, InputModalities)] {
    static RULES: OnceLock<Vec<(Regex, InputModalities)>> = OnceLock::new();
    RULES.get_or_init(|| {
        [
            (r"^gemini-3", FULL_MULTIMODAL),
            (r"^gemini-", FULL_MULTIMODAL),
            (
                r"^gpt-5",
                InputModalities {
                    image: true,
                    ..empty()
                },
            ),
            (
                r"^gpt-",
                InputModalities {
                    image: true,
                    ..empty()
                },
            ),
            (
                r"^o\d",
                InputModalities {
                    image: true,
                    ..empty()
                },
            ),
            (
                r"^claude-",
                InputModalities {
                    image: true,
                    pdf: true,
                    ..empty()
                },
            ),
            (
                r"^qwen3\.5-plus",
                InputModalities {
                    image: true,
                    video: true,
                    ..empty()
                },
            ),
            (
                r"^qwen3\.6-plus",
                InputModalities {
                    image: true,
                    video: true,
                    ..empty()
                },
            ),
            (
                r"^qwen3\.7-plus",
                InputModalities {
                    image: true,
                    video: true,
                    ..empty()
                },
            ),
            (
                r"^qwen3\.8-max",
                InputModalities {
                    image: true,
                    ..empty()
                },
            ),
            (
                r"^coder-model$",
                InputModalities {
                    image: true,
                    video: true,
                    ..empty()
                },
            ),
            (
                r"^qwen-vl-",
                InputModalities {
                    image: true,
                    video: true,
                    ..empty()
                },
            ),
            (
                r"^qwen3-vl-",
                InputModalities {
                    image: true,
                    video: true,
                    ..empty()
                },
            ),
            (r"^qwen3-coder-", empty()),
            (
                r"^qwen3\.6-35b",
                InputModalities {
                    image: true,
                    video: true,
                    ..empty()
                },
            ),
            (
                r"^qwen3\.8-27b",
                InputModalities {
                    image: true,
                    video: true,
                    ..empty()
                },
            ),
            (
                r"^qwen3\.8-flash-next",
                InputModalities {
                    image: true,
                    ..empty()
                },
            ),
            (r"^qwen", empty()),
            (r"^deepseek", empty()),
            (
                r"^glm-4\.5v",
                InputModalities {
                    image: true,
                    ..empty()
                },
            ),
            (r"^glm-5(?:-|$)", empty()),
            (r"^glm-", empty()),
            (
                r"^minimax-m3",
                InputModalities {
                    image: true,
                    video: true,
                    ..empty()
                },
            ),
            (r"^minimax-", empty()),
            (
                r"^kimi-k3",
                InputModalities {
                    image: true,
                    video: true,
                    ..empty()
                },
            ),
            (
                r"^kimi-k2\.",
                InputModalities {
                    image: true,
                    video: true,
                    ..empty()
                },
            ),
            (r"^kimi-", empty()),
            (r"^doubao-seed(ance|ream)", empty()),
            (
                r"^doubao-seed",
                InputModalities {
                    image: true,
                    ..empty()
                },
            ),
            (
                r"^doubao-.*(vision|vl)",
                InputModalities {
                    image: true,
                    ..empty()
                },
            ),
            (r"^doubao", empty()),
        ]
        .into_iter()
        .map(|(pattern, modalities)| {
            (
                Regex::new(pattern).expect("valid modality model pattern"),
                modalities,
            )
        })
        .collect()
    })
}

const fn empty() -> InputModalities {
    InputModalities {
        image: false,
        pdf: false,
        audio: false,
        video: false,
    }
}

/// Return the source-compatible defaults for a model ID. Unknown models are
/// text-only so unsupported media is not sent speculatively.
pub fn default_modalities(model: &str) -> InputModalities {
    let normalized = normalize(model);
    modality_rules()
        .iter()
        .find(|(pattern, _)| pattern.is_match(&normalized))
        .map_or_else(empty, |(_, modalities)| *modalities)
}

/// Canopy's OpenAI-compatible reasoning controls apply to Qwen-family wire IDs
/// and the legacy `coder-model` alias.
pub fn is_canopy_family_wire_model(model: Option<&str>) -> bool {
    model.is_some_and(|model| {
        let normalized = model.to_ascii_lowercase();
        normalized.starts_with("qwen") || normalized == "coder-model"
    })
}

pub fn is_glm_wire_model(model: Option<&str>) -> bool {
    model.is_some_and(|model| model.to_ascii_lowercase().starts_with("glm-"))
}

/// Qwen 3.8 Max accepts the tiered `reasoning_effort` request field.
pub fn is_tiered_effort_wire_model(model: Option<&str>) -> bool {
    model.is_some_and(|model| model.to_ascii_lowercase().starts_with("qwen3.8-max"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_ordered_provider_modality_defaults() {
        assert_eq!(
            default_modalities("gemini-2.5-flash"),
            InputModalities {
                image: true,
                pdf: true,
                audio: true,
                video: true,
            }
        );
        assert_eq!(
            default_modalities("qwen3.6-plus-latest"),
            InputModalities {
                image: true,
                video: true,
                ..empty()
            }
        );
        assert_eq!(default_modalities("qwen3-coder-plus"), empty());
        assert_eq!(
            default_modalities("qwen3.8-max-20260901"),
            InputModalities {
                image: true,
                ..empty()
            }
        );
        assert_eq!(
            default_modalities("glm-4.5v"),
            InputModalities {
                image: true,
                ..empty()
            }
        );
        assert_eq!(
            default_modalities("minimax-m3"),
            InputModalities {
                image: true,
                video: true,
                ..empty()
            }
        );
        assert_eq!(default_modalities("doubao-seedance"), empty());
        assert_eq!(
            default_modalities("doubao-vision-pro"),
            InputModalities {
                image: true,
                ..empty()
            }
        );
        assert_eq!(default_modalities("unknown-model"), empty());
    }

    #[test]
    fn wire_family_predicates_keep_qwen_aliases_and_tiered_gate_separate() {
        assert!(is_canopy_family_wire_model(Some("Qwen3.7-Plus")));
        assert!(is_canopy_family_wire_model(Some("coder-model")));
        assert!(!is_canopy_family_wire_model(Some("glm-5.2")));
        assert!(is_glm_wire_model(Some("GLM-5.2")));
        assert!(is_tiered_effort_wire_model(Some("qwen3.8-max-latest")));
        assert!(!is_tiered_effort_wire_model(Some("qwen3.7-plus")));
    }
}
