//! Register skill frontmatter hooks as session-scoped hooks.
//!
//! Port of `packages/core/src/hooks/registerSkillHooks.ts`. Skill hook
//! definitions stay as JSON, matching the Rust `SkillHooksSettings` boundary;
//! the session manager and native hook runners consume that same config shape.

use serde_json::{Map, Value};

use super::planner::HookEventName;
use super::session_manager::{SessionHookOptions, SessionHooksManager};
use crate::skills::SkillConfig;

/// Register a skill's command and HTTP hooks for the current session.
///
/// Function hooks and unsupported hook types are skipped, as skill frontmatter
/// supports only command and HTTP hooks. For command hooks, `CANOPY_SKILL_ROOT`
/// is set from the skill root and overrides any value in the hook's `env` map.
/// Invalid event names and malformed JSON entries are ignored because the
/// source skill loader normally filters those before registration.
pub fn register_skill_hooks(
    session_hooks_manager: &mut SessionHooksManager,
    session_id: &str,
    skill: &SkillConfig,
) -> usize {
    let Some(hooks_settings) = skill.hooks.as_ref() else {
        return 0;
    };

    let skill_root = skill
        .skill_root
        .as_ref()
        .filter(|path| !path.as_os_str().is_empty())
        .map(|path| path.to_string_lossy().into_owned());
    let mut registered_count = 0;

    // SkillHooksSettings is currently a HashMap, so event iteration cannot
    // preserve TypeScript object's insertion order. Sorting makes registration
    // deterministic; the order of matchers and hooks within each event is
    // preserved by their source vectors.
    let mut event_names = hooks_settings.keys().collect::<Vec<_>>();
    event_names.sort_unstable();

    for event_name in event_names {
        let Ok(event) = serde_json::from_value::<HookEventName>(Value::String(event_name.clone()))
        else {
            continue;
        };

        let Some(matchers) = hooks_settings.get(event_name) else {
            continue;
        };
        for matcher in matchers {
            let Some(matcher_object) = matcher.as_object() else {
                continue;
            };
            let matcher_pattern = matcher_object
                .get("matcher")
                .and_then(Value::as_str)
                .filter(|pattern| !pattern.is_empty())
                .unwrap_or_default();
            let Some(hooks) = matcher_object.get("hooks").and_then(Value::as_array) else {
                continue;
            };

            for hook in hooks {
                let Some(hook_type) = hook.get("type").and_then(Value::as_str) else {
                    continue;
                };

                if hook_type == "function" {
                    continue;
                }
                if hook_type != "command" && hook_type != "http" {
                    continue;
                }

                let mut hook_config = hook.clone();
                if hook_type == "command" {
                    if let (Some(skill_root), Some(config)) =
                        (skill_root.as_ref(), hook_config.as_object_mut())
                    {
                        let env = config
                            .entry("env")
                            .or_insert_with(|| Value::Object(Map::new()));
                        if !env.is_object() {
                            *env = Value::Object(Map::new());
                        }
                        if let Some(env) = env.as_object_mut() {
                            env.insert(
                                "CANOPY_SKILL_ROOT".to_owned(),
                                Value::String(skill_root.clone()),
                            );
                        }
                    }
                }

                session_hooks_manager.add_session_hook(
                    session_id,
                    event,
                    matcher_pattern,
                    hook_config,
                    Some(SessionHookOptions {
                        sequential: None,
                        skill_root: skill_root.clone(),
                    }),
                );
                registered_count += 1;
            }
        }
    }

    registered_count
}

/// Unregister a skill's hooks.
///
/// As in the TypeScript implementation, registration does not track hook IDs
/// per skill, so session hooks remain until the session is cleared and this
/// function currently always returns zero.
pub fn unregister_skill_hooks(
    _session_hooks_manager: &mut SessionHooksManager,
    _session_id: &str,
    _skill: &SkillConfig,
) -> usize {
    0
}
