use serde_json::Value;
use std::future::Future;
use std::time::Duration;
use thiserror::Error;

const MIN_DISCOVERY_TIMEOUT_MS: f64 = 100.0;
const MAX_DISCOVERY_TIMEOUT_MS: f64 = 300_000.0;
const STDIO_DEFAULT_TIMEOUT_MS: f64 = 30_000.0;
const REMOTE_DEFAULT_TIMEOUT_MS: f64 = 5_000.0;

pub fn discovery_timeout_for(config: &Value) -> f64 {
    let object = config.as_object();
    if let Some(override_ms) = object
        .and_then(|value| value.get("discoveryTimeoutMs"))
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
    {
        return override_ms.clamp(MIN_DISCOVERY_TIMEOUT_MS, MAX_DISCOVERY_TIMEOUT_MS);
    }

    let is_remote = ["httpUrl", "url", "tcp"].iter().any(|field| {
        object
            .and_then(|value| value.get(*field))
            .is_some_and(js_truthy)
    });
    if is_remote {
        REMOTE_DEFAULT_TIMEOUT_MS
    } else {
        STDIO_DEFAULT_TIMEOUT_MS
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

#[derive(Debug, Error)]
pub enum DiscoveryTimeoutError<E: std::fmt::Debug + std::fmt::Display> {
    #[error("{0}")]
    Task(E),
    #[error(
        "Timed out after {timeout_ms}ms: {label}. The MCP server may be hung; pool will roll back the spawn/restart and free its budget slot."
    )]
    Timeout { timeout_ms: String, label: String },
    #[error("discovery task failed to join: {0}")]
    Join(String),
}

/// Race a discovery future against a timeout while leaving the spawned future
/// running after timeout, matching the source Promise race. The caller is
/// responsible for shutting down the MCP transport on timeout.
pub async fn run_with_timeout<F, T, E>(
    task: F,
    timeout_ms: f64,
    label: impl Into<String>,
) -> Result<T, DiscoveryTimeoutError<E>>
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + std::fmt::Debug + std::fmt::Display + 'static,
{
    let timeout_ms = timeout_ms.max(0.0);
    let timeout = Duration::from_secs_f64(timeout_ms / 1000.0);
    let label = label.into();
    let mut handle = tokio::spawn(task);
    match tokio::time::timeout(timeout, &mut handle).await {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(error))) => Err(DiscoveryTimeoutError::Task(error)),
        Ok(Err(error)) => Err(DiscoveryTimeoutError::Join(error.to_string())),
        Err(_) => Err(DiscoveryTimeoutError::Timeout {
            timeout_ms: format_number(timeout_ms),
            label,
        }),
    }
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{DiscoveryTimeoutError, discovery_timeout_for, run_with_timeout};
    use serde_json::json;
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[test]
    fn chooses_transport_defaults_and_clamps_finite_overrides() {
        assert_eq!(discovery_timeout_for(&json!({"command":"node"})), 30_000.0);
        assert_eq!(
            discovery_timeout_for(&json!({"httpUrl":"https://mcp"})),
            5_000.0
        );
        assert_eq!(
            discovery_timeout_for(&json!({"url":"https://mcp/sse"})),
            5_000.0
        );
        assert_eq!(discovery_timeout_for(&json!({"tcp":"ws://mcp"})), 5_000.0);
        assert_eq!(
            discovery_timeout_for(&json!({"discoveryTimeoutMs":50})),
            100.0
        );
        assert_eq!(
            discovery_timeout_for(&json!({"discoveryTimeoutMs":500_000})),
            300_000.0
        );
        assert_eq!(
            discovery_timeout_for(&json!({"discoveryTimeoutMs":null})),
            30_000.0
        );
    }

    #[tokio::test]
    async fn returns_task_value_and_propagates_task_errors() {
        assert_eq!(
            run_with_timeout(async { Ok::<_, String>(7) }, 100.0, "fast")
                .await
                .unwrap(),
            7
        );
        assert!(matches!(
            run_with_timeout(async { Err::<(), _>("failure") }, 100.0, "failed").await,
            Err(DiscoveryTimeoutError::Task("failure"))
        ));
    }

    #[tokio::test]
    async fn timeout_detaches_the_task_for_transport_cleanup_to_handle() {
        let (sender, receiver) = oneshot::channel();
        let error = run_with_timeout(
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let _ = sender.send(());
                Ok::<_, String>(())
            },
            1.0,
            "slow discovery",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, DiscoveryTimeoutError::Timeout { .. }));
        assert!(
            error
                .to_string()
                .contains("Timed out after 1ms: slow discovery")
        );
        tokio::time::timeout(Duration::from_secs(1), receiver)
            .await
            .unwrap()
            .unwrap();
    }
}
