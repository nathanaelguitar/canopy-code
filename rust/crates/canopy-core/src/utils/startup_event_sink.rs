//! Cross-package startup event forwarding with an isolated callback boundary.
//!
//! Ported from `packages/core/src/utils/startupEventSink.ts`. The registry is
//! instance-scoped so Rust callers can own it with their application runtime;
//! an unset sink is a no-op. Panics from the event callback are contained and
//! reported through the injected logger.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, RwLock};

/// The source allows strings, numbers, and booleans as event attributes.
#[derive(Clone, Debug, PartialEq)]
pub enum StartupEventAttribute {
    String(String),
    Number(f64),
    Boolean(bool),
}

pub type StartupEventAttrs = BTreeMap<String, StartupEventAttribute>;
pub type StartupEventCallback = dyn Fn(&str, Option<&StartupEventAttrs>) + Send + Sync + 'static;
pub type StartupEventLogger = dyn Fn(&str) + Send + Sync + 'static;

/// Thread-safe sink registration and dispatch with a caller-provided failure
/// logger. Callbacks and logging run without holding the registry lock.
pub struct StartupEventSinkRegistry {
    sink: RwLock<Option<Arc<StartupEventCallback>>>,
    logger: Arc<StartupEventLogger>,
}

impl StartupEventSinkRegistry {
    pub fn new(logger: impl Fn(&str) + Send + Sync + 'static) -> Self {
        Self {
            sink: RwLock::new(None),
            logger: Arc::new(logger),
        }
    }

    /// Set or clear the active event handler. Passing `None` disables events.
    pub fn set_startup_event_sink(&self, handler: Option<Arc<StartupEventCallback>>) {
        *self
            .sink
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = handler;
    }

    /// Record an event, doing nothing if no handler is registered.
    ///
    /// Handler panics are swallowed and passed to the injected logger. Logger
    /// panics are also contained so neither callback can disrupt startup work.
    pub fn record_startup_event(&self, name: &str, attrs: Option<&StartupEventAttrs>) {
        let handler = self
            .sink
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(handler) = handler else {
            return;
        };

        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| handler(name, attrs))) {
            let detail = panic_detail(payload.as_ref());
            let message = format!("startup event sink threw for '{name}': {detail}");
            let _ = catch_unwind(AssertUnwindSafe(|| (self.logger)(&message)));
        }
    }
}

impl Default for StartupEventSinkRegistry {
    fn default() -> Self {
        Self::new(|_| {})
    }
}

fn panic_detail(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&'static str>()
                .map(|message| (*message).to_owned())
        })
        .unwrap_or_else(|| "non-string panic payload".to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{
        StartupEventAttribute, StartupEventAttrs, StartupEventCallback, StartupEventSinkRegistry,
    };

    #[test]
    fn unset_and_cleared_sinks_are_no_ops() {
        let registry = StartupEventSinkRegistry::default();
        registry.record_startup_event("before", None);

        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&calls);
        let handler: Arc<StartupEventCallback> = Arc::new(move |name, _| {
            observed.lock().unwrap().push(name.to_owned());
        });
        registry.set_startup_event_sink(Some(handler));
        registry.record_startup_event("one", None);
        registry.set_startup_event_sink(None);
        registry.record_startup_event("two", None);

        assert_eq!(*calls.lock().unwrap(), ["one"]);
    }

    #[test]
    fn forwards_event_name_and_typed_attributes() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&received);
        let registry = StartupEventSinkRegistry::default();
        registry.set_startup_event_sink(Some(Arc::new(move |name, attrs| {
            observed
                .lock()
                .unwrap()
                .push((name.to_owned(), attrs.cloned()));
        })));

        let attrs = StartupEventAttrs::from([
            (
                "outcome".to_owned(),
                StartupEventAttribute::String("ready".to_owned()),
            ),
            ("attempt".to_owned(), StartupEventAttribute::Number(2.0)),
            ("cached".to_owned(), StartupEventAttribute::Boolean(true)),
        ]);
        registry.record_startup_event("mcp_server_ready", Some(&attrs));

        let events = received.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "mcp_server_ready");
        assert_eq!(events[0].1.as_ref(), Some(&attrs));
    }

    #[test]
    fn contains_callback_panics_and_reports_through_injected_logger() {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let logged = Arc::clone(&messages);
        let registry = StartupEventSinkRegistry::new(move |message| {
            logged.lock().unwrap().push(message.to_owned());
        });
        registry.set_startup_event_sink(Some(Arc::new(|_, _| {
            panic!("sink should not break callers");
        })));

        registry.record_startup_event("startup_ready", None);

        assert_eq!(
            *messages.lock().unwrap(),
            ["startup event sink threw for 'startup_ready': sink should not break callers"]
        );
    }

    #[test]
    fn contains_panics_from_the_logger_too() {
        let registry = StartupEventSinkRegistry::new(|_| panic!("logger failure"));
        registry.set_startup_event_sink(Some(Arc::new(|_, _| panic!("sink failure"))));
        registry.record_startup_event("still_safe", None);
    }
}
