//! Native Rust implementation of the mobile-mcp protocol, tool runtime, and
//! device helpers. Hardware access uses bounded child processes and the
//! `mobilecli`, `adb`, `ios`, `simctl`, and WebDriverAgent interfaces used by
//! the TypeScript implementation.

pub mod android;
pub mod coord;
pub mod devices;
pub mod filter;
pub mod ios;
pub mod ios_simulator;
pub mod logger;
pub mod protocol;
pub mod runner;
pub mod sse;
pub mod telemetry;
pub mod tools;
pub mod wda;

pub const SERVER_NAME: &str = "mobile-mcp";
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
