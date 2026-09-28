//! Usage dashboard aggregation over durable and live usage history.
//!
//! The implementation lives alongside history loading in `usage_history` so
//! dashboard aggregation and persisted record semantics share one source of
//! truth. This module provides the dashboard-focused public API.

pub use super::usage_history::{
    TimeRange, UsageDailyPoint, UsageDashboard, UsageDashboardOptions, UsageDashboardTotals,
    UsageHeatmapDay, UsageModelShare, UsageSkillCall, UsageSummaryRecord, build_usage_dashboard,
    load_usage_dashboard,
};
