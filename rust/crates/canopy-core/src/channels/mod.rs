//! Channel sanitizers, authorization gates, persistence, and protocol helpers.
//!
//! Platform adapters and daemon wiring remain outside this module. `PairingStore`
//! is the narrow interface authorization gates use for pairing state.

mod gate_types;

pub mod block_streamer;
pub mod channel_loop_scheduler;
pub mod channel_loop_store;
pub mod channel_loop_tools;
pub mod channel_polling;
pub mod channel_prompt;
pub mod dingtalk_card_types;
pub mod dingtalk_markdown;
pub mod dingtalk_media;
pub mod dingtalk_outbound_image;
pub mod dm_gate;
pub mod feishu_markdown;
pub mod feishu_media;
pub mod feishu_question_card;
pub mod feishu_question_controller;
pub mod github_adapter;
pub mod github_mention;
pub mod gitlab_adapter;
pub mod gitlab_mention;
pub mod group_gate;
pub mod group_history;
pub mod inbound_commands;
pub mod memory_intent;
pub mod memory_recall;
pub mod observed_contacts;
pub mod pairing_store;
pub mod paths;
pub mod proactive_delivery_error;
pub mod qqbot_accounts;
pub mod qqbot_api;
pub mod qqbot_cron;
pub mod qqbot_gateway;
pub mod qqbot_message_projection;
pub mod qqbot_persistence;
pub mod qqbot_routing;
pub mod qqbot_send;
pub mod qqbot_types;
pub mod sanitize;
pub mod sender_gate;
pub mod session_router;
pub mod telegram;
pub mod webhook_prompt;
pub mod wecom;
pub mod wecom_ws;
pub mod weixin_accounts;
pub mod weixin_api;
pub mod weixin_login;
pub mod weixin_media;
pub mod weixin_monitor;
pub mod weixin_outbound;
pub mod weixin_send_image;
pub mod weixin_send_utils;
pub mod weixin_types;
pub mod weixin_typing;

pub use channel_loop_store::SessionTarget;

pub use gate_types::{
    CreatePairingRequestResult, DmPolicy, Envelope, GroupConfig, GroupPolicy, PairingRejection,
    PairingStore, SenderPolicy,
};
