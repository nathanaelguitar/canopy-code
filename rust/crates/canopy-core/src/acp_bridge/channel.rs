//! Transport-neutral ACP channel contract.

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub type ChannelFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpChannelExitInfo {
    pub exit_code: Option<i32>,
    pub signal_code: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChannelFailure {
    Transport(String),
    ProcessExited(AcpChannelExitInfo),
    Planned,
}

/// Byte streams are framed by `ndjson`; this abstraction only owns lifecycle.
pub struct AcpChannel {
    pub incoming: mpsc::Receiver<Result<Bytes, ChannelFailure>>,
    pub outgoing: mpsc::Sender<Bytes>,
    pub transport_failed: Option<tokio::sync::watch::Receiver<Option<ChannelFailure>>>,
    pub exited: tokio::sync::watch::Receiver<Option<AcpChannelExitInfo>>,
    kill: Box<dyn Fn() -> ChannelFuture<'static, ()> + Send + Sync>,
    kill_sync: Box<dyn Fn() + Send + Sync>,
}

impl AcpChannel {
    pub fn new(
        incoming: mpsc::Receiver<Result<Bytes, ChannelFailure>>,
        outgoing: mpsc::Sender<Bytes>,
        exited: tokio::sync::watch::Receiver<Option<AcpChannelExitInfo>>,
        kill: impl Fn() -> ChannelFuture<'static, ()> + Send + Sync + 'static,
        kill_sync: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            incoming,
            outgoing,
            transport_failed: None,
            exited,
            kill: Box::new(kill),
            kill_sync: Box::new(kill_sync),
        }
    }

    pub async fn kill(&self) {
        (self.kill)().await;
    }

    pub fn kill_sync(&self) {
        (self.kill_sync)();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn channel_preserves_planned_exit_and_force_kill_hooks() {
        let (_incoming_tx, incoming) = mpsc::channel(1);
        let (outgoing, _outgoing_rx) = mpsc::channel(1);
        let (_exit_tx, exited) = tokio::sync::watch::channel(None);
        let killed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let async_killed = killed.clone();
        let sync_killed = killed.clone();
        let channel = AcpChannel::new(
            incoming,
            outgoing,
            exited,
            move || {
                let async_killed = async_killed.clone();
                Box::pin(async move {
                    async_killed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
            },
            move || {
                sync_killed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            },
        );
        channel.kill().await;
        channel.kill_sync();
        assert_eq!(killed.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}
