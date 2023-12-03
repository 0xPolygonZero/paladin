//! The [`Publisher`] end of a coordinated channel.
//!
//! The [`CoordinatedPublisher`] wraps a [`Publisher`] and does the following:
//! - Keeps track of the number of pending sends.
//! - Closes the channel when dropped or explicitly closed.
use std::sync::{atomic::Ordering, Arc};

use anyhow::{bail, Result};
use async_trait::async_trait;
use thiserror::Error;

use super::ChannelState;
use crate::queue::Publisher;

pub struct CoordinatedPublisher<T, Inner> {
    inner: Inner,
    state: Arc<ChannelState>,
    _marker: std::marker::PhantomData<T>,
}

impl<T, Inner> CoordinatedPublisher<T, Inner> {
    pub fn new(inner: Inner, state: Arc<ChannelState>) -> Self {
        Self {
            inner,
            state,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, Inner> Drop for CoordinatedPublisher<T, Inner> {
    fn drop(&mut self) {
        self.state.close();
    }
}

#[derive(Debug, Error)]
pub enum CoordinatedPublisherError {
    #[error("Inner error: {0}")]
    Inner(#[from] anyhow::Error),

    #[error("Publisher is closed")]
    PublisherClosed,
}

fn err_closed<T>() -> anyhow::Result<T> {
    bail!(CoordinatedPublisherError::PublisherClosed)
}

#[async_trait]
impl<T, Inner> Publisher<T> for CoordinatedPublisher<T, Inner>
where
    T: Send + Sync,
    Inner: Publisher<T> + Sync,
{
    async fn publish(&self, payload: &T) -> Result<()> {
        if self.state.closed.load(Ordering::SeqCst) {
            return err_closed();
        }

        self.state.num_pending_sends.fetch_add(1, Ordering::SeqCst);

        self.inner
            .publish(payload)
            .await
            .map_err(CoordinatedPublisherError::Inner)?;

        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await?;
        self.state.close();
        Ok(())
    }
}
