//! The [`Sink`] end of a coordinated channel.
//!
//! The [`CoordinatedSink`] wraps a [`Sink`] and does the following:
//! - Keeps track of the number of pending sends.
//! - Closes the channel when dropped or explicitly closed.
use anyhow::{anyhow, bail};
use futures::{ready, Sink};
use std::{
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use super::ChannelState;

/// The [`Sink`] end of a coordinated channel.
///
/// The [`Sink`] implementation adds the following functionality over the inner
/// [`Sink`]:
/// - Keeps track of the number of pending sends.
/// - Closes the channel when dropped or explicitly closed.
#[derive(Clone)]
pub struct CoordinatedSink<T: Unpin, Inner: Sink<T, Error = anyhow::Error>> {
    inner: Pin<Box<Inner>>,
    state: Arc<ChannelState>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Unpin, Inner: Sink<T, Error = anyhow::Error>> CoordinatedSink<T, Inner> {
    pub fn new(inner: Inner, state: Arc<ChannelState>) -> Self {
        Self {
            inner: Box::pin(inner),
            state,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Unpin, Inner: Sink<T, Error = anyhow::Error>> Drop for CoordinatedSink<T, Inner> {
    fn drop(&mut self) {
        self.state.close();
    }
}

#[derive(Debug)]
pub enum CoordinatedSinkError {
    Inner(anyhow::Error),

    SinkClosed,
}

fn err_closed<T>() -> anyhow::Result<T> {
    bail!(CoordinatedSinkError::SinkClosed)
}

impl std::fmt::Display for CoordinatedSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoordinatedSinkError::Inner(e) => write!(f, "Inner error: {}", e),
            CoordinatedSinkError::SinkClosed => write!(f, "Sink is closed"),
        }
    }
}
impl std::error::Error for CoordinatedSinkError {}

impl<T: Unpin, Inner: Sink<T, Error = anyhow::Error>> Sink<T> for CoordinatedSink<T, Inner> {
    type Error = anyhow::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.state.closed.load(Ordering::SeqCst) {
            return Poll::Ready(err_closed());
        }

        this.inner
            .as_mut()
            .poll_ready(cx)
            .map_err(|e| anyhow!(CoordinatedSinkError::Inner(e)))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.state.closed.load(Ordering::SeqCst) {
            return err_closed();
        }

        let send = this
            .inner
            .as_mut()
            .start_send(item)
            .map_err(|e| anyhow!(CoordinatedSinkError::Inner(e)));

        if send.is_ok() {
            this.state.num_pending_sends.fetch_add(1, Ordering::SeqCst);
        }

        send
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        if this.state.closed.load(Ordering::SeqCst) {
            return Poll::Ready(err_closed());
        }

        this.inner
            .as_mut()
            .poll_flush(cx)
            .map_err(|e| anyhow!(CoordinatedSinkError::Inner(e)))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        if this.state.closed.load(Ordering::SeqCst) {
            return Poll::Ready(Ok(()));
        }

        ready!(this
            .inner
            .as_mut()
            .poll_close(cx)
            .map_err(|e| anyhow!(CoordinatedSinkError::Inner(e)))?);
        this.state.close();
        Poll::Ready(Ok(()))
    }
}
