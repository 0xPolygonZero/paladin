//! The [`Sink`] end of a coordinated channel.
//!
//! The [`CoordinatedSink`] wraps a [`Sink`] and does the following:
//! - Keeps track of the number of pending sends.
//! - Closes the channel when dropped or explicitly closed.
use std::{
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use anyhow::{anyhow, bail};
use futures::{ready, Sink};
use pin_project::{pin_project, pinned_drop};
use thiserror::Error;

use super::ChannelState;

/// The [`Sink`] end of a coordinated channel.
///
/// The [`Sink`] implementation adds the following functionality over the inner
/// [`Sink`]:
/// - Keeps track of the number of pending sends.
/// - Closes the channel when dropped or explicitly closed.
#[pin_project(PinnedDrop)]
#[derive(Clone)]
pub struct CoordinatedSink<T, Inner> {
    #[pin]
    inner: Inner,
    state: Arc<ChannelState>,
    _marker: std::marker::PhantomData<T>,
}

impl<T, Inner> CoordinatedSink<T, Inner> {
    pub fn new(inner: Inner, state: Arc<ChannelState>) -> Self {
        Self {
            inner,
            state,
            _marker: std::marker::PhantomData,
        }
    }
}

#[pinned_drop]
impl<T, Inner> PinnedDrop for CoordinatedSink<T, Inner> {
    fn drop(self: Pin<&mut Self>) {
        self.state.close();
    }
}

#[derive(Debug, Error)]
pub enum CoordinatedSinkError {
    #[error("Inner error: {0}")]
    Inner(#[from] anyhow::Error),

    #[error("Sink is closed")]
    SinkClosed,
}

fn err_closed<T>() -> anyhow::Result<T> {
    bail!(CoordinatedSinkError::SinkClosed)
}

impl<T: Unpin, Inner: Sink<T, Error = anyhow::Error>> Sink<T> for CoordinatedSink<T, Inner> {
    type Error = anyhow::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        if this.state.closed.load(Ordering::SeqCst) {
            return Poll::Ready(err_closed());
        }

        this.inner
            .poll_ready(cx)
            .map_err(|e| anyhow!(CoordinatedSinkError::Inner(e)))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.project();
        if this.state.closed.load(Ordering::SeqCst) {
            return err_closed();
        }

        let send = this
            .inner
            .start_send(item)
            .map_err(|e| anyhow!(CoordinatedSinkError::Inner(e)));

        if send.is_ok() {
            this.state.num_pending_sends.fetch_add(1, Ordering::SeqCst);
        }

        send
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();

        if this.state.closed.load(Ordering::SeqCst) {
            return Poll::Ready(err_closed());
        }

        this.inner
            .poll_flush(cx)
            .map_err(|e| anyhow!(CoordinatedSinkError::Inner(e)))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();

        if this.state.closed.load(Ordering::SeqCst) {
            return Poll::Ready(Ok(()));
        }

        ready!(this
            .inner
            .poll_close(cx)
            .map_err(|e| anyhow!(CoordinatedSinkError::Inner(e)))?);
        this.state.close();
        Poll::Ready(Ok(()))
    }
}
