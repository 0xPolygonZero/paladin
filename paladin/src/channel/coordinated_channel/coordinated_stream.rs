//! The [`Stream`] end of a coordinated channel.
//!
//! The primary purpose of `CoordinatedStream` is to allow external code to
//! signal when the stream should be considered closed. Once signaled, the
//! stream will continue to drain the inner stream until it is empty. After
//! which, the stream will terminate.
//! This is particularly useful in scenarios where the stream should terminate
//! based on external conditions or events, rather than the inner stream's
//! natural end. For example an unbounded stream or a stream managed
//! by an external process.
//!
//! # Design notes
//! The [`CoordinatedStream`] wraps a [`Stream`] and does the following:
//! - Signals termination the stream when the sender is closed/dropped and are
//!   there are no unacknowledged sends.
//! - Decrements the number of pending sends when an item is yielded and it is
//!   acknowledged.

use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use futures::Stream;
use pin_project::pin_project;

use super::ChannelState;

/// The [`Stream`] end of a coordinated channel.
///
/// The primary purpose of `CoordinatedStream` is to allow external code to
/// signal when the stream should be considered closed. Once signaled, the
/// stream will continue to drain the inner stream until it is empty. After
/// which, the stream will terminate.
/// This is particularly useful in scenarios where the stream should terminate
/// based on external conditions or events, rather than the inner stream's
/// natural end. For example an unbounded stream or a stream managed
/// by an external process.
///
/// # Design notes
/// The [`CoordinatedStream`] wraps a [`Stream`] and does the following:
/// - Signals termination the stream when the sender is closed/dropped and are
///   there are no unacknowledged sends.
/// - Decrements the number of pending sends when an item is yielded and it is
///   acknowledged.
#[pin_project]
pub struct CoordinatedStream<S: Stream> {
    #[pin]
    inner: S,
    state: Arc<ChannelState>,
}

impl<S: Stream> CoordinatedStream<S> {
    pub fn new(inner: S, state: Arc<ChannelState>) -> Self {
        Self { inner, state }
    }
}

/// Message acknowledgement handle.
///
/// This introduces an additional step in messaging handling process. In
/// particular, the consumer must explicitly acknowledge the message before it
/// is considered handled. This avoids out-of-sync issues between the sender and
/// receiver when some processing should occur before an item is considered
/// handled.
pub struct CoordinatedAcker {
    state: Arc<ChannelState>,
    waker: Waker,
    did_ack: AtomicBool,
}

impl CoordinatedAcker {
    /// Create a new [`CoordinatedAcker`].
    pub fn new(state: Arc<ChannelState>, waker: Waker) -> Self {
        Self {
            state,
            waker,
            did_ack: AtomicBool::new(false),
        }
    }

    /// Acknowledge the message.
    ///
    /// This will decrement the number of pending sends and wake the stream if
    /// there are no more pending sends.
    pub fn ack(&self) {
        if self.did_ack.swap(true, Ordering::SeqCst) {
            return;
        }

        if self.state.num_pending_sends.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.waker.wake_by_ref();
        }
    }
}

impl<S: Stream> Stream for CoordinatedStream<S> {
    type Item = (S::Item, CoordinatedAcker);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let item = this.inner.poll_next(cx);

        match item {
            // If the inner stream has an item available, return it with an acker.
            Poll::Ready(Some(item)) => Poll::Ready(Some((
                item,
                CoordinatedAcker::new(this.state.clone(), cx.waker().clone()),
            ))),
            Poll::Pending => {
                // If the inner stream is pending, check that the channel is closed and there
                // are no pending sends. If so, return `None` to signal that the
                // stream has terminated.
                let num_pending_sends = this.state.num_pending_sends.load(Ordering::SeqCst);
                if this.state.closed.load(Ordering::SeqCst) && num_pending_sends == 0 {
                    return Poll::Ready(None);
                }

                // Otherwise, return pending.
                Poll::Pending
            }
            // Likely unreachable because the expectation is that we're dealing with unbounded
            // senders, which will never implicitly terminate, but return `None` if the
            // inner stream is closed and there are no pending sends. Otherwise, return
            // pending as there are pending sends.
            Poll::Ready(None) => {
                if this.state.num_pending_sends.load(Ordering::SeqCst) == 0 {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
        }
    }
}
