//! State coordination between two distinct channel pipes (a
//! [`Sink`] and a [`Stream`]).
//!
//! Generally when working with channels, the expectation is that closing or
//! dropping a sender will signal to the receiver that the channel is closed.
//! See [`std::sync::mpsc::Sender`] for an example of this. Additionally, it's
//! expected that when a channel is closed, the receiver will be able to drain
//! any remaining items from the channel. When coordinating across distinct
//! channels, this is not possible, as they don't implicitly share state.
//!
//! This module provides a [`coordinated_channel`] function that solves these
//! problems by binding the sender and receiver to a shared state that tracks
//! sender closure and pending sends.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use futures::{Sink, Stream};

use self::{coordinated_sink::CoordinatedSink, coordinated_stream::CoordinatedStream};

pub mod coordinated_sink;
pub mod coordinated_stream;

/// The shared state between the sender and receiver of a coordinated channel.
#[derive(Default)]
pub struct ChannelState {
    /// Whether or not the channel is closed.
    closed: AtomicBool,
    /// The number of pending sends.
    num_pending_sends: AtomicUsize,
}

impl ChannelState {
    /// Create a new [`ChannelState`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the channel as closed.
    pub fn close(&self) {
        if self.closed.load(Ordering::SeqCst) {
            return;
        }

        self.closed.store(true, Ordering::SeqCst);
    }
}

/// State coordination between two distinct channel pipes (a
/// [`Sink`] and a [`Stream`]).
///
/// Generally when working with channels, the expectation is that closing or
/// dropping a sender will signal to the receiver that the channel is closed.
/// See [`std::sync::mpsc::Sender`] for an example of this. Additionally, it's
/// expected that when a channel is closed, the receiver will be able to drain
/// any remaining items from the channel. When coordinating across distinct
/// channels, this is not possible, as they don't implicitly share state.
///
/// [`coordinated_channel`] solves these problems by binding the sender and
/// receiver to a shared state that tracks sender closure and pending sends.
pub fn coordinated_channel<
    A,
    B,
    Sender: Sink<A, Error = anyhow::Error>,
    Receiver: Stream<Item = B>,
>(
    sender: Sender,
    receiver: Receiver,
) -> (CoordinatedSink<A, Sender>, CoordinatedStream<Receiver>) {
    let state = Arc::new(ChannelState::new());
    let sender = CoordinatedSink::new(sender, state.clone());
    let receiver = CoordinatedStream::new(receiver, state.clone());

    (sender, receiver)
}
