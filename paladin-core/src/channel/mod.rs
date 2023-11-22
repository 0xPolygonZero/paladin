//! Generic channel behavior for distributed (inter-process) channels.
//!
//! It includes a bit more complexity than the traditional
//! [mpsc](std::sync::mpsc::channel) channel for the following reasons:
//! - It supports a notion of message acknowledgement.
//! - It supports a notion of resource release.
//! - Rather than returning a tuple of `(sender, receiver)`, it breaks each into
//!   separate methods.
//! This is because generally senders and receivers are usually instantiated in
//! separate process, as the channel is meant to facilitate inter process
//! communication. This avoids instantiating unnecessary resources when only one
//! is needed.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Result;
use async_trait::async_trait;
use futures::{Sink, Stream};
use pin_project::{pin_project, pinned_drop};

use crate::{acker::Acker, serializer::Serializable};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelType {
    ExactlyOnce,
    Broadcast,
}

/// Generic channel behavior for distributed (inter-process) channels.
///
/// It includes a bit more complexity than the traditional
/// [mpsc](std::sync::mpsc::channel) channel for the following reasons:
/// - It supports a notion of message acknowledgement.
/// - It supports a notion of resource release.
/// - Rather than returning a tuple of `(sender, receiver)`, it breaks each into
///   separate methods.
/// This is because generally senders and receivers are usually instantiated in
/// separate process, as the channel is meant to facilitate inter process
/// communication. This avoids instantiating unnecessary resources when only one
/// is needed.
#[async_trait]
pub trait Channel {
    type Sender<T: Serializable>: Sink<T>;
    type Acker: Acker;
    type Receiver<T: Serializable>: Stream<Item = (T, Self::Acker)>;

    async fn close(&self) -> Result<()>;

    /// Acquire the sender side of the channel.
    async fn sender<T: Serializable>(&self) -> Result<Self::Sender<T>>;

    /// Acquire the receiver side of the channel.
    async fn receiver<T: Serializable>(&self) -> Result<Self::Receiver<T>>;

    /// Mark the channel for release.
    fn release(&self);
}

/// Behavior for issuing new channels and retrieving existing channels.
///
/// Implementations should take care to ensure that the same channel is returned
/// for a given identifier, allocating a new channel only when necessary.
#[async_trait]
pub trait ChannelFactory {
    type Channel: Channel;

    /// Retrieve an existing channel. An identifier is provided when a channel
    /// is issued.
    async fn get(&self, identifier: &str, channel_type: ChannelType) -> Result<Self::Channel>;

    /// Issue a new channel. An identifier is returned which can be used to
    /// retrieve the channel later in some other process.
    async fn issue(&self, channel_type: ChannelType) -> Result<(String, Self::Channel)>;
}

/// Guard a channel and embed a particular pipe in the lease guard.
/// A single pipe is embedded, as the guard is meant to be held by a single end
/// of the channel. The lease guard will release the channel when it is dropped.
///
/// [`LeaseGuard`] implements [`Stream`] where the pipe is a [`Stream`], and can
/// be used as a [`Stream`] directly.
#[pin_project(PinnedDrop)]
pub struct LeaseGuard<C: Channel, Pipe> {
    #[pin]
    pipe: Pipe,
    channel: Option<C>,
}

/// Implement [`Stream`] for [`LeaseGuard`] where the pipe is a [`Stream`].
impl<C: Channel, Pipe: Stream> Stream for LeaseGuard<C, Pipe> {
    type Item = Pipe::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().pipe.poll_next(cx)
    }
}

impl<C: Channel, Pipe> std::ops::Deref for LeaseGuard<C, Pipe> {
    type Target = Pipe;

    fn deref(&self) -> &Self::Target {
        &self.pipe
    }
}

impl<C: Channel, Pipe> std::ops::DerefMut for LeaseGuard<C, Pipe> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pipe
    }
}

impl<C: Channel, Pipe> LeaseGuard<C, Pipe> {
    pub fn new(channel: C, pipe: Pipe) -> Self {
        Self {
            pipe,
            channel: Some(channel),
        }
    }
}

#[pinned_drop]
impl<C: Channel, Pipe> PinnedDrop for LeaseGuard<C, Pipe> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        if let Some(channel) = this.channel.take() {
            channel.release();
        }
    }
}

pub mod coordinated_channel;
pub mod queue;
