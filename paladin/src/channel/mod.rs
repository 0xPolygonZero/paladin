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

use crate::{acker::Acker, serializer::Serializable};
use anyhow::Result;
use async_trait::async_trait;
use futures::{Sink, Stream};
use std::pin::Pin;
use tracing::error;

trait Sender {
    fn close(&mut self) -> Result<()>;
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
pub trait Channel<T: Serializable>: Send + Sync + 'static {
    type Sender: Sink<T, Error = anyhow::Error> + Send;
    type Acker: Acker;
    type Receiver: Stream<Item = (T, Self::Acker)> + Send;

    /// Acquire the sender side of the channel.
    async fn sender(&self) -> Result<Self::Sender>;

    /// Acquire the receiver side of the channel.
    async fn receiver(&self) -> Result<Self::Receiver>;

    /// Release any resources associated with the channel.
    async fn release(&self) -> Result<()>;
}

/// Behavior for issuing new channels and retrieving existing channels.
///
/// Implementations should take care to ensure that the same channel is returned
/// for a given identifier, allocating a new channel only when necessary.
#[async_trait]
pub trait ChannelFactory: Send + Sync {
    type Channel<T: Serializable>: Channel<T>;

    /// Retrieve an existing channel. An identifier is provided when a channel
    /// is issued.
    async fn get<T: Serializable>(&self, identifier: &str) -> Result<Self::Channel<T>>;

    /// Issue a new channel. An identifier is returned which can be used to
    /// retrieve the channel later in some other process.
    async fn issue<T: Serializable>(&self) -> Result<(String, Self::Channel<T>)>;
}

/// Guard a channel and embed a particular pipe in the lease guard.
/// A single pipe is embedded, as the guard is meant to be held by a single end
/// of the channel. The lease guard will release the channel when it is dropped.
///
/// [`LeaseGuard`] implements [`Stream`] where the pipe is a [`Stream`], and can
/// be used as a [`Stream`] directly.
pub struct LeaseGuard<T: Serializable, C: Channel<T>, Pipe> {
    pipe: Pin<Box<Pipe>>,
    channel: Option<Pin<Box<C>>>,
    _phantom: std::marker::PhantomData<T>,
}

/// Implement [`Stream`] for [`LeaseGuard`] where the pipe is a [`Stream`].
impl<T: Serializable, C: Channel<T>, Pipe: Stream> Stream for LeaseGuard<T, C, Pipe> {
    type Item = Pipe::Item;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.pipe.as_mut().poll_next(cx)
    }
}

impl<T: Serializable, C: Channel<T>, Pipe> std::ops::Deref for LeaseGuard<T, C, Pipe> {
    type Target = Pin<Box<Pipe>>;

    fn deref(&self) -> &Self::Target {
        &self.pipe
    }
}

impl<T: Serializable, C: Channel<T>, Pipe> std::ops::DerefMut for LeaseGuard<T, C, Pipe> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pipe
    }
}

impl<T: Serializable, C: Channel<T>, Pipe> LeaseGuard<T, C, Pipe> {
    pub fn new(channel: C, pipe: Pipe) -> Self {
        Self {
            pipe: Box::pin(pipe),
            channel: Some(Box::pin(channel)),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: Serializable, C: Channel<T>, Pipe> Drop for LeaseGuard<T, C, Pipe> {
    fn drop(&mut self) {
        if let Some(channel) = self.channel.take() {
            tokio::spawn(async move {
                if let Err(e) = channel.release().await {
                    error!("Failed to release channel: {}", e);
                }
            });
        }
    }
}

pub mod coordinated_channel;
pub mod dynamic;
pub mod queue;
