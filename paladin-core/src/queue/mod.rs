//! Simplified interface for interacting with queues.
//!
//! Different queuing systems will provide many different configuration options
//! and features — we do not attempt to provide a unified interface for all of
//! them. Rather, we provided a bare minimum interface that is sufficient to
//! satisfy the semantics of this system. In particular, connection management,
//! queue declaration, queue consumption, and message publishing are the only
//! operations that are supported.

use anyhow::Result;
use async_trait::async_trait;
use futures::Stream;

use crate::{acker::Acker, serializer::Serializable};

#[async_trait]
pub trait Queue {
    /// The type of connection used to interact with the queue.
    /// Cloning operations should use a resource pool or connection manager to
    /// ensure cloning is cheap.
    type Connection: Connection;

    /// Establish a connection to the queue.
    async fn get_connection(&self) -> Result<Self::Connection>;
}

/// A connection to a queue.
/// Connections should be cheap to clone such that references need not be passed
/// around.
#[async_trait]
pub trait Connection: Send + Sync + Clone + 'static {
    type QueueHandle: QueueHandle;

    /// Close the connection.
    async fn close(&self) -> Result<()>;

    /// Queue declaration should be idempotent, in that it should instantiate a
    /// queue if it does not exist, and otherwise return the existing queue.
    async fn declare_queue(&self, name: &str) -> Result<Self::QueueHandle>;

    /// Delete the queue.
    async fn delete_queue(&self, name: &str) -> Result<()>;
}

/// A handle to a queue.
/// Handles should be cheap to clone such that references need not be passed
/// around.
#[async_trait]
pub trait QueueHandle: Clone + Send + Sync + Unpin + 'static {
    type Consumer: Consumer;

    /// Publish a message to the queue.
    /// The implementation should be take care of serializing the payload before
    /// publishing.
    async fn publish<PayloadTarget: Serializable>(&self, payload: &PayloadTarget) -> Result<()>;

    /// Declare a queue consumer.
    async fn declare_consumer(&self, consumer_name: &str) -> Result<Self::Consumer>;
}

/// A queue consumer instance.
#[async_trait]
pub trait Consumer: Clone + Send + Sync + Unpin + 'static {
    type Acker: Acker;
    type Stream<PayloadTarget: Serializable>: Stream<Item = (PayloadTarget, Self::Acker)>
        + Send
        + Sync
        + Unpin;

    /// Consume messages from the queue as a [`Stream`].
    /// The implementation should take care of deserializing the payload before
    /// yielding it.
    async fn stream<PayloadTarget: Serializable>(self) -> Result<Self::Stream<PayloadTarget>>;
}

pub mod amqp;
pub mod in_memory;
pub mod sink;
