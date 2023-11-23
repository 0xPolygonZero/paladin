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

/// The delivery mode of a message.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DeliveryMode {
    /// A persistent message will be persisted to disk and will survive a broker
    /// restart if the queue is durable. If the queue is non-durable, the
    /// message message will be persisted to disk until it is delivered to a
    /// consumer or until the broker is restarted.
    #[default]
    Persistent,
    /// An ephemeral message will not be persisted to disk and will be lost if
    /// the broker is restarted.
    Ephemeral,
}

/// The syndication mode of a queue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyndicationMode {
    /// A single-delivery queue will deliver a message to a single consumer.
    #[default]
    ExactlyOnce,
    /// A broadcast queue will deliver a message to all consumers.
    Broadcast,
}

/// The durability of a queue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QueueDurability {
    /// A non-durable queue will be deleted when the broker is restarted.
    #[default]
    NonDurable,
    /// A durable queue will survive a broker restart.
    Durable,
}

/// Queue declaration options.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueueOptions {
    /// The message delivery mode.
    pub delivery_mode: DeliveryMode,
    /// The syndication mode.
    pub syndication_mode: SyndicationMode,
    /// The durability of the queue.
    pub durability: QueueDurability,
}

/// A connection to a queue.
///
/// Connections should be cheap to clone such that references need not be passed
/// around.
#[async_trait]
pub trait Connection: Clone {
    type QueueHandle: QueueHandle;

    /// Close the connection.
    async fn close(&self) -> Result<()>;

    /// Declare a queue.
    ///
    /// Queue declaration should be idempotent, in that it should instantiate a
    /// queue if it does not exist, and otherwise return the existing queue.
    async fn declare_queue(&self, name: &str, options: QueueOptions) -> Result<Self::QueueHandle>;

    /// Delete the queue.
    async fn delete_queue(&self, name: &str) -> Result<()>;
}

/// A handle to a queue.
///
/// Handles should be cheap to clone such that references need not be passed
/// around.
#[async_trait]
pub trait QueueHandle: Clone {
    type Acker: Acker;
    type Consumer<T: Serializable>: Stream<Item = (T, Self::Acker)>;

    /// Publish a message to the queue.
    ///
    /// The implementation should be take care of serializing the payload before
    /// publishing.
    async fn publish<PayloadTarget: Serializable>(&self, payload: &PayloadTarget) -> Result<()>;

    /// Declare a queue consumer.
    async fn declare_consumer<PayloadTarget: Serializable>(
        &self,
        consumer_name: &str,
    ) -> Result<Self::Consumer<PayloadTarget>>;
}

pub mod amqp;
pub mod in_memory;
pub mod sink;
