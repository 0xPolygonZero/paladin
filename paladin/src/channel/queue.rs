//! [`Channel`] and [`ChannelFactory`] implementations generic over a
//! [`Queue`](crate::queue::Queue).
//!
//! This allows any queue implementation, such as RabbitMQ, Kafka, etc, to be
//! used like a [`Channel`]. Downstream code can interact with a queue as if it
//! were a [`Channel`] (like [`std::sync::mpsc::channel`]), allowing for a
//! familiar and unified interface.
//!
//! # Examples
//!
//! **Acquiring a sender**
//!
//! ```no_run
//! use paladin::{
//!     serializer::Serializer,
//!     queue::{Queue, Connection, amqp::{AMQPQueue, AMQPQueueOptions}},
//!     channel::{Channel, ChannelFactory, queue::QueueChannelFactory},
//! };
//! use serde::{Serialize, Deserialize};
//! use anyhow::Result;
//! use futures::SinkExt;
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyStruct {
//!     field: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!    // Establish a connection
//!    let amqp = AMQPQueue::new(AMQPQueueOptions {
//!         uri: "amqp://localhost:5672",
//!         qos: Some(1),
//!         serializer: Serializer::Cbor,
//!     });
//!     let conn = amqp.get_connection().await?;
//!
//!     // Build the factory
//!     let amqp_channel_factory = QueueChannelFactory::new(conn);
//!     // Get a channel
//!     let channel = amqp_channel_factory.get::<MyStruct>("my_queue").await?;
//!     // Get a sender pipe
//!     let mut sender = channel.sender().await?;
//!     // Dispatch a message
//!     sender.send(MyStruct { field: "hello world".to_string() }).await?;
//!
//!     Ok(())
//! }
//! ```
//! **Acquiring a receiver**
//!
//! ```no_run
//! use paladin::{
//!     serializer::Serializer,
//!     acker::Acker,
//!     queue::{Queue, Connection, amqp::{AMQPQueue, AMQPQueueOptions}},
//!     channel::{Channel, ChannelFactory, queue::QueueChannelFactory},
//! };
//! use serde::{Serialize, Deserialize};
//! use anyhow::Result;
//! use futures::StreamExt;
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyStruct {
//!     field: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!    // Establish a connection
//!    let amqp = AMQPQueue::new(AMQPQueueOptions {
//!         uri: "amqp://localhost:5672",
//!         qos: Some(1),
//!         serializer: Serializer::Cbor,
//!     });
//!     let conn = amqp.get_connection().await?;
//!
//!     // Build the factory
//!     let amqp_channel_factory = QueueChannelFactory::new(conn);
//!     // Get a channel
//!     let channel = amqp_channel_factory.get::<MyStruct>("my_queue").await?;
//!     // Get a receiver pipe
//!     let mut receiver = channel.receiver().await?;
//!     // Receive messages
//!     while let Some((message, acker)) = receiver.next().await {
//!         // ...
//!         acker.ack().await?;
//!     }
//!
//!     Ok(())
//! }

use crate::{
    channel::{Channel, ChannelFactory},
    queue::{sink::QueueSink, Connection, Consumer, QueueHandle},
    serializer::Serializable,
};
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

/// A [`ChannelFactory`] implementation for a queue.
pub struct QueueChannelFactory<Conn: Connection> {
    connection: Conn,
}

impl<Conn: Connection> QueueChannelFactory<Conn> {
    pub fn new(connection: Conn) -> Self {
        Self { connection }
    }
}

/// A [`Channel`] implementation for a queue.
///
/// Note that sender, receiver, and release operations are all lazily evaluated
/// -- the resources aren't actually allocated until they are used.
pub struct QueueChannel<T: Serializable, Conn: Connection> {
    connection: Conn,
    identifier: String,
    _phantom: std::marker::PhantomData<T>,
}

#[async_trait]
impl<
        T: Serializable,
        CHandle: Consumer,
        QHandle: QueueHandle<Consumer = CHandle>,
        Conn: Connection<QueueHandle = QHandle>,
    > Channel<T> for QueueChannel<T, Conn>
{
    type Acker = CHandle::Acker;
    type Sender = QueueSink<T, QHandle>;
    type Receiver = CHandle::Stream<T>;

    /// Get a sender for the underlying queue.
    async fn sender(&self) -> Result<Self::Sender> {
        let queue = self.connection.declare_queue(&self.identifier).await?;
        Ok(QueueSink::new(queue))
    }

    /// Get a receiver for the underlying queue.
    async fn receiver(&self) -> Result<Self::Receiver> {
        let queue = self.connection.declare_queue(&self.identifier).await?;
        let consumer = queue.declare_consumer("").await?;

        Ok(consumer.stream().await?)
    }

    /// Delete the underlying queue.
    async fn release(&self) -> Result<()> {
        self.connection.delete_queue(&self.identifier).await?;
        Ok(())
    }
}

#[async_trait]
impl<Conn: Connection> ChannelFactory for QueueChannelFactory<Conn> {
    type Channel<T: Serializable> = QueueChannel<T, Conn>;

    /// Get an existing channel.
    async fn get<T: Serializable>(&self, identifier: &str) -> Result<Self::Channel<T>> {
        Ok(QueueChannel {
            connection: self.connection.clone(),
            identifier: identifier.to_string(),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Issue a new channel, generating a new UUID as the identifier.
    async fn issue<T: Serializable>(&self) -> Result<(String, Self::Channel<T>)> {
        let identifier = Uuid::new_v4().to_string();
        Ok((
            identifier.clone(),
            QueueChannel {
                connection: self.connection.clone(),
                identifier,
                _phantom: std::marker::PhantomData,
            },
        ))
    }
}
