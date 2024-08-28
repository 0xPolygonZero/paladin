//! [`Channel`] and [`ChannelFactory`] implementations generic over a
//! [`Connection`].
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
//!     queue::{Connection, Publisher, amqp::{AMQPConnection, AMQPConnectionOptions}},
//!     channel::{Channel, ChannelType, ChannelFactory, queue::QueueChannelFactory},
//! };
//! use uuid::Uuid;
//! use serde::{Serialize, Deserialize};
//! use anyhow::Result;
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyStruct {
//!     field: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Establish a connection
//!     let conn = AMQPConnection::new(AMQPConnectionOptions {
//!         uri: "amqp://localhost:5672",
//!         qos: Some(1),
//!         serializer: Default::default(),
//!     }).await?;
//!
//!     // Build the factory
//!     let amqp_channel_factory = QueueChannelFactory::new(conn);
//!     // Get a channel
//!     let (_, channel) = amqp_channel_factory.issue(ChannelType::ExactlyOnce).await?;
//!     // Get a sender pipe
//!     let mut sender = channel.sender::<MyStruct>().await?;
//!     // Dispatch a message
//!     sender.publish(&MyStruct { field: "hello world".to_string() }).await?;
//!
//!     Ok(())
//! }
//! ```
//! **Acquiring a receiver**
//!
//! ```no_run
//! use paladin::{
//!     acker::Acker,
//!     queue::{Connection, amqp::{AMQPConnection, AMQPConnectionOptions}},
//!     channel::{Channel, ChannelType, ChannelFactory, queue::QueueChannelFactory},
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
//!     // Establish a connection
//!     let conn = AMQPConnection::new(AMQPConnectionOptions {
//!         uri: "amqp://localhost:5672",
//!         qos: Some(1),
//!         serializer: Default::default(),
//!     }).await?;
//!
//!     // Build the factory
//!     let amqp_channel_factory = QueueChannelFactory::new(conn);
//!     // Get a channel
//!     let (_, channel) = amqp_channel_factory.issue(ChannelType::ExactlyOnce).await?;
//!     // Get a receiver pipe
//!     let mut receiver = channel.receiver::<MyStruct>().await?;
//!     // Receive messages
//!     while let Some((message, acker)) = receiver.next().await {
//!         // ...
//!         acker.ack().await?;
//!     }
//!
//!     Ok(())
//! }

use anyhow::Result;
use async_trait::async_trait;

use crate::{
    channel::{Channel, ChannelFactory, ChannelType},
    queue::{
        Connection, DeliveryMode, QueueDurability, QueueHandle, QueueOptions, SyndicationMode,
    },
    serializer::Serializable,
};

impl From<ChannelType> for QueueOptions {
    fn from(channel_type: ChannelType) -> Self {
        match channel_type {
            ChannelType::ExactlyOnce => QueueOptions {
                syndication_mode: SyndicationMode::ExactlyOnce,
                delivery_mode: DeliveryMode::Persistent,
                durability: QueueDurability::NonDurable,
            },
            ChannelType::Broadcast => QueueOptions {
                syndication_mode: SyndicationMode::Broadcast,
                delivery_mode: DeliveryMode::Persistent,
                durability: QueueDurability::NonDurable,
            },
        }
    }
}

/// A [`ChannelFactory`] implementation for a queue.
#[derive(Clone)]
pub struct QueueChannelFactory<Conn> {
    connection: Conn,
}

impl<Conn> QueueChannelFactory<Conn> {
    pub fn new(connection: Conn) -> Self {
        Self { connection }
    }
}

/// A [`Channel`] implementation for a queue.
///
/// Note that sender, receiver, and release operations are all lazily evaluated
/// -- the resources aren't actually allocated until they are used.
#[derive(Clone)]
pub struct QueueChannel<Conn> {
    connection: Conn,
    identifier: String,
    channel_type: ChannelType,
}

#[async_trait]
impl<
        QHandle: QueueHandle + Send + Sync + 'static,
        Conn: Connection<QueueHandle = QHandle> + Send + Sync + 'static,
    > Channel for QueueChannel<Conn>
{
    type Acker = <QHandle as QueueHandle>::Acker;
    type Sender<'a, T: Serializable + 'a> = <QHandle as QueueHandle>::Publisher<T>;
    type Receiver<'a, T: Serializable + 'a> = <QHandle as QueueHandle>::Consumer<T>;

    /// Close the underlying connection.
    async fn close(&self) -> Result<()> {
        self.connection.close().await?;
        Ok(())
    }

    /// Get a sender for the underlying queue.
    async fn sender<'a, T: Serializable + 'a>(&self) -> Result<Self::Sender<'a, T>> {
        let queue = self
            .connection
            .declare_queue(self.identifier.as_str(), self.channel_type.into())
            .await?;

        Ok(queue.publisher())
    }

    /// Get a receiver for the underlying queue.
    async fn receiver<'a, T: Serializable + 'a>(&self) -> Result<Self::Receiver<'a, T>> {
        // TODO - this is a bit of a hack, but it works for now
        use rand::{distributions::Alphanumeric, Rng}; // 0.8
        let iden: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        let queue = self
            .connection
            .declare_queue(self.identifier.as_str(), self.channel_type.into())
            .await?;
        let consumer = queue.declare_consumer(iden.as_str()).await?;

        Ok(consumer)
    }

    /// Delete the underlying queue.
    fn release(&self) {
        let conn = self.connection.clone();
        let identifier = self.identifier.clone();

        tokio::spawn(async move {
            let identifier = identifier.as_str();
            _ = conn.delete_queue(identifier).await;
        });
    }
}

#[async_trait]
impl<Conn: Connection + Send + Sync + 'static> ChannelFactory for QueueChannelFactory<Conn>
where
    <Conn as Connection>::QueueHandle: Send + Sync + 'static,
{
    type Channel = QueueChannel<Conn>;

    /// Get an existing channel.
    async fn get(&self, identifier: String, channel_type: ChannelType) -> Result<Self::Channel> {
        Ok(QueueChannel {
            connection: self.connection.clone(),
            identifier,
            channel_type,
        })
    }

    /// Issue a new channel, generating a new UUID as the identifier.
    async fn issue(&self, channel_type: ChannelType) -> Result<(String, Self::Channel)> {
        use rand::{distributions::Alphanumeric, Rng}; // 0.8

        let identifier: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();

        Ok((
            identifier.clone(),
            QueueChannel {
                connection: self.connection.clone(),
                identifier,
                channel_type,
            },
        ))
    }
}
