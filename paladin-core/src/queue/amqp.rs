//! AMQP queue binding using [`lapin`].
//!
//! # Example
//!
//! ```no_run
//! use paladin::{
//!     acker::Acker,
//!     queue::{
//!         Connection, QueueOptions, QueueHandle,
//!         amqp::{AMQPConnection, AMQPConnectionOptions}
//!     }
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
//!     let conn = AMQPConnection::new(AMQPConnectionOptions {
//!         uri: "amqp://localhost:5672",
//!         qos: Some(1),
//!         serializer: Default::default(),
//!     }).await?;
//!     let queue = conn.declare_queue("my_queue", QueueOptions::default()).await?;
//!
//!     // Publish a message
//!     queue.publish(&MyStruct { field: "hello world".to_string() }).await?;
//!
//!     let mut consumer = queue.declare_consumer::<MyStruct>("my_consumer").await?;
//!     while let Some((payload, delivery)) = consumer.next().await {
//!         // ...
//!         delivery.ack().await?;
//!         break;
//!     }
//!     // ...
//!
//!     Ok(())
//! }
//! ```
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Result;
use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use futures::Stream;
use lapin::{options::QueueDeleteOptions, types::FieldTable, ExchangeKind};
use pin_project::pin_project;
use tracing::{error, instrument};

use super::{
    Connection, DeliveryMode, Publisher, QueueDurability, QueueHandle, QueueOptions,
    SyndicationMode,
};
use crate::{
    acker::Acker,
    serializer::{Serializable, Serializer},
};

impl DeliveryMode {
    fn into_message_properties(self) -> lapin::BasicProperties {
        match self {
            DeliveryMode::Persistent => lapin::BasicProperties::default().with_delivery_mode(2),
            DeliveryMode::Ephemeral => lapin::BasicProperties::default(),
        }
    }
}

impl QueueDurability {
    fn into_queue_declare_options(self) -> lapin::options::QueueDeclareOptions {
        match self {
            QueueDurability::NonDurable => lapin::options::QueueDeclareOptions::default(),
            QueueDurability::Durable => lapin::options::QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
        }
    }

    fn into_exchange_declare_options(self) -> lapin::options::ExchangeDeclareOptions {
        match self {
            QueueDurability::NonDurable => lapin::options::ExchangeDeclareOptions::default(),
            QueueDurability::Durable => lapin::options::ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
        }
    }
}

impl QueueOptions {
    fn into_declare_arguments(self) -> FieldTable {
        let mut arguments = FieldTable::default();
        if let QueueDurability::NonDurable = self.durability {
            // Auto expire non-durable queues after 30 minutes of inactivity
            arguments.insert("x-expires".into(), 1800000.into());
        }
        arguments
    }

    fn into_queue_declare_options(self) -> lapin::options::QueueDeclareOptions {
        self.durability.into_queue_declare_options()
    }

    fn into_exchange_declare_options(self) -> lapin::options::ExchangeDeclareOptions {
        self.durability.into_exchange_declare_options()
    }

    fn into_message_properties(self) -> lapin::BasicProperties {
        self.delivery_mode.into_message_properties()
    }
}

/// Options for creating an [`AMQPConnection`].
pub struct AMQPConnectionOptions<'a> {
    /// The AMQP URI to connect to.
    pub uri: &'a str,
    /// The Quality of Service to use for the queue.
    /// This determines how many unacknowledged messages the broker will deliver
    /// to the consumer before requiring acknowledgements. By setting this,
    /// you can control the rate at which messages are delivered to a consumer,
    /// thus affecting throughput and ensuring that a single consumer
    /// doesn't get overwhelmed. See <https://www.rabbitmq.com/consumer-prefetch.html>
    pub qos: Option<u16>,
    pub serializer: Serializer,
}

/// A instance of a connection to an AMQP queue.
///
/// # Example
/// ```no_run
/// use paladin::queue::{
///     amqp::{AMQPConnection, AMQPConnectionOptions}
/// };
/// # use anyhow::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let conn = AMQPConnection::new(AMQPConnectionOptions {
///     uri: "amqp://localhost:5672",
///     qos: Some(1),
///     serializer: Default::default(),
/// }).await?;
///
/// Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct AMQPConnection {
    channel: lapin::Channel,
    connection: Arc<lapin::Connection>,
    serializer: Serializer,
    queue_options: DashMap<String, QueueOptions>,
}

impl AMQPConnection {
    /// Get a handle to the connection.
    pub async fn new(
        AMQPConnectionOptions {
            uri,
            qos,
            serializer,
        }: AMQPConnectionOptions<'_>,
    ) -> Result<Self> {
        let options = lapin::ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);

        let connection = lapin::Connection::connect(uri, options).await?;
        let channel = connection.create_channel().await?;

        channel
            .basic_qos(qos.unwrap_or(1), Default::default())
            .await?;

        Ok(AMQPConnection {
            channel,
            connection: Arc::new(connection),
            serializer,
            queue_options: DashMap::new(),
        })
    }

    async fn declare_broadcast(
        &self,
        name: &str,
        options: QueueOptions,
    ) -> Result<<AMQPConnection as Connection>::QueueHandle> {
        FieldTable::default().insert("x-expires".into(), 60000.into());
        self.channel
            .exchange_declare(
                name,
                ExchangeKind::Fanout,
                options.into_exchange_declare_options(),
                options.into_declare_arguments(),
            )
            .await?;

        self.channel
            .queue_declare(
                name,
                options.into_queue_declare_options(),
                options.into_declare_arguments(),
            )
            .await?;

        self.channel
            .queue_bind(
                name,
                name,
                "", // Routing key is ignored in fanout exchanges
                Default::default(),
                Default::default(),
            )
            .await?;

        Ok(AMQPQueueHandle {
            channel: self.channel.clone(),
            name: name.to_string(),
            serializer: self.serializer,
            options,
        })
    }

    async fn delete_broadcast(&self, name: &str) -> Result<()> {
        self.channel
            .queue_delete(name, QueueDeleteOptions::default())
            .await?;

        self.channel
            .exchange_delete(name, Default::default())
            .await?;

        Ok(())
    }

    async fn declare_single(
        &self,
        name: &str,
        options: QueueOptions,
    ) -> Result<<AMQPConnection as Connection>::QueueHandle> {
        self.channel
            .queue_declare(
                name,
                options.into_queue_declare_options(),
                options.into_declare_arguments(),
            )
            .await?;

        Ok(AMQPQueueHandle {
            channel: self.channel.clone(),
            name: name.to_string(),
            serializer: self.serializer,
            options,
        })
    }

    async fn delete_single(&self, name: &str) -> Result<()> {
        self.channel
            .queue_delete(name, QueueDeleteOptions::default())
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Connection for AMQPConnection {
    type QueueHandle = AMQPQueueHandle;

    async fn close(&self) -> Result<()> {
        _ = self.channel.close(200, "Goodbye").await;
        _ = self.connection.close(200, "Goodbye").await;

        Ok(())
    }

    /// Declare an AMQP queue with the given name.
    ///
    /// ```no_run
    /// # use paladin::{
    ///     queue::{Connection, QueueOptions, amqp::{AMQPConnection, AMQPConnectionOptions}}
    /// };
    /// # use anyhow::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let conn = AMQPConnection::new(AMQPConnectionOptions {
    ///     uri: "amqp://localhost:5672",
    ///     qos: Some(1),
    ///     serializer: Default::default(),
    /// }).await?;
    /// let queue = conn.declare_queue("my_queue", QueueOptions::default()).await?;
    ///
    /// Ok(())
    /// # }
    async fn declare_queue(&self, name: &str, options: QueueOptions) -> Result<Self::QueueHandle> {
        match options.syndication_mode {
            SyndicationMode::ExactlyOnce => self.declare_single(name, options).await,
            SyndicationMode::Broadcast => self.declare_broadcast(name, options).await,
        }
    }

    async fn delete_queue(&self, name: &str) -> Result<()> {
        match self.queue_options.entry(name.to_string()) {
            Entry::Occupied(options) => {
                match options.get().syndication_mode {
                    SyndicationMode::ExactlyOnce => self.delete_single(name).await?,
                    SyndicationMode::Broadcast => self.delete_broadcast(name).await?,
                };

                options.remove();

                Ok(())
            }
            Entry::Vacant(_) => self.delete_broadcast(name).await,
        }
    }
}

impl AMQPQueueHandle {
    async fn publish_broadcast<PayloadTarget: Serializable>(
        &self,
        payload: &PayloadTarget,
    ) -> Result<()> {
        self.channel
            .basic_publish(
                &self.name,
                "", // Routing key is ignored in fanout exchanges
                Default::default(),
                &self.serializer.to_bytes(payload)?,
                self.options.into_message_properties(),
            )
            .await?
            .await?;

        Ok(())
    }

    async fn publish_single<PayloadTarget: Serializable>(
        &self,
        payload: &PayloadTarget,
    ) -> Result<()> {
        self.channel
            .basic_publish(
                "",
                &self.name,
                Default::default(),
                &self.serializer.to_bytes(payload)?,
                self.options.into_message_properties(),
            )
            .await?
            .await?;

        Ok(())
    }
}

/// A handle to an AMQP queue.
#[derive(Clone)]
pub struct AMQPQueueHandle {
    channel: lapin::Channel,
    name: String,
    serializer: Serializer,
    options: QueueOptions,
}

pub struct AMQPPublisher<T> {
    queue_handle: AMQPQueueHandle,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> AMQPPublisher<T> {
    pub fn new(queue_handle: AMQPQueueHandle) -> Self {
        Self {
            queue_handle,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Serializable> Publisher<T> for AMQPPublisher<T> {
    /// Publish a message to the queue.
    ///
    /// # Example
    /// ```no_run
    /// use paladin::{
    ///     queue::{
    ///         Connection,
    ///         QueueHandle,
    ///         QueueOptions,
    ///         amqp::{AMQPConnection, AMQPConnectionOptions}
    ///     }
    /// };
    /// use serde::{Serialize, Deserialize};
    /// use anyhow::Result;
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct MyStruct {
    ///     field: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let conn = AMQPConnection::new(AMQPConnectionOptions {
    ///         uri: "amqp://localhost:5672",
    ///         qos: Some(1),
    ///         serializer: Default::default(),
    ///     }).await?;
    ///     let queue = conn.declare_queue("my_queue", QueueOptions::default()).await?;
    ///
    ///     let payload = MyStruct {
    ///        field: "hello world".to_string(),
    ///     };
    ///
    ///     queue.publish(&payload).await?;
    ///
    ///     Ok(())
    /// }
    #[instrument(skip_all, level = "trace")]
    async fn publish(&self, payload: &T) -> Result<()> {
        match self.queue_handle.options.syndication_mode {
            SyndicationMode::ExactlyOnce => self.queue_handle.publish_single(payload).await,
            SyndicationMode::Broadcast => self.queue_handle.publish_broadcast(payload).await,
        }
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl QueueHandle for AMQPQueueHandle {
    type Acker = AMQPAcker;
    type Consumer<PayloadTarget: Serializable> = AMQPConsumer<PayloadTarget>;
    type Publisher<PayloadTarget: Serializable> = AMQPPublisher<PayloadTarget>;

    fn publisher<PayloadTarget: Serializable>(&self) -> Self::Publisher<PayloadTarget> {
        AMQPPublisher::new(self.clone())
    }

    /// Get a consumer instance to the queue.
    /// ```no_run
    /// use paladin::{
    ///     queue::{
    ///         Connection,
    ///         QueueHandle,
    ///         QueueOptions,
    ///         amqp::{AMQPConnection, AMQPConnectionOptions}
    ///     }
    /// };
    /// use serde::{Serialize, Deserialize};
    /// use anyhow::Result;
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct MyStruct {
    ///     field: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let conn = AMQPConnection::new(AMQPConnectionOptions {
    ///         uri: "amqp://localhost:5672",
    ///         qos: Some(1),
    ///         serializer: Default::default(),
    ///     }).await?;
    ///     let queue = conn.declare_queue("my_queue", QueueOptions::default()).await?;
    ///
    ///     let consumer = queue.declare_consumer::<MyStruct>("my_consumer").await?;
    ///
    ///     Ok(())
    /// }
    #[instrument(skip(self), level = "trace")]
    async fn declare_consumer<PayloadTarget: Serializable>(
        &self,
        consumer_name: &str,
    ) -> Result<Self::Consumer<PayloadTarget>> {
        let consumer = self
            .channel
            .basic_consume(
                &self.name,
                consumer_name,
                Default::default(),
                Default::default(),
            )
            .await?;

        Ok(AMQPConsumer {
            inner: consumer,
            serializer: self.serializer,
            _phantom: std::marker::PhantomData,
        })
    }
}

#[pin_project]
pub struct AMQPConsumer<PayloadTarget> {
    #[pin]
    inner: lapin::Consumer,
    serializer: Serializer,
    _phantom: std::marker::PhantomData<PayloadTarget>,
}

impl<PayloadTarget: Serializable> Stream for AMQPConsumer<PayloadTarget> {
    type Item = (PayloadTarget, AMQPAcker);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(delivery))) => {
                let payload = this.serializer.from_bytes(&delivery.data);
                match payload {
                    Ok(payload) => Poll::Ready(Some((payload, AMQPAcker { delivery }))),
                    Err(err) => {
                        error!("Error deserializing message, error: {err}");
                        Poll::Pending
                    }
                }
            }
            Poll::Ready(Some(Err(err))) => {
                let err = anyhow::Error::from(err);
                error!("Error receiving message, error: {err}",);
                Poll::Pending
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
/// An acker for an AMQP consumer.
pub struct AMQPAcker {
    delivery: lapin::message::Delivery,
}

#[async_trait]
impl Acker for AMQPAcker {
    async fn ack(&self) -> Result<()> {
        Ok(self.delivery.ack(Default::default()).await?)
    }

    async fn nack(&self) -> Result<()> {
        Ok(self.delivery.nack(Default::default()).await?)
    }
}
