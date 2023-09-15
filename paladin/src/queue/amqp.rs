//! AMQP queue binding using [`lapin`].
//!
//! # Example
//!
//! ```no_run
//! use paladin::{
//!     serializer::Serializer,
//!     acker::Acker,
//!     queue::{
//!         Queue, Connection, QueueHandle, Consumer, amqp::{AMQPQueue, AMQPQueueOptions}
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
//!    let amqp = AMQPQueue::new(AMQPQueueOptions {
//!         uri: "amqp://localhost:5672",
//!         qos: Some(1),
//!         serializer: Serializer::Cbor,
//!     });
//!     let conn = amqp.get_connection().await?;
//!     let queue = conn.declare_queue("my_queue").await?;
//!
//!     // Publish a message
//!     queue.publish(&MyStruct { field: "hello world".to_string() }).await?;
//!
//!     let consumer = queue.declare_consumer("my_consumer").await?;
//!     // Stream the results
//!     let mut stream = consumer.stream::<MyStruct>().await?;
//!     while let Some((payload, delivery)) = stream.next().await {
//!         // ...
//!         delivery.ack().await?;
//!         break;
//!     }
//!     // ...
//!
//!     Ok(())
//! }
//! ```
use super::{Connection, Consumer, Queue, QueueHandle};
use crate::{
    acker::Acker,
    serializer::{Serializable, Serializer},
};
use anyhow::Result;
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use lapin::options::QueueDeleteOptions;
use std::pin::Pin;
use tracing::{error, instrument};

/// A [`Queue`] implementation for AMQP.
///
/// The given serializer will be threaded through to the associated
/// [`AMQPQueueHandle`] and [`AMQPConsumer`] such that serialization /
/// deserialization can be performed automatically.
///
/// ```no_run
/// use paladin::queue::amqp::{AMQPQueue, AMQPQueueOptions};
/// use paladin::serializer::Serializer;
///
/// let amqp = AMQPQueue::new(AMQPQueueOptions {
///     uri: "amqp://localhost:5672",
///     qos: Some(1),
///     serializer: Serializer::Cbor,
/// });
/// ```
pub struct AMQPQueue {
    /// The AMQP URI to connect to.
    uri: String,
    /// The Quality of Service to use for the queue.
    /// This determines how many unacknowledged messages the broker will deliver
    /// to the consumer before requiring acknowledgements. By setting this,
    /// you can control the rate at which messages are delivered to a consumer,
    /// thus affecting throughput and ensuring that a single consumer
    /// doesn't get overwhelmed. See <https://www.rabbitmq.com/consumer-prefetch.html>
    qos: u16,
    /// The AMQP channel. Will be lazily initialized upon calling
    /// `get_connection`.
    channel: Option<lapin::Channel>,
    serializer: Serializer,
}

/// Options for creating an [`AMQPQueue`].
pub struct AMQPQueueOptions<'a> {
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

impl AMQPQueue {
    pub fn new(options: AMQPQueueOptions) -> Self {
        Self {
            uri: options.uri.to_string(),
            qos: options.qos.unwrap_or(1),
            channel: None,
            serializer: options.serializer,
        }
    }
}

#[async_trait]
impl Queue for AMQPQueue {
    type Connection = AMQPConnection;

    async fn get_connection(&self) -> Result<Self::Connection> {
        match self.channel {
            Some(ref channel) => Ok(AMQPConnection {
                channel: channel.clone(),
                serializer: self.serializer,
            }),
            None => {
                let options = lapin::ConnectionProperties::default()
                    .with_executor(tokio_executor_trait::Tokio::current())
                    .with_reactor(tokio_reactor_trait::Tokio);

                let connection = lapin::Connection::connect(&self.uri, options).await?;
                let channel = connection.create_channel().await?;

                channel.basic_qos(self.qos, Default::default()).await?;

                Ok(AMQPConnection {
                    channel,
                    serializer: self.serializer,
                })
            }
        }
    }
}

/// A instance of a connection to an AMQP queue.
///
/// # Example
/// ```no_run
/// use paladin::queue::{Queue, amqp::{AMQPQueue, AMQPQueueOptions}};
/// use paladin::serializer::Serializer;
/// # use anyhow::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let amqp = AMQPQueue::new(AMQPQueueOptions {
///     uri: "amqp://localhost:5672",
///     qos: Some(1),
///     serializer: Serializer::Cbor,
/// });
/// let conn = amqp.get_connection().await?;
///
/// Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct AMQPConnection {
    channel: lapin::Channel,
    serializer: Serializer,
}

#[async_trait]
impl Connection for AMQPConnection {
    type QueueHandle = AMQPQueueHandle;

    /// Declare an AMQP queue with the given name.
    /// ```no_run
    /// # use paladin::{
    ///     serializer::Serializer,
    ///     queue::{Queue, Connection, amqp::{AMQPQueue, AMQPQueueOptions}}
    /// };
    /// # use anyhow::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let amqp = AMQPQueue::new(AMQPQueueOptions {
    ///     uri: "amqp://localhost:5672",
    ///     qos: Some(1),
    ///     serializer: Serializer::Cbor,
    /// });
    /// let conn = amqp.get_connection().await?;
    /// let queue = conn.declare_queue("my_queue").await?;
    ///
    /// Ok(())
    /// # }
    async fn declare_queue(&self, name: &str) -> Result<Self::QueueHandle> {
        self.channel
            .queue_declare(name, Default::default(), Default::default())
            .await?;

        Ok(AMQPQueueHandle {
            channel: self.channel.clone(),
            name: name.to_string(),
            serializer: self.serializer,
        })
    }

    async fn delete_queue(&self, name: &str) -> Result<()> {
        self.channel
            .queue_delete(name, QueueDeleteOptions::default())
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
}

#[async_trait]
impl QueueHandle for AMQPQueueHandle {
    type Consumer = AMQPConsumer;

    /// Publish a message to the queue.
    ///
    /// # Example
    /// ```no_run
    /// use paladin::{
    ///     serializer::Serializer,
    ///     queue::{Queue, Connection, QueueHandle, amqp::{AMQPQueue, AMQPQueueOptions}}
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
    ///    let amqp = AMQPQueue::new(AMQPQueueOptions {
    ///         uri: "amqp://localhost:5672",
    ///         qos: Some(1),
    ///         serializer: Serializer::Cbor,
    ///     });
    ///     let conn = amqp.get_connection().await?;
    ///     let queue = conn.declare_queue("my_queue").await?;
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
    async fn publish<PayloadTarget: Serializable>(&self, payload: &PayloadTarget) -> Result<()> {
        self.channel
            .basic_publish(
                "",
                &self.name,
                Default::default(),
                &self.serializer.to_bytes(payload)?,
                lapin::BasicProperties::default().with_delivery_mode(2),
            )
            .await?
            .await?;

        Ok(())
    }

    /// Get a consumer instance to the queue.
    /// ```no_run
    /// use paladin::{
    ///     serializer::Serializer,
    ///     queue::{Queue, Connection, QueueHandle, amqp::{AMQPQueue, AMQPQueueOptions}}
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
    ///    let amqp = AMQPQueue::new(AMQPQueueOptions {
    ///         uri: "amqp://localhost:5672",
    ///         qos: Some(1),
    ///         serializer: Serializer::Cbor,
    ///     });
    ///     let conn = amqp.get_connection().await?;
    ///     let queue = conn.declare_queue("my_queue").await?;
    ///
    ///     let consumer = queue.declare_consumer("my_consumer").await?;
    ///
    ///     Ok(())
    /// }
    #[instrument(skip(self), level = "trace")]
    async fn declare_consumer(&self, consumer_name: &str) -> Result<Self::Consumer> {
        Ok(AMQPConsumer {
            channel: self.channel.clone(),
            queue_name: self.name.clone(),
            consumer_name: consumer_name.to_string(),
            serializer: self.serializer,
        })
    }

    /// Delete a queue.
    /// ```no_run
    /// use paladin::{
    ///     serializer::Serializer,
    ///     queue::{Queue, Connection, QueueHandle, amqp::{AMQPQueue, AMQPQueueOptions}}
    /// };
    /// use serde::{Serialize, Deserialize};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///    let amqp = AMQPQueue::new(AMQPQueueOptions {
    ///         uri: "amqp://localhost:5672",
    ///         qos: Some(1),
    ///         serializer: Serializer::Cbor,
    ///     });
    ///     let conn = amqp.get_connection().await?;
    ///     let queue = conn.declare_queue("my_queue").await?;
    ///     queue.delete().await?;
    ///
    ///     Ok(())
    /// }
    #[instrument(skip(self), level = "trace")]
    async fn delete(self) -> Result<()> {
        self.channel
            .queue_delete(&self.name, Default::default())
            .await?;

        Ok(())
    }
}

/// A consumer instance for an [`AMQPQueueHandle`].
#[derive(Clone)]
pub struct AMQPConsumer {
    channel: lapin::Channel,
    queue_name: String,
    consumer_name: String,
    serializer: Serializer,
}

#[async_trait]
impl Consumer for AMQPConsumer {
    type Acker = AMQPAcker;
    type Stream<PayloadTarget: Serializable> =
        Pin<Box<dyn Stream<Item = (PayloadTarget, Self::Acker)> + Send + Sync>>;

    /// Stream the results of the consumer.
    ///
    /// ```no_run
    /// use paladin::{
    ///     serializer::Serializer,
    ///     acker::Acker,
    ///     queue::{Queue, Connection, QueueHandle, Consumer, amqp::{AMQPQueue, AMQPQueueOptions}}
    /// };
    /// use serde::{Serialize, Deserialize};
    /// use anyhow::Result;
    /// use futures::StreamExt;
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct MyStruct {
    ///     field: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///    let amqp = AMQPQueue::new(AMQPQueueOptions {
    ///         uri: "amqp://localhost:5672",
    ///         qos: Some(1),
    ///         serializer: Serializer::Cbor,
    ///     });
    ///     let conn = amqp.get_connection().await?;
    ///     let queue = conn.declare_queue("my_queue").await?;
    ///
    ///     // Publish a message
    ///     queue.publish(&MyStruct { field: "hello world".to_string() }).await?;
    ///
    ///     let consumer = queue.declare_consumer("my_consumer").await?;
    ///     // Stream the results
    ///     let mut stream = consumer.stream::<MyStruct>().await?;
    ///     while let Some((payload, delivery)) = stream.next().await {
    ///         // ...
    ///         delivery.ack().await?;
    ///         break;
    ///     }
    ///     // ...
    ///
    ///     Ok(())
    /// }
    #[instrument(skip(self), level = "trace")]
    async fn stream<PayloadTarget: Serializable>(self) -> Result<Self::Stream<PayloadTarget>> {
        let serializer = self.serializer;
        let consumer = self
            .channel
            .basic_consume(
                &self.queue_name,
                &self.consumer_name,
                Default::default(),
                Default::default(),
            )
            .await?;

        let stream = consumer
            .map_err(anyhow::Error::from)
            .filter_map(move |res| async move {
                match res {
                    Ok(delivery) => match serializer.from_bytes(&delivery.data) {
                        Ok(payload) => Some((payload, AMQPAcker { delivery })),
                        Err(err) => {
                            error!("Error deserializing message, error: {err}");
                            None
                        }
                    },
                    Err(err) => {
                        error!("Error receiving message, error: {err}",);
                        None
                    }
                }
            });

        Ok(Box::pin(stream))
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
}
