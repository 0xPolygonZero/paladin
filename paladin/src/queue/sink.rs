//! Provides a [`Sink`] implementation generic over any [`QueueHandle`].
//!
//! This module introduces a `QueueSink` that abstracts the intricacies of a
//! queue system, allowing users to interact with it as a simple [`Sink`].
//! This allows decoupling downstream components from queues such that they can
//! simply expect standard [`Sink`] behavior.
//!
//! # Examples
//!
//! ```no_run
//! # use paladin::queue::{Queue, Connection, amqp::{AMQPQueue, AMQPQueueOptions}};
//! # use paladin::serializer::Serializer;
//! # use paladin::queue::sink::QueueSink;
//! # use anyhow::Result;
//! use serde::{Serialize, Deserialize};
//! use futures::{Sink, SinkExt};
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyStruct {
//!     field: String,
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let amqp = AMQPQueue::new(AMQPQueueOptions {
//!     uri: "amqp://localhost:5672",
//!     qos: Some(1),
//!     serializer: Serializer::Cbor,
//! });
//! let conn = amqp.get_connection().await?;
//! let queue = conn.declare_queue("my_queue").await?;
//! let mut sink = QueueSink::new(queue);
//! sink.send(MyStruct { field: "hello world".to_string() }).await?;
//!
//! Ok(())
//! # }
//! ```
//!
//! # Design Notes
//!
//! - The `QueueSink` struct holds a phantom data marker for type safety without
//!   runtime overhead.
//! - The `From` trait is implemented for `QueueSink`, allowing for easy
//!   conversion from a `QueueHandle`.
//! - The sink's readiness is always set to ready, as it expects a `QueueHandle`
//!   as a parameter, which already has an established connection.
//! - The `poll_flush` method ensures that all spawned tasks are completed
//!   before the sink is considered flushed.
//! - The `start_send` method spawns a new task for each item, ensuring
//!   asynchronous processing.

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Result;
use futures::{ready, FutureExt, Sink};
use tokio::task::JoinHandle;

use crate::{queue::QueueHandle, serializer::Serializable};

/// A generic [`Sink`] implementation for [`QueueHandle`].
/// Abstracts away a Queue dependency from the caller such they may simply
/// require a [`Sink`].
pub struct QueueSink<Data, Handle>
where
    Handle: QueueHandle,
{
    _phantom: std::marker::PhantomData<Data>,
    queue_handle: Handle,
    send_futures: VecDeque<JoinHandle<Result<()>>>,
}

impl<Data, Handle> QueueSink<Data, Handle>
where
    Handle: QueueHandle,
{
    /// Create a new [`QueueSink`] instance from a [`QueueHandle`].
    ///
    ///
    /// ```no_run
    /// # use paladin::queue::{Queue, Connection, amqp::{AMQPQueue, AMQPQueueOptions}};
    /// # use paladin::serializer::Serializer;
    /// # use paladin::queue::sink::QueueSink;
    /// # use anyhow::Result;
    /// use serde::{Serialize, Deserialize};
    /// use futures::{Sink, SinkExt};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct MyStruct {
    ///     field: String,
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let amqp = AMQPQueue::new(AMQPQueueOptions {
    ///     uri: "amqp://localhost:5672",
    ///     qos: Some(1),
    ///     serializer: Serializer::Cbor,
    /// });
    /// let conn = amqp.get_connection().await?;
    /// let queue = conn.declare_queue("my_queue").await?;
    /// let mut sink = QueueSink::new(queue);
    /// sink.send(MyStruct { field: "hello world".to_string() }).await?;
    ///
    /// Ok(())
    /// # }
    pub fn new(queue_handle: Handle) -> Self {
        Self {
            _phantom: std::marker::PhantomData,
            queue_handle,
            send_futures: VecDeque::new(),
        }
    }
}

impl<Data, Handle: QueueHandle> From<Handle> for QueueSink<Data, Handle> {
    fn from(queue_handle: Handle) -> Self {
        Self::new(queue_handle)
    }
}

impl<Data, Handle> Sink<Data> for QueueSink<Data, Handle>
where
    Data: Serializable,
    Handle: QueueHandle,
{
    type Error = anyhow::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        // Flush the sink.
        ready!(self.poll_flush(cx)?);

        Poll::Ready(Ok(()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();

        while let Some(future) = this.send_futures.front_mut() {
            match future.poll_unpin(cx) {
                Poll::Ready(result) => {
                    this.send_futures.pop_front();
                    if let Err(e) = result {
                        return Poll::Ready(Err(e.into()));
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Data) -> Result<()> {
        let this = self.get_mut();
        let queue_handle = this.queue_handle.clone();

        let fut = tokio::spawn(async move { queue_handle.publish(&item).await });
        this.send_futures.push_back(fut);
        Ok(())
    }
}
