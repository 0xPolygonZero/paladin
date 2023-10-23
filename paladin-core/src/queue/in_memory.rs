//! An in-memory implementation of [`Queue`] and its associated types.
//!
//! This implementation is useful for testing and debugging purposes, as it
//! provides a simple, in-memory queue that can be used to emulate a real queue.
//! It uses asynchronous synchronization primitives to faithfully emulate the
//! behavior of a real queue, and is well suited for a multi-threaded and/or
//! asynchronous environment.
//!
//! The [`InMemoryConnection`] is cloneable can be used to simulate a connection
//! pool to a real queue. Each clone of the connection will maintain references
//! to same underlying queues.
use std::{
    collections::{HashMap, VecDeque},
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Result;
use async_trait::async_trait;
use futures::{
    lock::{Mutex, OwnedMutexLockFuture},
    ready, Future, Stream,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::PollSemaphore;

use super::{Connection, Consumer, Queue, QueueHandle};
use crate::{
    acker::NoopAcker,
    serializer::{Serializable, Serializer},
};

/// An in-memory implementation of [`Queue`].
///
/// ```
/// use paladin::queue::{Queue, in_memory::InMemoryQueue};
/// use paladin::serializer::Serializer;
/// use anyhow::Result;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let queue = InMemoryQueue::new(Serializer::Cbor);
///     let connection = queue.get_connection().await?;
///     
///     Ok(())
/// }
/// ```
pub struct InMemoryQueue {
    /// The connection to the queue.
    connection: InMemoryConnection,
}

impl InMemoryQueue {
    /// Create a new in-memory queue.
    pub fn new(serializer: Serializer) -> Self {
        Self {
            connection: InMemoryConnection::new(serializer),
        }
    }
}

#[async_trait]
impl Queue for InMemoryQueue {
    type Connection = InMemoryConnection;

    async fn get_connection(&self) -> Result<Self::Connection> {
        Ok(self.connection.clone())
    }
}

/// An in-memory implementation of [`Connection`].
///
/// This implementation maintains a stable set of queues, and is cloneable. Each
/// clone of the connection will maintain references to the same underlying
/// queues.
///
/// ```
/// use paladin::queue::{Queue, Connection, in_memory::InMemoryQueue};
/// use paladin::serializer::Serializer;
/// use anyhow::Result;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let queue = InMemoryQueue::new(Serializer::Cbor);
///     let connection = queue.get_connection().await?;
///     // Declare a queue
///     let handle = connection.declare_queue("my_queue").await?;
///
///     // ...
///     // Delete a queue
///     connection.delete_queue("my_queue").await?;
///     
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct InMemoryConnection {
    /// The queues managed by this connection. Queues are indexed by their name
    /// and stored in an atomically reference counted pointer to allow for
    /// multiple clones of the connection to maintain references to the same
    /// queues.
    queues: Arc<Mutex<HashMap<String, InMemoryQueueHandle>>>,
    /// The serializer to use for serializing and deserializing messages.
    serializer: Serializer,
}

impl InMemoryConnection {
    pub fn new(serializer: Serializer) -> Self {
        Self {
            queues: Default::default(),
            serializer,
        }
    }
}

#[async_trait]
impl Connection for InMemoryConnection {
    type QueueHandle = InMemoryQueueHandle;

    async fn declare_queue(&self, name: &str) -> Result<Self::QueueHandle> {
        let mut lock = self.queues.lock().await;
        match lock.get(name) {
            Some(queue) => Ok(queue.clone()),
            None => {
                let queue = InMemoryQueueHandle::new(self.serializer);
                lock.insert(name.to_string(), queue.clone());
                Ok(queue)
            }
        }
    }

    async fn delete_queue(&self, name: &str) -> Result<()> {
        let mut lock = self.queues.lock().await;
        if lock.get(name).is_some() {
            lock.remove(name);
        }
        Ok(())
    }
}

/// An in-memory implementation of [`QueueHandle`].
///
/// Cloning this handle will create a new handle that points to the same set of
/// messages and synchronization state.
///
/// ```
/// use paladin::{
///     serializer::Serializer,
///     acker::Acker,
///     queue::{Queue, Connection, QueueHandle, Consumer, in_memory::InMemoryQueue}
/// };
/// use serde::{Serialize, Deserialize};
/// use anyhow::Result;
/// use futures::StreamExt;
///
/// #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
/// struct MyStruct {
///     field: String,
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let queue = InMemoryQueue::new(Serializer::Cbor);
///     let connection = queue.get_connection().await?;
///     // Declare a queue
///     let handle = connection.declare_queue("my_queue").await?;
///
///     // Publish a message
///     handle.publish(&MyStruct { field: "Hello, World!".to_string() }).await?;
///
///     // Consume the message
///     let consumer = handle.declare_consumer("my_consumer").await?;
///     if let Some((message, acker)) = consumer.stream::<MyStruct>().await?.next().await {
///         acker.ack().await?;
///         assert_eq!(message, MyStruct { field: "Hello, World!".to_string() });
///     }
///     
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct InMemoryQueueHandle {
    /// The messages in the queue. They're stored as raw bytes to simulate a
    /// real queue where serialization and deserialization is required.
    messages: Arc<Mutex<VecDeque<Vec<u8>>>>,
    /// The number of messages in the queue.
    num_messages: PollSemaphore,
    /// The serializer to use for serializing and deserializing messages.
    serializer: Serializer,
}

impl InMemoryQueueHandle {
    pub fn new(serializer: Serializer) -> Self {
        Self {
            messages: Default::default(),
            num_messages: PollSemaphore::new(Arc::new(Semaphore::new(0))),
            serializer,
        }
    }
}

#[async_trait]
impl QueueHandle for InMemoryQueueHandle {
    type Consumer = InMemoryConsumer;

    async fn publish<PayloadTarget: Serializable>(&self, payload: &PayloadTarget) -> Result<()> {
        let mut lock = self.messages.lock().await;
        lock.push_back(self.serializer.to_bytes(payload)?);
        self.num_messages.add_permits(1);
        drop(lock);

        Ok(())
    }

    async fn declare_consumer(&self, _consumer_name: &str) -> Result<Self::Consumer> {
        Ok(InMemoryConsumer {
            messages: self.messages.clone(),
            num_messages: self.num_messages.clone(),
            serializer: self.serializer,
        })
    }
}

/// An in-memory implementation of [`Consumer`].
///
/// ```
/// use paladin::{
///     serializer::Serializer,
///     acker::Acker,
///     queue::{Queue, Connection, QueueHandle, Consumer, in_memory::InMemoryQueue}
/// };
/// use serde::{Serialize, Deserialize};
/// use anyhow::Result;
/// use futures::StreamExt;
///
/// #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
/// struct MyStruct {
///     field: String,
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let queue = InMemoryQueue::new(Serializer::Cbor);
///     let connection = queue.get_connection().await?;
///     // Declare a queue
///     let handle = connection.declare_queue("my_queue").await?;
///
///     // Publish a message
///     handle.publish(&MyStruct { field: "Hello, World!".to_string() }).await?;
///
///     // Consume the message
///     let consumer = handle.declare_consumer("my_consumer").await?;
///     if let Some((message, acker)) = consumer.stream::<MyStruct>().await?.next().await {
///         acker.ack().await?;
///         assert_eq!(message, MyStruct { field: "Hello, World!".to_string() });
///     }
///     
///     Ok(())
/// }
/// ```
///
/// # Design
/// The consumer holds a reference to its owning queue's messages and a
/// [`PollSemaphore`] that is used to synchronize with the queue's message
/// availability. When the consumer is polled, it will acquire a
/// permit from the semaphore, which will return pending if no messages are
/// available. This effectively simulates a message push.
/// Once a permit is acquired, the consumer will pop a message from the queue
/// and release the permit, signaling to the queue that a message has been
/// consumed.
#[derive(Clone)]
pub struct InMemoryConsumer {
    messages: Arc<Mutex<VecDeque<Vec<u8>>>>,
    serializer: Serializer,
    num_messages: PollSemaphore,
}

/// A [`Stream`] implementation for [`InMemoryConsumer`].
///
/// # Design
/// This stream will poll the semaphore to acquire a permit, which will return
/// pending if no messages are available. Once a permit is acquired, the stream
/// will attempt to acquire a lock on the queue's messages. Once the lock is
/// acquired, the stream will pop a message from the queue and release the
/// permit, signaling to the queue that a message has been consumed.
pub struct ConsumerStream<T: Serializable> {
    _marker: std::marker::PhantomData<T>,
    messages: Arc<Mutex<VecDeque<Vec<u8>>>>,
    lock_fut: Option<(
        OwnedMutexLockFuture<VecDeque<Vec<u8>>>,
        OwnedSemaphorePermit,
    )>,
    serializer: Serializer,
    num_messages: PollSemaphore,
}

impl<T: Serializable> Stream for ConsumerStream<T> {
    type Item = (T, NoopAcker);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut();

        // If we have a lock future, poll it
        match this.lock_fut.take() {
            Some((mut fut, permit)) => {
                match pin!(&mut fut).poll(cx) {
                    Poll::Ready(mut lock) => {
                        // We have a lock, pop the message
                        let item = lock.pop_front();
                        // Release the permit, signaling that we've consumed a message
                        permit.forget();
                        // Release the lock
                        drop(lock);
                        // Clear the lock future
                        this.lock_fut = None;

                        match item {
                            Some(item) => {
                                let item = this
                                    .serializer
                                    .from_bytes(&item)
                                    .expect("failed to deserialize");

                                Poll::Ready(Some((item, NoopAcker::new())))
                            }
                            None => {
                                // Should never happen given that permits should correspond 1:1 to
                                // messages. Error out so we can debug the logic error.
                                unreachable!("permit was acquired, but no message was available")
                            }
                        }
                    }
                    // Lock future is not ready
                    Poll::Pending => {
                        this.lock_fut = Some((fut, permit));
                        Poll::Pending
                    }
                }
            }
            // Otherwise, wait for a message
            None => {
                let permit = ready!(this.num_messages.poll_acquire(cx));
                match permit {
                    // If we have a permit, a message should be available
                    Some(permit) => {
                        // Create a lock future and poll ourselves
                        this.lock_fut = Some((this.messages.clone().lock_owned(), permit));
                        self.poll_next(cx)
                    }
                    None => Poll::Pending,
                }
            }
        }
    }
}

#[async_trait]
impl Consumer for InMemoryConsumer {
    type Acker = NoopAcker;
    type Stream<T: Serializable> = ConsumerStream<T>;

    async fn stream<T: Serializable>(self) -> Result<Self::Stream<T>> {
        Ok(ConsumerStream {
            messages: self.messages,
            serializer: self.serializer,
            num_messages: self.num_messages,
            lock_fut: None,
            _marker: std::marker::PhantomData,
        })
    }
}
