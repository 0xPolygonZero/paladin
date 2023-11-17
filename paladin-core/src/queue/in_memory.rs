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
    collections::VecDeque,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Result;
use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use futures::{
    lock::{Mutex, OwnedMutexLockFuture},
    ready, Future, Stream,
};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
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
    /// The queues managed by this connection.
    ///
    /// Queues are indexed by their name  and stored in an atomically reference
    /// counted pointer to allow for multiple clones of the connection to
    /// maintain references to the same queues.
    queues: Arc<DashMap<String, InMemoryQueueHandle>>,
    /// The broadcasts managed by this connection.
    ///
    /// Broadcasts are indexed by their name and stored in an atomically
    /// reference counted pointer to allow for multiple clones of the connection
    /// to maintain references to the same broadcasts.
    broadcasts: Arc<DashMap<String, InMemoryBroadcastHandle>>,
    /// The serializer to use for serializing and deserializing messages.
    serializer: Serializer,
}

impl InMemoryConnection {
    pub fn new(serializer: Serializer) -> Self {
        Self {
            queues: Default::default(),
            broadcasts: Default::default(),
            serializer,
        }
    }
}

#[async_trait]
impl Connection for InMemoryConnection {
    type QueueHandle = InMemoryQueueHandle;
    type BroadcastHandle = InMemoryBroadcastHandle;

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn declare_queue(&self, name: &str) -> Result<Self::QueueHandle> {
        match self.queues.entry(name.to_string()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let queue = InMemoryQueueHandle::new(self.serializer);
                entry.insert(queue.clone());
                Ok(queue)
            }
        }
    }

    async fn delete_queue(&self, name: &str) -> Result<()> {
        self.queues.remove(name);

        Ok(())
    }

    async fn declare_broadcast(&self, name: &str) -> Result<Self::BroadcastHandle> {
        match self.broadcasts.entry(name.to_string()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let queue = InMemoryBroadcastHandle::new(self.serializer);
                entry.insert(queue.clone());
                Ok(queue)
            }
        }
    }

    async fn delete_broadcast(&self, name: &str) -> Result<()> {
        self.broadcasts.remove(name);

        Ok(())
    }
}

/// An per-consumer queue for [`InMemoryBroadcastHandle`].
///
/// This queue is used to synchronize messages between the broadcast handle and
/// its consumers. The [`InMemoryConsumer`] will be able to pull messages from
/// this queue, and the associated [`InMemoryBroadcastHandle`] will be able to
/// push messages to this queue.
#[derive(Clone)]
pub struct ConsumerBroadcastQueue {
    /// The messages in the queue.
    messages: Arc<Mutex<VecDeque<Vec<u8>>>>,
    /// The number of messages in the queue.
    num_messages: PollSemaphore,
}

/// An in-memory implementation of [`QueueHandle`] for broadcast queues.
///
/// # Example
///
/// ```
/// # use paladin::{
/// #    serializer::Serializer,
/// #    acker::Acker,
/// #    queue::{Queue, Connection, QueueHandle, Consumer, in_memory::InMemoryQueue}
/// # };
/// # use serde::{Serialize, Deserialize};
/// # use anyhow::Result;
/// # use futures::StreamExt;
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
///     let handle = connection.declare_broadcast("my_queue").await?;
///
///     // Publish a message
///     handle.publish(&MyStruct { field: "Hello, World!".to_string() }).await?;
///
///     // Consume the message in the first consumer
///     let consumer = handle.declare_consumer("consumer_1").await?;
///     if let Some((message, acker)) = consumer.stream::<MyStruct>().await?.next().await {
///         acker.ack().await?;
///         assert_eq!(message, MyStruct { field: "Hello, World!".to_string() });
///     }
///
///     // Consume the message in the second consumer
///     let consumer = handle.declare_consumer("consumer_2").await?;
///     if let Some((message, acker)) = consumer.stream::<MyStruct>().await?.next().await {
///         acker.ack().await?;
///         assert_eq!(message, MyStruct { field: "Hello, World!".to_string() });
///     }
/// #
/// #    Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct InMemoryBroadcastHandle {
    /// The consumers subscribed to the broadcast channel.
    consumers: Arc<DashMap<String, ConsumerBroadcastQueue>>,
    /// All messages.
    ///
    /// This can be used to replay messages to new consumers.
    message_history: Arc<RwLock<VecDeque<Vec<u8>>>>,
    /// The serializer to use for serializing and deserializing messages.
    serializer: Serializer,
}

impl InMemoryBroadcastHandle {
    pub fn new(serializer: Serializer) -> Self {
        Self {
            consumers: Default::default(),
            message_history: Default::default(),
            serializer,
        }
    }
}

#[async_trait]
impl QueueHandle for InMemoryBroadcastHandle {
    type Consumer = InMemoryConsumer;

    /// Publish a message to the broadcast channel.
    ///
    /// This will push the message to all consumers of the broadcast channel.
    async fn publish<PayloadTarget: Serializable>(&self, payload: &PayloadTarget) -> Result<()> {
        let bytes = self.serializer.to_bytes(payload)?;
        {
            let mut history = self.message_history.write().await;
            history.push_back(bytes.clone());
        }
        for consumer in self.consumers.iter() {
            let mut lock = consumer.messages.lock().await;
            lock.push_back(bytes.clone());
            consumer.num_messages.add_permits(1);
        }
        Ok(())
    }

    async fn declare_consumer(&self, consumer_name: &str) -> Result<Self::Consumer> {
        let consumer = {
            let message_history = self.message_history.read().await;
            let messages = message_history.clone();
            ConsumerBroadcastQueue {
                num_messages: PollSemaphore::new(Arc::new(Semaphore::new(messages.len()))),
                messages: Arc::new(Mutex::new(messages)),
            }
        };

        self.consumers
            .insert(consumer_name.to_string(), consumer.clone());

        Ok(InMemoryConsumer {
            messages: consumer.messages.clone(),
            num_messages: consumer.num_messages.clone(),
            serializer: self.serializer,
        })
    }
}

/// An in-memory implementation of [`QueueHandle`].
///
/// Cloning this handle will create a new handle that points to the same set of
/// messages and synchronization state.
///
/// # Example
///
/// ```
/// # use paladin::{
/// #    serializer::Serializer,
/// #    acker::Acker,
/// #    queue::{Queue, Connection, QueueHandle, Consumer, in_memory::InMemoryQueue}
/// # };
/// # use serde::{Serialize, Deserialize};
/// # use anyhow::Result;
/// # use futures::StreamExt;
/// #
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
/// #    
/// #    Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct InMemoryQueueHandle {
    /// The messages in the queue.
    ///
    /// They're stored as raw bytes to simulate a real queue where serialization
    /// and deserialization is required.
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

#[cfg(test)]
mod helpers {
    use std::time::Duration;

    use futures::StreamExt;
    use serde::{Deserialize, Serialize};
    use tokio::{
        task::{JoinError, JoinHandle},
        try_join,
    };

    use super::*;
    use crate::acker::Acker;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    pub(super) struct Payload {
        field: String,
    }

    pub(super) fn new_payload(message: &str) -> Payload {
        Payload {
            field: message.to_string(),
        }
    }

    pub(super) async fn with_timeout<O, F: Future<Output = Result<O, JoinError>>>(
        fut: F,
    ) -> Option<O> {
        let timeout = tokio::time::sleep(Duration::from_millis(10));

        tokio::select! {
            result = fut => {
                Some(result.unwrap())
            }
            _ = timeout => {
                None
            }
        }
    }

    pub(super) fn consume_next(consumer: InMemoryConsumer) -> JoinHandle<Payload> {
        tokio::spawn(async move {
            let mut stream = consumer.stream::<Payload>().await.unwrap();
            let (payload, acker) = stream.next().await.unwrap();
            acker.ack().await.unwrap();
            payload
        })
    }

    pub(super) async fn consumers<H: QueueHandle>(queue: &H) -> (H::Consumer, H::Consumer) {
        try_join!(queue.declare_consumer("1"), queue.declare_consumer("2")).unwrap()
    }

    pub(super) fn publish<H: QueueHandle>(queue: &H, payload: &Payload) -> JoinHandle<Result<()>> {
        let payload = payload.clone();
        let queue = queue.clone();
        tokio::spawn(async move { queue.publish(&payload).await })
    }
}

#[cfg(test)]
mod exactly_once {

    use tokio::{join, try_join};

    use super::helpers::*;
    use super::*;

    async fn queue_handle() -> InMemoryQueueHandle {
        let queue = InMemoryQueue::new(Serializer::Cbor);
        let connection = queue.get_connection().await.unwrap();
        connection.declare_queue("my_queue").await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn single_message_delivers_once_publish_first() {
        let queue = queue_handle().await;
        publish(&queue, &new_payload("1"));

        let (c1, c2) = consumers(&queue).await;
        let (r1, r2) = (consume_next(c1), consume_next(c2));
        let (r1, r2) = join!(with_timeout(r1), with_timeout(r2));

        assert!([r1.clone(), r2.clone()].iter().any(|r| r.is_none()));
        assert!([r1.clone(), r2.clone()]
            .iter()
            .any(|r| r == &Some(new_payload("1"))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn single_message_delivers_once_publish_last() {
        let queue = queue_handle().await;

        let (c1, c2) = consumers(&queue).await;
        let (r1, r2) = (consume_next(c1), consume_next(c2));

        publish(&queue, &new_payload("1"));

        let (r1, r2) = join!(with_timeout(r1), with_timeout(r2));

        assert!([r1.clone(), r2.clone()].iter().any(|p| p.is_none()));
        assert!([r1.clone(), r2.clone()]
            .iter()
            .any(|p| p == &Some(new_payload("1"))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn double_message_delivers_once_publish_first() {
        let queue = queue_handle().await;
        publish(&queue, &new_payload("1"));
        publish(&queue, &new_payload("2"));
        let (c1, c2) = consumers(&queue).await;
        let (r1, r2) = (consume_next(c1), consume_next(c2));
        let (r1, r2) = try_join!(r1, r2).unwrap();

        assert_ne!(r1, r2)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn double_message_delivers_once_publish_last() {
        let queue = queue_handle().await;

        let (c1, c2) = consumers(&queue).await;
        let (r1, r2) = (consume_next(c1), consume_next(c2));
        publish(&queue, &new_payload("1"));
        publish(&queue, &new_payload("2"));
        let (r1, r2) = try_join!(r1, r2).unwrap();

        assert_ne!(r1, r2)
    }
}

#[cfg(test)]
mod broadcast {
    use tokio::try_join;

    use super::helpers::*;
    use super::*;

    async fn broadcast_handle() -> InMemoryBroadcastHandle {
        let queue = InMemoryQueue::new(Serializer::Cbor);
        let connection = queue.get_connection().await.unwrap();
        connection.declare_broadcast("my_queue").await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn single_message_delivers_to_all_publish_first() {
        let queue = broadcast_handle().await;
        let expected = new_payload("1");
        publish(&queue, &expected);

        let (c1, c2) = consumers(&queue).await;
        let (r1, r2) = try_join!(consume_next(c1), consume_next(c2)).unwrap();

        assert_eq!(expected, r1);
        assert_eq!(r1, r2)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn single_message_delivers_to_all_publish_last() {
        let queue = broadcast_handle().await;
        let expected = new_payload("1");

        let (c1, c2) = consumers(&queue).await;
        let (r1, r2) = (consume_next(c1), consume_next(c2));
        publish(&queue, &expected);
        let (r1, r2) = try_join!(r1, r2).unwrap();

        assert_eq!(expected, r1);
        assert_eq!(r1, r2)
    }
}
