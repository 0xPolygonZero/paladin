//! An in-memory implementation of [`Connection`] and its associated types.
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
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use crossbeam::queue::SegQueue;
use dashmap::{mapref::entry::Entry, DashMap};
use futures::{ready, Stream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::PollSemaphore;

use super::{Connection, DeliveryMode, QueueHandle, QueueOptions, SyndicationMode};
use crate::{
    acker::NoopAcker,
    serializer::{Serializable, Serializer},
};

/// An in-memory implementation of [`Connection`].
///
/// This implementation maintains a stable set of queues, and is cloneable. Each
/// clone of the connection will maintain references to the same underlying
/// queues.
///
/// ```
/// use paladin::queue::{Connection, QueueOptions, in_memory::InMemoryConnection};
/// use paladin::serializer::Serializer;
/// use anyhow::Result;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let connection = InMemoryConnection::new(Serializer::default());
///     // Declare a queue
///     let handle = connection.declare_queue("my_queue", QueueOptions::default()).await?;
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

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn declare_queue(&self, name: &str, options: QueueOptions) -> Result<Self::QueueHandle> {
        match self.queues.entry(name.to_string()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let queue = InMemoryQueueHandle::new(self.serializer, options);
                entry.insert(queue.clone());
                Ok(queue)
            }
        }
    }

    async fn delete_queue(&self, name: &str) -> Result<()> {
        self.queues.remove(name);

        Ok(())
    }
}

/// Queue implementation for [`SyndicationMode::ExactlyOnce`].
///
/// Exactly once queues must guarantee that each message is delivered to exactly
/// one consumer. As such, this implementation only has a single `messages`
/// queue, from which, all consumers will pop messages.
#[derive(Clone)]
struct ExactlyOnceQueue {
    /// The messages in the queue.
    messages: Arc<SegQueue<Bytes>>,
    /// The number of messages in the queue.
    num_messages: PollSemaphore,
    /// The queue options.
    _options: QueueOptions,
    /// The serializer to use for serializing and deserializing messages.
    serializer: Serializer,
}

impl Default for ExactlyOnceQueue {
    fn default() -> Self {
        Self {
            messages: Default::default(),
            num_messages: PollSemaphore::new(Arc::new(Semaphore::new(0))),
            _options: Default::default(),
            serializer: Default::default(),
        }
    }
}

impl ExactlyOnceQueue {
    fn new(options: QueueOptions, serializer: Serializer) -> Self {
        Self {
            messages: Default::default(),
            num_messages: PollSemaphore::new(Arc::new(Semaphore::new(0))),
            _options: options,
            serializer,
        }
    }

    fn publish<PayloadTarget: Serializable>(&self, payload: &PayloadTarget) -> Result<()> {
        let bytes = Bytes::from(self.serializer.to_bytes(payload)?);
        self.messages.push(bytes.clone());
        self.num_messages.add_permits(1);
        Ok(())
    }

    fn declare_consumer<PayloadTarget: Serializable>(
        &self,
        _consumer_name: &str,
    ) -> Result<InMemoryConsumer<PayloadTarget>> {
        Ok(InMemoryConsumer {
            messages: self.messages.clone(),
            num_messages: self.num_messages.clone(),
            serializer: self.serializer,
            permit: None,
            _marker: std::marker::PhantomData,
        })
    }
}

/// A per-consumer broadcast queue for [`InMemoryQueueHandle`].
///
/// This queue is used to synchronize messages between the broadcast handle and
/// its consumers.
///
/// This differs from the implementation of [`ExactlyOnceQueue`] in that each
/// consumer has its own queue of messages. This allows for each consumer to
/// have its own queue of messages, which is required for broadcast queues.
#[derive(Clone)]
struct BroadcastConsumer {
    /// The messages in the queue.
    messages: Arc<SegQueue<Bytes>>,
    /// The number of messages in the queue.
    num_messages: PollSemaphore,
    /// The seen message IDs.
    seen: Arc<DashMap<u64, ()>>,
}

impl Default for BroadcastConsumer {
    fn default() -> Self {
        Self {
            messages: Default::default(),
            num_messages: PollSemaphore::new(Arc::new(Semaphore::new(0))),
            seen: Default::default(),
        }
    }
}

/// Queue implementation for [`SyndicationMode::Broadcast`].
///
/// Broadcast queues must guarantee that each message is delivered to every
/// consumer. As such, this implementation maintains a set of consumers, each
/// with their own queue of messages.
///
/// # Design
/// This implementation maintains a map of consumers, each with their own queue
/// of messages. When a message is published, it is pushed to each consumer's
/// queue.
///
/// A message history is maintained to support queues declared with the
/// [`DeliveryMode::Persistent`] option. If this is enabled, messages will be
/// pushed to the history queue when there are no consumers. When a new consumer
/// is declared, the history queue will be drained into the new consumer's
/// queue.
///
/// We maintain a global message counter to assign a unique ID to each message.
/// This allows us to avoid sending messages to consumers that have already seen
/// the message, especially in a heavily concurrent environment.
#[derive(Clone, Default)]
struct BroadcastQueue {
    /// The consumers subscribed to the broadcast queue.
    consumers: Arc<DashMap<String, BroadcastConsumer>>,
    /// All messages.
    ///
    /// This can be used to replay messages to new consumers with delivery mode
    /// set to [`DeliveryMode::Persistent`].
    history: Arc<SegQueue<(u64, Bytes)>>,
    /// A global message counter.
    message_counter: Arc<AtomicU64>,
    /// The queue options.
    options: QueueOptions,
    /// The serializer to use for serializing and deserializing messages.
    serializer: Serializer,
}

impl BroadcastQueue {
    fn new(options: QueueOptions, serializer: Serializer) -> Self {
        Self {
            options,
            serializer,
            ..Default::default()
        }
    }

    fn publish<PayloadTarget: Serializable>(&self, payload: &PayloadTarget) -> Result<()> {
        let bytes = Bytes::from(self.serializer.to_bytes(payload)?);
        // Assign a unique message ID to the message such that consumers can
        // track which messages they've seen.
        let message_id = self.message_counter.fetch_add(1, Ordering::Relaxed);

        // If the delivery mode is persistent and there are no consumers, push
        // the message to the history queue such that new consumers can replay
        // the message.
        if DeliveryMode::Persistent == self.options.delivery_mode && self.consumers.is_empty() {
            self.history.push((message_id, bytes.clone()));
        }

        for consumer in self.consumers.iter() {
            // Newly added consumers may _concurrently_ see messages from history while
            // propagating the new message. In other words, a new consumer may come online
            // _while_ new messages are being published. To prevent this, we check if the
            // consumer has already seen the message.
            if DeliveryMode::Persistent == self.options.delivery_mode {
                match consumer.seen.entry(message_id) {
                    Entry::Occupied(_) => continue,
                    Entry::Vacant(entry) => {
                        entry.insert(());
                    }
                }
            }

            consumer.messages.push(bytes.clone());
            consumer.num_messages.add_permits(1);
        }

        Ok(())
    }

    fn declare_consumer<PayloadTarget: Serializable>(
        &self,
        consumer_name: &str,
    ) -> Result<InMemoryConsumer<PayloadTarget>> {
        match self.consumers.entry(consumer_name.to_string()) {
            Entry::Occupied(entry) => {
                let consumer = entry.get().clone();
                Ok(InMemoryConsumer {
                    messages: consumer.messages.clone(),
                    num_messages: consumer.num_messages.clone(),
                    serializer: self.serializer,
                    permit: None,
                    _marker: std::marker::PhantomData,
                })
            }
            Entry::Vacant(entry) => {
                // If the delivery mode is persistent and there are messages in
                // the history queue, there are unseen messages that need to be
                // replayed.
                let (messages, seen) = if DeliveryMode::Persistent == self.options.delivery_mode
                    && !self.history.is_empty()
                {
                    let messages = SegQueue::new();
                    let mut seen = HashMap::new();

                    // Populate the new consumer's queue with messages from the
                    // history queue.
                    while let Some((message_id, message)) = self.history.pop() {
                        // Ensure that during a concurrent publish the message is not replayed.
                        match seen.entry(message_id) {
                            std::collections::hash_map::Entry::Occupied(_) => continue,
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                entry.insert(());
                            }
                        }

                        messages.push(message);
                    }
                    (messages, seen)
                } else {
                    (Default::default(), Default::default())
                };

                let consumer = BroadcastConsumer {
                    num_messages: PollSemaphore::new(Arc::new(Semaphore::new(seen.len()))),
                    messages: Arc::new(messages),
                    seen: Arc::new(seen.into_iter().collect()),
                };

                entry.insert(consumer.clone());
                Ok(InMemoryConsumer {
                    messages: consumer.messages.clone(),
                    num_messages: consumer.num_messages.clone(),
                    serializer: self.serializer,
                    permit: None,
                    _marker: std::marker::PhantomData,
                })
            }
        }
    }
}

/// An in-memory implementation of [`QueueHandle`].
///
/// # Example
///
/// ## Exactly Once Mode
/// ```
/// # use paladin::{
/// #     serializer::Serializer,
/// #     acker::Acker,
/// #     queue::{
/// #         Connection,
/// #         SyndicationMode,
/// #         DeliveryMode,
/// #         QueueOptions,
/// #         QueueHandle,
/// #         in_memory::InMemoryConnection,
/// #     }
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
///     let connection = InMemoryConnection::new(Serializer::default());
///
///     // Declare a queue
///     let handle = connection.declare_queue("my_queue", QueueOptions {
///         syndication_mode: SyndicationMode::ExactlyOnce,
///         delivery_mode: DeliveryMode::Persistent,
///         ..Default::default()
///     }).await?;
///
///     // Publish a message
///     handle.publish(&MyStruct { field: "Hello, World!".to_string() }).await?;
///
///     // Consume the message
///     let mut consumer = handle.declare_consumer::<MyStruct>("my_consumer").await?;
///     if let Some((message, acker)) = consumer.next().await {
///         acker.ack().await?;
///         assert_eq!(message, MyStruct { field: "Hello, World!".to_string() });
///     }
/// #    
/// #    Ok(())
/// }
/// ```
///
/// ## Broadcast Mode
/// ```
/// # use paladin::{
/// #     serializer::Serializer,
/// #     acker::Acker,
/// #         queue::{Connection,
/// #         QueueOptions,
/// #         SyndicationMode,
/// #         DeliveryMode,
/// #         QueueHandle,
/// #         in_memory::InMemoryConnection
/// #     }
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
///     let connection = InMemoryConnection::new(Serializer::default());
///
///     // Declare a queue
///     let handle = connection.declare_queue("my_queue", QueueOptions {
///         syndication_mode: SyndicationMode::Broadcast,
///         ..Default::default()
///     }).await?;
///
///     let mut consumer_1 = handle.declare_consumer::<MyStruct>("consumer_1").await?;
///     let mut consumer_2 = handle.declare_consumer::<MyStruct>("consumer_2").await?;
///
///     // Consume the message in the first consumer
///     let consumer_task_1 = tokio::spawn(async move {
///         let (message, acker) = consumer_1.next().await.unwrap();
///         acker.ack().await.unwrap();
///         message
///     });
///
///     // Consume the message in the second consumer
///     let consumer_task_2 = tokio::spawn(async move {
///         let (message, acker) = consumer_2.next().await.unwrap();
///         acker.ack().await.unwrap();
///         message
///     });
///
///     // Publish a message
///     handle.publish(&MyStruct { field: "Hello, World!".to_string() }).await?;
///
///     assert_eq!(consumer_task_1.await.unwrap(), MyStruct { field: "Hello, World!".to_string() });
///     assert_eq!(consumer_task_2.await.unwrap(), MyStruct { field: "Hello, World!".to_string() });
/// #
/// #    Ok(())
/// }
/// ```
#[derive(Clone, Default)]
pub struct InMemoryQueueHandle {
    /// The broadcast queue instance.
    broadcast_queue: BroadcastQueue,
    /// The exactly once queue instance.
    exactly_once_queue: ExactlyOnceQueue,
    /// The queue options.
    options: QueueOptions,
}

impl InMemoryQueueHandle {
    pub fn new(serializer: Serializer, options: QueueOptions) -> Self {
        Self {
            options,
            broadcast_queue: BroadcastQueue::new(options, serializer),
            exactly_once_queue: ExactlyOnceQueue::new(options, serializer),
        }
    }
}

#[async_trait]
impl QueueHandle for InMemoryQueueHandle {
    type Acker = NoopAcker;
    type Consumer<PayloadTarget: Serializable> = InMemoryConsumer<PayloadTarget>;

    /// Publish a message to the broadcast channel.
    ///
    /// This will push the message to all consumers of the broadcast channel.
    async fn publish<PayloadTarget: Serializable>(&self, payload: &PayloadTarget) -> Result<()> {
        match self.options.syndication_mode {
            SyndicationMode::ExactlyOnce => self.exactly_once_queue.publish(payload),
            SyndicationMode::Broadcast => self.broadcast_queue.publish(payload),
        }
    }

    async fn declare_consumer<PayloadTarget: Serializable>(
        &self,
        consumer_name: &str,
    ) -> Result<Self::Consumer<PayloadTarget>> {
        match self.options.syndication_mode {
            SyndicationMode::ExactlyOnce => self.exactly_once_queue.declare_consumer(consumer_name),
            SyndicationMode::Broadcast => self.broadcast_queue.declare_consumer(consumer_name),
        }
    }
}

/// A [`Stream`] implementation for [`InMemoryConsumer`].
///
/// # Design
/// This stream will poll the semaphore to acquire a permit, which will return
/// pending if no messages are available. Once a permit is acquired, the stream
/// will attempt to acquire a lock on the queue's messages. Once the lock is
/// acquired, the stream will pop a message from the queue and release the
/// permit, signaling to the queue that a message has been consumed.
pub struct InMemoryConsumer<T> {
    _marker: std::marker::PhantomData<T>,
    messages: Arc<SegQueue<Bytes>>,
    permit: Option<OwnedSemaphorePermit>,
    serializer: Serializer,
    num_messages: PollSemaphore,
}

impl<T: Serializable> Stream for InMemoryConsumer<T> {
    type Item = (T, NoopAcker);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut();

        // If we have a lock future, poll it
        match this.permit.take() {
            Some(permit) => {
                // We have a lock, pop the message
                let item = this.messages.pop();
                // Release the permit, signaling that we've consumed a message
                permit.forget();
                // Clear the lock future
                this.permit = None;

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

            // Otherwise, wait for a message
            None => {
                let permit = ready!(this.num_messages.poll_acquire(cx));
                match permit {
                    // If we have a permit, a message should be available
                    Some(permit) => {
                        // Create a lock future and poll ourselves
                        this.permit = Some(permit);
                        self.poll_next(cx)
                    }
                    None => Poll::Pending,
                }
            }
        }
    }
}

#[cfg(test)]
mod helpers {
    use std::time::Duration;

    use futures::{Future, StreamExt};
    use serde::{Deserialize, Serialize};
    use tokio::{
        task::{JoinError, JoinHandle},
        try_join,
    };

    use super::*;
    use crate::acker::Acker;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    pub(super) struct Payload {
        pub(super) field: i64,
    }

    pub(super) fn new_payload(field: i64) -> Payload {
        Payload { field }
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

    pub(super) fn consume_next(mut consumer: InMemoryConsumer<Payload>) -> JoinHandle<Payload> {
        tokio::spawn(async move {
            let (payload, acker) = consumer.next().await.unwrap();
            acker.ack().await.unwrap();
            payload
        })
    }

    pub(super) fn consume_n(
        consumer: InMemoryConsumer<Payload>,
        n: usize,
    ) -> JoinHandle<Vec<Payload>> {
        tokio::spawn(async move {
            consumer
                .then(|(payload, acker)| async move {
                    acker.ack().await.unwrap();
                    payload
                })
                .take(n)
                .collect::<Vec<_>>()
                .await
        })
    }

    pub(super) fn consume_n_select(
        c1: InMemoryConsumer<Payload>,
        c2: InMemoryConsumer<Payload>,
        n: usize,
    ) -> JoinHandle<Vec<Payload>> {
        tokio::spawn(async move {
            futures::stream::select(c1, c2)
                .then(|(payload, acker)| async move {
                    acker.ack().await.unwrap();
                    payload
                })
                .take(n)
                .collect::<Vec<_>>()
                .await
        })
    }

    pub(super) async fn consumers<P: Serializable, H: QueueHandle>(
        queue: &H,
    ) -> (H::Consumer<P>, H::Consumer<P>) {
        try_join!(queue.declare_consumer("1"), queue.declare_consumer("2")).unwrap()
    }

    pub(super) fn publish<H: QueueHandle + Send + Sync + 'static>(
        queue: &H,
        payload: &Payload,
    ) -> JoinHandle<Result<()>> {
        let payload = payload.clone();
        let queue = queue.clone();
        tokio::spawn(async move { queue.publish(&payload).await })
    }

    pub(super) fn publish_multi<H: QueueHandle + Send + Sync + 'static>(
        queue: &H,
        payload: &[Payload],
    ) -> Vec<JoinHandle<Result<()>>> {
        payload.iter().map(|p| publish(queue, p)).collect()
    }
}

#[cfg(test)]
mod exactly_once {

    use tokio::{join, try_join};

    use super::helpers::*;
    use super::*;
    use crate::queue::*;

    async fn queue_handle() -> InMemoryQueueHandle {
        let connection = InMemoryConnection::new(Serializer::default());
        connection
            .declare_queue(
                "my_queue",
                QueueOptions {
                    delivery_mode: DeliveryMode::Persistent,
                    syndication_mode: SyndicationMode::ExactlyOnce,
                    durability: QueueDurability::NonDurable,
                },
            )
            .await
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn single_message_delivers_once_publish_first() {
        let queue = queue_handle().await;
        publish(&queue, &new_payload(1));

        let (c1, c2) = consumers(&queue).await;
        let (r1, r2) = (consume_next(c1), consume_next(c2));
        let (r1, r2) = join!(with_timeout(r1), with_timeout(r2));

        assert!([r1.clone(), r2.clone()].iter().any(|r| r.is_none()));
        assert!([r1.clone(), r2.clone()]
            .into_iter()
            .any(|r| r == Some(new_payload(1))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn single_message_delivers_once_publish_last() {
        let queue = queue_handle().await;

        let (c1, c2) = consumers(&queue).await;
        let (r1, r2) = (consume_next(c1), consume_next(c2));

        publish(&queue, &new_payload(1));

        let (r1, r2) = join!(with_timeout(r1), with_timeout(r2));

        assert!([r1.clone(), r2.clone()].iter().any(|p| p.is_none()));
        assert!([r1.clone(), r2.clone()]
            .into_iter()
            .any(|p| p == Some(new_payload(1))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn double_message_delivers_once_publish_first() {
        let queue = queue_handle().await;
        publish(&queue, &new_payload(1));
        publish(&queue, &new_payload(2));
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
        publish(&queue, &new_payload(1));
        publish(&queue, &new_payload(2));
        let (r1, r2) = try_join!(r1, r2).unwrap();

        assert_ne!(r1, r2)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn many_messages_single_consumer() {
        let queue = queue_handle().await;
        let payloads = (0..100).map(new_payload).collect::<Vec<_>>();
        publish_multi(&queue, &payloads);

        let c = queue.declare_consumer("1").await.unwrap();
        let mut results = consume_n(c, payloads.len()).await.unwrap();
        results.sort_by_key(|a| a.field);
        assert_eq!(payloads, results)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn many_messages_two_consumers() {
        let queue = queue_handle().await;
        let payloads = (0..100).map(new_payload).collect::<Vec<_>>();
        publish_multi(&queue, &payloads);

        let (c1, c2) = consumers(&queue).await;
        let mut results = consume_n_select(c1, c2, payloads.len()).await.unwrap();
        results.sort_by_key(|a| a.field);
        assert_eq!(payloads, results)
    }
}

#[cfg(test)]
mod broadcast {
    use tokio::{join, try_join};

    use super::helpers::*;
    use super::*;
    use crate::queue::*;

    async fn broadcast_handle() -> InMemoryQueueHandle {
        let connection = InMemoryConnection::new(Serializer::Cbor);
        connection
            .declare_queue(
                "my_broadcast_queue",
                QueueOptions {
                    delivery_mode: DeliveryMode::Persistent,
                    syndication_mode: SyndicationMode::Broadcast,
                    durability: QueueDurability::NonDurable,
                },
            )
            .await
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn single_message_delivers_to_all_publish_last() {
        let queue = broadcast_handle().await;
        let expected = new_payload(1);

        let (c1, c2) = consumers(&queue).await;
        publish(&queue, &expected);
        let (r1, r2) = try_join!(consume_next(c1), consume_next(c2)).unwrap();

        assert_eq!(expected, r1);
        assert_eq!(r1, r2)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn single_message_delivers_to_at_least_one_publish_first() {
        let queue = broadcast_handle().await;

        publish(&queue, &new_payload(1));

        let (c1, c2) = consumers(&queue).await;
        let (r1, r2) = (consume_next(c1), consume_next(c2));
        let (r1, r2) = join!(with_timeout(r1), with_timeout(r2));

        assert!([r1, r2].into_iter().any(|r| r == Some(new_payload(1))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn many_messages_single_consumer_publish_first() {
        let queue = broadcast_handle().await;
        let payloads = (0..5).map(new_payload).collect::<Vec<_>>();
        publish_multi(&queue, &payloads);
        let c = queue.declare_consumer("1").await.unwrap();

        let mut results = consume_n(c, payloads.len()).await.unwrap();
        results.sort_by_key(|a| a.field);

        assert_eq!(payloads, results)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn many_messages_single_consumer_publish_last() {
        let queue = broadcast_handle().await;
        let payloads = (0..5).map(new_payload).collect::<Vec<_>>();
        let c = queue.declare_consumer("1").await.unwrap();
        publish_multi(&queue, &payloads);

        let mut results = consume_n(c, payloads.len()).await.unwrap();
        results.sort_by_key(|a| a.field);

        assert_eq!(payloads, results)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn many_messages_multi_consumer_publish_first() {
        let queue = broadcast_handle().await;
        let payloads = (0..5).map(new_payload).collect::<Vec<_>>();
        publish_multi(&queue, &payloads);
        let (c1, c2) = consumers(&queue).await;

        let (mut r1, mut r2) = join!(
            with_timeout(consume_n(c1, payloads.len())),
            with_timeout(consume_n(c2, payloads.len()))
        );
        if let Some(v) = r1.as_mut() {
            v.sort_by_key(|a| a.field);
        }
        if let Some(v) = r2.as_mut() {
            v.sort_by_key(|a| a.field);
        }
        let expected = Some(payloads);

        assert!([r1, r2].into_iter().any(|r| r == expected));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn many_messages_multi_consumer_publish_last() {
        let queue = broadcast_handle().await;
        let payloads = (0..5).map(new_payload).collect::<Vec<_>>();
        let (c1, c2) = consumers(&queue).await;
        publish_multi(&queue, &payloads);

        let (mut r1, mut r2) = join!(
            with_timeout(consume_n(c1, payloads.len())),
            with_timeout(consume_n(c2, payloads.len()))
        );
        if let Some(v) = r1.as_mut() {
            v.sort_by_key(|a| a.field);
        }
        if let Some(v) = r2.as_mut() {
            v.sort_by_key(|a| a.field);
        }
        let expected = Some(payloads);

        assert!([r1, r2].into_iter().any(|r| r == expected));
    }
}
