use std::{ops::RangeInclusive, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify, RwLock};
use uuid::Uuid;

use super::IndexedStream;
use crate::{
    contiguous::{Contiguous, ContiguousQueue},
    directive::Foldable,
    operation::{Monoid, Operation},
    runtime::Runtime,
    task::{Task, TaskOutput},
};

/// Metadata for a [`Fold`] [`Task`].
///
/// The metadata contains the range of indices that the result of the task
/// represents. Because we're orchestrating a distributed fold operation, and we
/// need to maintain associativity of the operation, we need to know the range
/// of indices that a given result comprises.
#[derive(Serialize, Deserialize, Debug)]
struct Metadata {
    range: RangeInclusive<usize>,
}

impl<Op: Operation> TaskOutput<Op, Metadata> {
    /// Check if the result is the final result of the fold operation.
    ///
    /// This is the case if the range of indices that the result represents is
    /// equal to the size of the job, as it means that the result is the
    /// combination of all the elements in the job.
    fn is_final(&self, job_size: usize) -> bool {
        self.metadata.range.end() - self.metadata.range.start() + 1 == job_size
    }
}

/// A [`Contiguous`] [`TaskOutput`] for [`Task`]s.
impl<Op: Operation> Contiguous for TaskOutput<Op, Metadata> {
    type Key = usize;

    /// Check if the given [`TaskOutput`] is contiguous with the given one.
    fn is_contiguous(&self, other: &Self) -> bool {
        self.metadata.range.end() + 1 == *other.metadata.range.start()
            || *other.metadata.range.end() + 1 == *self.metadata.range.start()
    }

    /// Get the key of the [`TaskOutput`].
    ///
    /// We can be sure that the start of the range is safe, as Stream input with
    /// duplicated indices is not allowed and constitutes a logic error.
    fn key(&self) -> &Self::Key {
        self.metadata.range.start()
    }
}

type Sender<'a, Op> =
    Box<dyn Sink<Task<'a, Op, Metadata>, Error = anyhow::Error> + Send + Unpin + 'a>;

/// A [`Dispatcher`] abstracts over the common functionality of queuing and
/// dispatching contiguous [`Task`]s to worker processes.
///
/// It is effectively a thin wrapper around a [`ContiguousQueue`], specified to
/// the `Fold` semantics.
///
/// It handles:
/// - Queuing [`TaskResult`]s.
/// - Dequeuing [`TaskResult`]s.
/// - Attempting to dispatch [`TaskResult`]s if they are contiguous with another
///   [`TaskResult`].
struct Dispatcher<'a, Op: Monoid> {
    op: &'a Op,
    assembler: Arc<Mutex<ContiguousQueue<TaskOutput<Op, Metadata>>>>,
    sender: Arc<Mutex<Sender<'a, Op>>>,
    channel_identifier: Uuid,
}

impl<'a, Op: Monoid> Dispatcher<'a, Op> {
    fn new(
        op: &'a Op,
        assembler: Arc<Mutex<ContiguousQueue<TaskOutput<Op, Metadata>>>>,
        sender: Arc<Mutex<Sender<'a, Op>>>,
        channel_identifier: Uuid,
    ) -> Self {
        Self {
            op,
            assembler,
            sender,
            channel_identifier,
        }
    }

    /// Queue the given [`TaskResult`].
    async fn queue(&self, result: TaskOutput<Op, Metadata>) {
        let mut assembler = self.assembler.lock().await;
        assembler.queue(result);
    }

    /// Dequeue the [`TaskResult`] at the given index.
    async fn dequeue(&self, idx: &usize) -> Option<TaskOutput<Op, Metadata>> {
        let mut assembler = self.assembler.lock().await;
        assembler.dequeue(idx)
    }

    /// Attempt to dispatch the given [`TaskResult`].
    ///
    /// If the given [`TaskResult`] is contiguous with another [`TaskResult`],
    /// then the two are combined and dispatched for further combination.
    async fn try_dispatch(&self, result: TaskOutput<Op, Metadata>) -> Result<()> {
        let mut assembler = self.assembler.lock().await;

        if let Some((lhs, rhs)) = assembler.acquire_contiguous_pair_or_queue(result) {
            let task = Task {
                routing_key: self.channel_identifier,
                metadata: Metadata {
                    range: *lhs.metadata.range.start()..=*rhs.metadata.range.end(),
                },
                op: self.op,
                input: (lhs.output, rhs.output),
            };
            let mut sender = self.sender.lock().await;
            sender.send(task).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl<'a, A: Send + 'a, B: Send + 'a> Foldable<'a, B> for IndexedStream<'a, A> {
    async fn f_fold<M: Monoid<Elem = A>>(self, m: &'a M, runtime: &Runtime) -> Result<A> {
        let (channel_identifier, sender, mut receiver) =
            runtime.lease_coordinated_task_channel().await?;

        // mutable access to the assembler. So we wrap it in an Arc<Mutex<>>.
        let assembler = Arc::new(Mutex::new(ContiguousQueue::new()));
        // Both the initialization step and the result stream need to asynchronous
        // mutable access to the sender. So we wrap it in an Arc<Mutex<>>.
        let sender = Arc::new(Mutex::new(sender));
        // Initialize the dispatcher.
        let dispatcher = Arc::new(Dispatcher::new(
            m,
            assembler.clone(),
            sender.clone(),
            channel_identifier,
        ));
        // The size of the input is not known until every item in the input stream has
        // been received. We don't want to block result stream consumption
        // waiting for the initialization future to finish, as there
        // may be a significant delay if the input operations take a long time to
        // compute. As such, we provide this `resolved_input_size`, which is an
        // `RwLock` over an `Option<usize>`. If the lock contains a `Some`, then
        // the initialization has completed and the size is known.
        // If it contains a None, then the initialization has not completed, and the
        // total size is not yet known.
        let resolved_input_size = Arc::new(RwLock::new(None as Option<usize>));
        // Messages shouldn't be dispatched to worker processes until at least two
        // messages have been received, as there is no work to do if there are
        // fewer than 2 items in the input. If the input is empty, we simply
        // return the empty element. If the input has a single element, we
        // simply return it.
        let should_dispatch = Arc::new(Notify::new());

        let resolved_size_clone = resolved_input_size.clone();

        // Initialize the assembler, dispatching tasks as contiguous pairs become
        // available. We're doing double duty here, as we also compute the size
        // of the job by tallying the number of inputs.
        let init = self
            .try_fold(0, |sum, (idx, item)| {
                let dispatcher = dispatcher.clone();
                let should_dispatch = should_dispatch.clone();

                async move {
                    // Given that the operation is a monoid, and input type must match output type,
                    // we can safely place the input into a `TaskResult`.
                    // Because this represents an uncombined input, we set the
                    // range equal to its index.
                    let item_result = TaskOutput {
                        metadata: Metadata { range: idx..=idx },
                        output: item,
                    };

                    let next_sum = sum + 1;
                    if next_sum < 2 {
                        // Do not attempt a dispatch if we've seen fewer than two inputs.
                        dispatcher.queue(item_result).await;
                    } else {
                        // Now that we've seen at least two inputs, we can start dispatching.
                        // Notify the result stream that it can start consuming.
                        should_dispatch.notify_one();
                        // Dispatch the task.
                        dispatcher.try_dispatch(item_result).await?;
                    }

                    // Tally the number of inputs.
                    Ok::<_, anyhow::Error>(next_sum)
                }
            })
            .and_then(|size| async move {
                // Once the above future has completed, the size of the job is known.
                // We can place the final value in `resolved_job_size`.
                let mut lock = resolved_size_clone.write().await;
                *lock = Some(size);
                Ok::<_, anyhow::Error>(size)
            });

        let result_handle = {
            let resolved_input_size = resolved_input_size.clone();
            let dispatcher = dispatcher.clone();
            let should_dispatch = should_dispatch.clone();

            async move {
                // Wait until at least two inputs have been received.
                should_dispatch.notified().await;

                while let Some((result, acker)) = receiver.next().await {
                    let result = result?;
                    // Check to see if the input size is known.
                    if let Some(job_size) = *resolved_input_size.read().await {
                        // If it is, we can check to see if the result is the final result
                        // (i.e., its range comprises the size of
                        // the input).
                        if result.is_final(job_size) {
                            sender.lock().await.close().await?;
                            return Ok(result.output);
                        }
                    }

                    dispatcher.try_dispatch(result).await?;
                    acker.ack().await?;
                }

                Ok(m.empty())
            }
        };

        let size = init.await?;
        match size {
            0 => {
                // If the input is empty, we return the empty element.
                return Ok(m.empty());
            }
            1 => {
                // If the input has a single element, we return it.
                // The dispatcher is guaranteed to have queued the single input element at this
                // point, so we can safely dequeue it. If it doesn't, this is a
                // logic error.
                return Ok(dispatcher
                    .dequeue(&0)
                    .await
                    .expect("Expected dispatcher to have a single element at index 0")
                    .output);
            }
            _ => {}
        }

        result_handle.await
    }
}
