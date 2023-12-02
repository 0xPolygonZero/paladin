use std::{
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::Result;
use async_trait::async_trait;
use crossbeam::atomic::AtomicCell;
use futures::{
    channel::mpsc::{self, Sender},
    try_join, SinkExt, StreamExt, TryFutureExt, TryStreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::{select, sync::Notify};
use uuid::Uuid;

use super::IndexedStream;
use crate::{
    contiguous::{Contiguous, ContiguousQueue},
    directive::Foldable,
    operation::{Monoid, Operation},
    queue::PublisherExt,
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
struct Dispatcher<'a, M: Monoid> {
    m: &'a M,
    assembler: Arc<ContiguousQueue<TaskOutput<M, Metadata>>>,
    tx: Sender<Task<'a, M, Metadata>>,
    channel_identifier: Uuid,
}

impl<'a, Op: Monoid + 'static> Dispatcher<'a, Op> {
    /// Queue the given [`TaskResult`].
    async fn queue(&self, result: TaskOutput<Op, Metadata>) {
        self.assembler.queue(result);
    }

    /// Dequeue the [`TaskResult`] at the given index.
    async fn dequeue(&self, idx: &usize) -> Option<TaskOutput<Op, Metadata>> {
        self.assembler.dequeue(idx)
    }

    /// Attempt to dispatch the given [`TaskResult`].
    ///
    /// If the given [`TaskResult`] is contiguous with another [`TaskResult`],
    /// then the two are combined and dispatched for further combination.
    async fn try_dispatch(&self, result: TaskOutput<Op, Metadata>) -> Result<()> {
        if let Some((lhs, rhs)) = self.assembler.acquire_contiguous_pair_or_queue(result) {
            let task = Task {
                routing_key: self.channel_identifier,
                metadata: Metadata {
                    range: *lhs.metadata.range.start()..=*rhs.metadata.range.end(),
                },
                op: self.m,
                input: (lhs.output, rhs.output),
            };
            let mut tx = self.tx.clone();
            tx.send(task).await?;
        }

        Ok(())
    }
}

/// Maximum concurrent operations per task.
///
/// This heuristic restricts the number of simultaneous operations on a _single_
/// task to prevent any single task from monopolizing the thread pool. In other
/// words, we want to ensure as many disparate tasks make progress concurrently
/// with each other as possible.
///
/// In the future, it may be worth making this configurable.
const MAX_CONCURRENCY_PER_TASK: usize = 10;

#[async_trait]
impl<'a, A: Send + Sync + 'a, B: Send + 'a> Foldable<'a, B> for IndexedStream<'a, A> {
    async fn f_fold<M: Monoid<Elem = A>>(self, m: &'a M, runtime: &Runtime) -> Result<A>
    where
        M: 'static,
    {
        let (channel_identifier, sender, receiver) =
            runtime.lease_coordinated_task_channel().await?;
        // Rather than dispatching tasks directly on `sender`, we instead dispatch them
        // on a channel. This allows us to consume tasks as a stream rather than
        // awaiting each publish individually.
        let (mut tx, rx) = mpsc::channel::<Task<'a, M, Metadata>>(MAX_CONCURRENCY_PER_TASK);
        let assembler = Arc::new(ContiguousQueue::new());
        let sender = Arc::new(sender);
        let dispatcher = Arc::new(Dispatcher {
            m,
            assembler: assembler.clone(),
            tx: tx.clone(),
            channel_identifier,
        });
        // Messages shouldn't be dispatched to worker processes until at least two
        // messages have been received, as there is no work to do if there are
        // fewer than 2 items in the input. If the input is empty, we simply
        // return the empty element. If the input has a single element, we
        // simply return it.
        let should_dispatch = Arc::new(Notify::new());
        // The running count of inputs.
        let count = Arc::new(AtomicUsize::new(0));
        // The size of the input is not known until every item in the input stream has
        // been received. We don't want to block result stream consumption waiting for
        // the initialization future to finish, as there may be a significant delay if
        // the input operations take a long time to compute. As such, we provide this
        // `resolved_input_size`, which is an [`AtomicUsize`]. If it contains
        // [`usize::MAX`], then the initialization has not completed, and the total size
        // is not yet known.
        let resolved_input_size = Arc::new(AtomicUsize::new(usize::MAX));

        // Initialize the assembler, dispatching tasks as contiguous pairs become
        // available. We're doing double duty here, as we also compute the size
        // of the job by tallying the number of inputs.
        let init = self
            .try_for_each_concurrent(MAX_CONCURRENCY_PER_TASK, |(idx, item)| {
                let dispatcher = dispatcher.clone();
                let should_dispatch = should_dispatch.clone();
                let count = count.clone();
                async move {
                    // Given that the operation is a monoid, and input type must match output type,
                    // we can safely place the input into a `TaskResult`.
                    // Because this represents an uncombined input, we set the
                    // range equal to its index.
                    let item_result = TaskOutput {
                        metadata: Metadata { range: idx..=idx },
                        output: item,
                    };

                    let next_sum = count.fetch_add(1, Ordering::Relaxed) + 1;
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
                    Ok::<_, anyhow::Error>(())
                }
            })
            .and_then({
                let count = count.clone();
                let dispatcher = dispatcher.clone();
                let resolved_input_size = resolved_input_size.clone();
                |_| {
                    async move {
                        // Once the above future has completed, the size of the job is known.
                        // We can place the final value in `resolved_job_size`.
                        let size = count.load(Ordering::Relaxed);
                        resolved_input_size.store(size, Ordering::Release);
                        match size {
                            // If the input is empty, we return the empty element.
                            0 => Ok(m.empty()),
                            // If the input has a single element, we return it.
                            //
                            // The dispatcher is guaranteed to have queued the single input element
                            // at this point, so we can safely dequeue it. If it doesn't, this is a
                            // logic error.
                            1 => Ok(dispatcher
                                .dequeue(&0)
                                .await
                                .expect("Expected dispatcher to have a single element at index 0")
                                .output),
                            // Otherwise, we wait for the result stream to complete.
                            _ => futures::future::pending().await,
                        }
                    }
                }
            });

        let (final_result_tx, final_result_rx) = futures::channel::oneshot::channel::<M::Elem>();
        let result_processor = {
            let dispatcher = dispatcher.clone();
            let should_dispatch = should_dispatch.clone();
            let final_result_tx = Arc::new(AtomicCell::new(Some(final_result_tx)));

            async move {
                // Wait until at least two inputs have been received.
                should_dispatch.notified().await;

                receiver
                    .map(Ok)
                    .try_for_each_concurrent(MAX_CONCURRENCY_PER_TASK, |(result, acker)| {
                        let resolved_input_size = resolved_input_size.clone();
                        let dispatcher = dispatcher.clone();
                        let final_result_tx = final_result_tx.clone();

                        async move {
                            let result = result?;
                            // Check to see if the input size is known and if the result is the
                            // final result (i.e., its range comprises the size of the input).
                            let resolved_size = resolved_input_size.load(Ordering::Acquire);
                            if usize::MAX != resolved_size && result.is_final(resolved_size) {
                                acker.ack().await?;
                                final_result_tx
                                    .take()
                                    .ok_or_else(|| anyhow::anyhow!("final result tx taken"))?
                                    .send(result.output)
                                    .map_err(|_| anyhow::anyhow!("final result already sent"))?;

                                return Ok::<_, anyhow::Error>(());
                            }

                            try_join!(dispatcher.try_dispatch(result), acker.ack())?;

                            Ok(())
                        }
                    })
                    .await?;

                unreachable!("Result stream should never complete")
            }
        };

        let task_handler = {
            let sender = sender.clone();
            async move {
                sender
                    .publish_all(rx.map(Ok), MAX_CONCURRENCY_PER_TASK)
                    .await?;

                futures::future::pending().await
            }
        };

        select! {
            exit_early = init => exit_early,
            task_handler = task_handler => task_handler,
            result_processor = result_processor => result_processor,
            final_result = final_result_rx => {
                tx.close().await?;
                sender.close().await?;
                Ok(final_result?)
            },
        }
    }
}
