//! [`Fold`] implementation for [`Runtime`].

use std::{ops::RangeInclusive, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use futures::{Sink, SinkExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::{error, instrument};

use crate::{
    contiguous::{Contiguous, ContiguousQueue},
    directive::{Directive, Evaluator, Fold},
    operation::{Monoid, OpKind, Operation},
    runtime::Runtime,
    task::{AnyTask, RemoteExecute, Task, TaskResult},
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

impl<Op: Operation> TaskResult<Op, Metadata> {
    /// Check if the result is the final result of the fold operation.
    ///
    /// This is the case if the range of indices that the result represents is
    /// equal to the size of the job, as it means that the result is the
    /// combination of all the elements in the job.
    fn is_final(&self, job_size: usize) -> bool {
        self.metadata.range.end() - self.metadata.range.start() + 1 == job_size
    }
}

/// A [`Contiguous`] [`TaskResult`] for [`Fold`] [`Task`]s.
impl<Op: Operation> Contiguous for TaskResult<Op, Metadata> {
    type Key = usize;

    /// Check if the given [`TaskResult`] is contiguous with the given one.
    fn is_contiguous(&self, other: &Self) -> bool {
        self.metadata.range.end() + 1 == *other.metadata.range.start()
            || *other.metadata.range.end() + 1 == *self.metadata.range.start()
    }

    /// Get the key of the [`TaskResult`].
    ///
    /// We can be sure that the start of the range is safe, as Stream input with
    /// duplicated indices is not allowed and constitutes a logic error.
    fn key(&self) -> &Self::Key {
        self.metadata.range.start()
    }
}

type Sender<Op> = Box<dyn Sink<Task<Op, Metadata>, Error = anyhow::Error> + Send + Sync + Unpin>;

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
struct Dispatcher<Op: Monoid> {
    assembler: Arc<Mutex<ContiguousQueue<TaskResult<Op, Metadata>>>>,
    sender: Arc<Mutex<Sender<Op>>>,
    channel_identifier: String,
}

impl<Op: Monoid> Dispatcher<Op> {
    fn new(
        assembler: Arc<Mutex<ContiguousQueue<TaskResult<Op, Metadata>>>>,
        sender: Arc<Mutex<Sender<Op>>>,
        channel_identifier: String,
    ) -> Self {
        Self {
            assembler,
            sender,
            channel_identifier,
        }
    }

    /// Queue the given [`TaskResult`].
    async fn queue(&self, result: TaskResult<Op, Metadata>) {
        let mut assembler = self.assembler.lock().await;
        assembler.queue(result);
    }

    /// Dequeue the [`TaskResult`] at the given index.
    async fn dequeue(&self, idx: &usize) -> Option<TaskResult<Op, Metadata>> {
        let mut assembler = self.assembler.lock().await;
        assembler.dequeue(idx)
    }

    /// Attempt to dispatch the given [`TaskResult`].
    ///
    /// If the given [`TaskResult`] is contiguous with another [`TaskResult`],
    /// then the two are combined and dispatched for further combination.
    async fn try_dispatch(&self, result: TaskResult<Op, Metadata>) -> Result<()> {
        let mut assembler = self.assembler.lock().await;

        if let Some((lhs, rhs)) = assembler.acquire_contiguous_pair_or_queue(result) {
            let task = Task {
                routing_key: self.channel_identifier.clone(),
                metadata: Metadata {
                    range: *lhs.metadata.range.start()..=*rhs.metadata.range.end(),
                },
                op: lhs.op.clone(),
                input: (lhs.output, rhs.output),
            };
            let mut sender = self.sender.lock().await;
            sender.send(task).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl<
        'a,
        Kind: OpKind,
        Op: Monoid<Kind = Kind>,
        InputStream: Stream<Item = (usize, Op::Elem)> + 'a + Send + Unpin,
        Input: Directive<Output = InputStream> + 'a,
    > Evaluator<'a, Fold<Op, InputStream, Input>> for Runtime<Kind>
where
    Runtime<Kind>: Evaluator<'a, Input>,
    AnyTask<Kind>: RemoteExecute<Kind>,
{
    #[instrument(skip_all, fields(directive = "Fold", op = ?fold.op), level = "debug")]
    async fn evaluate(&'a self, fold: Fold<Op, InputStream, Input>) -> Result<Op::Elem> {
        let input = self.evaluate(fold.input).await?;

        let (channel_identifier, sender, mut receiver) = self
            .lease_coordinated_task_channel::<Op, Metadata>()
            .await?;
        // Both the initialization step and the result stream need to asynchronous
        // mutable access to the assembler. So we wrap it in an Arc<Mutex<>>.
        let assembler = Arc::new(Mutex::new(ContiguousQueue::new()));
        // Both the initialization step and the result stream need to asynchronous
        // mutable access to the sender. So we wrap it in an Arc<Mutex<>>.
        let sender = Arc::new(Mutex::new(sender));
        // Initialize the dispatcher.
        let dispatcher = Arc::new(Dispatcher::new(
            assembler.clone(),
            sender.clone(),
            channel_identifier.clone(),
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
        let init = input
            .map(Ok)
            .try_fold(0, |sum, (idx, item)| {
                let op = fold.op.clone();
                let dispatcher = dispatcher.clone();
                let should_dispatch = should_dispatch.clone();

                async move {
                    // Given that the operation is a monoid, and input type must match output type,
                    // we can safely place the input into a `TaskResult`.
                    // Because this represents an uncombined input, we set the
                    // range equal to its index.
                    let item_result = TaskResult {
                        op,
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
                        should_dispatch.notify_waiters();
                        // Dispatch the task.
                        // At this point, a failed dispatch is actually a fatal error, as we don't
                        // have any redelivery guarantees, given that we're
                        // operating on an in-memory stream. It may be worth
                        // implementing a redelivery mechanism in the future if we find we encounter
                        // a lot of issues here, but for now we don't
                        // attempt to solve that. So we propagate the error (`?`).
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

        let resolved_size_clone = resolved_input_size.clone();
        let dispatcher_clone = dispatcher.clone();
        let should_dispatch = should_dispatch.clone();
        let op = fold.op.clone();
        let result_handle = tokio::spawn(async move {
            // Wait until at least two inputs have been received.
            should_dispatch.notified().await;

            while let Some((result, acker)) = receiver.next().await {
                // Check to see if the input size is known.
                if let Some(job_size) = *resolved_size_clone.clone().read().await {
                    // If it is, we can check to see if the result is the final result (i.e., its
                    // range comprises the size of the input).
                    if result.is_final(job_size) {
                        return Ok(result.output);
                    }
                }

                match dispatcher_clone.try_dispatch(result).await {
                    Ok(()) => {
                        // We don't error out on a failed ack, as we assume the message will be
                        // redelivered and we'll have another chance to ack.
                        // This is a low probability event, perhaps
                        // due to some kind of network failure.
                        if let Err(e) = acker.ack().await {
                            error!("Failed to ack result: {}", e);
                        }
                    }
                    // We failed to dispatch, and similarly to above don't error out.
                    // Assume we'll have another chance to dispatch after redelivery.
                    Err(e) => error!("Failed to dispatch result: {}", e),
                }
            }

            Ok(op.empty())
        });

        let size = init.await?;
        match size {
            0 => {
                // Abort the result handle, as it will never receive a result.
                result_handle.abort();
                // If the input is empty, we return the empty element.
                return Ok(fold.op.empty());
            }
            1 => {
                // Abort the result handle, as it will never receive a result.
                result_handle.abort();
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

        result_handle.await?
    }
}
