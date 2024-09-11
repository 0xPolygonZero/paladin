use std::fmt::Debug;

use anyhow::Result;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use super::IndexedStream;
use crate::{
    directive::Functor, operation::Operation, queue::PublisherExt, runtime::Runtime, task::Task,
};

/// Metadata for the [`IndexedStream`] functor.
///
/// In our functor implementation, we're simply mapping over the input stream,
/// and given that tasks will be completed in an arbitrary order, we associate
/// each task with its original input, which will be passed through in the
/// output stream, preserving the original ordering.
#[derive(Serialize, Deserialize, Debug)]
struct Metadata {
    idx: usize,
}

const MAX_CONCURRENCY: usize = 10;

#[async_trait]
impl<'a, A: Send + Sync + 'a, B: Send + 'a> Functor<'a, B> for IndexedStream<'a, A> {
    async fn f_map<Op: Operation>(self, op: &'a Op, runtime: &Runtime) -> Result<Self::Target>
    where
        Op: Operation<Input = A, Output = B>,
    {
        let (channel_identifier, sender, receiver) =
            runtime.lease_coordinated_task_channel().await?;

        // Place the sending task into a stream so that it can be combined with the
        // output stream with `select`. This will allow us to forward any errors that
        // occur while sending tasks to the output stream; any errors that occur
        // while sending tasks means that the entire operation should fail, as
        // the output stream will be incomplete.
        let sender_stream = futures::stream::once(async move {
            let task_stream = self.map_ok(|(idx, input)| Task {
                routing_key: channel_identifier.clone(),
                metadata: Metadata { idx },
                op,
                input,
            });

            sender.publish_all(task_stream, MAX_CONCURRENCY).await?;
            sender.close().await?;
            Ok::<(), anyhow::Error>(())
        })
        .filter_map(|result| async move {
            // Ignore successful results, while forwarding errors.
            result.err().map(Err)
        });

        let result_stream = receiver.then(move |(result, acker)| {
            Box::pin(async move {
                acker.ack().await?;
                result.map(|r| (r.metadata.idx, r.output))
            })
        });

        Ok(IndexedStream::new(futures::stream::select(
            sender_stream,
            result_stream,
        )))
    }
}
