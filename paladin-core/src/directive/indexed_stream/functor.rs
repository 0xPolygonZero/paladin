use anyhow::Result;
use async_trait::async_trait;
use futures::{FutureExt, SinkExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use super::IndexedStream;
use crate::{directive::Functor, operation::Operation, runtime::Runtime, task::Task};

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

#[async_trait]
impl<A: Send + 'static, B: Send + 'static> Functor<B> for IndexedStream<A> {
    async fn f_map<Op: Operation<Input = A, Output = B>>(
        mut self,
        op: Op,
        runtime: &Runtime,
    ) -> Result<Self::Target> {
        let (channel_identifier, mut sender, receiver) = runtime
            .lease_coordinated_task_channel::<Op, Metadata>()
            .await?;

        // Dispatch tasks in a thread to avoid blocking until all input is received.
        // This way we can process results as they come in.
        let sender_fut = tokio::spawn(async move {
            let mut task_stream = self.map_ok(|(idx, input)| Task {
                routing_key: channel_identifier.clone(),
                metadata: Metadata { idx },
                op: op.clone(),
                input,
            });

            sender.send_all(&mut task_stream).await?;
            sender.close().await?;
            Ok::<(), anyhow::Error>(())
        });

        // Convert the sender thread into a stream so that we can combine it with the
        // output stream. This will allow us to forward any errors that occur
        // while sending tasks to the output stream; any errors that occur while
        // sending tasks means that the entire operation should fail, as the
        // output stream will be incomplete.
        let sender_stream = sender_fut.into_stream().filter_map(|result| {
            Box::pin(async move {
                match result {
                    // We're only interested in errors, as the sender thread will
                    // only return an error if it fails to send a task.
                    Ok(Ok(())) => None,
                    // If the sender thread returns an error, we want to forward
                    // it to the output stream.
                    Ok(Err(e)) => Some(Err(e)),
                    // If the sender thread panics, we want to forward the panic
                    // to the output stream.
                    Err(e) => Some(Err(anyhow::Error::from(e))),
                }
            })
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
