use anyhow::Result;
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::error;

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
impl<A: Send, B: Send + 'static> Functor<B> for IndexedStream<A> {
    async fn f_map<Op: Operation<Input = A, Output = B>>(
        self,
        op: Op,
        runtime: &Runtime,
    ) -> Result<Self::Target> {
        let (channel_identifier, mut sender, receiver) = runtime
            .lease_coordinated_task_channel::<Op, Metadata>()
            .await?;

        let mut task_stream = self
            .map(|(idx, input)| Task {
                routing_key: channel_identifier.clone(),
                metadata: Metadata { idx },
                op: op.clone(),
                input,
            })
            .map(Ok);

        sender.send_all(&mut task_stream).await?;
        sender.close().await?;

        let results = receiver.filter_map(|(result, acker)| {
            Box::pin(async move {
                match acker.ack().await {
                    Ok(()) => Some((result.metadata.idx, result.output)),
                    // We don't error out on a failed ack, but filter out the message, as we assume
                    // the message will be redelivered and we'll have another
                    // opportunity to ack. This is a low probability event, perhaps
                    // due to some kind of network failure, but we want to prevent duplicated
                    // results being propagated through the stream, which could
                    // occur if a message was pushed through without acking.
                    Err(e) => {
                        error!("Failed to ack result: {}", e);
                        None
                    }
                }
            })
        });

        Ok(IndexedStream::new(results))
    }
}
