//! [`Map`] implementation for [`Runtime`].

use crate::{
    directive::{Directive, Evaluator, Map},
    operation::{OpKind, Operation},
    runtime::Runtime,
    task::{AnyTask, RemoteExecute, Task},
};
use anyhow::Result;
use async_trait::async_trait;
use futures::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::{error, instrument};

#[derive(Serialize, Deserialize, Debug)]
struct Metadata {
    idx: usize,
}

#[async_trait]
impl<
        'a,
        Kind: OpKind,
        Op: Operation<Kind = Kind>,
        InputStream: Stream<Item = (usize, Op::Input)> + 'a + Send + Unpin,
        Input: Directive<Output = InputStream> + 'a,
    > Evaluator<'a, Map<Op, InputStream, Input>> for Runtime<Kind>
where
    Runtime<Kind>: Evaluator<'a, Input>,
    AnyTask<Kind>: RemoteExecute<Kind>,
{
    #[instrument(skip_all, fields(directive = "Map", op = ?map.op), level = "debug")]
    async fn evaluate(
        &'a self,
        map: Map<Op, InputStream, Input>,
    ) -> Result<Box<dyn Stream<Item = (usize, Op::Output)> + Send + Unpin>> {
        let input = self.evaluate(map.input).await?;

        let (channel_identifier, mut sender, receiver) = self
            .lease_coordinated_task_channel::<Op, Metadata>()
            .await?;

        let mut task_stream = input
            .map(|(idx, input)| Task {
                routing_key: channel_identifier.clone(),
                metadata: Metadata { idx },
                op: map.op.clone(),
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

        Ok(Box::new(results))
    }
}
