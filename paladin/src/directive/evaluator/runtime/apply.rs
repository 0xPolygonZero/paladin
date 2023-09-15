//! [`Apply`] implementation for [`Runtime`].

use crate::{
    directive::{Apply, Directive, Evaluator},
    operation::{OpKind, Operation},
    runtime::Runtime,
    task::Task,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use tracing::{error, instrument};

#[async_trait]
impl<'a, Kind: OpKind, Op: Operation<Kind = Kind>, Input: Directive<Output = Op::Input> + 'a>
    Evaluator<'a, Apply<Op, Input>> for Runtime<Kind>
where
    Runtime<Kind>: Evaluator<'a, Input>,
{
    #[instrument(skip_all, fields(directive = "Apply", op = ?apply.op), level = "debug")]
    async fn evaluate(&'a self, apply: Apply<Op, Input>) -> Result<Op::Output> {
        let input = self.evaluate(apply.input).await?;

        let (channel_identifier, mut sender, mut receiver) =
            self.lease_coordinated_task_channel::<Op, ()>().await?;

        let task = Task {
            routing_key: channel_identifier.clone(),
            metadata: (),
            op: apply.op.clone(),
            input,
        };

        sender.send(task).await?;
        sender.close().await?;

        while let Some((result, acker)) = receiver.next().await {
            match acker.ack().await {
                Ok(()) => return Ok(result.output),
                // We don't error out on a failed ack, but filter out the message, as we assume it
                // will be redelivered and we'll have another opportunity to ack.
                // This is a low probability event, perhaps due to some kind of
                // network failure, but we want to prevent duplicated results being propagated
                // through the stream, which could occur if a message was pushed through without
                // acking.
                Err(e) => error!("Failed to ack result: {}", e),
            }
        }

        Err(anyhow!("No result received"))
    }
}
