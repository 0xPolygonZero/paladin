use anyhow::Result;
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};

use super::Literal;
use crate::{directive::Functor, operation::Operation, runtime::Runtime, task::Task};

#[async_trait]
impl<A: Send, B: Send + 'static> Functor<B> for Literal<A> {
    async fn f_map<Op: Operation<Input = A, Output = B>>(
        self,
        op: Op,
        runtime: &Runtime,
    ) -> Result<Self::Target> {
        let (channel_identifier, mut sender, mut receiver) =
            runtime.lease_coordinated_task_channel::<Op, ()>().await?;

        let task = Task {
            routing_key: channel_identifier.clone(),
            metadata: (),
            op: op.clone(),
            input: self.0,
        };

        sender.send(task).await?;
        sender.close().await?;

        if let Some((result, acker)) = receiver.next().await {
            acker.ack().await?;
            return Ok(Literal(result?.output));
        }

        anyhow::bail!("No results received")
    }
}
