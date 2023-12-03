use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;

use super::Literal;
use crate::{directive::Functor, operation::Operation, runtime::Runtime, task::Task};

#[async_trait]
impl<'a, A: Send, B: Send> Functor<'a, B> for Literal<A> {
    async fn f_map<Op: Operation<Input = A, Output = B>>(
        self,
        op: &'a Op,
        runtime: &Runtime,
    ) -> Result<Self::Target> {
        let (channel_identifier, sender, mut receiver) =
            runtime.lease_coordinated_task_channel().await?;

        let task = Task {
            routing_key: channel_identifier,
            metadata: (),
            op,
            input: self.0,
        };

        sender.publish(&task).await?;
        sender.close().await?;

        if let Some((result, acker)) = receiver.next().await {
            acker.ack().await?;
            return Ok(Literal(result?.output));
        }

        anyhow::bail!("No results received")
    }
}
