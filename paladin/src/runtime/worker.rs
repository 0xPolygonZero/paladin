//! A default worker loop that can be used to process
//! [`Task`](crate::task::Task)s.
use crate::{
    acker::Acker,
    config::Config,
    get_runtime,
    operation::OpKind,
    task::{AnyTask, RemoteExecute},
};
use anyhow::Result;
use futures::StreamExt;
use tracing::{error, info_span, instrument, Instrument};

/// A default worker loop that can be used to process
/// [`Task`](crate::task::Task)s.
///
/// Worker implementations generally wont vary, as the their
/// primary responsibility is to process incoming tasks. We provide one
/// out of the box that will work for most use cases. Users are free to
/// implement their own if they need to.
///
/// # Example
/// ```no_run
/// use anyhow::Result;
/// use paladin::{
///     runtime::{Runtime, worker::worker_loop},
///     config::Config,
///     task::Task,
///     operation::Operation,
///     opkind_derive::OpKind,
/// };
/// use clap::Parser;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
/// enum MyOps {
///     // ... your operations
/// #   StringLength(StringLength),
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct Metadata {
///     id: usize,
/// }
/// # #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
/// # struct StringLength;
/// # impl Operation for StringLength {
/// #    type Input = String;
/// #    type Output = usize;
/// #    type Kind = MyOps;
/// #    
/// #    fn execute(&self, input: Self::Input) -> Result<Self::Output> {
/// #       Ok(input.len())
/// #    }
/// # }
///
/// #[derive(Parser, Debug)]
/// pub struct Cli {
///     #[command(flatten)]
///     pub options: Config,
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let args = Cli::parse();
///     worker_loop::<MyOps>(&args.options).await?;
///
///     Ok(())
/// }
/// ```
#[instrument]
pub async fn worker_loop<Kind>(config: &Config) -> Result<()>
where
    Kind: OpKind,
    AnyTask<Kind>: RemoteExecute<Kind>,
{
    let runtime = get_runtime::<Kind>(config).await?;

    let mut task_stream = runtime.get_task_receiver().await?;

    while let Some((payload, delivery)) = task_stream.next().await {
        let span =
            info_span!("remote_execute", routing_key = %payload.routing_key, op = ?payload.op_kind);

        let execution = {
            payload.remote_execute(&runtime).instrument(span).await?;

            delivery.ack().await?;

            Ok::<(), anyhow::Error>(())
        };

        if let Err(e) = execution {
            error!("Failed to process task {e}");
        }
    }

    Ok(())
}
