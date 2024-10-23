use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use ops::{register, CharToString, StringConcat};
use paladin::runtime::CommandIpc;
use paladin::{
    config::Config,
    directive::{indexed_stream::IndexedStream, Directive},
    runtime::Runtime,
};
use tracing::{error, info, warn};

mod init;

#[derive(Parser, Debug)]
pub struct Cli {
    #[command(flatten)]
    pub options: Config,
    /// Optional timeout for job in the seconds
    #[arg(long, short, env = "JOB_TIMEOUT")]
    pub timeout: Option<u64>,
}

const INPUT: &str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

async fn set_abort_timeout_job(timeout: u64, runtime: Arc<Runtime>) {
    tokio::spawn(async move {
        let command_channel = runtime
            .get_command_ipc_sender()
            .await
            .expect("retrieved ipc sender");
        tokio::time::sleep(Duration::from_secs(timeout)).await;
        warn!("User timeout expired, aborting the execution...");
        if let Err(e) = command_channel
            .publish(&CommandIpc::Abort {
                routing_key: paladin::runtime::COMMAND_IPC_ABORT_ALL_KEY.into(),
            })
            .await
        {
            error!("Unable to send abort signal: {e:?}");
        } else {
            info!("Abort signal successfully sent");
        }
    });
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    init::tracing();

    let args = Cli::parse();
    let runtime = std::sync::Arc::new(Runtime::from_config(&args.options, register()).await?);

    let input: Vec<char> = INPUT.chars().collect();
    let computation = IndexedStream::from(input)
        .map(&CharToString)
        .fold(&StringConcat);

    if let Some(timeout) = args.timeout {
        set_abort_timeout_job(timeout, runtime.clone()).await;
    }

    let result = computation.run(&runtime).await;
    runtime
        .close()
        .await
        .inspect_err(|e| tracing::error!("Failed to close runtime: {e:?}"))?;

    result
        .map(|value| {
            info!("result: {:?}", value);
            assert_eq!(value, INPUT.to_string());
        })
        .inspect_err(|e| tracing::error!("Leader finished with error:{e:?}"))
}
