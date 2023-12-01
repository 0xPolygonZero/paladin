use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use ops::register;
use paladin::{config::Config, runtime::WorkerRuntime};

mod init;

#[derive(Parser, Debug)]
pub struct Cli {
    #[command(flatten)]
    pub options: Config,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    init::tracing();

    let args = Cli::parse();

    let runtime = WorkerRuntime::from_config(&args.options, register()).await?;
    runtime.main_loop().await?;

    Ok(())
}
