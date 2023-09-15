use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use ops::Ops;
use paladin::{config::Config, runtime::worker::worker_loop};

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

    worker_loop::<Ops>(&args.options).await?;

    Ok(())
}
