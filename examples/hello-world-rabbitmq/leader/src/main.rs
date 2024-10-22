use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use ops::{register, CharToString, StringConcat};
use paladin::{
    config::Config,
    directive::{indexed_stream::IndexedStream, Directive},
    runtime::Runtime,
};
use tracing::info;

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
    let runtime = Runtime::from_config(&args.options, register()).await?;

    let input = ['h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', '!', 's', 'm', 'a', 'l', 'l', '!',
        'S', 'm', 'o', 'r', 'i', 'm', 'e', 'v', 'i', 'v', 'e', 'r', 'o', '!'];
    let computation = IndexedStream::from(input)
        .map(&CharToString)
        .fold(&StringConcat);

    let result = computation.run(&runtime).await;
    runtime.close().await?;

    let result = result?;

    info!("result: {:?}", result);
    assert_eq!(result, "hello world!small!Smorimevivero!".to_string());

    Ok(())
}
