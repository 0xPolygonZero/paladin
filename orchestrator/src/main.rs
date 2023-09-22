use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use ops::{CharToString, StringConcat};
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
    let runtime = Runtime::from_config(&args.options).await?;

    let input = ['h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', '!'];
    let computation = IndexedStream::from(input)
        .map(CharToString)
        .fold(StringConcat);

    let result = computation.run(&runtime).await?;
    info!("result: {:?}", result);
    assert_eq!(result, "hello world!".to_string());

    std::future::pending::<()>().await;
    Ok(())
}
