use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use ops::{CharToString, GenericMultiplication, MultiplyBy, StringConcat};
use paladin::{
    config::Config,
    directive::{indexed, lit, Directive, Evaluator},
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

    let concat_input = indexed(['h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', '!'])
        .map(CharToString)
        .fold(StringConcat);
    let result = runtime.evaluate(concat_input).await?;
    assert_eq!(result, "hello world!".to_string());
    info!("result: {}", result);

    let multiplication = indexed([1, 2, 3, 4, 5, 6]).fold(GenericMultiplication::<i32>::default());
    let result = runtime.evaluate(multiplication).await?;
    assert_eq!(result, 720);
    info!("result: {}", result);

    let multiply_by = lit(2).apply(MultiplyBy(3));
    let result = runtime.evaluate(multiply_by).await?;
    assert_eq!(result, 6);
    info!("result: {}", result);

    std::future::pending::<()>().await;
    Ok(())
}
