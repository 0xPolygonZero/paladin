use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use ops::{CharToString, GenericMultiplication, MultiplyBy, StringConcat};
use paladin::{
    config::Config,
    directive::{apply, fold, indexed, lit, map, Evaluator},
    get_runtime,
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
    let runtime = get_runtime(&args.options).await?;

    let concat_input = indexed(['h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', '!']);
    let string_concat = fold(StringConcat, map(CharToString, lit(concat_input)));

    let multiplication = fold(
        GenericMultiplication::<i32>::default(),
        lit(indexed([1, 2, 3, 4, 5, 6])),
    );

    let result: String = runtime.evaluate(string_concat).await?;
    info!("{result:?}");

    let result: i32 = runtime.evaluate(multiplication).await?;
    info!("{result:?}");

    let mult: i32 = runtime.evaluate(apply(MultiplyBy(3), lit(2))).await?;
    info!("{mult:?}");

    std::future::pending::<()>().await;
    Ok(())
}
