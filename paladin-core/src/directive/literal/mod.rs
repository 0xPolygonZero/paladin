/// A literal value.
///
/// Can be used to execute operations over arbitrary values.
///
/// ## Example
/// ```
/// # use paladin::{
/// #    RemoteExecute,
/// #    operation::{Operation, Result},
/// #    directive::{Directive, Literal},
/// #    runtime::Runtime,
/// # };
/// # use serde::{Deserialize, Serialize};
/// #
/// # #[derive(Deserialize, Serialize, RemoteExecute)]
/// struct MultiplyBy(i32);
/// impl Operation for MultiplyBy {
///     type Input = i32;
///     type Output = i32;
///
///     fn execute(&self, input: i32) -> Result<i32> {
///         Ok(self.0 * input)
///     }
/// }
///
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// # let runtime = Runtime::in_memory().await?;
/// let computation = Literal(5).map(&MultiplyBy(2));
/// let result = computation.run(&runtime).await?;
/// assert_eq!(result, Literal(10));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Literal<T>(pub T);

impl_hkt!(Literal);
impl_lit!(Literal<T>);

mod functor;
