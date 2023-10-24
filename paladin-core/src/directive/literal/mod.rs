/// A literal value.
///
/// Can be used to execute operations over arbitrary values.
///
/// ## Example
/// ```
/// # use paladin::{
/// #    operation::Operation,
/// #    directive::{Directive, Literal},
/// #    opkind_derive::OpKind,
/// #    runtime::Runtime,
/// # };
/// # use serde::{Deserialize, Serialize};
/// # use anyhow::Result;
/// #
/// # #[derive(Clone, Copy, Debug, Deserialize, Serialize)]
/// struct MultiplyBy(i32);
/// impl Operation for MultiplyBy {
///     type Input = i32;
///     type Output = i32;
///     type Kind = MyOps;
///
///     fn execute(&self, input: i32) -> Result<i32> {
///         Ok(self.0 * input)
///     }
/// }
/// #
/// # #[derive(OpKind, Copy, Clone, Debug, Deserialize, Serialize)]
/// # enum MyOps {
/// #    MultiplyBy(MultiplyBy),
/// # }
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # let runtime = Runtime::in_memory().await?;
/// let computation = Literal(5).map(MultiplyBy(2));
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
