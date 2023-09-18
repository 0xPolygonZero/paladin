//! Orchestration directives that are used to build the execution tree.
//!
//! In essence, a [`Directive`] encapsulates higher-order evaluation semantics,
//! dictating how various operations are orchestrated and combined in the larger
//! execution flow.
//!
//! The [`Evaluator`] trait defines the evaluation mechanism for directives in
//! the execution tree.

use anyhow::Result;
use async_trait::async_trait;
use futures::{Stream, StreamExt};

use crate::operation::{Monoid, Operation};

pub mod evaluator;

/// A [`Directive`] serves as a node within an execution tree, encapsulating
/// higher-order evaluation semantics.
///
/// The key characteristics and intentions behind the [`Directive`] are as
/// follows:
///
/// - **Evaluation Context**: Currently, a [`Directive`] is designed to be
///   evaluated on a single orchestration node.
/// This implies that the computational cost associated with a directive's logic
/// should be minimal. The main role of a directive is to orchestrate the remote
/// execution of more resource-intensive [`Operation`]s and aggregate their
/// results.
///
/// - **Generality and Composition**: Directives are intended to be highly
///   composable.
/// They should be designed with a general signature to enable chaining and
/// flexible composition, thereby forming diverse execution trees. Higher-order
/// directives must be capable of taking other directives as input, provided
/// there's a type match between output and input. An exception to this pattern
/// is the [`Literal`] directive, which introduces values from memory into the
/// execution context, typically acting as leaves in the execution tree.
///
/// - **Design Rationale**: Using a trait for representing directives (as
///   opposed to an enum) was a conscious design choice.
/// This approach leverages Rust's type system, ensuring type safety during
/// polymorphic composition and benefiting from static dispatch.
pub trait Directive: Send + Sync {
    type Input;
    type Output;

    /// Fold this [`Directive`]'s output over the given [`Monoid::combine`]
    /// until a single value is produced.
    ///
    /// Note that the output type of the this directive must be an indexed
    /// stream (`Stream<Item = (usize, M::Elem)>`). This allows
    /// parallelization of the operation while maintaining order.
    ///
    /// # Example
    /// ```
    /// # use paladin::{
    /// #    operation::{Operation, Monoid},
    /// #    directive::{Directive, Evaluator, indexed},
    /// #    opkind_derive::OpKind,
    /// #    runtime::Runtime,
    /// #    config::{self, Config}
    /// # };
    /// # use serde::{Deserialize, Serialize};
    /// # use anyhow::Result;
    /// #
    /// # #[derive(Clone, Copy, Debug, Deserialize, Serialize)]
    /// struct Multiply;
    /// impl Monoid for Multiply {
    ///     type Elem = i32;
    ///     type Kind = MyOps;
    ///
    ///     fn combine(&self, a: i32, b: i32) -> Result<i32> {
    ///         Ok(a * b)
    ///     }
    ///
    ///     fn empty(&self) -> i32 {
    ///         1
    ///     }
    /// }
    /// #
    /// # #[derive(OpKind, Copy, Clone, Debug, Deserialize, Serialize)]
    /// # enum MyOps {
    /// #    Multiply(Multiply),
    /// # }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let runtime = Runtime::from_config(&Config { runtime: config::Runtime::InMemory, ..Default::default() }).await?;
    /// let computation = indexed([1, 2, 3, 4, 5]).fold(Multiply);
    /// let result = runtime.evaluate(computation).await?;
    /// assert_eq!(result, 120);
    /// # Ok(())
    /// # }
    /// ```
    fn fold<M, S>(self, op: M) -> Fold<M, Self::Output, Self>
    where
        M: Monoid,
        S: Stream<Item = (usize, M::Elem)>,

        Self: Sized + Directive<Output = S>,
    {
        Fold { op, input: self }
    }

    /// Map this [`Directive`]'s output over the given [`Operation`].
    ///
    /// Note that the output type of this directive and input type of the given
    /// [`Operation`] must be an indexed stream (`Stream<Item = (usize,
    /// Op::Input)>`). This allows parallelization of the operation while
    /// maintaining order.
    ///
    /// # Example
    ///
    /// Computing the length of a stream of strings:
    /// ```
    /// # use paladin::{
    /// #    operation::Operation,
    /// #    directive::{Directive, Evaluator, indexed, unindexed},
    /// #    opkind_derive::OpKind,
    /// #    runtime::Runtime,
    /// #    config::{self, Config}
    /// # };
    /// # use serde::{Deserialize, Serialize};
    /// # use anyhow::Result;
    /// # use futures::StreamExt;
    /// #
    /// # #[derive(Clone, Copy, Debug, Deserialize, Serialize)]
    /// struct Length;
    /// impl Operation for Length {
    ///     type Input = String;
    ///     type Output = usize;
    ///     type Kind = MyOps;
    ///
    ///     fn execute(&self, input: String) -> Result<usize> {
    ///         Ok(input.len())
    ///     }
    /// }
    /// #
    /// # #[derive(OpKind, Copy, Clone, Debug, Deserialize, Serialize)]
    /// # enum MyOps {
    /// #    Length(Length),
    /// # }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let runtime = Runtime::from_config(&Config { runtime: config::Runtime::InMemory, ..Default::default() }).await?;
    /// let input = ["hel", "lo", " world", "!"].iter().map(|s| s.to_string());
    /// let computation = indexed(input).map(Length);
    /// let result = runtime.evaluate(computation).await?;
    /// // The output is an indexed stream, convert it into a sorted vec
    /// let vec_result = unindexed(result).await
    ///     .into_iter()
    ///     .collect::<Vec<_>>();
    ///
    /// assert_eq!(vec_result, vec![3, 2, 6, 1]);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Multiplying a stream of integers by 2:
    ///
    /// ```
    /// # use paladin::{
    /// #    operation::Operation,
    /// #    directive::{Directive, Evaluator, indexed, unindexed},
    /// #    opkind_derive::OpKind,
    /// #    runtime::Runtime,
    /// #    config::{self, Config}
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
    /// # let runtime = Runtime::from_config(&Config { runtime: config::Runtime::InMemory, ..Default::default() }).await?;
    /// let computation = indexed([1, 2, 3, 4, 5]).map(MultiplyBy(2));
    /// let result = runtime.evaluate(computation).await?;
    /// // The output is an indexed stream, convert it into a sorted vec
    /// let vec_result = unindexed(result).await
    ///     .into_iter()
    ///     .collect::<Vec<_>>();
    ///
    /// assert_eq!(vec_result, vec![2, 4, 6, 8, 10]);
    /// # Ok(())
    /// # }
    /// ```
    fn map<Op: Operation, S>(self, op: Op) -> Map<Op, S, Self>
    where
        S: Stream<Item = (usize, Op::Input)>,
        Self: Sized + Directive<Output = S>,
    {
        Map { op, input: self }
    }

    /// Apply the given [`Operation`] to this [`Directive`]'s output.
    ///
    /// # Example
    ///
    /// ```
    /// # use paladin::{
    /// #    operation::Operation,
    /// #    directive::{Directive, Evaluator, lit},
    /// #    opkind_derive::OpKind,
    /// #    runtime::Runtime,
    /// #    config::{self, Config}
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
    /// # let runtime = Runtime::from_config(&Config { runtime: config::Runtime::InMemory, ..Default::default() }).await?;
    /// let computation = lit(21).apply(MultiplyBy(2));
    /// let result = runtime.evaluate(computation).await?;
    ///
    /// assert_eq!(result, 42);
    /// # Ok(())
    /// # }
    /// ```
    fn apply<Op: Operation>(self, op: Op) -> Apply<Op, Self>
    where
        Self: Sized + Directive<Output = Op::Input>,
    {
        Apply { op, input: self }
    }
}

/// The [`Evaluator`] trait defines the evaluation mechanism for directives in
/// the execution tree.
///
/// This trait lays the foundation for actual computation or processing to take
/// place for a given [`Directive`]. As [`Directive`]s act as nodes within an
/// execution tree, representing various computational semantics, it is the
/// evaluator's role implement those semantics.
///
/// # Example
///
/// Implementers of this trait will provide the actual logic for evaluating a
/// directive, typically orchestrating calls to remote services or conducting
/// some form of aggregation or transformation based on the directive's
/// semantics.
///
/// ```rust
/// # use paladin::directive::{Directive, Evaluator};
/// # use anyhow::Result;
/// # use async_trait::async_trait;
/// # struct SomeDirective;
/// # impl Directive for SomeDirective {
/// #    type Input = ();
/// #    type Output = SomeOutput;
/// # }
/// # struct SomeOutput;
/// # struct SomeEvaluator;
/// #[async_trait]
/// impl<'a> Evaluator<'a, SomeDirective> for SomeEvaluator {
///     async fn evaluate(&'a self, directive: SomeDirective) -> Result<SomeOutput> {
///         // ... Actual evaluation logic
///        # Ok(SomeOutput)
///     }
/// }
/// ```
#[async_trait]
pub trait Evaluator<'a, D: Directive>: Sync {
    async fn evaluate(&'a self, directive: D) -> Result<D::Output>;
}

/// Directive for [`lit`].
///
/// `Literal` is a directive that lifts an arbitrary value into the
/// execution context, typically acting as leaves in the execution tree.
#[derive(Debug)]
pub struct Literal<Output: Send + Sync>(pub Output);

/// Construct a [`Literal`] from an arbitrary value.
///
/// A [`Literal`] is a directive that lifts an arbitrary value into the
/// execution context, typically acting as leaves in the execution tree.
pub fn lit<Output: Send + Sync>(output: Output) -> Literal<Output> {
    Literal(output)
}

impl<Output: Send + Sync> Directive for Literal<Output> {
    type Input = Output;
    type Output = Output;
}

/// [`Literal`] [`Evaluator`] for all evaluators.
///
/// [`Literal`]s shouldn't need to vary by [`Evaluator`], so a blanket
/// [`Literal`] evaluator implementation is provided. It simply returns the
/// inner value.
#[async_trait]
impl<'a, Output: Send + Sync + 'a, T: Send + Sync> Evaluator<'a, Literal<Output>> for T {
    async fn evaluate(&'a self, op: Literal<Output>) -> Result<Output> {
        Ok(op.0)
    }
}

/// Directive for [`apply`](Directive::apply).
#[derive(Debug)]
pub struct Apply<Op: Operation, Input: Directive<Output = Op::Input>> {
    op: Op,
    input: Input,
}

impl<Op: Operation, Input: Directive<Output = Op::Input>> Directive for Apply<Op, Input> {
    type Input = Input;
    type Output = Op::Output;
}

/// Utility for creating an indexed stream (`Stream<Item = (usize,
/// Op::Input)>`) from any [`IntoIterator`].
///
/// This is useful for lifting iterable values into the execution context such
/// that they can be used by directives that expect an indexed stream as input,
/// like [`Map`] or [`Fold`].
///
/// # Example
/// ```
/// # use paladin::{
/// #    operation::{Operation, Monoid},
/// #    directive::{Directive, Evaluator, indexed},
/// #    opkind_derive::OpKind,
/// #    runtime::Runtime,
/// #    config::{self, Config}
/// # };
/// # use serde::{Deserialize, Serialize};
/// # use anyhow::Result;
/// #
/// # #[derive(Clone, Copy, Debug, Deserialize, Serialize)]
/// struct Multiply;
/// impl Monoid for Multiply {
///     type Elem = i32;
///     type Kind = MyOps;
///
///     fn combine(&self, a: i32, b: i32) -> Result<i32> {
///         Ok(a * b)
///     }
///
///     fn empty(&self) -> i32 {
///         1
///     }
/// }
/// #
/// # #[derive(OpKind, Copy, Clone, Debug, Deserialize, Serialize)]
/// # enum MyOps {
/// #    Multiply(Multiply),
/// # }
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # let runtime = Runtime::from_config(&Config { runtime: config::Runtime::InMemory, ..Default::default() }).await?;
/// let computation = indexed([1, 2, 3, 4, 5]).fold(Multiply);
/// let result = runtime.evaluate(computation).await?;
/// assert_eq!(result, 120);
/// # Ok(())
/// # }
/// ```
pub fn indexed<Item, IntoIter: IntoIterator<Item = Item>>(
    iter: IntoIter,
) -> Literal<
    futures::stream::Iter<std::iter::Enumerate<<IntoIter as std::iter::IntoIterator>::IntoIter>>,
>
where
    IntoIter::IntoIter: Send + Sync,
{
    lit(futures::stream::iter(iter.into_iter().enumerate()))
}

/// Utility for converting an indexed stream (`Stream<Item = (usize, Item)>`)
/// into a sorted [`IntoIterator`].
pub async fn unindexed<Item, S: Stream<Item = (usize, Item)>>(
    stream: S,
) -> impl IntoIterator<Item = Item> {
    let mut vec = stream.collect::<Vec<_>>().await;
    vec.sort_by(|a, b| a.0.cmp(&b.0));
    vec.into_iter().map(|(_, v)| v)
}

/// Directive for [`map`](Directive::map).
///
/// [`Map`] applies a given [`Operation`] to each element of the input.
///
/// Generally speaking, [`Map`] implementations should be order preserving in
/// the sense that the output should maintain the order of the input.
/// For maximum throughput, [`Map`] implementations should be parallelized, and
/// expect an indexed stream as input (`Stream<Item = (usize, Op::Input)>`).
/// By coupling the index with the input, the [`Map`] implementation can
/// parallelize the operation and still maintain order.
#[derive(Debug)]
pub struct Map<
    Op: Operation,
    InputStream: Stream<Item = (usize, Op::Input)>,
    Input: Directive<Output = InputStream>,
> {
    op: Op,
    input: Input,
}

impl<
        Op: Operation,
        InputStream: Stream<Item = (usize, Op::Input)>,
        Input: Directive<Output = InputStream>,
    > Directive for Map<Op, InputStream, Input>
{
    type Input = Input;
    type Output = Box<dyn Stream<Item = (usize, Op::Output)> + Send + Unpin>;
}

/// Directive for [`fold`](Directive::fold).
///
/// A [`Fold`] combines elements of the given input stream with the provided
/// [`Monoid::combine`] until a single value is produced.
///
/// Similar to [`Map`], [`Fold`] implementations should deal with indexed
/// streams (`Stream<Item = (usize, Op::Input)>`) such that combine operations
/// can be parallelized.
#[derive(Debug)]
pub struct Fold<
    Op: Monoid,
    InputStream: Stream<Item = (usize, Op::Elem)>,
    Input: Directive<Output = InputStream>,
> {
    op: Op,
    input: Input,
}

impl<
        Op: Monoid,
        InputStream: Stream<Item = (usize, Op::Elem)>,
        Input: Directive<Output = InputStream>,
    > Directive for Fold<Op, InputStream, Input>
{
    type Input = Input;
    type Output = Op::Elem;
}
