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

/// A [`Literal`] is a directive that lifts an arbitrary value into the
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

/// [`Literal`]s shouldn't need to vary by [`Evaluator`], so a blanket
/// [`Literal`] evaluator implementation is provided.
#[async_trait]
impl<'a, Output: Send + Sync + 'a, T: Send + Sync> Evaluator<'a, Literal<Output>> for T {
    async fn evaluate(&'a self, op: Literal<Output>) -> Result<Output> {
        Ok(op.0)
    }
}

/// Function application.
///
/// [`Apply`] is a directive that applies the given [`Operation`] to the given
/// input.
#[derive(Debug)]
pub struct Apply<Op: Operation, Input: Directive<Output = Op::Input>> {
    op: Op,
    input: Input,
}

impl<Op: Operation, Input: Directive<Output = Op::Input>> Directive for Apply<Op, Input> {
    type Input = Input;
    type Output = Op::Output;
}

/// Function application.
///
/// [`Apply`] is a directive that applies the given [`Operation`] to the given
/// input.
pub fn apply<Op: Operation, Input: Directive<Output = Op::Input>>(
    op: Op,
    input: Input,
) -> Apply<Op, Input> {
    Apply { op, input }
}

/// Utility for creating an indexed stream from any [`IntoIterator`].
pub struct IndexedStream<Item, IntoIter: IntoIterator<Item = Item>> {
    iter: futures::stream::Iter<
        std::iter::Enumerate<<IntoIter as std::iter::IntoIterator>::IntoIter>,
    >,
}

impl<Item, IntoIter: IntoIterator<Item = Item>> IndexedStream<Item, IntoIter> {
    pub fn new(iter: IntoIter) -> Self {
        Self {
            iter: futures::stream::iter(iter.into_iter().enumerate()),
        }
    }
}

/// Utility for creating an [`IndexedStream`] from any [`IntoIterator`].
pub fn indexed<Item, IntoIter: IntoIterator<Item = Item>>(
    iter: IntoIter,
) -> IndexedStream<Item, IntoIter> {
    IndexedStream::new(iter)
}

/// Stream implementation for [`IndexedStream`].
impl<Item, IntoIter: IntoIterator<Item = Item>> Stream for IndexedStream<Item, IntoIter> {
    type Item = (usize, Item);

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.iter.poll_next_unpin(cx)
    }
}

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

/// Map the given [`Operation`] over the given input.
pub fn map<
    Op: Operation,
    InputStream: Stream<Item = (usize, Op::Input)>,
    Input: Directive<Output = InputStream>,
>(
    op: Op,
    input: Input,
) -> Map<Op, InputStream, Input> {
    Map { op, input }
}

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

/// Fold the given input stream using the provided [`Monoid::combine`] until a
/// single value is produced.
pub fn fold<
    Op: Monoid,
    InputStream: Stream<Item = (usize, Op::Elem)>,
    Input: Directive<Output = InputStream>,
>(
    op: Op,
    input: Input,
) -> Fold<Op, InputStream, Input> {
    Fold { op, input }
}
