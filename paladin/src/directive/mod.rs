//! Orchestration directives that are used to build the execution tree.
//!
//! In essence, a [`Directive`] encapsulates higher-order evaluation semantics,
//! dictating how various operations are orchestrated and combined in the larger
//! execution flow.
use anyhow::Result;
use async_trait::async_trait;

use crate::{
    operation::{Monoid, Operation},
    runtime::Runtime,
};

/// A [`Directive`] serves as a node within an execution tree, encapsulating
/// higher-order evaluation semantics.
///
/// The key characteristics and intentions behind the [`Directive`] are as
/// follows:
///
/// - **Evaluation Context**: Currently, a [`Directive`] is designed to be
///   evaluated on a single orchestration node. This implies that the
///   computational cost associated with a directive's logic should be minimal.
///   The main role of a directive is to orchestrate the remote execution of
///   more resource-intensive [`Operation`]s and aggregate their results.
///
/// - **Generality and Composition**: Directives are intended to be highly
///   composable. They should be designed with a general signature to enable
///   chaining and flexible composition, thereby forming diverse execution
///   trees. Higher-order directives must be capable of taking other directives
///   as input, provided there's a type match between output and input.
///
/// - **Design Rationale**: Using a trait for representing directives (as
///   opposed to an enum) was a conscious design choice. This approach leverages
///   Rust's type system, ensuring type safety during polymorphic composition
///   and benefiting from static dispatch.
#[async_trait]
pub trait Directive: Send {
    type Input;
    type Output;

    /// Map this [`Directive`]'s output over the given [`Operation`].
    ///
    /// Note that the [`Directive`] _must_ evaluate to a [`Functor`] for it to
    /// be mappable.
    ///
    /// # Example
    ///
    /// Computing the length of a stream of strings:
    /// ```
    /// # use paladin::{
    /// #    operation::Operation,
    /// #    directive::{Directive, IndexedStream},
    /// #    opkind_derive::OpKind,
    /// #    runtime::Runtime,
    /// # };
    /// # use serde::{Deserialize, Serialize};
    /// # use anyhow::Result;
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
    /// # let runtime = Runtime::in_memory().await?;
    /// let input = ["hel", "lo", " world", "!"].iter().map(|s| s.to_string());
    /// let computation = IndexedStream::from(input).map(Length);
    /// let result = computation.run(&runtime).await?;
    /// // The output is an indexed stream, convert it into a sorted vec
    /// let vec_result = result.into_values_sorted().await
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
    /// #    directive::{Directive, IndexedStream},
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
    /// let computation = IndexedStream::from([1, 2, 3, 4, 5]).map(MultiplyBy(2));
    /// let result = computation.run(&runtime).await?;
    /// // The output is an indexed stream, convert it into a sorted vec
    /// let vec_result = result.into_values_sorted().await
    ///     .into_iter()
    ///     .collect::<Vec<_>>();
    ///
    /// assert_eq!(vec_result, vec![2, 4, 6, 8, 10]);
    /// # Ok(())
    /// # }
    /// ```
    fn map<Op, F>(self, op: Op) -> Map<Op, F, Self>
    where
        Op: Operation,
        F: Functor<Op::Output, A = Op::Input>,
        Self: Sized + Directive<Output = F>,
    {
        Map { op, input: self }
    }

    /// Fold this [`Directive`] over the given [`Monoid`] until a single value
    /// is produced.
    ///
    /// # Example
    /// ```
    /// # use paladin::{
    /// #    operation::{Operation, Monoid},
    /// #    directive::{Directive, IndexedStream},
    /// #    opkind_derive::OpKind,
    /// #    runtime::Runtime,
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
    /// # let runtime = Runtime::in_memory().await?;
    /// let computation = IndexedStream::from([1, 2, 3, 4, 5]).fold(Multiply);
    /// let result = computation.run(&runtime).await?;
    /// assert_eq!(result, 120);
    /// # Ok(())
    /// # }
    /// ```
    fn fold<M, F>(self, m: M) -> Fold<M, F, Self>
    where
        M: Monoid,
        F: Foldable<M::Elem, A = M::Elem>,
        Self: Sized + Directive<Output = F>,
    {
        Fold { m, input: self }
    }

    /// Run this [`Directive`] on the given [`Runtime`].
    ///
    /// This is where the evaluation semantics of a given [`Directive`] are
    /// defined.
    async fn run(self, runtime: &Runtime) -> Result<Self::Output>;
}

/// A macro for implementing [`Directive`] for trivial types.
///
/// We define "trivial type" here as a type that doesn't involve any semantics
/// other than lifting itself into the directive chain. Practically, this allows
/// arbitrary types to be chained with other higher order directives. These
/// types are typically used as leaves in the execution tree.
macro_rules! impl_lit {
    ($struct_name:ident) => {
        #[async_trait::async_trait]
        impl $crate::directive::Directive for $struct_name {
            type Input = Self;
            type Output = Self;

            async fn run(self, _: &$crate::runtime::Runtime) -> anyhow::Result<Self::Output> {
                Ok(self)
            }
        }
    };
    ($struct_name:ident<$($generics:ident),+>) => {
        #[async_trait::async_trait]
        impl<$($generics: Send),+> $crate::directive::Directive for $struct_name<$($generics),+> {
            type Input = Self;
            type Output = Self;

            async fn run(self, _: &$crate::runtime::Runtime) -> anyhow::Result<Self::Output> {
                Ok(self)
            }
        }
    };
}

/// Higher kinded type (HKT) trait.
///
/// Higher kinded types are types that depend on other types; often called "type
/// constructors". For example, [`Option`] is a _higher kinded_ type because it
/// depends on a type to be given to its type _constructor_ before it may be
/// used as a type.
pub trait HKT<U> {
    type A;
    type Target;
}

/// A macro for implementing [`HKT`] for `* -> *` kinded types.
///
/// A `* -> *` kinded type is a type constructor that take a single type as
/// input, like [`Option`].
/// ```
macro_rules! impl_hkt {
    ($t: ident) => {
        impl<T, U> $crate::directive::HKT<U> for $t<T> {
            type A = T;
            type Target = $t<U>;
        }
    };
}

/// A [`Functor`] represents some type that can be mapped over.
///
/// Implementing types must be higher kinded, in that they must be
/// parameterized by some other type(s).
///
/// Functors are _structure preserving_, in the sense that the implementing
/// type constructor is preserved after mapping. For example, mapping over an
/// [`Option`] will always return an [`Option`] -- it is _structure preserving_
/// -- only the value contained within the [`Option`] is touched.
#[async_trait]
pub trait Functor<B>: HKT<B> {
    async fn f_map<Op>(self, op: Op, runtime: &Runtime) -> Result<Self::Target>
    where
        Op: Operation<Input = Self::A, Output = B>;
}

/// A representation of an arbitrary [`Functor`] mapping.
///
/// This struct is what facilitates lazy evaluation of [`Functor`] mappings,
/// allowing them to be chained together and evaluated in a single pass.
///
/// Where a [`Functor`] defines the semantics of a mapping over a type, [`Map`]
/// represents an instance of a mapping over that type. In other words, [`Map`]
/// is simply a pairing of a [`Functor`] with the [`Operation`] that will be
/// mapped over it.
///
/// A subtle implementation detail is that we do not actually embed a
/// [`Functor`], but rather a [`Directive`] that outputs a [`Functor`]. This is
/// what allows us to chain [`Directive`]s together, as the output of one
/// [`Directive`] is the input to the next.
pub struct Map<Op: Operation, F: Functor<Op::Output, A = Op::Input>, D: Directive<Output = F>> {
    op: Op,
    input: D,
}

/// [`Directive`] implementation for [`Map`].
///
/// The implementation drives the evaluation of the input [`Directive`],
/// returning the input [`Functor`], and then mapping the given [`Operation`]
/// over it.
#[async_trait]
impl<Op: Operation, F: Functor<Op::Output, A = Op::Input> + Send, D: Directive<Output = F>>
    Directive for Map<Op, F, D>
{
    type Input = F;
    type Output = F::Target;

    async fn run(self, runtime: &Runtime) -> Result<Self::Output> {
        self.input.run(runtime).await?.f_map(self.op, runtime).await
    }
}

/// A [`Foldable`] represents some type that can be folded over with a
/// [`Monoid`].
///
/// In particular, the provided [`Monoid`] is used to combine the values of the
/// of the [`Foldable`] until a single value is produced. [`Foldable`]
/// implementations should preserve associativity of combination.
///
/// Implementing types must be higher kinded, in that they must be
/// parameterized by some other type(s).
#[async_trait]
pub trait Foldable<B>: HKT<B> {
    async fn f_fold<M>(self, m: M, runtime: &Runtime) -> Result<Self::A>
    where
        M: Monoid<Elem = Self::A>;
}

/// A representation of an arbitrary [`Foldable`] fold.
///
/// This struct is what facilitates lazy evaluation of [`Fold`]s, allowing them
/// to be chained together with other [`Directive`], and evaluated in a single
/// pass.
///
/// Where a [`Foldable`] defines the semantics of folding over a structure,
/// [`Fold`] represents an instance of a fold over that type. In other words,
/// [`Fold`] is simply a pairing of a [`Foldable`] with the [`Monoid`] that
/// will be used to combine its values.
///
/// A subtle implementation detail is that we do not actually embed a
/// [`Foldable`], but rather a [`Directive`] that outputs a [`Foldable`]. This
/// is what allows us to chain [`Directive`]s together, as the output of one
/// [`Directive`] is the input to the next.
pub struct Fold<M: Monoid, F: Foldable<M::Elem, A = M::Elem>, D: Directive<Output = F>> {
    m: M,
    input: D,
}

/// [`Directive`] implementation for [`Fold`].
///
/// The implementation drives the evaluation of the input [`Directive`],
/// returning the input [`Foldable`], and then folding the given [`Monoid`]
/// over it.
#[async_trait]
impl<M: Monoid, F: Foldable<M::Elem, A = M::Elem> + Send, D: Directive<Output = F>> Directive
    for Fold<M, F, D>
{
    type Input = F;
    type Output = M::Elem;

    async fn run(self, runtime: &Runtime) -> Result<Self::Output> {
        self.input.run(runtime).await?.f_fold(self.m, runtime).await
    }
}

pub mod indexed_stream;
pub use indexed_stream::IndexedStream;
