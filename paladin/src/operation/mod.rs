//! Remote operation traits.
//!
//! This module defines the core traits that enable users to define the
//! semantics of their system. Operations define the signature and semantics of
//! a computation. Akin to a function that maps an input to an output.
//!
//! Key components of this module include:
//! ## [`Operation`]
//! Represents a generic operation that can be executed by a remote machine.
//! It defines the signature and semantics of a computation.
//!
//! ## [`Monoid`]
//! Represents a binary [`Operation`] whose elements can be  combined in an
//! associative manner. It's a specialized form of an [`Operation`] that has an
//! identity element and an associative binary operation. This makes it
//! _foldable_.
//!
//! ## [`Operation`] implementation of [`Monoid`]
//! An automatic implementation of the [`Operation`] trait for [`Monoid`]s trait
//! is provided. An [`Operation`] is strictly more general than a [`Monoid`], so
//! we can trivially derive an [`Operation`] for every [`Monoid`].
//!
//! ## [`OpKind`]
//! A trait used to create a registry of all available operations.It's used to
//! facilitate serialization, deserialization, and remote dynamic execution of
//! operations.
//!
//! # Notes
//!
//! - Operations are monomorphic over their implementing type. This is
//!   illustrated by the fact that the `Input` and `Output` types are
//!   _associated_ types of the [`Operation`] trait. This obviates the need to
//!   make [`OpKind`] enums generic and thus simplifies downstream code by
//!   removing the need for threading generic parameters through the system. It
//!   is, however, possible to implement [`Operation`]s for generic types. see
//!   the [example](#defining-a-polymorphic-monoid). They just must be
//!   monomorphized in the enum definition. It is not yet clear that
//!   polymorphism at the level of [`OpKind`] needed. This may change as the
//!   system sees more use, in which case we can revisit this design decision.
//!
//! # Usage:
//! Operations are the semantic building blocks of the system. They define what
//! computations can be performed by the system. By implementing the
//! [`Operation`] trait for a type, it can be remotely executed. Operations are
//! not necessarily meant to be executed directly, but rather embedded into
//! [`Task`](crate::task::Task)s, which contain operation arguments, additional
//! metadata, and routing information to facilitate remote execution.
//!
//! ## Example
//! ### Defining an [`Operation`]:
//!
//! ```
//! use paladin::{operation::Operation, opkind_derive::OpKind};
//! use serde::{Deserialize, Serialize};
//! use anyhow::Result;
//!
//! #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
//! struct StringLength;
//!
//! impl Operation for StringLength {
//!     type Input = String;
//!     type Output = usize;
//!     type Kind = MyOps;
//!     
//!     fn execute(&self, input: Self::Input) -> Result<Self::Output> {
//!         Ok(input.len())
//!     }
//! }
//!
//! #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
//! enum MyOps {
//!    StringLength(StringLength),
//! }
//! ```
//!
//! ### Defining a [`Monoid`]:
//!
//! ```
//! use paladin::{operation::{Monoid, Operation}, opkind_derive::OpKind};
//! use serde::{Deserialize, Serialize};
//! use anyhow::Result;
//!
//! #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
//! struct StringConcat;
//!
//! impl Monoid for StringConcat {
//!     type Elem = String;
//!     type Kind = MyOps;
//!     
//!     fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem> {
//!         Ok(a + &b)
//!     }
//!     
//!     fn empty(&self) -> Self::Elem {
//!         String::new()
//!     }
//! }
//!
//! #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
//! enum MyOps {
//!    StringConcat(StringConcat),
//! }
//! ```
//!
//! ### An [`Operation`] with constructor arguments:
//!
//! ```
//! use paladin::{operation::{Monoid, Operation}, opkind_derive::OpKind};
//! use serde::{Deserialize, Serialize};
//! use anyhow::Result;
//!
//! #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
//! struct MultiplyBy(i32);
//!
//! impl Operation for MultiplyBy {
//!     type Input = i32;
//!     type Output = i32;
//!     type Kind = MyOps;
//!     
//!     fn execute(&self, input: Self::Input) -> Result<Self::Output> {
//!         Ok(self.0 * input)
//!     }
//! }
//!
//! #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
//! enum MyOps {
//!     MultiplyBy(MultiplyBy),
//! }
//! ```
//!
//! ### Defining a polymorphic [`Monoid`]:
//!
//! ```
//! use paladin::{
//!     operation::{Monoid, Operation},
//!     opkind_derive::OpKind,
//!     serializer::Serializable,
//! };
//! use serde::{Deserialize, Serialize};
//! use anyhow::Result;
//! use std::{ops::Mul, fmt::Debug};
//! use num_traits::One;
//!
//! #[derive(Serialize, Deserialize, Debug, Clone, Copy, Default)]
//! struct GenericMultiplication<T>(std::marker::PhantomData<T>);
//!
//! impl<T: Mul<Output = T> + One + Serializable + Debug + Clone> Monoid for GenericMultiplication<T>
//! where
//!     MyOps: From<GenericMultiplication<T>>,
//! {
//!     type Elem = T;
//!     type Kind = MyOps;
//!
//!     fn empty(&self) -> Self::Elem {
//!         T::one()
//!     }
//!
//!     fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem> {
//!         Ok(a * b)
//!     }
//! }
//!
//! #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
//! enum MyOps {
//!     // Monomorphize
//!     GenericMultiplicationI32(GenericMultiplication<i32>),
//!     GenericMultiplicationI64(GenericMultiplication<i64>),
//! }
//! ```
//!
//! Later on ...
//! ```
//! # use paladin::{
//! #    operation::{Monoid, Operation},
//! #    opkind_derive::OpKind,
//! #    serializer::Serializable,
//! # };
//! # use serde::{Deserialize, Serialize};
//! # use anyhow::Result;
//! # use std::{ops::Mul, fmt::Debug};
//! # use num_traits::One;
//! #
//! # #[derive(Serialize, Deserialize, Debug, Clone, Copy, Default)]
//! # struct GenericMultiplication<T>(std::marker::PhantomData<T>);
//! #
//! # impl<T: Mul<Output = T> + One + Serializable + Debug + Clone> Monoid for GenericMultiplication<T>
//! # where
//! #    MyOps: From<GenericMultiplication<T>>,
//! # {
//! #    type Elem = T;
//! #    type Kind = MyOps;
//! #
//! #    fn empty(&self) -> Self::Elem {
//! #        T::one()
//! #    }
//! #
//! #    fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem> {
//! #        Ok(a * b)
//! #    }
//! # }
//! #
//! # #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
//! # enum MyOps {
//! #    // Monomorphize
//! #    GenericMultiplicationI32(GenericMultiplication<i32>),
//! #    GenericMultiplicationI64(GenericMultiplication<i64>),
//! # }
//! #
//! use paladin::{
//!     directive::{fold, indexed, lit},
//! };
//!
//! fn main () {
//!     let expr = fold(
//!         GenericMultiplication::<i32>::default(),
//!         lit(indexed([1, 2, 3, 4, 5, 6])),
//!     );
//! }
//! ```

use crate::serializer::Serializable;
use anyhow::Result;
use std::fmt::Debug;

/// An operation that can be performed by a worker.
///
/// Akin to a function that maps an input to an output, it defines the signature
/// and semantics of a computation.
pub trait Operation: Serializable + Clone + Debug + Into<Self::Kind> {
    /// The input type of the operation.
    type Input: Serializable + Debug;
    /// The output type of the operation.
    type Output: Serializable + Debug;
    /// The operation registry type.
    type Kind: OpKind;

    /// Execute the operation on the given input.
    fn execute(&self, input: Self::Input) -> Result<Self::Output>;
}

/// A registry of all available operations.
///
/// Implementations MUST be an enum. The implementation should group all
/// available [`Operation`]s into a single registry. This enables operations to
/// be serialized and executed by a remote service in an opaque manner. In
/// particular, the remote service need not know the exact type of operation,
/// but rather, only the _kind_ of possible operations.
///
/// Note that a [`Runtime`](crate::runtime::Runtime) instance expects to be
/// specialized with an [`OpKind`] type -- this is what enables dynamic
/// execution behavior across all available operations.
///
/// # Design rationale
/// In a world where operations are all executed in the same process, `Box<dyn
/// Operation>` would theoretically suffice. However, in a distributed system,
/// we need to serialize operations and send them back and forth between remote
/// machines. Generic trait object serialization / deserialization is
/// non-trivial and is not supported by [`serde`] out of the box. Solutions
/// like <https://docs.rs/typetag> were explored, but, unfortunately, they do
/// not support generic types.
///
/// A `proc_macro_derive` is provided to simplify the process of facilitating
/// opaque execution of operations, [`crate::opkind_derive`]. It is highly
/// recommended to use this derive macro to implement [`OpKind`] for your
/// operation registry, as there are a lot of boilerplate details that need to
/// be taken care of.
pub trait OpKind: Serializable + Clone + Debug {}

/// An associative binary [`Operation`].
pub trait Monoid: Serializable + Clone + Debug + Into<Self::Kind> {
    /// The type of the elements that can be combined by the operation.
    /// Note that unlike an [`Operation`], a [`Monoid`] is a binary operation on
    /// elements of the same type. As such, it does not have distinct
    /// `Input` and `Output` types, but rather, a single `Elem` type.
    type Elem: Serializable + Debug;

    /// The operation registry type.
    type Kind: OpKind;

    /// Get the identity element of the operation. Practically, this will be
    /// used when attempting to fold over a collection of elements, and that
    /// collection is empty.
    fn empty(&self) -> Self::Elem;

    /// Combine two elements using the operation.
    fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem>;
}

/// Implement the [`Operation`] trait for types that support the binary
/// operation
impl<T> Operation for T
where
    T: Monoid,
{
    /// The input type is a tuple of two elements of the same type.
    type Input = (T::Elem, T::Elem);
    /// The output type is a single, combined, element of the same type.
    type Output = T::Elem;

    type Kind = T::Kind;

    /// Execute the operation on the given input.
    fn execute(&self, (a, b): Self::Input) -> Result<Self::Output> {
        self.combine(a, b)
    }
}
