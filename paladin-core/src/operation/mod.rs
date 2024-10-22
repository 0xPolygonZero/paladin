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
//! use paladin::{RemoteExecute, operation::{Operation, Result}};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, RemoteExecute)]
//! struct StringLength;
//!
//! impl Operation for StringLength {
//!     type Input = String;
//!     type Output = usize;
//!     
//!     fn execute(&self, input: Self::Input) -> Result<Self::Output> {
//!         Ok(input.len())
//!     }
//! }
//! ```
//!
//! ### Defining a [`Monoid`]:
//!
//! ```
//! use paladin::{RemoteExecute, operation::{Monoid, Operation, Result}};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, RemoteExecute)]
//! struct StringConcat;
//!
//! impl Monoid for StringConcat {
//!     type Elem = String;
//!     
//!     fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem> {
//!         Ok(a + &b)
//!     }
//!     
//!     fn empty(&self) -> Self::Elem {
//!         String::new()
//!     }
//! }
//! ```
//!
//! ### An [`Operation`] with constructor arguments:
//!
//! ```
//! use paladin::{RemoteExecute, operation::{Monoid, Operation, Result}};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, RemoteExecute)]
//! struct MultiplyBy(i32);
//!
//! impl Operation for MultiplyBy {
//!     type Input = i32;
//!     type Output = i32;
//!     
//!     fn execute(&self, input: Self::Input) -> Result<Self::Output> {
//!         Ok(self.0 * input)
//!     }
//! }
//! ```
use std::fmt::Debug;

use bytes::Bytes;

use crate::serializer::{Serializable, Serializer};

/// An operation that is identifiable and executable by the runtime in a
/// distributed environment.
///
/// It is used to facilitate serialization, deserialization, and dynamic
/// execution of operations.
///
/// These will be automatically implemented by the
/// [`RemoteExecute`](crate::RemoteExecute) derive macro.
pub trait RemoteExecute {
    const ID: u8;
}

/// An operation that can be performed by a worker.
///
/// Akin to a function that maps an input to an output, it defines the signature
/// and semantics of a computation.
pub trait Operation: RemoteExecute + Serializable {
    /// The input type of the operation.
    type Input: Serializable + Debug;
    /// The output type of the operation.
    type Output: Serializable + Debug;

    /// Execute the operation on the given input.
    fn execute(
        &self,
        input: Self::Input,
        abort_signal: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<Self::Output>;

    /// Get the input from a byte representation.
    fn input_from_bytes(&self, serializer: Serializer, input: &[u8]) -> Result<Self::Input> {
        Ok(serializer
            .from_bytes(input)
            .map_err(|err| FatalError::from_anyhow(err, Default::default()))?)
    }

    /// Get a byte representation of the output.
    fn output_to_bytes(&self, serializer: Serializer, output: Self::Output) -> Result<Bytes> {
        Ok(serializer
            .to_bytes(&output)
            .map_err(|err| FatalError::from_anyhow(err, Default::default()))?)
    }

    /// Execute the operation on the given input as bytes.
    fn execute_as_bytes(
        &self,
        serializer: Serializer,
        input: &[u8],
        abort_signal: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<Bytes> {
        self.input_from_bytes(serializer, input)
            .and_then(|input| self.execute(input, abort_signal))
            .and_then(|output| self.output_to_bytes(serializer, output))
    }

    /// Get a byte representation of the operation.
    fn as_bytes(&self, serializer: Serializer) -> Result<Bytes> {
        let output = serializer
            .to_bytes(self)
            .map_err(|err| FatalError::from_anyhow(err, Default::default()))?;
        Ok(output)
    }

    fn from_bytes(serializer: Serializer, input: &[u8]) -> Result<Self> {
        let this = serializer
            .from_bytes(input)
            .map_err(|err| FatalError::from_anyhow(err, Default::default()))?;
        Ok(this)
    }
}

/// An associative binary [`Operation`].
pub trait Monoid: RemoteExecute + Serializable {
    /// The type of the elements that can be combined by the operation.
    /// Note that unlike an [`Operation`], a [`Monoid`] is a binary operation on
    /// elements of the same type. As such, it does not have distinct
    /// `Input` and `Output` types, but rather, a single `Elem` type.
    type Elem: Serializable + Debug;

    /// Get the identity element of the operation. Practically, this will be
    /// used when attempting to fold over a collection of elements, and that
    /// collection is empty.
    fn empty(&self) -> Self::Elem;

    /// Combine two elements using the operation.
    fn combine(
        &self,
        a: Self::Elem,
        b: Self::Elem,
        abort_signal: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<Self::Elem>;
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

    /// Execute the operation on the given input.
    fn execute(
        &self,
        (a, b): Self::Input,
        abort_signal: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<Self::Output> {
        self.combine(a, b, abort_signal)
    }
}

/// Marker types for [`Operation`]s.
pub mod marker {
    /// A [`Marker`] that can be used like [`Phantom`](std::marker::PhantomData)
    /// to force module inclusion of [`Operation`](super::Operation)
    /// implementations.
    #[derive(Clone, Copy)]
    pub struct Marker;
}

/// Generate an operation registry for external crates.
///
/// This macro generates a `register()` function that can be used to force
/// module inclusion of [`Operation`] implementations.
///
/// Note this is only necessary for operations that are defined in a crate
/// external to the worker runtime instantiation. If the operations are defined
/// in the same crate as the worker runtime instantiation, then the operations
/// will naturally be included in the worker runtime binary.
///
/// The `register()` function must be defined _within_ the external operations
/// module such that it is imported and called _from_ the worker module. This
/// scheme ensures that the compiler does not exclude the external operations
/// from its compilation.
///
/// **You will have problems if your operations are defined in a separate crate
/// from your worker runtime and you do not call this macro from within your
/// operation module.**
///
/// # Example
/// Let's say you have a workspace with the following structure:
/// ```text
/// my_workspace
/// ├── my_operations
/// │   ├── Cargo.toml
/// │   └── src
/// │       └── lib.rs
/// └── my_worker
///    ├── Cargo.toml
///    └── src
///         └── main.rs
/// ```
///
/// In `my_operations`, you define your operations:
/// ```
/// use paladin::{registry, RemoteExecute, operation::{Operation, Result}};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, RemoteExecute)]
/// struct MyOperation;
///
/// impl Operation for MyOperation {
///     type Input = ();
///     type Output = ();
///     
///     fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
///         Ok(())
///     }
/// }
///
/// // A `register()` function is generated and exported by the `registry!()` macro.
/// // This must be called from within the operation module.
/// registry!();
/// ```
///
/// In `my_worker`, you instantiate the worker runtime:
/// ```
/// # mod my_operations {
/// #    use paladin::{registry, RemoteExecute, operation::{Operation, Result}};
/// #    use serde::{Deserialize, Serialize};
/// #
/// #    #[derive(Serialize, Deserialize, RemoteExecute)]
/// #    struct MyOperation;
/// #
/// #    impl Operation for MyOperation {
/// #        type Input = ();
/// #        type Output = ();
/// #    
/// #        fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
/// #            Ok(())
/// #        }
/// #    }
/// #
/// #    registry!();
/// # }
/// #
/// use paladin::{runtime::WorkerRuntime, config::{Config, Runtime}};
/// use my_operations::register;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let config = Config {
///         runtime: Runtime::InMemory,
///         ..Default::default()
///     };
///
///     let runtime = WorkerRuntime::from_config(&config, register()).await?;
///
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! registry {
    () => {
        /// Register external operations with the runtime.
        #[inline(never)]
        pub fn register() -> ::paladin::operation::marker::Marker {
            ::paladin::operation::marker::Marker
        }
    };
}

mod error;
pub use error::*;
