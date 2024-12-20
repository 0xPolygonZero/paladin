#![cfg_attr(docsrs, feature(doc_cfg))]

//! Distributed computation library for Rust.
//!
//! Paladin aims to simplify the challenge of writing distributed programs. It
//! provides a declarative API, allowing developers to articulate their
//! distributed programs clearly and concisely, without thinking about the
//! complexities of distributed systems programming.
//!
//! Features:
//! - **Declarative API**: Express distributed computations with clarity and
//!   ease.
//! - **Automated Distribution**: Paladin’s runtime seamlessly handles task
//!   distribution and execution across the cluster.
//! - **Simplified Development**: Concentrate on the program logic, leaving the
//!   complexities of distributed systems to Paladin.
//! - **Infrastructure Agnostic**: Paladin is generic over its messaging backend
//!   and infrastructure provider. Bring your own infra!
//!
//! # How to use Paladin
//!
//! When writing your programs, you will interact with two core APIs,
//! [`Operation`](crate::operation::Operation)s and
//! [`Directive`](crate::directive::Directive)s. You will define your
//! distributed computations in terms of
//! [`Operation`](crate::operation::Operation)s, and construct your programs
//! using Paladin's provided [`Directive`](crate::directive::Directive)s.
//!
//! In general, Paladin assumes that distributed programs will fundamentally
//! operate over a stream or async-iterator-like data structure. In particular,
//! Paladin's [`Directive`](crate::directive::Directive) API operates over
//! [functorial](crate::directive::Functor) or
//! [foldable](crate::directive::Foldable) data structures, providing methods
//! [`map`](crate::directive::Directive::map) and
//! [`fold`](crate::directive::Directive::fold), respectively. This
//! generalization allows Paladin to support a wide variety of parallel data
//! structures. Paladin provides one such data structure out of the box,
//! [`IndexedStream`](crate::directive::indexed_stream::IndexedStream).
//! [`IndexedStream`](crate::directive::indexed_stream::IndexedStream)
//! implements both the [`Functor`](crate::directive::Functor) and
//! [`Foldable`](crate::directive::Foldable) traits, and as such should be able
//! to handle any distributed algorithm that can be expressed in terms of
//! [`map`](crate::directive::Directive::map) and
//! [`fold`](crate::directive::Directive::fold). We recommend using
//! [`IndexedStream`](crate::directive::indexed_stream::IndexedStream) for all
//! your programs, as it has been highly optimized for parallelism, although
//! of course you are free to define your own.
//!
//! ## Defining Operations
//!
//! Operations are the semantic building blocks of your system. By implementing
//! [`Operation`](crate::operation::Operation) for a type, it can be given to
//! [`Directive`](crate::directive::Directive)s and remotely executed.
//!
//! ```
//! use paladin::{RemoteExecute, operation::{Operation, Result}, AbortSignal,};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, RemoteExecute)]
//! struct FibAt;
//!
//! impl Operation for FibAt {
//!     type Input = u64;
//!     type Output = u64;
//!
//!     fn execute(&self, input: Self::Input, abort: AbortSignal) -> Result<Self::Output> {
//!         match input {
//!             0 => Ok(0),
//!             1 => Ok(1),
//!             _ => {
//!                 let mut a = 0;
//!                 let mut b = 1;
//!                 for _ in 2..=input {
//!                     let temp = a;
//!                     a = b;
//!                     b = temp + b;
//!                 }
//!                 Ok(b)
//!             }
//!         }
//!     }
//! }
//!
//! # fn main() {
//! assert_eq!(FibAt.execute(10, None).unwrap(), 55);
//! # }
//! ```
//!
//! ## Constructing a program
//!
//! Once operations have been defined, they can be plugged into Paladin's
//! [`Directive`](crate::directive::Directive)s to construct a distributed
//! program.
//!
//! ```
//! use paladin::{RemoteExecute, operation::{Operation, Result}, AbortSignal};
//! use serde::{Deserialize, Serialize};
//! #
//! # #[derive(Serialize, Deserialize, RemoteExecute)]
//! # struct FibAt;
//! #
//! # impl Operation for FibAt {
//! #    type Input = u64;
//! #    type Output = u64;
//! #
//! #    fn execute(&self, input: Self::Input, abort: AbortSignal) -> Result<Self::Output> {
//! #        match input {
//! #            0 => Ok(0),
//! #            1 => Ok(1),
//! #            _ => {
//! #                let mut a = 0;
//! #                let mut b = 1;
//! #                for _ in 2..=input {
//! #                    let temp = a;
//! #                    a = b;
//! #                    b = temp + b;
//! #                }
//! #                Ok(b)
//! #            }
//! #        }
//! #    }
//! # }
//! #
//! use paladin::{
//!     operation::Monoid,
//!     directive::{indexed_stream::IndexedStream, Directive},
//!     runtime::Runtime,
//! };
//!
//! // Define a Sum monoid.
//! #[derive(Serialize, Deserialize, RemoteExecute)]
//! struct Sum;
//!
//! impl Monoid for Sum {
//!     type Elem = u64;
//!
//!     fn combine(&self, a: Self::Elem, b: Self::Elem, abort: AbortSignal) -> Result<Self::Elem> {
//!        Ok(a + b)
//!     }
//!
//!     fn empty(&self) -> Self::Elem {
//!        0
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let runtime = Runtime::in_memory().await?;
//!     let stream = IndexedStream::from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
//!     // Compute the fibonacci number at each element in the stream with our
//!     // previously declared `FibAt` operation.
//!     let fibs = stream.map(&FibAt);
//!     // Sum the fibonacci numbers.
//!     let sum = fibs.fold(&Sum);
//!
//!     // Run the computation.
//!     let result = sum.run(&runtime).await;
//!
//!     // Close the runtime
//!     runtime.close().await?;
//!
//!     assert_eq!(result?, 143);
//! #   Ok(())
//! }
//! ```
//!
//! In this example program, we define an algorithm for computing the fibonacci
//! number at each element in a stream, and then summing the results. Behind the
//! scenes, Paladin will distribute the computations across the cluster and
//! return the result back to the main thread. Note that in this example, we're
//! using Paladin's multi-threaded in-memory runtime, which can be useful for
//! testing and debugging. In a real-world setting, one would use a distributed
//! runtime, such as Paladin's AMQP runtime.
//!
//! ## Application and deployment architecture
//!
//! We suggest the following project layout:
//! ```bash
//! ops
//! ├── Cargo.toml
//! └── src
//!    └── lib.rs
//! worker
//! ├── Cargo.toml
//! └── src
//!    └── main.rs
//! leader
//! ├── Cargo.toml
//! └── src
//!    └── main.rs
//! ```
//!
//! Here's a breakdown:
//! - `ops`: A library with your operation definitions and their registry,
//!   shared between `worker` and `leader`.
//! - `worker`: Executes operations on remote machines.
//! - `leader`: Coordinates the distributed computation.
//!
//! For deployment, use one `leader` for multiple `workers`. Currently, only one
//! `leader` deployment is supported. Future versions might offer state
//! persistence for better failover and fault tolerance.
//!
//! ### Worker main
//!
//! Given that virtually all workers will do exactly the same thing, Paladin's
//! worker runtime provides a
//! [`WorkerRuntime::main_loop`](crate::runtime::WorkerRuntime::main_loop). In
//! general, most of your logic should exist in `ops` and `leader`.
pub mod acker;
pub mod channel;
pub mod common;
pub mod config;
pub mod contiguous;
pub mod directive;
pub mod operation;
pub mod queue;
pub mod runtime;
pub mod serializer;
pub mod task;
pub use async_trait::async_trait;
pub use paladin_opkind_derive::*;

pub type AbortSignal = Option<std::sync::Arc<std::sync::atomic::AtomicBool>>;

// Not public API. Used by generated code.
#[doc(hidden)]
pub mod __private {
    #[doc(hidden)]
    pub use bytes;
    #[doc(hidden)]
    pub use futures;
    #[doc(hidden)]
    pub use linkme;
    #[doc(hidden)]
    pub use tokio;
    #[doc(hidden)]
    pub use tracing;

    #[doc(hidden)]
    #[linkme::distributed_slice]
    pub static OPERATIONS: [fn(
        crate::task::AnyTask,
        abort: crate::AbortSignal,
    ) -> futures::future::BoxFuture<
        'static,
        crate::operation::Result<crate::task::AnyTaskOutput>,
    >];
}
